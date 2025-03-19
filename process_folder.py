import xml.etree.ElementTree as ET
import gzip
import os
import re
import psycopg2
import psycopg2.extras
import concurrent.futures
import io
import time
import logging
import argparse
import sys
from typing import Dict, List, Set, Optional, Any
from contextlib import contextmanager
from functools import lru_cache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("pubmed_processor.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("PubMedProcessor")

# Constants for performance tuning
CHUNK_SIZE = 5000  # Number of records to process at once
BATCH_SIZE = 1000  # Number of records to insert in a single COPY operation
MAX_WORKERS = 8  # Number of parallel workers (adjust based on CPU cores)
FILE_BUFFER_SIZE = 64  # Buffer size in MB for file I/O
DB_CONNECTION_TIMEOUT = 60  # Database connection timeout in seconds
LRU_CACHE_SIZE = 10000  # Size of normalization cache

# Default connection config
DB_CONFIG = {
    "host": "localhost",
    "database": "pubmed_integration",
    "user": "pubmed",
    "password": "pubmed_password",
    "application_name": "pubmed_processor",
}


@contextmanager
def timed_operation(description: str):
    """Context manager to time operations."""
    start_time = time.time()
    logger.info(f"Starting {description}")
    try:
        yield
    finally:
        elapsed = time.time() - start_time
        logger.info(f"Completed {description} in {elapsed:.2f} seconds")


@lru_cache(maxsize=LRU_CACHE_SIZE)
def normalize_text(text: str) -> str:
    """Normalize text with caching for better performance."""
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[^\w\s]", "", text)
    return text.strip()


class DatabaseManager:
    """Manages database connections and operations with performance optimizations."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.setup_db()

    def setup_db(self):
        """Setup database schema and indexes if needed."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Check if we need to create temporary tables
                cursor.execute(
                    """
                    CREATE TEMP TABLE IF NOT EXISTS pubmed_records_temp (
                        pmid VARCHAR(20),
                        title TEXT NOT NULL,
                        title_normalized TEXT,
                        journal VARCHAR(255),
                        volume VARCHAR(50),
                        issue VARCHAR(50),
                        year INT,
                        abstract TEXT,
                        source_file VARCHAR(255)
                    )
                """
                )

                cursor.execute(
                    """
                    CREATE TEMP TABLE IF NOT EXISTS pubmed_authors_temp (
                        pmid VARCHAR(20),
                        author_name TEXT NOT NULL,
                        position INT
                    )
                """
                )

                cursor.execute(
                    """
                    CREATE TEMP TABLE IF NOT EXISTS pubmed_mesh_temp (
                        pmid VARCHAR(20),
                        mesh_term TEXT NOT NULL
                    )
                """
                )

                conn.commit()

    @contextmanager
    def get_connection(self):
        """Get a connection with optimal settings."""
        conn = psycopg2.connect(
            **self.config,
            connect_timeout=DB_CONNECTION_TIMEOUT,
            options="-c statement_timeout=3600000",  # 1-hour statement timeout
        )
        try:
            # Optimize connection settings
            with conn.cursor() as cursor:
                # Set appropriate work_mem for bulk operations
                cursor.execute("SET work_mem = '128MB'")
                # Use faster but less strict transactions
                cursor.execute("SET synchronous_commit = off")
                cursor.execute("SET max_parallel_workers_per_gather = 4")
            conn.commit()
            yield conn
        finally:
            conn.close()

    def clear_temp_tables(self, conn):
        """Clear temporary tables or recreate them if they don't exist."""
        with conn.cursor() as cursor:
            try:
                cursor.execute("TRUNCATE TABLE pubmed_records_temp")
                cursor.execute("TRUNCATE TABLE pubmed_authors_temp")
                cursor.execute("TRUNCATE TABLE pubmed_mesh_temp")
            except psycopg2.errors.UndefinedTable:
                # Tables don't exist yet in this session, create them
                self.setup_temp_tables(conn)
            conn.commit()

    def setup_temp_tables(self, conn):
        """Setup temporary tables for this session."""
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TEMP TABLE pubmed_records_temp (
                    pmid VARCHAR(20),
                    title TEXT NOT NULL,
                    title_normalized TEXT,
                    journal VARCHAR(255),
                    volume VARCHAR(50),
                    issue VARCHAR(50),
                    year INT,
                    abstract TEXT,
                    source_file VARCHAR(255)
                )
            """
            )

            cursor.execute(
                """
                CREATE TEMP TABLE pubmed_authors_temp (
                    pmid VARCHAR(20),
                    author_name TEXT NOT NULL,
                    position INT
                )
            """
            )

            cursor.execute(
                """
                CREATE TEMP TABLE pubmed_mesh_temp (
                    pmid VARCHAR(20),
                    mesh_term TEXT NOT NULL
                )
            """
            )

            conn.commit()

    def merge_temp_tables(self, conn):
        """Merge data from temporary tables into main tables."""
        with timed_operation("merging temporary tables into main tables"):
            with conn.cursor() as cursor:
                # Insert authors first to get their IDs
                cursor.execute(
                    """
                    INSERT INTO authors (name, name_normalized)
                    SELECT DISTINCT author_name, normalize_text(author_name)
                    FROM pubmed_authors_temp
                    ON CONFLICT (name) DO NOTHING
                """
                )

                # Insert mesh terms
                cursor.execute(
                    """
                    INSERT INTO mesh_terms (term)
                    SELECT DISTINCT mesh_term
                    FROM pubmed_mesh_temp
                    ON CONFLICT (term) DO NOTHING
                """
                )

                # Insert pubmed records
                cursor.execute(
                    """
                    INSERT INTO pubmed_records 
                    (pmid, title, title_normalized, journal, volume, issue, year, abstract, source_file)
                    SELECT 
                        pmid, title, title_normalized, journal, volume, issue, year, abstract, source_file
                    FROM pubmed_records_temp
                    ON CONFLICT (pmid) DO UPDATE SET
                        title = EXCLUDED.title,
                        title_normalized = EXCLUDED.title_normalized,
                        journal = EXCLUDED.journal,
                        volume = EXCLUDED.volume,
                        issue = EXCLUDED.issue,
                        year = EXCLUDED.year,
                        abstract = EXCLUDED.abstract,
                        source_file = EXCLUDED.source_file,
                        processed_at = NOW()
                """
                )

                # Link authors to pubmed records
                cursor.execute(
                    """
                    INSERT INTO pubmed_authors (pubmed_id, author_id, position)
                    SELECT 
                        t.pmid, a.id, t.position
                    FROM pubmed_authors_temp t
                    JOIN authors a ON t.author_name = a.name
                    ON CONFLICT (pubmed_id, author_id) DO UPDATE SET
                        position = EXCLUDED.position
                """
                )

                # Link mesh terms to pubmed records
                cursor.execute(
                    """
                    INSERT INTO pubmed_mesh_terms (pubmed_id, mesh_id)
                    SELECT 
                        t.pmid, m.id
                    FROM pubmed_mesh_temp t
                    JOIN mesh_terms m ON t.mesh_term = m.term
                    ON CONFLICT (pubmed_id, mesh_id) DO NOTHING
                """
                )

                conn.commit()

            logger.info("Successfully merged temporary tables")


class PubMedXmlProcessor:
    """Processes PubMed XML files with optimized streaming and memory efficiency."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.processed_files: Set[str] = set()

    def process_folder(self, folder_path: str) -> int:
        """Process all XML files in a folder."""
        files = [
            os.path.join(folder_path, f)
            for f in os.listdir(folder_path)
            if f.endswith(".xml.gz") and os.path.isfile(os.path.join(folder_path, f))
        ]

        total_files = len(files)
        logger.info(f"Found {total_files} XML files in {folder_path}")

        # Process files in parallel
        total_records = 0
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=MAX_WORKERS
        ) as executor:
            # Process files in batches to control memory usage
            for i in range(0, len(files), CHUNK_SIZE):
                chunk = files[i : i + CHUNK_SIZE]
                logger.info(
                    f"Processing batch of {len(chunk)} files ({i+1}-{i+len(chunk)}/{total_files})"
                )

                # Submit all files in this chunk for processing
                future_to_file = {
                    executor.submit(self.process_xml_file, file_path): file_path
                    for file_path in chunk
                }

                # Collect results as they complete
                batch_records = 0
                batch_results = []

                for future in concurrent.futures.as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        result = future.result()
                        if result:
                            batch_results.append(result)
                            batch_records += sum(len(data) for data in result.values())
                            self.processed_files.add(os.path.basename(file_path))
                    except Exception as e:
                        logger.error(f"Error processing {file_path}: {e}")

                # Import this batch of results to database
                if batch_results:
                    self.import_batch_to_database(batch_results)
                    total_records += batch_records

                logger.info(f"Completed batch. Total records so far: {total_records}")

        return total_records

    def process_xml_file(self, file_path: str) -> Optional[Dict[str, List]]:
        """Process a single XML file and return extracted data."""
        file_name = os.path.basename(file_path)

        # Skip if already processed
        if file_name in self.processed_files:
            logger.info(f"Skipping already processed file: {file_name}")
            return None

        logger.info(f"Processing {file_name}")

        pubmed_records = []
        author_links = []
        mesh_terms = []

        try:
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                # Use iterparse for memory efficiency
                context = ET.iterparse(f, events=("start", "end"))

                # Current state variables
                in_pubmed_article = False
                current_pmid = None
                current_title = None
                current_journal = None
                current_volume = None
                current_issue = None
                current_year = None
                current_abstract = None
                current_authors = []
                current_mesh = []

                # Element path tracking
                path = []

                for event, elem in context:
                    if event == "start":
                        path.append(elem.tag)

                        if elem.tag == "PubmedArticle":
                            in_pubmed_article = True
                            current_pmid = None
                            current_title = None
                            current_journal = None
                            current_volume = None
                            current_issue = None
                            current_year = None
                            current_abstract = None
                            current_authors = []
                            current_mesh = []

                    elif event == "end":
                        if (
                            elem.tag == "PMID"
                            and in_pubmed_article
                            and "/".join(path[:-1]).endswith("MedlineCitation")
                        ):
                            current_pmid = elem.text

                        elif elem.tag == "ArticleTitle" and in_pubmed_article:
                            current_title = elem.text

                        elif elem.tag == "Title" and "/".join(path[:-1]).endswith(
                            "Journal"
                        ):
                            current_journal = elem.text

                        elif elem.tag == "Volume" and in_pubmed_article:
                            current_volume = elem.text

                        elif elem.tag == "Issue" and in_pubmed_article:
                            current_issue = elem.text

                        elif elem.tag == "Year" and "/".join(path[:-1]).endswith(
                            "PubDate"
                        ):
                            current_year = elem.text

                        elif elem.tag == "AbstractText" and in_pubmed_article:
                            if current_abstract is None:
                                current_abstract = elem.text
                            elif elem.text:
                                current_abstract += " " + elem.text

                        elif elem.tag == "Author" and in_pubmed_article:
                            # Process author
                            last_name = elem.findtext("LastName", "")
                            fore_name = elem.findtext("ForeName", "")
                            author_name = f"{fore_name} {last_name}".strip()
                            if author_name:
                                current_authors.append(author_name)

                        elif elem.tag == "DescriptorName" and in_pubmed_article:
                            if elem.text:
                                current_mesh.append(elem.text)

                        elif elem.tag == "PubmedArticle":
                            # End of article, add to records if we have essential data
                            if current_pmid and current_title:
                                # Add record
                                pubmed_records.append(
                                    [
                                        current_pmid,
                                        current_title,
                                        normalize_text(current_title),
                                        current_journal,
                                        current_volume,
                                        current_issue,
                                        current_year,
                                        current_abstract,
                                        file_name,
                                    ]
                                )

                                # Add authors
                                for i, author in enumerate(current_authors):
                                    author_links.append([current_pmid, author, i + 1])

                                # Add mesh terms
                                for term in current_mesh:
                                    mesh_terms.append([current_pmid, term])

                            in_pubmed_article = False

                        # Pop the path
                        path.pop()

                        # Clear element to save memory
                        elem.clear()

            logger.info(f"Extracted {len(pubmed_records)} records from {file_name}")

            return {
                "pubmed_records": pubmed_records,
                "author_links": author_links,
                "mesh_terms": mesh_terms,
            }

        except Exception as e:
            logger.error(f"Error processing {file_name}: {e}")
            return None

    def import_batch_to_database(self, batch_results: List[Dict[str, List]]):
        """Import a batch of results to the database using COPY for efficiency."""
        with timed_operation("importing batch to database"):
            # Aggregate all data from the batch
            all_records = []
            all_authors = []
            all_mesh = []

            for result in batch_results:
                all_records.extend(result.get("pubmed_records", []))
                all_authors.extend(result.get("author_links", []))
                all_mesh.extend(result.get("mesh_terms", []))

            with self.db_manager.get_connection() as conn:
                # Clear temporary tables
                self.db_manager.clear_temp_tables(conn)

                # Use COPY for fast import
                with conn.cursor() as cursor:
                    # Import pubmed records
                    if all_records:
                        self._copy_data(
                            cursor,
                            "pubmed_records_temp",
                            all_records,
                            columns=[
                                "pmid",
                                "title",
                                "title_normalized",
                                "journal",
                                "volume",
                                "issue",
                                "year",
                                "abstract",
                                "source_file",
                            ],
                        )

                    # Import authors
                    if all_authors:
                        self._copy_data(
                            cursor,
                            "pubmed_authors_temp",
                            all_authors,
                            columns=["pmid", "author_name", "position"],
                        )

                    # Import mesh terms
                    if all_mesh:
                        self._copy_data(
                            cursor,
                            "pubmed_mesh_temp",
                            all_mesh,
                            columns=["pmid", "mesh_term"],
                        )

                # Merge the data from temporary tables to main tables
                self.db_manager.merge_temp_tables(conn)

    def _copy_data(self, cursor, table_name, data, columns):
        """Use COPY to efficiently load data into PostgreSQL."""
        if not data:
            return

        # Process in batches to avoid huge memory allocations
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i : i + BATCH_SIZE]

            # Create StringIO for COPY
            copy_buffer = io.StringIO()
            for row in batch:
                # Format row as tab-separated values
                copy_buffer.write(
                    "\t".join(
                        str(field if field is not None else "")
                        .replace("\t", " ")
                        .replace("\n", " ")
                        for field in row
                    )
                    + "\n"
                )

            copy_buffer.seek(0)

            # Execute COPY command
            cursor.copy_expert(
                f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH NULL AS ''",
                copy_buffer,
            )


class MatchingProcessor:
    """Handles matching of publications with PubMed records."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def create_matches(self) -> int:
        """Create matches between publications and PubMed records."""
        with timed_operation("creating matches"):
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    # First create exact title matches
                    cursor.execute(
                        """
                        INSERT INTO publication_pubmed_matches (publication_id, pmid, match_quality, match_type)
                        SELECT 
                            p.id, pr.pmid, 100.0, 'exact_title'::match_type
                        FROM 
                            publications p
                        JOIN 
                            pubmed_records pr ON p.title_normalized = pr.title_normalized
                        LEFT JOIN
                            publication_pubmed_matches m ON p.id = m.publication_id
                        WHERE 
                            m.publication_id IS NULL 
                            AND p.pmid IS NULL
                        ON CONFLICT (publication_id, pmid) DO NOTHING
                    """
                    )
                    exact_matches = cursor.rowcount
                    logger.info(f"Created {exact_matches} exact title matches")

                    # Create fuzzy title + year matches
                    cursor.execute(
                        """
                        INSERT INTO publication_pubmed_matches (publication_id, pmid, match_quality, match_type)
                        SELECT 
                            p.id, pr.pmid, 
                            (similarity(p.title_normalized, pr.title_normalized) * 90.0), 
                            'title_year'::match_type
                        FROM 
                            publications p
                        JOIN 
                            pubmed_records pr ON p.year::text = pr.year::text
                            AND similarity(p.title_normalized, pr.title_normalized) > 0.85
                        LEFT JOIN
                            publication_pubmed_matches m ON p.id = m.publication_id
                        WHERE 
                            m.publication_id IS NULL 
                            AND p.pmid IS NULL
                        ON CONFLICT (publication_id, pmid) DO NOTHING
                    """
                    )
                    year_matches = cursor.rowcount
                    logger.info(f"Created {year_matches} title+year matches")

                    conn.commit()

                    return exact_matches + year_matches


def main():
    global MAX_WORKERS  # Add this line to access the global variable

    parser = argparse.ArgumentParser(
        description="Process PubMed XML files and match with publications"
    )
    parser.add_argument(
        "--base-folder", required=True, help="Base folder containing PubMed XML files"
    )
    parser.add_argument("--db-host", default="localhost", help="Database host")
    parser.add_argument("--db-name", default="pubmed_integration", help="Database name")
    parser.add_argument("--db-user", default="pubmed", help="Database user")
    parser.add_argument(
        "--db-password", default="pubmed_password", help="Database password"
    )
    parser.add_argument(
        "--match", action="store_true", help="Run matching process after import"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,  # Default is taken from global MAX_WORKERS
        help=f"Number of worker processes (default: {MAX_WORKERS})",
    )

    args = parser.parse_args()

    # Now safely use the argument value to update MAX_WORKERS
    MAX_WORKERS = args.workers

    # Configure database connection
    db_config = {
        "host": args.db_host,
        "database": args.db_name,
        "user": args.db_user,
        "password": args.db_password,
        "application_name": "pubmed_processor",
    }

    # Initialize database manager
    db_manager = DatabaseManager(db_config)

    # Process PubMed XML files
    processor = PubMedXmlProcessor(db_manager)

    with timed_operation("total processing"):
        base_folder = args.base_folder
        total_records = 0

        # Find all convertir folders
        convertir_folders = [
            os.path.join(base_folder, d)
            for d in os.listdir(base_folder)
            if d.startswith("convertir_")
            and os.path.isdir(os.path.join(base_folder, d))
        ]

        # Process each folder
        for folder in convertir_folders:
            logger.info(f"Processing folder: {folder}")
            records = processor.process_folder(folder)
            total_records += records
            logger.info(f"Processed {records} records from {folder}")

        logger.info(f"Total processed records: {total_records}")

        # Run matching if requested
        if args.match:
            matcher = MatchingProcessor(db_manager)
            match_count = matcher.create_matches()
            logger.info(
                f"Created {match_count} matches between publications and PubMed records"
            )


if __name__ == "__main__":
    main()
