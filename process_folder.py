import json
import multiprocessing
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
        try:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE pubmed_records_temp")
                cursor.execute("TRUNCATE TABLE pubmed_authors_temp")
                cursor.execute("TRUNCATE TABLE pubmed_mesh_temp")
                conn.commit()
        except psycopg2.errors.UndefinedTable:
            # Tables don't exist, rollback the failed transaction
            conn.rollback()

            # Then create the tables in a new transaction
            self.setup_temp_tables(conn)

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
        """Merge data from temporary tables into main tables with proper ordering to maintain referential integrity."""
        with conn.cursor() as cursor:
            # First, ensure we have proper referential integrity by validating temp tables
            cursor.execute(
                """
                -- Delete any mesh terms referencing non-existent PMIDs
                DELETE FROM pubmed_mesh_temp
                WHERE pmid NOT IN (SELECT pmid FROM pubmed_records_temp)
            """
            )

            cursor.execute(
                """
                -- Delete any author links referencing non-existent PMIDs
                DELETE FROM pubmed_authors_temp
                WHERE pmid NOT IN (SELECT pmid FROM pubmed_records_temp)
            """
            )

            # Insert PubMed records FIRST to establish foreign key references
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
            pubmed_count = cursor.rowcount
            logger.info(f"Merged {pubmed_count} PubMed records")

            # Insert authors
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

            # Link authors to PubMed records WITH validation join
            cursor.execute(
                """
                INSERT INTO pubmed_authors (pubmed_id, author_id, position)
                SELECT 
                    t.pmid, a.id, t.position
                FROM pubmed_authors_temp t
                JOIN authors a ON t.author_name = a.name
                JOIN pubmed_records pr ON t.pmid = pr.pmid
                ON CONFLICT (pubmed_id, author_id) DO UPDATE SET
                    position = EXCLUDED.position
            """
            )

            # Link mesh terms to PubMed records WITH validation join
            cursor.execute(
                """
                INSERT INTO pubmed_mesh_terms (pubmed_id, mesh_id)
                SELECT 
                    t.pmid, m.id
                FROM pubmed_mesh_temp t
                JOIN mesh_terms m ON t.mesh_term = m.term
                JOIN pubmed_records pr ON t.pmid = pr.pmid
                ON CONFLICT (pubmed_id, mesh_id) DO NOTHING
            """
            )

            conn.commit()


class PubMedXmlProcessor:
    """Processes PubMed XML files with improved error handling and data validation."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.processed_files = set()

    def process_folder(self, folder_path: str) -> int:
        """Process all XML files in a folder with proper error handling."""
        files = [
            os.path.join(folder_path, f)
            for f in os.listdir(folder_path)
            if f.endswith(".xml.gz") and os.path.isfile(os.path.join(folder_path, f))
        ]

        total_files = len(files)
        logger.info(f"Found {total_files} XML files in {folder_path}")

        # Create checkpoint file path
        checkpoint_path = os.path.join(folder_path, "processor_checkpoint.txt")

        # Load previously processed files
        if os.path.exists(checkpoint_path):
            try:
                with open(checkpoint_path, "r") as f:
                    self.processed_files = set(
                        line.strip() for line in f if line.strip()
                    )
                logger.info(
                    f"Loaded {len(self.processed_files)} previously processed files from checkpoint"
                )
            except Exception as e:
                logger.warning(f"Failed to load checkpoint file: {e}")

        # Process files in parallel with error handling
        total_records = 0

        # Process in chunks to control memory usage
        for i in range(0, len(files), CHUNK_SIZE):
            chunk = files[i : i + CHUNK_SIZE]
            logger.info(
                f"Processing batch of {len(chunk)} files ({i+1}-{i+len(chunk)}/{total_files})"
            )

            # Filter out already processed files
            chunk = [
                f for f in chunk if os.path.basename(f) not in self.processed_files
            ]
            if not chunk:
                logger.info("All files in this batch were already processed, skipping")
                continue

            # Process files in this chunk
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=MAX_WORKERS
            ) as executor:
                future_to_file = {
                    executor.submit(self.process_xml_file, file_path): file_path
                    for file_path in chunk
                }

                batch_results = []
                for future in concurrent.futures.as_completed(future_to_file):
                    file_path = future_to_file[future]
                    file_name = os.path.basename(file_path)

                    try:
                        result = future.result()
                        if result:
                            # Validate data integrity before adding to batch
                            if self.validate_extracted_data(result):
                                batch_results.append(result)
                                self.processed_files.add(file_name)
                                # Update checkpoint after each successful file
                                self.update_checkpoint(checkpoint_path, file_name)
                    except Exception as e:
                        logger.error(f"Error processing {file_name}: {e}")

            # Import this batch to database if we have results
            if batch_results:
                try:
                    self.import_batch_to_database(batch_results)
                    batch_count = sum(
                        len(r.get("pubmed_records", [])) for r in batch_results
                    )
                    total_records += batch_count
                    logger.info(
                        f"Imported {batch_count} records from {len(batch_results)} files"
                    )
                except Exception as e:
                    logger.error(f"Failed to import batch: {e}")

            logger.info(f"Completed batch. Total records so far: {total_records}")

        return total_records

    def validate_extracted_data(self, data):
        """Validate that extracted data has proper referential integrity."""
        if not data or "pubmed_records" not in data or not data["pubmed_records"]:
            return False

        # Get all valid PMIDs from records
        valid_pmids = set(
            record[0] for record in data["pubmed_records"] if record and record[0]
        )

        # Validate author links
        if "author_links" in data:
            data["author_links"] = [
                link for link in data["author_links"] if link and link[0] in valid_pmids
            ]

        # Validate mesh terms
        if "mesh_terms" in data:
            data["mesh_terms"] = [
                term for term in data["mesh_terms"] if term and term[0] in valid_pmids
            ]

        return True

    def update_checkpoint(self, checkpoint_path, file_name):
        """Update the checkpoint file with a processed file."""
        try:
            with open(checkpoint_path, "a") as f:
                f.write(f"{file_name}\n")
        except Exception as e:
            logger.warning(f"Failed to update checkpoint file: {e}")

    def process_xml_file(self, file_path: str) -> Optional[Dict[str, List]]:
        """Process a single XML file with improved error handling."""
        file_name = os.path.basename(file_path)

        if file_name in self.processed_files:
            logger.info(f"Skipping already processed file: {file_name}")
            return None

        logger.info(f"Processing {file_name}")

        try:
            with gzip.open(file_path, "rt", encoding="utf-8", errors="replace") as f:
                # Process XML content with error handling for invalid encodings
                return self.process_xml_content(f, file_name)
        except Exception as e:
            logger.error(f"Error opening/reading {file_name}: {e}")
            return None

    def process_xml_content(self, file_obj, file_name):
        """Process XML content with robust error handling."""
        pubmed_records = []
        author_links = []
        mesh_terms = []

        try:
            # Use iterparse for memory-efficient XML processing
            context = ET.iterparse(file_obj, events=("start", "end"))

            # Current state variables
            current_pmid = None
            current_title = None
            current_journal = None
            current_volume = None
            current_issue = None
            current_year = None
            current_abstract = None
            current_authors = []
            current_mesh = []

            # Tracking state
            in_article = False
            in_title = False
            in_abstract = False
            in_abstract_text = False
            in_journal = False
            in_journal_title = False
            in_journal_issue = False
            in_volume = False
            in_issue = False
            in_pub_date = False
            in_year = False
            in_author = False
            in_author_last_name = False
            in_author_fore_name = False
            in_author_initials = False
            in_mesh_heading = False
            in_mesh_descriptor = False

            # Buffer for collecting text
            text_buffer = []
            author_position = 0

            # Processing logic for XML
            for event, elem in context:
                tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

                # Start of elements
                if event == "start":
                    if tag == "PubmedArticle":
                        in_article = True
                        current_pmid = None
                        current_title = None
                        current_journal = None
                        current_volume = None
                        current_issue = None
                        current_year = None
                        current_abstract = None
                        current_authors = []
                        current_mesh = []
                        author_position = 0

                    elif tag == "PMID" and in_article and current_pmid is None:
                        text_buffer = []

                    elif tag == "ArticleTitle" and in_article:
                        in_title = True
                        text_buffer = []

                    elif tag == "Abstract" and in_article:
                        in_abstract = True
                        current_abstract = ""

                    elif tag == "AbstractText" and in_abstract:
                        in_abstract_text = True
                        text_buffer = []

                    elif tag == "Journal" and in_article:
                        in_journal = True

                    elif tag == "Title" and in_journal:
                        in_journal_title = True
                        text_buffer = []

                    elif tag == "JournalIssue" and in_journal:
                        in_journal_issue = True

                    elif tag == "Volume" and in_journal_issue:
                        in_volume = True
                        text_buffer = []

                    elif tag == "Issue" and in_journal_issue:
                        in_issue = True
                        text_buffer = []

                    elif tag == "PubDate" and in_journal_issue:
                        in_pub_date = True

                    elif tag == "Year" and in_pub_date:
                        in_year = True
                        text_buffer = []

                    elif tag == "Author" and in_article:
                        in_author = True
                        author_position += 1
                        current_author_last = None
                        current_author_fore = None
                        current_author_initials = None

                    elif tag == "LastName" and in_author:
                        in_author_last_name = True
                        text_buffer = []

                    elif tag == "ForeName" and in_author:
                        in_author_fore_name = True
                        text_buffer = []

                    elif tag == "Initials" and in_author:
                        in_author_initials = True
                        text_buffer = []

                    elif tag == "MeshHeading" and in_article:
                        in_mesh_heading = True
                        current_mesh_term = None

                    elif tag == "DescriptorName" and in_mesh_heading:
                        in_mesh_descriptor = True
                        text_buffer = []

                # Collect text content
                elif event == "end":
                    # PMID
                    if tag == "PMID" and in_article and current_pmid is None:
                        current_pmid = "".join(text_buffer).strip()

                    # Title
                    elif tag == "ArticleTitle" and in_title:
                        in_title = False
                        current_title = "".join(text_buffer).strip()

                    # Abstract Text
                    elif tag == "AbstractText" and in_abstract_text:
                        in_abstract_text = False
                        if current_abstract:
                            current_abstract += " " + "".join(text_buffer).strip()
                        else:
                            current_abstract = "".join(text_buffer).strip()

                    # Abstract (end)
                    elif tag == "Abstract":
                        in_abstract = False

                    # Journal Title
                    elif tag == "Title" and in_journal_title:
                        in_journal_title = False
                        current_journal = "".join(text_buffer).strip()

                    # Volume
                    elif tag == "Volume" and in_volume:
                        in_volume = False
                        current_volume = "".join(text_buffer).strip()

                    # Issue
                    elif tag == "Issue" and in_issue:
                        in_issue = False
                        current_issue = "".join(text_buffer).strip()

                    # Year
                    elif tag == "Year" and in_year:
                        in_year = False
                        try:
                            current_year = int("".join(text_buffer).strip())
                        except ValueError:
                            # Handle invalid year formats
                            current_year = None

                    # End of PubDate
                    elif tag == "PubDate":
                        in_pub_date = False

                    # End of JournalIssue
                    elif tag == "JournalIssue":
                        in_journal_issue = False

                    # End of Journal
                    elif tag == "Journal":
                        in_journal = False

                    # Author parts
                    elif tag == "LastName" and in_author_last_name:
                        in_author_last_name = False
                        current_author_last = "".join(text_buffer).strip()

                    elif tag == "ForeName" and in_author_fore_name:
                        in_author_fore_name = False
                        current_author_fore = "".join(text_buffer).strip()

                    elif tag == "Initials" and in_author_initials:
                        in_author_initials = False
                        current_author_initials = "".join(text_buffer).strip()

                    # End of Author - construct author name
                    elif tag == "Author" and in_author:
                        in_author = False
                        author_name = ""

                        # Construct author name with available parts
                        if current_author_last:
                            author_name = current_author_last

                            if current_author_fore:
                                author_name = f"{current_author_fore} {author_name}"
                            elif current_author_initials:
                                author_name = f"{current_author_initials} {author_name}"

                        # Only add valid author names
                        if author_name and current_pmid:
                            author_links.append(
                                (current_pmid, author_name, author_position)
                            )

                    # MeSH term
                    elif tag == "DescriptorName" and in_mesh_descriptor:
                        in_mesh_descriptor = False
                        current_mesh_term = "".join(text_buffer).strip()

                        # Add mesh term if valid
                        if current_mesh_term and current_pmid:
                            mesh_terms.append((current_pmid, current_mesh_term))

                    # End of MeshHeading
                    elif tag == "MeshHeading":
                        in_mesh_heading = False

                    # End of article - add record to results
                    elif tag == "PubmedArticle":
                        in_article = False

                        # Only add records with valid PMID and title
                        if current_pmid and current_title:
                            # Normalize title for better matching
                            title_normalized = normalize_text(current_title)

                            # Add complete record
                            pubmed_records.append(
                                (
                                    current_pmid,
                                    current_title,
                                    title_normalized,
                                    current_journal or "",
                                    current_volume or "",
                                    current_issue or "",
                                    current_year,
                                    current_abstract or "",
                                    file_name,
                                )
                            )

                    # Collect text content for current element
                    if elem.text and any(
                        [
                            in_title,
                            in_abstract_text,
                            in_journal_title,
                            in_volume,
                            in_issue,
                            in_year,
                            in_author_last_name,
                            in_author_fore_name,
                            in_author_initials,
                            in_mesh_descriptor,
                        ]
                    ):
                        text_buffer.append(elem.text)

                    # Clear element to save memory
                    elem.clear()

            return {
                "pubmed_records": pubmed_records,
                "author_links": author_links,
                "mesh_terms": mesh_terms,
            }

        except ET.ParseError as e:
            logger.error(f"XML parsing error in {file_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error processing {file_name}: {e}")
            return None

    def import_batch_to_database(self, batch_results: List[Dict[str, List]]):
        """Import a batch of results to database with improved error handling."""
        if not batch_results:
            return

        all_records = []
        all_authors = []
        all_mesh = []

        # Aggregate data with validation
        for result in batch_results:
            if "pubmed_records" in result:
                all_records.extend(result["pubmed_records"])
            if "author_links" in result:
                all_authors.extend(result["author_links"])
            if "mesh_terms" in result:
                all_mesh.extend(result["mesh_terms"])

        with self.db_manager.get_connection() as conn:
            # Clear temporary tables
            self.db_manager.clear_temp_tables(conn)

            # Use COPY for fast import with error handling
            with conn.cursor() as cursor:
                try:
                    self.copy_data(
                        cursor,
                        "pubmed_records_temp",
                        all_records,
                        [
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
                except Exception as e:
                    logger.error(f"Error copying record data: {e}")
                    raise

                try:
                    self.copy_data(
                        cursor,
                        "pubmed_authors_temp",
                        all_authors,
                        ["pmid", "author_name", "position"],
                    )
                except Exception as e:
                    logger.error(f"Error copying author data: {e}")

                try:
                    self.copy_data(
                        cursor, "pubmed_mesh_temp", all_mesh, ["pmid", "mesh_term"]
                    )
                except Exception as e:
                    logger.error(f"Error copying mesh data: {e}")

            # Merge the data with proper ordering for foreign keys
            try:
                self.db_manager.merge_temp_tables(conn)
            except Exception as e:
                logger.error(f"Error merging tables: {e}")
                raise

    def copy_data(self, cursor, table_name, data, columns):
        """Copy data to database with improved encoding handling."""
        if not data:
            return

        # Create buffer for COPY
        copy_buffer = io.StringIO()

        # Process data with encoding safety
        for row in data:
            # Ensure row has the right number of fields
            if len(row) != len(columns):
                logger.warning(
                    f"Row has {len(row)} columns, expected {len(columns)}. Skipping."
                )
                continue

            # Sanitize fields for encoding issues
            sanitized_row = []
            for field in row:
                if field is None:
                    sanitized_row.append("")
                    continue

                # Convert to string and sanitize
                if not isinstance(field, str):
                    field = str(field)

                # Handle encoding issues
                field_bytes = field.encode("utf-8", errors="replace")
                sanitized = field_bytes.decode("utf-8", errors="replace")

                # Replace tabs and newlines for TSV format
                sanitized = sanitized.replace("\t", " ").replace("\n", " ")

                sanitized_row.append(sanitized)

            # Write sanitized row to buffer
            copy_buffer.write("\t".join(sanitized_row) + "\n")

        # Reset buffer position
        copy_buffer.seek(0)

        try:
            # Execute COPY
            cursor.copy_expert(
                f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH NULL AS ''",
                copy_buffer,
            )
        except Exception as e:
            logger.error(f"COPY operation failed for {table_name}: {e}")
            conn = cursor.connection
            conn.rollback()

            # Fall back to row-by-row insertion if needed
            self.insert_rows_individually(cursor, table_name, data, columns)

    def insert_rows_individually(self, cursor, table_name, rows, columns):
        """Insert rows individually when COPY fails."""
        success_count = 0
        column_list = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"

        for row in rows:
            if len(row) != len(columns):
                continue

            try:
                # Sanitize each value
                values = []
                for val in row:
                    if val is None:
                        values.append(None)
                    else:
                        # Convert and sanitize
                        str_val = str(val)
                        bytes_val = str_val.encode("utf-8", errors="replace")
                        sanitized = bytes_val.decode("utf-8", errors="replace")
                        values.append(sanitized)

                cursor.execute(query, values)
                success_count += 1

                # Commit periodically
                if success_count % 100 == 0:
                    cursor.connection.commit()

            except Exception as e:
                logger.warning(f"Failed to insert row: {e}")

        # Final commit
        cursor.connection.commit()
        logger.info(f"Inserted {success_count} rows individually")


class MatchingProcessor:
    """Handles matching of publications with PubMed records."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def create_matches(self) -> int:
        """Create matches between publications and PubMed records."""
        total_matches = 0

        with timed_operation("creating matches"):
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Exact title matches - fixed query that doesn't reference p.pmid
                    cursor.execute(
                        """
                        INSERT INTO publication_pubmed_matches (publication_id, pmid, match_quality, match_type)
                        SELECT
                            p.id, pr.pmid, 100.0, 'exact_title'::match_type
                        FROM
                            publications p
                        JOIN
                            pubmed_records pr ON p.title_normalized = pr.title_normalized
                        WHERE
                            NOT EXISTS (
                                SELECT 1 FROM publication_pubmed_matches 
                                WHERE publication_id = p.id
                            )
                        ON CONFLICT (publication_id, pmid) DO NOTHING
                    """
                    )
                    exact_matches = cursor.rowcount
                    total_matches += exact_matches
                    logger.info(f"Created {exact_matches} exact title matches")

                    # Title+year matches with fixed query
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
                        WHERE 
                            NOT EXISTS (
                                SELECT 1 FROM publication_pubmed_matches 
                                WHERE publication_id = p.id
                            )
                        ON CONFLICT (publication_id, pmid) DO NOTHING
                    """
                    )
                    title_year_matches = cursor.rowcount
                    total_matches += title_year_matches
                    logger.info(f"Created {title_year_matches} title+year matches")

                    conn.commit()
                    return total_matches


def main():
    global MAX_WORKERS
    parser = argparse.ArgumentParser(
        description="Process PubMed XML files with enhanced error handling"
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
        "--workers", type=int, default=MAX_WORKERS, help="Number of worker processes"
    )
    parser.add_argument("--retry", action="store_true", help="Retry failed files")

    args = parser.parse_args()

    # Set worker count
    MAX_WORKERS = args.workers

    # Configure database
    db_config = {
        "host": args.db_host,
        "database": args.db_name,
        "user": args.db_user,
        "password": args.db_password,
        "application_name": "pubmed_processor",
    }

    # Initialize database
    db_manager = DatabaseManager(db_config)

    # Process XML files
    processor = PubMedXmlProcessor(db_manager)

    with timed_operation("total processing"):
        base_folder = args.base_folder
        total_records = 0

        # Find folders to process
        xml_folders = [
            os.path.join(base_folder, d)
            for d in os.listdir(base_folder)
            if os.path.isdir(os.path.join(base_folder, d))
        ]

        # Process each folder with error handling
        for folder in xml_folders:
            try:
                logger.info(f"Processing folder: {folder}")
                records = processor.process_folder(folder)
                total_records += records
                logger.info(f"Processed {records} records from {folder}")
            except Exception as e:
                logger.error(f"Error processing folder {folder}: {e}")
                # Continue with next folder

        logger.info(f"Total processed records: {total_records}")

        # Run matching if requested
        if args.match:
            try:
                matcher = MatchingProcessor(db_manager)
                matcher.create_matches()
            except Exception as e:
                logger.error(f"Error during matching process: {e}")


if __name__ == "__main__":
    main()
