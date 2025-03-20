#!/usr/bin/env python3
import argparse
import concurrent.futures
import functools
import gzip
import logging
import multiprocessing
import os
import re
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, List, Optional, Set, Tuple, Union, Any

import lxml.etree as ET
import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# ========================= CONFIGURATION =========================

CHUNK_SIZE = 10  # Number of files to process in parallel
BATCH_SIZE = 5000  # Number of records to insert at once
MAX_WORKERS = max(1, multiprocessing.cpu_count() - 1)  # Optimal worker count
LRU_CACHE_SIZE = 100000  # Size for normalization function caching
XML_BUFFER_SIZE = 16 * 1024 * 1024  # 16MB buffer for XML parsing
DB_CONNECT_TIMEOUT = 30  # Database connection timeout in seconds
DEBUG_XML = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("pubmed_processor.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("PubMedProcessor")

DB_CONFIG = {
    "host": os.environ.get("PGHOST", "localhost"),
    "database": os.environ.get("PGDATABASE", "pubmed_integration"),
    "user": os.environ.get("PGUSER", "pubmed"),
    "password": os.environ.get("PGPASSWORD", "pubmed_password"),
    "application_name": "pubmed_processor",
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}

# ========================= UTILITIES =========================


@contextmanager
def timed_operation(description: str):
    start_time = time.time()
    logger.info(f"Starting {description}")
    try:
        yield
    finally:
        elapsed = time.time() - start_time
        logger.info(f"Completed {description} in {elapsed:.2f} seconds")


@lru_cache(maxsize=LRU_CACHE_SIZE)
def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[^\w\s]", "", text)
    return text.strip()


def get_optimal_thread_count() -> int:
    cpu_count = multiprocessing.cpu_count()
    return max(1, int(cpu_count * 0.75))


# ========================= DATA STRUCTURES =========================


@dataclass
class PubMedRecord:
    pmid: str
    title: str
    title_normalized: str
    journal: str = ""
    issn: str = ""
    volume: str = ""
    issue: str = ""
    pub_date: str = ""
    year: Optional[int] = None
    abstract: str = ""
    authors: List[Tuple[str, int]] = None
    mesh_terms: List[str] = None
    source_file: str = ""

    def __post_init__(self):
        if self.authors is None:
            self.authors = []
        if self.mesh_terms is None:
            self.mesh_terms = []


# ========================= DATABASE MANAGEMENT =========================


class DatabaseManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.conn = None

    def connect(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(
                **self.config,
                connect_timeout=DB_CONNECT_TIMEOUT,
            )
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with self.conn.cursor() as cursor:
                cursor.execute("SET work_mem TO '256MB'")
                cursor.execute("SET synchronous_commit TO OFF")
                cursor.execute("SET max_parallel_workers_per_gather TO 4")
        return self.conn

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            self.conn = None

    def clear_temp_tables(self):
        conn = self.connect()
        with conn.cursor() as cursor:
            try:
                cursor.execute("TRUNCATE TABLE temp_pubmed_records")
                cursor.execute("TRUNCATE TABLE temp_pubmed_authors")
                cursor.execute("TRUNCATE TABLE temp_pubmed_mesh")
            except psycopg2.Error:
                conn.rollback()
                self._create_temp_tables(cursor)

    def _create_temp_tables(self, cursor):
        cursor.execute(
            """
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_pubmed_records (
                pmid VARCHAR(20) NOT NULL,
                title TEXT NOT NULL,
                title_normalized TEXT NOT NULL,
                journal VARCHAR(255),
                issn VARCHAR(20),
                volume VARCHAR(50),
                issue VARCHAR(50),
                pub_date VARCHAR(100),
                year INT,
                abstract TEXT,
                source_file VARCHAR(255)
            )
            """
        )
        cursor.execute(
            """
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_pubmed_authors (
                pmid VARCHAR(20) NOT NULL,
                author_name TEXT NOT NULL,
                position INT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_pubmed_mesh (
                pmid VARCHAR(20) NOT NULL,
                mesh_term TEXT NOT NULL
            )
            """
        )

    def bulk_insert_records(self, records: List[PubMedRecord]) -> int:
        if not records:
            return 0

        conn = self.connect()
        self.clear_temp_tables()

        unique_records = {}
        for record in records:
            unique_records[record.pmid] = record

        deduplicated_records = list(unique_records.values())
        logger.info(
            f"Deduplicated {len(records)} records to {len(deduplicated_records)} unique PMIDs"
        )

        with conn.cursor() as cursor:
            total_records = 0
            pubmed_rows = []
            author_rows = []
            mesh_rows = []

            for record in deduplicated_records:
                pubmed_rows.append(
                    (
                        record.pmid,
                        record.title,
                        record.title_normalized,
                        record.journal,
                        record.issn,
                        record.volume,
                        record.issue,
                        record.pub_date,
                        record.year,
                        record.abstract,
                        record.source_file,
                    )
                )
                for author_name, position in record.authors:
                    author_rows.append((record.pmid, author_name, position))
                for mesh_term in record.mesh_terms:
                    mesh_rows.append((record.pmid, mesh_term))

            try:
                self._copy_records(cursor, "temp_pubmed_records", pubmed_rows)
                self._copy_records(cursor, "temp_pubmed_authors", author_rows)
                self._copy_records(cursor, "temp_pubmed_mesh", mesh_rows)
                self._merge_temp_data(cursor)
                total_records = len(pubmed_rows)
            except Exception as e:
                logger.error(f"Database error during bulk insert: {e}")
                conn.rollback()
                raise

            return total_records

    def _copy_records(self, cursor, table_name: str, rows: List[Tuple]) -> None:
        if not rows:
            return

        from io import StringIO

        buffer = StringIO()
        for row in rows:
            line = []
            for item in row:
                if item is None:
                    line.append("")
                elif isinstance(item, str):
                    escaped = (
                        item.replace("\\", "\\\\")
                        .replace("\t", "\\t")
                        .replace("\n", "\\n")
                        .replace("\r", "\\r")
                    )
                    line.append(escaped)
                else:
                    line.append(str(item))
            buffer.write("\t".join(line) + "\n")
        buffer.seek(0)
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH NULL AS ''", buffer)

    def _merge_temp_data(self, cursor) -> None:
        cursor.execute(
            """
            INSERT INTO pubmed_records (
                pmid, title, title_normalized, journal, issn, volume, issue, 
                year, abstract, processed_at, source_file
            )
            SELECT 
                pmid, title, title_normalized, journal, issn, volume, issue,
                year, abstract, NOW(), source_file
            FROM temp_pubmed_records
            ON CONFLICT (pmid) DO UPDATE SET
                title = EXCLUDED.title,
                title_normalized = EXCLUDED.title_normalized,
                journal = EXCLUDED.journal,
                issn = EXCLUDED.issn,
                volume = EXCLUDED.volume, 
                issue = EXCLUDED.issue,
                year = EXCLUDED.year,
                abstract = EXCLUDED.abstract,
                processed_at = NOW(),
                source_file = EXCLUDED.source_file
            """
        )
        cursor.execute(
            """
            INSERT INTO authors (name, name_normalized)
            SELECT DISTINCT author_name, normalize_text(author_name)
            FROM temp_pubmed_authors
            ON CONFLICT (name) DO NOTHING
            """
        )
        cursor.execute(
            """
            INSERT INTO mesh_terms (term)
            SELECT DISTINCT mesh_term
            FROM temp_pubmed_mesh
            ON CONFLICT (term) DO NOTHING
            """
        )
        cursor.execute(
            """
            INSERT INTO pubmed_authors (pubmed_id, author_id, position)
            SELECT 
                t.pmid, a.id, t.position
            FROM temp_pubmed_authors t
            JOIN authors a ON t.author_name = a.name
            ON CONFLICT (pubmed_id, author_id) DO UPDATE SET
                position = EXCLUDED.position
            """
        )
        cursor.execute(
            """
            INSERT INTO pubmed_mesh_terms (pubmed_id, mesh_id)
            SELECT 
                t.pmid, m.id
            FROM temp_pubmed_mesh t
            JOIN mesh_terms m ON t.mesh_term = m.term
            ON CONFLICT (pubmed_id, mesh_id) DO NOTHING
            """
        )


# ========================= XML PROCESSING =========================


def parse_xml_file(file_path: str) -> List[PubMedRecord]:
    """Función de nivel superior para procesar un archivo XML comprimido."""
    records = []
    filename = os.path.basename(file_path)
    logger.info(f"Processing {filename}")
    try:
        with gzip.open(file_path, "rb") as f:
            peek = f.read(2048)
            f.seek(0)
            namespace = None
            ns_match = re.search(
                r'xmlns="([^"]+)"', peek.decode("utf-8", errors="ignore")
            )
            if ns_match:
                namespace = ns_match.group(1)
                logger.info(f"Detected namespace: {namespace} in {filename}")
            ns_prefix = "{" + namespace + "}" if namespace else ""
            context = ET.iterparse(f, events=("start", "end"))
            current_record = None
            element_stack = []
            pmid = None
            title = ""
            journal = ""
            year = None
            abstract = ""
            authors = []
            mesh_terms = []

            for event, elem in context:
                tag = elem.tag
                if namespace and tag.startswith(ns_prefix):
                    tag = tag[len(ns_prefix) :]
                if event == "start":
                    element_stack.append(tag)
                    if tag in ("PubmedArticle", "PubmedBookArticle"):
                        pmid = None
                        title = ""
                        journal = ""
                        year = None
                        abstract = ""
                        authors = []
                        mesh_terms = []
                elif event == "end":
                    if element_stack:
                        element_stack.pop()
                    path = "/".join(element_stack + [tag])
                    if tag == "PMID" and "PubmedArticle" in element_stack:
                        pmid = elem.text.strip() if elem.text else ""
                    elif tag == "ArticleTitle":
                        title = elem.text.strip() if elem.text else ""
                    elif tag == "Title" and "Journal" in element_stack:
                        journal = elem.text.strip() if elem.text else ""
                    elif tag == "Year" and any(
                        x in element_stack for x in ("PubDate", "JournalInfo")
                    ):
                        try:
                            year = int(elem.text.strip()) if elem.text else None
                        except ValueError:
                            year = None
                    elif tag == "AbstractText":
                        text = elem.text.strip() if elem.text else ""
                        if text:
                            abstract += " " + text if abstract else text
                    elif tag == "Author":
                        last_name = next(
                            (e.text for e in elem.findall("./LastName") or []), ""
                        )
                        fore_name = next(
                            (e.text for e in elem.findall("./ForeName") or []), ""
                        )
                        if last_name or fore_name:
                            author_name = f"{fore_name} {last_name}".strip()
                            if author_name:
                                authors.append((author_name, len(authors) + 1))
                    elif tag == "DescriptorName":
                        term = elem.text.strip() if elem.text else ""
                        if term:
                            mesh_terms.append(term)
                    elif tag in ("PubmedArticle", "PubmedBookArticle"):
                        if pmid and title:
                            record = PubMedRecord(
                                pmid=pmid,
                                title=title,
                                title_normalized=normalize_text(title),
                                journal=journal,
                                year=year,
                                abstract=abstract,
                                authors=authors,
                                mesh_terms=mesh_terms,
                                source_file=filename,
                            )
                            records.append(record)
                            if len(records) % 100 == 0:
                                logger.info(
                                    f"Extracted {len(records)} records so far from {filename}"
                                )
                    if event == "end":
                        elem.clear()
                        while elem.getprevious() is not None:
                            del elem.getparent()[0]
        logger.info(f"Extracted {len(records)} records from {filename}")
        return records
    except Exception as e:
        logger.error(f"Error processing {filename}: {str(e)}")
        return []


class XmlProcessor:
    """Clase encargada de la orquestación del procesamiento de XML."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.processed_files = set()
        self.checkpoint_path = None

    def load_checkpoint(self, base_folder: str) -> None:
        self.checkpoint_path = os.path.join(base_folder, "processor_checkpoint.txt")
        if os.path.exists(self.checkpoint_path):
            try:
                with open(self.checkpoint_path, "r") as f:
                    self.processed_files = set(
                        line.strip() for line in f if line.strip()
                    )
                logger.info(
                    f"Loaded {len(self.processed_files)} already processed files from checkpoint"
                )
            except Exception as e:
                logger.warning(f"Failed to load checkpoint file: {e}")

    def update_checkpoint(self, filename: str) -> None:
        if self.checkpoint_path:
            try:
                with open(self.checkpoint_path, "a") as f:
                    f.write(f"{filename}\n")
            except Exception as e:
                logger.warning(f"Failed to update checkpoint file: {e}")

    def process_folder(self, folder_path: str) -> int:
        self.load_checkpoint(folder_path)
        xml_files = []
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.lower().endswith(".xml.gz"):
                    xml_path = os.path.join(root, file)
                    if os.path.basename(xml_path) not in self.processed_files:
                        xml_files.append(xml_path)
        if not xml_files:
            logger.info(f"No new XML files to process in {folder_path}")
            return 0

        logger.info(f"Found {len(xml_files)} XML files to process in {folder_path}")
        total_records = 0
        for i in range(0, len(xml_files), CHUNK_SIZE):
            chunk = xml_files[i : i + CHUNK_SIZE]
            logger.info(
                f"Processing chunk of {len(chunk)} files ({i+1}-{i+len(chunk)}/{len(xml_files)})"
            )
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=MAX_WORKERS
            ) as executor:
                future_to_file = {
                    executor.submit(parse_xml_file, file_path): file_path
                    for file_path in chunk
                }
                results = []
                for future in concurrent.futures.as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        result = future.result()
                        if result:
                            results.append(result)
                            filename = os.path.basename(file_path)
                            self.processed_files.add(filename)
                            self.update_checkpoint(filename)
                    except Exception as e:
                        logger.error(
                            f"Error processing {os.path.basename(file_path)}: {e}"
                        )
            if results:
                all_records = []
                for result in results:
                    all_records.extend(result)
                for j in range(0, len(all_records), BATCH_SIZE):
                    batch = all_records[j : j + BATCH_SIZE]
                    try:
                        inserted = self.db_manager.bulk_insert_records(batch)
                        total_records += inserted
                        logger.info(
                            f"Inserted {inserted} records (total: {total_records})"
                        )
                    except Exception as e:
                        logger.error(f"Error inserting batch into database: {e}")
        return total_records


# ========================= MATCHING ENGINE =========================


class MatchingEngine:
    """Creates matches between publications and PubMed records with optimized algorithms."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def create_matches(self) -> int:
        """Create matches between publications and PubMed records using database operations."""
        conn = self.db_manager.connect()

        with timed_operation("creating matches"):
            with conn.cursor() as cursor:
                # Create exact title matches (most confident)
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

                # Create title + year matches (secondary confidence)
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

                # Create fuzzy title matches (lower confidence)
                cursor.execute(
                    """
                    INSERT INTO publication_pubmed_matches (publication_id, pmid, match_quality, match_type)
                    SELECT 
                        p.id, pr.pmid, 
                        (similarity(p.title_normalized, pr.title_normalized) * 80.0), 
                        'fuzzy_title'::match_type
                    FROM 
                        publications p
                    JOIN 
                        pubmed_records pr ON 
                        similarity(p.title_normalized, pr.title_normalized) > 0.9
                    WHERE 
                        NOT EXISTS (
                            SELECT 1 FROM publication_pubmed_matches 
                            WHERE publication_id = p.id
                        )
                    ON CONFLICT (publication_id, pmid) DO NOTHING
                """
                )
                fuzzy_matches = cursor.rowcount

                # Add DOI matches if DOI field exists
                try:
                    cursor.execute(
                        """
                        INSERT INTO publication_pubmed_matches (publication_id, pmid, match_quality, match_type)
                        SELECT 
                            p.id, pr.pmid, 100.0, 'doi_match'::match_type
                        FROM 
                            publications p
                        JOIN 
                            pubmed_records pr ON pr.doi = p.doi
                        WHERE 
                            p.doi IS NOT NULL AND pr.doi IS NOT NULL
                            AND NOT EXISTS (
                                SELECT 1 FROM publication_pubmed_matches 
                                WHERE publication_id = p.id
                            )
                        ON CONFLICT (publication_id, pmid) DO NOTHING
                    """
                    )
                    doi_matches = cursor.rowcount
                except psycopg2.Error:
                    doi_matches = 0  # DOI field might not exist

                total_matches = (
                    exact_matches + title_year_matches + fuzzy_matches + doi_matches
                )

                logger.info(
                    f"Created matches: {exact_matches} exact, {title_year_matches} title+year, {fuzzy_matches} fuzzy, {doi_matches} DOI"
                )
                return total_matches


# ========================= CSV IMPORTER =========================


class CsvImporter:
    """Efficiently imports publication data from CSV files."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def import_csv(self, csv_path: str) -> int:
        """Import user publications from CSV file using COPY for best performance."""
        import csv

        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            return 0

        # Count records first
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            total_rows = sum(1 for _ in reader) - 1  # Subtract header

        logger.info(f"Importing {total_rows} publications from {csv_path}")

        conn = self.db_manager.connect()

        with conn.cursor() as cursor:
            try:
                # Create temporary table
                cursor.execute(
                    """
                    CREATE TEMPORARY TABLE temp_publications (
                        cfrespublid VARCHAR(255),
                        title TEXT,
                        cfpersid_id VARCHAR(255),
                        author TEXT,
                        cfrespubldate TEXT,
                        year TEXT,
                        cfdctype_id INTEGER,
                        cfdctype TEXT,
                        volume TEXT,
                        number TEXT,
                        pages TEXT
                    )
                """
                )

                # Use COPY for fast import
                with open(csv_path, "r", encoding="utf-8") as f:
                    cursor.copy_expert(
                        """
                        COPY temp_publications FROM STDIN WITH 
                        CSV HEADER DELIMITER ',' QUOTE '"' ESCAPE '\\'
                        """,
                        f,
                    )

                # Add normalized fields
                cursor.execute(
                    """
                    UPDATE temp_publications 
                    SET title = COALESCE(title, '')
                """
                )

                # Insert into main publications table
                cursor.execute(
                    """
                    INSERT INTO publications (
                        id, title, title_normalized, person_id, author, author_normalized,
                        publication_date, year, doc_type_id, doc_type, volume, number, pages,
                        created_at, updated_at
                    )
                    SELECT
                        cfrespublid,
                        title,
                        normalize_text(title),
                        cfpersid_id,
                        author,
                        normalize_text(COALESCE(author, '')),
                        cfrespubldate::DATE,
                        year::INTEGER,
                        cfdctype_id,
                        cfdctype,
                        volume,
                        number,
                        pages,
                        NOW(),
                        NOW()
                    FROM temp_publications
                    ON CONFLICT (id) DO UPDATE SET
                        title = EXCLUDED.title,
                        title_normalized = EXCLUDED.title_normalized,
                        person_id = EXCLUDED.person_id,
                        author = EXCLUDED.author,
                        author_normalized = EXCLUDED.author_normalized,
                        publication_date = EXCLUDED.publication_date,
                        year = EXCLUDED.year,
                        doc_type_id = EXCLUDED.doc_type_id,
                        doc_type = EXCLUDED.doc_type,
                        volume = EXCLUDED.volume,
                        number = EXCLUDED.number,
                        pages = EXCLUDED.pages,
                        updated_at = NOW()
                """
                )

                return cursor.rowcount
            except Exception as e:
                logger.error(f"Error importing CSV: {e}")
                conn.rollback()
                raise


def main():
    global BATCH_SIZE, MAX_WORKERS
    parser = argparse.ArgumentParser(description="PubMed XML Processor and Matcher")
    parser.add_argument("--xml-folder", help="Folder containing PubMed XML files")
    parser.add_argument("--csv-file", help="CSV file with publications to import")
    parser.add_argument("--match", action="store_true", help="Run matching process")
    parser.add_argument("--db-host", default="localhost", help="Database host")
    parser.add_argument("--db-name", default="pubmed_integration", help="Database name")
    parser.add_argument("--db-user", default="pubmed", help="Database user")
    parser.add_argument("--db-password", help="Database password")
    parser.add_argument(
        "--batch-size", type=int, default=BATCH_SIZE, help="Batch size for processing"
    )
    parser.add_argument(
        "--workers", type=int, default=MAX_WORKERS, help="Number of worker processes"
    )
    args = parser.parse_args()

    db_config = DB_CONFIG.copy()
    if args.db_host:
        db_config["host"] = args.db_host
    if args.db_name:
        db_config["database"] = args.db_name
    if args.db_user:
        db_config["user"] = args.db_user
    if args.db_password:
        db_config["password"] = args.db_password

    if args.batch_size:
        BATCH_SIZE = args.batch_size
    if args.workers:
        MAX_WORKERS = args.workers

    db_manager = DatabaseManager(db_config)
    try:
        if args.csv_file:
            with timed_operation("CSV import"):
                importer = CsvImporter(db_manager)
                count = importer.import_csv(args.csv_file)
                logger.info(f"Imported {count} publications from CSV")
        if args.xml_folder:
            with timed_operation("XML processing"):
                processor = XmlProcessor(db_manager)
                total_records = processor.process_folder(args.xml_folder)
                logger.info(f"Processed {total_records} PubMed records from XML files")

    finally:
        db_manager.close()


if __name__ == "__main__":
    main()
