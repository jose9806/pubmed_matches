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
    journal_iso: str = ""
    issn: str = ""
    issn_type: str = ""
    issn_linking: str = ""
    volume: str = ""
    issue: str = ""
    pagination: str = ""
    doi: str = ""
    pub_date: str = ""
    year: Optional[int] = None
    pub_status: str = ""
    language: str = ""
    vernacular_title: str = ""
    medline_ta: str = ""
    nlm_unique_id: str = ""
    country: str = ""
    abstract: str = ""
    authors: List[Tuple[str, int]] = None
    mesh_terms: List[str] = None
    chemicals: List[str] = None
    keywords: List[str] = None
    publication_types: List[str] = None
    source_file: str = ""

    def __post_init__(self):
        if self.authors is None:
            self.authors = []
        if self.mesh_terms is None:
            self.mesh_terms = []
        if self.chemicals is None:
            self.chemicals = []
        if self.keywords is None:
            self.keywords = []
        if self.publication_types is None:
            self.publication_types = []


# ========================= DATABASE MANAGEMENT =========================


class DatabaseManager:
    """Manages database operations for PubMed record processing."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.conn = None
        self.temp_tables_created = False

    def connect(self):
        """Establish connection to the database with optimized settings."""
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
        """Close the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            self.conn = None

    def validate_schema(self):
        """Validate the database schema and report any missing columns."""
        conn = self.connect()
        with conn.cursor() as cursor:
            try:
                # Check if pubmed_records table has all required columns
                cursor.execute(
                    """
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'pubmed_records'
                """
                )
                existing_columns = {row[0] for row in cursor.fetchall()}

                # Define expected columns based on our schema
                expected_columns = {
                    "pmid",
                    "title",
                    "title_normalized",
                    "journal",
                    "journal_iso",
                    "issn",
                    "issn_linking",
                    "issn_type",
                    "volume",
                    "issue",
                    "pagination",
                    "doi",
                    "year",
                    "pub_date",
                    "pub_status",
                    "language",
                    "vernacular_title",
                    "abstract",
                    "medline_ta",
                    "nlm_unique_id",
                    "country",
                    "processed_at",
                    "source_file",
                }

                missing_columns = expected_columns - existing_columns
                if missing_columns:
                    logger.warning(
                        f"Schema validation: Missing columns in pubmed_records: {missing_columns}"
                    )
                    return False
                return True
            except Exception as e:
                logger.error(f"Schema validation error: {e}")
                return False

    def create_temp_tables(self):
        """Create temporary tables that match our permanent schema."""
        conn = self.connect()
        with conn.cursor() as cursor:
            try:
                # Create temporary table for pubmed records
                cursor.execute(
                    """
                CREATE TEMPORARY TABLE IF NOT EXISTS temp_pubmed_records (
                    pmid VARCHAR(20) NOT NULL,
                    title TEXT NOT NULL,
                    title_normalized TEXT NOT NULL,
                    journal VARCHAR(255),
                    journal_iso VARCHAR(100),
                    issn VARCHAR(20),
                    issn_linking VARCHAR(20),
                    issn_type VARCHAR(20),
                    volume VARCHAR(50),
                    issue VARCHAR(50),
                    pagination VARCHAR(50),
                    doi VARCHAR(100),
                    year INT,
                    pub_date VARCHAR(100),
                    pub_status VARCHAR(50),
                    language VARCHAR(50),
                    vernacular_title TEXT,
                    medline_ta VARCHAR(255),
                    nlm_unique_id VARCHAR(100),
                    country VARCHAR(100),
                    abstract TEXT,
                    source_file VARCHAR(255)
                )
                """
                )

                # Create temporary table for authors
                cursor.execute(
                    """
                CREATE TEMPORARY TABLE IF NOT EXISTS temp_pubmed_authors (
                    pmid VARCHAR(20) NOT NULL,
                    author_name TEXT NOT NULL,
                    position INT NOT NULL
                )
                """
                )

                # Create temporary table for mesh terms
                cursor.execute(
                    """
                CREATE TEMPORARY TABLE IF NOT EXISTS temp_pubmed_mesh (
                    pmid VARCHAR(20) NOT NULL,
                    mesh_term TEXT NOT NULL
                )
                """
                )

                # Create temporary table for chemicals
                cursor.execute(
                    """
                CREATE TEMPORARY TABLE IF NOT EXISTS temp_pubmed_chemicals (
                    pmid VARCHAR(20) NOT NULL,
                    chemical_name TEXT NOT NULL
                )
                """
                )

                # Create temporary table for keywords
                cursor.execute(
                    """
                CREATE TEMPORARY TABLE IF NOT EXISTS temp_pubmed_keywords (
                    pmid VARCHAR(20) NOT NULL,
                    keyword TEXT NOT NULL
                )
                """
                )

                # Create temporary table for publication types
                cursor.execute(
                    """
                CREATE TEMPORARY TABLE IF NOT EXISTS temp_pubmed_publication_types (
                    pmid VARCHAR(20) NOT NULL,
                    publication_type TEXT NOT NULL
                )
                """
                )

                self.temp_tables_created = True
                logger.info("Temporary tables created successfully")
            except Exception as e:
                logger.error(f"Error creating temporary tables: {e}")
                conn.rollback()
                raise

    def clear_temp_tables(self):
        """Truncate all temporary tables to prepare for a new batch."""
        if not self.temp_tables_created:
            self.create_temp_tables()
            return

        conn = self.connect()
        with conn.cursor() as cursor:
            try:
                cursor.execute("TRUNCATE TABLE temp_pubmed_records")
                cursor.execute("TRUNCATE TABLE temp_pubmed_authors")
                cursor.execute("TRUNCATE TABLE temp_pubmed_mesh")
                cursor.execute("TRUNCATE TABLE temp_pubmed_chemicals")
                cursor.execute("TRUNCATE TABLE temp_pubmed_keywords")
                cursor.execute("TRUNCATE TABLE temp_pubmed_publication_types")
                logger.info("Temporary tables truncated")
            except psycopg2.Error as e:
                logger.error(f"Error truncating temp tables: {e}")
                conn.rollback()
                self.create_temp_tables()

    def bulk_insert_records(self, records: List[PubMedRecord]) -> int:
        """Insert a batch of PubMed records into the database."""
        if not records:
            return 0

        conn = self.connect()

        # Validate and create temp tables if needed
        if not self.temp_tables_created:
            self.create_temp_tables()
        else:
            self.clear_temp_tables()

        # Deduplicate records by PMID
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
            chemical_rows = []
            keyword_rows = []
            publication_type_rows = []

            # Prepare data for bulk insert
            for record in deduplicated_records:
                # Format public date for database if it's not already in the right format
                pub_date = record.pub_date

                # Prepare main record data
                pubmed_rows.append(
                    (
                        record.pmid,
                        record.title,
                        record.title_normalized,
                        record.journal,
                        getattr(record, "journal_iso", ""),
                        getattr(record, "issn", ""),
                        getattr(record, "issn_linking", ""),
                        getattr(record, "issn_type", ""),
                        getattr(record, "volume", ""),
                        getattr(record, "issue", ""),
                        getattr(record, "pagination", ""),
                        getattr(record, "doi", ""),
                        record.year,
                        pub_date,
                        getattr(record, "pub_status", ""),
                        getattr(record, "language", ""),
                        getattr(record, "vernacular_title", ""),
                        getattr(record, "medline_ta", ""),
                        getattr(record, "nlm_unique_id", ""),
                        getattr(record, "country", ""),
                        record.abstract,
                        record.source_file,
                    )
                )

                # Prepare author data
                for author_name, position in record.authors:
                    if author_name and author_name.strip():
                        author_rows.append((record.pmid, author_name, position))

                # Prepare mesh term data
                for mesh_term in record.mesh_terms:
                    if mesh_term and mesh_term.strip():
                        mesh_rows.append((record.pmid, mesh_term))

                # Handle other reference data if available
                if hasattr(record, "chemicals") and record.chemicals:
                    for chemical in record.chemicals:
                        chemical_rows.append((record.pmid, chemical))

                if hasattr(record, "keywords") and record.keywords:
                    for keyword in record.keywords:
                        keyword_rows.append((record.pmid, keyword))

                if hasattr(record, "publication_types") and record.publication_types:
                    for pub_type in record.publication_types:
                        publication_type_rows.append((record.pmid, pub_type))

            try:
                # Log batch statistics for debugging
                logger.info(
                    f"Inserting batch: {len(pubmed_rows)} records, {len(author_rows)} authors, {len(mesh_rows)} mesh terms"
                )
                if not author_rows:
                    logger.warning("No author data found in this batch!")
                    # Debug the first few records to see if they have authors
                    for i, record in enumerate(deduplicated_records[:3]):
                        logger.warning(
                            f"Record {i} (PMID: {record.pmid}) has {len(record.authors)} authors"
                        )

                # Perform bulk inserts
                self._copy_records(cursor, "temp_pubmed_records", pubmed_rows)
                if author_rows:
                    self._copy_records(cursor, "temp_pubmed_authors", author_rows)
                if mesh_rows:
                    self._copy_records(cursor, "temp_pubmed_mesh", mesh_rows)
                if chemical_rows:
                    self._copy_records(cursor, "temp_pubmed_chemicals", chemical_rows)
                if keyword_rows:
                    self._copy_records(cursor, "temp_pubmed_keywords", keyword_rows)
                if publication_type_rows:
                    self._copy_records(
                        cursor, "temp_pubmed_publication_types", publication_type_rows
                    )

                # Merge data from temp tables to permanent tables
                self._merge_temp_data(cursor)
                total_records = len(pubmed_rows)

                # Verify insertion success
                cursor.execute(
                    "SELECT COUNT(*) FROM pubmed_records WHERE pmid = ANY(%s)",
                    ([r.pmid for r in deduplicated_records],),
                )
                inserted_count = cursor.fetchone()[0]
                logger.info(
                    f"Verification: {inserted_count}/{len(deduplicated_records)} records in database"
                )

                if author_rows:
                    # Check author insertion
                    sample_pmids = [
                        author_rows[i][0] for i in range(min(5, len(author_rows)))
                    ]
                    cursor.execute(
                        """
                        SELECT COUNT(*) FROM pubmed_authors pa 
                        JOIN authors a ON pa.author_id = a.id
                        WHERE pa.pubmed_id = ANY(%s)
                    """,
                        (sample_pmids,),
                    )
                    author_count = cursor.fetchone()[0]
                    logger.info(
                        f"Verification: Found {author_count} authors for {len(sample_pmids)} sample PMIDs"
                    )

            except Exception as e:
                logger.error(f"Database error during bulk insert: {e}")
                logger.error(f"Error type: {type(e).__name__}")
                if isinstance(e, psycopg2.Error):
                    logger.error(f"SQL State: {e.pgcode}, Message: {e.pgerror}")
                conn.rollback()
                raise

            return total_records

    def _copy_records(self, cursor, table_name: str, rows: List[Tuple]) -> None:
        """Use PostgreSQL COPY for efficient bulk data loading."""
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
                    # Handle special characters for PostgreSQL COPY
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
        try:
            cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH NULL AS ''", buffer)
            logger.debug(f"Copied {len(rows)} rows to {table_name}")
        except Exception as e:
            logger.error(f"Error during COPY to {table_name}: {e}")
            # For debugging, show a sample of problematic data
            if len(rows) > 0:
                logger.error(f"Sample data (first row): {rows[0]}")
            raise

    def _merge_temp_data(self, cursor) -> None:
        """Merge data from temporary tables into permanent tables."""
        try:
            # First de-duplicate the temporary table to avoid the constraint violation
            cursor.execute(
                """
                CREATE TEMPORARY TABLE temp_pubmed_records_dedup AS
                SELECT DISTINCT ON (pmid) 
                    pmid, title, title_normalized, journal, journal_iso, issn, issn_linking,
                    issn_type, volume, issue, pagination, doi, year, pub_date, pub_status,
                    language, vernacular_title, medline_ta, nlm_unique_id, country,
                    abstract, source_file
                FROM temp_pubmed_records
                ORDER BY pmid, (LENGTH(title) + LENGTH(abstract)) DESC
                """
            )

            # Replace the original temp table with the de-duplicated version
            cursor.execute("DROP TABLE temp_pubmed_records")
            cursor.execute(
                "ALTER TABLE temp_pubmed_records_dedup RENAME TO temp_pubmed_records"
            )

            # 1. Insert main PubMed records
            cursor.execute(
                """
                INSERT INTO pubmed_records (
                    pmid, title, title_normalized, journal, journal_iso, issn, issn_linking,
                    issn_type, volume, issue, pagination, doi, year, pub_date, pub_status,
                    language, vernacular_title, medline_ta, nlm_unique_id, country,
                    abstract, processed_at, source_file
                )
                SELECT 
                    pmid, title, title_normalized, journal, journal_iso, issn, issn_linking,
                    issn_type, volume, issue, pagination, doi, year, 
                    CASE WHEN pub_date ~ '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$' THEN pub_date::DATE
                        WHEN pub_date ~ '^[0-9]{4}-[0-9]{1,2}$' THEN (pub_date || '-01')::DATE
                        WHEN pub_date ~ '^[0-9]{4}$' THEN (pub_date || '-01-01')::DATE
                        ELSE NULL
                    END as pub_date,
                    pub_status, language, vernacular_title, medline_ta, nlm_unique_id, country,
                    abstract, NOW(), source_file
                FROM temp_pubmed_records
                ON CONFLICT (pmid) DO UPDATE SET
                    title = EXCLUDED.title,
                    title_normalized = EXCLUDED.title_normalized,
                    journal = EXCLUDED.journal,
                    journal_iso = EXCLUDED.journal_iso,
                    issn = EXCLUDED.issn,
                    issn_linking = EXCLUDED.issn_linking,
                    issn_type = EXCLUDED.issn_type,
                    volume = EXCLUDED.volume, 
                    issue = EXCLUDED.issue,
                    pagination = EXCLUDED.pagination,
                    doi = EXCLUDED.doi,
                    year = EXCLUDED.year,
                    pub_date = EXCLUDED.pub_date,
                    pub_status = EXCLUDED.pub_status,
                    language = EXCLUDED.language,
                    vernacular_title = EXCLUDED.vernacular_title,
                    medline_ta = EXCLUDED.medline_ta,
                    nlm_unique_id = EXCLUDED.nlm_unique_id,
                    country = EXCLUDED.country,
                    abstract = EXCLUDED.abstract,
                    processed_at = NOW(),
                    source_file = EXCLUDED.source_file
                """
            )
            logger.info(f"Merged {cursor.rowcount} records into pubmed_records table")

            # De-duplicate the other temporary tables as well before merging
            self._deduplicate_relations_table(
                cursor, "temp_pubmed_authors", ["pmid", "author_name"]
            )
            self._deduplicate_relations_table(
                cursor, "temp_pubmed_mesh", ["pmid", "mesh_term"]
            )
            self._deduplicate_relations_table(
                cursor, "temp_pubmed_chemicals", ["pmid", "chemical_name"]
            )
            self._deduplicate_relations_table(
                cursor, "temp_pubmed_keywords", ["pmid", "keyword"]
            )
            self._deduplicate_relations_table(
                cursor, "temp_pubmed_publication_types", ["pmid", "publication_type"]
            )

            # 2. Insert authors and link to PubMed records
            # - First insert unique authors with ON CONFLICT DO NOTHING
            cursor.execute(
                """
                INSERT INTO authors (name, name_normalized)
                SELECT DISTINCT author_name, normalize_text(author_name)
                FROM temp_pubmed_authors
                ON CONFLICT (name) DO NOTHING
                """
            )
            author_insert_count = cursor.rowcount
            logger.info(f"Inserted {author_insert_count} new unique authors")

            # - Then link authors to PubMed records
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
            author_link_count = cursor.rowcount
            logger.info(f"Linked {author_link_count} authors to PubMed records")

            # 3. Insert MeSH terms and link to PubMed records
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
                INSERT INTO pubmed_mesh_terms (pubmed_id, mesh_id)
                SELECT 
                    t.pmid, m.id
                FROM temp_pubmed_mesh t
                JOIN mesh_terms m ON t.mesh_term = m.term
                ON CONFLICT (pubmed_id, mesh_id) DO NOTHING
                """
            )

            # 4. Handle chemicals if present
            try:
                cursor.execute(
                    """
                    INSERT INTO chemicals (name)
                    SELECT DISTINCT chemical_name
                    FROM temp_pubmed_chemicals
                    ON CONFLICT (name) DO NOTHING
                    """
                )

                cursor.execute(
                    """
                    INSERT INTO pubmed_chemicals (pubmed_id, chemical_id)
                    SELECT 
                        t.pmid, c.id
                    FROM temp_pubmed_chemicals t
                    JOIN chemicals c ON t.chemical_name = c.name
                    ON CONFLICT (pubmed_id, chemical_id) DO NOTHING
                    """
                )
            except Exception as e:
                logger.warning(f"Chemical data insertion skipped: {e}")

            # 5. Handle keywords if present
            try:
                cursor.execute(
                    """
                    INSERT INTO keywords (keyword)
                    SELECT DISTINCT keyword
                    FROM temp_pubmed_keywords
                    ON CONFLICT (keyword) DO NOTHING
                    """
                )

                cursor.execute(
                    """
                    INSERT INTO pubmed_keywords (pubmed_id, keyword_id)
                    SELECT 
                        t.pmid, k.id
                    FROM temp_pubmed_keywords t
                    JOIN keywords k ON t.keyword = k.keyword
                    ON CONFLICT (pubmed_id, keyword_id) DO NOTHING
                    """
                )
            except Exception as e:
                logger.warning(f"Keyword data insertion skipped: {e}")

            # 6. Handle publication types if present
            try:
                cursor.execute(
                    """
                    INSERT INTO publication_types (type)
                    SELECT DISTINCT publication_type
                    FROM temp_pubmed_publication_types
                    ON CONFLICT (type) DO NOTHING
                    """
                )

                cursor.execute(
                    """
                    INSERT INTO pubmed_publication_types (pubmed_id, type_id)
                    SELECT 
                        t.pmid, pt.id
                    FROM temp_pubmed_publication_types t
                    JOIN publication_types pt ON t.publication_type = pt.type
                    ON CONFLICT (pubmed_id, type_id) DO NOTHING
                    """
                )
            except Exception as e:
                logger.warning(f"Publication type data insertion skipped: {e}")

        except Exception as e:
            logger.error(f"Error during temp data merge: {e}")
            if isinstance(e, psycopg2.Error):
                logger.error(f"SQL State: {e.pgcode}, Message: {e.pgerror}")
            raise

    def _deduplicate_relations_table(self, cursor, table_name, unique_columns):
        """De-duplicates a temporary relation table based on the specified unique columns."""
        try:
            # Create column list for the query
            column_list = ", ".join(unique_columns)

            # Create the deduplication query
            dedup_table = f"{table_name}_dedup"
            cursor.execute(
                f"""
                CREATE TEMPORARY TABLE {dedup_table} AS
                SELECT DISTINCT ON ({column_list}) *
                FROM {table_name}
            """
            )

            # Replace the original table with the deduplicated one
            cursor.execute(f"DROP TABLE {table_name}")
            cursor.execute(f"ALTER TABLE {dedup_table} RENAME TO {table_name}")

        except Exception as e:
            logger.warning(f"Error deduplicating {table_name}: {e}")


# ========================= XML PROCESSING =========================


def parse_xml_file(file_path: str) -> List[PubMedRecord]:
    records = []
    filename = os.path.basename(file_path)

    try:
        with gzip.open(file_path, "rb") as f:
            context = ET.iterparse(f, events=("start", "end"))
            in_pubmed_article = False
            current_path = []

            for event, elem in context:
                tag = elem.tag.split("}")[-1]

                if event == "start":
                    current_path.append(tag)
                    if tag == "PubmedArticle":
                        in_pubmed_article = True
                        pmid = title = journal = journal_iso = issn = issn_type = ""
                        issn_linking = volume = issue = pagination = doi = pub_date = ""
                        year = pub_status = language = vernacular_title = abstract = ""
                        authors = []
                        mesh_terms = []
                        chemicals = []
                        keywords = []
                        publication_types = []
                        in_author_list = in_author = False
                        current_author_last_name = ""
                        current_author_fore_name = ""
                        current_author_initials = ""
                        current_author_position = 0

                    elif tag == "AuthorList":
                        in_author_list = True
                        current_author_position = 0

                    elif tag == "Author" and in_author_list:
                        in_author = True
                        current_author_last_name = ""
                        current_author_fore_name = ""
                        current_author_initials = ""
                        current_author_position += 1

                elif event == "end":
                    if not in_pubmed_article:
                        current_path.pop()
                        elem.clear()
                        continue

                    if tag == "LastName":
                        current_author_last_name = (
                            elem.text.strip() if elem.text else ""
                        )
                    elif tag == "ForeName":
                        current_author_fore_name = (
                            elem.text.strip() if elem.text else ""
                        )
                    elif tag == "Initials":
                        current_author_initials = elem.text.strip() if elem.text else ""
                    elif tag == "CollectiveName":
                        collective_name = elem.text.strip() if elem.text else ""
                        if collective_name:
                            authors.append((collective_name, current_author_position))
                    elif tag == "Author":
                        in_author = False
                        name_part = current_author_fore_name or current_author_initials
                        if current_author_last_name or name_part:
                            if current_author_last_name and name_part:
                                author_name = f"{name_part} {current_author_last_name}"
                            else:
                                author_name = current_author_last_name or name_part
                            if author_name.strip():
                                authors.append(
                                    (author_name.strip(), current_author_position)
                                )
                    elif tag == "AuthorList":
                        in_author_list = False
                    elif tag == "PMID" and current_path[-2] in [
                        "PubmedData",
                        "MedlineCitation",
                    ]:
                        pmid = elem.text.strip() if elem.text else ""
                    elif tag == "ArticleTitle":
                        title = elem.text.strip() if elem.text else ""
                    elif tag == "Title" and "Journal" in current_path:
                        journal = elem.text.strip() if elem.text else ""
                    elif tag == "ISOAbbreviation":
                        journal_iso = elem.text.strip() if elem.text else ""
                    elif tag == "ISSN":
                        issn = elem.text.strip() if elem.text else ""
                        issn_type = elem.get("IssnType", "")
                    elif tag == "ISSNLinking":
                        issn_linking = elem.text.strip() if elem.text else ""
                    elif tag == "Volume":
                        volume = elem.text.strip() if elem.text else ""
                    elif tag == "Issue":
                        issue = elem.text.strip() if elem.text else ""
                    elif tag == "MedlinePgn":
                        pagination = elem.text.strip() if elem.text else ""
                    elif tag == "ArticleId" and elem.get("IdType") == "doi":
                        doi = elem.text.strip() if elem.text else ""
                    elif tag == "Year" and "PubDate" in current_path:
                        year_text = elem.text.strip() if elem.text else ""
                        try:
                            year = int(year_text) if year_text.isdigit() else None
                        except:
                            year = None
                    elif tag == "AbstractText":
                        abstract_text = elem.text.strip() if elem.text else ""
                        if abstract_text:
                            abstract += (
                                (" " + abstract_text) if abstract else abstract_text
                            )
                    elif tag == "DescriptorName":
                        mesh_term = elem.text.strip() if elem.text else ""
                        if mesh_term:
                            mesh_terms.append(mesh_term)
                    elif tag == "PubmedArticle":
                        in_pubmed_article = False
                        if pmid and title:
                            record = PubMedRecord(
                                pmid=pmid,
                                title=title,
                                title_normalized=normalize_text(title),
                                journal=journal,
                                journal_iso=journal_iso,
                                issn=issn,
                                issn_type=issn_type,
                                issn_linking=issn_linking,
                                volume=volume,
                                issue=issue,
                                pagination=pagination,
                                doi=doi,
                                pub_date=pub_date,
                                year=year,
                                pub_status=pub_status,
                                language=language,
                                vernacular_title=vernacular_title,
                                abstract=abstract,
                                authors=authors,
                                mesh_terms=mesh_terms,
                                chemicals=chemicals,
                                keywords=keywords,
                                publication_types=publication_types,
                                source_file=filename,
                            )
                            records.append(record)

                    current_path.pop()
                    elem.clear()

        return records

    except Exception as e:
        import traceback

        print(f"Error: {e}")
        print(traceback.format_exc())
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
