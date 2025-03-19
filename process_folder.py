#!/usr/bin/env python3
import pandas as pd
import xml.etree.ElementTree as ET
import gzip
import os
import re
import psycopg2
import concurrent.futures
import io


# Connection config
DB_CONFIG = {
    "host": "localhost",
    "database": "pubmed_integration",
    "user": "pubmed",
    "password": "pubmed_password",
}


# Function to normalize text
def normalize_text(text):
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[^\w\s]", "", text)
    return text


# Process a single XML file and return CSV-formatted data
def process_xml_file(file_path):
    file_name = os.path.basename(file_path)
    print(f"Processing {file_name}")

    pubmed_records = []
    author_links = []
    mesh_terms = []

    try:
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            # Streaming parser to avoid loading entire file
            context = ET.iterparse(f, events=("end",))
            for event, elem in context:
                if elem.tag == "PubmedArticle":
                    # Extract PMID
                    pmid_elem = elem.find(".//PMID")
                    if pmid_elem is None:
                        elem.clear()
                        continue

                    pmid = pmid_elem.text

                    # Extract title
                    title_elem = elem.find(".//ArticleTitle")
                    title = title_elem.text if title_elem is not None else ""

                    # Extract journal, volume, issue, year
                    journal_elem = elem.find(".//Journal/Title")
                    journal = journal_elem.text if journal_elem is not None else ""

                    volume_elem = elem.find(".//JournalIssue/Volume")
                    volume = volume_elem.text if volume_elem is not None else ""

                    issue_elem = elem.find(".//JournalIssue/Issue")
                    issue = issue_elem.text if issue_elem is not None else ""

                    year_elem = elem.find(".//PubDate/Year")
                    year = year_elem.text if year_elem is not None else ""

                    # Extract abstract
                    abstract_elem = elem.find(".//Abstract/AbstractText")
                    abstract = abstract_elem.text if abstract_elem is not None else ""

                    # Add to records
                    pubmed_records.append(
                        [
                            pmid,
                            title,
                            normalize_text(title),
                            journal,
                            volume,
                            issue,
                            year,
                            abstract,
                            file_name,
                        ]
                    )

                    # Process authors
                    for i, author_elem in enumerate(elem.findall(".//Author")):
                        last_name = author_elem.findtext("LastName", "")
                        fore_name = author_elem.findtext("ForeName", "")
                        author_name = f"{fore_name} {last_name}".strip()
                        if author_name:
                            author_links.append([pmid, author_name, i + 1])

                    # Process MeSH terms
                    for mesh_elem in elem.findall(
                        ".//MeshHeadingList/MeshHeading/DescriptorName"
                    ):
                        if mesh_elem is not None and mesh_elem.text:
                            mesh_terms.append([pmid, mesh_elem.text])

                    # Clear element to save memory
                    elem.clear()

    except Exception as e:
        print(f"Error processing {file_name}: {e}")
        return None

    return {
        "pubmed_records": pubmed_records,
        "author_links": author_links,
        "mesh_terms": mesh_terms,
    }


# Import data to PostgreSQL using COPY
def import_data_to_postgres(data, conn):
    cursor = conn.cursor()

    # Import pubmed_records
    if data["pubmed_records"]:
        pubmed_copy = io.StringIO()
        for record in data["pubmed_records"]:
            pubmed_copy.write(
                "\t".join(
                    [
                        str(field).replace("\t", " ").replace("\n", " ")
                        for field in record
                    ]
                )
                + "\n"
            )
        pubmed_copy.seek(0)

        cursor.copy_expert(
            """
            COPY pubmed_records_temp (
                pmid, title, title_normalized, journal, volume, issue, year, abstract, source_file
            ) FROM STDIN WITH NULL AS ''
            """,
            pubmed_copy,
        )

    # Similar COPY operations for authors and mesh terms
    # ...

    conn.commit()
    cursor.close()


# Process all files in a folder concurrently
def process_folder(folder_path, conn, max_workers=8):
    files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".xml.gz")
    ]

    # Set up temporary tables for bulk import
    setup_temp_tables(conn)

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(process_xml_file, files):
            if result:
                import_data_to_postgres(result, conn)

    # Merge temp tables into main tables
    merge_temp_tables(conn)


# Main processing function
def main():
    # Base folder containing all convertir_X folders
    base_folder = "Pubmed_all"

    # Get all convertir folders
    convertir_folders = [
        os.path.join(base_folder, d)
        for d in os.listdir(base_folder)
        if d.startswith("convertir_") and os.path.isdir(os.path.join(base_folder, d))
    ]

    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)

    # Process each folder
    for folder in convertir_folders:
        print(f"Processing folder: {folder}")
        process_folder(folder, conn)

    # Create matches
    create_matches(conn)

    conn.close()


if __name__ == "__main__":
    main()
