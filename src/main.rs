use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use clap::{App, Arg};
use csv::{ReaderBuilder, WriterBuilder};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use strsim::normalized_levenshtein;
use tokio_postgres::{Client, NoTls};

// Configuration constants
const BATCH_SIZE: usize = 5000;
const SIMILARITY_THRESHOLD: f64 = 0.85;
const HIGH_SIMILARITY_THRESHOLD: f64 = 0.90;
const DOI_MATCH_QUALITY: f64 = 100.0;
const EXACT_TITLE_MATCH_QUALITY: f64 = 100.0;
const TITLE_YEAR_MATCH_QUALITY: f64 = 90.0;
const FUZZY_TITLE_MATCH_QUALITY: f64 = 80.0;

// Structs for CSV handling
#[derive(Debug, Deserialize)]
struct Publication {
    cfrespublid: String,
    title: String,
    cfpersid_id: String,
    author: String,
    cfrespubldate: String,
    year: String,
    cfdctype_id: String,
    cfdctype: String,
    volume: String,
    number: String,
    pages: String,
}

#[derive(Debug, Serialize)]
struct EnrichedPublication {
    // Original fields
    cfrespublid: String,
    title: String,
    cfpersid_id: String,
    author: String,
    cfrespubldate: String,
    year: String,
    cfdctype_id: String,
    cfdctype: String,
    volume: String,
    number: String,
    pages: String,

    // Enriched fields
    pmid: String,
    pubmed_title: String,
    pubmed_journal: String,
    pubmed_abstract: String,
    pubmed_doi: String,
    pubmed_authors: String,
    pubmed_mesh_terms: String,
    match_quality: f64,
    match_type: String,
}

// Database models
#[derive(Debug)]
struct PubMedRecord {
    pmid: String,
    title: String,
    title_normalized: String,
    journal: String,
    abstract_text: String,
    doi: Option<String>,
    year: Option<i32>,
    authors: Vec<String>,
    mesh_terms: Vec<String>,
}

#[derive(Debug, Clone)]
struct Match {
    publication_id: String,
    pmid: String,
    match_quality: f64,
    match_type: String,
}

// Database handler
struct DbHandler {
    client: Client,
}

impl DbHandler {
    async fn new(connection_str: &str) -> Result<Self, Box<dyn Error>> {
        let (client, connection) = tokio_postgres::connect(connection_str, NoTls).await?;

        // Spawn the connection task to a separate handle
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Optimize database for the matching task
        client
            .batch_execute(
                "
            SET work_mem = '256MB';
            SET synchronous_commit = OFF;
            SET max_parallel_workers_per_gather = 4;
        ",
            )
            .await?;

        Ok(DbHandler { client })
    }

    async fn fetch_pubmed_records(
        &self,
        batch_size: usize,
        offset: usize,
    ) -> Result<Vec<PubMedRecord>, Box<dyn Error>> {
        let query = "
            SELECT pr.pmid, pr.title, pr.title_normalized, 
                   COALESCE(pr.journal, '') as journal, 
                   COALESCE(pr.abstract, '') as abstract_text,
                   pr.doi, pr.year
            FROM pubmed_records pr
            ORDER BY pr.pmid
            LIMIT $1 OFFSET $2
        ";

        let rows = self
            .client
            .query(query, &[&(batch_size as i64), &(offset as i64)])
            .await?;

        let mut records = Vec::with_capacity(rows.len());

        for row in rows {
            let pmid: String = row.get("pmid");

            // Fetch authors for this PMID
            let authors = self.fetch_authors(&pmid).await?;

            // Fetch mesh terms for this PMID
            let mesh_terms = self.fetch_mesh_terms(&pmid).await?;

            records.push(PubMedRecord {
                pmid,
                title: row.get("title"),
                title_normalized: row.get("title_normalized"),
                journal: row.get("journal"),
                abstract_text: row.get("abstract_text"),
                doi: row.get("doi"),
                year: row.get("year"),
                authors,
                mesh_terms,
            });
        }

        Ok(records)
    }

    async fn fetch_authors(&self, pmid: &str) -> Result<Vec<String>, Box<dyn Error>> {
        let query = "
            SELECT a.name
            FROM pubmed_authors pa
            JOIN authors a ON pa.author_id = a.id
            WHERE pa.pubmed_id = $1
            ORDER BY pa.position
        ";

        let rows = self.client.query(query, &[&pmid]).await?;

        let authors = rows
            .iter()
            .map(|row| row.get::<_, String>("name"))
            .collect();

        Ok(authors)
    }

    async fn fetch_mesh_terms(&self, pmid: &str) -> Result<Vec<String>, Box<dyn Error>> {
        let query = "
            SELECT m.term
            FROM pubmed_mesh_terms pmt
            JOIN mesh_terms m ON pmt.mesh_id = m.id
            WHERE pmt.pubmed_id = $1
        ";

        let rows = self.client.query(query, &[&pmid]).await?;

        let mesh_terms = rows
            .iter()
            .map(|row| row.get::<_, String>("term"))
            .collect();

        Ok(mesh_terms)
    }

    async fn count_pubmed_records(&self) -> Result<i64, Box<dyn Error>> {
        let row = self
            .client
            .query_one("SELECT COUNT(*) FROM pubmed_records", &[])
            .await?;
        Ok(row.get::<_, i64>(0))
    }

    async fn insert_matches(&self, matches: &[Match]) -> Result<(), Box<dyn Error>> {
        // For smaller numbers of matches, using individual inserts can be simpler
        if matches.len() < 1000 {
            for batch in matches.chunks(100) {
                let mut query = String::from(
                    "INSERT INTO publication_pubmed_matches 
                    (publication_id, pmid, match_quality, match_type) VALUES ",
                );

                for (i, m) in batch.iter().enumerate() {
                    let match_type = m.match_type.replace('\'', "''"); // Escape single quotes
                    if i > 0 {
                        query.push_str(", ");
                    }
                    query.push_str(&format!(
                        "('{}', '{}', {}, '{}'::{}))",
                        m.publication_id.replace('\'', "''"),
                        m.pmid.replace('\'', "''"),
                        m.match_quality,
                        match_type,
                        "match_type"
                    ));
                }

                query.push_str(
                    " ON CONFLICT (publication_id, pmid) DO UPDATE SET 
                               match_quality = EXCLUDED.match_quality,
                               match_type = EXCLUDED.match_type",
                );

                self.client.execute(&query, &[]).await?;
            }
        } else {
            // For larger numbers of matches, create a temporary table
            self.client
                .batch_execute(
                    "
                CREATE TEMPORARY TABLE IF NOT EXISTS temp_matches (
                    publication_id VARCHAR(255),
                    pmid VARCHAR(20),
                    match_quality NUMERIC(5,2),
                    match_type VARCHAR(50)
                );
                
                TRUNCATE TABLE temp_matches;
            ",
                )
                .await?;

            // Insert matches in batches
            for batch in matches.chunks(1000) {
                let mut query = String::from(
                    "INSERT INTO temp_matches (publication_id, pmid, match_quality, match_type) VALUES "
                );

                for (i, m) in batch.iter().enumerate() {
                    let match_type = m.match_type.replace('\'', "''"); // Escape single quotes
                    if i > 0 {
                        query.push_str(", ");
                    }
                    query.push_str(&format!(
                        "('{}', '{}', {}, '{}')",
                        m.publication_id.replace('\'', "''"),
                        m.pmid.replace('\'', "''"),
                        m.match_quality,
                        match_type
                    ));
                }

                self.client.execute(&query, &[]).await?;
            }

            // Insert from temp table to the actual table with conflict handling
            self.client.execute("
                INSERT INTO publication_pubmed_matches (publication_id, pmid, match_quality, match_type)
                SELECT publication_id, pmid, match_quality, match_type::match_type
                FROM temp_matches
                ON CONFLICT (publication_id, pmid) 
                DO UPDATE SET 
                    match_quality = EXCLUDED.match_quality,
                    match_type = EXCLUDED.match_type
            ", &[]).await?;
        }

        // Insert from temp table to the actual table with conflict handling
        self.client
            .execute(
                "
            INSERT INTO publication_pubmed_matches (publication_id, pmid, match_quality, match_type)
            SELECT publication_id, pmid, match_quality, match_type::match_type
            FROM temp_matches
            ON CONFLICT (publication_id, pmid) 
            DO UPDATE SET 
                match_quality = EXCLUDED.match_quality,
                match_type = EXCLUDED.match_type
        ",
                &[],
            )
            .await?;

        Ok(())
    }

    async fn fetch_enriched_publications(
        &self,
        publication_ids: &[String],
    ) -> Result<HashMap<String, PubMedRecord>, Box<dyn Error>> {
        let query = "
            WITH matched_publications AS (
                SELECT 
                    m.publication_id, 
                    m.pmid,
                    m.match_quality,
                    m.match_type
                FROM 
                    publication_pubmed_matches m
                WHERE 
                    m.publication_id = ANY($1)
            )
            SELECT 
                mp.publication_id,
                pr.pmid, 
                pr.title, 
                pr.title_normalized,
                COALESCE(pr.journal, '') as journal,
                COALESCE(pr.abstract, '') as abstract_text,
                pr.doi,
                pr.year,
                STRING_AGG(DISTINCT a.name, '; ') AS authors,
                STRING_AGG(DISTINCT mt.term, '; ') AS mesh_terms
            FROM 
                matched_publications mp
            JOIN 
                pubmed_records pr ON mp.pmid = pr.pmid
            LEFT JOIN 
                pubmed_authors pa ON pr.pmid = pa.pubmed_id
            LEFT JOIN 
                authors a ON pa.author_id = a.id
            LEFT JOIN 
                pubmed_mesh_terms pmt ON pr.pmid = pmt.pubmed_id
            LEFT JOIN 
                mesh_terms mt ON pmt.mesh_id = mt.id
            GROUP BY 
                mp.publication_id, pr.pmid, pr.title, pr.title_normalized, pr.journal, pr.abstract, pr.doi, pr.year
        ";

        let rows = self.client.query(query, &[&publication_ids]).await?;

        let mut enriched_records = HashMap::new();

        for row in rows {
            let publication_id: String = row.get("publication_id");
            let authors_str: String = row.get("authors");
            let mesh_terms_str: String = row.get("mesh_terms");

            let authors = authors_str
                .split("; ")
                .map(|s| s.to_string())
                .collect::<Vec<String>>();

            let mesh_terms = mesh_terms_str
                .split("; ")
                .map(|s| s.to_string())
                .collect::<Vec<String>>();

            enriched_records.insert(
                publication_id,
                PubMedRecord {
                    pmid: row.get("pmid"),
                    title: row.get("title"),
                    title_normalized: row.get("title_normalized"),
                    journal: row.get("journal"),
                    abstract_text: row.get("abstract_text"),
                    doi: row.get("doi"),
                    year: row.get("year"),
                    authors,
                    mesh_terms,
                },
            );
        }

        Ok(enriched_records)
    }
}

// Utilities
fn normalize_text(text: &str) -> String {
    text.to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ")
}

// Matching logic
fn find_matches(publication: &Publication, pubmed_records: &[PubMedRecord]) -> Vec<Match> {
    let mut matches = Vec::new();

    // Normalize publication title for comparison
    let title_normalized = normalize_text(&publication.title);

    // Check if we should try to parse year
    let publication_year = publication.year.parse::<i32>().ok();

    for record in pubmed_records {
        let mut match_quality = 0.0;
        let mut match_type = String::new();

        // Exact title match
        if title_normalized == record.title_normalized {
            match_quality = EXACT_TITLE_MATCH_QUALITY;
            match_type = "exact_title".to_string();
        }
        // Try DOI match if available
        else if !publication.title.is_empty()
            && record.doi.is_some()
            && !record.doi.as_ref().unwrap().is_empty()
            && record.doi.as_ref().unwrap() == &publication.title
        {
            match_quality = DOI_MATCH_QUALITY;
            match_type = "doi_match".to_string();
        }
        // Title + year match
        else if publication_year.is_some()
            && record.year.is_some()
            && publication_year == record.year
        {
            let similarity = normalized_levenshtein(&title_normalized, &record.title_normalized);
            if similarity > SIMILARITY_THRESHOLD {
                match_quality = similarity * TITLE_YEAR_MATCH_QUALITY;
                match_type = "title_year".to_string();
            }
        }
        // Fuzzy title match (high threshold)
        else if !title_normalized.is_empty() {
            let similarity = normalized_levenshtein(&title_normalized, &record.title_normalized);
            if similarity > HIGH_SIMILARITY_THRESHOLD {
                match_quality = similarity * FUZZY_TITLE_MATCH_QUALITY;
                match_type = "fuzzy_title".to_string();
            }
        }

        // If we found a match, add it to the list
        if match_quality > 0.0 {
            matches.push(Match {
                publication_id: publication.cfrespublid.clone(),
                pmid: record.pmid.clone(),
                match_quality,
                match_type,
            });
        }
    }

    // Sort by match quality (descending) and return only the best match
    matches.sort_by(|a, b| b.match_quality.partial_cmp(&a.match_quality).unwrap());

    if matches.is_empty() {
        return Vec::new();
    }

    // Return only the best match for this publication
    vec![matches[0].clone()]
}

// Main function with parallel processing
async fn process_publications(
    db_handler: &DbHandler,
    publications: &[Publication],
    progress_bar: &ProgressBar,
) -> Result<Vec<Match>, Box<dyn Error>> {
    // First, load all PubMed records into memory for faster matching
    let total_records = db_handler.count_pubmed_records().await?;
    println!("Loading {} PubMed records into memory...", total_records);

    let mut all_pubmed_records = Vec::new();
    let mut offset = 0;

    while (offset as i64) < total_records {
        let batch = db_handler.fetch_pubmed_records(BATCH_SIZE, offset).await?;
        all_pubmed_records.extend(batch);
        offset += BATCH_SIZE;

        print!(
            "\rLoaded {}/{} records ({:.1}%)...",
            offset.min(total_records as usize),
            total_records,
            100.0 * (offset as f64) / (total_records as f64)
        );
    }
    println!("\nAll PubMed records loaded.");

    // Use Arc to share the records across threads
    let pubmed_records = Arc::new(all_pubmed_records);

    // Use a mutex to collect matches from all threads
    let all_matches = Arc::new(Mutex::new(Vec::new()));

    // Process publications in parallel using Rayon
    publications.par_chunks(250).for_each(|chunk| {
        let thread_matches: Vec<Match> = chunk
            .iter()
            .flat_map(|pub_| {
                let matches = find_matches(pub_, &pubmed_records);
                progress_bar.inc(1);
                matches
            })
            .collect();

        // Add the matches to the shared vector
        if !thread_matches.is_empty() {
            let mut all_matches_lock = all_matches.lock().unwrap();
            all_matches_lock.extend(thread_matches);
        }
    });

    // Return the collected matches
    let result = Arc::try_unwrap(all_matches)
        .expect("Failed to unwrap Arc")
        .into_inner()
        .expect("Failed to unwrap Mutex");

    Ok(result)
}

async fn enrich_and_save_publications(
    db_handler: &DbHandler,
    publications: &[Publication],
    matches: &[Match],
    output_path: &str,
) -> Result<(), Box<dyn Error>> {
    // Create a mapping of publication IDs to matches
    let mut publication_to_match = HashMap::new();
    for m in matches {
        publication_to_match.insert(m.publication_id.clone(), m);
    }

    // Get all publication IDs that have matches
    let publication_ids: Vec<String> = matches.iter().map(|m| m.publication_id.clone()).collect();

    // Fetch enriched PubMed data for all matched publications
    let enriched_data = db_handler
        .fetch_enriched_publications(&publication_ids)
        .await?;

    // Create and configure a CSV writer
    let file = File::create(output_path)?;
    let mut writer = WriterBuilder::new()
        .delimiter(b',')
        .quote_style(csv::QuoteStyle::Necessary)
        .from_writer(file);

    // Write enriched publications
    for pub_ in publications {
        if let Some(m) = publication_to_match.get(&pub_.cfrespublid) {
            if let Some(pubmed_data) = enriched_data.get(&pub_.cfrespublid) {
                let enriched_pub = EnrichedPublication {
                    // Original fields
                    cfrespublid: pub_.cfrespublid.clone(),
                    title: pub_.title.clone(),
                    cfpersid_id: pub_.cfpersid_id.clone(),
                    author: pub_.author.clone(),
                    cfrespubldate: pub_.cfrespubldate.clone(),
                    year: pub_.year.clone(),
                    cfdctype_id: pub_.cfdctype_id.clone(),
                    cfdctype: pub_.cfdctype.clone(),
                    volume: pub_.volume.clone(),
                    number: pub_.number.clone(),
                    pages: pub_.pages.clone(),

                    // Enriched fields
                    pmid: pubmed_data.pmid.clone(),
                    pubmed_title: pubmed_data.title.clone(),
                    pubmed_journal: pubmed_data.journal.clone(),
                    pubmed_abstract: pubmed_data.abstract_text.clone(),
                    pubmed_doi: pubmed_data.doi.clone().unwrap_or_default(),
                    pubmed_authors: pubmed_data.authors.join("; "),
                    pubmed_mesh_terms: pubmed_data.mesh_terms.join("; "),
                    match_quality: m.match_quality,
                    match_type: m.match_type.clone(),
                };

                writer.serialize(enriched_pub)?;
            } else {
                // Fall back to just the original publication if no enriched data is available
                let enriched_pub = EnrichedPublication {
                    // Original fields
                    cfrespublid: pub_.cfrespublid.clone(),
                    title: pub_.title.clone(),
                    cfpersid_id: pub_.cfpersid_id.clone(),
                    author: pub_.author.clone(),
                    cfrespubldate: pub_.cfrespubldate.clone(),
                    year: pub_.year.clone(),
                    cfdctype_id: pub_.cfdctype_id.clone(),
                    cfdctype: pub_.cfdctype.clone(),
                    volume: pub_.volume.clone(),
                    number: pub_.number.clone(),
                    pages: pub_.pages.clone(),

                    // Empty enriched fields
                    pmid: String::new(),
                    pubmed_title: String::new(),
                    pubmed_journal: String::new(),
                    pubmed_abstract: String::new(),
                    pubmed_doi: String::new(),
                    pubmed_authors: String::new(),
                    pubmed_mesh_terms: String::new(),
                    match_quality: 0.0,
                    match_type: String::new(),
                };

                writer.serialize(enriched_pub)?;
            }
        } else {
            // No match found, write original publication with empty enriched fields
            let enriched_pub = EnrichedPublication {
                // Original fields
                cfrespublid: pub_.cfrespublid.clone(),
                title: pub_.title.clone(),
                cfpersid_id: pub_.cfpersid_id.clone(),
                author: pub_.author.clone(),
                cfrespubldate: pub_.cfrespubldate.clone(),
                year: pub_.year.clone(),
                cfdctype_id: pub_.cfdctype_id.clone(),
                cfdctype: pub_.cfdctype.clone(),
                volume: pub_.volume.clone(),
                number: pub_.number.clone(),
                pages: pub_.pages.clone(),

                // Empty enriched fields
                pmid: String::new(),
                pubmed_title: String::new(),
                pubmed_journal: String::new(),
                pubmed_abstract: String::new(),
                pubmed_doi: String::new(),
                pubmed_authors: String::new(),
                pubmed_mesh_terms: String::new(),
                match_quality: 0.0,
                match_type: String::new(),
            };

            writer.serialize(enriched_pub)?;
        }
    }

    writer.flush()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("PubMed Publication Matcher")
        .version("1.0")
        .author("Your Name <your.email@example.com>")
        .about("High-performance fuzzy matching for publications and PubMed records")
        .arg(
            Arg::with_name("input")
                .long("input")
                .short("i")
                .value_name("FILE")
                .help("CSV file containing publications to match")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("output")
                .long("output")
                .short("o")
                .value_name("FILE")
                .help("Output enriched CSV file")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("db-host")
                .long("db-host")
                .value_name("HOST")
                .help("Database host")
                .default_value("localhost")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("db-name")
                .long("db-name")
                .value_name("NAME")
                .help("Database name")
                .default_value("pubmed_integration")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("db-user")
                .long("db-user")
                .value_name("USER")
                .help("Database user")
                .default_value("pubmed")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("db-password")
                .long("db-password")
                .value_name("PASSWORD")
                .help("Database password")
                .default_value("pubmed_password")
                .takes_value(true),
        )
        .get_matches();

    let input_path = matches.value_of("input").unwrap();
    let output_path = matches.value_of("output").unwrap();

    // Get database connection details
    let db_host = matches.value_of("db-host").unwrap();
    let db_name = matches.value_of("db-name").unwrap();
    let db_user = matches.value_of("db-user").unwrap();
    let db_password = matches.value_of("db-password").unwrap();

    // Build connection string
    let connection_str = format!(
        "host={} dbname={} user={} password={}",
        db_host, db_name, db_user, db_password
    );

    // Initialize database handler
    println!("Connecting to database...");
    let db_handler = DbHandler::new(&connection_str).await?;

    // Read the input CSV file
    println!("Reading publications from {}...", input_path);
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .from_path(input_path)?;

    let publications: Vec<Publication> =
        rdr.deserialize().collect::<Result<Vec<Publication>, _>>()?;

    println!("Loaded {} publications.", publications.len());

    // Set up progress bar
    let pb = ProgressBar::new(publications.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({eta}) {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    // Start the matching process
    let start_time = Instant::now();
    println!("Finding matches...");

    let matches = process_publications(&db_handler, &publications, &pb).await?;
    pb.finish_with_message("Matching complete");

    println!(
        "Found {} matches in {:.2} seconds",
        matches.len(),
        start_time.elapsed().as_secs_f64()
    );

    // Save matches to database
    println!("Saving matches to database...");
    db_handler.insert_matches(&matches).await?;

    // Enrich and save publications
    println!("Enriching publications and saving to {}...", output_path);
    enrich_and_save_publications(&db_handler, &publications, &matches, output_path).await?;

    println!(
        "All done! Total time: {:.2} seconds",
        start_time.elapsed().as_secs_f64()
    );

    Ok(())
}
