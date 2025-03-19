use anyhow::{Context, Result};
use futures::{stream, StreamExt, TryStreamExt};
use log::{debug, error, info, warn};
use num_traits::ToPrimitive;
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, QueryBuilder, Transaction};
use std::time::Instant;

use crate::models::{MatchResult, PubMedRecord, Publication};
use crate::utils::normalize_string;

// Size constants for optimal processing
const BATCH_SIZE: usize = 1000;
const MAX_CONNECTIONS: u32 = 20;
const CONCURRENT_TASKS: usize = 4;

pub struct DatabaseService {
    pool: PgPool,
}

impl DatabaseService {
    /// Create a new DatabaseService with optimized connection pool
    pub async fn new(database_url: &str) -> Result<Self> {
        // Configure connection pool for optimal performance
        let pool = PgPoolOptions::new()
            .max_connections(MAX_CONNECTIONS)
            .min_connections(2)
            .max_lifetime(std::time::Duration::from_secs(1800)) // 30 minutes lifetime
            .idle_timeout(std::time::Duration::from_secs(600)) // 10 minutes idle timeout
            .connect(database_url)
            .await
            .context("Failed to connect to PostgreSQL database")?;

        info!("Successfully connected to PostgreSQL database");

        Ok(Self { pool })
    }

    /// Load publications from database with optimized query
    pub async fn load_publications(&self) -> Result<Vec<Publication>> {
        let start = Instant::now();
        info!("Loading publications from database");

        // Use streaming to handle large result sets
        let stream = sqlx::query_as!(
            Publication,
            r#"
            SELECT
                id as "id!",
                title as "title!",
                person_id,
                author,
                publication_date::TEXT,
                year::TEXT,
                doc_type_id::TEXT,
                doc_type,
                volume,
                number,
                pages,
                NULL as "pmid",
                NULL as "pubmed_authors",
                NULL as "pubmed_abstract",
                NULL as "pubmed_journal",
                NULL as "pubmed_mesh",
                NULL as "match_quality: f64",
                NULL as "match_type",
                NULL as "enrichment_date"
            FROM publications
            "#
        )
        .fetch(&self.pool);

        // Process the stream to avoid loading all records into memory at once
        let publications = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        info!(
            "Loaded {} publications in {:?}",
            publications.len(),
            start.elapsed()
        );

        Ok(publications)
    }

    /// Import publications into database with batched processing
    pub async fn import_publications(&self, publications: &[Publication]) -> Result<usize> {
        let start = Instant::now();
        info!(
            "Importing {} publications into database using batch processing",
            publications.len()
        );

        let mut count = 0;

        // Process in batches to optimize database load
        for chunk in publications.chunks(BATCH_SIZE) {
            let chunk_start = Instant::now();
            debug!("Processing batch of {} publications", chunk.len());

            // Begin a transaction for the entire batch
            let mut tx = self.pool.begin().await?;

            // Use a bulk insert approach for better performance
            self.batch_insert_publications(&mut tx, chunk).await?;

            // Commit the transaction
            tx.commit().await?;

            count += chunk.len();
            debug!(
                "Batch processed {} publications in {:?}",
                chunk.len(),
                chunk_start.elapsed()
            );
        }

        info!("Imported {} publications in {:?}", count, start.elapsed());
        Ok(count)
    }

    /// Helper method to perform a batch insert of publications
    async fn batch_insert_publications<'a>(
        &self,
        tx: &mut Transaction<'a, Postgres>,
        publications: &[Publication],
    ) -> Result<()> {
        // Clear existing data from the persistent table
        sqlx::query!(r#"TRUNCATE TABLE pub_import"#)
            .execute(&mut **tx)
            .await?;

        // Prepare data for insertion
        let mut query_builder = QueryBuilder::new(
        "INSERT INTO pub_import (id, title, title_normalized, person_id, author, author_normalized, \
        publication_date, year, doc_type_id, doc_type, volume, number, pages) "
    );

        query_builder.push_values(publications, |mut b, pub_record| {
            let title_normalized = normalize_string(&pub_record.title);
            let author_normalized = pub_record
                .author
                .as_ref()
                .map_or(String::new(), |a| normalize_string(a));

            let year = pub_record.year.as_ref().and_then(|y| y.parse::<i32>().ok());

            let doc_type_id = pub_record
                .doc_type_id
                .as_ref()
                .and_then(|id| id.parse::<i32>().ok());

            let publication_date = pub_record
                .publication_date
                .as_ref()
                .and_then(|d| d.parse::<sqlx::types::chrono::NaiveDate>().ok());

            b.push_bind(&pub_record.id)
                .push_bind(&pub_record.title)
                .push_bind(&title_normalized)
                .push_bind(&pub_record.person_id)
                .push_bind(&pub_record.author)
                .push_bind(&author_normalized)
                .push_bind(publication_date)
                .push_bind(year)
                .push_bind(doc_type_id)
                .push_bind(&pub_record.doc_type)
                .push_bind(&pub_record.volume)
                .push_bind(&pub_record.number)
                .push_bind(&pub_record.pages);
        });

        // Execute the batch insert
        query_builder.build().execute(&mut **tx).await?;

        // Now perform an upsert from the staging table to the main table
        sqlx::query!(
            r#"
        INSERT INTO publications (
            id, title, title_normalized, person_id, author, author_normalized, 
            publication_date, year, doc_type_id, doc_type, volume, number, pages
        )
        SELECT 
            id, title, title_normalized, person_id, author, author_normalized, 
            publication_date, year, doc_type_id, doc_type, volume, number, pages
        FROM pub_import
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
        "#
        )
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    /// Import PubMed records with parallel processing
    pub async fn import_pubmed_records(&self, records: &[PubMedRecord]) -> Result<usize> {
        let start = Instant::now();
        info!(
            "Importing {} PubMed records using parallel batches",
            records.len()
        );

        let mut total_count = 0;

        // Process in chunks to avoid overwhelming database
        for (chunk_idx, chunk) in records.chunks(BATCH_SIZE).enumerate() {
            debug!(
                "Processing PubMed batch {}: {} records",
                chunk_idx + 1,
                chunk.len()
            );

            // Process each batch with parallel sub-batches for better throughput
            let sub_batch_size = (chunk.len() + CONCURRENT_TASKS - 1) / CONCURRENT_TASKS;

            // Create multiple concurrent tasks for this batch
            let mut handles = Vec::new();
            for sub_chunk in chunk.chunks(sub_batch_size) {
                let sub_records = sub_chunk.to_vec();
                let pool = self.pool.clone();

                let handle = tokio::spawn(async move {
                    let mut tx = pool.begin().await?;
                    let mut count = 0;

                    // Insert PubMed records
                    for record in &sub_records {
                        if Self::insert_pubmed_record(&mut tx, record).await.is_ok() {
                            count += 1;
                        }
                    }

                    tx.commit().await?;
                    Ok::<usize, anyhow::Error>(count)
                });

                handles.push(handle);
            }

            // Collect results from all concurrent tasks
            for handle in handles {
                match handle.await {
                    Ok(Ok(count)) => total_count += count,
                    Ok(Err(e)) => error!("Error in PubMed batch processing: {}", e),
                    Err(e) => error!("Task failed: {}", e),
                }
            }

            debug!(
                "Completed PubMed batch {}, total so far: {}",
                chunk_idx + 1,
                total_count
            );
        }

        info!(
            "Imported {} PubMed records in {:?}",
            total_count,
            start.elapsed()
        );
        Ok(total_count)
    }

    /// Helper method to insert a single PubMed record within a transaction
    async fn insert_pubmed_record<'a>(
        tx: &mut Transaction<'a, Postgres>,
        record: &PubMedRecord,
    ) -> Result<()> {
        // Insert the main PubMed record
        let title_normalized = normalize_string(&record.title);
        let year = record.year.parse::<i32>().ok();

        sqlx::query!(
            r#"
            INSERT INTO pubmed_records 
            (pmid, title, title_normalized, journal, volume, issue, year, abstract, source_file)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
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
            "#,
            record.pmid,
            record.title,
            title_normalized,
            record.journal,
            record.volume,
            record.issue,
            year,
            record.abstract_text,
            record.file_source
        )
        .execute(&mut **tx)
        .await?;

        // Process authors if present
        if !record.authors.is_empty() {
            let authors: Vec<&str> = record.authors.split("; ").collect();

            for (i, author_name) in authors.iter().enumerate() {
                // Insert author if they don't exist
                let author_row = sqlx::query!(
                    r#"
                    INSERT INTO authors (name, name_normalized)
                    VALUES ($1, $2)
                    ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                    RETURNING id
                    "#,
                    author_name,
                    normalize_string(author_name)
                )
                .fetch_one(&mut **tx)
                .await?;

                // Link author to PubMed record
                sqlx::query!(
                    r#"
                    INSERT INTO pubmed_authors (pubmed_id, author_id, position)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (pubmed_id, author_id) DO UPDATE SET position = EXCLUDED.position
                    "#,
                    record.pmid,
                    author_row.id,
                    i as i32 + 1
                )
                .execute(&mut **tx)
                .await?;
            }
        }

        // Process MeSH headings
        for term in &record.mesh_headings {
            // Insert MeSH term if it doesn't exist
            let term_row = sqlx::query!(
                r#"
                INSERT INTO mesh_terms (term)
                VALUES ($1)
                ON CONFLICT (term) DO UPDATE SET term = EXCLUDED.term
                RETURNING id
                "#,
                term
            )
            .fetch_one(&mut **tx)
            .await?;

            // Link term to PubMed record
            sqlx::query!(
                r#"
                INSERT INTO pubmed_mesh_terms (pubmed_id, mesh_id)
                VALUES ($1, $2)
                ON CONFLICT (pubmed_id, mesh_id) DO NOTHING
                "#,
                record.pmid,
                term_row.id
            )
            .execute(&mut **tx)
            .await?;
        }

        Ok(())
    }

    /// Load PubMed records with optimized query and streaming
    pub async fn load_pubmed_records(&self) -> Result<Vec<PubMedRecord>> {
        let start = Instant::now();
        info!("Loading PubMed records with optimized query");

        let records = sqlx::query!(
            r#"
            SELECT 
                pr.pmid, 
                pr.title, 
                pr.journal, 
                pr.volume, 
                pr.issue, 
                pr.year::TEXT, 
                pr.abstract as "abstract_text",
                pr.source_file as "file_source",
                (
                    SELECT STRING_AGG(a.name, '; ' ORDER BY pa.position)
                    FROM pubmed_authors pa 
                    JOIN authors a ON pa.author_id = a.id
                    WHERE pa.pubmed_id = pr.pmid
                ) as "authors",
                ARRAY(
                    SELECT mt.term
                    FROM pubmed_mesh_terms pmt
                    JOIN mesh_terms mt ON pmt.mesh_id = mt.id
                    WHERE pmt.pubmed_id = pr.pmid
                    ORDER BY mt.term
                ) as "mesh_headings"
            FROM pubmed_records pr
            "#
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to load PubMed records from database")?;

        let pubmed_records: Vec<PubMedRecord> = records
            .into_iter()
            .map(|row| {
                let title = row.title;
                let authors = row.authors.unwrap_or_else(|| String::default());

                PubMedRecord {
                    pmid: row.pmid,
                    title: title.clone(),
                    title_norm: normalize_string(&title),
                    authors: authors.clone(),
                    authors_norm: normalize_string(&authors),
                    abstract_text: row.abstract_text,
                    journal: row.journal,
                    volume: row.volume,
                    issue: row.issue,
                    year: row.year.unwrap_or_default(),
                    mesh_headings: row.mesh_headings.unwrap_or_default(),
                    file_source: row.file_source.unwrap_or_default(),
                }
            })
            .collect();

        info!(
            "Loaded {} PubMed records in {:?}",
            pubmed_records.len(),
            start.elapsed()
        );

        Ok(pubmed_records)
    }

    /// Create matches between publications and PubMed records with batch processing
    pub async fn create_matches(&self, matches: &[MatchResult]) -> Result<usize> {
        let start = Instant::now();
        info!("Creating {} matches with batch processing", matches.len());

        let mut count = 0;

        // Process in batches
        for chunk in matches.chunks(BATCH_SIZE) {
            let mut tx = self.pool.begin().await?;

            // Use a query builder for bulk inserts
            let mut query_builder = QueryBuilder::new(
                "INSERT INTO publication_pubmed_matches (publication_id, pmid, match_quality, match_type) "
            );

            query_builder.push_values(chunk, |mut b, match_result| {
                let match_type = match match_result.match_type.as_str() {
                    "exact_title" => "exact_title",
                    "title_author" => "title_author",
                    "title_year" => "title_year",
                    "fuzzy_title" => "fuzzy_title",
                    _ => "manual",
                };

                b.push_bind(&match_result.publication_id)
                    .push_bind(&match_result.pmid)
                    .push_bind(match_result.match_quality as f64)
                    .push_bind(match_type);
            });

            query_builder.push(
                " ON CONFLICT (publication_id, pmid) DO UPDATE SET
                    match_quality = EXCLUDED.match_quality,
                    match_type = EXCLUDED.match_type,
                    created_at = NOW()",
            );

            // Execute the batch insert
            let result = query_builder.build().execute(tx.as_mut()).await;

            match result {
                Ok(result) => {
                    count += result.rows_affected() as usize;
                    tx.commit().await?;
                }
                Err(e) => {
                    error!("Failed to insert batch of matches: {}", e);
                    tx.rollback().await?;
                }
            }
        }

        info!("Created {} matches in {:?}", count, start.elapsed());
        Ok(count)
    }

    /// Get enriched publications with optimized query
    pub async fn get_enriched_publications(&self) -> Result<Vec<Publication>> {
        let start = Instant::now();
        info!("Retrieving enriched publications with optimized query");

        // Use a materialized CTE for better performance
        let enriched = sqlx::query!(
            r#"
            WITH pubmed_data AS MATERIALIZED (
                SELECT 
                    pr.pmid,
                    pr.journal,
                    pr.abstract,
                    (
                        SELECT STRING_AGG(a.name, '; ' ORDER BY pa.position)
                        FROM pubmed_authors pa 
                        JOIN authors a ON pa.author_id = a.id
                        WHERE pa.pubmed_id = pr.pmid
                    ) as authors,
                    (
                        SELECT STRING_AGG(mt.term, '; ' ORDER BY mt.term)
                        FROM pubmed_mesh_terms pmt
                        JOIN mesh_terms mt ON pmt.mesh_id = mt.id
                        WHERE pmt.pubmed_id = pr.pmid
                    ) as mesh_terms
                FROM pubmed_records pr
            )
            SELECT 
                p.id,
                p.title,
                p.person_id,
                p.author,
                p.publication_date::TEXT,
                p.year::TEXT,
                p.doc_type_id::TEXT,
                p.doc_type,
                p.volume,
                p.number,
                p.pages,
                m.pmid,
                pd.authors as "pubmed_authors",
                pd.abstract as "pubmed_abstract",
                pd.journal as "pubmed_journal",
                pd.mesh_terms as "pubmed_mesh",
                m.match_quality,
                m.match_type::TEXT,
                m.created_at::TEXT as "enrichment_date"
            FROM 
                publications p
            JOIN 
                publication_pubmed_matches m ON p.id = m.publication_id
            JOIN 
                pubmed_data pd ON m.pmid = pd.pmid
            "#
        )
        .fetch(&self.pool);

        // Process results directly from the stream
        let publications = enriched
            .map(|row_result| {
                let row = row_result?;

                Ok::<Publication, anyhow::Error>(Publication {
                    id: row.id,
                    title: row.title,
                    person_id: row.person_id,
                    author: row.author,
                    publication_date: row.publication_date,
                    year: row.year,
                    doc_type_id: row.doc_type_id,
                    doc_type: row.doc_type,
                    volume: row.volume,
                    number: row.number,
                    pages: row.pages,
                    pmid: row.pmid,
                    pubmed_authors: row.pubmed_authors,
                    pubmed_abstract: row.pubmed_abstract,
                    pubmed_journal: row.pubmed_journal,
                    pubmed_mesh: row.pubmed_mesh,
                    match_quality: row.match_quality.and_then(|x| x.to_f64()),
                    match_type: row.match_type,
                    enrichment_date: row.enrichment_date,
                })
            })
            .try_collect::<Vec<_>>()
            .await?;

        info!(
            "Retrieved {} enriched publications in {:?}",
            publications.len(),
            start.elapsed()
        );

        Ok(publications)
    }
}
