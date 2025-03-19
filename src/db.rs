use anyhow::{Context, Result};
use log::{debug, error, info};
use num_traits::ToPrimitive;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::time::Instant;

use crate::models::{MatchResult, PubMedRecord, Publication};
use crate::utils::normalize_string;

pub struct DatabaseService {
    pool: PgPool,
}

impl DatabaseService {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .context("Failed to connect to PostgreSQL database")?;

        info!("Successfully connected to PostgreSQL database");

        Ok(Self { pool })
    }

    /// Load publications from database
    pub async fn load_publications(&self) -> Result<Vec<Publication>> {
        let start = Instant::now();
        info!("Loading publications from database");

        let publications = sqlx::query_as!(
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
        .fetch_all(&self.pool)
        .await
        .context("Failed to load publications from database")?;

        info!(
            "Loaded {} publications in {:?}",
            publications.len(),
            start.elapsed()
        );

        Ok(publications)
    }

    /// Import publications into database
    pub async fn import_publications(&self, publications: &[Publication]) -> Result<usize> {
        let start = Instant::now();
        info!(
            "Importing {} publications into database",
            publications.len()
        );

        let mut count = 0;

        for publication in publications {
            let title_normalized = normalize_string(&publication.title);
            let author_normalized = publication
                .author
                .as_ref()
                .map_or(String::new(), |a| normalize_string(a));

            // Convert year to integer if possible
            let year = publication
                .year
                .as_ref()
                .and_then(|y| y.parse::<i32>().ok());

            // Convert doc_type_id to integer if possible
            let doc_type_id = publication
                .doc_type_id
                .as_ref()
                .and_then(|id| id.parse::<i32>().ok());

            let result = sqlx::query!(
                r#"
                INSERT INTO publications 
                (id, title, title_normalized, person_id, author, author_normalized, 
                 publication_date, year, doc_type_id, doc_type, volume, number, pages)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
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
                "#,
                publication.id,
                publication.title,
                title_normalized,
                publication.person_id,
                publication.author,
                author_normalized,
                publication
                    .publication_date
                    .as_ref()
                    .and_then(|d| d.parse::<sqlx::types::chrono::NaiveDate>().ok()),
                year,
                doc_type_id,
                publication.doc_type,
                publication.volume,
                publication.number,
                publication.pages
            )
            .execute(&self.pool)
            .await;

            match result {
                Ok(_) => count += 1,
                Err(e) => error!("Failed to insert publication {}: {}", publication.id, e),
            }

            if count % 1000 == 0 {
                debug!("Imported {} publications", count);
            }
        }

        info!("Imported {} publications in {:?}", count, start.elapsed());

        Ok(count)
    }

    /// Import PubMed records into database
    pub async fn import_pubmed_records(&self, records: &[PubMedRecord]) -> Result<usize> {
        let start = Instant::now();
        info!("Importing {} PubMed records into database", records.len());

        let mut count = 0;

        for record in records {
            // Begin a transaction
            let mut tx = self.pool.begin().await?;

            // Insert the main PubMed record
            let title_normalized = normalize_string(&record.title);

            // Convert year to integer if possible
            let year = record.year.parse::<i32>().ok();

            let result = sqlx::query!(
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
            .execute(&mut *tx)
            .await;

            if let Err(e) = result {
                error!("Failed to insert PubMed record {}: {}", record.pmid, e);
                continue;
            }

            // Process authors
            if !record.authors.is_empty() {
                let authors: Vec<&str> = record.authors.split("; ").collect();

                for (i, author_name) in authors.iter().enumerate() {
                    // Insert author if they don't exist
                    let author_result = sqlx::query!(
                        r#"
                        INSERT INTO authors (name, name_normalized)
                        VALUES ($1, $2)
                        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                        RETURNING id
                        "#,
                        author_name,
                        normalize_string(author_name)
                    )
                    .fetch_one(&mut *tx)
                    .await;

                    if let Ok(author_row) = author_result {
                        // Link author to PubMed record
                        let _ = sqlx::query!(
                            r#"
                            INSERT INTO pubmed_authors (pubmed_id, author_id, position)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (pubmed_id, author_id) DO UPDATE SET position = EXCLUDED.position
                            "#,
                            record.pmid,
                            author_row.id,
                            i as i32 + 1
                        )
                        .execute(&mut *tx)
                        .await;
                    }
                }
            }

            // Process MeSH headings
            for term in &record.mesh_headings {
                // Insert MeSH term if it doesn't exist
                let term_result = sqlx::query!(
                    r#"
                    INSERT INTO mesh_terms (term)
                    VALUES ($1)
                    ON CONFLICT (term) DO UPDATE SET term = EXCLUDED.term
                    RETURNING id
                    "#,
                    term
                )
                .fetch_one(&mut *tx)
                .await;

                if let Ok(term_row) = term_result {
                    // Link term to PubMed record
                    let _ = sqlx::query!(
                        r#"
                        INSERT INTO pubmed_mesh_terms (pubmed_id, mesh_id)
                        VALUES ($1, $2)
                        ON CONFLICT (pubmed_id, mesh_id) DO NOTHING
                        "#,
                        record.pmid,
                        term_row.id
                    )
                    .execute(&mut *tx)
                    .await;
                }
            }

            // Commit the transaction
            if let Err(e) = tx.commit().await {
                error!(
                    "Failed to commit transaction for PubMed record {}: {}",
                    record.pmid, e
                );
                continue;
            }

            count += 1;

            if count % 1000 == 0 {
                debug!("Imported {} PubMed records", count);
            }
        }

        info!("Imported {} PubMed records in {:?}", count, start.elapsed());

        Ok(count)
    }

    /// Load PubMed records from database
    pub async fn load_pubmed_records(&self) -> Result<Vec<PubMedRecord>> {
        let start = Instant::now();
        info!("Loading PubMed records from database");

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
                STRING_AGG(DISTINCT a.name, '; ') as "authors",
                ARRAY_AGG(DISTINCT mt.term) FILTER (WHERE mt.term IS NOT NULL) as "mesh_headings"
            FROM pubmed_records pr
            LEFT JOIN pubmed_authors pa ON pr.pmid = pa.pubmed_id
            LEFT JOIN authors a ON pa.author_id = a.id
            LEFT JOIN pubmed_mesh_terms pmt ON pr.pmid = pmt.pubmed_id
            LEFT JOIN mesh_terms mt ON pmt.mesh_id = mt.id
            GROUP BY pr.pmid
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

    /// Create matches between publications and PubMed records
    pub async fn create_matches(&self, matches: &[MatchResult]) -> Result<usize> {
        let start = Instant::now();
        info!("Creating {} matches in database", matches.len());

        let mut count = 0;

        for match_result in matches {
            let match_type = match match_result.match_type.as_str() {
                "exact_title" => "exact_title",
                "title_author" => "title_author",
                "title_year" => "title_year",
                "fuzzy_title" => "fuzzy_title",
                _ => "manual",
            };

            let result = sqlx::query!(
                r#"
                INSERT INTO publication_pubmed_matches 
                (publication_id, pmid, match_quality, match_type)
                VALUES ($1, $2, $3, $4::match_type)
                ON CONFLICT (publication_id, pmid) DO UPDATE SET
                    match_quality = EXCLUDED.match_quality,
                    match_type = EXCLUDED.match_type
                "#,
                match_result.publication_id,
                match_result.pmid,
                match_result.match_quality as f64,
                match_type as &str
            )
            .execute(&self.pool)
            .await;

            match result {
                Ok(_) => count += 1,
                Err(e) => error!(
                    "Failed to insert match {}-{}: {}",
                    match_result.publication_id, match_result.pmid, e
                ),
            }
        }

        info!("Created {} matches in {:?}", count, start.elapsed());

        Ok(count)
    }

    /// Get enriched publications from the database (publications with PubMed data)
    pub async fn get_enriched_publications(&self) -> Result<Vec<Publication>> {
        let start = Instant::now();
        info!("Retrieving enriched publications from database");

        let enriched = sqlx::query!(
            r#"
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
                pr.pmid,
                STRING_AGG(DISTINCT a.name, '; ') as "pubmed_authors",
                pr.abstract as "pubmed_abstract",
                pr.journal as "pubmed_journal",
                STRING_AGG(DISTINCT mt.term, '; ') as "pubmed_mesh",
                m.match_quality,
                m.match_type::TEXT,
                m.created_at::TEXT as "enrichment_date"
            FROM 
                publications p
            JOIN 
                publication_pubmed_matches m ON p.id = m.publication_id
            JOIN 
                pubmed_records pr ON m.pmid = pr.pmid
            LEFT JOIN 
                pubmed_authors pa ON pr.pmid = pa.pubmed_id
            LEFT JOIN 
                authors a ON pa.author_id = a.id
            LEFT JOIN 
                pubmed_mesh_terms pmt ON pr.pmid = pmt.pubmed_id
            LEFT JOIN 
                mesh_terms mt ON pmt.mesh_id = mt.id
            GROUP BY 
                p.id, pr.pmid, pr.abstract, pr.journal, m.match_quality, m.match_type, m.created_at
            "#
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to retrieve enriched publications")?;

        let publications: Vec<Publication> = enriched
            .into_iter()
            .map(|row| Publication {
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
                pmid: Some(row.pmid),
                pubmed_authors: row.pubmed_authors,
                pubmed_abstract: row.pubmed_abstract,
                pubmed_journal: row.pubmed_journal,
                pubmed_mesh: row.pubmed_mesh,
                match_quality: row.match_quality.and_then(|x| x.to_f64()),
                match_type: row.match_type,
                enrichment_date: row.enrichment_date,
            })
            .collect();

        info!(
            "Retrieved {} enriched publications in {:?}",
            publications.len(),
            start.elapsed()
        );

        Ok(publications)
    }
}
