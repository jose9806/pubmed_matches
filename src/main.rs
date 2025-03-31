use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use chrono;
use clap::{Arg, ArgAction, Command};
use csv::{ReaderBuilder, WriterBuilder};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json;
use strsim::normalized_levenshtein;
use tokio_postgres::{Client, NoTls};

// Configuraciones optimizadas
const QUERY_BATCH_SIZE: usize = 100_000; // Tamaño del lote para consultas a la BD
const PARALLEL_CHUNK_SIZE: usize = 500; // Tamaño de chunk para procesamiento paralelo
const SIMILARITY_THRESHOLD: f64 = 0.85; // Umbral para coincidencias difusas
const HIGH_SIMILARITY_THRESHOLD: f64 = 0.90; // Umbral alto para coincidencias de alta calidad

// Calidades de match
const DOI_MATCH_QUALITY: f64 = 100.0;
const EXACT_TITLE_MATCH_QUALITY: f64 = 100.0;
const TITLE_YEAR_MATCH_QUALITY: f64 = 90.0;
const FUZZY_TITLE_MATCH_QUALITY: f64 = 80.0;

// Structs para la gestión de datos
#[derive(Debug, Deserialize, Clone)]
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
#[derive(Debug, Clone)]
struct EnrichedData {
    pmid: String,
    pubmed_title: String,
    journal: String,
    abstract_text: String,
    doi: Option<String>,
    authors: String,
    mesh_terms: String,
    match_quality: f64,
    match_type: String,
}
#[derive(Debug, Serialize)]
struct EnrichedPublication {
    // Campos originales
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

    // Campos enriquecidos
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Match {
    publication_id: String,
    pmid: String,
    match_quality: f64,
    match_type: String,
}

// Define un struct para gestionar los checkpoints
struct CheckpointManager {
    checkpoint_dir: PathBuf,
    current_job_id: String,
}

impl CheckpointManager {
    fn new(output_path: &str) -> Result<Self, Box<dyn Error>> {
        // Crear un ID de trabajo basado en la ruta de salida
        let output_file = Path::new(output_path);
        let file_stem = output_file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("job");

        let current_job_id = format!(
            "{}-{}",
            file_stem,
            chrono::Local::now().format("%Y%m%d-%H%M%S")
        );

        // Crear directorio de checkpoint
        let checkpoint_dir = PathBuf::from("checkpoints").join(&current_job_id);
        fs::create_dir_all(&checkpoint_dir)?;

        println!("Created checkpoint directory: {}", checkpoint_dir.display());
        println!("Job ID: {}", current_job_id);

        Ok(CheckpointManager {
            checkpoint_dir,
            current_job_id,
        })
    }

    fn new_with_dir(dir: &str, output_path: &str) -> Result<Self, Box<dyn Error>> {
        // Check if the specified directory exists and contains checkpoint files
        let checkpoint_dir = PathBuf::from(dir);

        if checkpoint_dir.exists()
            && (checkpoint_dir.join("load.done").exists()
                || checkpoint_dir.join("processed_publications.txt").exists())
        {
            // This is an existing checkpoint directory, use it directly
            let current_job_id = checkpoint_dir
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string();

            println!(
                "Resuming from existing checkpoint directory: {}",
                checkpoint_dir.display()
            );
            println!("Job ID: {}", current_job_id);

            return Ok(CheckpointManager {
                checkpoint_dir,
                current_job_id,
            });
        }

        // Otherwise, create a new checkpoint directory with timestamp
        let output_file = Path::new(output_path);
        let file_stem = output_file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("job");

        let current_job_id = format!(
            "{}-{}",
            file_stem,
            chrono::Local::now().format("%Y%m%d-%H%M%S")
        );

        // Create checkpoint directory
        let checkpoint_dir = PathBuf::from(dir).join(&current_job_id);
        fs::create_dir_all(&checkpoint_dir)?;

        println!("Created checkpoint directory: {}", checkpoint_dir.display());
        println!("Job ID: {}", current_job_id);

        Ok(CheckpointManager {
            checkpoint_dir,
            current_job_id,
        })
    }

    // Verificar si una etapa ha sido completada
    fn is_stage_completed(&self, stage_name: &str) -> bool {
        let checkpoint_file = self.checkpoint_dir.join(format!("{}.done", stage_name));
        checkpoint_file.exists()
    }

    // Marcar una etapa como completada
    fn mark_stage_completed(&self, stage_name: &str) -> Result<(), Box<dyn Error>> {
        let checkpoint_file = self.checkpoint_dir.join(format!("{}.done", stage_name));
        let mut file = File::create(checkpoint_file)?;
        let timestamp = chrono::Local::now().to_rfc3339();
        writeln!(file, "{}", timestamp)?;
        println!("Checkpoint created for stage: {}", stage_name);
        Ok(())
    }

    // Guardar IDs de publicaciones procesadas
    fn save_processed_publications(
        &self,
        publication_ids: &[String],
    ) -> Result<(), Box<dyn Error>> {
        let processed_file = self.checkpoint_dir.join("processed_publications.txt");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(processed_file)?;

        for id in publication_ids {
            writeln!(file, "{}", id)?;
        }

        Ok(())
    }

    // Cargar IDs de publicaciones previamente procesadas
    fn load_processed_publications(&self) -> Result<HashSet<String>, Box<dyn Error>> {
        let processed_file = self.checkpoint_dir.join("processed_publications.txt");

        if !processed_file.exists() {
            return Ok(HashSet::new());
        }

        let file = File::open(processed_file)?;
        let reader = BufReader::new(file);
        let mut processed_ids = HashSet::new();

        for line in reader.lines() {
            if let Ok(id) = line {
                processed_ids.insert(id);
            }
        }

        println!(
            "Loaded {} previously processed publications",
            processed_ids.len()
        );
        Ok(processed_ids)
    }

    // Guardar coincidencias en un archivo de checkpoint
    fn save_matches(&self, matches: &[Match]) -> Result<(), Box<dyn Error>> {
        let matches_file = self.checkpoint_dir.join("matches.csv");

        // Verificar si el archivo existe para determinar si necesitamos escribir encabezados
        let write_header = !matches_file.exists();

        let mut writer = WriterBuilder::new()
            .delimiter(b',')
            .has_headers(write_header)
            .from_writer(if write_header {
                File::create(&matches_file)?
            } else {
                OpenOptions::new().append(true).open(&matches_file)?
            });

        for m in matches {
            writer.serialize(m)?;
        }

        writer.flush()?;
        println!("Saved {} matches to checkpoint", matches.len());
        Ok(())
    }

    // Cargar coincidencias previamente guardadas
    fn load_matches(&self) -> Result<Vec<Match>, Box<dyn Error>> {
        let matches_file = self.checkpoint_dir.join("matches.csv");

        if !matches_file.exists() {
            return Ok(Vec::new());
        }

        let mut reader = ReaderBuilder::new()
            .delimiter(b',')
            .has_headers(true)
            .from_path(matches_file)?;

        let matches: Vec<Match> = reader.deserialize().collect::<Result<_, _>>()?;
        println!("Loaded {} matches from checkpoint", matches.len());

        Ok(matches)
    }

    // Crear un archivo de resumen con detalles de ejecución
    fn write_summary(
        &self,
        start_time: Instant,
        publication_count: usize,
        match_count: usize,
        matched_publication_count: usize,
    ) -> Result<(), Box<dyn Error>> {
        let summary_file = self.checkpoint_dir.join("summary.txt");
        let mut file = File::create(summary_file)?;

        writeln!(file, "Job ID: {}", self.current_job_id)?;
        writeln!(
            file,
            "Execution time: {:.2} seconds",
            start_time.elapsed().as_secs_f64()
        )?;
        writeln!(file, "Total publications: {}", publication_count)?;
        writeln!(file, "Publications matched: {}", matched_publication_count)?;
        writeln!(
            file,
            "Match rate: {:.2}%",
            (matched_publication_count as f64 / publication_count as f64) * 100.0
        )?;
        writeln!(file, "Total matches: {}", match_count)?;

        Ok(())
    }
    fn save_year_offset(&self, year: Option<i32>, offset: i64) -> Result<(), Box<dyn Error>> {
        let year_str = year.map_or("NULL".to_string(), |y| y.to_string());
        let offset_file = self
            .checkpoint_dir
            .join(format!("year_{}_offset.txt", year_str));
        let mut file = File::create(offset_file)?;
        writeln!(file, "{}", offset)?;
        println!("Saved offset {} for year={}", offset, year_str);
        Ok(())
    }

    // Add this new method to load year-specific offsets
    fn load_year_offset(&self, year: Option<i32>) -> Result<Option<i64>, Box<dyn Error>> {
        let year_str = year.map_or("NULL".to_string(), |y| y.to_string());
        let offset_file = self
            .checkpoint_dir
            .join(format!("year_{}_offset.txt", year_str));

        if !offset_file.exists() {
            return Ok(None);
        }

        let file = File::open(offset_file)?;
        let reader = BufReader::new(file);

        if let Some(Ok(line)) = reader.lines().next() {
            if let Ok(offset) = line.parse::<i64>() {
                println!("Loaded offset {} for year={}", offset, year_str);
                return Ok(Some(offset));
            }
        }

        Ok(None)
    }

    // Add this new method to save completed years
    fn save_completed_year(&self, year: Option<i32>) -> Result<(), Box<dyn Error>> {
        let year_str = year.map_or("NULL".to_string(), |y| y.to_string());
        let year_file = self.checkpoint_dir.join("completed_years.txt");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(year_file)?;

        writeln!(file, "{}", year_str)?;
        println!("Marked year={} as completed", year_str);
        Ok(())
    }

    // Add this new method to load completed years
    fn load_completed_years(&self) -> Result<HashSet<String>, Box<dyn Error>> {
        let year_file = self.checkpoint_dir.join("completed_years.txt");

        if !year_file.exists() {
            return Ok(HashSet::new());
        }

        let file = File::open(year_file)?;
        let reader = BufReader::new(file);
        let mut completed_years = HashSet::new();

        for line in reader.lines() {
            if let Ok(year_str) = line {
                completed_years.insert(year_str);
            }
        }

        println!("Loaded {} completed years", completed_years.len());
        Ok(completed_years)
    }

    // Add this method to save batch-specific matches
    fn save_batch_matches(
        &self,
        year: Option<i32>,
        batch: usize,
        matches: &[Match],
    ) -> Result<(), Box<dyn Error>> {
        if matches.is_empty() {
            return Ok(());
        }

        let year_str = year.map_or("NULL".to_string(), |y| y.to_string());
        let batch_file = self
            .checkpoint_dir
            .join(format!("matches_year_{}_batch_{}.csv", year_str, batch));

        // Verify if the file exists to determine if we need to write headers
        let write_header = !batch_file.exists();

        let mut writer = WriterBuilder::new()
            .delimiter(b',')
            .has_headers(write_header)
            .from_writer(if write_header {
                File::create(&batch_file)?
            } else {
                OpenOptions::new().append(true).open(&batch_file)?
            });

        for m in matches {
            writer.serialize(m)?;
        }

        writer.flush()?;
        println!(
            "Saved {} matches for year={} batch={}",
            matches.len(),
            year_str,
            batch
        );
        Ok(())
    }

    // Add this method to load all batch matches for a specific year
    fn load_batch_matches_for_year(&self, year: Option<i32>) -> Result<Vec<Match>, Box<dyn Error>> {
        let year_str = year.map_or("NULL".to_string(), |y| y.to_string());
        let pattern = format!("matches_year_{}_batch_*.csv", year_str);

        let mut all_matches = Vec::new();

        // Find all batch files for this year
        for entry in fs::read_dir(&self.checkpoint_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                if let Some(filename) = path.file_name() {
                    if let Some(filename_str) = filename.to_str() {
                        if filename_str.starts_with(&format!("matches_year_{}_batch_", year_str))
                            && filename_str.ends_with(".csv")
                        {
                            // Found a batch file, read matches
                            let mut reader = ReaderBuilder::new()
                                .delimiter(b',')
                                .has_headers(true)
                                .from_path(&path)?;

                            let batch_matches: Vec<Match> =
                                reader.deserialize().collect::<Result<_, _>>()?;
                            println!(
                                "Loaded {} matches from {}",
                                batch_matches.len(),
                                filename_str
                            );
                            all_matches.extend(batch_matches);
                        }
                    }
                }
            }
        }

        println!(
            "Loaded total {} matches for year={}",
            all_matches.len(),
            year_str
        );
        Ok(all_matches)
    }

    // Add this method to save the processing state for recovery
    fn save_processing_state(
        &self,
        year: Option<i32>,
        offset: i64,
        batch: usize,
        pub_processed: usize,
        error_msg: &str,
    ) -> Result<(), Box<dyn Error>> {
        let year_str = year.map_or("NULL".to_string(), |y| y.to_string());
        let state_file = self
            .checkpoint_dir
            .join(format!("recovery_state_year_{}.json", year_str));

        let state = serde_json::json!({
            "year": year_str,
            "offset": offset,
            "batch": batch,
            "publications_processed": pub_processed,
            "error": error_msg,
            "timestamp": chrono::Local::now().to_rfc3339()
        });

        let mut file = File::create(state_file)?;
        file.write_all(serde_json::to_string_pretty(&state)?.as_bytes())?;

        println!(
            "Saved recovery state for year={} at offset={}",
            year_str, offset
        );
        Ok(())
    }

    // Add this method to load the processing state for recovery
    fn load_processing_state(
        &self,
        year: Option<i32>,
    ) -> Result<Option<serde_json::Value>, Box<dyn Error>> {
        let year_str = year.map_or("NULL".to_string(), |y| y.to_string());
        let state_file = self
            .checkpoint_dir
            .join(format!("recovery_state_year_{}.json", year_str));

        if !state_file.exists() {
            return Ok(None);
        }

        let file = File::open(state_file)?;
        let reader = BufReader::new(file);
        let state: serde_json::Value = serde_json::from_reader(reader)?;

        println!("Loaded recovery state for year={}", year_str);
        Ok(Some(state))
    }
}

// Gestor de base de datos
struct DbHandler {
    client: Client,
}

impl DbHandler {
    async fn fetch_direct_publication_data(
        &self,
        publication_ids: &[String],
    ) -> Result<HashMap<String, EnrichedData>, Box<dyn Error>> {
        if publication_ids.is_empty() {
            return Ok(HashMap::new());
        }

        println!(
            "Consultando datos completos para {} publicaciones",
            publication_ids.len()
        );

        // Resultados finales
        let mut results = HashMap::new();

        // Barra de progreso
        let pb = ProgressBar::new(publication_ids.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.green/blue} {pos:>7}/{len:7} {percent}% {eta} {msg}")
        .unwrap());
        pb.set_message("Obteniendo datos completos");

        // Procesar en lotes
        for chunk in publication_ids.chunks(100) {
            // Crear lista de IDs para usar en IN
            let ids_list = chunk
                .iter()
                .map(|id| format!("'{}'", id.replace('\'', "''"))) // Escapar comillas
                .collect::<Vec<_>>()
                .join(",");

            // Consulta usando interpolación directa (segura en este caso)
            let query = format!(
                r#"
            SELECT 
                m.publication_id, 
                m.pmid,
                m.match_quality::float8 AS match_quality,
                m.match_type::text AS match_type_str,
                pr.title AS pubmed_title,
                COALESCE(pr.journal, '') AS journal,
                COALESCE(pr.abstract, '') AS abstract_text,
                pr.doi,
                COALESCE(
                    (SELECT string_agg(a.name, '; ') 
                     FROM pubmed_authors pa 
                     JOIN authors a ON pa.author_id = a.id 
                     WHERE pa.pubmed_id = m.pmid
                     GROUP BY pa.pubmed_id), 
                    ''
                ) AS authors,
                COALESCE(
                    (SELECT string_agg(mt.term, '; ') 
                     FROM pubmed_mesh_terms pmt 
                     JOIN mesh_terms mt ON pmt.mesh_id = mt.id 
                     WHERE pmt.pubmed_id = m.pmid
                     GROUP BY pmt.pubmed_id), 
                    ''
                ) AS mesh_terms
            FROM 
                publication_pubmed_matches m
            JOIN 
                pubmed_records pr ON m.pmid = pr.pmid
            WHERE 
                m.publication_id IN ({})
        "#,
                ids_list
            );

            // Ejecutar consulta sin parámetros
            let rows = self.client.query(&query, &[]).await?;

            for row in rows {
                let pub_id: String = row.get("publication_id");
                let match_quality: f64 = row.get("match_quality");

                results.insert(
                    pub_id,
                    EnrichedData {
                        pmid: row.get("pmid"),
                        pubmed_title: row.get("pubmed_title"),
                        journal: row.get("journal"),
                        abstract_text: row.get("abstract_text"),
                        doi: row.get::<_, Option<String>>("doi"),
                        authors: row.get("authors"),
                        mesh_terms: row.get("mesh_terms"),
                        match_quality,
                        match_type: row.get("match_type_str"),
                    },
                );
            }

            pb.inc(chunk.len() as u64);
        }

        pb.finish_with_message(format!(
            "Datos obtenidos para {} publicaciones",
            results.len()
        ));
        Ok(results)
    }
    async fn new_with_memory_limit(
        connection_str: &str,
        max_memory_mb: usize,
    ) -> Result<Self, Box<dyn Error>> {
        let (client, connection) = tokio_postgres::connect(connection_str, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Optimize the database with safer memory settings
        client
            .batch_execute(&format!(
                "
                SET work_mem = '{}MB';
                SET maintenance_work_mem = '{}MB';
                SET synchronous_commit = OFF;
                SET max_parallel_workers_per_gather = 4;
                SET effective_io_concurrency = 100;
                SET random_page_cost = 1.1;
                SET jit = ON;
                ",
                max_memory_mb / 4, // Use 1/4 of max memory for work_mem
                max_memory_mb / 2  // Use 1/2 of max memory for maintenance_work_mem
            ))
            .await?;

        Ok(DbHandler { client })
    }
    async fn new(connection_str: &str) -> Result<Self, Box<dyn Error>> {
        let (client, connection) = tokio_postgres::connect(connection_str, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Optimizar la base de datos para la tarea de matching
        client
            .batch_execute(
                "
            SET work_mem = '1GB';
            SET maintenance_work_mem = '1GB';
            SET synchronous_commit = OFF;
            SET max_parallel_workers_per_gather = 8;
            SET effective_io_concurrency = 200;
            SET random_page_cost = 1.1;
            SET jit = ON;
        ",
            )
            .await?;

        Ok(DbHandler { client })
    }

    // Método para obtener años distintos disponibles en la base de datos
    async fn get_distinct_years(&self) -> Result<Vec<Option<i32>>, Box<dyn Error>> {
        let query = "
            SELECT DISTINCT year 
            FROM pubmed_records 
            ORDER BY year
        ";

        let rows = self.client.query(query, &[]).await?;

        let years = rows
            .iter()
            .map(|row| row.get::<_, Option<i32>>(0))
            .collect();

        Ok(years)
    }

    // Método para enriquecer registros PubMed con autores y términos de malla
    async fn enrich_pubmed_records(
        &self,
        records: &mut [PubMedRecord],
    ) -> Result<(), Box<dyn Error>> {
        if records.is_empty() {
            return Ok(());
        }

        // Recopilar PMIDs para este lote
        let pmids: Vec<String> = records.iter().map(|r| r.pmid.clone()).collect();

        // Obtener autores y términos de malla en paralelo
        let (authors_map, mesh_map) = tokio::join!(
            self.fetch_authors_batch(&pmids),
            self.fetch_mesh_terms_batch(&pmids)
        );

        let authors_map = authors_map?;
        let mesh_map = mesh_map?;

        // Enriquecer registros con autores y términos de malla
        for record in records.iter_mut() {
            if let Some(authors) = authors_map.get(&record.pmid) {
                record.authors = authors.clone();
            }

            if let Some(mesh_terms) = mesh_map.get(&record.pmid) {
                record.mesh_terms = mesh_terms.clone();
            }
        }

        Ok(())
    }

    async fn fetch_authors_batch(
        &self,
        pmids: &[String],
    ) -> Result<HashMap<String, Vec<String>>, Box<dyn Error>> {
        if pmids.is_empty() {
            return Ok(HashMap::new());
        }

        let query = "
            SELECT pa.pubmed_id, a.name
            FROM pubmed_authors pa
            JOIN authors a ON pa.author_id = a.id
            WHERE pa.pubmed_id = ANY($1)
            ORDER BY pa.pubmed_id, pa.position
        ";

        let rows = self.client.query(query, &[&pmids]).await?;

        let mut result = HashMap::new();
        for row in rows {
            let pmid: String = row.get("pubmed_id");
            let name: String = row.get("name");

            result.entry(pmid).or_insert_with(Vec::new).push(name);
        }

        Ok(result)
    }

    async fn fetch_mesh_terms_batch(
        &self,
        pmids: &[String],
    ) -> Result<HashMap<String, Vec<String>>, Box<dyn Error>> {
        if pmids.is_empty() {
            return Ok(HashMap::new());
        }

        let query = "
            SELECT pmt.pubmed_id, m.term
            FROM pubmed_mesh_terms pmt
            JOIN mesh_terms m ON pmt.mesh_id = m.id
            WHERE pmt.pubmed_id = ANY($1)
        ";

        let rows = self.client.query(query, &[&pmids]).await?;

        let mut result = HashMap::new();
        for row in rows {
            let pmid: String = row.get("pubmed_id");
            let term: String = row.get("term");

            result.entry(pmid).or_insert_with(Vec::new).push(term);
        }

        Ok(result)
    }

    async fn insert_matches(
        &self,
        matches: &[Match],
        skip_insert: bool,
    ) -> Result<(), Box<dyn Error>> {
        // Skip insertion if requested (for testing)
        if skip_insert {
            println!("Skipping database insertion as requested");
            return Ok(());
        }

        if matches.is_empty() {
            println!("No matches to insert");
            return Ok(());
        }

        // Create a progress bar for the insertion process
        let pb = ProgressBar::new(matches.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.yellow/blue} {pos:>7}/{len:7} {percent}% {eta} {msg}")
        .unwrap()
        .progress_chars("##-"));
        pb.set_message("Inserting matches into database");

        // For a smaller number of matches, individual insertions are simpler
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
                    // FIX: Removed extra closing parenthesis at the end
                    query.push_str(&format!(
                        "('{}', '{}', {}, '{}'::{})",
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
                pb.inc(batch.len() as u64);
            }
        } else {
            // For a larger number of matches, use batch insertions
            // Create a temporary table for bulk insertion
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
                pb.inc(batch.len() as u64);
            }

            // Transfer from temporary table to real table
            pb.set_message("Transferring matches to permanent table");
            self.client
                .execute(
                    "
            INSERT INTO publication_pubmed_matches (publication_id, pmid, match_quality, match_type)
            SELECT publication_id, pmid, match_quality::numeric(5,2), match_type::match_type
            FROM temp_matches
            ON CONFLICT (publication_id, pmid) 
            DO UPDATE SET 
                match_quality = EXCLUDED.match_quality,
                match_type = EXCLUDED.match_type
        ",
                    &[],
                )
                .await?;
        }

        pb.finish_with_message(format!("Inserted {} matches into database", matches.len()));

        Ok(())
    }
    async fn insert_publications_before_matches(
        &self,
        publications: &[Publication],
    ) -> Result<(), Box<dyn Error>> {
        println!("Inserting publications into database...");

        // Prepare publications insert statement
        let stmt = self
            .client
            .prepare(
                "INSERT INTO publications (id, title, title_normalized, year) 
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (id) DO NOTHING",
            )
            .await?;

        for pub_ in publications {
            let year = pub_.year.parse::<i32>().ok();

            self.client
                .execute(
                    &stmt,
                    &[
                        &pub_.cfrespublid,
                        &pub_.title,
                        &normalize_text(&pub_.title), // Use your normalization function
                        &year,
                    ],
                )
                .await?;
        }

        println!("Inserted {} publications", publications.len());
        Ok(())
    }
    async fn count_pubmed_records_by_year(&self, year: Option<i32>) -> Result<i64, Box<dyn Error>> {
        let query = if let Some(year_val) = year {
            "SELECT COUNT(*) FROM pubmed_records WHERE year = $1"
        } else {
            "SELECT COUNT(*) FROM pubmed_records WHERE year IS NULL"
        };

        let row = if let Some(year_val) = year {
            self.client.query_one(query, &[&year_val]).await?
        } else {
            self.client.query_one(query, &[]).await?
        };

        Ok(row.get::<_, i64>(0))
    }
    async fn fetch_pubmed_records_by_year_with_size(
        &self,
        year: Option<i32>,
        offset: i64,
        batch_size: i64,
    ) -> Result<(Vec<PubMedRecord>, bool), Box<dyn Error>> {
        // Adjust the query based on whether a year is provided
        let query = if let Some(_year_val) = year {
            "
        SELECT pr.pmid, pr.title, pr.title_normalized, 
               COALESCE(pr.journal, '') as journal, 
               COALESCE(pr.abstract, '') as abstract_text,
               pr.doi, pr.year
        FROM pubmed_records pr
        WHERE pr.year = $1
        ORDER BY pr.pmid
        LIMIT $2 OFFSET $3
        "
        } else {
            "
        SELECT pr.pmid, pr.title, pr.title_normalized, 
               COALESCE(pr.journal, '') as journal, 
               COALESCE(pr.abstract, '') as abstract_text,
               pr.doi, pr.year
        FROM pubmed_records pr
        WHERE pr.year IS NULL
        ORDER BY pr.pmid
        LIMIT $1 OFFSET $2
        "
        };

        // Execute the query with appropriate parameters
        let rows = if let Some(year_val) = year {
            self.client
                .query(query, &[&year_val, &batch_size, &offset])
                .await?
        } else {
            self.client.query(query, &[&batch_size, &offset]).await?
        };

        // Check if there are more records available
        let has_more = rows.len() as i64 == batch_size;

        // Process the results
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let pmid: String = row.get("pmid");
            records.push(PubMedRecord {
                pmid: pmid.clone(),
                title: row.get("title"),
                title_normalized: row.get("title_normalized"),
                journal: row.get("journal"),
                abstract_text: row.get("abstract_text"),
                doi: row.get("doi"),
                year: row.get("year"),
                authors: Vec::new(), // We don't load authors and mesh terms for each record
                mesh_terms: Vec::new(), // They will only be loaded for matching records
            });
        }

        Ok((records, has_more))
    }
}

// Utilidades
fn normalize_text(text: &str) -> String {
    text.to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ")
}

// Lógica de matching
fn find_matches(publication: &Publication, pubmed_records: &[PubMedRecord]) -> Vec<Match> {
    let mut matches = Vec::new();

    // Normalizar el título de la publicación para comparación
    let title_normalized = normalize_text(&publication.title);
    if title_normalized.is_empty() {
        return matches;
    }

    // Verificar si deberíamos intentar analizar el año
    let publication_year = publication.year.parse::<i32>().ok();

    // Para un procesamiento más rápido, utilizar un hashmap para encontrar rápidamente registros con años coincidentes
    let mut matched_by_year = false;

    // Si tenemos un año, intentar coincidir primero por año ya que es más rápido
    if let Some(pub_year) = publication_year {
        for record in pubmed_records {
            if let Some(record_year) = record.year {
                if pub_year == record_year {
                    // Calcular similitud de cadena para coincidencia de título
                    if title_normalized == record.title_normalized {
                        // Coincidencia exacta de título + año (máxima confianza)
                        matches.push(Match {
                            publication_id: publication.cfrespublid.clone(),
                            pmid: record.pmid.clone(),
                            match_quality: EXACT_TITLE_MATCH_QUALITY,
                            match_type: "exact_title".to_string(),
                        });
                        matched_by_year = true;
                    } else {
                        // Coincidencia difusa de título + año
                        let similarity =
                            normalized_levenshtein(&title_normalized, &record.title_normalized);
                        if similarity > SIMILARITY_THRESHOLD {
                            matches.push(Match {
                                publication_id: publication.cfrespublid.clone(),
                                pmid: record.pmid.clone(),
                                match_quality: similarity * TITLE_YEAR_MATCH_QUALITY,
                                match_type: "title_year".to_string(),
                            });
                            matched_by_year = true;
                        }
                    }
                }
            }
        }
    }

    // Si no encontramos coincidencias por año, probar otras estrategias
    if !matched_by_year {
        for record in pubmed_records {
            // Verificar coincidencia de DOI si está disponible
            if !publication.title.is_empty()
                && record.doi.is_some()
                && !record.doi.as_ref().unwrap().is_empty()
                && record.doi.as_ref().unwrap() == &publication.title
            {
                matches.push(Match {
                    publication_id: publication.cfrespublid.clone(),
                    pmid: record.pmid.clone(),
                    match_quality: DOI_MATCH_QUALITY,
                    match_type: "doi_match".to_string(),
                });
                continue;
            }

            // Probar coincidencia exacta de título
            if title_normalized == record.title_normalized {
                matches.push(Match {
                    publication_id: publication.cfrespublid.clone(),
                    pmid: record.pmid.clone(),
                    match_quality: EXACT_TITLE_MATCH_QUALITY,
                    match_type: "exact_title".to_string(),
                });
                continue;
            }

            // Coincidencia difusa de título con umbral alto
            let similarity = normalized_levenshtein(&title_normalized, &record.title_normalized);
            if similarity > HIGH_SIMILARITY_THRESHOLD {
                matches.push(Match {
                    publication_id: publication.cfrespublid.clone(),
                    pmid: record.pmid.clone(),
                    match_quality: similarity * FUZZY_TITLE_MATCH_QUALITY,
                    match_type: "fuzzy_title".to_string(),
                });
            }
        }
    }

    // Ordenar por calidad de coincidencia (descendente)
    matches.sort_by(|a, b| b.match_quality.partial_cmp(&a.match_quality).unwrap());

    // Devolver solo la coincidencia de mayor calidad
    if !matches.is_empty() {
        return vec![matches[0].clone()];
    }

    Vec::new()
}

// Procesar un lote de publicaciones y devolver las coincidencias
async fn process_publications_by_year(
    db_handler: &DbHandler,
    checkpoint_manager: &CheckpointManager,
    publications: &[&Publication],
    year: Option<i32>,
    progress_bar: &ProgressBar,
    resume: bool,
) -> Result<Vec<Match>, Box<dyn Error>> {
    // Filter publications for the specified year
    let year_publications: Vec<&Publication> = if let Some(year_val) = year {
        publications
            .iter()
            .filter(|p| p.year.parse::<i32>().ok() == Some(year_val))
            .copied()
            .collect()
    } else {
        // Publications without year or with year that cannot be parsed
        publications
            .iter()
            .filter(|p| p.year.parse::<i32>().is_err() || p.year.is_empty())
            .copied()
            .collect()
    };

    if year_publications.is_empty() {
        return Ok(Vec::new());
    }

    let year_str = year.map_or("NULL".to_string(), |y| y.to_string());
    println!(
        "Processing {} publications with year={}",
        year_publications.len(),
        year_str
    );

    // Check if this year has already been completed
    let completed_years = checkpoint_manager.load_completed_years()?;
    if completed_years.contains(&year_str) {
        println!(
            "Year {} already completed, loading matches from checkpoint",
            year_str
        );
        return checkpoint_manager.load_batch_matches_for_year(year);
    }

    // Get the total number of records for this year
    let total_records = db_handler.count_pubmed_records_by_year(year).await?;
    println!(
        "Total PubMed records for year={}: {}",
        year_str, total_records
    );

    // Initialize variables for batch processing
    let mut all_matches = Vec::new();
    let mut offset = 0;
    let mut batch_count = 0;

    // If resuming, check for a saved offset
    if resume {
        if let Some(saved_offset) = checkpoint_manager.load_year_offset(year)? {
            offset = saved_offset;
            println!(
                "Resuming processing for year={} from offset={}",
                year_str, offset
            );

            // Also load matches that were already found
            all_matches = checkpoint_manager.load_batch_matches_for_year(year)?;
        }

        // Check for recovery state
        if let Some(state) = checkpoint_manager.load_processing_state(year)? {
            if let Some(state_offset) = state.get("offset").and_then(|o| o.as_i64()) {
                if state_offset > offset {
                    offset = state_offset;
                    println!("Recovered from error state at offset={}", offset);
                }
            }
        }
    }

    // Dynamic batch size adjustment
    let mut current_batch_size = QUERY_BATCH_SIZE;

    // Special handling for NULL year which may have a very large dataset
    if year.is_none() && total_records > 1_000_000 {
        current_batch_size = QUERY_BATCH_SIZE / 10;
        println!(
            "Using reduced batch size {} for NULL year processing",
            current_batch_size
        );
    }

    // Process all records for this year in batches
    while offset < total_records {
        batch_count += 1;

        // Try to get the next batch of records with error handling
        let result = try_get_pubmed_batch(db_handler, year, offset, current_batch_size).await;

        match result {
            Ok((pubmed_records, has_more)) => {
                println!(
                    "Retrieved {} PubMed records for year={} (batch {}, offset {})",
                    pubmed_records.len(),
                    year_str,
                    batch_count,
                    offset
                );

                if pubmed_records.is_empty() {
                    break;
                }

                // Use a mutex to collect matches from all threads
                let batch_matches = Arc::new(Mutex::new(Vec::new()));
                let matched_count = Arc::new(Mutex::new(0));

                // Process publications in parallel using Rayon
                year_publications
                    .par_chunks(PARALLEL_CHUNK_SIZE)
                    .for_each(|chunk| {
                        let thread_matches: Vec<Match> = chunk
                            .iter()
                            .flat_map(|pub_| {
                                let matches = find_matches(pub_, &pubmed_records);

                                if !matches.is_empty() {
                                    let mut count = matched_count.lock().unwrap();
                                    *count += 1;
                                }

                                matches
                            })
                            .collect();

                        // Add the matches to the shared vector
                        if !thread_matches.is_empty() {
                            let mut all_matches_lock = batch_matches.lock().unwrap();
                            all_matches_lock.extend(thread_matches);
                        }
                    });

                // Extract the matches for this batch
                let current_batch_matches = Arc::try_unwrap(batch_matches)
                    .expect("Failed to unwrap Arc")
                    .into_inner()
                    .expect("Failed to unwrap Mutex");

                // If we found matches, enrich them with authors and mesh terms
                if !current_batch_matches.is_empty() {
                    // Collect PMIDs from the matches
                    let pmids: Vec<String> = current_batch_matches
                        .iter()
                        .map(|m| m.pmid.clone())
                        .collect();

                    // Get PubMed records to enrich
                    let mut to_enrich = Vec::new();
                    for pmid in &pmids {
                        if let Some(idx) = pubmed_records.iter().position(|r| r.pmid == *pmid) {
                            to_enrich.push(pubmed_records[idx].clone());
                        }
                    }

                    // Enrich records
                    let mut to_enrich_vec = to_enrich;
                    if !to_enrich_vec.is_empty() {
                        println!(
                            "Enriching {} matched PubMed records with authors and mesh terms",
                            to_enrich_vec.len()
                        );
                        db_handler.enrich_pubmed_records(&mut to_enrich_vec).await?;
                    }
                }

                // Save the batch matches to a checkpoint
                checkpoint_manager.save_batch_matches(year, batch_count, &current_batch_matches)?;

                // Add the batch matches to the overall result
                all_matches.extend(current_batch_matches);

                // Save the current offset for potential resume
                checkpoint_manager.save_year_offset(year, offset + pubmed_records.len() as i64)?;

                // Update the offset for the next batch
                offset += pubmed_records.len() as i64;

                // No more records, exit the loop
                if !has_more || pubmed_records.len() < current_batch_size {
                    break;
                }
            }
            Err(e) => {
                // Save recovery state with error information
                checkpoint_manager.save_processing_state(
                    year,
                    offset,
                    batch_count,
                    year_publications.len(),
                    &format!("{}", e),
                )?;

                // If we encounter a memory error, reduce batch size and retry
                if e.to_string().contains("No space left on device") {
                    // Half the current batch size and try again
                    current_batch_size = current_batch_size / 2;
                    if current_batch_size < 100 {
                        // If batch size gets too small, something else is wrong
                        return Err(format!(
                            "Failed to process year={} even with minimal batch size: {}",
                            year_str, e
                        )
                        .into());
                    }

                    println!(
                        "Memory error encountered. Reducing batch size to {} and retrying.",
                        current_batch_size
                    );
                    continue;
                } else {
                    // For other errors, propagate upward
                    return Err(format!(
                        "Error processing year={} at offset={}: {}",
                        year_str, offset, e
                    )
                    .into());
                }
            }
        }
    }

    // Increment the progress bar for all publications of this year
    progress_bar.inc(year_publications.len() as u64);

    // Mark this year as completed
    checkpoint_manager.save_completed_year(year)?;

    // Report on matches found
    println!(
        "Found {} matches for year={} after processing {} batches",
        all_matches.len(),
        year_str,
        batch_count
    );

    Ok(all_matches)
}
async fn try_get_pubmed_batch(
    db_handler: &DbHandler,
    year: Option<i32>,
    offset: i64,
    batch_size: usize,
) -> Result<(Vec<PubMedRecord>, bool), Box<dyn Error>> {
    // Create a smaller adjusted query batch size
    let adjusted_batch_size = std::cmp::min(batch_size, QUERY_BATCH_SIZE);

    // Set a timeout for the database query
    let timeout = tokio::time::timeout(
        std::time::Duration::from_secs(120), // 2 minute timeout
        db_handler.fetch_pubmed_records_by_year_with_size(year, offset, adjusted_batch_size as i64),
    )
    .await;

    match timeout {
        Ok(result) => result,
        Err(_) => {
            // Timeout occurred, try again with smaller batch
            println!("Query timeout. Retrying with smaller batch size.");
            let smaller_size = adjusted_batch_size / 2;
            db_handler
                .fetch_pubmed_records_by_year_with_size(year, offset, smaller_size as i64)
                .await
        }
    }
}
async fn enrich_and_save_publications(
    db_handler: &DbHandler,
    publications: &[Publication],
    matches: &[Match],
    output_path: &str,
) -> Result<(), Box<dyn Error>> {
    // Crear barra de progreso
    let pb = ProgressBar::new(publications.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "[{elapsed_precise}] {bar:40.magenta/blue} {pos:>7}/{len:7} {percent}% {eta} {msg}",
            )
            .unwrap()
            .progress_chars("##-"),
    );
    pb.set_message("Enriching publications");

    // Mapeo directo de ID de publicación a su Match correspondiente
    let publication_to_match: HashMap<String, &Match> = matches
        .iter()
        .map(|m| (m.publication_id.clone(), m))
        .collect();

    // Obtener IDs de publicaciones con matches
    let publication_ids: Vec<String> = matches.iter().map(|m| m.publication_id.clone()).collect();

    pb.set_message("Obtener datos enriquecidos directamente de la BD");

    // Consultar la base de datos directamente para todos los valores
    let enriched_data = db_handler
        .fetch_direct_publication_data(&publication_ids)
        .await?;

    pb.set_message("Escribiendo CSV enriquecido");

    // Crear el escritor CSV
    let file = File::create(output_path)?;
    let buf_writer = std::io::BufWriter::with_capacity(256 * 1024, file);
    let mut writer = WriterBuilder::new()
        .delimiter(b',')
        .quote_style(csv::QuoteStyle::Necessary)
        .from_writer(buf_writer);

    let mut matched_count = 0;

    // Procesar cada publicación
    for pub_ in publications {
        let pub_id = &pub_.cfrespublid;

        if let Some(enriched) = enriched_data.get(pub_id) {
            // Publicación con datos enriquecidos
            matched_count += 1;

            let enriched_pub = EnrichedPublication {
                // Campos originales
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

                // Campos enriquecidos directamente del resultado de consulta
                pmid: enriched.pmid.clone(),
                pubmed_title: enriched.pubmed_title.clone(),
                pubmed_journal: enriched.journal.clone(),
                pubmed_abstract: enriched.abstract_text.clone(),
                pubmed_doi: enriched.doi.clone().unwrap_or_default(),
                pubmed_authors: enriched.authors.clone(),
                pubmed_mesh_terms: enriched.mesh_terms.clone(),
                match_quality: enriched.match_quality,
                match_type: enriched.match_type.clone(),
            };

            writer.serialize(enriched_pub)?;
        } else if let Some(m) = publication_to_match.get(pub_id) {
            // Publicación con match pero sin datos enriquecidos
            matched_count += 1;

            let enriched_pub = EnrichedPublication {
                // Campos originales
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

                // Campos enriquecidos básicos del Match
                pmid: m.pmid.clone(),
                pubmed_title: String::new(),
                pubmed_journal: String::new(),
                pubmed_abstract: String::new(),
                pubmed_doi: String::new(),
                pubmed_authors: String::new(),
                pubmed_mesh_terms: String::new(),
                match_quality: m.match_quality,
                match_type: m.match_type.clone(),
            };

            writer.serialize(enriched_pub)?;
        } else {
            // Publicación sin match
            let enriched_pub = EnrichedPublication {
                // Campos originales
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

                // Campos enriquecidos vacíos
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

        pb.inc(1);
    }

    writer.flush()?;

    // Mostrar estadísticas
    let match_rate = (matched_count as f64 / publications.len() as f64) * 100.0;
    pb.finish_with_message(format!(
        "CSV generado con {} publicaciones ({} con match, {:.1}% tasa de match)",
        publications.len(),
        matched_count,
        match_rate
    ));

    Ok(())
}

async fn process_null_year_publications(
    db_handler: &DbHandler,
    checkpoint_manager: &CheckpointManager,
    publications: &[&Publication],
    progress_bar: &ProgressBar,
    resume: bool,
) -> Result<Vec<Match>, Box<dyn Error>> {
    // Filter publications with NULL year
    let null_year_publications: Vec<&Publication> = publications
        .iter()
        .filter(|p| p.year.parse::<i32>().is_err() || p.year.is_empty())
        .copied()
        .collect();

    if null_year_publications.is_empty() {
        return Ok(Vec::new());
    }

    println!(
        "Processing {} publications with NULL year",
        null_year_publications.len()
    );

    // Check if NULL year has already been completed
    let completed_years = checkpoint_manager.load_completed_years()?;
    if completed_years.contains("NULL") {
        println!("NULL year already completed, loading matches from checkpoint");
        return checkpoint_manager.load_batch_matches_for_year(None);
    }

    // Get total number of NULL year records
    let total_records = db_handler.count_pubmed_records_by_year(None).await?;
    println!("Total PubMed records with NULL year: {}", total_records);

    // Initialize variables for batch processing
    let mut all_matches = Vec::new();
    let mut offset = 0;
    let mut batch_count = 0;

    // Use a much smaller batch size for NULL year
    let mut batch_size = 5_000; // Start with a small batch size

    // If resuming, check for a saved offset
    if resume {
        if let Some(saved_offset) = checkpoint_manager.load_year_offset(None)? {
            offset = saved_offset;
            println!("Resuming NULL year processing from offset={}", offset);

            // Also load matches that were already found
            all_matches = checkpoint_manager.load_batch_matches_for_year(None)?;
        }

        // Check for recovery state
        if let Some(state) = checkpoint_manager.load_processing_state(None)? {
            if let Some(state_offset) = state.get("offset").and_then(|o| o.as_i64()) {
                if state_offset > offset {
                    offset = state_offset;
                    println!("Recovered from error state at offset={}", offset);
                }

                // Possibly adjust batch size based on previous failure
                if let Some(batch_size_value) = state.get("batch_size").and_then(|b| b.as_u64()) {
                    // If we had a previous batch size that failed, use half of that
                    batch_size = std::cmp::min(batch_size, (batch_size_value / 2) as usize);
                    println!(
                        "Adjusted batch size to {} based on previous failure",
                        batch_size
                    );
                }
            }
        }
    }

    // Process records in smaller incremental batches
    while offset < total_records {
        batch_count += 1;

        // Save current state before attempting batch
        checkpoint_manager.save_processing_state(
            None,
            offset,
            batch_count,
            null_year_publications.len(),
            "In progress",
        )?;

        println!(
            "Processing NULL year batch {} with offset={} and batch_size={}",
            batch_count, offset, batch_size
        );

        // Try to get the next batch with error handling
        let result = try_get_pubmed_batch(db_handler, None, offset, batch_size).await;

        match result {
            Ok((pubmed_records, has_more)) => {
                println!(
                    "Retrieved {} PubMed records for NULL year (batch {}, offset {})",
                    pubmed_records.len(),
                    batch_count,
                    offset
                );

                if pubmed_records.is_empty() {
                    break;
                }

                // Use a mutex to collect matches from all threads
                let batch_matches = Arc::new(Mutex::new(Vec::new()));
                let matched_count = Arc::new(Mutex::new(0));

                // Process publications in parallel using Rayon
                null_year_publications
                    .par_chunks(PARALLEL_CHUNK_SIZE)
                    .for_each(|chunk| {
                        let thread_matches: Vec<Match> = chunk
                            .iter()
                            .flat_map(|pub_| {
                                let matches = find_matches(pub_, &pubmed_records);

                                if !matches.is_empty() {
                                    let mut count = matched_count.lock().unwrap();
                                    *count += 1;
                                }

                                matches
                            })
                            .collect();

                        // Add the matches to the shared vector
                        if !thread_matches.is_empty() {
                            let mut all_matches_lock = batch_matches.lock().unwrap();
                            all_matches_lock.extend(thread_matches);
                        }
                    });

                // Extract the matches for this batch
                let current_batch_matches = Arc::try_unwrap(batch_matches)
                    .expect("Failed to unwrap Arc")
                    .into_inner()
                    .expect("Failed to unwrap Mutex");

                // Save the batch matches to a checkpoint
                checkpoint_manager.save_batch_matches(None, batch_count, &current_batch_matches)?;

                // Add the batch matches to the overall result
                all_matches.extend(current_batch_matches);

                // Save the current offset for potential resume
                checkpoint_manager.save_year_offset(None, offset + pubmed_records.len() as i64)?;

                // Update the offset for the next batch
                offset += pubmed_records.len() as i64;

                // If this batch was successful, we can try to increase the batch size slightly
                if batch_size < QUERY_BATCH_SIZE / 5 {
                    batch_size = std::cmp::min(batch_size + 1000, QUERY_BATCH_SIZE / 5);
                    println!("Increasing batch size to {} for next batch", batch_size);
                }

                // No more records, exit the loop
                if !has_more || pubmed_records.len() < batch_size {
                    break;
                }
            }
            Err(e) => {
                // Save recovery state with error information
                checkpoint_manager.save_processing_state(
                    None,
                    offset,
                    batch_count,
                    null_year_publications.len(),
                    &format!("{}", e),
                )?;

                println!("Error processing NULL year batch: {}", e);

                // If we encounter a memory error, reduce batch size and retry
                if e.to_string().contains("No space left on device") {
                    // Reduce batch size significantly
                    batch_size = batch_size / 2;
                    if batch_size < 100 {
                        // If batch size gets too small, something else is wrong
                        return Err(format!(
                            "Failed to process NULL year even with minimal batch size: {}",
                            e
                        )
                        .into());
                    }

                    println!(
                        "Memory error encountered. Reducing batch size to {} and retrying.",
                        batch_size
                    );

                    // Add a delay before retrying to allow system to recover
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                } else {
                    // For other errors, propagate upward
                    return Err(
                        format!("Error processing NULL year at offset={}: {}", offset, e).into(),
                    );
                }
            }
        }
    }

    // Increment the progress bar for all NULL year publications
    progress_bar.inc(null_year_publications.len() as u64);

    // Mark NULL year as completed
    checkpoint_manager.save_completed_year(None)?;

    // Report on matches found
    println!(
        "Found {} matches for NULL year after processing {} batches",
        all_matches.len(),
        batch_count
    );

    Ok(all_matches)
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Parse command line arguments
    let matches = Command::new("PubMed Publication Matcher")
        .version("1.0")
        .author("Your Name <your.email@example.com>")
        .about("High-performance fuzzy matching for publications and PubMed records")
        .arg(
            Arg::new("input")
                .long("input")
                .short('i')
                .value_name("FILE")
                .help("CSV file containing publications to match")
                .required(true),
        )
        .arg(
            Arg::new("output")
                .long("output")
                .short('o')
                .value_name("FILE")
                .help("Output enriched CSV file")
                .required(true),
        )
        .arg(
            Arg::new("db-host")
                .long("db-host")
                .value_name("HOST")
                .help("Database host")
                .default_value("localhost"),
        )
        .arg(
            Arg::new("db-name")
                .long("db-name")
                .value_name("NAME")
                .help("Database name")
                .default_value("pubmed_integration"),
        )
        .arg(
            Arg::new("db-user")
                .long("db-user")
                .value_name("USER")
                .help("Database user")
                .default_value("pubmed"),
        )
        .arg(
            Arg::new("db-password")
                .long("db-password")
                .value_name("PASSWORD")
                .help("Database password")
                .default_value("pubmed_password"),
        )
        .arg(
            Arg::new("workers")
                .long("workers")
                .value_name("NUM")
                .help("Number of worker threads")
                .default_value("32"),
        )
        .arg(
            Arg::new("skip-db-insert")
                .long("skip-db-insert")
                .help("Skip database insertion (for testing)")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("stage")
                .long("stage")
                .value_name("STAGE")
                .help("Specific stage to run (load, match, save, enrich, all)")
                .default_value("all"),
        )
        .arg(
            Arg::new("resume")
                .long("resume")
                .help("Resume from last checkpoint")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("checkpoint-dir")
                .long("checkpoint-dir")
                .value_name("DIR")
                .help("Directory for checkpoints"),
        )
        .arg(
            Arg::new("max-memory")
                .long("max-memory")
                .value_name("MB")
                .help("Maximum memory to use for database operations (in MB)")
                .default_value("256"),
        )
        .get_matches();

    let input_path = matches.get_one::<String>("input").unwrap();
    let output_path = matches.get_one::<String>("output").unwrap();

    // Get database connection details
    let db_host = matches.get_one::<String>("db-host").unwrap();
    let db_name = matches.get_one::<String>("db-name").unwrap();
    let db_user = matches.get_one::<String>("db-user").unwrap();
    let db_password = matches.get_one::<String>("db-password").unwrap();

    // Get worker thread count
    let workers = matches
        .get_one::<String>("workers")
        .map(|s| s.parse::<usize>().unwrap_or(32))
        .unwrap_or(32);

    // Check if we should skip database insertion
    let skip_db_insert = matches.get_flag("skip-db-insert");

    // Get checkpoint parameters
    let stage = matches.get_one::<String>("stage").unwrap();
    let resume = matches.get_flag("resume");
    let checkpoint_dir = matches.get_one::<String>("checkpoint-dir");

    // Get memory limits
    let max_memory = matches
        .get_one::<String>("max-memory")
        .map(|s| s.parse::<usize>().unwrap_or(256))
        .unwrap_or(256);

    // Initialize the checkpoint manager
    let checkpoint_manager = if let Some(dir) = checkpoint_dir {
        CheckpointManager::new_with_dir(dir, output_path)?
    } else {
        CheckpointManager::new(output_path)?
    };

    // Build connection string
    let connection_str = format!(
        "host={} dbname={} user={} password={}",
        db_host, db_name, db_user, db_password
    );

    // Initialize progress tracking and start time
    let start_time = Instant::now();

    // Set up the number of worker threads
    rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build_global()
        .unwrap_or_else(|e| eprintln!("Failed to set thread pool size: {}", e));

    println!("Using {} worker threads", workers);
    println!("Maximum database memory: {} MB", max_memory);

    // Connect to the database with optimized settings
    println!("Connecting to database...");
    let db_handler = DbHandler::new_with_memory_limit(&connection_str, max_memory).await?;

    // Determine which stages to run
    let run_load = stage == "all" || stage == "load";
    let run_match = stage == "all" || stage == "match";
    let run_save = stage == "all" || stage == "save";
    let run_enrich = stage == "all" || stage == "enrich";

    // STAGE 1: Load publications
    let publications = if run_load && (!resume || !checkpoint_manager.is_stage_completed("load")) {
        println!("STAGE 1: Loading publications from {}...", input_path);

        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .delimiter(b',')
            .from_path(input_path)?;

        let publications: Vec<Publication> =
            rdr.deserialize().collect::<Result<Vec<Publication>, _>>()?;

        println!(
            "Loaded {} publications in {:?}",
            publications.len(),
            start_time.elapsed()
        );

        // Save loaded publications to checkpoint
        checkpoint_manager.mark_stage_completed("load")?;

        publications
    } else if resume || run_match || run_save || run_enrich {
        println!("Loading publications from checkpoint or original file...");

        // We need to load publications from the original file
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .delimiter(b',')
            .from_path(input_path)?;

        let publications: Vec<Publication> =
            rdr.deserialize().collect::<Result<Vec<Publication>, _>>()?;

        println!("Loaded {} publications from file", publications.len());
        publications
    } else {
        Vec::new()
    };

    // STAGE 2: Matching publications
    let matches = if run_match && (!resume || !checkpoint_manager.is_stage_completed("match")) {
        println!("STAGE 2: Finding matches for publications...");

        // Load previously processed publications if resuming
        let processed_pubs = if resume {
            checkpoint_manager.load_processed_publications()?
        } else {
            HashSet::new()
        };

        // Load previously found matches if resuming
        let mut existing_matches = if resume {
            checkpoint_manager.load_matches()?
        } else {
            Vec::new()
        };

        // Filter out already processed publications
        let remaining_pubs: Vec<&Publication> = publications
            .iter()
            .filter(|p| !processed_pubs.contains(&p.cfrespublid))
            .collect();

        println!(
            "Finding matches for {} remaining publications",
            remaining_pubs.len()
        );

        if remaining_pubs.is_empty() {
            println!("No new publications to process. Using previously found matches.");
            existing_matches
        } else {
            // Get distinct years available in the database
            println!("Retrieving distinct years from PubMed database...");
            let mut years = db_handler.get_distinct_years().await?;
            println!("Found {} distinct years in PubMed database", years.len());

            // Set up progress bar
            let pb = ProgressBar::new(remaining_pubs.len() as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({eta}) {msg}")
                    .unwrap()
                    .progress_chars("##-"),
            );
            pb.set_message("Finding matches by year");

            // Process by year to minimize memory usage
            let start_matching = Instant::now();
            let mut year_stats = Vec::new();

            // First identify if NULL year exists and move it to the end for special handling
            let has_null_year = years.contains(&None);
            if has_null_year {
                // Remove NULL year to process it separately at the end
                years.retain(|&y| y.is_some());
            }

            // Process regular years (non-NULL) first
            for year in &years {
                let year_start = Instant::now();

                // Check if this year is already completed
                let year_str = year.map_or("NULL".to_string(), |y| y.to_string());
                let completed_years = checkpoint_manager.load_completed_years()?;

                if completed_years.contains(&year_str) {
                    println!(
                        "Year {} already completed, loading matches from checkpoint",
                        year_str
                    );
                    let year_matches = checkpoint_manager.load_batch_matches_for_year(*year)?;
                    existing_matches.extend(year_matches.clone());

                    // Update statistics
                    let processed_ids: Vec<String> = remaining_pubs
                        .iter()
                        .filter(|p| p.year.parse::<i32>().ok() == *year)
                        .map(|p| p.cfrespublid.clone())
                        .collect();

                    year_stats.push((
                        year_str,
                        year_matches.len(),
                        processed_ids.len(),
                        year_start.elapsed(),
                    ));

                    // Increment progress bar
                    pb.inc(processed_ids.len() as u64);
                    continue;
                }

                let year_matches = process_publications_by_year(
                    &db_handler,
                    &checkpoint_manager,
                    &remaining_pubs,
                    *year,
                    &pb,
                    resume,
                )
                .await?;

                let year_elapsed = year_start.elapsed();

                // Save matches for this year
                if !year_matches.is_empty() {
                    println!(
                        "Found {} matches for year {} in {:?}",
                        year_matches.len(),
                        year_str,
                        year_elapsed
                    );

                    // Add to existing matches
                    existing_matches.extend(year_matches.clone());

                    // Save IDs of processed publications for this year
                    let processed_ids: Vec<String> = remaining_pubs
                        .iter()
                        .filter(|p| p.year.parse::<i32>().ok() == *year)
                        .map(|p| p.cfrespublid.clone())
                        .collect();

                    if !processed_ids.is_empty() {
                        checkpoint_manager.save_processed_publications(&processed_ids)?;
                    }

                    // Update statistics
                    year_stats.push((
                        year_str,
                        year_matches.len(),
                        processed_ids.len(),
                        year_elapsed,
                    ));
                }
            }

            // Process NULL year last with special handling
            if has_null_year {
                println!("Processing NULL year with enhanced handling...");
                let null_year_start = Instant::now();

                // Check if NULL year is already completed
                let completed_years = checkpoint_manager.load_completed_years()?;
                let null_year_matches = if completed_years.contains("NULL") {
                    println!("NULL year already completed, loading matches from checkpoint");
                    checkpoint_manager.load_batch_matches_for_year(None)?
                } else {
                    // Use specialized function for NULL year processing
                    process_null_year_publications(
                        &db_handler,
                        &checkpoint_manager,
                        &remaining_pubs,
                        &pb,
                        resume,
                    )
                    .await?
                };

                let null_year_elapsed = null_year_start.elapsed();

                if !null_year_matches.is_empty() {
                    println!(
                        "Found {} matches for NULL year in {:?}",
                        null_year_matches.len(),
                        null_year_elapsed
                    );

                    existing_matches.extend(null_year_matches.clone());

                    // Save IDs of publications with NULL or invalid year
                    let processed_ids: Vec<String> = remaining_pubs
                        .iter()
                        .filter(|p| p.year.parse::<i32>().is_err() || p.year.is_empty())
                        .map(|p| p.cfrespublid.clone())
                        .collect();

                    if !processed_ids.is_empty() {
                        checkpoint_manager.save_processed_publications(&processed_ids)?;
                    }

                    // Update statistics
                    year_stats.push((
                        "NULL".to_string(),
                        null_year_matches.len(),
                        processed_ids.len(),
                        null_year_elapsed,
                    ));
                }
            }

            pb.finish_with_message(format!(
                "Matching complete in {:?}",
                start_matching.elapsed()
            ));

            // Show detailed statistics by year
            println!("\n=== YEAR-BY-YEAR MATCHING STATISTICS ===");
            println!(
                "| {:<10} | {:<15} | {:<20} | {:<15} |",
                "Year", "Matches Found", "Publications Processed", "Processing Time"
            );
            println!("|{:-<12}|{:-<17}|{:-<22}|{:-<17}|", "", "", "", "");

            for (year, matches_count, pubs_count, elapsed) in year_stats {
                println!(
                    "| {:<10} | {:<15} | {:<20} | {:<15?} |",
                    year, matches_count, pubs_count, elapsed
                );
            }

            println!("\nTotal matches found: {}", existing_matches.len());
            println!("Total matching time: {:?}", start_matching.elapsed());

            // Mark stage as completed
            checkpoint_manager.mark_stage_completed("match")?;

            existing_matches
        }
    } else if resume || run_save || run_enrich {
        println!("Loading matches from checkpoint...");
        checkpoint_manager.load_matches()?
    } else {
        Vec::new()
    };
    if run_save && (!resume || !checkpoint_manager.is_stage_completed("save_pubs")) {
        println!("Inserting publications into database...");
        db_handler
            .insert_publications_before_matches(&publications)
            .await?;
        checkpoint_manager.mark_stage_completed("save_pubs")?;
    }
    // STAGE 3: Save matches to database
    if run_save && (!resume || !checkpoint_manager.is_stage_completed("save")) {
        println!("STAGE 3: Saving {} matches to database...", matches.len());

        // Save matches to database
        db_handler.insert_matches(&matches, skip_db_insert).await?;

        // Mark stage as completed
        checkpoint_manager.mark_stage_completed("save")?;
    }

    // STAGE 4: Enrich and save publications
    if run_enrich && (!resume || !checkpoint_manager.is_stage_completed("enrich")) {
        println!(
            "STAGE 4: Enriching and saving publications to {}...",
            output_path
        );

        // Enrich and save publications
        enrich_and_save_publications(&db_handler, &publications, &matches, output_path).await?;

        // Mark stage as completed
        checkpoint_manager.mark_stage_completed("enrich")?;
    }

    // Create summary
    let unique_publications_matched = matches
        .iter()
        .map(|m| &m.publication_id)
        .collect::<HashSet<&String>>()
        .len();

    checkpoint_manager.write_summary(
        start_time,
        publications.len(),
        matches.len(),
        unique_publications_matched,
    )?;

    // Print summary statistics
    let match_rate = (unique_publications_matched as f64 / publications.len() as f64) * 100.0;

    println!("\n=== MATCHING SUMMARY ===");
    println!("Total publications: {}", publications.len());
    println!(
        "Publications matched: {} ({:.1}%)",
        unique_publications_matched, match_rate
    );
    println!("Total matches found: {}", matches.len());

    // Count match types
    let mut match_types = HashMap::new();
    for m in &matches {
        *match_types.entry(m.match_type.clone()).or_insert(0) += 1;
    }

    println!("Match types:");
    for (match_type, count) in match_types.iter() {
        let percentage = (*count as f64 / matches.len() as f64) * 100.0;
        println!("  - {}: {} ({:.1}%)", match_type, count, percentage);
    }

    println!(
        "Total processing time: {:.2} seconds",
        start_time.elapsed().as_secs_f64()
    );
    println!(
        "Checkpoint directory: {}",
        checkpoint_manager.checkpoint_dir.display()
    );

    println!("All requested stages completed successfully!");

    Ok(())
}
