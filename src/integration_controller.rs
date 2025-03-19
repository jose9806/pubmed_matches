use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::db::DatabaseService;

pub enum OperationType {
    IoIntensive,
    CpuIntensive,
}
use crate::matching_service::MatchingService;
use crate::models::{IntegrationConfig, MatchingStats};
use crate::publication_service::PublicationDataService;
use crate::pubmed_engine::PubMedProcessingEngine;

pub struct ParallelProcessingConfig {
    pub max_threads: usize,
    pub adaptive_threading: bool,
    pub io_bound_thread_multiplier: f32,
    pub cpu_bound_thread_multiplier: f32,
    pub memory_limit_per_thread_mb: usize,
}

impl Default for ParallelProcessingConfig {
    fn default() -> Self {
        Self {
            max_threads: num_cpus::get(),
            adaptive_threading: true,
            io_bound_thread_multiplier: 2.0,
            cpu_bound_thread_multiplier: 1.0,
            memory_limit_per_thread_mb: 512,
        }
    }
}

impl ParallelProcessingConfig {
    pub fn calculate_optimal_threads(&self, operation_type: OperationType) -> usize {
        let available_memory = self.get_available_system_memory_mb();
        let base_threads = match self.adaptive_threading {
            true => num_cpus::get(),
            false => self.max_threads,
        };

        // Calculate thread count based on operation type
        let thread_count = match operation_type {
            OperationType::IoIntensive => {
                (base_threads as f32 * self.io_bound_thread_multiplier) as usize
            },
            OperationType::CpuIntensive => {
                (base_threads as f32 * self.cpu_bound_thread_multiplier) as usize
            },
        };

        // Limit by available memory
        let memory_limited_threads = available_memory / self.memory_limit_per_thread_mb;

        thread_count.min(memory_limited_threads).max(1)
    }
    
    fn get_available_system_memory_mb(&self) -> usize {
        // Simple implementation - in a real application, you would use a system-specific method
        // to determine available memory
        4096 // Default to 4GB
    }
}
pub struct IntegrationController {
    config: IntegrationConfig,
    publication_service: PublicationDataService,
    pubmed_engine: PubMedProcessingEngine,
    matching_service: MatchingService,
    db_service: Option<DatabaseService>,
}

impl IntegrationController {
    /// Create a new integration controller with default configuration
    pub fn new() -> Self {
        Self {
            config: IntegrationConfig::default(),
            publication_service: PublicationDataService::new(),
            pubmed_engine: PubMedProcessingEngine::new(5),
            matching_service: MatchingService::new(),
            db_service: None,
        }
    }

    /// Create a new integration controller with custom configuration
    pub fn with_config(config: IntegrationConfig) -> Self {
        Self {
            config: config.clone(),
            publication_service: PublicationDataService::new(),
            pubmed_engine: PubMedProcessingEngine::new(config.batch_size),
            matching_service: MatchingService::with_config(config),
            db_service: None,
        }
    }

    /// Set the database service
    pub fn with_database(mut self, db_service: DatabaseService) -> Self {
        self.db_service = Some(db_service);
        self
    }

    /// Execute the integration process with database
    pub async fn execute_with_database(&mut self) -> Result<MatchingStats> {
        info!("Starting integration process with database");
        let start_time = Instant::now();

        let db = self
            .db_service
            .as_ref()
            .context("Database service not initialized")?;

        // 1. Load publications from database
        let publications = db.load_publications().await?;
        info!("Loaded {} publications from database", publications.len());

        // Fix: Use single add_publication for each publication
        for publication in &publications {
            self.publication_service
                .add_publication(publication.clone());
        }

        self.publication_service.create_indexes()?;

        // 2. Load PubMed records from database
        let pubmed_records = db.load_pubmed_records().await?;
        info!(
            "Loaded {} PubMed records from database",
            pubmed_records.len()
        );

        // 3. Find matches between publications and PubMed records
        let (matches, stats) = self.matching_service.find_matches(
            &self.publication_service,
            &pubmed_records
                .into_iter()
                .map(|r| (r.pmid.clone(), r))
                .collect(),
        )?;

        // 4. Store matches in database
        let saved_count = db.create_matches(&matches).await?;
        info!("Saved {} matches to database", saved_count);

        let duration = start_time.elapsed();
        info!(
            "Integration complete in {:.2?}. Found {} matches for {} publications ({:.2}% matched)",
            duration,
            stats.matched_publications,
            stats.total_publications,
            (stats.matched_publications as f64 / stats.total_publications as f64) * 100.0
        );

        Ok(stats)
    }

    /// Process CSV data and store in database
    pub async fn process_csv_to_database<P: AsRef<Path>>(&self, csv_path: P) -> Result<usize> {
        info!(
            "Processing CSV data from {} to database",
            csv_path.as_ref().display()
        );

        let db = self
            .db_service
            .as_ref()
            .context("Database service not initialized")?;

        // Load CSV data
        let mut service = PublicationDataService::new();
        service.load_from_csv(csv_path)?;

        // Fix: Get all publications using IDs and get_publication
        let publication_ids = service.get_all_ids();
        let mut publications = Vec::new();

        for id in publication_ids {
            if let Some(publication) = service.get_publication(&id) {
                publications.push(publication);
            }
        }

        let count = db.import_publications(&publications).await?;

        info!("Processed {} publications from CSV to database", count);

        Ok(count)
    }

    /// Process PubMed XML files and store in database
    pub async fn process_xml_to_database<P: AsRef<Path>>(
        &self,
        xml_folders: &[P],
    ) -> Result<usize> {
        info!(
            "Processing PubMed XML files from {} folders to database",
            xml_folders.len()
        );

        let db = self
            .db_service
            .as_ref()
            .context("Database service not initialized")?;

        // Convert folder paths
        let folder_paths: Vec<&Path> = xml_folders.iter().map(|p| p.as_ref()).collect();

        // Process XML files
        let pubmed_engine = PubMedProcessingEngine::new(self.config.batch_size);
        let pubmed_records = pubmed_engine.process_folders(&folder_paths)?;

        // Convert to Vec
        let records: Vec<_> = pubmed_records.into_values().collect();

        // Store in database
        let count = db.import_pubmed_records(&records).await?;

        info!("Processed {} PubMed records to database", count);

        Ok(count)
    }

    /// Export enriched publications to CSV
    pub async fn export_enriched_publications<P: AsRef<Path>>(
        &self,
        output_path: P,
    ) -> Result<usize> {
        info!(
            "Exporting enriched publications to {}",
            output_path.as_ref().display()
        );

        let db = self
            .db_service
            .as_ref()
            .context("Database service not initialized")?;

        // Get enriched publications from database
        let enriched = db.get_enriched_publications().await?;

        // Create a temporary publication service to save to CSV
        let mut service = PublicationDataService::new();

        // Add the enriched publications
        for pub_record in &enriched {
            service.add_publication(pub_record.clone());
        }

        // Save to CSV
        service.save_to_csv(output_path)?;

        info!("Exported {} enriched publications to CSV", enriched.len());

        Ok(enriched.len())
    }
    // In src/integration_controller.rs
    pub async fn execute_streaming<F>(&mut self, callback: F) -> Result<MatchingStats>
    where
        F: FnMut(PublicationData) -> Result<()>,
    {
        let start_time = Instant::now();

        // Process publications in small batches, streaming results to callback
        let chunk_size = 500;
        let mut batch_start = 0;

        loop {
            let publications_batch = self
                .db_service
                .load_publications_batch(batch_start, chunk_size)
                .await?;

            if publications_batch.is_empty() {
                break;
            }

            // Process this batch, streaming results rather than accumulating
            for publication in &publications_batch {
                self.publication_service
                    .add_publication(publication.clone());
            }

            // Find matches for this batch only
            let (batch_matches, _) = self.matching_service.find_matches_for_batch(
                &self.publication_service,
                &publications_batch,
                &self.pubmed_records,
            )?;

            // Stream results through callback
            callback(PublicationData {
                publications: publications_batch,
                matches: batch_matches,
            })?;

            // Prepare for next batch
            batch_start += publications_batch.len();

            // Clear batch data to free memory
            self.publication_service.clear_batch(&publications_batch);
        }

        // Return statistics
        Ok(self.matching_service.get_stats())
    }
}
