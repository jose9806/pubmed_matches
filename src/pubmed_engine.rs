use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, error, info, warn};
use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::models::PubMedRecord;
use crate::utils::{get_filename, normalize_string, time_operation};

// Optimal buffer sizes determined through benchmarking
const FILE_BUFFER_SIZE: usize = 256 * 1024; // 256KB
const XML_BUFFER_SIZE: usize = 64 * 1024; // 64KB
const BATCH_PROCESSING_SIZE: usize = 100; // Process 100 records at a time

/// Engine for processing PubMed XML files with optimized performance
#[derive(Clone)]
pub struct PubMedProcessingEngine {
    batch_size: usize,
    max_retries: usize,
    progress_bar: Option<ProgressBar>,
    // Cache to avoid repeated normalization
    normalization_cache: Arc<Mutex<HashMap<String, String>>>,
}

impl PubMedProcessingEngine {
    /// Create a new PubMed processing engine
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            max_retries: 3,
            progress_bar: None,
            normalization_cache: Arc::new(Mutex::new(HashMap::with_capacity(10_000))),
        }
    }

    /// Set a progress bar for tracking file processing
    pub fn with_progress_bar(mut self, progress_bar: ProgressBar) -> Self {
        self.progress_bar = Some(progress_bar);
        self
    }

    /// Process all XML files in the specified folders with streaming architecture
    pub fn process_folders_streaming<P: AsRef<Path>, F>(
        &self,
        folders: &[P],
        mut callback: F,
    ) -> Result<usize>
    where
        F: FnMut(Vec<PubMedRecord>) -> Result<()>,
    {
        info!(
            "Processing PubMed files from {} folders with streaming",
            folders.len()
        );

        let xml_files = self.collect_xml_files(folders)?;
        info!("Found {} XML files to process", xml_files.len());

        if let Some(pb) = &self.progress_bar {
            pb.set_length(xml_files.len() as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}")
                    .unwrap()
                    .progress_chars("=> "),
            );
            pb.set_message("Processing XML files...");
        }

        // Process files in batches to control memory usage
        let total_processed = Arc::new(Mutex::new(0));
        let processed_files = Arc::new(Mutex::new(HashSet::new()));

        for (batch_idx, batch) in xml_files.chunks(self.batch_size).enumerate() {
            info!(
                "Processing batch {}/{} with {} files",
                batch_idx + 1,
                (xml_files.len() + self.batch_size - 1) / self.batch_size,
                batch.len()
            );

            // Process this batch of files in parallel
            let batch_results: Vec<_> = batch
                .par_iter()
                .filter_map(|file_path| {
                    let filename = get_filename(file_path);

                    // Skip if already processed
                    {
                        let processed = processed_files.lock().unwrap();
                        if processed.contains(&filename) {
                            debug!("Skipping already processed file: {}", filename);
                            return None;
                        }
                    }

                    // Track processing time for this file
                    let start = Instant::now();
                    debug!("Starting to process file: {}", filename);

                    match self.process_file(file_path) {
                        Ok(records) => {
                            let duration = start.elapsed();
                            info!(
                                "Successfully processed {} with {} records in {:.2?}",
                                filename,
                                records.len(),
                                duration
                            );

                            if let Some(pb) = &self.progress_bar {
                                pb.inc(1);
                                pb.set_message(format!("Processed {}", filename));
                            }

                            // Mark as processed
                            {
                                let mut processed = processed_files.lock().unwrap();
                                processed.insert(filename);
                            }

                            Some(records)
                        }
                        Err(e) => {
                            error!("Failed to process {}: {}", filename, e);

                            if let Some(pb) = &self.progress_bar {
                                pb.inc(1);
                                pb.set_message(format!("Failed: {}", filename));
                            }

                            None
                        }
                    }
                })
                .collect();

            // Flatten batch results and send to callback in smaller chunks
            let all_records: Vec<PubMedRecord> = batch_results.into_iter().flatten().collect();

            // Update total processed count
            {
                let mut total = total_processed.lock().unwrap();
                *total += all_records.len();
            }

            // Process in smaller chunks to control memory usage
            for chunk in all_records.chunks(BATCH_PROCESSING_SIZE) {
                callback(chunk.to_vec())?;
            }

            info!(
                "Batch {}/{} complete. Total records so far: {}",
                batch_idx + 1,
                (xml_files.len() + self.batch_size - 1) / self.batch_size,
                *total_processed.lock().unwrap()
            );
        }

        let final_count = *total_processed.lock().unwrap();

        if let Some(pb) = &self.progress_bar {
            pb.finish_with_message(format!(
                "Processed all files. Found {} records",
                final_count
            ));
        }

        info!(
            "Completed processing {} XML files. Found {} total PubMed records",
            xml_files.len(),
            final_count
        );

        Ok(final_count)
    }

    /// Process all XML files in the specified folders (original in-memory version)
    pub fn process_folders<P: AsRef<Path>>(
        &self,
        folders: &[P],
    ) -> Result<HashMap<String, PubMedRecord>> {
        info!("Processing PubMed files from {} folders", folders.len());

        let mut all_records = HashMap::new();

        // Use the streaming version with a collector callback
        let records_collector = Arc::new(Mutex::new(&mut all_records));

        self.process_folders_streaming(folders, |records_chunk| {
            let mut collector = records_collector.lock().unwrap();
            for record in records_chunk {
                collector.insert(record.pmid.clone(), record);
            }
            Ok(())
        })?;

        Ok(all_records)
    }

    /// Process a single XML file with retries
    pub fn process_file<P: AsRef<Path>>(&self, path: P) -> Result<Vec<PubMedRecord>> {
        let file_path = path.as_ref();
        let filename = get_filename(file_path);

        for attempt in 1..=self.max_retries {
            match self.process_file_attempt(file_path) {
                Ok(records) => return Ok(records),
                Err(e) => {
                    if attempt < self.max_retries {
                        warn!(
                            "Error processing {} on attempt {}/{}: {}. Retrying...",
                            filename, attempt, self.max_retries, e
                        );
                        std::thread::sleep(Duration::from_secs(1));
                    } else {
                        error!(
                            "Failed to process {} after {} attempts: {}",
                            filename, self.max_retries, e
                        );
                        return Err(e);
                    }
                }
            }
        }

        // This should never be reached due to the return in the loop
        Err(anyhow::anyhow!("Failed to process file after retries"))
    }

    /// Single attempt to process an XML file
    fn process_file_attempt<P: AsRef<Path>>(&self, path: P) -> Result<Vec<PubMedRecord>> {
        time_operation("process_file", || -> Result<Vec<PubMedRecord>> {
            let file_path = path.as_ref();
            let filename = get_filename(file_path);

            let file = File::open(file_path)
                .with_context(|| format!("Failed to open file: {:?}", file_path))?;

            // Use optimized buffer size for better I/O performance
            let buf_reader = BufReader::with_capacity(FILE_BUFFER_SIZE, file);
            let gz_decoder = GzDecoder::new(buf_reader);

            self.process_xml_stream(gz_decoder, &filename)
        })
    }

    /// Normalized text with caching to avoid repeated normalization
    fn cached_normalize(&self, text: &str) -> String {
        // Fast path for empty strings
        if text.is_empty() {
            return String::new();
        }

        // Check cache first
        let mut cache = self.normalization_cache.lock().unwrap();

        if let Some(normalized) = cache.get(text) {
            return normalized.clone();
        }

        // Not in cache, perform normalization
        let normalized = normalize_string(text);

        // Only cache if the cache isn't too large to prevent memory leaks
        if cache.len() < 100_000 {
            cache.insert(text.to_string(), normalized.clone());
        }

        normalized
    }

    /// Process a stream of XML data with optimized buffer management
    fn process_xml_stream<R: Read>(
        &self,
        reader: R,
        source_name: &str,
    ) -> Result<Vec<PubMedRecord>> {
        // Pre-allocate based on typical record counts
        let mut records = Vec::with_capacity(5000);

        // Create optimized buffer reader
        let mut xml_reader =
            Reader::from_reader(BufReader::with_capacity(FILE_BUFFER_SIZE, reader));
        xml_reader.trim_text(true);

        // Configure parser for better performance
        xml_reader.expand_empty_elements(true);
        xml_reader.check_end_names(false);

        // Use a pre-allocated buffer
        let mut buffer = Vec::with_capacity(XML_BUFFER_SIZE);
        let mut article_count = 0;

        // Current article data
        let mut pmid = String::with_capacity(10);
        let mut title = String::with_capacity(200);
        let mut volume = String::with_capacity(10);
        let mut issue = String::with_capacity(10);
        let mut year = String::with_capacity(4);
        let mut journal = String::with_capacity(100);
        let mut abstract_text = String::with_capacity(2000);
        let mut authors = Vec::with_capacity(10);
        let mut mesh_headings = Vec::with_capacity(20);

        // Parsing state
        let mut in_pmid = false;
        let mut in_article_title = false;
        let mut in_volume = false;
        let mut in_issue = false;
        let mut in_year = false;
        let mut in_journal_title = false;
        let mut in_abstract_text = false;
        let mut in_last_name = false;
        let mut in_fore_name = false;
        let mut in_mesh_heading = false;
        let mut current_last_name = String::with_capacity(30);
        let mut current_fore_name = String::with_capacity(30);

        loop {
            match xml_reader.read_event_into(&mut buffer) {
                Ok(Event::Start(ref e)) => {
                    match e.name().as_ref() {
                        b"PMID" => in_pmid = true,
                        b"ArticleTitle" => in_article_title = true,
                        b"Volume" => in_volume = true,
                        b"Issue" => in_issue = true,
                        b"Year" => in_year = true,
                        b"Title" => in_journal_title = true,
                        b"AbstractText" => in_abstract_text = true,
                        b"LastName" => in_last_name = true,
                        b"ForeName" => in_fore_name = true,
                        b"DescriptorName" => in_mesh_heading = true,
                        b"PubmedArticle" => {
                            // Reset for new article
                            pmid.clear();
                            title.clear();
                            volume.clear();
                            issue.clear();
                            year.clear();
                            journal.clear();
                            abstract_text.clear();
                            authors.clear();
                            mesh_headings.clear();
                            article_count += 1;
                        }
                        _ => {}
                    }
                }
                Ok(Event::End(ref e)) => {
                    match e.name().as_ref() {
                        b"PMID" => in_pmid = false,
                        b"ArticleTitle" => in_article_title = false,
                        b"Volume" => in_volume = false,
                        b"Issue" => in_issue = false,
                        b"Year" => in_year = false,
                        b"Title" => in_journal_title = false,
                        b"AbstractText" => in_abstract_text = false,
                        b"LastName" => in_last_name = false,
                        b"ForeName" => in_fore_name = false,
                        b"DescriptorName" => in_mesh_heading = false,
                        b"Author" => {
                            // Add author if we have either last name or forename
                            if !current_last_name.is_empty() || !current_fore_name.is_empty() {
                                let author = format!(
                                    "{} {}",
                                    current_fore_name.trim(),
                                    current_last_name.trim()
                                )
                                .trim()
                                .to_string();

                                if !author.is_empty() {
                                    authors.push(author);
                                }
                            }
                            current_last_name.clear();
                            current_fore_name.clear();
                        }
                        b"PubmedArticle" => {
                            // Create record if we have at least PMID and title
                            if !pmid.is_empty() && !title.is_empty() {
                                let authors_str = authors.join("; ");

                                // Use cached normalization
                                let title_norm = self.cached_normalize(&title);
                                let authors_norm = self.cached_normalize(&authors_str);

                                records.push(PubMedRecord {
                                    pmid: pmid.clone(),
                                    title: title.clone(),
                                    title_norm,
                                    authors: authors_str,
                                    authors_norm,
                                    abstract_text: if abstract_text.is_empty() {
                                        None
                                    } else {
                                        Some(abstract_text.clone())
                                    },
                                    journal: if journal.is_empty() {
                                        None
                                    } else {
                                        Some(journal.clone())
                                    },
                                    volume: if volume.is_empty() {
                                        None
                                    } else {
                                        Some(volume.clone())
                                    },
                                    issue: if issue.is_empty() {
                                        None
                                    } else {
                                        Some(issue.clone())
                                    },
                                    year,
                                    mesh_headings: mesh_headings.clone(),
                                    file_source: source_name.to_string(),
                                });
                            }

                            // Reset for next article
                            pmid.clear();
                            title.clear();
                            volume.clear();
                            issue.clear();
                            year = String::new();
                            journal.clear();
                            abstract_text.clear();
                            authors.clear();
                            mesh_headings.clear();
                        }
                        _ => {}
                    }
                }
                Ok(Event::Text(e)) => {
                    let text = e.unescape().unwrap();
                    if in_pmid {
                        pmid = text.into_owned();
                    } else if in_article_title {
                        title = text.into_owned();
                    } else if in_volume {
                        volume = text.into_owned();
                    } else if in_issue {
                        issue = text.into_owned();
                    } else if in_year {
                        year = text.into_owned();
                    } else if in_journal_title {
                        journal = text.into_owned();
                    } else if in_abstract_text {
                        if !abstract_text.is_empty() {
                            abstract_text.push(' ');
                        }
                        abstract_text.push_str(&text);
                    } else if in_last_name {
                        current_last_name = text.into_owned();
                    } else if in_fore_name {
                        current_fore_name = text.into_owned();
                    } else if in_mesh_heading {
                        mesh_headings.push(text.into_owned());
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Error parsing XML at position {}: {}",
                        xml_reader.buffer_position(),
                        e
                    ));
                }
                _ => {}
            }

            // Clear buffer for reuse
            buffer.clear();
        }

        debug!(
            "Extracted {} PubMed records from {}",
            records.len(),
            source_name
        );

        Ok(records)
    }

    /// Collect all XML files from the specified folders with optimization for large directories
    fn collect_xml_files<P: AsRef<Path>>(&self, folders: &[P]) -> Result<Vec<PathBuf>> {
        let mut xml_files = Vec::new();

        for folder in folders {
            let folder_path = folder.as_ref();
            info!("Collecting XML files from {}", folder_path.display());

            // Use a streaming approach for large directories
            self.collect_xml_files_streaming(folder_path, |file_path| {
                xml_files.push(file_path);
                Ok(())
            })?;

            info!(
                "Found {} XML files in {}",
                xml_files.len(),
                folder_path.display()
            );
        }

        Ok(xml_files)
    }

    /// Stream file collection to avoid loading entire directory listing in memory
    fn collect_xml_files_streaming<P: AsRef<Path>, F>(
        &self,
        folder: P,
        mut callback: F,
    ) -> Result<usize>
    where
        F: FnMut(PathBuf) -> Result<()>,
    {
        let folder_path = folder.as_ref();
        let mut count = 0;

        for entry in fs::read_dir(folder_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file()
                && path.extension().map_or(false, |ext| ext == "gz")
                && path.to_string_lossy().to_lowercase().ends_with(".xml.gz")
            {
                callback(path)?;
                count += 1;
            }
        }

        Ok(count)
    }
}

impl Default for PubMedProcessingEngine {
    fn default() -> Self {
        Self::new(5)
    }
}
