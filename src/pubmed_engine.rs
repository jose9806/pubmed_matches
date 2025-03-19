use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use indicatif::ProgressBar;
use log::{debug, error, info, warn};
use quick_xml::events::Event;
use quick_xml::Reader;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::models::PubMedRecord;
use crate::utils::{get_filename, normalize_string, time_operation};

/// Engine for processing PubMed XML files
#[derive(Clone)]
pub struct PubMedProcessingEngine {
    batch_size: usize,
    max_retries: usize,
    progress_bar: Option<ProgressBar>,
}

impl PubMedProcessingEngine {
    /// Create a new PubMed processing engine
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            max_retries: 3,
            progress_bar: None,
        }
    }

    /// Set a progress bar for tracking file processing
    pub fn with_progress_bar(mut self, progress_bar: ProgressBar) -> Self {
        self.progress_bar = Some(progress_bar);
        self
    }

    /// Process all XML files in the specified folders
    pub fn process_folders<P: AsRef<Path>>(
        &self,
        folders: &[P],
    ) -> Result<HashMap<String, PubMedRecord>> {
        info!("Processing PubMed files from {} folders", folders.len());

        let xml_files = self.collect_xml_files(folders)?;
        info!("Found {} XML files to process", xml_files.len());

        if let Some(pb) = &self.progress_bar {
            pb.set_length(xml_files.len() as u64);
            pb.set_message("Processing XML files...");
        }

        // Process files in batches
        let all_records = Arc::new(Mutex::new(HashMap::new()));
        let processed_files = Arc::new(Mutex::new(HashSet::new()));

        for (batch_idx, batch) in xml_files.chunks(self.batch_size).enumerate() {
            info!(
                "Processing batch {}/{} with {} files",
                batch_idx + 1,
                (xml_files.len() + self.batch_size - 1) / self.batch_size,
                batch.len()
            );

            // Process batch in parallel
            let batch_results: Vec<_> = batch
                .par_iter()
                .map(|file_path| {
                    let filename = get_filename(file_path);

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

                            Some((filename, records))
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

            // Merge batch results into all_records
            let mut all_records_guard = all_records.lock().unwrap();
            let mut processed_guard = processed_files.lock().unwrap();

            for result in batch_results.into_iter().flatten() {
                let (filename, records) = result;

                for record in records {
                    all_records_guard.insert(record.pmid.clone(), record);
                }

                processed_guard.insert(filename);
            }

            info!(
                "Batch {}/{} complete. Total records so far: {}",
                batch_idx + 1,
                (xml_files.len() + self.batch_size - 1) / self.batch_size,
                all_records_guard.len()
            );
        }

        // No need to unwrap the Arc, we can use it directly
        if let Some(pb) = &self.progress_bar {
            pb.finish_with_message(format!(
                "Processed all files. Found {} records",
                all_records.lock().unwrap().len()
            ));
        }

        info!(
            "Completed processing {} XML files. Found {} total PubMed records",
            xml_files.len(),
            all_records.lock().unwrap().len()
        );

        // Extract the HashMap from the Mutex before returning
        let all_records = all_records.lock().unwrap().clone();
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

            let gz_decoder = GzDecoder::new(BufReader::new(file));

            self.process_xml_stream(gz_decoder, &filename)
        })
    }

    /// Process a stream of XML data
    fn process_xml_stream<R: Read>(
        &self,
        reader: R,
        source_name: &str,
    ) -> Result<Vec<PubMedRecord>> {
        let mut records = Vec::new();
        let mut xml_reader = Reader::from_reader(BufReader::new(reader));
        xml_reader.trim_text(true);

        let mut buf = Vec::new();
        let mut article_count = 0;

        // Current article data
        let mut pmid = String::new();
        let mut title = String::new();
        let mut volume = String::new();
        let mut issue = String::new();
        let mut year = String::new();
        let mut journal = String::new();
        let mut abstract_text = String::new();
        let mut authors = Vec::new();
        let mut mesh_headings = Vec::new();

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
        let mut current_last_name = String::new();
        let mut current_fore_name = String::new();

        loop {
            match xml_reader.read_event_into(&mut buf) {
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

                                records.push(PubMedRecord {
                                    pmid,
                                    title: title.clone(),
                                    title_norm: normalize_string(&title),
                                    authors: authors_str.clone(),
                                    authors_norm: normalize_string(&authors_str),
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
                            pmid = String::new();
                            title = String::new();
                            volume = String::new();
                            issue = String::new();
                            year = String::new();
                            journal = String::new();
                            abstract_text = String::new();
                            authors = Vec::new();
                            mesh_headings = Vec::new();
                        }
                        _ => {}
                    }
                }
                Ok(Event::Text(e)) => {
                    let text = e.unescape().unwrap().into_owned();
                    if in_pmid {
                        pmid = text;
                    } else if in_article_title {
                        title = text;
                    } else if in_volume {
                        volume = text;
                    } else if in_issue {
                        issue = text;
                    } else if in_year {
                        year = text;
                    } else if in_journal_title {
                        journal = text;
                    } else if in_abstract_text {
                        if !abstract_text.is_empty() {
                            abstract_text.push(' ');
                        }
                        abstract_text.push_str(&text);
                    } else if in_last_name {
                        current_last_name = text;
                    } else if in_fore_name {
                        current_fore_name = text;
                    } else if in_mesh_heading {
                        mesh_headings.push(text);
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

            buf.clear();
        }

        debug!(
            "Extracted {} PubMed records from {}",
            records.len(),
            source_name
        );

        Ok(records)
    }

    /// Collect all XML files from the specified folders
    fn collect_xml_files<P: AsRef<Path>>(&self, folders: &[P]) -> Result<Vec<PathBuf>> {
        let mut xml_files = Vec::new();

        for folder in folders {
            let folder_path = folder.as_ref();
            info!("Collecting XML files from {}", folder_path.display());

            let entries = fs::read_dir(folder_path)
                .with_context(|| format!("Failed to read directory: {}", folder_path.display()))?;

            let folder_files: Vec<PathBuf> = entries
                .filter_map(Result::ok)
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .to_lowercase()
                        .ends_with(".xml.gz")
                })
                .map(|entry| entry.path())
                .collect();

            info!(
                "Found {} XML files in {}",
                folder_files.len(),
                folder_path.display()
            );
            xml_files.extend(folder_files);
        }

        Ok(xml_files)
    }
}

impl Default for PubMedProcessingEngine {
    fn default() -> Self {
        Self::new(5)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_xml() -> Result<NamedTempFile> {
        let xml_content = r#"
        <!DOCTYPE PubmedArticleSet PUBLIC "-//NLM//DTD PubMedArticle, 1st January 2019//EN" "https://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_190101.dtd">
        <PubmedArticleSet>
        <PubmedArticle>
            <MedlineCitation Status="MEDLINE" Owner="NLM">
                <PMID Version="1">12345678</PMID>
                <Article PubModel="Print">
                    <Journal>
                        <Title>Test Journal</Title>
                        <JournalIssue CitedMedium="Internet">
                            <Volume>10</Volume>
                            <Issue>2</Issue>
                            <PubDate>
                                <Year>2020</Year>
                            </PubDate>
                        </JournalIssue>
                    </Journal>
                    <ArticleTitle>Test Article Title</ArticleTitle>
                    <Abstract>
                        <AbstractText>This is a test abstract.</AbstractText>
                    </Abstract>
                    <AuthorList CompleteYN="Y">
                        <Author ValidYN="Y">
                            <LastName>Smith</LastName>
                            <ForeName>John</ForeName>
                        </Author>
                        <Author ValidYN="Y">
                            <LastName>Doe</LastName>
                            <ForeName>Jane</ForeName>
                        </Author>
                    </AuthorList>
                </Article>
                <MeshHeadingList>
                    <MeshHeading>
                        <DescriptorName UI="D000001">Test Category 1</DescriptorName>
                    </MeshHeading>
                    <MeshHeading>
                        <DescriptorName UI="D000002">Test Category 2</DescriptorName>
                    </MeshHeading>
                </MeshHeadingList>
            </MedlineCitation>
        </PubmedArticle>
        </PubmedArticleSet>
        "#;

        // Create a temporary file with .xml.gz extension
        let mut temp_file = tempfile::Builder::new().suffix(".xml.gz").tempfile()?;

        // Write the gzipped XML content to the file
        let mut encoder = GzEncoder::new(temp_file.as_file_mut(), Compression::default());
        encoder.write_all(xml_content.as_bytes())?;
        encoder.finish()?;

        Ok(temp_file)
    }

    #[test]
    fn test_process_file() -> Result<()> {
        let engine = PubMedProcessingEngine::default();
        let test_file = create_test_xml()?;

        let records = engine.process_file(test_file.path())?;

        assert_eq!(records.len(), 1);

        let record = &records[0];
        assert_eq!(record.pmid, "12345678");
        assert_eq!(record.title, "Test Article Title");
        assert_eq!(record.authors, "John Smith; Jane Doe");
        assert_eq!(record.year, "2020");
        assert_eq!(record.volume, Some("10".to_string()));
        assert_eq!(record.issue, Some("2".to_string()));
        assert_eq!(record.journal, Some("Test Journal".to_string()));
        assert_eq!(
            record.abstract_text,
            Some("This is a test abstract.".to_string())
        );
        assert_eq!(
            record.mesh_headings,
            vec!["Test Category 1", "Test Category 2"]
        );

        Ok(())
    }
}
