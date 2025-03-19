use anyhow::{Context, Result};
use csv::{Reader, Writer};
use dashmap::DashMap;
use log::{debug, info, warn};
use parking_lot::RwLock;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::sync::Arc;

use crate::models::{MatchResult, NormalizedPublication, PubMedRecord, Publication};
use crate::utils::{normalize_string, time_operation};

/// Service for managing publication data
pub struct PublicationDataService {
    publications: Arc<DashMap<String, Publication>>,
    normalized_publications: Arc<DashMap<String, NormalizedPublication>>,
    title_index: Arc<DashMap<String, Vec<String>>>,
    title_word_index: Arc<DashMap<String, Vec<String>>>,
    title_author_index: Arc<DashMap<String, Vec<String>>>,
    title_year_index: Arc<DashMap<String, Vec<String>>>,
    loaded_from_path: Option<String>,
    modified: Arc<RwLock<bool>>,
}
fn safe_substring(s: &str, max_chars: usize) -> String {
    s.chars().take(max_chars).collect()
}

impl PublicationDataService {
    /// Create a new, empty publication service
    pub fn new() -> Self {
        Self {
            publications: Arc::new(DashMap::new()),
            normalized_publications: Arc::new(DashMap::new()),
            title_index: Arc::new(DashMap::new()),
            title_word_index: Arc::new(DashMap::new()),
            title_author_index: Arc::new(DashMap::new()),
            title_year_index: Arc::new(DashMap::new()),
            loaded_from_path: None,
            modified: Arc::new(RwLock::new(false)),
        }
    }

    /// Load publications from a CSV file
    pub fn load_from_csv<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        info!("Loading publications from CSV file: {:?}", path.as_ref());

        time_operation("load_publications", || -> Result<()> {
            let file = File::open(path.as_ref())
                .with_context(|| format!("Failed to open CSV file: {:?}", path.as_ref()))?;

            let mut reader = Reader::from_reader(BufReader::new(file));
            let mut count = 0;

            for result in reader.deserialize() {
                let publication: Publication =
                    result.with_context(|| format!("Failed to parse record {}", count + 1))?;

                self.add_publication(publication);
                count += 1;

                if count % 1000 == 0 {
                    debug!("Loaded {} publications", count);
                }
            }

            info!("Successfully loaded {} publications", count);
            self.loaded_from_path = Some(path.as_ref().to_string_lossy().to_string());

            Ok(())
        })?;

        // Create indexes after loading all publications
        self.create_indexes()?;

        Ok(())
    }

    /// Save publications to a CSV file
    pub fn save_to_csv<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        info!(
            "Saving {} publications to CSV file: {:?}",
            self.publications.len(),
            path.as_ref()
        );

        time_operation("save_publications", || -> Result<()> {
            let file = File::create(path.as_ref())
                .with_context(|| format!("Failed to create CSV file: {:?}", path.as_ref()))?;

            let mut writer = Writer::from_writer(BufWriter::new(file));
            let mut count = 0;

            // Convert DashMap to vector to avoid holding multiple references during iteration
            let publications: Vec<_> = self
                .publications
                .iter()
                .map(|r| r.value().clone())
                .collect();

            for publication in publications {
                writer
                    .serialize(&publication)
                    .with_context(|| format!("Failed to serialize record {}", count + 1))?;

                count += 1;

                if count % 1000 == 0 {
                    debug!("Saved {} publications", count);
                }
            }

            writer.flush()?;
            info!("Successfully saved {} publications", count);

            Ok(())
        })?;

        // Reset modified flag after successful save
        let mut modified = self.modified.write();
        *modified = false;

        Ok(())
    }

    /// Create indexes for efficient lookup
    pub fn create_indexes(&self) -> Result<()> {
        info!("Creating publication indexes");

        time_operation("create_indexes", || -> Result<()> {
            // Clear any existing indexes
            self.title_index.clear();
            self.title_word_index.clear();
            self.title_author_index.clear();
            self.title_year_index.clear();
            self.normalized_publications.clear();

            // Process each publication
            let publications: Vec<_> = self
                .publications
                .iter()
                .map(|r| r.value().clone())
                .collect();

            publications.par_iter().for_each(|publication| {
                let title_norm = normalize_string(&publication.title);

                // Create normalized publication
                let normalized = NormalizedPublication {
                    id: publication.id.clone(),
                    title: publication.title.clone(),
                    title_norm: title_norm.clone(),
                    author_norm: publication
                        .author
                        .as_ref()
                        .map_or(String::new(), |a| normalize_string(a)),
                    year: publication
                        .year
                        .as_ref()
                        .map_or(String::new(), Clone::clone),
                    volume: publication
                        .volume
                        .as_ref()
                        .map_or(String::new(), Clone::clone),
                    number: publication
                        .number
                        .as_ref()
                        .map_or(String::new(), Clone::clone),
                };

                self.normalized_publications
                    .insert(publication.id.clone(), normalized);

                // Add to title index
                self.title_index
                    .entry(title_norm.clone())
                    .or_insert_with(Vec::new)
                    .push(publication.id.clone());

                // Add to word index
                for word in title_norm.split_whitespace() {
                    if word.len() > 3 {
                        self.title_word_index
                            .entry(word.to_string())
                            .or_insert_with(Vec::new)
                            .push(publication.id.clone());
                    }
                }

                // Add to title+author index if author exists
                if let Some(author) = &publication.author {
                    let author_norm = normalize_string(author);
                    if !author_norm.is_empty() {
                        let key = format!(
                            "{}|{}",
                            safe_substring(&title_norm, 20),
                            safe_substring(&author_norm, 20)
                        );

                        self.title_author_index
                            .entry(key)
                            .or_insert_with(Vec::new)
                            .push(publication.id.clone());
                    }
                }

                // Add to title+year index if year exists
                if let Some(year) = &publication.year {
                    if !year.is_empty() {
                        let key = format!("{}|{}", safe_substring(&title_norm, 20), year);

                        self.title_year_index
                            .entry(key)
                            .or_insert_with(Vec::new)
                            .push(publication.id.clone());
                    }
                }
            });

            info!(
                "Indexes created: {} titles, {} words, {} title+author pairs, {} title+year pairs",
                self.title_index.len(),
                self.title_word_index.len(),
                self.title_author_index.len(),
                self.title_year_index.len()
            );

            Ok(())
        })
    }
    pub fn get_all_publications(&self) -> Vec<Publication> {
        self.publications
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Add multiple publications at once
    pub fn add_publications(&self, publications: &[Publication]) {
        for publication in publications {
            self.add_publication(publication.clone());
        }
    }
    /// Add a single publication to the service
    pub fn add_publication(&self, publication: Publication) {
        self.publications
            .insert(publication.id.clone(), publication);
        let mut modified = self.modified.write();
        *modified = true;
    }

    /// Get a publication by ID
    pub fn get_publication(&self, id: &str) -> Option<Publication> {
        self.publications.get(id).map(|p| p.value().clone())
    }

    /// Get a normalized publication by ID
    pub fn get_normalized_publication(&self, id: &str) -> Option<NormalizedPublication> {
        self.normalized_publications
            .get(id)
            .map(|p| p.value().clone())
    }

    /// Get publications by title (exact match)
    pub fn get_publications_by_title(&self, title: &str) -> Vec<Publication> {
        let title_norm = normalize_string(title);

        match self.title_index.get(&title_norm) {
            Some(ids) => ids
                .iter()
                .filter_map(|id| self.get_publication(id))
                .collect(),
            None => Vec::new(),
        }
    }

    /// Get publication IDs that potentially match the given title words
    pub fn get_publication_ids_by_title_words(&self, title: &str) -> HashSet<String> {
        let title_norm = normalize_string(title);
        let mut result = HashSet::new();

        // Extract significant words from title
        let words: Vec<_> = title_norm
            .split_whitespace()
            .filter(|word| word.len() > 3)
            .collect();

        // Find publications containing these words
        for word in words {
            if let Some(ids) = self.title_word_index.get(word) {
                for id in ids.iter() {
                    result.insert(id.clone());
                }
            }
        }

        result
    }

    /// Get publication IDs by title+author key
    pub fn get_publication_ids_by_title_author(&self, title: &str, author: &str) -> Vec<String> {
        let title_norm = normalize_string(title);
        let author_norm = normalize_string(author);

        let key = format!(
            "{}|{}",
            safe_substring(&title_norm, 20),
            safe_substring(&author_norm, 20)
        );

        match self.title_author_index.get(&key) {
            Some(ids) => ids.value().clone(),
            None => Vec::new(),
        }
    }

    /// Get publication IDs by title+year key
    pub fn get_publication_ids_by_title_year(&self, title: &str, year: &str) -> Vec<String> {
        let title_norm = normalize_string(title);

        let key = format!("{}|{}", safe_substring(&title_norm, 20), year);

        match self.title_year_index.get(&key) {
            Some(ids) => ids.value().clone(),
            None => Vec::new(),
        }
    }

    /// Update publications with match results and PubMed data
    pub fn apply_matches(
        &self,
        matches: &[MatchResult],
        pubmed_records: &HashMap<String, PubMedRecord>,
    ) -> Result<usize> {
        info!("Applying {} matches to publications", matches.len());

        let mut updated_count = 0;

        for match_result in matches {
            if let Some(mut publication) = self.publications.get_mut(&match_result.publication_id) {
                if let Some(pubmed_record) = pubmed_records.get(&match_result.pmid) {
                    // Update publication with PubMed data
                    let publication = publication.value_mut();
                    publication.pmid = Some(pubmed_record.pmid.clone());
                    publication.pubmed_authors = Some(pubmed_record.authors.clone());
                    publication.pubmed_abstract = pubmed_record.abstract_text.clone();
                    publication.pubmed_journal = pubmed_record.journal.clone();
                    publication.pubmed_mesh = Some(pubmed_record.mesh_headings.join("; "));
                    publication.match_quality = Some(match_result.match_quality);
                    publication.match_type = Some(match_result.match_type.clone());
                    publication.enrichment_date = Some(crate::utils::current_datetime_string());

                    updated_count += 1;
                } else {
                    warn!(
                        "Cannot update publication {}: PubMed record {} not found",
                        match_result.publication_id, match_result.pmid
                    );
                }
            } else {
                warn!(
                    "Cannot update publication {}: not found in database",
                    match_result.publication_id
                );
            }
        }

        // Set modified flag if any publications were updated
        if updated_count > 0 {
            let mut modified = self.modified.write();
            *modified = true;
        }

        info!("Updated {} publications with PubMed data", updated_count);

        Ok(updated_count)
    }

    /// Get the number of publications
    pub fn len(&self) -> usize {
        self.publications.len()
    }

    /// Check if the publications list is empty
    pub fn is_empty(&self) -> bool {
        self.publications.is_empty()
    }

    /// Check if the publication data has been modified since loading
    pub fn is_modified(&self) -> bool {
        *self.modified.read()
    }

    /// Get all publication IDs
    pub fn get_all_ids(&self) -> Vec<String> {
        self.publications.iter().map(|r| r.key().clone()).collect()
    }
}

impl Default for PublicationDataService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn create_test_publication(
        id: &str,
        title: &str,
        author: Option<&str>,
        year: Option<&str>,
    ) -> Publication {
        Publication {
            id: id.to_string(),
            title: title.to_string(),
            person_id: None,
            author: author.map(|s| s.to_string()),
            publication_date: None,
            year: year.map(|s| s.to_string()),
            doc_type_id: None,
            doc_type: None,
            volume: None,
            number: None,
            pages: None,
            pmid: None,
            pubmed_authors: None,
            pubmed_abstract: None,
            pubmed_journal: None,
            pubmed_mesh: None,
            match_quality: None,
            match_type: None,
            enrichment_date: None,
        }
    }

    #[test]
    fn test_add_and_get_publication() {
        let service = PublicationDataService::new();
        let pub1 = create_test_publication("1", "Test Title", Some("Author"), Some("2020"));

        service.add_publication(pub1.clone());

        let retrieved = service.get_publication("1").unwrap();
        assert_eq!(retrieved.id, "1");
        assert_eq!(retrieved.title, "Test Title");
    }

    #[test]
    fn test_create_indexes() {
        let service = PublicationDataService::new();

        service.add_publication(create_test_publication(
            "1",
            "Machine Learning Applications",
            Some("Smith J"),
            Some("2020"),
        ));
        service.add_publication(create_test_publication(
            "2",
            "Deep Learning",
            Some("Johnson A"),
            Some("2021"),
        ));
        service.add_publication(create_test_publication(
            "3",
            "Machine Learning Methods",
            Some("Brown B"),
            Some("2019"),
        ));

        service.create_indexes().unwrap();

        // Test title word index
        let machine_ids = service.get_publication_ids_by_title_words("machine");
        assert_eq!(machine_ids.len(), 2);
        assert!(machine_ids.contains("1"));
        assert!(machine_ids.contains("3"));

        // Test title+author index
        let smith_ids =
            service.get_publication_ids_by_title_author("Machine Learning Applications", "Smith J");
        assert_eq!(smith_ids.len(), 1);
        assert_eq!(smith_ids[0], "1");

        // Test title+year index
        let year_ids = service.get_publication_ids_by_title_year("Deep Learning", "2021");
        assert_eq!(year_ids.len(), 1);
        assert_eq!(year_ids[0], "2");
    }

    #[test]
    fn test_save_and_load_csv() -> Result<()> {
        let mut service = PublicationDataService::new();

        service.add_publication(create_test_publication(
            "1",
            "Title 1",
            Some("Author 1"),
            Some("2020"),
        ));
        service.add_publication(create_test_publication(
            "2",
            "Title 2",
            Some("Author 2"),
            Some("2021"),
        ));

        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        service.save_to_csv(path)?;

        let mut loaded_service = PublicationDataService::new();
        loaded_service.load_from_csv(path)?;

        assert_eq!(loaded_service.len(), 2);
        assert!(loaded_service.get_publication("1").is_some());
        assert!(loaded_service.get_publication("2").is_some());

        Ok(())
    }
}
