use anyhow::Result;
use chrono::Utc;
use log::{debug, info, warn};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use strsim::jaro_winkler;

use crate::models::{
    IntegrationConfig, MatchResult, MatchStrategy, MatchingStats, PubMedRecord, Publication,
};
use crate::publication_service::PublicationDataService;
use crate::utils::{normalize_string, time_operation};

/// Service for matching publications with PubMed records
pub struct MatchingService {
    config: IntegrationConfig,
    stats: Arc<Mutex<MatchingStats>>,
}

impl MatchingService {
    /// Create a new matching service with default configuration
    pub fn new() -> Self {
        Self {
            config: IntegrationConfig::default(),
            stats: Arc::new(Mutex::new(MatchingStats::new())),
        }
    }

    /// Create a new matching service with custom configuration
    pub fn with_config(config: IntegrationConfig) -> Self {
        Self {
            config,
            stats: Arc::new(Mutex::new(MatchingStats::new())),
        }
    }

    /// Find matches between publications and PubMed records
    pub fn find_matches(
        &self,
        publication_service: &PublicationDataService,
        pubmed_records: &HashMap<String, PubMedRecord>,
    ) -> Result<(Vec<MatchResult>, MatchingStats)> {
        info!(
            "Finding matches between {} publications and {} PubMed records",
            publication_service.len(),
            pubmed_records.len()
        );

        // Reset stats
        {
            let mut stats = self.stats.lock().unwrap();
            *stats = MatchingStats::new();
            stats.total_publications = publication_service.len();
            stats.total_pubmed_records = pubmed_records.len();
        }

        let all_matches = time_operation("find_matches", || -> Result<Vec<MatchResult>> {
            let publication_ids = publication_service.get_all_ids();
            let matches = Arc::new(Mutex::new(Vec::new()));

            // Process publications in parallel
            publication_ids.par_iter().for_each(|pub_id| {
                if let Some(publication) = publication_service.get_publication(pub_id) {
                    // Skip if already has a PMID
                    if publication.pmid.is_some() {
                        debug!("Skipping publication {} as it already has a PMID", pub_id);
                        return;
                    }

                    // Try to match this publication
                    match self.match_publication(&publication, publication_service, pubmed_records)
                    {
                        Ok(pub_matches) => {
                            if !pub_matches.is_empty() {
                                // Add matches and update statistics
                                let mut matches_guard = matches.lock().unwrap();
                                let mut stats_guard = self.stats.lock().unwrap();

                                for m in pub_matches {
                                    stats_guard.add_match(&m);
                                    matches_guard.push(m);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Error matching publication {}: {}", pub_id, e);
                        }
                    }
                }
            });

            // Extract final matches
            let matches_mutex = Arc::try_unwrap(matches).map_err(|arc| arc).unwrap();
            let final_matches = matches_mutex.lock().unwrap().clone();

            Ok(final_matches)
        })?;

        // Get the final statistics
        let stats = self.stats.lock().unwrap().clone();

        info!(
            "Matching complete: found {} matches for {} publications ({:.2}% matched)",
            all_matches.len(),
            stats.matched_publications,
            (stats.matched_publications as f64 / stats.total_publications as f64) * 100.0
        );

        // Log match type statistics
        for (match_type, count) in &stats.matches_by_type {
            info!(
                "  - Match type '{}': {} matches ({:.2}%)",
                match_type,
                count,
                (*count as f64 / all_matches.len() as f64) * 100.0
            );
        }

        Ok((all_matches, stats))
    }

    /// Match a single publication with PubMed records
    fn match_publication(
        &self,
        publication: &Publication,
        publication_service: &PublicationDataService,
        pubmed_records: &HashMap<String, PubMedRecord>,
    ) -> Result<Vec<MatchResult>> {
        let mut matches = Vec::new();

        // Apply different matching strategies in order of confidence
        if self
            .config
            .enabled_strategies
            .contains(&MatchStrategy::ExactTitle)
        {
            matches.extend(self.match_by_exact_title(publication, pubmed_records)?);
        }

        if matches.len() < self.config.max_matches_per_publication
            && self
                .config
                .enabled_strategies
                .contains(&MatchStrategy::TitleAuthor)
        {
            matches.extend(self.match_by_title_author(
                publication,
                publication_service,
                pubmed_records,
            )?);
        }

        if matches.len() < self.config.max_matches_per_publication
            && self
                .config
                .enabled_strategies
                .contains(&MatchStrategy::TitleYear)
        {
            matches.extend(self.match_by_title_year(
                publication,
                publication_service,
                pubmed_records,
            )?);
        }

        if matches.len() < self.config.max_matches_per_publication
            && self
                .config
                .enabled_strategies
                .contains(&MatchStrategy::FuzzyTitle)
        {
            matches.extend(self.match_by_fuzzy_title(
                publication,
                publication_service,
                pubmed_records,
            )?);
        }

        // Sort matches by quality and limit to max allowed per publication
        matches.sort_by(|a, b| b.match_quality.partial_cmp(&a.match_quality).unwrap());
        matches.truncate(self.config.max_matches_per_publication);

        Ok(matches)
    }

    /// Match by exact title
    fn match_by_exact_title(
        &self,
        publication: &Publication,
        pubmed_records: &HashMap<String, PubMedRecord>,
    ) -> Result<Vec<MatchResult>> {
        let mut matches = Vec::new();
        let title_norm = normalize_string(&publication.title);

        for (pmid, record) in pubmed_records {
            if record.title_norm == title_norm {
                matches.push(MatchResult {
                    publication_id: publication.id.clone(),
                    pmid: pmid.clone(),
                    match_quality: 100.0,
                    match_type: MatchStrategy::ExactTitle.to_string(),
                    timestamp: Utc::now(),
                });

                // Early return if we've reached the limit
                if matches.len() >= self.config.max_matches_per_publication {
                    break;
                }
            }
        }

        Ok(matches)
    }

    /// Match by title and author
    fn match_by_title_author(
        &self,
        publication: &Publication,
        publication_service: &PublicationDataService,
        pubmed_records: &HashMap<String, PubMedRecord>,
    ) -> Result<Vec<MatchResult>> {
        let mut matches = Vec::new();

        // Skip if publication has no author
        let author = match &publication.author {
            Some(a) if !a.is_empty() => a,
            _ => return Ok(matches),
        };

        // Get candidate PubMed records by title
        let title_candidates =
            publication_service.get_publication_ids_by_title_words(&publication.title);
        let title_norm = normalize_string(&publication.title);
        let author_norm = normalize_string(author);

        // Match with PubMed records
        for (pmid, record) in pubmed_records {
            // Rough title similarity check
            let title_similarity = jaro_winkler(&title_norm, &record.title_norm);
            if title_similarity < 0.8 {
                continue;
            }

            // Check for author match
            let author_similarity = jaro_winkler(&author_norm, &record.authors_norm);
            if author_similarity >= 0.7 {
                // Combined score: 70% title, 30% author
                let combined_score = (title_similarity * 0.7 + author_similarity * 0.3) * 100.0;

                if combined_score >= self.config.min_match_quality {
                    matches.push(MatchResult {
                        publication_id: publication.id.clone(),
                        pmid: pmid.clone(),
                        match_quality: combined_score,
                        match_type: MatchStrategy::TitleAuthor.to_string(),
                        timestamp: Utc::now(),
                    });

                    // Early return if we've reached the limit
                    if matches.len() >= self.config.max_matches_per_publication {
                        break;
                    }
                }
            }
        }

        Ok(matches)
    }

    /// Match by title and year
    fn match_by_title_year(
        &self,
        publication: &Publication,
        publication_service: &PublicationDataService,
        pubmed_records: &HashMap<String, PubMedRecord>,
    ) -> Result<Vec<MatchResult>> {
        let mut matches = Vec::new();

        // Skip if publication has no year
        let year = match &publication.year {
            Some(y) if !y.is_empty() => y,
            _ => return Ok(matches),
        };

        let title_norm = normalize_string(&publication.title);

        // Check PubMed records for title+year match
        for (pmid, record) in pubmed_records {
            if record.year == *year {
                let title_similarity = jaro_winkler(&title_norm, &record.title_norm);

                if title_similarity >= 0.85 {
                    let score = title_similarity * 90.0; // Max 90% score for title+year

                    if score >= self.config.min_match_quality {
                        matches.push(MatchResult {
                            publication_id: publication.id.clone(),
                            pmid: pmid.clone(),
                            match_quality: score,
                            match_type: MatchStrategy::TitleYear.to_string(),
                            timestamp: Utc::now(),
                        });

                        // Early return if we've reached the limit
                        if matches.len() >= self.config.max_matches_per_publication {
                            break;
                        }
                    }
                }
            }
        }

        Ok(matches)
    }

    /// Match by fuzzy title
    fn match_by_fuzzy_title(
        &self,
        publication: &Publication,
        publication_service: &PublicationDataService,
        pubmed_records: &HashMap<String, PubMedRecord>,
    ) -> Result<Vec<MatchResult>> {
        let mut matches = Vec::new();
        let title_norm = normalize_string(&publication.title);

        // Get candidate PMIDs based on title words
        let mut candidates = HashSet::new();
        let title_words: Vec<_> = title_norm
            .split_whitespace()
            .filter(|word| word.len() > 3)
            .collect();

        // Find publications containing at least 2 significant words from the title
        for (pmid, record) in pubmed_records {
            let mut word_matches = 0;

            for word in &title_words {
                if record.title_norm.contains(word) {
                    word_matches += 1;
                    if word_matches >= 2 {
                        candidates.insert(pmid.clone());
                        break;
                    }
                }
            }
        }

        // Calculate similarity scores for candidates
        for pmid in candidates {
            if let Some(record) = pubmed_records.get(&pmid) {
                let similarity = jaro_winkler(&title_norm, &record.title_norm);
                let score = similarity * 85.0; // Max 85% score for fuzzy title match

                if score >= self.config.fuzzy_match_threshold {
                    matches.push(MatchResult {
                        publication_id: publication.id.clone(),
                        pmid: pmid.clone(),
                        match_quality: score,
                        match_type: MatchStrategy::FuzzyTitle.to_string(),
                        timestamp: Utc::now(),
                    });

                    // Early return if we've reached the limit
                    if matches.len() >= self.config.max_matches_per_publication {
                        break;
                    }
                }
            }
        }

        Ok(matches)
    }

    /// Get the current matching statistics
    pub fn get_stats(&self) -> MatchingStats {
        self.stats.lock().unwrap().clone()
    }
}

impl Default for MatchingService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Publication;

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

    fn create_test_pubmed_record(
        pmid: &str,
        title: &str,
        authors: &str,
        year: &str,
    ) -> PubMedRecord {
        PubMedRecord {
            pmid: pmid.to_string(),
            title: title.to_string(),
            title_norm: normalize_string(title),
            authors: authors.to_string(),
            authors_norm: normalize_string(authors),
            abstract_text: None,
            journal: None,
            volume: None,
            issue: None,
            year: year.to_string(),
            mesh_headings: Vec::new(),
            file_source: "test.xml.gz".to_string(),
        }
    }

    #[test]
    fn test_exact_title_match() {
        let service = MatchingService::new();
        let publication = create_test_publication(
            "pub1",
            "Machine Learning Methods",
            Some("Smith J"),
            Some("2020"),
        );

        let mut pubmed_records = HashMap::new();
        pubmed_records.insert(
            "pm1".to_string(),
            create_test_pubmed_record(
                "pm1",
                "Machine Learning Methods",
                "Smith J; Brown T",
                "2020",
            ),
        );
        pubmed_records.insert(
            "pm2".to_string(),
            create_test_pubmed_record("pm2", "Deep Learning Applications", "Johnson A", "2021"),
        );

        let matches = service
            .match_by_exact_title(&publication, &pubmed_records)
            .unwrap();

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pmid, "pm1");
        assert_eq!(matches[0].match_quality, 100.0);
        assert_eq!(matches[0].match_type, "exact_title");
    }

    #[test]
    fn test_title_author_match() {
        let service = MatchingService::new();
        let publication_service = PublicationDataService::new();
        let publication = create_test_publication(
            "pub1",
            "Machine Learning Approaches",
            Some("Smith John"),
            Some("2020"),
        );

        let mut pubmed_records = HashMap::new();
        pubmed_records.insert(
            "pm1".to_string(),
            create_test_pubmed_record(
                "pm1",
                "Machine Learning Methods",
                "Smith J; Brown T",
                "2020",
            ),
        );
        pubmed_records.insert(
            "pm2".to_string(),
            create_test_pubmed_record(
                "pm2",
                "Machine Learning Approaches",
                "Smith John; Johnson A",
                "2020",
            ),
        );

        let matches = service
            .match_by_title_author(&publication, &publication_service, &pubmed_records)
            .unwrap();

        assert!(!matches.is_empty());
        let perfect_match = matches.iter().find(|m| m.pmid == "pm2");
        assert!(perfect_match.is_some());

        let perfect_match = perfect_match.unwrap();
        assert!(perfect_match.match_quality > 90.0);
        assert_eq!(perfect_match.match_type, "title_author");
    }

    #[test]
    fn test_fuzzy_title_match() {
        let service = MatchingService::new();
        let publication_service = PublicationDataService::new();
        let publication = create_test_publication(
            "pub1",
            "Introduction to Machine Learning Algorithms",
            Some("Smith J"),
            Some("2020"),
        );

        let mut pubmed_records = HashMap::new();
        pubmed_records.insert(
            "pm1".to_string(),
            create_test_pubmed_record(
                "pm1",
                "Introduction to Machine Learning Methods",
                "Brown T",
                "2020",
            ),
        );
        pubmed_records.insert(
            "pm2".to_string(),
            create_test_pubmed_record("pm2", "Deep Learning Applications", "Johnson A", "2021"),
        );

        let matches = service
            .match_by_fuzzy_title(&publication, &publication_service, &pubmed_records)
            .unwrap();

        assert!(!matches.is_empty());
        assert_eq!(matches[0].pmid, "pm1");
        assert!(matches[0].match_quality > 80.0);
        assert_eq!(matches[0].match_type, "fuzzy_title");
    }
}
