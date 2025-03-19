use anyhow::Result;
use chrono::Utc;
use log::{debug, info, warn};
use rayon::prelude::*;
use std::collections::{hash_map::DefaultHasher, HashMap, HashSet};
use std::hash::Hasher;
use std::sync::{Arc, Mutex, RwLock};
use strsim::jaro_winkler;

use crate::models::{
    IntegrationConfig, MatchResult, MatchStrategy, MatchingStats, PubMedRecord, Publication,
};
use crate::publication_service::PublicationDataService;
use crate::utils::{normalize_string, time_operation};

// Size of match candidate pools
const MAX_CANDIDATE_POOL_SIZE: usize = 200;
const MIN_TITLE_WORD_LENGTH: usize = 4;
const COMMON_WORDS: [&str; 20] = [
    "about", "after", "again", "along", "could", "every", "first", "found", "great", "house",
    "large", "learn", "never", "other", "place", "plant", "point", "right", "small", "study",
];

fn create_ngrams(text: &str, n: usize) -> HashSet<String> {
    let mut ngrams = HashSet::new();
    let chars: Vec<char> = text.chars().collect();

    for i in 0..=(chars.len().saturating_sub(n)) {
        let ngram: String = chars[i..i + n].iter().collect();
        ngrams.insert(ngram);
    }

    ngrams
}

fn ngram_similarity(text1: &str, text2: &str, n: usize) -> f64 {
    let ngrams1 = create_ngrams(text1, n);
    let ngrams2 = create_ngrams(text2, n);

    let intersection: HashSet<_> = ngrams1.intersection(&ngrams2).collect();
    let union_size = ngrams1.len() + ngrams2.len() - intersection.len();

    if union_size == 0 {
        return 0.0;
    }

    intersection.len() as f64 / union_size as f64
}
/// Cache structures for matching optimization
struct MatchingCache {
    // Cache for normalized strings
    normalized_strings: RwLock<HashMap<String, String>>,

    // Cache for title word indices
    pubmed_word_index: RwLock<Option<HashMap<String, Vec<String>>>>,

    // Cache for similarity scores
    similarity_scores: RwLock<HashMap<(String, String), f64>>,

    // Set of common words to filter out
    common_words: HashSet<String>,
}
struct LshMatcher {
    hash_tables: Vec<HashMap<u64, Vec<String>>>,
    hash_functions: Vec<Box<dyn for<'a> Fn(&'a str) -> u64 + 'static>>,
}

impl LshMatcher {
    fn new(num_tables: usize, num_functions: usize) -> Self {
        let mut hash_functions = Vec::with_capacity(num_functions);

        // Create random hash functions
        for i in 0..num_functions {
            let seed = i as u64;
            hash_functions.push(Box::new(
                move |text: &str| {
                    let mut hasher = DefaultHasher::new();
                    hasher.write_u64(seed);
                    hasher.write(text.as_bytes());
                    hasher.finish()
                }
            ) as Box<dyn for<'a> Fn(&'a str) -> u64 + 'static>);
        }

        Self {
            hash_tables: vec![HashMap::new(); num_tables],
            hash_functions,
        }
    }

    fn index_pubmed_records(&mut self, records: &HashMap<String, PubMedRecord>) {
        for (pmid, record) in records {
            for (table_idx, table) in self.hash_tables.iter_mut().enumerate() {
                let hash_fn = &self.hash_functions[table_idx % self.hash_functions.len()];
                let hash = hash_fn(&record.title_norm);
                table
                    .entry(hash)
                    .or_insert_with(Vec::new)
                    .push(pmid.clone());
            }
        }
    }

    fn find_candidates(&self, title: &str) -> HashSet<String> {
        let mut candidates = HashSet::new();

        for (table_idx, table) in self.hash_tables.iter().enumerate() {
            let hash_fn = &self.hash_functions[table_idx % self.hash_functions.len()];
            let hash = hash_fn(title);

            if let Some(pmids) = table.get(&hash) {
                for pmid in pmids {
                    candidates.insert(pmid.clone());
                }
            }
        }

        candidates
    }
}

impl MatchingCache {
    fn new() -> Self {
        let mut common_words = HashSet::new();
        for word in &COMMON_WORDS {
            common_words.insert(word.to_string());
        }

        Self {
            normalized_strings: RwLock::new(HashMap::with_capacity(10_000)),
            pubmed_word_index: RwLock::new(None),
            similarity_scores: RwLock::new(HashMap::with_capacity(10_000)),
            common_words,
        }
    }

    fn get_normalized(&self, text: &str) -> String {
        // Fast path for empty strings
        if text.is_empty() {
            return String::new();
        }

        // Check read lock first for better concurrency
        {
            let cache = self.normalized_strings.read().unwrap();
            if let Some(normalized) = cache.get(text) {
                return normalized.clone();
            }
        }

        // Not found, normalize and update cache
        let normalized = normalize_string(text);

        let mut cache = self.normalized_strings.write().unwrap();
        // Only cache if we have reasonable space
        if cache.len() < 100_000 {
            cache.insert(text.to_string(), normalized.clone());
        }

        normalized
    }

    fn get_similarity(&self, s1: &str, s2: &str) -> f64 {
        let key = if s1 < s2 {
            (s1.to_string(), s2.to_string())
        } else {
            (s2.to_string(), s1.to_string())
        };

        // Check read lock first
        {
            let cache = self.similarity_scores.read().unwrap();
            if let Some(score) = cache.get(&key) {
                return *score;
            }
        }

        // Calculate and store
        let score = jaro_winkler(s1, s2);

        let mut cache = self.similarity_scores.write().unwrap();
        // Only cache if we have reasonable space
        if cache.len() < 100_000 {
            cache.insert(key, score);
        }

        score
    }

    fn build_pubmed_word_index(&self, pubmed_records: &HashMap<String, PubMedRecord>) {
        let mut pubmed_index = self.pubmed_word_index.write().unwrap();

        if pubmed_index.is_none() {
            info!("Building PubMed title word index for optimized matching...");
            let start = std::time::Instant::now();

            // Build word index
            let mut index: HashMap<String, Vec<String>> = HashMap::new();
            let mut word_counts: HashMap<String, usize> = HashMap::new();

            // First pass: count word frequencies across all titles
            for (pmid, record) in pubmed_records {
                let words = record
                    .title_norm
                    .split_whitespace()
                    .filter(|w| w.len() >= MIN_TITLE_WORD_LENGTH && !self.common_words.contains(*w))
                    .collect::<Vec<_>>();

                for word in words {
                    *word_counts.entry(word.to_string()).or_insert(0) += 1;
                }
            }

            // Second pass: build index with words that aren't too common
            for (pmid, record) in pubmed_records {
                let words = record
                    .title_norm
                    .split_whitespace()
                    .filter(|w| {
                        w.len() >= MIN_TITLE_WORD_LENGTH
                            && !self.common_words.contains(*w)
                            && *word_counts.get(*w).unwrap_or(&0) < pubmed_records.len() / 10
                    })
                    .collect::<Vec<_>>();

                for word in words {
                    index
                        .entry(word.to_string())
                        .or_insert_with(Vec::new)
                        .push(pmid.clone());
                }
            }

            info!(
                "Built PubMed word index with {} unique words in {:?}",
                index.len(),
                start.elapsed()
            );

            *pubmed_index = Some(index);
        }
    }

    fn get_pubmed_candidates_by_words(
        &self,
        title: &str,
        pubmed_records: &HashMap<String, PubMedRecord>,
    ) -> Vec<String> {
        // Ensure index is built
        {
            let index = self.pubmed_word_index.read().unwrap();
            if index.is_none() {
                drop(index);
                self.build_pubmed_word_index(pubmed_records);
            }
        }

        // Get normalized words from title
        let title_norm = self.get_normalized(title);
        let words: Vec<_> = title_norm
            .split_whitespace()
            .filter(|w| w.len() >= MIN_TITLE_WORD_LENGTH && !self.common_words.contains(*w))
            .collect();

        if words.is_empty() {
            return Vec::new();
        }

        // Use index to find candidate PMIDs
        let index = self.pubmed_word_index.read().unwrap();
        if let Some(index) = &*index {
            // Count matching PMIDs for each word
            let mut pmid_counts: HashMap<String, usize> = HashMap::new();

            for word in words {
                if let Some(pmids) = index.get(word) {
                    for pmid in pmids {
                        *pmid_counts.entry(pmid.clone()).or_insert(0) += 1;
                    }
                }
            }

            // Select candidates with at least 2 matching words, sorted by match count
            let mut candidates: Vec<_> = pmid_counts
                .into_iter()
                .filter(|(_, count)| *count >= 2)
                .collect();

            candidates.sort_by(|a, b| b.1.cmp(&a.1));

            // Return limited set of best candidates
            candidates
                .into_iter()
                .take(MAX_CANDIDATE_POOL_SIZE)
                .map(|(pmid, _)| pmid)
                .collect()
        } else {
            Vec::new()
        }
    }
}

/// Service for matching publications with PubMed records
pub struct MatchingService {
    config: IntegrationConfig,
    stats: Arc<Mutex<MatchingStats>>,
    cache: MatchingCache,
}

impl MatchingService {
    /// Create a new matching service with default configuration
    pub fn new() -> Self {
        Self {
            config: IntegrationConfig::default(),
            stats: Arc::new(Mutex::new(MatchingStats::new())),
            cache: MatchingCache::new(),
        }
    }

    /// Create a new matching service with custom configuration
    pub fn with_config(config: IntegrationConfig) -> Self {
        Self {
            config,
            stats: Arc::new(Mutex::new(MatchingStats::new())),
            cache: MatchingCache::new(),
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

        // Pre-build the PubMed word index for faster matching
        self.cache.build_pubmed_word_index(pubmed_records);

        let all_matches = time_operation("find_matches", || -> Result<Vec<MatchResult>> {
            let publication_ids = publication_service.get_all_ids();
            let matches = Arc::new(Mutex::new(Vec::new()));

            // Process publications in parallel batches to better control memory
            let num_threads = rayon::current_num_threads();
            let batch_size = (publication_ids.len() + num_threads - 1) / num_threads;

            // Process in batches to improve locality and reduce lock contention
            publication_ids.par_chunks(batch_size).for_each(|id_batch| {
                let mut batch_matches = Vec::new();

                for pub_id in id_batch {
                    if let Some(publication) = publication_service.get_publication(pub_id) {
                        // Skip if already has a PMID
                        if publication.pmid.is_some() {
                            debug!("Skipping publication {} as it already has a PMID", pub_id);
                            continue;
                        }

                        // Try to match this publication
                        match self.match_publication(
                            &publication,
                            publication_service,
                            pubmed_records,
                        ) {
                            Ok(pub_matches) => {
                                if !pub_matches.is_empty() {
                                    batch_matches.extend(pub_matches);
                                }
                            }
                            Err(e) => {
                                warn!("Error matching publication {}: {}", pub_id, e);
                            }
                        }
                    }
                }

                // Add all batch matches at once to reduce lock contention
                if !batch_matches.is_empty() {
                    let mut matches_guard = matches.lock().unwrap();
                    let mut stats_guard = self.stats.lock().unwrap();

                    for m in batch_matches {
                        stats_guard.add_match(&m);
                        matches_guard.push(m);
                    }
                }
            });

            // Extract final matches
            let matches = Arc::try_unwrap(matches).unwrap().into_inner().unwrap();
            Ok(matches)
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
        let title_norm = self.cache.get_normalized(&publication.title);

        // Optimization: use a more efficient pre-filtering mechanism
        let candidates = self
            .cache
            .get_pubmed_candidates_by_words(&publication.title, pubmed_records);

        // If we have promising candidates, check only those
        if !candidates.is_empty() {
            for pmid in &candidates {
                if let Some(record) = pubmed_records.get(pmid) {
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
            }
        } else {
            // Fallback to checking all records if we couldn't find candidates
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
        }

        Ok(matches)
    }

    /// Match by title and author with optimized implementation
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
        let candidates = self
            .cache
            .get_pubmed_candidates_by_words(&publication.title, pubmed_records);

        // Cache normalized publication data
        let title_norm = self.cache.get_normalized(&publication.title);
        let author_norm = self.cache.get_normalized(author);

        // Match with candidate PubMed records
        for pmid in candidates {
            if let Some(record) = pubmed_records.get(&pmid) {
                // Use cached similarity calculation
                let title_similarity = self.cache.get_similarity(&title_norm, &record.title_norm);

                // Rough title similarity check
                if title_similarity < 0.8 {
                    continue;
                }

                // Check for author match
                let author_similarity = self
                    .cache
                    .get_similarity(&author_norm, &record.authors_norm);
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
        }

        Ok(matches)
    }

    /// Match by title and year with optimized implementation
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

        // Get candidate PubMed records by title
        let candidates = self
            .cache
            .get_pubmed_candidates_by_words(&publication.title, pubmed_records);
        let title_norm = self.cache.get_normalized(&publication.title);

        // Check candidate PubMed records for title+year match
        for pmid in candidates {
            if let Some(record) = pubmed_records.get(&pmid) {
                if record.year == *year {
                    let title_similarity =
                        self.cache.get_similarity(&title_norm, &record.title_norm);

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
        }

        Ok(matches)
    }

    /// Match by fuzzy title with improved optimization
    fn match_by_fuzzy_title(
        &self,
        publication: &Publication,
        publication_service: &PublicationDataService,
        pubmed_records: &HashMap<String, PubMedRecord>,
    ) -> Result<Vec<MatchResult>> {
        let mut matches = Vec::new();
        let title_norm = self.cache.get_normalized(&publication.title);

        // Get candidate PubMed records by title words
        let candidates = self
            .cache
            .get_pubmed_candidates_by_words(&publication.title, pubmed_records);

        // Calculate similarity scores for candidates
        for pmid in candidates {
            if let Some(record) = pubmed_records.get(&pmid) {
                let similarity = self.cache.get_similarity(&title_norm, &record.title_norm);
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
