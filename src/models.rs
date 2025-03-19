use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;

/// Publication record from user_publications.csv
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Publication {
    #[serde(rename = "cfrespublid")]
    pub id: String,
    pub title: String,
    #[serde(rename = "cfpersid_id", default)]
    pub person_id: Option<String>,
    #[serde(default)]
    pub author: Option<String>,
    #[serde(rename = "cfrespubldate", default)]
    pub publication_date: Option<String>,
    #[serde(default)]
    pub year: Option<String>,
    #[serde(rename = "cfdctype_id", default)]
    pub doc_type_id: Option<String>,
    #[serde(rename = "cfdctype", default)]
    pub doc_type: Option<String>,
    #[serde(default)]
    pub volume: Option<String>,
    #[serde(rename = "number", default)]
    pub number: Option<String>,
    #[serde(default)]
    pub pages: Option<String>,

    // PubMed enrichment fields
    #[serde(default)]
    pub pmid: Option<String>,
    #[serde(default)]
    pub pubmed_authors: Option<String>,
    #[serde(default)]
    pub pubmed_abstract: Option<String>,
    #[serde(default)]
    pub pubmed_journal: Option<String>,
    #[serde(default)]
    pub pubmed_mesh: Option<String>,
    #[serde(default)]
    pub match_quality: Option<f64>,
    #[serde(default)]
    pub match_type: Option<String>,
    #[serde(default)]
    pub enrichment_date: Option<String>,
}

/// Publication record with normalized fields for matching
#[derive(Debug, Clone)]
pub struct NormalizedPublication {
    pub id: String,
    pub title: String,
    pub title_norm: String,
    pub author_norm: String,
    pub year: String,
    pub volume: String,
    pub number: String,
}

/// PubMed record containing relevant fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubMedRecord {
    pub pmid: String,
    pub title: String,
    pub title_norm: String,
    pub authors: String,
    pub authors_norm: String,
    pub abstract_text: Option<String>,
    pub journal: Option<String>,
    pub volume: Option<String>,
    pub issue: Option<String>,
    pub year: String,
    pub mesh_headings: Vec<String>,
    pub file_source: String,
}

/// Match result linking Publications to PubMed records
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchResult {
    pub publication_id: String,
    pub pmid: String,
    pub match_quality: f64,
    pub match_type: String,
    pub timestamp: DateTime<Utc>,
}

impl fmt::Display for MatchResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Match[pub_id={}, pmid={}, quality={:.2}, type={}]",
            self.publication_id, self.pmid, self.match_quality, self.match_type
        )
    }
}

/// Statistics about the matching process
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MatchingStats {
    pub total_publications: usize,
    pub total_pubmed_records: usize,
    pub matched_publications: usize,
    pub matches_by_type: HashMap<String, usize>,
    pub average_match_quality: f64,
}

impl MatchingStats {
    pub fn new() -> Self {
        Self {
            total_publications: 0,
            total_pubmed_records: 0,
            matched_publications: 0,
            matches_by_type: HashMap::new(),
            average_match_quality: 0.0,
        }
    }

    pub fn add_match(&mut self, match_result: &MatchResult) {
        self.matched_publications += 1;
        *self
            .matches_by_type
            .entry(match_result.match_type.clone())
            .or_insert(0) += 1;

        // Update running average
        let current_sum = self.average_match_quality * (self.matched_publications - 1) as f64;
        self.average_match_quality =
            (current_sum + match_result.match_quality) / self.matched_publications as f64;
    }
}

/// Enum representing different matching strategies
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MatchStrategy {
    ExactTitle,
    TitleAuthor,
    TitleYear,
    FuzzyTitle,
    Manual,
}

impl fmt::Display for MatchStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MatchStrategy::ExactTitle => write!(f, "exact_title"),
            MatchStrategy::TitleAuthor => write!(f, "title_author"),
            MatchStrategy::TitleYear => write!(f, "title_year"),
            MatchStrategy::FuzzyTitle => write!(f, "fuzzy_title"),
            MatchStrategy::Manual => write!(f, "manual"),
        }
    }
}

/// Configuration for the integration process
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationConfig {
    pub min_match_quality: f64,
    pub max_matches_per_publication: usize,
    pub fuzzy_match_threshold: f64,
    pub batch_size: usize,
    pub checkpoint_interval: usize,
    pub enabled_strategies: Vec<MatchStrategy>,
}

impl Default for IntegrationConfig {
    fn default() -> Self {
        Self {
            min_match_quality: 75.0,
            max_matches_per_publication: 1,
            fuzzy_match_threshold: 85.0,
            batch_size: 5,
            checkpoint_interval: 1000,
            enabled_strategies: vec![
                MatchStrategy::ExactTitle,
                MatchStrategy::TitleAuthor,
                MatchStrategy::TitleYear,
                MatchStrategy::FuzzyTitle,
            ],
        }
    }
}

/// Checkpoint data for resuming integration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub processed_files: Vec<String>,
    pub matches: Vec<MatchResult>,
    pub stats: MatchingStats,
    pub timestamp: DateTime<Utc>,
}

impl Checkpoint {
    pub fn new() -> Self {
        Self {
            processed_files: Vec::new(),
            matches: Vec::new(),
            stats: MatchingStats::new(),
            timestamp: Utc::now(),
        }
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let file = std::fs::File::create(path)?;
        bincode::serialize_into(file, self)?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let checkpoint: Self = bincode::deserialize_from(file)?;
        Ok(checkpoint)
    }
}
