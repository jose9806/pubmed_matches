mod db;
mod integration_controller;
mod matching_service;
mod models;
mod publication_service;
mod pubmed_engine;
mod utils;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use db::DatabaseService;
use dotenv::dotenv;
use integration_controller::IntegrationController;
use log::{error, info};
use std::path::PathBuf;

use models::{IntegrationConfig, MatchStrategy};
use utils::setup_logging;

#[derive(Debug, Clone, ValueEnum, PartialEq)]
enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Error => write!(f, "error"),
            LogLevel::Warn => write!(f, "warn"),
            LogLevel::Info => write!(f, "info"),
            LogLevel::Debug => write!(f, "debug"),
        }
    }
}

#[derive(Debug, Clone, ValueEnum, PartialEq)]
enum MatchingStrategy {
    ExactTitle,
    TitleAuthor,
    TitleYear,
    FuzzyTitle,
    All,
}

impl From<MatchingStrategy> for MatchStrategy {
    fn from(strategy: MatchingStrategy) -> Self {
        match strategy {
            MatchingStrategy::ExactTitle => MatchStrategy::ExactTitle,
            MatchingStrategy::TitleAuthor => MatchStrategy::TitleAuthor,
            MatchingStrategy::TitleYear => MatchStrategy::TitleYear,
            MatchingStrategy::FuzzyTitle => MatchStrategy::FuzzyTitle,
            MatchingStrategy::All => unreachable!("'All' is handled separately"),
        }
    }
}

/// Command line arguments
#[derive(Parser)]
#[clap(
    name = "PubMed Integration",
    about = "Integrates PubMed data with user publications",
    version
)]
struct Args {
    /// Path to the CSV file containing user publications
    #[clap(short, long)]
    csv_path: Option<PathBuf>,

    /// Folders containing PubMed XML files
    #[clap(short, long)]
    xml_folders: Option<Vec<PathBuf>>,

    /// Output path for the enriched CSV
    #[clap(short, long, default_value = "enriched_publications.csv")]
    output_path: PathBuf,

    /// Batch size for processing XML files
    #[clap(short, long, default_value = "5")]
    batch_size: usize,

    /// Minimum match quality threshold (0-100)
    #[clap(long, default_value = "75.0")]
    min_match_quality: f64,

    /// Maximum matches per publication
    #[clap(long, default_value = "1")]
    max_matches_per_publication: usize,

    /// Threshold for fuzzy matching (0-100)
    #[clap(long, default_value = "85.0")]
    fuzzy_match_threshold: f64,

    /// Matching strategies to use
    #[clap(long, value_enum, default_value = "all")]
    strategies: Vec<MatchingStrategy>,

    /// Logging level
    #[clap(short, long, value_enum, default_value = "info")]
    log_level: LogLevel,

    /// Database connection URL
    #[clap(long)]
    database_url: Option<String>,

    /// Import CSV data to database
    #[clap(long)]
    import_csv: bool,

    /// Import XML data to database
    #[clap(long)]
    import_xml: bool,

    /// Run the matching process
    #[clap(long)]
    run_matching: bool,

    /// Export enriched publications to CSV
    #[clap(long)]
    export_enriched: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file
    dotenv().ok();

    // Parse command line arguments
    let args = Args::parse();

    // Setup logging
    setup_logging(&args.log_level.to_string())?;

    // Create integration configuration
    let mut config = IntegrationConfig {
        min_match_quality: args.min_match_quality,
        max_matches_per_publication: args.max_matches_per_publication,
        fuzzy_match_threshold: args.fuzzy_match_threshold,
        batch_size: args.batch_size,
        checkpoint_interval: 100,
        enabled_strategies: Vec::new(),
    };

    // Configure matching strategies
    if args.strategies.contains(&MatchingStrategy::All) {
        config.enabled_strategies = vec![
            MatchStrategy::ExactTitle,
            MatchStrategy::TitleAuthor,
            MatchStrategy::TitleYear,
            MatchStrategy::FuzzyTitle,
        ];
    } else {
        for strategy in &args.strategies {
            if *strategy != MatchingStrategy::All {
                config
                    .enabled_strategies
                    .push(MatchStrategy::from(strategy.clone()));
            }
        }
    }

    // Check if database URL is provided
    let database_url = args
        .database_url
        .or_else(|| std::env::var("DATABASE_URL").ok());
    if let Some(database_url) = database_url {
        // Connect to database
        let db_service = DatabaseService::new(&database_url).await?;

        // Create integration controller
        let mut controller = IntegrationController::with_config(config).with_database(db_service);

        // Import CSV data if requested
        if args.import_csv {
            if let Some(csv_path) = &args.csv_path {
                let count = controller.process_csv_to_database(csv_path).await?;
                info!("Imported {} publications from CSV to database", count);
            } else {
                error!("CSV path must be specified with --csv-path when using --import-csv");
                return Err(anyhow::anyhow!("CSV path not specified"));
            }
        }

        // Import XML data if requested
        if args.import_xml {
            if let Some(ref xml_folders) = args.xml_folders {
                let count = controller.process_xml_to_database(xml_folders).await?;
                info!("Imported {} PubMed records from XML to database", count);
            } else {
                error!("XML folders must be specified with --xml-folders when using --import-xml");
                return Err(anyhow::anyhow!("XML folders not specified"));
            }
        }

        // Run matching process if requested
        if args.run_matching {
            let stats = controller.execute_with_database().await?;
            info!(
                "Matching completed: matched {} of {} publications ({:.2}%)",
                stats.matched_publications,
                stats.total_publications,
                (stats.matched_publications as f64 / stats.total_publications as f64) * 100.0
            );
        }

        // Export enriched publications if requested
        if args.export_enriched {
            let count = controller
                .export_enriched_publications(&args.output_path)
                .await?;
            info!(
                "Exported {} enriched publications to {}",
                count,
                args.output_path.display()
            );
        }

        Ok(())
    } else {
        error!("Database URL must be specified with --database-url or DATABASE_URL environment variable");
        Err(anyhow::anyhow!("Database URL not specified"))
    }
}
