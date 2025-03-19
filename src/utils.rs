use anyhow::Result;
use chrono::Local;
use env_logger::Builder;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use lazy_static::lazy_static;
use log::{debug, info, LevelFilter};
use regex::Regex;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

/// Setup logging with file and console output
pub fn setup_logging(log_level: &str) -> Result<()> {
    let level = match log_level.to_lowercase().as_str() {
        "debug" => LevelFilter::Debug,
        "info" => LevelFilter::Info,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };

    let log_filename = format!(
        "pubmed_integration_{}.log",
        Local::now().format("%Y%m%d_%H%M%S")
    );

    // Create a custom format that includes timestamps
    let mut builder = Builder::new();
    builder
        .format(|buf, record| {
            writeln!(
                buf,
                "{} - {} - {}: {}",
                Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.target(),
                record.args()
            )
        })
        .filter(None, level);

    // Log to file and stderr
    if let Ok(file) = std::fs::File::create(&log_filename) {
        builder.target(env_logger::Target::Pipe(Box::new(file)));
    }

    builder.init();

    info!(
        "Logging initialized at level {} to {} and stderr",
        level, log_filename
    );

    Ok(())
}

/// Initialize a multi-progress bar for tracking progress
pub fn setup_progress_bars() -> MultiProgress {
    let mp = MultiProgress::new();
    mp
}

/// Create a progress bar for file processing
pub fn create_file_progress_bar(mp: &MultiProgress, total: u64, description: &str) -> ProgressBar {
    let pb = mp.add(ProgressBar::new(total));
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} {prefix:.bold.dim} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("=> "),
    );
    pb.set_prefix(description.to_string());
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

/// Create a spinner progress bar for long-running operations
pub fn create_spinner_progress_bar(mp: &MultiProgress, description: &str) -> ProgressBar {
    let pb = mp.add(ProgressBar::new_spinner());
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {prefix:.bold.dim} [{elapsed_precise}] {msg}")
            .unwrap(),
    );
    pb.set_prefix(description.to_string());
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

/// Time a function execution and log its duration
pub fn time_operation<F, T>(name: &str, f: F) -> T
where
    F: FnOnce() -> T,
{
    let start = Instant::now();
    debug!("Starting operation: {}", name);

    let result = f();

    let duration = start.elapsed();
    debug!("Completed operation: {} in {:.2?}", name, duration);

    result
}

/// Normalize a string for matching purposes (lowercase, remove punctuation, etc.)
pub fn normalize_string(s: &str) -> String {
    lazy_static! {
        static ref PUNCTUATION_RE: Regex = Regex::new(r"[^\w\s]").unwrap();
    }

    PUNCTUATION_RE
        .replace_all(&s.to_lowercase(), "")
        .trim()
        .to_string()
}

/// Format bytes as human readable string (KB, MB, etc.)
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 7] = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];

    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_index])
}

/// Get filename from path
pub fn get_filename(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown_file".to_string())
}

/// Create a date-time string for the current time
pub fn current_datetime_string() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}
