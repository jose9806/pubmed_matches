# PubMed Integration System

A high-performance system for integrating academic publications with PubMed data. This system efficiently processes PubMed XML files, matches them with existing publication records, and enriches your publication database with valuable PubMed metadata.

## Features

- **High-Performance XML Processing**: Efficiently processes large PubMed XML files with optimized memory usage
- **Intelligent Matching Algorithms**: Multiple matching strategies (exact title, fuzzy title, title+author, title+year)
- **Database Integration**: PostgreSQL-based storage with optimized schema and queries
- **Parallel Processing**: Utilizes multi-core systems for faster processing
- **Memory Efficiency**: Streaming architecture minimizes memory footprint
- **Scalable Design**: Handles millions of publications and PubMed records

## System Requirements

- **Rust** (1.70 or newer)
- **Python** (3.9 or newer)
- **PostgreSQL** (13 or newer)
- **Hardware**: 
  - Minimum: 4 CPU cores, 8GB RAM, 50GB storage
  - Recommended: 8+ CPU cores, 16GB+ RAM, SSD storage

## Installation

### Using Docker (Recommended)

1. Ensure Docker and Docker Compose are installed
2. Clone the repository
3. Build and start the containers:

```bash
docker-compose up -d
```

### Manual Installation

#### 1. Set Up PostgreSQL Database

```bash
# Create database and user
sudo -u postgres psql
postgres=# CREATE USER pubmed WITH PASSWORD 'pubmed_password';
postgres=# CREATE DATABASE pubmed_integration OWNER pubmed;
postgres=# \q

# Initialize schema
psql -U pubmed -d pubmed_integration -f database/init/01-schema.sql
```

#### 2. Install Rust Components

```bash
# Install Rust if not already installed
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build the project
cargo build --release
```

#### 3. Install Python Dependencies

```bash
# Using pip
pip install pandas psycopg2-binary lxml tqdm

# Or using Poetry
poetry install
```

## Project Structure

- `src/` - Rust source code
  - `main.rs` - Entry point and CLI interface
  - `db.rs` - Database service
  - `matching_service.rs` - Publication matching logic
  - `models.rs` - Data models
  - `pubmed_engine.rs` - XML processing engine
  - `publication_service.rs` - Publication management
  - `utils.rs` - Utility functions
  - `integration_controller.rs` - Orchestration logic
- `database/init/` - Database initialization scripts
- `process_folder.py` - Python script for processing PubMed folders

## Usage

### Rust CLI Tool

The main Rust tool provides a comprehensive command-line interface:

```bash
# Basic usage
./target/release/pubmed_integration \
  --csv-path /path/to/publications.csv \
  --xml-folders /path/to/pubmed/xml/folder \
  --output-path enriched_publications.csv \
  --database-url postgres://pubmed:pubmed_password@localhost:5432/pubmed_integration \
  --strategies all

# Command options
  --csv-path PATH             Path to CSV file with publications
  --xml-folders FOLDERS...    Folders containing PubMed XML files
  --output-path PATH          Output path for enriched CSV [default: enriched_publications.csv]
  --batch-size N              Batch size for processing [default: 5]
  --min-match-quality N       Minimum match quality threshold (0-100) [default: 75.0]
  --max-matches-per-publication N  Maximum matches per publication [default: 1]
  --fuzzy-match-threshold N   Threshold for fuzzy matching (0-100) [default: 85.0]
  --strategies STRATS...      Matching strategies [default: all]
  --log-level LEVEL           Logging level [default: info]
  --database-url URL          Database connection URL
  --import-csv                Import CSV data to database
  --import-xml                Import XML data to database
  --run-matching              Run the matching process
  --export-enriched           Export enriched publications to CSV
```

### Python Script for Bulk Processing

For larger datasets, the Python script offers optimized bulk processing:

```bash
python process_folder.py \
  --base-folder /path/to/pubmed/data \
  --db-host localhost \
  --db-name pubmed_integration \
  --db-user pubmed \
  --db-password pubmed_password \
  --match \
  --workers 8
```

### Docker Execution

When using Docker, you can run the processing directly with:

```bash
# Set the data path environment variable
export PUBMED_DATA_PATH=/path/to/pubmed/data

# Run the processor container
docker-compose up pubmed_processor
```

## Complete Processing Workflow

A typical workflow consists of these steps:

1. **Import Publications**:
   ```bash
   ./target/release/pubmed_integration \
     --csv-path publications.csv \
     --database-url postgres://pubmed:pubmed_password@localhost:5432/pubmed_integration \
     --import-csv
   ```

2. **Import PubMed XML Data**:
   ```bash
   ./target/release/pubmed_integration \
     --xml-folders /path/to/pubmed/xml \
     --database-url postgres://pubmed:pubmed_password@localhost:5432/pubmed_integration \
     --import-xml
   ```

3. **Run Matching Process**:
   ```bash
   ./target/release/pubmed_integration \
     --database-url postgres://pubmed:pubmed_password@localhost:5432/pubmed_integration \
     --run-matching
   ```

4. **Export Enriched Publications**:
   ```bash
   ./target/release/pubmed_integration \
     --database-url postgres://pubmed:pubmed_password@localhost:5432/pubmed_integration \
     --output-path enriched_publications.csv \
     --export-enriched
   ```

Or, to run all steps in one command:

```bash
./target/release/pubmed_integration \
  --csv-path publications.csv \
  --xml-folders /path/to/pubmed/xml \
  --database-url postgres://pubmed:pubmed_password@localhost:5432/pubmed_integration \
  --import-csv \
  --import-xml \
  --run-matching \
  --export-enriched
```

## Performance Optimization

The system has been optimized for maximum performance:

### Database Optimizations

- Efficient schema with appropriate indexes
- Batch database operations
- Connection pooling
- Materialized views for frequent queries

### Processing Optimizations

- Streaming XML parsing
- Parallel processing with rayon
- Memory-efficient data structures
- Caching for expensive operations
- Adaptive threading based on workload

### Configuration

For larger datasets, adjust these settings in PostgreSQL:

```
shared_buffers = 4GB
work_mem = 128MB
maintenance_work_mem = 1GB
effective_cache_size = 12GB
max_worker_processes = 16
max_parallel_workers_per_gather = 8
```

And increase batch sizes:

```bash
./target/release/pubmed_integration \
  --batch-size 10 \
  --fuzzy-match-threshold 90.0
```

## Database Schema

The database uses the following main tables:

- `publications` - User publications
- `pubmed_records` - PubMed records
- `authors` - Author information
- `mesh_terms` - MeSH terms
- `publication_pubmed_matches` - Matches between publications and PubMed records

See `database/init/01-schema.sql` for the complete schema.

## Troubleshooting

### Memory Issues

If you encounter memory problems with large datasets:

```bash
# Reduce batch size
./target/release/pubmed_integration --batch-size 2

# Process XML files with Python script
python process_folder.py --workers 4
```

### Database Performance

For better database performance:

```bash
# Add indexes for specific query patterns
CREATE INDEX idx_pubmed_year_title ON pubmed_records (year, title_normalized);

# Analyze tables after large imports
ANALYZE publications;
ANALYZE pubmed_records;
```

