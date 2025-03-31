#!/bin/bash
set -e

# Default values
INPUT_CSV="Pubmed_all/user_publications.csv"
OUTPUT_CSV="enriched_publications.csv"
DB_HOST="localhost"
DB_NAME="pubmed_integration"
DB_USER="pubmed"
DB_PASSWORD="pubmed_password"
WORKERS=32
SKIP_DB_INSERT=""
STAGE="all"
RESUME=""
CHECKPOINT_DIR=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --input)
      INPUT_CSV="$2"
      shift 2
      ;;
    --output)
      OUTPUT_CSV="$2"
      shift 2
      ;;
    --db-host)
      DB_HOST="$2"
      shift 2
      ;;
    --db-name)
      DB_NAME="$2"
      shift 2
      ;;
    --db-user)
      DB_USER="$2"
      shift 2
      ;;
    --db-password)
      DB_PASSWORD="$2"
      shift 2
      ;;
    --workers)
      WORKERS="$2"
      shift 2
      ;;
    --skip-db-insert)
      SKIP_DB_INSERT="--skip-db-insert"
      shift
      ;;
    --stage)
      STAGE="$2"
      shift 2
      ;;
    --resume)
      RESUME="--resume"
      shift
      ;;
    --checkpoint-dir)
      CHECKPOINT_DIR="--checkpoint-dir $2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo ""
      echo "Options:"
      echo "  --input FILE          Input CSV file (default: user_publications.csv)"
      echo "  --output FILE         Output enriched CSV file (default: enriched_publications.csv)"
      echo "  --db-host HOST        Database host (default: localhost)"
      echo "  --db-name NAME        Database name (default: pubmed_integration)"
      echo "  --db-user USER        Database user (default: pubmed)"
      echo "  --db-password PASS    Database password (default: pubmed_password)"
      echo "  --workers NUM         Number of worker threads (default: 32)"
      echo "  --skip-db-insert      Skip database insertion (for testing)"
      echo "  --stage STAGE         Specific stage to run (load, match, save, enrich, all)"
      echo "  --resume              Resume from last checkpoint"
      echo "  --checkpoint-dir DIR  Directory for checkpoints"
      echo "  --help                Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Check if the executable exists
if [ ! -f "target/release/pubmed-matcher" ]; then
  echo "Executable not found. Building the project first..."
  ./scripts/build.sh
fi

# Set environment variables for PostgreSQL
export PGHOST=$DB_HOST
export PGDATABASE=$DB_NAME
export PGUSER=$DB_USER
export PGPASSWORD=$DB_PASSWORD

echo "Starting PubMed matcher with checkpointing..."
echo "Input: $INPUT_CSV"
echo "Output: $OUTPUT_CSV"
echo "Database host: $DB_HOST"
echo "Database name: $DB_NAME"
echo "Database user: $DB_USER"
echo "Worker threads: $WORKERS"
echo "Stage: $STAGE"
if [ ! -z "$RESUME" ]; then
  echo "Resuming from last checkpoint"
fi

# Run the matcher
./target/release/pubmed-matcher \
  --input "$INPUT_CSV" \
  --output "$OUTPUT_CSV" \
  --db-host "$DB_HOST" \
  --db-name "$DB_NAME" \
  --db-user "$DB_USER" \
  --db-password "$DB_PASSWORD" \
  --workers "$WORKERS" \
  --stage "$STAGE" \
  $RESUME \
  $CHECKPOINT_DIR \
  $SKIP_DB_INSERT

echo "Matching process completed for stage: $STAGE"

if [ "$STAGE" == "all" ] || [ "$STAGE" == "enrich" ]; then
  echo "Enriched publications saved to $OUTPUT_CSV"
fi

# Display available checkpoints if resuming or using specific stages
if [ ! -z "$RESUME" ] || [ "$STAGE" != "all" ]; then
  echo ""
  echo "Available checkpoints:"
  ls -la checkpoints/
fi