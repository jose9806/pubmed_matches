#!/bin/bash
set -e

# Default values
INPUT_CSV="user_publications.csv"
OUTPUT_CSV="enriched_publications.csv"
DB_HOST="localhost"
DB_NAME="pubmed_integration"
DB_USER="pubmed"
DB_PASSWORD="pubmed_password"

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
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Check if the executable exists
if [ ! -f "target/release/pubmed-matcher" ]; then
  echo "Executable not found. Building the project first..."
  ./build.sh
fi

# Set environment variables for PostgreSQL
export PGHOST=$DB_HOST
export PGDATABASE=$DB_NAME
export PGUSER=$DB_USER
export PGPASSWORD=$DB_PASSWORD

echo "Starting PubMed matcher..."
echo "Input: $INPUT_CSV"
echo "Output: $OUTPUT_CSV"
echo "Database host: $DB_HOST"
echo "Database name: $DB_NAME"
echo "Database user: $DB_USER"

# Run the matcher
./target/release/pubmed-matcher \
  --input "$INPUT_CSV" \
  --output "$OUTPUT_CSV" \
  --db-host "$DB_HOST" \
  --db-name "$DB_NAME" \
  --db-user "$DB_USER" \
  --db-password "$DB_PASSWORD"

echo "Matching process completed. Enriched publications saved to $OUTPUT_CSV"
