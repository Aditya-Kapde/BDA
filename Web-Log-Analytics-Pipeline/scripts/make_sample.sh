#!/usr/bin/env bash
# scripts/make_sample.sh — Generates 1,000,000-row sample from raw clickstream CSV
set -euo pipefail

TARGET_RECORDS=1000000
EXPECTED_LINES=$((TARGET_RECORDS + 1)) # Header + 1M data records
OUTPUT_DIR="data/sample"
OUTPUT_FILE="${OUTPUT_DIR}/sample_1m.csv"

# Resolve input file
INPUT_FILE="${1:-}"
if [[ -z "$INPUT_FILE" ]]; then
    if [[ -f "data/raw/2019-Nov.csv" ]]; then
        INPUT_FILE="data/raw/2019-Nov.csv"
    elif [[ -f "data/raw/2019-Oct.csv" ]]; then
        INPUT_FILE="data/raw/2019-Oct.csv"
    else
        echo "Error: No input file specified and no default found in data/raw/." >&2
        echo "Usage: $0 [path/to/raw.csv]" >&2
        exit 1
    fi
fi

if [[ ! -f "$INPUT_FILE" ]]; then
    echo "Error: Input file '$INPUT_FILE' does not exist." >&2
    exit 1
fi

echo "Input source : $INPUT_FILE"
echo "Output target: $OUTPUT_FILE"

# Guard against replacing a larger valid sample with a smaller one
if [[ -f "$OUTPUT_FILE" ]]; then
    EXISTING_LINES=$(wc -l < "$OUTPUT_FILE" | tr -d ' ')
    if [[ "$EXISTING_LINES" -ge "$EXPECTED_LINES" ]]; then
        echo "Valid sample already exists with $EXISTING_LINES lines. Skipping extraction."
        exit 0
    else
        echo "Existing sample incomplete ($EXISTING_LINES lines). Regenerating..."
    fi
fi

mkdir -p "$OUTPUT_DIR"

# Verify input has at least the required number of lines using bounded check
echo "Verifying input size..."
INPUT_HEAD_COUNT=$(head -n "$EXPECTED_LINES" "$INPUT_FILE" | wc -l | tr -d ' ')
if [[ "$INPUT_HEAD_COUNT" -lt "$EXPECTED_LINES" ]]; then
    echo "Error: Input file '$INPUT_FILE' has only $INPUT_HEAD_COUNT lines (fewer than required $EXPECTED_LINES)." >&2
    exit 1
fi

# Stream first 1,000,001 lines (header + 1,000,000 records) with constant memory
echo "Extracting header and first $TARGET_RECORDS data rows..."
head -n "$EXPECTED_LINES" "$INPUT_FILE" > "${OUTPUT_FILE}.tmp"
mv "${OUTPUT_FILE}.tmp" "$OUTPUT_FILE"

WRITTEN_LINES=$(wc -l < "$OUTPUT_FILE" | tr -d ' ')
echo "Extraction successful: $WRITTEN_LINES lines written to $OUTPUT_FILE."
