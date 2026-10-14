#!/usr/bin/env bash
# scripts/ingest_to_hdfs.sh — Uploads day-partitioned clickstream data to HDFS /clickstream/raw/
set -euo pipefail

HDFS_RAW_ROOT="/clickstream/raw"

usage() {
    echo "Usage: $0 <local_partitions_dir> [--force]" >&2
    echo "  <local_partitions_dir>: Directory containing dt=* partition folders" >&2
    echo "  --force               : Overwrite existing partition directories in HDFS" >&2
    exit 1
}

if [[ $# -lt 1 ]]; then
    usage
fi

LOCAL_DIR="$1"
FORCE=false
if [[ "${2:-}" == "--force" ]]; then
    FORCE=true
fi

if [[ ! -d "$LOCAL_DIR" ]]; then
    echo "Error: Local partitions directory '$LOCAL_DIR' does not exist." >&2
    exit 1
fi

# Pre-flight check: ensure hdfs command exists and HDFS cluster is reachable
if ! command -v hdfs &> /dev/null; then
    echo "Error: 'hdfs' command not found in PATH." >&2
    echo "Please ensure Hadoop client binaries are installed and configured (see docs/SETUP_ENVIRONMENT.md)." >&2
    exit 2
fi

echo "Checking HDFS connectivity..."
if ! hdfs dfsadmin -safemode get &> /dev/null; then
    echo "Error: Cannot connect to HDFS NameNode. Is the cluster running?" >&2
    echo "Check 'docker compose ps' or ensure HDFS service is healthy." >&2
    exit 2
fi

echo "Ensuring HDFS root directory $HDFS_RAW_ROOT exists..."
hdfs dfs -mkdir -p "$HDFS_RAW_ROOT"

UPLOADED_COUNT=0
SKIPPED_COUNT=0

# Iterate through partition directories
for part_path in "$LOCAL_DIR"/dt=*; do
    if [[ ! -d "$part_path" ]]; then
        continue
    fi
    part_name=$(basename "$part_path")
    hdfs_target="$HDFS_RAW_ROOT/$part_name"

    # Check if partition already exists in HDFS
    if hdfs dfs -test -d "$hdfs_target"; then
        if [[ "$FORCE" == "true" ]]; then
            echo "Partition $part_name exists. Overwriting (--force)..."
            hdfs dfs -rm -r -f "$hdfs_target"
            hdfs dfs -put "$part_path" "$HDFS_RAW_ROOT/"
            echo "Uploaded $part_name -> $hdfs_target"
            UPLOADED_COUNT=$((UPLOADED_COUNT + 1))
        else
            echo "Partition $part_name exists. Skipping (use --force to overwrite)."
            SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
        fi
    else
        echo "Uploading $part_name -> $hdfs_target..."
        hdfs dfs -put "$part_path" "$HDFS_RAW_ROOT/"
        UPLOADED_COUNT=$((UPLOADED_COUNT + 1))
    fi
done

echo ""
echo "=== Ingestion to HDFS Complete ==="
echo "Uploaded partitions : $UPLOADED_COUNT"
echo "Skipped partitions  : $SKIPPED_COUNT"
echo "Target HDFS layout  :"
hdfs dfs -ls "$HDFS_RAW_ROOT"
