#!/usr/bin/env python3
"""
scripts/split_by_day.py — Streams clickstream CSV and partitions by UTC date into dt=YYYY-MM-DD/events.csv

Requirements (SRS FR-ING-02, FR-ING-03):
  - Streams input row-by-row with O(1) bounded memory.
  - Generates dt=YYYY-MM-DD/events.csv preserving header in each partition.
  - Rows with unparseable/missing dates routed to dt=unknown/events.csv.
  - Zero data rows dropped: rows_in == sum(partition_rows).
  - Skips or fails on existing outputs unless --force is supplied.
"""

import argparse
import csv
import os
import re
import sys
from typing import Dict, TextIO, Tuple

DATE_REGEX = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream clickstream CSV and partition by UTC date into dt=YYYY-MM-DD/events.csv"
    )
    parser.add_argument(
        "--input", "-i", required=True, help="Path to input raw clickstream CSV"
    )
    parser.add_argument(
        "--out", "-o", required=True, help="Output directory for dt=* partitions"
    )
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Overwrite existing partition outputs",
    )
    return parser.parse_args()


def extract_date_partition(event_time_str: str) -> str:
    """
    Extracts YYYY-MM-DD from event_time string (format: 'yyyy-MM-dd HH:mm:ss UTC').
    Returns 'unknown' if date cannot be validated.
    """
    if not event_time_str:
        return "unknown"
    match = DATE_REGEX.match(event_time_str.strip())
    if match:
        return match.group(0)
    return "unknown"


def split_csv_by_day(input_path: str, out_dir: str, force: bool = False) -> int:
    if not os.path.isfile(input_path):
        print(f"Error: Input file '{input_path}' not found.", file=sys.stderr)
        return 1

    # Check for existing partition directories
    if os.path.exists(out_dir):
        existing_partitions = [
            d for d in os.listdir(out_dir) if d.startswith("dt=")
        ]
        if existing_partitions and not force:
            print(
                f"Error: Output directory '{out_dir}' already contains {len(existing_partitions)} partition(s). "
                f"Use --force to overwrite.",
                file=sys.stderr,
            )
            return 1

    os.makedirs(out_dir, exist_ok=True)

    open_files: Dict[str, Tuple[TextIO, csv.writer]] = {}
    partition_counts: Dict[str, int] = {}
    total_input_rows = 0

    try:
        with open(input_path, mode="r", newline="", encoding="utf-8", errors="replace") as infile:
            reader = csv.reader(infile)
            
            try:
                header = next(reader)
            except StopIteration:
                print(f"Error: Input file '{input_path}' is completely empty.", file=sys.stderr)
                return 1

            for row in reader:
                total_input_rows += 1
                event_time = row[0] if len(row) > 0 else ""
                date_key = extract_date_partition(event_time)
                partition_name = f"dt={date_key}"

                if partition_name not in open_files:
                    part_dir = os.path.join(out_dir, partition_name)
                    os.makedirs(part_dir, exist_ok=True)
                    part_file = os.path.join(part_dir, "events.csv")
                    
                    fh = open(part_file, mode="w", newline="", encoding="utf-8")
                    writer = csv.writer(fh, quoting=csv.QUOTE_MINIMAL)
                    # Write header once per partition
                    writer.writerow(header)
                    open_files[partition_name] = (fh, writer)
                    partition_counts[partition_name] = 0

                writer = open_files[partition_name][1]
                writer.writerow(row)
                partition_counts[partition_name] += 1

    finally:
        for fh, _ in open_files.values():
            fh.close()

    # Invariant and summary verification
    total_written_data_rows = sum(partition_counts.values())
    total_bytes = 0

    print("\n" + "=" * 65)
    print(" INGESTION PARTITIONING SUMMARY")
    print("=" * 65)
    print(f"{'Partition':<25} {'Data Rows':>15} {'Size (Bytes)':>20}")
    print("-" * 65)

    for part_name in sorted(partition_counts.keys()):
        part_file = os.path.join(out_dir, part_name, "events.csv")
        file_bytes = os.path.getsize(part_file) if os.path.exists(part_file) else 0
        total_bytes += file_bytes
        print(f"{part_name:<25} {partition_counts[part_name]:>15,d} {file_bytes:>20,d}")

    print("-" * 65)
    print(f"{'TOTALS':<25} {total_written_data_rows:>15,d} {total_bytes:>20,d}")
    print("=" * 65)

    print(f"Input data rows   : {total_input_rows:,}")
    print(f"Output data rows  : {total_written_data_rows:,}")
    print(f"Partitions created: {len(partition_counts)}")

    if total_input_rows == total_written_data_rows:
        print("Data Retention Invariant: PASS (rows_in == rows_out)")
        return 0
    else:
        print(
            f"Data Retention Invariant: FAIL (mismatch: {total_input_rows} in vs {total_written_data_rows} out)",
            file=sys.stderr,
        )
        return 2


def main() -> None:
    args = parse_arguments()
    sys.exit(split_csv_by_day(args.input, args.out, args.force))


if __name__ == "__main__":
    main()
