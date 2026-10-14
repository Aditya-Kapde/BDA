#!/usr/bin/env python3
"""
tests/test_data_ingestion.py — Unit and integration tests for data ingestion scripts

Tests:
  1. Date partitioning (dt=YYYY-MM-DD)
  2. Unknown date handling (dt=unknown)
  3. Header preservation in partitions
  4. Row-count reconciliation invariant (rows_in == sum(partition_rows))
  5. Empty inputs and invalid arguments
  6. Existing output protection and --force behavior
  7. CSV values with commas or quoted fields
  8. Sample generation and insufficient input failure
  9. HDFS uploader argument validation
"""

import csv
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

# Paths
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(REPO_ROOT, "scripts")
SPLIT_SCRIPT = os.path.join(SCRIPTS_DIR, "split_by_day.py")
SAMPLE_SCRIPT = os.path.join(SCRIPTS_DIR, "make_sample.sh")
INGEST_SCRIPT = os.path.join(SCRIPTS_DIR, "ingest_to_hdfs.sh")
FIXTURE_PATH = os.path.join(
    REPO_ROOT, "src", "test", "resources", "fixtures", "mini_clickstream.csv"
)


def to_bash_path(path: str) -> str:
    """Converts a Windows path (e.g. D:\\foo) to a WSL path (/mnt/d/foo) for bash."""
    abs_p = os.path.abspath(path)
    if len(abs_p) >= 2 and abs_p[1] == ":":
        drive = abs_p[0].lower()
        rest = abs_p[2:].replace("\\", "/")
        return f"/mnt/{drive}{rest}"
    return abs_p.replace("\\", "/")


class TestSplitByDay(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_mini_clickstream_partitioning(self):
        """Verify partitioning, unknown-date routing, header preservation and row reconciliation on mini_clickstream."""
        out_dir = os.path.join(self.temp_dir, "partitions")
        cmd = [sys.executable, SPLIT_SCRIPT, "--input", FIXTURE_PATH, "--out", out_dir]
        result = subprocess.run(cmd, capture_output=True, text=True)

        self.assertEqual(
            result.returncode, 0, f"split_by_day failed: {result.stderr}"
        )
        self.assertIn("Data Retention Invariant: PASS", result.stdout)

        # Check partitions created
        partitions = sorted([d for d in os.listdir(out_dir) if d.startswith("dt=")])
        self.assertIn("dt=2019-11-01", partitions)
        self.assertIn("dt=2019-11-02", partitions)
        self.assertIn("dt=unknown", partitions)

        # Verify header preservation and non-empty rows in each partition
        total_data_rows = 0
        with open(FIXTURE_PATH, "r", encoding="utf-8") as f:
            fixture_rows = list(csv.reader(f))
            expected_header = fixture_rows[0]
            expected_data_rows = len(fixture_rows) - 1

        for part in partitions:
            part_csv = os.path.join(out_dir, part, "events.csv")
            self.assertTrue(os.path.isfile(part_csv))
            with open(part_csv, "r", encoding="utf-8") as pf:
                rows = list(csv.reader(pf))
                self.assertGreater(len(rows), 1, f"Partition {part} has no data rows")
                self.assertEqual(rows[0], expected_header, f"Header mismatch in {part}")
                total_data_rows += len(rows) - 1

        self.assertEqual(total_data_rows, expected_data_rows)

    def test_quoted_fields_and_commas(self):
        """Verify CSV fields containing commas and quotes are parsed and written properly."""
        test_csv = os.path.join(self.temp_dir, "test_quoted.csv")
        out_dir = os.path.join(self.temp_dir, "partitions_quoted")

        header = [
            "event_time", "event_type", "product_id", "category_id",
            "category_code", "brand", "price", "user_id", "user_session"
        ]
        row1 = [
            "2019-11-05 10:00:00 UTC", "view", "101", "201",
            "electronics,audio.headphone", "brand, with comma", "29.99", "1", "s1"
        ]
        row2 = [
            "2019-11-05 11:00:00 UTC", "cart", "102", "202",
            "", "", "10.00", "2", "s2"
        ]

        with open(test_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerow(row1)
            writer.writerow(row2)

        cmd = [sys.executable, SPLIT_SCRIPT, "--input", test_csv, "--out", out_dir]
        result = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)

        part_csv = os.path.join(out_dir, "dt=2019-11-05", "events.csv")
        self.assertTrue(os.path.isfile(part_csv))
        with open(part_csv, "r", newline="", encoding="utf-8") as f:
            reader = list(csv.reader(f))
            self.assertEqual(reader[0], header)
            self.assertEqual(reader[1], row1)
            self.assertEqual(reader[2], row2)

    def test_existing_output_protection_and_force(self):
        """Verify partition directory refuses overwrite without --force, and succeeds with --force."""
        out_dir = os.path.join(self.temp_dir, "partitions_force")
        cmd1 = [sys.executable, SPLIT_SCRIPT, "--input", FIXTURE_PATH, "--out", out_dir]
        res1 = subprocess.run(cmd1, capture_output=True, text=True)
        self.assertEqual(res1.returncode, 0)

        # Run again without --force -> must fail with code 1
        cmd2 = [sys.executable, SPLIT_SCRIPT, "--input", FIXTURE_PATH, "--out", out_dir]
        res2 = subprocess.run(cmd2, capture_output=True, text=True)
        self.assertEqual(res2.returncode, 1)
        self.assertIn("already contains", res2.stderr)

        # Run with --force -> must succeed with code 0
        cmd3 = [sys.executable, SPLIT_SCRIPT, "--input", FIXTURE_PATH, "--out", out_dir, "--force"]
        res3 = subprocess.run(cmd3, capture_output=True, text=True)
        self.assertEqual(res3.returncode, 0)

    def test_missing_input_and_empty_file(self):
        """Verify split_by_day handles missing input and completely empty files safely."""
        out_dir = os.path.join(self.temp_dir, "out_missing")
        # Missing file
        cmd1 = [sys.executable, SPLIT_SCRIPT, "--input", "non_existent.csv", "--out", out_dir]
        res1 = subprocess.run(cmd1, capture_output=True, text=True)
        self.assertEqual(res1.returncode, 1)

        # Completely empty file
        empty_csv = os.path.join(self.temp_dir, "empty.csv")
        open(empty_csv, "w").close()
        cmd2 = [sys.executable, SPLIT_SCRIPT, "--input", empty_csv, "--out", out_dir]
        res2 = subprocess.run(cmd2, capture_output=True, text=True)
        self.assertEqual(res2.returncode, 1)
        self.assertIn("completely empty", res2.stderr)


class TestMakeSample(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_make_sample_insufficient_records_fails(self):
        """make_sample.sh must fail clearly when input has fewer than 1,000,001 rows."""
        small_csv = os.path.join(self.temp_dir, "small.csv")
        with open(small_csv, "w") as f:
            f.write("header\n1\n2\n3\n")

        if shutil.which("bash"):
            cmd = ["bash", to_bash_path(SAMPLE_SCRIPT), to_bash_path(small_csv)]
            result = subprocess.run(cmd, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("fewer than required", result.stderr)

    def test_make_sample_missing_input_fails(self):
        """make_sample.sh must fail clearly when input file is missing."""
        if shutil.which("bash"):
            cmd = [
                "bash",
                to_bash_path(SAMPLE_SCRIPT),
                to_bash_path(os.path.join(self.temp_dir, "nonexistent.csv")),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)


class TestIngestToHdfs(unittest.TestCase):
    def test_ingest_argument_validation(self):
        """ingest_to_hdfs.sh must validate argument count and non-existent input."""
        if shutil.which("bash"):
            # No args
            res1 = subprocess.run(
                ["bash", to_bash_path(INGEST_SCRIPT)],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res1.returncode, 1)
            self.assertIn("Usage:", res1.stderr)

            # Nonexistent dir
            res2 = subprocess.run(
                ["bash", to_bash_path(INGEST_SCRIPT), "/nonexistent/path/dir"],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res2.returncode, 1)
            self.assertIn("does not exist", res2.stderr)


if __name__ == "__main__":
    unittest.main()
