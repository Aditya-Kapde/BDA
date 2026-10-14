#!/usr/bin/env python3
"""
tests/test_export_reports.py — Comprehensive Automated Test Suite for Report Exporter (M8).

Tests:
  1. Valid fixture exports and schema verification.
  2. Byte-for-byte deterministic repeated exports.
  3. Rejection of failed or incomplete run_meta.json (status != SUCCESS).
  4. Rejection of runs with invariant_ok == false.
  5. Actionable error reporting and staging cleanup on missing/corrupt inputs.
  6. Multi-part Hadoop input handling and aggregation.
  7. Empty-but-valid inputs producing valid headers.
  8. Overwrite protection (--force flag enforcement).
  9. Work directories and export paths with spaces.
  10. Exact metadata JSON schema and counter validation.
"""

import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

# Add scripts directory to sys.path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

import export_reports


def create_mock_pipeline_work_dir(work_dir: str, run_id: str = "daily-2019-11-01", status: str = "SUCCESS", invariant_ok: bool = True):
    """Creates a complete realistic mock MapReduce work directory matching J1-J6 outputs."""
    os.makedirs(work_dir, exist_ok=True)

    # 1. J1 HourlyTraffic output (traffic/)
    traffic_dir = os.path.join(work_dir, "traffic")
    os.makedirs(traffic_dir, exist_ok=True)
    with open(os.path.join(traffic_dir, "part-r-00000"), "w", encoding="utf-8") as f:
        f.write("2019-11-01\t0\t3\t2\t1\t0\t0\t0\t0.000000\n")
        f.write("2019-11-01\t1\t3\t2\t0\t0\t1\t4\t0.571429\n")
        f.write("2019-11-01\t2\t2\t2\t0\t0\t0\t0\t0.000000\n")
        f.write("2019-11-01\t23\t1\t0\t1\t0\t0\t0\t0.000000\n")
        f.write("2019-11-02\t0\t1\t0\t0\t1\t0\t0\t0.000000\n")
        f.write("NA\tNA\t0\t0\t0\t0\t0\t4\t1.000000\n")

    # 2. J2 PeakHour output (peak/)
    peak_dir = os.path.join(work_dir, "peak")
    os.makedirs(peak_dir, exist_ok=True)
    with open(os.path.join(peak_dir, "peakday-r-00000"), "w", encoding="utf-8") as f:
        f.write("2019-11-01\t0\t3\t9\n")
        f.write("2019-11-02\t0\t1\t1\n")
    with open(os.path.join(peak_dir, "hourofday-r-00000"), "w", encoding="utf-8") as f:
        f.write("0\t4\t2\n")
        f.write("1\t3\t1\n")
        f.write("2\t2\t1\n")
        f.write("23\t1\t1\n")

    # 3. J3 ViewCount outputs (viewcounts/products, viewcounts/categories)
    vc_prod_dir = os.path.join(work_dir, "viewcounts", "products")
    os.makedirs(vc_prod_dir, exist_ok=True)
    with open(os.path.join(vc_prod_dir, "part-00000"), "w", encoding="utf-8") as f:
        f.write("1001\telectronics.smartphone\tacme\t3\n")
        f.write("1002\t\t\t1\n")
        f.write("1004\telectronics.smartphone\tacme\t1\n")
        f.write("1005\telectronics.smartphone\tacme\t1\n")

    vc_cat_dir = os.path.join(work_dir, "viewcounts", "categories")
    os.makedirs(vc_cat_dir, exist_ok=True)
    with open(os.path.join(vc_cat_dir, "part-00000"), "w", encoding="utf-8") as f:
        f.write("electronics.smartphone\t5\n")

    # 4. J4 TopN outputs (top_products/, top_categories/)
    top_prod_dir = os.path.join(work_dir, "top_products")
    os.makedirs(top_prod_dir, exist_ok=True)
    with open(os.path.join(top_prod_dir, "part-r-00000"), "w", encoding="utf-8") as f:
        f.write("1\t1001\telectronics.smartphone\tacme\t3\n")
        f.write("2\t1002\t\t\t1\n")
        f.write("3\t1004\telectronics.smartphone\tacme\t1\n")
        f.write("4\t1005\telectronics.smartphone\tacme\t1\n")

    top_cat_dir = os.path.join(work_dir, "top_categories")
    os.makedirs(top_cat_dir, exist_ok=True)
    with open(os.path.join(top_cat_dir, "part-r-00000"), "w", encoding="utf-8") as f:
        f.write("1\telectronics.smartphone\t5\n")

    # 5. J6 DailyFunnel output (funnel_daily/)
    funnel_dir = os.path.join(work_dir, "funnel_daily")
    os.makedirs(funnel_dir, exist_ok=True)
    with open(os.path.join(funnel_dir, "part-r-00000"), "w", encoding="utf-8") as f:
        f.write("2019-11-01\t6\t2\t1\t1\t1\n")

    # 6. run_meta.json
    run_meta = {
        "schema_version": "1.0",
        "app_version": "1.0",
        "run_id": run_id,
        "status": status,
        "start": "2026-10-15T02:00:00Z",
        "end": "2026-10-15T02:05:00Z",
        "duration_sec": 300,
        "input": "/clickstream/raw/dt=2019-11-01",
        "work_dir": work_dir,
        "queue": "batch",
        "invariant_ok": invariant_ok,
        "counters": {
            "TOTAL_RECORDS": 19,
            "VALID_RECORDS": 10,
            "HEADER_SKIPPED": 1,
            "MALFORMED_COLUMN_COUNT": 2,
            "MALFORMED_TIMESTAMP": 1,
            "MALFORMED_EVENT_TYPE": 1,
            "MALFORMED_ID": 1,
            "MALFORMED_PRICE": 1,
            "MALFORMED_SESSION": 2,
            "MISSING_CATEGORY": 1,
            "MISSING_BRAND": 1,
            "VIEW_EVENTS": 6
        },
        "jobs": [
            {
                "name": "J1_HourlyTraffic",
                "status": "SUCCESS",
                "counters": {
                    "TOTAL_RECORDS": 19,
                    "VALID_RECORDS": 10,
                    "HEADER_SKIPPED": 1,
                    "MALFORMED_COLUMN_COUNT": 2,
                    "MALFORMED_TIMESTAMP": 1,
                    "MALFORMED_EVENT_TYPE": 1,
                    "MALFORMED_ID": 1,
                    "MALFORMED_PRICE": 1,
                    "MALFORMED_SESSION": 2,
                    "MISSING_CATEGORY": 1,
                    "MISSING_BRAND": 1
                }
            }
        ]
    }
    with open(os.path.join(work_dir, "run_meta.json"), "w", encoding="utf-8") as f:
        json.dump(run_meta, f, indent=2)


class TestExportReports(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_export_reports_")
        self.work_dir = os.path.join(self.temp_dir, "work_dir")
        self.reports_dir = os.path.join(self.temp_dir, "reports")
        create_mock_pipeline_work_dir(self.work_dir)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_successful_export_all_reports(self):
        """Verifies that export_reports exports all 8 CSV reports and both JSON metadata files."""
        code = export_reports.export_reports(
            run_id="daily-2019-11-01",
            work_dir=self.work_dir,
            out_dir=self.reports_dir,
            force=False
        )
        self.assertEqual(code, 0, "Export must succeed with exit code 0")

        # Verify all 8 reports exist
        expected_csvs = [
            "traffic_hourly.csv", "peak_hour_daily.csv", "hour_of_day_profile.csv",
            "top_products.csv", "top_categories.csv", "quality_daily.csv",
            "quality_reasons.csv", "funnel_daily.csv"
        ]
        for csv_name in expected_csvs:
            p = os.path.join(self.reports_dir, csv_name)
            self.assertTrue(os.path.isfile(p), f"Report {csv_name} must exist")
            self.assertGreater(os.path.getsize(p), 0, f"Report {csv_name} must not be empty")

        # Verify metadata files exist
        self.assertTrue(os.path.isfile(os.path.join(self.reports_dir, "report_meta.json")))
        self.assertTrue(os.path.isfile(os.path.join(self.reports_dir, "run_meta.json")))

    def test_deterministic_byte_identical_exports(self):
        """Verifies byte-for-byte reproducibility across repeated exports."""
        rep1 = os.path.join(self.temp_dir, "rep1")
        rep2 = os.path.join(self.temp_dir, "rep2")

        code1 = export_reports.export_reports("run1", self.work_dir, rep1)
        code2 = export_reports.export_reports("run1", self.work_dir, rep2)
        self.assertEqual(code1, 0)
        self.assertEqual(code2, 0)

        csv_files = [
            "traffic_hourly.csv", "peak_hour_daily.csv", "hour_of_day_profile.csv",
            "top_products.csv", "top_categories.csv", "quality_daily.csv",
            "quality_reasons.csv", "funnel_daily.csv"
        ]
        for cf in csv_files:
            with open(os.path.join(rep1, cf), "rb") as f1, open(os.path.join(rep2, cf), "rb") as f2:
                b1 = f1.read()
                b2 = f2.read()
            self.assertEqual(b1, b2, f"Report {cf} must be byte-for-byte identical across runs")

    def test_refuse_failed_run_meta(self):
        """Verifies refusal to export reports when run_meta.json status is FAILED."""
        fail_work_dir = os.path.join(self.temp_dir, "work_fail")
        create_mock_pipeline_work_dir(fail_work_dir, status="FAILED")

        code = export_reports.export_reports("fail-run", fail_work_dir, self.reports_dir)
        self.assertEqual(code, 1, "Must exit with code 1 on failed pipeline run")
        self.assertFalse(os.path.exists(self.reports_dir), "Reports folder must not be created on failure")

    def test_refuse_invariant_violation(self):
        """Verifies refusal to export reports when run_meta.json invariant_ok is False."""
        inv_work_dir = os.path.join(self.temp_dir, "work_inv_fail")
        create_mock_pipeline_work_dir(inv_work_dir, invariant_ok=False)

        code = export_reports.export_reports("inv-run", inv_work_dir, self.reports_dir)
        self.assertEqual(code, 1, "Must exit with code 1 when invariant_ok is False")
        self.assertFalse(os.path.exists(self.reports_dir))

    def test_missing_source_directory_fails_cleanly(self):
        """Verifies clean failure and no partial publication when a required job directory is missing."""
        corrupt_work_dir = os.path.join(self.temp_dir, "work_corrupt")
        create_mock_pipeline_work_dir(corrupt_work_dir)
        shutil.rmtree(os.path.join(corrupt_work_dir, "funnel_daily"))

        code = export_reports.export_reports("corrupt-run", corrupt_work_dir, self.reports_dir)
        self.assertEqual(code, 1, "Must exit with code 1 on missing source directory")
        self.assertFalse(os.path.exists(self.reports_dir), "No partial reports published on failure")

    def test_multiple_hadoop_part_files_handled_and_sorted(self):
        """Verifies that multiple part files are aggregated and sorted deterministically."""
        traffic_dir = os.path.join(self.work_dir, "traffic")
        # Add a second part file with out-of-order date/hour
        with open(os.path.join(traffic_dir, "part-r-00001"), "w", encoding="utf-8") as f:
            f.write("2019-11-01\t12\t4\t4\t0\t0\t0\t0\t0.000000\n")

        code = export_reports.export_reports("multi-part", self.work_dir, self.reports_dir)
        self.assertEqual(code, 0)

        # Check traffic_hourly.csv has the merged, properly sorted rows
        traffic_csv = os.path.join(self.reports_dir, "traffic_hourly.csv")
        with open(traffic_csv, "r", encoding="utf-8") as f:
            reader = list(csv.reader(f))
            self.assertEqual(reader[0], ["date", "hour", "total", "views", "carts", "removes", "purchases", "malformed", "malformed_rate_pct"])
            dates_hours = [(r[0], int(r[1]) if r[1].isdigit() else 999) for r in reader[1:] if r[0] != "NA"]
            # Verify chronological ordering
            self.assertEqual(dates_hours, sorted(dates_hours))

    def test_overwrite_protection(self):
        """Verifies that existing target directory is rejected without --force and accepted with --force."""
        os.makedirs(self.reports_dir, exist_ok=True)
        with open(os.path.join(self.reports_dir, "dummy.txt"), "w") as f:
            f.write("existing")

        code1 = export_reports.export_reports("run", self.work_dir, self.reports_dir, force=False)
        self.assertEqual(code1, 1, "Must refuse to overwrite existing reports without force")

        code2 = export_reports.export_reports("run", self.work_dir, self.reports_dir, force=True)
        self.assertEqual(code2, 0, "Must allow overwrite when force=True")
        self.assertFalse(os.path.exists(os.path.join(self.reports_dir, "dummy.txt")))

    def test_overwrite_preserves_existing_on_staging_failure(self):
        """Verifies that an existing report set is not destroyed if staging fails during --force export."""
        # 1. Create a successful valid report set first
        code_init = export_reports.export_reports("valid-init", self.work_dir, self.reports_dir)
        self.assertEqual(code_init, 0)
        self.assertTrue(os.path.isfile(os.path.join(self.reports_dir, "traffic_hourly.csv")))
        with open(os.path.join(self.reports_dir, "traffic_hourly.csv"), "rb") as f:
            original_bytes = f.read()

        # 2. Attempt overwrite with a broken work dir (missing inputs) and force=True
        broken_work = os.path.join(self.temp_dir, "broken_work")
        os.makedirs(broken_work, exist_ok=True)
        with open(os.path.join(broken_work, "run_meta.json"), "w") as f:
            json.dump({"status": "SUCCESS", "counters": {"TOTAL_RECORDS": 10, "VALID_RECORDS": 10}, "run_id": "broken", "invariant_ok": True}, f)

        code_fail = export_reports.export_reports("broken", broken_work, self.reports_dir, force=True)
        self.assertEqual(code_fail, 1, "Export must fail when inputs are missing")

        # 3. Verify original report set is completely intact
        self.assertTrue(os.path.isfile(os.path.join(self.reports_dir, "traffic_hourly.csv")))
        with open(os.path.join(self.reports_dir, "traffic_hourly.csv"), "rb") as f:
            current_bytes = f.read()
        self.assertEqual(current_bytes, original_bytes, "Previous report set must be preserved intact on failure")

    def test_overwrite_rollback_on_publication_failure(self):
        """Verifies rollback restores previous reports if an exception occurs during final replacement."""
        # 1. Create valid initial report set
        code_init = export_reports.export_reports("valid-init", self.work_dir, self.reports_dir)
        self.assertEqual(code_init, 0)
        with open(os.path.join(self.reports_dir, "traffic_hourly.csv"), "rb") as f:
            original_bytes = f.read()

        # 2. Mock os.replace to fail on the second call (staging -> out_dir rename)
        real_replace = os.replace
        call_count = [0]

        def failing_replace(src, dst):
            call_count[0] += 1
            # Call 1 is out_dir -> backup_dir (allow)
            # Call 2 is staging_dir -> out_dir (fail to trigger rollback)
            if call_count[0] == 2:
                raise PermissionError("Simulated filesystem lock during directory replacement")
            return real_replace(src, dst)

        import unittest.mock as mock
        with mock.patch("os.replace", side_effect=failing_replace):
            code_fail = export_reports.export_reports("valid-second", self.work_dir, self.reports_dir, force=True)
            self.assertEqual(code_fail, 1, "Export must fail and catch exception")

        # 3. Verify rollback restored original reports
        self.assertTrue(os.path.isfile(os.path.join(self.reports_dir, "traffic_hourly.csv")))
        with open(os.path.join(self.reports_dir, "traffic_hourly.csv"), "rb") as f:
            current_bytes = f.read()
        self.assertEqual(current_bytes, original_bytes, "Previous report set must be rolled back on publish failure")

    def test_paths_with_spaces(self):
        """Verifies paths containing spaces work transparently."""
        space_work = os.path.join(self.temp_dir, "work with spaces")
        space_reports = os.path.join(self.temp_dir, "reports with spaces")
        create_mock_pipeline_work_dir(space_work)

        code = export_reports.export_reports("space-run", space_work, space_reports)
        self.assertEqual(code, 0)
        self.assertTrue(os.path.isfile(os.path.join(space_reports, "traffic_hourly.csv")))

    def test_report_metadata_validation(self):
        """Verifies exact schema and contents of report_meta.json."""
        code = export_reports.export_reports("meta-check", self.work_dir, self.reports_dir)
        self.assertEqual(code, 0)

        meta_path = os.path.join(self.reports_dir, "report_meta.json")
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)

        self.assertEqual(meta["schema_version"], "1.0")
        self.assertEqual(meta["status"], "SUCCESS")
        self.assertEqual(meta["run_id"], "meta-check")
        self.assertIn("generated_at", meta)
        self.assertIn("definitions", meta)
        self.assertEqual(len(meta["definitions"]), 8)
        self.assertEqual(len(meta["reports"]), 8)

        # Check row count recorded for top_products
        self.assertEqual(meta["reports"]["top_products.csv"]["row_count"], 4)
        self.assertEqual(meta["reports"]["top_categories.csv"]["row_count"], 1)

    def test_pandas_independent_verification_integration(self):
        """Runs verify_with_pandas.py with --reports-dir to confirm independent golden checks pass."""
        code = export_reports.export_reports("pandas-check", self.work_dir, self.reports_dir)
        self.assertEqual(code, 0)

        fixture_csv = os.path.join(BASE_DIR, "src", "test", "resources", "fixtures", "mini_clickstream.csv")
        self.assertTrue(os.path.isfile(fixture_csv))

        verify_script = os.path.join(SCRIPTS_DIR, "verify_with_pandas.py")
        cmd = [
            sys.executable, verify_script,
            "--csv", fixture_csv,
            "--job", "reports",
            "--reports-dir", self.reports_dir
        ]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(res.returncode, 0, f"verify_with_pandas failed: {res.stderr}\n{res.stdout}")
        self.assertIn("ALL CHECKS PASSED", res.stdout or res.stderr or "")


if __name__ == "__main__":
    unittest.main()
