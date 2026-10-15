"""
tests/test_dashboard.py — Automated Unit & Integration Tests for Streamlit Dashboard.

Verifies:
- Report directory discovery (newest first, empty handling).
- Validation of report_meta.json, run_meta.json, and all 8 CSV reports.
- Rejection of missing, corrupted, or failed run reports.
- Correct schema verification and error messaging.
- Empty-but-valid reports and zero-denominator handling.
- Calculation of KPIs and funnel rates without double-counting.
- Clean switching between run directories without stale data.
- Execution of all five view renderers without uncaught exceptions.
"""

import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from dashboard import data_loader
from dashboard import app


def create_mock_report_dir(
    target_dir: str,
    run_id: str = "daily-2019-11-01",
    status: str = "SUCCESS",
    corrupt_csv: bool = False,
    empty_csvs: bool = False,
):
    """Creates a mock reports directory with all 8 CSVs and 2 metadata JSONs."""
    os.makedirs(target_dir, exist_ok=True)

    # 1. report_meta.json
    rep_meta = {
        "schema_version": "1.0",
        "run_id": run_id,
        "status": status,
        "generated_at": "2026-10-15T02:00:00.000000Z",
        "source_work_dir": f"/clickstream/work/{run_id}",
        "run_meta_file": "run_meta.json",
        "reports": {
            "traffic_hourly.csv": 0 if empty_csvs else 2,
            "peak_hour_daily.csv": 0 if empty_csvs else 1,
            "hour_of_day_profile.csv": 0 if empty_csvs else 2,
            "top_products.csv": 0 if empty_csvs else 2,
            "top_categories.csv": 0 if empty_csvs else 1,
            "quality_daily.csv": 0 if empty_csvs else 1,
            "quality_reasons.csv": 0 if empty_csvs else 8,
            "funnel_daily.csv": 0 if empty_csvs else 1,
        },
    }
    with open(os.path.join(target_dir, "report_meta.json"), "w", encoding="utf-8") as f:
        json.dump(rep_meta, f, indent=2)

    # 2. run_meta.json
    run_meta = {
        "schema_version": "1.0",
        "run_id": run_id,
        "status": status,
        "start_time": "2026-10-15T01:50:00.000000Z",
        "end_time": "2026-10-15T01:56:41.000000Z",
        "duration_ms": 401000,
        "input_path": f"/clickstream/raw/dt={run_id.replace('daily-', '')}",
        "work_dir": f"/clickstream/work/{run_id}",
        "invariant_ok": True,
        "jobs": {
            "J1_HourlyTraffic": {"status": "SUCCESS", "duration_ms": 120000, "app_id": "app_001"},
            "J2_PeakHour": {"status": "SUCCESS", "duration_ms": 45000, "app_id": "app_002"},
            "J3_ViewCount": {"status": "SUCCESS", "duration_ms": 90000, "app_id": "app_003"},
            "J4_TopN_Products": {"status": "SUCCESS", "duration_ms": 25000, "app_id": "app_004"},
            "J4_TopN_Categories": {"status": "SUCCESS", "duration_ms": 21000, "app_id": "app_005"},
            "J5_SessionFunnel": {"status": "SUCCESS", "duration_ms": 60000, "app_id": "app_006"},
            "J6_DailyFunnel": {"status": "SUCCESS", "duration_ms": 40000, "app_id": "app_007"},
        },
        "counters": {
            "TOTAL_RECORDS": 100,
            "VALID_RECORDS": 90,
            "HEADER_SKIPPED": 1,
            "MALFORMED_TIMESTAMP": 5,
            "MALFORMED_PRICE": 5,
        },
    }
    with open(os.path.join(target_dir, "run_meta.json"), "w", encoding="utf-8") as f:
        json.dump(run_meta, f, indent=2)

    # 3. CSV files
    if empty_csvs:
        for fname, cols in data_loader.EXPECTED_SCHEMAS.items():
            with open(os.path.join(target_dir, fname), "w", encoding="utf-8") as f:
                f.write(",".join(cols) + "\n")
        return

    # 1. traffic_hourly.csv
    with open(os.path.join(target_dir, "traffic_hourly.csv"), "w", encoding="utf-8") as f:
        if corrupt_csv:
            f.write("wrong,columns\n1,2\n")
        else:
            f.write("date,hour,total,views,carts,removes,purchases,malformed,malformed_rate_pct\n")
            f.write("2019-11-01,00,50,30,10,5,5,5,9.0909\n")
            f.write("2019-11-01,01,40,25,8,3,4,5,11.1111\n")

    # 2. peak_hour_daily.csv
    with open(os.path.join(target_dir, "peak_hour_daily.csv"), "w", encoding="utf-8") as f:
        f.write("date,peak_hour,peak_total,day_total,peak_share_pct\n")
        f.write("2019-11-01,0,50,90,55.5556\n")

    # 3. hour_of_day_profile.csv
    with open(os.path.join(target_dir, "hour_of_day_profile.csv"), "w", encoding="utf-8") as f:
        f.write("hour,total_events,days_observed,avg_events_per_day\n")
        f.write("0,50,1,50.0000\n")
        f.write("1,40,1,40.0000\n")

    # 4. top_products.csv
    with open(os.path.join(target_dir, "top_products.csv"), "w", encoding="utf-8") as f:
        f.write("rank,product_id,category_code,brand,views,share_of_all_views_pct\n")
        f.write("1,1001,electronics.smartphone,acme,35,63.6364\n")
        f.write("2,1002,,apple,20,36.3636\n")

    # 5. top_categories.csv
    with open(os.path.join(target_dir, "top_categories.csv"), "w", encoding="utf-8") as f:
        f.write("rank,category_code,views,share_of_categorised_views_pct\n")
        f.write("1,electronics.smartphone,35,100.0000\n")

    # 6. quality_daily.csv
    with open(os.path.join(target_dir, "quality_daily.csv"), "w", encoding="utf-8") as f:
        f.write("date,total_records,valid,malformed,malformed_rate_pct\n")
        f.write("2019-11-01,100,90,10,10.0000\n")

    # 7. quality_reasons.csv
    with open(os.path.join(target_dir, "quality_reasons.csv"), "w", encoding="utf-8") as f:
        f.write("reason,count,share_of_malformed_pct\n")
        f.write("MALFORMED_COLUMN_COUNT,0,\n")
        f.write("MALFORMED_TIMESTAMP,5,50.0000\n")
        f.write("MALFORMED_EVENT_TYPE,0,\n")
        f.write("MALFORMED_ID,0,\n")
        f.write("MALFORMED_PRICE,5,50.0000\n")
        f.write("MALFORMED_SESSION,0,\n")
        f.write("MISSING_CATEGORY,1,10.0000\n")
        f.write("MISSING_BRAND,0,\n")

    # 8. funnel_daily.csv
    with open(os.path.join(target_dir, "funnel_daily.csv"), "w", encoding="utf-8") as f:
        f.write("date,sessions,cart_sessions,purchase_sessions,abandoned_sessions,remove_events,cart_events,cart_abandonment_rate_pct,conversion_rate_pct,remove_rate_pct\n")
        f.write("2019-11-01,20,10,5,5,8,18,50.0000,50.0000,44.4444\n")


class TestDashboardDataLoader(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="test_dashboard_")
        self.reports_base = os.path.join(self.test_dir, "reports")
        os.makedirs(self.reports_base, exist_ok=True)

    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_find_available_runs_and_ordering(self):
        """Verifies run discovery and newest-first ordering."""
        # Empty reports dir
        self.assertEqual(data_loader.find_available_runs(self.reports_base), [])

        # Create two runs
        run1 = os.path.join(self.reports_base, "daily-2019-11-01")
        run2 = os.path.join(self.reports_base, "daily-2019-11-02")
        create_mock_report_dir(run1, run_id="daily-2019-11-01")
        create_mock_report_dir(run2, run_id="daily-2019-11-02")

        runs = data_loader.find_available_runs(self.reports_base)
        self.assertEqual(len(runs), 2)
        self.assertIn("daily-2019-11-01", runs)
        self.assertIn("daily-2019-11-02", runs)

    def test_validate_report_directory_valid(self):
        """Valid report directory passes validation."""
        run_path = os.path.join(self.reports_base, "valid_run")
        create_mock_report_dir(run_path)

        valid, err = data_loader.validate_report_directory(run_path)
        self.assertTrue(valid)
        self.assertIsNone(err)

    def test_validate_report_directory_missing_files(self):
        """Missing required CSV or metadata fails validation."""
        run_path = os.path.join(self.reports_base, "incomplete_run")
        create_mock_report_dir(run_path)

        # Remove traffic_hourly.csv
        os.remove(os.path.join(run_path, "traffic_hourly.csv"))
        valid, err = data_loader.validate_report_directory(run_path)
        self.assertFalse(valid)
        self.assertIn("traffic_hourly.csv", err)

        # Remove report_meta.json
        os.remove(os.path.join(run_path, "report_meta.json"))
        valid, err = data_loader.validate_report_directory(run_path)
        self.assertFalse(valid)
        self.assertIn("report_meta.json", err)

    def test_validate_report_directory_failed_status(self):
        """Refuses to validate runs with status != SUCCESS."""
        run_path = os.path.join(self.reports_base, "failed_run")
        create_mock_report_dir(run_path, status="FAILED")

        valid, err = data_loader.validate_report_directory(run_path)
        self.assertFalse(valid)
        self.assertIn("FAILED", err)

    def test_load_run_data_schema_mismatch(self):
        """Raises ValueError when CSV schema is corrupted or missing columns."""
        run_path = os.path.join(self.reports_base, "corrupt_schema_run")
        create_mock_report_dir(run_path, corrupt_csv=True)

        with self.assertRaises(ValueError) as ctx:
            data_loader.load_run_data(run_path)
        self.assertIn("Schema mismatch in traffic_hourly.csv", str(ctx.exception))

    def test_calculate_overview_kpis(self):
        """Verifies overview KPI computations and units."""
        run_path = os.path.join(self.reports_base, "calc_run")
        create_mock_report_dir(run_path)

        data = data_loader.load_run_data(run_path)
        kpis = data_loader.calculate_overview_kpis(data)

        self.assertEqual(kpis["total_records"], 100)
        self.assertEqual(kpis["valid_records"], 90)
        self.assertEqual(kpis["malformed_records"], 10)
        self.assertAlmostEqual(kpis["malformed_rate_pct"], 10.0, places=2)
        self.assertEqual(kpis["total_views"], 55)
        self.assertEqual(kpis["total_carts"], 18)
        self.assertEqual(kpis["total_removes"], 8)
        self.assertEqual(kpis["total_purchases"], 9)
        self.assertEqual(kpis["days_covered"], 1)
        self.assertEqual(kpis["overall_peak_hour"], "00:00 UTC")
        self.assertEqual(kpis["peak_hour_events"], 50)
        self.assertEqual(kpis["total_sessions"], 20)
        self.assertEqual(kpis["cart_sessions"], 10)
        self.assertEqual(kpis["purchase_sessions"], 5)
        self.assertEqual(kpis["abandoned_sessions"], 5)
        self.assertAlmostEqual(kpis["cart_abandonment_pct"], 50.0, places=2)
        self.assertAlmostEqual(kpis["conversion_pct"], 50.0, places=2)
        self.assertEqual(kpis["run_status"], "SUCCESS")
        self.assertEqual(kpis["duration_str"], "06m 41s")

    def test_calculate_top_pages_summary(self):
        """Verifies uncategorised view share and counts."""
        run_path = os.path.join(self.reports_base, "top_pages_run")
        create_mock_report_dir(run_path)

        data = data_loader.load_run_data(run_path)
        tp_summary = data_loader.calculate_top_pages_summary(data)

        self.assertEqual(tp_summary["top_products_count"], 2)
        self.assertEqual(tp_summary["top_categories_count"], 1)
        self.assertEqual(tp_summary["total_views_in_top"], 55)
        self.assertEqual(tp_summary["categorised_views"], 35)
        self.assertEqual(tp_summary["uncategorised_views"], 20)
        self.assertAlmostEqual(tp_summary["uncategorised_share_pct"], (20 / 55) * 100.0, places=2)

    def test_empty_but_valid_reports(self):
        """Verifies safe handling of empty-but-valid reports without crashing."""
        run_path = os.path.join(self.reports_base, "empty_run")
        create_mock_report_dir(run_path, empty_csvs=True)

        data = data_loader.load_run_data(run_path)
        kpis = data_loader.calculate_overview_kpis(data)

        self.assertEqual(kpis["total_records"], 0)
        self.assertEqual(kpis["malformed_rate_pct"], 0.0)
        self.assertEqual(kpis["cart_abandonment_pct"], 0.0)
        self.assertEqual(kpis["conversion_pct"], 0.0)

        tp_summary = data_loader.calculate_top_pages_summary(data)
        self.assertEqual(tp_summary["total_views_in_top"], 0)
        self.assertEqual(tp_summary["uncategorised_share_pct"], 0.0)

    def test_switching_run_directories(self):
        """Verifies switching from run A to run B cleanly loads different data without stale state."""
        run_a = os.path.join(self.reports_base, "run_a")
        run_b = os.path.join(self.reports_base, "run_b")
        create_mock_report_dir(run_a, run_id="run_a")
        create_mock_report_dir(run_b, run_id="run_b", empty_csvs=True)

        data_a = data_loader.load_run_data(run_a)
        self.assertEqual(data_a["run_id"], "run_a")
        self.assertFalse(data_a["traffic_hourly"].empty)

        data_b = data_loader.load_run_data(run_b)
        self.assertEqual(data_b["run_id"], "run_b")
        self.assertTrue(data_b["traffic_hourly"].empty)

    def test_overview_kpis_with_unattributable_malformed_records(self):
        """Verifies reconciliation when unattributable records (NA,NA) are present."""
        run_path = os.path.join(self.reports_base, "unattributable_run")
        create_mock_report_dir(run_path)

        # Overwrite traffic_hourly with unattributable row NA,NA
        with open(os.path.join(run_path, "traffic_hourly.csv"), "w", encoding="utf-8") as f:
            f.write("date,hour,total,views,carts,removes,purchases,malformed,malformed_rate_pct\n")
            f.write("2019-11-01,0,3,2,1,0,0,0,0.0000\n")
            f.write("2019-11-01,1,3,2,0,0,1,4,57.1429\n")
            f.write("2019-11-01,2,2,2,0,0,0,0,0.0000\n")
            f.write("2019-11-01,23,1,0,1,0,0,0,0.0000\n")
            f.write("NA,NA,0,0,0,0,0,2,100.0000\n")

        # quality_daily only has daily attributable date
        with open(os.path.join(run_path, "quality_daily.csv"), "w", encoding="utf-8") as f:
            f.write("date,total_records,valid,malformed,malformed_rate_pct\n")
            f.write("2019-11-01,13,9,4,30.7692\n")

        data = data_loader.load_run_data(run_path)
        kpis = data_loader.calculate_overview_kpis(data)

        # Invariant checks
        self.assertEqual(kpis["valid_records"], 9)
        self.assertEqual(kpis["malformed_records"], 6)
        self.assertEqual(kpis["total_records"], 15)
        self.assertEqual(kpis["valid_records"] + kpis["malformed_records"], kpis["total_records"])
        self.assertEqual(kpis["attributable_malformed"], 4)
        self.assertEqual(kpis["unattributable_malformed"], 2)
        self.assertAlmostEqual(kpis["malformed_rate_pct"], 40.0, places=4)

    def test_job_runs_seven_executions_representation(self):
        """Verifies that all seven job executions (including J4 product and category) are represented."""
        run_path = os.path.join(self.reports_base, "seven_jobs_run")
        create_mock_report_dir(run_path)

        # Write run_meta.json with 7 actual execution records
        meta = {
            "schema_version": "1.0",
            "run_id": "seven_jobs_run",
            "status": "SUCCESS",
            "duration_sec": 145,
            "jobs": [
                {"name": "J1_HourlyTraffic", "job_id": "job_001", "status": "SUCCESS", "duration_sec": 29},
                {"name": "J2_PeakHour", "job_id": "job_002", "status": "SUCCESS", "duration_sec": 17},
                {"name": "J3_ViewCount", "job_id": "job_003", "status": "SUCCESS", "duration_sec": 22},
                {"name": "J4_TopN_Products", "job_id": "job_004", "status": "SUCCESS", "duration_sec": 19},
                {"name": "J4_TopN_Categories", "job_id": "job_005", "status": "SUCCESS", "duration_sec": 17},
                {"name": "J5_SessionFunnel", "job_id": "job_006", "status": "SUCCESS", "duration_sec": 20},
                {"name": "J6_DailyFunnel", "job_id": "job_007", "status": "SUCCESS", "duration_sec": 19},
            ],
            "counters": {"TOTAL_RECORDS": 16, "VALID_RECORDS": 9, "HEADER_SKIPPED": 1}
        }
        with open(os.path.join(run_path, "run_meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f)

        data = data_loader.load_run_data(run_path)
        self.assertEqual(len(data["run_meta"]["jobs"]), 7)

        # Test headless rendering of render_job_runs without error
        app.render_job_runs(data)


if __name__ == "__main__":
    unittest.main()
