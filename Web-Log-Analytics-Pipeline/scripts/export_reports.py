#!/usr/bin/env python3
"""
scripts/export_reports.py — Production Exporter for Batch MapReduce Clickstream Reports.

Reads MapReduce output directories (J1–J6) and run_meta.json from a pipeline run's work directory,
computes derived metrics per SRS §3.5, and safely publishes eight standardized, byte-reproducible CSV
reports plus report_meta.json and a replica of run_meta.json.

Reports exported:
  1. traffic_hourly.csv
  2. peak_hour_daily.csv
  3. hour_of_day_profile.csv
  4. top_products.csv
  5. top_categories.csv
  6. quality_daily.csv
  7. quality_reasons.csv
  8. funnel_daily.csv

Authoritative requirements:
  - SRS §3.3.11, §3.5, §3.5.6
  - PRD §6.1, §6.2, §6.3
"""

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

REPORT_DEFINITIONS = {
    "traffic_hourly": "Hourly event counts (views, carts, removes, purchases) and malformed error rate per date and hour bucket (0-23 UTC).",
    "peak_hour_daily": "Hour of maximum traffic per day, its event volume, and its percentage share of day total.",
    "hour_of_day_profile": "Aggregated events per hour across all observed dates, and average events per day.",
    "top_products": "Top-N most viewed products ranked by view count with percentage share of all view events.",
    "top_categories": "Top-N most viewed categories ranked by view count with percentage share of categorised view events.",
    "quality_daily": "Daily data-quality summary showing total records, valid records, malformed records, and malformed rate.",
    "quality_reasons": "Breakdown of record quality counter counts by specific parser failure reason across the run.",
    "funnel_daily": "Daily behavioural conversion funnel metrics: sessions, cart sessions, purchase sessions, abandonment rate, and remove rate."
}

QUALITY_REASON_COUNTERS = [
    "MALFORMED_COLUMN_COUNT",
    "MALFORMED_TIMESTAMP",
    "MALFORMED_EVENT_TYPE",
    "MALFORMED_ID",
    "MALFORMED_PRICE",
    "MALFORMED_SESSION",
    "MISSING_CATEGORY",
    "MISSING_BRAND"
]


def list_part_files(dir_path: str, prefix_filter: Optional[str] = None) -> List[str]:
    """
    Lists non-hidden, non-metadata data files in a directory, sorted lexicographically.
    Ignores _SUCCESS, .crc, .* files.
    """
    if not os.path.isdir(dir_path):
        return []
    
    files = []
    for entry in sorted(os.listdir(dir_path)):
        if entry.startswith(".") or entry.startswith("_"):
            continue
        if prefix_filter and not entry.startswith(prefix_filter):
            continue
        full_path = os.path.join(dir_path, entry)
        if os.path.isfile(full_path):
            files.append(full_path)
    return files


def stream_tab_lines(dir_path: str, prefix_filter: Optional[str] = None) -> Generator[List[str], None, None]:
    """
    Yields tab-separated fields line-by-line from all matching part files in dir_path.
    Streams line-by-line with O(1) memory.
    """
    part_files = list_part_files(dir_path, prefix_filter)
    if not part_files:
        # Check if dir_path is HDFS or if HDFS CLI fallback is applicable
        if dir_path.startswith("hdfs://") or not os.path.exists(dir_path):
            try:
                cmd = ["hdfs", "dfs", "-cat", f"{dir_path}/*"]
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8")
                assert proc.stdout is not None
                for line in proc.stdout:
                    line = line.strip("\r\n")
                    if not line:
                        continue
                    yield line.split("\t")
                proc.wait()
                return
            except Exception:
                pass
        return

    for fp in part_files:
        with open(fp, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip("\r\n")
                if not line:
                    continue
                yield line.split("\t")


def safe_div_pct(numerator: float, denominator: float) -> str:
    """Calculates (numerator / denominator) * 100 with 4 decimals. Returns empty string on zero denominator."""
    if denominator == 0:
        return ""
    val = (numerator / denominator) * 100.0
    return f"{val:.4f}"


def safe_div(numerator: float, denominator: float) -> str:
    """Calculates (numerator / denominator) with 4 decimals. Returns empty string on zero denominator."""
    if denominator == 0:
        return ""
    val = numerator / denominator
    return f"{val:.4f}"


def export_traffic_hourly(traffic_dir: str, out_path: str) -> Tuple[int, Dict[str, Dict[str, int]]]:
    """
    Exports traffic_hourly.csv from J1 output.
    Header: date,hour,total,views,carts,removes,purchases,malformed,malformed_rate_pct
    Also accumulates and returns (row_count, traffic_by_date).
    """
    rows = []
    traffic_by_date: Dict[str, Dict[str, int]] = {}

    found_any = False
    for parts in stream_tab_lines(traffic_dir):
        found_any = True
        if len(parts) < 8:
            continue
        date_str = parts[0].strip()
        hour_str = parts[1].strip()
        total = int(parts[2].strip())
        views = int(parts[3].strip())
        carts = int(parts[4].strip())
        removes = int(parts[5].strip())
        purchases = int(parts[6].strip())
        malformed = int(parts[7].strip())

        denom = total + malformed
        rate_str = safe_div_pct(malformed, denom)

        # Parse hour for numeric sorting
        hour_sort_key = int(hour_str) if hour_str.isdigit() else 999
        date_sort_key = date_str if date_str != "NA" else "zzzz-zz-zz"

        rows.append((date_sort_key, hour_sort_key, [
            date_str, hour_str, str(total), str(views), str(carts),
            str(removes), str(purchases), str(malformed), rate_str
        ]))

        # Accumulate daily summary (excluding NA from daily attribution if unattributable)
        if date_str != "NA":
            if date_str not in traffic_by_date:
                traffic_by_date[date_str] = {"total": 0, "malformed": 0, "carts": 0}
            traffic_by_date[date_str]["total"] += total
            traffic_by_date[date_str]["malformed"] += malformed
            traffic_by_date[date_str]["carts"] += carts

    if not found_any:
        raise ValueError(f"Missing or empty source outputs in traffic directory: {traffic_dir}")

    # Deterministic sort: date asc, hour asc
    rows.sort(key=lambda x: (x[0], x[1]))

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["date", "hour", "total", "views", "carts", "removes", "purchases", "malformed", "malformed_rate_pct"])
        for _, _, cols in rows:
            writer.writerow(cols)

    return len(rows), traffic_by_date


def export_peak_hour_daily(peak_dir: str, out_path: str) -> int:
    """
    Exports peak_hour_daily.csv from J2 peakday output.
    Header: date,peak_hour,peak_total,day_total,peak_share_pct
    """
    rows = []
    found_any = False
    for parts in stream_tab_lines(peak_dir, prefix_filter="peakday"):
        found_any = True
        if len(parts) < 4:
            continue
        date_str = parts[0].strip()
        peak_hour = parts[1].strip()
        peak_total = int(parts[2].strip())
        day_total = int(parts[3].strip())
        share_str = safe_div_pct(peak_total, day_total)

        rows.append((date_str, [date_str, peak_hour, str(peak_total), str(day_total), share_str]))

    if not found_any:
        raise ValueError(f"Missing or empty J2 peakday outputs in peak directory: {peak_dir}")

    rows.sort(key=lambda x: x[0])

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["date", "peak_hour", "peak_total", "day_total", "peak_share_pct"])
        for _, cols in rows:
            writer.writerow(cols)

    return len(rows)


def export_hour_of_day_profile(peak_dir: str, out_path: str) -> int:
    """
    Exports hour_of_day_profile.csv from J2 hourofday output.
    Header: hour,total_events,days_observed,avg_events_per_day
    """
    rows = []
    found_any = False
    for parts in stream_tab_lines(peak_dir, prefix_filter="hourofday"):
        found_any = True
        if len(parts) < 3:
            continue
        hour_str = parts[0].strip()
        total_events = int(parts[1].strip())
        days_observed = int(parts[2].strip())
        avg_str = safe_div(total_events, days_observed)

        hour_num = int(hour_str) if hour_str.isdigit() else 999
        rows.append((hour_num, [hour_str, str(total_events), str(days_observed), avg_str]))

    if not found_any:
        raise ValueError(f"Missing or empty J2 hourofday outputs in peak directory: {peak_dir}")

    rows.sort(key=lambda x: x[0])

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["hour", "total_events", "days_observed", "avg_events_per_day"])
        for _, cols in rows:
            writer.writerow(cols)

    return len(rows)


def compute_total_product_views(viewcounts_prod_dir: str, counters: Dict[str, Any]) -> int:
    """Calculates sum of all product views from J3 products part files."""
    total_views = 0
    found = False
    for parts in stream_tab_lines(viewcounts_prod_dir):
        found = True
        if len(parts) >= 4:
            total_views += int(parts[3].strip())
        elif len(parts) == 2:
            total_views += int(parts[1].strip())
    
    if not found or total_views == 0:
        # Fallback to VIEW_EVENTS counter if present
        total_views = int(counters.get("VIEW_EVENTS", counters.get("VALID_RECORDS", 0)))
    return total_views


def compute_total_category_views(viewcounts_cat_dir: str) -> int:
    """Calculates sum of all categorised views from J3 categories part files."""
    total_views = 0
    for parts in stream_tab_lines(viewcounts_cat_dir):
        if len(parts) >= 2:
            total_views += int(parts[1].strip())
    return total_views


def export_top_products(top_prod_dir: str, viewcounts_prod_dir: str, counters: Dict[str, Any], out_path: str) -> int:
    """
    Exports top_products.csv from J4 top_products output.
    Header: rank,product_id,category_code,brand,views,share_of_all_views_pct
    """
    total_views = compute_total_product_views(viewcounts_prod_dir, counters)

    rows = []
    found_any = False
    for parts in stream_tab_lines(top_prod_dir):
        found_any = True
        if len(parts) < 5:
            continue
        rank = int(parts[0].strip())
        product_id = parts[1].strip()
        category_code = parts[2].strip()
        brand = parts[3].strip()
        views = int(parts[4].strip())
        share_str = safe_div_pct(views, total_views)

        rows.append((rank, [str(rank), product_id, category_code, brand, str(views), share_str]))

    if not found_any:
        raise ValueError(f"Missing or empty source outputs in top_products directory: {top_prod_dir}")

    rows.sort(key=lambda x: x[0])

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["rank", "product_id", "category_code", "brand", "views", "share_of_all_views_pct"])
        for _, cols in rows:
            writer.writerow(cols)

    return len(rows)


def export_top_categories(top_cat_dir: str, viewcounts_cat_dir: str, out_path: str) -> int:
    """
    Exports top_categories.csv from J4 top_categories output.
    Header: rank,category_code,views,share_of_categorised_views_pct
    """
    total_cat_views = compute_total_category_views(viewcounts_cat_dir)

    rows = []
    found_any = False
    for parts in stream_tab_lines(top_cat_dir):
        found_any = True
        if len(parts) < 3:
            continue
        rank = int(parts[0].strip())
        category_code = parts[1].strip()
        views = int(parts[2].strip())
        share_str = safe_div_pct(views, total_cat_views)

        rows.append((rank, [str(rank), category_code, str(views), share_str]))

    if not found_any:
        raise ValueError(f"Missing or empty source outputs in top_categories directory: {top_cat_dir}")

    rows.sort(key=lambda x: x[0])

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["rank", "category_code", "views", "share_of_categorised_views_pct"])
        for _, cols in rows:
            writer.writerow(cols)

    return len(rows)


def export_quality_daily(traffic_by_date: Dict[str, Dict[str, int]], out_path: str) -> int:
    """
    Exports quality_daily.csv by aggregating J1 data per date.
    Header: date,total_records,valid,malformed,malformed_rate_pct
    """
    rows = []
    for date_str in sorted(traffic_by_date.keys()):
        data = traffic_by_date[date_str]
        valid = data["total"]
        malformed = data["malformed"]
        total_records = valid + malformed
        rate_str = safe_div_pct(malformed, total_records)
        rows.append([date_str, str(total_records), str(valid), str(malformed), rate_str])

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["date", "total_records", "valid", "malformed", "malformed_rate_pct"])
        for cols in rows:
            writer.writerow(cols)

    return len(rows)


def export_quality_reasons(counters: Dict[str, Any], out_path: str) -> int:
    """
    Exports quality_reasons.csv from pipeline run counters in normative SRS §3.3.3 order.
    Header: reason,count
    """
    rows = []
    for reason in QUALITY_REASON_COUNTERS:
        count = int(counters.get(reason, 0))
        rows.append([reason, str(count)])

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["reason", "count"])
        for cols in rows:
            writer.writerow(cols)

    return len(rows)


def export_funnel_daily(funnel_dir: str, traffic_by_date: Dict[str, Dict[str, int]], out_path: str) -> int:
    """
    Exports funnel_daily.csv from J6 daily funnel output and J1 cart events per date.
    Header: date,sessions,cart_sessions,purchase_sessions,abandoned_sessions,cart_abandonment_rate_pct,conversion_rate_pct,cart_events,remove_events,remove_rate_pct
    """
    rows = []
    found_any = False
    for parts in stream_tab_lines(funnel_dir):
        found_any = True
        if len(parts) < 6:
            continue
        date_str = parts[0].strip()
        sessions = int(parts[1].strip())
        cart_sessions = int(parts[2].strip())
        purchase_sessions = int(parts[3].strip())
        abandoned_sessions = int(parts[4].strip())
        remove_events = int(parts[5].strip())

        cart_events = traffic_by_date.get(date_str, {}).get("carts", 0)

        abandonment_pct = safe_div_pct(abandoned_sessions, cart_sessions)
        conversion_pct = safe_div_pct(purchase_sessions, sessions)
        remove_rate_pct = safe_div_pct(remove_events, cart_events)

        rows.append((date_str, [
            date_str, str(sessions), str(cart_sessions), str(purchase_sessions),
            str(abandoned_sessions), abandonment_pct, conversion_pct,
            str(cart_events), str(remove_events), remove_rate_pct
        ]))

    if not found_any:
        raise ValueError(f"Missing or empty source outputs in funnel_daily directory: {funnel_dir}")

    rows.sort(key=lambda x: x[0])

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow([
            "date", "sessions", "cart_sessions", "purchase_sessions", "abandoned_sessions",
            "cart_abandonment_rate_pct", "conversion_rate_pct", "cart_events", "remove_events", "remove_rate_pct"
        ])
        for _, cols in rows:
            writer.writerow(cols)

    return len(rows)


def extract_counters_from_run_meta(run_meta: Dict[str, Any]) -> Dict[str, int]:
    """Extracts whole-run QualityCounter values aggregated across all jobs in run_meta.json."""
    extracted: Dict[str, int] = {}
    
    # 1. Check top-level counters if present
    if "counters" in run_meta and isinstance(run_meta["counters"], dict):
        for k, v in run_meta["counters"].items():
            extracted[k] = int(v)

    # 2. Check jobs list in run_meta
    for job in run_meta.get("jobs", []):
        job_counters = job.get("counters", {})
        if isinstance(job_counters, dict):
            for k, v in job_counters.items():
                val = int(v)
                extracted[k] = max(extracted.get(k, 0), val)

    return extracted


def build_report_metadata(
    run_id: str,
    run_meta: Dict[str, Any],
    report_stats: Dict[str, Dict[str, Any]],
    hadoop_version: str = "3.3.6"
) -> Dict[str, Any]:
    """Constructs normative report_meta.json structure per SRS §3.5 and §3.3.11."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    counters = extract_counters_from_run_meta(run_meta)

    return {
        "schema_version": "1.0",
        "app_version": "1.0",
        "run_id": run_id,
        "status": "SUCCESS",
        "generated_at": now_utc,
        "hadoop_version": hadoop_version,
        "source_run_meta": "run_meta.json",
        "source_input": run_meta.get("input", ""),
        "source_work_dir": run_meta.get("work_dir", ""),
        "reports": report_stats,
        "counters": counters,
        "definitions": REPORT_DEFINITIONS
    }


def export_reports(
    run_id: str,
    work_dir: str,
    out_dir: str,
    force: bool = False,
    hadoop_version: str = "3.3.6"
) -> int:
    """
    Main orchestration function for exporting all 8 reports.
    Executes staged generation into a temporary staging folder, validates all outputs,
    and atomically publishes to out_dir.
    """
    if not os.path.exists(work_dir):
        print(f"Error: Work directory not found: {work_dir}", file=sys.stderr)
        return 1

    run_meta_path = os.path.join(work_dir, "run_meta.json")
    if not os.path.exists(run_meta_path):
        print(f"Error: run_meta.json not found in work directory: {work_dir}", file=sys.stderr)
        return 1

    with open(run_meta_path, "r", encoding="utf-8") as f:
        try:
            run_meta = json.load(f)
        except Exception as e:
            print(f"Error: Failed to parse run_meta.json: {e}", file=sys.stderr)
            return 1

    status = run_meta.get("status")
    if status != "SUCCESS":
        print(f"Error: Pipeline run '{run_id}' status is '{status}' (not SUCCESS). Refusing to export reports.", file=sys.stderr)
        return 1

    if run_meta.get("invariant_ok") is False:
        print(f"Error: Pipeline run '{run_id}' failed reconciliation invariant. Refusing to export reports.", file=sys.stderr)
        return 1

    # Overwrite protection on target reports folder
    if os.path.exists(out_dir):
        if not force:
            print(f"Error: Target reports directory already exists: {out_dir}", file=sys.stderr)
            print("Use --force or --overwrite to replace existing reports.", file=sys.stderr)
            return 1

    # Staging directory for atomic publication
    staging_dir = os.path.abspath(f"{out_dir}.tmp.{os.getpid()}")
    if os.path.exists(staging_dir):
        shutil.rmtree(staging_dir, ignore_errors=True)
    os.makedirs(staging_dir, exist_ok=True)

    counters = extract_counters_from_run_meta(run_meta)
    report_stats: Dict[str, Dict[str, Any]] = {}

    try:
        # Define source directories
        traffic_dir = os.path.join(work_dir, "traffic")
        peak_dir = os.path.join(work_dir, "peak")
        viewcounts_prod_dir = os.path.join(work_dir, "viewcounts", "products")
        viewcounts_cat_dir = os.path.join(work_dir, "viewcounts", "categories")
        top_prod_dir = os.path.join(work_dir, "top_products")
        top_cat_dir = os.path.join(work_dir, "top_categories")
        funnel_dir = os.path.join(work_dir, "funnel_daily")

        # 1. traffic_hourly.csv
        p1 = os.path.join(staging_dir, "traffic_hourly.csv")
        r1, traffic_by_date = export_traffic_hourly(traffic_dir, p1)
        report_stats["traffic_hourly.csv"] = {
            "row_count": r1,
            "columns": ["date", "hour", "total", "views", "carts", "removes", "purchases", "malformed", "malformed_rate_pct"]
        }

        # 2. peak_hour_daily.csv
        p2 = os.path.join(staging_dir, "peak_hour_daily.csv")
        r2 = export_peak_hour_daily(peak_dir, p2)
        report_stats["peak_hour_daily.csv"] = {
            "row_count": r2,
            "columns": ["date", "peak_hour", "peak_total", "day_total", "peak_share_pct"]
        }

        # 3. hour_of_day_profile.csv
        p3 = os.path.join(staging_dir, "hour_of_day_profile.csv")
        r3 = export_hour_of_day_profile(peak_dir, p3)
        report_stats["hour_of_day_profile.csv"] = {
            "row_count": r3,
            "columns": ["hour", "total_events", "days_observed", "avg_events_per_day"]
        }

        # 4. top_products.csv
        p4 = os.path.join(staging_dir, "top_products.csv")
        r4 = export_top_products(top_prod_dir, viewcounts_prod_dir, counters, p4)
        report_stats["top_products.csv"] = {
            "row_count": r4,
            "columns": ["rank", "product_id", "category_code", "brand", "views", "share_of_all_views_pct"]
        }

        # 5. top_categories.csv
        p5 = os.path.join(staging_dir, "top_categories.csv")
        r5 = export_top_categories(top_cat_dir, viewcounts_cat_dir, p5)
        report_stats["top_categories.csv"] = {
            "row_count": r5,
            "columns": ["rank", "category_code", "views", "share_of_categorised_views_pct"]
        }

        # 6. quality_daily.csv
        p6 = os.path.join(staging_dir, "quality_daily.csv")
        r6 = export_quality_daily(traffic_by_date, p6)
        report_stats["quality_daily.csv"] = {
            "row_count": r6,
            "columns": ["date", "total_records", "valid", "malformed", "malformed_rate_pct"]
        }

        # 7. quality_reasons.csv
        p7 = os.path.join(staging_dir, "quality_reasons.csv")
        r7 = export_quality_reasons(counters, p7)
        report_stats["quality_reasons.csv"] = {
            "row_count": r7,
            "columns": ["reason", "count"]
        }

        # 8. funnel_daily.csv
        p8 = os.path.join(staging_dir, "funnel_daily.csv")
        r8 = export_funnel_daily(funnel_dir, traffic_by_date, p8)
        report_stats["funnel_daily.csv"] = {
            "row_count": r8,
            "columns": [
                "date", "sessions", "cart_sessions", "purchase_sessions", "abandoned_sessions",
                "cart_abandonment_rate_pct", "conversion_rate_pct", "cart_events", "remove_events", "remove_rate_pct"
            ]
        }

        # 9. report_meta.json
        report_meta = build_report_metadata(run_id, run_meta, report_stats, hadoop_version)
        p_meta = os.path.join(staging_dir, "report_meta.json")
        with open(p_meta, "w", encoding="utf-8") as f:
            json.dump(report_meta, f, indent=2)

        # 10. Copy run_meta.json into reports folder (FR-RPT-04)
        p_run_meta = os.path.join(staging_dir, "run_meta.json")
        shutil.copyfile(run_meta_path, p_run_meta)

        # Validate that all 8 reports and 2 JSON metadata files exist and are non-empty
        required_artifacts = [
            "traffic_hourly.csv", "peak_hour_daily.csv", "hour_of_day_profile.csv",
            "top_products.csv", "top_categories.csv", "quality_daily.csv",
            "quality_reasons.csv", "funnel_daily.csv", "report_meta.json", "run_meta.json"
        ]
        for artifact in required_artifacts:
            art_path = os.path.join(staging_dir, artifact)
            if not os.path.isfile(art_path) or os.path.getsize(art_path) == 0:
                raise RuntimeError(f"Report validation failed: artifact '{artifact}' is missing or empty")

        # Atomic publication with rollback protection:
        backup_dir = f"{out_dir}.bak.{os.getpid()}"
        if os.path.exists(backup_dir):
            shutil.rmtree(backup_dir, ignore_errors=True)

        has_existing = os.path.exists(out_dir)
        if has_existing:
            os.replace(out_dir, backup_dir)

        try:
            parent_out = os.path.dirname(out_dir)
            if parent_out and not os.path.exists(parent_out):
                os.makedirs(parent_out, exist_ok=True)
            os.replace(staging_dir, out_dir)
        except Exception as pub_err:
            # Rollback: restore previous valid report set if replacement failed
            if has_existing and os.path.exists(backup_dir) and not os.path.exists(out_dir):
                os.replace(backup_dir, out_dir)
            raise pub_err

        # Clean up backup after confirmed successful replacement
        if os.path.exists(backup_dir):
            shutil.rmtree(backup_dir, ignore_errors=True)

        print(f"[export] Successfully exported 8 reports and metadata to: {out_dir}")
        return 0

    except Exception as e:
        print(f"Error during report export: {e}", file=sys.stderr)
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir, ignore_errors=True)
        backup_dir = f"{out_dir}.bak.{os.getpid()}"
        if os.path.exists(backup_dir) and not os.path.exists(out_dir):
            os.replace(backup_dir, out_dir)
        elif os.path.exists(backup_dir):
            shutil.rmtree(backup_dir, ignore_errors=True)
        return 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export MapReduce Clickstream pipeline outputs to standardized CSV reports."
    )
    parser.add_argument("--run-id", required=True, help="Unique run identifier (e.g. daily-2019-11-01)")
    parser.add_argument("--work-dir", default=None, help="Path to pipeline work directory containing J1..J6 outputs")
    parser.add_argument("--out", "--reports-dir", dest="out_dir", default=None, help="Target reports directory (default: reports/<run_id>)")
    parser.add_argument("--force", "--overwrite", action="store_true", help="Overwrite existing reports directory if present")
    parser.add_argument("--hadoop-version", default="3.3.6", help="Hadoop cluster version recorded in report metadata")

    args = parser.parse_args()

    # Inferred work directory if not specified
    work_dir = args.work_dir
    if not work_dir:
        candidates = [
            os.path.join("data", "work", args.run_id),
            os.path.join("work", args.run_id),
            os.path.join("target", "work", args.run_id),
            f"/clickstream/work/{args.run_id}"
        ]
        for c in candidates:
            if os.path.exists(c):
                work_dir = c
                break
        if not work_dir:
            work_dir = os.path.join("work", args.run_id)

    # Inferred reports directory if not specified
    out_dir = args.out_dir
    if not out_dir:
        out_dir = os.path.join("reports", args.run_id)

    code = export_reports(
        run_id=args.run_id,
        work_dir=work_dir,
        out_dir=out_dir,
        force=args.force,
        hadoop_version=args.hadoop_version
    )
    sys.exit(code)


if __name__ == "__main__":
    main()
