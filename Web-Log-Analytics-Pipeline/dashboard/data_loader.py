"""
dashboard/data_loader.py — Robust, decoupled data loading and validation for Streamlit Dashboard.

Strictly follows SRS §3.3.12 (FR-DSH-01..08) and PRD §6, §8:
- Validates presence and schemas of report_meta.json, run_meta.json, and all 8 CSV reports.
- Extracts KPIs with explicit units without double-counting.
- Handles empty-but-valid reports and division by zero.
- Independent of Streamlit rendering so it can be 100% unit-tested headless.
"""

import json
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

REQUIRED_CSV_FILES = [
    "traffic_hourly.csv",
    "peak_hour_daily.csv",
    "hour_of_day_profile.csv",
    "top_products.csv",
    "top_categories.csv",
    "quality_daily.csv",
    "quality_reasons.csv",
    "funnel_daily.csv",
]

EXPECTED_SCHEMAS = {
    "traffic_hourly.csv": [
        "date", "hour", "total", "views", "carts", "removes", "purchases", "malformed", "malformed_rate_pct"
    ],
    "peak_hour_daily.csv": [
        "date", "peak_hour", "peak_total", "day_total", "peak_share_pct"
    ],
    "hour_of_day_profile.csv": [
        "hour", "total_events", "days_observed", "avg_events_per_day"
    ],
    "top_products.csv": [
        "rank", "product_id", "category_code", "brand", "views", "share_of_all_views_pct"
    ],
    "top_categories.csv": [
        "rank", "category_code", "views", "share_of_categorised_views_pct"
    ],
    "quality_daily.csv": [
        "date", "total_records", "valid", "malformed", "malformed_rate_pct"
    ],
    "quality_reasons.csv": [
        "reason", "count"
    ],
    "funnel_daily.csv": [
        "date", "sessions", "cart_sessions", "purchase_sessions", "abandoned_sessions",
        "cart_abandonment_rate_pct", "conversion_rate_pct", "cart_events", "remove_events", "remove_rate_pct"
    ],
}


def find_available_runs(base_reports_dir: str) -> List[str]:
    """
    Finds all valid or partially valid run subdirectories in base_reports_dir.
    Returns list of run directory names, sorted newest first by directory mtime.
    """
    if not os.path.isdir(base_reports_dir):
        return []

    entries = []
    for item in os.listdir(base_reports_dir):
        full_path = os.path.join(base_reports_dir, item)
        if os.path.isdir(full_path) and not item.startswith("."):
            mtime = os.path.getmtime(full_path)
            entries.append((item, mtime))

    # Sort newest first
    entries.sort(key=lambda x: x[1], reverse=True)
    return [name for name, _ in entries]


def validate_report_directory(report_dir: str) -> Tuple[bool, Optional[str]]:
    """
    Validates report directory structure, required files, and metadata.
    Returns (is_valid, error_message).
    """
    if not os.path.isdir(report_dir):
        return False, f"Directory does not exist: {report_dir}"

    # 1. Validate report_meta.json
    rep_meta_path = os.path.join(report_dir, "report_meta.json")
    if not os.path.isfile(rep_meta_path):
        return False, f"Missing report metadata file: report_meta.json in {report_dir}"

    try:
        with open(rep_meta_path, "r", encoding="utf-8") as f:
            rep_meta = json.load(f)
    except Exception as e:
        return False, f"Corrupted report_meta.json: {e}"

    if rep_meta.get("status") != "SUCCESS":
        status_val = rep_meta.get("status", "UNKNOWN")
        return False, f"Report status is '{status_val}' (expected SUCCESS)"

    # 2. Validate run_meta.json
    run_meta_path = os.path.join(report_dir, "run_meta.json")
    if not os.path.isfile(run_meta_path):
        return False, f"Missing pipeline metadata file: run_meta.json in {report_dir}"

    try:
        with open(run_meta_path, "r", encoding="utf-8") as f:
            run_meta = json.load(f)
    except Exception as e:
        return False, f"Corrupted run_meta.json: {e}"

    if run_meta.get("status") != "SUCCESS":
        status_val = run_meta.get("status", "UNKNOWN")
        return False, f"Pipeline run status in run_meta.json is '{status_val}' (expected SUCCESS)"

    # 3. Validate presence of all 8 CSV files
    for fname in REQUIRED_CSV_FILES:
        fpath = os.path.join(report_dir, fname)
        if not os.path.isfile(fpath):
            return False, f"Missing required CSV report: {fname}"

    return True, None


def load_run_data(report_dir: str) -> Dict[str, Any]:
    """
    Loads all metadata and CSV reports from the specified directory.
    Validates column schemas and returns a dictionary of typed dataframes and metadata.
    Raises ValueError with a specific diagnostic message if validation fails.
    """
    valid, err = validate_report_directory(report_dir)
    if not valid:
        raise ValueError(err)

    # Load JSON metadata
    with open(os.path.join(report_dir, "report_meta.json"), "r", encoding="utf-8") as f:
        report_meta = json.load(f)

    with open(os.path.join(report_dir, "run_meta.json"), "r", encoding="utf-8") as f:
        run_meta = json.load(f)

    dfs: Dict[str, pd.DataFrame] = {}

    for fname in REQUIRED_CSV_FILES:
        fpath = os.path.join(report_dir, fname)
        exp_cols = EXPECTED_SCHEMAS[fname]

        # Use string dtype and preserve literal 'NA' strings without float coercion
        df = pd.read_csv(fpath, dtype=str, keep_default_na=False)

        actual_cols = list(df.columns)
        if fname == "quality_reasons.csv" and actual_cols in [["reason", "count"], ["reason", "count", "share_of_malformed_pct"]]:
            pass
        elif fname == "funnel_daily.csv" and set(actual_cols) == set(exp_cols):
            pass
        elif actual_cols != exp_cols:
            raise ValueError(
                f"Schema mismatch in {fname}: expected columns {exp_cols}, but found {actual_cols}"
            )

        key = fname.replace(".csv", "")
        dfs[key] = df

    return {
        "report_dir": report_dir,
        "run_id": report_meta.get("run_id", "unknown"),
        "report_meta": report_meta,
        "run_meta": run_meta,
        "traffic_hourly": dfs["traffic_hourly"],
        "peak_hour_daily": dfs["peak_hour_daily"],
        "hour_of_day_profile": dfs["hour_of_day_profile"],
        "top_products": dfs["top_products"],
        "top_categories": dfs["top_categories"],
        "quality_daily": dfs["quality_daily"],
        "quality_reasons": dfs["quality_reasons"],
        "funnel_daily": dfs["funnel_daily"],
    }


def get_report_raw_csv(data: Dict[str, Any], filename: str) -> str:
    """
    Returns the exact raw CSV string from the selected run's report directory
    to preserve column ordering, row values, precision, and encoding byte-for-byte.
    Falls back to dataframe to_csv() only if the file cannot be read from disk.
    """
    report_dir = data.get("report_dir")
    if report_dir:
        filepath = os.path.join(report_dir, filename)
        if os.path.isfile(filepath):
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    return f.read()
            except Exception:
                pass
    key = filename.replace(".csv", "")
    df = data.get(key)
    if df is not None and isinstance(df, pd.DataFrame):
        return df.to_csv(index=False)
    return ""


def format_duration_ms(ms: Optional[int]) -> str:
    """Formats milliseconds into a readable mm:ss string or seconds."""
    if ms is None or ms < 0:
        return "N/A"
    total_sec = ms // 1000
    minutes = total_sec // 60
    seconds = total_sec % 60
    if minutes > 0:
        return f"{minutes:02d}m {seconds:02d}s"
    return f"{total_sec}s"


def calculate_overview_kpis(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculates summary KPIs for View 1 (Overview) without double-counting.
    """
    th = data["traffic_hourly"]
    qd = data["quality_daily"]
    fd = data["funnel_daily"]
    hod = data["hour_of_day_profile"]
    run_meta = data["run_meta"]

    # 1. Total events and breakdown
    # Primary authoritative source is traffic_hourly, which reconciles all processed records (including unattributable NA date records)
    if not th.empty:
        valid_records = th["total"].astype(int).sum()
        malformed_records = th["malformed"].astype(int).sum()
        total_records = valid_records + malformed_records
    elif not qd.empty:
        valid_records = qd["valid"].astype(int).sum()
        malformed_records = qd["malformed"].astype(int).sum()
        total_records = qd["total_records"].astype(int).sum()
    else:
        valid_records = 0
        malformed_records = 0
        total_records = 0

    malformed_rate_pct = (
        (malformed_records / total_records * 100.0) if total_records > 0 else 0.0
    )

    attributable_malformed = qd["malformed"].astype(int).sum() if not qd.empty else malformed_records
    unattributable_malformed = max(0, malformed_records - attributable_malformed)

    # 2. Event type totals from traffic_hourly
    if not th.empty:
        total_views = th["views"].astype(int).sum()
        total_carts = th["carts"].astype(int).sum()
        total_removes = th["removes"].astype(int).sum()
        total_purchases = th["purchases"].astype(int).sum()
        unique_dates = sorted([str(d) for d in th["date"].unique() if str(d) not in ("NA", "nan", "")])
        days_covered = len(unique_dates)
    else:
        total_views = 0
        total_carts = 0
        total_removes = 0
        total_purchases = 0
        days_covered = 0

    # 3. Overall peak hour from hour_of_day_profile
    overall_peak_hour = "N/A"
    peak_hour_events = 0
    if not hod.empty:
        hod_sorted = hod.copy()
        hod_sorted["total_events_int"] = hod_sorted["total_events"].astype(int)
        hod_sorted["hour_int"] = hod_sorted["hour"].astype(int)
        hod_sorted = hod_sorted.sort_values(
            by=["total_events_int", "hour_int"], ascending=[False, True]
        )
        top_row = hod_sorted.iloc[0]
        overall_peak_hour = f"{int(top_row['hour']):02d}:00 UTC"
        peak_hour_events = int(top_row["total_events_int"])

    # 4. Behavioral funnel totals from funnel_daily
    if not fd.empty:
        total_sessions = fd["sessions"].astype(int).sum()
        cart_sessions = fd["cart_sessions"].astype(int).sum()
        purchase_sessions = fd["purchase_sessions"].astype(int).sum()
        abandoned_sessions = fd["abandoned_sessions"].astype(int).sum()
    else:
        total_sessions = 0
        cart_sessions = 0
        purchase_sessions = 0
        abandoned_sessions = 0

    cart_abandonment_pct = (
        (abandoned_sessions / cart_sessions * 100.0) if cart_sessions > 0 else 0.0
    )
    conversion_pct = (
        (purchase_sessions / cart_sessions * 100.0) if cart_sessions > 0 else 0.0
    )

    # 5. Run status & duration
    run_status = run_meta.get("status", "SUCCESS")
    duration_sec = run_meta.get("duration_sec")
    duration_ms = run_meta.get("duration_ms")
    if duration_sec is not None:
        duration_str = f"{duration_sec // 60:02d}m {duration_sec % 60:02d}s" if duration_sec >= 60 else f"{duration_sec}s"
    else:
        duration_str = format_duration_ms(duration_ms)

    return {
        "total_records": total_records,
        "valid_records": valid_records,
        "malformed_records": malformed_records,
        "attributable_malformed": attributable_malformed,
        "unattributable_malformed": unattributable_malformed,
        "malformed_rate_pct": malformed_rate_pct,
        "total_views": total_views,
        "total_carts": total_carts,
        "total_removes": total_removes,
        "total_purchases": total_purchases,
        "days_covered": days_covered,
        "overall_peak_hour": overall_peak_hour,
        "peak_hour_events": peak_hour_events,
        "total_sessions": total_sessions,
        "cart_sessions": cart_sessions,
        "purchase_sessions": purchase_sessions,
        "abandoned_sessions": abandoned_sessions,
        "cart_abandonment_pct": cart_abandonment_pct,
        "conversion_pct": conversion_pct,
        "run_status": run_status,
        "duration_str": duration_str,
    }


def calculate_top_pages_summary(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculates summary metrics for View 3 (Top Pages / Products & Categories).
    """
    tp = data["top_products"]
    tc = data["top_categories"]
    report_meta = data["report_meta"]

    total_views = 0
    if not tp.empty:
        total_views = tp["views"].astype(int).sum()

    categorised_views = 0
    if not tc.empty:
        categorised_views = tc["views"].astype(int).sum()

    # Share of categorised views
    uncategorised_views = max(0, total_views - categorised_views)
    uncategorised_share_pct = (
        (uncategorised_views / total_views * 100.0) if total_views > 0 else 0.0
    )

    return {
        "top_products_count": len(tp),
        "top_categories_count": len(tc),
        "total_views_in_top": total_views,
        "categorised_views": categorised_views,
        "uncategorised_views": uncategorised_views,
        "uncategorised_share_pct": uncategorised_share_pct,
    }


def calculate_funnel_summary(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculates behavioral and data quality metrics for View 4 (Errors & Funnel).
    """
    fd = data["funnel_daily"]
    qr = data["quality_reasons"]
    qd = data["quality_daily"]

    # Daily aggregation
    daily_records = []
    if not fd.empty:
        for _, row in fd.iterrows():
            sess = int(row["sessions"])
            cart_sess = int(row["cart_sessions"])
            purch_sess = int(row["purchase_sessions"])
            aban_sess = int(row["abandoned_sessions"])
            rem_events = int(row["remove_events"])
            cart_events = int(row["cart_events"])

            daily_records.append({
                "date": row["date"],
                "sessions": sess,
                "cart_sessions": cart_sess,
                "purchase_sessions": purch_sess,
                "abandoned_sessions": aban_sess,
                "remove_events": rem_events,
                "cart_events": cart_events,
                "cart_abandonment_rate_pct": float(row["cart_abandonment_rate_pct"]) if row["cart_abandonment_rate_pct"] else 0.0,
                "conversion_rate_pct": float(row["conversion_rate_pct"]) if row["conversion_rate_pct"] else 0.0,
                "remove_rate_pct": float(row["remove_rate_pct"]) if row["remove_rate_pct"] else 0.0,
            })

    # Reasons list
    reasons_list = []
    if not qr.empty:
        total_malformed = sum(int(row["count"]) for _, row in qr.iterrows())
        for _, row in qr.iterrows():
            cnt = int(row["count"])
            if "share_of_malformed_pct" in row and row["share_of_malformed_pct"]:
                share_pct = float(row["share_of_malformed_pct"])
            else:
                share_pct = (cnt / total_malformed * 100.0) if total_malformed > 0 else 0.0
            reasons_list.append({
                "reason": row["reason"],
                "count": cnt,
                "share_pct": share_pct,
            })

    return {
        "daily_funnel": daily_records,
        "quality_reasons": reasons_list,
    }
