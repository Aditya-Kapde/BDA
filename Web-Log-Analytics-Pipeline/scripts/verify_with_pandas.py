#!/usr/bin/env python3
"""
scripts/verify_with_pandas.py — Independent pandas baseline and verification for MapReduce Jobs J1 and J2.

Computes expected hourly traffic, event counts, malformed records, peak hours, and hour-of-day profiles
independently using Python/pandas, applying the exact SRS §3.3.3 and §3.3.5 rules.

Usage:
  python scripts/verify_with_pandas.py [--csv <raw_or_sample.csv>] [--job {j1,j2,all}]
                                       [--mr-output <path_to_j1_output>] [--j2-output <path_to_j2_output>]
"""

import argparse
import csv
import os
import re
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

DATE_REGEX = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])")
TIME_REGEX = re.compile(
    r"^(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])[ T]([01]\d|2[0-3]):([0-5]\d):([0-5]\d)"
)
VALID_EVENT_TYPES = {"view", "cart", "remove_from_cart", "purchase"}


def is_leap_year(year: int) -> bool:
    return ((year % 4 == 0) and (year % 100 != 0)) or (year % 400 == 0)


def is_valid_timestamp(ts_str: str) -> bool:
    if not ts_str or len(ts_str) < 19:
        return False
    match = TIME_REGEX.match(ts_str)
    if not match:
        return False
    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))

    days_in_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if month == 2 and is_leap_year(year):
        max_days = 29
    else:
        max_days = days_in_month[month]

    return 1 <= day <= max_days


def compute_baseline_j1(csv_path: str) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Independently parses input CSV per SRS §3.3.3 and aggregates hourly traffic (Job J1).
    Returns (baseline_df, quality_counters).
    """
    counters = {
        "TOTAL_RECORDS": 0,
        "VALID_RECORDS": 0,
        "HEADER_SKIPPED": 0,
        "MALFORMED_COLUMN_COUNT": 0,
        "MALFORMED_TIMESTAMP": 0,
        "MALFORMED_EVENT_TYPE": 0,
        "MALFORMED_ID": 0,
        "MALFORMED_PRICE": 0,
        "MALFORMED_SESSION": 0,
        "MISSING_CATEGORY": 0,
        "MISSING_BRAND": 0,
    }

    # Hourly buckets: key = (date, hour) -> [total, views, carts, removes, purchases, malformed]
    buckets: Dict[Tuple[str, str], List[int]] = {}

    def get_bucket(k_date: str, k_hour: str) -> List[int]:
        key = (k_date, k_hour)
        if key not in buckets:
            buckets[key] = [0, 0, 0, 0, 0, 0]  # total, views, carts, removes, purchases, malformed
        return buckets[key]

    with open(csv_path, mode="r", newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)

        for line_num, row in enumerate(reader, start=1):
            if not row:
                # Blank line
                counters["TOTAL_RECORDS"] += 1
                counters["MALFORMED_COLUMN_COUNT"] += 1
                b = get_bucket("NA", "NA")
                b[5] += 1
                continue

            raw_first_col = row[0].strip()

            # Rule 1: Header Check
            if line_num == 1 and raw_first_col.startswith("event_time"):
                counters["TOTAL_RECORDS"] += 1
                counters["HEADER_SKIPPED"] += 1
                continue

            counters["TOTAL_RECORDS"] += 1

            # Rule 2: Column Count
            if len(row) != 9:
                counters["MALFORMED_COLUMN_COUNT"] += 1
                b = get_bucket("NA", "NA")
                b[5] += 1
                continue

            f_time = row[0].strip()
            f_type = row[1].strip()
            f_pid = row[2].strip()
            f_cid = row[3].strip()
            f_cat = row[4].strip()
            f_brand = row[5].strip()
            f_price = row[6].strip()
            f_uid = row[7].strip()
            f_session = row[8].strip()

            # Rule 3: Timestamp Validation
            if not is_valid_timestamp(f_time):
                counters["MALFORMED_TIMESTAMP"] += 1
                b = get_bucket("NA", "NA")
                b[5] += 1
                continue

            # Record is attributable from here on
            rec_date = f_time[:10]
            rec_hour = str(int(f_time[11:13]))  # 0-23
            b = get_bucket(rec_date, rec_hour)

            # Rule 4: Event Type
            if f_type not in VALID_EVENT_TYPES:
                counters["MALFORMED_EVENT_TYPE"] += 1
                b[5] += 1
                continue

            # Rule 5: IDs Long Validation
            if not (f_pid.isdigit() and f_cid.isdigit() and f_uid.isdigit()):
                counters["MALFORMED_ID"] += 1
                b[5] += 1
                continue

            # Rule 6: Price Validation
            try:
                price_val = float(f_price)
                if price_val < 0.0:
                    counters["MALFORMED_PRICE"] += 1
                    b[5] += 1
                    continue
            except ValueError:
                counters["MALFORMED_PRICE"] += 1
                b[5] += 1
                continue

            # Rule 7: User Session
            if not f_session:
                counters["MALFORMED_SESSION"] += 1
                b[5] += 1
                continue

            # VALID Record
            counters["VALID_RECORDS"] += 1
            if not f_cat:
                counters["MISSING_CATEGORY"] += 1
            if not f_brand:
                counters["MISSING_BRAND"] += 1

            # Update event counts
            b[0] += 1  # total valid events
            if f_type == "view":
                b[1] += 1
            elif f_type == "cart":
                b[2] += 1
            elif f_type == "remove_from_cart":
                b[3] += 1
            elif f_type == "purchase":
                b[4] += 1

    # Convert buckets to DataFrame
    rows = []
    for (d, h), counts in sorted(buckets.items()):
        total, views, carts, removes, purchases, malformed = counts
        denom = total + malformed
        error_rate = (malformed / denom) if denom > 0 else 0.0
        rows.append({
            "date": d,
            "hour": h,
            "total": total,
            "views": views,
            "carts": carts,
            "removes": removes,
            "purchases": purchases,
            "malformed": malformed,
            "error_rate": f"{error_rate:.6f}",
        })

    df = pd.DataFrame(rows)
    return df, counters


def compute_baseline_j2(j1_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Independently derives Job J2 outputs (peakday and hourofday) from J1 output DataFrame per SRS §3.3.5.
    Returns (peakday_df, hourofday_df).
    """
    # 1. Filter out NA rows
    valid_j1 = j1_df[(j1_df["date"] != "NA") & (j1_df["hour"] != "NA")].copy()
    valid_j1["hour_int"] = valid_j1["hour"].astype(int)
    valid_j1["total_int"] = valid_j1["total"].astype(int)

    # 2. Derive peakday: date, peak_hour, peak_total, day_total
    # Tie-breaking rule: max total, tie -> smallest hour
    peakday_rows = []
    for date, group in valid_j1.groupby("date"):
        # Sort by total descending, then hour_int ascending
        sorted_group = group.sort_values(by=["total_int", "hour_int"], ascending=[False, True])
        top_row = sorted_group.iloc[0]
        day_total = int(group["total_int"].sum())
        peakday_rows.append({
            "date": date,
            "peak_hour": int(top_row["hour_int"]),
            "peak_total": int(top_row["total_int"]),
            "day_total": day_total,
        })

    peakday_df = pd.DataFrame(peakday_rows).sort_values(by="date")

    # 3. Derive hourofday: hour, total_events, days_observed
    hourofday_rows = []
    for hour_int, group in valid_j1.groupby("hour_int"):
        total_events = int(group["total_int"].sum())
        days_observed = int(group["date"].nunique())
        hourofday_rows.append({
            "hour": int(hour_int),
            "total_events": total_events,
            "days_observed": days_observed,
        })

    hourofday_df = pd.DataFrame(hourofday_rows).sort_values(by="hour")
    return peakday_df, hourofday_df


def verify_against_mr_j1(baseline_df: pd.DataFrame, mr_output_path: str) -> bool:
    """
    Compares baseline DataFrame with MapReduce TSV output for J1.
    """
    if not os.path.exists(mr_output_path):
        print(f"Error: MapReduce output path '{mr_output_path}' not found.", file=sys.stderr)
        return False

    mr_lines = []
    if os.path.isdir(mr_output_path):
        for fname in sorted(os.listdir(mr_output_path)):
            if fname.startswith("part-") and not fname.endswith(".crc"):
                fpath = os.path.join(mr_output_path, fname)
                with open(fpath, "r", encoding="utf-8") as f:
                    mr_lines.extend(line.strip() for line in f if line.strip())
    else:
        with open(mr_output_path, "r", encoding="utf-8") as f:
            mr_lines.extend(line.strip() for line in f if line.strip())

    expected_rows = len(baseline_df)
    actual_rows = len(mr_lines)

    print(f"MapReduce J1 Output Rows : {actual_rows}")
    print(f"Baseline Expected Rows   : {expected_rows}")

    if actual_rows != expected_rows:
        print(f"FAIL: Row count mismatch: expected {expected_rows}, got {actual_rows}", file=sys.stderr)
        return False

    mr_dict = {}
    for line in mr_lines:
        parts = line.split("\t")
        if len(parts) != 9:
            print(f"FAIL: Malformed TSV output line (columns != 9): {line}", file=sys.stderr)
            return False
        k = (parts[0], parts[1])
        mr_dict[k] = parts[2:]

    mismatches = 0
    for _, b_row in baseline_df.iterrows():
        key = (str(b_row["date"]), str(b_row["hour"]))
        if key not in mr_dict:
            print(f"FAIL: Missing key in MR output: {key}", file=sys.stderr)
            mismatches += 1
            continue

        mr_vals = mr_dict[key]
        expected_vals = [
            str(b_row["total"]),
            str(b_row["views"]),
            str(b_row["carts"]),
            str(b_row["removes"]),
            str(b_row["purchases"]),
            str(b_row["malformed"]),
            str(b_row["error_rate"]),
        ]

        if mr_vals != expected_vals:
            print(f"FAIL: Mismatch for key {key}:", file=sys.stderr)
            print(f"  Expected: {expected_vals}", file=sys.stderr)
            print(f"  Actual  : {mr_vals}", file=sys.stderr)
            mismatches += 1

    return mismatches == 0


def verify_against_mr_j2(expected_peakday: pd.DataFrame, expected_hourofday: pd.DataFrame, j2_output_dir: str) -> bool:
    """
    Compares baseline DataFrames with MapReduce J2 MultipleOutputs (peakday and hourofday).
    """
    if not os.path.exists(j2_output_dir):
        print(f"Error: J2 output directory '{j2_output_dir}' not found.", file=sys.stderr)
        return False

    peakday_lines = []
    hourofday_lines = []

    for fname in sorted(os.listdir(j2_output_dir)):
        if fname.startswith("peakday-") and not fname.endswith(".crc"):
            with open(os.path.join(j2_output_dir, fname), "r", encoding="utf-8") as f:
                peakday_lines.extend(line.strip() for line in f if line.strip())
        elif fname.startswith("hourofday-") and not fname.endswith(".crc"):
            with open(os.path.join(j2_output_dir, fname), "r", encoding="utf-8") as f:
                hourofday_lines.extend(line.strip() for line in f if line.strip())

    mismatches = 0

    # 1. Verify peakday
    print(f"Verifying J2 'peakday' output ({len(peakday_lines)} actual rows vs {len(expected_peakday)} expected)...")
    if len(peakday_lines) != len(expected_peakday):
        print(f"FAIL: peakday row count mismatch: expected {len(expected_peakday)}, got {len(peakday_lines)}", file=sys.stderr)
        mismatches += 1
    else:
        actual_peakday_map = {}
        for l in peakday_lines:
            cols = l.split("\t")
            if len(cols) != 4:
                print(f"FAIL: peakday line columns != 4: {l}", file=sys.stderr)
                mismatches += 1
                continue
            actual_peakday_map[cols[0]] = (int(cols[1]), int(cols[2]), int(cols[3]))

        for _, row in expected_peakday.iterrows():
            d = str(row["date"])
            if d not in actual_peakday_map:
                print(f"FAIL: peakday missing date {d}", file=sys.stderr)
                mismatches += 1
            else:
                act = actual_peakday_map[d]
                exp = (int(row["peak_hour"]), int(row["peak_total"]), int(row["day_total"]))
                if act != exp:
                    print(f"FAIL: peakday mismatch for {d}: expected {exp}, got {act}", file=sys.stderr)
                    mismatches += 1

    # 2. Verify hourofday
    print(f"Verifying J2 'hourofday' output ({len(hourofday_lines)} actual rows vs {len(expected_hourofday)} expected)...")
    if len(hourofday_lines) != len(expected_hourofday):
        print(f"FAIL: hourofday row count mismatch: expected {len(expected_hourofday)}, got {len(hourofday_lines)}", file=sys.stderr)
        mismatches += 1
    else:
        actual_hod_map = {}
        for l in hourofday_lines:
            cols = l.split("\t")
            if len(cols) != 3:
                print(f"FAIL: hourofday line columns != 3: {l}", file=sys.stderr)
                mismatches += 1
                continue
            actual_hod_map[int(cols[0])] = (int(cols[1]), int(cols[2]))

        for _, row in expected_hourofday.iterrows():
            h = int(row["hour"])
            if h not in actual_hod_map:
                print(f"FAIL: hourofday missing hour {h}", file=sys.stderr)
                mismatches += 1
            else:
                act = actual_hod_map[h]
                exp = (int(row["total_events"]), int(row["days_observed"]))
                if act != exp:
                    print(f"FAIL: hourofday mismatch for hour {h}: expected {exp}, got {act}", file=sys.stderr)
                    mismatches += 1

    return mismatches == 0


def compute_baseline_j3(csv_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    """
    Independently computes Job J3 (ViewCount) baseline product-level and category-level views from raw CSV.
    Strictly follows SRS §3.3.6 (FR-MR-12):
      - Filters for event_type == 'view' only.
      - Product views: product_id -> (views, category_code, brand)
        Metadata resolution: non-empty overrides empty; if multiple non-empty, lexicographical minimum.
      - Category views: category_code -> views
        If category is empty, increments VIEWS_WITHOUT_CATEGORY and omits from category view table.
    Returns (products_df, categories_df, counters).
    """
    counters = {
        "TOTAL_RECORDS": 0,
        "VALID_RECORDS": 0,
        "HEADER_SKIPPED": 0,
        "MALFORMED_COLUMN_COUNT": 0,
        "MALFORMED_TIMESTAMP": 0,
        "MALFORMED_EVENT_TYPE": 0,
        "MALFORMED_ID": 0,
        "MALFORMED_PRICE": 0,
        "MALFORMED_SESSION": 0,
        "MISSING_CATEGORY": 0,
        "MISSING_BRAND": 0,
        "VIEW_EVENTS": 0,
        "VIEWS_WITHOUT_CATEGORY": 0,
    }

    product_views: Dict[str, List[Any]] = {}  # pid -> [count, category, brand]
    category_views: Dict[str, int] = {}       # cat -> count

    def merge_metadata(curr: str, new_val: str) -> str:
        if not curr:
            return new_val
        if not new_val:
            return curr
        return min(curr, new_val)

    with open(csv_path, mode="r", newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for line_num, row in enumerate(reader, start=1):
            if not row:
                counters["TOTAL_RECORDS"] += 1
                counters["MALFORMED_COLUMN_COUNT"] += 1
                continue

            raw_first_col = row[0].strip()

            # Rule 1: Header Check
            if line_num == 1 and raw_first_col.startswith("event_time"):
                counters["TOTAL_RECORDS"] += 1
                counters["HEADER_SKIPPED"] += 1
                continue

            counters["TOTAL_RECORDS"] += 1

            # Rule 2: Column Count
            if len(row) != 9:
                counters["MALFORMED_COLUMN_COUNT"] += 1
                continue

            f_time = row[0].strip()
            f_type = row[1].strip()
            f_pid = row[2].strip()
            f_cid = row[3].strip()
            f_cat = row[4].strip()
            f_brand = row[5].strip()
            f_price = row[6].strip()
            f_uid = row[7].strip()
            f_session = row[8].strip()

            # Rule 3: Timestamp format
            if not is_valid_timestamp(f_time):
                counters["MALFORMED_TIMESTAMP"] += 1
                continue

            # Rule 4: Event Type
            if f_type not in VALID_EVENT_TYPES:
                counters["MALFORMED_EVENT_TYPE"] += 1
                continue

            # Rule 5: IDs Long Validation
            if not (f_pid.isdigit() and f_cid.isdigit() and f_uid.isdigit()):
                counters["MALFORMED_ID"] += 1
                continue

            # Rule 6: Price non-negative float
            try:
                price = float(f_price)
                if price < 0.0:
                    counters["MALFORMED_PRICE"] += 1
                    continue
            except ValueError:
                counters["MALFORMED_PRICE"] += 1
                continue

            # Rule 7: User Session
            if not f_session:
                counters["MALFORMED_SESSION"] += 1
                continue

            # VALID Record
            counters["VALID_RECORDS"] += 1
            if not f_cat:
                counters["MISSING_CATEGORY"] += 1
            if not f_brand:
                counters["MISSING_BRAND"] += 1

            # J3 Event Type Filter: ONLY 'view' events count
            if f_type != "view":
                continue

            counters["VIEW_EVENTS"] += 1

            # 1. Product view accumulation
            pid_str = f_pid
            if pid_str not in product_views:
                product_views[pid_str] = [1, f_cat, f_brand]
            else:
                entry = product_views[pid_str]
                entry[0] += 1
                entry[1] = merge_metadata(entry[1], f_cat)
                entry[2] = merge_metadata(entry[2], f_brand)

            # 2. Category view accumulation
            if f_cat:
                category_views[f_cat] = category_views.get(f_cat, 0) + 1
            else:
                counters["VIEWS_WITHOUT_CATEGORY"] += 1

    # Build products_df
    prod_rows = []
    for pid_str, (cnt, cat, brd) in sorted(product_views.items(), key=lambda x: int(x[0])):
        prod_rows.append({
            "product_id": pid_str,
            "category_code": cat,
            "brand": brd,
            "views": cnt
        })
    products_df = pd.DataFrame(prod_rows) if prod_rows else pd.DataFrame(columns=["product_id", "category_code", "brand", "views"])

    # Build categories_df
    cat_rows = []
    for cat_code, cnt in sorted(category_views.items()):
        cat_rows.append({
            "category_code": cat_code,
            "views": cnt
        })
    categories_df = pd.DataFrame(cat_rows) if cat_rows else pd.DataFrame(columns=["category_code", "views"])

    return products_df, categories_df, counters


def verify_against_mr_j3(expected_products: pd.DataFrame, expected_categories: pd.DataFrame, j3_output_dir: str) -> bool:
    """
    Compares baseline DataFrames with MapReduce J3 MultipleOutputs (products and categories).
    Supports output files located in:
      - j3_output_dir/products/part-* or j3_output_dir/products-*
      - j3_output_dir/categories/part-* or j3_output_dir/categories-*
    """
    if not os.path.exists(j3_output_dir):
        print(f"Error: J3 output directory '{j3_output_dir}' not found.", file=sys.stderr)
        return False

    product_lines = []
    category_lines = []

    # Search recursively for products and categories output files
    for root, _, files in os.walk(j3_output_dir):
        for fname in sorted(files):
            if fname.endswith(".crc") or fname.startswith("."):
                continue
            fpath = os.path.join(root, fname)
            rel_path = os.path.relpath(fpath, j3_output_dir).replace("\\", "/")
            if "products" in rel_path and ("part-" in fname or fname.startswith("products")):
                with open(fpath, "r", encoding="utf-8") as f:
                    product_lines.extend(line.strip() for line in f if line.strip())
            elif "categories" in rel_path and ("part-" in fname or fname.startswith("categories")):
                with open(fpath, "r", encoding="utf-8") as f:
                    category_lines.extend(line.strip() for line in f if line.strip())

    mismatches = 0

    # 1. Verify products
    print(f"Verifying J3 'products' output ({len(product_lines)} actual rows vs {len(expected_products)} expected)...")
    if len(product_lines) != len(expected_products):
        print(f"FAIL: products row count mismatch: expected {len(expected_products)}, got {len(product_lines)}", file=sys.stderr)
        mismatches += 1
    else:
        actual_prod_map = {}
        for l in product_lines:
            cols = l.split("\t")
            if len(cols) != 4:
                print(f"FAIL: products line columns != 4: {l}", file=sys.stderr)
                mismatches += 1
                continue
            actual_prod_map[cols[0]] = (cols[1], cols[2], int(cols[3]))

        for _, row in expected_products.iterrows():
            pid = str(row["product_id"])
            if pid not in actual_prod_map:
                print(f"FAIL: products missing product_id {pid}", file=sys.stderr)
                mismatches += 1
            else:
                act = actual_prod_map[pid]
                exp = (str(row["category_code"]), str(row["brand"]), int(row["views"]))
                if act != exp:
                    print(f"FAIL: products mismatch for {pid}: expected {exp}, got {act}", file=sys.stderr)
                    mismatches += 1

    # 2. Verify categories
    print(f"Verifying J3 'categories' output ({len(category_lines)} actual rows vs {len(expected_categories)} expected)...")
    if len(category_lines) != len(expected_categories):
        print(f"FAIL: categories row count mismatch: expected {len(expected_categories)}, got {len(category_lines)}", file=sys.stderr)
        mismatches += 1
    else:
        actual_cat_map = {}
        for l in category_lines:
            cols = l.split("\t")
            if len(cols) != 2:
                print(f"FAIL: categories line columns != 2: {l}", file=sys.stderr)
                mismatches += 1
                continue
            actual_cat_map[cols[0]] = int(cols[1])

        for _, row in expected_categories.iterrows():
            cat = str(row["category_code"])
            if cat not in actual_cat_map:
                print(f"FAIL: categories missing category_code {cat}", file=sys.stderr)
                mismatches += 1
            else:
                act = actual_cat_map[cat]
                exp = int(row["views"])
                if act != exp:
                    print(f"FAIL: categories mismatch for {cat}: expected {exp}, got {act}", file=sys.stderr)
                    mismatches += 1

    return mismatches == 0


def compute_baseline_j4(products_df: pd.DataFrame, categories_df: pd.DataFrame, topn: int = 10) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Independently computes Job J4 (TopN) rankings from J3 outputs per SRS §3.3.7.
    Ranking order: views descending, then entity key ascending (numeric for product_id, lexical for category_code).
    Returns (top_products_df, top_categories_df).
    Output schemas:
      - top_products: rank, product_id, category_code, brand, views
      - top_categories: rank, category_code, views
    """
    # 1. Top Products
    if not products_df.empty:
        prod_copy = products_df.copy()
        prod_copy["views_int"] = prod_copy["views"].astype(int)
        prod_copy["pid_int"] = prod_copy["product_id"].astype(int)
        sorted_prod = prod_copy.sort_values(by=["views_int", "pid_int"], ascending=[False, True]).head(topn)
        sorted_prod["rank"] = range(1, len(sorted_prod) + 1)
        top_products_df = sorted_prod[["rank", "product_id", "category_code", "brand", "views"]].reset_index(drop=True)
    else:
        top_products_df = pd.DataFrame(columns=["rank", "product_id", "category_code", "brand", "views"])

    # 2. Top Categories
    if not categories_df.empty:
        cat_copy = categories_df.copy()
        cat_copy["views_int"] = cat_copy["views"].astype(int)
        sorted_cat = cat_copy.sort_values(by=["views_int", "category_code"], ascending=[False, True]).head(topn)
        sorted_cat["rank"] = range(1, len(sorted_cat) + 1)
        top_categories_df = sorted_cat[["rank", "category_code", "views"]].reset_index(drop=True)
    else:
        top_categories_df = pd.DataFrame(columns=["rank", "category_code", "views"])

    return top_products_df, top_categories_df


def verify_against_mr_j4(expected_top_products: pd.DataFrame, expected_top_categories: pd.DataFrame,
                          top_products_dir: Optional[str], top_categories_dir: Optional[str]) -> bool:
    """
    Compares baseline TopN DataFrames with MapReduce J4 outputs for products and categories.
    Output format:
      - products: rank \t product_id \t category_code \t brand \t views
      - categories: rank \t category_code \t views
    """
    mismatches = 0

    def read_mr_lines(path: str) -> List[str]:
        lines = []
        if os.path.isdir(path):
            for fname in sorted(os.listdir(path)):
                if fname.startswith("part-") and not fname.endswith(".crc"):
                    with open(os.path.join(path, fname), "r", encoding="utf-8") as f:
                        lines.extend(line.strip() for line in f if line.strip())
        elif os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                lines.extend(line.strip() for line in f if line.strip())
        return lines

    if top_products_dir:
        print(f"\nVerifying J4 Top Products output at: {top_products_dir}")
        if not os.path.exists(top_products_dir):
            print(f"FAIL: top_products path not found: {top_products_dir}", file=sys.stderr)
            return False

        lines = read_mr_lines(top_products_dir)
        if len(lines) != len(expected_top_products):
            print(f"FAIL: top_products row count mismatch: expected {len(expected_top_products)}, got {len(lines)}", file=sys.stderr)
            mismatches += 1
        else:
            for idx, line in enumerate(lines):
                cols = line.split("\t")
                if len(cols) != 5:
                    print(f"FAIL: top_products line columns != 5 (got {len(cols)}): {line}", file=sys.stderr)
                    mismatches += 1
                    continue
                exp_row = expected_top_products.iloc[idx]
                act = (int(cols[0]), cols[1], cols[2], cols[3], int(cols[4]))
                exp = (int(exp_row["rank"]), str(exp_row["product_id"]), str(exp_row["category_code"]), str(exp_row["brand"]), int(exp_row["views"]))
                if act != exp:
                    print(f"FAIL: top_products mismatch at row {idx+1}: expected {exp}, got {act}", file=sys.stderr)
                    mismatches += 1

    if top_categories_dir:
        print(f"\nVerifying J4 Top Categories output at: {top_categories_dir}")
        if not os.path.exists(top_categories_dir):
            print(f"FAIL: top_categories path not found: {top_categories_dir}", file=sys.stderr)
            return False

        lines = read_mr_lines(top_categories_dir)
        if len(lines) != len(expected_top_categories):
            print(f"FAIL: top_categories row count mismatch: expected {len(expected_top_categories)}, got {len(lines)}", file=sys.stderr)
            mismatches += 1
        else:
            for idx, line in enumerate(lines):
                cols = line.split("\t")
                if len(cols) != 3:
                    print(f"FAIL: top_categories line columns != 3 (got {len(cols)}): {line}", file=sys.stderr)
                    mismatches += 1
                    continue
                exp_row = expected_top_categories.iloc[idx]
                act = (int(cols[0]), cols[1], int(cols[2]))
                exp = (int(exp_row["rank"]), str(exp_row["category_code"]), int(exp_row["views"]))
                if act != exp:
                    print(f"FAIL: top_categories mismatch at row {idx+1}: expected {exp}, got {act}", file=sys.stderr)
                    mismatches += 1

    return mismatches == 0


def compute_baseline_j5(csv_path: str) -> Tuple[pd.DataFrame, Dict[str, any]]:
    """
    Independently aggregates session-level funnels (Job J5) per SRS §3.3.8.
    Applies the exact 7 validation rules per SRS §3.3.3.
    Groups valid records by user_session.
    Attribution date = UTC date of min(event_time).
    Returns (session_funnel_df, stats_dict).
    Columns: date, sessions, cart, purchase, abandoned, removes.
    """
    import calendar
    import time

    FLAG_VIEW = 0x01
    FLAG_CART = 0x02
    FLAG_PURCHASE = 0x04

    # session_map: user_session -> [min_epoch, flags_mask, remove_count]
    sessions: Dict[str, List[int]] = {}
    counters = {
        "TOTAL_RECORDS": 0,
        "VALID_RECORDS": 0,
        "HEADER_SKIPPED": 0,
        "MALFORMED_RECORDS": 0,
    }

    with open(csv_path, mode="r", newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for line_num, row in enumerate(reader, start=1):
            if not row:
                counters["TOTAL_RECORDS"] += 1
                counters["MALFORMED_RECORDS"] += 1
                continue

            raw_first_col = row[0].strip()
            if line_num == 1 and raw_first_col.startswith("event_time"):
                counters["TOTAL_RECORDS"] += 1
                counters["HEADER_SKIPPED"] += 1
                continue

            counters["TOTAL_RECORDS"] += 1
            if len(row) != 9:
                counters["MALFORMED_RECORDS"] += 1
                continue

            f_time = row[0].strip()
            f_type = row[1].strip()
            f_pid = row[2].strip()
            f_cid = row[3].strip()
            f_cat = row[4].strip()
            f_brand = row[5].strip()
            f_price = row[6].strip()
            f_uid = row[7].strip()
            f_session = row[8].strip()

            if not is_valid_timestamp(f_time):
                counters["MALFORMED_RECORDS"] += 1
                continue
            if f_type not in VALID_EVENT_TYPES:
                counters["MALFORMED_RECORDS"] += 1
                continue
            if not (f_pid.isdigit() and f_cid.isdigit() and f_uid.isdigit()):
                counters["MALFORMED_RECORDS"] += 1
                continue
            try:
                if float(f_price) < 0.0:
                    counters["MALFORMED_RECORDS"] += 1
                    continue
            except ValueError:
                counters["MALFORMED_RECORDS"] += 1
                continue
            if not f_session:
                counters["MALFORMED_RECORDS"] += 1
                continue

            counters["VALID_RECORDS"] += 1

            # Parse epoch (UTC)
            epoch_sec = calendar.timegm(time.strptime(f_time[:19], "%Y-%m-%d %H:%M:%S"))

            flags = 0
            removes = 0
            if f_type == "view":
                flags |= FLAG_VIEW
            elif f_type == "cart":
                flags |= FLAG_CART
            elif f_type == "purchase":
                flags |= FLAG_PURCHASE
            elif f_type == "remove_from_cart":
                removes = 1

            if f_session not in sessions:
                sessions[f_session] = [epoch_sec, flags, removes]
            else:
                entry = sessions[f_session]
                if epoch_sec < entry[0]:
                    entry[0] = epoch_sec
                entry[1] |= flags
                entry[2] += removes

    # Aggregate by date of earliest event
    # date -> [sessions, cart_sessions, purchase_sessions, abandoned_sessions, remove_events_in_cart_sessions]
    daily_funnel: Dict[str, List[int]] = {}
    for sess_id, (min_epoch, flags, removes) in sessions.items():
        sess_date = time.strftime("%Y-%m-%d", time.gmtime(min_epoch))
        if sess_date not in daily_funnel:
            daily_funnel[sess_date] = [0, 0, 0, 0, 0]

        m = daily_funnel[sess_date]
        m[0] += 1  # total sessions

        has_cart = bool(flags & FLAG_CART)
        has_purchase = bool(flags & FLAG_PURCHASE)
        is_abandoned = has_cart and not has_purchase

        if has_cart:
            m[1] += 1
            m[4] += removes  # remove events in cart sessions
        if has_purchase:
            m[2] += 1
        if is_abandoned:
            m[3] += 1

    rows = []
    for d in sorted(daily_funnel.keys()):
        m = daily_funnel[d]
        rows.append({
            "date": d,
            "sessions": m[0],
            "cart": m[1],
            "purchase": m[2],
            "abandoned": m[3],
            "removes": m[4],
        })

    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["date", "sessions", "cart", "purchase", "abandoned", "removes"])
    stats = {
        "distinct_sessions": len(sessions),
        "counters": counters,
    }
    return df, stats


def verify_against_mr_j5(expected_df: pd.DataFrame, j5_output_dir: str) -> bool:
    """
    Compares baseline J5 DataFrame with MapReduce J5 output directory.
    Output schema: date \t sessions \t cart \t purchase \t abandoned \t removes
    """
    if not os.path.exists(j5_output_dir):
        print(f"Error: J5 output directory '{j5_output_dir}' not found.", file=sys.stderr)
        return False

    lines = []
    if os.path.isdir(j5_output_dir):
        for fname in sorted(os.listdir(j5_output_dir)):
            if fname.startswith("part-") and not fname.endswith(".crc"):
                with open(os.path.join(j5_output_dir, fname), "r", encoding="utf-8") as f:
                    lines.extend(l.strip() for l in f if l.strip())
    elif os.path.isfile(j5_output_dir):
        with open(j5_output_dir, "r", encoding="utf-8") as f:
            lines.extend(l.strip() for l in f if l.strip())

    mismatches = 0
    print(f"\nVerifying J5 session funnel output ({len(lines)} actual rows vs {len(expected_df)} expected)...")
    if len(lines) != len(expected_df):
        print(f"FAIL: J5 row count mismatch: expected {len(expected_df)}, got {len(lines)}", file=sys.stderr)
        mismatches += 1
    else:
        for idx, line in enumerate(lines):
            cols = line.split("\t")
            if len(cols) != 6:
                print(f"FAIL: J5 line columns != 6 (got {len(cols)}): {line}", file=sys.stderr)
                mismatches += 1
                continue
            exp_row = expected_df.iloc[idx]
            act = (cols[0], int(cols[1]), int(cols[2]), int(cols[3]), int(cols[4]), int(cols[5]))
            exp = (str(exp_row["date"]), int(exp_row["sessions"]), int(exp_row["cart"]),
                   int(exp_row["purchase"]), int(exp_row["abandoned"]), int(exp_row["removes"]))
            if act != exp:
                print(f"FAIL: J5 mismatch at row {idx+1}: expected {exp}, got {act}", file=sys.stderr)
                mismatches += 1

    return mismatches == 0


def compute_baseline_j6(j5_df: pd.DataFrame) -> pd.DataFrame:
    """
    Independently consolidates J5 partial daily funnel outputs into global daily funnel metrics (Job J6) per SRS §3.3.9.
    Groups by date and performs element-wise summation of:
      - sessions
      - cart
      - purchase
      - abandoned
      - removes
    Returns consolidated DataFrame sorted chronologically by date.
    Schema: date, sessions, cart, purchase, abandoned, removes.
    """
    if j5_df.empty:
        return pd.DataFrame(columns=["date", "sessions", "cart", "purchase", "abandoned", "removes"])

    grouped = j5_df.groupby("date", as_index=False).agg({
        "sessions": "sum",
        "cart": "sum",
        "purchase": "sum",
        "abandoned": "sum",
        "removes": "sum"
    }).sort_values(by="date").reset_index(drop=True)

    # Cast to integer
    for col in ["sessions", "cart", "purchase", "abandoned", "removes"]:
        grouped[col] = grouped[col].astype(int)

    return grouped


def verify_against_mr_j6(expected_df: pd.DataFrame, j6_output_dir: str) -> bool:
    """
    Compares baseline J6 DataFrame with MapReduce J6 output directory.
    Output schema: date \t sessions \t cart_sessions \t purchase_sessions \t abandoned_sessions \t remove_events
    """
    if not os.path.exists(j6_output_dir):
        print(f"Error: J6 output directory '{j6_output_dir}' not found.", file=sys.stderr)
        return False

    lines = []
    if os.path.isdir(j6_output_dir):
        for fname in sorted(os.listdir(j6_output_dir)):
            if fname.startswith("part-") and not fname.endswith(".crc"):
                with open(os.path.join(j6_output_dir, fname), "r", encoding="utf-8") as f:
                    lines.extend(l.strip() for l in f if l.strip())
    elif os.path.isfile(j6_output_dir):
        with open(j6_output_dir, "r", encoding="utf-8") as f:
            lines.extend(l.strip() for l in f if l.strip())

    mismatches = 0
    print(f"\nVerifying J6 daily funnel output ({len(lines)} actual rows vs {len(expected_df)} expected)...")
    if len(lines) != len(expected_df):
        print(f"FAIL: J6 row count mismatch: expected {len(expected_df)}, got {len(lines)}", file=sys.stderr)
        mismatches += 1
    else:
        for idx, line in enumerate(lines):
            cols = line.split("\t")
            if len(cols) != 6:
                print(f"FAIL: J6 line columns != 6 (got {len(cols)}): {line}", file=sys.stderr)
                mismatches += 1
                continue
            exp_row = expected_df.iloc[idx]
            act = (cols[0], int(cols[1]), int(cols[2]), int(cols[3]), int(cols[4]), int(cols[5]))
            exp = (str(exp_row["date"]), int(exp_row["sessions"]), int(exp_row["cart"]),
                   int(exp_row["purchase"]), int(exp_row["abandoned"]), int(exp_row["removes"]))
            if act != exp:
                print(f"FAIL: J6 mismatch at row {idx+1}: expected {exp}, got {act}", file=sys.stderr)
                mismatches += 1

    return mismatches == 0


def main():
    parser = argparse.ArgumentParser(description="Independent pandas baseline verification for Jobs J1, J2, J3, J4, J5, and J6")
    parser.add_argument("--csv", help="Input raw or sample CSV path")
    parser.add_argument("--job", choices=["j1", "j2", "j3", "j4", "j5", "j6", "all"], default="all", help="Job baseline to compute/verify (default: all)")
    parser.add_argument("--topn", type=int, default=10, help="Top-N cutoff value (default: 10)")
    parser.add_argument("--mr-output", help="Path to MapReduce J1 output file or directory")
    parser.add_argument("--j2-output", help="Path to MapReduce J2 output directory (with peakday-* and hourofday-*)")
    parser.add_argument("--j3-output", help="Path to MapReduce J3 output directory (with products and categories)")
    parser.add_argument("--top-products-output", help="Path to MapReduce J4 top products output directory")
    parser.add_argument("--top-categories-output", help="Path to MapReduce J4 top categories output directory")
    parser.add_argument("--j5-output", help="Path to MapReduce J5 output directory (with session_partial/part-r-*)")
    parser.add_argument("--j6-output", help="Path to MapReduce J6 output directory (with funnel_daily/part-r-*)")
    args = parser.parse_args()

    # Determine input CSV
    csv_path = args.csv
    if not csv_path:
        sample_path = os.path.join("data", "sample", "sample_1m.csv")
        fixture_path = os.path.join("src", "test", "resources", "fixtures", "mini_clickstream.csv")
        if os.path.isfile(sample_path):
            csv_path = sample_path
            print(f"[verify] Using sample dataset: {csv_path}")
        elif os.path.isfile(fixture_path):
            csv_path = fixture_path
            print(f"[verify] Sample absent; using test fixture: {csv_path}")
        else:
            print("Error: No CSV specified and no default sample/fixture found.", file=sys.stderr)
            sys.exit(1)

    print(f"Computing independent pandas baseline from: {csv_path}")

    # Job J1 Baseline
    if args.job in ["j1", "j2", "all"]:
        j1_df, counters = compute_baseline_j1(csv_path)

        if args.job in ["j1", "all"]:
            print("\n" + "=" * 60)
            print(" INDEPENDENT PANDAS BASELINE SUMMARY (JOB J1: HourlyTraffic)")
            print("=" * 60)
            print(f"Total Records  : {counters['TOTAL_RECORDS']}")
            print(f"Valid Records  : {counters['VALID_RECORDS']}")
            print(f"Header Skipped : {counters['HEADER_SKIPPED']}")
            print(f"Malformed Total: {sum(v for k, v in counters.items() if k.startswith('MALFORMED_'))}")
            print("-" * 60)
            print("Baseline Hourly Table:")
            print(j1_df.to_string(index=False))
            print("=" * 60)

            # Invariant verification: TOTAL_RECORDS - HEADER_SKIPPED == sum(total) + sum(malformed)
            total_valid = j1_df["total"].astype(int).sum()
            total_malformed = j1_df["malformed"].astype(int).sum()
            expected_data_rows = counters["TOTAL_RECORDS"] - counters["HEADER_SKIPPED"]

            print(f"Reconciliation Check: {total_valid} + {total_malformed} = {total_valid + total_malformed} vs {expected_data_rows}")
            if (total_valid + total_malformed) != expected_data_rows:
                print("FAIL: Reconciliation invariant violated in baseline calculation!", file=sys.stderr)
                sys.exit(2)
            print("Reconciliation Invariant: PASS (sum(total + malformed) == TOTAL_RECORDS - HEADER_SKIPPED)")

            if args.mr_output:
                print(f"\nComparing baseline against MapReduce J1 output at: {args.mr_output}")
                if not verify_against_mr_j1(j1_df, args.mr_output):
                    print("\nFAIL: J1 verification checks failed.", file=sys.stderr)
                    sys.exit(1)

        if args.job in ["j2", "all"]:
            peakday_df, hourofday_df = compute_baseline_j2(j1_df)

            print("\n" + "=" * 60)
            print(" INDEPENDENT PANDAS BASELINE SUMMARY (JOB J2: PeakHour)")
            print("=" * 60)
            print("1. Peak Day Profile (date, peak_hour, peak_total, day_total):")
            print(peakday_df.to_string(index=False))
            print("\n2. Hour-of-Day Profile (hour, total_events, days_observed):")
            print(hourofday_df.to_string(index=False))
            print("=" * 60)

            if args.j2_output:
                print(f"\nComparing baseline against MapReduce J2 MultipleOutputs at: {args.j2_output}")
                if not verify_against_mr_j2(peakday_df, hourofday_df, args.j2_output):
                    print("\nFAIL: J2 verification checks failed.", file=sys.stderr)
                    sys.exit(1)

    # Job J3 Baseline
    prod_df = None
    cat_df = None
    j3_counters = None
    if args.job in ["j3", "j4", "all"]:
        prod_df, cat_df, j3_counters = compute_baseline_j3(csv_path)

        if args.job in ["j3", "all"]:
            print("\n" + "=" * 60)
            print(" INDEPENDENT PANDAS BASELINE SUMMARY (JOB J3: ViewCount)")
            print("=" * 60)
            print(f"Total View Events      : {j3_counters['VIEW_EVENTS']}")
            print(f"Views Without Category : {j3_counters['VIEWS_WITHOUT_CATEGORY']}")
            print(f"Unique Products Viewed : {len(prod_df)}")
            print(f"Unique Categories      : {len(cat_df)}")
            print("-" * 60)
            print("1. Product Views Profile (product_id, category_code, brand, views):")
            print(prod_df.to_string(index=False))
            print("\n2. Category Views Profile (category_code, views):")
            print(cat_df.to_string(index=False))
            print("=" * 60)

            total_prod_views = int(prod_df["views"].sum()) if not prod_df.empty else 0
            total_cat_views = int(cat_df["views"].sum()) if not cat_df.empty else 0

            print(f"Product Views Invariant Check : {total_prod_views} == {j3_counters['VIEW_EVENTS']}")
            if total_prod_views != j3_counters["VIEW_EVENTS"]:
                print("FAIL: Product views sum invariant violated!", file=sys.stderr)
                sys.exit(2)

            expected_cat_views = j3_counters["VIEW_EVENTS"] - j3_counters["VIEWS_WITHOUT_CATEGORY"]
            print(f"Category Views Invariant Check: {total_cat_views} == {expected_cat_views}")
            if total_cat_views != expected_cat_views:
                print("FAIL: Category views sum invariant violated!", file=sys.stderr)
                sys.exit(2)
            print("J3 Reconciliation Invariants  : PASS")

            if args.j3_output:
                print(f"\nComparing baseline against MapReduce J3 MultipleOutputs at: {args.j3_output}")
                if not verify_against_mr_j3(prod_df, cat_df, args.j3_output):
                    print("\nFAIL: J3 verification checks failed.", file=sys.stderr)
                    sys.exit(1)

    # Job J4 Baseline
    if args.job in ["j4", "all"]:
        top_prod_df, top_cat_df = compute_baseline_j4(prod_df, cat_df, topn=args.topn)

        print("\n" + "=" * 60)
        print(f" INDEPENDENT PANDAS BASELINE SUMMARY (JOB J4: TopN, N={args.topn})")
        print("=" * 60)
        print("1. Top-N Products (rank, product_id, category_code, brand, views):")
        print(top_prod_df.to_string(index=False))
        print(f"\n2. Top-N Categories (rank, category_code, views):")
        print(top_cat_df.to_string(index=False))
        print("=" * 60)

        # Invariant checks
        expected_prod_count = min(len(prod_df), args.topn)
        expected_cat_count = min(len(cat_df), args.topn)
        if len(top_prod_df) != expected_prod_count:
            print(f"FAIL: Top products row count {len(top_prod_df)} != expected {expected_prod_count}", file=sys.stderr)
            sys.exit(2)
        if len(top_cat_df) != expected_cat_count:
            print(f"FAIL: Top categories row count {len(top_cat_df)} != expected {expected_cat_count}", file=sys.stderr)
            sys.exit(2)

        # Monotonicity check
        if not top_prod_df.empty:
            views_list = top_prod_df["views"].astype(int).tolist()
            for i in range(len(views_list) - 1):
                if views_list[i] < views_list[i + 1]:
                    print("FAIL: Top products views not monotonically descending!", file=sys.stderr)
                    sys.exit(2)

        if not top_cat_df.empty:
            cat_views_list = top_cat_df["views"].astype(int).tolist()
            for i in range(len(cat_views_list) - 1):
                if cat_views_list[i] < cat_views_list[i + 1]:
                    print("FAIL: Top categories views not monotonically descending!", file=sys.stderr)
                    sys.exit(2)

        print("J4 Invariant Checks           : PASS (size bounded, ranks sequential, counts monotonic)")

        if args.top_products_output or args.top_categories_output:
            if not verify_against_mr_j4(top_prod_df, top_cat_df, args.top_products_output, args.top_categories_output):
                print("\nFAIL: J4 verification checks failed.", file=sys.stderr)
                sys.exit(1)

    # Job J5 Baseline
    j5_df = None
    if args.job in ["j5", "j6", "all"]:
        j5_df, j5_stats = compute_baseline_j5(csv_path)

        if args.job in ["j5", "all"]:
            print("\n" + "=" * 60)
            print(" INDEPENDENT PANDAS BASELINE SUMMARY (JOB J5: SessionFunnel)")
            print("=" * 60)
            print(f"Distinct Valid Sessions: {j5_stats['distinct_sessions']}")
            print(f"Total Raw Records      : {j5_stats['counters']['TOTAL_RECORDS']}")
            print(f"Valid Records          : {j5_stats['counters']['VALID_RECORDS']}")
            print(f"Header Skipped         : {j5_stats['counters']['HEADER_SKIPPED']}")
            print(f"Malformed Records      : {j5_stats['counters']['MALFORMED_RECORDS']}")
            print("-" * 60)
            print("Baseline Session Funnel Table (date, sessions, cart, purchase, abandoned, removes):")
            print(j5_df.to_string(index=False))
            print("=" * 60)

            # Invariant checks per SRS §3.3.8
            if not j5_df.empty:
                total_sess = int(j5_df["sessions"].sum())
                total_cart = int(j5_df["cart"].sum())
                total_purch = int(j5_df["purchase"].sum())
                total_aband = int(j5_df["abandoned"].sum())

                print(f"Distinct Sessions Check       : {total_sess} == {j5_stats['distinct_sessions']}")
                if total_sess != j5_stats["distinct_sessions"]:
                    print("FAIL: Total sessions sum != distinct sessions count!", file=sys.stderr)
                    sys.exit(2)

                print(f"Abandonment Invariant Check   : {total_aband} <= {total_cart}")
                if total_aband > total_cart:
                    print("FAIL: Abandoned sessions exceed cart sessions!", file=sys.stderr)
                    sys.exit(2)

                print("J5 Invariant Checks           : PASS (funnel consistency, abandonment bounded by cart)")

            if args.j5_output:
                if not verify_against_mr_j5(j5_df, args.j5_output):
                    print("\nFAIL: J5 verification checks failed.", file=sys.stderr)
                    sys.exit(1)

    # Job J6 Baseline
    if args.job in ["j6", "all"]:
        j6_df = compute_baseline_j6(j5_df)

        print("\n" + "=" * 60)
        print(" INDEPENDENT PANDAS BASELINE SUMMARY (JOB J6: DailyFunnel)")
        print("=" * 60)
        print(f"Unique Consolidated Dates : {len(j6_df)}")
        print("-" * 60)
        print("Consolidated Daily Funnel Table (date, sessions, cart_sessions, purchase_sessions, abandoned_sessions, remove_events):")
        print(j6_df.to_string(index=False))
        print("=" * 60)

        # Invariant checks per SRS §3.3.9
        if not j6_df.empty:
            total_j6_sess = int(j6_df["sessions"].sum())
            total_j6_cart = int(j6_df["cart"].sum())
            total_j6_purch = int(j6_df["purchase"].sum())
            total_j6_aband = int(j6_df["abandoned"].sum())
            total_j6_removes = int(j6_df["removes"].sum())

            total_j5_sess = int(j5_df["sessions"].sum())
            total_j5_cart = int(j5_df["cart"].sum())
            total_j5_purch = int(j5_df["purchase"].sum())
            total_j5_aband = int(j5_df["abandoned"].sum())
            total_j5_removes = int(j5_df["removes"].sum())

            print(f"Consolidation Invariant: sessions ({total_j6_sess} == {total_j5_sess}), cart ({total_j6_cart} == {total_j5_cart}), purchase ({total_j6_purch} == {total_j5_purch}), abandoned ({total_j6_aband} == {total_j5_aband}), removes ({total_j6_removes} == {total_j5_removes})")
            if (total_j6_sess != total_j5_sess or total_j6_cart != total_j5_cart or
                total_j6_purch != total_j5_purch or total_j6_aband != total_j5_aband or
                total_j6_removes != total_j5_removes):
                print("FAIL: J6 daily consolidation totals do not equal J5 sums!", file=sys.stderr)
                sys.exit(2)

            if total_j6_aband > total_j6_cart:
                print("FAIL: Consolidated abandoned sessions exceed cart sessions!", file=sys.stderr)
                sys.exit(2)

            print("J6 Invariant Checks           : PASS (global sum equality, abandonment bounded by cart)")

        if args.j6_output:
            if not verify_against_mr_j6(j6_df, args.j6_output):
                print("\nFAIL: J6 verification checks failed.", file=sys.stderr)
                sys.exit(1)

    print("\nALL CHECKS PASSED (Baselines verified against SRS rules and invariants)")
    sys.exit(0)


if __name__ == "__main__":
    main()
