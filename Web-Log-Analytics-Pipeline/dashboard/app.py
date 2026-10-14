"""
dashboard/app.py — Streamlit Analytics Dashboard for Project #53.

Implements the five authoritative views specified in SRS §3.3.12 (FR-DSH-01..08) & PRD §6, §8:
1. Overview
2. Peak Traffic
3. Top Pages
4. Errors & Funnel
5. Job Runs

Consumes standardized CSV reports and metadata exported by scripts/export_reports.py.
"""

import os
import sys
from typing import Any, Dict, Optional, Tuple

# Ensure UTF-8 stdout/stderr on Windows to prevent cp1252 charmap encoding errors
if sys.platform.startswith("win"):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# 1. Compatibility shim for Starlette >= 0.38 with Streamlit <= 1.61
try:
    import starlette.middleware.gzip as _sm_gzip
    if not hasattr(_sm_gzip, "DEFAULT_EXCLUDED_CONTENT_TYPES"):
        _sm_gzip.DEFAULT_EXCLUDED_CONTENT_TYPES = frozenset()
    if not hasattr(_sm_gzip, "IdentityResponder"):
        class _IdentityResponder:
            def __init__(self, app):
                self.app = app
            async def __call__(self, scope, receive, send):
                await self.app(scope, receive, send)
        _sm_gzip.IdentityResponder = _IdentityResponder
except ImportError:
    pass

import pandas as pd
import streamlit as st

# Add project root and dashboard to path for data_loader
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dashboard import data_loader

# Configure page layout
st.set_page_config(
    page_title="Clickstream Log Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


def get_default_reports_dir() -> str:
    """Derives default reports directory from CLI args, env var, or workspace default."""
    # Check CLI argument e.g. --reports-dir <dir>
    for i, arg in enumerate(sys.argv):
        if arg == "--reports-dir" and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
        if arg.startswith("--reports-dir="):
            return arg.split("=", 1)[1]

    # Check environment variable
    env_dir = os.environ.get("CLICKSTREAM_REPORTS_DIR")
    if env_dir:
        return env_dir

    # Workspace default
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")


def render_sidebar(base_reports_dir: str) -> Tuple[Optional[str], str]:
    """Renders sidebar run selector and navigation."""
    st.sidebar.title("Clickstream Analytics")
    st.sidebar.caption("Batch MapReduce Log Pipeline (Project #53)")

    st.sidebar.markdown("---")
    st.sidebar.subheader("Run Selector")

    runs = data_loader.find_available_runs(base_reports_dir)

    selected_run = None
    if not runs:
        st.sidebar.warning(f"No run folders found in `{base_reports_dir}`")
    else:
        selected_run = st.sidebar.selectbox(
            "Select Run (Newest First)",
            options=runs,
            index=0,
            help="Select an exported pipeline run to view its analytics."
        )

    st.sidebar.markdown("---")
    st.sidebar.subheader("Navigation")
    views = [
        "Overview",
        "Peak Traffic",
        "Top Pages",
        "Errors & Funnel",
        "Job Runs",
    ]
    selected_view = st.sidebar.radio("Go to view:", views, index=0)

    st.sidebar.markdown("---")
    st.sidebar.caption(f"Reports Base: `{os.path.basename(base_reports_dir)}`")

    return selected_run, selected_view


def render_overview(data: Dict[str, Any]):
    """View 1: Overview — Summary KPIs + Hourly traffic trend + Data Quality snapshot."""
    st.header("Overview")
    st.caption("High-level operational KPIs, traffic trends, and behavioral funnel summary.")

    kpis = data_loader.calculate_overview_kpis(data)

    # 1. Metric Cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Clickstream Events", f"{kpis['total_records']:,}")
        st.caption("Valid records + malformed records (reconciled)")
    with col2:
        st.metric("Overall Malformed Rate", f"{kpis['malformed_rate_pct']:.4f}%")
        st.caption("Malformed ÷ Total records")
    with col3:
        st.metric("Cart Abandonment Rate", f"{kpis['cart_abandonment_pct']:.2f}%")
        st.caption("Cart sessions without purchase ÷ Cart sessions")
    with col4:
        st.metric("Purchase Conversion Rate", f"{kpis['conversion_pct']:.2f}%")
        st.caption("Purchase sessions ÷ Cart sessions")

    col5, col6, col7, col8 = st.columns(4)
    with col5:
        st.metric("Days Covered", f"{kpis['days_covered']}")
        st.caption("Distinct UTC dates observed")
    with col6:
        st.metric("Overall Peak Traffic Hour", kpis["overall_peak_hour"])
        st.caption(f"Volume: {kpis['peak_hour_events']:,} events")
    with col7:
        st.metric("Total Views", f"{kpis['total_views']:,}")
        st.caption("Product page view events")
    with col8:
        st.metric("Pipeline Run Status", kpis["run_status"], delta=f"Duration: {kpis['duration_str']}")
        st.caption("Status recorded in run_meta.json")

    st.markdown("---")

    # 2. Hourly Traffic Line Chart
    st.subheader("Hourly Traffic Trends")
    th = data["traffic_hourly"].copy()
    if not th.empty:
        # Create a combined datetime or timestamp index for chart
        th["events_total"] = th["total"].astype(int)
        th["views_cnt"] = th["views"].astype(int)
        th["carts_cnt"] = th["carts"].astype(int)
        th["removes_cnt"] = th["removes"].astype(int)
        th["purchases_cnt"] = th["purchases"].astype(int)
        th["timestamp_label"] = th["date"] + " " + th["hour"].apply(lambda h: f"{int(h):02d}:00 UTC")

        chart_df = th.set_index("timestamp_label")[["events_total", "views_cnt", "carts_cnt", "purchases_cnt"]]
        st.line_chart(chart_df)
    else:
        st.info("No hourly traffic records available for this run.")

    st.markdown("---")

    # 3. Data Quality Snapshot
    st.subheader("Data Quality Daily Breakdown")
    qd = data["quality_daily"].copy()
    if not qd.empty:
        st.dataframe(qd, use_container_width=True)
        st.download_button(
            label="Download quality_daily.csv",
            data=qd.to_csv(index=False),
            file_name="quality_daily.csv",
            mime="text/csv",
        )
    else:
        st.info("No daily quality records available.")


def render_peak_traffic(data: Dict[str, Any]):
    """View 2: Peak Traffic — HOD bar chart, event breakdown, and daily peak table."""
    st.header("Peak Traffic")
    st.caption("Identification of high-load traffic hours, hour-of-day profiles, and hourly distribution in UTC.")

    # 1. Hour of Day Profile Bar Chart
    st.subheader("Hour-of-Day Traffic Profile (00–23 UTC)")
    hod = data["hour_of_day_profile"].copy()
    if not hod.empty:
        hod["hour_int"] = hod["hour"].astype(int)
        hod["total_events_int"] = hod["total_events"].astype(int)
        hod["avg_events_float"] = hod["avg_events_per_day"].astype(float)
        hod = hod.sort_values(by="hour_int")

        hod["hour_label"] = hod["hour_int"].apply(lambda h: f"{h:02d}:00")
        chart_data = hod.set_index("hour_label")[["total_events_int"]]
        st.bar_chart(chart_data)
        st.caption("Total traffic events aggregated across all observed days by UTC hour.")
    else:
        st.info("No hour-of-day profile data available.")

    st.markdown("---")

    # 2. Event Type Breakdown per Hour
    st.subheader("Event Type Composition per Hour")
    th = data["traffic_hourly"].copy()
    if not th.empty:
        th["views_cnt"] = th["views"].astype(int)
        th["carts_cnt"] = th["carts"].astype(int)
        th["removes_cnt"] = th["removes"].astype(int)
        th["purchases_cnt"] = th["purchases"].astype(int)
        th["hour_label"] = th["hour"].apply(lambda h: f"{int(h):02d}:00 UTC")

        # Group by hour to get profile across run
        by_hour = th.groupby("hour_label", as_index=False)[["views_cnt", "carts_cnt", "removes_cnt", "purchases_cnt"]].sum()
        st.bar_chart(by_hour.set_index("hour_label"))
        st.caption("Distribution of event types (Views, Add to Cart, Remove from Cart, Purchases) by hour.")

    st.markdown("---")

    # 3. Peak Hour Daily Table
    st.subheader("Daily Peak Hour Summary")
    pk = data["peak_hour_daily"].copy()
    if not pk.empty:
        st.dataframe(pk, use_container_width=True)
        st.download_button(
            label="Download peak_hour_daily.csv",
            data=pk.to_csv(index=False),
            file_name="peak_hour_daily.csv",
            mime="text/csv",
        )
    else:
        st.info("No daily peak hour data available.")


def render_top_pages(data: Dict[str, Any]):
    """View 3: Top Pages — Products and Categories rankings."""
    st.header("Top Pages")
    st.info(
        "Note: The clickstream dataset records product views and categories rather than conventional URLs. "
        "In accordance with PRD §4 & §6.2, 'Pages' represent individual product pages and product categories."
    )

    tp_summary = data_loader.calculate_top_pages_summary(data)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Views in Top Ranked", f"{tp_summary['total_views_in_top']:,}")
    with col2:
        st.metric("Categorised Views", f"{tp_summary['categorised_views']:,}")
    with col3:
        st.metric("Uncategorised View Share", f"{tp_summary['uncategorised_share_pct']:.2f}%")
        st.caption("Views with empty category_code ÷ Total Views")

    st.markdown("---")

    # Top Products
    st.subheader("Top Products by Views")
    tp = data["top_products"].copy()
    if not tp.empty:
        tp["views_int"] = tp["views"].astype(int)
        # Create descriptive entity label: product_id · brand · category
        tp["display_label"] = tp.apply(
            lambda r: f"P#{r['product_id']} ({r['brand'] or 'Unknown Brand'} / {r['category_code'] or 'Uncategorised'})",
            axis=1
        )
        st.bar_chart(tp.set_index("display_label")[["views_int"]])

        st.dataframe(tp[["rank", "product_id", "category_code", "brand", "views", "share_of_all_views_pct"]], use_container_width=True)
        st.download_button(
            label="Download top_products.csv",
            data=tp.to_csv(index=False),
            file_name="top_products.csv",
            mime="text/csv",
        )
    else:
        st.warning("No eligible product view records found in top_products.csv.")

    st.markdown("---")

    # Top Categories
    st.subheader("Top Categories by Views")
    tc = data["top_categories"].copy()
    if not tc.empty:
        tc["views_int"] = tc["views"].astype(int)
        st.bar_chart(tc.set_index("category_code")[["views_int"]])

        st.dataframe(tc[["rank", "category_code", "views", "share_of_categorised_views_pct"]], use_container_width=True)
        st.download_button(
            label="Download top_categories.csv",
            data=tc.to_csv(index=False),
            file_name="top_categories.csv",
            mime="text/csv",
        )
    else:
        st.warning("No eligible category view records found in top_categories.csv.")


def render_errors_and_funnel(data: Dict[str, Any]):
    """View 4: Errors & Funnel — Section A: Data Quality, Section B: Behavioral Funnel."""
    st.header("Errors & Funnel")
    st.caption("Data-quality malformed records and behavioral funnel attrition.")

    # ---------------- Section A: Data Quality ----------------
    st.subheader("Section A: Data-Quality Error Rate")
    st.markdown(
        "> **Formula:** $\\text{Malformed Rate} = \\frac{\\text{Malformed Records}}{\\text{Valid Records} + \\text{Malformed Records}} \\times 100\\%$"
    )

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Malformed Rate Trend per Day")
        qd = data["quality_daily"].copy()
        if not qd.empty:
            qd["rate_float"] = qd["malformed_rate_pct"].astype(float)
            st.line_chart(qd.set_index("date")[["rate_float"]])
        else:
            st.info("No daily quality data available.")

    with col2:
        st.markdown("#### Malformed Records by Reason")
        qr = data["quality_reasons"].copy()
        if not qr.empty:
            qr["count_int"] = qr["count"].astype(int)
            st.bar_chart(qr.set_index("reason")[["count_int"]])
        else:
            st.info("No quality reasons data available.")

    if not qr.empty:
        st.dataframe(qr, use_container_width=True)
        st.download_button(
            label="Download quality_reasons.csv",
            data=qr.to_csv(index=False),
            file_name="quality_reasons.csv",
            mime="text/csv",
        )

    st.markdown("---")

    # ---------------- Section B: Behavioral Funnel ----------------
    st.subheader("Section B: Behavioral Funnel & Conversion Rates")
    st.markdown(
        "> **Formulas:**\n"
        "> - **Cart Abandonment Rate:** $\\frac{\\text{Sessions with cart but no purchase}}{\\text{Sessions with cart}} \\times 100\\%$\n"
        "> - **Conversion Rate:** $\\frac{\\text{Sessions with purchase}}{\\text{Sessions with cart}} \\times 100\\%$\n"
        "> - **Remove Rate:** $\\frac{\\text{remove\\_from\\_cart events}}{\\text{cart events}} \\times 100\\%$"
    )

    fd = data["funnel_daily"].copy()
    if not fd.empty:
        # Funnel stage visual
        sess_total = fd["sessions"].astype(int).sum()
        cart_sess = fd["cart_sessions"].astype(int).sum()
        purch_sess = fd["purchase_sessions"].astype(int).sum()
        aban_sess = fd["abandoned_sessions"].astype(int).sum()

        fcol1, fcol2, fcol3, fcol4 = st.columns(4)
        with fcol1:
            st.metric("Total Sessions", f"{sess_total:,}")
            st.caption("Distinct user sessions")
        with fcol2:
            st.metric("Cart Sessions", f"{cart_sess:,}")
            st.caption("Sessions adding items to cart")
        with fcol3:
            st.metric("Purchase Sessions", f"{purch_sess:,}")
            st.caption("Sessions completing purchase")
        with fcol4:
            st.metric("Abandoned Sessions", f"{aban_sess:,}")
            st.caption("Cart sessions without purchase")

        st.markdown("#### Funnel Stage Progression (Sessions)")
        funnel_df = pd.DataFrame({
            "Funnel Stage": ["All Sessions", "Cart Sessions", "Purchase Sessions"],
            "Sessions": [sess_total, cart_sess, purch_sess],
        })
        st.bar_chart(funnel_df.set_index("Funnel Stage"))

        st.markdown("#### Consolidated Daily Funnel Table")
        st.dataframe(fd, use_container_width=True)
        st.download_button(
            label="Download funnel_daily.csv",
            data=fd.to_csv(index=False),
            file_name="funnel_daily.csv",
            mime="text/csv",
        )
    else:
        st.warning("No funnel data available for this run.")


def render_job_runs(data: Dict[str, Any]):
    """View 5: Job Runs — Pipeline status, job timings, counters, and report metadata."""
    st.header("Job Runs & Pipeline Diagnostics")
    st.caption("Observability into MapReduce execution, YARN job IDs, counters, and reconciliation invariants.")

    run_meta = data["run_meta"]
    rep_meta = data["report_meta"]

    # 1. Pipeline Execution Overview
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Pipeline Run ID", run_meta.get("run_id", "N/A"))
        st.caption(f"Input: `{run_meta.get('input_path', 'N/A')}`")
    with col2:
        status = run_meta.get("status", "UNKNOWN")
        delta_color = "normal" if status == "SUCCESS" else "inverse"
        st.metric("Pipeline Status", status)
        st.caption(f"Invariant Checked: {'PASS' if run_meta.get('invariant_ok') else 'FAIL'}")
    with col3:
        duration_ms = run_meta.get("duration_ms", 0)
        st.metric("Total Execution Duration", data_loader.format_duration_ms(duration_ms))
        st.caption(f"Work Dir: `{run_meta.get('work_dir', 'N/A')}`")

    # Mode indicator
    st.info(
        f"**Execution Timestamp:** Started `{run_meta.get('start_time', 'N/A')}` | Ended `{run_meta.get('end_time', 'N/A')}`\n\n"
        f"**Report Generated:** `{rep_meta.get('generated_at', 'N/A')}` (Schema v{rep_meta.get('schema_version', '1.0')})"
    )

    st.markdown("---")

    # 2. MapReduce Jobs Execution Stages (J1–J6)
    st.subheader("MapReduce Job Stages (J1–J6)")
    jobs_dict = run_meta.get("jobs", {})
    if jobs_dict:
        job_rows = []
        for jname, jinfo in jobs_dict.items():
            job_rows.append({
                "Job Stage": jname,
                "Status": jinfo.get("status", "UNKNOWN"),
                "Duration (ms)": jinfo.get("duration_ms", 0),
                "Duration Formatted": data_loader.format_duration_ms(jinfo.get("duration_ms")),
                "YARN App ID / Job ID": jinfo.get("app_id") or jinfo.get("job_id") or "Local/Standalone",
            })
        jdf = pd.DataFrame(job_rows)
        st.dataframe(jdf, use_container_width=True)
    else:
        st.info("No per-job execution records found in run_meta.json.")

    st.markdown("---")

    # 3. Pipeline Hadoop Counters
    st.subheader("Pipeline Quality & System Counters")
    counters_dict = run_meta.get("counters", {})
    if counters_dict:
        counter_rows = []
        for cname, cval in sorted(counters_dict.items()):
            counter_rows.append({
                "Counter Name": cname,
                "Value": cval,
            })
        cdf = pd.DataFrame(counter_rows)
        st.dataframe(cdf, use_container_width=True)
    else:
        st.info("No framework counters recorded in run_meta.json.")

    st.markdown("---")

    # 4. Exported Reports Summary
    st.subheader("Exported Report Record Counts")
    rep_counts = rep_meta.get("reports", {})
    if rep_counts:
        rc_rows = [{"Report File": fn, "Records Exported": cnt} for fn, cnt in sorted(rep_counts.items())]
        st.dataframe(pd.DataFrame(rc_rows), use_container_width=True)


def main():
    """Main Streamlit application entrypoint."""
    reports_base = get_default_reports_dir()

    # Sidebar
    selected_run, selected_view = render_sidebar(reports_base)

    if not selected_run:
        st.title("Batch Clickstream Analytics Dashboard")
        st.warning(f"No completed pipeline reports found in `{reports_base}`.")
        st.markdown(
            "### How to generate reports:\n"
            "Run the pipeline or export reports from an existing MapReduce run using:\n"
            "```bash\n"
            "python scripts/export_reports.py \\\n"
            "  --run-id daily-2019-11-01 \\\n"
            "  --work-dir /clickstream/work/daily-2019-11-01 \\\n"
            "  --reports-dir reports/daily-2019-11-01\n"
            "```"
        )
        return

    run_dir = os.path.join(reports_base, selected_run)

    # Load and validate run data
    try:
        data = data_loader.load_run_data(run_dir)
    except Exception as e:
        st.error(f"Failed to load run data from `{run_dir}`: {e}")
        st.info(
            "Ensure the selected directory contains valid `report_meta.json`, `run_meta.json`, "
            "and all 8 standardized CSV reports produced by `scripts/export_reports.py`."
        )
        return

    # Render selected view
    if selected_view == "Overview":
        render_overview(data)
    elif selected_view == "Peak Traffic":
        render_peak_traffic(data)
    elif selected_view == "Top Pages":
        render_top_pages(data)
    elif selected_view == "Errors & Funnel":
        render_errors_and_funnel(data)
    elif selected_view == "Job Runs":
        render_job_runs(data)


if __name__ == "__main__":
    main()
