"""
dashboard/app.py — Streamlit Analytics Dashboard for Project #53.
Batch MapReduce Clickstream Log Analysis.

Implements the five authoritative views specified in SRS §3.3.12 (FR-DSH-01..08) & PRD §6, §8:
1. Overview
2. Peak Traffic
3. Top Pages (Products & Categories)
4. Errors & Funnel
5. Job Runs & MapReduce Architecture

Consumes standardized CSV reports and metadata exported by scripts/export_reports.py.
"""

import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

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

import altair as alt
import pandas as pd
import streamlit as st

# Add project root and dashboard to path for data_loader
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dashboard import data_loader

# Configure page layout
st.set_page_config(
    page_title="Clickstream Analytics | Batch MapReduce",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------------------------------------------------------
# Custom Premium Dark CSS Design System
# -----------------------------------------------------------------------------
CUSTOM_DARK_CSS = """
<style>
/* Global resets & typography */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
}

/* Background gradient */
.stApp {
    background: radial-gradient(circle at 15% 15%, #0F172A 0%, #0B1220 100%);
    color: #F8FAFC;
}

/* Sidebar styling */
section[data-testid="stSidebar"] {
    background-color: #0F172A !important;
    border-right: 1px solid #1E293B;
}

/* Header Banner */
.header-container {
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.7) 0%, rgba(15, 23, 42, 0.8) 100%);
    border: 1px solid #1E293B;
    border-radius: 12px;
    padding: 18px 24px;
    margin-bottom: 24px;
    box-shadow: 0 4px 20px -2px rgba(0, 0, 0, 0.3);
}
.header-title {
    font-size: 1.55rem;
    font-weight: 700;
    color: #F8FAFC;
    letter-spacing: -0.02em;
    margin: 0;
    display: flex;
    align-items: center;
    gap: 10px;
}
.header-subtitle {
    font-size: 0.88rem;
    color: #94A3B8;
    margin-top: 4px;
}
.badge-group {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 12px;
    align-items: center;
}
.badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 10px;
    border-radius: 9999px;
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.03em;
}
.badge-success {
    background-color: rgba(16, 185, 129, 0.15);
    color: #34D399;
    border: 1px solid rgba(16, 185, 129, 0.3);
}
.badge-info {
    background-color: rgba(56, 189, 248, 0.15);
    color: #38BDF8;
    border: 1px solid rgba(56, 189, 248, 0.3);
}
.badge-neutral {
    background-color: rgba(100, 116, 139, 0.15);
    color: #CBD5E1;
    border: 1px solid rgba(100, 116, 139, 0.3);
}
.badge-dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background-color: currentColor;
}

/* Metric Cards */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
}
.kpi-card {
    background: #111827;
    border: 1px solid #1E293B;
    border-radius: 12px;
    padding: 18px 20px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    transition: transform 0.15s ease, border-color 0.15s ease;
}
.kpi-card:hover {
    border-color: #334155;
    transform: translateY(-1px);
}
.kpi-label {
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
    color: #94A3B8;
    letter-spacing: 0.05em;
    margin-bottom: 8px;
}
.kpi-value {
    font-size: 1.85rem;
    font-weight: 700;
    color: #F8FAFC;
    letter-spacing: -0.02em;
    line-height: 1.1;
    margin-bottom: 6px;
}
.kpi-subtext {
    font-size: 0.78rem;
    color: #64748B;
    line-height: 1.3;
}
.kpi-highlight-cyan {
    color: #38BDF8;
}
.kpi-highlight-emerald {
    color: #34D399;
}
.kpi-highlight-amber {
    color: #FBBF24;
}

/* Section Containers */
.section-card {
    background: #111827;
    border: 1px solid #1E293B;
    border-radius: 12px;
    padding: 20px 22px;
    margin-bottom: 24px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
}
.section-title {
    font-size: 1.15rem;
    font-weight: 600;
    color: #F1F5F9;
    margin-bottom: 4px;
    letter-spacing: -0.01em;
}
.section-subtitle {
    font-size: 0.82rem;
    color: #64748B;
    margin-bottom: 16px;
}

/* Funnel Visual Container */
.funnel-container {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    background: #0B1220;
    border: 1px solid #1E293B;
    border-radius: 10px;
    padding: 16px 20px;
    margin: 16px 0;
}
.funnel-step {
    flex: 1;
    text-align: center;
    padding: 12px;
    background: #111827;
    border: 1px solid #1E293B;
    border-radius: 8px;
}
.funnel-step-name {
    font-size: 0.75rem;
    font-weight: 600;
    color: #94A3B8;
    text-transform: uppercase;
}
.funnel-step-val {
    font-size: 1.45rem;
    font-weight: 700;
    color: #F8FAFC;
    margin: 4px 0;
}
.funnel-step-sub {
    font-size: 0.75rem;
    color: #38BDF8;
}
.funnel-arrow {
    font-size: 1.2rem;
    color: #475569;
    font-weight: bold;
}

/* Streamlit component adjustments */
div[data-testid="stMetricValue"] {
    font-size: 1.8rem !important;
    font-weight: 700 !important;
    color: #F8FAFC !important;
}
div[data-testid="stMetricLabel"] {
    font-size: 0.78rem !important;
    text-transform: uppercase !important;
    color: #94A3B8 !important;
    font-weight: 600 !important;
}

/* Tables */
div[data-testid="stDataFrame"] {
    border: 1px solid #1E293B;
    border-radius: 8px;
    overflow: hidden;
}
</style>
"""
st.markdown(CUSTOM_DARK_CSS, unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# Altair Dark Theme Helper
# -----------------------------------------------------------------------------
def apply_dark_theme(chart: alt.Chart) -> alt.Chart:
    """Applies a consistent, restrained dark color scheme to an Altair chart."""
    return (
        chart.configure_view(strokeWidth=0)
        .configure_axis(
            labelColor="#94A3B8",
            titleColor="#CBD5E1",
            gridColor="#1E293B",
            domainColor="#334155",
            tickColor="#334155",
            labelFontSize=11,
            titleFontSize=12,
        )
        .configure_legend(
            labelColor="#CBD5E1",
            titleColor="#E2E8F0",
            titleFontSize=12,
            labelFontSize=11,
        )
    )


def get_default_reports_dir() -> str:
    """Derives default reports directory from CLI args, env var, or workspace default."""
    for i, arg in enumerate(sys.argv):
        if arg == "--reports-dir" and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
        if arg.startswith("--reports-dir="):
            return arg.split("=", 1)[1]

    env_dir = os.environ.get("CLICKSTREAM_REPORTS_DIR")
    if env_dir:
        return env_dir

    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")


def render_sidebar(base_reports_dir: str) -> Tuple[Optional[str], str]:
    """Renders a compact, modern dark sidebar for run selection and navigation."""
    with st.sidebar:
        st.markdown(
            """
            <div style="padding: 10px 0 16px 0;">
                <div style="font-size: 1.25rem; font-weight: 700; color: #F8FAFC; display: flex; align-items: center; gap: 8px;">
                    <span style="color: #38BDF8;">⚡</span> Clickstream
                </div>
                <div style="font-size: 0.78rem; color: #64748B;">Batch MapReduce Analytics</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown('<div class="kpi-label">PIPELINE RUN SELECTION</div>', unsafe_allow_html=True)
        runs = data_loader.find_available_runs(base_reports_dir)

        selected_run = None
        if not runs:
            st.warning(f"No run folders found in `{base_reports_dir}`")
        else:
            selected_run = st.selectbox(
                "Select Run",
                options=runs,
                index=0,
                help="Select an exported pipeline run to view its analytics.",
                label_visibility="collapsed",
            )
            st.caption(f"Found {len(runs)} exported run(s)")

        st.markdown("<hr style='border: 0; border-top: 1px solid #1E293B; margin: 16px 0;'>", unsafe_allow_html=True)
        st.markdown('<div class="kpi-label">NAVIGATION WORKSPACE</div>', unsafe_allow_html=True)

        views = [
            "Overview",
            "Peak Traffic",
            "Top Pages",
            "Errors & Funnel",
            "Job Runs",
        ]
        selected_view = st.radio("Go to view:", views, index=0, label_visibility="collapsed")

        st.markdown("<hr style='border: 0; border-top: 1px solid #1E293B; margin: 16px 0;'>", unsafe_allow_html=True)
        st.markdown(
            f"""
            <div style="font-size: 0.72rem; color: #64748B; line-height: 1.4;">
                <div><strong>Stack:</strong> Hadoop 3.3.6 · OpenJDK 11</div>
                <div><strong>Orchestration:</strong> YARN CapacityScheduler</div>
                <div><strong>Reports:</strong> <code>{os.path.basename(base_reports_dir)}/</code></div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    return selected_run, selected_view


def render_top_header(data: Dict[str, Any], selected_run: str):
    """Renders a cohesive top header banner with execution metadata badges."""
    run_meta = data.get("run_meta", {})
    rep_meta = data.get("report_meta", {})

    status = run_meta.get("status", rep_meta.get("status", "UNKNOWN"))
    is_success = (status == "SUCCESS")
    status_badge_class = "badge-success" if is_success else "badge-neutral"

    duration_ms = run_meta.get("duration_ms", 0)
    dur_str = data_loader.format_duration_ms(duration_ms) if duration_ms > 0 else (
        f"{run_meta.get('duration_sec', 0)}s" if run_meta.get("duration_sec") else "N/A"
    )
    gen_time = rep_meta.get("generated_at", run_meta.get("end_time", run_meta.get("end", "N/A")))
    if gen_time and "T" in gen_time:
        gen_time_clean = gen_time.replace("Z", " UTC").replace("T", " ")
    else:
        gen_time_clean = gen_time

    st.markdown(
        f"""
        <div class="header-container">
            <div style="display: flex; justify-content: space-between; align-items: flex-start; flex-wrap: wrap; gap: 12px;">
                <div>
                    <h1 class="header-title">⚡ Clickstream Analytics</h1>
                    <div class="header-subtitle">Batch MapReduce Log Pipeline · Project #53 · Pseudo-Distributed YARN</div>
                </div>
                <div class="badge-group">
                    <span class="badge badge-info">
                        <span class="badge-dot"></span> RUN: {selected_run}
                    </span>
                    <span class="badge {status_badge_class}">
                        <span class="badge-dot"></span> STATUS: {status}
                    </span>
                    <span class="badge badge-neutral">
                        ⏱️ DURATION: {dur_str}
                    </span>
                    <span class="badge badge-neutral">
                        📅 GENERATED: {gen_time_clean}
                    </span>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# -----------------------------------------------------------------------------
# VIEW 1: OVERVIEW
# -----------------------------------------------------------------------------
def render_overview(data: Dict[str, Any]):
    """View 1: Overview — Summary KPIs + Hourly traffic trend + Data Quality snapshot."""
    kpis = data_loader.calculate_overview_kpis(data)

    # 1. Responsive KPI Grid
    st.markdown(
        f"""
        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-label">Total Clickstream Events</div>
                <div class="kpi-value">{kpis['total_records']:,}</div>
                <div class="kpi-subtext">Valid records + malformed records (reconciled)</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Overall Malformed Rate</div>
                <div class="kpi-value kpi-highlight-amber">{kpis['malformed_rate_pct']:.2f}%</div>
                <div class="kpi-subtext">{kpis['malformed_records']} malformed ({kpis.get('attributable_malformed', 0)} daily + {kpis.get('unattributable_malformed', 0)} unparseable) ÷ {kpis['total_records']} total</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Cart Abandonment Rate</div>
                <div class="kpi-value kpi-highlight-amber">{kpis['cart_abandonment_pct']:.2f}%</div>
                <div class="kpi-subtext">Cart sessions without purchase ÷ Cart sessions</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Purchase Conversion Rate</div>
                <div class="kpi-value kpi-highlight-emerald">{kpis['conversion_pct']:.2f}%</div>
                <div class="kpi-subtext">Purchase sessions ÷ Cart sessions</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Total Product Views</div>
                <div class="kpi-value kpi-highlight-cyan">{kpis['total_views']:,}</div>
                <div class="kpi-subtext">Product page view events recorded</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Days Covered</div>
                <div class="kpi-value">{kpis['days_covered']}</div>
                <div class="kpi-subtext">Distinct UTC calendar dates observed</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Overall Peak Traffic Hour</div>
                <div class="kpi-value kpi-highlight-cyan">{kpis['overall_peak_hour']}</div>
                <div class="kpi-subtext">Volume: {kpis['peak_hour_events']:,} events at peak</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Pipeline Run Status</div>
                <div class="kpi-value kpi-highlight-emerald">{kpis['run_status']}</div>
                <div class="kpi-subtext">Completed in {kpis['duration_str']}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 2. Hourly Traffic Trend Chart
    st.markdown(
        """
        <div class="section-card">
            <div class="section-title">Hourly Traffic Trends</div>
            <div class="section-subtitle">Aggregated clickstream volume across observed UTC hours (Job J1: HourlyTraffic)</div>
        """,
        unsafe_allow_html=True,
    )

    th = data["traffic_hourly"].copy()
    if not th.empty:
        valid_th = th[th["hour"].astype(str).str.isdigit()].copy()
        if not valid_th.empty:
            valid_th["events_total"] = valid_th["total"].astype(int)
            valid_th["views_cnt"] = valid_th["views"].astype(int)
            valid_th["carts_cnt"] = valid_th["carts"].astype(int)
            valid_th["purchases_cnt"] = valid_th["purchases"].astype(int)
            valid_th["removes_cnt"] = valid_th["removes"].astype(int)
            valid_th["timestamp_label"] = (
                valid_th["date"] + " " + valid_th["hour"].apply(lambda h: f"{int(h):02d}:00 UTC")
            )

            # Reshape for multi-series Altair chart
            chart_data = valid_th.melt(
                id_vars=["timestamp_label"],
                value_vars=["events_total", "views_cnt", "carts_cnt", "purchases_cnt"],
                var_name="Metric",
                value_name="Count",
            )
            metric_labels = {
                "events_total": "Total Events",
                "views_cnt": "Views",
                "carts_cnt": "Carts",
                "purchases_cnt": "Purchases",
            }
            chart_data["Metric"] = chart_data["Metric"].map(metric_labels)

            color_scale = alt.Scale(
                domain=["Total Events", "Views", "Carts", "Purchases"],
                range=["#38BDF8", "#818CF8", "#FBBF24", "#34D399"],
            )

            trend_chart = (
                alt.Chart(chart_data)
                .mark_line(point=True, strokeWidth=2.5)
                .encode(
                    x=alt.X("timestamp_label:N", title="UTC Hour", axis=alt.Axis(labelAngle=-25)),
                    y=alt.Y("Count:Q", title="Event Count"),
                    color=alt.Color("Metric:N", scale=color_scale, title="Series"),
                    tooltip=["timestamp_label", "Metric", "Count"],
                )
                .properties(height=300)
            )
            st.altair_chart(apply_dark_theme(trend_chart), use_container_width=True)
        else:
            st.info("No valid hourly traffic trend records available.")
    else:
        st.info("No hourly traffic records available for this run.")

    st.markdown("</div>", unsafe_allow_html=True)

    # 3. Two-Column Breakdown: Event Composition & Behavioral Funnel Snapshot
    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown(
            """
            <div class="section-card">
                <div class="section-title">Event Type Distribution</div>
                <div class="section-subtitle">Breakdown of valid clickstream events by user action</div>
            """,
            unsafe_allow_html=True,
        )
        total_views = kpis["total_views"]
        total_carts = kpis["total_carts"]
        total_purchases = kpis["total_purchases"]
        total_removes = kpis["total_removes"]

        event_dist = pd.DataFrame({
            "Event Type": ["Product Views", "Add to Cart", "Purchases", "Remove from Cart"],
            "Count": [total_views, total_carts, total_purchases, total_removes],
        })
        if event_dist["Count"].sum() > 0:
            bar_chart = (
                alt.Chart(event_dist)
                .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
                .encode(
                    y=alt.Y("Event Type:N", sort="-x", title=None),
                    x=alt.X("Count:Q", title="Total Events"),
                    color=alt.Color(
                        "Event Type:N",
                        scale=alt.Scale(
                            domain=["Product Views", "Add to Cart", "Purchases", "Remove from Cart"],
                            range=["#818CF8", "#FBBF24", "#34D399", "#F43F5E"],
                        ),
                        legend=None,
                    ),
                    tooltip=["Event Type", "Count"],
                )
                .properties(height=180)
            )
            st.altair_chart(apply_dark_theme(bar_chart), use_container_width=True)
        else:
            st.info("Zero event volume recorded.")
        st.markdown("</div>", unsafe_allow_html=True)

    with col_right:
        st.markdown(
            """
            <div class="section-card">
                <div class="section-title">Session Conversion Funnel</div>
                <div class="section-subtitle">Aggregated user progression from visit to purchase</div>
            """,
            unsafe_allow_html=True,
        )
        total_sess = kpis["total_sessions"]
        cart_sess = kpis["cart_sessions"]
        purch_sess = kpis["purchase_sessions"]

        st.markdown(
            f"""
            <div class="funnel-container">
                <div class="funnel-step">
                    <div class="funnel-step-name">Total Sessions</div>
                    <div class="funnel-step-val">{total_sess:,}</div>
                    <div class="funnel-step-sub">100% of visits</div>
                </div>
                <div class="funnel-arrow">→</div>
                <div class="funnel-step">
                    <div class="funnel-step-name">Cart Sessions</div>
                    <div class="funnel-step-val">{cart_sess:,}</div>
                    <div class="funnel-step-sub">{(cart_sess / total_sess * 100) if total_sess else 0:.1f}% intent</div>
                </div>
                <div class="funnel-arrow">→</div>
                <div class="funnel-step">
                    <div class="funnel-step-name">Purchases</div>
                    <div class="funnel-step-val">{purch_sess:,}</div>
                    <div class="funnel-step-sub">{kpis['conversion_pct']:.1f}% converted</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)

    # 4. Data Quality Daily Table Snapshot
    st.markdown(
        """
        <div class="section-card">
            <div class="section-title">Daily Data Quality Breakdown</div>
            <div class="section-subtitle">Standardized quality accounting by UTC date partition (quality_daily.csv)</div>
        """,
        unsafe_allow_html=True,
    )
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
        st.info("No daily quality breakdown records available.")
    st.markdown("</div>", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# VIEW 2: PEAK TRAFFIC
# -----------------------------------------------------------------------------
def render_peak_traffic(data: Dict[str, Any]):
    """View 2: Peak Traffic — Daily peak hour + Hour-of-day profile."""
    kpis = data_loader.calculate_overview_kpis(data)

    st.markdown(
        f"""
        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-label">Overall Peak Hour</div>
                <div class="kpi-value kpi-highlight-cyan">{kpis['overall_peak_hour']}</div>
                <div class="kpi-subtext">Highest traffic hour observed across all days</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Peak Hour Volume</div>
                <div class="kpi-value">{kpis['peak_hour_events']:,}</div>
                <div class="kpi-subtext">Total events recorded during peak hour</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Days Observed</div>
                <div class="kpi-value">{kpis['days_covered']}</div>
                <div class="kpi-subtext">Calendar dates in hour-of-day baseline</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Total Valid Events</div>
                <div class="kpi-value kpi-highlight-emerald">{kpis['valid_records']:,}</div>
                <div class="kpi-subtext">Valid events parsed without domain error</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Hour of Day Profile Chart (Reference Image 2)
    st.markdown(
        """
        <div class="section-card">
            <div class="section-title">Average Traffic by Hour of Day (UTC)</div>
            <div class="section-subtitle">Normalized distribution across 24 UTC hours (Job J2: PeakHour Profile)</div>
        """,
        unsafe_allow_html=True,
    )

    hod = data["hour_of_day_profile"].copy()
    if not hod.empty:
        hod["hour_int"] = hod["hour"].astype(int)
        hod["events_int"] = hod["total_events"].astype(int)
        hod["avg_events"] = hod["avg_events_per_day"].astype(float)
        hod["hour_label"] = hod["hour_int"].apply(lambda h: f"{h:02d}:00")

        # Flag peak hour for highlighting
        max_events = hod["events_int"].max()
        hod["is_peak"] = hod["events_int"] == max_events

        bar_chart = (
            alt.Chart(hod)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("hour_label:N", sort=alt.SortField("hour_int"), title="Hour of Day (UTC)"),
                y=alt.Y("events_int:Q", title="Total Event Volume"),
                color=alt.condition(
                    alt.datum.is_peak,
                    alt.value("#FBBF24"),  # Peak in amber
                    alt.value("#38BDF8"),  # Normal in sky blue
                ),
                tooltip=[
                    alt.Tooltip("hour_label:N", title="Hour (UTC)"),
                    alt.Tooltip("events_int:Q", title="Total Events"),
                    alt.Tooltip("days_observed:Q", title="Days Observed"),
                    alt.Tooltip("avg_events:Q", title="Avg Events/Day", format=".2f"),
                ],
            )
            .properties(height=300)
        )
        st.altair_chart(apply_dark_theme(bar_chart), use_container_width=True)
    else:
        st.info("No hour-of-day profile records available.")
    st.markdown("</div>", unsafe_allow_html=True)

    # Tables in two columns
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(
            """
            <div class="section-card">
                <div class="section-title">Daily Peak Hour Summary</div>
                <div class="section-subtitle">Peak hour identified per calendar day (peak_hour_daily.csv)</div>
            """,
            unsafe_allow_html=True,
        )
        pd_df = data["peak_hour_daily"].copy()
        if not pd_df.empty:
            st.dataframe(pd_df, use_container_width=True)
            st.download_button(
                label="Download peak_hour_daily.csv",
                data=pd_df.to_csv(index=False),
                file_name="peak_hour_daily.csv",
                mime="text/csv",
            )
        else:
            st.info("No peak hour daily data.")
        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        st.markdown(
            """
            <div class="section-card">
                <div class="section-title">Hourly Traffic Profile Data</div>
                <div class="section-subtitle">Aggregated totals and daily averages (hour_of_day_profile.csv)</div>
            """,
            unsafe_allow_html=True,
        )
        if not hod.empty:
            st.dataframe(
                hod[["hour", "total_events", "days_observed", "avg_events_per_day"]],
                use_container_width=True,
            )
            st.download_button(
                label="Download hour_of_day_profile.csv",
                data=hod.to_csv(index=False),
                file_name="hour_of_day_profile.csv",
                mime="text/csv",
            )
        else:
            st.info("No profile data.")
        st.markdown("</div>", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# VIEW 3: TOP PAGES (PRODUCTS & CATEGORIES)
# -----------------------------------------------------------------------------
def render_top_pages(data: Dict[str, Any]):
    """View 3: Top Pages — Products and Categories rankings."""
    st.markdown(
        """
        <div style="background-color: rgba(56, 189, 248, 0.08); border-left: 3px solid #38BDF8; padding: 12px 16px; border-radius: 6px; margin-bottom: 20px; font-size: 0.84rem; color: #CBD5E1;">
            <strong>Note on Clickstream Architecture:</strong> The raw e-commerce clickstream schema records individual product page views (<code>product_id</code>) and categorized paths (<code>category_code</code>) rather than conventional web URLs. In strict conformance with PRD §4 & §6.2, "Pages" represents top products and categories.
        </div>
        """,
        unsafe_allow_html=True,
    )

    tp_summary = data_loader.calculate_top_pages_summary(data)

    st.markdown(
        f"""
        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-label">Top Ranked Views</div>
                <div class="kpi-value kpi-highlight-cyan">{tp_summary['total_views_in_top']:,}</div>
                <div class="kpi-subtext">Views captured across top-ranked entities</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Categorised Views</div>
                <div class="kpi-value kpi-highlight-emerald">{tp_summary['categorised_views']:,}</div>
                <div class="kpi-subtext">Views with valid category taxonomy</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Uncategorised View Share</div>
                <div class="kpi-value kpi-highlight-amber">{tp_summary['uncategorised_share_pct']:.2f}%</div>
                <div class="kpi-subtext">Missing/blank category code share</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    tab_prod, tab_cat = st.tabs(["Top Products by Views", "Top Categories by Views"])

    with tab_prod:
        st.markdown(
            """
            <div class="section-card">
                <div class="section-title">Top Products Leaderboard</div>
                <div class="section-subtitle">Ranked by total page view volume (Job J4: TopN Products)</div>
            """,
            unsafe_allow_html=True,
        )
        tp = data["top_products"].copy()
        if not tp.empty:
            tp["views_int"] = tp["views"].astype(int)
            tp["display_label"] = tp.apply(
                lambda r: f"P#{r['product_id']} ({r['brand'] or 'Unknown'} / {r['category_code'] or 'Uncategorised'})",
                axis=1,
            )

            # Search filter
            search_query = st.text_input("Filter products by ID, Brand, or Category:", placeholder="Type to filter...")
            filtered_tp = tp
            if search_query:
                q = search_query.lower()
                filtered_tp = tp[
                    tp["product_id"].str.lower().str.contains(q)
                    | tp["brand"].str.lower().str.contains(q)
                    | tp["category_code"].str.lower().str.contains(q)
                ]

            if not filtered_tp.empty:
                prod_chart = (
                    alt.Chart(filtered_tp)
                    .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#38BDF8")
                    .encode(
                        y=alt.Y("display_label:N", sort="-x", title=None),
                        x=alt.X("views_int:Q", title="Page Views"),
                        tooltip=["rank", "product_id", "category_code", "brand", "views_int", "share_of_all_views_pct"],
                    )
                    .properties(height=max(160, len(filtered_tp) * 32))
                )
                st.altair_chart(apply_dark_theme(prod_chart), use_container_width=True)

                st.dataframe(
                    filtered_tp[["rank", "product_id", "category_code", "brand", "views", "share_of_all_views_pct"]],
                    use_container_width=True,
                )
            else:
                st.info(f"No products match filter: '{search_query}'")

            st.download_button(
                label="Download top_products.csv",
                data=tp.to_csv(index=False),
                file_name="top_products.csv",
                mime="text/csv",
            )
        else:
            st.info("No top products records available.")
        st.markdown("</div>", unsafe_allow_html=True)

    with tab_cat:
        st.markdown(
            """
            <div class="section-card">
                <div class="section-title">Top Categories Leaderboard</div>
                <div class="section-subtitle">Ranked by aggregated view volume (Job J4: TopN Categories)</div>
            """,
            unsafe_allow_html=True,
        )
        tc = data["top_categories"].copy()
        if not tc.empty:
            tc["views_int"] = tc["views"].astype(int)

            cat_chart = (
                alt.Chart(tc)
                .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#818CF8")
                .encode(
                    y=alt.Y("category_code:N", sort="-x", title=None),
                    x=alt.X("views_int:Q", title="Total Views"),
                    tooltip=["rank", "category_code", "views_int", "share_of_categorised_views_pct"],
                )
                .properties(height=max(140, len(tc) * 36))
            )
            st.altair_chart(apply_dark_theme(cat_chart), use_container_width=True)

            st.dataframe(
                tc[["rank", "category_code", "views", "share_of_categorised_views_pct"]],
                use_container_width=True,
            )
            st.download_button(
                label="Download top_categories.csv",
                data=tc.to_csv(index=False),
                file_name="top_categories.csv",
                mime="text/csv",
            )
        else:
            st.info("No top categories records available.")
        st.markdown("</div>", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# VIEW 4: ERRORS & FUNNEL
# -----------------------------------------------------------------------------
def render_errors_and_funnel(data: Dict[str, Any]):
    """View 4: Errors & Funnel — Data quality validation and session funnel stages."""
    kpis = data_loader.calculate_overview_kpis(data)

    tab_quality, tab_funnel = st.tabs(["Data Quality & Schema Errors", "Behavioral Session Funnel"])

    with tab_quality:
        st.markdown(
            f"""
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-label">Total Non-Header Events</div>
                    <div class="kpi-value">{kpis['total_records']:,}</div>
                    <div class="kpi-subtext">Sum of valid records and malformed records</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Valid Records</div>
                    <div class="kpi-value kpi-highlight-emerald">{kpis['valid_records']:,}</div>
                    <div class="kpi-subtext">{100.0 - kpis['malformed_rate_pct']:.2f}% clean records</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Total Malformed Records</div>
                    <div class="kpi-value kpi-highlight-amber">{kpis['malformed_records']:,}</div>
                    <div class="kpi-subtext">{kpis['malformed_rate_pct']:.4f}% overall defect rate</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Date-Attributable Malformed</div>
                    <div class="kpi-value">{kpis.get('attributable_malformed', 0):,}</div>
                    <div class="kpi-subtext">Valid timestamp; failed domain checks</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown(
            """
            <div class="section-card">
                <div class="section-title">Malformed Record Reasons Breakdown</div>
                <div class="section-subtitle">Distribution of parser failure categories (quality_reasons.csv)</div>
            """,
            unsafe_allow_html=True,
        )
        qr = data["quality_reasons"].copy()
        if not qr.empty:
            qr["count_int"] = qr["count"].astype(int)
            total_mal = qr["count_int"].sum()
            qr["share_pct"] = qr["count_int"].apply(lambda c: (c / total_mal * 100.0) if total_mal > 0 else 0.0)

            reason_chart = (
                alt.Chart(qr)
                .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#F87171")
                .encode(
                    y=alt.Y("reason:N", sort="-x", title=None),
                    x=alt.X("count_int:Q", title="Defect Count"),
                    tooltip=["reason", "count_int", alt.Tooltip("share_pct:Q", format=".2f", title="Share (%)")],
                )
                .properties(height=max(160, len(qr) * 28))
            )
            st.altair_chart(apply_dark_theme(reason_chart), use_container_width=True)

            st.dataframe(qr, use_container_width=True)
            st.download_button(
                label="Download quality_reasons.csv",
                data=qr.to_csv(index=False),
                file_name="quality_reasons.csv",
                mime="text/csv",
            )
        else:
            st.info("No error reasons recorded.")
        st.markdown("</div>", unsafe_allow_html=True)

    with tab_funnel:
        fd = data["funnel_daily"].copy()
        if not fd.empty:
            sess_total = int(fd["sessions"].astype(int).sum())
            cart_sess = int(fd["cart_sessions"].astype(int).sum())
            purch_sess = int(fd["purchase_sessions"].astype(int).sum())
            aban_sess = int(fd["abandoned_sessions"].astype(int).sum())
            rem_events = int(fd["remove_events"].astype(int).sum())

            st.markdown(
                f"""
                <div class="kpi-grid">
                    <div class="kpi-card">
                        <div class="kpi-label">Total Unique Sessions</div>
                        <div class="kpi-value">{sess_total:,}</div>
                        <div class="kpi-subtext">Distinct session identifiers</div>
                    </div>
                    <div class="kpi-card">
                        <div class="kpi-label">Cart Sessions</div>
                        <div class="kpi-value kpi-highlight-cyan">{cart_sess:,}</div>
                        <div class="kpi-subtext">{(cart_sess / sess_total * 100) if sess_total else 0:.1f}% intent share</div>
                    </div>
                    <div class="kpi-card">
                        <div class="kpi-label">Purchase Sessions</div>
                        <div class="kpi-value kpi-highlight-emerald">{purch_sess:,}</div>
                        <div class="kpi-subtext">{kpis['conversion_pct']:.2f}% conversion rate</div>
                    </div>
                    <div class="kpi-card">
                        <div class="kpi-label">Abandoned Sessions</div>
                        <div class="kpi-value kpi-highlight-amber">{aban_sess:,}</div>
                        <div class="kpi-subtext">{kpis['cart_abandonment_pct']:.2f}% abandonment rate</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            # Horizontal visual funnel
            st.markdown(
                f"""
                <div class="section-card">
                    <div class="section-title">Session Progression & Drop-off Flow</div>
                    <div class="section-subtitle">Multi-stage conversion path from visit to completed checkout</div>
                    <div class="funnel-container">
                        <div class="funnel-step">
                            <div class="funnel-step-name">1. All Sessions</div>
                            <div class="funnel-step-val">{sess_total:,}</div>
                            <div class="funnel-step-sub">100.0% Baseline</div>
                        </div>
                        <div class="funnel-arrow">→</div>
                        <div class="funnel-step">
                            <div class="funnel-step-name">2. Add to Cart</div>
                            <div class="funnel-step-val">{cart_sess:,}</div>
                            <div class="funnel-step-sub">{(cart_sess / sess_total * 100) if sess_total else 0:.1f}% Intent Rate</div>
                        </div>
                        <div class="funnel-arrow">→</div>
                        <div class="funnel-step">
                            <div class="funnel-step-name">3. Purchase Completed</div>
                            <div class="funnel-step-val">{purch_sess:,}</div>
                            <div class="funnel-step-sub">{kpis['conversion_pct']:.1f}% Conversion</div>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            st.markdown(
                """
                <div class="section-card">
                    <div class="section-title">Consolidated Daily Funnel Table</div>
                    <div class="section-subtitle">Daily session funnel metrics and rates (funnel_daily.csv)</div>
                """,
                unsafe_allow_html=True,
            )
            st.dataframe(fd, use_container_width=True)
            st.download_button(
                label="Download funnel_daily.csv",
                data=fd.to_csv(index=False),
                file_name="funnel_daily.csv",
                mime="text/csv",
            )
            st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info("No funnel data available for this run.")


# -----------------------------------------------------------------------------
# VIEW 5: JOB RUNS & MAPREDUCE ARCHITECTURE
# -----------------------------------------------------------------------------
def render_job_runs(data: Dict[str, Any]):
    """View 5: Job Runs — Pipeline status, job timings, counters, and report metadata."""
    run_meta = data["run_meta"]
    rep_meta = data["report_meta"]

    # 1. Pipeline Execution Overview
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Pipeline Run ID", run_meta.get("run_id", "N/A"))
        st.caption(f"Input: `{run_meta.get('input_path', run_meta.get('input', 'N/A'))}`")
    with col2:
        status = run_meta.get("status", "UNKNOWN")
        st.metric("Pipeline Status", status)
        st.caption(f"Invariant Checked: {'PASS' if run_meta.get('invariant_ok') else 'FAIL'}")
    with col3:
        duration_ms = run_meta.get("duration_ms", 0)
        dur_str = data_loader.format_duration_ms(duration_ms) if duration_ms > 0 else (
            f"{run_meta.get('duration_sec', 0)}s" if run_meta.get("duration_sec") else "N/A"
        )
        st.metric("Total Pipeline Duration", dur_str)
        st.caption(f"Work Dir: `{run_meta.get('work_dir', 'N/A')}`")

    st.markdown("<hr style='border: 0; border-top: 1px solid #1E293B; margin: 16px 0;'>", unsafe_allow_html=True)

    # 2. Pipeline Execution Stages Flow
    st.markdown(
        """
        <div class="section-card">
            <div class="section-title">MapReduce Pipeline Orchestration DAG</div>
            <div class="section-subtitle">6 Logical Stages (J1–J6) executed across 7 physical YARN MapReduce jobs</div>
            <div style="background: #0B1220; border: 1px solid #1E293B; border-radius: 8px; padding: 16px; font-family: monospace; font-size: 0.82rem; color: #CBD5E1; line-height: 1.6;">
                <div>[Raw Input] ──▶ <strong>Stage J1: HourlyTraffic</strong> (job_...0001 / job_...0008)</div>
                <div>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;├──▶ <strong>Stage J2: PeakHour</strong> (job_...0002 / job_...0009)</div>
                <div>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└──▶ <strong>Stage J3: ViewCount</strong> (job_...0003 / job_...0010)</div>
                <div>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;├──▶ <strong>Stage J4: TopN Products</strong> (job_...0004 / job_...0011)</div>
                <div>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└──▶ <strong>Stage J4: TopN Categories</strong> (job_...0005 / job_...0012)</div>
                <div>[Raw Input] ──▶ <strong>Stage J5: SessionFunnel</strong> (job_...0006 / job_...0013)</div>
                <div>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└──▶ <strong>Stage J6: DailyFunnel Consolidation</strong> (job_...0007 / job_...0014)</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 3. Executions Table
    st.markdown(
        """
        <div class="section-card">
            <div class="section-title">Physical YARN Job Executions (7 Jobs)</div>
            <div class="section-subtitle">Real YARN application identifiers, final statuses, and execution durations</div>
        """,
        unsafe_allow_html=True,
    )

    jobs_data = run_meta.get("jobs")
    stage_mapping = {
        "J1_HourlyTraffic": "Stage J1 (Hourly Traffic)",
        "J2_PeakHour": "Stage J2 (Peak Hour)",
        "J3_ViewCount": "Stage J3 (View Count)",
        "J4_TopN_Products": "Stage J4 (Top-N Products)",
        "J4_TopN_Categories": "Stage J4 (Top-N Categories)",
        "J5_SessionFunnel": "Stage J5 (Session Funnel)",
        "J6_DailyFunnel": "Stage J6 (Daily Funnel Consolidation)",
    }

    if jobs_data:
        job_rows = []
        if isinstance(jobs_data, dict):
            for jname, jinfo in jobs_data.items():
                dur_ms = jinfo.get("duration_ms", (jinfo.get("duration_sec", 0) * 1000))
                job_rows.append({
                    "Analytical Stage": stage_mapping.get(jname, jname),
                    "Job Stage": jname,
                    "Status": jinfo.get("status", "UNKNOWN"),
                    "Duration": data_loader.format_duration_ms(dur_ms),
                    "YARN Job ID": jinfo.get("app_id") or jinfo.get("job_id") or "Local/Standalone",
                })
        elif isinstance(jobs_data, list):
            for jinfo in jobs_data:
                jname = jinfo.get("name", "Unknown")
                dur_sec = jinfo.get("duration_sec", 0)
                dur_ms = jinfo.get("duration_ms", dur_sec * 1000)
                job_rows.append({
                    "Analytical Stage": stage_mapping.get(jname, jname),
                    "Job Stage": jname,
                    "Status": jinfo.get("status", "UNKNOWN"),
                    "Duration": f"{dur_sec}s" if dur_sec else data_loader.format_duration_ms(dur_ms),
                    "YARN Job ID": jinfo.get("job_id") or jinfo.get("app_id") or "Local/Standalone",
                })
        jdf = pd.DataFrame(job_rows)
        st.dataframe(jdf, use_container_width=True)
    else:
        st.info("No per-job execution records found in run_meta.json.")
    st.markdown("</div>", unsafe_allow_html=True)

    # 4. Pipeline Hadoop Counters
    st.markdown(
        """
        <div class="section-card">
            <div class="section-title">Hadoop Quality & System Counters</div>
            <div class="section-subtitle">Aggregated framework counters and data-quality counters recorded during run</div>
        """,
        unsafe_allow_html=True,
    )
    counters_dict = run_meta.get("counters", {})
    if counters_dict:
        counter_rows = [{"Counter Name": cname, "Value": f"{cval:,}" if isinstance(cval, int) else cval} for cname, cval in sorted(counters_dict.items())]
        st.dataframe(pd.DataFrame(counter_rows), use_container_width=True)
    else:
        st.info("No framework counters recorded in run_meta.json.")
    st.markdown("</div>", unsafe_allow_html=True)

    # 5. Exported Reports Summary
    st.markdown(
        """
        <div class="section-card">
            <div class="section-title">Standardized CSV Reports Manifest</div>
            <div class="section-subtitle">Row counts and files produced by scripts/export_reports.py (report_meta.json)</div>
        """,
        unsafe_allow_html=True,
    )
    rep_counts = rep_meta.get("reports", {})
    if rep_counts:
        rc_rows = [{"Report File": fn, "Records Exported": cnt} for fn, cnt in sorted(rep_counts.items())]
        st.dataframe(pd.DataFrame(rc_rows), use_container_width=True)
    else:
        st.info("No report counts available.")
    st.markdown("</div>", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# Main Application Flow
# -----------------------------------------------------------------------------
def main():
    """Main Streamlit application entrypoint."""
    reports_base = get_default_reports_dir()

    # Sidebar
    selected_run, selected_view = render_sidebar(reports_base)

    if not selected_run:
        st.markdown(
            f"""
            <div class="header-container">
                <h1 class="header-title">⚡ Clickstream Analytics</h1>
                <div class="header-subtitle">Batch MapReduce Log Pipeline · Project #53</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
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

    # Render persistent top header
    render_top_header(data, selected_run)

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
