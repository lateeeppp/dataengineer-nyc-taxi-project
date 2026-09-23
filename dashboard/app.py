"""
dashboard.app
Executive Business Intelligence Dashboard - NYC Yellow Taxi Analytics.
Design: Google Analytics / Looker Studio Minimalist Style (Pure White Canvas, Crisp Typography, No Emojis).
Connected live to Amazon Redshift Serverless via AWS Redshift Data API.
"""

import subprocess
import sys

from streamlit.runtime import exists

# Prevent NoSessionContext and missing ScriptRunContext warnings if user runs `uv run dashboard/app.py`
if not exists():
    cmd = [sys.executable, "-m", "streamlit", "run", __file__] + sys.argv[1:]
    sys.exit(subprocess.call(cmd))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

try:
    from dashboard.queries import (
        fetch_daily_metrics,
        fetch_mom_metrics,
        fetch_payment_breakdown,
        fetch_top_zones,
    )
except ModuleNotFoundError:
    from queries import (
        fetch_daily_metrics,
        fetch_mom_metrics,
        fetch_payment_breakdown,
        fetch_top_zones,
    )

# -----------------------------------------------------------------------------
# Streamlit Page Configuration (Must be first Streamlit call)
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="NYC Taxi BI Dashboard",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------------------------------------------------------
# Google Analytics / Modern Minimalist Custom CSS
# Pure white background, subtle light-gray sidebar, crisp typography, no emojis.
# -----------------------------------------------------------------------------
st.markdown(
    """
    <style>
        /* Base Canvas */
        .stApp {
            background-color: #ffffff;
            color: #202124;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
        }

        /* Sidebar Styling */
        section[data-testid="stSidebar"] {
            background-color: #f8f9fa;
            border-right: 1px solid #e9ecef;
            padding-top: 1.5rem;
        }
        section[data-testid="stSidebar"] div[data-testid="stMarkdownContainer"] p,
        section[data-testid="stSidebar"] div[data-testid="stMarkdownContainer"] span,
        section[data-testid="stSidebar"] label {
            color: #495057;
            font-size: 0.85rem;
        }

        /* Sidebar Brand & Section Labels */
        .sidebar-brand-title {
            font-size: 1.15rem;
            font-weight: 700;
            color: #202124;
            letter-spacing: -0.01em;
            margin-bottom: 2px;
        }
        .sidebar-brand-subtitle {
            font-size: 0.72rem;
            color: #70757a;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 10px;
        }
        .sidebar-section-label {
            font-size: 0.74rem;
            font-weight: 600;
            color: #5f6368;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-top: 1rem;
            margin-bottom: 0.5rem;
        }

        /* Radio Buttons Converted to Clean Navigation Pills */
        section[data-testid="stSidebar"] div[data-testid="stRadio"] > div[role="radiogroup"] {
            display: flex;
            flex-direction: column;
            gap: 4px;
        }
        section[data-testid="stSidebar"] div[data-testid="stRadio"] > div[role="radiogroup"] > label {
            background-color: transparent;
            padding: 8px 12px !important;
            border-radius: 6px !important;
            margin: 0 !important;
            cursor: pointer !important;
            transition: all 0.15s ease !important;
            display: flex !important;
            align-items: center !important;
            width: 100% !important;
            border: 1px solid transparent !important;
        }
        /* Hide default circular radio indicator */
        section[data-testid="stSidebar"] div[data-testid="stRadio"] > div[role="radiogroup"] > label > div:first-child {
            display: none !important;
        }
        /* Default item text */
        section[data-testid="stSidebar"] div[data-testid="stRadio"] > div[role="radiogroup"] > label p,
        section[data-testid="stSidebar"] div[data-testid="stRadio"] > div[role="radiogroup"] > label span {
            color: #495057 !important;
            font-size: 0.88rem !important;
            font-weight: 500 !important;
            margin: 0 !important;
        }
        /* Hover effect */
        section[data-testid="stSidebar"] div[data-testid="stRadio"] > div[role="radiogroup"] > label:hover {
            background-color: #f1f3f4 !important;
        }
        /* Active Selected Pill (Matches NYC Taxi BI Dashboard style) */
        section[data-testid="stSidebar"] div[data-testid="stRadio"] > div[role="radiogroup"] > label:has(input:checked) {
            background-color: #e8f0fe !important;
        }
        section[data-testid="stSidebar"] div[data-testid="stRadio"] > div[role="radiogroup"] > label:has(input:checked) p,
        section[data-testid="stSidebar"] div[data-testid="stRadio"] > div[role="radiogroup"] > label:has(input:checked) span {
            color: #1a73e8 !important;
            font-weight: 600 !important;
        }

        /* Clean High-Contrast Selectbox */
        section[data-testid="stSidebar"] div[data-testid="stSelectbox"] {
            margin-top: 4px;
        }
        section[data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] > div {
            border: 1px solid #dadce0 !important;
            border-radius: 6px !important;
            background-color: #ffffff !important;
            color: #202124 !important;
            font-size: 0.86rem !important;
            box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04) !important;
        }

        /* Main Page Typography */
        .main-page-title {
            font-size: 1.85rem;
            font-weight: 700;
            color: #202124;
            letter-spacing: -0.02em;
            margin-bottom: 1.25rem;
            line-height: 1.2;
        }
        .section-header {
            font-size: 1.05rem;
            font-weight: 600;
            color: #202124;
            margin-bottom: 0.85rem;
        }
        .section-subtext {
            font-size: 0.8rem;
            font-weight: 400;
            color: #5f6368;
            margin-top: -0.5rem;
            margin-bottom: 0.9rem;
        }

        /* Metric Block Styling (Borderless, Google Analytics Style) */
        .metric-container {
            display: flex;
            flex-direction: column;
            padding-right: 15px;
        }
        .metric-label {
            font-size: 0.8rem;
            font-weight: 500;
            color: #5f6368;
            margin-bottom: 6px;
            letter-spacing: -0.01em;
        }
        .metric-value {
            font-size: 1.75rem;
            font-weight: 600;
            color: #202124;
            letter-spacing: -0.02em;
            line-height: 1.15;
        }
        .metric-delta-positive {
            font-size: 0.76rem;
            font-weight: 600;
            color: #137333;
            margin-top: 4px;
        }
        .metric-delta-negative {
            font-size: 0.76rem;
            font-weight: 600;
            color: #c5221f;
            margin-top: 4px;
        }
        .metric-delta-neutral {
            font-size: 0.76rem;
            font-weight: 400;
            color: #70757a;
            margin-top: 4px;
        }

        /* Clean Divider */
        .clean-hr {
            border: 0;
            height: 1px;
            background-color: #e8eaed;
            margin: 1.6rem 0 1.6rem 0;
        }

        /* Data Table Styling */
        div[data-testid="stDataFrame"] {
            border: 1px solid #e8eaed;
            border-radius: 4px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------------------------------------------------------
# Data Loading & Redshift Caching
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Data Loading & Redshift Caching
# -----------------------------------------------------------------------------
@st.cache_data(ttl=600, show_spinner=False)
def load_mom_data():
    return fetch_mom_metrics()


@st.cache_data(ttl=600, show_spinner=False)
def load_daily_data():
    return fetch_daily_metrics()


@st.cache_data(ttl=600, show_spinner=False)
def load_zone_data():
    return fetch_top_zones()


@st.cache_data(ttl=600, show_spinner=False)
def load_payment_data():
    return fetch_payment_breakdown()


try:
    with st.spinner("Connecting to Amazon Redshift Serverless..."):
        df_mom = load_mom_data()
        df_daily = load_daily_data()
        df_zones = load_zone_data()
        df_payment = load_payment_data()
except Exception as e:
    st.error(f"Error connecting to Redshift: {e}")
    st.stop()

# -----------------------------------------------------------------------------
# Sidebar Navigation (Matches Reference Design in media_1790157671702.png)
# -----------------------------------------------------------------------------
with st.sidebar:
    st.markdown(
        """
        <div class="sidebar-brand-title">NYC Taxi Analytics</div>
        <div class="sidebar-brand-subtitle">Executive BI Dashboard</div>
        <hr class="clean-hr" style="margin: 0.4rem 0 0.8rem 0;">
        <div class="sidebar-section-label" style="margin-top: 0;">Navigation</div>
        """,
        unsafe_allow_html=True,
    )

    selected_page = st.radio(
        label="Navigation Menu",
        options=[
            "Summary & KPIs",
            "Zone & Route Analysis",
            "Payment Dynamics",
            "Data Catalog",
        ],
        index=0,
        label_visibility="collapsed",
    )

    st.markdown(
        """
        <hr class="clean-hr" style="margin: 1.2rem 0 0.6rem 0;">
        <div class="sidebar-section-label" style="margin-top: 0;">Period Filter</div>
        """,
        unsafe_allow_html=True,
    )

    all_periods = df_mom["period_label"].tolist()
    period_options = ["All Periods (Aggregated)"] + all_periods

    selected_period = st.selectbox(
        label="Select Reporting Period",
        options=period_options,
        index=0,
        label_visibility="collapsed",
    )

    st.markdown(
        """
        <hr class="clean-hr" style="margin: 1.4rem 0 1rem 0;">
        <div style="font-size: 0.72rem; color: #70757a; line-height: 1.6;">
            <strong style="color: #3c4043;">Data Warehouse</strong><br>
            Engine: Redshift Serverless<br>
            Database: dev<br>
            Schema: nyc_taxi_gold<br>
            Status: Synchronized
        </div>
        """,
        unsafe_allow_html=True,
    )

# -----------------------------------------------------------------------------
# Dynamic Slicing & Aggregation Engine (Sub-millisecond in-memory response)
# -----------------------------------------------------------------------------
is_all = selected_period == "All Periods (Aggregated)"
period_label_note = "" if is_all else f" ({selected_period})"

# 1. Basic Analysis: KPI scorecards data
df_kpi = df_mom if is_all else df_mom[df_mom["period_label"] == selected_period]

# 2. Basic Analysis: Dynamic Chart Traces
if is_all:
    chart_x = df_mom["period_label"]
    chart_trips = df_mom["total_trips"]
    chart_revenue = df_mom["total_revenue_usd"]
    chart_duration = df_mom["avg_duration_minutes"]
    chart_x_title = "Month"
    sub_area_title = "Monthly Completed Yellow Taxi Rides (All Periods)"
    sub_combo_title = "Monthly Gross Revenue vs. Average Ride Duration (All Periods)"
else:
    df_daily_sub = df_daily[df_daily["period_label"] == selected_period]
    chart_x = df_daily_sub["day_label"]
    chart_trips = df_daily_sub["total_trips"]
    chart_revenue = df_daily_sub["total_revenue_usd"]
    chart_duration = df_daily_sub["avg_duration_minutes"]
    chart_x_title = f"Days in {selected_period}"
    sub_area_title = f"Daily Completed Yellow Taxi Rides ({selected_period})"
    sub_combo_title = f"Daily Gross Revenue vs. Average Ride Duration ({selected_period})"

# 3. Zone & Route Analysis: Dynamic Aggregation
if is_all:
    df_zones_active = (
        df_zones.groupby(["borough", "zone"], as_index=False)
        .agg({
            "total_trips": "sum",
            "total_revenue_usd": "sum",
            "avg_fare_usd": "mean",
        })
        .sort_values("total_trips", ascending=False)
    )
else:
    df_zones_active = (
        df_zones[df_zones["period_label"] == selected_period]
        .groupby(["borough", "zone"], as_index=False)
        .agg({
            "total_trips": "sum",
            "total_revenue_usd": "sum",
            "avg_fare_usd": "mean",
        })
        .sort_values("total_trips", ascending=False)
    )

# 4. Payment Dynamics: Dynamic Aggregation
if is_all:
    df_pay_active = (
        df_payment.groupby("payment_type_name", as_index=False)
        .agg({
            "transaction_count": "sum",
            "total_volume_usd": "sum",
            "avg_tip_usd": "mean",
            "effective_tip_rate_pct": "mean",
        })
        .sort_values("transaction_count", ascending=False)
    )
else:
    df_pay_active = (
        df_payment[df_payment["period_label"] == selected_period]
        .groupby("payment_type_name", as_index=False)
        .agg({
            "transaction_count": "sum",
            "total_volume_usd": "sum",
            "avg_tip_usd": "mean",
            "effective_tip_rate_pct": "mean",
        })
        .sort_values("transaction_count", ascending=False)
    )

# -----------------------------------------------------------------------------
# PAGE 1: BASIC ANALYSIS (Google Analytics Layout with NYC Taxi Metrics)
# -----------------------------------------------------------------------------
if selected_page == "Summary & KPIs":
    st.markdown('<div class="main-page-title">Summary & KPIs</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-header">Key Metrics</div>', unsafe_allow_html=True)

    # Calculate Aggregates based on Period Selection
    total_trips_val = df_kpi["total_trips"].sum() if not df_kpi.empty else 0
    total_revenue_val = df_kpi["total_revenue_usd"].sum() if not df_kpi.empty else 0
    avg_distance_val = (
        df_kpi["avg_distance_miles"].mean()
        if not df_kpi.empty
        else 0.0
    )
    avg_duration_val = (
        df_kpi["avg_duration_minutes"].mean()
        if not df_kpi.empty
        else 0.0
    )

    # Latest MoM Growth
    latest_row = df_kpi.iloc[-1] if not df_kpi.empty else None
    mom_rev_pct = (
        latest_row["mom_revenue_growth_pct"] if latest_row is not None else None
    )
    mom_trip_pct = (
        latest_row["mom_trip_growth_pct"] if latest_row is not None else None
    )

    # 5 Horizontal Metric Columns (Borderless, Exact GA Layout)
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.markdown(
            f"""
            <div class="metric-container">
                <span class="metric-label">Total Trips</span>
                <span class="metric-value">{total_trips_val:,.0f}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            f"""
            <div class="metric-container">
                <span class="metric-label">Gross Revenue</span>
                <span class="metric-value">${total_revenue_val:,.0f}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col3:
        st.markdown(
            f"""
            <div class="metric-container">
                <span class="metric-label">Avg Trip Distance</span>
                <span class="metric-value">{avg_distance_val:.2f} mi</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col4:
        st.markdown(
            f"""
            <div class="metric-container">
                <span class="metric-label">Avg Trip Duration</span>
                <span class="metric-value">{avg_duration_val:.1f} min</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col5:
        # Display MoM Revenue Growth
        if mom_rev_pct is not None and not pd.isna(mom_rev_pct):
            growth_str = f"{mom_rev_pct:+.1f}%"
        else:
            growth_str = "Baseline"
        st.markdown(
            f"""
            <div class="metric-container">
                <span class="metric-label">MoM Revenue Growth</span>
                <span class="metric-value">{growth_str}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Clean horizontal divider line
    st.markdown('<hr class="clean-hr">', unsafe_allow_html=True)

    # -------------------------------------------------------------------------
    # Two Charts Row (Identical to Reference Layout)
    # Left: Filled Area Chart
    # Right: Dual-axis Bar + Line Chart
    # -------------------------------------------------------------------------
    chart_col_left, chart_col_right = st.columns(2)

    with chart_col_left:
        st.markdown(
            '<div class="section-header">Trip Volume Trend</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="section-subtext">{sub_area_title}</div>',
            unsafe_allow_html=True,
        )

        fig_area = go.Figure()
        fig_area.add_trace(
            go.Scatter(
                x=chart_x,
                y=chart_trips,
                mode="lines",
                fill="tozeroy",
                line=dict(color="#4285f4", width=2),
                fillcolor="rgba(66, 133, 244, 0.42)",
                name="Trips",
                hovertemplate="<b>%{x}</b><br>Trips: %{y:,.0f}<extra></extra>",
            )
        )

        fig_area.update_layout(
            paper_bgcolor="#ffffff",
            plot_bgcolor="#ffffff",
            height=340,
            margin=dict(l=40, r=20, t=10, b=40),
            font=dict(family="Roboto, sans-serif", size=11, color="#5f6368"),
            xaxis=dict(
                title=dict(text=chart_x_title, font=dict(size=12, color="#5f6368")),
                showgrid=False,
                linecolor="#dadce0",
                tickfont=dict(color="#5f6368", size=10),
            ),
            yaxis=dict(
                title=dict(text="Total Trips", font=dict(size=12, color="#5f6368")),
                showgrid=True,
                gridcolor="#f1f3f4",
                linecolor="#dadce0",
                zeroline=False,
                tickfont=dict(color="#5f6368", size=10),
            ),
            showlegend=False,
        )
        st.plotly_chart(fig_area, use_container_width=True)

    with chart_col_right:
        st.markdown(
            '<div class="section-header">Revenue &amp; Trip Duration</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="section-subtext">{sub_combo_title}</div>',
            unsafe_allow_html=True,
        )

        fig_combo = go.Figure()

        # Primary Bar Trace (Revenue in USD)
        fig_combo.add_trace(
            go.Bar(
                x=chart_x,
                y=chart_revenue,
                name="Gross Revenue ($)",
                marker_color="#4285f4",
                yaxis="y",
                hovertemplate="<b>%{x}</b><br>Revenue: $%{y:,.2f}<extra></extra>",
            )
        )

        # Secondary Line Trace (Average Duration in Minutes)
        fig_combo.add_trace(
            go.Scatter(
                x=chart_x,
                y=chart_duration,
                name="Avg Duration (min)",
                mode="lines+markers",
                line=dict(color="#ea8600", width=2),
                marker=dict(size=5, color="#ea8600"),
                yaxis="y2",
                hovertemplate="<b>%{x}</b><br>Avg Duration: %{y:.1f} min<extra></extra>",
            )
        )

        fig_combo.update_layout(
            paper_bgcolor="#ffffff",
            plot_bgcolor="#ffffff",
            height=340,
            margin=dict(l=40, r=40, t=10, b=40),
            font=dict(family="Roboto, sans-serif", size=11, color="#5f6368"),
            legend=dict(
                orientation="v",
                yanchor="top",
                y=0.98,
                xanchor="left",
                x=0.03,
                bgcolor="rgba(255, 255, 255, 0.85)",
                bordercolor="#e8eaed",
                borderwidth=1,
                font=dict(size=10, color="#3c4043"),
            ),
            xaxis=dict(
                title=dict(text=chart_x_title, font=dict(size=12, color="#5f6368")),
                showgrid=False,
                linecolor="#dadce0",
                tickfont=dict(color="#5f6368", size=10),
            ),
            yaxis=dict(
                title=dict(text="Gross Revenue ($ USD)", font=dict(size=12, color="#5f6368")),
                showgrid=True,
                gridcolor="#f1f3f4",
                linecolor="#dadce0",
                zeroline=False,
                tickfont=dict(color="#5f6368", size=10),
            ),
            yaxis2=dict(
                title=dict(text="Avg Duration (min)", font=dict(size=12, color="#5f6368")),
                overlaying="y",
                side="right",
                showgrid=False,
                zeroline=False,
                tickfont=dict(color="#5f6368", size=10),
            ),
        )
        st.plotly_chart(fig_combo, use_container_width=True)

# -----------------------------------------------------------------------------
# PAGE 2: ZONE & ROUTE ANALYSIS (100% Dynamically Filtered)
# -----------------------------------------------------------------------------
elif selected_page == "Zone & Route Analysis":
    st.markdown('<div class="main-page-title">Zone & Location Dynamics</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="section-header">Top Pickup Zones (Redshift dim_location){period_label_note}</div>', unsafe_allow_html=True)

    zone_col_left, zone_col_right = st.columns([1.5, 1])

    with zone_col_left:
        df_top10 = df_zones_active.head(10).sort_values("total_trips", ascending=True)
        fig_zone = px.bar(
            df_top10,
            x="total_trips",
            y="zone",
            orientation="h",
            color_discrete_sequence=["#4285f4"],
            text="total_trips",
        )
        fig_zone.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
        fig_zone.update_layout(
            paper_bgcolor="#ffffff",
            plot_bgcolor="#ffffff",
            height=380,
            margin=dict(l=10, r=40, t=10, b=30),
            font=dict(family="Roboto, sans-serif", size=11, color="#5f6368"),
            xaxis=dict(title="Trips", showgrid=True, gridcolor="#f1f3f4", zeroline=False),
            yaxis=dict(title="", showgrid=False),
        )
        st.plotly_chart(fig_zone, use_container_width=True)

    with zone_col_right:
        df_borough = df_zones_active.groupby("borough", as_index=False)["total_revenue_usd"].sum().sort_values("total_revenue_usd", ascending=False)
        fig_pie = px.pie(
            df_borough,
            names="borough",
            values="total_revenue_usd",
            color_discrete_sequence=["#4285f4", "#ea8600", "#34a853", "#9aa0a6", "#fbbc04"],
            hole=0.45,
        )
        fig_pie.update_layout(
            paper_bgcolor="#ffffff",
            plot_bgcolor="#ffffff",
            height=380,
            margin=dict(l=10, r=10, t=10, b=30),
            font=dict(family="Roboto, sans-serif", size=11, color="#5f6368"),
            legend=dict(orientation="h", y=-0.1),
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    st.markdown('<hr class="clean-hr">', unsafe_allow_html=True)
    st.markdown(f'<div class="section-header">Granular Location Rollup{period_label_note}</div>', unsafe_allow_html=True)
    st.dataframe(
        df_zones_active.style.format(
            {
                "total_trips": "{:,.0f}",
                "total_revenue_usd": "${:,.2f}",
                "avg_fare_usd": "${:,.2f}",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

# -----------------------------------------------------------------------------
# PAGE 3: PAYMENT DYNAMICS (100% Dynamically Filtered)
# -----------------------------------------------------------------------------
elif selected_page == "Payment Dynamics":
    st.markdown('<div class="main-page-title">Payment Dynamics & Tip Analysis</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="section-header">Settlement Methods & Tip Ratios{period_label_note}</div>', unsafe_allow_html=True)

    pay_left, pay_right = st.columns(2)

    with pay_left:
        fig_pay_vol = px.bar(
            df_pay_active,
            x="payment_type_name",
            y="transaction_count",
            color_discrete_sequence=["#4285f4"],
            text="transaction_count",
        )
        fig_pay_vol.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
        fig_pay_vol.update_layout(
            paper_bgcolor="#ffffff",
            plot_bgcolor="#ffffff",
            height=340,
            margin=dict(l=30, r=20, t=20, b=40),
            font=dict(family="Roboto, sans-serif", size=11, color="#5f6368"),
            xaxis=dict(title="Payment Type", showgrid=False),
            yaxis=dict(title="Transaction Count", showgrid=True, gridcolor="#f1f3f4"),
        )
        st.plotly_chart(fig_pay_vol, use_container_width=True)

    with pay_right:
        fig_tip = px.bar(
            df_pay_active,
            x="payment_type_name",
            y="effective_tip_rate_pct",
            color_discrete_sequence=["#34a853"],
            text="effective_tip_rate_pct",
        )
        fig_tip.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        fig_tip.update_layout(
            paper_bgcolor="#ffffff",
            plot_bgcolor="#ffffff",
            height=340,
            margin=dict(l=30, r=20, t=20, b=40),
            font=dict(family="Roboto, sans-serif", size=11, color="#5f6368"),
            xaxis=dict(title="Payment Type", showgrid=False),
            yaxis=dict(title="Tip Rate (%)", showgrid=True, gridcolor="#f1f3f4"),
        )
        st.plotly_chart(fig_tip, use_container_width=True)

    st.markdown('<hr class="clean-hr">', unsafe_allow_html=True)
    st.markdown(f'<div class="section-header">Settlement Audit Table{period_label_note}</div>', unsafe_allow_html=True)
    st.dataframe(
        df_pay_active.style.format(
            {
                "transaction_count": "{:,.0f}",
                "total_volume_usd": "${:,.2f}",
                "avg_tip_usd": "${:,.2f}",
                "effective_tip_rate_pct": "{:.2f}%",
            },
            na_rep="-",
        ),
        use_container_width=True,
        hide_index=True,
    )

# -----------------------------------------------------------------------------
# PAGE 4: DATA CATALOG
# -----------------------------------------------------------------------------
elif selected_page == "Data Catalog":
    st.markdown('<div class="main-page-title">Lakehouse Data Catalog</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-header">Star Schema Architecture (Kimball Model)</div>', unsafe_allow_html=True)

    catalog_data = [
        {"Table": "dim_date", "Type": "Dimension", "Grain": "Day", "Distribution": "ALL", "Description": "Temporal dimension with calendar attributes and date keys."},
        {"Table": "dim_location", "Type": "Dimension", "Grain": "TLC Taxi Zone (1..265)", "Distribution": "ALL", "Description": "Lookup table for borough, zone, and service zones."},
        {"Table": "dim_vendor", "Type": "Dimension", "Grain": "TPEP Provider", "Distribution": "ALL", "Description": "Creative Mobile Tech, VeriFone Inc."},
        {"Table": "dim_rate_code", "Type": "Dimension", "Grain": "Rate Code", "Distribution": "ALL", "Description": "Standard rate, JFK, Newark, Westchester, Negotiated, etc."},
        {"Table": "dim_payment_type", "Type": "Dimension", "Grain": "Payment Method", "Distribution": "ALL", "Description": "Credit card, Cash, No charge, Dispute, Unknown."},
        {"Table": "fact_yellow_taxi_trip", "Type": "Fact (Transactional)", "Grain": "Per Completed Ride", "Distribution": "KEY (pickup_location_key)", "Description": "Core measurement table storing trip fares, durations, distances, tips, and fees."},
    ]
    df_catalog = pd.DataFrame(catalog_data)
    st.dataframe(df_catalog, use_container_width=True, hide_index=True)

    st.markdown('<hr class="clean-hr">', unsafe_allow_html=True)
    st.markdown('<div class="section-header">Warehouse Specifications</div>', unsafe_allow_html=True)
    specs = {
        "Attribute": [
            "Compute Engine",
            "Storage Engine",
            "Query API",
            "Orchestrator",
            "Processing Engine",
            "Data Quality Gate",
        ],
        "Specification": [
            "Amazon Redshift Serverless (Workgroup: nyc-taxi-workgroup)",
            "Amazon S3 (Bronze, Silver, Gold S3 Buckets in Parquet)",
            "AWS Redshift Data API (Boto3 asynchronous execution)",
            "Apache Airflow 3.x (Docker Compose)",
            "AWS Glue 6.0 (Apache Spark 4.1 Serverless)",
            "Zero-NULL Quality Gate with AirflowSkipException",
        ],
    }
    st.dataframe(pd.DataFrame(specs), use_container_width=True, hide_index=True)
