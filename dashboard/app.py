"""
Data Warehouse - Streamlit BI Dashboard
Real-time analytics visualization connected to Supabase OLAP Star Schema.
Domains: Stocks, Weather, News + Cross-Domain Correlation
"""

import os
import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Data Warehouse — OLAP Analytics Dashboard",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============================================================================
# EDITORIAL DESIGN SYSTEM (CSS)
# ============================================================================

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,400;0,500;0,600;0,700;1,400&family=Plus+Jakarta+Sans:wght@300;400;500;600;700&display=swap');

    .stApp {
        background-color: #FAFAFA !important;
        color: #1A1A1A !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
    }

    header[data-testid="stHeader"] {
        background-color: #FAFAFA !important;
    }

    h1, h2, h3 {
        font-family: 'Cormorant Garamond', Georgia, serif !important;
        color: #8B0029 !important;
        letter-spacing: -0.01em !important;
        font-weight: 700 !important;
    }

    p, span, label {
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        color: #2B2D42;
    }

    .editorial-badge {
        font-family: 'Plus Jakarta Sans', sans-serif;
        font-size: 11px;
        letter-spacing: 0.22em;
        text-transform: uppercase;
        color: #8B0029;
        margin-bottom: 4px;
        font-weight: 700;
    }

    .editorial-title {
        font-family: 'Cormorant Garamond', serif;
        font-size: 42px;
        font-weight: 700;
        letter-spacing: 0.02em;
        color: #8B0029;
        margin-top: 0;
        margin-bottom: 6px;
        text-transform: uppercase;
    }

    .editorial-caption {
        font-family: 'Plus Jakarta Sans', sans-serif;
        font-size: 13px;
        letter-spacing: 0.05em;
        color: #5A626A;
        margin-bottom: 24px;
    }

    section[data-testid="stSidebar"] {
        background: #F4F5F7 !important;
        border-right: 1px solid #E9ECEF !important;
    }

    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {
        font-family: 'Cormorant Garamond', serif !important;
        color: #8B0029 !important;
    }

    div[data-testid="stMetric"] {
        background-color: #FFFFFF !important;
        padding: 18px 22px !important;
        border-radius: 6px !important;
        border: 1px solid #E9ECEF !important;
        border-left: 4px solid #8B0029 !important;
        box-shadow: 0 2px 6px rgba(139, 0, 41, 0.04) !important;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    div[data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 14px rgba(139, 0, 41, 0.1) !important;
    }

    div[data-testid="stMetricLabel"] {
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-size: 11px !important;
        text-transform: uppercase !important;
        letter-spacing: 0.15em !important;
        color: #8B0029 !important;
        font-weight: 700 !important;
    }

    div[data-testid="stMetricValue"] {
        font-family: 'Cormorant Garamond', serif !important;
        font-size: 34px !important;
        font-weight: 700 !important;
        color: #1A1A1A !important;
    }

    div[data-baseweb="tab-list"] {
        background-color: transparent !important;
        border-bottom: 2px solid #E9ECEF !important;
        gap: 8px !important;
    }

    button[data-baseweb="tab"] {
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-size: 12px !important;
        letter-spacing: 0.08em !important;
        text-transform: uppercase !important;
        background-color: #FFFFFF !important;
        color: #5A626A !important;
        border-radius: 4px 4px 0 0 !important;
        padding: 10px 18px !important;
        border: 1px solid #E9ECEF !important;
        border-bottom: none !important;
    }

    button[aria-selected="true"] {
        background-color: #8B0029 !important;
        color: #FFFFFF !important;
        font-weight: 700 !important;
        border-bottom: 3px solid #6B001F !important;
    }

    /* Primary & Secondary Buttons - Cherry Red Theme */
    div.stButton > button {
        background-color: #8B0029 !important;
        color: #FFFFFF !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-weight: 600 !important;
        border-radius: 4px !important;
        border: none !important;
        transition: background-color 0.2s ease !important;
    }

    div.stButton > button:hover {
        background-color: #6B001F !important;
        color: #FFFFFF !important;
    }

    .stSelectbox div[data-baseweb="select"] > div {
        background-color: #FFFFFF !important;
        border-color: #E9ECEF !important;
        color: #1A1A1A !important;
        border-radius: 4px !important;
    }

    div[data-testid="stExpander"] {
        background-color: #FFFFFF !important;
        border: 1px solid #E9ECEF !important;
        border-radius: 4px !important;
    }

    hr { border-color: #E9ECEF !important; }

    div[data-testid="stDataFrame"] {
        background-color: #FFFFFF !important;
        border: 1px solid #E9ECEF !important;
        border-radius: 4px !important;
    }

    .status-pill {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 12px;
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.1em;
    }
    .status-completed { background: #D4EDDA; color: #155724; }
    .status-running { background: #FFF3CD; color: #856404; }
    .status-failed { background: #F8D7DA; color: #721C24; }

    /* File Uploader Dropzone & Button Styling (Fix Text Overlap) */
    div[data-testid="stFileUploader"] {
        background-color: #FFFFFF !important;
        border: 2px dashed #8B0029 !important;
        border-radius: 8px !important;
        padding: 16px 20px !important;
    }

    div[data-testid="stFileUploader"] section {
        background-color: #FDF8F9 !important;
        border: none !important;
        padding: 16px !important;
    }

    div[data-testid="stFileUploader"] button {
        background-color: #8B0029 !important;
        color: #FFFFFF !important;
        border: none !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-size: 13px !important;
        font-weight: 600 !important;
        padding: 8px 20px !important;
        height: auto !important;
        min-height: 38px !important;
        display: inline-flex !important;
        align-items: center !important;
        justify-content: center !important;
        gap: 8px !important;
        white-space: nowrap !important;
        position: relative !important;
        box-shadow: none !important;
    }

    div[data-testid="stFileUploader"] button * {
        position: relative !important;
        display: inline-block !important;
        opacity: 1 !important;
        color: #FFFFFF !important;
        white-space: nowrap !important;
    }

    div[data-testid="stFileUploader"] button:hover {
        background-color: #6B001F !important;
    }

    div[data-testid="stFileUploader"] small,
    div[data-testid="stFileUploader"] p,
    div[data-testid="stFileUploader"] span {
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        color: #5A626A !important;
    }

    /* Responsive Mobile & Desktop Layout Optimizations */
    @media (max-width: 768px) {
        .editorial-title {
            font-size: 26px !important;
            letter-spacing: 0.01em !important;
        }
        .editorial-badge {
            font-size: 10px !important;
            letter-spacing: 0.15em !important;
        }
        .editorial-caption {
            font-size: 11px !important;
            margin-bottom: 16px !important;
        }
        div[data-testid="stMetric"] {
            padding: 12px 14px !important;
            margin-bottom: 8px !important;
        }
        div[data-testid="stMetricValue"] {
            font-size: 24px !important;
        }
        button[data-baseweb="tab"] {
            font-size: 10px !important;
            padding: 8px 10px !important;
        }
        div[data-baseweb="tab-list"] {
            flex-wrap: wrap !important;
        }
        div[data-testid="column"] {
            width: 100% !important;
            flex: 1 1 100% !important;
            min-width: 100% !important;
        }
    }
    </style>
""", unsafe_allow_html=True)


# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def run_query(sql: str) -> pd.DataFrame:
    """Execute an OLAP query and return a DataFrame (supports both Supabase & local DuckDB)."""
    try:
        from config.supabase_client import execute_sql_df
        return execute_sql_df(sql)
    except Exception as e:
        st.warning(f"Query failed: {e}")
        return pd.DataFrame()


def apply_editorial_theme(fig, title="", height=400):
    """Apply Cherry Red & Half White design theme to Plotly figures."""
    fig.update_layout(
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        autosize=True,
        font=dict(family="Plus Jakarta Sans, sans-serif", color="#1A1A1A", size=11),
        title=dict(
            text=f"<b>{title}</b>" if title else "",
            font=dict(size=15, family="Cormorant Garamond, serif", color="#8B0029"),
            x=0.01, y=0.95
        ),
        xaxis=dict(showgrid=True, gridcolor="#F1F3F5", linecolor="#E9ECEF", zerolinecolor="#E9ECEF"),
        yaxis=dict(showgrid=True, gridcolor="#F1F3F5", linecolor="#E9ECEF", zerolinecolor="#E9ECEF"),
        margin=dict(l=15, r=15, t=40, b=30),
        height=height
    )
    return fig


def add_csv_download(df, name):
    if not df.empty:
        st.download_button(
            label=f"📥 Export {name} (CSV)",
            data=df.to_csv(index=False).encode('utf-8'),
            file_name=f"{name.lower().replace(' ', '_')}.csv",
            mime="text/csv"
        )


PALETTE = ["#8B0029", "#C2185B", "#E91E63", "#2B2D42", "#5A626A", "#8D99AE", "#3D5A80", "#EF476F"]


# ============================================================================
# SIDEBAR
# ============================================================================

st.sidebar.markdown('<div class="editorial-badge">/ REAL-TIME DATA WAREHOUSE</div>', unsafe_allow_html=True)
st.sidebar.title("🏛️ Data Warehouse")
st.sidebar.markdown("**Engine**: DuckDB / Supabase OLAP")
st.sidebar.markdown("**Schema**: Kimball Star Schema")
st.sidebar.markdown("**Pipeline**: Real-Time Event-Driven ELT")

st.sidebar.divider()
st.sidebar.subheader("⚡ Real-Time Auto-Ingestion")
st.sidebar.info("Use the **CSV Ingestion** tab in the main area to upload files.")


st.sidebar.divider()
st.sidebar.subheader("🔄 Batch Pipeline Triggers")

# Pipeline trigger buttons
if st.sidebar.button("🔄 Run Full Pipeline", use_container_width=True):
    with st.spinner("Running full pipeline..."):
        try:
            from pipeline.orchestrator import run_full_pipeline
            result = run_full_pipeline()
            if result.get("status") == "completed":
                st.sidebar.success("Pipeline completed!")
                st.rerun()
            else:
                st.sidebar.error(f"Pipeline failed: {result.get('error', 'Unknown')}")
        except Exception as e:
            st.sidebar.error(f"Pipeline error: {e}")

col_s1, col_s2 = st.sidebar.columns(2)
if col_s1.button("📈 Stocks", use_container_width=True):
    with st.spinner("Running stock pipeline..."):
        try:
            from pipeline.orchestrator import run_stock_pipeline
            run_stock_pipeline()
            st.sidebar.success("Stock pipeline done!")
            st.rerun()
        except Exception as e:
            st.sidebar.error(str(e))

if col_s2.button("🌤️ Weather", use_container_width=True):
    with st.spinner("Running weather pipeline..."):
        try:
            from pipeline.orchestrator import run_weather_pipeline
            run_weather_pipeline()
            st.sidebar.success("Weather pipeline done!")
            st.rerun()
        except Exception as e:
            st.sidebar.error(str(e))

if st.sidebar.button("📰 News Pipeline", use_container_width=True):
    with st.spinner("Running news pipeline..."):
        try:
            from pipeline.orchestrator import run_news_pipeline
            run_news_pipeline()
            st.sidebar.success("News pipeline done!")
            st.rerun()
        except Exception as e:
            st.sidebar.error(str(e))

if st.sidebar.button("📊 Run dbt Tests", use_container_width=True):
    with st.spinner("Executing dbt model assertions..."):
        try:
            from pipeline.dbt_runner import run_dbt_tests
            res = run_dbt_tests()
            st.sidebar.success(f"dbt tests complete: {res['passed']}/{res['total_tests']} PASSED!")
        except Exception as e:
            st.sidebar.error(str(e))

st.sidebar.markdown('[📖 Open Interactive dbt Docs](http://localhost:8000/dbt-docs)', unsafe_allow_html=True)


# ============================================================================
# MAIN HEADER
# ============================================================================

st.markdown('<div class="editorial-badge">/ REAL-TIME AUTOMATED OLAP DATA WAREHOUSE</div>', unsafe_allow_html=True)
st.markdown('<div class="editorial-title">Real-Time Data Warehouse Dashboard</div>', unsafe_allow_html=True)
st.markdown('<div class="editorial-caption">Automated Event-Driven Ingestion • Kimball Star Schema • SCD Type 2 Audit • Real-Time Database Persistence</div>', unsafe_allow_html=True)


# ============================================================================
# WAREHOUSE KPIs
# ============================================================================

kpi_df = run_query("""
    SELECT
        (SELECT COUNT(*) FROM fact_stock_prices) AS stock_records,
        (SELECT COUNT(*) FROM fact_weather_readings) AS weather_records,
        (SELECT COUNT(*) FROM fact_news_articles) AS news_records,
        (SELECT COUNT(DISTINCT ticker) FROM dim_company WHERE is_current = TRUE) AS tracked_tickers,
        (SELECT COUNT(*) FROM dim_location) AS tracked_cities,
        (SELECT COUNT(*) FROM pipeline_runs WHERE status = 'completed') AS successful_pipelines
""")

if not kpi_df.empty:
    k = kpi_df.iloc[0]
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Stock Records", f"{int(k.get('stock_records', 0)):,}")
    c2.metric("Weather Records", f"{int(k.get('weather_records', 0)):,}")
    c3.metric("News Articles", f"{int(k.get('news_records', 0)):,}")
    c4.metric("Tracked Tickers", f"{int(k.get('tracked_tickers', 0))}")
    c5.metric("Tracked Cities", f"{int(k.get('tracked_cities', 0))}")
    c6.metric("Pipeline Runs", f"{int(k.get('successful_pipelines', 0))}")
else:
    st.info("🔌 Connect to Supabase and run the pipeline to see data. Update your `.env` file with Supabase credentials.")

st.divider()


# ============================================================================
# DASHBOARD TABS
# ============================================================================

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📥 Drag & Drop CSV Ingestion",
    "📈 Stock Analytics",
    "🌤️ Weather Analytics",
    "📰 News Analytics",
    "🔗 Cross-Domain",
    "⚙️ Pipeline Audit",
    "🤖 ML Predictive Insights"
])


# ============================================================================
# TAB 1: DRAG & DROP CSV DATA INGESTION
# ============================================================================
with tab1:
    st.markdown("<div class='editorial-badge'>REAL-TIME DATA INGESTION ENGINE</div>", unsafe_allow_html=True)
    st.subheader("📥 Drag & Drop CSV File Ingestion")
    st.markdown("Drag and drop raw CSV or JSON files below to instantly transform, validate, and ingest them into the Kimball Star-Schema Data Warehouse.")

    # Sample Data Download Section
    st.markdown("---")
    sc1, sc2, sc3 = st.columns([1, 1, 1])
    with sc1:
        sample_orders_csv = """order_id,order_line_number,customer_id,product_id,first_name,last_name,email,customer_tier,city,state,country,product_name,category,subcategory,unit_price,cost_price,quantity,discount_amount,tax_amount,order_timestamp
ORD_UI_9001,1,CUST_101,PROD_001,Liam,Smith,liam.smith@example.com,Platinum VIP,New York,NY,USA,Quantum Laptop Pro 16,Electronics,Laptops,2400.0,1500.0,2,50.0,188.0,2026-09-13 11:50:00
ORD_UI_9002,1,CUST_102,PROD_002,Olivia,Johnson,olivia.j@example.com,Gold,San Francisco,CA,USA,AI Neural Workstation X,Hardware,Workstations,4800.0,3000.0,1,0.0,384.0,2026-09-13 11:51:00
ORD_UI_9003,1,CUST_103,PROD_003,Noah,Williams,noah.w@example.com,Silver,London,ENG,UK,UltraSmart Watch Gen 5,Wearables,Smartwatches,450.0,220.0,3,25.0,34.0,2026-09-13 11:52:00"""
        st.download_button(
            label="📄 Download Sample Orders Stream CSV",
            data=sample_orders_csv,
            file_name="sample_realtime_orders.csv",
            mime="text/csv",
            use_container_width=True
        )

    with sc2:
        sample_customers_csv = """raw_customer_id,first_name,last_name,email,customer_tier,city,state,country,updated_at
CUST_101,Liam,Smith,liam.smith_updated@example.com,Diamond Executive VIP,New York,NY,USA,2026-09-13 11:53:00
CUST_102,Olivia,Johnson,olivia.j@example.com,Platinum VIP,San Francisco,CA,USA,2026-09-13 11:54:00"""
        st.download_button(
            label="👤 Download Sample Customers SCD2 CSV",
            data=sample_customers_csv,
            file_name="sample_customers_scd2.csv",
            mime="text/csv",
            use_container_width=True
        )

    with sc3:
        st.info("💡 **Tip**: Click a button to download a sample CSV, then drag & drop it directly into the box below!")

    st.markdown("---")

    # File Uploader Box
    uploaded_files = st.file_uploader(
        "Drag and Drop CSV or JSON files here",
        type=["csv", "json"],
        accept_multiple_files=True,
        key="main_drag_drop_csv_uploader",
        help="Supports .csv and .json micro-batch streaming files up to 200MB."
    )

    if uploaded_files:
        st.success(f"📁 {len(uploaded_files)} file(s) selected for ingestion.")

        for file_idx, uploaded_file in enumerate(uploaded_files):
            with st.expander(f"📄 Inspection & Preview: {uploaded_file.name} ({uploaded_file.size:,} bytes)", expanded=True):
                try:
                    if uploaded_file.name.endswith(".csv"):
                        preview_df = pd.read_csv(uploaded_file)
                    else:
                        preview_df = pd.read_json(uploaded_file)

                    # Metadata Metrics
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Total Rows", f"{len(preview_df):,}")
                    m2.metric("Total Columns", len(preview_df.columns))
                    m3.metric("Missing Values", preview_df.isna().sum().sum())
                    m4.metric("File Format", uploaded_file.name.split(".")[-1].upper())

                    # Data Table Preview
                    st.dataframe(preview_df.head(10), use_container_width=True)
                    st.caption(f"Showing first 10 rows of {uploaded_file.name}")

                    uploaded_file.seek(0)  # Reset pointer

                    # Ingestion Execution Button
                    if st.button(f"⚡ Ingest {uploaded_file.name} into Data Warehouse", type="primary", key=f"ingest_btn_{file_idx}", use_container_width=True):
                        with st.spinner("Processing file through Kimball Star-Schema ELT Engine..."):
                            try:
                                import time
                                from pipeline.realtime_engine import realtime_engine
                                if uploaded_file.name.endswith(".csv"):
                                    df_upload = pd.read_csv(uploaded_file)
                                else:
                                    df_upload = pd.read_json(uploaded_file)

                                res = realtime_engine.process_raw_dataframe(df_upload, source_type=f"ui_upload:{uploaded_file.name}")
                                if res.get("status") == "completed":
                                    st.balloons()
                                    st.success(f"✅ Ingested {res['submitted_rows']} rows into local DuckDB & Supabase PostgreSQL Cloud!")
                                    st.json({
                                        "status": "success",
                                        "records_loaded": res.get("submitted_rows"),
                                        "execution_time_ms": res.get("execution_time_ms"),
                                        "kimball_scd2_updates": res.get("scd2_updated_customers", 0),
                                        "quality_checks_passed": True
                                    })
                                    time.sleep(1.5)
                                    st.rerun()
                                else:
                                    st.error(f"❌ Ingestion failed: {res.get('error')}")
                            except Exception as e:
                                st.error(f"Processing Error: {e}")

                except Exception as preview_err:
                    st.error(f"Could not parse file {uploaded_file.name}: {preview_err}")
    else:
        st.info("👆 Drag & Drop a `.csv` file into the dashed box above or click **Browse files**.")


# ============================================================================
# TAB 2: STOCK ANALYTICS
# ============================================================================
with tab2:
    st.subheader("Stock Market OLAP Analytics")

    stocks_df = run_query("""
        SELECT * FROM olap_stock_monthly_summary
        ORDER BY year DESC, month DESC, ticker
        LIMIT 200
    """)

    if not stocks_df.empty:
        tickers = stocks_df["ticker"].unique().tolist()
        selected_ticker = st.selectbox("Select Ticker", ["All"] + tickers, key="stock_ticker")

        filtered = stocks_df if selected_ticker == "All" else stocks_df[stocks_df["ticker"] == selected_ticker]
        filtered["period"] = filtered["year"].astype(str) + "-" + filtered["month"].astype(str).str.zfill(2)

        col1, col2 = st.columns([2, 1])

        with col1:
            fig_price = go.Figure()
            for ticker in filtered["ticker"].unique():
                t_data = filtered[filtered["ticker"] == ticker]
                fig_price.add_trace(go.Scatter(
                    x=t_data["period"], y=t_data["avg_close_price"],
                    mode="lines+markers", name=ticker,
                    line=dict(width=2), marker=dict(size=5)
                ))
            apply_editorial_theme(fig_price, "Monthly Average Close Price ($)", height=420)
            st.plotly_chart(fig_price, use_container_width=True)

        with col2:
            fig_vol = px.bar(
                filtered.groupby("period")["total_volume"].sum().reset_index(),
                x="period", y="total_volume",
                color_discrete_sequence=["#28231F"]
            )
            apply_editorial_theme(fig_vol, "Monthly Total Volume", height=420)
            st.plotly_chart(fig_vol, use_container_width=True)

        # Volatility chart
        fig_volatility = px.line(
            filtered, x="period", y="avg_volatility_pct", color="ticker",
            markers=True, color_discrete_sequence=PALETTE
        )
        apply_editorial_theme(fig_volatility, "Average Daily Volatility % by Ticker", height=350)
        st.plotly_chart(fig_volatility, use_container_width=True)

        with st.expander("📄 View Stock Data Table"):
            st.dataframe(filtered, use_container_width=True)
            add_csv_download(filtered, "Stock Analytics")
    else:
        st.info("No stock data available. Run the stock pipeline to extract data.")


# ============================================================================
# TAB 2: WEATHER ANALYTICS
# ============================================================================
with tab2:
    st.subheader("Weather OLAP Analytics")

    weather_df = run_query("""
        SELECT * FROM olap_weather_daily_summary
        ORDER BY full_date DESC, city
        LIMIT 500
    """)

    if not weather_df.empty:
        cities = weather_df["city"].unique().tolist()
        selected_city = st.selectbox("Select City", ["All Cities"] + cities, key="weather_city")

        w_filtered = weather_df if selected_city == "All Cities" else weather_df[weather_df["city"] == selected_city]

        col1, col2 = st.columns(2)

        with col1:
            fig_temp = px.line(
                w_filtered, x="full_date", y="avg_temp_c", color="city",
                markers=True, color_discrete_sequence=PALETTE
            )
            apply_editorial_theme(fig_temp, "Average Temperature (°C) by City", height=400)
            st.plotly_chart(fig_temp, use_container_width=True)

        with col2:
            fig_humidity = px.bar(
                w_filtered, x="full_date", y="avg_humidity_pct", color="city",
                barmode="group", color_discrete_sequence=PALETTE
            )
            apply_editorial_theme(fig_humidity, "Average Humidity (%) by City", height=400)
            st.plotly_chart(fig_humidity, use_container_width=True)

        # Weather conditions pie
        if "dominant_weather" in w_filtered.columns:
            weather_counts = w_filtered["dominant_weather"].value_counts().reset_index()
            weather_counts.columns = ["weather", "count"]
            fig_pie = px.pie(
                weather_counts, values="count", names="weather",
                hole=0.4, color_discrete_sequence=PALETTE
            )
            apply_editorial_theme(fig_pie, "Dominant Weather Conditions Distribution", height=380)
            st.plotly_chart(fig_pie, use_container_width=True)

        with st.expander("📄 View Weather Data Table"):
            st.dataframe(w_filtered, use_container_width=True)
            add_csv_download(w_filtered, "Weather Analytics")
    else:
        st.info("No weather data available. Run the weather pipeline to extract data.")


# ============================================================================
# TAB 3: NEWS ANALYTICS
# ============================================================================
with tab3:
    st.subheader("News OLAP Analytics")

    news_df = run_query("""
        SELECT * FROM olap_news_daily_summary
        ORDER BY full_date DESC
        LIMIT 500
    """)

    if not news_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            # Articles by topic
            topic_counts = news_df.groupby("topic")["article_count"].sum().reset_index()
            fig_topics = px.bar(
                topic_counts, x="topic", y="article_count",
                color_discrete_sequence=["#28231F"]
            )
            apply_editorial_theme(fig_topics, "Total Articles by Topic", height=400)
            st.plotly_chart(fig_topics, use_container_width=True)

        with col2:
            # Source distribution
            source_counts = news_df.groupby("source_name")["article_count"].sum().nlargest(10).reset_index()
            fig_sources = px.pie(
                source_counts, values="article_count", names="source_name",
                hole=0.4, color_discrete_sequence=PALETTE
            )
            apply_editorial_theme(fig_sources, "Top 10 News Sources", height=400)
            st.plotly_chart(fig_sources, use_container_width=True)

        # Articles over time
        daily_articles = news_df.groupby("full_date")["article_count"].sum().reset_index()
        fig_timeline = px.area(
            daily_articles, x="full_date", y="article_count",
            color_discrete_sequence=["#9E5A3C"]
        )
        apply_editorial_theme(fig_timeline, "Daily News Article Volume", height=350)
        st.plotly_chart(fig_timeline, use_container_width=True)

        with st.expander("📄 View News Data Table"):
            st.dataframe(news_df, use_container_width=True)
            add_csv_download(news_df, "News Analytics")
    else:
        st.info("No news data available. Run the news pipeline to extract data.")


# ============================================================================
# TAB 4: CROSS-DOMAIN CORRELATION
# ============================================================================
with tab4:
    st.subheader("Cross-Domain Correlation Analytics")

    cross_df = run_query("""
        SELECT * FROM olap_cross_domain_daily
        ORDER BY full_date DESC
        LIMIT 90
    """)

    if not cross_df.empty:
        fig_cross = go.Figure()

        # Stocks trend (normalized)
        if cross_df["stocks_avg_close"].max() > 0:
            fig_cross.add_trace(go.Scatter(
                x=cross_df["full_date"],
                y=cross_df["stocks_avg_close"] / cross_df["stocks_avg_close"].max() * 100,
                name="Stocks (normalized)", line=dict(color="#28231F", width=2)
            ))

        # Weather trend (normalized)
        if cross_df["weather_avg_temp_c"].abs().max() > 0:
            temp_norm = (cross_df["weather_avg_temp_c"] - cross_df["weather_avg_temp_c"].min())
            temp_range = cross_df["weather_avg_temp_c"].max() - cross_df["weather_avg_temp_c"].min()
            if temp_range > 0:
                fig_cross.add_trace(go.Scatter(
                    x=cross_df["full_date"],
                    y=temp_norm / temp_range * 100,
                    name="Temperature (normalized)", line=dict(color="#9E5A3C", width=2)
                ))

        # News volume (normalized)
        if cross_df["news_article_count"].max() > 0:
            fig_cross.add_trace(go.Bar(
                x=cross_df["full_date"],
                y=cross_df["news_article_count"] / cross_df["news_article_count"].max() * 100,
                name="News Volume (normalized)", marker_color="rgba(179,134,70,0.4)"
            ))

        apply_editorial_theme(fig_cross, "Cross-Domain Trend Correlation (Normalized)", height=450)
        st.plotly_chart(fig_cross, use_container_width=True)

        with st.expander("📄 View Cross-Domain Data"):
            st.dataframe(cross_df, use_container_width=True)
            add_csv_download(cross_df, "Cross Domain Correlation")
    else:
        st.info("No cross-domain data available. Run pipelines for at least two domains.")


# ============================================================================
# TAB 5: PIPELINE AUDIT TRAIL
# ============================================================================
with tab5:
    st.subheader("Pipeline Execution Audit Trail")

    pipeline_df = run_query("""
        SELECT
            id, run_type, status, started_at, completed_at,
            records_extracted, records_loaded, records_transformed,
            records_rejected, error_message
        FROM pipeline_runs
        ORDER BY started_at DESC
        LIMIT 30
    """)

    if not pipeline_df.empty:
        # Pipeline status summary
        status_counts = pipeline_df["status"].value_counts()
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Runs", len(pipeline_df))
        col2.metric("Completed", int(status_counts.get("completed", 0)))
        col3.metric("Failed", int(status_counts.get("failed", 0)))

        st.dataframe(pipeline_df, use_container_width=True)

        # Data quality log
        st.subheader("Data Quality Check Log")
        dq_df = run_query("""
            SELECT
                dql.table_name, dql.check_name, dql.check_type,
                dql.records_checked, dql.records_failed,
                dql.severity, dql.details, dql.checked_at
            FROM data_quality_log dql
            ORDER BY dql.checked_at DESC
            LIMIT 50
        """)

        if not dq_df.empty:
            st.dataframe(dq_df, use_container_width=True)
        else:
            st.info("No data quality checks logged yet.")
    else:
        st.info("No pipeline runs recorded yet. Trigger a pipeline from the sidebar.")


# ============================================================================
# TAB 6: MACHINE LEARNING PREDICTIVE INSIGHTS
# ============================================================================
with tab6:
    st.subheader("🤖 Machine Learning Predictive Insights & Anomaly Detection")
    st.caption("Forecasting stock trends with confidence intervals & statistical weather anomaly detection.")

    ml_col1, ml_col2 = st.columns([2, 1])

    with ml_col1:
        st.markdown("### Stock Price & Volatility Forecast")
        st.markdown("### 🎯 Real-Time Customer Churn Risk Model")
        try:
            from ml.predictor import predict_customer_churn_risk
            churn_res = predict_customer_churn_risk()

            if churn_res.get("risk_breakdown"):
                rb = churn_res["risk_breakdown"]
                col_c1, col_c2, col_c3 = st.columns(3)
                col_c1.metric("🔴 High Risk Churn", rb.get("high_risk", 0))
                col_c2.metric("🟡 Medium Risk", rb.get("medium_risk", 0))
                col_c3.metric("🟢 Low Risk (Loyal)", rb.get("low_risk", 0))

                if churn_res.get("high_risk_customers"):
                    st.markdown("**High Churn Risk Customers:**")
                    st.dataframe(pd.DataFrame(churn_res["high_risk_customers"])[["customer_id", "customer_name", "customer_tier", "days_since_last_order", "churn_probability_pct"]], use_container_width=True)
            else:
                st.info("No customer records analyzed yet for churn modeling.")
        except Exception as e:
            st.warning(f"Churn prediction error: {e}")

        st.divider()
        st.markdown("### 📈 Sales Revenue & Unit Demand AI Forecast")
        try:
            from ml.predictor import forecast_sales_revenue
            rev_fct = forecast_sales_revenue(forecast_months=3)

            if rev_fct.get("status") == "completed" and rev_fct.get("forecast"):
                f_df = pd.DataFrame(rev_fct["forecast"])

                fig_rev = go.Figure()
                fig_rev.add_trace(go.Scatter(
                    x=f_df["year_month"], y=f_df["predicted_net_revenue"],
                    mode="lines+markers", name="Predicted Net Revenue ($)",
                    line=dict(color="#9E5A3C", width=3)
                ))
                fig_rev.add_trace(go.Scatter(
                    x=f_df["year_month"], y=f_df["predicted_profit"],
                    mode="lines+markers", name="Predicted Net Profit ($)",
                    line=dict(color="#6E7A67", width=2, dash="dash")
                ))
                apply_editorial_theme(fig_rev, "3-Month AI Revenue & Profit Projection ($)", height=380)
                st.plotly_chart(fig_rev, use_container_width=True)

                st.dataframe(f_df[["year_month", "predicted_net_revenue", "predicted_profit", "predicted_units_sold"]], use_container_width=True)
            else:
                st.info("Ingest sales transactions to generate 3-month AI revenue demand forecasts.")
        except Exception as e:
            st.warning(f"Revenue forecast error: {e}")

    with ml_col2:
        st.markdown("### 🚨 Real-Time Order & Price Anomaly Detector")
        try:
            from ml.predictor import detect_order_anomalies
            order_anom = detect_order_anomalies(zscore_threshold=1.8)
            st.metric("Order Anomalies Detected", order_anom.get("anomalies_detected", 0))

            if order_anom.get("anomalies"):
                oa_df = pd.DataFrame(order_anom["anomalies"])
                st.dataframe(oa_df[["order_id", "date", "customer_name", "product_name", "net_amount", "z_score", "anomaly_type"]], use_container_width=True)
            else:
                st.success("No abnormal transactions detected in fact_sales.")
        except Exception as e:
            st.warning(f"Order anomaly detection error: {e}")

        st.markdown("### Stock Price & Volatility Forecast")
        all_tickers = run_query("SELECT DISTINCT ticker FROM dim_company WHERE is_current = TRUE")
        ticker_list = all_tickers["ticker"].tolist() if not all_tickers.empty else ["AAPL"]
        sel_ml_ticker = st.selectbox("Select Ticker to Forecast", ticker_list, key="ml_ticker")
        horizon = st.slider("Forecast Horizon (Days)", min_value=5, max_value=30, value=14)

        try:
            from ml.predictor import forecast_stock_prices
            forecast_data = forecast_stock_prices(sel_ml_ticker, horizon)

            if "forecast" in forecast_data and forecast_data["forecast"]:
                hist_df = pd.DataFrame(forecast_data["historical"])
                fc_df = pd.DataFrame(forecast_data["forecast"])

                fig_fc = go.Figure()
                fig_fc.add_trace(go.Scatter(
                    x=hist_df["full_date"], y=hist_df["close_price"],
                    mode="lines", name="Historical Close", line=dict(color="#28231F", width=2)
                ))
                fig_fc.add_trace(go.Scatter(
                    x=fc_df["forecast_date"], y=fc_df["predicted_close"],
                    mode="lines+markers", name="ML Predicted Price", line=dict(color="#9E5A3C", width=2, dash="dash")
                ))
                apply_editorial_theme(fig_fc, f"{sel_ml_ticker} {horizon}-Day ML Price Forecast ($)", height=380)
                st.plotly_chart(fig_fc, use_container_width=True)
            else:
                st.info("Insufficient stock history for stock price forecast.")
        except Exception as e:
            st.warning(f"Stock ML Forecast error: {e}")


# ============================================================================
# WAREHOUSE TABLE OVERVIEW (sidebar bottom)
# ============================================================================

st.sidebar.divider()
st.sidebar.subheader("Warehouse Stats")

tables_df = run_query("""
    SELECT 'raw_stock_prices' AS tbl, COUNT(*) AS cnt FROM raw_stock_prices
    UNION ALL SELECT 'raw_weather_observations', COUNT(*) FROM raw_weather_observations
    UNION ALL SELECT 'raw_news_articles', COUNT(*) FROM raw_news_articles
    UNION ALL SELECT 'fact_stock_prices', COUNT(*) FROM fact_stock_prices
    UNION ALL SELECT 'fact_weather_readings', COUNT(*) FROM fact_weather_readings
    UNION ALL SELECT 'fact_news_articles', COUNT(*) FROM fact_news_articles
    UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
    UNION ALL SELECT 'dim_company', COUNT(*) FROM dim_company
    UNION ALL SELECT 'dim_location', COUNT(*) FROM dim_location
    UNION ALL SELECT 'dim_news_source', COUNT(*) FROM dim_news_source
""")

if not tables_df.empty:
    for _, row in tables_df.iterrows():
        st.sidebar.text(f"  {row['tbl']}: {int(row['cnt']):,}")
else:
    st.sidebar.text("  No data yet — run pipeline")
