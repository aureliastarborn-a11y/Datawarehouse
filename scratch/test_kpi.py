import sys
sys.path.insert(0, '.')
from config.supabase_client import execute_sql_df

kpi_df = execute_sql_df("""
    SELECT
        (SELECT COUNT(*) FROM fact_stock_prices) AS stock_records,
        (SELECT COUNT(*) FROM fact_weather_readings) AS weather_records,
        (SELECT COUNT(*) FROM fact_news_articles) AS news_records,
        (SELECT COUNT(DISTINCT ticker) FROM dim_company) AS tracked_tickers,
        (SELECT COUNT(DISTINCT city) FROM dim_location) AS tracked_cities,
        (SELECT COUNT(*) FROM pipeline_runs) AS pipeline_runs
""")
print("--- Header KPIs ---")
print(kpi_df)

print("--- Weather OLAP Summary ---")
weather_df = execute_sql_df("""
    SELECT * FROM olap_weather_daily_summary
    ORDER BY full_date DESC, city
    LIMIT 500
""")
print(weather_df)
