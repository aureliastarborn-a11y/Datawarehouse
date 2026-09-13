import sys
sys.path.insert(0, '.')
from config.supabase_client import execute_sql_df

print("--- olap_weather_daily_summary ---")
print(execute_sql_df("SELECT * FROM olap_weather_daily_summary"))

print("--- fallback query ---")
print(execute_sql_df("""
    SELECT
        CAST(d.full_date AS VARCHAR) AS full_date,
        d.day_name,
        d.month_name,
        d.year,
        l.city,
        l.country_code,
        COUNT(*) AS observation_count,
        ROUND(AVG(f.temp_celsius), 2) AS avg_temp_c,
        ROUND(MIN(f.temp_min_c), 2) AS daily_min_temp_c,
        ROUND(MAX(f.temp_max_c), 2) AS daily_max_temp_c,
        ROUND(AVG(f.humidity_pct), 1) AS avg_humidity_pct,
        ROUND(AVG(f.wind_speed_ms), 2) AS avg_wind_speed_ms,
        ROUND(AVG(f.pressure_hpa), 0) AS avg_pressure_hpa,
        ROUND(SUM(f.rain_1h_mm), 2) AS total_rain_mm,
        ROUND(SUM(f.snow_1h_mm), 2) AS total_snow_mm,
        'Clear' AS dominant_weather
    FROM fact_weather_readings f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_location l ON f.location_key = l.location_key
    GROUP BY d.full_date, d.day_name, d.month_name, d.year, l.city, l.country_code
    ORDER BY full_date DESC, l.city
"""))
