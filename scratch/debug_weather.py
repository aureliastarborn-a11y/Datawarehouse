import sys
sys.path.insert(0, '.')
from config.supabase_client import get_pg_connection

conn = get_pg_connection()
cur = conn.cursor()

# Check the fact_weather_readings table schema and constraints
cur.execute("""
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_name = 'fact_weather_readings'
    ORDER BY ordinal_position
""")
print("=== fact_weather_readings SCHEMA ===")
for row in cur.fetchall():
    print(f"  {row[0]:30s} {row[1]:20s} nullable={row[2]:5s} default={row[3]}")

# Check constraints
cur.execute("""
    SELECT conname, contype, pg_get_constraintdef(oid)
    FROM pg_constraint
    WHERE conrelid = 'fact_weather_readings'::regclass
""")
print("\n=== CONSTRAINTS ===")
for row in cur.fetchall():
    print(f"  {row[0]}: type={row[1]} def={row[2]}")

# Now try the INSERT manually and see what happens
print("\n=== TESTING INSERT ===")
try:
    cur.execute("""
        SELECT
            TO_CHAR(r.observation_dt::DATE, 'YYYYMMDD')::INTEGER AS date_key,
            dl.location_key,
            EXTRACT(HOUR FROM r.observation_dt)::INTEGER AS observation_hour,
            r.temp_celsius, r.feels_like_c, r.temp_min_c, r.temp_max_c,
            r.pressure_hpa, r.humidity_pct, r.wind_speed_ms, r.wind_deg,
            r.clouds_pct, r.weather_main, r.weather_desc, r.visibility_m,
            COALESCE(r.rain_1h_mm, 0) AS rain, COALESCE(r.snow_1h_mm, 0) AS snow
        FROM raw_weather_observations r
        JOIN dim_location dl ON r.city = dl.city AND r.country_code = dl.country_code
        JOIN dim_date dd ON dd.date_key = TO_CHAR(r.observation_dt::DATE, 'YYYYMMDD')::INTEGER
        WHERE r.temp_celsius BETWEEN -90 AND 60
    """)
    rows = cur.fetchall()
    print(f"SELECT returns {len(rows)} rows")
    for row in rows:
        print(f"  date_key={row[0]} loc_key={row[1]} hour={row[2]} temp={row[3]}")
except Exception as e:
    print(f"SELECT ERROR: {e}")
    conn.rollback()

# Try the actual insert
print("\n=== ACTUAL INSERT ===")
try:
    cur.execute("""
        INSERT INTO fact_weather_readings (
            date_key, location_key, observation_hour,
            temp_celsius, feels_like_c, temp_min_c, temp_max_c,
            pressure_hpa, humidity_pct, wind_speed_ms, wind_deg,
            clouds_pct, weather_main, weather_desc, visibility_m, rain_1h_mm, snow_1h_mm
        )
        SELECT
            TO_CHAR(r.observation_dt::DATE, 'YYYYMMDD')::INTEGER,
            dl.location_key, EXTRACT(HOUR FROM r.observation_dt)::INTEGER,
            r.temp_celsius, r.feels_like_c, r.temp_min_c, r.temp_max_c,
            r.pressure_hpa, r.humidity_pct, r.wind_speed_ms, r.wind_deg,
            r.clouds_pct, r.weather_main, r.weather_desc, r.visibility_m,
            COALESCE(r.rain_1h_mm, 0), COALESCE(r.snow_1h_mm, 0)
        FROM raw_weather_observations r
        JOIN dim_location dl ON r.city = dl.city AND r.country_code = dl.country_code
        JOIN dim_date dd ON dd.date_key = TO_CHAR(r.observation_dt::DATE, 'YYYYMMDD')::INTEGER
        WHERE r.temp_celsius BETWEEN -90 AND 60
    """)
    print(f"Inserted: {cur.rowcount} rows")
    conn.commit()
    print("COMMITTED!")
except Exception as e:
    print(f"INSERT ERROR: {e}")
    conn.rollback()

# Verify
cur2 = conn.cursor()
cur2.execute("SELECT COUNT(*) FROM fact_weather_readings")
print(f"\nfact_weather_readings count after insert: {cur2.fetchone()[0]}")

conn.close()
