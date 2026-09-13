import sys
sys.path.insert(0, '.')
from config.supabase_client import execute_sql_df

print("--- raw_weather_observations contents ---")
df_raw = execute_sql_df("SELECT * FROM raw_weather_observations")
print(df_raw.T)
