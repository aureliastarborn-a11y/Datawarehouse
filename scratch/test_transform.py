import sys
sys.path.insert(0, '.')
from pipeline.transformers import transform_weather_data

res = transform_weather_data()
print("Result of transform_weather_data:", res)
