"""
Data Warehouse - External API Data Extractors
Connects to Alpha Vantage, OpenWeatherMap, and NewsAPI to extract real-world data.
Includes automatic fallback to seed/synthetic data generation when API keys are not provided or rate limited.
"""

import time
import random
import httpx
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional

random.seed(42)

# ============================================================================
# MOCK / FALLBACK DATA GENERATORS
# ============================================================================

def generate_mock_stocks(ticker: str, days: int = 60) -> List[Dict[str, Any]]:
    """Generate realistic synthetic OHLCV stock history."""
    base_prices = {
        "AAPL": 180.0, "MSFT": 410.0, "GOOGL": 175.0, "AMZN": 185.0, "TSLA": 220.0
    }
    base = base_prices.get(ticker.upper(), 150.0)
    records = []
    current_date = datetime.now() - timedelta(days=days)

    for i in range(days):
        # Skip weekends
        if current_date.weekday() in (5, 6):
            current_date += timedelta(days=1)
            continue

        change_pct = random.uniform(-0.03, 0.035)
        open_price = round(base * (1 + random.uniform(-0.01, 0.01)), 2)
        close_price = round(open_price * (1 + change_pct), 2)
        high_price = round(max(open_price, close_price) * (1 + random.uniform(0.002, 0.015)), 2)
        low_price = round(min(open_price, close_price) * (1 - random.uniform(0.002, 0.015)), 2)
        volume = random.randint(15_000_000, 85_000_000)

        records.append({
            "ticker": ticker.upper(),
            "trade_date": current_date.strftime("%Y-%m-%d"),
            "open_price": open_price,
            "high_price": high_price,
            "low_price": low_price,
            "close_price": close_price,
            "volume": volume,
            "source": "alpha_vantage_mock",
        })

        base = close_price
        current_date += timedelta(days=1)

    return records


def generate_mock_weather(city: str) -> Dict[str, Any]:
    """Generate realistic current weather observation."""
    city_defaults = {
        "New York": ("US", 18.5, 65, "Rain", "light rain", 40.7128, -74.0060),
        "London": ("GB", 14.2, 78, "Clouds", "overcast clouds", 51.5074, -0.1278),
        "Tokyo": ("JP", 24.0, 55, "Clear", "clear sky", 35.6762, 139.6503),
        "Mumbai": ("IN", 31.5, 82, "Thunderstorm", "heavy intensity rain", 19.0760, 72.8777),
        "Sydney": ("AU", 21.0, 60, "Clear", "sunny", -33.8688, 151.2093),
    }
    country, base_temp, base_hum, main, desc, lat, lon = city_defaults.get(
        city, ("US", 20.0, 60, "Clouds", "few clouds", 0.0, 0.0)
    )

    temp = round(base_temp + random.uniform(-2.0, 2.0), 1)

    return {
        "city": city,
        "country_code": country,
        "observation_dt": datetime.utcnow().isoformat(),
        "temp_celsius": temp,
        "feels_like_c": round(temp + random.uniform(-1.0, 2.0), 1),
        "temp_min_c": round(temp - random.uniform(1.0, 3.0), 1),
        "temp_max_c": round(temp + random.uniform(1.0, 3.0), 1),
        "pressure_hpa": random.randint(1008, 1022),
        "humidity_pct": min(100, max(20, base_hum + random.randint(-10, 10))),
        "wind_speed_ms": round(random.uniform(1.5, 8.5), 1),
        "wind_deg": random.randint(0, 360),
        "clouds_pct": random.randint(10, 90),
        "weather_main": main,
        "weather_desc": desc,
        "visibility_m": 10000,
        "rain_1h_mm": round(random.uniform(0.0, 2.5), 2) if main == "Rain" else 0.0,
        "snow_1h_mm": 0.0,
        "latitude": lat,
        "longitude": lon,
        "source": "openweathermap_mock",
    }


def generate_mock_news(topic: str, count: int = 10) -> List[Dict[str, Any]]:
    """Generate realistic news headlines for a topic."""
    sources = ["TechCrunch", "Reuters", "Bloomberg", "The Verge", "Wall Street Journal", "Wired"]
    topics_headlines = {
        "technology": [
            "AI Breakthrough: Next-Gen Language Models Achieve New Reasoning Benchmarks",
            "Quantum Computing Startup Raises $500M in Series C Funding",
            "Global Semiconductor Industry Reports Record Q2 Chip Shipments",
            "Major Cloud Infrastructure Providers Expand Renewable Data Center Footprint",
            "Cybersecurity Experts Warn of Sophisticated Zero-Day Vulnerability"
        ],
        "business": [
            "Central Banks Signal Potential Interest Rate Cuts Amid Cooling Inflation",
            "Global Supply Chains Stabilize as Freight Shipping Costs Normalize",
            "E-Commerce Enterprise Sales Surge 18% Year-Over-Year in Quarter Audit",
            "Venture Capital Investments Rebound in Technology and Clean Energy Sectors",
            "Stock Markets Reach All-Time Highs Powered by Tech Rally"
        ],
        "science": [
            "James Webb Telescope Detects Atmospheric Water Vapor on Exoplanet",
            "Fusion Energy Reactor Sustains Record Plasma Temperature in Test",
            "New Oceanographic Study Maps Deep-Sea Coral Ecosystems",
            "Renewable Battery Storage Breakthrough Doubles Energy Density",
            "Genomic Sequencing Uncovers Ancient Migration Patterns"
        ]
    }
    headlines = topics_headlines.get(topic.lower(), [
        f"Global Trends Report Highlights Innovation in {topic.title()}",
        f"Key Industry Insights & Market Developments for {topic.title()}"
    ])

    records = []
    base_time = datetime.utcnow()

    for i in range(count):
        headline = headlines[i % len(headlines)]
        source = random.choice(sources)
        pub_time = base_time - timedelta(hours=i * 3 + random.randint(0, 59))

        records.append({
            "source_id": source.lower().replace(" ", "-"),
            "source_name": source,
            "author": f"Senior Editor {i+1}",
            "title": f"{headline} (#{i+1})",
            "description": f"Comprehensive analysis and reporting on recent developments regarding {topic}.",
            "url": f"https://example.com/news/{topic}/{i+1000}",
            "url_to_image": f"https://example.com/images/{topic}_{i+1}.jpg",
            "published_at": pub_time.isoformat(),
            "content": f"Full coverage report on {topic} and industry impact.",
            "topic": topic,
            "source_api": "newsapi_mock",
        })

    return records


# ============================================================================
# Alpha Vantage - Stock Extractor
# ============================================================================

def extract_stock_data(
    ticker: str,
    api_key: str,
    output_size: str = "compact"
) -> List[Dict[str, Any]]:
    """Extract daily stock price data from Alpha Vantage API or mock fallback."""
    if not api_key or api_key == "demo" or "your-" in api_key:
        print(f"  [MOCK] Alpha Vantage API key not set — generating synthetic stock data for {ticker}")
        return generate_mock_stocks(ticker)

    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "outputsize": output_size,
        "apikey": api_key,
    }

    print(f"  [EXTRACT] Fetching stock data for {ticker}...")

    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

        if "Error Message" in data or "Note" in data or "Information" in data:
            print(f"  [MOCK FALLBACK] Alpha Vantage notice — using fallback data for {ticker}")
            return generate_mock_stocks(ticker)

        time_series = data.get("Time Series (Daily)", {})
        if not time_series:
            return generate_mock_stocks(ticker)

        records = []
        for date_str, values in time_series.items():
            records.append({
                "ticker": ticker.upper(),
                "trade_date": date_str,
                "open_price": float(values.get("1. open", 0)),
                "high_price": float(values.get("2. high", 0)),
                "low_price": float(values.get("3. low", 0)),
                "close_price": float(values.get("4. close", 0)),
                "volume": int(values.get("5. volume", 0)),
                "source": "alpha_vantage",
            })

        print(f"  [OK] Extracted {len(records)} trading days for {ticker}")
        return records

    except Exception as e:
        print(f"  [MOCK FALLBACK] Error fetching {ticker} ({e}) — using fallback data")
        return generate_mock_stocks(ticker)


def extract_multiple_stocks(
    tickers: List[str],
    api_key: str,
    delay_seconds: float = 12.0
) -> List[Dict[str, Any]]:
    all_records = []
    use_delay = bool(api_key and api_key != "demo" and "your-" not in api_key)

    for i, ticker in enumerate(tickers):
        records = extract_stock_data(ticker, api_key)
        all_records.extend(records)

        if use_delay and i < len(tickers) - 1:
            time.sleep(delay_seconds)

    print(f"  [SUMMARY] Total stock records extracted: {len(all_records)}")
    return all_records


# ============================================================================
# OpenWeatherMap - Weather Extractor
# ============================================================================

def extract_weather_data(
    city: str,
    api_key: str,
    units: str = "metric"
) -> Optional[Dict[str, Any]]:
    """Extract current weather observation from OpenWeatherMap API or mock fallback."""
    if not api_key or api_key == "demo" or "your-" in api_key:
        print(f"  [MOCK] OpenWeatherMap API key not set — generating synthetic weather for {city}")
        return generate_mock_weather(city)

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": api_key, "units": units}

    print(f"  [EXTRACT] Fetching weather data for {city}...")

    try:
        with httpx.Client(timeout=20.0) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

        if data.get("cod") != 200:
            return generate_mock_weather(city)

        weather_main = data.get("weather", [{}])[0]
        main_data = data.get("main", {})
        wind_data = data.get("wind", {})
        clouds_data = data.get("clouds", {})
        rain_data = data.get("rain", {})
        snow_data = data.get("snow", {})
        coord_data = data.get("coord", {})

        record = {
            "city": city,
            "country_code": data.get("sys", {}).get("country", ""),
            "observation_dt": datetime.utcfromtimestamp(data.get("dt", 0)).isoformat(),
            "temp_celsius": main_data.get("temp", 0),
            "feels_like_c": main_data.get("feels_like", 0),
            "temp_min_c": main_data.get("temp_min", 0),
            "temp_max_c": main_data.get("temp_max", 0),
            "pressure_hpa": main_data.get("pressure", 0),
            "humidity_pct": main_data.get("humidity", 0),
            "wind_speed_ms": wind_data.get("speed", 0),
            "wind_deg": wind_data.get("deg", 0),
            "clouds_pct": clouds_data.get("all", 0),
            "weather_main": weather_main.get("main", ""),
            "weather_desc": weather_main.get("description", ""),
            "visibility_m": data.get("visibility", 0),
            "rain_1h_mm": rain_data.get("1h", 0),
            "snow_1h_mm": snow_data.get("snow", 0),
            "latitude": coord_data.get("lat", 0),
            "longitude": coord_data.get("lon", 0),
            "source": "openweathermap",
        }
        return record

    except Exception as e:
        print(f"  [MOCK FALLBACK] Weather error for {city} ({e}) — using fallback data")
        return generate_mock_weather(city)


def extract_multiple_weather(
    cities: List[str],
    api_key: str,
    delay_seconds: float = 1.0
) -> List[Dict[str, Any]]:
    records = []
    for city in cities:
        rec = extract_weather_data(city, api_key)
        if rec:
            records.append(rec)
    print(f"  [SUMMARY] Total weather observations extracted: {len(records)}")
    return records


# ============================================================================
# NewsAPI - News Extractor
# ============================================================================

def extract_news_data(
    topic: str,
    api_key: str,
    page_size: int = 20,
    language: str = "en"
) -> List[Dict[str, Any]]:
    """Extract top news headlines from NewsAPI or mock fallback."""
    if not api_key or api_key == "demo" or "your-" in api_key:
        print(f"  [MOCK] NewsAPI key not set — generating synthetic news articles for topic '{topic}'")
        return generate_mock_news(topic)

    url = "https://newsapi.org/v2/everything"
    params = {
        "q": topic, "language": language,
        "pageSize": min(page_size, 100),
        "sortBy": "publishedAt", "apiKey": api_key
    }

    print(f"  [EXTRACT] Fetching news articles for topic: '{topic}'...")

    try:
        with httpx.Client(timeout=20.0) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

        if data.get("status") != "ok":
            return generate_mock_news(topic)

        articles = data.get("articles", [])
        records = []
        for article in articles:
            if article.get("title") == "[Removed]" or not article.get("title"):
                continue
            records.append({
                "source_id": (article.get("source") or {}).get("id", ""),
                "source_name": (article.get("source") or {}).get("name", "Unknown"),
                "author": article.get("author", ""),
                "title": article.get("title", ""),
                "description": article.get("description", ""),
                "url": article.get("url", ""),
                "url_to_image": article.get("urlToImage", ""),
                "published_at": article.get("publishedAt", ""),
                "content": (article.get("content") or "")[:500],
                "topic": topic,
                "source_api": "newsapi",
            })
        return records

    except Exception as e:
        print(f"  [MOCK FALLBACK] News error for {topic} ({e}) — using fallback data")
        return generate_mock_news(topic)


def extract_multiple_news(
    topics: List[str],
    api_key: str,
    delay_seconds: float = 1.0
) -> List[Dict[str, Any]]:
    all_records = []
    for topic in topics:
        recs = extract_news_data(topic, api_key)
        all_records.extend(recs)
    print(f"  [SUMMARY] Total news articles extracted: {len(all_records)}")
    return all_records
