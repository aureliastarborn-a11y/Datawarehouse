"""
Data Warehouse - Machine Learning & Predictive Analytics Engine
Predictive forecasting and anomaly detection built on top of the Kimball Star Schema.
"""

import sys
import os
import math
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.supabase_client import execute_sql_df


# ============================================================================
# STOCK PRICE & VOLATILITY FORECASTING
# ============================================================================

def forecast_stock_prices(ticker: str, forecast_days: int = 14) -> Dict[str, Any]:
    """
    Predict future stock prices and volatility confidence intervals
    using Exponential Smoothing & Linear Trend Regression.
    """
    sql = """
        SELECT
            d.full_date,
            f.close_price,
            f.volume,
            f.daily_return_pct,
            f.daily_range_pct
        FROM fact_stock_prices f
        JOIN dim_company c ON f.company_key = c.company_key
        JOIN dim_date d ON f.date_key = d.date_key
        WHERE c.ticker = %s
        ORDER BY d.full_date ASC
    """
    df = execute_sql_df(sql, [ticker.upper()])

    if df.empty or len(df) < 5:
        return {
            "ticker": ticker,
            "error": "Insufficient historical data for forecasting (need >= 5 records)",
            "historical": [],
            "forecast": []
        }

    df["full_date"] = pd.to_datetime(df["full_date"])
    prices = df["close_price"].values
    n = len(prices)

    # 1. Fit Linear Trend (Regression: y = mx + c)
    x = np.arange(n)
    slope, intercept = np.polyfit(x, prices, 1)

    # 2. Fit Holt's Exponential Smoothing (Alpha=0.4, Beta=0.2)
    alpha, beta = 0.4, 0.2
    level = prices[0]
    trend = prices[1] - prices[0]

    for i in range(1, n):
        last_level = level
        level = alpha * prices[i] + (1 - alpha) * (level + trend)
        trend = beta * (level - last_level) + (1 - beta) * trend

    # Historical volatility (Standard Deviation of daily returns)
    returns = df["daily_return_pct"].dropna().values
    volatility = float(np.std(returns)) if len(returns) > 0 else 2.0
    last_close = float(prices[-1])
    last_date = df["full_date"].iloc[-1]

    # Generate Forecast Horizon
    forecast_rows = []
    current_date = last_date

    for h in range(1, forecast_days + 1):
        current_date += timedelta(days=1)
        # Skip weekends
        while current_date.weekday() in (5, 6):
            current_date += timedelta(days=1)

        # Combined prediction (60% Holt Exponential Smoothing + 40% Linear Trend)
        holt_pred = level + h * trend
        linear_pred = intercept + (n + h - 1) * slope
        predicted_price = round(float(0.6 * holt_pred + 0.4 * linear_pred), 2)

        # Confidence intervals (95% CI based on historical volatility)
        margin = round(predicted_price * (volatility / 100.0) * math.sqrt(h) * 1.96, 2)
        upper_bound = round(predicted_price + margin, 2)
        lower_bound = round(max(1.0, predicted_price - margin), 2)

        pred_change_pct = round(((predicted_price - last_close) / last_close) * 100, 2)

        forecast_rows.append({
            "forecast_date": current_date.strftime("%Y-%m-%d"),
            "horizon_day": h,
            "predicted_close": predicted_price,
            "upper_bound_95": upper_bound,
            "lower_bound_95": lower_bound,
            "predicted_change_pct": pred_change_pct,
            "expected_volatility_pct": round(volatility, 2)
        })

    historical_list = df.tail(30).to_dict(orient="records")
    for row in historical_list:
        row["full_date"] = row["full_date"].strftime("%Y-%m-%d")

    return {
        "ticker": ticker.upper(),
        "last_historical_date": last_date.strftime("%Y-%m-%d"),
        "last_historical_close": last_close,
        "historical_volatility_pct": round(volatility, 2),
        "trend_direction": "Bullish" if slope > 0 else "Bearish",
        "historical": historical_list,
        "forecast": forecast_rows
    }


# ============================================================================
# WEATHER ANOMALY DETECTION
# ============================================================================

def detect_weather_anomalies(threshold_zscore: float = 1.5) -> Dict[str, Any]:
    """
    Detect statistical weather anomalies (extreme temperatures/humidity)
    using Z-score statistical outlier calculation per city.
    """
    sql = """
        SELECT
            d.full_date,
            l.city,
            l.country_code,
            f.temp_celsius,
            f.humidity_pct,
            f.wind_speed_ms,
            f.weather_main,
            f.weather_desc
        FROM fact_weather_readings f
        JOIN dim_location l ON f.location_key = l.location_key
        JOIN dim_date d ON f.date_key = d.date_key
        ORDER BY l.city, d.full_date ASC
    """
    df = execute_sql_df(sql)

    if df.empty:
        return {"anomalies_found": 0, "anomalies": []}

    df["full_date"] = pd.to_datetime(df["full_date"]).dt.strftime("%Y-%m-%d")
    anomalies = []

    for city, group in df.groupby("city"):
        temps = group["temp_celsius"].values
        mean_temp = np.mean(temps)
        std_temp = np.std(temps) if len(temps) > 1 else 1.0
        if std_temp == 0:
            std_temp = 1.0

        for idx, row in group.iterrows():
            t = row["temp_celsius"]
            z_score = round(float((t - mean_temp) / std_temp), 2)

            if abs(z_score) >= threshold_zscore:
                anomalies.append({
                    "date": row["full_date"],
                    "city": city,
                    "country": row["country_code"],
                    "temp_celsius": t,
                    "city_avg_temp": round(float(mean_temp), 1),
                    "z_score": z_score,
                    "anomaly_type": "Heatwave (High Temp)" if z_score > 0 else "Cold Snap (Low Temp)",
                    "severity": "High" if abs(z_score) >= 2.0 else "Moderate",
                    "weather_condition": row["weather_desc"]
                })

    return {
        "total_observations_analyzed": len(df),
        "anomalies_found": len(anomalies),
        "zscore_threshold": threshold_zscore,
        "anomalies": anomalies
    }


# ============================================================================
# NEWS SENTIMENT & CORRELATION ANALYSIS
# ============================================================================

def analyze_news_sentiment() -> Dict[str, Any]:
    """
    Analyze news headlines for sentiment orientation and topic distribution.
    """
    sql = """
        SELECT
            d.full_date,
            f.topic,
            s.source_name,
            f.title,
            f.title_word_count,
            f.has_image
        FROM fact_news_articles f
        JOIN dim_news_source s ON f.source_key = s.source_key
        JOIN dim_date d ON f.date_key = d.date_key
    """
    df = execute_sql_df(sql)

    if df.empty:
        return {"total_articles": 0, "sentiment_by_topic": []}

    # Lexicon-based sentiment scoring helper
    positive_words = {"breakthrough", "surge", "record", "rebound", "growth", "highs", "funding", "success", "cut", "stabilize"}
    negative_words = {"warn", "vulnerability", "risk", "drop", "decline", "fall", "threat", "crisis", "slump"}

    def compute_sentiment(text):
        words = set(str(text).lower().split())
        pos = len(words.intersection(positive_words))
        neg = len(words.intersection(negative_words))
        if pos > neg:
            return "Positive", 0.65
        elif neg > pos:
            return "Negative", -0.65
        else:
            return "Neutral", 0.0

    df["sentiment_label"], df["sentiment_score"] = zip(*df["title"].map(compute_sentiment))

    summary = []
    for topic, group in df.groupby("topic"):
        pos_cnt = sum(group["sentiment_label"] == "Positive")
        neg_cnt = sum(group["sentiment_label"] == "Negative")
        neu_cnt = sum(group["sentiment_label"] == "Neutral")
        avg_score = round(float(group["sentiment_score"].mean()), 2)

        summary.append({
            "topic": topic,
            "article_count": len(group),
            "positive_count": pos_cnt,
            "negative_count": neg_cnt,
            "neutral_count": neu_cnt,
            "avg_sentiment_score": avg_score,
            "overall_sentiment": "Bullish" if avg_score > 0.1 else ("Bearish" if avg_score < -0.1 else "Neutral")
        })

    return {
        "total_articles": len(df),
        "sentiment_by_topic": summary,
        "recent_classified_headlines": df[["full_date", "topic", "title", "sentiment_label"]].head(15).to_dict(orient="records")
    }


# ============================================================================
# REAL-TIME CUSTOMER CHURN RISK MODEL
# ============================================================================

def predict_customer_churn_risk() -> Dict[str, Any]:
    """
    Predict customer churn probability (%) and risk tier based on order recency,
    purchase frequency, total monetary spend, and tier history in Kimball Star Schema.
    """
    sql = """
        SELECT
            c.customer_id,
            c.first_name,
            c.last_name,
            c.customer_tier,
            c.city,
            COUNT(f.sales_fact_key) AS total_orders,
            COALESCE(SUM(f.net_amount), 0.0) AS total_spend,
            MAX(d.full_date) AS last_order_date
        FROM dim_customer c
        LEFT JOIN fact_sales f ON c.customer_key = f.customer_key
        LEFT JOIN dim_date d ON f.order_date_key = d.date_key
        WHERE c.is_current = TRUE
        GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_tier, c.city
    """
    df = execute_sql_df(sql)

    if df.empty:
        return {"total_customers_analyzed": 0, "high_risk_customers": [], "churn_summary": {}}

    now = datetime.now()
    churn_profiles = []
    high_count, med_count, low_count = 0, 0, 0

    for idx, row in df.iterrows():
        last_date = pd.to_datetime(row["last_order_date"]) if pd.notnull(row["last_order_date"]) else now - timedelta(days=180)
        days_since_last = (now - last_date).days if pd.notnull(row["last_order_date"]) else 180
        total_orders = int(row["total_orders"])
        total_spend = float(row["total_spend"])

        # Calculate Churn Probability % using weighted logistic-style scoring
        recency_score = min(1.0, days_since_last / 120.0)
        frequency_score = max(0.0, 1.0 - (total_orders / 10.0))
        monetary_score = max(0.0, 1.0 - (total_spend / 5000.0))

        churn_prob = round(float((0.5 * recency_score + 0.3 * frequency_score + 0.2 * monetary_score) * 100), 1)
        churn_prob = min(99.9, max(5.0, churn_prob))

        if churn_prob >= 70.0:
            risk_tier = "High Risk"
            high_count += 1
        elif churn_prob >= 40.0:
            risk_tier = "Medium Risk"
            med_count += 1
        else:
            risk_tier = "Low Risk (Loyal)"
            low_count += 1

        churn_profiles.append({
            "customer_id": row["customer_id"],
            "customer_name": f"{row['first_name']} {row['last_name']}".strip(),
            "customer_tier": row["customer_tier"],
            "city": row["city"],
            "total_orders": total_orders,
            "total_spend": round(total_spend, 2),
            "days_since_last_order": days_since_last,
            "churn_probability_pct": churn_prob,
            "risk_tier": risk_tier
        })

    # Sort high risk customers first
    churn_profiles.sort(key=lambda x: x["churn_probability_pct"], reverse=True)

    return {
        "total_customers_analyzed": len(df),
        "risk_breakdown": {
            "high_risk": high_count,
            "medium_risk": med_count,
            "low_risk": low_count
        },
        "high_risk_customers": [p for p in churn_profiles if p["risk_tier"] == "High Risk"][:20],
        "all_customer_churn_scores": churn_profiles
    }


# ============================================================================
# REAL-TIME SALES REVENUE & DEMAND FORECASTING
# ============================================================================

def forecast_sales_revenue(forecast_months: int = 3) -> Dict[str, Any]:
    """
    Predict monthly net revenue, profit margins, and unit demand for upcoming months
    using Holt-Winters Double Exponential Smoothing on historical fact_sales.
    """
    sql = """
        SELECT
            d.year,
            d.month,
            d.year || '-' || LPAD(CAST(d.month AS VARCHAR), 2, '0') AS year_month,
            SUM(f.net_amount) AS total_net_revenue,
            SUM(f.profit_amount) AS total_profit,
            SUM(f.quantity) AS total_units_sold,
            COUNT(DISTINCT f.order_id) AS total_orders
        FROM fact_sales f
        JOIN dim_date d ON f.order_date_key = d.date_key
        GROUP BY d.year, d.month
        ORDER BY year_month ASC
    """
    df = execute_sql_df(sql)

    if df.empty or len(df) < 2:
        return {
            "status": "insufficient_data",
            "message": "Need >= 2 monthly aggregates in fact_sales to generate AI forecasts.",
            "historical": [],
            "forecast": []
        }

    revenues = df["total_net_revenue"].astype(float).values
    units = df["total_units_sold"].astype(float).values
    n = len(revenues)

    # Holt's Double Exponential Smoothing
    alpha, beta = 0.5, 0.3
    level_rev = revenues[0]
    trend_rev = (revenues[-1] - revenues[0]) / max(1, n - 1)

    level_unit = units[0]
    trend_unit = (units[-1] - units[0]) / max(1, n - 1)

    for i in range(1, n):
        l_last = level_rev
        level_rev = alpha * revenues[i] + (1 - alpha) * (level_rev + trend_rev)
        trend_rev = beta * (level_rev - l_last) + (1 - beta) * trend_rev

        u_last = level_unit
        level_unit = alpha * units[i] + (1 - alpha) * (level_unit + trend_unit)
        trend_unit = beta * (level_unit - u_last) + (1 - beta) * trend_unit

    last_ym = df["year_month"].iloc[-1]
    last_year, last_month = map(int, last_ym.split("-"))

    forecast_rows = []
    for m in range(1, forecast_months + 1):
        target_m = last_month + m
        target_y = last_year + (target_m - 1) // 12
        target_m = ((target_m - 1) % 12) + 1
        ym_str = f"{target_y:04d}-{target_m:02d}"

        pred_rev = round(max(0.0, float(level_rev + m * trend_rev)), 2)
        pred_units = int(max(1, float(level_unit + m * trend_unit)))
        est_profit = round(pred_rev * 0.45, 2)  # Assuming historical ~45% margin

        forecast_rows.append({
            "year_month": ym_str,
            "forecast_month_step": m,
            "predicted_net_revenue": pred_rev,
            "predicted_profit": est_profit,
            "predicted_units_sold": pred_units,
            "confidence_lower_95": round(max(0.0, pred_rev * 0.88), 2),
            "confidence_upper_95": round(pred_rev * 1.12, 2)
        })

    hist_list = df.to_dict(orient="records")

    return {
        "status": "completed",
        "historical_months_analyzed": n,
        "last_historical_month": last_ym,
        "last_month_revenue": float(revenues[-1]),
        "historical": hist_list,
        "forecast": forecast_rows
    }


# ============================================================================
# REAL-TIME ORDER & PRICE ANOMALY DETECTOR
# ============================================================================

def detect_order_anomalies(zscore_threshold: float = 2.0) -> Dict[str, Any]:
    """
    Detect abnormal transaction amounts, price spikes, or unusual order quantities
    in fact_sales using Z-Score statistical anomaly detection.
    """
    sql = """
        SELECT
            f.sales_fact_key,
            f.order_id,
            d.full_date,
            c.customer_id,
            c.first_name || ' ' || c.last_name AS customer_name,
            p.product_id,
            p.product_name,
            f.quantity,
            f.unit_price,
            f.net_amount,
            f.profit_amount,
            f.profit_margin_pct
        FROM fact_sales f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        JOIN dim_product p ON f.product_key = p.product_key
        JOIN dim_date d ON f.order_date_key = d.date_key
        ORDER BY d.full_date DESC
    """
    df = execute_sql_df(sql)

    if df.empty:
        return {"total_transactions_analyzed": 0, "anomalies_detected": 0, "anomalies": []}

    net_amounts = df["net_amount"].astype(float).values
    mean_net = float(np.mean(net_amounts))
    std_net = float(np.std(net_amounts)) if len(net_amounts) > 1 else 1.0
    if std_net == 0:
        std_net = 1.0

    anomalies = []
    for idx, row in df.iterrows():
        net_val = float(row["net_amount"])
        z_score = round((net_val - mean_net) / std_net, 2)

        if abs(z_score) >= zscore_threshold:
            anomalies.append({
                "order_id": row["order_id"],
                "date": str(row["full_date"]),
                "customer_name": row["customer_name"],
                "product_name": row["product_name"],
                "quantity": int(row["quantity"]),
                "net_amount": round(net_val, 2),
                "avg_transaction_amount": round(mean_net, 2),
                "z_score": z_score,
                "anomaly_type": "High Spender / VIP Surge" if z_score > 0 else "Abnormal Low Transaction",
                "severity": "Critical" if abs(z_score) >= 3.0 else "High"
            })

    return {
        "total_transactions_analyzed": len(df),
        "anomalies_detected": len(anomalies),
        "zscore_threshold": zscore_threshold,
        "anomalies": anomalies
    }


# ============================================================================
# AUTOMATED ML INFERENCE RUNNER FOR REAL-TIME PIPELINE
# ============================================================================

def run_realtime_ml_inference(run_id: str = None) -> Dict[str, Any]:
    """
    High-level trigger called automatically after data ingestion to calculate
    instant real-time ML predictions across Churn Risk, Revenue Forecast, and Anomalies.
    """
    churn_res = predict_customer_churn_risk()
    revenue_res = forecast_sales_revenue(forecast_months=3)
    anomaly_res = detect_order_anomalies(zscore_threshold=1.8)

    return {
        "generated_at": datetime.now().isoformat(),
        "churn_risk_summary": churn_res.get("risk_breakdown", {}),
        "high_risk_customer_count": len(churn_res.get("high_risk_customers", [])),
        "revenue_forecast_status": revenue_res.get("status"),
        "next_month_projected_revenue": revenue_res.get("forecast", [{}])[0].get("predicted_net_revenue", 0.0) if revenue_res.get("forecast") else 0.0,
        "order_anomalies_count": anomaly_res.get("anomalies_detected", 0)
    }


if __name__ == "__main__":
    print("Testing ML Engine...")
    forecast = forecast_stock_prices("AAPL", 7)
    print(f"AAPL Forecast (7 days): {forecast['trend_direction']} trend, 1st day pred: {forecast['forecast'][0]['predicted_close']}")
    anomalies = detect_weather_anomalies(1.2)
    print(f"Weather Anomalies Detected: {anomalies['anomalies_found']}")
    churn = predict_customer_churn_risk()
    print(f"Customer Churn Risk Breakdown: {churn.get('risk_breakdown')}")
    rev_fct = forecast_sales_revenue(3)
    print(f"Revenue Forecast Status: {rev_fct.get('status')}")

