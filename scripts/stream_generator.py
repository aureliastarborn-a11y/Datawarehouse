"""
Data Warehouse - High-Frequency Mock Stream Event Generator
Simulates a live e-commerce production environment by POSTing micro-batches of order events
to the Real-Time Ingestion REST API endpoint (POST /api/v1/ingest/json).
"""

import sys
import os
import time
import random
import requests
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

API_URL = os.getenv("API_URL", "http://127.0.0.1:8000/api/v1/ingest/json")
API_KEY = os.getenv("API_KEY", "dw-secret-key-2024")

FIRST_NAMES = ["Liam", "Olivia", "Noah", "Emma", "Oliver", "Ava", "Elijah", "Sophia", "Lucas", "Isabella"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
CITIES = [
    ("New York", "NY", "USA"),
    ("San Francisco", "CA", "USA"),
    ("London", "ENG", "UK"),
    ("Tokyo", "Tokyo", "Japan"),
    ("Paris", "IDF", "France"),
    ("Berlin", "Berlin", "Germany"),
    ("Sydney", "NSW", "Australia")
]
TIERS = ["Bronze", "Silver", "Gold", "Platinum VIP", "Diamond Executive VIP"]

PRODUCTS = [
    {"id": "PROD_STREAM_1", "name": "Quantum Laptop Pro 16", "category": "Electronics", "subcategory": "Laptops", "price": 2400.0, "cost": 1500.0},
    {"id": "PROD_STREAM_2", "name": "AI Neural Workstation X", "category": "Hardware", "subcategory": "Workstations", "price": 4800.0, "cost": 3000.0},
    {"id": "PROD_STREAM_3", "name": "UltraSmart Watch Gen 5", "category": "Wearables", "subcategory": "Smartwatches", "price": 450.0, "cost": 220.0},
    {"id": "PROD_STREAM_4", "name": "Cloud Edge Router 10G", "category": "Networking", "subcategory": "Routers", "price": 1200.0, "cost": 750.0},
    {"id": "PROD_STREAM_5", "name": "ErgoDesk Pro Electric", "category": "Furniture", "subcategory": "Desks", "price": 850.0, "cost": 400.0}
]


def generate_streaming_event(event_id: int) -> dict:
    """Generate a single realistic order event payload."""
    fname = random.choice(FIRST_NAMES)
    lname = random.choice(LAST_NAMES)
    city, state, country = random.choice(CITIES)
    prod = random.choice(PRODUCTS)
    cust_id = f"CUST_STREAM_{random.randint(1, 20)}"
    qty = random.randint(1, 3)
    discount = round(random.uniform(0, 100), 2)
    tax = round((prod["price"] * qty - discount) * 0.08, 2)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return {
        "order_id": f"ORD_STREAM_{event_id:06d}",
        "order_line_number": 1,
        "customer_id": cust_id,
        "product_id": prod["id"],
        "first_name": fname,
        "last_name": lname,
        "email": f"{fname.lower()}.{lname.lower()}@example.com",
        "customer_tier": random.choice(TIERS),
        "city": city,
        "state": state,
        "country": country,
        "product_name": prod["name"],
        "category": prod["category"],
        "subcategory": prod["subcategory"],
        "unit_price": prod["price"],
        "cost_price": prod["cost"],
        "quantity": qty,
        "discount_amount": discount,
        "tax_amount": tax,
        "order_timestamp": timestamp,
        "updated_at": timestamp
    }


def run_stream_generator(interval_seconds: float = 2.0, max_events: int = 50):
    """Continuously post streaming events to the FastAPI Ingestion REST endpoint."""
    print("======================================================================")
    print("  STARTING HIGH-FREQUENCY REAL-TIME STREAM EVENT GENERATOR")
    print("======================================================================")
    print(f"Target API Endpoint: {API_URL}")
    print(f"Stream Interval: {interval_seconds} seconds | Max Events: {max_events}\n")

    headers = {"X-API-Key": API_KEY, "Content-Type": "application/json"}
    event_counter = 1000

    for i in range(max_events):
        event_counter += 1
        payload = [generate_streaming_event(event_counter)]

        try:
            res = requests.post(API_URL, json=payload, headers=headers, timeout=5.0)
            if res.status_code == 200:
                data = res.json()
                summary = data.get("summary", {})
                print(f"[STREAM EVENT #{i+1}] POSTed {payload[0]['order_id']} | Status: {res.status_code} | Ingestion Latency: {summary.get('execution_time_ms')}ms")
            else:
                print(f"[STREAM EVENT #{i+1}] Ingestion HTTP Error {res.status_code}: {res.text}")
        except Exception as e:
            print(f"[STREAM EVENT #{i+1}] Connection Exception: {e}")

        time.sleep(interval_seconds)

    print("\n======================================================================")
    print(f"  COMPLETED STREAMING GENERATION ({max_events} events dispatched)")
    print("======================================================================")


if __name__ == "__main__":
    run_stream_generator(interval_seconds=1.5, max_events=10)
