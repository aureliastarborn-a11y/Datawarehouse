#!/usr/bin/env python3
"""
Enterprise Data Warehouse System - Mock Raw Data Generator
Generates seed datasets simulating realistic e-commerce operations, 
including dirty data, formatting edge cases, and SCD Type 2 updates.
"""

import os
import sys
import csv
import json
import random
from datetime import datetime, timedelta

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# Create output directories
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

random.seed(42)

def generate_products():
    categories = {
        "Electronics": ["Laptops", "Smartphones", "Headphones", "Monitors", "Keyboards"],
        "Apparel": ["Men's Jackets", "Women's Dresses", "Footwear", "Sportswear"],
        "Home & Kitchen": ["Coffee Makers", "Blenders", "Air Purifiers", "Cookware"],
        "Books & Media": ["Fiction Bestellers", "Tech Manuals", "Self-Help"]
    }
    
    products = []
    prod_num = 1
    for cat, subcats in categories.items():
        for subcat in subcats:
            for i in range(1, 4):
                prod_id = f"PROD-{prod_num:04d}"
                name = f"{subcat} Model {chr(64 + i)}"
                unit_price = round(random.uniform(20.0, 1200.0), 2)
                cost_price = round(unit_price * random.uniform(0.4, 0.7), 2)
                products.append({
                    "raw_product_id": prod_id,
                    "product_name": f"  {name}  ", # Intentional whitespace to test cleaning
                    "category": cat,
                    "subcategory": subcat,
                    "unit_price": unit_price,
                    "cost_price": cost_price,
                    "updated_at": "2024-01-01 00:00:00"
                })
                prod_num += 1
                
    file_path = os.path.join(DATA_DIR, "raw_products.csv")
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(products[0].keys()))
        writer.writeheader()
        writer.writerows(products)
    print(f"[OK] Created raw_products.csv with {len(products)} products at {file_path}")
    return products

def generate_customers():
    first_names = ["John", "Jane", "Alice", "Robert", "Emily", "Michael", "Sarah", "David", "Jessica", "Daniel", "Laura", "James"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
    cities_states = [
        ("New York", "NY", "USA"),
        ("Los Angeles", "CA", "USA"),
        ("Chicago", "IL", "USA"),
        ("Houston", "TX", "USA"),
        ("Toronto", "ON", "Canada"),
        ("London", "ENG", "UK"),
        ("Berlin", "BER", "Germany")
    ]
    tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    
    customers_initial = []
    customer_updates = []
    
    start_base_date = datetime(2024, 1, 1, 9, 0, 0)
    
    for i in range(1, 101):
        cust_id = f"CUST-{i:04d}"
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        city, state, country = random.choice(cities_states)
        tier = random.choice(tiers)
        email = f"{fn}.{ln}{i}@Example.Com" # Mixed case email to test standardization
        
        # Initial Customer Record (Batch 1 - 2024-01-01)
        customers_initial.append({
            "raw_customer_id": cust_id,
            "first_name": f"{fn} ",
            "last_name": ln,
            "email": email,
            "customer_tier": tier if random.random() > 0.1 else "", # 10% null tier to test default handling
            "city": city,
            "state": state,
            "country": country,
            "updated_at": start_base_date.strftime("%Y-%m-%d %H:%M:%S")
        })
        
        # 30% of customers experience an SCD Type 2 update later in the timeline (e.g. tier upgrade or move to new city)
        if i % 3 == 0:
            update_date = start_base_date + timedelta(days=random.randint(90, 300))
            new_tier = "Gold" if tier == "Silver" or tier == "Bronze" else "Platinum"
            new_city, new_state, new_country = random.choice(cities_states)
            
            customer_updates.append({
                "raw_customer_id": cust_id,
                "first_name": fn,
                "last_name": ln,
                "email": f"{fn.lower()}.{ln.lower()}{i}@newdomain.com",
                "customer_tier": new_tier,
                "city": new_city,
                "state": new_state,
                "country": new_country,
                "updated_at": update_date.strftime("%Y-%m-%d %H:%M:%S")
            })

    # Save initial customers
    path_init = os.path.join(DATA_DIR, "raw_customers_batch1.csv")
    with open(path_init, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(customers_initial[0].keys()))
        writer.writeheader()
        writer.writerows(customers_initial)
        
    # Save customer updates (SCD Type 2 trigger batch)
    path_updates = os.path.join(DATA_DIR, "raw_customers_batch2.csv")
    with open(path_updates, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(customer_updates[0].keys()))
        writer.writeheader()
        writer.writerows(customer_updates)

    print(f"[OK] Created raw_customers_batch1.csv ({len(customers_initial)} recs) and batch2 updates ({len(customer_updates)} recs)")
    return customers_initial, customer_updates

def generate_orders(products, customers):
    orders = []
    order_num = 1001
    
    start_date = datetime(2024, 1, 2)
    end_date = datetime(2025, 12, 31)
    date_delta_days = (end_date - start_date).days
    
    for _ in range(600): # 600 orders
        order_id = f"ORD-{order_num}"
        cust_id = f"CUST-{random.randint(1, 100):04d}"
        
        order_time = start_date + timedelta(
            days=random.randint(0, date_delta_days),
            hours=random.randint(8, 20),
            minutes=random.randint(0, 59)
        )
        
        num_items = random.randint(1, 4)
        selected_prods = random.sample(products, num_items)
        
        for line_idx, prod in enumerate(selected_prods, start=1):
            qty = random.randint(1, 3)
            unit_price = float(prod["unit_price"])
            gross = qty * unit_price
            discount = round(gross * random.choice([0.0, 0.05, 0.10, 0.15]), 2)
            tax = round((gross - discount) * 0.08, 2)
            
            orders.append({
                "order_id": order_id,
                "order_line_number": line_idx,
                "customer_id": cust_id,
                "product_id": prod["raw_product_id"],
                "order_timestamp": order_time.strftime("%Y-%m-%d %H:%M:%S"),
                "quantity": qty,
                "unit_price": unit_price,
                "discount_amount": discount,
                "tax_amount": tax
            })
        order_num += 1

    # Add duplicate & dirty line items to test data validation gates
    orders.append(orders[0].copy()) # Exact Duplicate
    orders.append({
        "order_id": "ORD-9999",
        "order_line_number": 1,
        "customer_id": "", # Missing Customer ID (Orphan check)
        "product_id": "PROD-0001",
        "order_timestamp": "2024-06-01 12:00:00",
        "quantity": -5, # Corrupt Quantity
        "unit_price": 100.0,
        "discount_amount": 0.0,
        "tax_amount": 8.0
    })

    file_path = os.path.join(DATA_DIR, "raw_orders.csv")
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(orders[0].keys()))
        writer.writeheader()
        writer.writerows(orders)
        
    print(f"[OK] Created raw_orders.csv with {len(orders)} order lines at {file_path}")

def main():
    print("[INIT] Generating Mock Datasets for Enterprise Data Warehouse Pipeline...")
    prods = generate_products()
    cust_initial, cust_updates = generate_customers()
    generate_orders(prods, cust_initial)
    print("[SUCCESS] Mock Data Generation Completed Successfully!")

if __name__ == "__main__":
    main()
