"""
Data Warehouse - Real-Time Webhook & Alert Notification Engine
Handles automated Slack, Discord, and Email webhook alerts for:
  1. Data Quality Check Failures & Corrupt Value Filters
  2. High-Value VIP Customer Transactions (>= $1,000)
  3. Customer Churn & Anomaly Surge Alerts
"""

import sys
import os
import json
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# Environment Configurations
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
ALERT_MIN_TRANSACTION_AMOUNT = float(os.getenv("ALERT_MIN_TRANSACTION_AMOUNT", "1000.0"))

# In-Memory Notification Audit Trail
NOTIFICATION_HISTORY: List[Dict[str, Any]] = []


def send_webhook_notification(
    title: str,
    message: str,
    alert_type: str = "info",
    metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Core notification dispatcher.
    Sends rich formatted JSON payloads to Slack or Discord webhooks.
    Falls back to console logger if webhook URLs are not set.
    """
    timestamp = datetime.now().isoformat()

    color_map = {
        "critical": "#DC3545",  # Red
        "warning": "#FFC107",   # Yellow / Orange
        "vip": "#D4AF37",       # Gold
        "success": "#28A745",   # Green
        "info": "#17A2B8"       # Blue
    }
    color = color_map.get(alert_type.lower(), "#17A2B8")

    record = {
        "timestamp": timestamp,
        "title": title,
        "message": message,
        "alert_type": alert_type,
        "metadata": metadata or {},
        "dispatched_to": []
    }

    # 1. Dispatch to Slack Webhook
    if SLACK_WEBHOOK_URL and "hooks.slack.com" in SLACK_WEBHOOK_URL:
        try:
            slack_payload = {
                "attachments": [
                    {
                        "color": color,
                        "title": f"🏛️ Data Warehouse Alert: {title}",
                        "text": message,
                        "fields": [
                            {"title": k, "value": str(v), "short": True}
                            for k, v in (metadata or {}).items()
                        ],
                        "footer": "Real-Time Data Warehouse Engine",
                        "ts": int(datetime.now().timestamp())
                    }
                ]
            }
            res = requests.post(SLACK_WEBHOOK_URL, json=slack_payload, timeout=3.0)
            if res.status_code == 200:
                record["dispatched_to"].append("slack")
        except Exception as e:
            print(f"[NOTIFICATION WARNING] Slack dispatch failed: {e}")

    # 2. Dispatch to Discord Webhook
    if DISCORD_WEBHOOK_URL and "discord.com" in DISCORD_WEBHOOK_URL:
        try:
            discord_payload = {
                "embeds": [
                    {
                        "title": f"🏛️ DW Alert: {title}",
                        "description": message,
                        "color": int(color.replace("#", ""), 16),
                        "fields": [
                            {"name": k, "value": str(v), "inline": True}
                            for k, v in (metadata or {}).items()
                        ],
                        "footer": {"text": "Real-Time Automated Data Warehouse"}
                    }
                ]
            }
            res = requests.post(DISCORD_WEBHOOK_URL, json=discord_payload, timeout=3.0)
            if res.status_code in (200, 204):
                record["dispatched_to"].append("discord")
        except Exception as e:
            print(f"[NOTIFICATION WARNING] Discord dispatch failed: {e}")

    # 3. Fallback Console Logger
    if not record["dispatched_to"]:
        record["dispatched_to"].append("console_log")
        print(f"\n[REAL-TIME ALERT ({alert_type.upper()})] {title}")
        print(f"  Details: {message}")
        if metadata:
            print(f"  Metadata: {json.dumps(metadata)}")

    NOTIFICATION_HISTORY.insert(0, record)
    if len(NOTIFICATION_HISTORY) > 100:
        NOTIFICATION_HISTORY.pop()

    return record


def send_quality_failure_alert(
    table_name: str,
    check_name: str,
    records_failed: int,
    details: str,
    pipeline_run_id: str = None
) -> Dict[str, Any]:
    """Trigger alert when Data Quality Check Filter Gate rejects records."""
    title = f"Data Quality Check Failed on '{table_name}'"
    message = f"Check '{check_name}' failed for {records_failed} record(s).\nDetails: {details}"

    return send_webhook_notification(
        title=title,
        message=message,
        alert_type="critical",
        metadata={
            "table_name": table_name,
            "check_name": check_name,
            "records_failed": records_failed,
            "pipeline_run_id": pipeline_run_id or "N/A"
        }
    )


def send_high_value_transaction_alert(
    order_id: str,
    customer_name: str,
    net_amount: float,
    customer_tier: str
) -> Dict[str, Any]:
    """Trigger alert when a high-value VIP transaction occurs."""
    title = f"High-Value VIP Order Detected: ${net_amount:,.2f}"
    message = f"Order '{order_id}' placed by customer '{customer_name}' ({customer_tier} Tier) for ${net_amount:,.2f} net revenue."

    return send_webhook_notification(
        title=title,
        message=message,
        alert_type="vip",
        metadata={
            "order_id": order_id,
            "customer_name": customer_name,
            "customer_tier": customer_tier,
            "net_amount": f"${net_amount:,.2f}"
        }
    )


def get_notification_history(limit: int = 20) -> List[Dict[str, Any]]:
    """Return historical dispatched notification records."""
    return NOTIFICATION_HISTORY[:limit]
