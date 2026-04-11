"""Shared test fixtures and synthetic data factories for R&A VOC Analyzer."""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from voc_agent.storage.db import init_db, get_connection


# ── Database Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    """Create a temporary SQLite database with schema initialized."""
    db_path = tmp_path / "test_voc.db"
    init_db(db_path)
    return db_path


@pytest.fixture
def db_conn(tmp_db: Path):
    """Yield a connection to the temporary test database."""
    with get_connection(tmp_db) as conn:
        yield conn


# ── Synthetic Data Factories ──────────────────────────────────────────────────


def make_channel1_message(
    user_id: str = "12345678",
    mrr: float | None = 132,
    plan: str = "Standard plan",
    csat: str = "Poor",
    feedback: str = "The reporting is confusing.",
    survey_type: str = "In-App Feedback Badge",
    ts_offset_days: int = 0,
    page_url: str = "https://us1.admin.mailchimp.com/analytics/",
) -> dict[str, Any]:
    """Create a synthetic Channel 1 (#mc-reporting-analytics-feedback) message."""
    base_ts = 1775600000.0 + (ts_offset_days * 86400)
    mrr_str = f"  {int(mrr)}" if mrr is not None else " null"
    csat_line = f"*CSAT* {csat}" if csat else "*CSAT* "

    text = (
        f"*New Survey Response from the {survey_type}*\n\n"
        f"*MRR:*{mrr_str}\n"
        f"*Plan:* {plan} \n"
        f"*User ID:* {user_id}\n"
        f"{csat_line}\n"
        f"*Current Page* <{page_url}>\n"
        f"*Feedback:* {feedback}\n\n"
        f"*Fullstory:* <https://app.fullstory.com/ui/ZHBMT/client-session/test>\n"
        f"_______________________________________"
    )

    return {
        "type": "message",
        "subtype": "bot_message",
        "ts": str(base_ts),
        "bot_id": "W017BFA7JKT",
        "text": text,
        "username": "Qualtrics",
    }


def make_channel2_message(
    user_id: str = "12345678",
    mrr: float = 450,
    plan: str = "Paid",
    csat: str = "Poor",
    feedback: str = "The reporting is confusing.",
    survey_type: str = "Feedback Badge",
    ts_offset_days: int = 0,
    prs_score: int | None = None,
    prs_reason: str | None = None,
) -> dict[str, Any]:
    """Create a synthetic Channel 2 (#hvc_feedback) message."""
    base_ts = 1775600000.0 + (ts_offset_days * 86400)

    if prs_score is not None:
        text = (
            f"*Response from the PRS Survey*\n\n"
            f"*User ID:* {user_id}  | *{plan}* | *MRR:* {int(mrr)}\n\n"
            f"*PRS:* {prs_score}\n"
            f"*Reason:* {prs_reason or 'Price'}\n"
            f"*Feedback:* {feedback}\n\n"
            f"*<https://app.fullstory.com/test|FS Session Replay>*"
        )
    else:
        text = (
            f"*Response from {survey_type}*\n\n"
            f"*User ID:* {user_id}  | *{plan}* | *MRR:*  {int(mrr)}\n"
            f"*Page URL:* <https://us1.admin.mailchimp.com/analytics/>\n\n"
            f"*CSAT:* {csat}\n"
            f"*Feedback:* {feedback}\n\n"
            f"*<https://app.fullstory.com/test|FS Session Replay>*\n"
            f"_______________________________________"
        )

    return {
        "type": "message",
        "subtype": "bot_message",
        "ts": str(base_ts),
        "bot_id": "W017BFA7JKT",
        "text": text,
        "username": "Qualtrics",
    }


def make_channel3_message(
    customer_name: str = "Acme Corp",
    customer_uid: str = "99999999",
    mrr: float = 7000,
    impacted_product: str = "Analytics",
    goal: str = "See accurate delivery rates",
    constraints: str = "Delivery rate rounds to 100% incorrectly",
    criticality: str = "P1 (High) \u2013 Absence of this causing significant pain, potential churn risk",
    ts_offset_days: int = 0,
) -> dict[str, Any]:
    """Create a synthetic Channel 3 (#mc-hvc-escalations) message."""
    base_ts = 1775600000.0 + (ts_offset_days * 86400)

    text = (
        ":postal_horn: *New HVC Product Feedback Received* :postal_horn:\n\n"
        f"*Customer Name*\n{customer_name}\n"
        f"*Source*\nCustomer Success - Strategic\n"
        f"*Submitter*\n<@U02KPL71J3E|Test User>\n"
        f"*Impacted Product*\n{impacted_product}\n"
        f"*Goal: what is the user trying to accomplish?* \n{goal}\n"
        f"*Constraints: what constraints is the user facing?*\n{constraints}\n"
        f"*Workaround details*\nNo effective workaround.\n"
        f"*Supportive materials*\n\n"
        f"*Criticality (if specific customer request)*\n{criticality}\n"
        f"*Customer UID*\n{customer_uid}\n"
        f"*MRR*\n{int(mrr)}"
    )

    return {
        "type": "message",
        "subtype": "bot_message",
        "ts": str(base_ts),
        "bot_id": "B082KKRGCF2",
        "text": text,
        "username": "Existing Customer Feedback Intake (Global)",
    }


def make_parsed_feedback(
    sentiment: str = "negative",
    category: str = "data_quality",
    subcategory: str | None = "consistency",
    mrr: float | None = 450,
    iso_week: str = "2026-W10",
    feedback_text: str = "Numbers don't match between reports",
) -> dict[str, Any]:
    """Create a synthetic classified VOC record for analytics testing."""
    return {
        "voc_id": str(uuid.uuid4()),
        "canonical_text": feedback_text,
        "content_hash": "abc123",
        "iso_week_first_seen": iso_week,
        "customer_tier": "paid_gte_299" if mrr and mrr >= 299 else "paid_lt_299" if mrr and mrr > 0 else "free",
        "mrr_usd": mrr,
        "sentiment": sentiment,
        "category": category,
        "subcategory": subcategory,
        "confidence": 0.85,
    }


def make_weekly_data(
    n_weeks: int = 13,
    start_week: str = "2026-W01",
    sentiment_dist: dict[str, float] | None = None,
    base_volume: int = 50,
) -> list[dict[str, Any]]:
    """Generate n weeks of synthetic analytics data."""
    if sentiment_dist is None:
        sentiment_dist = {"positive": 0.2, "neutral": 0.3, "negative": 0.5}

    records = []
    year, week_num = int(start_week[:4]), int(start_week[6:])

    for i in range(n_weeks):
        w = week_num + i
        y = year + (w - 1) // 52
        w = ((w - 1) % 52) + 1
        iso_week = f"{y}-W{w:02d}"

        for sentiment, ratio in sentiment_dist.items():
            count = max(1, int(base_volume * ratio))
            records.append({
                "iso_week": iso_week,
                "segment": "all",
                "category": "data_quality",
                "sentiment": sentiment,
                "voc_count": count,
                "unique_customers": max(1, count - 5),
                "mean_confidence": 0.82,
            })

    return records
