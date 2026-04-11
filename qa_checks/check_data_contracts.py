"""Data contract validation using pandera.

Gate 3: Run against actual pipeline output. Hard-fail if ANY contract is violated.
These checks ensure data integrity before the dashboard is published.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema
import structlog

logger = structlog.get_logger()

VALID_SENTIMENTS = {"positive", "neutral", "negative"}
VALID_CATEGORIES = {"feature_gap", "bug_or_error", "data_quality", "performance_ux", "other_or_praise"}
VALID_SUBCATEGORIES = {"accuracy", "consistency", "availability", "freshness", "coverage"}
VALID_TIERS = {"free", "paid_lt_299", "paid_gte_299", "unknown"}
ISO_WEEK_PATTERN = r"^\d{4}-W\d{2}$"


# ── Pandera Schemas ───────────────────────────────────────────────────────────

canonical_vocs_schema = DataFrameSchema(
    {
        "voc_id": Column(str, nullable=False, unique=True),
        "canonical_text": Column(str, nullable=False),
        "content_hash": Column(str, nullable=False),
        "iso_week_first_seen": Column(str, Check.str_matches(ISO_WEEK_PATTERN), nullable=False),
        "customer_tier": Column(str, Check.isin(VALID_TIERS), nullable=False),
        "mrr_usd": Column(float, Check.ge(0), nullable=True),
    },
    coerce=True,
)

classifications_schema = DataFrameSchema(
    {
        "voc_id": Column(str, nullable=False),
        "sentiment": Column(str, Check.isin(VALID_SENTIMENTS), nullable=False),
        "sentiment_score": Column(float, Check.in_range(-1.0, 1.0), nullable=False),
        "category": Column(str, Check.isin(VALID_CATEGORIES), nullable=False),
        "confidence": Column(float, Check.in_range(0.0, 1.0), nullable=False),
    },
    coerce=True,
)

snapshots_schema = DataFrameSchema(
    {
        "iso_week": Column(str, Check.str_matches(ISO_WEEK_PATTERN), nullable=False),
        "segment": Column(str, nullable=False),
        "category": Column(str, Check.isin(VALID_CATEGORIES), nullable=False),
        "sentiment": Column(str, Check.isin(VALID_SENTIMENTS), nullable=False),
        "voc_count": Column(int, Check.ge(0), nullable=False),
    },
    coerce=True,
)


# ── Contract Check Functions ──────────────────────────────────────────────────

def check_canonical_vocs(conn: sqlite3.Connection) -> list[str]:
    """Validate canonical_vocs table against schema."""
    failures = []
    df = pd.read_sql_query("SELECT * FROM canonical_vocs", conn)

    if df.empty:
        failures.append("canonical_vocs table is empty")
        return failures

    try:
        canonical_vocs_schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        for _, row in e.failure_cases.iterrows():
            failures.append(f"canonical_vocs: {row.get('check', 'unknown')} failed on column {row.get('column', 'unknown')}")

    # Check: no duplicate voc_id
    dup_count = df["voc_id"].duplicated().sum()
    if dup_count > 0:
        failures.append(f"canonical_vocs: {dup_count} duplicate voc_id values")

    return failures


def check_classifications(conn: sqlite3.Connection) -> list[str]:
    """Validate classifications table."""
    failures = []
    df = pd.read_sql_query("SELECT * FROM classifications", conn)

    if df.empty:
        failures.append("classifications table is empty")
        return failures

    try:
        classifications_schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        for _, row in e.failure_cases.iterrows():
            failures.append(f"classifications: {row.get('check', 'unknown')} failed")

    # Check: data_quality rows must have subcategory
    dq_rows = df[df["category"] == "data_quality"]
    if not dq_rows.empty and "subcategory" in df.columns:
        missing_sub = dq_rows["subcategory"].isna().sum()
        if missing_sub > 0:
            failures.append(f"classifications: {missing_sub} data_quality rows missing subcategory")

    # Check: confidence median >= 0.65
    confidence_median = df["confidence"].median()
    if confidence_median < 0.65:
        failures.append(f"classifications: confidence median {confidence_median:.2f} < 0.65 (prompt drift?)")

    return failures


def check_snapshots(conn: sqlite3.Connection) -> list[str]:
    """Validate snapshots table."""
    failures = []
    df = pd.read_sql_query("SELECT * FROM snapshots", conn)

    if df.empty:
        failures.append("snapshots table is empty")
        return failures

    try:
        snapshots_schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        for _, row in e.failure_cases.iterrows():
            failures.append(f"snapshots: {row.get('check', 'unknown')} failed")

    # Check: no week with < 5 messages (ingestion break)
    all_seg = df[df["segment"] == "all"]
    weekly_totals = all_seg.groupby("iso_week")["voc_count"].sum()
    low_weeks = weekly_totals[weekly_totals < 5]
    if len(low_weeks) > 0:
        failures.append(f"snapshots: {len(low_weeks)} weeks with < 5 VOCs (ingestion break?): {list(low_weeks.index[:3])}")

    # Check: unknown-tier share < 25%
    if "segment" in df.columns:
        for week in df["iso_week"].unique():
            week_data = df[df["iso_week"] == week]
            all_count = week_data[week_data["segment"] == "all"]["voc_count"].sum()
            # Unknown tier rows only appear in 'all', not in other segments
            # So compare all vs sum of free+paid segments
            known_segments = week_data[week_data["segment"].isin(["free", "paid", "paid_gte_299", "paid_lt_299"])]
            known_count = known_segments.groupby("segment")["voc_count"].sum()
            # paid includes both lt and gte, so use max of paid vs (lt+gte)
            if all_count > 0:
                paid_total = known_count.get("paid", 0)
                free_total = known_count.get("free", 0)
                known = paid_total + free_total
                unknown_share = (all_count - known) / all_count if known < all_count else 0
                if unknown_share > 0.25:
                    failures.append(f"snapshots: unknown-tier share {unknown_share:.0%} > 25% in week {week}")
                    break  # Report first occurrence only

    # Check: no category with 0 VOCs for 3 consecutive weeks
    categories = all_seg["category"].unique()
    for cat in categories:
        cat_weekly = all_seg[all_seg["category"] == cat].groupby("iso_week")["voc_count"].sum().sort_index()
        zero_streak = 0
        for count in cat_weekly:
            if count == 0:
                zero_streak += 1
                if zero_streak >= 3:
                    failures.append(f"snapshots: category '{cat}' has 0 VOCs for 3+ consecutive weeks (label rot?)")
                    break
            else:
                zero_streak = 0

    return failures


def run_all_contracts(conn: sqlite3.Connection) -> dict[str, Any]:
    """Run all data contract checks. Returns pass/fail status and details."""
    all_failures: list[str] = []

    all_failures.extend(check_canonical_vocs(conn))
    all_failures.extend(check_classifications(conn))
    all_failures.extend(check_snapshots(conn))

    status = "PASS" if not all_failures else "FAIL"

    result = {
        "status": status,
        "failure_count": len(all_failures),
        "failures": all_failures,
    }

    if all_failures:
        logger.error("data_contract_check_failed", **result)
    else:
        logger.info("data_contract_check_passed")

    return result
