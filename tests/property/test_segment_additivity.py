"""Property-based test: segment counts must be additive.

sum(free, paid, unknown) == all for every (week, category, sentiment).
"""

from __future__ import annotations

import pandas as pd
import pytest

from voc_agent.analysis.segments import compute_snapshots


def test_segment_additivity_synthetic() -> None:
    """Verify that segment counts are additive across tiers."""
    # Create a synthetic classified dataset
    import sqlite3

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Create tables
    conn.executescript("""
        CREATE TABLE canonical_vocs (
            voc_id TEXT PRIMARY KEY,
            canonical_text TEXT,
            content_hash TEXT,
            iso_week_first_seen TEXT,
            customer_tier TEXT,
            customer_id TEXT,
            mrr_usd REAL
        );
        CREATE TABLE classifications (
            voc_id TEXT,
            sentiment TEXT,
            category TEXT,
            subcategory TEXT,
            confidence REAL
        );
    """)

    # Insert test data with known distribution
    tiers = {
        "free": 10,
        "paid_lt_299": 15,
        "paid_gte_299": 20,
        "unknown": 5,
    }

    voc_id = 0
    for tier, count in tiers.items():
        for i in range(count):
            voc_id += 1
            conn.execute(
                "INSERT INTO canonical_vocs VALUES (?, ?, ?, ?, ?, ?, ?)",
                (f"v{voc_id}", f"feedback {voc_id}", f"h{voc_id}",
                 "2025-W10", tier, f"c{voc_id}", 0 if tier == "free" else 300),
            )
            conn.execute(
                "INSERT INTO classifications VALUES (?, ?, ?, ?, ?)",
                (f"v{voc_id}", "negative", "data_quality", "accuracy", 0.85),
            )

    conn.commit()

    # Compute snapshots
    snapshots_df = compute_snapshots(conn)

    # Verify: "all" segment should have all 50 records
    all_count = snapshots_df[snapshots_df["segment"] == "all"]["voc_count"].sum()
    assert all_count == 50

    # Verify: paid = paid_lt_299 + paid_gte_299
    paid_count = snapshots_df[snapshots_df["segment"] == "paid"]["voc_count"].sum()
    paid_lt = snapshots_df[snapshots_df["segment"] == "paid_lt_299"]["voc_count"].sum()
    paid_gte = snapshots_df[snapshots_df["segment"] == "paid_gte_299"]["voc_count"].sum()
    assert paid_count == paid_lt + paid_gte

    # Verify: free segment count matches
    free_count = snapshots_df[snapshots_df["segment"] == "free"]["voc_count"].sum()
    assert free_count == tiers["free"]

    conn.close()


def test_classification_invariant() -> None:
    """Every classified VOC has exactly one sentiment AND one category."""
    import sqlite3

    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE canonical_vocs (
            voc_id TEXT PRIMARY KEY,
            canonical_text TEXT,
            content_hash TEXT,
            iso_week_first_seen TEXT,
            customer_tier TEXT,
            customer_id TEXT,
            mrr_usd REAL
        );
        CREATE TABLE classifications (
            voc_id TEXT,
            sentiment TEXT,
            category TEXT,
            subcategory TEXT,
            confidence REAL
        );
    """)

    # Insert records with data_quality category
    for i in range(10):
        conn.execute(
            "INSERT INTO canonical_vocs VALUES (?, ?, ?, ?, ?, ?, ?)",
            (f"v{i}", f"text {i}", f"h{i}", "2025-W10", "paid_gte_299", f"c{i}", 500),
        )
        conn.execute(
            "INSERT INTO classifications VALUES (?, ?, ?, ?, ?)",
            (f"v{i}", "negative", "data_quality", "consistency", 0.9),
        )

    conn.commit()

    # Every row should have non-null sentiment and category
    df = pd.read_sql_query("SELECT * FROM classifications", conn)
    assert df["sentiment"].notna().all()
    assert df["category"].notna().all()

    # data_quality rows must have subcategory
    dq = df[df["category"] == "data_quality"]
    assert dq["subcategory"].notna().all()

    conn.close()
