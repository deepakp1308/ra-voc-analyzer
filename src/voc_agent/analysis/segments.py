"""Segment computation and snapshot materialization.

Every aggregate is computed 5 times (all, free, paid, paid_gte_299, paid_lt_299).
Results are stored in the snapshots table for fast report generation.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from voc_agent.storage.db import get_connection

logger = structlog.get_logger()

SEGMENTS = {
    "all": None,  # No filter
    "free": "free",
    "paid": ("paid_lt_299", "paid_gte_299"),  # Both paid tiers
    "paid_gte_299": "paid_gte_299",
    "paid_lt_299": "paid_lt_299",
}


def compute_snapshots(conn: Any) -> pd.DataFrame:
    """Compute weekly rollup snapshots for all segments.

    Returns a DataFrame with columns: iso_week, segment, category, sentiment,
    voc_count, unique_customers, mean_confidence.
    """
    # Load classified VOCs with tier info
    query = """
        SELECT
            cv.iso_week_first_seen AS iso_week,
            cv.customer_tier,
            cv.customer_id,
            c.category,
            c.sentiment,
            c.confidence
        FROM canonical_vocs cv
        JOIN classifications c ON cv.voc_id = c.voc_id
        WHERE c.sentiment IS NOT NULL
    """
    df = pd.read_sql_query(query, conn)

    if df.empty:
        logger.warning("no_classified_vocs_for_snapshots")
        return pd.DataFrame()

    all_snapshots = []

    for segment_name, tier_filter in SEGMENTS.items():
        if tier_filter is None:
            segment_df = df  # All records
        elif isinstance(tier_filter, tuple):
            segment_df = df[df["customer_tier"].isin(tier_filter)]
        else:
            segment_df = df[df["customer_tier"] == tier_filter]

        if segment_df.empty:
            continue

        # Group by week x category x sentiment
        grouped = (
            segment_df.groupby(["iso_week", "category", "sentiment"])
            .agg(
                voc_count=("category", "size"),
                unique_customers=("customer_id", "nunique"),
                mean_confidence=("confidence", "mean"),
            )
            .reset_index()
        )
        grouped["segment"] = segment_name
        all_snapshots.append(grouped)

    if not all_snapshots:
        return pd.DataFrame()

    result = pd.concat(all_snapshots, ignore_index=True)
    result["mean_confidence"] = result["mean_confidence"].round(3)

    logger.info(
        "snapshots_computed",
        total_rows=len(result),
        segments=list(SEGMENTS.keys()),
        weeks=result["iso_week"].nunique(),
    )

    return result


def store_snapshots(conn: Any, snapshots_df: pd.DataFrame) -> int:
    """Store computed snapshots in the database (replace existing)."""
    if snapshots_df.empty:
        return 0

    # Clear existing snapshots for the weeks we're updating
    weeks = snapshots_df["iso_week"].unique().tolist()
    placeholders = ",".join(["?" for _ in weeks])
    conn.execute(f"DELETE FROM snapshots WHERE iso_week IN ({placeholders})", weeks)

    # Insert new snapshots
    rows = snapshots_df.to_dict("records")
    for row in rows:
        conn.execute(
            """INSERT INTO snapshots
               (iso_week, segment, category, sentiment, voc_count, unique_customers, mean_confidence)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                row["iso_week"], row["segment"], row["category"],
                row["sentiment"], row["voc_count"], row["unique_customers"],
                row["mean_confidence"],
            ),
        )

    logger.info("snapshots_stored", rows=len(rows))
    return len(rows)


def run_analysis() -> None:
    """Main entry point: compute and store all snapshots."""
    with get_connection() as conn:
        snapshots_df = compute_snapshots(conn)
        if not snapshots_df.empty:
            store_snapshots(conn, snapshots_df)
