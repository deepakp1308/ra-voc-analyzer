"""90-day data quality deep dive.

Computes subcategory-level trends for the data_quality category
over the last 13 weeks, with linear regression slopes and
reduced/increased/steady labels.
"""

from __future__ import annotations

from typing import Any, Literal

import pandas as pd
import structlog

logger = structlog.get_logger()

SUBCATEGORIES = ["accuracy", "consistency", "availability", "freshness", "coverage"]


def compute_dq_deep_dive(
    conn: Any,
    segment: str = "all",
    n_weeks: int = 13,
) -> dict[str, Any]:
    """Compute the 90-day data quality deep dive.

    Returns dict with:
    - subcategory_trends: weekly counts per subcategory
    - subcategory_slopes: trend direction per subcategory
    - top_examples: top 5 verbatim examples per subcategory
    """
    # Get weekly data_quality counts by subcategory
    tier_clause = _tier_clause(segment)

    query = f"""
        SELECT
            cv.iso_week_first_seen AS iso_week,
            c.subcategory,
            COUNT(*) AS voc_count,
            AVG(c.confidence) AS mean_confidence
        FROM canonical_vocs cv
        JOIN classifications c ON cv.voc_id = c.voc_id
        WHERE c.category = 'data_quality'
          AND c.subcategory IS NOT NULL
          {tier_clause}
        GROUP BY cv.iso_week_first_seen, c.subcategory
        ORDER BY cv.iso_week_first_seen
    """
    df = pd.read_sql_query(query, conn)

    if df.empty:
        return {"subcategory_trends": [], "subcategory_slopes": [], "top_examples": []}

    # Get the last n_weeks
    all_weeks = sorted(df["iso_week"].unique())
    recent_weeks = all_weeks[-n_weeks:] if len(all_weeks) >= n_weeks else all_weeks
    df = df[df["iso_week"].isin(recent_weeks)]

    # Compute slopes per subcategory
    slopes = []
    for subcat in SUBCATEGORIES:
        subcat_df = df[df["subcategory"] == subcat].sort_values("iso_week")
        if len(subcat_df) < 3:
            slopes.append({
                "subcategory": subcat,
                "direction": "insufficient_data",
                "slope": 0.0,
                "first_4wk_mean": 0.0,
                "last_4wk_mean": 0.0,
                "total_count": 0,
            })
            continue

        counts = subcat_df["voc_count"].values
        slope = _linear_slope(counts)
        first_4wk = float(counts[:4].mean()) if len(counts) >= 4 else float(counts.mean())
        last_4wk = float(counts[-4:].mean()) if len(counts) >= 4 else float(counts.mean())

        direction = _classify_slope(slope, first_4wk, last_4wk)

        slopes.append({
            "subcategory": subcat,
            "direction": direction,
            "slope": round(slope, 3),
            "first_4wk_mean": round(first_4wk, 1),
            "last_4wk_mean": round(last_4wk, 1),
            "total_count": int(subcat_df["voc_count"].sum()),
        })

    # Get top examples per subcategory
    examples = _get_top_examples(conn, segment, n=5)

    return {
        "subcategory_trends": df.to_dict("records"),
        "subcategory_slopes": slopes,
        "top_examples": examples,
    }


def _classify_slope(
    slope: float, first_4wk_mean: float, last_4wk_mean: float
) -> Literal["reduced", "increased", "steady"]:
    """Classify trend direction per the spec.

    - Reduced: slope < 0 AND last-4wk mean < first-4wk mean * 0.85
    - Increased: slope > 0 AND last-4wk mean > first-4wk mean * 1.15
    - Steady: otherwise
    """
    if first_4wk_mean == 0:
        return "steady"

    if slope < 0 and last_4wk_mean < first_4wk_mean * 0.85:
        return "reduced"
    if slope > 0 and last_4wk_mean > first_4wk_mean * 1.15:
        return "increased"
    return "steady"


def _linear_slope(values: Any) -> float:
    """Compute slope via simple linear regression on sequential values."""
    n = len(values)
    if n < 2:
        return 0.0

    x = list(range(n))
    x_mean = sum(x) / n
    y_mean = sum(values) / n

    numerator = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, values))
    denominator = sum((xi - x_mean) ** 2 for xi in x)

    if denominator == 0:
        return 0.0

    return numerator / denominator


def _tier_clause(segment: str) -> str:
    """Build SQL WHERE clause for segment filtering."""
    if segment == "all":
        return ""
    if segment == "paid":
        return "AND cv.customer_tier IN ('paid_lt_299', 'paid_gte_299')"
    return f"AND cv.customer_tier = '{segment}'"


def _get_top_examples(conn: Any, segment: str, n: int = 5) -> list[dict[str, Any]]:
    """Get top N recent, high-confidence examples per data_quality subcategory."""
    tier_clause = _tier_clause(segment)

    query = f"""
        SELECT
            c.subcategory,
            cv.canonical_text,
            c.confidence,
            cv.iso_week_first_seen AS iso_week,
            cv.customer_tier,
            cv.mrr_usd
        FROM canonical_vocs cv
        JOIN classifications c ON cv.voc_id = c.voc_id
        WHERE c.category = 'data_quality'
          AND c.subcategory IS NOT NULL
          {tier_clause}
        ORDER BY cv.first_seen_utc DESC, c.confidence DESC
        LIMIT {n * 5}
    """
    rows = conn.execute(query).fetchall()

    # Group by subcategory, take top N per subcategory
    by_subcat: dict[str, list] = {}
    for row in rows:
        subcat = row["subcategory"]
        if subcat not in by_subcat:
            by_subcat[subcat] = []
        if len(by_subcat[subcat]) < n:
            by_subcat[subcat].append({
                "subcategory": subcat,
                "text": row["canonical_text"][:300],
                "confidence": row["confidence"],
                "iso_week": row["iso_week"],
                "tier": row["customer_tier"],
                "mrr": row["mrr_usd"],
            })

    # Flatten
    return [ex for examples in by_subcat.values() for ex in examples]
