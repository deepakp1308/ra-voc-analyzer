"""Trend computation: Weekly, MoM, QoQ, YoY deltas.

Reuses pure-function patterns from ra-executive-dashboard/analytics.py.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import structlog

logger = structlog.get_logger()


def compute_weekly_trends(snapshots_df: pd.DataFrame, segment: str = "all") -> pd.DataFrame:
    """Compute weekly time series for a given segment.

    Returns DataFrame with columns: iso_week, category, sentiment, voc_count,
    plus rolling 4-week moving average.
    """
    seg_df = snapshots_df[snapshots_df["segment"] == segment].copy()
    if seg_df.empty:
        return pd.DataFrame()

    # Pivot to get counts per week
    weekly = (
        seg_df.groupby(["iso_week", "sentiment"])["voc_count"]
        .sum()
        .reset_index()
        .pivot(index="iso_week", columns="sentiment", values="voc_count")
        .fillna(0)
        .sort_index()
    )

    # Add total and sentiment ratio
    weekly["total"] = weekly.sum(axis=1)
    if "negative" in weekly.columns:
        weekly["negative_ratio"] = weekly["negative"] / weekly["total"].replace(0, 1)
    else:
        weekly["negative_ratio"] = 0.0

    # 4-week moving average
    weekly["total_ma4"] = weekly["total"].rolling(4, min_periods=1).mean()
    weekly["negative_ratio_ma4"] = weekly["negative_ratio"].rolling(4, min_periods=1).mean()

    return weekly.reset_index()


def compute_mom_delta(snapshots_df: pd.DataFrame, segment: str = "all") -> dict[str, Any]:
    """Compute month-over-month delta for the most recent complete month."""
    seg_df = snapshots_df[snapshots_df["segment"] == segment].copy()
    if seg_df.empty:
        return {}

    # Convert iso_week to approximate month
    seg_df["month"] = seg_df["iso_week"].apply(_iso_week_to_month)
    monthly = seg_df.groupby("month")["voc_count"].sum().sort_index()

    if len(monthly) < 2:
        return {}

    current = monthly.iloc[-1]
    prior = monthly.iloc[-2]
    delta = current - prior
    pct = _safe_pct_change(prior, current)

    return {
        "current_month": monthly.index[-1],
        "prior_month": monthly.index[-2],
        "current_count": int(current),
        "prior_count": int(prior),
        "absolute_delta": int(delta),
        "pct_delta": round(pct, 1),
    }


def compute_qoq_delta(snapshots_df: pd.DataFrame, segment: str = "all") -> dict[str, Any]:
    """Compute quarter-over-quarter delta using rolling 13-week windows."""
    seg_df = snapshots_df[snapshots_df["segment"] == segment].copy()
    if seg_df.empty:
        return {}

    weekly = seg_df.groupby("iso_week")["voc_count"].sum().sort_index()

    if len(weekly) < 26:  # Need at least 2 quarters
        return {}

    current_q = weekly.iloc[-13:].sum()
    prior_q = weekly.iloc[-26:-13].sum()
    delta = current_q - prior_q
    pct = _safe_pct_change(prior_q, current_q)

    return {
        "current_quarter_total": int(current_q),
        "prior_quarter_total": int(prior_q),
        "absolute_delta": int(delta),
        "pct_delta": round(pct, 1),
    }


def compute_yoy_delta(
    snapshots_df: pd.DataFrame, segment: str = "all", current_week: str | None = None
) -> dict[str, Any]:
    """Compute year-over-year delta for matching ISO weeks, 4-week smoothed."""
    seg_df = snapshots_df[snapshots_df["segment"] == segment].copy()
    if seg_df.empty:
        return {}

    weekly = seg_df.groupby("iso_week")["voc_count"].sum().sort_index()
    weekly_ma = weekly.rolling(4, min_periods=1).mean()

    if len(weekly) < 52:
        return {}

    current_val = weekly_ma.iloc[-1]
    # Find the same week last year (52 weeks back)
    prior_val = weekly_ma.iloc[-52] if len(weekly_ma) >= 52 else None

    if prior_val is None:
        return {}

    delta = current_val - prior_val
    pct = _safe_pct_change(prior_val, current_val)

    return {
        "current_week": weekly.index[-1],
        "prior_year_week": weekly.index[-52] if len(weekly) >= 52 else None,
        "current_smoothed": round(float(current_val), 1),
        "prior_smoothed": round(float(prior_val), 1),
        "pct_delta": round(pct, 1),
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_pct_change(old: float, new: float) -> float:
    """Compute percentage change, handling zero division."""
    if old == 0:
        return 100.0 if new > 0 else 0.0
    return ((new - old) / abs(old)) * 100


def _iso_week_to_month(iso_week: str) -> str:
    """Convert '2025-W03' to '2025-01' (approximate month)."""
    try:
        year = int(iso_week[:4])
        week = int(iso_week.split("W")[1])
        # Approximate: week 1-4 = Jan, 5-8 = Feb, etc.
        month = min(12, max(1, (week - 1) // 4 + 1))
        return f"{year}-{month:02d}"
    except (ValueError, IndexError):
        return iso_week
