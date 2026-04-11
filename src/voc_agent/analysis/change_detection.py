"""Change detection: pre/post Aug 1, 2025 comparison with statistical significance.

Computes whether each (category x segment) has improved, degraded, or held steady
since August 2025, using a two-sample approach on weekly means.
"""

from __future__ import annotations

import math
from typing import Any, Literal

import pandas as pd
import structlog

logger = structlog.get_logger()

CHANGE_DATE = "2025-W31"  # Approximate ISO week for Aug 1, 2025
SIGNIFICANCE_THRESHOLD = 0.05


def compute_change_deltas(
    snapshots_df: pd.DataFrame,
    segment: str = "all",
) -> list[dict[str, Any]]:
    """Compute pre/post Aug 2025 change deltas for each category.

    Returns list of dicts with: category, pre_mean, post_mean, direction, significant.
    """
    seg_df = snapshots_df[snapshots_df["segment"] == segment].copy()
    if seg_df.empty:
        return []

    results = []
    categories = seg_df["category"].unique()

    for category in categories:
        cat_df = seg_df[seg_df["category"] == category]
        weekly = cat_df.groupby("iso_week")["voc_count"].sum().sort_index()

        pre = weekly[weekly.index < CHANGE_DATE]
        post = weekly[weekly.index >= CHANGE_DATE]

        if len(pre) < 4 or len(post) < 4:
            continue

        pre_mean = float(pre.mean())
        post_mean = float(post.mean())
        pre_std = float(pre.std()) if len(pre) > 1 else 0.0
        post_std = float(post.std()) if len(post) > 1 else 0.0

        # Two-sample t-test approximation
        significant = _two_sample_t_test(
            pre_mean, pre_std, len(pre),
            post_mean, post_std, len(post),
        )

        direction = _classify_direction(pre_mean, post_mean, significant)

        results.append({
            "category": category,
            "segment": segment,
            "pre_mean_weekly": round(pre_mean, 1),
            "post_mean_weekly": round(post_mean, 1),
            "pct_change": round(_safe_pct(pre_mean, post_mean), 1),
            "direction": direction,
            "significant": significant,
            "pre_weeks": len(pre),
            "post_weeks": len(post),
        })

    return results


def _classify_direction(
    pre_mean: float, post_mean: float, significant: bool
) -> Literal["improved", "degraded", "steady"]:
    """Classify the direction of change.

    For negative categories (bugs, data quality), a decrease is 'improved'.
    """
    if not significant:
        return "steady"
    if post_mean < pre_mean:
        return "improved"  # Fewer complaints = improvement
    return "degraded"  # More complaints = degradation


def _two_sample_t_test(
    mean1: float, std1: float, n1: int,
    mean2: float, std2: float, n2: int,
) -> bool:
    """Approximate two-sample t-test. Returns True if p < 0.05."""
    if n1 < 2 or n2 < 2:
        return False

    # Pooled standard error
    se = math.sqrt((std1**2 / n1) + (std2**2 / n2))
    if se == 0:
        return False

    t_stat = abs(mean2 - mean1) / se

    # Welch's degrees of freedom (simplified)
    df = n1 + n2 - 2

    # Approximate critical value for p < 0.05 (two-tailed)
    # For df >= 30, t_critical ~ 2.0; for smaller df, slightly higher
    t_critical = 2.0 if df >= 30 else 2.1 if df >= 10 else 2.3

    return t_stat > t_critical


def _safe_pct(old: float, new: float) -> float:
    if old == 0:
        return 100.0 if new > 0 else 0.0
    return ((new - old) / abs(old)) * 100
