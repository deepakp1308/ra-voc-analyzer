"""Inline MRR enrichment — parses MRR directly from Slack message fields.

This is the primary enrichment source. All 3 channels embed MRR in the message text.
Covers ~90%+ of records. CSV fallback handles the rest.
"""

from __future__ import annotations

from typing import Any

from voc_agent.classification.contracts import CustomerTier, compute_customer_tier


def enrich_from_inline(voc: dict[str, Any]) -> dict[str, Any]:
    """Enrich a canonical VOC with customer tier from inline MRR.

    Updates the voc dict in place and returns it.
    """
    mrr = voc.get("mrr_usd")

    if mrr is not None:
        voc["customer_tier"] = compute_customer_tier(mrr)
        voc["enrichment_source"] = "inline"
    else:
        voc["customer_tier"] = "unknown"
        voc["enrichment_source"] = "none"

    return voc


def enrich_batch(vocs: list[dict[str, Any]]) -> dict[str, int]:
    """Enrich a batch of canonical VOCs and return tier counts.

    Returns:
        Dict with counts per tier: {free: N, paid_lt_299: N, paid_gte_299: N, unknown: N}
    """
    counts: dict[str, int] = {
        "free": 0,
        "paid_lt_299": 0,
        "paid_gte_299": 0,
        "unknown": 0,
    }

    for voc in vocs:
        enrich_from_inline(voc)
        tier = voc["customer_tier"]
        counts[tier] = counts.get(tier, 0) + 1

    return counts
