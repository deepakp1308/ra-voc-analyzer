"""Hash-based deduplication (Pass 1).

Collapses raw messages with identical content hashes into a single
canonical VOC. Prefers Channel 2 (HVC) as the canonical source when
duplicates span channels, because it has richer metadata.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

import structlog

from voc_agent.dedup.normalize import content_hash

logger = structlog.get_logger()

# Channel priority for canonical selection (higher = preferred)
CHANNEL_PRIORITY = {
    "C051Y4H98VB": 3,  # #hvc_feedback — richest metadata
    "C095FJ3SQF4": 2,  # #mc-hvc-escalations — structured escalations
    "C06SW7512P2": 1,  # #mc-reporting-analytics-feedback — primary volume
}


def deduplicate_messages(
    raw_messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Deduplicate raw messages by content hash.

    Args:
        raw_messages: List of parsed raw message dicts. Each must have:
            - 'id': unique message ID (channel_id:ts)
            - 'channel_id': source channel
            - 'parsed_feedback': the feedback text to hash
            - 'parsed_user_id': customer user ID (optional)
            - 'ts': Slack timestamp
            - 'posted_at_utc': ISO timestamp
            - 'iso_week': ISO week string

    Returns:
        List of canonical VOC dicts ready for insertion into canonical_vocs table.
    """
    # Group by content hash
    hash_groups: dict[str, list[dict[str, Any]]] = {}

    for msg in raw_messages:
        feedback = msg.get("parsed_feedback") or ""
        if not feedback.strip():
            continue

        h = content_hash(feedback)
        if h not in hash_groups:
            hash_groups[h] = []
        hash_groups[h].append(msg)

    # For each group, pick the canonical record and collapse duplicates
    canonical_vocs = []
    total_dupes = 0

    for h, group in hash_groups.items():
        # Sort by channel priority (highest first), then by timestamp (earliest first)
        group.sort(
            key=lambda m: (
                -CHANNEL_PRIORITY.get(m["channel_id"], 0),
                m["ts"],
            )
        )

        canonical = group[0]  # Best source, earliest post
        dup_count = len(group)
        if dup_count > 1:
            total_dupes += dup_count - 1

        # Collect all source message IDs
        source_ids = [m["id"] for m in group]

        # Determine timestamps
        timestamps = [m["posted_at_utc"] for m in group]
        first_seen = min(timestamps)
        last_seen = max(timestamps)

        canonical_vocs.append({
            "voc_id": str(uuid.uuid4()),
            "canonical_text": canonical.get("parsed_feedback", ""),
            "content_hash": h,
            "first_seen_utc": first_seen,
            "last_seen_utc": last_seen,
            "iso_week_first_seen": canonical.get("iso_week", ""),
            "source_message_ids": json.dumps(source_ids),
            "dup_count": dup_count,
            "customer_id": canonical.get("parsed_user_id"),
            "mrr_usd": canonical.get("parsed_mrr"),
            # Tier computed later by enrichment layer
            "customer_tier": "unknown",
            "enrichment_source": "none",
        })

    logger.info(
        "dedup_complete",
        raw_count=len(raw_messages),
        canonical_count=len(canonical_vocs),
        duplicates_removed=total_dupes,
        dedup_rate=f"{total_dupes / max(len(raw_messages), 1):.1%}",
    )

    return canonical_vocs
