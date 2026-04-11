"""Backfill orchestrator for historical and incremental ingestion.

- First run: full backfill from 2025-01-01
- Subsequent runs: incremental (last 14 days with 7-day overlap)
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog
import yaml

from voc_agent.dedup.hash_dedup import deduplicate_messages
from voc_agent.enrichment.inline_enricher import enrich_batch
from voc_agent.ingestion.parser_escalation import parse_escalation_message
from voc_agent.ingestion.parser_feedback import parse_feedback_message
from voc_agent.ingestion.parser_hvc import parse_hvc_message
from voc_agent.ingestion.slack_client import (
    fetch_channel_history,
    ts_to_iso_week,
    ts_to_utc_iso,
)
from voc_agent.storage.db import get_connection

logger = structlog.get_logger()

PARSERS = {
    "parser_feedback": parse_feedback_message,
    "parser_hvc": parse_hvc_message,
    "parser_escalation": parse_escalation_message,
}


def load_channel_config() -> dict[str, Any]:
    """Load channel configuration from YAML."""
    config_path = Path("config/channels.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


def run_ingestion(
    backfill_from: date | None = None,
    channel_id: str | None = None,
    limit: int | None = None,
) -> dict[str, int]:
    """Run the full ingestion pipeline.

    Args:
        backfill_from: If set, fetch from this date. Otherwise incremental (last 14 days).
        channel_id: If set, only ingest this channel. Otherwise all configured channels.
        limit: Max messages per channel (for testing).

    Returns:
        Dict with counts: {ingested: N, deduped: N, enriched: N}
    """
    config = load_channel_config()
    channels = config["channels"]

    all_raw_messages: list[dict[str, Any]] = []
    stats = {"ingested": 0, "deduped": 0, "enriched": 0}

    for ch_key, ch_config in channels.items():
        ch_id = ch_config["id"]

        if channel_id and ch_id != channel_id:
            continue

        parser_name = ch_config["parser"]
        parser_fn = PARSERS.get(parser_name)
        if not parser_fn:
            logger.error("unknown_parser", channel=ch_key, parser=parser_name)
            continue

        # Determine time range
        if backfill_from:
            oldest_ts = datetime.combine(backfill_from, datetime.min.time(), tzinfo=timezone.utc).timestamp()
        else:
            # Incremental: last 14 days
            oldest_ts = (datetime.now(timezone.utc) - timedelta(days=14)).timestamp()

        ch_start_date = ch_config.get("start_date")
        if ch_start_date:
            ch_start_ts = datetime.combine(
                date.fromisoformat(ch_start_date), datetime.min.time(), tzinfo=timezone.utc
            ).timestamp()
            oldest_ts = max(oldest_ts, ch_start_ts)

        logger.info("ingesting_channel", channel=ch_key, channel_id=ch_id, oldest_ts=oldest_ts)

        # Fetch from Slack
        messages = fetch_channel_history(ch_id, oldest=oldest_ts, limit=limit)

        # Parse each message
        for msg in messages:
            text = msg.get("text", "")
            ts = float(msg.get("ts", 0))

            parsed = parser_fn(text)

            raw_record = {
                "id": f"{ch_id}:{ts}",
                "channel_id": ch_id,
                "channel_name": ch_config["name"],
                "ts": ts,
                "posted_at_utc": ts_to_utc_iso(ts),
                "posted_at_pt": ts_to_utc_iso(ts),  # Simplified; could add PT conversion
                "iso_week": ts_to_iso_week(ts),
                "author_id": msg.get("bot_id") or msg.get("user"),
                "author_name": msg.get("username"),
                "text": text,
                "thread_ts": float(msg["thread_ts"]) if msg.get("thread_ts") else None,
                "permalink": None,
                "parsed_user_id": parsed.get("user_id"),
                "parsed_mrr": parsed.get("mrr"),
                "parsed_plan": parsed.get("plan"),
                "parsed_csat_raw": parsed.get("csat_raw"),
                "parsed_feedback": parsed.get("feedback_text"),
                "parsed_survey_type": parsed.get("survey_type"),
                "parsed_page_url": parsed.get("page_url"),
                "parsed_fullstory_url": parsed.get("fullstory_url"),
                "parsed_customer_name": parsed.get("customer_name"),
                "parsed_criticality": parsed.get("criticality"),
                "parsed_impacted_product": parsed.get("impacted_product"),
                "parsed_goal": parsed.get("goal"),
                "parsed_constraints": parsed.get("constraints"),
                "parsed_prs_score": parsed.get("prs_score"),
                "parsed_prs_reason": parsed.get("prs_reason"),
                "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            all_raw_messages.append(raw_record)

        stats["ingested"] += len(messages)
        logger.info("channel_parsed", channel=ch_key, messages=len(messages))

    # Store raw messages
    with get_connection() as conn:
        for msg in all_raw_messages:
            conn.execute(
                """INSERT OR IGNORE INTO raw_messages
                   (id, channel_id, channel_name, ts, posted_at_utc, posted_at_pt,
                    iso_week, author_id, author_name, text, thread_ts, permalink,
                    parsed_user_id, parsed_mrr, parsed_plan, parsed_csat_raw,
                    parsed_feedback, parsed_survey_type, parsed_page_url,
                    parsed_fullstory_url, parsed_customer_name, parsed_criticality,
                    parsed_impacted_product, parsed_goal, parsed_constraints,
                    parsed_prs_score, parsed_prs_reason, fetched_at_utc)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                tuple(msg.values()),
            )

        # Dedup
        canonical_vocs = deduplicate_messages(all_raw_messages)
        stats["deduped"] = len(canonical_vocs)

        # Enrich
        tier_counts = enrich_batch(canonical_vocs)
        stats["enriched"] = sum(tier_counts.values())

        # Store canonical VOCs
        for voc in canonical_vocs:
            conn.execute(
                """INSERT OR IGNORE INTO canonical_vocs
                   (voc_id, canonical_text, content_hash, first_seen_utc, last_seen_utc,
                    iso_week_first_seen, source_message_ids, dup_count,
                    customer_id, customer_tier, mrr_usd, enrichment_source)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    voc["voc_id"], voc["canonical_text"], voc["content_hash"],
                    voc["first_seen_utc"], voc["last_seen_utc"],
                    voc["iso_week_first_seen"], voc["source_message_ids"],
                    voc["dup_count"], voc["customer_id"], voc["customer_tier"],
                    voc["mrr_usd"], voc["enrichment_source"],
                ),
            )

    logger.info("ingestion_complete", **stats, tier_counts=tier_counts)
    return stats
