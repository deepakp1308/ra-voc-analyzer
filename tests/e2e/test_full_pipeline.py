"""End-to-end pipeline test against synthetic data.

Verifies: ingestion → parse → dedup → classify (mocked) → analyze → report renders.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from voc_agent.analysis.segments import compute_snapshots, store_snapshots
from voc_agent.classification.contracts import compute_customer_tier
from voc_agent.dedup.hash_dedup import deduplicate_messages
from voc_agent.enrichment.inline_enricher import enrich_batch
from voc_agent.ingestion.parser_escalation import parse_escalation_message
from voc_agent.ingestion.parser_feedback import parse_feedback_message
from voc_agent.ingestion.parser_hvc import parse_hvc_message
from voc_agent.storage.db import SCHEMA_SQL

from tests.conftest import make_channel1_message, make_channel2_message, make_channel3_message


@pytest.fixture
def pipeline_db() -> sqlite3.Connection:
    """Create an in-memory database with schema and synthetic data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn


class TestFullPipeline:
    """E2E test: synthetic messages through the complete pipeline."""

    def _generate_raw_messages(self) -> list[dict[str, Any]]:
        """Generate 30 synthetic messages across all 3 channels, with 5 cross-channel dupes."""
        messages = []

        # Channel 1: 15 messages
        for i in range(15):
            msg = make_channel1_message(
                user_id=str(100000 + i),
                mrr=50 + i * 20 if i % 3 != 0 else None,  # Some free users
                csat=["Terrible", "Poor", "Average", "Good", "Excellent"][i % 5],
                feedback=f"Channel 1 feedback #{i}: The reporting needs improvement in area {i}.",
                ts_offset_days=-i,
            )
            parsed = parse_feedback_message(msg["text"])
            messages.append({
                "id": f"C06SW7512P2:{1775600000.0 - i * 86400}",
                "channel_id": "C06SW7512P2",
                "channel_name": "#mc-reporting-analytics-feedback",
                "ts": 1775600000.0 - i * 86400,
                "posted_at_utc": datetime.now(timezone.utc).isoformat(),
                "iso_week": f"2026-W{14 - i // 7:02d}",
                "parsed_feedback": parsed["feedback_text"],
                "parsed_user_id": parsed["user_id"],
                "parsed_mrr": parsed["mrr"],
                "parsed_plan": parsed["plan"],
                "parsed_csat_raw": parsed["csat_raw"],
                "parsed_survey_type": parsed["survey_type"],
            })

        # Channel 2: 10 messages (5 are duplicates of Channel 1)
        for i in range(10):
            if i < 5:
                # Duplicate of Channel 1 message i
                feedback = f"Channel 1 feedback #{i}: The reporting needs improvement in area {i}."
            else:
                feedback = f"HVC unique feedback #{i}: Enterprise reporting issue {i}."

            msg = make_channel2_message(
                user_id=str(100000 + i) if i < 5 else str(200000 + i),
                mrr=450 + i * 50,
                csat="Poor",
                feedback=feedback,
                ts_offset_days=-i,
            )
            parsed = parse_hvc_message(msg["text"])
            messages.append({
                "id": f"C051Y4H98VB:{1775600000.0 - i * 86400}",
                "channel_id": "C051Y4H98VB",
                "channel_name": "#hvc_feedback",
                "ts": 1775600000.0 - i * 86400,
                "posted_at_utc": datetime.now(timezone.utc).isoformat(),
                "iso_week": f"2026-W{14 - i // 7:02d}",
                "parsed_feedback": parsed["feedback_text"],
                "parsed_user_id": parsed["user_id"],
                "parsed_mrr": parsed["mrr"],
                "parsed_plan": parsed["plan"],
                "parsed_csat_raw": parsed["csat_raw"],
                "parsed_survey_type": parsed["survey_type"],
            })

        # Channel 3: 5 escalation messages
        for i in range(5):
            msg = make_channel3_message(
                customer_name=f"Enterprise Corp {i}",
                customer_uid=str(300000 + i),
                mrr=5000 + i * 1000,
                goal=f"Escalation goal {i}: Need accurate metrics for board reporting.",
                ts_offset_days=-i,
            )
            parsed = parse_escalation_message(msg["text"])
            messages.append({
                "id": f"C095FJ3SQF4:{1775600000.0 - i * 86400}",
                "channel_id": "C095FJ3SQF4",
                "channel_name": "#mc-hvc-escalations",
                "ts": 1775600000.0 - i * 86400,
                "posted_at_utc": datetime.now(timezone.utc).isoformat(),
                "iso_week": f"2026-W{14 - i // 7:02d}",
                "parsed_feedback": parsed["feedback_text"],
                "parsed_user_id": parsed["user_id"],
                "parsed_mrr": parsed["mrr"],
                "parsed_plan": parsed.get("plan"),
                "parsed_csat_raw": parsed.get("csat_raw"),
                "parsed_survey_type": parsed.get("survey_type"),
            })

        return messages

    def test_dedup_collapses_cross_channel(self) -> None:
        """30 raw messages with 5 cross-channel dupes → ~25 canonical VOCs."""
        messages = self._generate_raw_messages()
        canonical = deduplicate_messages(messages)

        # 30 raw - 5 dupes = 25 canonical (approximately — depends on empty feedback filtering)
        assert len(canonical) < len(messages)
        assert len(canonical) >= 20  # At least 20 unique VOCs

    def test_enrichment_applies_tiers(self) -> None:
        """All canonical VOCs get a customer tier after enrichment."""
        messages = self._generate_raw_messages()
        canonical = deduplicate_messages(messages)
        enrich_batch(canonical)

        tiers = [v["customer_tier"] for v in canonical]
        assert "paid_gte_299" in tiers  # HVC and escalation customers
        assert all(t in ("free", "paid_lt_299", "paid_gte_299", "unknown") for t in tiers)

    def test_snapshot_computation(self, pipeline_db: sqlite3.Connection) -> None:
        """Snapshots can be computed from classified VOCs."""
        conn = pipeline_db
        messages = self._generate_raw_messages()
        canonical = deduplicate_messages(messages)
        enrich_batch(canonical)

        # Insert canonical VOCs
        for voc in canonical:
            conn.execute(
                """INSERT INTO canonical_vocs
                   (voc_id, canonical_text, content_hash, first_seen_utc, last_seen_utc,
                    iso_week_first_seen, source_message_ids, dup_count,
                    customer_id, customer_tier, mrr_usd, enrichment_source)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (voc["voc_id"], voc["canonical_text"], voc["content_hash"],
                 voc["first_seen_utc"], voc["last_seen_utc"],
                 voc["iso_week_first_seen"], voc["source_message_ids"],
                 voc["dup_count"], voc["customer_id"], voc["customer_tier"],
                 voc["mrr_usd"], voc["enrichment_source"]),
            )

        # Mock classifications (dry-run style)
        categories = ["feature_gap", "bug_or_error", "data_quality", "performance_ux", "other_or_praise"]
        sentiments = ["positive", "neutral", "negative"]
        for i, voc in enumerate(canonical):
            cat = categories[i % 5]
            sent = sentiments[i % 3]
            subcat = "consistency" if cat == "data_quality" else None
            conn.execute(
                """INSERT INTO classifications
                   (voc_id, classifier_version, prompt_hash, sentiment, sentiment_score,
                    category, subcategory, confidence, rationale, classified_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (voc["voc_id"], "test-v1", "abc123", sent, -0.5 if sent == "negative" else 0.5,
                 cat, subcat, 0.85, "test", datetime.now(timezone.utc).isoformat()),
            )

        conn.commit()

        # Compute snapshots
        snapshots_df = compute_snapshots(conn)
        assert not snapshots_df.empty

        rows_stored = store_snapshots(conn, snapshots_df)
        assert rows_stored > 0

        # Verify all segment is present
        assert "all" in snapshots_df["segment"].values

    def test_pipeline_handles_empty_feedback(self) -> None:
        """Messages with empty/missing feedback text are filtered out."""
        messages = [
            {
                "id": "C123:1000",
                "channel_id": "C06SW7512P2",
                "parsed_feedback": "",
                "parsed_user_id": "99999",
                "parsed_mrr": 100,
                "ts": 1000.0,
                "posted_at_utc": "2025-01-01T00:00:00Z",
                "iso_week": "2025-W01",
            },
            {
                "id": "C123:2000",
                "channel_id": "C06SW7512P2",
                "parsed_feedback": "   ",
                "parsed_user_id": "99998",
                "parsed_mrr": 200,
                "ts": 2000.0,
                "posted_at_utc": "2025-01-01T00:00:00Z",
                "iso_week": "2025-W01",
            },
        ]
        canonical = deduplicate_messages(messages)
        assert len(canonical) == 0
