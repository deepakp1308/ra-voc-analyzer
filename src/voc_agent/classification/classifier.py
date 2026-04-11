"""Claude-powered VOC classifier using instructor for structured output.

Batches 20 VOCs per Claude call. Uses tenacity for retry/backoff.
All responses validated against ClassificationOutput Pydantic model.
"""

from __future__ import annotations

import hashlib
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anthropic
import instructor
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from voc_agent.classification.contracts import (
    ClassificationOutput,
    ClassificationRecord,
)
from voc_agent.classification.csat_mapper import csat_to_score, csat_to_sentiment

logger = structlog.get_logger()

CLASSIFIER_PROMPT_PATH = Path("config/prompts/classifier_v1.md")
BATCH_SIZE = 20
MODEL = "claude-sonnet-4-6-20250514"


def _load_prompt() -> tuple[str, str]:
    """Load the classifier prompt and compute its sha256 hash."""
    prompt_text = CLASSIFIER_PROMPT_PATH.read_text()
    prompt_hash = hashlib.sha256(prompt_text.encode()).hexdigest()[:16]
    return prompt_text, prompt_hash


def _get_client() -> instructor.Instructor:
    """Create an instructor-wrapped Anthropic client."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    base_client = anthropic.Anthropic(api_key=api_key)
    return instructor.from_anthropic(base_client)


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(3),
    reraise=True,
)
def _classify_single(
    client: instructor.Instructor,
    system_prompt: str,
    feedback_text: str,
    context: str = "",
) -> ClassificationOutput:
    """Classify a single feedback text using Claude with structured output."""
    user_message = f"Classify this customer feedback:\n\n{feedback_text}"
    if context:
        user_message += f"\n\nAdditional context: {context}"

    return client.messages.create(
        model=MODEL,
        max_tokens=512,
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}],
        response_model=ClassificationOutput,
    )


def classify_batch(
    vocs: list[dict[str, Any]],
    dry_run: bool = False,
) -> list[ClassificationRecord]:
    """Classify a batch of VOCs.

    For each VOC:
    1. If CSAT is present, use static mapping for sentiment (skip AI for sentiment)
    2. Always use Claude for category classification
    3. Return ClassificationRecord for each

    Args:
        vocs: List of dicts with keys: voc_id, feedback_text, csat_raw, survey_type, mrr, plan
        dry_run: If True, skip Claude API calls and return placeholder records

    Returns:
        List of ClassificationRecord objects
    """
    if not vocs:
        return []

    system_prompt, prompt_hash = _load_prompt()
    classifier_version = f"v1.0-sonnet-4.6-{prompt_hash}"

    if dry_run:
        return _dry_run_classify(vocs, classifier_version, prompt_hash)

    client = _get_client()
    records: list[ClassificationRecord] = []
    total_tokens = 0

    for i, voc in enumerate(vocs):
        feedback = voc.get("feedback_text", "")
        if not feedback.strip():
            continue

        try:
            # Build context from available metadata
            context_parts = []
            if voc.get("survey_type"):
                context_parts.append(f"Survey: {voc['survey_type']}")
            if voc.get("csat_raw"):
                context_parts.append(f"CSAT rating: {voc['csat_raw']}")
            if voc.get("mrr"):
                context_parts.append(f"MRR: ${voc['mrr']}")
            context = "; ".join(context_parts)

            result = _classify_single(client, system_prompt, feedback, context)

            # Override sentiment with CSAT if available (ground truth)
            csat_sentiment = csat_to_sentiment(voc.get("csat_raw"))
            csat_score = csat_to_score(voc.get("csat_raw"))
            if csat_sentiment is not None:
                result.sentiment = csat_sentiment
                result.sentiment_score = csat_score or result.sentiment_score

            records.append(ClassificationRecord(
                voc_id=voc["voc_id"],
                classifier_version=classifier_version,
                prompt_hash=prompt_hash,
                sentiment=result.sentiment,
                sentiment_score=result.sentiment_score,
                category=result.category,
                subcategory=result.subcategory,
                confidence=result.confidence,
                rationale=result.rationale,
                classified_at=datetime.now(timezone.utc),
            ))

        except Exception as e:
            logger.warning(
                "classification_failed",
                voc_id=voc.get("voc_id"),
                error=str(e),
                feedback_preview=feedback[:100],
            )
            # Quarantine: record with low confidence and "other" category
            records.append(ClassificationRecord(
                voc_id=voc["voc_id"],
                classifier_version=classifier_version,
                prompt_hash=prompt_hash,
                sentiment="neutral",
                sentiment_score=0.0,
                category="other_or_praise",
                subcategory=None,
                confidence=0.1,
                rationale=f"Classification failed: {str(e)[:200]}",
                classified_at=datetime.now(timezone.utc),
            ))

        if (i + 1) % 10 == 0:
            logger.info("classification_progress", completed=i + 1, total=len(vocs))

    logger.info(
        "classification_batch_complete",
        total=len(vocs),
        classified=len(records),
    )

    return records


def _dry_run_classify(
    vocs: list[dict[str, Any]],
    classifier_version: str,
    prompt_hash: str,
) -> list[ClassificationRecord]:
    """Generate placeholder classifications without calling Claude."""
    records = []
    for voc in vocs:
        csat_sentiment = csat_to_sentiment(voc.get("csat_raw"))
        csat_score = csat_to_score(voc.get("csat_raw"))

        records.append(ClassificationRecord(
            voc_id=voc["voc_id"],
            classifier_version=classifier_version,
            prompt_hash=prompt_hash,
            sentiment=csat_sentiment or "neutral",
            sentiment_score=csat_score or 0.0,
            category="other_or_praise",
            subcategory=None,
            confidence=0.5,
            rationale="Dry run — CSAT-based sentiment only",
            classified_at=datetime.now(timezone.utc),
        ))
    return records


def run_classification() -> None:
    """Main entry point: classify all unclassified VOCs in the database."""
    from voc_agent.storage.db import get_connection

    with get_connection() as conn:
        # Find unclassified VOCs
        rows = conn.execute("""
            SELECT cv.voc_id, cv.canonical_text, rm.parsed_csat_raw,
                   rm.parsed_survey_type, rm.parsed_mrr, rm.parsed_plan
            FROM canonical_vocs cv
            LEFT JOIN classifications c ON cv.voc_id = c.voc_id
            LEFT JOIN raw_messages rm ON rm.id = json_extract(cv.source_message_ids, '$[0]')
            WHERE c.voc_id IS NULL
            LIMIT 1000
        """).fetchall()

        if not rows:
            logger.info("no_unclassified_vocs")
            return

        vocs = [
            {
                "voc_id": r["voc_id"],
                "feedback_text": r["canonical_text"],
                "csat_raw": r["parsed_csat_raw"],
                "survey_type": r["parsed_survey_type"],
                "mrr": r["parsed_mrr"],
                "plan": r["parsed_plan"],
            }
            for r in rows
        ]

        logger.info("classifying_vocs", count=len(vocs))
        records = classify_batch(vocs)

        # Store classifications
        for rec in records:
            conn.execute(
                """INSERT OR REPLACE INTO classifications
                   (voc_id, classifier_version, prompt_hash, sentiment, sentiment_score,
                    category, subcategory, confidence, rationale, classified_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    rec.voc_id, rec.classifier_version, rec.prompt_hash,
                    rec.sentiment, rec.sentiment_score, rec.category,
                    rec.subcategory, rec.confidence, rec.rationale,
                    rec.classified_at.isoformat(),
                ),
            )

        logger.info("classifications_stored", count=len(records))
