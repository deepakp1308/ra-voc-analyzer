"""Pydantic contracts for LLM classification outputs.

Every Claude response is validated against these models.
Failed validation goes to quarantine/, never a silent drop.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator


# ── Enums ─────────────────────────────────────────────────────────────────────

Sentiment = Literal["positive", "neutral", "negative"]

Category = Literal[
    "feature_gap",
    "bug_or_error",
    "data_quality",
    "performance_ux",
    "other_or_praise",
]

DataQualitySubcategory = Literal[
    "accuracy",
    "consistency",
    "availability",
    "freshness",
    "coverage",
]


# ── Classification Output ─────────────────────────────────────────────────────

class ClassificationOutput(BaseModel):
    """Structured output from Claude classifier. Validated by instructor."""

    sentiment: Sentiment
    sentiment_score: float = Field(ge=-1.0, le=1.0)
    category: Category
    subcategory: Optional[DataQualitySubcategory] = None
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str = Field(max_length=240)

    @model_validator(mode="after")
    def validate_subcategory_rules(self) -> ClassificationOutput:
        """Enforce: subcategory required iff category == 'data_quality'."""
        if self.category == "data_quality" and self.subcategory is None:
            raise ValueError("subcategory is required when category is 'data_quality'")
        if self.category != "data_quality" and self.subcategory is not None:
            raise ValueError("subcategory must be null when category is not 'data_quality'")
        return self


class BatchClassificationInput(BaseModel):
    """A single item in a classification batch."""

    voc_id: str
    feedback_text: str
    csat_raw: Optional[str] = None
    survey_type: Optional[str] = None
    page_url: Optional[str] = None
    mrr: Optional[float] = None
    plan: Optional[str] = None


class ClassificationRecord(BaseModel):
    """Full classification record for storage."""

    voc_id: str
    classifier_version: str
    prompt_hash: str
    sentiment: Sentiment
    sentiment_score: float
    category: Category
    subcategory: Optional[DataQualitySubcategory] = None
    confidence: float
    rationale: str
    classified_at: datetime


# ── Parsed Message Models ─────────────────────────────────────────────────────

class ParsedFeedback(BaseModel):
    """Common fields extracted from any channel's VOC message."""

    channel_id: str
    channel_name: str
    message_ts: float
    posted_at_utc: datetime
    iso_week: str

    user_id: Optional[str] = None
    customer_name: Optional[str] = None
    mrr: Optional[float] = None
    plan: Optional[str] = None
    csat_raw: Optional[str] = None
    feedback_text: Optional[str] = None
    survey_type: Optional[str] = None
    page_url: Optional[str] = None
    fullstory_url: Optional[str] = None

    # Channel 3 specific
    criticality: Optional[str] = None
    impacted_product: Optional[str] = None
    goal: Optional[str] = None
    constraints: Optional[str] = None

    # Channel 2 PRS specific
    prs_score: Optional[int] = None
    prs_reason: Optional[str] = None


# ── Customer Tier ─────────────────────────────────────────────────────────────

CustomerTier = Literal["free", "paid_lt_299", "paid_gte_299", "unknown"]


def compute_customer_tier(mrr: float | None) -> CustomerTier:
    """Determine customer tier from MRR value."""
    if mrr is None:
        return "unknown"
    if mrr <= 0:
        return "free"
    if mrr >= 299.0:
        return "paid_gte_299"
    return "paid_lt_299"
