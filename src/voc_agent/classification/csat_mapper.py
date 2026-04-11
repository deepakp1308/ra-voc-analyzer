"""Static CSAT → sentiment mapping with internationalization support.

CSAT ratings from Qualtrics surveys provide ground-truth sentiment for most entries.
This module handles English and localized CSAT values observed in real channel data.
"""

from __future__ import annotations

from typing import Optional

from voc_agent.classification.contracts import Sentiment

# ── English CSAT Mapping ──────────────────────────────────────────────────────

CSAT_SENTIMENT_MAP: dict[str, Sentiment] = {
    "terrible": "negative",
    "poor": "negative",
    "average": "neutral",
    "good": "positive",
    "excellent": "positive",
}

# ── Localized CSAT → English normalization ────────────────────────────────────
# Built from actual observed values in #hvc_feedback and #mc-reporting-analytics-feedback.

CSAT_LOCALIZED: dict[str, str] = {
    # Spanish
    "horrible": "terrible",
    "malo": "poor",
    "regular": "average",
    "promedio": "average",
    "medianamente satisfecho": "average",
    "bueno": "good",
    "excelente": "excellent",
    # German
    "schrecklich": "terrible",
    "schlecht": "poor",
    "durchschnittlich": "average",
    "gut": "good",
    "ausgezeichnet": "excellent",
    # French
    "terrible": "terrible",
    "mauvais": "poor",
    "moyen": "average",
    "bon": "good",
    "tres bon": "good",
    "très bon": "good",
    # Portuguese
    "terrivel": "terrible",
    "terrível": "terrible",
    "ruim": "poor",
    "medio": "average",
    "médio": "average",
    "bom": "good",
    # Italian
    "terribile": "terrible",
    "scarso": "poor",
    "nella media": "average",
    "buono": "good",
    "eccellente": "excellent",
    # Dutch
    "verschrikkelijk": "terrible",
    "slecht": "poor",
    "gemiddeld": "average",
    "goed": "good",
    "uitstekend": "excellent",
}


def normalize_csat(csat_raw: str | None) -> str | None:
    """Normalize a raw CSAT value to English (lowercase). Returns None if unmappable."""
    if not csat_raw or not csat_raw.strip():
        return None

    cleaned = csat_raw.strip().lower()

    # Direct English match
    if cleaned in CSAT_SENTIMENT_MAP:
        return cleaned

    # Localized → English
    if cleaned in CSAT_LOCALIZED:
        return CSAT_LOCALIZED[cleaned]

    return None


def csat_to_sentiment(csat_raw: str | None) -> Optional[Sentiment]:
    """Map a CSAT rating to sentiment. Returns None if CSAT is missing or unrecognized."""
    normalized = normalize_csat(csat_raw)
    if normalized is None:
        return None
    return CSAT_SENTIMENT_MAP.get(normalized)


def csat_to_score(csat_raw: str | None) -> float | None:
    """Map a CSAT rating to a numeric score (-1.0 to 1.0)."""
    normalized = normalize_csat(csat_raw)
    if normalized is None:
        return None

    score_map: dict[str, float] = {
        "terrible": -0.9,
        "poor": -0.5,
        "average": 0.0,
        "good": 0.5,
        "excellent": 0.9,
    }
    return score_map.get(normalized)
