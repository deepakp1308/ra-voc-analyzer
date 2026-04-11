"""Unit tests for CSAT → sentiment mapping with i18n support."""

import pytest

from voc_agent.classification.csat_mapper import (
    csat_to_score,
    csat_to_sentiment,
    normalize_csat,
)


class TestNormalizeCsat:
    """Tests for CSAT normalization to English."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Terrible", "terrible"),
            ("Poor", "poor"),
            ("Average", "average"),
            ("Good", "good"),
            ("Excellent", "excellent"),
            ("TERRIBLE", "terrible"),
            ("  Good  ", "good"),
        ],
    )
    def test_english_values(self, raw: str, expected: str) -> None:
        assert normalize_csat(raw) == expected

    @pytest.mark.parametrize(
        "raw,expected",
        [
            # Spanish
            ("Horrible", "terrible"),
            ("Malo", "poor"),
            ("Bueno", "good"),
            ("Excelente", "excellent"),
            ("Medianamente satisfecho", "average"),
            # German
            ("Schrecklich", "terrible"),
            ("Schlecht", "poor"),
            ("Gut", "good"),
            ("Ausgezeichnet", "excellent"),
            # French
            ("Mauvais", "poor"),
            ("Moyen", "average"),
            ("Bon", "good"),
            # Portuguese
            ("Terrível", "terrible"),
            ("Ruim", "poor"),
            ("Bom", "good"),
        ],
    )
    def test_localized_values(self, raw: str, expected: str) -> None:
        assert normalize_csat(raw) == expected

    @pytest.mark.parametrize("raw", [None, "", "  ", "Unknown", "N/A", "😀"])
    def test_unmappable_values(self, raw: str | None) -> None:
        assert normalize_csat(raw) is None


class TestCsatToSentiment:
    """Tests for CSAT → sentiment mapping."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Terrible", "negative"),
            ("Poor", "negative"),
            ("Average", "neutral"),
            ("Good", "positive"),
            ("Excellent", "positive"),
        ],
    )
    def test_english_sentiment(self, raw: str, expected: str) -> None:
        assert csat_to_sentiment(raw) == expected

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Gut", "positive"),  # German: Good
            ("Schlecht", "negative"),  # German: Poor
            ("Medianamente satisfecho", "neutral"),  # Spanish: Average
        ],
    )
    def test_localized_sentiment(self, raw: str, expected: str) -> None:
        assert csat_to_sentiment(raw) == expected

    def test_none_returns_none(self) -> None:
        assert csat_to_sentiment(None) is None

    def test_unknown_returns_none(self) -> None:
        assert csat_to_sentiment("SomeRandomValue") is None


class TestCsatToScore:
    """Tests for CSAT → numeric score mapping."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Terrible", -0.9),
            ("Poor", -0.5),
            ("Average", 0.0),
            ("Good", 0.5),
            ("Excellent", 0.9),
        ],
    )
    def test_score_mapping(self, raw: str, expected: float) -> None:
        assert csat_to_score(raw) == expected

    def test_none_returns_none(self) -> None:
        assert csat_to_score(None) is None

    def test_localized_score(self) -> None:
        assert csat_to_score("Gut") == 0.5  # German: Good → 0.5
