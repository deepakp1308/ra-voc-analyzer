"""Unit tests for Pydantic classification contracts."""

import pytest
from pydantic import ValidationError

from voc_agent.classification.contracts import (
    ClassificationOutput,
    compute_customer_tier,
)


class TestClassificationOutput:
    """Tests for the ClassificationOutput Pydantic model."""

    def test_valid_data_quality_with_subcategory(self) -> None:
        result = ClassificationOutput(
            sentiment="negative",
            sentiment_score=-0.7,
            category="data_quality",
            subcategory="consistency",
            confidence=0.85,
            rationale="Numbers disagree between reports",
        )
        assert result.category == "data_quality"
        assert result.subcategory == "consistency"

    def test_valid_non_data_quality_without_subcategory(self) -> None:
        result = ClassificationOutput(
            sentiment="negative",
            sentiment_score=-0.8,
            category="performance_ux",
            subcategory=None,
            confidence=0.9,
            rationale="Too many clicks needed",
        )
        assert result.subcategory is None

    def test_data_quality_missing_subcategory_raises(self) -> None:
        with pytest.raises(ValidationError, match="subcategory is required"):
            ClassificationOutput(
                sentiment="negative",
                sentiment_score=-0.5,
                category="data_quality",
                subcategory=None,
                confidence=0.8,
                rationale="Data issue",
            )

    def test_non_data_quality_with_subcategory_raises(self) -> None:
        with pytest.raises(ValidationError, match="subcategory must be null"):
            ClassificationOutput(
                sentiment="negative",
                sentiment_score=-0.5,
                category="bug_or_error",
                subcategory="accuracy",
                confidence=0.8,
                rationale="Bug report",
            )

    def test_invalid_category_raises(self) -> None:
        with pytest.raises(ValidationError):
            ClassificationOutput(
                sentiment="negative",
                sentiment_score=-0.5,
                category="invented_category",
                confidence=0.8,
                rationale="Test",
            )

    def test_invalid_sentiment_raises(self) -> None:
        with pytest.raises(ValidationError):
            ClassificationOutput(
                sentiment="very_negative",
                sentiment_score=-0.5,
                category="bug_or_error",
                confidence=0.8,
                rationale="Test",
            )

    def test_score_out_of_range_raises(self) -> None:
        with pytest.raises(ValidationError):
            ClassificationOutput(
                sentiment="negative",
                sentiment_score=-1.5,
                category="bug_or_error",
                confidence=0.8,
                rationale="Test",
            )

    def test_confidence_out_of_range_raises(self) -> None:
        with pytest.raises(ValidationError):
            ClassificationOutput(
                sentiment="positive",
                sentiment_score=0.8,
                category="other_or_praise",
                confidence=1.5,
                rationale="Test",
            )

    def test_rationale_too_long_raises(self) -> None:
        with pytest.raises(ValidationError):
            ClassificationOutput(
                sentiment="neutral",
                sentiment_score=0.0,
                category="feature_gap",
                confidence=0.7,
                rationale="x" * 241,
            )

    @pytest.mark.parametrize(
        "subcategory",
        ["accuracy", "consistency", "availability", "freshness", "coverage"],
    )
    def test_all_valid_subcategories(self, subcategory: str) -> None:
        result = ClassificationOutput(
            sentiment="negative",
            sentiment_score=-0.6,
            category="data_quality",
            subcategory=subcategory,
            confidence=0.8,
            rationale="Data issue",
        )
        assert result.subcategory == subcategory

    @pytest.mark.parametrize(
        "category",
        ["feature_gap", "bug_or_error", "performance_ux", "other_or_praise"],
    )
    def test_all_non_dq_categories(self, category: str) -> None:
        result = ClassificationOutput(
            sentiment="negative",
            sentiment_score=-0.5,
            category=category,
            subcategory=None,
            confidence=0.8,
            rationale="Test",
        )
        assert result.category == category


class TestComputeCustomerTier:
    """Tests for MRR → customer tier bucketing."""

    def test_none_is_unknown(self) -> None:
        assert compute_customer_tier(None) == "unknown"

    def test_zero_is_free(self) -> None:
        assert compute_customer_tier(0) == "free"

    def test_negative_is_free(self) -> None:
        assert compute_customer_tier(-5) == "free"

    def test_one_cent_is_paid_lt_299(self) -> None:
        assert compute_customer_tier(0.01) == "paid_lt_299"

    def test_298_99_is_paid_lt_299(self) -> None:
        assert compute_customer_tier(298.99) == "paid_lt_299"

    def test_299_00_is_paid_gte_299(self) -> None:
        assert compute_customer_tier(299.00) == "paid_gte_299"

    def test_299_01_is_paid_gte_299(self) -> None:
        assert compute_customer_tier(299.01) == "paid_gte_299"

    def test_high_mrr_is_paid_gte_299(self) -> None:
        assert compute_customer_tier(57000) == "paid_gte_299"
