"""Unit tests for trend computation and change detection."""

import pytest
import pandas as pd

from voc_agent.analysis.trends import (
    compute_weekly_trends,
    compute_mom_delta,
    compute_qoq_delta,
    _safe_pct_change,
    _iso_week_to_month,
)
from voc_agent.analysis.change_detection import (
    _classify_direction,
    _two_sample_t_test,
)
from voc_agent.analysis.deep_dive import _classify_slope, _linear_slope


class TestSafePctChange:
    def test_normal_increase(self) -> None:
        assert _safe_pct_change(100, 120) == 20.0

    def test_normal_decrease(self) -> None:
        assert _safe_pct_change(100, 80) == -20.0

    def test_zero_to_positive(self) -> None:
        assert _safe_pct_change(0, 50) == 100.0

    def test_zero_to_zero(self) -> None:
        assert _safe_pct_change(0, 0) == 0.0

    def test_no_change(self) -> None:
        assert _safe_pct_change(100, 100) == 0.0


class TestIsoWeekToMonth:
    def test_week_1(self) -> None:
        assert _iso_week_to_month("2025-W01") == "2025-01"

    def test_week_10(self) -> None:
        assert _iso_week_to_month("2025-W10") == "2025-03"

    def test_week_52(self) -> None:
        assert _iso_week_to_month("2025-W52") == "2025-12"  # or close to it


class TestWeeklyTrends:
    def _make_snapshots(self, n_weeks: int = 10) -> pd.DataFrame:
        rows = []
        for i in range(n_weeks):
            week = f"2025-W{i+1:02d}"
            for sentiment in ["positive", "neutral", "negative"]:
                rows.append({
                    "iso_week": week,
                    "segment": "all",
                    "category": "data_quality",
                    "sentiment": sentiment,
                    "voc_count": 10 + i,
                    "unique_customers": 8,
                    "mean_confidence": 0.85,
                })
        return pd.DataFrame(rows)

    def test_returns_dataframe(self) -> None:
        df = self._make_snapshots()
        result = compute_weekly_trends(df, "all")
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_has_total_column(self) -> None:
        df = self._make_snapshots()
        result = compute_weekly_trends(df, "all")
        assert "total" in result.columns

    def test_has_moving_average(self) -> None:
        df = self._make_snapshots()
        result = compute_weekly_trends(df, "all")
        assert "total_ma4" in result.columns

    def test_empty_segment_returns_empty(self) -> None:
        df = self._make_snapshots()
        result = compute_weekly_trends(df, "nonexistent")
        assert result.empty


class TestMomDelta:
    def _make_monthly_data(self) -> pd.DataFrame:
        rows = []
        for i in range(8):
            week = f"2025-W{i*4+1:02d}"
            rows.append({
                "iso_week": week,
                "segment": "all",
                "category": "data_quality",
                "sentiment": "negative",
                "voc_count": 50 + i * 5,
                "unique_customers": 30,
                "mean_confidence": 0.8,
            })
        return pd.DataFrame(rows)

    def test_returns_delta(self) -> None:
        df = self._make_monthly_data()
        result = compute_mom_delta(df, "all")
        assert "pct_delta" in result
        assert "absolute_delta" in result

    def test_empty_returns_empty(self) -> None:
        empty_df = pd.DataFrame(columns=["iso_week", "segment", "category", "sentiment", "voc_count"])
        result = compute_mom_delta(empty_df, "all")
        assert result == {}


class TestClassifyDirection:
    def test_significant_decrease_is_improved(self) -> None:
        assert _classify_direction(50, 30, True) == "improved"

    def test_significant_increase_is_degraded(self) -> None:
        assert _classify_direction(30, 50, True) == "degraded"

    def test_not_significant_is_steady(self) -> None:
        assert _classify_direction(50, 52, False) == "steady"


class TestLinearSlope:
    def test_increasing(self) -> None:
        from voc_agent.analysis.deep_dive import _linear_slope
        slope = _linear_slope([1, 2, 3, 4, 5])
        assert slope > 0

    def test_decreasing(self) -> None:
        from voc_agent.analysis.deep_dive import _linear_slope
        slope = _linear_slope([5, 4, 3, 2, 1])
        assert slope < 0

    def test_flat(self) -> None:
        from voc_agent.analysis.deep_dive import _linear_slope
        slope = _linear_slope([3, 3, 3, 3])
        assert slope == 0.0

    def test_single_value(self) -> None:
        from voc_agent.analysis.deep_dive import _linear_slope
        slope = _linear_slope([5])
        assert slope == 0.0


class TestClassifySlope:
    def test_reduced(self) -> None:
        assert _classify_slope(-2.0, 20.0, 10.0) == "reduced"

    def test_increased(self) -> None:
        assert _classify_slope(2.0, 10.0, 20.0) == "increased"

    def test_steady_small_change(self) -> None:
        assert _classify_slope(0.1, 10.0, 10.5) == "steady"

    def test_steady_zero_baseline(self) -> None:
        assert _classify_slope(0.0, 0.0, 0.0) == "steady"


class TestTwoSampleTTest:
    def test_clearly_different(self) -> None:
        assert _two_sample_t_test(10, 2, 30, 20, 2, 30) is True

    def test_clearly_same(self) -> None:
        assert _two_sample_t_test(10, 5, 30, 10.5, 5, 30) is False

    def test_small_samples_one_each(self) -> None:
        # With only 1 sample each, should not be significant
        assert _two_sample_t_test(10, 2, 1, 20, 2, 1) is False

    def test_zero_std(self) -> None:
        assert _two_sample_t_test(10, 0, 30, 10, 0, 30) is False
