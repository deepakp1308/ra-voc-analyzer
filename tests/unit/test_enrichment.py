"""Unit tests for inline MRR enrichment and tier bucketing."""

import pytest

from voc_agent.enrichment.inline_enricher import enrich_batch, enrich_from_inline


class TestEnrichFromInline:
    def test_paid_gte_299(self) -> None:
        voc = {"mrr_usd": 450.0, "customer_tier": "unknown", "enrichment_source": "none"}
        enrich_from_inline(voc)
        assert voc["customer_tier"] == "paid_gte_299"
        assert voc["enrichment_source"] == "inline"

    def test_paid_lt_299(self) -> None:
        voc = {"mrr_usd": 132.0, "customer_tier": "unknown", "enrichment_source": "none"}
        enrich_from_inline(voc)
        assert voc["customer_tier"] == "paid_lt_299"

    def test_free(self) -> None:
        voc = {"mrr_usd": 0, "customer_tier": "unknown", "enrichment_source": "none"}
        enrich_from_inline(voc)
        assert voc["customer_tier"] == "free"

    def test_none_mrr_is_unknown(self) -> None:
        voc = {"mrr_usd": None, "customer_tier": "unknown", "enrichment_source": "none"}
        enrich_from_inline(voc)
        assert voc["customer_tier"] == "unknown"
        assert voc["enrichment_source"] == "none"

    def test_boundary_298_99(self) -> None:
        voc = {"mrr_usd": 298.99, "customer_tier": "unknown", "enrichment_source": "none"}
        enrich_from_inline(voc)
        assert voc["customer_tier"] == "paid_lt_299"

    def test_boundary_299_00(self) -> None:
        voc = {"mrr_usd": 299.00, "customer_tier": "unknown", "enrichment_source": "none"}
        enrich_from_inline(voc)
        assert voc["customer_tier"] == "paid_gte_299"

    def test_boundary_299_01(self) -> None:
        voc = {"mrr_usd": 299.01, "customer_tier": "unknown", "enrichment_source": "none"}
        enrich_from_inline(voc)
        assert voc["customer_tier"] == "paid_gte_299"

    def test_high_mrr_enterprise(self) -> None:
        voc = {"mrr_usd": 57000.0, "customer_tier": "unknown", "enrichment_source": "none"}
        enrich_from_inline(voc)
        assert voc["customer_tier"] == "paid_gte_299"

    def test_one_cent(self) -> None:
        voc = {"mrr_usd": 0.01, "customer_tier": "unknown", "enrichment_source": "none"}
        enrich_from_inline(voc)
        assert voc["customer_tier"] == "paid_lt_299"


class TestEnrichBatch:
    def test_batch_counts(self) -> None:
        vocs = [
            {"mrr_usd": 450.0, "customer_tier": "unknown", "enrichment_source": "none"},
            {"mrr_usd": 132.0, "customer_tier": "unknown", "enrichment_source": "none"},
            {"mrr_usd": None, "customer_tier": "unknown", "enrichment_source": "none"},
            {"mrr_usd": 0, "customer_tier": "unknown", "enrichment_source": "none"},
            {"mrr_usd": 299.0, "customer_tier": "unknown", "enrichment_source": "none"},
        ]
        counts = enrich_batch(vocs)
        assert counts["paid_gte_299"] == 2  # 450 and 299
        assert counts["paid_lt_299"] == 1  # 132
        assert counts["unknown"] == 1      # None
        assert counts["free"] == 1         # 0

    def test_empty_batch(self) -> None:
        counts = enrich_batch([])
        assert all(v == 0 for v in counts.values())
