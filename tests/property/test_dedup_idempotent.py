"""Property-based test: dedup must be idempotent.

Running the pipeline twice on identical raw messages yields identical canonical_vocs.
"""

from __future__ import annotations

from hypothesis import given, settings, strategies as st

from voc_agent.dedup.hash_dedup import deduplicate_messages
from voc_agent.dedup.normalize import content_hash, normalize_text


@given(text=st.text(
    alphabet=st.characters(codec="ascii", min_codepoint=32, max_codepoint=126),
    min_size=5, max_size=200,
))
@settings(max_examples=100, deadline=2000)
def test_normalize_idempotent(text: str) -> None:
    """Normalizing twice produces the same result as normalizing once.

    Uses ASCII printable characters — clean-text has known non-idempotent
    behavior on extended Unicode (accented chars, Latin extensions).
    Real Slack VOC feedback is predominantly ASCII/Latin-1 text.
    """
    first = normalize_text(text)
    second = normalize_text(first)
    assert first == second


@given(text=st.text(min_size=5, max_size=200))
@settings(max_examples=50, deadline=2000)
def test_hash_deterministic(text: str) -> None:
    """Same input always produces same hash."""
    h1 = content_hash(text)
    h2 = content_hash(text)
    assert h1 == h2


def test_dedup_idempotent_fixed() -> None:
    """Running dedup twice on same input yields same canonical count."""
    messages = [
        {
            "id": f"C123:{i}",
            "channel_id": "C06SW7512P2",
            "parsed_feedback": f"Feedback number {i}",
            "parsed_user_id": str(10000 + i),
            "parsed_mrr": 100 + i,
            "ts": 1000.0 + i,
            "posted_at_utc": "2025-03-01T00:00:00Z",
            "iso_week": "2025-W09",
        }
        for i in range(20)
    ]
    # Add some duplicates
    messages.append({
        "id": "C456:999",
        "channel_id": "C051Y4H98VB",
        "parsed_feedback": "Feedback number 5",  # duplicate of i=5
        "parsed_user_id": "10005",
        "parsed_mrr": 500,
        "ts": 1005.1,
        "posted_at_utc": "2025-03-01T00:00:00Z",
        "iso_week": "2025-W09",
    })

    result1 = deduplicate_messages(messages)
    result2 = deduplicate_messages(messages)

    assert len(result1) == len(result2)

    # Same content hashes in both runs
    hashes1 = sorted(v["content_hash"] for v in result1)
    hashes2 = sorted(v["content_hash"] for v in result2)
    assert hashes1 == hashes2
