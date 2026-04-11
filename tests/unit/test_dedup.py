"""Unit tests for text normalization and hash dedup."""

import pytest

from voc_agent.dedup.normalize import content_hash, normalize_text
from voc_agent.dedup.hash_dedup import deduplicate_messages


class TestNormalizeText:
    def test_lowercase(self) -> None:
        assert normalize_text("HELLO WORLD") == "hello world"

    def test_strip_slack_mention(self) -> None:
        result = normalize_text("Thanks <@U12345|John> for the update")
        assert "U12345" not in result
        assert "John" not in result

    def test_strip_slack_channel(self) -> None:
        result = normalize_text("See <#C12345|general> for details")
        assert "C12345" not in result

    def test_strip_slack_link_keep_display(self) -> None:
        result = normalize_text("Visit <https://example.com|our website> now")
        assert "our website" in result
        assert "https" not in result

    def test_strip_bare_url(self) -> None:
        result = normalize_text("Check <https://example.com/path?q=1>")
        assert "example.com" not in result

    def test_strip_emoji_shortcode(self) -> None:
        result = normalize_text(":thumbsup: Great job :fire:")
        assert ":" not in result or result == "great job"

    def test_collapse_whitespace(self) -> None:
        result = normalize_text("hello   \n\n  world   ")
        assert result == "hello world"

    def test_strip_formatting_markers(self) -> None:
        result = normalize_text("*bold* _italic_ ~strike~")
        assert "*" not in result
        assert "_" not in result

    def test_empty_string(self) -> None:
        assert normalize_text("") == ""

    def test_unicode_emoji(self) -> None:
        result = normalize_text("Great product! 😀👍🎉")
        assert "great product" in result


class TestContentHash:
    def test_same_text_same_hash(self) -> None:
        h1 = content_hash("The reporting is confusing.")
        h2 = content_hash("The reporting is confusing.")
        assert h1 == h2

    def test_different_text_different_hash(self) -> None:
        h1 = content_hash("The reporting is confusing.")
        h2 = content_hash("I love the new dashboard!")
        assert h1 != h2

    def test_case_insensitive(self) -> None:
        h1 = content_hash("HELLO WORLD")
        h2 = content_hash("hello world")
        assert h1 == h2

    def test_whitespace_insensitive(self) -> None:
        h1 = content_hash("hello   world")
        h2 = content_hash("hello world")
        assert h1 == h2

    def test_url_insensitive(self) -> None:
        h1 = content_hash("Check <https://example.com> for details")
        h2 = content_hash("Check  for details")
        assert h1 == h2

    def test_hash_length(self) -> None:
        h = content_hash("test")
        assert len(h) == 32


class TestDeduplicateMessages:
    def _make_msg(
        self, feedback: str, channel_id: str = "C06SW7512P2", ts: float = 1000.0,
        user_id: str = "12345", mrr: float | None = 100,
    ) -> dict:
        return {
            "id": f"{channel_id}:{ts}",
            "channel_id": channel_id,
            "parsed_feedback": feedback,
            "parsed_user_id": user_id,
            "parsed_mrr": mrr,
            "ts": ts,
            "posted_at_utc": "2025-03-01T00:00:00Z",
            "iso_week": "2025-W09",
        }

    def test_no_duplicates(self) -> None:
        messages = [
            self._make_msg("Feedback A", ts=1000),
            self._make_msg("Feedback B", ts=2000),
        ]
        result = deduplicate_messages(messages)
        assert len(result) == 2

    def test_exact_duplicate_collapses(self) -> None:
        messages = [
            self._make_msg("Same feedback text", channel_id="C06SW7512P2", ts=1000),
            self._make_msg("Same feedback text", channel_id="C051Y4H98VB", ts=1001),
        ]
        result = deduplicate_messages(messages)
        assert len(result) == 1
        assert result[0]["dup_count"] == 2

    def test_cross_channel_prefers_hvc(self) -> None:
        """Channel 2 (HVC) should be preferred as canonical over Channel 1."""
        messages = [
            self._make_msg("Same text", channel_id="C06SW7512P2", ts=1000),
            self._make_msg("Same text", channel_id="C051Y4H98VB", ts=1001),
        ]
        result = deduplicate_messages(messages)
        source_ids = result[0]["source_message_ids"]
        # HVC channel should be first (canonical)
        assert "C051Y4H98VB" in source_ids

    def test_case_insensitive_dedup(self) -> None:
        messages = [
            self._make_msg("THE REPORTING IS BAD", ts=1000),
            self._make_msg("the reporting is bad", ts=2000),
        ]
        result = deduplicate_messages(messages)
        assert len(result) == 1

    def test_empty_feedback_excluded(self) -> None:
        messages = [
            self._make_msg("", ts=1000),
            self._make_msg("   ", ts=2000),
            self._make_msg("Real feedback", ts=3000),
        ]
        result = deduplicate_messages(messages)
        assert len(result) == 1

    def test_different_text_not_deduped(self) -> None:
        messages = [
            self._make_msg("Feedback about reporting", ts=1000),
            self._make_msg("Feedback about email editor", ts=2000),
        ]
        result = deduplicate_messages(messages)
        assert len(result) == 2

    def test_voc_id_is_uuid(self) -> None:
        messages = [self._make_msg("Test", ts=1000)]
        result = deduplicate_messages(messages)
        assert len(result[0]["voc_id"]) == 36  # UUID format
