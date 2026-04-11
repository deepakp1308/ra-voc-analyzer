"""Text normalization for deduplication.

Uses clean-text + ftfy for robust preprocessing of Slack messages.
Normalization is the foundation of hash-based dedup — identical normalized
text produces identical content hashes.
"""

from __future__ import annotations

import hashlib
import re

import ftfy
from cleantext import clean


def normalize_text(text: str) -> str:
    """Normalize feedback text for dedup comparison.

    Steps:
    1. Fix Unicode encoding issues (ftfy)
    2. Strip Slack-specific formatting (mentions, channels, links, emoji)
    3. Clean text (lowercase, collapse whitespace, strip punctuation artifacts)
    4. Remove non-content noise
    """
    if not text:
        return ""

    # Fix mojibake and encoding issues
    text = ftfy.fix_text(text)

    # Strip Slack user mentions: <@U12345|username> or <@U12345>
    text = re.sub(r"<@[A-Z0-9]+(?:\|[^>]+)?>", "", text)

    # Strip Slack channel mentions: <#C12345|channel-name> or <#C12345>
    text = re.sub(r"<#[A-Z0-9]+(?:\|[^>]+)?>", "", text)

    # Strip Slack links: <url|display> → keep display text, or <url> → remove
    text = re.sub(r"<(https?://[^|>]+)\|([^>]+)>", r"\2", text)
    text = re.sub(r"<https?://[^>]+>", "", text)

    # Strip Slack emoji shortcodes: :emoji_name:
    text = re.sub(r":[a-zA-Z0-9_+-]+:", "", text)

    # Strip Unicode emoji
    text = re.sub(
        r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF"
        r"\U0001F1E0-\U0001F1FF\U00002702-\U000027B0\U0001FA00-\U0001FA6F"
        r"\U0001FA70-\U0001FAFF\U00002600-\U000026FF\U0000FE00-\U0000FE0F"
        r"\U0000200D]+",
        "",
        text,
    )

    # Strip Slack formatting markers and backslashes: *, _, ~, \
    text = re.sub(r"[*_~`\\]", "", text)

    # Use clean-text for comprehensive cleanup
    text = clean(
        text,
        lower=True,
        no_urls=True,
        no_emails=True,
        no_phone_numbers=True,
        no_currency_symbols=True,
        replace_with_url="",
        replace_with_email="",
        replace_with_phone_number="",
        replace_with_currency_symbol="",
    )

    # Second ftfy pass to fix any encoding artifacts introduced by clean-text
    text = ftfy.fix_text(text)

    # Collapse multiple whitespace/newlines into single space
    text = re.sub(r"\s+", " ", text).strip()

    return text


def content_hash(text: str) -> str:
    """Compute sha256 hash of normalized text for dedup.

    Returns first 32 chars of the hex digest (128 bits — collision-safe
    for the expected dataset size of <100K records).
    """
    normalized = normalize_text(text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:32]
