"""Slack API client for VOC ingestion.

Handles paginated conversations.history with rate limiting via tenacity.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

import structlog
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

logger = structlog.get_logger()


def _is_rate_limited(exc: BaseException) -> bool:
    """Check if exception is a Slack rate limit error."""
    return isinstance(exc, SlackApiError) and exc.response.status_code == 429


@retry(
    retry=retry_if_exception(_is_rate_limited),
    wait=wait_exponential(multiplier=2, min=5, max=120),
    stop=stop_after_attempt(5),
    reraise=True,
)
def fetch_channel_history(
    channel_id: str,
    oldest: float | None = None,
    latest: float | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch all messages from a Slack channel with pagination.

    Args:
        channel_id: Slack channel ID
        oldest: Unix timestamp for oldest message (inclusive)
        latest: Unix timestamp for latest message (inclusive)
        limit: Max total messages to fetch (None = all)

    Returns:
        List of message dicts from Slack API
    """
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        logger.error("missing_slack_bot_token")
        return []

    client = WebClient(token=token)
    messages: list[dict[str, Any]] = []
    cursor = None
    page = 0

    while True:
        kwargs: dict[str, Any] = {
            "channel": channel_id,
            "limit": 200,  # Slack max per request
        }
        if oldest is not None:
            kwargs["oldest"] = str(oldest)
        if latest is not None:
            kwargs["latest"] = str(latest)
        if cursor:
            kwargs["cursor"] = cursor

        response = client.conversations_history(**kwargs)
        batch = response.get("messages", [])
        messages.extend(batch)
        page += 1

        logger.debug(
            "slack_page_fetched",
            channel=channel_id,
            page=page,
            batch_size=len(batch),
            total=len(messages),
        )

        if limit and len(messages) >= limit:
            messages = messages[:limit]
            break

        cursor = response.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

        # Rate limit: 1 second between pages
        time.sleep(1.0)

    logger.info(
        "channel_history_fetched",
        channel=channel_id,
        total_messages=len(messages),
        pages=page,
    )

    return messages


def ts_to_iso_week(ts: float) -> str:
    """Convert a Slack timestamp to ISO week string (e.g., '2025-W03')."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    iso_cal = dt.isocalendar()
    return f"{iso_cal[0]}-W{iso_cal[1]:02d}"


def ts_to_utc_iso(ts: float) -> str:
    """Convert a Slack timestamp to UTC ISO string."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
