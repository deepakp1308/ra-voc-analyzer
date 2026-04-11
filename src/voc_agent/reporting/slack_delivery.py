"""Slack delivery: DM the weekly report to Deepak with Block Kit TL;DR."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import structlog
from slack_sdk import WebClient
from slackblocks import SectionBlock, HeaderBlock, DividerBlock, Message

logger = structlog.get_logger()


def deliver_report(
    tldr: dict[str, Any],
    report_paths: dict[str, Path],
    dashboard_url: str | None = None,
) -> None:
    """Send the weekly report via Slack DM.

    1. Upload PDF as file attachment
    2. Post Block Kit TL;DR summary
    """
    token = os.environ.get("SLACK_BOT_TOKEN")
    recipient = os.environ.get("REPORT_RECIPIENT_SLACK_ID")

    if not token or not recipient:
        logger.warning("slack_delivery_skipped", reason="Missing SLACK_BOT_TOKEN or REPORT_RECIPIENT_SLACK_ID")
        return

    client = WebClient(token=token)

    # Build TL;DR message
    total = tldr.get("total_vocs", 0)
    mix = tldr.get("sentiment_mix", {})
    week = tldr.get("current_week", "unknown")

    summary_text = (
        f"*R&A VOC Weekly Report — {week}*\n\n"
        f"*{total}* VOCs this week: "
        f"{mix.get('positive', 0)} positive, "
        f"{mix.get('neutral', 0)} neutral, "
        f"{mix.get('negative', 0)} negative"
    )

    if dashboard_url:
        summary_text += f"\n\n<{dashboard_url}|View full dashboard>"

    # Post the summary message
    msg_response = client.chat_postMessage(
        channel=recipient,
        text=summary_text,
        mrkdwn=True,
    )
    logger.info("slack_tldr_sent", channel=recipient, ts=msg_response.get("ts"))

    # Upload PDF if available
    pdf_path = report_paths.get("pdf")
    if pdf_path and pdf_path.exists():
        client.files_upload_v2(
            channel=recipient,
            file=str(pdf_path),
            filename=f"ra-voc-report-{week}.pdf",
            title=f"R&A VOC Report — {week}",
            initial_comment="Full PDF report attached.",
        )
        logger.info("slack_pdf_uploaded", path=str(pdf_path))


if __name__ == "__main__":
    """Standalone delivery for GitHub Actions (post-pipeline step)."""
    import json

    # Load the most recent report metadata
    docs_index = Path("docs/index.json")
    if not docs_index.exists():
        logger.error("no_archive_index")
        exit(1)

    archive = json.loads(docs_index.read_text())
    if not archive.get("weeks"):
        logger.error("no_archive_weeks")
        exit(1)

    latest_week = archive["weeks"][-1]

    # Minimal TL;DR from the report (would be passed from pipeline in production)
    tldr = {"total_vocs": 0, "sentiment_mix": {}, "current_week": latest_week}
    paths = {}

    pdf_path = Path(f"reports/{latest_week}.pdf")
    if pdf_path.exists():
        paths["pdf"] = pdf_path

    deliver_report(tldr, paths)
