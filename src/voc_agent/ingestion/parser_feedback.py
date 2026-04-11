"""Parser for Channel 1: #mc-reporting-analytics-feedback (C06SW7512P2).

Qualtrics bot posts survey responses in two formats:
- "In-App Feedback Badge" — uses `*CSAT*` (no colon)
- "In-App Survey" — uses `*CSAT:*` (with colon)

Observed message structure:
    *New Survey Response from the In-App Feedback Badge*
    *MRR:*  1440
    *Plan:* Premium plan
    *User ID:* 149472166
    *CSAT* Terrible
    *Current Page* <url>
    *Feedback:* multi-line text
    *Fullstory:* <url>
"""

from __future__ import annotations

import re
from typing import Any


def parse_feedback_message(text: str) -> dict[str, Any]:
    """Extract structured fields from a Channel 1 Qualtrics message.

    Returns a dict with parsed fields. Missing fields are None.
    """
    result: dict[str, Any] = {
        "survey_type": None,
        "mrr": None,
        "plan": None,
        "user_id": None,
        "csat_raw": None,
        "feedback_text": None,
        "page_url": None,
        "fullstory_url": None,
    }

    if not text:
        return result

    # Survey type
    survey_match = re.search(
        r"\*New Survey Response from the (In-App (?:Feedback Badge|Survey))\*", text
    )
    if survey_match:
        result["survey_type"] = survey_match.group(1)

    # MRR — handles: "*MRR:*  1440", "*MRR:* null", "*MRR:* \nnull"
    mrr_match = re.search(r"\*MRR:\*\s*([\d.]+|null)", text)
    if mrr_match:
        val = mrr_match.group(1)
        result["mrr"] = float(val) if val != "null" else None

    # Plan
    plan_match = re.search(r"\*Plan:\*\s*(.+?)(?:\n|$)", text)
    if plan_match:
        result["plan"] = plan_match.group(1).strip()

    # User ID
    uid_match = re.search(r"\*User ID:\*\s*(\d+)", text)
    if uid_match:
        result["user_id"] = uid_match.group(1)

    # CSAT — two variants: "*CSAT*" (no colon) and "*CSAT:*"
    # "*CSAT* Terrible" — colon outside bold
    # "*CSAT:* Good" — colon inside bold
    # "*CSAT* \n" — empty (no rating given)
    csat_match = re.search(r"\*CSAT:?\*:?\s*([^\n*]+?)(?:\n|$)", text)
    if csat_match:
        csat_val = csat_match.group(1).strip()
        if csat_val:
            result["csat_raw"] = csat_val

    # Current Page URL — handles Slack link format: <url|display> or <url>
    page_match = re.search(r"\*Current Page\*?\s*<([^|>]+)", text)
    if page_match:
        result["page_url"] = page_match.group(1)

    # Feedback text — everything between "*Feedback:*" and the next field marker
    feedback_match = re.search(
        r"\*Feedback:\*\s*(.*?)(?=\n\*(?:Fullstory|Full[Ss]tory URL)|\n_{5,}|\Z)",
        text,
        re.DOTALL,
    )
    if feedback_match:
        result["feedback_text"] = feedback_match.group(1).strip()

    # Fullstory URL — handles both "*Fullstory:*" and "*Fullstory URL:*"
    fs_match = re.search(r"\*(?:Fullstory|Fullstory URL):\*\s*<?([^>|\s]+)", text)
    if fs_match:
        result["fullstory_url"] = fs_match.group(1)

    return result
