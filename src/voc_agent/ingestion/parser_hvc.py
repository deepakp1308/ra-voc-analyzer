"""Parser for Channel 2: #hvc_feedback (C051Y4H98VB).

Qualtrics bot posts HVC ($299+) survey responses in three formats:
- "Feedback Badge" — CSAT + feedback with compound User/Plan/MRR line
- "CSAT Survey" — CSAT + feedback, simpler format
- "PRS Survey" — Product Recommendation Score (0-10) + reason + feedback

Observed message structure (Feedback Badge):
    *Response from Feedback Badge*
    *User ID:* 149472166  | *Premium plan* | *MRR:*  1440
    *Page URL:* <url>
    *CSAT:* Terrible
    *Feedback:* text
    *<url|FS Session Replay>*

Observed message structure (PRS Survey):
    *Response from the PRS Survey*
    *User ID:* 175352541  | *Premium plan* | *MRR:* 818
    *PRS:* 0
    *Reason:* Price
    *Feedback:* text
    *<url|FS Session Replay>*
"""

from __future__ import annotations

import re
from typing import Any


def parse_hvc_message(text: str) -> dict[str, Any]:
    """Extract structured fields from a Channel 2 HVC message.

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
        "prs_score": None,
        "prs_reason": None,
    }

    if not text:
        return result

    # Survey type — "Response from Feedback Badge", "Response from CSAT Survey", "Response from the PRS Survey"
    survey_match = re.search(
        r"\*Response from (?:the )?(Feedback Badge|CSAT Survey|PRS Survey)\*", text
    )
    if survey_match:
        result["survey_type"] = survey_match.group(1)

    # Compound line: *User ID:* 149472166  | *Premium plan* | *MRR:*  1440
    compound_match = re.search(
        r"\*User ID:\*\s*(\d+)\s*\|\s*\*([^*]+)\*\s*\|\s*\*MRR:\*\s*([\d.]+|null)",
        text,
    )
    if compound_match:
        result["user_id"] = compound_match.group(1)
        result["plan"] = compound_match.group(2).strip()
        mrr_val = compound_match.group(3)
        result["mrr"] = float(mrr_val) if mrr_val != "null" else None
    else:
        # Fallback: try individual field extraction
        uid_match = re.search(r"\*User ID:\*\s*(\d+)", text)
        if uid_match:
            result["user_id"] = uid_match.group(1)

        mrr_match = re.search(r"\*MRR:\*\s*([\d.]+|null)", text)
        if mrr_match:
            val = mrr_match.group(1)
            result["mrr"] = float(val) if val != "null" else None

    # Page URL
    page_match = re.search(r"\*Page URL:\*\s*<([^|>]+)", text)
    if page_match:
        result["page_url"] = page_match.group(1)

    # CSAT
    csat_match = re.search(r"\*CSAT:\*\s*(.+?)(?:\n|$)", text)
    if csat_match:
        csat_val = csat_match.group(1).strip()
        if csat_val:
            result["csat_raw"] = csat_val

    # PRS Score (0-10)
    prs_match = re.search(r"\*PRS:\*\s*(\d+)", text)
    if prs_match:
        result["prs_score"] = int(prs_match.group(1))

    # PRS Reason
    reason_match = re.search(r"\*Reason:\*\s*(.+?)(?:\n|$)", text)
    if reason_match:
        reason_val = reason_match.group(1).strip()
        # Filter out HTML artifacts that sometimes appear
        if reason_val and not reason_val.startswith("<style"):
            result["prs_reason"] = reason_val

    # Feedback text
    feedback_match = re.search(
        r"\*Feedback:\*\s*(.*?)(?=\n\*<|\n_{5,}|\n<\||\Z)",
        text,
        re.DOTALL,
    )
    if feedback_match:
        result["feedback_text"] = feedback_match.group(1).strip()

    # Fullstory URL — format: *<url|FS Session Replay>*
    fs_match = re.search(r"\*?<([^|>]+)\|FS Session Replay>\*?", text)
    if fs_match:
        result["fullstory_url"] = fs_match.group(1)

    return result
