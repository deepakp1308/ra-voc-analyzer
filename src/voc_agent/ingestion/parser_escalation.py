"""Parser for Channel 3: #mc-hvc-escalations (C095FJ3SQF4).

Two bot types post here:
1. "Existing Customer Feedback Intake (Global)" — structured product feedback
2. "Product Help (CSM - OB - PDM)" — escalation requests

Observed message structure (Feedback Intake):
    :postal_horn: *New HVC Product Feedback Received* :postal_horn:
    *Customer Name*
    World Central Kitchen
    *Source*
    Customer Success - Strategic
    *Submitter*
    <@U039NUU82EL|Rachel Benner>
    *Impacted Product*
    Analytics
    *Goal: what is the user trying to accomplish?*
    See accurate delivery rates
    *Constraints: what constraints is the user facing?*
    Delivery rate rounds to 100% incorrectly
    *Workaround details*
    Manual calculation from raw numbers
    *Criticality (if specific customer request)*
    P1 (High) – Absence of this causing significant pain
    *Customer UID*
    7165809
    *MRR*
    6664

Observed message structure (Product Help):
    :successtse:
    *Customer UID*: 77161842
    *SF Case*: NA
    *Customer Name*: HC Brands
    *MRR*: 3488
    *Requestor's Team*: Customer Success - Strategic
    *Topic*: Analytics & Reporting
    *Criticality*: P1 (High) - Significant pain, potential churn
    [free-form description]
"""

from __future__ import annotations

import re
from typing import Any


def parse_escalation_message(text: str) -> dict[str, Any]:
    """Extract structured fields from a Channel 3 escalation message.

    Returns a dict with parsed fields. Missing fields are None.
    """
    result: dict[str, Any] = {
        "survey_type": "Escalation",
        "mrr": None,
        "plan": None,
        "user_id": None,
        "customer_name": None,
        "csat_raw": None,
        "feedback_text": None,
        "page_url": None,
        "fullstory_url": None,
        "criticality": None,
        "impacted_product": None,
        "goal": None,
        "constraints": None,
        "source_team": None,
        "submitter": None,
    }

    if not text:
        return result

    # Detect which format: Feedback Intake vs Product Help
    is_feedback_intake = "New HVC Product Feedback Received" in text
    is_product_help = text.startswith(":successtse:") or "*SF Case*" in text

    if is_feedback_intake:
        _parse_feedback_intake(text, result)
    elif is_product_help:
        _parse_product_help(text, result)
    else:
        # Fallback: try to extract whatever we can
        _parse_generic_escalation(text, result)

    return result


def _parse_feedback_intake(text: str, result: dict[str, Any]) -> None:
    """Parse the 'Existing Customer Feedback Intake (Global)' format."""

    # Customer Name — field value is on the NEXT line after the label
    _extract_multiline_field(text, r"\*Customer Name\*", "customer_name", result)

    # Source
    _extract_multiline_field(text, r"\*Source\*", "source_team", result)

    # Submitter — format: <@U039NUU82EL|Rachel Benner>
    submitter_match = re.search(r"\*Submitter\*\n<@[^|]+\|([^>]+)>", text)
    if submitter_match:
        result["submitter"] = submitter_match.group(1)

    # Impacted Product
    _extract_multiline_field(text, r"\*Impacted Product\*", "impacted_product", result)

    # Goal
    goal_match = re.search(
        r"\*Goal:.*?\*\s*\n(.*?)(?=\n\*Constraints|\n\*Workaround|\Z)",
        text,
        re.DOTALL,
    )
    if goal_match:
        result["goal"] = goal_match.group(1).strip()

    # Constraints
    constraints_match = re.search(
        r"\*Constraints:.*?\*\s*\n(.*?)(?=\n\*Workaround|\n\*Supportive|\Z)",
        text,
        re.DOTALL,
    )
    if constraints_match:
        result["constraints"] = constraints_match.group(1).strip()

    # Build feedback_text from goal + constraints (the main content)
    parts = []
    if result["goal"]:
        parts.append(result["goal"])
    if result["constraints"]:
        parts.append(result["constraints"])
    result["feedback_text"] = " | ".join(parts) if parts else None

    # Criticality
    crit_match = re.search(
        r"\*Criticality.*?\*\s*\n(.+?)(?:\n|$)", text
    )
    if crit_match:
        result["criticality"] = crit_match.group(1).strip()

    # Customer UID
    uid_match = re.search(r"\*Customer UID\*\s*\n(\d+)", text)
    if uid_match:
        result["user_id"] = uid_match.group(1)

    # MRR
    mrr_match = re.search(r"\*MRR\*\s*\n(\d+)", text)
    if mrr_match:
        result["mrr"] = float(mrr_match.group(1))


def _parse_product_help(text: str, result: dict[str, Any]) -> None:
    """Parse the 'Product Help (CSM - OB - PDM)' format."""

    # Customer UID — inline format: *Customer UID*: 77161842
    uid_match = re.search(r"\*(?:Customer UID|<[^>]+>)\*:\s*(\d+)", text)
    if uid_match:
        result["user_id"] = uid_match.group(1)

    # Customer Name
    name_match = re.search(r"\*Customer Name\*:\s*(.+?)(?:\n|$)", text)
    if name_match:
        result["customer_name"] = name_match.group(1).strip()

    # MRR
    mrr_match = re.search(r"\*MRR\*:\s*(\d+)", text)
    if mrr_match:
        result["mrr"] = float(mrr_match.group(1))

    # Requestor's Team
    team_match = re.search(r"\*Requestor.s Team\*:\s*(.+?)(?:\n|$)", text)
    if team_match:
        result["source_team"] = team_match.group(1).strip()

    # Topic → impacted_product
    topic_match = re.search(r"\*Topic\*:\s*(.+?)(?:\n|$)", text)
    if topic_match:
        result["impacted_product"] = topic_match.group(1).strip()

    # Criticality
    crit_match = re.search(r"\*Criticality\*:\s*(.+?)(?:\n|$)", text)
    if crit_match:
        result["criticality"] = crit_match.group(1).strip()

    # Feedback text — everything after the structured fields
    # Find the last structured field and take everything after it
    lines = text.split("\n")
    feedback_lines = []
    past_structured = False
    for line in lines:
        if past_structured:
            feedback_lines.append(line)
        elif line.startswith("*CC*") or (
            not line.startswith("*") and not line.startswith(":") and len(line) > 20
        ):
            # Once we hit non-structured content, capture it
            if not line.startswith("*CC*"):
                feedback_lines.append(line)
            past_structured = True

    if feedback_lines:
        result["feedback_text"] = "\n".join(feedback_lines).strip()


def _parse_generic_escalation(text: str, result: dict[str, Any]) -> None:
    """Fallback parser for unrecognized escalation formats."""
    # Try to extract MRR from any format
    mrr_match = re.search(r"\*MRR\*[:\s]*(\d+)", text)
    if mrr_match:
        result["mrr"] = float(mrr_match.group(1))

    uid_match = re.search(r"\*Customer UID\*[:\s]*(\d+)", text)
    if uid_match:
        result["user_id"] = uid_match.group(1)

    # Use full text as feedback
    result["feedback_text"] = text[:500] if len(text) > 500 else text


def _extract_multiline_field(
    text: str, label_pattern: str, field_name: str, result: dict[str, Any]
) -> None:
    """Extract a field where the value is on the line AFTER the label."""
    match = re.search(rf"{label_pattern}\s*\n(.+?)(?:\n|$)", text)
    if match:
        result[field_name] = match.group(1).strip()
