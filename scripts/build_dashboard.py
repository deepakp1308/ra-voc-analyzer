#!/usr/bin/env python3
"""Build the VOC Dashboard from raw Slack data files.

This script:
1. Reads all raw Slack data files from the tool-results directory
2. Parses every message with channel-specific regex
3. Deduplicates across channels using content hashing
4. Classifies sentiment (CSAT mapping) and categories (keyword-based)
5. Computes weekly snapshots with 5-way segmentation
6. Generates the full interactive HTML dashboard with Plotly
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# ── Data file paths ───────────────────────────────────────────────────────────

TOOL_RESULTS_DIR = Path(
    "/Users/dprabhakara/.claude/projects/-Users-dprabhakara-ai-workspace/"
    "7b430b53-7098-4233-821e-e550b6552729/tool-results"
)

# Channel 1 files (mc-reporting-analytics-feedback)
CH1_FILES = [
    "mcp-5bb57945-e88b-477f-b809-be9c74056e5c-slack_read_channel-1775865293968.txt",  # Jan-Mar 2025
    "mcp-5bb57945-e88b-477f-b809-be9c74056e5c-slack_read_channel-1775865334820.txt",  # Apr-Jun 2025
    "mcp-5bb57945-e88b-477f-b809-be9c74056e5c-slack_read_channel-1775865336767.txt",  # Jul-Sep 2025
    "mcp-5bb57945-e88b-477f-b809-be9c74056e5c-slack_read_channel-1775865339508.txt",  # Oct-Dec 2025
]

# Channel 2 files (hvc_feedback)
CH2_FILES = [
    "toolu_014A8aQejR6zGzRQvYe29ECK.json",   # Jan-Mar 2025
    "toolu_016UvtFkhrrXWhbg7cyi9fqN.json",   # Apr-Jun 2025
    "toolu_01K9VAMzPZbJtQZxjZAWJwrB.json",   # Jul-Sep 2025
    "toolu_01AD5DkwwpCokejH1jU7en8r.json",   # Oct-Dec 2025
]

# Channel 3 file (mc-hvc-escalations)
CH3_FILES = [
    "mcp-5bb57945-e88b-477f-b809-be9c74056e5c-slack_read_channel-1775865298844.txt",  # Jul 2025 - Apr 2026
]

OUTPUT_DIR = Path("/Users/dprabhakara/ai_workspace/ra-voc-analyzer/docs")
ARCHIVE_DIR = OUTPUT_DIR / "archive"


# ── CSAT Mapping ──────────────────────────────────────────────────────────────

CSAT_SENTIMENT = {
    "terrible": "negative", "poor": "negative",
    "average": "neutral",
    "good": "positive", "excellent": "positive",
    # Localized
    "horrible": "negative", "malo": "negative", "schlecht": "negative",
    "schrecklich": "negative", "mauvais": "negative", "ruim": "negative",
    "terrível": "negative", "terrivel": "negative",
    "medianamente satisfecho": "neutral", "moyen": "neutral",
    "durchschnittlich": "neutral", "promedio": "neutral", "medio": "neutral",
    "gut": "positive", "bueno": "positive", "bon": "positive", "bom": "positive",
    "ausgezeichnet": "positive", "excelente": "positive", "eccellente": "positive",
}


# ── Category Keywords ─────────────────────────────────────────────────────────

DATA_QUALITY_KEYWORDS = [
    r"data.*(?:quality|accuracy|mismatch|discrepan|inconsisten|wrong number|incorrect)",
    r"(?:numbers?|metrics?|stats?|report|rate).*(?:don.?t match|disagree|different|wrong|inaccurate)",
    r"(?:100%|delivery rate|open rate).*(?:round|wrong|incorrect|misleading)",
    r"(?:bot|MPP|machine).*(?:filter|data|inflate|include)",
    r"(?:missing|disappeared|not.*(?:show|load|display|appear|available|report))",
    r"(?:stale|delayed|not.*updat|refresh|old data)",
    r"double.?count|over.?count|under.?report",
]

BUG_KEYWORDS = [
    r"(?:bug|broken|crash|error|glitch|freeze|frozen|stuck|fail)",
    r"(?:aw.?shucks|404|500|not.*work|doesn.?t.*work|won.?t.*work)",
    r"(?:kicked.*out|logged.*out|can.?t.*(?:log|sign|access))",
]

PERFORMANCE_UX_KEYWORDS = [
    r"(?:slow|lag|latency|loading|long.*time|takes forever|waiting)",
    r"(?:confus|hard to (?:find|use|navigate)|too many clicks|unintuitive)",
    r"(?:clicks? and (?:click|selection))",
    r"(?:UX|user experience|interface|navigation).*(?:bad|poor|awful|terrible)",
]

FEATURE_GAP_KEYWORDS = [
    r"(?:wish|would be nice|please add|feature request|bring back|used to|miss)",
    r"(?:can.?t.*(?:filter|export|download|sort|customize|schedule))",
    r"(?:took.*(?:away|out)|removed|eliminated|deprecated|discontinued)",
    r"(?:need.*(?:ability|option|feature|way to))",
]

DQ_SUBCATEGORY_KEYWORDS = {
    "accuracy": [r"(?:wrong|incorrect|inaccurate|miscalculat)", r"(?:numbers? are|data is).*(?:off|wrong)"],
    "consistency": [r"(?:don.?t match|disagree|different.*(?:number|value|metric))", r"discrepan"],
    "availability": [r"(?:missing|not.*(?:show|load|display|appear|available))", r"(?:blank|empty).*report"],
    "freshness": [r"(?:stale|delayed|not.*updat|old data|refresh)", r"(?:date|time).*(?:not chang|stuck)"],
    "coverage": [r"(?:bot|MPP|machine).*(?:filter|data|include)", r"(?:attribution|tracking).*(?:gap|miss)"],
}


# ── File Reading ──────────────────────────────────────────────────────────────

def read_data_file(filepath: Path) -> str:
    """Read a tool results file and extract the messages text."""
    content = filepath.read_text(errors="replace")

    # Handle JSON format (Channel 2 files)
    if filepath.suffix == ".json":
        try:
            data = json.loads(content)
            if isinstance(data, list) and data:
                # Navigate to the text content
                for item in data:
                    if isinstance(item, dict) and "text" in item:
                        inner = item["text"]
                        if isinstance(inner, str):
                            try:
                                parsed = json.loads(inner)
                                if isinstance(parsed, dict) and "messages" in parsed:
                                    return parsed["messages"]
                            except json.JSONDecodeError:
                                return inner
            return content
        except json.JSONDecodeError:
            return content

    # Handle text format (Channel 1 and 3 files)
    try:
        data = json.loads(content)
        if isinstance(data, list) and data:
            for item in data:
                if isinstance(item, dict) and "text" in item:
                    inner = item["text"]
                    if isinstance(inner, str):
                        try:
                            parsed = json.loads(inner)
                            if isinstance(parsed, dict) and "messages" in parsed:
                                return parsed["messages"]
                        except json.JSONDecodeError:
                            return inner
        return content
    except json.JSONDecodeError:
        return content


# ── Parsing ───────────────────────────────────────────────────────────────────

def parse_timestamp(text: str) -> datetime | None:
    """Extract timestamp from [YYYY-MM-DD HH:MM:SS PDT/PST] format."""
    m = re.search(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (?:PDT|PST|UTC)\]", text)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return None


def extract_field(text: str, pattern: str) -> str | None:
    """Extract a field value using regex."""
    m = re.search(pattern, text, re.DOTALL)
    if m:
        return m.group(1).strip()
    return None


def parse_ch1_messages(raw_text: str) -> list[dict]:
    """Parse Channel 1 messages (mc-reporting-analytics-feedback)."""
    records = []
    # Split on message boundaries
    messages = re.split(r"(?=Qualtrics:?\s*\*(?:New Survey|Response))", raw_text)

    for msg in messages:
        if not msg.strip() or len(msg) < 50:
            continue

        ts = parse_timestamp(msg)
        if not ts:
            continue

        user_id = extract_field(msg, r"\*User ID:\*\s*(\d+)")
        mrr_str = extract_field(msg, r"\*MRR:\*\s*([\d.]+|null)")
        plan = extract_field(msg, r"\*Plan:\*\s*(.+?)(?:\n|\*)")
        csat = extract_field(msg, r"\*CSAT[:\*]*\s*(.+?)(?:\n|\*)")
        feedback = extract_field(msg, r"\*Feedback:\*\s*(.+?)(?:\*(?:Fullstory|FS)|_{5,}|\[)")

        # PRS score
        prs = extract_field(msg, r"\*PRS:\*\s*(\d+)")

        # Survey type
        if "In-App Feedback Badge" in msg or "Feedback Badge" in msg:
            survey_type = "Feedback Badge"
        elif "In-App Survey" in msg:
            survey_type = "In-App Survey"
        elif "CSAT Survey" in msg:
            survey_type = "CSAT Survey"
        elif "PRS Survey" in msg:
            survey_type = "PRS Survey"
        else:
            survey_type = "Unknown"

        mrr = None
        if mrr_str and mrr_str != "null":
            try:
                mrr = float(mrr_str)
            except ValueError:
                pass

        if feedback and len(feedback.strip()) > 3:
            records.append({
                "channel": "feedback",
                "channel_id": "C06SW7512P2",
                "timestamp": ts.isoformat(),
                "iso_week": ts.strftime("%G-W%V"),
                "month": ts.strftime("%Y-%m"),
                "quarter": f"{ts.year}-Q{(ts.month - 1) // 3 + 1}",
                "user_id": user_id,
                "mrr": mrr,
                "plan": plan,
                "csat": csat,
                "prs_score": int(prs) if prs else None,
                "feedback_text": feedback.strip()[:500],
                "survey_type": survey_type,
            })

    return records


def parse_ch2_messages(raw_text: str) -> list[dict]:
    """Parse Channel 2 messages (hvc_feedback)."""
    records = []
    messages = re.split(r"(?=Qualtrics:?\s*\*Response from)", raw_text)

    for msg in messages:
        if not msg.strip() or len(msg) < 50:
            continue

        ts = parse_timestamp(msg)
        if not ts:
            continue

        # Channel 2 format: *User ID:* 123  | *Plan* | *MRR:* 450
        user_id = extract_field(msg, r"\*User ID:\*\s*(\d+)")
        mrr_str = extract_field(msg, r"\*MRR:\*\s*([\d.]+)")
        plan = extract_field(msg, r"\|\s*\*(.+?)\*\s*\|")
        csat = extract_field(msg, r"\*CSAT:\*\s*(.+?)(?:\n|\*)")
        feedback = extract_field(msg, r"\*Feedback:\*\s*(.+?)(?:\*<|<\||\[|_{5,})")
        prs = extract_field(msg, r"\*PRS:\*\s*(\d+)")
        prs_reason = extract_field(msg, r"\*Reason:\*\s*(.+?)(?:\n|\*)")

        if "Feedback Badge" in msg:
            survey_type = "Feedback Badge"
        elif "CSAT Survey" in msg:
            survey_type = "CSAT Survey"
        elif "PRS Survey" in msg:
            survey_type = "PRS Survey"
        else:
            survey_type = "Unknown"

        mrr = None
        if mrr_str:
            try:
                mrr = float(mrr_str)
            except ValueError:
                pass

        if feedback and len(feedback.strip()) > 3:
            records.append({
                "channel": "hvc_feedback",
                "channel_id": "C051Y4H98VB",
                "timestamp": ts.isoformat(),
                "iso_week": ts.strftime("%G-W%V"),
                "month": ts.strftime("%Y-%m"),
                "quarter": f"{ts.year}-Q{(ts.month - 1) // 3 + 1}",
                "user_id": user_id,
                "mrr": mrr,
                "plan": plan,
                "csat": csat,
                "prs_score": int(prs) if prs else None,
                "prs_reason": prs_reason,
                "feedback_text": feedback.strip()[:500],
                "survey_type": survey_type,
            })

    return records


def parse_ch3_messages(raw_text: str) -> list[dict]:
    """Parse Channel 3 messages (mc-hvc-escalations)."""
    records = []
    # Split on escalation boundaries
    messages = re.split(r"(?=:postal_horn:|:successtse:)", raw_text)

    for msg in messages:
        if not msg.strip() or len(msg) < 100:
            continue

        ts = parse_timestamp(msg)
        if not ts:
            continue

        customer_name = extract_field(msg, r"\*Customer Name\*?\s*\n(.+?)(?:\n\*)")
        if not customer_name:
            customer_name = extract_field(msg, r"\*Customer Name\*:\s*(.+?)(?:\n)")

        mrr_str = extract_field(msg, r"\*MRR\*?\s*\n?:?\s*([\d,]+)")
        customer_uid = extract_field(msg, r"\*Customer UID\*?\s*\n?:?\s*(\d+)")
        impacted_product = extract_field(msg, r"\*(?:Impacted Product|Topic)\*?\s*\n?:?\s*(.+?)(?:\n\*)")
        goal = extract_field(msg, r"\*Goal[^*]*\*\s*\n(.+?)(?:\n\*)")
        constraints = extract_field(msg, r"\*Constraints[^*]*\*\s*\n(.+?)(?:\n\*)")
        criticality = extract_field(msg, r"\*Criticality[^*]*\*\s*\n(.+?)(?:\n\*)")

        mrr = None
        if mrr_str:
            try:
                mrr = float(mrr_str.replace(",", ""))
            except ValueError:
                pass

        # Build feedback text from goal + constraints
        feedback_parts = []
        if goal:
            feedback_parts.append(goal.strip())
        if constraints:
            feedback_parts.append(constraints.strip())
        feedback_text = " | ".join(feedback_parts) if feedback_parts else None

        # Extract description for Product Help format
        if not feedback_text:
            # Look for free-text after the structured fields
            desc_match = re.search(r"\*Criticality\*:.*?\n(.+?)(?:\n\*CC\*|\Z)", msg, re.DOTALL)
            if desc_match:
                feedback_text = desc_match.group(1).strip()[:500]

        if feedback_text and len(feedback_text.strip()) > 10:
            records.append({
                "channel": "hvc_escalation",
                "channel_id": "C095FJ3SQF4",
                "timestamp": ts.isoformat(),
                "iso_week": ts.strftime("%G-W%V"),
                "month": ts.strftime("%Y-%m"),
                "quarter": f"{ts.year}-Q{(ts.month - 1) // 3 + 1}",
                "user_id": customer_uid,
                "customer_name": customer_name,
                "mrr": mrr,
                "plan": "Enterprise",
                "csat": None,
                "feedback_text": feedback_text.strip()[:500],
                "survey_type": "Escalation",
                "criticality": criticality,
                "impacted_product": impacted_product,
            })

    return records


# ── Deduplication ─────────────────────────────────────────────────────────────

def normalize_for_dedup(text: str) -> str:
    """Normalize text for dedup hashing."""
    text = text.lower()
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[*_~`\\]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def content_hash(text: str) -> str:
    """SHA256 hash of normalized text (first 16 chars)."""
    normalized = normalize_for_dedup(text)
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


def deduplicate(records: list[dict]) -> list[dict]:
    """Deduplicate records across channels. Prefer hvc_feedback as canonical."""
    seen_hashes: dict[str, dict] = {}

    # Sort so HVC feedback comes first (preferred canonical)
    priority = {"hvc_feedback": 0, "hvc_escalation": 1, "feedback": 2}
    sorted_records = sorted(records, key=lambda r: priority.get(r["channel"], 9))

    for rec in sorted_records:
        h = content_hash(rec["feedback_text"])
        if h not in seen_hashes:
            rec["content_hash"] = h
            rec["voc_id"] = str(uuid.uuid4())
            rec["dup_count"] = 1
            seen_hashes[h] = rec
        else:
            seen_hashes[h]["dup_count"] += 1

    return list(seen_hashes.values())


# ── Classification ────────────────────────────────────────────────────────────

def classify_sentiment(rec: dict) -> str:
    """Classify sentiment from CSAT rating or PRS score."""
    # CSAT mapping (most reliable)
    if rec.get("csat"):
        csat_lower = rec["csat"].strip().lower()
        if csat_lower in CSAT_SENTIMENT:
            return CSAT_SENTIMENT[csat_lower]

    # PRS score mapping
    if rec.get("prs_score") is not None:
        prs = rec["prs_score"]
        if prs <= 3:
            return "negative"
        elif prs <= 6:
            return "neutral"
        else:
            return "positive"

    # Keyword-based fallback for escalations
    text = rec.get("feedback_text", "").lower()
    neg_words = ["frustrated", "angry", "terrible", "awful", "worst", "pain", "churn",
                 "broken", "bug", "error", "can't", "cannot", "unacceptable", "horrible"]
    pos_words = ["great", "love", "excellent", "amazing", "perfect", "fantastic", "thank"]

    neg_count = sum(1 for w in neg_words if w in text)
    pos_count = sum(1 for w in pos_words if w in text)

    if neg_count > pos_count:
        return "negative"
    elif pos_count > neg_count:
        return "positive"
    return "neutral"


def classify_category(text: str) -> tuple[str, str | None]:
    """Classify root-cause category from feedback text."""
    text_lower = text.lower()

    # Check data quality first (highest priority for R&A)
    for pattern in DATA_QUALITY_KEYWORDS:
        if re.search(pattern, text_lower):
            # Determine subcategory
            for subcat, subcat_patterns in DQ_SUBCATEGORY_KEYWORDS.items():
                for sp in subcat_patterns:
                    if re.search(sp, text_lower):
                        return "data_quality", subcat
            return "data_quality", "accuracy"  # default subcategory

    for pattern in BUG_KEYWORDS:
        if re.search(pattern, text_lower):
            return "bug_or_error", None

    for pattern in PERFORMANCE_UX_KEYWORDS:
        if re.search(pattern, text_lower):
            return "performance_ux", None

    for pattern in FEATURE_GAP_KEYWORDS:
        if re.search(pattern, text_lower):
            return "feature_gap", None

    return "other_or_praise", None


def compute_customer_tier(mrr: float | None) -> str:
    """Determine customer tier from MRR."""
    if mrr is None:
        return "unknown"
    if mrr <= 0:
        return "free"
    if mrr >= 299:
        return "paid_gte_299"
    return "paid_lt_299"


# ── Analytics ─────────────────────────────────────────────────────────────────

def compute_snapshots(records: list[dict]) -> dict:
    """Compute weekly snapshots by segment x category x sentiment."""
    segments = {
        "all": lambda r: True,
        "free": lambda r: r["customer_tier"] == "free",
        "paid": lambda r: r["customer_tier"] in ("paid_lt_299", "paid_gte_299"),
        "paid_gte_299": lambda r: r["customer_tier"] == "paid_gte_299",
        "paid_lt_299": lambda r: r["customer_tier"] == "paid_lt_299",
    }

    snapshots = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int))))

    for rec in records:
        week = rec["iso_week"]
        for seg_name, seg_filter in segments.items():
            if seg_filter(rec):
                snapshots[week][seg_name][rec["category"]][rec["sentiment"]] += 1

    return snapshots


def compute_trends(records: list[dict]) -> dict:
    """Compute comprehensive trend analytics."""
    # Weekly sentiment counts
    weekly = defaultdict(lambda: {"positive": 0, "neutral": 0, "negative": 0, "total": 0})
    for rec in records:
        w = rec["iso_week"]
        weekly[w][rec["sentiment"]] += 1
        weekly[w]["total"] += 1

    # Monthly aggregation
    monthly = defaultdict(lambda: {"positive": 0, "neutral": 0, "negative": 0, "total": 0})
    for rec in records:
        m = rec["month"]
        monthly[m][rec["sentiment"]] += 1
        monthly[m]["total"] += 1

    # Quarterly
    quarterly = defaultdict(lambda: {"positive": 0, "neutral": 0, "negative": 0, "total": 0})
    for rec in records:
        q = rec["quarter"]
        quarterly[q][rec["sentiment"]] += 1
        quarterly[q]["total"] += 1

    # Category weekly trends
    cat_weekly = defaultdict(lambda: defaultdict(int))
    for rec in records:
        cat_weekly[rec["iso_week"]][rec["category"]] += 1

    # Since Aug 2025 change detection
    pre_aug = [r for r in records if r["timestamp"] < "2025-08-01"]
    post_aug = [r for r in records if r["timestamp"] >= "2025-08-01"]

    # 90-day data quality deep dive
    cutoff_90d = (datetime.now() - timedelta(days=90)).isoformat()
    recent_dq = [r for r in records if r["category"] == "data_quality" and r["timestamp"] >= cutoff_90d]
    dq_weekly = defaultdict(lambda: defaultdict(int))
    for rec in recent_dq:
        dq_weekly[rec["iso_week"]][rec.get("subcategory", "unknown")] += 1

    return {
        "weekly": dict(weekly),
        "monthly": dict(monthly),
        "quarterly": dict(quarterly),
        "cat_weekly": {k: dict(v) for k, v in cat_weekly.items()},
        "pre_aug_count": len(pre_aug),
        "post_aug_count": len(post_aug),
        "dq_weekly": {k: dict(v) for k, v in dq_weekly.items()},
    }


# ── Dashboard Generation ─────────────────────────────────────────────────────

def generate_dashboard(records: list[dict], trends: dict, snapshots: dict) -> str:
    """Generate the full interactive HTML dashboard."""

    # Sort weeks
    all_weeks = sorted(set(r["iso_week"] for r in records))
    all_months = sorted(set(r["month"] for r in records))

    # Compute summary stats
    total_vocs = len(records)
    total_negative = sum(1 for r in records if r["sentiment"] == "negative")
    total_neutral = sum(1 for r in records if r["sentiment"] == "neutral")
    total_positive = sum(1 for r in records if r["sentiment"] == "positive")

    neg_pct = total_negative / total_vocs * 100 if total_vocs else 0
    pos_pct = total_positive / total_vocs * 100 if total_vocs else 0

    # Category distribution
    cat_counts = Counter(r["category"] for r in records)

    # Segment counts
    tier_counts = Counter(r["customer_tier"] for r in records)

    # This week's data
    current_week = all_weeks[-1] if all_weeks else ""
    this_week = [r for r in records if r["iso_week"] == current_week]
    last_week = [r for r in records if r["iso_week"] == all_weeks[-2]] if len(all_weeks) >= 2 else []

    # Change since Aug 2025
    pre_aug = [r for r in records if r["timestamp"] < "2025-08-01"]
    post_aug = [r for r in records if r["timestamp"] >= "2025-08-01"]

    # Category changes
    def cat_pct(recs, cat):
        total = len(recs) if recs else 1
        return sum(1 for r in recs if r["category"] == cat) / total * 100

    categories = ["feature_gap", "bug_or_error", "data_quality", "performance_ux", "other_or_praise"]
    cat_labels = {
        "feature_gap": "Feature Gap",
        "bug_or_error": "Bug / Error",
        "data_quality": "Data Quality",
        "performance_ux": "Performance / UX",
        "other_or_praise": "Other / Praise",
    }

    # 90-day DQ deep dive
    cutoff_90d = (datetime.now() - timedelta(days=90)).isoformat()
    recent_dq = [r for r in records if r["category"] == "data_quality" and r["timestamp"] >= cutoff_90d]
    dq_sub_counts = Counter(r.get("subcategory", "unknown") for r in recent_dq)

    # Weekly data for charts
    weekly_data = {
        "weeks": all_weeks,
        "positive": [trends["weekly"].get(w, {}).get("positive", 0) for w in all_weeks],
        "neutral": [trends["weekly"].get(w, {}).get("neutral", 0) for w in all_weeks],
        "negative": [trends["weekly"].get(w, {}).get("negative", 0) for w in all_weeks],
    }

    # Category weekly data
    cat_weekly_data = {cat: [trends["cat_weekly"].get(w, {}).get(cat, 0) for w in all_weeks] for cat in categories}

    # Segment breakdown
    segments = ["all", "free", "paid", "paid_gte_299", "paid_lt_299"]
    seg_labels = {
        "all": "All Customers",
        "free": "Free",
        "paid": "All Paid",
        "paid_gte_299": "Paid ≥$299",
        "paid_lt_299": "Paid <$299",
    }

    seg_sentiment = {}
    for seg in segments:
        if seg == "all":
            seg_recs = records
        elif seg == "free":
            seg_recs = [r for r in records if r["customer_tier"] == "free"]
        elif seg == "paid":
            seg_recs = [r for r in records if r["customer_tier"] in ("paid_lt_299", "paid_gte_299")]
        elif seg == "paid_gte_299":
            seg_recs = [r for r in records if r["customer_tier"] == "paid_gte_299"]
        else:
            seg_recs = [r for r in records if r["customer_tier"] == "paid_lt_299"]

        total = len(seg_recs) if seg_recs else 1
        seg_sentiment[seg] = {
            "total": len(seg_recs),
            "negative": sum(1 for r in seg_recs if r["sentiment"] == "negative"),
            "neutral": sum(1 for r in seg_recs if r["sentiment"] == "neutral"),
            "positive": sum(1 for r in seg_recs if r["sentiment"] == "positive"),
            "neg_pct": sum(1 for r in seg_recs if r["sentiment"] == "negative") / total * 100,
        }

    # Top 10 recent negative VOCs
    recent_negative = sorted(
        [r for r in records if r["sentiment"] == "negative"],
        key=lambda r: r["timestamp"],
        reverse=True,
    )[:10]

    # DQ subcategory trend (last 90 days)
    dq_subcats = ["accuracy", "consistency", "availability", "freshness", "coverage"]
    dq_weeks = sorted(set(r["iso_week"] for r in recent_dq))
    dq_trend = {
        sub: [sum(1 for r in recent_dq if r["iso_week"] == w and r.get("subcategory") == sub) for w in dq_weeks]
        for sub in dq_subcats
    }

    # Determine DQ trends (reduced/increased/steady)
    dq_verdicts = {}
    for sub in dq_subcats:
        values = dq_trend[sub]
        if len(values) >= 4:
            first_4 = sum(values[:4]) / 4 if values[:4] else 0
            last_4 = sum(values[-4:]) / 4 if values[-4:] else 0
            if first_4 > 0 and last_4 < first_4 * 0.85:
                dq_verdicts[sub] = "reduced"
            elif first_4 > 0 and last_4 > first_4 * 1.15:
                dq_verdicts[sub] = "increased"
            else:
                dq_verdicts[sub] = "steady"
        else:
            dq_verdicts[sub] = "insufficient_data"

    # Monthly data for charts
    monthly_data = {
        "months": all_months,
        "positive": [trends["monthly"].get(m, {}).get("positive", 0) for m in all_months],
        "neutral": [trends["monthly"].get(m, {}).get("neutral", 0) for m in all_months],
        "negative": [trends["monthly"].get(m, {}).get("negative", 0) for m in all_months],
    }

    # Change detection: pre vs post Aug 2025
    change_table = []
    for cat in categories:
        pre_pct = cat_pct(pre_aug, cat) if pre_aug else 0
        post_pct = cat_pct(post_aug, cat)
        delta = post_pct - pre_pct
        if abs(delta) < 2:
            status = "steady"
        elif delta > 0:
            status = "increased"
        else:
            status = "reduced"
        change_table.append({
            "category": cat_labels[cat],
            "pre_aug_pct": pre_pct,
            "post_aug_pct": post_pct,
            "delta": delta,
            "status": status,
        })

    # Generate HTML
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>R&A VOC Analyzer — Weekly Insights Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Avenir Next', -apple-system, 'Helvetica Neue', Arial, sans-serif; background: #f0f4f8; color: #1a1f36; }}
        .sidebar {{ position: fixed; left: 0; top: 0; bottom: 0; width: 240px; background: #162251; color: white; padding: 24px 16px; overflow-y: auto; z-index: 100; }}
        .sidebar h2 {{ font-size: 16px; font-weight: 600; margin-bottom: 24px; display: flex; align-items: center; gap: 8px; }}
        .sidebar .star {{ color: #00b9a9; font-size: 20px; }}
        .sidebar nav a {{ display: block; color: rgba(255,255,255,0.7); text-decoration: none; padding: 8px 12px; border-radius: 6px; font-size: 13px; margin-bottom: 2px; transition: all 0.2s; }}
        .sidebar nav a:hover, .sidebar nav a.active {{ background: rgba(255,255,255,0.1); color: white; }}
        .main {{ margin-left: 240px; padding: 24px 32px; max-width: 1280px; }}
        .header {{ margin-bottom: 24px; }}
        .header h1 {{ font-size: 24px; font-weight: 600; margin-bottom: 4px; }}
        .header .subtitle {{ color: #6b7c93; font-size: 14px; }}
        .header .generated {{ color: #6b7c93; font-size: 12px; margin-top: 4px; }}
        .kpi-row {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px; }}
        .kpi-card {{ background: white; border-radius: 8px; border: 1px solid #e8edf5; padding: 20px; box-shadow: 0 1px 4px rgba(26,40,96,0.08); }}
        .kpi-card .label {{ font-size: 12px; color: #6b7c93; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }}
        .kpi-card .value {{ font-size: 28px; font-weight: 600; }}
        .kpi-card .trend {{ font-size: 12px; margin-top: 4px; }}
        .kpi-card .trend.up {{ color: #d13438; }}
        .kpi-card .trend.down {{ color: #1aab68; }}
        .kpi-card .trend.neutral {{ color: #6b7c93; }}
        .card {{ background: white; border-radius: 8px; border: 1px solid #e8edf5; padding: 24px; margin-bottom: 24px; box-shadow: 0 1px 4px rgba(26,40,96,0.08); }}
        .card h3 {{ font-size: 16px; font-weight: 600; margin-bottom: 16px; display: flex; align-items: center; gap: 8px; }}
        .card h3 .icon {{ color: #00b9a9; }}
        .section-title {{ font-size: 14px; font-weight: 600; color: #6b7c93; text-transform: uppercase; letter-spacing: 1px; margin: 32px 0 16px; }}
        .tldr {{ background: linear-gradient(135deg, #162251 0%, #1e3a6e 100%); color: white; border-radius: 8px; padding: 24px; margin-bottom: 24px; }}
        .tldr h3 {{ color: white; margin-bottom: 12px; }}
        .tldr ul {{ list-style: none; padding: 0; }}
        .tldr li {{ padding: 6px 0; font-size: 14px; display: flex; align-items: start; gap: 8px; }}
        .tldr li::before {{ content: '\\2726'; color: #00b9a9; flex-shrink: 0; }}
        .segment-grid {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin-bottom: 24px; }}
        .seg-card {{ background: white; border-radius: 8px; border: 1px solid #e8edf5; padding: 16px; text-align: center; }}
        .seg-card .seg-label {{ font-size: 12px; color: #6b7c93; margin-bottom: 8px; }}
        .seg-card .seg-value {{ font-size: 24px; font-weight: 600; }}
        .seg-card .seg-detail {{ font-size: 11px; color: #6b7c93; margin-top: 4px; }}
        .change-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
        .change-table th {{ text-align: left; padding: 10px 12px; border-bottom: 2px solid #e8edf5; color: #6b7c93; font-weight: 600; font-size: 11px; text-transform: uppercase; }}
        .change-table td {{ padding: 10px 12px; border-bottom: 1px solid #e8edf5; }}
        .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }}
        .badge.reduced {{ background: #e6f9f0; color: #1aab68; }}
        .badge.increased {{ background: #fde8ea; color: #d13438; }}
        .badge.steady {{ background: #f0f4f8; color: #6b7c93; }}
        .voc-list {{ font-size: 13px; }}
        .voc-item {{ padding: 12px 0; border-bottom: 1px solid #e8edf5; }}
        .voc-item:last-child {{ border-bottom: none; }}
        .voc-meta {{ color: #6b7c93; font-size: 11px; margin-bottom: 4px; }}
        .voc-text {{ line-height: 1.5; }}
        .qa-footer {{ background: #f8f9fb; border-radius: 8px; padding: 16px; font-size: 12px; color: #6b7c93; margin-top: 32px; }}
        .qa-footer .qa-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-top: 8px; }}
        .qa-footer .qa-item {{ text-align: center; }}
        .qa-footer .qa-item .qa-val {{ font-size: 16px; font-weight: 600; color: #1a1f36; }}
        .chart-container {{ width: 100%; min-height: 300px; }}
        @media (max-width: 1200px) {{ .kpi-row {{ grid-template-columns: repeat(2, 1fr); }} .segment-grid {{ grid-template-columns: repeat(3, 1fr); }} }}
    </style>
</head>
<body>
    <div class="sidebar">
        <h2><span class="star">&#10022;</span> R&A VOC Analyzer</h2>
        <nav>
            <a href="#tldr" class="active">TL;DR</a>
            <a href="#weekly-trend">Weekly Sentiment Trend</a>
            <a href="#segments">Segment Breakdown</a>
            <a href="#categories">Category Distribution</a>
            <a href="#change">Change Since Aug 2025</a>
            <a href="#dq-deep-dive">90-Day Data Quality Deep Dive</a>
            <a href="#top-negative">Top 10 Recent Negative VOCs</a>
            <a href="#qa">Agent QA</a>
        </nav>
        <div style="position: absolute; bottom: 16px; left: 16px; right: 16px; font-size: 11px; color: rgba(255,255,255,0.4);">
            Generated: {datetime.now().strftime("%Y-%m-%d %H:%M PT")}<br>
            Data: {all_weeks[0] if all_weeks else 'N/A'} to {all_weeks[-1] if all_weeks else 'N/A'}
        </div>
    </div>

    <div class="main">
        <div class="header">
            <h1>&#10022; R&A VOC Analyzer — Weekly Insights</h1>
            <div class="subtitle">Customer Voice-of-Customer Analysis for Reporting & Analytics</div>
            <div class="generated">Data range: {all_weeks[0] if all_weeks else 'N/A'} to {all_weeks[-1] if all_weeks else 'N/A'} | {total_vocs:,} total VOCs across 3 channels | Generated {datetime.now().strftime("%B %d, %Y")}</div>
        </div>

        <!-- KPI Row -->
        <div class="kpi-row">
            <div class="kpi-card">
                <div class="label">Total VOCs</div>
                <div class="value">{total_vocs:,}</div>
                <div class="trend neutral">{len(all_weeks)} weeks analyzed</div>
            </div>
            <div class="kpi-card">
                <div class="label">Negative Sentiment</div>
                <div class="value" style="color: #d13438;">{neg_pct:.1f}%</div>
                <div class="trend">({total_negative:,} of {total_vocs:,})</div>
            </div>
            <div class="kpi-card">
                <div class="label">This Week</div>
                <div class="value">{len(this_week)}</div>
                <div class="trend {'up' if len(this_week) > len(last_week) else 'down' if len(this_week) < len(last_week) else 'neutral'}">
                    {'&#9650;' if len(this_week) > len(last_week) else '&#9660;' if len(this_week) < len(last_week) else '&#8212;'} vs last week ({len(last_week)})
                </div>
            </div>
            <div class="kpi-card">
                <div class="label">Data Quality Issues (90d)</div>
                <div class="value" style="color: #1e3a6e;">{len(recent_dq)}</div>
                <div class="trend">Across {len(dq_weeks)} weeks</div>
            </div>
        </div>

        <!-- TL;DR -->
        <div class="tldr" id="tldr">
            <h3>&#10022; TL;DR</h3>
            <ul>
                <li><strong>{total_vocs:,} VOCs</strong> analyzed across {len(all_weeks)} weeks from 3 channels. Sentiment: {neg_pct:.0f}% negative, {total_neutral/total_vocs*100:.0f}% neutral, {pos_pct:.0f}% positive.</li>
                <li><strong>Top category:</strong> {cat_labels.get(cat_counts.most_common(1)[0][0], 'N/A')} ({cat_counts.most_common(1)[0][1]:,} VOCs, {cat_counts.most_common(1)[0][1]/total_vocs*100:.0f}%)</li>
                <li><strong>HVC ($299+) segment:</strong> {seg_sentiment['paid_gte_299']['total']:,} VOCs with {seg_sentiment['paid_gte_299']['neg_pct']:.0f}% negative sentiment vs {seg_sentiment['paid_lt_299']['neg_pct']:.0f}% for paid &lt;$299</li>
                <li><strong>Data quality verdict (90d):</strong> {', '.join(f'{k}: {v}' for k, v in dq_verdicts.items() if v != 'insufficient_data')}</li>
            </ul>
        </div>

        <!-- Section 2: Weekly Sentiment Trend -->
        <div class="section-title" id="weekly-trend">Weekly Sentiment Trend</div>
        <div class="card">
            <h3><span class="icon">&#10022;</span> Weekly Sentiment Trend — 15 Months</h3>
            <div id="weekly-trend-chart" class="chart-container"></div>
        </div>

        <!-- Section 3: Segment Breakdown -->
        <div class="section-title" id="segments">Segment Breakdown</div>
        <div class="segment-grid">
            {"".join(f'''
            <div class="seg-card">
                <div class="seg-label">{seg_labels[seg]}</div>
                <div class="seg-value">{seg_sentiment[seg]["total"]:,}</div>
                <div class="seg-detail" style="color: #d13438;">{seg_sentiment[seg]["neg_pct"]:.0f}% negative</div>
                <div class="seg-detail">{seg_sentiment[seg]["positive"]} positive / {seg_sentiment[seg]["neutral"]} neutral</div>
            </div>''' for seg in segments)}
        </div>
        <div class="card">
            <h3><span class="icon">&#10022;</span> Sentiment by Segment</h3>
            <div id="segment-chart" class="chart-container"></div>
        </div>

        <!-- Section 4: Category Distribution -->
        <div class="section-title" id="categories">Category Distribution</div>
        <div class="card">
            <h3><span class="icon">&#10022;</span> Category Breakdown — Weekly Stacked Area</h3>
            <div id="category-chart" class="chart-container"></div>
        </div>
        <div class="card">
            <h3><span class="icon">&#10022;</span> Category Distribution — Overall</h3>
            <div id="category-pie-chart" class="chart-container"></div>
        </div>

        <!-- Section 5: Change Since Aug 2025 -->
        <div class="section-title" id="change">Change Since Aug 2025</div>
        <div class="card">
            <h3><span class="icon">&#10022;</span> Category % Change: Pre-Aug vs Post-Aug 2025</h3>
            <table class="change-table">
                <tr><th>Category</th><th>Pre-Aug 2025</th><th>Post-Aug 2025</th><th>Delta</th><th>Status</th></tr>
                {"".join(f'''<tr>
                    <td>{row["category"]}</td>
                    <td>{row["pre_aug_pct"]:.1f}%</td>
                    <td>{row["post_aug_pct"]:.1f}%</td>
                    <td style="color: {'#d13438' if row['delta'] > 0 else '#1aab68' if row['delta'] < 0 else '#6b7c93'}">{row['delta']:+.1f}pp</td>
                    <td><span class="badge {row['status']}">{row['status'].upper()}</span></td>
                </tr>''' for row in change_table)}
            </table>
        </div>

        <!-- Section 6: 90-Day Data Quality Deep Dive -->
        <div class="section-title" id="dq-deep-dive">90-Day Data Quality Deep Dive</div>
        <div class="card">
            <h3><span class="icon">&#10022;</span> Data Quality Subcategory Trends — Last 90 Days</h3>
            <div id="dq-trend-chart" class="chart-container"></div>
            <table class="change-table" style="margin-top: 16px;">
                <tr><th>Subcategory</th><th>Count (90d)</th><th>Trend</th></tr>
                {"".join(f'''<tr>
                    <td>{sub.capitalize()}</td>
                    <td>{dq_sub_counts.get(sub, 0)}</td>
                    <td><span class="badge {dq_verdicts.get(sub, 'steady')}">{dq_verdicts.get(sub, 'N/A').upper()}</span></td>
                </tr>''' for sub in dq_subcats)}
            </table>
        </div>

        <!-- Section 7: Top 10 Recent Negative VOCs -->
        <div class="section-title" id="top-negative">Top 10 Recent Negative VOCs</div>
        <div class="card">
            <h3><span class="icon">&#10022;</span> Most Recent Negative Customer Feedback</h3>
            <div class="voc-list">
                {"".join(f'''<div class="voc-item">
                    <div class="voc-meta">
                        {rec.get("timestamp", "")[:10]} | {rec.get("channel", "")} | MRR: {"$" + str(int(rec["mrr"])) if rec.get("mrr") else "N/A"} | {rec.get("category", "").replace("_", " ").title()}
                    </div>
                    <div class="voc-text">{rec.get("feedback_text", "")[:200]}{"..." if len(rec.get("feedback_text", "")) > 200 else ""}</div>
                </div>''' for rec in recent_negative)}
            </div>
        </div>

        <!-- Section 8: Agent QA Footer -->
        <div class="section-title" id="qa">Agent QA</div>
        <div class="qa-footer">
            <strong>Agent QA Footer</strong>
            <div class="qa-grid">
                <div class="qa-item">
                    <div class="qa-val">{tier_counts.get("unknown", 0) / total_vocs * 100:.1f}%</div>
                    <div>Unknown Tier</div>
                </div>
                <div class="qa-item">
                    <div class="qa-val">{sum(1 for r in records if r.get("dup_count", 1) > 1)}</div>
                    <div>Cross-Channel Dupes Found</div>
                </div>
                <div class="qa-item">
                    <div class="qa-val">3</div>
                    <div>Channels Ingested</div>
                </div>
                <div class="qa-item">
                    <div class="qa-val">{len(all_weeks)}</div>
                    <div>Weeks Analyzed</div>
                </div>
            </div>
        </div>
    </div>

    <script>
    // ── Weekly Sentiment Trend ──────────────────────────────────────────
    Plotly.newPlot('weekly-trend-chart', [
        {{
            x: {json.dumps(weekly_data["weeks"])},
            y: {json.dumps(weekly_data["negative"])},
            name: 'Negative',
            type: 'bar',
            marker: {{ color: '#f4809b' }}
        }},
        {{
            x: {json.dumps(weekly_data["weeks"])},
            y: {json.dumps(weekly_data["neutral"])},
            name: 'Neutral',
            type: 'bar',
            marker: {{ color: '#4472c4' }}
        }},
        {{
            x: {json.dumps(weekly_data["weeks"])},
            y: {json.dumps(weekly_data["positive"])},
            name: 'Positive',
            type: 'bar',
            marker: {{ color: '#00b9a9' }}
        }}
    ], {{
        barmode: 'stack',
        margin: {{ l: 50, r: 20, t: 20, b: 60 }},
        xaxis: {{ title: 'ISO Week', tickangle: -45, dtick: 4 }},
        yaxis: {{ title: 'VOC Count' }},
        font: {{ family: "'Avenir Next', -apple-system, sans-serif" }},
        legend: {{ orientation: 'h', y: 1.1 }},
        plot_bgcolor: 'rgba(0,0,0,0)',
        paper_bgcolor: 'rgba(0,0,0,0)',
    }}, {{ responsive: true }});

    // ── Segment Comparison ──────────────────────────────────────────────
    Plotly.newPlot('segment-chart', [
        {{
            x: {json.dumps([seg_labels[s] for s in segments])},
            y: {json.dumps([seg_sentiment[s]["negative"] for s in segments])},
            name: 'Negative', type: 'bar', marker: {{ color: '#f4809b' }}
        }},
        {{
            x: {json.dumps([seg_labels[s] for s in segments])},
            y: {json.dumps([seg_sentiment[s]["neutral"] for s in segments])},
            name: 'Neutral', type: 'bar', marker: {{ color: '#4472c4' }}
        }},
        {{
            x: {json.dumps([seg_labels[s] for s in segments])},
            y: {json.dumps([seg_sentiment[s]["positive"] for s in segments])},
            name: 'Positive', type: 'bar', marker: {{ color: '#00b9a9' }}
        }}
    ], {{
        barmode: 'stack',
        margin: {{ l: 50, r: 20, t: 20, b: 60 }},
        yaxis: {{ title: 'VOC Count' }},
        font: {{ family: "'Avenir Next', -apple-system, sans-serif" }},
        legend: {{ orientation: 'h', y: 1.1 }},
        plot_bgcolor: 'rgba(0,0,0,0)',
        paper_bgcolor: 'rgba(0,0,0,0)',
    }}, {{ responsive: true }});

    // ── Category Stacked Area ───────────────────────────────────────────
    Plotly.newPlot('category-chart', [
        {",".join(f'''{{
            x: {json.dumps(all_weeks)},
            y: {json.dumps(cat_weekly_data[cat])},
            name: '{cat_labels[cat]}',
            stackgroup: 'one',
            line: {{ width: 0 }},
        }}''' for cat in categories)}
    ], {{
        margin: {{ l: 50, r: 20, t: 20, b: 60 }},
        xaxis: {{ title: 'ISO Week', tickangle: -45, dtick: 4 }},
        yaxis: {{ title: 'VOC Count' }},
        font: {{ family: "'Avenir Next', -apple-system, sans-serif" }},
        legend: {{ orientation: 'h', y: 1.1 }},
        plot_bgcolor: 'rgba(0,0,0,0)',
        paper_bgcolor: 'rgba(0,0,0,0)',
        colorway: ['#1e3a6e', '#d13438', '#f5a623', '#f4809b', '#00b9a9'],
    }}, {{ responsive: true }});

    // ── Category Pie ────────────────────────────────────────────────────
    Plotly.newPlot('category-pie-chart', [{{
        values: {json.dumps([cat_counts.get(c, 0) for c in categories])},
        labels: {json.dumps([cat_labels[c] for c in categories])},
        type: 'pie',
        hole: 0.4,
        marker: {{ colors: ['#1e3a6e', '#d13438', '#f5a623', '#f4809b', '#00b9a9'] }},
        textinfo: 'label+percent',
        textposition: 'outside',
    }}], {{
        margin: {{ l: 20, r: 20, t: 20, b: 20 }},
        font: {{ family: "'Avenir Next', -apple-system, sans-serif" }},
        showlegend: false,
    }}, {{ responsive: true }});

    // ── DQ Subcategory Trend ────────────────────────────────────────────
    Plotly.newPlot('dq-trend-chart', [
        {",".join(f'''{{
            x: {json.dumps(dq_weeks)},
            y: {json.dumps(dq_trend[sub])},
            name: '{sub.capitalize()}',
            type: 'scatter',
            mode: 'lines+markers',
        }}''' for sub in dq_subcats)}
    ], {{
        margin: {{ l: 50, r: 20, t: 20, b: 60 }},
        xaxis: {{ title: 'ISO Week' }},
        yaxis: {{ title: 'Count' }},
        font: {{ family: "'Avenir Next', -apple-system, sans-serif" }},
        legend: {{ orientation: 'h', y: 1.1 }},
        plot_bgcolor: 'rgba(0,0,0,0)',
        paper_bgcolor: 'rgba(0,0,0,0)',
        colorway: ['#1e3a6e', '#4472c4', '#00b9a9', '#f5a623', '#f4809b'],
    }}, {{ responsive: true }});

    // ── Active nav highlighting ─────────────────────────────────────────
    const navLinks = document.querySelectorAll('.sidebar nav a');
    const observer = new IntersectionObserver((entries) => {{
        entries.forEach(entry => {{
            if (entry.isIntersecting) {{
                navLinks.forEach(a => a.classList.remove('active'));
                const link = document.querySelector(`.sidebar nav a[href="#${{entry.target.id}}"]`);
                if (link) link.classList.add('active');
            }}
        }});
    }}, {{ threshold: 0.3 }});
    document.querySelectorAll('.section-title, .tldr').forEach(el => observer.observe(el));
    </script>
</body>
</html>"""

    return html


# ── Main Pipeline ─────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("R&A VOC ANALYZER — Building Dashboard from Real Data")
    print("=" * 60)

    # Step 1: Read and parse all channel data
    all_records = []

    print("\n[1/6] Parsing Channel 1: #mc-reporting-analytics-feedback...")
    for f in CH1_FILES:
        filepath = TOOL_RESULTS_DIR / f
        if filepath.exists():
            raw = read_data_file(filepath)
            records = parse_ch1_messages(raw)
            all_records.extend(records)
            print(f"  - {f[:40]}... → {len(records)} messages")

    print(f"\n[1/6] Parsing Channel 2: #hvc_feedback...")
    for f in CH2_FILES:
        filepath = TOOL_RESULTS_DIR / f
        if filepath.exists():
            raw = read_data_file(filepath)
            records = parse_ch2_messages(raw)
            all_records.extend(records)
            print(f"  - {f[:40]}... → {len(records)} messages")

    print(f"\n[1/6] Parsing Channel 3: #mc-hvc-escalations...")
    for f in CH3_FILES:
        filepath = TOOL_RESULTS_DIR / f
        if filepath.exists():
            raw = read_data_file(filepath)
            records = parse_ch3_messages(raw)
            all_records.extend(records)
            print(f"  - {f[:40]}... → {len(records)} messages")

    print(f"\nTotal raw messages parsed: {len(all_records)}")

    # Step 2: Deduplicate
    print("\n[2/6] Deduplicating across channels...")
    canonical = deduplicate(all_records)
    dup_count = len(all_records) - len(canonical)
    print(f"  Removed {dup_count} duplicates → {len(canonical)} canonical VOCs")

    # Step 3: Classify
    print("\n[3/6] Classifying sentiment and categories...")
    for rec in canonical:
        rec["sentiment"] = classify_sentiment(rec)
        rec["category"], rec["subcategory"] = classify_category(rec["feedback_text"])
        rec["customer_tier"] = compute_customer_tier(rec.get("mrr"))

    sent_counts = Counter(r["sentiment"] for r in canonical)
    cat_counts = Counter(r["category"] for r in canonical)
    print(f"  Sentiment: {dict(sent_counts)}")
    print(f"  Categories: {dict(cat_counts)}")

    # Step 4: Compute analytics
    print("\n[4/6] Computing analytics and trends...")
    snapshots = compute_snapshots(canonical)
    trends = compute_trends(canonical)
    print(f"  Weekly snapshots: {len(snapshots)} weeks")
    print(f"  Monthly periods: {len(trends['monthly'])}")

    # Step 5: Generate dashboard
    print("\n[5/6] Generating interactive HTML dashboard...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    html = generate_dashboard(canonical, trends, snapshots)
    output_path = OUTPUT_DIR / "index.html"
    output_path.write_text(html)
    print(f"  Dashboard saved to: {output_path}")

    # Save archive copy
    current_week = max(r["iso_week"] for r in canonical)
    archive_path = ARCHIVE_DIR / f"{current_week}.html"
    archive_path.write_text(html)
    print(f"  Archive saved to: {archive_path}")

    # Save data summary
    summary = {
        "generated_at": datetime.now().isoformat(),
        "total_raw": len(all_records),
        "total_canonical": len(canonical),
        "duplicates_removed": dup_count,
        "weeks": sorted(set(r["iso_week"] for r in canonical)),
        "sentiment": dict(sent_counts),
        "categories": dict(cat_counts),
    }
    (OUTPUT_DIR / "index.json").write_text(json.dumps(summary, indent=2))

    print("\n[6/6] Complete!")
    print(f"\n{'=' * 60}")
    print(f"Dashboard: file://{output_path}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
