"""Report renderer: assembles the weekly HTML dashboard with Plotly charts.

Generates:
- docs/index.html (current week, deployed to GitHub Pages)
- docs/archive/{iso_week}.html (historical archive)
- reports/{iso_week}.pdf (PDF via weasyprint)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import structlog
from jinja2 import Environment, FileSystemLoader

from voc_agent.analysis.change_detection import compute_change_deltas
from voc_agent.analysis.deep_dive import compute_dq_deep_dive
from voc_agent.analysis.trends import (
    compute_mom_delta,
    compute_qoq_delta,
    compute_weekly_trends,
    compute_yoy_delta,
)
from voc_agent.storage.db import get_connection

logger = structlog.get_logger()

TEMPLATE_DIR = Path("src/voc_agent/reporting/templates")
DOCS_DIR = Path("docs")
REPORTS_DIR = Path("reports")

# Intuit FY27 design tokens
COLORS = {
    "navy": "#1e3a6e",
    "blue": "#4472c4",
    "teal": "#00b9a9",
    "pink": "#f4809b",
    "green": "#1aab68",
    "red": "#d13438",
    "light_blue": "#9abde0",
    "bg": "#f0f4f8",
    "card": "#ffffff",
    "text_primary": "#1a1f36",
    "text_secondary": "#6b7c93",
}

SENTIMENT_COLORS = {
    "positive": COLORS["teal"],
    "neutral": COLORS["blue"],
    "negative": COLORS["pink"],
}

CATEGORY_COLORS = {
    "feature_gap": COLORS["navy"],
    "bug_or_error": COLORS["red"],
    "data_quality": COLORS["pink"],
    "performance_ux": COLORS["blue"],
    "other_or_praise": COLORS["teal"],
}


def build_report_data(conn: Any, iso_week: str | None = None) -> dict[str, Any]:
    """Assemble all data needed for the report template."""
    # Load snapshots
    snapshots_df = pd.read_sql_query("SELECT * FROM snapshots", conn)
    if snapshots_df.empty:
        logger.warning("no_snapshots_for_report")
        return {}

    current_week = iso_week or snapshots_df["iso_week"].max()

    # Build report sections
    data: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "current_week": current_week,
        "total_weeks": snapshots_df["iso_week"].nunique(),
    }

    # Section 1: TL;DR
    data["tldr"] = _build_tldr(snapshots_df, current_week)

    # Section 2: Weekly sentiment trend chart
    data["weekly_trend_chart"] = _build_weekly_sentiment_chart(snapshots_df)

    # Section 3: Segment cuts
    data["segment_cuts"] = _build_segment_summary(snapshots_df, current_week)

    # Section 4: Category breakdown chart
    data["category_chart"] = _build_category_chart(snapshots_df)

    # Section 5: Since-Aug-2025 delta table
    data["change_deltas"] = {}
    for seg in ["all", "paid", "paid_gte_299", "paid_lt_299"]:
        data["change_deltas"][seg] = compute_change_deltas(snapshots_df, seg)

    # Section 6: 90-day DQ deep dive
    data["dq_deep_dive"] = compute_dq_deep_dive(conn)

    # Section 7: Top recent negative VOCs
    data["top_negatives"] = _get_top_negative_vocs(conn, n=10)

    # Trend deltas
    data["mom"] = compute_mom_delta(snapshots_df)
    data["qoq"] = compute_qoq_delta(snapshots_df)
    data["yoy"] = compute_yoy_delta(snapshots_df)

    return data


def render_html(data: dict[str, Any]) -> str:
    """Render the report HTML from the Jinja2 template."""
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    env.filters["tojson_safe"] = lambda v: json.dumps(v, default=str)
    template = env.get_template("report.html.j2")
    return template.render(**data, colors=COLORS)


def save_report(html: str, iso_week: str) -> dict[str, Path]:
    """Save the report to docs/ (GitHub Pages) and reports/ (PDF)."""
    paths: dict[str, Path] = {}

    # Ensure directories exist
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    (DOCS_DIR / "archive").mkdir(exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Save current week as index.html
    index_path = DOCS_DIR / "index.html"
    index_path.write_text(html)
    paths["index"] = index_path

    # Save to archive
    archive_path = DOCS_DIR / "archive" / f"{iso_week}.html"
    archive_path.write_text(html)
    paths["archive"] = archive_path

    # Generate PDF
    try:
        from weasyprint import HTML
        pdf_path = REPORTS_DIR / f"{iso_week}.pdf"
        HTML(string=html).write_pdf(str(pdf_path))
        paths["pdf"] = pdf_path
        logger.info("pdf_generated", path=str(pdf_path))
    except ImportError:
        logger.warning("weasyprint_not_available", msg="PDF generation skipped")
    except Exception as e:
        logger.warning("pdf_generation_failed", error=str(e))

    logger.info("report_saved", paths={k: str(v) for k, v in paths.items()})
    return paths


def run_report(iso_week: str | None = None) -> None:
    """Main entry point: generate and save the weekly report."""
    with get_connection() as conn:
        data = build_report_data(conn, iso_week)
        if not data:
            logger.warning("empty_report_data")
            return

        html = render_html(data)
        paths = save_report(html, data["current_week"])

        # Update archive index
        _update_archive_index()

        logger.info("report_complete", week=data["current_week"])


# ── Chart Builders ────────────────────────────────────────────────────────────

def _build_weekly_sentiment_chart(snapshots_df: pd.DataFrame) -> str:
    """Build stacked bar chart JSON config for weekly sentiment trends."""
    weekly = compute_weekly_trends(snapshots_df, "all")
    if weekly.empty:
        return "{}"

    weeks = weekly["iso_week"].tolist()
    fig = go.Figure()

    for sentiment, color in SENTIMENT_COLORS.items():
        if sentiment in weekly.columns:
            fig.add_trace(go.Bar(
                name=sentiment.capitalize(),
                x=weeks,
                y=weekly[sentiment].tolist(),
                marker_color=color,
            ))

    fig.update_layout(
        barmode="stack",
        title="Weekly VOC Volume by Sentiment",
        xaxis_title="Week",
        yaxis_title="VOC Count",
        plot_bgcolor=COLORS["bg"],
        paper_bgcolor=COLORS["card"],
        font=dict(family="Avenir Next, -apple-system, Helvetica Neue, Arial"),
    )

    return fig.to_json()


def _build_category_chart(snapshots_df: pd.DataFrame) -> str:
    """Build 100% stacked area chart for category breakdown."""
    seg_df = snapshots_df[snapshots_df["segment"] == "all"]
    if seg_df.empty:
        return "{}"

    weekly_cat = (
        seg_df.groupby(["iso_week", "category"])["voc_count"]
        .sum()
        .reset_index()
        .pivot(index="iso_week", columns="category", values="voc_count")
        .fillna(0)
        .sort_index()
    )

    # Normalize to 100%
    row_totals = weekly_cat.sum(axis=1)
    weekly_pct = weekly_cat.div(row_totals.replace(0, 1), axis=0) * 100

    fig = go.Figure()
    for cat, color in CATEGORY_COLORS.items():
        if cat in weekly_pct.columns:
            fig.add_trace(go.Scatter(
                name=cat.replace("_", " ").title(),
                x=weekly_pct.index.tolist(),
                y=weekly_pct[cat].tolist(),
                mode="lines",
                stackgroup="one",
                line=dict(color=color),
                fillcolor=color,
            ))

    fig.update_layout(
        title="Category Distribution (% of Weekly VOCs)",
        xaxis_title="Week",
        yaxis_title="% of VOCs",
        yaxis=dict(range=[0, 100]),
        plot_bgcolor=COLORS["bg"],
        paper_bgcolor=COLORS["card"],
        font=dict(family="Avenir Next, -apple-system, Helvetica Neue, Arial"),
    )

    return fig.to_json()


# ── Helper Functions ──────────────────────────────────────────────────────────

def _build_tldr(snapshots_df: pd.DataFrame, current_week: str) -> dict[str, Any]:
    """Build TL;DR section: 4 key bullets."""
    week_data = snapshots_df[
        (snapshots_df["iso_week"] == current_week) & (snapshots_df["segment"] == "all")
    ]

    total = int(week_data["voc_count"].sum()) if not week_data.empty else 0
    sentiment_mix = {}
    for s in ["positive", "neutral", "negative"]:
        count = int(week_data[week_data["sentiment"] == s]["voc_count"].sum())
        sentiment_mix[s] = count

    return {
        "total_vocs": total,
        "sentiment_mix": sentiment_mix,
        "current_week": current_week,
    }


def _build_segment_summary(
    snapshots_df: pd.DataFrame, current_week: str
) -> dict[str, dict[str, Any]]:
    """Build summary stats per segment for the current week."""
    summaries = {}
    for seg in ["all", "free", "paid", "paid_gte_299", "paid_lt_299"]:
        seg_week = snapshots_df[
            (snapshots_df["iso_week"] == current_week) & (snapshots_df["segment"] == seg)
        ]
        total = int(seg_week["voc_count"].sum())
        neg = int(seg_week[seg_week["sentiment"] == "negative"]["voc_count"].sum())
        summaries[seg] = {
            "total": total,
            "negative": neg,
            "negative_ratio": round(neg / max(total, 1) * 100, 1),
        }
    return summaries


def _get_top_negative_vocs(conn: Any, n: int = 10) -> list[dict[str, Any]]:
    """Get the top N most recent negative VOCs with details."""
    rows = conn.execute("""
        SELECT
            cv.canonical_text,
            cv.mrr_usd,
            cv.customer_tier,
            cv.iso_week_first_seen,
            c.category,
            c.subcategory,
            c.confidence,
            c.rationale
        FROM canonical_vocs cv
        JOIN classifications c ON cv.voc_id = c.voc_id
        WHERE c.sentiment = 'negative'
        ORDER BY cv.first_seen_utc DESC
        LIMIT ?
    """, (n,)).fetchall()

    return [
        {
            "text": r["canonical_text"][:200],
            "mrr": r["mrr_usd"],
            "tier": r["customer_tier"],
            "week": r["iso_week_first_seen"],
            "category": r["category"],
            "subcategory": r["subcategory"],
            "confidence": r["confidence"],
        }
        for r in rows
    ]


def _update_archive_index() -> None:
    """Update docs/index.json with list of available archive weeks."""
    archive_dir = DOCS_DIR / "archive"
    weeks = sorted([
        f.stem for f in archive_dir.glob("*.html")
    ])
    index_path = DOCS_DIR / "index.json"
    index_path.write_text(json.dumps({"weeks": weeks, "count": len(weeks)}))
