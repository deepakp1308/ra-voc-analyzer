"""Report integrity checks (Gate 4).

Validates the generated HTML dashboard and PDF before publishing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger()

REQUIRED_SECTIONS = [
    "TL;DR",
    "Weekly Sentiment Trend",
    "Segment Breakdown",
    "Category Distribution",
    "Change Since Aug 2025",
    "90-Day Data Quality Deep Dive",
    "Top 10 Recent Negative VOCs",
    "Agent QA",
]

MIN_HTML_SIZE = 10_000  # 10KB minimum
MIN_PDF_SIZE = 50_000   # 50KB minimum


def check_report_integrity(
    html_path: Path | None = None,
    pdf_path: Path | None = None,
) -> dict[str, Any]:
    """Run all report integrity checks.

    Returns dict with status (PASS/FAIL) and list of failures.
    """
    failures: list[str] = []

    # Default paths
    if html_path is None:
        html_path = Path("docs/index.html")
    if pdf_path is None:
        # Find most recent PDF
        reports_dir = Path("reports")
        pdfs = sorted(reports_dir.glob("*.pdf")) if reports_dir.exists() else []
        pdf_path = pdfs[-1] if pdfs else None

    # Check HTML exists and is non-trivial
    if not html_path.exists():
        failures.append(f"HTML report not found at {html_path}")
    else:
        html_size = html_path.stat().st_size
        if html_size < MIN_HTML_SIZE:
            failures.append(f"HTML report too small ({html_size} bytes < {MIN_HTML_SIZE})")

        html_content = html_path.read_text()

        # Check all 8 sections present
        for section in REQUIRED_SECTIONS:
            if section not in html_content:
                failures.append(f"Missing report section: '{section}'")

        # Check Plotly chart containers exist
        if 'id="weekly-trend-chart"' not in html_content:
            failures.append("Missing weekly-trend-chart container")
        if 'id="category-chart"' not in html_content:
            failures.append("Missing category-chart container")

        # Check Plotly library loaded
        if "plotly" not in html_content.lower():
            failures.append("Plotly library not loaded in HTML")

        # Check design tokens are present
        if "#1e3a6e" not in html_content:  # navy
            failures.append("Intuit design token (navy) not found in HTML")

    # Check PDF exists and is non-trivial
    if pdf_path and pdf_path.exists():
        pdf_size = pdf_path.stat().st_size
        if pdf_size < MIN_PDF_SIZE:
            failures.append(f"PDF report too small ({pdf_size} bytes < {MIN_PDF_SIZE})")
    elif pdf_path:
        failures.append(f"PDF report not found at {pdf_path}")
    # PDF is optional — don't fail if weasyprint isn't available

    # Check archive index
    archive_index = Path("docs/index.json")
    if archive_index.exists():
        try:
            data = json.loads(archive_index.read_text())
            if not data.get("weeks"):
                failures.append("Archive index has no weeks listed")
        except json.JSONDecodeError:
            failures.append("Archive index.json is not valid JSON")

    status = "PASS" if not failures else "FAIL"
    result = {"status": status, "failure_count": len(failures), "failures": failures}

    if failures:
        logger.error("report_integrity_check_failed", **result)
    else:
        logger.info("report_integrity_check_passed")

    return result
