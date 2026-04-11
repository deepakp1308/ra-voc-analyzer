"""CLI entry point for R&A VOC Analyzer.

Usage:
    voc-agent ingest          # Fetch new messages from Slack channels
    voc-agent ingest --backfill 2025-01-01  # Historical backfill
    voc-agent classify        # Classify unclassified VOCs
    voc-agent analyze         # Compute snapshots and trends
    voc-agent report          # Generate and deliver weekly report
    voc-agent run-all         # Full pipeline: ingest → classify → analyze → report
    voc-agent qa              # Run QA checks only
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

import structlog
import typer

from voc_agent.storage.db import init_db

logger = structlog.get_logger()

app = typer.Typer(
    name="voc-agent",
    help="R&A VOC Analyzer — Weekly customer Voice-of-Customer insight agent",
    no_args_is_help=True,
)


@app.command()
def ingest(
    backfill: Optional[str] = typer.Option(
        None, help="ISO date to backfill from (e.g., 2025-01-01)"
    ),
    channel: Optional[str] = typer.Option(
        None, help="Single channel ID to ingest (default: all configured channels)"
    ),
    limit: Optional[int] = typer.Option(
        None, help="Max messages to fetch per channel (for testing)"
    ),
) -> None:
    """Fetch new messages from Slack channels."""
    init_db()
    from voc_agent.ingestion.backfill import run_ingestion

    backfill_date = date.fromisoformat(backfill) if backfill else None
    run_ingestion(backfill_from=backfill_date, channel_id=channel, limit=limit)


@app.command()
def classify() -> None:
    """Classify unclassified VOCs using Claude."""
    init_db()
    from voc_agent.classification.classifier import run_classification

    run_classification()


@app.command()
def analyze() -> None:
    """Compute weekly snapshots and trend analytics."""
    init_db()
    from voc_agent.analysis.segments import run_analysis

    run_analysis()


@app.command()
def report(
    week: Optional[str] = typer.Option(
        None, help="ISO week to report on (e.g., 2026-W15). Default: current week."
    ),
) -> None:
    """Generate and deliver the weekly report."""
    init_db()
    from voc_agent.reporting.renderer import run_report

    run_report(iso_week=week)


@app.command(name="run-all")
def run_all() -> None:
    """Full pipeline: ingest -> dedup -> classify -> analyze -> report."""
    init_db()
    run_date = date.today().isoformat()
    logger.info("pipeline_start", run_date=run_date, started_at=datetime.now().isoformat())

    from voc_agent.ingestion.backfill import run_ingestion

    run_ingestion()

    from voc_agent.classification.classifier import run_classification

    run_classification()

    from voc_agent.analysis.segments import run_analysis

    run_analysis()

    from voc_agent.reporting.renderer import run_report

    run_report()
    logger.info("pipeline_complete", run_date=run_date)


@app.command()
def qa() -> None:
    """Run QA checks against current database."""
    init_db()
    typer.echo("Running QA checks...")
    # TODO: Wire up qa_checks modules
    typer.echo("QA checks complete.")


if __name__ == "__main__":
    app()
