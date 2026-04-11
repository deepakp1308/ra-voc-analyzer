"""SQLite database schema, connection management, and migrations."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import structlog

logger = structlog.get_logger()

DEFAULT_DB_PATH = Path("data/voc.db")

SCHEMA_VERSION = 1

SCHEMA_SQL = """
-- ── raw_messages ─────────────────────────────────────────────────────────────
-- One row per Slack message seen, before dedup.
CREATE TABLE IF NOT EXISTS raw_messages (
    id                    TEXT PRIMARY KEY,   -- f"{channel_id}:{ts}"
    channel_id            TEXT NOT NULL,
    channel_name          TEXT NOT NULL,
    ts                    REAL NOT NULL,      -- Slack ts (unix float)
    posted_at_utc         TEXT NOT NULL,      -- ISO timestamp
    posted_at_pt          TEXT NOT NULL,      -- ISO timestamp in America/Los_Angeles
    iso_week              TEXT NOT NULL,      -- '2025-W03'
    author_id             TEXT,
    author_name           TEXT,
    text                  TEXT NOT NULL,
    thread_ts             REAL,
    permalink             TEXT,
    -- Parsed fields (extracted by channel-specific parser)
    parsed_user_id        TEXT,
    parsed_mrr            REAL,
    parsed_plan           TEXT,
    parsed_csat_raw       TEXT,
    parsed_feedback       TEXT,
    parsed_survey_type    TEXT,
    parsed_page_url       TEXT,
    parsed_fullstory_url  TEXT,
    -- Channel 3 specific
    parsed_customer_name  TEXT,
    parsed_criticality    TEXT,
    parsed_impacted_product TEXT,
    parsed_goal           TEXT,
    parsed_constraints    TEXT,
    parsed_prs_score      INTEGER,
    parsed_prs_reason     TEXT,
    fetched_at_utc        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_week ON raw_messages(iso_week);
CREATE INDEX IF NOT EXISTS idx_raw_channel_ts ON raw_messages(channel_id, ts);
CREATE INDEX IF NOT EXISTS idx_raw_user_id ON raw_messages(parsed_user_id);

-- ── canonical_vocs ───────────────────────────────────────────────────────────
-- Post-dedup entity. One row per unique VOC.
CREATE TABLE IF NOT EXISTS canonical_vocs (
    voc_id                TEXT PRIMARY KEY,   -- uuid4
    canonical_text        TEXT NOT NULL,
    content_hash          TEXT NOT NULL,      -- sha256(normalized_text)
    first_seen_utc        TEXT NOT NULL,
    last_seen_utc         TEXT NOT NULL,
    iso_week_first_seen   TEXT NOT NULL,
    source_message_ids    TEXT NOT NULL,      -- JSON array of raw_messages.id
    dup_count             INTEGER NOT NULL DEFAULT 1,
    customer_id           TEXT,              -- nullable
    customer_tier         TEXT NOT NULL DEFAULT 'unknown',
    mrr_usd               REAL,
    enrichment_source     TEXT               -- inline | csv | none
);

CREATE INDEX IF NOT EXISTS idx_canon_hash ON canonical_vocs(content_hash);
CREATE INDEX IF NOT EXISTS idx_canon_week ON canonical_vocs(iso_week_first_seen);
CREATE INDEX IF NOT EXISTS idx_canon_tier ON canonical_vocs(customer_tier);

-- ── classifications ──────────────────────────────────────────────────────────
-- LLM outputs, versioned by prompt hash for reproducibility.
CREATE TABLE IF NOT EXISTS classifications (
    voc_id                TEXT NOT NULL,
    classifier_version    TEXT NOT NULL,      -- e.g. 'v1.0-sonnet-4.6'
    prompt_hash           TEXT NOT NULL,
    sentiment             TEXT NOT NULL,      -- positive | neutral | negative
    sentiment_score       REAL NOT NULL,      -- -1.0 .. 1.0
    category              TEXT NOT NULL,      -- one of 5 locked categories
    subcategory           TEXT,              -- required iff category='data_quality'
    confidence            REAL NOT NULL,      -- 0..1
    rationale             TEXT,
    classified_at         TEXT NOT NULL,
    PRIMARY KEY (voc_id, classifier_version)
);

CREATE INDEX IF NOT EXISTS idx_class_sentiment ON classifications(sentiment);
CREATE INDEX IF NOT EXISTS idx_class_category ON classifications(category);

-- ── snapshots ────────────────────────────────────────────────────────────────
-- Materialized weekly rollup. One row per (iso_week x segment x category x sentiment).
-- This is what the report reads from.
CREATE TABLE IF NOT EXISTS snapshots (
    iso_week              TEXT NOT NULL,
    segment               TEXT NOT NULL,      -- all | free | paid | paid_gte_299 | paid_lt_299
    category              TEXT NOT NULL,
    sentiment             TEXT NOT NULL,
    voc_count             INTEGER NOT NULL,
    unique_customers      INTEGER NOT NULL,
    mean_confidence       REAL NOT NULL,
    PRIMARY KEY (iso_week, segment, category, sentiment)
);

CREATE INDEX IF NOT EXISTS idx_snap_week ON snapshots(iso_week);
CREATE INDEX IF NOT EXISTS idx_snap_segment ON snapshots(segment);

-- ── ingestion_state ──────────────────────────────────────────────────────────
-- Tracks last fetched timestamp per channel for incremental ingestion.
CREATE TABLE IF NOT EXISTS ingestion_state (
    channel_id            TEXT PRIMARY KEY,
    last_message_ts       REAL,             -- Slack ts of last fetched message
    last_run_at           TEXT,             -- ISO timestamp
    total_messages_fetched INTEGER DEFAULT 0,
    total_messages_stored  INTEGER DEFAULT 0
);

-- ── runs ─────────────────────────────────────────────────────────────────────
-- Observability: metadata for each pipeline run.
CREATE TABLE IF NOT EXISTS runs (
    run_id                TEXT PRIMARY KEY,
    run_date              TEXT NOT NULL,      -- ISO date of the Friday this run covers
    started_at            TEXT NOT NULL,
    ended_at              TEXT,
    status                TEXT NOT NULL DEFAULT 'running',  -- running | success | failed
    rows_ingested         INTEGER DEFAULT 0,
    rows_deduped          INTEGER DEFAULT 0,
    rows_classified       INTEGER DEFAULT 0,
    tokens_used           INTEGER DEFAULT 0,
    cost_usd              REAL DEFAULT 0.0,
    error_message         TEXT,
    report_path           TEXT
);

-- ── schema_version ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);
"""


def get_db_path() -> Path:
    """Return the database path, creating parent directories if needed."""
    path = Path(DEFAULT_DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


@contextmanager
def get_connection(db_path: Path | None = None) -> Generator[sqlite3.Connection, None, None]:
    """Context manager for SQLite connection with WAL mode and foreign keys."""
    path = db_path or get_db_path()
    conn = sqlite3.connect(str(path), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Path | None = None) -> None:
    """Initialize database schema. Safe to call multiple times (IF NOT EXISTS)."""
    path = db_path or get_db_path()
    with get_connection(path) as conn:
        conn.executescript(SCHEMA_SQL)
        # Record schema version
        existing = conn.execute(
            "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
        ).fetchone()
        if existing is None or existing[0] < SCHEMA_VERSION:
            conn.execute(
                "INSERT OR REPLACE INTO schema_version (version, applied_at) VALUES (?, datetime('now'))",
                (SCHEMA_VERSION,),
            )
    logger.info("database_initialized", path=str(path), schema_version=SCHEMA_VERSION)


def row_count(conn: sqlite3.Connection, table: str) -> int:
    """Return the row count for a table."""
    result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608
    return result[0] if result else 0
