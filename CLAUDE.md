# R&A VOC Analyzer

## Purpose
Weekly automated agent that ingests customer VOC from 3 Slack channels, deduplicates, classifies sentiment + root-cause, segments by MRR tier, computes trends, and delivers a GitHub Pages dashboard + PDF report every Friday at 4 PM PST.

## Architecture
- **Python 3.11** — no LangChain/LlamaIndex
- **SQLite WAL** — `data/voc.db`
- **Claude Sonnet 4.6** via `instructor` for classification only
- **Pydantic v2** contracts for all data models
- **Plotly + Jinja2 + weasyprint** for HTML dashboard + PDF
- **GitHub Actions** cron for scheduling, **GitHub Pages** for hosting

## Slack Channels
- `#mc-reporting-analytics-feedback` (C06SW7512P2) — Qualtrics surveys
- `#hvc_feedback` (C051Y4H98VB) — HVC $299+ surveys (overlaps with channel 1)
- `#mc-hvc-escalations` (C095FJ3SQF4) — Structured escalations (started July 2025)

## Key Commands
```bash
voc-agent ingest                    # Incremental fetch
voc-agent ingest --backfill 2025-01-01  # Full backfill
voc-agent classify                  # Classify unclassified VOCs
voc-agent analyze                   # Compute snapshots
voc-agent report                    # Generate dashboard + PDF
voc-agent run-all                   # Full pipeline
pytest tests/unit/ -v               # Unit tests
pytest tests/property/ -v           # Property tests
pytest tests/contract/ -v           # Data contract validation
```

## Design System
Uses Intuit FY27 design tokens — see `config/` for color values.

## Testing
- 6 mandatory QA gates must ALL pass before dashboard publish
- Golden dataset: `tests/fixtures/golden_vocs.jsonl` (300 hand-labeled VOCs)
- Prompt regression via promptfoo config

## Categories (locked — 5 exactly)
1. `feature_gap` — missing/removed features
2. `bug_or_error` — broken behavior
3. `data_quality` — accuracy, consistency, availability, freshness, coverage
4. `performance_ux` — slowness, confusing UX
5. `other_or_praise` — positive/generic/off-topic
