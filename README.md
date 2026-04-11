# R&A VOC Analyzer Agent

**Automated Voice-of-Customer insight engine for Mailchimp Reporting & Analytics**

A production-grade agent that ingests customer feedback from 3 Slack channels, deduplicates cross-channel VOCs, classifies sentiment and root-cause categories, segments by customer tier and MRR, computes multi-horizon trends, and delivers an interactive insight dashboard every week.

> **Design goals:** accuracy > novelty, reproducibility > cleverness, observability > silence.

---

## Dashboard

**Live:** [GitHub Pages Dashboard](https://{org}.github.io/ra-voc-analyzer/)

The dashboard updates automatically every **Sunday at 11:00 PM PST** and is ready by **Monday 1:00 AM PST** — 52 weeks of continuous operation (April 2026 through April 2027).

---

## What This Agent Does

### Data Ingestion
- Pulls customer feedback from **3 Slack channels** using the Slack API with paginated, rate-limited requests
- Handles incremental ingestion (last 14 days with 7-day overlap safety net) and full historical backfill (Jan 2025 onward)
- Parses **3 distinct message formats** with channel-specific regex parsers:
  - **Qualtrics In-App Survey/Badge** — CSAT ratings, MRR, plan, feedback text
  - **HVC Feedback** — CSAT/PRS surveys for $299+ MRR customers
  - **HVC Escalations** — structured product feedback with criticality (P0-P3)

### Cross-Channel Deduplication
- **Pass 1 (Hash):** SHA256 of normalized feedback text — catches copy-paste forwards across channels (covers ~85%+ of duplicates)
- **Pass 2 (Semantic):** RapidFuzz token_set_ratio within 7-day window — catches paraphrased duplicates (feature-flagged)
- Prefers HVC channel version as canonical record for richer metadata
- Never silently merges — suspected duplicates logged for manual review

### Sentiment Classification
- **CSAT Mapping (primary):** Deterministic mapping from survey ratings (Terrible/Poor/Average/Good/Excellent) with i18n support for 6 languages (English, Spanish, German, French, Portuguese, Italian)
- **PRS Score Mapping:** Product Recommendation Score (0-10) mapped to sentiment
- **Claude AI Classification (for unrated feedback):** Instructor-powered structured output with Pydantic validation, auto-retry on malformed responses
- **Confidence threshold:** Below 0.6 defaults to neutral — no guessing

### Root-Cause Categorization
5 locked categories (no category drift):

| Category | What It Captures |
|----------|-----------------|
| `feature_gap` | Missing features, feature requests, removed/deprecated features |
| `bug_or_error` | Broken behavior, error messages, crashes, wrong results |
| `data_quality` | Accuracy, consistency, availability, freshness, coverage (bot/MPP filtering) |
| `performance_ux` | Slowness, latency, confusing navigation, too many clicks |
| `other_or_praise` | Positive feedback, generic commentary, off-topic |

**Data Quality Subcategories** (powers the 90-day deep dive):
- `accuracy` — numbers are wrong
- `consistency` — same metric, different values across surfaces
- `availability` — data missing or not loading
- `freshness` — stale or delayed data
- `coverage` — bot/MPP filtering gaps, attribution issues

### Customer Segmentation
Every metric is computed across **5 segments:**

| Segment | Definition |
|---------|-----------|
| All Customers | Everyone including free, paid, and unknown tier |
| Free | MRR = $0 or null with Free plan |
| All Paid | MRR > $0 |
| Paid >= $299 | High-Value Customers (HVC threshold) |
| Paid < $299 | Standard paid customers |

MRR is extracted directly from Slack messages (inline in Qualtrics surveys). CSV fallback for records with missing MRR.

### Trend Analytics
| View | Definition |
|------|-----------|
| Weekly | Count & % by segment x category x sentiment, ISO weeks |
| Month-over-Month | Current vs prior calendar month, absolute + % delta |
| Quarter-over-Quarter | Rolling 13-week windows |
| Year-over-Year | Same ISO week prior year, 4-week smoothed |
| Since Aug 2025 | Pre/post comparison with improved/degraded/steady labels |

### 90-Day Data Quality Deep Dive
The critical question this agent answers:

> *"In the last 90 days, is data quality / availability / consistency getting better or worse, and in which subcategories?"*

- Weekly `data_quality` negative-sentiment counts for last 13 weeks, broken by subcategory
- Linear regression slope per subcategory to determine trend direction:
  - **Reduced:** slope < 0 AND last-4-week mean < first-4-week mean x 0.85
  - **Increased:** slope > 0 AND last-4-week mean > first-4-week mean x 1.15
  - **Steady:** otherwise
- Top 5 verbatim examples per subcategory (most recent, highest confidence)

### Dashboard Report (8 Sections)
1. **TL;DR** — 4 bullets: total VOCs, sentiment mix, biggest mover, data-quality verdict
2. **Weekly Sentiment Trend** — interactive stacked bar chart, 15+ months
3. **Segment Breakdown** — small multiples across all 5 customer segments
4. **Category Distribution** — 100% stacked area + donut chart, weekly
5. **Change Since Aug 2025** — improved/degraded/steady per category x segment
6. **90-Day Data Quality Deep Dive** — subcategory slopes + trend labels
7. **Top 10 Recent Negative VOCs** — verbatim quotes with metadata
8. **Agent QA Footer** — unknown-tier %, dedup rate, confidence distribution, run duration

---

## Data Sources

| Channel | Slack ID | Format | Start Date | Volume |
|---------|----------|--------|------------|--------|
| #mc-reporting-analytics-feedback | C06SW7512P2 | Qualtrics In-App Survey/Badge | Jan 2025 | ~5-10/day |
| #hvc_feedback | C051Y4H98VB | Qualtrics CSAT/PRS/Badge (HVC $299+) | Jan 2025 | ~5-10/day |
| #mc-hvc-escalations | C095FJ3SQF4 | Structured HVC Escalations | Jul 2025 | ~2-5/week |

---

## Architecture

```
Slack API (3 channels)          Claude Sonnet 4.6
        |                            |
        v                            v
+--------------------------------------------------+
| Ingestion Layer                                  |
| slack_client.py -> 3 channel parsers             |
| -> dedup (hash + semantic) -> enrichment         |
+--------------------------------------------------+
        |
        v
+--------------------------------------------------+
| SQLite (voc.db) — WAL mode                       |
| raw_messages | canonical_vocs | classifications  |
| snapshots (materialized weekly rollup)           |
+--------------------------------------------------+
        |
        v
+--------------------------------------------------+
| Analysis Layer (pandas + pandera)                |
| segments | trends | change_detection | deep_dive |
+--------------------------------------------------+
        |
        v
+--------------------------------------------------+
| Report Renderer (Jinja2 + Plotly)                |
| HTML dashboard | Slack DM + Block Kit TL;DR      |
+--------------------------------------------------+
        |
        v
   GitHub Pages (auto-deployed weekly)
```

### Tech Stack

| Concern | Choice | Why |
|---------|--------|-----|
| Language | Python 3.11 | Matches R&A tooling |
| LLM | Claude Sonnet 4.6 via `instructor` | Pydantic-validated structured output |
| Slack | `slack_sdk` | Official SDK, handles pagination |
| Retry | `tenacity` | Exponential backoff for API calls |
| Text Processing | `clean-text` + `ftfy` + `RapidFuzz` | Normalization + fuzzy dedup |
| Language Detection | `lingua-py` | Best accuracy on short text |
| Storage | SQLite WAL | Zero-ops, file-based |
| Validation | Pydantic v2 + `pandera` | Data contracts at every stage |
| Charts | Plotly | Interactive HTML charts |
| Scheduling | GitHub Actions cron | Auditable, secrets vault |
| Prompt Eval | `promptfoo` | CI regression testing |
| Testing | pytest + hypothesis + `deepeval` | Unit + property + LLM eval |

**No LangChain/LlamaIndex.** LLMs are used only as classifiers inside typed Pydantic contracts.

---

## Schedule

| When | What |
|------|------|
| **Sunday 11:00 PM PST** | Pipeline starts: ingest, classify, analyze, render |
| **Monday 1:00 AM PST** | Dashboard published to GitHub Pages, Slack DM sent |
| **Duration** | 52 weeks (April 2026 through April 2027) |

The GitHub Actions workflow runs every Monday at 07:00 UTC (= Sunday 11 PM PST). The full pipeline completes in ~60 minutes. If any QA gate fails, the dashboard is NOT published and Deepak receives a failure notification via Slack DM.

---

## Quality Gates (6 Mandatory)

**All must pass before the dashboard is published. No exceptions.**

| Gate | What It Checks |
|------|---------------|
| **Gate 1: Unit Tests** | 199 tests across 12 modules — parsers, dedup, enrichment, classification, trends, reports |
| **Gate 2: Property Tests** | Hypothesis-based: dedup idempotency, segment additivity, classification invariants |
| **Gate 3: Data Contracts** | Pandera validation on pipeline output — no empty weeks, tier share limits, confidence thresholds |
| **Gate 4: Report Integrity** | HTML structure, all 8 sections present, Plotly configs valid, design tokens correct |
| **Gate 5: Prompt Regression** | promptfoo eval: F1 >= 0.80 sentiment, >= 0.75 category (on classifier prompt changes) |
| **Gate 6: Cost Budget** | Claude API cost < $25/run, runtime < 30 min, Slack API calls < 500 |

---

## Quick Start

### Prerequisites
- Python 3.11+
- Slack Bot Token with scopes: `channels:history`, `groups:history`, `users:read`, `chat:write`, `files:write`
- Anthropic API Key (Claude Sonnet 4.6)

### Setup

```bash
# Clone and install
git clone <repo-url> && cd ra-voc-analyzer
cp .env.example .env
# Fill in SLACK_BOT_TOKEN, ANTHROPIC_API_KEY, REPORT_RECIPIENT_SLACK_ID

pip install -e ".[dev]"
```

### Run the Pipeline

```bash
# Full backfill (first time — fetches all data from Jan 2025)
voc-agent ingest --backfill 2025-01-01
voc-agent classify
voc-agent analyze
voc-agent report

# Or run everything at once
voc-agent run-all

# Incremental (weekly — fetches last 14 days only)
voc-agent run-all
```

### Run Tests

```bash
# All tests (199 tests)
pytest tests/ -v

# Just unit tests
pytest tests/unit/ -v

# Property-based tests
pytest tests/property/ -v

# Data contract validation (requires pipeline output)
pytest tests/contract/ -v

# QA checks
python -m qa_checks --mode all
```

### Generate Dashboard Locally

```bash
python scripts/build_dashboard.py
open docs/index.html
```

---

## Runbook

### How to Rerun a Specific Week
```bash
# Trigger via GitHub Actions
gh workflow run weekly.yml
```

### How to Add a New Channel
1. Add the channel config to `config/channels.yaml`
2. Create a new parser in `src/voc_agent/ingestion/parser_<name>.py`
3. Add unit tests in `tests/unit/test_parser_<name>.py`
4. Run `pytest tests/unit/` to verify

### How to Update the Classifier Prompt
1. Edit `config/prompts/classifier_v1.md`
2. Bump the version (e.g., `classifier_v2.md`)
3. Run `scripts/golden_eval.sh` to validate against the golden dataset
4. If F1 >= 0.80 and accuracy >= 0.75, merge the change
5. Re-classify the backfill in a shadow table, diff before cutover

### How to Re-label the Golden Set
1. Edit `tests/fixtures/golden_vocs.jsonl`
2. Run `scripts/golden_eval.sh` to recompute baselines
3. Update the threshold comments in `tests/e2e/test_full_pipeline.py`

---

## Project Structure

```
ra-voc-analyzer/
├── .github/workflows/weekly.yml    # GitHub Actions cron (Sun 11pm PST)
├── config/
│   ├── channels.yaml               # 3 Slack channel configs
│   ├── segments.yaml               # 5 customer segment definitions
│   └── prompts/classifier_v1.md    # Pinned classifier prompt
├── src/voc_agent/
│   ├── cli.py                      # typer CLI: ingest/classify/analyze/report/run-all
│   ├── ingestion/                  # Slack client + 3 channel parsers
│   ├── dedup/                      # Hash + semantic deduplication
│   ├── enrichment/                 # MRR parsing + CSV fallback
│   ├── classification/             # Claude classifier + CSAT mapper + Pydantic contracts
│   ├── analysis/                   # Segments, trends, change detection, DQ deep dive
│   ├── reporting/                  # Jinja2 + Plotly dashboard + Slack delivery
│   ├── storage/                    # SQLite schema + migrations
│   └── observability/              # structlog + run metadata
├── tests/
│   ├── unit/ (12 test modules)     # 199 tests
│   ├── property/                   # Hypothesis-based tests
│   ├── contract/                   # Pandera data validation
│   ├── e2e/                        # Full pipeline test
│   └── fixtures/                   # Golden dataset + test data
├── qa_checks/                      # 6 mandatory QA gates
├── docs/                           # GitHub Pages output
│   ├── index.html                  # Current week dashboard
│   └── archive/                    # Historical weekly dashboards
├── scripts/
│   └── build_dashboard.py          # Standalone dashboard builder
└── data/                           # SQLite DB + CSV enrichment
```

---

## Design System

Uses the **Intuit FY27 design system** tokens:

| Token | Hex | Use |
|-------|-----|-----|
| Sidebar | `#162251` | Navigation background (dark navy) |
| Page Background | `#f0f4f8` | Main canvas |
| Primary Text | `#1a1f36` | Headings |
| Secondary Text | `#6b7c93` | Labels, metadata |
| Chart Navy | `#1e3a6e` | Primary bars |
| Chart Blue | `#4472c4` | Secondary bars |
| Chart Teal | `#00b9a9` | Positive trends, AI accents |
| Chart Pink | `#f4809b` | Negative/cost |
| Green | `#1aab68` | Growth indicators |
| Red | `#d13438` | Errors, negative sentiment |

Font: Avenir Next, -apple-system, Helvetica Neue, Arial

---

## Cost

| Component | Estimated Weekly Cost |
|-----------|---------------------|
| Claude API (classification) | ~$2-5 (50-100 new VOCs x 20/batch) |
| Slack API | Free (bot token) |
| GitHub Actions | Free (public repo) |
| GitHub Pages | Free |
| **Total** | **~$2-5/week** |

Budget cap: $25/week (enforced by Gate 6). If exceeded, pipeline aborts and alerts.

---

## License

Internal — Mailchimp / Intuit R&A Team

**Owner:** Deepak Prabhakara (R&A PM)
**Built with:** Claude Code
