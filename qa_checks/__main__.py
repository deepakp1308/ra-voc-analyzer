"""QA checks runner — invoked as `python -m qa_checks --mode [report|budget|all]`."""

from __future__ import annotations

import argparse
import json
import sys

import structlog

from voc_agent.storage.db import get_connection

logger = structlog.get_logger()


def main() -> None:
    parser = argparse.ArgumentParser(description="R&A VOC Analyzer QA Checks")
    parser.add_argument(
        "--mode",
        choices=["report", "budget", "contracts", "all"],
        default="all",
        help="Which checks to run",
    )
    args = parser.parse_args()

    failures: list[str] = []

    if args.mode in ("contracts", "all"):
        from qa_checks.check_data_contracts import run_all_contracts

        with get_connection() as conn:
            result = run_all_contracts(conn)
            if result["status"] == "FAIL":
                failures.extend(result["failures"])

    if args.mode in ("report", "all"):
        from qa_checks.check_report import check_report_integrity

        result = check_report_integrity()
        if result["status"] == "FAIL":
            failures.extend(result["failures"])

    if args.mode in ("budget", "all"):
        # Gate 6: Cost & Performance Budget
        with get_connection() as conn:
            row = conn.execute(
                "SELECT cost_usd, ended_at, started_at FROM runs ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            if row:
                cost = row["cost_usd"] or 0
                if cost > 25.0:
                    failures.append(f"Cost budget exceeded: ${cost:.2f} > $25.00")

    if failures:
        print(f"\nQA CHECK FAILED — {len(failures)} issue(s):", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        sys.exit(1)
    else:
        print("QA checks PASSED")
        sys.exit(0)


if __name__ == "__main__":
    main()
