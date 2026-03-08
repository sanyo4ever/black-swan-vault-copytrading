#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import unittest
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Black Swan Vault QA certification gate (tests + data quality audit)."
    )
    parser.add_argument(
        "--database",
        default="",
        help="Database DSN/path for data quality audit. Defaults to DATABASE_URL or DATABASE_PATH env var.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=5000,
        help="Max traders to inspect during data quality audit.",
    )
    parser.add_argument(
        "--freshness-minutes",
        type=int,
        default=60,
        help="Freshness threshold for ACTIVE_LISTED trader activity checks.",
    )
    parser.add_argument(
        "--min-active-listed",
        type=int,
        default=0,
        help="Fail if active listed trader count is below this threshold.",
    )
    parser.add_argument(
        "--skip-tests",
        action="store_true",
        help="Skip unittest suite execution.",
    )
    parser.add_argument(
        "--skip-db-audit",
        action="store_true",
        help="Skip database data-quality audit.",
    )
    parser.add_argument(
        "--json-out",
        default="",
        help="Optional path to write machine-readable JSON report.",
    )
    return parser.parse_args()


def _run_unit_tests(*, repo_root: Path) -> dict[str, Any]:
    loader = unittest.defaultTestLoader
    suite = loader.discover(
        start_dir=str(repo_root / "tests"),
        pattern="test_*.py",
        top_level_dir=str(repo_root),
    )
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return {
        "passed": result.wasSuccessful(),
        "run": int(result.testsRun),
        "failures": len(result.failures),
        "errors": len(result.errors),
        "skipped": len(getattr(result, "skipped", [])),
    }


def _resolve_database_dsn(cli_value: str) -> str:
    explicit = str(cli_value or "").strip()
    if explicit:
        return explicit
    for env_name in ("DATABASE_URL", "DATABASE_PATH"):
        value = str(os.getenv(env_name, "")).strip()
        if value:
            return value
    return ""


def _write_json_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    args = _parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from bot.qa import run_data_quality_audit

    report: dict[str, Any] = {
        "timestamp": datetime.now(tz=UTC).isoformat(),
        "tests": {"skipped": True},
        "data_quality": {"skipped": True},
        "gate": {"passed": True, "errors": []},
    }
    gate_errors: list[str] = []

    if not args.skip_tests:
        tests_report = _run_unit_tests(repo_root=repo_root)
        report["tests"] = tests_report
        if not tests_report["passed"]:
            gate_errors.append(
                f"unittest failed (failures={tests_report['failures']}, errors={tests_report['errors']})"
            )

    if not args.skip_db_audit:
        database_dsn = _resolve_database_dsn(args.database)
        if not database_dsn:
            report["data_quality"] = {
                "skipped": True,
                "reason": "DATABASE_URL/DATABASE_PATH is not configured",
            }
        else:
            try:
                dq = run_data_quality_audit(
                    database_dsn=database_dsn,
                    max_rows=max(1, int(args.max_rows)),
                    freshness_minutes=max(1, int(args.freshness_minutes)),
                )
            except Exception as exc:
                dq = {
                    "passed": False,
                    "critical_issues": 1,
                    "warning_issues": 0,
                    "checked_traders": 0,
                    "active_listed_traders": 0,
                    "fresh_active_listed_traders": 0,
                    "issues_sample": [
                        {
                            "severity": "critical",
                            "code": "audit_runtime_failure",
                            "message": str(exc),
                            "address": None,
                        }
                    ],
                    "runtime_error": str(exc),
                }
            report["data_quality"] = dq
            if not dq["passed"]:
                gate_errors.append(
                    f"data quality audit failed (critical_issues={dq['critical_issues']})"
                )
            if int(dq.get("active_listed_traders", 0)) < max(0, int(args.min_active_listed)):
                gate_errors.append(
                    "active listed trader count is below threshold "
                    f"({dq.get('active_listed_traders', 0)} < {args.min_active_listed})"
                )

    report["gate"] = {
        "passed": len(gate_errors) == 0,
        "errors": gate_errors,
    }

    if args.json_out:
        _write_json_report(Path(args.json_out), report)

    tests_info = report["tests"]
    dq_info = report["data_quality"]
    print("QA Certification Summary")
    print(f"- Gate: {'PASS' if report['gate']['passed'] else 'FAIL'}")
    if not tests_info.get("skipped", False):
        print(
            "- Tests: "
            f"run={tests_info['run']} failures={tests_info['failures']} "
            f"errors={tests_info['errors']} skipped={tests_info['skipped']}"
        )
    else:
        print("- Tests: skipped")

    if not dq_info.get("skipped", False):
        print(
            "- Data quality: "
            f"checked={dq_info['checked_traders']} "
            f"critical={dq_info['critical_issues']} "
            f"warnings={dq_info['warning_issues']} "
            f"active_listed={dq_info['active_listed_traders']} "
            f"fresh_active={dq_info['fresh_active_listed_traders']}"
        )
    else:
        print(f"- Data quality: skipped ({dq_info.get('reason', 'disabled')})")

    if gate_errors:
        print("- Gate errors:")
        for item in gate_errors:
            print(f"  * {item}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
