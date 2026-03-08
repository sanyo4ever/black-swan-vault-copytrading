#!/usr/bin/env python3
"""Lightweight documentation review gate for pre-push/CI usage."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")

EXCLUDED_DOCS = {
    "PROJECT_MEMORY.md",
}

REQUIRED_DOCS = (
    "README.md",
    "CONTRIBUTING.md",
    "deploy/README.md",
    "docs/README.md",
    "docs/PRODUCTION_ARCHITECTURE_UBUNTU.md",
    "docs/SUBSCRIBED_DELIVERY_ARCHITECTURE.md",
    "docs/TRADER_DISCOVERY_ARCHITECTURE.md",
)

FORBIDDEN_PATTERNS = (
    (
        re.compile(r"\btelegram\s+stars\b", re.IGNORECASE),
        "Use donation/open-source wording instead of Telegram Stars payment flow.",
    ),
    (
        re.compile(r"\bXTR\b"),
        "Use current donation-supported model wording (no XTR billing flow).",
    ),
    (
        re.compile(
            r"\b(24h|24-hour|24 hour)\b.{0,60}\b(subscription|subscriptions|ttl|expire|expires|expiry|lifetime)\b",
            re.IGNORECASE,
        ),
        "Subscriptions are permanent until explicit /stop. Do not document 24h subscription TTL.",
    ),
    (
        re.compile(r"\bsubscription\s+ttl\b", re.IGNORECASE),
        "Do not describe subscription TTL. Current model is permanent until /stop.",
    ),
)


def _git_tracked_markdown_files() -> list[Path]:
    completed = subprocess.run(
        ["git", "ls-files"],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    files: list[Path] = []
    for raw in completed.stdout.splitlines():
        if not raw.endswith(".md"):
            continue
        if raw in EXCLUDED_DOCS:
            continue
        files.append(REPO_ROOT / raw)
    return files


def _extract_link_target(raw_target: str) -> str:
    target = raw_target.strip().strip("<>").strip()
    if not target:
        return ""
    # Handle markdown links with optional title: (path "title")
    if " " in target and not target.startswith(("http://", "https://")):
        target = target.split(" ", 1)[0]
    return target


def _is_external_or_anchor(target: str) -> bool:
    return target.startswith(
        ("http://", "https://", "mailto:", "tel:", "#")
    )


def _check_links(file_path: Path) -> list[str]:
    text = file_path.read_text(encoding="utf-8")
    errors: list[str] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        for match in LINK_RE.finditer(line):
            target = _extract_link_target(match.group(1))
            if not target or _is_external_or_anchor(target):
                continue

            base_target = target.split("#", 1)[0]
            if not base_target:
                continue

            if base_target.startswith("/"):
                resolved = (REPO_ROOT / base_target.lstrip("/")).resolve()
            else:
                resolved = (file_path.parent / base_target).resolve()

            if not resolved.exists():
                rel_file = file_path.relative_to(REPO_ROOT)
                errors.append(
                    f"{rel_file}:{line_number} broken local link -> {base_target}"
                )
    return errors


def _check_forbidden_terms(file_path: Path) -> list[str]:
    text = file_path.read_text(encoding="utf-8")
    errors: list[str] = []
    rel_file = file_path.relative_to(REPO_ROOT)
    for pattern, hint in FORBIDDEN_PATTERNS:
        for match in pattern.finditer(text):
            line_number = text.count("\n", 0, match.start()) + 1
            snippet = text[match.start() : match.start() + 80].replace("\n", " ")
            errors.append(f"{rel_file}:{line_number} {hint} (found: {snippet!r})")
    return errors


def _check_required_docs() -> list[str]:
    errors: list[str] = []
    for required in REQUIRED_DOCS:
        if not (REPO_ROOT / required).exists():
            errors.append(f"Missing required document: {required}")
    return errors


def main() -> int:
    files = _git_tracked_markdown_files()
    if not files:
        print("No markdown files found.")
        return 1

    errors: list[str] = []
    errors.extend(_check_required_docs())
    for file_path in files:
        errors.extend(_check_links(file_path))
        errors.extend(_check_forbidden_terms(file_path))

    if errors:
        print("Documentation review FAILED")
        for error in errors:
            print(f"- {error}")
        return 1

    print(f"Documentation review PASS (checked {len(files)} markdown files)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
