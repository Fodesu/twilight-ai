#!/usr/bin/env python3
"""Local readability gate for the new ChatView and materialized-history code.

Run AFTER cargo fmt / gofmt. Enforce physical indentation, including continuation
lines and tests: Rust <=12 spaces, Go <=3 tabs. No nesting suppression comments.
Legacy repository files are not silently reformatted or claimed compliant.
This check is deliberately not wired into CI yet.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]


def violations(path):
    maximum = 12 if path.suffix == ".rs" else 3
    character = " " if path.suffix == ".rs" else "\t"
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        prefix = line[:len(line) - len(line.lstrip())]
        if len(prefix) > maximum or prefix.replace(character, ""):
            yield f"{path.relative_to(ROOT)}:{number}: indentation exceeds three levels or uses mixed whitespace"


def main():
    chat = ROOT / "cmd/twilight-chat"
    paths = sorted((chat / "src").rglob("*.rs"))
    paths += sorted((chat / "crates").glob("*/src/**/*.rs"))
    paths += sorted((ROOT / "agent/app").glob("chat*.go"))
    paths += sorted((ROOT / "agent/app/http").glob("chat*.go"))
    paths += [
        ROOT / "agent/app/history.go",
        ROOT / "agent/app/http/history_test.go",
        ROOT / "agent/component/cloudtest/chat_client_test.go",
    ]
    errors = [error for path in paths for error in violations(path)]
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        return 1
    print(f"Indent lint: {len(paths)} files, maximum three levels; OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
