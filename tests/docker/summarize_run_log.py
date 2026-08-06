#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Summarize fullstack .test results from run.sh log output.

Parse logs captured from a columnar fullstack run, for example:

    cd tests/fullstack-test-next-gen-columnar
    ENABLE_NEXT_GEN=true ./run.sh 2>&1 | tee run.0526141800.log
    python3 ../docker/summarize_run_log.py run.0526141800.log
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path


ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")
TEST_LINE_RE = re.compile(r"^(.+\.test): (Running|OK|Failed)\b")
TIMEOUT_RE = re.compile(r"^\s+Error: test timed out after \d+s\s*$")


@dataclass
class TestResult:
    status: str = "unknown"
    timed_out: bool = False


@dataclass
class Summary:
    total: int = 0
    ok: int = 0
    failed: int = 0
    timeout: int = 0
    tests: dict[str, TestResult] = field(default_factory=dict)


def parse_run_log(lines: list[str]) -> Summary:
    summary = Summary()
    current_test: str | None = None
    saw_timeout_for_current = False

    for raw_line in lines:
        line = ANSI_ESCAPE_RE.sub("", raw_line).rstrip("\n")

        match = TEST_LINE_RE.match(line)
        if match:
            test_path, status = match.group(1), match.group(2)

            if status == "Running":
                current_test = test_path
                saw_timeout_for_current = False
                summary.tests.setdefault(test_path, TestResult())
                continue

            result = summary.tests.setdefault(test_path, TestResult())
            result.status = status.lower()
            summary.total += 1

            if status == "OK":
                summary.ok += 1
            elif status == "Failed":
                if result.timed_out or saw_timeout_for_current:
                    result.timed_out = True
                    summary.timeout += 1
                else:
                    summary.failed += 1
            continue

        if current_test is not None and TIMEOUT_RE.match(line):
            saw_timeout_for_current = True
            summary.tests[current_test].timed_out = True

    return summary


def print_summary(summary: Summary, show_details: bool) -> None:
    print("Fullstack test summary")
    print(f"  total:   {summary.total}")
    print(f"  ok:      {summary.ok}")
    print(f"  failed:  {summary.failed}")
    print(f"  timeout: {summary.timeout}")

    if show_details:
        timeout_tests = sorted(path for path, result in summary.tests.items() if result.timed_out)
        failed_tests = sorted(
            path
            for path, result in summary.tests.items()
            if result.status == "failed" and not result.timed_out
        )

        if timeout_tests:
            print("\nTimeout tests:")
            for path in timeout_tests:
                print(f"  - {path}")

        if failed_tests:
            print("\nFailed tests:")
            for path in failed_tests:
                print(f"  - {path}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "log_file",
        type=Path,
        help="path to run.sh log file (e.g. run.0526141800.log from tee)",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="print lists of failed and timeout tests",
    )
    args = parser.parse_args()

    if not args.log_file.is_file():
        print(f"error: log file not found: {args.log_file}", file=sys.stderr)
        return 1

    summary = parse_run_log(args.log_file.read_text(encoding="utf-8", errors="replace").splitlines())
    print_summary(summary, args.details)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
