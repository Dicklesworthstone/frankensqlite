#!/usr/bin/env python3
"""Reproduce GH#530 against baseline, candidate, and a live stock SQLite CLI.

python3 scripts/benchmark_issue_530.py --baseline-fsqlite /path/to/old/fsqlite \
    --fsqlite /path/to/new/fsqlite --sqlite /path/to/sqlite3 \
    --baseline-revision OLD_SHA --candidate-revision NEW_SHA \
    --work-dir /path/to/measurements --output /path/to/new-result.json

For a published-release investigation without an executable candidate:
python3 scripts/benchmark_issue_530.py --release-fsqlite /path/to/released/fsqlite \
    --release-revision RELEASE_SHA --sqlite /path/to/sqlite3 \
    --work-dir /path/to/measurements --output /path/to/release-stock.json

The first four cases are the issue's exact SQL. Controls vary expression work,
join shape, and FROM source. Stock SQLite must have its native generate_series
extension; no recursive-CTE or Python-generator substitute is used.
single_count_having_fallback deliberately bypasses the existing lone-TVF
streaming path; it diagnoses materialization cost, not optimized candidate coverage.

The native .timer measures each SELECT and scalar output, excluding process
startup, fixture setup, warmups, and shutdown. This is a CLI comparison, not an
execute-only library benchmark: shell metadata preparation boundaries can differ.
All answers, including warmups, are checked. Three-engine order rotates each
round; two-engine release/stock order alternates. Release mode reports no
candidate or before/after comparison. Inputs, EXPLAIN/EQP output, raw logs,
binary hashes, and samples are kept.
Each profile-*.sql repeats only setup and the named workload, without correctness
queries or timers; whole-process profiles still include startup and setup.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

TIMER = re.compile(
    r"^Run Time:\s*(?:real\s+)?([0-9]+(?:\.[0-9]+)?)\s*(ms|us|µs|ns|s)?"
    r"(?:\s+user\s+([0-9.]+)\s+sys\s+([0-9.]+))?\s*$"
)
PREFIX = [".mode list", ".headers off"]
TABLE_SETUP = ["CREATE TABLE src(value INTEGER);",
               "INSERT INTO src SELECT value FROM generate_series(1,1000);"]
JSON_VALUES = json.dumps(list(range(1, 1001)), separators=(",", ":"))

# (SELECT, expected scalar, untimed setup). Answers use independent arithmetic.
CASES = {
    "single_count": ("SELECT count(*) FROM generate_series(1,1000000);", 1_000_000, []),
    "cartesian_count": ("SELECT count(*) FROM generate_series(1,1000)a,generate_series(1,1000)b;",
                        1_000_000, []),
    "single_sum": ("SELECT sum(value) FROM generate_series(1,1000000);", 500_000_500_000, []),
    "cartesian_sum": ("SELECT sum((a.value-1)*1000+b.value) FROM "
                      "generate_series(1,1000)a,generate_series(1,1000)b;", 500_000_500_000, []),
    # HAVING runs once, after aggregation, but excludes the existing single-TVF
    # streaming admission. This diagnostic deliberately retains materialization.
    "single_count_having_fallback": ("SELECT count(*) FROM generate_series(1,1000000) "
                                     "HAVING count(*)>=0;", 1_000_000, []),
    "cartesian_sum_constant": ("SELECT sum(1) FROM generate_series(1,1000)a,"
                               "generate_series(1,1000)b;", 1_000_000, []),
    "cartesian_sum_left": ("SELECT sum(a.value) FROM generate_series(1,1000)a,"
                           "generate_series(1,1000)b;", 500_500_000, []),
    "cartesian_sum_right": ("SELECT sum(b.value) FROM generate_series(1,1000)a,"
                            "generate_series(1,1000)b;", 500_500_000, []),
    "asymmetric_small_outer": ("SELECT count(*) FROM generate_series(1,10)a CROSS JOIN "
                               "generate_series(1,100000)b;", 1_000_000, []),
    "asymmetric_large_outer": ("SELECT count(*) FROM generate_series(1,100000)a CROSS JOIN "
                               "generate_series(1,10)b;", 1_000_000, []),
    "three_way_count": ("SELECT count(*) FROM generate_series(1,100)a,"
                        "generate_series(1,100)b,generate_series(1,100)c;", 1_000_000, []),
    "filtered_count": ("SELECT count(*) FROM generate_series(1,1000)a,"
                       "generate_series(1,1000)b WHERE a.value<=500 AND b.value%2=0;",
                       250_000, []),
    "table_count": ("SELECT count(*) FROM src a CROSS JOIN src b;", 1_000_000, TABLE_SETUP),
    "table_sum": ("SELECT sum((a.value-1)*1000+b.value) FROM src a CROSS JOIN src b;",
                  500_000_500_000, TABLE_SETUP),
    "json_count": (f"SELECT count(*) FROM json_each('{JSON_VALUES}')a,"
                   f"json_each('{JSON_VALUES}')b;", 1_000_000, []),
    "json_sum": (f"SELECT sum((a.value-1)*1000+b.value) FROM json_each('{JSON_VALUES}')a,"
                 f"json_each('{JSON_VALUES}')b;", 500_000_500_000, []),
}


def timer_seconds(line: str) -> float | None:
    match = TIMER.fullmatch(line.strip())
    if match is None:
        return None
    scale = {None: 1.0, "s": 1.0, "ms": 1e-3, "us": 1e-6, "µs": 1e-6, "ns": 1e-9}
    return float(match[1]) * scale[match[2]]


def binary_info(executable: str, revision: str | None, timeout: float) -> dict:
    resolved = shutil.which(executable)
    if resolved is None:
        raise ValueError(f"executable not found: {executable}")
    path = Path(resolved).resolve()
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    version = subprocess.run([str(path), "--version"], capture_output=True, text=True,
                             timeout=timeout, check=True)
    return {"path": str(path), "sha256": digest, "bytes": path.stat().st_size,
            "version": version.stdout.strip(), "source_revision_supplied": revision}


def run_cli(binary: dict, script: str, init: Path, directory: Path,
            timeout: float, stem: str) -> subprocess.CompletedProcess:
    try:
        result = subprocess.run([binary["path"], "-batch", "-init", str(init), ":memory:"],
                                input=script, text=True, capture_output=True,
                                timeout=timeout, check=False)
    except subprocess.TimeoutExpired as error:
        for suffix, partial in (("stdout", error.stdout), ("stderr", error.stderr)):
            content = partial.decode("utf-8", errors="replace") if isinstance(partial, bytes) else partial
            (directory / f"{stem}.{suffix}").write_text(content or "", encoding="utf-8")
        raise
    (directory / f"{stem}.stdout").write_text(result.stdout, encoding="utf-8")
    (directory / f"{stem}.stderr").write_text(result.stderr, encoding="utf-8")
    return result


def validate_sample(result: subprocess.CompletedProcess, expected: int, warmups: int) -> dict:
    if result.returncode:
        raise ValueError(f"CLI exit {result.returncode}: {result.stderr}")
    timings = [seconds for line in (result.stdout + "\n" + result.stderr).splitlines()
               if (seconds := timer_seconds(line)) is not None]
    if len(timings) != 1 or not math.isfinite(timings[0]) or timings[0] <= 0:
        raise ValueError(f"expected one positive native statement timer, got {timings}")
    errors = [line for line in result.stderr.splitlines()
              if line.strip() and timer_seconds(line) is None]
    values = [line.strip() for line in result.stdout.splitlines()
              if line.strip() and timer_seconds(line) is None]
    if errors or values != [str(expected)] * (warmups + 1):
        raise ValueError(f"wrong result or CLI error: values={values!r}, stderr={errors!r}")
    timer_line = next(line for line in (result.stdout + "\n" + result.stderr).splitlines()
                      if timer_seconds(line) is not None)
    match = TIMER.fullmatch(timer_line.strip())
    cpu = float(match[3]) + float(match[4]) if match[3] is not None else None
    return {"seconds": timings[0], "cpu_seconds": cpu, "timer_line": timer_line}


def summarize(samples: list[dict]) -> dict:
    summary = {}
    for engine in dict.fromkeys(sample["engine"] for sample in samples):
        times = [sample["seconds"] for sample in samples if sample["engine"] == engine]
        summary[engine] = {"runs": len(times), "median_seconds": statistics.median(times),
                           "min_seconds": min(times), "max_seconds": max(times)}
        cpu = [sample["cpu_seconds"] for sample in samples if sample["engine"] == engine]
        if all(value is not None for value in cpu):
            summary[engine]["median_cpu_seconds"] = statistics.median(cpu)
    stock = summary["stock"]["median_seconds"]
    comparisons = {f"{engine}_over_stock": result["median_seconds"] / stock
                   for engine, result in summary.items() if engine != "stock"}
    if "baseline" in summary and "candidate" in summary:
        comparisons["baseline_over_candidate"] = (summary["baseline"]["median_seconds"] /
                                                   summary["candidate"]["median_seconds"])
    summary.update(comparisons)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--fsqlite", help="candidate executable; requires --baseline-fsqlite")
    mode.add_argument("--release-fsqlite", help="published release; compare only with stock SQLite")
    parser.add_argument("--baseline-fsqlite")
    parser.add_argument("--sqlite", default="sqlite3")
    parser.add_argument("--candidate-revision")
    parser.add_argument("--baseline-revision")
    parser.add_argument("--release-revision")
    parser.add_argument("--stock-revision")
    parser.add_argument("--build-note", default="unspecified; binary hashes identify executables")
    parser.add_argument("--case", choices=tuple(CASES), action="append")
    parser.add_argument("--repeats", type=int, default=6)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--profile-repeats", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.fsqlite and not args.baseline_fsqlite:
        parser.error("candidate comparison requires --baseline-fsqlite")
    if args.release_fsqlite and (args.baseline_fsqlite or args.baseline_revision or args.candidate_revision):
        parser.error("release mode does not accept baseline or candidate arguments")
    if args.fsqlite and args.release_revision:
        parser.error("--release-revision is only valid with --release-fsqlite")
    if (args.repeats < 1 or args.warmups < 0 or args.profile_repeats < 1 or
            not math.isfinite(args.timeout) or args.timeout <= 0):
        parser.error("positive repeats/profile-repeats/timeout and nonnegative warmups required")
    if args.case and len(args.case) != len(set(args.case)):
        parser.error("each --case may be specified only once")
    if args.output.resolve() == args.work_dir.resolve():
        parser.error("--output must name a file distinct from --work-dir")
    if args.output.exists():
        parser.error("output already exists; choose a new result path")
    args.work_dir.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="issue530-", dir=args.work_dir.resolve()))
    init = work / "empty-init.sql"
    init.write_text("", encoding="utf-8")
    engines = ([("released", args.release_fsqlite, args.release_revision)] if args.release_fsqlite else
               [("baseline", args.baseline_fsqlite, args.baseline_revision),
                ("candidate", args.fsqlite, args.candidate_revision)])
    engines.append(("stock", args.sqlite, args.stock_revision))
    rotations = tuple(itertools.permutations(engine for engine, _, _ in engines))
    report = {"schema": 1, "issue": 530, "status": "running",
              "comparison_scope": ("published release versus stock; no candidate/before-after measurements"
                                   if args.release_fsqlite else "baseline and candidate versus live stock"),
              "started_utc": datetime.now(timezone.utc).isoformat(),
              "platform": platform.platform(), "machine": platform.machine(),
              "cpu_affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
              "build_note": args.build_note, "work_directory": str(work),
              "timing_scope": "native CLI SELECT timer, scalar output; startup/setup/warmup excluded",
              "timing_resolution_note": "FrankenSQLite real timers round to 1 ms (up to 0.5 ms error per sample); user+sys CPU fields print microseconds. Do not interpret small differences below this precision.",
              "warmups_per_sample": args.warmups, "repeats": args.repeats,
              "settings": "engine defaults; empty explicit init file; in-memory databases",
              "engine_order": rotations, "results": {}}
    try:
        report["binaries"] = {
            engine: binary_info(path, revision, args.timeout)
            for engine, path, revision in engines}
        for name in args.case or CASES:
            sql, expected, setup = CASES[name]
            directory = work / name
            directory.mkdir()
            script = "\n".join([*PREFIX, *setup, *([sql] * args.warmups),
                                ".timer on", sql, ".timer off", ""])
            (directory / "sample.sql").write_text(script, encoding="utf-8")
            (work / f"profile-{name}.sql").write_text(
                "\n".join([*PREFIX, *setup, *([sql] * args.profile_repeats), ""]), encoding="utf-8")
            entry = {"sql": sql, "setup": setup, "expected": expected, "samples": [], "plans": {}}
            report["results"][name] = entry
            for engine, binary in report["binaries"].items():
                # Plans are separate from timing and retained even if a diagnostic
                # is unsupported. Query success is required in every measured run.
                plans = run_cli(binary, "\n".join([*PREFIX, *setup,
                                "EXPLAIN QUERY PLAN " + sql, "EXPLAIN " + sql, ""]),
                                init, directory, args.timeout, f"{engine}-plans")
                entry["plans"][engine] = {"returncode": plans.returncode,
                                           "stdout": plans.stdout, "stderr": plans.stderr}
            for repetition in range(args.repeats):
                for engine in rotations[repetition % len(rotations)]:
                    result = run_cli(report["binaries"][engine], script, init, directory,
                                     args.timeout, f"{engine}-{repetition}")
                    measurement = validate_sample(result, expected, args.warmups)
                    entry["samples"].append({"engine": engine, "repetition": repetition,
                                              **measurement})
            entry["summary"] = summarize(entry["samples"])
            print(name, json.dumps(entry["summary"]), flush=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        report["status"] = "ok"
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        report["status"] = "error"
        report["error"] = str(error)
        print(f"benchmark failed: {error}; logs: {work}", file=sys.stderr)
    report["finished_utc"] = datetime.now(timezone.utc).isoformat()
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0 if report["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
