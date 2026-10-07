#!/usr/bin/env python3
"""Reproduce GH#491 without replacing INT PRIMARY KEY with a rowid alias.

python3 scripts/benchmark_issue_491.py --fsqlite target/release/fsqlite \
    --sqlite sqlite3 --baseline-fsqlite /path/to/baseline/fsqlite --output result.json
python3 scripts/benchmark_issue_491.py --self-test

Requires native CLI binaries with generate_series and .timer support. No CTE or
Python-generator timing fallback is allowed. Each sample uses a fresh database and HOME;
only INSERT is timed (including its autocommit), not startup, DDL, verification,
or checkpoint. Defaults and explicit WAL are reported separately. Keep normal
concurrency and durability settings; do not disable them to make a result green.

--profile-sql-dir writes separate, validation-free SQL inputs for perf/flamegraph
runs. Profile a FRESH database, e.g.:
  perf record --call-graph dwarf -- target/release/fsqlite /tmp/new-491.db < input.sql
Such profiles include process startup, DDL, source setup and connection teardown;
they are NOT statement-only profiles. Never use instrumented timings as baselines.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import shutil
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path

CASES = ("int_pk", "no_pk", "integer_pk", "secondary")
TIMER = re.compile(
    r"^Run Time:\s*(?:real\s+)?([0-9]+(?:\.[0-9]+)?)\s*(ms|us|µs|ns|s)?"
    r"(?:\s+user\s+[0-9.]+\s+sys\s+[0-9.]+)?\s*$"
)


def fixture(case: str, source: str, storage: str, rows: int) -> tuple[list[str], str]:
    setup = []
    if storage == "wal":
        setup.append("PRAGMA journal_mode=WAL;")
    schema = {"int_pk": "a int primary key", "no_pk": "a int",
              "integer_pk": "a integer primary key",
              "secondary": "a int primary key,b int,c int"}[case]
    setup.append(f"CREATE TABLE t({schema});")
    if case == "secondary":
        setup += ["CREATE INDEX t_b ON t(b);", "CREATE UNIQUE INDEX t_c ON t(c);"]
    select_from = f"generate_series(1,{rows})"
    if source == "materialized":
        setup += ["CREATE TABLE src(value INTEGER PRIMARY KEY);",
                  f"INSERT INTO src SELECT value FROM {select_from};"]
        select_from = "src"
    projection = f"value,value%97,{rows}-value" if case == "secondary" else "value"
    return setup, f"INSERT INTO t SELECT {projection} FROM {select_from};"


def checks(case: str, rows: int) -> list[str]:
    # Count + distinct + range proves the complete sequence, not just a checksum.
    bad = "a IS NULL OR a<>rowid"
    if case == "secondary":
        bad += f" OR b IS NULL OR b<>a%97 OR c IS NULL OR c<>{rows}-a"
    queries = [f"SELECT 'I491_ROWS',count(*),count(DISTINCT a),min(a),max(a),"
               f"sum(CASE WHEN {bad} THEN 1 ELSE 0 END) FROM t NOT INDEXED;"]
    indexes = []
    if case in ("int_pk", "secondary"):
        indexes.append(("sqlite_autoindex_t_1", "a IS NULL OR a<>rowid"))
    if case == "secondary":
        indexes += [("t_b", "b IS NULL OR b<>rowid%97"),
                    ("t_c", f"c IS NULL OR c<>{rows}-rowid")]
    for index, predicate in indexes:
        queries.append(f"SELECT 'I491_INDEX_{index}',count(*),count(DISTINCT rowid),"
                       f"min(rowid),max(rowid),sum(CASE WHEN {predicate} THEN 1 ELSE 0 END) "
                       f"FROM t INDEXED BY {index};")
    return queries


def script(case: str, source: str, storage: str, rows: int, verify: bool = True) -> str:
    setup, insert = fixture(case, source, storage, rows)
    lines = [".mode list", ".headers off", ".separator |", *setup]
    if verify:
        lines += ["SELECT 'I491_SETTINGS';", "SELECT sqlite_version();",
                  "PRAGMA journal_mode;", "PRAGMA synchronous;", "PRAGMA page_size;",
                  "SELECT 'I491_SETTINGS_END';"]
    lines += [".timer on", insert, ".timer off"]
    if verify:
        lines += checks(case, rows)
        if storage != "memory":
            lines += ["SELECT 'I491_CHECKPOINT';", "PRAGMA wal_checkpoint(TRUNCATE);",
                      "SELECT 'I491_CHECKPOINT_END';"]
    return "\n".join(lines) + "\n"


def timer_seconds(line: str) -> float | None:
    match = TIMER.fullmatch(line.strip())
    if not match:
        return None
    scale = {None: 1.0, "s": 1.0, "ms": 1e-3, "us": 1e-6, "µs": 1e-6, "ns": 1e-9}
    return float(match[1]) * scale[match[2]]


def validate_output(stdout: str, stderr: str, case: str, rows: int, storage: str) -> dict:
    timings = [value for line in (stdout + "\n" + stderr).splitlines()
               if (value := timer_seconds(line)) is not None]
    if len(timings) != 1 or not math.isfinite(timings[0]) or timings[0] <= 0:
        raise ValueError("expected exactly one positive INSERT timer; increase --rows if rounded to zero")
    errors = [line for line in stderr.splitlines() if line.strip() and timer_seconds(line) is None]
    if errors or re.search(r"(?im)^\s*(?:error|parse error|runtime error|unknown command)\b", stdout):
        raise ValueError(f"CLI reported an error: {stderr or stdout}")
    lines = [line.strip() for line in stdout.splitlines()]
    begin, end = lines.index("I491_SETTINGS"), lines.index("I491_SETTINGS_END")
    settings = lines[begin + 1:end]
    if len(settings) != 4:
        raise ValueError(f"unexpected engine/version/journal/synchronous/page-size metadata: {settings}")
    if storage == "wal" and settings[1].lower() != "wal":
        raise ValueError("requested WAL but engine did not activate it")
    expected = f"{rows}|{rows}|1|{rows}|0"
    for query in checks(case, rows):
        marker = query.split("'", 2)[1]
        matches = [line for line in lines if line.startswith(marker + "|")]
        if matches != [marker + "|" + expected]:
            raise ValueError(f"failed table/index oracle {marker}: {matches}")
    if storage != "memory":
        start, stop = lines.index("I491_CHECKPOINT"), lines.index("I491_CHECKPOINT_END")
        checkpoint = lines[start + 1:stop]
        if len(checkpoint) != 1 or not checkpoint[0].startswith("0|"):
            raise ValueError(f"checkpoint did not complete: {checkpoint}")
    return {"seconds": timings[0], "version": settings[0], "journal_mode": settings[1],
            "synchronous": settings[2], "page_size": settings[3]}


def verify_file(path: Path, case: str, rows: int) -> None:
    # Read-only stock inspection happens before any FrankenSQLite reopen.
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        if db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise ValueError("stock SQLite rejected persisted table/index integrity")
        columns = "rowid,a,b,c" if case == "secondary" else "rowid,a"
        cursor = db.execute(f"SELECT {columns} FROM t NOT INDEXED ORDER BY rowid")
        for value in range(1, rows + 1):
            expected = (value, value, value % 97, rows - value) if case == "secondary" else (value, value)
            if cursor.fetchone() != expected:
                raise ValueError(f"stock SQLite row/payload mismatch at {value}")
        if cursor.fetchone() is not None:
            raise ValueError("stock SQLite found trailing phantom rows")
        for query in checks(case, rows):
            record = db.execute(query).fetchall()
            if len(record) != 1 or record[0][1:] != (rows, rows, 1, rows, 0):
                raise ValueError(f"stock SQLite index oracle failed: {record}")


def binary_info(executable: str) -> dict:
    resolved = shutil.which(executable)
    if resolved is None:
        raise ValueError(f"executable not found: {executable}")
    path = Path(resolved).resolve()
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return {"path": str(path), "sha256": digest.hexdigest()}


def sample(binary: dict, case: str, source: str, storage: str, rows: int, timeout: float) -> dict:
    with tempfile.TemporaryDirectory(prefix="fsqlite-491-") as directory:
        path = Path(directory) / "fresh.db"
        target = ":memory:" if storage == "memory" else str(path)
        start = time.perf_counter()
        result = subprocess.run([binary["path"], target], input=script(case, source, storage, rows),
                                capture_output=True, text=True, timeout=timeout, check=False,
                                env={**os.environ, "HOME": directory, "USERPROFILE": directory})
        elapsed = time.perf_counter() - start
        if result.returncode:
            raise ValueError(f"CLI exited {result.returncode}: {result.stderr}\n{result.stdout}")
        metrics = validate_output(result.stdout, result.stderr, case, rows, storage)
        if storage != "memory":
            verify_file(path, case, rows)
        metrics["process_seconds_including_cli_checks"] = elapsed
        return metrics


def summarize(samples: list[dict], max_regression: float) -> tuple[dict, bool]:
    summary = {}
    for name in sorted({item["engine"] for item in samples}):
        selected = [item for item in samples if item["engine"] == name]
        settings = {(s["version"], s["journal_mode"], s["synchronous"], s["page_size"]) for s in selected}
        if len(settings) != 1:
            raise ValueError(f"engine settings changed between repetitions: {name}")
        summary[name] = {"median_seconds": statistics.median(s["seconds"] for s in selected),
                         "samples": len(selected), "settings": list(next(iter(settings)))}
    regression = False
    for reference in ("stock", "baseline"):
        if reference not in summary:
            continue
        ratio = summary["candidate"]["median_seconds"] / summary[reference]["median_seconds"]
        summary[f"candidate_over_{reference}"] = ratio
        # Different defaults are evidence, not comparable durability modes.
        matched = summary["candidate"]["settings"][1:] == summary[reference]["settings"][1:]
        summary[f"settings_match_{reference}"] = matched
        if reference == "baseline":
            if not matched:
                raise ValueError("baseline/candidate durability or page-size settings differ")
            regression = ratio > max_regression
    return summary, regression


def self_test() -> int:
    class Tests(unittest.TestCase):
        def test_timer_formats(self):
            for text, value in [("Run Time: real 0.250 user 0.24 sys 0.01", .25),
                                ("Run Time: 5.517 s", 5.517), ("Run Time: 42ms", .042)]:
                self.assertAlmostEqual(timer_seconds(text), value)
            self.assertIsNone(timer_seconds("error: insert failed"))

        def test_exact_pk_fixture(self):
            setup, insert = fixture("int_pk", "series", "file", 1_000_000)
            self.assertEqual(setup, ["CREATE TABLE t(a int primary key);"])
            self.assertEqual(insert, "INSERT INTO t SELECT value FROM generate_series(1,1000000);")
            text = script("int_pk", "series", "memory", 1000, False)
            self.assertEqual(text.count(".timer on"), 1)
            self.assertNotIn("I491_ROWS", text)

        def test_stock_oracles_and_corruption_rejection(self):
            for case in CASES:
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "oracle.db"
                    with sqlite3.connect(path) as db:
                        setup, insert = fixture(case, "series", "wal", 100)
                        for sql in setup:
                            db.execute(sql)
                        # Explicitly correctness-only: Python's SQLite lacks the
                        # shell extension, so construct the same finite source.
                        db.execute("CREATE TABLE src(value INTEGER PRIMARY KEY)")
                        db.executemany("INSERT INTO src VALUES(?)", [(i,) for i in range(1, 101)])
                        db.execute(insert.replace("generate_series(1,100)", "src"))
                    verify_file(path, case, 100)
                    with sqlite3.connect(path) as db:
                        db.execute("DELETE FROM t WHERE a=50")
                    with self.assertRaises(ValueError):
                        verify_file(path, case, 100)

        def test_output_fails_closed(self):
            text = ("I491_SETTINGS\nversion\nmemory\n2\n4096\nI491_SETTINGS_END\n"
                    "Run Time: real 0.1 user 0.1 sys 0.0\nI491_ROWS|10|10|1|10|0\n"
                    "I491_INDEX_sqlite_autoindex_t_1|10|10|1|10|0\n")
            self.assertEqual(validate_output(text, "", "int_pk", 10, "memory")["seconds"], .1)
            for broken in [text.replace("|10|10|", "|9|9|"),
                           text.replace("real 0.1", "real 0.0"), text + "Run Time: 1s\n",
                           text.replace("I491_INDEX_sqlite_autoindex_t_1", "missing")]:
                with self.assertRaises(ValueError):
                    validate_output(broken, "", "int_pk", 10, "memory")
            with self.assertRaises(ValueError):
                validate_output(text, "Error: unique constraint", "int_pk", 10, "memory")

        def test_regression_and_settings_gate(self):
            samples = [{"engine": engine, "seconds": seconds, "version": "v",
                        "journal_mode": "wal", "synchronous": "2", "page_size": "4096"}
                       for engine, seconds in [("baseline", 1), ("candidate", 1.3)]]
            self.assertTrue(summarize(samples, 1.1)[1])
            samples[1]["seconds"] = .8
            self.assertFalse(summarize(samples, 1.1)[1])
            samples[1]["synchronous"] = "0"
            with self.assertRaises(ValueError):
                summarize(samples, 1.1)

    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
    return 0 if result.wasSuccessful() else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fsqlite", default="target/release/fsqlite")
    parser.add_argument("--sqlite", default="sqlite3")
    parser.add_argument("--baseline-fsqlite")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--case", choices=CASES, action="append")
    parser.add_argument("--storage", choices=("memory", "file", "wal"), action="append")
    parser.add_argument("--source", choices=("series", "materialized"), action="append")
    parser.add_argument("--max-regression", type=float, default=1.10)
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--output", type=Path, default=Path("issue491-results.json"))
    parser.add_argument("--profile-sql-dir", type=Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if (args.rows < 1 or args.repeats < 1 or args.warmups < 0 or
            not math.isfinite(args.max_regression) or args.max_regression < 1 or
            not math.isfinite(args.timeout) or args.timeout <= 0):
        parser.error("positive rows/repeats/timeout, nonnegative warmups and max-regression >= 1 required")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = {"schema": 1, "status": "running", "platform": platform.platform(),
              "stock_file_oracle_version": sqlite3.sqlite_version, "rows": args.rows,
              "warmups": args.warmups, "max_regression": args.max_regression,
              "benchmark_scope": "INSERT including autocommit only", "results": {}}
    regressions = False
    try:
        names = {"candidate": args.fsqlite, "stock": args.sqlite}
        if args.baseline_fsqlite:
            names["baseline"] = args.baseline_fsqlite
        binaries = {name: binary_info(path) for name, path in names.items()}
        report["binaries"] = binaries
        for case in args.case or CASES:
            for storage in args.storage or ("memory", "file", "wal"):
                for source in args.source or ("series", "materialized"):
                    key = f"{case}/{storage}/{source}"
                    entry = {"samples": [], "sql": script(case, source, storage, args.rows)}
                    report["results"][key] = entry
                    if args.profile_sql_dir:
                        args.profile_sql_dir.mkdir(parents=True, exist_ok=True)
                        output = args.profile_sql_dir / (key.replace("/", "_") + ".sql")
                        output.write_text(script(case, source, storage, args.rows, False), encoding="utf-8")
                    for repetition in range(-args.warmups, args.repeats):
                        order = list(binaries)
                        if repetition % 2:
                            order.reverse()
                        for engine in order:
                            result = sample(binaries[engine], case, source, storage, args.rows, args.timeout)
                            if repetition >= 0:
                                entry["samples"].append({"engine": engine, "repetition": repetition, **result})
                    entry["summary"], regressed = summarize(entry["samples"], args.max_regression)
                    regressions |= regressed
                    print(key, json.dumps(entry["summary"]), file=sys.stderr)
                    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        report["status"] = "regression" if regressions else "ok"
        report["baseline_regression_checked"] = bool(args.baseline_fsqlite)
    except (OSError, ValueError, sqlite3.Error, subprocess.SubprocessError) as error:
        report["status"] = "error"
        report["error"] = str(error)
        print(f"benchmark failed: {error}", file=sys.stderr)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 2 if report["status"] == "error" else int(regressions)


if __name__ == "__main__":
    sys.exit(main())
