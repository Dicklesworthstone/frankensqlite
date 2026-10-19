#!/usr/bin/env python3
"""GH#521: compare baseline, candidate, and a live stock SQLite CLI.

python3 scripts/benchmark_issue_521.py --fsqlite /path/to/candidate/fsqlite \
    --baseline-fsqlite /path/to/baseline/fsqlite --sqlite /path/to/sqlite3 \
    --baseline-revision OLD_SHA --candidate-revision NEW_SHA \
    --work-dir /path/to/measurements --output /path/to/new-result.json

Each fresh database receives the reported schema and 12,000 individual INSERTs.
The wall timer includes CLI startup, DDL, all INSERTs, COMMIT when requested,
and connection shutdown. Input generation and subsequent verification are
excluded. Engine durability/concurrency defaults are preserved and reported;
an empty explicit init script prevents user shell startup SQL from changing them.
Every run is independently inspected by stock SQLite before a native CLI reopen.
Databases, inputs, logs, and JSON are retained; this script deletes no artifacts.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import platform
import random
import shutil
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

ROWS = 12_000
ORDERS = ("descending", "ascending", "alternating", "permuted")
ROTATIONS = (("baseline", "candidate", "stock"), ("candidate", "stock", "baseline"),
             ("stock", "baseline", "candidate"), ("stock", "candidate", "baseline"),
             ("candidate", "baseline", "stock"), ("baseline", "stock", "candidate"))


def ordered_keys(order: str, seed: int) -> list[int]:
    keys = list(range(1, ROWS + 1))
    if order == "descending":
        keys.reverse()
    elif order == "alternating":
        keys = list(itertools.chain.from_iterable(zip(reversed(keys), keys)))[:ROWS]
    elif order == "permuted":
        random.Random(seed).shuffle(keys)
    return keys


def command(binary: dict, database: Path, init: Path) -> list[str]:
    return [binary["path"], "-batch", "-init", str(init), str(database)]


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


def probe_settings(binary: dict, engine: str, directory: Path, init: Path, timeout: float) -> dict:
    names = ["sqlite_version", "journal_mode", "synchronous", "page_size"]
    sql = ["SELECT sqlite_version();", "PRAGMA journal_mode;", "PRAGMA synchronous;",
           "PRAGMA page_size;"]
    if engine != "stock":
        names.append("concurrent_mode")
        sql.append("PRAGMA fsqlite.concurrent_mode;")
    result = subprocess.run(command(binary, directory / f"{engine}-settings.db", init),
                            input="\n".join(sql) + "\n", capture_output=True, text=True,
                            timeout=timeout, check=True)
    values = result.stdout.splitlines()
    if result.stderr.strip() or len(values) != len(names):
        raise ValueError(f"{engine} settings probe failed: {result.stdout!r} {result.stderr!r}")
    settings = dict(zip(names, values))
    if settings["page_size"] != "4096":
        raise ValueError(f"{engine} must reproduce the reported 4096-byte default: {settings}")
    if engine != "stock" and settings["concurrent_mode"].lower() not in ("1", "on", "true"):
        raise ValueError(f"{engine} default concurrent mode is not enabled: {settings}")
    return settings


def query_oracles(keys: list[int]) -> list[tuple[str, list[tuple]]]:
    by_rowid = list(enumerate(keys, 1))
    by_key = sorted(by_rowid, key=lambda row: (row[1], row[0]))
    checks = [("PRAGMA integrity_check", [("ok",)]),
              ("SELECT rowid,x FROM a NOT INDEXED ORDER BY rowid", by_rowid),
              ("SELECT rowid,x FROM a INDEXED BY ia ORDER BY x,rowid", by_key),
              ("SELECT rowid,x FROM a INDEXED BY ia ORDER BY x DESC,rowid DESC", by_key[::-1])]
    for key in (0, 1, 127, 128, ROWS // 2, ROWS, ROWS + 1):
        checks.append((f"SELECT rowid,x FROM a INDEXED BY ia WHERE x={key}",
                       [row for row in by_key if row[1] == key]))
    for low, high in ((1, 17), (ROWS // 2 - 19, ROWS // 2 + 19),
                      (ROWS - 17, ROWS), (ROWS + 1, ROWS + 17)):
        for direction in ("ASC", "DESC"):
            expected = [row for row in by_key if low <= row[1] <= high]
            if direction == "DESC":
                expected.reverse()
            checks.append((f"SELECT rowid,x FROM a INDEXED BY ia WHERE x BETWEEN {low} AND {high} "
                           f"ORDER BY x {direction},rowid {direction}", expected))
    return checks


def inspect_file(path: Path, checks: list[tuple[str, list[tuple]]]) -> dict:
    if not path.is_file():
        raise ValueError("CLI did not create its database")
    metrics = {"file_bytes": path.stat().st_size,
               "wal_bytes": Path(str(path) + "-wal").stat().st_size
               if Path(str(path) + "-wal").exists() else 0}
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        for query, expected in checks:
            actual = db.execute(query).fetchall()
            if actual != expected:
                raise ValueError(f"stock persisted-query mismatch: {query}; "
                                 f"expected {len(expected)} rows, got {len(actual)}")
        metrics["page_size"] = db.execute("PRAGMA page_size").fetchone()[0]
        metrics["page_count"] = db.execute("PRAGMA page_count").fetchone()[0]
        metrics["freelist_count"] = db.execute("PRAGMA freelist_count").fetchone()[0]
        pages = db.execute("SELECT pagetype,ncell,unused,path FROM dbstat WHERE name='ia'").fetchall()
        leaves = [page for page in pages if page[0] == "leaf"]
        if not leaves or sum(page[1] for page in pages) != ROWS:
            raise ValueError("dbstat index leaf and interior records do not account for all 12000 rows")
        if len({page[3].count("/") for page in leaves}) != 1:
            raise ValueError("index leaves have unequal depth")
        metrics.update(index_pages=len(pages), leaf_pages=len(leaves),
                       one_cell_leaves=sum(page[1] == 1 for page in leaves),
                       min_leaf_cells=min(page[1] for page in leaves),
                       max_leaf_cells=max(page[1] for page in leaves),
                       index_unused_bytes=sum(page[2] for page in pages),
                       logical_bytes=metrics["page_count"] * metrics["page_size"])
    return metrics


def native_checks(binary: dict, database: Path, init: Path, checks: list, timeout: float) -> None:
    sql, expected = [".mode list", ".headers off"], []
    for index, (query, rows) in enumerate(checks):
        marker = f"I521_QUERY_{index}"
        sql.extend([f"SELECT '{marker}';", query + ";"])
        expected.extend([marker, *("|".join(map(str, row)) for row in rows)])
    result = subprocess.run(command(binary, database, init), input="\n".join(sql) + "\n",
                            capture_output=True, text=True, timeout=timeout, check=False)
    (database.parent / "verify.stdout").write_text(result.stdout, encoding="utf-8")
    (database.parent / "verify.stderr").write_text(result.stderr, encoding="utf-8")
    if result.returncode or result.stderr.strip() or result.stdout.splitlines() != expected:
        raise ValueError(f"native CLI query verification failed (exit {result.returncode}); see verify logs")


def sample(binary: dict, engine: str, sql_path: Path, checks: list, init: Path,
           directory: Path, timeout: float) -> dict:
    directory.mkdir()
    database = directory / "fresh.db"
    result = {"engine": engine, "directory": str(directory), "errors": []}
    with sql_path.open("rb") as source, (directory / "run.stdout").open("wb") as stdout, \
            (directory / "run.stderr").open("wb") as stderr:
        start = time.perf_counter()
        try:
            completed = subprocess.run(command(binary, database, init), stdin=source,
                                       stdout=stdout, stderr=stderr, timeout=timeout, check=False)
            result["returncode"] = completed.returncode
        except (OSError, subprocess.SubprocessError) as error:
            result["errors"].append(f"CLI: {error}")
        result["wall_seconds"] = time.perf_counter() - start
    if result.get("returncode") != 0:
        result["errors"].append(f"CLI exit: {result.get('returncode')}")
    if (directory / "run.stderr").read_text(encoding="utf-8", errors="replace").strip():
        result["errors"].append("CLI emitted stderr; see run.stderr")
    # Always inspect failed/timeout runs too; partial SQL success is not a pass.
    try:
        result["geometry"] = inspect_file(database, checks)
        result["stock_queries_ok"] = True
        geometry = result["geometry"]
        result["density_ok"] = geometry["leaf_pages"] < 100 and geometry["one_cell_leaves"] == 0
        if engine == "candidate" and not result["density_ok"]:
            result["errors"].append("candidate fails <100 leaves and zero singleton leaves")
    except (OSError, ValueError, sqlite3.Error) as error:
        result["errors"].append(f"stock verification: {error}")
    if database.is_file():
        try:
            native_checks(binary, database, init, checks, timeout)
            result["native_queries_ok"] = True
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            result["errors"].append(f"native verification: {error}")
    return result


def summarize(samples: list[dict], limit: float | None) -> dict:
    summary = {}
    for engine in ("baseline", "candidate", "stock"):
        selected = [item for item in samples if item["engine"] == engine]
        valid = [item["wall_seconds"] for item in selected if not item["errors"]]
        summary[engine] = {"runs": len(selected), "valid_runs": len(valid),
                           "median_wall_seconds": statistics.median(valid) if valid else None}
    if all(summary[engine]["valid_runs"] == summary[engine]["runs"] for engine in summary):
        for reference in ("baseline", "stock"):
            summary[f"candidate_over_{reference}"] = (summary["candidate"]["median_wall_seconds"] /
                                                       summary[reference]["median_wall_seconds"])
    summary["performance_gate_ok"] = (limit is None or "candidate_over_baseline" not in summary or
                                      summary["candidate_over_baseline"] <= limit)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fsqlite", required=True)
    parser.add_argument("--baseline-fsqlite", required=True)
    parser.add_argument("--sqlite", default="sqlite3")
    parser.add_argument("--candidate-revision")
    parser.add_argument("--baseline-revision")
    parser.add_argument("--stock-revision")
    parser.add_argument("--build-note", default="unspecified; binary hashes identify the measured executables")
    parser.add_argument("--order", choices=ORDERS, action="append")
    parser.add_argument("--mode", choices=("autocommit", "begin"), action="append")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--seed", type=int, default=521)
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--max-regression", type=float, help="optional candidate/baseline median time limit")
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if (args.repeats < 1 or not math.isfinite(args.timeout) or args.timeout <= 0 or
            (args.max_regression is not None and
             (not math.isfinite(args.max_regression) or args.max_regression < 1))):
        parser.error("positive repeats/timeout and max-regression >= 1 required")
    if args.output.exists():
        parser.error("output already exists; select a new result path")
    args.work_dir.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="issue521-", dir=args.work_dir.resolve()))
    init = work / "empty-init.sql"
    init.write_text("", encoding="utf-8")
    report = {"schema": 1, "status": "running", "started_utc": datetime.now(timezone.utc).isoformat(),
              "platform": platform.platform(), "python": platform.python_version(),
              "stock_file_oracle_version": sqlite3.sqlite_version, "rows": ROWS, "seed": args.seed,
              "build_note": args.build_note, "work_directory": str(work), "arguments": vars(args).copy(),
              "scope": "wall time: startup + DDL + 12000 individual inserts + optional COMMIT + shutdown; "
                       "input generation, settings probes, and all verification excluded",
              "results": {}, "errors": []}
    for key in ("work_dir", "output"):
        report["arguments"][key] = str(report["arguments"][key])
    started = time.perf_counter()
    try:
        binaries = {engine: binary_info(executable, revision, args.timeout)
                    for engine, executable, revision in
                    (("baseline", args.baseline_fsqlite, args.baseline_revision),
                     ("candidate", args.fsqlite, args.candidate_revision),
                     ("stock", args.sqlite, args.stock_revision))}
        report["binaries"] = binaries
        report["settings"] = {engine: probe_settings(binary, engine, work, init, args.timeout)
                              for engine, binary in binaries.items()}
        comparable = ("journal_mode", "synchronous", "page_size", "concurrent_mode")
        if any(report["settings"]["baseline"][key] != report["settings"]["candidate"][key]
               for key in comparable):
            raise ValueError("baseline and candidate defaults differ; timings are not a matched comparison")
        report["stock_storage_defaults_match_candidate"] = all(
            report["settings"]["stock"][key] == report["settings"]["candidate"][key]
            for key in comparable if key != "concurrent_mode")
        cases = itertools.product(args.order or ORDERS, args.mode or ("autocommit", "begin"))
        for case_number, (order, mode) in enumerate(cases):
            keys = ordered_keys(order, args.seed)
            checks = query_oracles(keys)
            sql = ["CREATE TABLE a(x INTEGER); CREATE INDEX ia ON a(x);"]
            sql += (["BEGIN;"] if mode == "begin" else [])
            sql += [f"INSERT INTO a VALUES({key});" for key in keys]
            sql += (["COMMIT;"] if mode == "begin" else [])
            sql_path = work / f"{case_number}-{order}-{mode}.sql"
            sql_path.write_text("\n".join(sql) + "\n", encoding="utf-8")
            entry = {"sql_path": str(sql_path), "sql_sha256": hashlib.sha256(sql_path.read_bytes()).hexdigest(),
                     "samples": [], "engine_orders": []}
            report["results"][f"{order}/{mode}"] = entry
            for repetition in range(args.repeats):
                # Every three repetitions put each engine in each position;
                # the next three reverse predecessor/successor relationships.
                rotation = 3 * ((case_number + repetition // 3) % 2) + (case_number + repetition) % 3
                engine_order = ROTATIONS[rotation]
                entry["engine_orders"].append(engine_order)
                for engine in engine_order:
                    item = sample(binaries[engine], engine, sql_path, checks, init,
                                  work / f"{case_number}-{repetition}-{engine}", args.timeout)
                    item["repetition"] = repetition
                    entry["samples"].append(item)
                    print(f"{order}/{mode} {repetition} {engine}: {item['wall_seconds']:.6f}s "
                          f"geometry={item.get('geometry')} errors={item['errors']}", file=sys.stderr)
                    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
            entry["summary"] = summarize(entry["samples"], args.max_regression)
        failed = any(item["errors"] for entry in report["results"].values() for item in entry["samples"])
        regressed = any(not entry["summary"]["performance_gate_ok"] for entry in report["results"].values())
        report["status"] = "error" if failed else "regression" if regressed else "ok"
    except (OSError, ValueError, sqlite3.Error, subprocess.SubprocessError) as error:
        report["status"] = "error"
        report["errors"].append(str(error))
    report["total_wall_seconds_including_verification"] = time.perf_counter() - started
    encoded = json.dumps(report, indent=2) + "\n"
    try:
        args.output.write_text(encoded, encoding="utf-8")
    except OSError as error:
        report["status"] = "error"
        report["errors"].append(f"could not save JSON: {error}")
        encoded = json.dumps(report, indent=2) + "\n"
    print(encoded, end="")
    return {"ok": 0, "regression": 1, "error": 2}[report["status"]]


if __name__ == "__main__":
    sys.exit(main())
