#!/usr/bin/env python3
"""Reproduce GH#531 with live baseline, candidate, and C SQLite executables.

python3 scripts/benchmark_issue_531.py --baseline-fsqlite /path/to/old/fsqlite \
    --fsqlite /path/to/new/fsqlite --sqlite /path/to/sqlite3 \
    --build-manifest /path/to/builds.json --work-dir /path/with/free/space

For a small stock-only fixture/protocol check (NO candidate qualification):
python3 scripts/benchmark_issue_531.py --oracle-only --sqlite /path/to/sqlite3 \
    --posts 1000 --users 100 --trials 1 --work-dir /path/with/free/space

The default fixture is the issue's CLI recipe: 50,000 users, 500,000 posts,
and 2,000,000 attempted likes inserts, producing 500,000 DISTINCT likes.
--likes-mode four-per-post instead creates 2,000,000 distinct likes at the
default scale. That is a separately labelled synthetic fixture, not the
reporter's unattached Rust-harness data. Scaling preserves the SQL formulas.

Each engine reads its own initially byte-identical stock-created database.
Native statement timers exclude seeding, process startup, PRAGMA setup, and
untimed warmups. They INCLUDE SQL parsing/preparation and shell result output;
this CLI experiment is not the report's prepared-statement Rust benchmark.
Every warmup and measured result is compared in full with live C SQLite.
Only queries without ORDER BY are compared as multisets. Engine order rotates
through all permutations. Raw SQL, output, timers, initial database hashes,
binary hashes, supplied build provenance, and final integrity checks survive.
No artifacts are deleted and existing result files are never overwritten.

--memory runs a separate Linux process-memory experiment without query timers
or warmups. A fresh child pauses after opening its file database and after each
checked result; /proc VmRSS and VmHWM distinguish first-query high water from
memory retained after repeated queries. Whole-process RSS is not query heap,
and these counters do not measure rows examined.

The comparison build manifest must contain baseline and candidate objects,
each with source_revision, source_sha256 (including Cargo.lock, connection.rs,
and codegen.rs), binary_sha256, command, toolchain, profile, features, and
rustflags. Source/build declarations are supplied provenance, not information
recovered from a stripped executable. The binary hash is verified, and the
compiler, profile, feature selection, flags, and dependency lock must match.
Optional ratio limits fail the run; there is no machine-independent default
speed threshold. A zero-resolution timer never becomes an infinite speedup.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

from benchmark_issue_530 import binary_info, run_memory_cli, summarize_memory, timer_seconds


class Case(NamedTuple):
    sql: str
    ordered: bool
    reported_shape: bool = True


def fixture_sql(users: int, posts: int, likes_mode: str) -> tuple[str, int]:
    if users < 1 or posts < 21:
        raise ValueError("users must be positive and posts must be at least 21")
    if likes_mode == "cli":
        likes = f"i * 48271 % {users} + 1, i * 16807 % {posts} + 1"
        period = math.lcm(users // math.gcd(48271, users), posts // math.gcd(16807, posts))
        expected_likes = min(4 * posts, period)
    elif likes_mode == "four-per-post":
        if users < 4:
            raise ValueError("four-per-post needs at least four users")
        likes = (f"(((i-1) % {posts}) * 48271 + (i-1) / {posts}) % {users} + 1, "
                 f"(i-1) % {posts} + 1")
        expected_likes = 4 * posts
    else:
        raise ValueError(f"unknown likes mode: {likes_mode}")
    return f"""PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT NOT NULL UNIQUE);
CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE likes (user_id INTEGER NOT NULL, post_id INTEGER NOT NULL, PRIMARY KEY (user_id, post_id));
CREATE INDEX posts_created_at_id_idx ON posts (created_at DESC, id DESC);
CREATE INDEX likes_post_id_idx ON likes (post_id);
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < {users})
INSERT INTO users SELECT i, 'user' || i FROM n;
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < {posts})
INSERT INTO posts SELECT i, i * 48271 % {users} + 1, 'post body ' || i, printf('2025-%02d-%02dT%02d:%02d:%02d.%03dZ', i % 12 + 1, i % 28 + 1, i % 24, i % 60, i / 7 % 60, i % 1000) FROM n;
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < {4 * posts})
INSERT OR IGNORE INTO likes SELECT {likes} FROM n;
ANALYZE;
VACUUM;
PRAGMA journal_mode=WAL;
""", expected_likes


def cases(posts: int) -> dict[str, Case]:
    point = posts * 4 // 5
    ids = ",".join(str(posts * i // 21) for i in range(1, 21))
    newest = "ORDER BY p.created_at DESC, p.id DESC LIMIT 20"
    count = "(SELECT count(*) FROM likes l WHERE l.post_id = p.id)"
    join = "FROM posts p JOIN users u ON u.id = p.user_id"
    return {
        "group_eq": Case(f"SELECT post_id, count(*) FROM likes WHERE post_id = {point} GROUP BY post_id;", False),
        "group_in20": Case(f"SELECT post_id, count(*) FROM likes WHERE post_id IN ({ids}) GROUP BY post_id;", False),
        "count_eq_control": Case(f"SELECT count(*) FROM likes WHERE post_id = {point};", False),
        "in20_control": Case(f"SELECT post_id FROM likes WHERE post_id IN ({ids});", False, False),
        "newest20_control": Case(f"SELECT p.id FROM posts p {newest};", True),
        "join_newest20": Case(f"SELECT p.id, u.username {join} {newest};", True),
        "newest20_count_wrapper": Case(f"SELECT count(*) FROM (SELECT p.id FROM posts p {newest});", False),
        "join_newest20_count_wrapper": Case(f"SELECT count(*) FROM (SELECT p.id, u.username {join} {newest});", False),
        "correlated_newest20": Case(f"SELECT p.id, {count} FROM posts p {newest};", True),
        "join_by_id_control": Case(f"SELECT p.id, p.body, p.created_at, u.username {join} WHERE p.id = {point};", False),
        "correlated_by_id_control": Case(f"SELECT p.id, {count} FROM posts p WHERE p.id = {point};", False),
        "join_correlated_by_id": Case(f"SELECT p.id, p.body, p.created_at, u.username, {count} AS like_count {join} WHERE p.id = {point};", False),
        "join_correlated_newest20": Case(f"SELECT p.id, p.body, p.created_at, u.username, {count} AS like_count {join} {newest};", True),
        "join_filtered_offset_control": Case(f"SELECT p.id, u.username {join} WHERE u.id % 2 = 0 {newest} OFFSET 7;", True, False),
    }


def digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def run_cli(binary: dict, database: Path | str, script: str, init: Path,
            directory: Path, stem: str, timeout: float) -> subprocess.CompletedProcess:
    command = [binary["path"], "-batch", "-init", str(init), str(database)]
    (directory / f"{stem}.sql").write_text(script, encoding="utf-8")
    started = time.monotonic()
    record = {"command": command}
    try:
        result = subprocess.run(command, input=script, text=True, capture_output=True,
                                timeout=timeout, check=False)
        record["returncode"] = result.returncode
        return result
    except subprocess.TimeoutExpired as error:
        record["error"] = f"timeout after {timeout} seconds"
        result = subprocess.CompletedProcess(command, -1, error.stdout or b"", error.stderr or b"")
        raise
    finally:
        record["process_wall_seconds"] = time.monotonic() - started
        if "result" in locals():
            for suffix, content in (("stdout", result.stdout), ("stderr", result.stderr)):
                if isinstance(content, bytes):
                    content = content.decode("utf-8", errors="replace")
                (directory / f"{stem}.{suffix}").write_text(content, encoding="utf-8")
        (directory / f"{stem}.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")


def rows(result: subprocess.CompletedProcess) -> list[str]:
    if result.returncode != 0 or result.stderr.strip():
        raise ValueError(f"CLI failed: exit={result.returncode}, stderr={result.stderr!r}")
    return result.stdout.splitlines()


def timing_script(sql: str, warmups: int, iterations: int, marker: str) -> str:
    script = [".mode quote", ".headers off", "PRAGMA synchronous=NORMAL;", "PRAGMA foreign_keys=ON;"]
    for index in range(warmups + iterations):
        script.append(f".print {marker}{index}")
        if index >= warmups:
            script.append(".timer on")
        script.extend([sql, ".timer off"])
    script.extend([f".print {marker}end", ""])
    return "\n".join(script)


def validate_sample(result: subprocess.CompletedProcess, expected: list[str], case: Case,
                    warmups: int, iterations: int, marker: str) -> dict:
    if result.returncode != 0:
        raise ValueError(f"CLI exit {result.returncode}: {result.stderr}")
    timing_streams = [[value for line in text.splitlines()
                       if (value := timer_seconds(line)) is not None]
                      for text in (result.stdout, result.stderr)]
    if all(timing_streams):
        raise ValueError("native timers unexpectedly appeared in both output streams")
    timings = timing_streams[0] + timing_streams[1]
    if len(timings) != iterations or any(not math.isfinite(value) or value < 0 for value in timings):
        raise ValueError(f"expected {iterations} finite nonnegative native timers, got {timings}")
    errors = [line for line in result.stderr.splitlines() if line.strip() and timer_seconds(line) is None]
    if errors:
        raise ValueError(f"CLI errors: {errors!r}")
    output = [line for line in result.stdout.splitlines() if timer_seconds(line) is None]
    normalize = (lambda values: values) if case.ordered else sorted
    position = 0
    for index in range(warmups + iterations):
        if position >= len(output) or output[position] != f"{marker}{index}":
            raise ValueError(f"missing result marker for execution {index}")
        position += 1
        end = position + len(expected)
        if normalize(output[position:end]) != normalize(expected):
            raise ValueError(f"result mismatch in execution {index}: expected={expected!r}, actual={output[position:end]!r}")
        position = end
    if output[position:] != [f"{marker}end"]:
        raise ValueError(f"unexpected trailing output: {output[position:]!r}")
    return {"seconds": statistics.mean(timings), "statement_seconds": timings,
            "warmups_checked": warmups, "measured_results_checked": iterations,
            "rows_per_execution": len(expected), "below_timer_resolution": 0 in timings}


def summarize(samples: list[dict]) -> dict:
    engines = {}
    for engine in dict.fromkeys(sample["engine"] for sample in samples):
        values = [sample["seconds"] for sample in samples if sample["engine"] == engine]
        engines[engine] = {"runs": len(values), "median_seconds": statistics.median(values),
                           "min_seconds": min(values), "max_seconds": max(values)}
    ratios = {}
    for numerator, denominator in (("baseline", "candidate"), ("candidate", "baseline"),
                                   ("baseline", "stock"), ("candidate", "stock")):
        if numerator in engines and denominator in engines:
            top, bottom = engines[numerator]["median_seconds"], engines[denominator]["median_seconds"]
            if top > 0 and bottom > 0:
                ratios[f"{numerator}_over_{denominator}"] = top / bottom
    return {"engines": engines, "ratios": ratios,
            "unresolved_timer_resolution": any(value["median_seconds"] == 0 for value in engines.values())}


def validate_builds(builds: dict, binaries: dict) -> None:
    required = {"source_revision", "source_sha256", "binary_sha256", "command",
                "toolchain", "profile", "features", "rustflags"}
    source_paths = {"Cargo.lock", "crates/fsqlite-core/src/connection.rs", "crates/fsqlite-vdbe/src/codegen.rs"}
    if not isinstance(builds, dict):
        raise ValueError("build manifest must be an object")
    for engine in ("baseline", "candidate"):
        build = builds.get(engine, {})
        if not isinstance(build, dict) or not required <= build.keys():
            raise ValueError(f"{engine} build manifest lacks required provenance")
        source_hashes = build["source_sha256"]
        if not isinstance(source_hashes, dict) or not source_paths <= source_hashes.keys():
            raise ValueError(f"{engine} build manifest lacks required source hashes")
        if any(not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None
               for value in source_hashes.values()):
            raise ValueError(f"{engine} build manifest contains an invalid source hash")
        if build["binary_sha256"] != binaries[engine]["sha256"]:
            raise ValueError(f"{engine} executable does not match its build manifest hash")
        if any(not build[key] for key in ("source_revision", "command", "toolchain", "profile")):
            raise ValueError(f"{engine} source revision, build command, compiler, and profile must be recorded")
    for key in ("toolchain", "profile", "features", "rustflags"):
        if builds["baseline"][key] != builds["candidate"][key]:
            raise ValueError(f"baseline/candidate build {key} differ")
    if builds["baseline"]["source_sha256"]["Cargo.lock"] != builds["candidate"]["source_sha256"]["Cargo.lock"]:
        raise ValueError("baseline and candidate dependency locks differ")
    if len({binary["sha256"] for binary in binaries.values()}) != 3:
        raise ValueError("comparison requires distinct baseline, candidate, and live stock executables")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sqlite", required=True)
    parser.add_argument("--fsqlite")
    parser.add_argument("--baseline-fsqlite")
    parser.add_argument("--build-manifest", type=Path)
    parser.add_argument("--oracle-only", action="store_true")
    parser.add_argument("--posts", type=int, default=500_000)
    parser.add_argument("--users", type=int)
    parser.add_argument("--likes-mode", choices=("cli", "four-per-post"), default="cli")
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--memory", action="store_true",
                        help="separate Linux process-RSS experiment; no query timings or warmups")
    parser.add_argument("--memory-queries", type=int,
                        help="checked queries per fresh process in --memory mode (default 3)")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--case", action="append", dest="case_names")
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--max-candidate-over-baseline", type=float)
    parser.add_argument("--max-candidate-over-stock", type=float)
    args = parser.parse_args()
    users = args.users if args.users is not None else max(1, args.posts // 10)
    if args.trials < 1 or args.iterations < 1 or args.warmups < 0 or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("trials/iterations/timeout must be positive and warmups nonnegative")
    limits = {"candidate_over_baseline": args.max_candidate_over_baseline,
              "candidate_over_stock": args.max_candidate_over_stock}
    if any(limit is not None and (not math.isfinite(limit) or limit <= 0) for limit in limits.values()):
        parser.error("ratio limits must be finite and positive")
    if not args.memory and args.memory_queries is not None:
        parser.error("--memory-queries requires --memory")
    if args.memory:
        if not sys.platform.startswith("linux"):
            parser.error("--memory requires Linux /proc VmRSS and VmHWM")
        if any(limits.values()):
            parser.error("timing ratio limits cannot be used in --memory mode")
        if args.warmups != 1 or args.iterations != 1:
            parser.error("--memory does not accept timing warmups/iterations; use --memory-queries")
    if args.memory_queries is None:
        args.memory_queries = 3
    if args.memory_queries < 1:
        parser.error("--memory-queries must be positive")
    if args.oracle_only:
        if args.fsqlite or args.baseline_fsqlite or args.build_manifest or any(limits.values()):
            parser.error("oracle-only mode cannot carry candidate/baseline identities or speed gates")
    elif not (args.fsqlite and args.baseline_fsqlite and args.build_manifest):
        parser.error("comparison requires both FrankenSQLite executables and --build-manifest")
    try:
        seed, expected_likes = fixture_sql(users, args.posts, args.likes_mode)
    except ValueError as error:
        parser.error(str(error))
    all_cases = cases(args.posts)
    selected = args.case_names or list(all_cases)
    if len(set(selected)) != len(selected) or any(name not in all_cases for name in selected):
        parser.error(f"--case must select distinct names from {', '.join(all_cases)}")
    if args.output and args.output.exists():
        parser.error("--output already exists; choose a new result file")
    args.work_dir.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="issue531-", dir=args.work_dir))
    init = directory / "empty-init.sql"
    init.write_text("", encoding="utf-8")
    output = args.output or directory / "result.json"
    report = {"schema": "issue531-cli-v2", "status": "running",
              "mode": "oracle_only" if args.oracle_only else "baseline_candidate_stock",
              "measurement_mode": "memory" if args.memory else "timing",
              "created_utc": datetime.now(timezone.utc).isoformat(), "platform": platform.platform(),
              "artifact_directory": str(directory.resolve()), "measurement": "native CLI statement timers; parse/prepare and result output included",
              "fixture": {"users": users, "posts": args.posts, "attempted_likes": 4 * args.posts,
                          "expected_unique_likes": expected_likes, "likes_mode": args.likes_mode,
                          "matches_reported_cli_scale": (users, args.posts, args.likes_mode) == (50_000, 500_000, "cli")},
              "trials": args.trials, "iterations_per_sample": args.iterations, "warmups_per_sample": args.warmups,
              "ratio_limits": limits, "cases": {}, "errors": []}
    if args.memory:
        for field in ("iterations_per_sample", "warmups_per_sample", "ratio_limits"):
            report.pop(field)
        report.update({
            "measurement": "Linux /proc VmRSS and VmHWM after opening the database and each checked query; fresh process per sample",
            "memory_queries_per_process": args.memory_queries,
            "memory_interpretation": "Approximate whole-process resident counters, not query heap. First-query high water includes process startup, database opening and setup, but excludes later queries and shutdown. Post-query RSS includes caches and allocator-retained storage; retained growth alone is not proof of a leak. No rows-examined metric is inferred from memory or result cardinality.",
        })
    try:
        builds = json.loads(args.build_manifest.read_text()) if args.build_manifest else {}
        if not isinstance(builds, dict):
            raise ValueError("build manifest must be an object")
        if not args.oracle_only and any(not isinstance(builds.get(engine), dict)
                                        for engine in ("baseline", "candidate")):
            raise ValueError("build manifest must contain baseline and candidate objects")
        binaries = {"stock": binary_info(args.sqlite, None, args.timeout)}
        if not args.oracle_only:
            binaries.update({engine: binary_info(executable, builds.get(engine, {}).get("source_revision"), args.timeout)
                             for engine, executable in (("baseline", args.baseline_fsqlite), ("candidate", args.fsqlite))})
            validate_builds(builds, binaries)
        report.update({"binaries": binaries, "supplied_build_provenance": builds})
        stock = binaries["stock"]
        report["stock_identity"] = rows(run_cli(stock, ":memory:", ".mode quote\nSELECT sqlite_version(), sqlite_source_id();\n", init, directory, "stock-identity", args.timeout))
        database = directory / "seed.db"
        seed_rows = rows(run_cli(stock, database, seed, init, directory, "seed", args.timeout))
        # Recent stock shells enable defensive mode and decline journal_mode=OFF
        # with the actual value "delete". Seeding is untimed; retain that fact
        # and require the requested final WAL mode in either shell configuration.
        report["fixture"]["seed_journal_mode_outputs"] = seed_rows
        if seed_rows not in (["off", "wal"], ["delete", "wal"]):
            raise ValueError(f"unexpected fixture setup output: {seed_rows!r}")
        verification = (".mode quote\nPRAGMA integrity_check;\n"
                        "SELECT (SELECT count(*) FROM users), (SELECT count(*) FROM posts), (SELECT count(*) FROM likes);\n")
        expected_verification = ["'ok'", f"{users},{args.posts},{expected_likes}"]
        if rows(run_cli(stock, database, verification, init, directory, "verify-seed", args.timeout)) != expected_verification:
            raise ValueError("fixture integrity or independently computed row counts differ")
        report["fixture"].update({"seed_sha256": digest(database), "database_bytes": database.stat().st_size})
        databases = {}
        for engine in binaries:
            target = directory / f"{engine}.db"
            shutil.copyfile(database, target)
            if digest(target) != report["fixture"]["seed_sha256"]:
                raise ValueError(f"{engine} database copy differs before measurement")
            databases[engine] = target
        engine_order = ["baseline", "candidate", "stock"] if not args.oracle_only else ["stock"]
        rotations = list(itertools.permutations(engine_order))
        for name in selected:
            case = all_cases[name]
            expected = rows(run_cli(stock, database, f".mode quote\n.headers off\n{case.sql}\n", init, directory, f"{name}-oracle", args.timeout))
            item = {"sql": case.sql, "ordered_comparison": case.ordered, "reported_shape": case.reported_shape,
                    "oracle_rows": expected, "query_plans": {}, "samples": [], "errors": [], "summary": None}
            report["cases"][name] = item
            for engine, binary in binaries.items():
                item["query_plans"][engine] = rows(run_cli(binary, databases[engine], f".mode list\nEXPLAIN QUERY PLAN {case.sql}\n", init, directory, f"{name}-{engine}-eqp", args.timeout))
            for trial in range(args.trials):
                for order, engine in enumerate(rotations[trial % len(rotations)]):
                    try:
                        stem = f"{name}-{trial}-{engine}"
                        if args.memory:
                            sample = run_memory_cli(
                                binaries[engine], case.sql, expected,
                                [".mode quote", ".headers off", "PRAGMA synchronous=NORMAL;", "PRAGMA foreign_keys=ON;"],
                                args.memory_queries, init, directory, args.timeout, stem,
                                database=databases[engine], ordered=case.ordered)
                            sample.update({"measured_results_checked": args.memory_queries,
                                           "rows_per_execution": len(expected)})
                        else:
                            marker = f"__issue531_{uuid.uuid4().hex}_"
                            script = timing_script(case.sql, args.warmups, args.iterations, marker)
                            result = run_cli(binaries[engine], databases[engine], script, init, directory, stem, args.timeout)
                            sample = validate_sample(result, expected, case, args.warmups, args.iterations, marker)
                        item["samples"].append({"engine": engine, "trial": trial, "engine_order": order, **sample})
                    except (OSError, ValueError, subprocess.SubprocessError) as error:
                        item["errors"].append({"engine": engine, "trial": trial, "message": str(error)})
            if not item["errors"]:
                item["summary"] = ({"engines": summarize_memory(item["samples"])} if args.memory
                                   else summarize(item["samples"]))
                for ratio, limit in limits.items():
                    if limit is not None:
                        measured = item["summary"]["ratios"].get(ratio)
                        if measured is None or measured > limit:
                            item["errors"].append({"ratio": ratio, "value": measured, "limit": limit})
            if item["errors"]:
                report["errors"].append(f"{name}: correctness, execution, timer, or performance gate failed")
        for engine, path in databases.items():
            if rows(run_cli(stock, path, verification, init, directory, f"verify-final-{engine}", args.timeout)) != expected_verification:
                raise ValueError(f"{engine} final integrity/row counts changed")
        report["status"] = "passed" if not report["errors"] else "failed"
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        report["errors"].append(str(error))
        report["status"] = "failed"
    finally:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("x", encoding="utf-8") as destination:
            json.dump(report, destination, indent=2)
            destination.write("\n")
        print(f"{report['status']}: {output}")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
