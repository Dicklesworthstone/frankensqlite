#!/usr/bin/env python3
"""Compare the original GH#518 reproducer before and after a storage change.

Build two copies of the reproducer published in
https://github.com/Dicklesworthstone/frankensqlite/issues/518, linking one to
baseline FrankenSQLite and one to the candidate. This runner does not build,
patch, or change the settings of either binary. Both must implement the issue's
``write|verify database total_MiB transaction_MiB row_KiB`` interface.

Example (Linux; fresh output directory required):
    python3 scripts/verify_wal_commit_memory.py \
        --baseline /build/baseline/fsqlite-wal-rss-repro \
        --candidate /build/candidate/fsqlite-wal-rss-repro \
        --output /tmp/gh518-comparison

Each repetition runs the 384 MiB single transaction and 24 x 16 MiB control
in separate processes and fresh databases for BOTH binaries. Run order is
seeded and shuffled. Every database gets independent, streaming stock-SQLite
readback BEFORE the reproducer's own fresh-process verification. Files and logs
are retained, including on failure; nothing is deleted or overwritten.

The default gate requires at least 25% lower large-case peak and post-close
RSS, and permits at most 10% higher small-case peak. These are explicit test
thresholds, not claimed results. Wall times and COMMIT endpoint RSS deltas are
reported too. VmHWM is a WHOLE-PROCESS peak, not a COMMIT-only peak: the original
reproducer does not reset it. Linux RSS and allocator retention are not exact
live-allocation measurements. Passing this runner is not a substitute for
native cancellation, fault-injection, fsync-ordering, and crash-recovery tests.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import sqlite3
import statistics
import subprocess
import sys
import time
from typing import Any

MIB = 1024 * 1024
KIB = 1024


class VerificationError(Exception):
    """A workload, its evidence, or a comparison failed verification."""


def integer(record: dict[str, Any], key: str, minimum: int = 0) -> int:
    value = record.get(key)
    if type(value) is not int or value < minimum:
        raise VerificationError(f"invalid {key}: {value!r}")
    return value


def records_from(path: Path) -> list[dict[str, Any]]:
    records = []
    with path.open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise VerificationError(f"{path}:{number}: invalid JSON") from error
            if not isinstance(record, dict):
                raise VerificationError(f"{path}:{number}: expected an object")
            records.append(record)
    return records


def dimensions(total_mib: int, transaction_mib: int, row_kib: int) -> tuple[int, int, int]:
    if any(type(value) is not int or value <= 0 for value in (total_mib, transaction_mib, row_kib)):
        raise VerificationError("workload sizes must be positive integers")
    total_bytes, transaction_bytes, row_bytes = total_mib * MIB, transaction_mib * MIB, row_kib * KIB
    if total_bytes % row_bytes or transaction_bytes % row_bytes:
        raise VerificationError("total and transaction sizes must be divisible by the row size")
    return total_bytes // row_bytes, transaction_bytes // row_bytes, row_bytes


def writer_metrics(
    records: list[dict[str, Any]], total_mib: int, transaction_mib: int,
    row_kib: int, sampled_peak_kib: int = 0,
) -> dict[str, Any]:
    total_rows, transaction_rows, row_bytes = dimensions(total_mib, transaction_mib, row_kib)
    if not records or records[0].get("phase") != "start":
        raise VerificationError("missing initial writer start checkpoint")
    committed = 0
    progress = 0
    pending = None
    all_committed = False
    closed = False
    commits = []
    peak = sampled_peak_kib
    previous_ms = 0
    for index, record in enumerate(records):
        phase = record.get("phase")
        rows = integer(record, "rows")
        payload_bytes = integer(record, "payload_bytes")
        elapsed_ms = integer(record, "elapsed_ms")
        rss = integer(record, "rss_kib", 1)
        peak = max(peak, rss, integer(record, "hwm_kib", 1))
        if elapsed_ms < previous_ms or closed:
            raise VerificationError("non-monotonic checkpoints or output after close")
        previous_ms = elapsed_ms
        if phase == "start":
            if index != 0 or rows != 0 or payload_bytes != 0:
                raise VerificationError("invalid or duplicated start checkpoint")
        elif phase == "inserting":
            last = min(committed + transaction_rows, total_rows)
            if pending is not None or all_committed or not max(committed, progress) < rows <= last:
                raise VerificationError("insertion progress outside an active transaction")
            if payload_bytes != rows * row_bytes:
                raise VerificationError("incorrect cumulative insertion payload")
            progress = rows
        elif phase == "commit_begin":
            last = min(committed + transaction_rows, total_rows)
            if pending is not None or all_committed or committed == total_rows or rows != last:
                raise VerificationError("unexpected COMMIT start or transaction size")
            if payload_bytes != (last - committed) * row_bytes:
                raise VerificationError("incorrect COMMIT payload size")
            pending = record
        elif phase == "commit_end":
            if pending is None or rows != pending["rows"] or payload_bytes != pending["payload_bytes"]:
                raise VerificationError("COMMIT end does not match its start")
            commits.append({
                "rows": rows, "payload_bytes": payload_bytes,
                "rss_before_kib": pending["rss_kib"], "rss_after_kib": rss,
                "rss_delta_kib": rss - pending["rss_kib"],
                "elapsed_ms": elapsed_ms - pending["elapsed_ms"],
            })
            committed = rows
            pending = None
        elif phase == "all_transactions_committed":
            if pending is not None or all_committed or committed != total_rows:
                raise VerificationError("premature or duplicated all-committed checkpoint")
            if rows != total_rows or payload_bytes != total_rows * row_bytes:
                raise VerificationError("incorrect all-committed totals")
            all_committed = True
        elif phase == "closed_without_checkpoint":
            if not all_committed or rows != total_rows or payload_bytes != total_rows * row_bytes:
                raise VerificationError("missing all-committed checkpoint or incorrect close totals")
            closed = True
        else:
            raise VerificationError(f"unexpected writer phase: {phase!r}")
    if not closed or not commits:
        raise VerificationError("writer exited without a complete committed-and-closed workload")
    return {
        "transactions": len(commits), "commits": commits,
        "peak_rss_kib": peak, "retained_after_close_kib": records[-1]["rss_kib"],
        "max_commit_endpoint_delta_kib": max(item["rss_delta_kib"] for item in commits),
        "commit_elapsed_ms": sum(item["elapsed_ms"] for item in commits),
        "writer_elapsed_ms": records[-1]["elapsed_ms"],
    }


def verifier_digest(records: list[dict[str, Any]], total_mib: int, row_kib: int) -> str:
    total_rows, _, row_bytes = dimensions(total_mib, total_mib, row_kib)
    if len(records) != 2 or records[0].get("phase") != "start":
        raise VerificationError("unexpected fresh-process verifier output")
    if integer(records[0], "rows") != 0 or integer(records[0], "payload_bytes") != 0:
        raise VerificationError("invalid verifier start checkpoint")
    record = records[1]
    digest = record.get("fnv1a64")
    if record.get("phase") != "all_bytes_verified":
        raise VerificationError("fresh-process verifier did not verify all bytes")
    if integer(record, "rows") != total_rows or integer(record, "payload_bytes") != total_rows * row_bytes:
        raise VerificationError("verifier row/payload totals differ from the requested workload")
    if not isinstance(digest, str) or len(digest) != 16 or any(char not in "0123456789abcdef" for char in digest):
        raise VerificationError("invalid FNV-1a digest")
    return digest


def stock_readback(path: Path, total_mib: int, row_kib: int) -> dict[str, Any]:
    """Independently check all IDs and payload bytes without loading the table."""
    total_rows, _, row_bytes = dimensions(total_mib, total_mib, row_kib)
    if path.is_symlink() or not path.is_file():
        raise VerificationError("writer did not create a regular database file")
    expected = bytes(index % 251 for index in range(row_bytes))
    digest = hashlib.sha256()
    seen = 0
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        if connection.execute("PRAGMA journal_mode").fetchone() != ("wal",):
            raise VerificationError("database is not in WAL mode")
        if connection.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise VerificationError("stock SQLite integrity_check failed")
        for identifier, payload in connection.execute("SELECT id,payload FROM records ORDER BY id"):
            seen += 1
            if type(identifier) is not int or identifier != seen or type(payload) is not bytes or payload != expected:
                raise VerificationError(f"stock SQLite readback mismatch at row {seen}")
            digest.update(payload)
        if seen != total_rows:
            raise VerificationError(f"stock SQLite read {seen} rows, expected {total_rows}")
    finally:
        connection.close()
    return {"rows": seen, "payload_bytes": seen * row_bytes, "sha256": digest.hexdigest()}


def rss_kib(pid: int) -> int:
    try:
        with Path(f"/proc/{pid}/status").open(encoding="ascii") as stream:
            for line in stream:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (FileNotFoundError, ProcessLookupError):
        pass
    return 0


def run_process(command: list[str], directory: Path, label: str, timeout: float) -> dict[str, Any]:
    stdout_path = directory / f"{label}.stdout.jsonl"
    stderr_path = directory / f"{label}.stderr.log"
    peak = 0
    started = time.monotonic()
    with stdout_path.open("x", encoding="utf-8") as stdout, stderr_path.open("x", encoding="utf-8") as stderr:
        process = subprocess.Popen(command, stdout=stdout, stderr=stderr)
        try:
            while process.poll() is None:
                peak = max(peak, rss_kib(process.pid))
                if time.monotonic() - started > timeout:
                    raise VerificationError(f"{label} timed out; logs retained in {directory}")
                time.sleep(0.01)
            if process.returncode != 0:
                raise VerificationError(f"{label} exited {process.returncode}; logs retained in {directory}")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
    return {
        "command": command, "stdout": str(stdout_path), "stderr": str(stderr_path),
        "sampled_peak_kib": peak, "process_elapsed_ms": (time.monotonic() - started) * 1000,
    }


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(MIB), b""):
            digest.update(block)
    return digest.hexdigest()


def compare_runs(runs: list[dict[str, Any]], repetitions: int, peak_ratio: float,
                 retained_ratio: float, control_ratio: float) -> dict[str, Any]:
    if type(repetitions) is not int or repetitions < 1:
        raise VerificationError("repetitions must be a positive integer")
    if any(not math.isfinite(value) or value <= 0 for value in (peak_ratio, retained_ratio, control_ratio)):
        raise VerificationError("comparison ratios must be finite and positive")
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    identities = set()
    for run in runs:
        key = (run["implementation"], run["case"])
        identity = (*key, run["repetition"])
        if key not in {(impl, case) for impl in ("baseline", "candidate") for case in ("large", "small")}:
            raise VerificationError("unknown comparison arm")
        if identity in identities or type(run["repetition"]) is not int or not 0 <= run["repetition"] < repetitions:
            raise VerificationError("duplicated or out-of-range comparison run")
        identities.add(identity)
        groups.setdefault(key, []).append(run)
    if len(groups) != 4 or any(len(group) != repetitions for group in groups.values()):
        raise VerificationError("incomplete baseline/candidate and large/small comparison")
    if len({run["fnv1a64"] for run in runs}) != 1 or len({run["stock_readback"]["sha256"] for run in runs}) != 1:
        raise VerificationError("readback digests differ across implementations, transaction sizes, or repetitions")
    medians: dict[str, dict[str, dict[str, float]]] = {}
    metrics = ("peak_rss_kib", "retained_after_close_kib", "max_commit_endpoint_delta_kib", "commit_elapsed_ms", "writer_elapsed_ms")
    for (implementation, case), group in groups.items():
        for run in group:
            for metric in metrics:
                minimum = 1 if metric in ("peak_rss_kib", "retained_after_close_kib") else (-sys.maxsize if metric == "max_commit_endpoint_delta_kib" else 0)
                integer(run["metrics"], metric, minimum)
        medians.setdefault(implementation, {})[case] = {
            metric: statistics.median(run["metrics"][metric] for run in group) for metric in metrics
        }
    gates = []
    for case, metric, limit in (
        ("large", "peak_rss_kib", peak_ratio),
        ("large", "retained_after_close_kib", retained_ratio),
        ("small", "peak_rss_kib", control_ratio),
    ):
        baseline = medians["baseline"][case][metric]
        candidate = medians["candidate"][case][metric]
        gates.append({"case": case, "metric": metric, "candidate_over_baseline": candidate / baseline,
                      "maximum_ratio": limit, "passed": candidate <= baseline * limit})
    return {"medians": medians, "gates": gates, "passed": all(gate["passed"] for gate in gates)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="new directory; all databases and logs are retained")
    parser.add_argument("--baseline-revision", default="unspecified", help="declared source revision, not inferred from binary")
    parser.add_argument("--candidate-revision", default="unspecified", help="declared source revision, not inferred from binary")
    parser.add_argument("--total-mib", type=int, default=384)
    parser.add_argument("--small-transaction-mib", type=int, default=16)
    parser.add_argument("--row-kib", type=int, default=32)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--seed", type=int, default=518)
    parser.add_argument("--timeout-seconds", type=float, default=900)
    parser.add_argument("--max-large-peak-ratio", type=float, default=0.75)
    parser.add_argument("--max-large-retained-ratio", type=float, default=0.75)
    parser.add_argument("--max-small-peak-ratio", type=float, default=1.10)
    args = parser.parse_args(argv)
    output = args.output.resolve()
    report: dict[str, Any] = {"schema_version": 1, "status": "error", "runs": []}
    created_output = False
    try:
        dimensions(args.total_mib, args.small_transaction_mib, args.row_kib)
        if args.small_transaction_mib >= args.total_mib:
            raise VerificationError("small-transaction control must be smaller than the total payload")
        if args.repetitions < 1 or not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0:
            raise VerificationError("repetitions and timeout must be positive")
        ratios = (args.max_large_peak_ratio, args.max_large_retained_ratio, args.max_small_peak_ratio)
        if any(not math.isfinite(value) or value <= 0 for value in ratios):
            raise VerificationError("comparison ratios must be finite and positive")
        if not Path("/proc/self/status").is_file():
            raise VerificationError("Linux /proc RSS measurements are required")
        binaries = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()}
        for binary in binaries.values():
            if not binary.is_file() or not os.access(binary, os.X_OK):
                raise VerificationError(f"not an executable reproducer: {binary}")
        hashes = {name: file_sha256(path) for name, path in binaries.items()}
        output.mkdir(parents=True, exist_ok=False)
        created_output = True
        report.update({
            "platform": platform.platform(), "python": platform.python_version(),
            "stock_sqlite_version": sqlite3.sqlite_version,
            "binaries": {name: {"path": str(path), "sha256": hashes[name]} for name, path in binaries.items()},
            "declared_source_revisions": {"baseline": args.baseline_revision, "candidate": args.candidate_revision},
            "workload": {"total_mib": args.total_mib, "small_transaction_mib": args.small_transaction_mib,
                         "row_kib": args.row_kib, "repetitions": args.repetitions, "seed": args.seed},
            "metric_scope": "peak_rss_kib is whole writer process; COMMIT deltas are endpoint RSS, not peak inside COMMIT",
        })
        rng = random.Random(args.seed)
        for repetition in range(args.repetitions):
            order = [(implementation, case) for implementation in binaries for case in ("large", "small")]
            rng.shuffle(order)
            for implementation, case in order:
                directory = output / f"{repetition:03d}-{implementation}-{case}"
                directory.mkdir()
                database = directory / "records.sqlite"
                transaction_mib = args.total_mib if case == "large" else args.small_transaction_mib
                parameters = [str(database), str(args.total_mib), str(transaction_mib), str(args.row_kib)]
                run: dict[str, Any] = {"implementation": implementation, "case": case, "repetition": repetition}
                report["runs"].append(run)
                run["write"] = run_process([str(binaries[implementation]), "write", *parameters], directory, "write", args.timeout_seconds)
                run["metrics"] = writer_metrics(records_from(Path(run["write"]["stdout"])), args.total_mib,
                                                transaction_mib, args.row_kib, run["write"]["sampled_peak_kib"])
                run["stock_readback"] = stock_readback(database, args.total_mib, args.row_kib)
                run["verify"] = run_process([str(binaries[implementation]), "verify", *parameters], directory, "verify", args.timeout_seconds)
                run["fnv1a64"] = verifier_digest(records_from(Path(run["verify"]["stdout"])), args.total_mib, args.row_kib)
                print(f"{repetition + 1}/{args.repetitions} {implementation} {case}: verified; peak {run['metrics']['peak_rss_kib']} KiB", flush=True)
        if any(file_sha256(path) != hashes[name] for name, path in binaries.items()):
            raise VerificationError("a reproducer binary changed during measurement")
        report["comparison"] = compare_runs(report["runs"], args.repetitions, *ratios)
        report["status"] = "passed" if report["comparison"]["passed"] else "failed"
        return 0 if report["comparison"]["passed"] else 1
    except (VerificationError, OSError, sqlite3.Error, ValueError) as error:
        report["error"] = str(error)
        print(f"verification error: {error}", file=sys.stderr)
        return 2
    finally:
        if created_output:
            with (output / "report.json").open("x", encoding="utf-8") as stream:
                json.dump(report, stream, indent=2, allow_nan=False)
                stream.write("\n")
            print(f"Report and retained databases: {output}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
