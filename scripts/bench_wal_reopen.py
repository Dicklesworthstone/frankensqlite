#!/usr/bin/env python3
"""Compare identical wal_reopen_probe builds on the same immutable native WAL.

Build crates/fsqlite-core/examples/wal_reopen_probe.rs at each revision using
the same toolchain, profile, and feature set. Copy the two executables to stable
paths. This controller alternates them in one invocation and records raw child
CPU, internal operation latency, checksums, binary identities, and file hashes.
The lower-level probe is distinct from end-to-end br show measurements.

Example:
  python3 scripts/bench_wal_reopen.py --baseline /abs/before --candidate /abs/after \
    --baseline-ref SHA --candidate-ref working-tree --build-profile 'dev opt1' \
    --output /abs/new-results --frames 1 11841 33086 178156

Optional --io-counter diagnostics run separately from timing. Outputs and WAL
fixtures are retained; this script never deletes files or overwrites a run.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import statistics
import subprocess
import sys
import time


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def save(path, value):
    with path.open("x") as target:
        json.dump(value, target, indent=2, sort_keys=True)
        target.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline-ref", required=True)
    parser.add_argument("--candidate-ref", required=True)
    parser.add_argument("--build-profile", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--frames", nargs="+", type=int, default=[1, 11841, 33086, 178156])
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--io-counter", type=Path)
    args = parser.parse_args()
    assert sys.platform == "linux", "native probe and optional interposer use Linux"
    assert args.iterations > 0 and args.rounds > 0
    assert all(frames >= 0 for frames in args.frames)
    assert len(set(args.frames)) == len(args.frames)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    binaries = {name: getattr(args, name).resolve() for name in ("baseline", "candidate")}
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(("FSQLITE_", "FRANKENSQLITE_"))
                   and key not in ("RUST_LOG", "LD_PRELOAD", "WAL_IO_OUTPUT")}
    environment["RUST_LOG"] = "error"
    counter = args.io_counter.resolve() if args.io_counter else None
    metadata = {
        "schema": "fsqlite.wal-reopen-probe.v1",
        "arguments": {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        "platform": platform.platform(),
        "python": sys.version,
        "binaries": {name: {"path": str(path), "sha256": sha256(path)} for name, path in binaries.items()},
        "counter": {"path": str(counter), "sha256": sha256(counter)} if counter else None,
        "controller_sha256": sha256(__file__),
        "timing": "warm immutable native WAL; alternating baseline/candidate subprocesses; uninstrumented",
        "cpu_scope": "whole child process including runtime startup, divided by iterations; not CPU of a single internal operation",
    }
    shutil.copyfile(__file__, output / "benchmark-source.py")
    save(output / "metadata.json", metadata)
    observations = []

    def run(label, name, command, *, kind, frames, mode, iterations=1, extra_environment=None):
        invocation = [str(binaries[name]), *map(str, command)]
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        start = time.monotonic_ns()
        child = subprocess.run(invocation, capture_output=True, env=extra_environment or environment,
                               timeout=600, check=False)
        stop = time.monotonic_ns()
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        prefix = output / label
        prefix.with_suffix(".stdout").write_bytes(child.stdout)
        prefix.with_suffix(".stderr").write_bytes(child.stderr)
        row = {"label": label, "binary": name, "kind": kind, "mode": mode, "frames": frames,
               "iterations": iterations, "command": invocation, "exit_code": child.returncode,
               "process_wall_seconds": (stop-start)/1e9,
               "process_user_seconds": after.ru_utime-before.ru_utime,
               "process_system_seconds": after.ru_stime-before.ru_stime}
        row["process_cpu_seconds"] = row["process_user_seconds"] + row["process_system_seconds"]
        row["cpu_seconds_per_iteration"] = row["process_cpu_seconds"] / iterations
        observations.append(row)
        assert child.returncode == 0, f"{label}: {child.stderr.decode(errors='replace')}"
        row["probe"] = json.loads(child.stdout)
        assert row["probe"]["frames"] == frames
        assert row["probe"]["mode"] == mode
        if kind != "fixture":
            assert row["probe"]["iterations"] == iterations
            assert len(row["probe"]["open_ns"]) == iterations
            assert len(row["probe"]["total_ns"]) == iterations
            assert len(row["probe"]["publication_ns"]) == (iterations if mode == "publication" else 0)
        save(prefix.with_suffix(".json"), row)
        return row

    try:
        fixtures = []
        for frames in args.frames:
            assert shutil.disk_usage(output).free > frames * 4120 + 512 * 1024 * 1024
            wal = output / f"frames-{frames}.db-wal"
            created = run(f"create-{frames}", "baseline", ["create", wal, frames],
                          kind="fixture", frames=frames, mode="create")["probe"]
            identity = {"bytes": wal.stat().st_size, "sha256": sha256(wal)}
            fixtures.append({"path": str(wal), **identity, **created})
            for mode in ("open", "publication"):
                tail = [created["checksum1"], created["checksum2"]]
                for name in binaries:
                    run(f"warm-{frames}-{mode}-{name}", name, [mode, wal, frames, 2, *tail],
                        kind="warmup", frames=frames, mode=mode, iterations=2)
                for repetition in range(args.rounds):
                    order = list(binaries)
                    if repetition % 2:
                        order.reverse()
                    for name in order:
                        run(f"timed-{frames}-{mode}-{repetition}-{name}", name,
                            [mode, wal, frames, args.iterations, *tail],
                            kind="sample", frames=frames, mode=mode, iterations=args.iterations)
                if counter:
                    for name in binaries:
                        label = f"io-{frames}-{mode}-{name}"
                        count_path = output / f"{label}.counts.json"
                        row = run(label, name, [mode, wal, frames, 1, *tail],
                                  kind="io-diagnostic", frames=frames, mode=mode,
                                  extra_environment={**environment, "LD_PRELOAD": str(counter),
                                                     "WAL_IO_OUTPUT": str(count_path)})
                        counts = json.loads(count_path.read_text())
                        assert counts.get("overflow_calls", 0) == 0 and counts.get("pread_failed_calls", 0) == 0
                        row["io_counts"] = counts
            assert wal.stat().st_size == identity["bytes"] and sha256(wal) == identity["sha256"], "reader mutated WAL"
        groups = []
        for frames in args.frames:
            for mode in ("open", "publication"):
                for name in binaries:
                    samples = [row for row in observations if row["kind"] == "sample"
                               and row["frames"] == frames and row["mode"] == mode and row["binary"] == name]
                    group = {"frames": frames, "mode": mode, "binary": name, "process_samples": len(samples)}
                    for field in ("open_ns", "publication_ns", "total_ns"):
                        values = [value for row in samples for value in row["probe"][field]]
                        if values:
                            group[field] = {"min": min(values), "median": statistics.median(values), "max": max(values)}
                    values = [row["cpu_seconds_per_iteration"] for row in samples]
                    group["cpu_seconds_per_iteration"] = {"min": min(values), "median": statistics.median(values), "max": max(values)}
                    groups.append(group)
        save(output / "summary.json", {"ok": True, "fixtures": fixtures, "groups": groups})
    finally:
        save(output / "observations.json", observations)


if __name__ == "__main__":
    main()
