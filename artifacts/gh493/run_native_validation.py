#!/usr/bin/env python3
"""Apply and validate GH493 on a clean checkout; never move a remote ref.

The runner creates real Rust source changes before Cargo starts. Only a passing
receipt may be used to stage a Git commit. No build-time source rewriting, fake
backend, substituted dependency, or ignored correctness regression is allowed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys
import time

import build_candidate_patch as candidate

ALLOWED_PATHS = [
    candidate.SOURCE,
    "crates/fsqlite-core/src/connection/cte_storage.rs",
    "crates/fsqlite-core/tests/gh493_pager_contract.rs",
]
CARGO = ["cargo", "test", "--release", "--locked", "-p", "fsqlite-core",
         "--no-default-features", "--features", "native,ext-json"]
PROFILE = ["--test", "gh493_schema_only_cte", "gh493_isolated_profile", "--",
           "--ignored", "--exact", "--nocapture", "--test-threads=1"]


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True).strip()


def run_logged(command: list[str], path: Path, extra_env: dict[str, str] | None = None) -> str:
    environment = os.environ.copy()
    environment.update(extra_env or {})
    with path.open("w", encoding="utf-8") as log:
        log.write("COMMAND " + json.dumps(command) + "\n")
        log.flush()
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, encoding="utf-8", errors="replace", env=environment)
        assert process.stdout is not None
        try:
            for line in process.stdout:
                log.write(line)
                log.flush()
                sys.stdout.write(line)
                sys.stdout.flush()
            code = process.wait()
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait()
    if code:
        raise RuntimeError(f"{command[0]} failed with exit {code}; see {path.name}")
    return path.read_text(encoding="utf-8")


def assert_test_count(log: str, minimum: int, *, exact: int | None = None) -> int:
    summaries = re.findall(r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", log)
    if not summaries or any(int(failed) for _, failed, _ in summaries):
        raise ValueError("missing successful native libtest summary")
    passed = sum(int(count) for count, _, _ in summaries)
    if passed < minimum or (exact is not None and passed != exact):
        raise ValueError(f"native test count {passed}, expected at least {minimum}, exact={exact}")
    return passed


def measurements(log: str, expectation: str) -> list[dict]:
    rows = [json.loads(line.partition("GH493_MEASUREMENT ")[2])
            for line in log.splitlines() if "GH493_MEASUREMENT " in line]
    if len(rows) != 9:
        raise ValueError(f"expected nine isolated measurements, found {len(rows)}")
    for shape in ("point", "join", "cte"):
        samples = [row for row in rows if row["shape"] == shape]
        if len(samples) != 3:
            raise ValueError(f"expected three {shape} samples")
        for sample in samples:
            if sample["expectation"] != expectation:
                raise ValueError("profile did not run under the selected expectation")
            if sample["query_ns"] <= 0 or sample["open_ns"] <= 0:
                raise ValueError("profile has no measured duration")
            if not (8 * 1024 * 1024 <= sample["fixture_bytes"] <= 24 * 1024 * 1024):
                raise ValueError("fixture geometry changed")
            if sample["result_rows"] != (1 if shape == "point" else 100):
                raise ValueError("profile result shape changed")
            hydrated = sample["hydrated_rows"]
            if expectation == "baseline" and shape == "cte":
                if hydrated < sample["bulk_rows"]:
                    raise ValueError("baseline did not reproduce unrelated-table hydration")
            elif hydrated != 0:
                raise ValueError(f"{expectation} {shape} hydrated {hydrated} persistent rows")
            if sample["rss_growth_kib"] is None or sample["hwm_after_kib"] is None:
                raise ValueError("Linux native memory measurements are missing")
    return rows


def summarize(rows: list[dict]) -> dict:
    return {shape: {
        "median_query_ns": statistics.median(row["query_ns"] for row in rows if row["shape"] == shape),
        "median_rss_growth_kib": statistics.median(row["rss_growth_kib"] for row in rows if row["shape"] == shape),
        "hydrated_rows": [row["hydrated_rows"] for row in rows if row["shape"] == shape],
    } for shape in ("point", "join", "cte")}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    args = parser.parse_args()
    results = args.results.resolve()
    results.mkdir(parents=True, exist_ok=True)
    root = Path(git("rev-parse", "--show-toplevel")).resolve()
    os.chdir(root)
    if results == root or root in results.parents:
        raise ValueError("keep validation artifacts outside the source checkout")
    if git("status", "--porcelain"):
        raise ValueError("validation requires a clean checkout")
    for executable in ("cargo", "rustc", "rustfmt"):
        if not shutil.which(executable):
            raise RuntimeError(f"missing native toolchain executable: {executable}")
    base = git("rev-parse", "HEAD")
    source_blob = git("hash-object", candidate.SOURCE)
    if source_blob != candidate.BASE_BLOB:
        raise ValueError(f"unreviewed source drift: {source_blob}")
    provenance = {
        "base_commit": base, "base_source_blob": source_blob,
        "toolchain": Path("rust-toolchain.toml").read_text(),
        "rustc": subprocess.check_output(["rustc", "-Vv"], text=True),
        "kernel": subprocess.check_output(["uname", "-a"], text=True),
        "build_environment": {key: value for key, value in os.environ.items()
                              if key.startswith("CARGO_PROFILE_") or key in ("RUSTFLAGS", "CARGO_BUILD_JOBS")},
        "profile": "release", "started_unix": time.time(),
    }
    (results / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    before_log = run_logged(CARGO + PROFILE, results / "before.log", {"GH493_EXPECT": "baseline"})
    assert_test_count(before_log, 10, exact=10)  # nine child tests and the parent
    before = measurements(before_log, "baseline")
    (results / "before.json").write_text(json.dumps(before, indent=2) + "\n")

    patch = candidate.build_patch(root, Path(__file__).resolve().parent)
    patch_path = results / "candidate.patch"
    patch_path.write_text(patch, encoding="utf-8")
    run_logged(["git", "apply", "--check", str(patch_path)], results / "apply-check.log")
    run_logged(["git", "apply", str(patch_path)], results / "apply.log")
    run_logged(["rustfmt", "--edition", "2024", *ALLOWED_PATHS[1:]], results / "rustfmt.log")
    # Keep formatting scoped to the two new files, not the 14 MB legacy module.
    run_logged(["rustfmt", "--edition", "2024", "--check", *ALLOWED_PATHS[1:]], results / "rustfmt-check.log")
    tests: dict[str, int] = {}
    cases = [
        ("contract", ["--test", "gh493_pager_contract"], 7),
        ("delegation", ["--lib", "bounded_cte_delegation_preserves_child_policy_and_zero_hydration"], 1),
        ("existing-cte", ["--lib", "cte"], None),
        ("existing-attach", ["--lib", "attach"], None),
        ("existing-temp", ["--lib", "temp"], None),
    ]
    for label, selection, exact in cases:
        log = run_logged(CARGO + selection + ["--", "--nocapture", "--test-threads=1"], results / (label + ".log"))
        tests[label] = assert_test_count(log, 1, exact=exact)
    run_logged(["cargo", "clippy", "--release", "--locked", "-p", "fsqlite-core",
                "--no-default-features", "--features", "native,ext-json", "--lib",
                "--test", "gh493_pager_contract", "--test", "gh493_schema_only_cte",
                "--", "-D", "warnings"], results / "clippy.log")
    after_log = run_logged(CARGO + PROFILE, results / "after.log", {"GH493_EXPECT": "bounded"})
    assert_test_count(after_log, 10, exact=10)
    after = measurements(after_log, "bounded")
    if {row["fixture_bytes"] for row in before} != {row["fixture_bytes"] for row in after}:
        raise ValueError("before/after fixture geometry differs")
    (results / "after.json").write_text(json.dumps(after, indent=2) + "\n")

    subprocess.run(["git", "add", "--", *ALLOWED_PATHS], check=True)
    changed = git("diff", "--cached", "--name-only").splitlines()
    if sorted(changed) != sorted(ALLOWED_PATHS) or git("diff", "--name-only") or git("ls-files", "--others", "--exclude-standard"):
        raise ValueError("validation changed files outside the three intended engine paths")
    run_logged(["git", "diff", "--cached", "--check"], results / "diff-check.log")
    (results / "integrated.patch").write_text(subprocess.check_output(["git", "diff", "--cached", "--binary"], text=True))
    receipt = {
        **provenance, "native_gates_passed": True, "tests": tests,
        "before_summary": summarize(before), "after_summary": summarize(after),
        "files": [{"path": path, "blob": git("hash-object", path),
                   "sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest()} for path in ALLOWED_PATHS],
        "finished_unix": time.time(), "remote_ref_updated": False,
    }
    # This receipt is deliberately last. A failed command never leaves a green receipt.
    (results / "validated.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"GH493 native validation failed: {error}", file=sys.stderr)
        raise SystemExit(1)
