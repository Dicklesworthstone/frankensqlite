#!/usr/bin/env python3
"""Prepare or build the temporary GH#533 instrumented native probe.

Preparation resolves metadata only. Build is an explicit separate action.
Registry packages must remain an exact subset of the engine's Cargo.lock.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tomllib


ROOT = Path(__file__).resolve().parent
WORK = ROOT.parent
CARGO = Path(os.environ.get("CARGO") or shutil.which("cargo") or "cargo")
TOOLCHAIN = "nightly-2026-08-31"
RUSTFLAGS = "-Zthreads=1 -Zunstable-options --jobs-backend=1 -Cembed-bitcode=no"
HOST = "x86_64-unknown-linux-gnu"
ENGINE = {
    "baseline": WORK / "frankensqlite-baseline",
    "candidate": WORK / "frankensqlite",
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git_output(engine: Path, *arguments: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(engine), *arguments], text=True
    ).strip()


def registry_package_key(package: dict) -> tuple:
    return (
        package["name"],
        package["version"],
        package["source"],
        package.get("checksum"),
    )


def prepare(arm: str) -> dict:
    engine = ENGINE[arm]
    destination = ROOT / arm
    manifest = destination / "Cargo.toml"
    engine_lock = engine / "Cargo.lock"
    probe_lock = destination / "Cargo.lock"
    if not probe_lock.exists():
        shutil.copyfile(engine_lock, probe_lock)
    metadata_command = [
        str(CARGO),
        "metadata",
        "--offline",
        "--filter-platform",
        HOST,
        "--format-version",
        "1",
        "--manifest-path",
        str(manifest),
    ]
    environment = dict(os.environ, RUSTUP_TOOLCHAIN=TOOLCHAIN)
    # This can update the scratch lock to add its root package and prune
    # unrelated workspace packages; it never changes the engine lock.
    metadata_text = subprocess.check_output(
        metadata_command, env=environment, cwd=ROOT, text=True
    )
    (destination / "metadata.json").write_text(metadata_text)
    metadata = json.loads(metadata_text)
    workspace_packages = tomllib.loads(engine_lock.read_text())["package"]
    probe_packages = tomllib.loads(probe_lock.read_text())["package"]
    allowed = {
        registry_package_key(package)
        for package in workspace_packages
        if "source" in package
    }
    actual = {
        registry_package_key(package)
        for package in probe_packages
        if "source" in package
    }
    drift = sorted(actual - allowed)
    if drift:
        raise RuntimeError(f"probe registry dependency drift: {drift!r}")
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    features = {
        package["name"]: nodes[package["id"]]["features"]
        for package in metadata["packages"]
        if package["name"] in {"asupersync", "fsqlite-core", "fsqlite-parser"}
        and package["id"] in nodes
    }
    if "test-internals" in features.get("asupersync", []):
        raise RuntimeError("diagnostic probe unexpectedly enables test-internals")
    if "bench-internals" in features.get("fsqlite-core", []):
        raise RuntimeError("diagnostic probe unexpectedly enables bench-internals")
    status = git_output(engine, "status", "--porcelain=v1", "--untracked-files=all")
    preparation = {
        "arm": arm,
        "engine": str(engine),
        "engine_head": git_output(engine, "rev-parse", "HEAD"),
        "engine_worktree_status": status,
        "engine_tracked_diff_sha256": hashlib.sha256(
            subprocess.check_output(["git", "-C", str(engine), "diff", "HEAD", "--"])
        ).hexdigest(),
        "probe_source_sha256": sha256(ROOT / "stage_probe.rs"),
        "manifest_sha256": sha256(manifest),
        "engine_lock_sha256": sha256(engine_lock),
        "probe_lock_sha256": sha256(probe_lock),
        "registry_packages_audited": len(actual),
        "registry_version_or_checksum_drift": [],
        "resolved_features": features,
        "metadata_command": metadata_command,
        "toolchain": TOOLCHAIN,
        "metadata_filter_platform": HOST,
        "native_compilation_performed_by_prepare": False,
    }
    (destination / "preparation.json").write_text(
        json.dumps(preparation, indent=2) + "\n"
    )
    print(json.dumps(preparation, indent=2), flush=True)
    return preparation


def build(arm: str, low_memory: bool) -> int:
    preparation = prepare(arm)
    destination = ROOT / arm
    manifest = destination / "Cargo.toml"
    revision = preparation["engine_head"]
    if preparation["engine_worktree_status"]:
        revision += "+worktree"
    environment_overrides = {
        "RUSTUP_TOOLCHAIN": TOOLCHAIN,
        "CARGO_TARGET_DIR": str(WORK / "native-target"),
        "CARGO_BUILD_JOBS": "1",
        "CARGO_INCREMENTAL": "0",
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_DEV_CODEGEN_UNITS": "256",
        "RUSTFLAGS": RUSTFLAGS,
        "ISSUE533_SOURCE_REVISION": revision,
    }
    command = [str(CARGO)]
    if low_memory:
        command += [
            "-Zcodegen-backend",
            "-Zprofile-hint-mostly-unused",
            "--config",
            "profile.dev.package.asupersync.hint-mostly-unused=true",
            "--config",
            'profile.dev.package.fsqlite-core.codegen-backend="cranelift"',
        ]
    command += ["build", "--locked", "--offline", "--manifest-path", str(manifest), "-j1"]
    receipt = {
        "preparation": preparation,
        "environment_overrides": environment_overrides,
        "build_command": command,
        "low_memory_opt_in": low_memory,
        "release_performance_build": False,
        "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    receipt_path = destination / "build-receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    print(shlex.join(command), flush=True)
    completed = subprocess.run(
        command, cwd=ROOT, env=dict(os.environ, **environment_overrides), check=False
    )
    receipt["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    receipt["exit_code"] = completed.returncode
    binary = WORK / "native-target" / "debug" / f"issue533-stage-probe-{arm}"
    if completed.returncode == 0 and binary.exists():
        receipt["binary"] = str(binary)
        receipt["binary_sha256"] = sha256(binary)
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    return completed.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "build"))
    parser.add_argument("arm", choices=tuple(ENGINE))
    parser.add_argument(
        "--low-memory",
        action="store_true",
        help="opt into asupersync hint-mostly-unused and core Cranelift (build only)",
    )
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(args.arm)
        return 0
    return build(args.arm, args.low_memory)


if __name__ == "__main__":
    sys.exit(main())
