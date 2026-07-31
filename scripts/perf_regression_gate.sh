#!/usr/bin/env bash
set -euo pipefail

# bd-zywqc.2: provisional concurrent-write performance regression guard.
#
# This guard compares absolute FrankenSQLite throughput emitted by
# mt-mvcc-bench: arithmetic mean at one writer (5% maximum drop) and median at
# eight writers (zero tolerated drop), matching the tracked contract. Paired
# FrankenSQLite/C-SQLite ratios remain diagnostics, not the gated metric. The
# guard is intentionally fail-closed: benchmark failures, missing baselines,
# malformed evidence, incompatible provenance, anything other than the exact
# configured 21 samples,
# regressions, and statistically inconclusive comparisons all return non-zero.
# The tracked zero-drop 8-writer threshold is therefore an unresolved release
# policy blocker for ordinary noisy measurements: a non-degenerate two-sided
# confidence interval can overlap zero even when baseline and candidate are
# unchanged. Keep the gate diagnostic and fail-closed until the acceptance
# owner specifies an equivalence margin or other statistically decidable rule;
# do not silently weaken the threshold here.
#
# The underlying v6 benchmark report is explicitly non-citable. A green result
# here is useful as a same-environment development guard, but it is not by
# itself sufficient release evidence.
#
# Usage:
#   ./scripts/perf_regression_gate.sh [--capture-baseline] [--target-dir DIR]
#                                     [--baseline-dir DIR] [--rows N]
#                                     [--analyze-only CURRENT.json]
#
# Modes:
#   Default:             run the benchmark and compare with an existing baseline
#   --capture-baseline:  validate and create a new baseline; never overwrite
#   --analyze-only:      skip Cargo and analyze an existing report (test/debug)
#
# Artifacts:
#   $TARGET_DIR/regression_gate_runs/$RUN_ID/current.json  raw v6 report
#   $TARGET_DIR/regression_gate_runs/$RUN_ID/result.json   guard verdict
#   $BASELINE_DIR/latest.json                              baseline envelope

readonly BEAD_ID="bd-zywqc.2"
readonly GATE_SCHEMA="fsqlite.perf_regression_gate.result.v2"
readonly BASELINE_SCHEMA="fsqlite.perf_regression_gate.baseline.v3"
readonly REPORT_SCHEMA="fsqlite-e2e.mt_mvcc_bench_report.v6"
readonly ITERATIONS=21
readonly MAX_FSQLITE_WPS_DROP_1T=0.05
readonly MAX_FSQLITE_WPS_DROP_8T=0.0

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/fsqlite-reggate-target}"
BASELINE_DIR="$REPO_ROOT/tests/perf/baselines"
CAPTURE_BASELINE=false
ROWS_PER_THREAD=500
ANALYZE_ONLY=""
ANALYZE_ONLY_REQUESTED=false

usage() {
    printf '%s\n' \
        "Usage: $0 [--capture-baseline] [--target-dir DIR]" \
        "          [--baseline-dir DIR] [--rows N]" \
        "          [--analyze-only CURRENT.json]" >&2
}

require_option_value() {
    local option="$1"
    local remaining="$2"
    if [[ "$remaining" -lt 2 ]]; then
        echo "[$BEAD_ID] FATAL: $option requires a value" >&2
        usage
        exit 2
    fi
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --capture-baseline)
            CAPTURE_BASELINE=true
            shift
            ;;
        --target-dir)
            require_option_value "$1" "$#"
            TARGET_DIR="$2"
            shift 2
            ;;
        --baseline-dir)
            require_option_value "$1" "$#"
            BASELINE_DIR="$2"
            shift 2
            ;;
        --rows)
            require_option_value "$1" "$#"
            ROWS_PER_THREAD="$2"
            shift 2
            ;;
        --analyze-only)
            require_option_value "$1" "$#"
            ANALYZE_ONLY="$2"
            ANALYZE_ONLY_REQUESTED=true
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "[$BEAD_ID] FATAL: unknown option: $1" >&2
            usage
            exit 2
            ;;
    esac
done

if [[ -z "$TARGET_DIR" || -z "$BASELINE_DIR" ]]; then
    echo "[$BEAD_ID] FATAL: target and baseline directories must be non-empty" >&2
    exit 2
fi
if [[ "$ANALYZE_ONLY_REQUESTED" = true && -z "$ANALYZE_ONLY" ]]; then
    echo "[$BEAD_ID] FATAL: --analyze-only requires a non-empty path" >&2
    exit 2
fi

# Normalize every user-controlled path before passing it to external commands.
# Absolute operands cannot be mistaken for options when a relative path starts
# with a dash, and the run/baseline receipts then bind stable paths.
if [[ "$TARGET_DIR" != /* ]]; then
    TARGET_DIR="$PWD/$TARGET_DIR"
fi
if [[ "$BASELINE_DIR" != /* ]]; then
    BASELINE_DIR="$PWD/$BASELINE_DIR"
fi
if [[ "$ANALYZE_ONLY_REQUESTED" = true && "$ANALYZE_ONLY" != /* ]]; then
    ANALYZE_ONLY="$PWD/$ANALYZE_ONLY"
fi

if [[ ! "$ROWS_PER_THREAD" =~ ^[1-9][0-9]*$ ]]; then
    echo "[$BEAD_ID] FATAL: --rows must be a positive integer" >&2
    exit 2
fi

if ! mkdir -p "$TARGET_DIR"; then
    echo "[$BEAD_ID] FATAL: cannot create target directory: $TARGET_DIR" >&2
    exit 2
fi

RUN_ID="${FSQLITE_REGGATE_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ 2>/dev/null || echo unknown)-$$}"
if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
    echo "[$BEAD_ID] FATAL: FSQLITE_REGGATE_RUN_ID contains unsupported characters" >&2
    exit 2
fi
RUNS_DIR="$TARGET_DIR/regression_gate_runs"
RUN_DIR="$RUNS_DIR/$RUN_ID"
if ! mkdir -p "$RUNS_DIR" || ! mkdir "$RUN_DIR"; then
    echo "[$BEAD_ID] FATAL: cannot exclusively create run directory: $RUN_DIR" >&2
    exit 2
fi

CURRENT_JSON="$RUN_DIR/current.json"
RESULT_JSON="$RUN_DIR/result.json"
BASELINE_JSON="$BASELINE_DIR/latest.json"
BENCH_LOG="$RUN_DIR/bench.log"
COMMIT_HASH="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
MEASUREMENT_MODE="measured"
EXPECTED_HISTORY_JSON=""

echo "[$BEAD_ID] Provisional performance regression guard"
echo "[$BEAD_ID] Commit: $COMMIT_HASH"
echo "[$BEAD_ID] Rows/thread: $ROWS_PER_THREAD"
echo "[$BEAD_ID] Iterations: $ITERATIONS"
echo "[$BEAD_ID] Target: $TARGET_DIR"
echo "[$BEAD_ID] Run directory: $RUN_DIR"

if [[ "$CAPTURE_BASELINE" = false && ! -f "$BASELINE_JSON" ]]; then
    echo "[$BEAD_ID] FATAL: no baseline at $BASELINE_JSON" >&2
    echo "[$BEAD_ID] Capture requires an explicit --capture-baseline run." >&2
    exit 2
fi
if [[ "$CAPTURE_BASELINE" = true && ( -e "$BASELINE_JSON" || -L "$BASELINE_JSON" ) ]]; then
    echo "[$BEAD_ID] FATAL: refusing to overwrite existing baseline: $BASELINE_JSON" >&2
    echo "[$BEAD_ID] Select a new --baseline-dir or obtain explicit cleanup authorization." >&2
    exit 2
fi

if [[ "$ANALYZE_ONLY_REQUESTED" = true ]]; then
    if [[ ! -f "$ANALYZE_ONLY" ]]; then
        echo "[$BEAD_ID] FATAL: --analyze-only report does not exist: $ANALYZE_ONLY" >&2
        exit 2
    fi
    if ! cp "$ANALYZE_ONLY" "$CURRENT_JSON"; then
        echo "[$BEAD_ID] FATAL: cannot snapshot --analyze-only report into $CURRENT_JSON" >&2
        exit 2
    fi
    MEASUREMENT_MODE="analyze_only"
    echo "[$BEAD_ID] Analyze-only source: $ANALYZE_ONLY"
    echo "[$BEAD_ID] Analyze-only snapshot: $CURRENT_JSON"
else
    # mt-mvcc-bench updates its history file for every structurally valid run,
    # including regressions. Give it a disposable, per-invocation path so the
    # immutable guard baseline can only change through --capture-baseline.
    HISTORY_JSON="$RUN_DIR/disposable_history.json"
    EXPECTED_HISTORY_JSON="$HISTORY_JSON"
    echo "[$BEAD_ID] Running paired mt-mvcc-bench (threads 1,8) ..."
    set +e
    env CARGO_TARGET_DIR="$TARGET_DIR" cargo run -p fsqlite-e2e \
        --bin mt-mvcc-bench --profile release-perf -- \
        --threads=1,8 \
        --rows-per-thread="$ROWS_PER_THREAD" \
        --iters="$ITERATIONS" \
        --history-json="$HISTORY_JSON" \
        --json-output="$CURRENT_JSON" 2>&1 | tee "$BENCH_LOG"
    pipeline_status=("${PIPESTATUS[@]}")
    set -e
    benchmark_status="${pipeline_status[0]}"
    tee_status="${pipeline_status[1]}"
    if [[ "$benchmark_status" -ne 0 || "$tee_status" -ne 0 ]]; then
        echo "[$BEAD_ID] FATAL: benchmark pipeline failed (benchmark=$benchmark_status, tee=$tee_status)" >&2
        exit 2
    fi
fi

if [[ ! -s "$CURRENT_JSON" ]]; then
    echo "[$BEAD_ID] FATAL: no non-empty JSON report at $CURRENT_JSON" >&2
    exit 2
fi

python3 - \
    "$CURRENT_JSON" \
    "$BASELINE_JSON" \
    "$RESULT_JSON" \
    "$COMMIT_HASH" \
    "$ROWS_PER_THREAD" \
    "$CAPTURE_BASELINE" \
    "$BASELINE_SCHEMA" \
    "$GATE_SCHEMA" \
    "$REPORT_SCHEMA" \
    "$ITERATIONS" \
    "$MAX_FSQLITE_WPS_DROP_1T" \
    "$MAX_FSQLITE_WPS_DROP_8T" \
    "$MEASUREMENT_MODE" \
    "$RUN_ID" \
    "$EXPECTED_HISTORY_JSON" <<'PYEOF'
import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
import sys
from pathlib import Path

(
    current_path,
    baseline_path,
    result_path,
    commit,
    expected_rows_raw,
    capture_raw,
    baseline_schema,
    gate_schema,
    report_schema,
    expected_iterations_raw,
    max_fsqlite_wps_drop_1t_raw,
    max_fsqlite_wps_drop_8t_raw,
    measurement_mode,
    run_id,
    expected_history_path,
) = sys.argv[1:]

expected_rows = int(expected_rows_raw)
capture = capture_raw == "true"
expected_iterations = int(expected_iterations_raw)
required_threads = (1, 8)
max_fsqlite_wps_drops = {
    1: float(max_fsqlite_wps_drop_1t_raw),
    8: float(max_fsqlite_wps_drop_8t_raw),
}
bootstrap_repetitions = 20_000
expected_settings_interpretation = (
    "Both engines proved the listed effective PRAGMA values; equal names and readbacks "
    "do not establish cross-engine semantic equivalence."
)
expected_accounting_interpretation = (
    "offered and committed writes share one row unit; attempted_writes counts physical "
    "INSERT calls; retried_operations records the existing engine-specific retry unit and "
    "is provenance only, not a cross-engine comparison metric."
)
expected_timing_interpretation = (
    "workload_elapsed_ns begins only after every worker has opened and proved its effective "
    "settings, and ends at the last worker's transaction terminal point before connection "
    "teardown; worker_startup_elapsed_ns is reported separately."
)
expected_non_citable_reason = (
    "v6 adds fail-closed settings, committed-work, integrity, timing, retry-policy, and "
    "configuration receipts, but bd-uh1fv still requires external watchdog, sanitized "
    "environment, matched retry/deadline semantics, complete build/toolchain provenance, "
    "counterbalanced topology receipts, immutable manifest, and independent verification."
)


class EvidenceError(Exception):
    pass


def fail(message):
    raise EvidenceError(message)


def read_json(path, label):
    try:
        with open(path, encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read {label} JSON {path}: {error}")
    if not isinstance(value, dict):
        fail(f"{label} JSON must be an object")
    return value


def command_output(*argv):
    try:
        return subprocess.run(
            argv,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        fail(f"cannot fingerprint command {' '.join(argv)}: {error}")


def first_cpu_model():
    cpuinfo = Path("/proc/cpuinfo")
    if not cpuinfo.is_file():
        return platform.processor() or "unknown"
    try:
        for line in cpuinfo.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.lower().startswith(("model name", "hardware")) and ":" in line:
                return line.split(":", 1)[1].strip()
    except OSError as error:
        fail(f"cannot read CPU fingerprint: {error}")
    return platform.processor() or "unknown"


def read_optional_text(path):
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except OSError:
        return "unavailable"


def affinity_cpu_count():
    getter = getattr(os, "sched_getaffinity", None)
    if getter is None:
        return "unavailable"
    try:
        return len(getter(0))
    except OSError as error:
        fail(f"cannot read CPU-affinity fingerprint: {error}")


def provenance_fingerprint():
    return {
        "schema_version": "fsqlite.perf_regression_gate.provenance.v1",
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_model": first_cpu_model(),
        "logical_cpu_count": os.cpu_count(),
        "affinity_cpu_count": affinity_cpu_count(),
        "cgroup_cpu_max": read_optional_text("/sys/fs/cgroup/cpu.max"),
        "cgroup_cpuset_effective": read_optional_text(
            "/sys/fs/cgroup/cpuset.cpus.effective"
        ),
        "cpu_governor": read_optional_text(
            "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
        ),
        "rustc_verbose": command_output("rustc", "-Vv"),
        "cargo_version": command_output("cargo", "-V"),
        "rustflags": os.environ.get("RUSTFLAGS", ""),
        "cargo_encoded_rustflags": os.environ.get("CARGO_ENCODED_RUSTFLAGS", ""),
        "rch_worker": os.environ.get("RCH_WORKER", ""),
        "profile": "release-perf",
        "benchmark": "mt-mvcc-bench",
        "threads": list(required_threads),
        "rows_per_thread": expected_rows,
        "measurement_mode": measurement_mode,
    }


def require_int(value, label, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, int):
        fail(f"{label} must be an integer")
    if positive and value <= 0:
        fail(f"{label} must be positive")
    return value


def require_number(value, label, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        fail(f"{label} must be numeric")
    value = float(value)
    if not math.isfinite(value):
        fail(f"{label} must be finite")
    if positive and value <= 0.0:
        fail(f"{label} must be positive")
    return value


def require_nonempty_string(value, label):
    if not isinstance(value, str) or not value:
        fail(f"{label} must be a non-empty string")
    return value


def arithmetic_mean(values):
    total = 0.0
    for value in values:
        total += value
    return total / len(values)


def duration_seconds(elapsed_ns):
    seconds, nanoseconds = divmod(elapsed_ns, 1_000_000_000)
    return float(seconds) + float(nanoseconds) / 1_000_000_000.0


def expected_effective_settings(concurrent_mode):
    return {
        "page_size_bytes": 4_096,
        "journal_mode": "wal",
        "synchronous": "normal",
        "cache_size": -64_000,
        "busy_timeout_ms": 5_000,
        "wal_autocheckpoint_pages": 1_000,
        "concurrent_mode": concurrent_mode,
    }


def expected_retry_policy(writers):
    timeout_seconds = 5 + writers * expected_rows // 5_000
    return {
        "csqlite_busy_timeout_ms": 5_000,
        "csqlite_max_operation_retries": 512,
        "csqlite_retry_sleep_ms": 1,
        "csqlite_retry_unit": "individual INSERT or COMMIT operation",
        "csqlite_retry_algorithm": (
            "csqlite.per-operation.fixed-1ms.busy-or-locked.max-512.v1"
        ),
        "fsqlite_transaction_timeout_ms": timeout_seconds * 1_000,
        "fsqlite_max_transaction_retries": 512,
        "fsqlite_retry_sleep_base_ms": 1,
        "fsqlite_retry_sleep_cap_ms": 29,
        "fsqlite_retry_unit": "whole BEGIN CONCURRENT transaction attempt",
        "fsqlite_retry_backoff_algorithm": (
            "fsqlite.whole-transaction.step-exp-every-8-cap-25ms-plus-thread-"
            "attempt-jitter-0-to-4ms.max-512-or-timeout.v1"
        ),
        "fsqlite_retryable_errors": (
            "Busy|BusyRecovery|BusySnapshot|DatabaseLocked|WriteConflict|"
            "SerializationFailure|PageBufferCapacityExhausted"
        ),
        "fsqlite_timeout_overridden": False,
    }


def validate_sample(sample, offered_writes, label):
    if not isinstance(sample, dict):
        fail(f"{label} must be an object")
    require_int(sample.get("worker_startup_elapsed_ns"), f"{label}.worker_startup_elapsed_ns", positive=True)
    elapsed_ns = require_int(sample.get("workload_elapsed_ns"), f"{label}.workload_elapsed_ns", positive=True)
    settings = sample.get("settings")
    if not isinstance(settings, dict) or not settings:
        fail(f"{label}.settings must be a non-empty object")
    accounting = sample.get("accounting")
    if not isinstance(accounting, dict):
        fail(f"{label}.accounting must be an object")
    if require_int(accounting.get("offered_writes"), f"{label}.accounting.offered_writes", positive=True) != offered_writes:
        fail(f"{label}.accounting.offered_writes does not match the configuration")
    attempted = require_int(accounting.get("attempted_writes"), f"{label}.accounting.attempted_writes", positive=True)
    succeeded = require_int(accounting.get("succeeded_writes"), f"{label}.accounting.succeeded_writes", positive=True)
    retried = require_int(accounting.get("retried_operations"), f"{label}.accounting.retried_operations")
    if retried < 0:
        fail(f"{label}.accounting.retried_operations must be non-negative")
    if attempted < succeeded or succeeded != offered_writes:
        fail(f"{label}.accounting does not prove all offered writes committed")
    failed_writes = require_int(
        accounting.get("failed_writes"), f"{label}.accounting.failed_writes"
    )
    worker_failed_writes = require_int(
        accounting.get("worker_reported_failed_writes"),
        f"{label}.accounting.worker_reported_failed_writes",
    )
    if failed_writes != 0 or worker_failed_writes != 0:
        fail(f"{label}.accounting reports failed writes")
    if accounting.get("exact") is not True or accounting.get("diagnostics") != []:
        fail(f"{label}.accounting is not exact and diagnostic-free")
    committed = sample.get("committed_state")
    if not isinstance(committed, dict):
        fail(f"{label}.committed_state must be an object")
    if committed.get("valid") is not True or committed.get("diagnostics") != []:
        fail(f"{label}.committed_state is not valid and diagnostic-free")
    expected_rows = require_int(
        committed.get("expected_rows"), f"{label}.committed_state.expected_rows"
    )
    observed_rows = require_int(
        committed.get("observed_rows"), f"{label}.committed_state.observed_rows"
    )
    if expected_rows != succeeded or observed_rows != succeeded:
        fail(f"{label}.committed_state row oracle does not match committed work")
    expected_id_sum = require_int(
        committed.get("expected_id_sum"), f"{label}.committed_state.expected_id_sum"
    )
    observed_id_sum = require_int(
        committed.get("observed_id_sum"), f"{label}.committed_state.observed_id_sum"
    )
    if expected_id_sum != observed_id_sum:
        fail(f"{label}.committed_state id-sum oracle disagrees")
    expected_payload_sha256 = committed.get("expected_payload_sha256")
    if expected_payload_sha256 != committed.get("observed_payload_sha256"):
        fail(f"{label}.committed_state payload oracle disagrees")
    if (
        not isinstance(expected_payload_sha256, str)
        or len(expected_payload_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_payload_sha256)
    ):
        fail(f"{label}.committed_state payload SHA-256 is not lowercase hexadecimal")
    if committed.get("integrity_check") != ["ok"]:
        fail(f"{label}.committed_state integrity_check is not exactly ['ok']")
    elapsed_seconds = duration_seconds(elapsed_ns)
    return {
        "settings": settings,
        "wps": succeeded / elapsed_seconds,
        "elapsed_ms": elapsed_seconds * 1_000.0,
    }


def percentile(values, quantile):
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = quantile * (len(ordered) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def close_enough(observed, expected):
    return math.isclose(observed, expected, rel_tol=1e-9, abs_tol=1e-9)


def bootstrap_median_ci95(ratios):
    state = 0x7A25_2026_C011_CAFE
    medians = []
    sample_count = len(ratios)
    for _ in range(10_000):
        resample = []
        for _ in range(sample_count):
            state = lcg_next(state)
            resample.append(ratios[state % sample_count])
        medians.append(statistics.median(resample))
    medians.sort()
    return medians[250], medians[9_750]


def ratio_stats(ratios):
    ratio_median = statistics.median(ratios)
    mean = arithmetic_mean(ratios)
    squared_deviation_total = 0.0
    for ratio in ratios:
        deviation = ratio - mean
        squared_deviation_total += deviation * deviation
    variance = squared_deviation_total / (len(ratios) - 1) if len(ratios) > 1 else 0.0
    cv_pct = 0.0 if mean == 0.0 else math.sqrt(variance) / abs(mean) * 100.0
    mad = statistics.median([abs(ratio - ratio_median) for ratio in ratios])
    return {
        "median": ratio_median,
        "ci95": bootstrap_median_ci95(ratios),
        "cv_pct": cv_pct,
        "mad": mad,
    }


def expected_median_ci_contract(null_ratios, claim_ratios):
    null = ratio_stats(null_ratios)
    claim = ratio_stats(claim_ratios)
    null_radius = max(abs(null["ci95"][0] - 1.0), abs(null["ci95"][1] - 1.0))
    decisive_effect = max(2.0 * null_radius, 0.01)
    min_decidable_gain = 1.0 + decisive_effect
    max_decidable_regression = 1.0 - decisive_effect
    claim_effect = abs(claim["median"] - 1.0)
    claim_margin = None if null_radius == 0.0 else claim_effect / null_radius
    if claim["ci95"][0] > min_decidable_gain:
        verdict = "FSQLITE_FASTER"
    elif claim["ci95"][1] < max_decidable_regression:
        verdict = "FSQLITE_SLOWER"
    else:
        verdict = "INCONCLUSIVE"
    return {
        "null_ratio_median": null["median"],
        "null_ratio_ci95_low": null["ci95"][0],
        "null_ratio_ci95_high": null["ci95"][1],
        "null_ratio_cv_pct": null["cv_pct"],
        "null_ratio_mad": null["mad"],
        "claim_ratio_median": claim["median"],
        "claim_ratio_ci95_low": claim["ci95"][0],
        "claim_ratio_ci95_high": claim["ci95"][1],
        "claim_ratio_cv_pct": claim["cv_pct"],
        "claim_ratio_mad": claim["mad"],
        "null_radius": null_radius,
        "min_decidable_gain": min_decidable_gain,
        "max_decidable_regression": max_decidable_regression,
        "claim_margin": claim_margin,
        "cv_gate": "never",
        "verdict": verdict,
    }


def validate_median_ci_contract(observed, expected, label):
    if set(observed) != set(expected):
        fail(f"{label} fields do not match the v6 median-CI contract")
    for field, expected_value in expected.items():
        observed_value = observed[field]
        if isinstance(expected_value, float):
            observed_number = require_number(observed_value, f"{label}.{field}")
            if not close_enough(observed_number, expected_value):
                fail(f"{label}.{field} disagrees with raw paired samples")
        elif observed_value != expected_value:
            fail(f"{label}.{field} disagrees with the v6 median-CI contract")


def validate_report(report, label, required_history_path=None):
    if report.get("schema_version") != report_schema:
        fail(f"{label}.schema_version must be {report_schema}")
    if report.get("citable") is not False:
        fail(f"{label}.citable must be false for the diagnostic-only v6 schema")
    if report.get("non_citable_reason") != expected_non_citable_reason:
        fail(f"{label}.non_citable_reason does not match the v6 contract")
    if report.get("workload_shape") != "shared_table":
        fail(f"{label}.workload_shape must be shared_table")
    if require_int(report.get("rows_per_thread"), f"{label}.rows_per_thread", positive=True) != expected_rows:
        fail(f"{label}.rows_per_thread does not match --rows={expected_rows}")
    iterations = require_int(report.get("iterations"), f"{label}.iterations", positive=True)
    if iterations != expected_iterations:
        fail(
            f"{label}.iterations={iterations} does not match the exact "
            f"configured count {expected_iterations}"
        )
    expected_interpretations = {
        "settings_interpretation": expected_settings_interpretation,
        "accounting_interpretation": expected_accounting_interpretation,
        "timing_interpretation": expected_timing_interpretation,
    }
    for field, expected_interpretation in expected_interpretations.items():
        if report.get(field) != expected_interpretation:
            fail(f"{label}.{field} does not match the v6 contract")

    pass_over_pass = report.get("pass_over_pass_gate")
    if not isinstance(pass_over_pass, dict):
        fail(f"{label}.pass_over_pass_gate must be an object")
    pass_over_pass_threshold = require_number(
        pass_over_pass.get("threshold_ratio_drop_pct"),
        f"{label}.pass_over_pass_gate.threshold_ratio_drop_pct",
    )
    comparable_pair_count = require_int(
        pass_over_pass.get("comparable_pair_count"),
        f"{label}.pass_over_pass_gate.comparable_pair_count",
    )
    if (
        pass_over_pass.get("schema_version")
        != "fsqlite-e2e.mt_mvcc_bench.pass_over_pass.v1"
        or not close_enough(pass_over_pass_threshold, 5.0)
        or pass_over_pass.get("status") != "no_prior_report"
        or pass_over_pass.get("previous_report_found") is not False
        or comparable_pair_count != 0
        or pass_over_pass.get("regressions") != []
    ):
        fail(f"{label}.pass_over_pass_gate is not the disposable-history v6 receipt")
    history_path = require_nonempty_string(
        pass_over_pass.get("history_json_path"),
        f"{label}.pass_over_pass_gate.history_json_path",
    )
    if required_history_path is not None and history_path != required_history_path:
        fail(f"{label}.pass_over_pass_gate does not match its bound history path")

    receipts = report.get("configuration_receipts")
    rows = report.get("thread_results")
    if not isinstance(receipts, list) or not isinstance(rows, list):
        fail(f"{label} must contain configuration_receipts and thread_results arrays")
    if len(receipts) != len(required_threads) or len(rows) != len(required_threads):
        fail(f"{label} must contain exactly the 1-thread and 8-thread configurations")

    receipts_by_thread = {}
    available_parallelism_values = set()
    for index, receipt in enumerate(receipts):
        receipt_label = f"{label}.configuration_receipts[{index}]"
        if not isinstance(receipt, dict):
            fail(f"{receipt_label} must be an object")
        writers = require_int(receipt.get("writers"), f"{receipt_label}.writers", positive=True)
        if writers in receipts_by_thread:
            fail(f"{label} contains duplicate {writers}-thread configuration receipts")
        if receipt.get("status") != "supported" or receipt.get("comparison_eligible") is not True or receipt.get("measured") is not True:
            fail(f"{receipt_label} is not a supported, measured, comparison-eligible configuration")
        available_parallelism = require_int(
            receipt.get("available_parallelism"),
            f"{receipt_label}.available_parallelism",
            positive=True,
        )
        max_supported_writers = require_int(
            receipt.get("max_supported_writers"),
            f"{receipt_label}.max_supported_writers",
            positive=True,
        )
        if max_supported_writers != 128:
            fail(f"{receipt_label}.max_supported_writers does not match the v6 contract")
        if available_parallelism < writers:
            fail(f"{receipt_label} claims support beyond the measured machine capacity")
        available_parallelism_values.add(available_parallelism)
        offered_writes_per_sample = require_int(
            receipt.get("offered_writes_per_sample"),
            f"{receipt_label}.offered_writes_per_sample",
            positive=True,
        )
        if offered_writes_per_sample != writers * expected_rows:
            fail(f"{receipt_label}.offered_writes_per_sample is inconsistent")
        if receipt.get("wal_autocheckpoint_overridden") is not False:
            fail(f"{receipt_label} uses a diagnostic-only checkpoint override")
        if require_int(
            receipt.get("wal_autocheckpoint_pages"),
            f"{receipt_label}.wal_autocheckpoint_pages",
        ) != 1_000:
            fail(f"{receipt_label} does not use the v6 default checkpoint cadence")
        retry_policy = receipt.get("retry_policy")
        if retry_policy != expected_retry_policy(writers):
            fail(f"{receipt_label} does not use the exact v6 retry-policy contract")
        require_nonempty_string(receipt.get("reason"), f"{receipt_label}.reason")
        receipts_by_thread[writers] = receipt
    if tuple(sorted(receipts_by_thread)) != required_threads:
        fail(f"{label} configurations must be exactly {required_threads}")
    if len(available_parallelism_values) != 1:
        fail(f"{label} configuration receipts disagree on available parallelism")

    rows_by_thread = {}
    for index, row in enumerate(rows):
        row_label = f"{label}.thread_results[{index}]"
        if not isinstance(row, dict):
            fail(f"{row_label} must be an object")
        threads = require_int(row.get("threads"), f"{row_label}.threads", positive=True)
        if threads in rows_by_thread:
            fail(f"{label} contains duplicate {threads}-thread results")
        if threads not in receipts_by_thread:
            fail(f"{row_label} has no matching configuration receipt")
        for metric in (
            "fsqlite_wps_p50", "fsqlite_wps_p95", "fsqlite_wps_p99",
            "sqlite_wps_p50", "sqlite_wps_p95", "sqlite_wps_p99",
            "throughput_ratio", "fsqlite_ms_p50", "fsqlite_ms_p95",
            "fsqlite_ms_p99", "sqlite_ms_p50", "sqlite_ms_p95",
            "sqlite_ms_p99", "time_ratio",
        ):
            require_number(row.get(metric), f"{row_label}.{metric}", positive=True)
        fsqlite_failed_rows = require_int(
            row.get("fsqlite_failed_rows"), f"{row_label}.fsqlite_failed_rows"
        )
        sqlite_failed_rows = require_int(
            row.get("sqlite_failed_rows"), f"{row_label}.sqlite_failed_rows"
        )
        if fsqlite_failed_rows != 0 or sqlite_failed_rows != 0:
            fail(f"{row_label} reports failed rows")
        contract = row.get("median_ci_contract")
        if not isinstance(contract, dict):
            fail(f"{row_label}.median_ci_contract must be present")
        truth = row.get("truth")
        if not isinstance(truth, dict) or truth.get("configuration") != receipts_by_thread[threads]:
            fail(f"{row_label}.truth configuration does not match its receipt")

        arms = {}
        offered_writes = threads * expected_rows
        for arm_name in ("null_c_a_samples", "null_c_b_samples", "sqlite_samples", "fsqlite_samples"):
            samples = truth.get(arm_name)
            if not isinstance(samples, list) or len(samples) != iterations:
                fail(f"{row_label}.truth.{arm_name} must contain exactly {iterations} samples")
            arms[arm_name] = [
                validate_sample(sample, offered_writes, f"{row_label}.truth.{arm_name}[{sample_index}]")
                for sample_index, sample in enumerate(samples)
            ]
            first_settings = arms[arm_name][0]["settings"]
            if any(sample["settings"] != first_settings for sample in arms[arm_name]):
                fail(f"{row_label}.truth.{arm_name} settings are not uniform")

        sqlite_settings = expected_effective_settings("sqlite_wal_single_writer")
        fsqlite_settings = expected_effective_settings("fsqlite_mvcc_on")
        for arm_name in ("null_c_a_samples", "null_c_b_samples", "sqlite_samples"):
            if arms[arm_name][0]["settings"] != sqlite_settings:
                fail(f"{row_label}.truth.{arm_name} does not prove the exact C-SQLite settings")
        if arms["fsqlite_samples"][0]["settings"] != fsqlite_settings:
            fail(f"{row_label}.truth.fsqlite_samples does not prove concurrent MVCC mode")
        null_ratios = [
            null_b["wps"] / null_a["wps"]
            for null_a, null_b in zip(
                arms["null_c_a_samples"], arms["null_c_b_samples"]
            )
        ]
        claim_ratios = [
            fsqlite["wps"] / sqlite["wps"]
            for sqlite, fsqlite in zip(arms["sqlite_samples"], arms["fsqlite_samples"])
        ]
        ratio_median = statistics.median(claim_ratios)
        if not close_enough(require_number(row["throughput_ratio"], f"{row_label}.throughput_ratio"), ratio_median):
            fail(f"{row_label}.throughput_ratio disagrees with raw paired samples")
        validate_median_ci_contract(
            contract,
            expected_median_ci_contract(null_ratios, claim_ratios),
            f"{row_label}.median_ci_contract",
        )
        for prefix, samples in (("sqlite", arms["sqlite_samples"]), ("fsqlite", arms["fsqlite_samples"])):
            wps = [sample["wps"] for sample in samples]
            elapsed_ms = [sample["elapsed_ms"] for sample in samples]
            for suffix, quantile in (("p50", 0.50), ("p95", 0.95), ("p99", 0.99)):
                for metric_name, values in (("wps", wps), ("ms", elapsed_ms)):
                    field = f"{prefix}_{metric_name}_{suffix}"
                    observed = require_number(row[field], f"{row_label}.{field}", positive=True)
                    if not close_enough(observed, percentile(values, quantile)):
                        fail(f"{row_label}.{field} disagrees with raw samples")
        expected_time_ratio = (
            percentile([sample["elapsed_ms"] for sample in arms["fsqlite_samples"]], 0.50)
            / percentile([sample["elapsed_ms"] for sample in arms["sqlite_samples"]], 0.50)
        )
        if not close_enough(
            require_number(row["time_ratio"], f"{row_label}.time_ratio", positive=True),
            expected_time_ratio,
        ):
            fail(f"{row_label}.time_ratio disagrees with raw samples")
        rows_by_thread[threads] = {
            "ratios": claim_ratios,
            "ratio_median": ratio_median,
            "fsqlite_wps": [sample["wps"] for sample in arms["fsqlite_samples"]],
        }
    if tuple(sorted(rows_by_thread)) != required_threads:
        fail(f"{label} thread results must be exactly {required_threads}")
    semantic_receipts = {
        threads: {
            "available_parallelism": receipt["available_parallelism"],
            "max_supported_writers": receipt["max_supported_writers"],
            "wal_autocheckpoint_pages": receipt["wal_autocheckpoint_pages"],
            "wal_autocheckpoint_overridden": receipt["wal_autocheckpoint_overridden"],
            "offered_writes_per_sample": receipt["offered_writes_per_sample"],
            "retry_policy": receipt["retry_policy"],
        }
        for threads, receipt in receipts_by_thread.items()
    }
    return {
        "iterations": iterations,
        "history_json_path": history_path,
        "contract": {
            "settings_interpretation": report["settings_interpretation"],
            "accounting_interpretation": report["accounting_interpretation"],
            "timing_interpretation": report["timing_interpretation"],
            "configuration_receipts": semantic_receipts,
        },
        "rows": rows_by_thread,
    }


def lcg_next(state):
    return (state * 6_364_136_223_846_793_005 + 1_442_695_040_888_963_407) & ((1 << 64) - 1)


def bootstrap_relative_statistic_delta(
    baseline_values,
    current_values,
    seed,
    statistic,
):
    state = seed
    deltas = []
    baseline_count = len(baseline_values)
    current_count = len(current_values)
    for _ in range(bootstrap_repetitions):
        baseline_resample = []
        current_resample = []
        for _ in range(baseline_count):
            state = lcg_next(state)
            baseline_resample.append(baseline_values[state % baseline_count])
        for _ in range(current_count):
            state = lcg_next(state)
            current_resample.append(current_values[state % current_count])
        baseline_statistic = statistic(baseline_resample)
        current_statistic = statistic(current_resample)
        if baseline_statistic <= 0.0:
            fail("bootstrap encountered a non-positive baseline throughput")
        deltas.append(current_statistic / baseline_statistic - 1.0)
    deltas.sort()
    low = deltas[bootstrap_repetitions * 25 // 1_000]
    high = deltas[min(bootstrap_repetitions * 975 // 1_000, bootstrap_repetitions - 1)]
    return low, high


def encoded_json(value):
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def fsync_directory(path):
    descriptor = None
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        os.fsync(descriptor)
    except OSError as error:
        fail(f"cannot fsync artifact directory {path}: {error}")
    finally:
        if descriptor is not None:
            os.close(descriptor)


def ensure_directory(path):
    directory = Path(path)
    missing = []
    cursor = directory
    while not cursor.exists():
        missing.append(cursor)
        parent = cursor.parent
        if parent == cursor:
            fail(f"cannot find an existing parent for artifact directory {directory}")
        cursor = parent
    if not cursor.is_dir():
        fail(f"artifact directory ancestor is not a directory: {cursor}")
    for item in reversed(missing):
        try:
            item.mkdir()
        except FileExistsError:
            if not item.is_dir():
                fail(f"artifact directory path is not a directory: {item}")
        except OSError as error:
            fail(f"cannot create artifact directory {item}: {error}")
        fsync_directory(item.parent)


def write_json(path, value, *, exclusive=True):
    destination = Path(path)
    try:
        ensure_directory(destination.parent)
        with destination.open(
            "x" if exclusive else "w",
            encoding="utf-8",
        ) as handle:
            handle.write(encoded_json(value).decode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
        fsync_directory(destination.parent)
    except OSError as error:
        operation = "create" if exclusive else "write"
        fail(f"cannot {operation} JSON artifact {path}: {error}")


def publish_baseline_json(path, value, publication_run_id):
    destination = Path(path)
    payload = encoded_json(value)
    payload_sha256 = hashlib.sha256(payload).hexdigest()
    versions_directory = destination.parent / "versions"
    version_path = versions_directory / f"{payload_sha256}.json"
    candidates_directory = versions_directory / "candidates"
    result_scope = hashlib.sha256(
        str(Path(result_path).resolve()).encode("utf-8")
    ).hexdigest()[:16]
    candidate_directory = candidates_directory / f"{publication_run_id}.{result_scope}"
    candidate_path = candidate_directory / "baseline.json"
    try:
        ensure_directory(versions_directory)
        ensure_directory(candidates_directory)
        candidate_directory.mkdir()
        fsync_directory(candidates_directory)
        with candidate_path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        fsync_directory(candidate_directory)
        if os.path.lexists(version_path):
            if version_path.is_symlink() or not version_path.is_file():
                fail(f"baseline version path is not a regular file: {version_path}")
            if version_path.read_bytes() != payload:
                fail(f"baseline version digest collision or corruption: {version_path}")
        else:
            try:
                os.link(candidate_path, version_path, follow_symlinks=False)
            except FileExistsError:
                if version_path.is_symlink() or not version_path.is_file():
                    fail(f"baseline version path is not a regular file: {version_path}")
                if version_path.read_bytes() != payload:
                    fail(
                        f"baseline version digest collision or corruption: {version_path}"
                    )
        # Always establish our own durability receipt for the content-addressed
        # directory entry. A concurrent identical capture may observe a version
        # another process just linked before that process fsyncs the directory;
        # publishing latest.json must not depend on the other process surviving.
        fsync_directory(versions_directory)
        os.link(version_path, destination, follow_symlinks=False)
        fsync_directory(destination.parent)
    except EvidenceError:
        raise
    except OSError as error:
        fail(f"cannot atomically publish baseline {path}: {error}")
    return str(version_path), str(candidate_path), payload_sha256


try:
    current_report = read_json(current_path, "current report")
    current_report_sha256 = hashlib.sha256(
        json.dumps(current_report, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    current_provenance = provenance_fingerprint()
    required_current_history_path = (
        expected_history_path if measurement_mode == "measured" else None
    )
    current_validated = validate_report(
        current_report,
        "current report",
        required_current_history_path,
    )
    if capture:
        baseline = {
            "schema_version": baseline_schema,
            "bead_id": "bd-zywqc.2",
            "analyzer_commit": commit,
            "capture_run_id": run_id,
            "measurement_mode": measurement_mode,
            "report_provenance_bound": False,
            "provenance": current_provenance,
            "release_evidence": False,
            "release_eligible": False,
            "report_history_json_path": current_validated["history_json_path"],
            "report_sha256": current_report_sha256,
            "report": current_report,
        }
        (
            baseline_version_path,
            baseline_candidate_path,
            baseline_envelope_sha256,
        ) = publish_baseline_json(baseline_path, baseline, run_id)
        result = {
            "schema_version": gate_schema,
            "bead_id": "bd-zywqc.2",
            "mode": "capture_baseline",
            "measurement_mode": measurement_mode,
            "report_provenance_bound": False,
            "analyzer_commit": commit,
            "current_report_sha256": current_report_sha256,
            "baseline_path": baseline_path,
            "baseline_version_path": baseline_version_path,
            "baseline_candidate_path": baseline_candidate_path,
            "baseline_envelope_sha256": baseline_envelope_sha256,
            "baseline_report_history_json_path": current_validated["history_json_path"],
            "iterations": current_validated["iterations"],
            "verdict": "baseline_captured",
            "release_evidence": False,
            "release_eligible": False,
        }
        write_json(result_path, result)
        print(f"[bd-zywqc.2] CAPTURED validated baseline: {baseline_path}")
        sys.exit(0)

    baseline_destination = Path(baseline_path)
    if baseline_destination.is_symlink() or not baseline_destination.is_file():
        fail("baseline latest path must be a regular file, not a symbolic link")
    baseline_envelope = read_json(baseline_path, "baseline envelope")
    if baseline_envelope.get("schema_version") != baseline_schema:
        fail(f"baseline must use {baseline_schema}; recapture explicitly")
    baseline_envelope_payload = encoded_json(baseline_envelope)
    baseline_envelope_sha256 = hashlib.sha256(baseline_envelope_payload).hexdigest()
    baseline_version_path = (
        baseline_destination.parent
        / "versions"
        / f"{baseline_envelope_sha256}.json"
    )
    if baseline_version_path.is_symlink() or not baseline_version_path.is_file():
        fail(
            "baseline envelope has no matching regular content-addressed version: "
            f"{baseline_version_path}"
        )
    try:
        same_baseline_file = os.path.samefile(
            baseline_destination, baseline_version_path
        )
        version_payload = baseline_version_path.read_bytes()
    except OSError as error:
        fail(f"cannot verify content-addressed baseline identity: {error}")
    if not same_baseline_file:
        fail("baseline latest path is not the matching content-addressed version")
    if version_payload != baseline_envelope_payload:
        fail("baseline envelope bytes are not canonical content-addressed JSON")
    if (
        baseline_envelope.get("bead_id") != "bd-zywqc.2"
        or baseline_envelope.get("measurement_mode") != measurement_mode
        or baseline_envelope.get("report_provenance_bound") is not False
        or baseline_envelope.get("release_evidence") is not False
        or baseline_envelope.get("release_eligible") is not False
    ):
        fail("baseline envelope does not identify a non-release diagnostic baseline")
    require_nonempty_string(
        baseline_envelope.get("analyzer_commit"),
        "baseline envelope.analyzer_commit",
    )
    baseline_capture_run_id = require_nonempty_string(
        baseline_envelope.get("capture_run_id"),
        "baseline envelope.capture_run_id",
    )
    if (
        len(baseline_capture_run_id) > 128
        or not baseline_capture_run_id[0].isalnum()
        or any(
            not (character.isalnum() or character in "._-")
            for character in baseline_capture_run_id
        )
    ):
        fail("baseline envelope.capture_run_id is invalid")
    baseline_history_path = require_nonempty_string(
        baseline_envelope.get("report_history_json_path"),
        "baseline envelope.report_history_json_path",
    )
    if measurement_mode == "measured":
        baseline_history = Path(baseline_history_path)
        if (
            baseline_history.name != "disposable_history.json"
            or baseline_history.parent.name != baseline_capture_run_id
            or baseline_history.parent.parent.name != "regression_gate_runs"
        ):
            fail(
                "baseline envelope does not bind a capture-run disposable history path"
            )
    if baseline_envelope.get("provenance") != current_provenance:
        fail("baseline provenance does not exactly match the current benchmark environment")
    baseline_report = baseline_envelope.get("report")
    if not isinstance(baseline_report, dict):
        fail("baseline envelope does not contain a report object")
    expected_digest = hashlib.sha256(
        json.dumps(baseline_report, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if baseline_envelope.get("report_sha256") != expected_digest:
        fail("baseline report SHA-256 does not match its envelope")
    baseline_validated = validate_report(
        baseline_report,
        "baseline report",
        baseline_history_path,
    )
    if baseline_validated["iterations"] != current_validated["iterations"]:
        fail("baseline and current reports use different iteration counts")
    if baseline_validated["contract"] != current_validated["contract"]:
        fail("baseline and current reports use incompatible benchmark configuration contracts")

    comparisons = []
    guard_status = "passed"
    for threads in required_threads:
        baseline_row = baseline_validated["rows"][threads]
        current_row = current_validated["rows"][threads]
        max_allowed_drop = max_fsqlite_wps_drops[threads]
        if threads == 1:
            statistic = arithmetic_mean
            metric = "fsqlite_wps_arithmetic_mean"
        else:
            statistic = statistics.median
            metric = "fsqlite_wps_median"
        baseline_statistic = statistic(baseline_row["fsqlite_wps"])
        current_statistic = statistic(current_row["fsqlite_wps"])
        observed_delta = current_statistic / baseline_statistic - 1.0
        seed = 0xF5_71_17_E0_2026 ^ threads
        ci_low, ci_high = bootstrap_relative_statistic_delta(
            baseline_row["fsqlite_wps"],
            current_row["fsqlite_wps"],
            seed,
            statistic,
        )
        allowed_delta = -max_allowed_drop
        if ci_high < allowed_delta and not close_enough(ci_high, allowed_delta):
            status = "regression"
        elif ci_low > allowed_delta or close_enough(ci_low, allowed_delta):
            status = "passed"
        else:
            status = "inconclusive"
        if status != "passed":
            guard_status = "failed"
        comparisons.append(
            {
                "threads": threads,
                "metric": metric,
                "baseline_fsqlite_wps": baseline_statistic,
                "current_fsqlite_wps": current_statistic,
                "relative_delta_pct": observed_delta * 100.0,
                "bootstrap_ci95_delta_pct": [ci_low * 100.0, ci_high * 100.0],
                "max_allowed_drop_pct": max_allowed_drop * 100.0,
                "baseline_fsqlite_to_csqlite_ratio_median_diagnostic": baseline_row["ratio_median"],
                "current_fsqlite_to_csqlite_ratio_median_diagnostic": current_row["ratio_median"],
                "status": status,
            }
        )

    result = {
        "schema_version": gate_schema,
        "bead_id": "bd-zywqc.2",
        "mode": "regression_guard",
        "measurement_mode": measurement_mode,
        "report_provenance_bound": False,
        "analyzer_commit": commit,
        "baseline_analyzer_commit": baseline_envelope.get("analyzer_commit"),
        "current_report_sha256": current_report_sha256,
        "baseline_report_sha256": expected_digest,
        "baseline_envelope_sha256": baseline_envelope_sha256,
        "baseline_path": baseline_path,
        "baseline_version_path": str(baseline_version_path),
        "iterations": current_validated["iterations"],
        "bootstrap_repetitions": bootstrap_repetitions,
        "guard_status": guard_status,
        "verdict": "diagnostic_only" if guard_status == "passed" else "failed",
        "release_evidence": False,
        "release_eligible": False,
        "comparisons": comparisons,
    }
    write_json(result_path, result)
    for comparison in comparisons:
        print(
            "  [{status}] {threads}t {metric} {baseline:.2f} -> {current:.2f} wps; "
            "delta {delta:+.2f}% (bootstrap 95% CI {low:+.2f}%..{high:+.2f}%)".format(
                status=comparison["status"].upper(),
                threads=comparison["threads"],
                metric=comparison["metric"],
                baseline=comparison["baseline_fsqlite_wps"],
                current=comparison["current_fsqlite_wps"],
                delta=comparison["relative_delta_pct"],
                low=comparison["bootstrap_ci95_delta_pct"][0],
                high=comparison["bootstrap_ci95_delta_pct"][1],
            )
        )
    if guard_status != "passed":
        print("[bd-zywqc.2] FAILED: regression or inconclusive evidence", file=sys.stderr)
        sys.exit(1)
    print("[bd-zywqc.2] PASSED provisional same-environment regression guard")
except EvidenceError as error:
    invalid_result = {
        "schema_version": gate_schema,
        "bead_id": "bd-zywqc.2",
        "mode": "capture_baseline" if capture else "regression_guard",
        "measurement_mode": measurement_mode,
        "report_provenance_bound": False,
        "analyzer_commit": commit,
        "verdict": "invalid_evidence",
        "release_evidence": False,
        "release_eligible": False,
        "error": str(error),
    }
    try:
        write_json(result_path, invalid_result)
    except EvidenceError as result_error:
        print(
            f"[bd-zywqc.2] additionally could not write invalid-evidence result: {result_error}",
            file=sys.stderr,
        )
    print(f"[bd-zywqc.2] INVALID EVIDENCE: {error}", file=sys.stderr)
    sys.exit(2)
PYEOF
