#!/usr/bin/env bash
set -euo pipefail

# bd-zywqc.2: fail-closed v9 concurrent-write performance diagnostic.
#
# This guard compares absolute FrankenSQLite throughput emitted by the v9
# one-row, separate-table mt-mvcc-bench contract under both shipped `release`
# and throughput-oriented `release-perf`. It also requires 8/1 and 16/8 scaling
# retention. Historical baseline/current samples are independent two-sample
# evidence; only the C/C and C/F arms inside each report are paired. The guard
# is intentionally fail-closed: benchmark failures, missing baselines,
# malformed evidence, incompatible provenance, anything other than the exact
# configured 21 counterbalanced rounds, regressions, and statistically
# inconclusive comparisons all return non-zero.
# The tracked zero-drop 8-writer threshold is therefore an unresolved release
# policy blocker for ordinary noisy measurements: a non-degenerate two-sided
# confidence interval can overlap zero even when baseline and candidate are
# unchanged. Keep the gate diagnostic and fail-closed until the acceptance
# owner specifies an equivalence margin or other statistically decidable rule;
# do not silently weaken the threshold here.
#
# The underlying v9 benchmark report is explicitly non-citable. A green result
# here is useful as a same-environment development guard, but it is not by
# itself sufficient release evidence. The result now carries a typed v2
# performance-admission blocker so downstream Phase-5 evidence cannot mistake
# a diagnostic report for authorization. v9 is permanently rejected from v2
# authorization; an immutable B/T pack, policy hash, raw evidence hashes,
# calibration receipt, and sensitivity receipt are separate requirements.
# The 16-writer and scaling margins are required caller inputs because no
# acceptance-owner values exist yet. Supplying them makes the diagnostic rule
# explicit; it does not authorize those values or promote the result to release
# evidence.
#
# Usage:
#   ./scripts/perf_regression_gate.sh [--capture-baseline] [--target-dir DIR]
#       [--baseline-dir DIR] [--rows N]
#       --max-drop-16t FRACTION
#       --max-scaling-drop-8-over-1 FRACTION
#       --max-scaling-drop-16-over-8 FRACTION
#
# Analyze-only additionally requires both profile reports and the exact graph:
#       --analyze-only-release CURRENT.release.json
#       --analyze-only-release-perf CURRENT.release-perf.json
#       --graph-artifact dependency-feature-graph.json
#       [--expected-commit SHA]
#
# Modes:
#   Default:             run the benchmark and compare with an existing baseline
#   --capture-baseline:  validate and create a new baseline; never overwrite
#   --analyze-only-*:    skip Cargo and analyze an existing report pair (test/debug)
#
# Artifacts:
#   $TARGET_DIR/regression_gate_runs/$RUN_ID/current.release.json
#   $TARGET_DIR/regression_gate_runs/$RUN_ID/current.release-perf.json
#   $TARGET_DIR/regression_gate_runs/$RUN_ID/dependency-feature-graph.json
#   $TARGET_DIR/regression_gate_runs/$RUN_ID/bench.{release,release-perf}.log
#   $TARGET_DIR/regression_gate_runs/$RUN_ID/result.json
#   $TARGET_DIR/regression_gate_builds/$RUN_ID/{release,release-perf}/
#   $BASELINE_DIR/latest.json

readonly BEAD_ID="bd-zywqc.2"
readonly GATE_SCHEMA="fsqlite.perf_regression_gate.result.v5"
readonly BASELINE_SCHEMA="fsqlite.perf_regression_gate.baseline.v6"
readonly REPORT_SCHEMA="fsqlite-e2e.mt_mvcc_bench_report.v9"
readonly ITERATIONS=21
readonly MAX_FSQLITE_WPS_DROP_1T=0.05
readonly MAX_FSQLITE_WPS_DROP_8T=0.0
readonly REQUIRED_THREADS="1,8,16"
readonly PROFILES="release release-perf"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"

TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/fsqlite-reggate-target}"
BASELINE_DIR="$REPO_ROOT/tests/perf/baselines"
CAPTURE_BASELINE=false
ROWS_PER_THREAD=500
ANALYZE_ONLY_RELEASE=""
ANALYZE_ONLY_RELEASE_PERF=""
GRAPH_ARTIFACT_INPUT=""
EXPECTED_COMMIT_OVERRIDE=""
MAX_FSQLITE_WPS_DROP_16T=""
MAX_SCALING_DROP_8_OVER_1=""
MAX_SCALING_DROP_16_OVER_8=""

usage() {
    printf '%s\n' \
        "Usage: $0 [--capture-baseline] [--target-dir DIR]" \
        "          [--baseline-dir DIR] [--rows N]" \
        "          --max-drop-16t FRACTION" \
        "          --max-scaling-drop-8-over-1 FRACTION" \
        "          --max-scaling-drop-16-over-8 FRACTION" \
        "          [--analyze-only-release CURRENT.release.json" \
        "           --analyze-only-release-perf CURRENT.release-perf.json" \
        "           --graph-artifact dependency-feature-graph.json" \
        "           --expected-commit SHA]" >&2
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
        --analyze-only-release)
            require_option_value "$1" "$#"
            ANALYZE_ONLY_RELEASE="$2"
            shift 2
            ;;
        --analyze-only-release-perf)
            require_option_value "$1" "$#"
            ANALYZE_ONLY_RELEASE_PERF="$2"
            shift 2
            ;;
        --graph-artifact)
            require_option_value "$1" "$#"
            GRAPH_ARTIFACT_INPUT="$2"
            shift 2
            ;;
        --expected-commit)
            require_option_value "$1" "$#"
            EXPECTED_COMMIT_OVERRIDE="$2"
            shift 2
            ;;
        --max-drop-16t)
            require_option_value "$1" "$#"
            MAX_FSQLITE_WPS_DROP_16T="$2"
            shift 2
            ;;
        --max-scaling-drop-8-over-1)
            require_option_value "$1" "$#"
            MAX_SCALING_DROP_8_OVER_1="$2"
            shift 2
            ;;
        --max-scaling-drop-16-over-8)
            require_option_value "$1" "$#"
            MAX_SCALING_DROP_16_OVER_8="$2"
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
analyze_only_count=0
[[ -n "$ANALYZE_ONLY_RELEASE" ]] && analyze_only_count=$((analyze_only_count + 1))
[[ -n "$ANALYZE_ONLY_RELEASE_PERF" ]] && analyze_only_count=$((analyze_only_count + 1))
[[ -n "$GRAPH_ARTIFACT_INPUT" ]] && analyze_only_count=$((analyze_only_count + 1))
if [[ "$analyze_only_count" -ne 0 && "$analyze_only_count" -ne 3 ]]; then
    echo "[$BEAD_ID] FATAL: analyze-only mode requires both profile reports and --graph-artifact" >&2
    exit 2
fi
ANALYZE_ONLY_REQUESTED=false
[[ "$analyze_only_count" -eq 3 ]] && ANALYZE_ONLY_REQUESTED=true
if [[ -n "$EXPECTED_COMMIT_OVERRIDE" && "$ANALYZE_ONLY_REQUESTED" != true ]]; then
    echo "[$BEAD_ID] FATAL: --expected-commit is only valid in analyze-only mode" >&2
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
if [[ "$ANALYZE_ONLY_REQUESTED" = true ]]; then
    [[ "$ANALYZE_ONLY_RELEASE" = /* ]] || ANALYZE_ONLY_RELEASE="$PWD/$ANALYZE_ONLY_RELEASE"
    [[ "$ANALYZE_ONLY_RELEASE_PERF" = /* ]] || ANALYZE_ONLY_RELEASE_PERF="$PWD/$ANALYZE_ONLY_RELEASE_PERF"
    [[ "$GRAPH_ARTIFACT_INPUT" = /* ]] || GRAPH_ARTIFACT_INPUT="$PWD/$GRAPH_ARTIFACT_INPUT"
fi

if [[ ! "$ROWS_PER_THREAD" =~ ^[1-9][0-9]*$ ]]; then
    echo "[$BEAD_ID] FATAL: --rows must be a positive integer" >&2
    exit 2
fi
validate_fraction() {
    local name="$1"
    local value="$2"
    if [[ -z "$value" ]] || ! python3 - "$value" <<'PYEOF'
import math
import sys
try:
    value = float(sys.argv[1])
except ValueError:
    raise SystemExit(1)
raise SystemExit(0 if math.isfinite(value) and 0.0 <= value < 1.0 else 1)
PYEOF
    then
        echo "[$BEAD_ID] FATAL: $name must be an explicit finite fraction in [0,1)" >&2
        exit 2
    fi
}
validate_fraction --max-drop-16t "$MAX_FSQLITE_WPS_DROP_16T"
validate_fraction --max-scaling-drop-8-over-1 "$MAX_SCALING_DROP_8_OVER_1"
validate_fraction --max-scaling-drop-16-over-8 "$MAX_SCALING_DROP_16_OVER_8"

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

CURRENT_RELEASE_JSON="$RUN_DIR/current.release.json"
CURRENT_RELEASE_PERF_JSON="$RUN_DIR/current.release-perf.json"
RESULT_JSON="$RUN_DIR/result.json"
BASELINE_JSON="$BASELINE_DIR/latest.json"
GRAPH_JSON="$RUN_DIR/dependency-feature-graph.json"
COMMIT_HASH="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
if [[ -n "$EXPECTED_COMMIT_OVERRIDE" ]]; then
    COMMIT_HASH="$EXPECTED_COMMIT_OVERRIDE"
fi
if [[ -z "$COMMIT_HASH" || "$COMMIT_HASH" == "unknown" ]]; then
    echo "[$BEAD_ID] FATAL: intended source commit is unknown" >&2
    exit 2
fi
MEASUREMENT_MODE="measured"

echo "[$BEAD_ID] Diagnostic-only v9 performance regression guard (v2 authorization rejected)"
echo "[$BEAD_ID] Commit: $COMMIT_HASH"
echo "[$BEAD_ID] Rows/thread: $ROWS_PER_THREAD"
echo "[$BEAD_ID] Iterations: $ITERATIONS"
echo "[$BEAD_ID] Profiles: $PROFILES"
echo "[$BEAD_ID] Threads: $REQUIRED_THREADS"
echo "[$BEAD_ID] Absolute maximum drops: 1t=$MAX_FSQLITE_WPS_DROP_1T 8t=$MAX_FSQLITE_WPS_DROP_8T 16t=$MAX_FSQLITE_WPS_DROP_16T"
echo "[$BEAD_ID] Scaling maximum drops (caller-supplied, policy-unresolved): 8/1=$MAX_SCALING_DROP_8_OVER_1 16/8=$MAX_SCALING_DROP_16_OVER_8"
echo "[$BEAD_ID] Target: $TARGET_DIR"
echo "[$BEAD_ID] Run directory: $RUN_DIR"

if ! cd "$REPO_ROOT"; then
    echo "[$BEAD_ID] FATAL: cannot enter repository root: $REPO_ROOT" >&2
    exit 2
fi

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
    for source in "$ANALYZE_ONLY_RELEASE" "$ANALYZE_ONLY_RELEASE_PERF" "$GRAPH_ARTIFACT_INPUT"; do
        [[ -f "$source" ]] || { echo "[$BEAD_ID] FATAL: analyze-only input does not exist: $source" >&2; exit 2; }
    done
    cp "$ANALYZE_ONLY_RELEASE" "$CURRENT_RELEASE_JSON"
    cp "$ANALYZE_ONLY_RELEASE_PERF" "$CURRENT_RELEASE_PERF_JSON"
    cp "$GRAPH_ARTIFACT_INPUT" "$GRAPH_JSON"
    MEASUREMENT_MODE="analyze_only"
    echo "[$BEAD_ID] Analyze-only release source: $ANALYZE_ONLY_RELEASE"
    echo "[$BEAD_ID] Analyze-only release-perf source: $ANALYZE_ONLY_RELEASE_PERF"
else
    GRAPH_RAW="$RUN_DIR/dependency-feature-graph.raw.txt"
    GRAPH_LOG="$RUN_DIR/dependency-feature-graph.log"
    BUILD_TARGET="$(rustc -vV 2>/dev/null | awk '/^host:/ { print $2 }')"
    [[ -n "$BUILD_TARGET" ]] || { echo "[$BEAD_ID] FATAL: cannot determine rustc host target" >&2; exit 2; }
    set +e
    cargo tree --locked --offline -p fsqlite-e2e -e features,no-dev \
        --no-default-features --target "$BUILD_TARGET" >"$GRAPH_RAW" 2>"$GRAPH_LOG"
    graph_status=$?
    set -e
    [[ "$graph_status" -eq 0 && -s "$GRAPH_RAW" ]] || { echo "[$BEAD_ID] FATAL: dependency-feature graph capture failed" >&2; exit 2; }
    python3 - "$REPO_ROOT" "$BUILD_TARGET" "$GRAPH_RAW" "$GRAPH_JSON" <<'PYEOF'
import json
import sys
from pathlib import Path
repo_root, target, raw_path, output_path = sys.argv[1:]
tree = Path(raw_path).read_text(encoding="utf-8").replace("\r\n", "\n")
tree = tree.replace(repo_root, "${WORKSPACE_ROOT}")
tree = "\n".join(line.rstrip() for line in tree.splitlines()) + "\n"
value = {
    "schema_version": "fsqlite.dependency_feature_graph.v1",
    "command": ["cargo", "tree", "--locked", "--offline", "-p", "fsqlite-e2e", "-e", "features,no-dev", "--no-default-features", "--target", target],
    "target": target,
    "tree": tree,
}
Path(output_path).write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
PYEOF
    GRAPH_SHA256="$(python3 - "$GRAPH_JSON" <<'PYEOF'
import hashlib
import sys
from pathlib import Path
print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PYEOF
    )"
    for profile in $PROFILES; do
        history_json="$RUN_DIR/disposable_history.$profile.json"
        bench_log="$RUN_DIR/bench.$profile.log"
        profile_target="$TARGET_DIR/regression_gate_builds/$RUN_ID/$profile"
        current_json="$RUN_DIR/current.$profile.json"
        nonce="$RUN_ID.$profile"
        echo "[$BEAD_ID] Running v9 mt-mvcc-bench profile=$profile threads=$REQUIRED_THREADS ..."
        set +e
        env CARGO_TARGET_DIR="$profile_target" \
            FSQLITE_BENCH_PROFILE_NAME="$profile" \
            FSQLITE_BENCH_BUILD_NONCE="$nonce" \
            FSQLITE_BENCH_RESOLVED_DEPENDENCY_FEATURE_GRAPH_SHA256="$GRAPH_SHA256" \
            cargo run --locked --offline -p fsqlite-e2e --no-default-features \
            --target "$BUILD_TARGET" \
            --bin mt-mvcc-bench --profile "$profile" -- \
            --threads="$REQUIRED_THREADS" \
            --rows-per-thread="$ROWS_PER_THREAD" \
            --iters="$ITERATIONS" \
            --separate-tables \
            --one-row-per-transaction \
            --history-json="$history_json" \
            --json-output="$current_json" 2>&1 | tee "$bench_log"
        pipeline_status=("${PIPESTATUS[@]}")
        set -e
        benchmark_status="${pipeline_status[0]}"
        tee_status="${pipeline_status[1]}"
        if [[ "$benchmark_status" -ne 0 || "$tee_status" -ne 0 ]]; then
            echo "[$BEAD_ID] FATAL: $profile benchmark pipeline failed (benchmark=$benchmark_status, tee=$tee_status)" >&2
            exit 2
        fi
        if [[ -e "$history_json" || -L "$history_json" ]]; then
            echo "[$BEAD_ID] FATAL: non-citable v9 benchmark unexpectedly created history: $history_json" >&2
            exit 2
        fi
    done
fi

for artifact in "$CURRENT_RELEASE_JSON" "$CURRENT_RELEASE_PERF_JSON" "$GRAPH_JSON"; do
    [[ -s "$artifact" ]] || { echo "[$BEAD_ID] FATAL: missing or empty artifact: $artifact" >&2; exit 2; }
done

python3 - \
    "$CURRENT_RELEASE_JSON" \
    "$CURRENT_RELEASE_PERF_JSON" \
    "$GRAPH_JSON" \
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
    "$MAX_FSQLITE_WPS_DROP_16T" \
    "$MAX_SCALING_DROP_8_OVER_1" \
    "$MAX_SCALING_DROP_16_OVER_8" \
    "$MEASUREMENT_MODE" \
    "$RUN_ID" <<'PYEOF'
import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
import sys
from pathlib import Path

# Active v9 analyzer. The implementation-local `v8_` names identify its direct
# analyzer lineage and are not serialized contract labels. The older v7
# implementation remains below solely so historical diffs stay reviewable;
# this entry point always exits before that code and therefore rejects prior
# report schemas at the boundary.
(
    current_release_path_v8,
    current_release_perf_path_v8,
    graph_path_v8,
    baseline_path_v8,
    result_path_v8,
    intended_commit_v8,
    expected_rows_raw_v8,
    capture_raw_v8,
    baseline_schema_v8,
    gate_schema_v8,
    report_schema_v8,
    expected_iterations_raw_v8,
    max_drop_1t_raw_v8,
    max_drop_8t_raw_v8,
    max_drop_16t_raw_v8,
    max_scaling_drop_8_over_1_raw_v8,
    max_scaling_drop_16_over_8_raw_v8,
    measurement_mode_v8,
    run_id_v8,
) = sys.argv[1:]

EXPECTED_NON_CITABLE_REASON_V9 = (
    "v9 extends the explicit one-row transaction/retry-unit contract with retryable "
    "statement-preparation truth under the shared worker deadline and an exact v2 FSQLite "
    "retry identity; it retains an optional build-attested resolved dependency/feature-graph "
    "digest, but remains non-citable: bd-uh1fv still requires an external watchdog, sanitized "
    "environment, matched retry/deadline semantics, counterbalanced topology receipts, "
    "immutable manifest, retained baseline history, and independent verification; a default "
    "build also leaves the graph digest unavailable."
)
EXPECTED_RELEASE_SCOPE_V9 = (
    "Narrow same-process, same-host F/C writer-throughput comparison for only this "
    "report's attested selected Cargo profile and the requested mt-mvcc-bench "
    "workload/configurations; this report does not cover other workloads or platforms, "
    "long-term baseline retention, independent reproduction, or overall release eligibility."
)
EXPECTED_SETTINGS_INTERPRETATION_V9 = (
    "Both engines proved the listed effective PRAGMA values; equal names and readbacks "
    "do not establish cross-engine semantic equivalence."
)
EXPECTED_ACCOUNTING_INTERPRETATION_V9 = (
    "offered and committed writes share one row unit; attempted_writes counts physical "
    "INSERT calls; retried_operations records the existing engine-specific retry unit and "
    "is provenance only, not a cross-engine comparison metric."
)
EXPECTED_TIMING_INTERPRETATION_V9 = (
    "workload_elapsed_ns begins only after every worker has opened and proved its effective "
    "settings, and ends at the last worker's transaction terminal point before connection "
    "teardown; worker_startup_elapsed_ns is reported separately."
)
GRAPH_ATTESTATION_AVAILABLE_V8 = (
    "available: the lowercase SHA-256 was supplied at build time through the rerun-sensitive "
    "FSQLITE_BENCH_RESOLVED_DEPENDENCY_FEATURE_GRAPH_SHA256 attestation input"
)
CSQLITE_RETRY_UNIT_V9 = "whole one-row BEGIN/INSERT/COMMIT transaction attempt"
FSQLITE_RETRY_UNIT_V9 = (
    "statement preparation or whole one-row BEGIN CONCURRENT/INSERT/COMMIT transaction attempt"
)
CSQLITE_RETRY_ALGORITHM_V9 = (
    "csqlite.whole-one-row-transaction.fixed-1ms.busy-or-locked.max-512-or-"
    "shared-worker-timeout.v1"
)
FSQLITE_RETRY_ALGORITHM_V9 = (
    "fsqlite.prepare-or-whole-one-row-transaction.step-exp-every-8-cap-25ms-plus-"
    "thread-attempt-jitter-0-to-4ms.max-512-or-shared-worker-timeout.v2"
)
FSQLITE_RETRYABLE_ERRORS_V9 = (
    "Busy|BusyRecovery|BusySnapshot|DatabaseLocked|WriteConflict|"
    "SerializationFailure|PageBufferCapacityExhausted"
)
REQUIRED_PROFILES_V8 = ("release", "release-perf")
REQUIRED_THREADS_V8 = (1, 8, 16)
BOOTSTRAP_REPETITIONS_V8 = 20_000
UNRESOLVED_RELEASE_COVERAGE_V8 = [
    "32-writer cell",
    "balanced, write-heavy, and read-heavy macro workloads",
    "INSERT, SELECT-by-primary-key, and UPDATE micro workloads",
    "calibration receipt",
    "synthetic-regression sensitivity proof",
    "flamegraph evidence",
    "rolling 30-day retained baselines",
    "historical baseline/current paired block deltas; this gate uses independent samples",
    "non-host release platforms",
    "external watchdog enforcement",
    "sanitized benchmark environment",
    "matched cross-engine retry/deadline semantics",
    "counterbalanced topology receipts",
    "immutable measurement manifest",
    "independent verification",
    "pinned-host enforcement",
]


class V8EvidenceError(Exception):
    pass


def v8_fail(message):
    raise V8EvidenceError(message)


def v8_require_object(value, label, fields=()):
    if not isinstance(value, dict):
        v8_fail(f"{label} must be an object")
    for field in fields:
        if field not in value:
            v8_fail(f"{label}.{field} must be present")
    return value


def v8_require_list(value, label):
    if not isinstance(value, list):
        v8_fail(f"{label} must be an array")
    return value


def v8_require_string(value, label):
    if not isinstance(value, str) or not value.strip():
        v8_fail(f"{label} must be a non-empty string")
    if value == "unknown" or value.startswith("unknown:"):
        v8_fail(f"{label} must be known")
    return value


def v8_require_int(value, label, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, int):
        v8_fail(f"{label} must be an integer")
    if positive and value <= 0:
        v8_fail(f"{label} must be positive")
    return value


def v8_require_number(value, label, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        v8_fail(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0.0):
        v8_fail(f"{label} must be {'positive and ' if positive else ''}finite")
    return result


def v8_require_exact(value, expected, label):
    if expected is None:
        if value is not None:
            v8_fail(f"{label} must be null")
    elif isinstance(expected, bool):
        if value is not expected:
            v8_fail(f"{label} must be the boolean {str(expected).lower()}")
    elif isinstance(expected, int):
        if isinstance(value, bool) or not isinstance(value, int) or value != expected:
            v8_fail(f"{label} must be the integer {expected}")
    elif isinstance(expected, float):
        if not isinstance(value, float) or not math.isfinite(value) or value != expected:
            v8_fail(f"{label} must be the floating-point value {expected}")
    elif isinstance(expected, str):
        if not isinstance(value, str) or value != expected:
            v8_fail(f"{label} does not match the exact string contract")
    elif isinstance(expected, list):
        observed = v8_require_list(value, label)
        if len(observed) != len(expected):
            v8_fail(f"{label} does not match the exact array length")
        for index, (observed_item, expected_item) in enumerate(zip(observed, expected)):
            v8_require_exact(observed_item, expected_item, f"{label}[{index}]")
    elif isinstance(expected, dict):
        observed = v8_require_object(value, label)
        if set(observed) != set(expected):
            v8_fail(f"{label} does not contain the exact object fields")
        for field, expected_item in expected.items():
            v8_require_exact(observed[field], expected_item, f"{label}.{field}")
    else:
        v8_fail(f"{label} has an unsupported analyzer contract type")
    return value


def v8_require_sha256(value, label):
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        v8_fail(f"{label} must be exactly 32 bytes of lowercase hexadecimal")
    return value


def v8_require_git_sha(value, label):
    if (
        not isinstance(value, str)
        or len(value) != 40
        or any(character not in "0123456789abcdef" for character in value)
    ):
        v8_fail(f"{label} must be a 40-character lowercase hexadecimal Git object ID")
    return value


def v8_require_fraction(value, label):
    result = float(value)
    if not math.isfinite(result) or result < 0.0 or result >= 1.0:
        v8_fail(f"{label} must be a finite fraction in [0,1)")
    return result


def v8_read_json(path, label):
    try:
        with open(path, encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        v8_fail(f"cannot read {label} JSON {path}: {error}")
    return v8_require_object(value, label)


def v8_canonical_compact_bytes(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )


def v8_canonical_pretty_bytes(value):
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def v8_validate_graph(graph, label):
    graph = v8_require_object(
        graph, label, ("schema_version", "command", "target", "tree")
    )
    if graph["schema_version"] != "fsqlite.dependency_feature_graph.v1":
        v8_fail(f"{label} schema is not v1")
    target = v8_require_string(graph["target"], f"{label}.target")
    expected_command = [
        "cargo",
        "tree",
        "--locked",
        "--offline",
        "-p",
        "fsqlite-e2e",
        "-e",
        "features,no-dev",
        "--no-default-features",
        "--target",
        target,
    ]
    if graph["command"] != expected_command:
        v8_fail(f"{label} command is not the exact locked no-dev feature graph")
    tree = v8_require_string(graph["tree"], f"{label}.tree")
    if not tree.endswith("\n") or "${WORKSPACE_ROOT}" not in tree:
        v8_fail(f"{label} tree is not newline-terminated and workspace-normalized")
    return graph


def v8_read_graph(path):
    try:
        raw = Path(path).read_bytes()
        graph = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        v8_fail(f"cannot read dependency/feature graph {path}: {error}")
    graph = v8_validate_graph(graph, "dependency/feature graph")
    if raw != v8_canonical_compact_bytes(graph):
        v8_fail("dependency/feature graph bytes are not canonical sorted compact JSON")
    return graph, hashlib.sha256(raw).hexdigest()


def v8_command_output(*argv):
    try:
        return subprocess.run(
            argv,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        v8_fail(f"cannot fingerprint command {' '.join(argv)}: {error}")


def v8_optional_text(path):
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except OSError:
        return "unavailable"


def v8_cpu_model():
    try:
        for line in Path("/proc/cpuinfo").read_text(
            encoding="utf-8", errors="replace"
        ).splitlines():
            if line.lower().startswith(("model name", "hardware")) and ":" in line:
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def v8_provenance(expected_rows, graph_target):
    affinity_getter = getattr(os, "sched_getaffinity", None)
    try:
        affinity_count = (
            len(affinity_getter(0)) if affinity_getter is not None else "unavailable"
        )
    except OSError as error:
        v8_fail(f"cannot read CPU-affinity fingerprint: {error}")
    return {
        "schema_version": "fsqlite.perf_regression_gate.provenance.v2",
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_model": v8_cpu_model(),
        "logical_cpu_count": os.cpu_count(),
        "affinity_cpu_count": affinity_count,
        "cgroup_cpu_max": v8_optional_text("/sys/fs/cgroup/cpu.max"),
        "cgroup_cpuset_effective": v8_optional_text(
            "/sys/fs/cgroup/cpuset.cpus.effective"
        ),
        "cpu_governor": v8_optional_text(
            "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
        ),
        "rustc_verbose": v8_command_output("rustc", "-Vv"),
        "cargo_version": v8_command_output("cargo", "-V"),
        "rustflags": os.environ.get("RUSTFLAGS", ""),
        "cargo_encoded_rustflags": os.environ.get("CARGO_ENCODED_RUSTFLAGS", ""),
        "rch_worker": os.environ.get("RCH_WORKER", ""),
        "profiles": list(REQUIRED_PROFILES_V8),
        "benchmark": "mt-mvcc-bench",
        "threads": list(REQUIRED_THREADS_V8),
        "rows_per_thread": expected_rows,
        "graph_target": graph_target,
        "measurement_mode": measurement_mode_v8,
    }


def v8_validate_snapshot(value, label):
    value = v8_require_object(
        value,
        label,
        ("sha256", "bytes_read", "metadata_size_bytes", "unix_device", "unix_inode", "error"),
    )
    if value["error"] is not None:
        v8_fail(f"{label}.error must be null")
    identity = {
        "sha256": v8_require_sha256(value["sha256"], f"{label}.sha256"),
        "bytes_read": v8_require_int(value["bytes_read"], f"{label}.bytes_read", positive=True),
        "metadata_size_bytes": v8_require_int(
            value["metadata_size_bytes"], f"{label}.metadata_size_bytes", positive=True
        ),
        "unix_device": v8_require_int(value["unix_device"], f"{label}.unix_device"),
        "unix_inode": v8_require_int(value["unix_inode"], f"{label}.unix_inode", positive=True),
    }
    if identity["bytes_read"] != identity["metadata_size_bytes"]:
        v8_fail(f"{label} byte count disagrees with metadata")
    return identity


def v8_validate_subject_identity(report, label, expected_commit, expected_nonce):
    subject = v8_require_object(
        report.get("subject_identity"),
        f"{label}.subject_identity",
        ("executable", "build_source", "runtime_source", "cargo_lock"),
    )
    executable = v8_require_object(
        subject["executable"],
        f"{label}.subject_identity.executable",
        (
            "current_exe_path",
            "canonical_path",
            "path_resolution_error",
            "process_id",
            "before_measurement",
            "after_measurement",
            "unchanged_during_measurement",
        ),
    )
    v8_require_string(executable["current_exe_path"], f"{label}.executable.current_exe_path")
    v8_require_string(executable["canonical_path"], f"{label}.executable.canonical_path")
    if executable["path_resolution_error"] is not None:
        v8_fail(f"{label}.executable.path_resolution_error must be null")
    v8_require_int(executable["process_id"], f"{label}.executable.process_id", positive=True)
    before_exe = v8_validate_snapshot(executable["before_measurement"], f"{label}.executable.before")
    after_exe = v8_validate_snapshot(executable["after_measurement"], f"{label}.executable.after")
    if before_exe != after_exe or executable["unchanged_during_measurement"] is not True:
        v8_fail(f"{label}.executable changed during measurement")

    build = v8_require_object(
        subject["build_source"],
        f"{label}.subject_identity.build_source",
        ("workspace_root", "git_sha", "git_branch", "git_tree_state", "build_nonce", "build_input_tracking"),
    )
    workspace_root = v8_require_string(build["workspace_root"], f"{label}.build.workspace_root")
    build_sha = v8_require_git_sha(build["git_sha"], f"{label}.build.git_sha")
    v8_require_string(build["git_branch"], f"{label}.build.git_branch")
    if build["git_tree_state"] != "clean" or build["build_input_tracking"] != "complete":
        v8_fail(f"{label}.build must be clean with complete input tracking")
    nonce = v8_require_string(build["build_nonce"], f"{label}.build.build_nonce")
    if expected_commit is not None and build_sha != expected_commit:
        v8_fail(f"{label}.build.git_sha does not match analyzer intended commit")
    if expected_nonce is not None and nonce != expected_nonce:
        v8_fail(f"{label}.build.build_nonce does not match its measured profile run")

    runtime = v8_require_object(
        subject["runtime_source"],
        f"{label}.subject_identity.runtime_source",
        ("before_measurement", "after_measurement", "same_clean_git_identity_at_capture_points", "stability_limitation"),
    )
    runtime_identities = []
    for point in ("before_measurement", "after_measurement"):
        receipt = v8_require_object(
            runtime[point],
            f"{label}.runtime.{point}",
            (
                "workspace_root",
                "canonical_workspace_root",
                "git_sha",
                "git_branch",
                "git_tree_state",
                "matches_build_git_sha",
                "discovery_errors",
            ),
        )
        identity = (
            v8_require_string(receipt["workspace_root"], f"{label}.runtime.{point}.workspace_root"),
            v8_require_string(receipt["canonical_workspace_root"], f"{label}.runtime.{point}.canonical_workspace_root"),
            v8_require_git_sha(receipt["git_sha"], f"{label}.runtime.{point}.git_sha"),
            v8_require_string(receipt["git_branch"], f"{label}.runtime.{point}.git_branch"),
        )
        if identity[0] != workspace_root or identity[1] != workspace_root or identity[2] != build_sha:
            v8_fail(f"{label}.runtime.{point} does not match the embedded build")
        if (
            receipt["git_tree_state"] != "clean"
            or receipt["matches_build_git_sha"] is not True
            or receipt["discovery_errors"] != []
        ):
            v8_fail(f"{label}.runtime.{point} is not clean and build-bound")
        runtime_identities.append(identity)
    if runtime_identities[0] != runtime_identities[1]:
        v8_fail(f"{label}.runtime source identity changed during measurement")
    if runtime["same_clean_git_identity_at_capture_points"] is not True:
        v8_fail(f"{label}.runtime stability receipt is not true")
    v8_require_string(runtime["stability_limitation"], f"{label}.runtime.stability_limitation")

    cargo_lock = v8_require_object(
        subject["cargo_lock"],
        f"{label}.subject_identity.cargo_lock",
        (
            "embedded_build_sha256",
            "embedded_build_size_bytes",
            "runtime_path",
            "before_measurement",
            "after_measurement",
            "before_matches_embedded_build",
            "after_matches_embedded_build",
            "unchanged_at_capture_points",
        ),
    )
    lock_sha = v8_require_sha256(cargo_lock["embedded_build_sha256"], f"{label}.cargo_lock.sha256")
    lock_size = v8_require_int(cargo_lock["embedded_build_size_bytes"], f"{label}.cargo_lock.size", positive=True)
    v8_require_string(cargo_lock["runtime_path"], f"{label}.cargo_lock.runtime_path")
    before_lock = v8_validate_snapshot(cargo_lock["before_measurement"], f"{label}.cargo_lock.before")
    after_lock = v8_validate_snapshot(cargo_lock["after_measurement"], f"{label}.cargo_lock.after")
    if before_lock != after_lock or before_lock["sha256"] != lock_sha or before_lock["bytes_read"] != lock_size:
        v8_fail(f"{label}.cargo_lock does not match its embedded build")
    for field in ("before_matches_embedded_build", "after_matches_embedded_build", "unchanged_at_capture_points"):
        if cargo_lock[field] is not True:
            v8_fail(f"{label}.cargo_lock.{field} must be true")
    return build_sha


def v8_decode_hex(value, label):
    if not isinstance(value, str) or len(value) % 2 or any(
        character not in "0123456789abcdef" for character in value
    ):
        v8_fail(f"{label} must be lowercase hexadecimal")
    return bytes.fromhex(value)


def v8_validate_build_configuration(value, label, profile, graph, graph_sha256):
    value = v8_require_object(
        value,
        label,
        (
            "cargo_profile",
            "selected_profile",
            "profile_label",
            "opt_level",
            "debug",
            "target",
            "build_host",
            "enabled_features",
            "rustflags",
            "profile_overrides_hex",
            "native_build_overrides_hex",
            "rustc_version_verbose",
            "cargo_version",
            "resolved_dependency_feature_graph_sha256",
            "resolved_dependency_feature_graph_limitation",
        ),
    )
    expected = {
        "release": ("release", "release", "release", "z"),
        "release-perf": ("release", "release-perf", "release-perf", "3"),
    }[profile]
    observed = tuple(value[field] for field in ("cargo_profile", "selected_profile", "profile_label", "opt_level"))
    if observed != expected or value["debug"] != "false":
        v8_fail(f"{label} does not identify the exact {profile} profile")
    if value["target"] != graph["target"]:
        v8_fail(f"{label}.target does not match the retained dependency/feature graph")
    v8_require_string(value["build_host"], f"{label}.build_host")
    if value["enabled_features"] != []:
        v8_fail(f"{label}.enabled_features must be empty for --no-default-features")
    rustflags = v8_require_object(
        value["rustflags"],
        f"{label}.rustflags",
        ("cargo_encoded_rustflags_present", "encoded_hex", "decoded_arguments", "decode_error"),
    )
    if not isinstance(rustflags["cargo_encoded_rustflags_present"], bool) or rustflags["decode_error"] is not None:
        v8_fail(f"{label}.rustflags is not a complete decodable receipt")
    encoded = v8_decode_hex(rustflags["encoded_hex"], f"{label}.rustflags.encoded_hex")
    try:
        decoded = encoded.decode("utf-8")
    except UnicodeDecodeError as error:
        v8_fail(f"{label}.rustflags is not UTF-8: {error}")
    expected_decoded = [part for part in decoded.split("\x1f") if part]
    if rustflags["decoded_arguments"] != expected_decoded:
        v8_fail(f"{label}.rustflags decoded arguments disagree with encoded bytes")
    if not rustflags["cargo_encoded_rustflags_present"] and encoded:
        v8_fail(f"{label}.rustflags claims absent bytes but contains data")
    v8_decode_hex(value["profile_overrides_hex"], f"{label}.profile_overrides_hex")
    v8_decode_hex(value["native_build_overrides_hex"], f"{label}.native_build_overrides_hex")
    v8_require_string(value["rustc_version_verbose"], f"{label}.rustc_version_verbose")
    v8_require_string(value["cargo_version"], f"{label}.cargo_version")
    if value["resolved_dependency_feature_graph_sha256"] != graph_sha256:
        v8_fail(f"{label} does not attest the exact retained dependency/feature graph bytes")
    if value["resolved_dependency_feature_graph_limitation"] != GRAPH_ATTESTATION_AVAILABLE_V8:
        v8_fail(f"{label} does not identify an available build-time graph attestation")
    return value


def v8_validate_invocation(value, label, expected_rows, expected_iterations, expected_history, expected_output):
    value = v8_require_object(
        value,
        label,
        ("argv_lossy", "argv_raw_hex", "raw_encoding", "length_prefixed_argv_sha256"),
    )
    lossy = v8_require_list(value["argv_lossy"], f"{label}.argv_lossy")
    raw_hex = v8_require_list(value["argv_raw_hex"], f"{label}.argv_raw_hex")
    if len(lossy) != len(raw_hex) or len(lossy) != 8 or value["raw_encoding"] != "unix_os_str_bytes":
        v8_fail(f"{label} must contain the exact eight-argument Unix invocation receipt")
    canonical = bytearray()
    decoded = []
    for index, encoded in enumerate(raw_hex):
        raw = v8_decode_hex(encoded, f"{label}.argv_raw_hex[{index}]")
        canonical.extend(len(raw).to_bytes(8, byteorder="little"))
        canonical.extend(raw)
        try:
            decoded.append(raw.decode("utf-8"))
        except UnicodeDecodeError as error:
            v8_fail(f"{label}.argv_raw_hex[{index}] is not UTF-8: {error}")
    if decoded != lossy:
        v8_fail(f"{label}.argv_lossy disagrees with its raw argument bytes")
    if hashlib.sha256(canonical).hexdigest() != value["length_prefixed_argv_sha256"]:
        v8_fail(f"{label} digest disagrees with raw arguments")
    required_prefix = [
        f"--threads={','.join(map(str, REQUIRED_THREADS_V8))}",
        f"--rows-per-thread={expected_rows}",
        f"--iters={expected_iterations}",
        "--separate-tables",
        "--one-row-per-transaction",
    ]
    if lossy[1:6] != required_prefix:
        v8_fail(f"{label} is not the exact 1,8,16 separate-table one-row invocation")
    if not lossy[6].startswith("--history-json=") or not lossy[7].startswith("--json-output="):
        v8_fail(f"{label} must bind history and JSON output paths")
    history = lossy[6].split("=", 1)[1]
    output = lossy[7].split("=", 1)[1]
    if not history or not output:
        v8_fail(f"{label} contains an empty artifact path")
    if expected_history is not None and history != expected_history:
        v8_fail(f"{label} history path does not match its measured profile run")
    if expected_output is not None and output != expected_output:
        v8_fail(f"{label} output path does not match its measured profile run")
    return history, output


def v8_validate_host(value, label):
    value = v8_require_object(value, label, ("host", "before_measurement", "after_measurement"))
    host = v8_require_object(
        value["host"],
        f"{label}.host",
        (
            "hostname",
            "cpu_model",
            "available_parallelism",
            "cpu_online",
            "cpu_present",
            "cpu_possible",
            "cpu_isolated",
            "cpu_topology",
            "scaling_governors_by_cpu",
            "kernel_release",
            "kernel_version",
            "numa_online_nodes",
            "numa_possible_nodes",
            "numa_node_directories",
            "unavailable_fields",
        ),
    )
    for field in ("hostname", "cpu_model", "cpu_online", "cpu_present", "cpu_possible", "kernel_release", "kernel_version"):
        v8_require_string(host[field], f"{label}.host.{field}")
    available = v8_require_int(host["available_parallelism"], f"{label}.host.available_parallelism", positive=True)
    if available < max(REQUIRED_THREADS_V8):
        v8_fail(f"{label}.host lacks capacity for the required 16-writer cell")
    topology = v8_require_object(
        host["cpu_topology"],
        f"{label}.host.cpu_topology",
        ("logical_cpu_directories", "physical_package_count", "physical_core_count"),
    )
    v8_require_int(
        topology["logical_cpu_directories"],
        f"{label}.host.cpu_topology.logical_cpu_directories",
        positive=True,
    )
    for field in ("physical_package_count", "physical_core_count"):
        if topology[field] is not None:
            v8_require_int(topology[field], f"{label}.host.cpu_topology.{field}", positive=True)
    governors = v8_require_object(
        host["scaling_governors_by_cpu"], f"{label}.host.scaling_governors_by_cpu"
    )
    for cpu, governor in governors.items():
        v8_require_string(cpu, f"{label}.host.scaling_governors_by_cpu key")
        v8_require_string(governor, f"{label}.host.scaling_governors_by_cpu.{cpu}")
    if host["cpu_isolated"] is not None:
        v8_require_string(host["cpu_isolated"], f"{label}.host.cpu_isolated")
    for field in ("numa_online_nodes", "numa_possible_nodes"):
        if host[field] is not None:
            v8_require_string(host[field], f"{label}.host.{field}")
    if host["numa_node_directories"] is not None:
        numa_directories = v8_require_int(
            host["numa_node_directories"], f"{label}.host.numa_node_directories"
        )
        if numa_directories < 0:
            v8_fail(f"{label}.host.numa_node_directories must be non-negative")
    unavailable = v8_require_list(host["unavailable_fields"], f"{label}.host.unavailable_fields")
    for index, field in enumerate(unavailable):
        v8_require_string(field, f"{label}.host.unavailable_fields[{index}]")
    if len(unavailable) != len(set(unavailable)):
        v8_fail(f"{label}.host.unavailable_fields must be unique")
    expected_unavailable = [
        field
        for field in (
            "hostname",
            "cpu_model",
            "available_parallelism",
            "cpu_online",
            "cpu_present",
            "cpu_possible",
            "kernel_release",
            "kernel_version",
            "numa_online_nodes",
            "numa_possible_nodes",
            "numa_node_directories",
        )
        if host[field] is None
    ]
    if host["cpu_isolated"] is None:
        expected_unavailable.append("cpu_isolated")
    if not governors:
        expected_unavailable.append("scaling_governors_by_cpu")
    v8_require_exact(
        unavailable, expected_unavailable, f"{label}.host.unavailable_fields"
    )
    placement_fields = (
        "process_cpu_affinity_mask",
        "process_cpu_affinity_list",
        "proc_self_cgroup",
        "cpuset_cpus_effective",
        "cpuset_mems_effective",
    )
    placements = []
    timestamps = []
    for point in ("before_measurement", "after_measurement"):
        dynamic = v8_require_object(
            value[point],
            f"{label}.{point}",
            (
                "unix_epoch_millis",
                *placement_fields,
                "load_average",
                "pressure_cpu",
                "pressure_memory",
                "pressure_io",
            ),
        )
        timestamps.append(v8_require_int(dynamic["unix_epoch_millis"], f"{label}.{point}.unix_epoch_millis", positive=True))
        placements.append({field: v8_require_string(dynamic[field], f"{label}.{point}.{field}") for field in placement_fields})
        for field in ("load_average", "pressure_cpu", "pressure_memory", "pressure_io"):
            if dynamic[field] is not None:
                v8_require_string(dynamic[field], f"{label}.{point}.{field}")
    if timestamps[1] < timestamps[0] or placements[0] != placements[1]:
        v8_fail(f"{label} timestamps or CPU/cgroup placement changed during measurement")
    return host, placements[0], available


def v8_expected_retry_policy(writers, expected_rows):
    timeout_ms = (5 + writers * expected_rows // 5_000) * 1_000
    return {
        "csqlite_busy_timeout_ms": 5_000,
        "csqlite_max_operation_retries": 0,
        "csqlite_max_transaction_retries": 512,
        "csqlite_retry_sleep_ms": 1,
        "csqlite_retry_unit": CSQLITE_RETRY_UNIT_V9,
        "csqlite_retry_algorithm": CSQLITE_RETRY_ALGORITHM_V9,
        "shared_worker_retry_timeout_ms": timeout_ms,
        "shared_worker_retry_timeout_overridden": False,
        "fsqlite_transaction_timeout_ms": timeout_ms,
        "fsqlite_max_transaction_retries": 512,
        "fsqlite_retry_sleep_base_ms": 1,
        "fsqlite_retry_sleep_cap_ms": 29,
        "fsqlite_retry_unit": FSQLITE_RETRY_UNIT_V9,
        "fsqlite_retry_backoff_algorithm": FSQLITE_RETRY_ALGORITHM_V9,
        "fsqlite_retryable_errors": FSQLITE_RETRYABLE_ERRORS_V9,
        "fsqlite_timeout_overridden": False,
    }


def v8_expected_settings(concurrent_mode):
    return {
        "page_size_bytes": 4_096,
        "journal_mode": "wal",
        "synchronous": "normal",
        "cache_size": -64_000,
        "busy_timeout_ms": 5_000,
        "wal_autocheckpoint_pages": 1_000,
        "concurrent_mode": concurrent_mode,
    }


def v8_duration_seconds(elapsed_ns):
    seconds, nanoseconds = divmod(elapsed_ns, 1_000_000_000)
    return float(seconds) + float(nanoseconds) / 1_000_000_000.0


def v8_validate_sample(sample, label, offered_writes, expected_settings):
    sample = v8_require_object(sample, label, ("worker_startup_elapsed_ns", "workload_elapsed_ns", "settings", "accounting", "committed_state"))
    v8_require_int(sample["worker_startup_elapsed_ns"], f"{label}.worker_startup_elapsed_ns", positive=True)
    elapsed_ns = v8_require_int(sample["workload_elapsed_ns"], f"{label}.workload_elapsed_ns", positive=True)
    v8_require_exact(
        sample["settings"], expected_settings, f"{label}.settings"
    )
    accounting = v8_require_object(sample["accounting"], f"{label}.accounting")
    if v8_require_int(accounting.get("offered_writes"), f"{label}.offered_writes", positive=True) != offered_writes:
        v8_fail(f"{label}.offered_writes disagrees with the configuration")
    attempted = v8_require_int(accounting.get("attempted_writes"), f"{label}.attempted_writes", positive=True)
    succeeded = v8_require_int(accounting.get("succeeded_writes"), f"{label}.succeeded_writes", positive=True)
    retried = v8_require_int(accounting.get("retried_operations"), f"{label}.retried_operations")
    if attempted < succeeded or succeeded != offered_writes or retried < 0:
        v8_fail(f"{label}.accounting does not prove all offered writes")
    failed = v8_require_int(accounting.get("failed_writes"), f"{label}.failed_writes")
    worker_failed = v8_require_int(
        accounting.get("worker_reported_failed_writes"),
        f"{label}.worker_reported_failed_writes",
    )
    v8_require_exact(accounting.get("diagnostics"), [], f"{label}.accounting.diagnostics")
    if failed != 0 or worker_failed != 0 or accounting.get("exact") is not True:
        v8_fail(f"{label}.accounting is not exact and failure-free")
    committed = v8_require_object(sample["committed_state"], f"{label}.committed_state")
    v8_require_exact(committed.get("diagnostics"), [], f"{label}.committed_state.diagnostics")
    v8_require_exact(committed.get("integrity_check"), ["ok"], f"{label}.committed_state.integrity_check")
    if committed.get("valid") is not True:
        v8_fail(f"{label}.committed_state is not valid and diagnostic-free")
    expected_committed_rows = v8_require_int(
        committed.get("expected_rows"), f"{label}.committed_state.expected_rows"
    )
    observed_committed_rows = v8_require_int(
        committed.get("observed_rows"), f"{label}.committed_state.observed_rows"
    )
    if expected_committed_rows != succeeded or observed_committed_rows != succeeded:
        v8_fail(f"{label}.committed_state row oracle disagrees")
    expected_id_sum = v8_require_int(
        committed.get("expected_id_sum"), f"{label}.committed_state.expected_id_sum"
    )
    observed_id_sum = v8_require_int(
        committed.get("observed_id_sum"), f"{label}.committed_state.observed_id_sum"
    )
    if expected_id_sum != observed_id_sum:
        v8_fail(f"{label}.committed_state id-sum oracle disagrees")
    expected_payload = v8_require_sha256(committed.get("expected_payload_sha256"), f"{label}.expected_payload_sha256")
    if committed.get("observed_payload_sha256") != expected_payload:
        v8_fail(f"{label}.committed_state payload oracle disagrees")
    elapsed_seconds = v8_duration_seconds(elapsed_ns)
    return {
        "wps": succeeded / elapsed_seconds,
        "elapsed_ms": elapsed_seconds * 1_000.0,
    }


def v8_percentile(values, quantile):
    ordered = sorted(values)
    rank = quantile * (len(ordered) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def v8_close(observed, expected):
    return math.isclose(observed, expected, rel_tol=1e-9, abs_tol=1e-9)


def v8_lcg_next(state):
    return (state * 6_364_136_223_846_793_005 + 1_442_695_040_888_963_407) & ((1 << 64) - 1)


def v8_ratio_stats(ratios):
    median = statistics.median(ratios)
    mean = sum(ratios) / len(ratios)
    variance = sum((ratio - mean) ** 2 for ratio in ratios) / (len(ratios) - 1)
    cv_pct = 0.0 if mean == 0.0 else math.sqrt(variance) / abs(mean) * 100.0
    mad = statistics.median(abs(ratio - median) for ratio in ratios)
    state = 0x7A25_2026_C011_CAFE
    medians = []
    for _ in range(10_000):
        resample = []
        for _ in ratios:
            state = v8_lcg_next(state)
            resample.append(ratios[state % len(ratios)])
        medians.append(statistics.median(resample))
    medians.sort()
    return median, medians[250], medians[9_750], cv_pct, mad


def v8_expected_median_contract(null_ratios, claim_ratios):
    null = v8_ratio_stats(null_ratios)
    claim = v8_ratio_stats(claim_ratios)
    null_radius = max(abs(null[1] - 1.0), abs(null[2] - 1.0))
    decisive_effect = max(2.0 * null_radius, 0.01)
    minimum_gain = 1.0 + decisive_effect
    maximum_regression = 1.0 - decisive_effect
    claim_margin = None if null_radius == 0.0 else abs(claim[0] - 1.0) / null_radius
    verdict = "INCONCLUSIVE"
    if claim[1] > minimum_gain:
        verdict = "FSQLITE_FASTER"
    elif claim[2] < maximum_regression:
        verdict = "FSQLITE_SLOWER"
    return {
        "null_ratio_median": null[0],
        "null_ratio_ci95_low": null[1],
        "null_ratio_ci95_high": null[2],
        "null_ratio_cv_pct": null[3],
        "null_ratio_mad": null[4],
        "claim_ratio_median": claim[0],
        "claim_ratio_ci95_low": claim[1],
        "claim_ratio_ci95_high": claim[2],
        "claim_ratio_cv_pct": claim[3],
        "claim_ratio_mad": claim[4],
        "null_radius": null_radius,
        "min_decidable_gain": minimum_gain,
        "max_decidable_regression": maximum_regression,
        "claim_margin": claim_margin,
        "cv_gate": "never",
        "verdict": verdict,
    }


def v8_validate_median_contract(observed, expected, label):
    observed = v8_require_object(observed, label)
    if set(observed) != set(expected):
        v8_fail(f"{label} fields do not match the v9 median-CI contract")
    for field, expected_value in expected.items():
        observed_value = observed[field]
        if isinstance(expected_value, float):
            if not v8_close(v8_require_number(observed_value, f"{label}.{field}"), expected_value):
                v8_fail(f"{label}.{field} disagrees with paired within-report samples")
        elif observed_value != expected_value:
            v8_fail(f"{label}.{field} disagrees with the v9 median-CI contract")


def v8_validate_report(
    report,
    label,
    profile,
    graph,
    graph_sha256,
    expected_rows,
    expected_iterations,
    *,
    expected_commit=None,
    expected_nonce=None,
    expected_history=None,
    expected_output=None,
):
    if report.get("schema_version") != report_schema_v8:
        v8_fail(f"{label}.schema_version must be {report_schema_v8}; prior schemas are not accepted")
    if report.get("citable") is not False or report.get("measurement_evidence_valid") is not True:
        v8_fail(f"{label} must be valid but explicitly non-citable v9 evidence")
    if report.get("non_citable_reason") != EXPECTED_NON_CITABLE_REASON_V9:
        v8_fail(f"{label}.non_citable_reason does not match the v9 contract")
    if report.get("release_regression_scope") != EXPECTED_RELEASE_SCOPE_V9:
        v8_fail(f"{label}.release_regression_scope does not match the v9 contract")
    if "release_evidence" in report or "release_eligible" in report:
        v8_fail(f"{label} must not smuggle a release claim")
    source_sha = v8_validate_subject_identity(report, label, expected_commit, expected_nonce)
    environment = v8_require_object(
        report.get("comparison_environment"),
        f"{label}.comparison_environment",
        ("build_configuration", "invocation", "measurement_host"),
    )
    build_configuration = v8_validate_build_configuration(
        environment["build_configuration"],
        f"{label}.build_configuration",
        profile,
        graph,
        graph_sha256,
    )
    history_path, output_path = v8_validate_invocation(
        environment["invocation"],
        f"{label}.invocation",
        expected_rows,
        expected_iterations,
        expected_history,
        expected_output,
    )
    host, placement, available_parallelism = v8_validate_host(
        environment["measurement_host"], f"{label}.measurement_host"
    )
    if report.get("workload_shape") != "separate_tables":
        v8_fail(f"{label}.workload_shape must be separate_tables")
    if (
        v8_require_int(report.get("rows_per_thread"), f"{label}.rows_per_thread", positive=True)
        != expected_rows
        or v8_require_int(report.get("iterations"), f"{label}.iterations", positive=True)
        != expected_iterations
    ):
        v8_fail(f"{label} does not use the exact rows/iterations contract")
    transaction = {
        "granularity": "one_row_per_transaction",
        "rows_per_transaction": 1,
        "prepared_statement_scope": "one successfully prepared statement per worker, reused across row transactions; transient preparation failures retry under the shared worker deadline",
        "duplicate_after_ambiguous_commit_policy": "fail_closed; a duplicate is never accepted as proof of exact id+payload",
        "csqlite_retry_unit": CSQLITE_RETRY_UNIT_V9,
        "fsqlite_retry_unit": FSQLITE_RETRY_UNIT_V9,
    }
    v8_require_exact(
        report.get("transaction_contract"), transaction, f"{label}.transaction_contract"
    )
    interpretations = {
        "settings_interpretation": EXPECTED_SETTINGS_INTERPRETATION_V9,
        "accounting_interpretation": EXPECTED_ACCOUNTING_INTERPRETATION_V9,
        "timing_interpretation": EXPECTED_TIMING_INTERPRETATION_V9,
    }
    for field, expected in interpretations.items():
        if report.get(field) != expected:
            v8_fail(f"{label}.{field} does not match the v9 contract")
    pass_gate = v8_require_object(report.get("pass_over_pass_gate"), f"{label}.pass_over_pass_gate")
    if (
        pass_gate.get("schema_version") != "fsqlite-e2e.mt_mvcc_bench.pass_over_pass.v1"
        or pass_gate.get("history_json_path") != history_path
        or not v8_close(v8_require_number(pass_gate.get("threshold_ratio_drop_pct"), f"{label}.pass_over_pass_gate.threshold"), 5.0)
        or pass_gate.get("status") != "disabled_non_citable"
        or not isinstance(pass_gate.get("previous_report_found"), bool)
        or v8_require_int(
            pass_gate.get("comparable_pair_count"),
            f"{label}.pass_over_pass_gate.comparable_pair_count",
        )
        != 0
    ):
        v8_fail(f"{label}.pass_over_pass_gate is not the disabled non-citable receipt")
    v8_require_exact(
        pass_gate.get("regressions"), [], f"{label}.pass_over_pass_gate.regressions"
    )
    if measurement_mode_v8 == "measured" and expected_history is not None:
        if pass_gate["previous_report_found"] or os.path.lexists(history_path):
            v8_fail(f"{label}.pass_over_pass_gate history path must remain absent")

    receipts = v8_require_list(report.get("configuration_receipts"), f"{label}.configuration_receipts")
    rows = v8_require_list(report.get("thread_results"), f"{label}.thread_results")
    if len(receipts) != 3 or len(rows) != 3:
        v8_fail(f"{label} must contain exactly the 1, 8, and 16 writer cells")
    receipts_by_thread = {}
    for index, receipt in enumerate(receipts):
        receipt_label = f"{label}.configuration_receipts[{index}]"
        receipt = v8_require_object(receipt, receipt_label)
        writers = v8_require_int(receipt.get("writers"), f"{receipt_label}.writers", positive=True)
        if writers in receipts_by_thread:
            v8_fail(f"{label} contains duplicate {writers}-writer receipts")
        if (
            receipt.get("status") != "supported"
            or receipt.get("comparison_eligible") is not True
            or receipt.get("measured") is not True
            or v8_require_int(
                receipt.get("available_parallelism"),
                f"{receipt_label}.available_parallelism",
                positive=True,
            )
            != available_parallelism
            or v8_require_int(
                receipt.get("max_supported_writers"),
                f"{receipt_label}.max_supported_writers",
                positive=True,
            )
            != 128
            or v8_require_int(
                receipt.get("wal_autocheckpoint_pages"),
                f"{receipt_label}.wal_autocheckpoint_pages",
                positive=True,
            )
            != 1_000
            or receipt.get("wal_autocheckpoint_overridden") is not False
            or v8_require_int(
                receipt.get("offered_writes_per_sample"),
                f"{receipt_label}.offered_writes_per_sample",
                positive=True,
            )
            != writers * expected_rows
        ):
            v8_fail(f"{receipt_label} does not match the exact shared-deadline v9 contract")
        v8_require_exact(
            receipt.get("retry_policy"),
            v8_expected_retry_policy(writers, expected_rows),
            f"{receipt_label}.retry_policy",
        )
        v8_require_string(receipt.get("reason"), f"{receipt_label}.reason")
        receipts_by_thread[writers] = receipt
    if tuple(sorted(receipts_by_thread)) != REQUIRED_THREADS_V8:
        v8_fail(f"{label} configurations must be exactly {REQUIRED_THREADS_V8}")

    rows_by_thread = {}
    even_order = ["csqlite_null_a", "csqlite_null_b", "csqlite_baseline", "fsqlite_candidate"]
    odd_order = ["fsqlite_candidate", "csqlite_baseline", "csqlite_null_b", "csqlite_null_a"]
    for index, row in enumerate(rows):
        row_label = f"{label}.thread_results[{index}]"
        row = v8_require_object(row, row_label)
        threads = v8_require_int(row.get("threads"), f"{row_label}.threads", positive=True)
        if threads not in receipts_by_thread or threads in rows_by_thread:
            v8_fail(f"{row_label} has no unique matching configuration receipt")
        truth = v8_require_object(row.get("truth"), f"{row_label}.truth")
        v8_require_exact(
            truth.get("configuration"),
            receipts_by_thread[threads],
            f"{row_label}.truth.configuration",
        )
        round_receipts = v8_require_list(truth.get("round_order_receipts"), f"{row_label}.truth.round_order_receipts")
        expected_round_receipts = [
            {"round_index": round_index, "execution_order": even_order if round_index % 2 == 0 else odd_order}
            for round_index in range(expected_iterations)
        ]
        v8_require_exact(
            round_receipts,
            expected_round_receipts,
            f"{row_label}.truth.round_order_receipts",
        )
        offered = threads * expected_rows
        arm_settings = {
            "null_c_a_samples": v8_expected_settings("sqlite_wal_single_writer"),
            "null_c_b_samples": v8_expected_settings("sqlite_wal_single_writer"),
            "sqlite_samples": v8_expected_settings("sqlite_wal_single_writer"),
            "fsqlite_samples": v8_expected_settings("fsqlite_mvcc_on"),
        }
        arms = {}
        for arm, settings in arm_settings.items():
            samples = v8_require_list(truth.get(arm), f"{row_label}.truth.{arm}")
            if len(samples) != expected_iterations:
                v8_fail(f"{row_label}.truth.{arm} must contain exactly {expected_iterations} samples")
            arms[arm] = [
                v8_validate_sample(sample, f"{row_label}.truth.{arm}[{sample_index}]", offered, settings)
                for sample_index, sample in enumerate(samples)
            ]
        null_ratios = [
            right["wps"] / left["wps"]
            for left, right in zip(arms["null_c_a_samples"], arms["null_c_b_samples"])
        ]
        claim_ratios = [
            candidate["wps"] / baseline["wps"]
            for baseline, candidate in zip(arms["sqlite_samples"], arms["fsqlite_samples"])
        ]
        v8_validate_median_contract(
            row.get("median_ci_contract"),
            v8_expected_median_contract(null_ratios, claim_ratios),
            f"{row_label}.median_ci_contract",
        )
        fsqlite_wps = [sample["wps"] for sample in arms["fsqlite_samples"]]
        sqlite_wps = [sample["wps"] for sample in arms["sqlite_samples"]]
        fsqlite_ms = [sample["elapsed_ms"] for sample in arms["fsqlite_samples"]]
        sqlite_ms = [sample["elapsed_ms"] for sample in arms["sqlite_samples"]]
        for prefix, wps_values, ms_values in (("fsqlite", fsqlite_wps, fsqlite_ms), ("sqlite", sqlite_wps, sqlite_ms)):
            for suffix, quantile in (("p50", 0.50), ("p95", 0.95), ("p99", 0.99)):
                for metric, values in (("wps", wps_values), ("ms", ms_values)):
                    field = f"{prefix}_{metric}_{suffix}"
                    if not v8_close(v8_require_number(row.get(field), f"{row_label}.{field}", positive=True), v8_percentile(values, quantile)):
                        v8_fail(f"{row_label}.{field} disagrees with raw samples")
        ratio_median = statistics.median(claim_ratios)
        if not v8_close(v8_require_number(row.get("throughput_ratio"), f"{row_label}.throughput_ratio", positive=True), ratio_median):
            v8_fail(f"{row_label}.throughput_ratio disagrees with paired within-report samples")
        expected_time_ratio = v8_percentile(fsqlite_ms, 0.50) / v8_percentile(sqlite_ms, 0.50)
        if not v8_close(v8_require_number(row.get("time_ratio"), f"{row_label}.time_ratio", positive=True), expected_time_ratio):
            v8_fail(f"{row_label}.time_ratio disagrees with raw samples")
        if (
            v8_require_int(row.get("fsqlite_failed_rows"), f"{row_label}.fsqlite_failed_rows")
            != 0
            or v8_require_int(row.get("sqlite_failed_rows"), f"{row_label}.sqlite_failed_rows")
            != 0
        ):
            v8_fail(f"{row_label} reports failed rows")
        rows_by_thread[threads] = {"fsqlite_wps": fsqlite_wps, "ratio_median": ratio_median}
    if tuple(sorted(rows_by_thread)) != REQUIRED_THREADS_V8:
        v8_fail(f"{label} thread results must be exactly {REQUIRED_THREADS_V8}")
    semantic_contract = {
        "transaction_contract": transaction,
        "configuration_receipts": {
            str(thread): {
                key: receipts_by_thread[thread][key]
                for key in (
                    "available_parallelism",
                    "max_supported_writers",
                    "wal_autocheckpoint_pages",
                    "wal_autocheckpoint_overridden",
                    "offered_writes_per_sample",
                    "retry_policy",
                )
            }
            for thread in REQUIRED_THREADS_V8
        },
        **interpretations,
    }
    cross_profile_build = {
        key: build_configuration[key]
        for key in (
            "cargo_profile",
            "target",
            "build_host",
            "enabled_features",
            "rustflags",
            "profile_overrides_hex",
            "native_build_overrides_hex",
            "rustc_version_verbose",
            "cargo_version",
            "resolved_dependency_feature_graph_sha256",
            "resolved_dependency_feature_graph_limitation",
        )
    }
    cross_revision_build = {
        key: build_configuration[key]
        for key in (
            "cargo_profile",
            "selected_profile",
            "profile_label",
            "opt_level",
            "debug",
            "target",
            "build_host",
            "enabled_features",
            "rustflags",
            "profile_overrides_hex",
            "native_build_overrides_hex",
            "rustc_version_verbose",
            "cargo_version",
        )
    }
    # Cargo reports the release profile family for both release and release-perf.
    return {
        "source_sha": source_sha,
        "history_path": history_path,
        "output_path": output_path,
        "rows": rows_by_thread,
        "contract": semantic_contract,
        "measurement_comparability": {
            # Historical engine revisions may legitimately resolve a different
            # dependency graph. Each graph is attested independently; all other
            # build/profile/host inputs must still match exactly.
            "build_configuration": cross_revision_build,
            "static_host": host,
            "stable_process_placement": placement,
        },
        "cross_profile_comparability": {
            "build": cross_profile_build,
            "static_host": host,
            "stable_process_placement": placement,
        },
    }


def v8_arithmetic_mean(values):
    return sum(values) / len(values)


def v8_bootstrap_relative_delta(baseline_values, current_values, seed, statistic):
    state = seed
    deltas = []
    for _ in range(BOOTSTRAP_REPETITIONS_V8):
        baseline_sample = []
        current_sample = []
        for _ in baseline_values:
            state = v8_lcg_next(state)
            baseline_sample.append(baseline_values[state % len(baseline_values)])
        for _ in current_values:
            state = v8_lcg_next(state)
            current_sample.append(current_values[state % len(current_values)])
        baseline_statistic = statistic(baseline_sample)
        current_statistic = statistic(current_sample)
        if baseline_statistic <= 0.0:
            v8_fail("independent bootstrap encountered non-positive baseline throughput")
        deltas.append(current_statistic / baseline_statistic - 1.0)
    deltas.sort()
    return deltas[BOOTSTRAP_REPETITIONS_V8 * 25 // 1_000], deltas[min(BOOTSTRAP_REPETITIONS_V8 * 975 // 1_000, BOOTSTRAP_REPETITIONS_V8 - 1)]


def v8_bootstrap_scaling_delta(baseline_numerator, baseline_denominator, current_numerator, current_denominator, seed):
    state = seed
    deltas = []
    arrays = (baseline_numerator, baseline_denominator, current_numerator, current_denominator)
    for _ in range(BOOTSTRAP_REPETITIONS_V8):
        resamples = []
        for values in arrays:
            sample = []
            for _ in values:
                state = v8_lcg_next(state)
                sample.append(values[state % len(values)])
            resamples.append(sample)
        baseline_scaling = statistics.median(resamples[0]) / statistics.median(resamples[1])
        current_scaling = statistics.median(resamples[2]) / statistics.median(resamples[3])
        if baseline_scaling <= 0.0:
            v8_fail("independent scaling bootstrap encountered non-positive baseline scaling")
        deltas.append(current_scaling / baseline_scaling - 1.0)
    deltas.sort()
    return deltas[BOOTSTRAP_REPETITIONS_V8 * 25 // 1_000], deltas[min(BOOTSTRAP_REPETITIONS_V8 * 975 // 1_000, BOOTSTRAP_REPETITIONS_V8 - 1)]


def v8_classify(ci_low, ci_high, maximum_drop):
    allowed_delta = -maximum_drop
    if ci_high < allowed_delta and not v8_close(ci_high, allowed_delta):
        return "regression"
    if ci_low > allowed_delta or v8_close(ci_low, allowed_delta):
        return "passed"
    return "inconclusive"


def v8_fsync_directory(path):
    descriptor = None
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        os.fsync(descriptor)
    except OSError as error:
        v8_fail(f"cannot fsync artifact directory {path}: {error}")
    finally:
        if descriptor is not None:
            os.close(descriptor)


def v8_ensure_directory(path):
    directory = Path(path)
    missing = []
    cursor = directory
    while not cursor.exists():
        missing.append(cursor)
        parent = cursor.parent
        if parent == cursor:
            v8_fail(f"cannot find existing parent for artifact directory {directory}")
        cursor = parent
    if not cursor.is_dir():
        v8_fail(f"artifact directory ancestor is not a directory: {cursor}")
    for item in reversed(missing):
        try:
            item.mkdir()
        except FileExistsError:
            if not item.is_dir():
                v8_fail(f"artifact directory path is not a directory: {item}")
        except OSError as error:
            v8_fail(f"cannot create artifact directory {item}: {error}")
        v8_fsync_directory(item.parent)


def v8_write_json(path, value, *, exclusive=True):
    destination = Path(path)
    try:
        v8_ensure_directory(destination.parent)
        with destination.open("xb" if exclusive else "wb") as handle:
            handle.write(v8_canonical_pretty_bytes(value))
            handle.flush()
            os.fsync(handle.fileno())
        v8_fsync_directory(destination.parent)
    except OSError as error:
        v8_fail(f"cannot {'create' if exclusive else 'write'} JSON artifact {path}: {error}")


def v8_publish_baseline(path, value, publication_run_id):
    destination = Path(path)
    payload = v8_canonical_pretty_bytes(value)
    digest = hashlib.sha256(payload).hexdigest()
    versions = destination.parent / "versions"
    version = versions / f"{digest}.json"
    result_scope = hashlib.sha256(str(Path(result_path_v8).resolve()).encode("utf-8")).hexdigest()[:16]
    candidate_dir = versions / "candidates" / f"{publication_run_id}.{result_scope}"
    candidate = candidate_dir / "baseline.json"
    try:
        v8_ensure_directory(versions / "candidates")
        candidate_dir.mkdir()
        v8_fsync_directory(candidate_dir.parent)
        with candidate.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        v8_fsync_directory(candidate_dir)
        if os.path.lexists(version):
            if version.is_symlink() or not version.is_file() or version.read_bytes() != payload:
                v8_fail(f"baseline version digest collision or corruption: {version}")
        else:
            try:
                os.link(candidate, version, follow_symlinks=False)
            except FileExistsError:
                if version.is_symlink() or not version.is_file() or version.read_bytes() != payload:
                    v8_fail(f"baseline version digest collision or corruption: {version}")
        v8_fsync_directory(versions)
        os.link(version, destination, follow_symlinks=False)
        v8_fsync_directory(destination.parent)
    except V8EvidenceError:
        raise
    except OSError as error:
        v8_fail(f"cannot atomically publish baseline {path}: {error}")
    return str(version), str(candidate), digest


def v8_validate_profile_pair(validated, label):
    first = validated["release"]
    second = validated["release-perf"]
    if first["source_sha"] != second["source_sha"]:
        v8_fail(f"{label} profiles were not built from the same source commit")
    if first["contract"] != second["contract"]:
        v8_fail(f"{label} profiles use different benchmark contracts")
    if first["cross_profile_comparability"] != second["cross_profile_comparability"]:
        v8_fail(f"{label} profiles do not share one comparable host/build environment")


def v8_report_digest(report):
    return hashlib.sha256(v8_canonical_compact_bytes(report)).hexdigest()


def v8_run():
    expected_rows = int(expected_rows_raw_v8)
    expected_iterations = int(expected_iterations_raw_v8)
    capture = capture_raw_v8 == "true"
    maximum_drops = {
        1: v8_require_fraction(max_drop_1t_raw_v8, "1t maximum drop"),
        8: v8_require_fraction(max_drop_8t_raw_v8, "8t maximum drop"),
        16: v8_require_fraction(max_drop_16t_raw_v8, "16t maximum drop"),
    }
    scaling_drops = {
        (8, 1): v8_require_fraction(max_scaling_drop_8_over_1_raw_v8, "8/1 scaling maximum drop"),
        (16, 8): v8_require_fraction(max_scaling_drop_16_over_8_raw_v8, "16/8 scaling maximum drop"),
    }
    margin_policy = {
        "1t_absolute": {
            "maximum_drop_fraction": maximum_drops[1],
            "source": "tracked bd-zywqc.2 contract",
        },
        "8t_absolute": {
            "maximum_drop_fraction": maximum_drops[8],
            "source": "tracked bd-zywqc.2 contract",
        },
        "16t_absolute": {
            "maximum_drop_fraction": maximum_drops[16],
            "source": "explicit caller input; acceptance-owner value remains unresolved",
        },
        "8_over_1_scaling": {
            "maximum_drop_fraction": scaling_drops[(8, 1)],
            "source": "explicit caller input; acceptance-owner value remains unresolved",
        },
        "16_over_8_scaling": {
            "maximum_drop_fraction": scaling_drops[(16, 8)],
            "source": "explicit caller input; acceptance-owner value remains unresolved",
        },
    }
    graph, graph_sha256 = v8_read_graph(graph_path_v8)
    provenance = v8_provenance(expected_rows, graph["target"])
    paths = {
        "release": current_release_path_v8,
        "release-perf": current_release_perf_path_v8,
    }
    reports = {profile: v8_read_json(path, f"current {profile} report") for profile, path in paths.items()}
    current_digests = {profile: v8_report_digest(report) for profile, report in reports.items()}
    current_validated = {}
    for profile in REQUIRED_PROFILES_V8:
        required_nonce = f"{run_id_v8}.{profile}" if measurement_mode_v8 == "measured" else None
        required_history = (
            str(Path(paths[profile]).parent / f"disposable_history.{profile}.json")
            if measurement_mode_v8 == "measured"
            else None
        )
        current_validated[profile] = v8_validate_report(
            reports[profile],
            f"current {profile} report",
            profile,
            graph,
            graph_sha256,
            expected_rows,
            expected_iterations,
            expected_commit=intended_commit_v8,
            expected_nonce=required_nonce,
            expected_history=required_history,
            expected_output=paths[profile] if measurement_mode_v8 == "measured" else None,
        )
    v8_validate_profile_pair(current_validated, "current")

    if capture:
        baseline = {
            "schema_version": baseline_schema_v8,
            "bead_id": "bd-zywqc.2",
            "analyzer_commit": intended_commit_v8,
            "capture_run_id": run_id_v8,
            "measurement_mode": measurement_mode_v8,
            "comparison_design": "independent_two_sample_bootstrap; no historical pairing",
            "unresolved_release_coverage": UNRESOLVED_RELEASE_COVERAGE_V8,
            "diagnostic_margin_policy": margin_policy,
            "provenance": provenance,
            "dependency_feature_graph_sha256": graph_sha256,
            "dependency_feature_graph": graph,
            "profiles": {
                profile: {
                    "report_sha256": current_digests[profile],
                    "report_history_json_path": current_validated[profile]["history_path"],
                    "report_output_path": current_validated[profile]["output_path"],
                    "report": reports[profile],
                }
                for profile in REQUIRED_PROFILES_V8
            },
            "release_evidence": False,
            "release_eligible": False,
        }
        version, candidate, envelope_digest = v8_publish_baseline(
            baseline_path_v8, baseline, run_id_v8
        )
        result = {
            "schema_version": gate_schema_v8,
            "bead_id": "bd-zywqc.2",
            "mode": "capture_baseline",
            "measurement_mode": measurement_mode_v8,
            "comparison_design": "independent_two_sample_bootstrap; no historical pairing",
            "unresolved_release_coverage": UNRESOLVED_RELEASE_COVERAGE_V8,
            "diagnostic_margin_policy": margin_policy,
            "analyzer_commit": intended_commit_v8,
            "current_report_sha256": current_digests,
            "dependency_feature_graph_sha256": graph_sha256,
            "baseline_path": baseline_path_v8,
            "baseline_version_path": version,
            "baseline_candidate_path": candidate,
            "baseline_envelope_sha256": envelope_digest,
            "iterations": expected_iterations,
            "verdict": "baseline_captured",
            "release_evidence": False,
            "release_eligible": False,
        }
        v8_write_json(result_path_v8, result)
        print(f"[bd-zywqc.2] CAPTURED validated dual-profile v9 baseline: {baseline_path_v8}")
        return 0

    destination = Path(baseline_path_v8)
    if destination.is_symlink() or not destination.is_file():
        v8_fail("baseline latest path must be a regular file, not a symbolic link")
    baseline = v8_read_json(baseline_path_v8, "baseline envelope")
    if baseline.get("schema_version") != baseline_schema_v8:
        v8_fail(f"baseline must use {baseline_schema_v8}; recapture explicitly")
    baseline_payload = v8_canonical_pretty_bytes(baseline)
    envelope_digest = hashlib.sha256(baseline_payload).hexdigest()
    version = destination.parent / "versions" / f"{envelope_digest}.json"
    if version.is_symlink() or not version.is_file():
        v8_fail(f"baseline envelope has no matching regular content-addressed version: {version}")
    try:
        if not os.path.samefile(destination, version):
            v8_fail("baseline latest path is not the matching content-addressed version")
        if version.read_bytes() != baseline_payload:
            v8_fail("baseline envelope bytes are not canonical content-addressed JSON")
    except OSError as error:
        v8_fail(f"cannot verify content-addressed baseline identity: {error}")
    if (
        baseline.get("bead_id") != "bd-zywqc.2"
        or baseline.get("measurement_mode") != measurement_mode_v8
        or baseline.get("comparison_design") != "independent_two_sample_bootstrap; no historical pairing"
        or baseline.get("unresolved_release_coverage") != UNRESOLVED_RELEASE_COVERAGE_V8
        or baseline.get("release_evidence") is not False
        or baseline.get("release_eligible") is not False
    ):
        v8_fail("baseline envelope is not the required diagnostic-only independent-sample envelope")
    capture_run_id = v8_require_string(baseline.get("capture_run_id"), "baseline.capture_run_id")
    baseline_analyzer_commit = v8_require_git_sha(
        baseline.get("analyzer_commit"), "baseline.analyzer_commit"
    )
    v8_require_exact(
        baseline.get("provenance"), provenance, "baseline.provenance"
    )
    baseline_graph = v8_validate_graph(
        baseline.get("dependency_feature_graph"),
        "baseline dependency/feature graph",
    )
    baseline_graph_sha256 = v8_require_sha256(
        baseline.get("dependency_feature_graph_sha256"),
        "baseline.dependency_feature_graph_sha256",
    )
    if (
        hashlib.sha256(v8_canonical_compact_bytes(baseline_graph)).hexdigest()
        != baseline_graph_sha256
    ):
        v8_fail("baseline dependency/feature graph digest does not match its retained graph")
    baseline_profiles = v8_require_object(baseline.get("profiles"), "baseline.profiles")
    if set(baseline_profiles) != set(REQUIRED_PROFILES_V8):
        v8_fail("baseline envelope must contain exactly release and release-perf reports")
    baseline_validated = {}
    baseline_digests = {}
    for profile in REQUIRED_PROFILES_V8:
        envelope_profile = v8_require_object(
            baseline_profiles[profile],
            f"baseline.profiles.{profile}",
            ("report_sha256", "report_history_json_path", "report_output_path", "report"),
        )
        report = v8_require_object(envelope_profile["report"], f"baseline {profile} report")
        digest = v8_report_digest(report)
        if envelope_profile["report_sha256"] != digest:
            v8_fail(f"baseline {profile} report digest does not match its envelope")
        expected_nonce = f"{capture_run_id}.{profile}" if measurement_mode_v8 == "measured" else None
        expected_history = v8_require_string(
            envelope_profile["report_history_json_path"],
            f"baseline.profiles.{profile}.report_history_json_path",
        )
        expected_output = v8_require_string(
            envelope_profile["report_output_path"],
            f"baseline.profiles.{profile}.report_output_path",
        )
        baseline_validated[profile] = v8_validate_report(
            report,
            f"baseline {profile} report",
            profile,
            baseline_graph,
            baseline_graph_sha256,
            expected_rows,
            expected_iterations,
            expected_commit=baseline_analyzer_commit,
            expected_nonce=expected_nonce,
            expected_history=expected_history,
            expected_output=expected_output,
        )
        baseline_digests[profile] = digest
    v8_validate_profile_pair(baseline_validated, "baseline")

    for profile in REQUIRED_PROFILES_V8:
        if baseline_validated[profile]["contract"] != current_validated[profile]["contract"]:
            v8_fail(f"baseline and current {profile} reports use incompatible v9 contracts")
        if baseline_validated[profile]["measurement_comparability"] != current_validated[profile]["measurement_comparability"]:
            v8_fail(f"baseline and current {profile} reports use incompatible measurement environments")

    comparisons = []
    scaling_comparisons = []
    guard_status = "passed"
    for profile_index, profile in enumerate(REQUIRED_PROFILES_V8):
        for threads in REQUIRED_THREADS_V8:
            baseline_row = baseline_validated[profile]["rows"][threads]
            current_row = current_validated[profile]["rows"][threads]
            statistic = v8_arithmetic_mean if threads == 1 else statistics.median
            metric = "fsqlite_wps_arithmetic_mean" if threads == 1 else "fsqlite_wps_median"
            baseline_statistic = statistic(baseline_row["fsqlite_wps"])
            current_statistic = statistic(current_row["fsqlite_wps"])
            ci_low, ci_high = v8_bootstrap_relative_delta(
                baseline_row["fsqlite_wps"],
                current_row["fsqlite_wps"],
                0xF5_71_17_E0_2026 ^ (profile_index << 12) ^ threads,
                statistic,
            )
            status = v8_classify(ci_low, ci_high, maximum_drops[threads])
            if status != "passed":
                guard_status = "failed"
            comparisons.append(
                {
                    "profile": profile,
                    "threads": threads,
                    "metric": metric,
                    "sampling_design": "independent_two_sample_bootstrap",
                    "baseline_fsqlite_wps": baseline_statistic,
                    "current_fsqlite_wps": current_statistic,
                    "relative_delta_pct": (current_statistic / baseline_statistic - 1.0) * 100.0,
                    "bootstrap_ci95_delta_pct": [ci_low * 100.0, ci_high * 100.0],
                    "max_allowed_drop_pct": maximum_drops[threads] * 100.0,
                    "baseline_fsqlite_to_csqlite_ratio_median_within_report_paired_diagnostic": baseline_row["ratio_median"],
                    "current_fsqlite_to_csqlite_ratio_median_within_report_paired_diagnostic": current_row["ratio_median"],
                    "status": status,
                }
            )
        for numerator, denominator in ((8, 1), (16, 8)):
            baseline_numerator = baseline_validated[profile]["rows"][numerator]["fsqlite_wps"]
            baseline_denominator = baseline_validated[profile]["rows"][denominator]["fsqlite_wps"]
            current_numerator = current_validated[profile]["rows"][numerator]["fsqlite_wps"]
            current_denominator = current_validated[profile]["rows"][denominator]["fsqlite_wps"]
            baseline_scaling = statistics.median(baseline_numerator) / statistics.median(baseline_denominator)
            current_scaling = statistics.median(current_numerator) / statistics.median(current_denominator)
            ci_low, ci_high = v8_bootstrap_scaling_delta(
                baseline_numerator,
                baseline_denominator,
                current_numerator,
                current_denominator,
                0x5CA1_1A6_2026 ^ (profile_index << 12) ^ (numerator << 4) ^ denominator,
            )
            maximum_drop = scaling_drops[(numerator, denominator)]
            status = v8_classify(ci_low, ci_high, maximum_drop)
            if status != "passed":
                guard_status = "failed"
            scaling_comparisons.append(
                {
                    "profile": profile,
                    "scaling": f"{numerator}/{denominator}",
                    "metric": "ratio_of_independently_resampled_fsqlite_wps_medians",
                    "sampling_design": "independent_four_sample_bootstrap",
                    "baseline_scaling_ratio": baseline_scaling,
                    "current_scaling_ratio": current_scaling,
                    "relative_delta_pct": (current_scaling / baseline_scaling - 1.0) * 100.0,
                    "bootstrap_ci95_delta_pct": [ci_low * 100.0, ci_high * 100.0],
                    "max_allowed_drop_pct": maximum_drop * 100.0,
                    "status": status,
                }
            )
    result = {
        "schema_version": gate_schema_v8,
        "bead_id": "bd-zywqc.2",
        "mode": "regression_guard",
        "measurement_mode": measurement_mode_v8,
        "comparison_design": "independent historical samples; within-report engine arms only are paired",
        "unresolved_release_coverage": UNRESOLVED_RELEASE_COVERAGE_V8,
        "diagnostic_margin_policy": margin_policy,
        "analyzer_commit": intended_commit_v8,
        "baseline_analyzer_commit": baseline_analyzer_commit,
        "current_report_sha256": current_digests,
        "baseline_report_sha256": baseline_digests,
        "current_dependency_feature_graph_sha256": graph_sha256,
        "baseline_dependency_feature_graph_sha256": baseline_graph_sha256,
        "baseline_envelope_sha256": envelope_digest,
        "baseline_path": baseline_path_v8,
        "baseline_version_path": str(version),
        "iterations": expected_iterations,
        "bootstrap_repetitions": BOOTSTRAP_REPETITIONS_V8,
        "guard_status": guard_status,
        "verdict": "diagnostic_only" if guard_status == "passed" else "failed",
        "release_evidence": False,
        "release_eligible": False,
        "performance_admission": {
            "schema_version": "fsqlite.performance_release_admission.v2",
            "status": "blocked_missing_authoritative_performance_policy",
            "release_authorized": False,
            "blockers": ["missing_authoritative_performance_policy"],
            "rationale": "No authoritative acceptance policy artifact is available. The v9 analyzer is diagnostic-only and cannot authorize a release. A v2 authorization requires an immutable B/T pack with non-v9 reports, policy, raw evidence, calibration, and sensitivity receipts.",
            "admission_pack": None,
        },
        "absolute_comparisons": comparisons,
        "scaling_comparisons": scaling_comparisons,
    }
    v8_write_json(result_path_v8, result)
    for comparison in comparisons:
        print(
            "  [{status}] {profile} {threads}t {metric}: {baseline:.2f} -> {current:.2f} wps; "
            "independent CI {low:+.2f}%..{high:+.2f}%".format(
                status=comparison["status"].upper(),
                profile=comparison["profile"],
                threads=comparison["threads"],
                metric=comparison["metric"],
                baseline=comparison["baseline_fsqlite_wps"],
                current=comparison["current_fsqlite_wps"],
                low=comparison["bootstrap_ci95_delta_pct"][0],
                high=comparison["bootstrap_ci95_delta_pct"][1],
            )
        )
    for comparison in scaling_comparisons:
        print(
            "  [{status}] {profile} scaling {scaling}: {baseline:.3f} -> {current:.3f}; "
            "independent CI {low:+.2f}%..{high:+.2f}%".format(
                status=comparison["status"].upper(),
                profile=comparison["profile"],
                scaling=comparison["scaling"],
                baseline=comparison["baseline_scaling_ratio"],
                current=comparison["current_scaling_ratio"],
                low=comparison["bootstrap_ci95_delta_pct"][0],
                high=comparison["bootstrap_ci95_delta_pct"][1],
            )
        )
    if guard_status != "passed":
        print("[bd-zywqc.2] FAILED: at least one profile/cell/scaling comparison regressed or was inconclusive", file=sys.stderr)
        return 1
    print("[bd-zywqc.2] PASSED diagnostic-only dual-profile v9 regression guard")
    return 0


try:
    sys.exit(v8_run())
except V8EvidenceError as error:
    invalid = {
        "schema_version": gate_schema_v8,
        "bead_id": "bd-zywqc.2",
        "mode": "capture_baseline" if capture_raw_v8 == "true" else "regression_guard",
        "measurement_mode": measurement_mode_v8,
        "analyzer_commit": intended_commit_v8,
        "unresolved_release_coverage": UNRESOLVED_RELEASE_COVERAGE_V8,
        "verdict": "invalid_evidence",
        "release_evidence": False,
        "release_eligible": False,
        "error": str(error),
    }
    try:
        v8_write_json(result_path_v8, invalid)
    except V8EvidenceError as result_error:
        print(f"[bd-zywqc.2] additionally could not write invalid result: {result_error}", file=sys.stderr)
    print(f"[bd-zywqc.2] INVALID EVIDENCE: {error}", file=sys.stderr)
    sys.exit(2)

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
    "v7 binds the running executable, build/runtime source identity, Cargo.lock, invocation, "
    "toolchain, and measurement host to this same-invocation comparison, but bd-uh1fv still "
    "requires external watchdog, sanitized environment, matched retry/deadline semantics, a "
    "build-attested resolved dependency/feature-graph digest, counterbalanced topology receipts, "
    "immutable manifest, retained baseline history, and independent verification."
)
expected_release_regression_scope = (
    "Narrow same-process, same-host F/C writer-throughput comparison for only the requested "
    "mt-mvcc-bench workload/configurations; this report does not cover the shipped release "
    "profile, other workloads or platforms, long-term baseline retention, independent "
    "reproduction, or overall release eligibility."
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


def require_known_receipt_value(value, label):
    value = require_nonempty_string(value, label)
    normalized = value.strip()
    if not normalized or normalized == "unknown" or normalized.startswith("unknown:"):
        fail(f"{label} must be known")
    return value


def require_object(value, label, required_fields=()):
    if not isinstance(value, dict):
        fail(f"{label} must be an object")
    for field in required_fields:
        if field not in value:
            fail(f"{label}.{field} must be present")
    return value


def require_list(value, label):
    if not isinstance(value, list):
        fail(f"{label} must be an array")
    return value


def require_lower_hex(value, label, *, exact_bytes=None):
    value = require_nonempty_string(value, label)
    if (
        len(value) % 2 != 0
        or any(character not in "0123456789abcdef" for character in value)
        or (exact_bytes is not None and len(value) != exact_bytes * 2)
    ):
        expected = (
            "lowercase hexadecimal"
            if exact_bytes is None
            else f"exactly {exact_bytes} bytes of lowercase hexadecimal"
        )
        fail(f"{label} must be {expected}")
    return value


def validate_complete_file_snapshot(value, label):
    snapshot = require_object(
        value,
        label,
        (
            "sha256",
            "bytes_read",
            "metadata_size_bytes",
            "unix_device",
            "unix_inode",
            "error",
        ),
    )
    if snapshot["error"] is not None:
        fail(f"{label}.error must be null for a complete snapshot")
    sha256 = require_lower_hex(snapshot["sha256"], f"{label}.sha256", exact_bytes=32)
    bytes_read = require_int(snapshot["bytes_read"], f"{label}.bytes_read", positive=True)
    metadata_size = require_int(
        snapshot["metadata_size_bytes"],
        f"{label}.metadata_size_bytes",
        positive=True,
    )
    if metadata_size != bytes_read:
        fail(f"{label} byte count disagrees with file metadata")
    unix_device = require_int(snapshot["unix_device"], f"{label}.unix_device")
    unix_inode = require_int(snapshot["unix_inode"], f"{label}.unix_inode", positive=True)
    if unix_device < 0:
        fail(f"{label}.unix_device must be non-negative")
    return {
        "sha256": sha256,
        "bytes_read": bytes_read,
        "metadata_size_bytes": metadata_size,
        "unix_device": unix_device,
        "unix_inode": unix_inode,
    }


def validate_stable_file_snapshots(before, after, label):
    before_identity = validate_complete_file_snapshot(before, f"{label}.before_measurement")
    after_identity = validate_complete_file_snapshot(after, f"{label}.after_measurement")
    if before_identity != after_identity:
        fail(f"{label} changed during measurement")
    return before_identity


def validate_hex_blob(value, label):
    if not isinstance(value, str):
        fail(f"{label} must be a hexadecimal string")
    if len(value) % 2 != 0 or any(
        character not in "0123456789abcdef" for character in value
    ):
        fail(f"{label} must be lowercase hexadecimal")
    return bytes.fromhex(value)


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
        fail(f"{label} fields do not match the v7 median-CI contract")
    for field, expected_value in expected.items():
        observed_value = observed[field]
        if isinstance(expected_value, float):
            observed_number = require_number(observed_value, f"{label}.{field}")
            if not close_enough(observed_number, expected_value):
                fail(f"{label}.{field} disagrees with raw paired samples")
        elif observed_value != expected_value:
            fail(f"{label}.{field} disagrees with the v7 median-CI contract")


def validate_report(
    report,
    label,
    required_history_path=None,
    required_build_nonce=None,
):
    if report.get("schema_version") != report_schema:
        fail(f"{label}.schema_version must be {report_schema}")
    if report.get("citable") is not False:
        fail(f"{label}.citable must be false for the diagnostic-only v7 schema")
    if report.get("measurement_evidence_valid") is not True:
        fail(f"{label}.measurement_evidence_valid must be true")
    if report.get("non_citable_reason") != expected_non_citable_reason:
        fail(f"{label}.non_citable_reason does not match the v7 contract")
    if report.get("release_regression_scope") != expected_release_regression_scope:
        fail(f"{label}.release_regression_scope does not match the v7 contract")
    if "release_eligible" in report or "release_evidence" in report:
        fail(f"{label} must not claim release eligibility or release evidence")

    subject_identity = require_object(
        report.get("subject_identity"),
        f"{label}.subject_identity",
        ("executable", "build_source", "runtime_source", "cargo_lock"),
    )
    executable = require_object(
        subject_identity["executable"],
        f"{label}.subject_identity.executable",
        (
            "current_exe_path",
            "canonical_path",
            "path_resolution_error",
            "process_id",
            "before_measurement",
            "after_measurement",
            "unchanged_during_measurement",
        ),
    )
    require_known_receipt_value(
        executable["current_exe_path"],
        f"{label}.subject_identity.executable.current_exe_path",
    )
    require_known_receipt_value(
        executable["canonical_path"],
        f"{label}.subject_identity.executable.canonical_path",
    )
    if executable["path_resolution_error"] is not None:
        fail(f"{label}.subject_identity.executable.path_resolution_error must be null")
    require_int(
        executable["process_id"],
        f"{label}.subject_identity.executable.process_id",
        positive=True,
    )
    validate_stable_file_snapshots(
        executable["before_measurement"],
        executable["after_measurement"],
        f"{label}.subject_identity.executable",
    )
    if executable["unchanged_during_measurement"] is not True:
        fail(
            f"{label}.subject_identity.executable.unchanged_during_measurement "
            "must be true"
        )

    build_source = require_object(
        subject_identity["build_source"],
        f"{label}.subject_identity.build_source",
        (
            "workspace_root",
            "git_sha",
            "git_branch",
            "git_tree_state",
            "build_nonce",
            "build_input_tracking",
        ),
    )
    for field in ("workspace_root", "git_sha", "git_branch"):
        require_known_receipt_value(
            build_source[field], f"{label}.subject_identity.build_source.{field}"
        )
    if build_source["git_tree_state"] != "clean":
        fail(f"{label}.subject_identity.build_source.git_tree_state must be clean")
    build_nonce = require_known_receipt_value(
        build_source["build_nonce"],
        f"{label}.subject_identity.build_source.build_nonce",
    )
    if required_build_nonce is not None and build_nonce != required_build_nonce:
        fail(
            f"{label}.subject_identity.build_source.build_nonce does not match its measured run"
        )
    if build_source["build_input_tracking"] != "complete":
        fail(
            f"{label}.subject_identity.build_source.build_input_tracking must be complete"
        )

    runtime_source = require_object(
        subject_identity["runtime_source"],
        f"{label}.subject_identity.runtime_source",
        (
            "before_measurement",
            "after_measurement",
            "same_clean_git_identity_at_capture_points",
            "stability_limitation",
        ),
    )
    runtime_canonical_roots = []
    runtime_branches = []
    for field in ("before_measurement", "after_measurement"):
        runtime_label = f"{label}.subject_identity.runtime_source.{field}"
        runtime_capture = require_object(
            runtime_source[field],
            runtime_label,
            (
                "workspace_root",
                "canonical_workspace_root",
                "git_sha",
                "git_branch",
                "git_tree_state",
                "matches_build_git_sha",
                "discovery_errors",
            ),
        )
        if (
            require_known_receipt_value(
                runtime_capture["workspace_root"], f"{runtime_label}.workspace_root"
            )
            != build_source["workspace_root"]
        ):
            fail(f"{runtime_label}.workspace_root does not match the embedded build")
        runtime_canonical_roots.append(
            require_known_receipt_value(
                runtime_capture["canonical_workspace_root"],
                f"{runtime_label}.canonical_workspace_root",
            )
        )
        if runtime_canonical_roots[-1] != build_source["workspace_root"]:
            fail(
                f"{runtime_label}.canonical_workspace_root does not match the embedded build"
            )
        runtime_git_sha = require_known_receipt_value(
            runtime_capture["git_sha"], f"{runtime_label}.git_sha"
        )
        runtime_branches.append(
            require_known_receipt_value(
                runtime_capture["git_branch"], f"{runtime_label}.git_branch"
            )
        )
        if runtime_git_sha != build_source["git_sha"]:
            fail(f"{runtime_label}.git_sha does not match the embedded build")
        if runtime_capture["git_tree_state"] != "clean":
            fail(f"{runtime_label}.git_tree_state must be clean")
        if runtime_capture["matches_build_git_sha"] is not True:
            fail(f"{runtime_label}.matches_build_git_sha must be true")
        discovery_errors = require_list(
            runtime_capture["discovery_errors"],
            f"{runtime_label}.discovery_errors",
        )
        if discovery_errors:
            fail(f"{runtime_label}.discovery_errors must be empty")
    if runtime_canonical_roots[0] != runtime_canonical_roots[1]:
        fail(f"{label}.subject_identity.runtime_source workspace changed during measurement")
    if runtime_branches[0] != runtime_branches[1]:
        fail(f"{label}.subject_identity.runtime_source branch changed during measurement")
    if runtime_source["same_clean_git_identity_at_capture_points"] is not True:
        fail(
            f"{label}.subject_identity.runtime_source."
            "same_clean_git_identity_at_capture_points must be true"
        )
    require_known_receipt_value(
        runtime_source["stability_limitation"],
        f"{label}.subject_identity.runtime_source.stability_limitation",
    )

    cargo_lock = require_object(
        subject_identity["cargo_lock"],
        f"{label}.subject_identity.cargo_lock",
        (
            "embedded_build_sha256",
            "embedded_build_size_bytes",
            "runtime_path",
            "before_measurement",
            "after_measurement",
            "before_matches_embedded_build",
            "after_matches_embedded_build",
            "unchanged_at_capture_points",
        ),
    )
    embedded_lock_sha256 = require_lower_hex(
        cargo_lock["embedded_build_sha256"],
        f"{label}.subject_identity.cargo_lock.embedded_build_sha256",
        exact_bytes=32,
    )
    embedded_lock_size = require_int(
        cargo_lock["embedded_build_size_bytes"],
        f"{label}.subject_identity.cargo_lock.embedded_build_size_bytes",
        positive=True,
    )
    require_known_receipt_value(
        cargo_lock["runtime_path"], f"{label}.subject_identity.cargo_lock.runtime_path"
    )
    lock_snapshot = validate_stable_file_snapshots(
        cargo_lock["before_measurement"],
        cargo_lock["after_measurement"],
        f"{label}.subject_identity.cargo_lock",
    )
    if (
        lock_snapshot["sha256"] != embedded_lock_sha256
        or lock_snapshot["bytes_read"] != embedded_lock_size
    ):
        fail(f"{label}.subject_identity.cargo_lock does not match the embedded build")
    for field in (
        "before_matches_embedded_build",
        "after_matches_embedded_build",
        "unchanged_at_capture_points",
    ):
        if cargo_lock[field] is not True:
            fail(f"{label}.subject_identity.cargo_lock.{field} must be true")
    comparison_environment = require_object(
        report.get("comparison_environment"),
        f"{label}.comparison_environment",
        ("build_configuration", "invocation", "measurement_host"),
    )
    build_configuration = require_object(
        comparison_environment["build_configuration"],
        f"{label}.comparison_environment.build_configuration",
        (
            "cargo_profile",
            "selected_profile",
            "profile_label",
            "opt_level",
            "debug",
            "target",
            "build_host",
            "enabled_features",
            "rustflags",
            "profile_overrides_hex",
            "native_build_overrides_hex",
            "rustc_version_verbose",
            "cargo_version",
            "resolved_dependency_feature_graph_sha256",
            "resolved_dependency_feature_graph_limitation",
        ),
    )
    if (
        build_configuration.get("selected_profile") != "release-perf"
        or build_configuration.get("profile_label") != "release-perf"
    ):
        fail(f"{label} was not built with the release-perf profile")
    for field in (
        "cargo_profile",
        "opt_level",
        "debug",
        "target",
        "build_host",
        "rustc_version_verbose",
        "cargo_version",
    ):
        require_known_receipt_value(
            build_configuration[field],
            f"{label}.comparison_environment.build_configuration.{field}",
        )
    enabled_features = require_list(
        build_configuration["enabled_features"],
        f"{label}.comparison_environment.build_configuration.enabled_features",
    )
    if any(not isinstance(feature, str) or not feature for feature in enabled_features):
        fail(
            f"{label}.comparison_environment.build_configuration.enabled_features "
            "must contain non-empty strings"
        )
    if enabled_features != sorted(set(enabled_features)):
        fail(
            f"{label}.comparison_environment.build_configuration.enabled_features "
            "must be sorted and unique"
        )
    rustflags = require_object(
        build_configuration["rustflags"],
        f"{label}.comparison_environment.build_configuration.rustflags",
        (
            "cargo_encoded_rustflags_present",
            "encoded_hex",
            "decoded_arguments",
            "decode_error",
        ),
    )
    if not isinstance(rustflags["cargo_encoded_rustflags_present"], bool):
        fail(
            f"{label}.comparison_environment.build_configuration.rustflags."
            "cargo_encoded_rustflags_present must be boolean"
        )
    if rustflags["decode_error"] is not None:
        fail(
            f"{label}.comparison_environment.build_configuration.rustflags.decode_error "
            "must be null"
        )
    encoded_rustflags = validate_hex_blob(
        rustflags["encoded_hex"],
        f"{label}.comparison_environment.build_configuration.rustflags.encoded_hex",
    )
    if not rustflags["cargo_encoded_rustflags_present"] and encoded_rustflags:
        fail(
            f"{label}.comparison_environment.build_configuration.rustflags "
            "claims absent CARGO_ENCODED_RUSTFLAGS but contains bytes"
        )
    try:
        decoded_rustflags = encoded_rustflags.decode("utf-8")
    except UnicodeDecodeError as error:
        fail(
            f"{label}.comparison_environment.build_configuration.rustflags "
            f"are not UTF-8: {error}"
        )
    expected_decoded_arguments = [
        argument for argument in decoded_rustflags.split("\x1f") if argument
    ]
    decoded_arguments = require_list(
        rustflags["decoded_arguments"],
        f"{label}.comparison_environment.build_configuration.rustflags.decoded_arguments",
    )
    if decoded_arguments != expected_decoded_arguments:
        fail(
            f"{label}.comparison_environment.build_configuration.rustflags."
            "decoded_arguments disagree with encoded_hex"
        )
    validate_hex_blob(
        build_configuration["profile_overrides_hex"],
        f"{label}.comparison_environment.build_configuration.profile_overrides_hex",
    )
    validate_hex_blob(
        build_configuration["native_build_overrides_hex"],
        f"{label}.comparison_environment.build_configuration.native_build_overrides_hex",
    )
    require_known_receipt_value(
        build_configuration["resolved_dependency_feature_graph_limitation"],
        f"{label}.comparison_environment.build_configuration."
        "resolved_dependency_feature_graph_limitation",
    )
    invocation = require_object(
        comparison_environment["invocation"],
        f"{label}.comparison_environment.invocation",
        ("argv_lossy", "argv_raw_hex", "raw_encoding", "length_prefixed_argv_sha256"),
    )
    argv_lossy = require_list(
        invocation["argv_lossy"], f"{label}.comparison_environment.invocation.argv_lossy"
    )
    argv_raw_hex = require_list(
        invocation["argv_raw_hex"],
        f"{label}.comparison_environment.invocation.argv_raw_hex",
    )
    if not argv_lossy or len(argv_lossy) != len(argv_raw_hex):
        fail(f"{label}.comparison_environment.invocation argv receipts are incomplete")
    if any(not isinstance(argument, str) for argument in argv_lossy):
        fail(f"{label}.comparison_environment.invocation.argv_lossy must contain strings")
    require_known_receipt_value(
        invocation["raw_encoding"],
        f"{label}.comparison_environment.invocation.raw_encoding",
    )
    canonical_argv = bytearray()
    for index, encoded_argument in enumerate(argv_raw_hex):
        raw_argument = validate_hex_blob(
            encoded_argument,
            f"{label}.comparison_environment.invocation.argv_raw_hex[{index}]",
        )
        canonical_argv.extend(len(raw_argument).to_bytes(8, byteorder="little"))
        canonical_argv.extend(raw_argument)
    invocation_sha256 = require_lower_hex(
        invocation["length_prefixed_argv_sha256"],
        f"{label}.comparison_environment.invocation.length_prefixed_argv_sha256",
        exact_bytes=32,
    )
    if hashlib.sha256(canonical_argv).hexdigest() != invocation_sha256:
        fail(f"{label}.comparison_environment.invocation digest disagrees with raw argv")
    measurement_host = require_object(
        comparison_environment["measurement_host"],
        f"{label}.comparison_environment.measurement_host",
        ("host", "before_measurement", "after_measurement"),
    )
    static_host = require_object(
        measurement_host["host"],
        f"{label}.comparison_environment.measurement_host.host",
        (
            "hostname",
            "cpu_model",
            "available_parallelism",
            "cpu_online",
            "cpu_present",
            "cpu_possible",
            "cpu_isolated",
            "cpu_topology",
            "scaling_governors_by_cpu",
            "kernel_release",
            "kernel_version",
            "numa_online_nodes",
            "numa_possible_nodes",
            "numa_node_directories",
            "unavailable_fields",
        ),
    )
    for field in (
        "hostname",
        "cpu_model",
        "cpu_online",
        "cpu_present",
        "cpu_possible",
        "kernel_release",
        "kernel_version",
    ):
        require_known_receipt_value(
            static_host[field],
            f"{label}.comparison_environment.measurement_host.host.{field}",
        )
    host_available_parallelism = require_int(
        static_host["available_parallelism"],
        f"{label}.comparison_environment.measurement_host.host.available_parallelism",
        positive=True,
    )
    cpu_topology = require_object(
        static_host["cpu_topology"],
        f"{label}.comparison_environment.measurement_host.host.cpu_topology",
        ("logical_cpu_directories", "physical_package_count", "physical_core_count"),
    )
    require_int(
        cpu_topology["logical_cpu_directories"],
        f"{label}.comparison_environment.measurement_host.host.cpu_topology."
        "logical_cpu_directories",
        positive=True,
    )
    for field in ("physical_package_count", "physical_core_count"):
        if cpu_topology[field] is not None:
            require_int(
                cpu_topology[field],
                f"{label}.comparison_environment.measurement_host.host.cpu_topology.{field}",
                positive=True,
            )
    governors = require_object(
        static_host["scaling_governors_by_cpu"],
        f"{label}.comparison_environment.measurement_host.host.scaling_governors_by_cpu",
    )
    if any(
        not isinstance(cpu, str)
        or not cpu
        or not isinstance(governor, str)
        or not governor
        for cpu, governor in governors.items()
    ):
        fail(
            f"{label}.comparison_environment.measurement_host.host."
            "scaling_governors_by_cpu must map non-empty strings"
        )
    if static_host["cpu_isolated"] is not None:
        require_nonempty_string(
            static_host["cpu_isolated"],
            f"{label}.comparison_environment.measurement_host.host.cpu_isolated",
        )
    for field in ("numa_online_nodes", "numa_possible_nodes"):
        if static_host[field] is not None:
            require_nonempty_string(
                static_host[field],
                f"{label}.comparison_environment.measurement_host.host.{field}",
            )
    if static_host["numa_node_directories"] is not None:
        numa_node_directories = require_int(
            static_host["numa_node_directories"],
            f"{label}.comparison_environment.measurement_host.host.numa_node_directories",
        )
        if numa_node_directories < 0:
            fail(
                f"{label}.comparison_environment.measurement_host.host."
                "numa_node_directories must be non-negative"
            )
    unavailable_fields = require_list(
        static_host["unavailable_fields"],
        f"{label}.comparison_environment.measurement_host.host.unavailable_fields",
    )
    if any(not isinstance(field, str) or not field for field in unavailable_fields):
        fail(
            f"{label}.comparison_environment.measurement_host.host.unavailable_fields "
            "must contain non-empty strings"
        )
    if len(unavailable_fields) != len(set(unavailable_fields)):
        fail(
            f"{label}.comparison_environment.measurement_host.host.unavailable_fields "
            "must be unique"
        )
    expected_unavailable_fields = [
        field
        for field in (
            "hostname",
            "cpu_model",
            "available_parallelism",
            "cpu_online",
            "cpu_present",
            "cpu_possible",
            "kernel_release",
            "kernel_version",
            "numa_online_nodes",
            "numa_possible_nodes",
            "numa_node_directories",
        )
        if static_host[field] is None
    ]
    if static_host["cpu_isolated"] is None:
        expected_unavailable_fields.append("cpu_isolated")
    if not governors:
        expected_unavailable_fields.append("scaling_governors_by_cpu")
    if unavailable_fields != expected_unavailable_fields:
        fail(
            f"{label}.comparison_environment.measurement_host.host.unavailable_fields "
            "disagrees with the captured host fields"
        )
    dynamic_hosts = {}
    for field in ("before_measurement", "after_measurement"):
        dynamic_label = f"{label}.comparison_environment.measurement_host.{field}"
        dynamic_host = require_object(
            measurement_host[field],
            dynamic_label,
            (
                "unix_epoch_millis",
                "process_cpu_affinity_mask",
                "process_cpu_affinity_list",
                "proc_self_cgroup",
                "cpuset_cpus_effective",
                "cpuset_mems_effective",
                "load_average",
                "pressure_cpu",
                "pressure_memory",
                "pressure_io",
            ),
        )
        require_int(
            dynamic_host["unix_epoch_millis"],
            f"{dynamic_label}.unix_epoch_millis",
            positive=True,
        )
        for placement_field in (
            "process_cpu_affinity_mask",
            "process_cpu_affinity_list",
            "proc_self_cgroup",
            "cpuset_cpus_effective",
            "cpuset_mems_effective",
        ):
            require_known_receipt_value(
                dynamic_host[placement_field], f"{dynamic_label}.{placement_field}"
            )
        for optional_field in (
            "load_average",
            "pressure_cpu",
            "pressure_memory",
            "pressure_io",
        ):
            if dynamic_host[optional_field] is not None:
                require_nonempty_string(
                    dynamic_host[optional_field], f"{dynamic_label}.{optional_field}"
                )
        dynamic_hosts[field] = dynamic_host
    if (
        dynamic_hosts["after_measurement"]["unix_epoch_millis"]
        < dynamic_hosts["before_measurement"]["unix_epoch_millis"]
    ):
        fail(f"{label} measurement host timestamps are reversed")
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
            fail(f"{label}.{field} does not match the v7 contract")

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
    previous_report_found = pass_over_pass.get("previous_report_found")
    if not isinstance(previous_report_found, bool):
        fail(f"{label}.pass_over_pass_gate.previous_report_found must be boolean")
    if (
        pass_over_pass.get("schema_version")
        != "fsqlite-e2e.mt_mvcc_bench.pass_over_pass.v1"
        or not close_enough(pass_over_pass_threshold, 5.0)
        or pass_over_pass.get("status") != "disabled_non_citable"
        or comparable_pair_count != 0
        or pass_over_pass.get("regressions") != []
    ):
        fail(f"{label}.pass_over_pass_gate is not the disabled non-citable v7 receipt")
    history_path = require_nonempty_string(
        pass_over_pass.get("history_json_path"),
        f"{label}.pass_over_pass_gate.history_json_path",
    )
    if required_history_path is not None and history_path != required_history_path:
        fail(f"{label}.pass_over_pass_gate does not match its bound history path")
    if measurement_mode == "measured" and required_history_path is not None:
        if previous_report_found:
            fail(f"{label}.pass_over_pass_gate unexpectedly found per-run history")
        if os.path.lexists(history_path):
            fail(f"{label}.pass_over_pass_gate history path must remain absent")

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
            fail(f"{receipt_label}.max_supported_writers does not match the v7 contract")
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
            fail(f"{receipt_label} does not use the v7 default checkpoint cadence")
        retry_policy = receipt.get("retry_policy")
        if retry_policy != expected_retry_policy(writers):
            fail(f"{receipt_label} does not use the exact v7 retry-policy contract")
        require_nonempty_string(receipt.get("reason"), f"{receipt_label}.reason")
        receipts_by_thread[writers] = receipt
    if tuple(sorted(receipts_by_thread)) != required_threads:
        fail(f"{label} configurations must be exactly {required_threads}")
    if len(available_parallelism_values) != 1:
        fail(f"{label} configuration receipts disagree on available parallelism")
    if available_parallelism_values != {host_available_parallelism}:
        fail(f"{label} host and configuration receipts disagree on available parallelism")

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
    placement_fields = (
        "process_cpu_affinity_mask",
        "process_cpu_affinity_list",
        "proc_self_cgroup",
        "cpuset_cpus_effective",
        "cpuset_mems_effective",
    )
    placement_before = {
        field: measurement_host["before_measurement"][field]
        for field in placement_fields
    }
    placement_after = {
        field: measurement_host["after_measurement"][field]
        for field in placement_fields
    }
    if placement_before != placement_after:
        fail(f"{label} CPU/cgroup placement changed during measurement")
    measurement_comparability = {
        "build_configuration": build_configuration,
        "static_host": static_host,
        "stable_process_placement": placement_before,
    }
    return {
        "iterations": iterations,
        "history_json_path": history_path,
        "measurement_comparability": measurement_comparability,
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
        run_id if measurement_mode == "measured" else None,
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
        baseline_capture_run_id if measurement_mode == "measured" else None,
    )
    if baseline_validated["iterations"] != current_validated["iterations"]:
        fail("baseline and current reports use different iteration counts")
    if baseline_validated["contract"] != current_validated["contract"]:
        fail("baseline and current reports use incompatible benchmark configuration contracts")
    if (
        baseline_validated["measurement_comparability"]
        != current_validated["measurement_comparability"]
    ):
        fail("baseline and current reports use incompatible v7 measurement environments")

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
