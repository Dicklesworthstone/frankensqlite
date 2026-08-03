#!/usr/bin/env bash
# bd-db300.1.7.2: Capture authoritative persistent 8t and 16t phase-attribution packs.
#
# Uses the Criterion bench entrypoint:
#   crates/fsqlite-e2e/benches/concurrent_write_persistent_bench.rs
#
# Capture surface:
#   FSQLITE_PERSISTENT_PHASE_ATTRIBUTION_DIR → provenance.json + samples.jsonl
#
# Thread counts exercised: 8, 16 (the two degraded regimes from 2026-03-20).
#
# Usage:
#   ./scripts/capture_persistent_phase_pack.sh [output_dir]
#
# Citation-grade capture contract (fail closed):
# - clean frozen source revision, 64-hex build nonce, and `cargo -vv` build log;
# - the daemon-authoritative non-hz1/non-hz2 worker and RCH scheduler-isolation trace;
# - a SHA-256-bound running benchmark binary; and
# - Criterion `estimates.json` inputs, not warmup-mixed phase samples, for headline throughput.
#
# The benchmark itself still emits warmup-mixed `samples.jsonl`. Those records
# remain phase-attribution evidence only. A producer refuses a release pack when
# the separately captured measurement-only Criterion inputs are unavailable.
#
# Output:
#   <output_dir>/
#     provenance/environment.yaml   — machine/build provenance
#     8t/provenance.json            — Criterion bench provenance (auto-generated)
#     8t/samples.jsonl              — per-iteration phase attribution (auto-generated)
#     8t/criterion_stdout.log       — raw Criterion output
#     16t/provenance.json
#     16t/samples.jsonl
#     16t/criterion_stdout.log
#     persistent_scorecard.json     — honest-gate verdicts for 8t / 16t
#     persistent_pack_manifest.json — machine-readable pack summary
#     summary.md                    — human-readable critical-regime surface
#     rerun.sh                      — one-command reproducibility entrypoint
set -euo pipefail

BEAD_ID="${BEAD_ID:-bd-db300.1.7.2}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
resolve_output_dir() {
    local requested_path="$1"
    local absolute_path="$requested_path"
    # Criterion benches run from the package directory, so the capture env must be absolute.
    if [[ "$absolute_path" != /* ]]; then
        absolute_path="${PROJECT_ROOT}/${absolute_path}"
    fi
    mkdir -p "$absolute_path"
    (
        cd "$absolute_path"
        pwd -P
    )
}

SELF_TEST="${FSQLITE_CAPTURE_SELF_TEST:-0}"
if [[ "$SELF_TEST" == "1" ]]; then
    OUTPUT_DIR=""
else
    OUTPUT_DIR="$(resolve_output_dir "${1:-artifacts/persistent_phase_pack_${TIMESTAMP}}")"
fi
PROVENANCE_DIR="${OUTPUT_DIR}/provenance"
SCORECARD_JSON="${OUTPUT_DIR}/persistent_scorecard.json"
MANIFEST_JSON="${OUTPUT_DIR}/persistent_pack_manifest.json"
SUMMARY_MD="${OUTPUT_DIR}/summary.md"
HASHES_TXT="${OUTPUT_DIR}/artifact_hashes.txt"
RERUN_SH="${OUTPUT_DIR}/rerun.sh"
BUILD_VV_LOG="${PROVENANCE_DIR}/cargo-build-vv.log"
CITATION_RECEIPT_JSON="${PROVENANCE_DIR}/citation_receipt.json"
BUILD_STATUS_TRACE="${PROVENANCE_DIR}/rch_build_status.jsonl"

THREAD_COUNTS_CSV="${THREAD_COUNTS:-1,8,16}"
RENDER_ONLY="${RENDER_ONLY:-0}"
SKIP_RUN="${SKIP_RUN:-0}"
FSQLITE_USE_RCH="${FSQLITE_USE_RCH:-0}"
RCH_BIN="${RCH_BIN:-rch}"
EQUIVALENCE_MARGIN_MAX_DROP=0.05
EQUIVALENCE_LOWER_BOUND_MIN=0.95
TAIL_COLLAPSE_P95_US="${TAIL_COLLAPSE_P95_US:-250000}"
TAIL_COLLAPSE_P99_US="${TAIL_COLLAPSE_P99_US:-500000}"
TAIL_COLLAPSE_MAX_US="${TAIL_COLLAPSE_MAX_US:-2000000}"
PHASE_B_COLLAPSE_P99_US="${PHASE_B_COLLAPSE_P99_US:-250000}"
WAL_APPEND_COLLAPSE_P99_US="${WAL_APPEND_COLLAPSE_P99_US:-250000}"
FROZEN_COMMIT="${FSQLITE_RELEASE_FROZEN_COMMIT:-}"
BUILD_NONCE="${FSQLITE_BENCH_BUILD_NONCE:-}"
ACTUAL_WORKER=""
ACTUAL_HOST=""
CRITERION_SAMPLE_SIZE="${FSQLITE_RELEASE_CRITERION_SAMPLE_SIZE:-}"
CRITERION_WARMUP_SECS="${FSQLITE_RELEASE_CRITERION_WARMUP_SECS:-}"
CRITERION_MEASUREMENT_SECS="${FSQLITE_RELEASE_CRITERION_MEASUREMENT_SECS:-}"
EXPECTED_ROWS_PER_THREAD=1000
EXPECTED_SYNC=normal

IFS=',' read -ra THREAD_COUNTS <<< "$THREAD_COUNTS_CSV"

die() {
    echo "[$BEAD_ID] FATAL: $*" >&2
    exit 2
}

require_lower_hex() {
    local value="$1"
    local length="$2"
    local label="$3"
    [[ "$value" =~ ^[0-9a-f]{${length}}$ ]] || die "$label must be exactly ${length} lowercase hexadecimal characters"
}

require_positive_integer() {
    local value="$1"
    local label="$2"
    [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "$label must be a positive integer"
}

require_allowed_remote_worker() {
    local worker="$1"
    [[ -n "$worker" ]] || die "RCH output did not identify a selected worker"
    [[ "$worker" != "hz1" && "$worker" != "hz2" ]] \
        || die "quarantined worker ${worker} is prohibited"
}

require_requested_actual_match() {
    local requested_worker="$1"
    local actual_worker="$2"
    [[ "$requested_worker" == "$actual_worker" ]] \
        || die "requested RCH_WORKER ${requested_worker} differs from daemon-selected worker ${actual_worker}"
}

validate_citation_contract() {
    [[ "$RENDER_ONLY" == "0" ]] || die "RENDER_ONLY is not valid for a citation-grade capture"
    [[ "$SKIP_RUN" == "0" ]] || die "SKIP_RUN is not valid for a citation-grade capture"
    [[ "$THREAD_COUNTS_CSV" == "1,8,16" ]] || die "THREAD_COUNTS must be exactly 1,8,16 for the release contract"
    [[ ! -e "$OUTPUT_DIR" || -d "$OUTPUT_DIR" ]] \
        || die "citation output path exists but is not a directory: $OUTPUT_DIR"
    [[ ! -d "$OUTPUT_DIR" || -z "$(find "$OUTPUT_DIR" -mindepth 1 -print -quit)" ]] \
        || die "citation output directory must be empty; refusing stale provenance or measurements"
    [[ "$FSQLITE_USE_RCH" == "1" ]] || die "FSQLITE_USE_RCH=1 is required for release capture"
    [[ "${RCH_REQUIRE_REMOTE:-}" == "1" ]] || die "RCH_REQUIRE_REMOTE=1 is required"
    [[ "${RCH_NO_SELF_HEALING:-}" == "1" ]] || die "RCH_NO_SELF_HEALING=1 is required"
    [[ -n "${RCH_WORKER:-}" ]] || die "RCH_WORKER must name the requested remote worker"
    [[ -n "$FROZEN_COMMIT" ]] || die "FSQLITE_RELEASE_FROZEN_COMMIT is required"
    require_lower_hex "$FROZEN_COMMIT" 40 FSQLITE_RELEASE_FROZEN_COMMIT
    [[ "$(git -C "$PROJECT_ROOT" rev-parse HEAD)" == "$FROZEN_COMMIT" ]] || die "HEAD does not match FSQLITE_RELEASE_FROZEN_COMMIT"
    [[ -z "$(git -C "$PROJECT_ROOT" status --porcelain --untracked-files=all)" ]] || die "release source checkout is not clean"
    [[ -n "$BUILD_NONCE" ]] || die "FSQLITE_BENCH_BUILD_NONCE is required"
    require_lower_hex "$BUILD_NONCE" 64 FSQLITE_BENCH_BUILD_NONCE
    require_positive_integer "$CRITERION_SAMPLE_SIZE" FSQLITE_RELEASE_CRITERION_SAMPLE_SIZE
    (( CRITERION_SAMPLE_SIZE >= 10 )) || die "Criterion sample size must be at least 10"
    require_positive_integer "$CRITERION_WARMUP_SECS" FSQLITE_RELEASE_CRITERION_WARMUP_SECS
    require_positive_integer "$CRITERION_MEASUREMENT_SECS" FSQLITE_RELEASE_CRITERION_MEASUREMENT_SECS
    grep -Fq 'const ROWS_PER_THREAD: i64 = 1000;' \
        "$PROJECT_ROOT/crates/fsqlite-e2e/benches/concurrent_write_persistent_bench.rs" \
        || die "benchmark rows-per-thread no longer matches the 1000-row release contract"
    grep -Fq 'PRAGMA synchronous = NORMAL;' \
        "$PROJECT_ROOT/crates/fsqlite-e2e/benches/concurrent_write_persistent_bench.rs" \
        || die "benchmark synchronous mode no longer matches the NORMAL release contract"
    grep -Fq 'running_binary_sha256' \
        "$PROJECT_ROOT/crates/fsqlite-e2e/benches/concurrent_write_persistent_bench.rs" \
        || die "benchmark provenance does not emit running_binary_sha256; refuse to run an uncitable pack"
    grep -Fq 'FSQLITE_BENCH_BUILD_NONCE' \
        "$PROJECT_ROOT/crates/fsqlite-e2e/benches/concurrent_write_persistent_bench.rs" \
        || die "benchmark provenance does not consume FSQLITE_BENCH_BUILD_NONCE; refuse to run an uncitable pack"
    ! rg -Fq 'group.sample_size(' \
        "$PROJECT_ROOT/crates/fsqlite-e2e/benches/concurrent_write_persistent_bench.rs" \
        || die "benchmark overrides Criterion sample-size; receipt inputs would not be authoritative"
    ! rg -Fq 'group.measurement_time(' \
        "$PROJECT_ROOT/crates/fsqlite-e2e/benches/concurrent_write_persistent_bench.rs" \
        || die "benchmark overrides Criterion measurement-time; receipt inputs would not be authoritative"
    grep -Fq 'flush_persistent_phase_capture();' \
        "$PROJECT_ROOT/crates/fsqlite-e2e/benches/concurrent_write_persistent_bench.rs" \
        || die "benchmark lacks post-timing capture flush; refuse timing-contaminated estimates"
}

run_synthetic_contract_checks() {
    local RCH_WORKER="healthy-worker"
    require_lower_hex "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" 64 synthetic_nonce
    require_lower_hex "0123456789abcdef0123456789abcdef01234567" 40 synthetic_commit
    require_positive_integer 21 synthetic_sample_count
    if (require_lower_hex "not-hex" 64 malformed_nonce); then
        die "synthetic validation did not reject malformed hexadecimal input"
    fi
    if (require_positive_integer 0 zero_sample_count); then
        die "synthetic validation did not reject a zero sample count"
    fi
    if (require_lower_hex "" 40 missing_frozen_commit); then
        die "synthetic validation did not reject a missing frozen commit"
    fi
    if (require_allowed_remote_worker hz1); then
        die "synthetic validation did not reject quarantined hz1"
    fi
    if (require_requested_actual_match healthy-worker other-worker); then
        die "synthetic validation did not reject a requested/actual worker mismatch"
    fi
    local good_trace
    local wrong_job_trace
    local missing_job_trace
    local coresident_trace
    good_trace='{"data":{"daemon":{"workers":[{"id":"healthy-worker","host":"vmi123","status":"healthy","circuit_state":"closed"}],"active_builds":[{"id":7,"worker_id":"healthy-worker"}]}}}'
    wrong_job_trace='{"data":{"daemon":{"workers":[{"id":"healthy-worker","host":"vmi123","status":"healthy","circuit_state":"closed"}],"active_builds":[{"id":8,"worker_id":"healthy-worker"}]}}}'
    missing_job_trace='{"data":{"daemon":{"workers":[{"id":"healthy-worker","host":"vmi123","status":"healthy","circuit_state":"closed"}],"active_builds":[]}}}'
    coresident_trace='{"data":{"daemon":{"workers":[{"id":"healthy-worker","host":"vmi123","status":"healthy","circuit_state":"closed"}],"active_builds":[{"id":7,"worker_id":"healthy-worker"},{"id":8,"worker_id":"healthy-worker"}]}}}'
    verify_scheduler_isolation_trace healthy-worker 7 <(printf '%s\n' "$good_trace")
    [[ "$(actual_host_from_status_trace healthy-worker <(printf '%s\n' "$good_trace"))" == "vmi123" ]] \
        || die "synthetic validation did not parse nested daemon host identity"
    [[ "$(rch_job_id_from_log <(printf '%s\n' '[*] Job j-7 submitted to healthy-worker'))" == "7" ]] \
        || die "synthetic validation did not normalize the RCH log job marker"
    [[ "$(actual_worker_from_rch_log <(printf '%s\n' '[RCH] remote healthy-worker accepted job j-7'))" == "healthy-worker" ]] \
        || die "synthetic validation did not parse the daemon-selected worker marker"
    if (rch_job_id_from_log <(printf '%s\n' 'no submitted job marker')); then
        die "synthetic validation did not reject a missing RCH job marker"
    fi
    if (rch_job_id_from_log <(printf '%s\n' '[*] Job j-7 submitted to healthy-worker' '[*] Job j-8 submitted to healthy-worker')); then
        die "synthetic validation did not reject multiple RCH job markers"
    fi
    if (actual_worker_from_rch_log <(printf '%s\n' '[RCH] remote healthy-worker accepted job j-7' '[RCH] remote other-worker accepted job j-7')); then
        die "synthetic validation did not reject multiple daemon-selected worker markers"
    fi
    if (verify_scheduler_isolation_trace healthy-worker 7 <(printf '%s\n' "$wrong_job_trace")); then
        die "synthetic validation did not reject a wrong numeric daemon job ID"
    fi
    if (verify_scheduler_isolation_trace healthy-worker 7 <(printf '%s\n' "$missing_job_trace")); then
        die "synthetic validation did not reject an absent expected daemon job ID"
    fi
    if (verify_scheduler_isolation_trace healthy-worker 7 <(printf '%s\n' "$coresident_trace")); then
        die "synthetic validation did not reject a co-resident daemon job"
    fi
    echo "[$BEAD_ID] synthetic citation-contract validation passed"
}

cargo_runner_label() {
    if [[ "$FSQLITE_USE_RCH" == "1" ]]; then
        echo "rch"
    else
        echo "local"
    fi
}

run_cargo() {
    if [[ "$FSQLITE_USE_RCH" == "1" ]]; then
        local env_args=(
            "FSQLITE_BENCH_BUILD_NONCE=${BUILD_NONCE}"
            "FSQLITE_BENCH_PROFILE_NAME=release-perf"
        )
        if [[ -n "${FSQLITE_PERSISTENT_PHASE_ATTRIBUTION_DIR:-}" ]]; then
            env_args+=("FSQLITE_PERSISTENT_PHASE_ATTRIBUTION_DIR=${FSQLITE_PERSISTENT_PHASE_ATTRIBUTION_DIR}")
        fi
        if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
            env_args+=("CARGO_TARGET_DIR=${CARGO_TARGET_DIR}")
        fi
        "$RCH_BIN" exec -- env "${env_args[@]}" cargo "$@"
    else
        cargo "$@"
    fi
}

actual_worker_from_rch_log() {
    local log_path="$1"
    local worker
    local worker_count

    worker="$(sed -nE 's/.*\[RCH\] remote ([^[:space:]]+) .*/\1/p' "$log_path" | sort -u)"
    worker_count="$(printf '%s\n' "$worker" | sed '/^$/d' | wc -l | tr -d ' ')"
    [[ "$worker_count" == "1" ]] \
        || die "RCH log must contain exactly one daemon-selected worker marker: $log_path"
    require_allowed_remote_worker "$worker"
    require_requested_actual_match "$RCH_WORKER" "$worker"
    printf '%s\n' "$worker"
}

rch_job_id_from_log() {
    local log_path="$1"
    local job_id
    local job_count

    job_id="$(sed -nE 's/^\[\*\] Job j-([0-9]+) submitted to .*/\1/p' "$log_path" | sort -u)"
    job_count="$(printf '%s\n' "$job_id" | sed '/^$/d' | wc -l | tr -d ' ')"
    [[ "$job_count" == "1" ]] \
        || die "RCH log must contain exactly one submitted job ID: $log_path"
    [[ "$job_id" =~ ^[0-9]+$ ]] \
        || die "RCH job ID must normalize to decimal digits: $log_path"
    printf '%s\n' "$job_id"
}

capture_status_snapshot() {
    local trace_path="$1"
    "$RCH_BIN" --no-self-healing status --workers --jobs --json >> "$trace_path" \
        || die "could not retain daemon status snapshot in $trace_path"
    printf '\n' >> "$trace_path"
}

verify_scheduler_isolation_trace() {
    local worker="$1"
    local expected_job_id="$2"
    local trace_path="$3"

    jq -s -e --arg worker "$worker" --argjson job "$expected_job_id" '
        length > 0 and
        all(.[];
            (.data.daemon.workers | type == "array") and
            ([.data.daemon.workers[] | select(.id == $worker and .status == "healthy" and .circuit_state == "closed")]
             | length == 1) and
            ([.data.daemon.active_builds[]? | select(.worker_id == $worker)] | length <= 1) and
            all(.data.daemon.active_builds[]? | select(.worker_id == $worker); .id == $job)
        ) and
        any(.[]; ([.data.daemon.active_builds[]? | select(.worker_id == $worker and .id == $job)] | length == 1))
    ' "$trace_path" >/dev/null \
        || die "RCH status snapshots do not prove exact job ${expected_job_id} as the sole active job on ${worker}"
}

actual_host_from_status_trace() {
    local worker="$1"
    local trace_path="$2"
    local host
    local host_count

    host="$(jq -sr --arg worker "$worker" '
        [.[].data.daemon.workers[] | select(.id == $worker) | .host] | unique[]' "$trace_path")"
    host_count="$(printf '%s\n' "$host" | sed '/^$/d' | wc -l | tr -d ' ')"
    [[ "$host_count" == "1" && -n "$host" ]] \
        || die "RCH status trace must identify exactly one host for worker ${worker}: $trace_path"
    printf '%s\n' "$host"
}

run_cargo_with_scheduler_trace() {
    local log_path="$1"
    local trace_path="$2"
    shift 2
    local cargo_pid
    local cargo_status
    local worker
    local job_id

    : > "$log_path"
    : > "$trace_path"
    (
        cd "$PROJECT_ROOT"
        run_cargo "$@"
    ) > "$log_path" 2>&1 &
    cargo_pid=$!
    while kill -0 "$cargo_pid" 2>/dev/null; do
        capture_status_snapshot "$trace_path"
        sleep 1
    done
    if wait "$cargo_pid"; then
        cargo_status=0
    else
        cargo_status=$?
    fi
    cat "$log_path"
    [[ "$cargo_status" -eq 0 ]] || die "RCH cargo command failed; retained transcript: $log_path"

    worker="$(actual_worker_from_rch_log "$log_path")"
    job_id="$(rch_job_id_from_log "$log_path")"
    if [[ -n "$ACTUAL_WORKER" && "$ACTUAL_WORKER" != "$worker" ]]; then
        die "RCH selected different workers across the release pack: ${ACTUAL_WORKER} then ${worker}"
    fi
    ACTUAL_WORKER="$worker"
    verify_scheduler_isolation_trace "$worker" "$job_id" "$trace_path"
    local host
    host="$(actual_host_from_status_trace "$worker" "$trace_path")"
    if [[ -n "$ACTUAL_HOST" && "$ACTUAL_HOST" != "$host" ]]; then
        die "RCH selected different host identities across the release pack: ${ACTUAL_HOST} then ${host}"
    fi
    ACTUAL_HOST="$host"
}

write_citation_receipt() {
    local build_log_sha256
    local binary_sha256
    local binary_path
    local status_trace_sha256
    local trace_1t_sha256
    local trace_8t_sha256
    local trace_16t_sha256
    local binary_1t_sha256
    local binary_16t_sha256
    local build_job_id
    local job_1t_id
    local job_8t_id
    local job_16t_id

    build_log_sha256="$(sha256sum "$BUILD_VV_LOG" | awk '{print $1}')"
    binary_sha256="$(jq -r '.running_binary_sha256' "$OUTPUT_DIR/8t/provenance.json")"
    binary_1t_sha256="$(jq -r '.running_binary_sha256' "$OUTPUT_DIR/1t/provenance.json")"
    binary_path="$(jq -r '.current_exe' "$OUTPUT_DIR/8t/provenance.json")"
    status_trace_sha256="$(sha256sum "$BUILD_STATUS_TRACE" | awk '{print $1}')"
    trace_1t_sha256="$(sha256sum "$OUTPUT_DIR/1t/rch_status.jsonl" | awk '{print $1}')"
    trace_8t_sha256="$(sha256sum "$OUTPUT_DIR/8t/rch_status.jsonl" | awk '{print $1}')"
    trace_16t_sha256="$(sha256sum "$OUTPUT_DIR/16t/rch_status.jsonl" | awk '{print $1}')"
    binary_16t_sha256="$(jq -r '.running_binary_sha256' "$OUTPUT_DIR/16t/provenance.json")"
    build_job_id="$(rch_job_id_from_log "$BUILD_VV_LOG")"
    job_1t_id="$(rch_job_id_from_log "$OUTPUT_DIR/1t/criterion_stdout.log")"
    job_8t_id="$(rch_job_id_from_log "$OUTPUT_DIR/8t/criterion_stdout.log")"
    job_16t_id="$(rch_job_id_from_log "$OUTPUT_DIR/16t/criterion_stdout.log")"
    require_lower_hex "$build_log_sha256" 64 cargo_build_vv_log_sha256
    require_lower_hex "$binary_sha256" 64 running_binary_sha256
    require_lower_hex "$binary_1t_sha256" 64 running_binary_1t_sha256
    require_lower_hex "$binary_16t_sha256" 64 running_binary_16t_sha256
    require_lower_hex "$status_trace_sha256" 64 rch_build_status_trace_sha256
    require_lower_hex "$trace_1t_sha256" 64 rch_1t_status_trace_sha256
    require_lower_hex "$trace_8t_sha256" 64 rch_8t_status_trace_sha256
    require_lower_hex "$trace_16t_sha256" 64 rch_16t_status_trace_sha256
    [[ "$build_job_id" =~ ^[0-9]+$ ]] || die "build RCH job ID must be numeric"
    [[ "$job_1t_id" =~ ^[0-9]+$ ]] || die "1t RCH job ID must be numeric"
    [[ "$job_8t_id" =~ ^[0-9]+$ ]] || die "8t RCH job ID must be numeric"
    [[ "$job_16t_id" =~ ^[0-9]+$ ]] || die "16t RCH job ID must be numeric"
    [[ "$binary_sha256" == "$binary_1t_sha256" && "$binary_sha256" == "$binary_16t_sha256" ]] \
        || die "1t, 8t, and 16t benchmarks did not report the same running-binary SHA-256"

    jq -n \
        --arg bead_id "$BEAD_ID" \
        --arg frozen_commit "$FROZEN_COMMIT" \
        --arg build_nonce "$BUILD_NONCE" \
        --arg requested_worker "$RCH_WORKER" \
        --arg actual_worker "$ACTUAL_WORKER" \
        --arg actual_host "$ACTUAL_HOST" \
        --arg running_binary "$binary_path" \
        --arg binary_sha256 "$binary_sha256" \
        --arg binary_1t_sha256 "$binary_1t_sha256" \
        --arg binary_16t_sha256 "$binary_16t_sha256" \
        --arg build_log_sha256 "$build_log_sha256" \
        --arg status_trace_sha256 "$status_trace_sha256" \
        --arg trace_1t_sha256 "$trace_1t_sha256" \
        --arg trace_8t_sha256 "$trace_8t_sha256" \
        --arg trace_16t_sha256 "$trace_16t_sha256" \
        --argjson build_job_id "$build_job_id" \
        --argjson job_1t_id "$job_1t_id" \
        --argjson job_8t_id "$job_8t_id" \
        --argjson job_16t_id "$job_16t_id" \
        --argjson sample_size "$CRITERION_SAMPLE_SIZE" \
        --argjson warmup_secs "$CRITERION_WARMUP_SECS" \
        --argjson measurement_secs "$CRITERION_MEASUREMENT_SECS" \
        '{
            schema_version: "fsqlite.release_persistent_phase_pack_citation_receipt.v1",
            bead_id: $bead_id,
            source: { commit: $frozen_commit, clean: true },
            build: {
                profile: "release-perf",
                nonce: $build_nonce,
                cargo_verbose_log: "provenance/cargo-build-vv.log",
                cargo_verbose_log_sha256: $build_log_sha256,
                running_binary: $running_binary,
                running_binary_sha256: $binary_sha256,
                phase_binary_sha256: { "1t": $binary_1t_sha256, "8t": $binary_sha256, "16t": $binary_16t_sha256 },
                provenance_bound: true
            },
            rch: {
                requested_worker: $requested_worker,
                actual_worker: $actual_worker,
                actual_host: $actual_host,
                require_remote: true,
                no_self_healing: true
            },
            rch_scheduler_isolation: {
                build_status_trace: "provenance/rch_build_status.jsonl",
                build_status_trace_sha256: $status_trace_sha256,
                build_job_id: $build_job_id,
                worker: $actual_worker,
                host: $actual_host,
                evidence: "daemon status snapshots sampled while each RCH job ran",
                phase_traces: {
                    "1t": { path: "1t/rch_status.jsonl", sha256: $trace_1t_sha256, job_id: $job_1t_id },
                    "8t": { path: "8t/rch_status.jsonl", sha256: $trace_8t_sha256, job_id: $job_8t_id },
                    "16t": { path: "16t/rch_status.jsonl", sha256: $trace_16t_sha256, job_id: $job_16t_id }
                }
            },
            workload: {
                benchmark: "persistent_concurrent_write_{1,8,16}t",
                rows_per_thread: 1000,
                synchronous: "NORMAL",
                threads: [1, 8, 16],
                criterion: {
                    sample_size: $sample_size,
                    warmup_secs: $warmup_secs,
                    measurement_secs: $measurement_secs,
                    export_root: "{phase}/criterion_measurements",
                    headline_source: "{phase}/criterion_measurements/{label}/{engine}/base/estimates.json"
                }
            }
        }' > "$CITATION_RECEIPT_JSON"
}

verify_run_artifacts() {
    local thread_count="$1"
    local run_dir="$2"
    local provenance="$run_dir/provenance.json"
    local samples="$run_dir/samples.jsonl"

    [[ -s "$provenance" ]] || die "${thread_count}t benchmark provenance.json is missing"
    [[ -s "$samples" ]] || die "${thread_count}t benchmark samples.jsonl is missing"
    jq -e \
        --arg expected_nonce "$BUILD_NONCE" \
        --argjson expected_threads "$thread_count" \
        --argjson expected_rows "$EXPECTED_ROWS_PER_THREAD" \
        '(.current_exe | type == "string" and length > 0) and
         (.running_binary_sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
         .build_nonce == $expected_nonce and
         .concurrency == $expected_threads and
         .rows_per_thread == $expected_rows and
         .synchronous == "NORMAL" and
         (.criterion_emission_scope | contains("written only after group.finish()"))' \
        "$provenance" >/dev/null \
        || die "${thread_count}t provenance lacks the required binary SHA-256/build-nonce/workload binding"
    grep -F -- "--sample-size ${CRITERION_SAMPLE_SIZE}" "$run_dir/criterion_stdout.log" >/dev/null \
        || die "${thread_count}t retained RCH transcript lacks the requested Criterion sample size"
    grep -F -- "--warm-up-time ${CRITERION_WARMUP_SECS}" "$run_dir/criterion_stdout.log" >/dev/null \
        || die "${thread_count}t retained RCH transcript lacks the requested Criterion warmup"
    grep -F -- "--measurement-time ${CRITERION_MEASUREMENT_SECS}" "$run_dir/criterion_stdout.log" >/dev/null \
        || die "${thread_count}t retained RCH transcript lacks the requested Criterion measurement time"
    jq -s -e \
        --argjson expected_threads "$thread_count" \
        --argjson expected_rows "$EXPECTED_ROWS_PER_THREAD" \
        'length > 0 and
         any(.[]; .engine == "sqlite3") and
         any(.[]; .engine == "fsqlite_mvcc") and
         all(.[]; .concurrency == $expected_threads and .rows_per_thread == $expected_rows and .synchronous == "NORMAL")' \
        "$samples" >/dev/null \
        || die "${thread_count}t samples violate the cited workload contract"
}

extract_measurement_only_estimates() {
    local thread_count="$1"
    local run_dir="$2"
    local label="persistent_concurrent_write_${thread_count}t"
    local engine
    local source

    for engine in csqlite_concurrent_persistent frankensqlite_concurrent_persistent; do
        source="${run_dir}/criterion_measurements/${label}/${engine}/base/estimates.json"
        [[ -r "$source" ]] || die "missing Criterion measurement-only estimates for ${label}/${engine}: $source"
        jq -e '
            .mean.point_estimate > 0 and
            .mean.confidence_interval.confidence_level == 0.95 and
            .mean.confidence_interval.lower_bound > 0 and
            .mean.confidence_interval.upper_bound > 0
        ' "$source" >/dev/null || die "invalid Criterion measurement-only estimates: $source"
        cp "$source" "$run_dir/criterion_measurement_${engine}.json"
    done
}

write_environment_provenance() {
    local cpu_model="unknown"
    cpu_model="$(awk -F: '/model name/ {gsub(/^[ \t]+/, "", $2); print $2; exit}' /proc/cpuinfo 2>/dev/null || true)"
    if [[ -z "${cpu_model}" ]]; then
        cpu_model="unknown"
    fi

    {
        echo "bead_id: ${BEAD_ID}"
        echo "capture_timestamp: ${TIMESTAMP}"
        echo "capture_script: scripts/capture_persistent_phase_pack.sh"
        echo "coordinator_hostname: $(hostname)"
        echo "coordinator_uname: $(uname -a)"
        echo "coordinator_cpu_model: ${cpu_model}"
        echo "coordinator_cpu_count: $(nproc 2>/dev/null || echo unknown)"
        echo "coordinator_memory_gb: $(free -g 2>/dev/null | awk '/^Mem:/{print $2}' || echo unknown)"
        echo "numa_nodes: $(ls -d /sys/devices/system/node/node* 2>/dev/null | wc -l || echo 1)"
        echo "load_avg: $(cat /proc/loadavg 2>/dev/null || echo unknown)"
        echo "git_commit: $(git -C "$PROJECT_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
        echo "git_branch: $(git -C "$PROJECT_ROOT" branch --show-current 2>/dev/null || echo unknown)"
        echo "git_dirty_files: $(git -C "$PROJECT_ROOT" diff --name-only 2>/dev/null | wc -l || echo unknown)"
        echo "rust_version: $(rustc --version 2>/dev/null || echo unknown)"
        echo "cargo_profile: release-perf"
        echo "cargo_runner: $(cargo_runner_label)"
        echo "cargo_target_dir: ${CARGO_TARGET_DIR:-unset}"
        echo "rch_bin: ${RCH_BIN}"
        echo "bench_entrypoint: crates/fsqlite-e2e/benches/concurrent_write_persistent_bench.rs"
        echo "capture_env: FSQLITE_PERSISTENT_PHASE_ATTRIBUTION_DIR"
        echo "reference_comparator: C SQLite via rusqlite (built-in to bench)"
        echo "thread_counts: [${THREAD_COUNTS_CSV}]"
        echo "rows_per_thread: ${EXPECTED_ROWS_PER_THREAD}"
        echo "synchronous: ${EXPECTED_SYNC}"
        echo "criterion_sample_size: ${CRITERION_SAMPLE_SIZE}"
        echo "criterion_warmup_secs: ${CRITERION_WARMUP_SECS}"
        echo "criterion_measurement_secs: ${CRITERION_MEASUREMENT_SECS}"
        echo "frozen_commit: ${FROZEN_COMMIT}"
        echo "build_nonce: ${BUILD_NONCE}"
        echo "requested_rch_worker: ${RCH_WORKER}"
        echo "daemon_authoritative_worker: recorded_after_execution_in_citation_receipt"
        echo "authoritative_remote_environment: benchmark provenance hostname/kernel_release plus daemon worker/host"
        echo "warmup_measurement_disclaimer: |"
        echo "  The Criterion harness runs warmup iterations before measurement."
        echo "  FSQLITE_PERSISTENT_PHASE_ATTRIBUTION_DIR captures ALL iterations"
        echo "  (warmup + measurement) in samples.jsonl. The harness does NOT tag"
        echo "  which samples are warmup vs measurement. Consumers should use"
        echo "  Criterion's own throughput estimates for authoritative throughput."
        echo "  The samples.jsonl is authoritative for phase-attribution"
        echo "  distributions and wake-reason accounting."
    } > "$PROVENANCE_DIR/environment.yaml"
}

ensure_bench_binary() {
    echo "--- Building release-perf benchmark binary ($(cargo_runner_label)) ---"
    run_cargo_with_scheduler_trace "$BUILD_VV_LOG" "$BUILD_STATUS_TRACE" \
        bench -vv --locked --profile release-perf -p fsqlite-e2e \
        --bench concurrent_write_persistent_bench --no-run
}

run_persistent_bench() {
    local thread_count="$1"
    local label
    local run_dir
    local provenance
    local samples

    label="${thread_count}t"
    run_dir="$OUTPUT_DIR/${label}"
    provenance="$run_dir/provenance.json"
    samples="$run_dir/samples.jsonl"

    mkdir -p "$run_dir"

    echo ""
    echo "=== Capturing ${label} persistent phase pack ==="
    echo "Thread count: $thread_count"
    echo "Phase attribution dir: $run_dir"

    FSQLITE_PERSISTENT_PHASE_ATTRIBUTION_DIR="$run_dir" \
        run_cargo_with_scheduler_trace "$run_dir/criterion_stdout.log" "$run_dir/rch_status.jsonl" \
            bench --locked --profile release-perf -p fsqlite-e2e \
            --bench concurrent_write_persistent_bench \
            -- --sample-size "$CRITERION_SAMPLE_SIZE" \
            --warm-up-time "$CRITERION_WARMUP_SECS" \
            --measurement-time "$CRITERION_MEASUREMENT_SECS" \
            "persistent_concurrent_write_${thread_count}t"

    extract_measurement_only_estimates "$thread_count" "$run_dir"
    verify_run_artifacts "$thread_count" "$run_dir"

    if [[ -f "$provenance" ]]; then
        echo "  provenance.json: $(wc -c < "$provenance") bytes"
    else
        echo "  WARNING: provenance.json not generated"
    fi

    if [[ -f "$samples" ]]; then
        echo "  samples.jsonl: $(wc -l < "$samples") records"
    else
        echo "  WARNING: samples.jsonl not generated"
    fi

    echo "--- ${label} capture complete ---"
}

render_reports() {
    python3 - \
        "$OUTPUT_DIR" \
        "$SCORECARD_JSON" \
        "$MANIFEST_JSON" \
        "$SUMMARY_MD" \
        "$BEAD_ID" \
        "$EQUIVALENCE_MARGIN_MAX_DROP" \
        "$EQUIVALENCE_LOWER_BOUND_MIN" \
        "$TAIL_COLLAPSE_P95_US" \
        "$TAIL_COLLAPSE_P99_US" \
        "$TAIL_COLLAPSE_MAX_US" \
        "$PHASE_B_COLLAPSE_P99_US" \
        "$WAL_APPEND_COLLAPSE_P99_US" \
        "$THREAD_COUNTS_CSV" <<'PY'
import json
import statistics
import sys
from pathlib import Path

output_dir = Path(sys.argv[1])
scorecard_path = Path(sys.argv[2])
manifest_path = Path(sys.argv[3])
summary_path = Path(sys.argv[4])
bead_id = sys.argv[5]
equivalence_margin_max_drop = float(sys.argv[6])
equivalence_lower_bound_min = float(sys.argv[7])
tail_collapse_p95_us = int(sys.argv[8])
tail_collapse_p99_us = int(sys.argv[9])
tail_collapse_max_us = int(sys.argv[10])
phase_b_collapse_p99_us = int(sys.argv[11])
wal_append_collapse_p99_us = int(sys.argv[12])
thread_labels = []
for item in sys.argv[13].split(","):
    item = item.strip()
    if not item:
        continue
    thread_labels.append(item if item.endswith("t") else f"{item}t")


def nested_get(mapping, *keys):
    current = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def median_value(values):
    usable = [value for value in values if value is not None]
    if not usable:
        return None
    return statistics.median(usable)


def ratio(numerator, denominator):
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def format_ratio(value):
    if value is None:
        return "n/a"
    return f"{value:.3f}x"


def format_us_triplet(row):
    if row is None:
        return "n/a"
    return f"{row['p50']}/{row['p95']}/{row['p99']}"


def median_nested(rows, *keys):
    return median_value([nested_get(row, *keys) for row in rows])


def format_optional_us(value):
    if value is None:
        return "n/a"
    return f"{int(value)}us"


def format_signed_us(value):
    if value is None:
        return "n/a"
    value = int(value)
    return f"+{value}us" if value > 0 else f"{value}us"


def format_retry_stage_counts(stage_counts):
    if not stage_counts:
        return "n/a"
    values = [
        stage_counts.get("retry_stage_begin_retries_median"),
        stage_counts.get("retry_stage_body_retries_median"),
        stage_counts.get("retry_stage_commit_retries_median"),
        stage_counts.get("retry_stage_duplicate_after_retry_exits_median"),
        stage_counts.get("retry_stage_total_retries_median"),
    ]
    if any(value is None for value in values):
        return "n/a"
    return "/".join(str(int(value)) for value in values)


def measurement_only_throughput_interval(estimates_path, total_rows):
    """Convert Criterion's measured mean-time estimate to throughput.

    `base/estimates.json` is emitted after Criterion's warmup phase and is the
    only accepted headline-throughput source for this producer.  The captured
    phase JSONL intentionally remains outside this conversion because it mixes
    warmup and measured iterations without an iteration-type tag.
    """
    try:
        estimates = json.loads(estimates_path.read_text(encoding="utf-8"))
        mean = estimates["mean"]
        confidence = mean["confidence_interval"]
        if confidence["confidence_level"] != 0.95:
            return None
        point = float(mean["point_estimate"])
        lower_time = float(confidence["lower_bound"])
        upper_time = float(confidence["upper_bound"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if point <= 0 or lower_time <= 0 or upper_time <= 0:
        return None
    return {
        "low": total_rows * 1_000_000_000 / upper_time,
        "mid": total_rows * 1_000_000_000 / point,
        "high": total_rows * 1_000_000_000 / lower_time,
        "confidence_level": 0.95,
        "source": str(estimates_path.name),
    }


def load_samples(samples_path):
    if not samples_path.exists():
        return []
    rows = []
    with samples_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def classify_throughput_lower_bound(ratio_value):
    if ratio_value is None:
        return None
    if ratio_value < equivalence_lower_bound_min:
        return "regression_exceeds_equivalence_margin"
    return "equivalent_within_margin"


critical_regimes = []
for label in thread_labels:
    regime_id = f"persistent_concurrent_write_{label}"
    run_dir = output_dir / label
    artifacts = {
        "provenance_json": f"{label}/provenance.json",
        "samples_jsonl": f"{label}/samples.jsonl",
        "criterion_stdout_log": f"{label}/criterion_stdout.log",
        "rch_status_trace_jsonl": f"{label}/rch_status.jsonl",
        "criterion_measurement_csqlite_json": f"{label}/criterion_measurement_csqlite_concurrent_persistent.json",
        "criterion_measurement_fsqlite_json": f"{label}/criterion_measurement_frankensqlite_concurrent_persistent.json",
    }
    artifact_paths = {name: output_dir / relpath for name, relpath in artifacts.items()}
    missing_artifacts = [name for name, path in artifact_paths.items() if not path.exists()]
    has_any_artifact = any(path.exists() for path in artifact_paths.values())

    regime = {
        "regime_id": regime_id,
        "thread_label": label,
        "concurrency": int(label[:-1]),
        "artifacts": artifacts,
        "missing_artifacts": missing_artifacts,
        "critical_surface_primary": True,
    }

    if not has_any_artifact:
        regime["coverage_state"] = "no_data"
        regime["verdict"] = "no_data"
        regime["measured_reasons"] = ["no persistent pack artifacts were captured for this regime"]
        critical_regimes.append(regime)
        continue

    sample_rows = load_samples(artifact_paths["samples_jsonl"])
    sqlite_rows = [row for row in sample_rows if row.get("engine") == "sqlite3"]
    fsqlite_rows = [row for row in sample_rows if row.get("engine") == "fsqlite_mvcc"]

    regime["sample_counts"] = {
        "sqlite3": len(sqlite_rows),
        "fsqlite_mvcc": len(fsqlite_rows),
    }

    if not sample_rows:
        regime["coverage_state"] = "incomplete"
        regime["verdict"] = "incomplete"
        regime["measured_reasons"] = ["samples.jsonl is missing or empty, so the regime has no comparable latency surface"]
        critical_regimes.append(regime)
        continue

    if not sqlite_rows or not fsqlite_rows:
        missing_engine = "sqlite3" if not sqlite_rows else "fsqlite_mvcc"
        regime["coverage_state"] = "incomplete"
        regime["verdict"] = "incomplete"
        regime["measured_reasons"] = [f"samples.jsonl is missing comparator rows for `{missing_engine}`"]
        critical_regimes.append(regime)
        continue

    sqlite_latency = {
        metric: median_value([nested_get(row, "latency_us", f"{metric}_us") for row in sqlite_rows])
        for metric in ("p50", "p95", "p99", "max")
    }
    fsqlite_latency = {
        metric: median_value([nested_get(row, "latency_us", f"{metric}_us") for row in fsqlite_rows])
        for metric in ("p50", "p95", "p99", "max")
    }
    latency_ratios = {
        metric: ratio(fsqlite_latency[metric], sqlite_latency[metric])
        for metric in ("p50", "p95", "p99")
    }
    phase_metric_rows = [row for row in fsqlite_rows if isinstance(row.get("phase_metrics"), dict)]
    wall_audit_rows = [
        row for row in fsqlite_rows
        if isinstance(row.get("operation_wall_time_audit"), dict)
    ]
    phase_metrics_medians = {
        "arrival_wait_p95_us": median_nested(phase_metric_rows, "phase_metrics", "hist_arrival_wait", "p95"),
        "wal_backend_lock_wait_p99_us": median_nested(phase_metric_rows, "phase_metrics", "hist_wal_backend_lock_wait", "p99"),
        "wal_append_p99_us": median_nested(phase_metric_rows, "phase_metrics", "hist_wal_append", "p99"),
        "phase_b_p99_us": median_nested(phase_metric_rows, "phase_metrics", "hist_phase_b", "p99"),
        "waiter_epoch_wait_p99_us": median_nested(phase_metric_rows, "phase_metrics", "hist_waiter_epoch_wait", "p99"),
        "wake_timeout_median": median_nested(phase_metric_rows, "phase_metrics", "wake_reasons", "timeout"),
        "wake_flusher_takeover_median": median_nested(phase_metric_rows, "phase_metrics", "wake_reasons", "flusher_takeover"),
        "wake_notify_median": median_nested(phase_metric_rows, "phase_metrics", "wake_reasons", "notify"),
        "lock_topology_limited_sample_count": sum(1 for row in fsqlite_rows if row.get("lock_topology_limited") is True),
    }
    operation_wall_time_audit_medians = {
        "wall_avg_us_per_operation": median_nested(wall_audit_rows, "operation_wall_time_audit", "wall_time", "avg_us_per_operation"),
        "begin_retry_handoff_avg_us_per_operation": median_nested(wall_audit_rows, "operation_wall_time_audit", "begin_retry_handoff", "avg_us_per_operation"),
        "statement_execute_body_avg_us_per_operation": median_nested(wall_audit_rows, "operation_wall_time_audit", "statement_execute_body", "avg_us_per_operation"),
        "commit_roundtrip_avg_us_per_operation": median_nested(wall_audit_rows, "operation_wall_time_audit", "commit_roundtrip", "avg_us_per_operation"),
        "rollback_cleanup_avg_us_per_operation": median_nested(wall_audit_rows, "operation_wall_time_audit", "rollback_cleanup", "avg_us_per_operation"),
        "retry_backoff_sleep_avg_us_per_operation": median_nested(wall_audit_rows, "operation_wall_time_audit", "retry_backoff_sleep", "avg_us_per_operation"),
        "commit_center_avg_us_per_recorded_commit": median_nested(
            wall_audit_rows,
            "operation_wall_time_audit",
            "measured_commit_sub_buckets",
            "commit_center",
            "avg_us_per_recorded_commit",
        ),
        "post_commit_cleanup_publish_avg_us_per_recorded_commit": median_nested(
            wall_audit_rows,
            "operation_wall_time_audit",
            "measured_commit_sub_buckets",
            "post_commit_cleanup_publish",
            "avg_us_per_recorded_commit",
        ),
        "measured_commit_roundtrip_gap_avg_us_per_recorded_commit": median_nested(
            wall_audit_rows,
            "operation_wall_time_audit",
            "measured_commit_roundtrip_gap",
            "avg_us_per_recorded_commit",
        ),
        "measured_commit_roundtrip_gap_abs_fraction_basis_points": median_nested(
            wall_audit_rows,
            "operation_wall_time_audit",
            "measured_commit_roundtrip_gap",
            "abs_fraction_basis_points",
        ),
        "residual_avg_us_per_operation": median_nested(
            wall_audit_rows,
            "operation_wall_time_audit",
            "residual",
            "avg_us_per_operation",
        ),
        "residual_abs_fraction_basis_points": median_nested(
            wall_audit_rows,
            "operation_wall_time_audit",
            "residual",
            "abs_fraction_basis_points",
        ),
        "retry_stage_begin_retries_median": median_nested(
            wall_audit_rows, "operation_wall_time_audit", "retry_stage_counts", "begin_retries"
        ),
        "retry_stage_body_retries_median": median_nested(
            wall_audit_rows, "operation_wall_time_audit", "retry_stage_counts", "body_retries"
        ),
        "retry_stage_commit_retries_median": median_nested(
            wall_audit_rows, "operation_wall_time_audit", "retry_stage_counts", "commit_retries"
        ),
        "retry_stage_duplicate_after_retry_exits_median": median_nested(
            wall_audit_rows,
            "operation_wall_time_audit",
            "retry_stage_counts",
            "duplicate_after_retry_exits",
        ),
        "retry_stage_total_retries_median": median_nested(
            wall_audit_rows, "operation_wall_time_audit", "retry_stage_counts", "total_retries"
        ),
    }

    regime["latency_medians_us"] = {
        "sqlite3": sqlite_latency,
        "fsqlite_mvcc": fsqlite_latency,
    }
    regime["latency_ratio_vs_sqlite"] = latency_ratios
    regime["phase_metrics_medians"] = phase_metrics_medians
    regime["operation_wall_time_audit_sample_count"] = len(wall_audit_rows)
    regime["operation_wall_time_audit_medians"] = operation_wall_time_audit_medians

    throughput_ratio = None
    throughput_lower_bound_ratio = None
    comparator_state = "missing_measurement_only_criterion_comparator"
    total_rows = int(label[:-1]) * 1000
    sqlite_interval = measurement_only_throughput_interval(
        artifact_paths["criterion_measurement_csqlite_json"], total_rows
    )
    fsqlite_interval = measurement_only_throughput_interval(
        artifact_paths["criterion_measurement_fsqlite_json"], total_rows
    )
    if sqlite_interval is not None and fsqlite_interval is not None:
        throughput_ratio = ratio(fsqlite_interval["mid"], sqlite_interval["mid"])
        # Throughput decreases as Criterion's mean-time estimate increases.
        # The fail-closed comparison therefore uses the candidate's 95% lower
        # throughput bound divided by SQLite's 95% upper throughput bound.
        throughput_lower_bound_ratio = ratio(fsqlite_interval["low"], sqlite_interval["high"])
        comparator_state = "same_pack_criterion_measurement_only_available"
        regime["throughput_midpoint_elem_per_sec"] = {
            "sqlite3": sqlite_interval["mid"],
            "fsqlite_mvcc": fsqlite_interval["mid"],
        }
        regime["throughput_interval_elem_per_sec"] = {
            "sqlite3": sqlite_interval,
            "fsqlite_mvcc": fsqlite_interval,
        }
    else:
        regime["throughput_midpoint_elem_per_sec"] = {
            "sqlite3": sqlite_interval["mid"] if sqlite_interval else None,
            "fsqlite_mvcc": fsqlite_interval["mid"] if fsqlite_interval else None,
        }

    throughput_band = classify_throughput_lower_bound(throughput_lower_bound_ratio)
    regime["throughput_ratio_vs_sqlite"] = throughput_ratio
    regime["throughput_ratio_95pct_lower_bound_vs_sqlite"] = throughput_lower_bound_ratio
    regime["throughput_band"] = throughput_band
    regime["comparator_state"] = comparator_state

    collapse_reasons = []
    if fsqlite_latency["p95"] is not None and fsqlite_latency["p95"] >= tail_collapse_p95_us:
        collapse_reasons.append(f"median p95 latency {int(fsqlite_latency['p95'])}us >= collapse threshold {tail_collapse_p95_us}us")
    if fsqlite_latency["p99"] is not None and fsqlite_latency["p99"] >= tail_collapse_p99_us:
        collapse_reasons.append(f"median p99 latency {int(fsqlite_latency['p99'])}us >= collapse threshold {tail_collapse_p99_us}us")
    if fsqlite_latency["max"] is not None and fsqlite_latency["max"] >= tail_collapse_max_us:
        collapse_reasons.append(f"median max latency {int(fsqlite_latency['max'])}us >= collapse threshold {tail_collapse_max_us}us")
    if phase_metrics_medians["wal_append_p99_us"] is not None and phase_metrics_medians["wal_append_p99_us"] >= wal_append_collapse_p99_us:
        collapse_reasons.append(
            f"wal_append p99 median {int(phase_metrics_medians['wal_append_p99_us'])}us >= collapse threshold {wal_append_collapse_p99_us}us"
        )
    if phase_metrics_medians["phase_b_p99_us"] is not None and phase_metrics_medians["phase_b_p99_us"] >= phase_b_collapse_p99_us:
        collapse_reasons.append(
            f"phase_B p99 median {int(phase_metrics_medians['phase_b_p99_us'])}us >= collapse threshold {phase_b_collapse_p99_us}us"
        )
    regime["collapse_override_reasons"] = collapse_reasons
    regime["collapse_override_applies"] = bool(collapse_reasons)

    measured_reasons = [
        (
            "Criterion bootstrap 95% conservative throughput ratio lower bound versus same-pack sqlite3 is "
            f"{format_ratio(throughput_lower_bound_ratio)} (required >= {equivalence_lower_bound_min:.2f}x; "
            f"declared maximum drop {equivalence_margin_max_drop:.0%})"
            if throughput_lower_bound_ratio is not None
            else "same-pack measurement-only Criterion throughput comparator is missing"
        ),
        f"measurement-only Criterion throughput midpoint ratio versus same-pack sqlite3 is {format_ratio(throughput_ratio)}" if throughput_ratio is not None else "measurement-only Criterion throughput midpoint is unavailable",
        f"median p50 latency is {format_ratio(latency_ratios['p50'])} vs same-pack sqlite3",
        f"median p95 latency is {format_ratio(latency_ratios['p95'])} vs same-pack sqlite3",
        f"median p99 latency is {format_ratio(latency_ratios['p99'])} vs same-pack sqlite3",
        f"wal_append p99 median {int(phase_metrics_medians['wal_append_p99_us'])}us" if phase_metrics_medians["wal_append_p99_us"] is not None else "wal_append p99 median unavailable",
        f"phase_B p99 median {int(phase_metrics_medians['phase_b_p99_us'])}us" if phase_metrics_medians["phase_b_p99_us"] is not None else "phase_B p99 median unavailable",
        (
            f"lock_topology_limited remained false in all captured MVCC samples"
            if phase_metrics_medians["lock_topology_limited_sample_count"] == 0
            else f"lock_topology_limited was true in {phase_metrics_medians['lock_topology_limited_sample_count']} captured MVCC samples"
        ),
    ]
    if wall_audit_rows:
        measured_reasons.append(
            "end-to-end wall avg/op median {} with begin/retry-handoff {}, statement body {}, commit roundtrip {}, rollback cleanup {}, retry backoff {}, residual {} ({} bp of wall)".format(
                format_optional_us(operation_wall_time_audit_medians["wall_avg_us_per_operation"]),
                format_optional_us(operation_wall_time_audit_medians["begin_retry_handoff_avg_us_per_operation"]),
                format_optional_us(operation_wall_time_audit_medians["statement_execute_body_avg_us_per_operation"]),
                format_optional_us(operation_wall_time_audit_medians["commit_roundtrip_avg_us_per_operation"]),
                format_optional_us(operation_wall_time_audit_medians["rollback_cleanup_avg_us_per_operation"]),
                format_optional_us(operation_wall_time_audit_medians["retry_backoff_sleep_avg_us_per_operation"]),
                format_signed_us(operation_wall_time_audit_medians["residual_avg_us_per_operation"]),
                "n/a"
                if operation_wall_time_audit_medians["residual_abs_fraction_basis_points"] is None
                else int(operation_wall_time_audit_medians["residual_abs_fraction_basis_points"]),
            )
        )
        measured_reasons.append(
            "measured commit center/post-commit medians are {commit_center}/{post_commit} per recorded commit; commit roundtrip gap is {commit_gap} ({commit_gap_bp} bp of roundtrip), which captures commit-side wall time the old commit-center-only view hid".format(
                commit_center=format_optional_us(
                    operation_wall_time_audit_medians["commit_center_avg_us_per_recorded_commit"]
                ),
                post_commit=format_optional_us(
                    operation_wall_time_audit_medians["post_commit_cleanup_publish_avg_us_per_recorded_commit"]
                ),
                commit_gap=format_signed_us(
                    operation_wall_time_audit_medians["measured_commit_roundtrip_gap_avg_us_per_recorded_commit"]
                ),
                commit_gap_bp="n/a"
                if operation_wall_time_audit_medians["measured_commit_roundtrip_gap_abs_fraction_basis_points"] is None
                else int(operation_wall_time_audit_medians["measured_commit_roundtrip_gap_abs_fraction_basis_points"]),
            )
        )
        measured_reasons.append(
            "retry stage count medians begin/body/commit/duplicate-after-retry/total = {}".format(
                format_retry_stage_counts(operation_wall_time_audit_medians)
            )
        )
    else:
        measured_reasons.append("operation wall-time audit is unavailable in captured MVCC samples")
    if missing_artifacts:
        regime["coverage_state"] = "incomplete"
        regime["verdict"] = "incomplete"
        measured_reasons.append(f"expected pack artifacts are missing: {', '.join(missing_artifacts)}")
    elif throughput_lower_bound_ratio is None:
        regime["coverage_state"] = "incomplete"
        regime["verdict"] = "incomplete"
        measured_reasons.append("measurement-only Criterion 95% conservative comparator missing keeps the regime non-green even if samples.jsonl exists")
    else:
        regime["coverage_state"] = "complete"
        if throughput_band == "regression_exceeds_equivalence_margin":
            regime["verdict"] = "regression_exceeds_equivalence_margin"
        elif collapse_reasons:
            regime["verdict"] = "collapse_red"
            measured_reasons.append("conservative throughput equivalence alone is not sufficient because collapse override applies")
        else:
            regime["verdict"] = "pass"
    regime["measured_reasons"] = measured_reasons
    critical_regimes.append(regime)


def conservative_scaling_lower_bound(numerator_regime, denominator_regime):
    """Return the 95% conservative scaling ratio for two MVCC thread regimes."""
    numerator = nested_get(numerator_regime, "throughput_interval_elem_per_sec", "fsqlite_mvcc")
    denominator = nested_get(denominator_regime, "throughput_interval_elem_per_sec", "fsqlite_mvcc")
    if numerator is None or denominator is None:
        return None
    return ratio(numerator.get("low"), denominator.get("high"))


regime_by_label = {regime["thread_label"]: regime for regime in critical_regimes}
scaling_gates = []
for numerator_label, denominator_label, gate_id in (
    ("8t", "1t", "throughput_scaling_8_over_1"),
    ("16t", "8t", "throughput_scaling_16_over_8"),
):
    numerator_regime = regime_by_label.get(numerator_label)
    denominator_regime = regime_by_label.get(denominator_label)
    scaling_lower_bound = None
    if numerator_regime is not None and denominator_regime is not None:
        scaling_lower_bound = conservative_scaling_lower_bound(numerator_regime, denominator_regime)
    if scaling_lower_bound is None:
        scaling_verdict = "incomplete"
    elif scaling_lower_bound < equivalence_lower_bound_min:
        scaling_verdict = "regression_exceeds_equivalence_margin"
    else:
        scaling_verdict = "pass"
    scaling_gates.append({
        "gate_id": gate_id,
        "numerator_regime": numerator_label,
        "denominator_regime": denominator_label,
        "metric": "fsqlite_mvcc_throughput",
        "criterion_confidence_level": 0.95,
        "conservative_ratio_lower_bound": scaling_lower_bound,
        "required_minimum": equivalence_lower_bound_min,
        "declared_maximum_drop": equivalence_margin_max_drop,
        "verdict": scaling_verdict,
        "reason": (
            "candidate 95% lower throughput bound divided by denominator 95% upper throughput bound"
            if scaling_lower_bound is not None
            else "both release-regime measurement-only Criterion intervals are required"
        ),
    })

pack_verdict = "pass"
lane_verdicts = [regime["verdict"] for regime in critical_regimes]
scaling_verdicts = [gate["verdict"] for gate in scaling_gates]
if not critical_regimes or all(verdict == "no_data" for verdict in lane_verdicts):
    pack_verdict = "no_data"
elif any(verdict in {"no_data", "incomplete"} for verdict in lane_verdicts) or any(verdict == "incomplete" for verdict in scaling_verdicts):
    pack_verdict = "incomplete"
elif any(verdict in {"regression_exceeds_equivalence_margin", "collapse_red"} for verdict in lane_verdicts) or any(verdict == "regression_exceeds_equivalence_margin" for verdict in scaling_verdicts):
    pack_verdict = "fail"

honest_gate_summary = {
    "verdict": pack_verdict,
    "critical_regime_count": len(critical_regimes),
    "complete_regime_count": sum(1 for regime in critical_regimes if regime["coverage_state"] == "complete"),
    "incomplete_regime_count": sum(1 for regime in critical_regimes if regime["coverage_state"] == "incomplete"),
    "no_data_regime_count": sum(1 for regime in critical_regimes if regime["coverage_state"] == "no_data"),
    "red_regimes": [regime["regime_id"] for regime in critical_regimes if regime["verdict"] in {"regression_exceeds_equivalence_margin", "collapse_red"}],
    "incomplete_regimes": [regime["regime_id"] for regime in critical_regimes if regime["verdict"] == "incomplete"],
    "no_data_regimes": [regime["regime_id"] for regime in critical_regimes if regime["verdict"] == "no_data"],
    "equivalence_margin_max_drop": equivalence_margin_max_drop,
    "equivalence_lower_bound_min": equivalence_lower_bound_min,
    "criterion_confidence_level": 0.95,
    "scaling_gates": scaling_gates,
    "failed_scaling_gates": [gate["gate_id"] for gate in scaling_gates if gate["verdict"] == "regression_exceeds_equivalence_margin"],
    "incomplete_scaling_gates": [gate["gate_id"] for gate in scaling_gates if gate["verdict"] == "incomplete"],
    "rule": "1t, 8t, and 16t must each meet the 5% equivalence margin at the Criterion bootstrap 95% conservative lower bound; 8-over-1 and 16-over-8 scaling use the same lower-bound rule; missing evidence and collapse remain non-green",
}

scorecard = {
    "schema_version": "bd-db300.persistent_phase_pack_scorecard.v5",
    "bead_id": bead_id,
    "run_id": output_dir.name,
    "entrypoint": "scripts/capture_persistent_phase_pack.sh",
    "pack_role": "honest_gate_phase_pack",
    "baseline_comparator": "sqlite3_same_pack",
    "shadow_lineage": "none",
    "critical_surface_primary": True,
    "aggregate_views_secondary_only": True,
    "equivalence_margin": {
        "maximum_allowed_drop": equivalence_margin_max_drop,
        "required_conservative_ratio_lower_bound": equivalence_lower_bound_min,
        "criterion_confidence_level": 0.95,
        "comparison": "candidate throughput 95% lower bound / comparator throughput 95% upper bound",
    },
    "comparator_contract": {
        "baseline_comparator": "sqlite3_same_pack",
        "comparator_engine": "sqlite3",
        "comparator_scope": "same thread regime, same pack",
        "aggregate_rows_are_secondary": True,
    },
    "operation_wall_time_audit_disclosure": {
        "avg_us_per_operation": "median_of_per_sample_averages",
        "avg_us_per_recorded_commit": "median_of_per_sample_averages",
        "retry_stage_counts": "median_of_per_sample_counts",
        "not_a_per_operation_quantile": True,
    },
    "honest_gate_summary": honest_gate_summary,
    "critical_regimes": critical_regimes,
    "scaling_gates": scaling_gates,
    "warmup_measurement_disclosure": {
        "samples_include_warmup": True,
        "samples_include_measurement": True,
        "authoritative_for": [
            "phase-attribution distributions",
            "wake-reason distributions",
        ],
        "measurement_only_criterion_authoritative_for": [
            "same-pack sqlite3 versus fsqlite_mvcc headline throughput",
            "throughput confidence intervals",
        ],
        "not_authoritative_for": [
            "headline throughput",
        ],
    },
}
scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n", encoding="utf-8")

manifest = {
    "schema_version": "bd-db300.persistent_phase_pack_manifest.v4",
    "bead_id": bead_id,
    "run_id": output_dir.name,
    "entrypoint": "scripts/capture_persistent_phase_pack.sh",
    "scorecard_json": scorecard_path.name,
    "summary_md": summary_path.name,
    "citation_receipt_json": "provenance/citation_receipt.json",
    "honest_gate_summary": honest_gate_summary,
    "scaling_gates": scaling_gates,
    "critical_regimes": [
        {
            "regime_id": regime["regime_id"],
            "verdict": regime["verdict"],
            "coverage_state": regime["coverage_state"],
            "artifacts": regime["artifacts"],
            "missing_artifacts": regime["missing_artifacts"],
        }
        for regime in critical_regimes
    ],
}
manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

summary_lines = [
    f"# {bead_id} Persistent Phase Pack",
    "",
    f"- run_id: `{output_dir.name}`",
    "- baseline_comparator: same-pack `sqlite3` measurement-only Criterion estimates + phase-attribution samples",
    f"- critical_regimes: `{', '.join(regime['regime_id'] for regime in critical_regimes)}`",
    "- disclosure: `samples.jsonl` mixes warmup and measurement; use it only for phase and wake-reason truth. Headline throughput derives solely from copied Criterion `base/estimates.json`.",
    "",
    "## Honest Gate Summary",
    "",
    f"- verdict: `{pack_verdict}`",
    f"- complete_regime_count: `{honest_gate_summary['complete_regime_count']}`",
    f"- incomplete_regime_count: `{honest_gate_summary['incomplete_regime_count']}`",
    f"- no_data_regime_count: `{honest_gate_summary['no_data_regime_count']}`",
    f"- policy: a uniform {equivalence_margin_max_drop:.0%} maximum regression is accepted only when the Criterion bootstrap 95% conservative ratio lower bound is at least `{equivalence_lower_bound_min:.2f}x`.",
    "- 1t, 8t, and 16t remain individually visible; no aggregate green can hide a red or incomplete regime.",
    "- 8-over-1 and 16-over-8 MVCC scaling use the same conservative lower-bound contract.",
    "- comparator-missing or partial packs are non-green and report `no_data`/`incomplete` instead of silently grading the regime.",
    "- conservative equivalence is not sufficient when the collapse override applies.",
    "",
    "## Critical Regimes (Primary Surface)",
    "",
    "| Regime | Verdict | Coverage | Midpoint throughput vs sqlite | 95% lower-bound ratio vs sqlite | Equivalence result | Collapse override | SQLite p50/p95/p99 (us) | FrankenSQLite p50/p95/p99 (us) | WAL append p99 (us) | Phase B p99 (us) | lock_topology_limited true samples |",
    "|--------|---------|----------|-------------------------------|----------------------------------|--------------------|-------------------|-------------------------|---------------------------------|---------------------|------------------|------------------------------------|",
]

for regime in critical_regimes:
    collapse_override = "yes" if regime.get("collapse_override_applies") else "no"
    phase_metrics = regime.get("phase_metrics_medians", {})
    summary_lines.append(
        "| {regime_id} | {verdict} | {coverage} | {throughput_ratio} | {throughput_lower_bound} | {throughput_band} | {collapse_override} | {sqlite_latency} | {fsqlite_latency} | {wal_append_p99} | {phase_b_p99} | {lock_topology_count} |".format(
            regime_id=regime["regime_id"],
            verdict=regime["verdict"],
            coverage=regime["coverage_state"],
            throughput_ratio=format_ratio(regime.get("throughput_ratio_vs_sqlite")),
            throughput_lower_bound=format_ratio(regime.get("throughput_ratio_95pct_lower_bound_vs_sqlite")),
            throughput_band=regime.get("throughput_band") or "n/a",
            collapse_override=collapse_override,
            sqlite_latency=format_us_triplet(regime.get("latency_medians_us", {}).get("sqlite3")),
            fsqlite_latency=format_us_triplet(regime.get("latency_medians_us", {}).get("fsqlite_mvcc")),
            wal_append_p99="n/a" if phase_metrics.get("wal_append_p99_us") is None else int(phase_metrics["wal_append_p99_us"]),
            phase_b_p99="n/a" if phase_metrics.get("phase_b_p99_us") is None else int(phase_metrics["phase_b_p99_us"]),
            lock_topology_count=phase_metrics.get("lock_topology_limited_sample_count", "n/a"),
        )
    )

summary_lines.extend([
    "",
    "## Scaling Gates",
    "",
    "| Gate | Conservative 95% lower-bound ratio | Required minimum | Verdict |",
    "|------|-------------------------------------|------------------|---------|",
])
for gate in scaling_gates:
    summary_lines.append(
        "| {gate_id} | {lower_bound} | {required_minimum:.2f}x | {verdict} |".format(
            gate_id=gate["gate_id"],
            lower_bound=format_ratio(gate["conservative_ratio_lower_bound"]),
            required_minimum=gate["required_minimum"],
            verdict=gate["verdict"],
        )
    )

summary_lines.extend([
    "",
    "## End-to-End Wall-Time Audit (FrankenSQLite Median-of-Per-Sample Averages)",
    "",
    "- disclosure: `avg/op` and `avg/recorded` columns below are medians of per-sample averages from captured MVCC samples; they are not per-operation p50/p95 quantiles.",
    "",
    "| Regime | Wall avg/op | Begin/retry-handoff avg/op | Commit roundtrip avg/op | Commit center avg/recorded | Post-commit avg/recorded | Commit roundtrip gap avg/recorded | Rollback avg/op | Backoff avg/op | Residual avg/op | Retry medians begin/body/commit/dup/total |",
    "|--------|-------------|----------------------------|-------------------------|----------------------------|--------------------------|-----------------------------------|-----------------|----------------|-----------------|-------------------------------------------|",
])
for regime in critical_regimes:
    wall_audit = regime.get("operation_wall_time_audit_medians", {})
    summary_lines.append(
        "| {regime_id} | {wall_avg} | {begin_retry_handoff} | {commit_roundtrip} | {commit_center} | {post_commit} | {commit_gap} | {rollback} | {backoff} | {residual} | {retry_counts} |".format(
            regime_id=regime["regime_id"],
            wall_avg=format_optional_us(wall_audit.get("wall_avg_us_per_operation")),
            begin_retry_handoff=format_optional_us(
                wall_audit.get("begin_retry_handoff_avg_us_per_operation")
            ),
            commit_roundtrip=format_optional_us(wall_audit.get("commit_roundtrip_avg_us_per_operation")),
            commit_center=format_optional_us(wall_audit.get("commit_center_avg_us_per_recorded_commit")),
            post_commit=format_optional_us(wall_audit.get("post_commit_cleanup_publish_avg_us_per_recorded_commit")),
            commit_gap=format_signed_us(wall_audit.get("measured_commit_roundtrip_gap_avg_us_per_recorded_commit")),
            rollback=format_optional_us(wall_audit.get("rollback_cleanup_avg_us_per_operation")),
            backoff=format_optional_us(wall_audit.get("retry_backoff_sleep_avg_us_per_operation")),
            residual=format_signed_us(wall_audit.get("residual_avg_us_per_operation")),
            retry_counts=format_retry_stage_counts(wall_audit),
        )
    )

summary_lines.extend(["", "## Regime Notes", ""])
for regime in critical_regimes:
    summary_lines.append(f"- `{regime['regime_id']}`: {regime['verdict']} ({regime['coverage_state']})")
    for note in regime["measured_reasons"]:
        summary_lines.append(f"  - {note}")
    if regime.get("missing_artifacts"):
        summary_lines.append(f"  - missing_artifacts: {', '.join(regime['missing_artifacts'])}")

summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
PY
}

hash_artifacts() {
    (
        cd "$OUTPUT_DIR"
        find . -type f ! -name "$(basename "$HASHES_TXT")" -print0 \
            | sort -z \
            | xargs -0 sha256sum > "$(basename "$HASHES_TXT")"
    )
}

write_rerun_script() {
    cat > "$RERUN_SH" <<RERUN_EOF
#!/usr/bin/env bash
# One-command rerun of the persistent phase-attribution pack after supplying
# a fresh nonce; Criterion estimates are exported into the fresh phase directory.
# Original capture: ${TIMESTAMP}
# Bead: ${BEAD_ID}
set -euo pipefail
cd "$PROJECT_ROOT"
export FSQLITE_USE_RCH=1
export RCH_REQUIRE_REMOTE=1
export RCH_NO_SELF_HEALING=1
export RCH_BIN="\${RCH_BIN:-${RCH_BIN}}"
if [[ -n "\${CARGO_TARGET_DIR:-${CARGO_TARGET_DIR:-}}" ]]; then
    export CARGO_TARGET_DIR="\${CARGO_TARGET_DIR:-${CARGO_TARGET_DIR:-}}"
fi
: "\${RCH_WORKER:?set a freshly verified non-hz1/non-hz2 RCH_WORKER}"
: "\${FSQLITE_RELEASE_FROZEN_COMMIT:?set the frozen 40-hex source commit}"
: "\${FSQLITE_BENCH_BUILD_NONCE:?set a fresh 64-hex build nonce}"
: "\${FSQLITE_RELEASE_CRITERION_SAMPLE_SIZE:?set Criterion sample size}"
: "\${FSQLITE_RELEASE_CRITERION_WARMUP_SECS:?set Criterion warmup seconds}"
: "\${FSQLITE_RELEASE_CRITERION_MEASUREMENT_SECS:?set Criterion measurement seconds}"
exec ./scripts/capture_persistent_phase_pack.sh "\${1:-$OUTPUT_DIR.rerun_\$(date +%Y%m%d_%H%M%S)}"
RERUN_EOF
    chmod +x "$RERUN_SH"
}

main() {
    echo "=== ${BEAD_ID}: Authoritative Persistent Phase-Attribution Pack ==="
    echo "Output: $OUTPUT_DIR"
    echo "Timestamp: $TIMESTAMP"

    if [[ "$SELF_TEST" == "1" ]]; then
        run_synthetic_contract_checks
        return
    fi

    validate_citation_contract
    mkdir -p "$PROVENANCE_DIR"

    echo "--- Capturing environment provenance ---"
    write_environment_provenance
    ensure_bench_binary

    echo ""
    echo "--- Citation RCH scheduler-isolation contract ---"
    echo "Daemon-authoritative worker: $ACTUAL_WORKER"
    echo "Daemon-reported host: $ACTUAL_HOST"
    echo "RCH scheduler trace: $BUILD_STATUS_TRACE"

    for thread_count in "${THREAD_COUNTS[@]}"; do
        run_persistent_bench "$thread_count"
    done
    write_citation_receipt

    echo ""
    echo "--- Rendering honest-gate reports ---"
    render_reports
    hash_artifacts
    write_rerun_script

    echo ""
    echo "=== Pack capture complete ==="
    echo "Output directory: $OUTPUT_DIR"
    echo "Scorecard: $SCORECARD_JSON"
    echo "Summary: $SUMMARY_MD"
    echo "Manifest: $MANIFEST_JSON"
    echo "Rerun: $RERUN_SH"
}

main "$@"
