#!/usr/bin/env bash
# Capture a reproducible c1 evidence + scorecard pack for:
#   - bd-db300.1.7.1 authoritative low-concurrency evidence refresh
#   - bd-db300.4.5.9 operator-grade c1 e2e comparison scripts and logs
#
# The pack keeps the original raw benchmark and hot-profile artifacts, then adds:
#   - structured lifecycle events
#   - machine-readable command ledger
#   - explicit provenance bundle
#   - scorecard JSON answering which c1 cells are still below target
#   - human-readable Markdown summary

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PRIMARY_BEAD_ID="${PRIMARY_BEAD_ID:-bd-db300.4.5.9}"
COVERED_BEADS="${COVERED_BEADS:-bd-db300.1.7.1,bd-db300.4.5.9}"
SCENARIO_ID="${SCENARIO_ID:-C1-E2E-COMPARISON}"
SEED="${SEED:-459}"
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${RUN_ID:-${PRIMARY_BEAD_ID}-${TIMESTAMP_UTC}-${SEED}}"
TRACE_ID="${TRACE_ID:-trace-${RUN_ID}}"
OUTPUT_DIR="${1:-${PROJECT_ROOT}/artifacts/${PRIMARY_BEAD_ID}/${RUN_ID}}"

die() {
  echo "[${PRIMARY_BEAD_ID}] FATAL: $*" >&2
  exit 2
}

# Fail closed on an output path that could silently land somewhere else or
# destroy an existing pack. Mirrors the citation guard in
# capture_persistent_phase_pack.sh, which this script previously lacked:
#
#   * absolute-only — `$1` is used verbatim here (no PROJECT_ROOT prefixing, as
#     the persistent script does), so a relative path resolves against the
#     caller's CWD and the pack lands outside the intended evidence tree;
#   * create-new — this script truncates events/commands JSONL and rewrites the
#     scorecard, manifest, summary, build metadata and hashes in place, so a
#     rerun over a populated directory destroys the prior evidence pack.
#
# `RENDER_ONLY=1` re-renders an existing pack by design and is therefore the one
# mode allowed to target a populated directory; it still may not target a
# relative path or a non-directory.
# Classify an output path as one of: absent | non-directory | dir-empty |
# dir-populated.
#
# The optional third argument supplies a simulated state so the self-test can
# cover every branch without creating or removing a single file — this
# repository forbids deletion outright, so a test that cleans up after itself
# is not an option, and one that does not would leave dirt behind. The
# simulated value is honoured ONLY under FSQLITE_CAPTURE_SELF_TEST=1, which
# exits before any capture work, so it cannot weaken a real run even if a
# caller passes a third argument by mistake.
probe_output_path_state() {
  local candidate="$1"
  local simulated="${2:-}"
  if [[ -n "$simulated" && "${FSQLITE_CAPTURE_SELF_TEST:-0}" == "1" ]]; then
    printf '%s' "$simulated"
    return 0
  fi
  if [[ ! -e "$candidate" ]]; then
    printf 'absent'
  elif [[ ! -d "$candidate" ]]; then
    printf 'non-directory'
  elif [[ -z "$(find "$candidate" -mindepth 1 -print -quit)" ]]; then
    printf 'dir-empty'
  else
    printf 'dir-populated'
  fi
}

validate_output_dir() {
  local candidate="$1"
  local render_only="${2:-0}"
  local simulated_state="${3:-}"
  [[ -n "$candidate" ]] || die "output directory must not be empty"
  [[ "$candidate" == /* ]] \
    || die "output directory must be absolute so the pack cannot land in a caller-relative path: ${candidate}"
  local state
  state="$(probe_output_path_state "$candidate" "$simulated_state")"
  [[ "$state" != "non-directory" ]] \
    || die "output path exists but is not a directory: ${candidate}"
  if [[ "$render_only" != "1" ]]; then
    [[ "$state" != "dir-populated" ]] \
      || die "output directory must be empty; refusing to overwrite an existing evidence pack: ${candidate}"
  fi
}

require_lower_hex() {
  local value="$1"
  local length="$2"
  local label="$3"
  [[ "$value" =~ ^[0-9a-f]{${length}}$ ]] \
    || die "${label} must be exactly ${length} lowercase hexadecimal characters"
}

# Git accessors are indirected so the release contract's refusal paths can be
# exercised without mutating this repository. The `+x` test honours an injected
# empty string, which is what an unresolvable HEAD and a clean worktree look
# like respectively.
release_head_commit() {
  if [[ -n "${C1_TEST_HEAD_COMMIT+x}" ]]; then
    printf '%s' "$C1_TEST_HEAD_COMMIT"
    return 0
  fi
  git -C "${PROJECT_ROOT}" rev-parse HEAD 2>/dev/null || true
}

# No `|| true` here, deliberately. Empty output means "clean", so masking a
# failed `git status` — a missing worktree, a contended index.lock, a
# permission error — would render it as a clean checkout and pass the release
# gate on the strength of a command that never ran. The caller distinguishes
# "empty because clean" from "empty because the command failed".
release_worktree_status() {
  if [[ -n "${C1_TEST_WORKTREE_STATUS+x}" ]]; then
    printf '%s' "$C1_TEST_WORKTREE_STATUS"
    return 0
  fi
  git -C "${PROJECT_ROOT}" status --porcelain --untracked-files=all
}

# Release mode is opt-in: setting FSQLITE_RELEASE_FROZEN_COMMIT asserts that
# this pack is citation-grade, and every ordinary (non-release) invocation is
# unaffected. Mirrors the source-provenance half of
# capture_persistent_phase_pack.sh's `validate_citation_contract`, which this
# script previously lacked entirely — it recorded `git_sha` and
# `git_dirty_entries` into build_metadata.json but never refused, so a
# scorecard could attest to a dirty tree or to a commit other than the one the
# release cites. Runs before any output directory is created or any benchmark
# is launched.
validate_release_contract() {
  local expected="$1"
  [[ -n "$expected" ]] || die "FSQLITE_RELEASE_FROZEN_COMMIT must not be empty in release mode"
  require_lower_hex "$expected" 40 FSQLITE_RELEASE_FROZEN_COMMIT
  local head_commit
  head_commit="$(release_head_commit)"
  [[ -n "$head_commit" ]] \
    || die "cannot resolve HEAD; release capture requires a Git worktree"
  [[ "$head_commit" == "$expected" ]] \
    || die "HEAD ${head_commit} does not match FSQLITE_RELEASE_FROZEN_COMMIT ${expected}"
  # Declared before assignment: `local status="$(cmd)"` would return the exit
  # status of `local`, not of the command, silently swallowing the failure.
  local worktree_status
  worktree_status="$(release_worktree_status)" \
    || die "cannot read the worktree status; refusing to assume a clean checkout"
  [[ -z "$worktree_status" ]] \
    || die "release source checkout is not clean; refusing to emit uncitable provenance"
}

# Workers whose measurements are not citable. Overridable so the list can track
# the fleet without editing this script; empty entries are ignored.
RELEASE_WORKER_DENYLIST="${FSQLITE_RELEASE_WORKER_DENYLIST:-hz1,hz2}"

# Applied to BOTH the requested worker and the worker the daemon actually
# selected, so a quarantined host cannot enter through the log-parsed side.
require_allowed_remote_worker() {
  local worker="$1"
  [[ -n "$worker" ]] || die "no remote worker was identified"
  local -a denied_workers=()
  IFS=',' read -ra denied_workers <<< "$RELEASE_WORKER_DENYLIST"
  local denied
  for denied in "${denied_workers[@]}"; do
    [[ -n "$denied" ]] || continue
    [[ "$worker" != "$denied" ]] \
      || die "worker ${worker} is quarantined and its measurements are not citable"
  done
}

# The poll interval reaches both `sleep` and the manifest, so a malformed value
# would otherwise surface as a confusing mid-build `sleep` failure or a crash
# while serialising provenance. Positive decimal seconds only.
require_poll_interval() {
  local value="$1"
  [[ "$value" =~ ^([0-9]+|[0-9]*\.[0-9]+)$ ]] \
    || die "FSQLITE_RELEASE_STATUS_POLL_SECONDS must be a positive number of seconds, found: ${value}"
  [[ "$value" =~ ^0*(\.0*)?$ ]] \
    && die "FSQLITE_RELEASE_STATUS_POLL_SECONDS must be greater than zero"
  return 0
}

# Optional corroboration from the build log.
#
# The daemon-authoritative worker now comes from the retained queue trace, not
# from log text. That is deliberate: `rch exec`'s marker grammar is not
# documented anywhere in `rch robot-docs`, and observed transcripts phrase the
# selection as `Selected worker: <id>` rather than the `[RCH] remote <id>` form
# this script previously required. Requiring an unverified marker would have
# made every release run fail on a log-format assumption while the daemon
# itself was reporting the truth.
#
# So: if any recognised marker is present it must be singular and must agree
# with the queue-derived worker; if none is present that is recorded as
# `absent` and the queue trace stands alone. This can only add a refusal, never
# manufacture agreement.
worker_marker_from_rch_log() {
  local log_path="$1"
  local worker worker_count
  worker="$(sed -nE -e 's/.*\[RCH\] remote ([^[:space:]]+) .*/\1/p' \
                    -e 's/^[[:space:]]*Selected worker:[[:space:]]*([^[:space:]]+).*/\1/p' \
            "$log_path" | sort -u)"
  worker_count="$(printf '%s\n' "$worker" | sed '/^$/d' | wc -l | tr -d ' ')"
  if [[ "$worker_count" == "0" ]]; then
    printf 'absent'
    return 0
  fi
  [[ "$worker_count" == "1" ]] \
    || die "build log carries ${worker_count} conflicting worker markers: ${log_path}"
  printf '%s' "$worker"
}

# Daemon state is polled through the same binary the build is offloaded with,
# with self-healing off so a probe cannot itself relocate work.
#
# Two distinct machine interfaces are sampled, because neither alone carries
# what the isolation claim needs:
#
#   * `rch queue --json` is the authoritative job interface. Its
#     `data.active_builds[]` entries carry the FULL job id, `worker_id`,
#     `project_id` and the submitted `command`. This is what identifies our
#     build and proves nothing else was running on the worker.
#   * `rch status --workers --jobs --json` is the only read-only interface that
#     exposes live worker health. `rch workers list --json` was checked and
#     carries static configuration only — `id`, `host`, `user`, `priority`,
#     `tags`, `total_slots` — with no `status` and no `circuit_state`, so it
#     cannot support a health claim.
#
# Both raw streams are retained in the pack. `--no-self-healing` writes an INFO
# line to stderr, so stderr is captured to a separate diagnostics file and is
# never parsed: only stdout enters a trace.
RCH_BIN="${RCH_BIN:-rch}"
RCH_STATUS_POLL_SECONDS="${FSQLITE_RELEASE_STATUS_POLL_SECONDS:-1}"

require_status_tooling() {
  command -v "$RCH_BIN" >/dev/null 2>&1 \
    || die "${RCH_BIN} is required to capture a scheduler-isolation trace"
  command -v python3 >/dev/null 2>&1 \
    || die "python3 is required to evaluate the scheduler-isolation trace"
}

# Job ids are 17-digit integers (observed: 29960766543102031), which exceed the
# 2^53 exactly-representable range of an IEEE-754 double. `jq` preserves an
# untouched literal but silently rounds the moment a value passes through
# arithmetic — `jq -n '29960766543102031 + 0'` yields ...030 — so a jq-based
# comparison is one refactor away from conflating adjacent job ids. Every
# predicate below is evaluated in python3, which has exact integer semantics
# and is already a hard dependency of this script.
capture_queue_snapshot() {
  local trace_path="$1"
  local diagnostics_path="$2"
  "$RCH_BIN" --no-self-healing queue --json >> "$trace_path" 2>> "$diagnostics_path" \
    || die "could not retain an rch queue snapshot in ${trace_path}"
  printf '\n' >> "$trace_path"
}

capture_worker_snapshot() {
  local trace_path="$1"
  local diagnostics_path="$2"
  "$RCH_BIN" --no-self-healing status --workers --jobs --json >> "$trace_path" 2>> "$diagnostics_path" \
    || die "could not retain an rch worker-health snapshot in ${trace_path}"
  printf '\n' >> "$trace_path"
}

# Single snapshot taken once the build process has returned, so the daemon's
# rolling `recent_builds` history has had the chance to record how the job
# actually ended. The polling samples only ever see a job while it is ACTIVE:
# they prove it was alone on the worker, but they cannot say whether it
# finished remotely, was cancelled, or exited non-zero. Truncating rather than
# appending keeps this a single document with a single verdict.
capture_completion_snapshot() {
  local snapshot_path="$1"
  local diagnostics_path="$2"
  "$RCH_BIN" --no-self-healing status --workers --jobs --json > "$snapshot_path" 2>> "$diagnostics_path" \
    || die "could not retain the rch completion snapshot in ${snapshot_path}"
}

# Adjudicate the two retained traces and print the job identity they establish.
#
# What this proves, stated exactly: at every recorded sample, the selected
# worker carried at most one active build, and every build it carried was ours.
# That is SAMPLED isolation at the recorded poll interval, not continuous
# isolation — a foreign job that began and ended entirely between two samples
# is not observable through this interface, and no claim is made that it cannot
# have happened. The manifest records the interval and sample count so a reader
# can size that gap rather than infer a guarantee that was never available.
#
# Race handling inside the window (polling opens before the offload is launched
# and closes when the build process exits):
#
#   * samples taken before the job is scheduled, or after it retires but before
#     the build process exits, legitimately show an idle worker. They cannot
#     violate the per-sample clause and they cannot satisfy the trace either,
#     because a separate clause requires the job to be positively observed at
#     least once. A build never observed at all is refused, not assumed.
#   * a foreign job on the worker at any sample fails the per-sample clause even
#     if it started after ours retired: inside the window that is
#     indistinguishable from overlap.
#
# Identification does not depend on build-log text. The job is whatever the
# daemon reports on the requested worker; its `command` must carry the caller's
# marker, so a build for some other target on that worker is refused rather
# than adopted.
verify_scheduler_isolation_trace() {
  local worker="$1"
  local command_marker="$2"
  local queue_trace="$3"
  local worker_trace="$4"
  python3 - "$worker" "$command_marker" "$queue_trace" "$worker_trace" <<'PY'
import json
import sys

worker, marker, queue_path, worker_path = sys.argv[1:5]


def fail(message):
    raise SystemExit(f"scheduler-isolation trace refused: {message}")


def snapshots(path, expected_command, description):
    """Parse concatenated JSON documents and validate each envelope."""
    with open(path, "r", encoding="utf-8") as handle:
        text = handle.read()
    decoder = json.JSONDecoder()
    index, documents = 0, []
    while index < len(text):
        while index < len(text) and text[index].isspace():
            index += 1
        if index >= len(text):
            break
        try:
            document, index = decoder.raw_decode(text, index)
        except ValueError as error:
            fail(f"{description} sample {len(documents) + 1} is not valid JSON: {error}")
        # Envelope validation is part of the contract: a snapshot that is not a
        # successful response to the exact expected command is not evidence,
        # and silently averaging it in would let an error payload read as an
        # idle worker.
        if not isinstance(document, dict):
            fail(f"{description} sample {len(documents) + 1} is not a JSON object")
        if document.get("api_version") != "1.0":
            fail(
                f"{description} sample {len(documents) + 1} has unexpected api_version "
                f"{document.get('api_version')!r}; refusing an unrecognised schema"
            )
        if document.get("command") != expected_command:
            fail(
                f"{description} sample {len(documents) + 1} answers command "
                f"{document.get('command')!r}, expected {expected_command!r}"
            )
        if document.get("success") is not True:
            fail(f"{description} sample {len(documents) + 1} reports success={document.get('success')!r}")
        if not isinstance(document.get("data"), dict):
            fail(f"{description} sample {len(documents) + 1} carries no data object")
        documents.append(document)
    if not documents:
        fail(f"{description} trace contains no samples")
    return documents


queue_docs = snapshots(queue_path, "queue", "rch queue")
worker_docs = snapshots(worker_path, "status", "rch status")

observed_ids, observed_commands, observed_projects = set(), set(), set()
positive_samples = 0
for position, document in enumerate(queue_docs, start=1):
    active = document["data"].get("active_builds")
    if not isinstance(active, list):
        fail(f"rch queue sample {position} carries no active_builds array")
    on_worker = [entry for entry in active if entry.get("worker_id") == worker]
    if len(on_worker) > 1:
        ids = sorted(str(entry.get("id")) for entry in on_worker)
        fail(
            f"rch queue sample {position} shows {len(on_worker)} concurrent builds on "
            f"{worker} (ids {ids}); the measurement was not isolated"
        )
    for entry in on_worker:
        # Exact integer identity. `str(entry.get("id"))` alone would turn a
        # missing id into the literal string "None" and adopt it as a job
        # identity, so the type is checked before the value is taken. bool is
        # excluded because it is an int subclass in Python.
        raw_id = entry.get("id")
        if isinstance(raw_id, bool) or not isinstance(raw_id, int):
            fail(
                f"rch queue sample {position} reports a build on {worker} whose id is "
                f"{raw_id!r}, not an integer; refusing an unidentifiable job"
            )
        observed_ids.add(str(raw_id))
        observed_commands.add(str(entry.get("command", "")))
        observed_projects.add(str(entry.get("project_id", "")))
        positive_samples += 1

if not observed_ids:
    fail(
        f"no build was ever observed on {worker} across {len(queue_docs)} queue sample(s); "
        "a build that was never sampled cannot be attested as isolated"
    )
if len(observed_ids) > 1:
    fail(f"{worker} carried more than one distinct job across the window: {sorted(observed_ids)}")

job_id = observed_ids.pop()
command = observed_commands.pop() if len(observed_commands) == 1 else ""
if marker and marker not in command:
    fail(
        f"the build observed on {worker} (job {job_id}) runs `{command}`, which does not "
        f"carry the expected marker `{marker}`; refusing to attribute a foreign build"
    )

for position, document in enumerate(worker_docs, start=1):
    daemon = document["data"].get("daemon")
    if not isinstance(daemon, dict) or not isinstance(daemon.get("workers"), list):
        fail(f"rch status sample {position} carries no data.daemon.workers array")
    matching = [entry for entry in daemon["workers"] if entry.get("id") == worker]
    if len(matching) != 1:
        fail(f"rch status sample {position} does not identify exactly one worker named {worker}")
    entry = matching[0]
    if entry.get("status") != "healthy" or entry.get("circuit_state") != "closed":
        fail(
            f"rch status sample {position} reports {worker} as status="
            f"{entry.get('status')!r} circuit_state={entry.get('circuit_state')!r}"
        )

print(
    json.dumps(
        {
            "job_id": job_id,
            "command": command,
            "project_id": sorted(observed_projects)[0] if observed_projects else "",
            "queue_samples": len(queue_docs),
            "worker_samples": len(worker_docs),
            "samples_observing_job": positive_samples,
        },
        sort_keys=True,
    )
)
PY
}

# Adjudicate the post-build completion snapshot.
#
# The active-sample trace establishes isolation but says nothing about how the
# job ENDED: a build cancelled by the stuck detector, relocated, or exiting
# non-zero is indistinguishable in the active view from one that succeeded.
# `wait` on the local wrapper is not a substitute either — it reports the
# wrapper's status, not the daemon's record of the remote job.
#
# So the daemon's own rolling history must carry exactly one record for this
# job, and that record must say it finished remotely, cleanly, uncancelled.
# Absence is a refusal, not a pass: a job that left no completion record is
# precisely the silent-success failure mode this pack exists to exclude, and it
# is what closes the gap between the last active sample and the build returning.
verify_release_completion() {
  local worker="$1"
  local job_id="$2"
  local project_id="$3"
  local command_line="$4"
  local snapshot_path="$5"
  python3 - "$worker" "$job_id" "$project_id" "$command_line" "$snapshot_path" <<'PY'
import json
import sys

worker, job_id, project_id, command_line, path = sys.argv[1:6]


def fail(message):
    raise SystemExit(f"completion snapshot refused: {message}")


try:
    with open(path, "r", encoding="utf-8") as handle:
        document = json.load(handle)
except (OSError, ValueError) as error:
    fail(f"could not read {path}: {error}")

if not isinstance(document, dict):
    fail("completion snapshot is not a JSON object")
if document.get("api_version") != "1.0":
    fail(f"completion snapshot has unexpected api_version {document.get('api_version')!r}")
if document.get("command") != "status":
    fail(f"completion snapshot answers command {document.get('command')!r}, expected 'status'")
if document.get("success") is not True:
    fail(f"completion snapshot reports success={document.get('success')!r}")
data = document.get("data")
if not isinstance(data, dict):
    fail("completion snapshot carries no data object")
daemon = data.get("daemon")
if not isinstance(daemon, dict):
    fail("completion snapshot carries no data.daemon object")
recent = daemon.get("recent_builds")
if not isinstance(recent, list):
    fail("completion snapshot carries no data.daemon.recent_builds array")

# Identity is matched on the FULL decimal id compared as a string, so a 17-digit
# id is never routed through a float. bool is excluded because it subclasses int.
matches = []
for entry in recent:
    if not isinstance(entry, dict):
        continue
    raw_id = entry.get("id")
    if isinstance(raw_id, bool) or not isinstance(raw_id, int):
        continue
    if str(raw_id) == job_id:
        matches.append(entry)

if not matches:
    fail(
        f"the daemon's recent_builds history carries no record for job {job_id}; "
        "a build that left no completion record cannot be attested as having finished"
    )
if len(matches) > 1:
    fail(f"the daemon's recent_builds history carries {len(matches)} records for job {job_id}")

entry = matches[0]
if entry.get("worker_id") != worker:
    fail(f"job {job_id} completed on worker {entry.get('worker_id')!r}, not {worker!r}")
if entry.get("project_id") != project_id:
    fail(f"job {job_id} completed for project {entry.get('project_id')!r}, not {project_id!r}")
if entry.get("command") != command_line:
    fail(
        f"job {job_id} completed running {entry.get('command')!r}, which is not the exact "
        f"command observed while it was active"
    )
if entry.get("location") != "remote":
    fail(f"job {job_id} completed with location {entry.get('location')!r}, not 'remote'")
if entry.get("exit_code") != 0:
    fail(f"job {job_id} completed with exit_code {entry.get('exit_code')!r}, not 0")
if entry.get("cancellation") is not None:
    fail(f"job {job_id} was cancelled: {entry.get('cancellation')!r}")

print(
    json.dumps(
        {
            "job_id": job_id,
            "worker_id": entry.get("worker_id"),
            "exit_code": entry.get("exit_code"),
            "location": entry.get("location"),
            "duration_ms": entry.get("duration_ms"),
        },
        sort_keys=True,
    )
)
PY
}

# Cross-check a log-derived worker marker against the daemon-derived one.
# `absent` is accepted; a present-but-different marker is refused.
assert_worker_marker_agrees() {
  local marker="$1"
  local daemon_worker="$2"
  [[ "$marker" == "absent" || "$marker" == "$daemon_worker" ]] \
    || die "build log names worker ${marker} but the daemon queue reports ${daemon_worker}"
}

# A release build that emitted no compile line did not rebuild anything: it
# either reused a cached artifact or was silently dropped by the offload layer
# (the failure mode where the wrapper exits 0 and no work is done). Either way
# the log is not proof of the artifact, so refuse it.
require_rebuild_evidence() {
  local log_path="$1"
  # The emptiness test is a better error message, not the gate — the compile
  # marker below already rejects an empty log. It is skipped for non-regular
  # paths because a pipe always stats as zero-length, which is how the
  # self-test feeds fixed log text in without creating a file.
  [[ ! -f "$log_path" || -s "$log_path" ]] \
    || die "release build produced no captured log: ${log_path}"
  grep -qE '(^|[^[:alnum:]])Compiling fsqlite-e2e[[:space:]]' "$log_path" \
    || die "release build log records no compilation of the benchmark crate; refusing to cite a build that did not happen: ${log_path}"
}

# Inode, size and mtime of a file, as one comparable token. Used to detect a
# replacement landing while the file is being hashed.
artifact_identity() {
  local path="$1"
  stat -c '%i:%s:%Y' "$path" 2>/dev/null || printf 'unreadable'
}

# Pure comparator so the mid-hash substitution contract is testable from fixed
# strings. An unreadable stat on either side is refused rather than treated as
# a match.
assert_artifact_identity_stable() {
  local before="$1"
  local after="$2"
  local path="$3"
  [[ "$before" != "unreadable" && "$after" != "unreadable" ]] \
    || die "could not pin the identity of ${path} across hashing"
  [[ "$before" == "$after" ]] \
    || die "artifact ${path} changed while it was being hashed (${before} -> ${after}); refusing a digest that describes neither file"
}

# Pure freshness comparator: the artifact must have been written no earlier
# than the moment the build was launched. A pre-existing binary that the build
# left untouched keeps an older mtime and is refused, which is what makes the
# forced rebuild enforceable rather than merely requested.
assert_artifact_rebuilt() {
  local artifact_mtime="$1"
  local build_started="$2"
  local path="$3"
  [[ "$artifact_mtime" =~ ^[0-9]+$ ]] \
    || die "could not read a modification time for the rebuilt artifact: ${path}"
  [[ "$build_started" =~ ^[0-9]+$ ]] \
    || die "could not read the build start time for ${path}"
  (( artifact_mtime >= build_started )) \
    || die "artifact ${path} predates the release build that was supposed to produce it (mtime ${artifact_mtime} < build start ${build_started}); refusing to cite a stale binary"
}

# Whether the c1 benchmark binary compiles the nonce into itself. It does not
# today: `FSQLITE_BENCH_BUILD_NONCE` is emitted as a `rustc-env` by the e2e
# build script and read via `env!` by two sibling benchmark binaries, but not
# by the one this pack runs. So the nonce cannot be recovered from the running
# binary and must not be recorded as a binding to it. What it does do is real
# and provable: `cargo:rerun-if-env-changed` makes a fresh nonce invalidate the
# crate, which is what forces the citation-grade rebuild below. The binding is
# probed rather than assumed so that the recorded claim upgrades by itself if
# the binary ever starts consuming it.
BENCH_BINARY_SOURCE="${PROJECT_ROOT}/crates/fsqlite-e2e/src/bin/realdb_e2e.rs"

release_nonce_binding() {
  local source_path="${1:-$BENCH_BINARY_SOURCE}"
  [[ -f "$source_path" ]] \
    || die "cannot classify the build nonce binding; benchmark binary source is missing: ${source_path}"
  if grep -Fq 'FSQLITE_BENCH_BUILD_NONCE' "$source_path"; then
    printf 'compiled_into_binary'
  else
    printf 'forces_rebuild_only'
  fi
}

# Execution-environment half of the citation contract, mirroring
# capture_persistent_phase_pack.sh:266-291. Source provenance alone does not
# make a pack citable: a measurement taken locally, self-healed onto an
# arbitrary host, or produced by an unidentified binary cannot be reproduced
# from the artifact.
validate_release_execution_contract() {
  [[ "${FSQLITE_USE_RCH:-}" == "1" ]] \
    || die "FSQLITE_USE_RCH=1 is required for release capture"
  [[ "${RCH_REQUIRE_REMOTE:-}" == "1" ]] \
    || die "RCH_REQUIRE_REMOTE=1 is required so a refused offload cannot fall back to a local build"
  [[ "${RCH_NO_SELF_HEALING:-}" == "1" ]] \
    || die "RCH_NO_SELF_HEALING=1 is required so the run cannot be silently relocated"

  local worker="${RCH_WORKER:-}"
  [[ -n "$worker" ]] || die "RCH_WORKER must name the requested remote worker"
  # A worker *set* records an intent, not the host that produced the numbers.
  [[ "$worker" != *,* ]] \
    || die "RCH_WORKER must name exactly one worker for a citable measurement, found: ${worker}"
  require_allowed_remote_worker "$worker"

  # Required not as an attestation but as a mechanism: a fresh nonce is what
  # invalidates the crate so the release build cannot be satisfied by a cached
  # artifact. See `release_nonce_binding` for what may and may not be claimed
  # about it afterwards.
  local nonce="${FSQLITE_BENCH_BUILD_NONCE:-}"
  [[ -n "$nonce" ]] || die "FSQLITE_BENCH_BUILD_NONCE is required to force a fresh release build"
  require_lower_hex "$nonce" 64 FSQLITE_BENCH_BUILD_NONCE

  # Validated here so an operator typo fails before a build is spent, rather
  # than mid-poll or at manifest time.
  require_poll_interval "${FSQLITE_RELEASE_STATUS_POLL_SECONDS:-1}"

  # Optional reproducibility pin. The authoritative digest is the one taken
  # from the artifact this run rebuilds; an operator-supplied value cannot be
  # the authority because release mode now refuses to accept any pre-existing
  # binary, so the digest is not knowable before the build. When supplied it is
  # asserted against the rebuilt artifact, which turns it into a real
  # bit-for-bit reproducibility claim rather than a restatement.
  local binary_sha="${FSQLITE_RELEASE_RUNNING_BINARY_SHA256:-}"
  if [[ -n "$binary_sha" ]]; then
    require_lower_hex "$binary_sha" 64 FSQLITE_RELEASE_RUNNING_BINARY_SHA256
  fi
}

# SHA-256 of a regular file, or die. Symlinks are refused because the digest
# must describe the object actually executed, not a name that can be repointed
# between verification and the benchmark.
sha256_of_regular_file() {
  local path="$1"
  [[ -n "$path" ]] || die "running binary path must not be empty"
  [[ -e "$path" ]] || die "running binary is missing: ${path}"
  [[ ! -L "$path" ]] || die "running binary must not be a symlink: ${path}"
  [[ -f "$path" ]] || die "running binary is not a regular file: ${path}"
  local digest=""
  if command -v sha256sum >/dev/null 2>&1; then
    digest="$(sha256sum "$path" | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1; then
    digest="$(shasum -a 256 "$path" | awk '{print $1}')"
  else
    die "sha256sum or shasum is required to verify the running binary"
  fi
  require_lower_hex "$digest" 64 "running binary digest"
  printf '%s' "$digest"
}

# Pure comparator, split out so the match/mismatch contract is testable from
# fixed strings without touching the filesystem.
assert_running_binary_digest() {
  local expected="$1"
  local actual="$2"
  local path="$3"
  local label="${4:-the expected release digest}"
  [[ -n "$expected" ]] || die "no expected digest was established for ${path}"
  [[ "$expected" == "$actual" ]] \
    || die "binary ${path} has SHA-256 ${actual}, which does not match ${label} ${expected}"
}

# Release-mode gate: every benchmark must be executed by the artifact whose
# digest this run adopted. `RELEASE_REBUILT_BINARY_SHA256` is taken from the
# artifact immediately after the verified rebuild, and this re-hashes the file
# on each use.
#
# Exact scope, because the difference matters: a substitution occurring AFTER
# the adopted digest was taken — before the first measurement or between two
# measurements — fails closed here with the substitution named. A substitution
# occurring in the gap between the build process exiting and that first digest
# being taken is adopted as the rebuilt artifact, and no check in this script
# can see it; the mtime gate rejects a stale file there but not a newer
# substituted one. That residual is inherent to observing a file after the fact
# and is stated rather than papered over.
#
# `RELEASE_RUNNING_BINARY_VERIFIED` is set only here, so the recorded flag can
# never claim more than was proven.
verify_running_binary() {
  local path="$1"
  local actual=""
  actual="$(sha256_of_regular_file "$path")" || exit 2
  assert_running_binary_digest "$RELEASE_REBUILT_BINARY_SHA256" "$actual" "$path" \
    "${RELEASE_EXPECTED_DIGEST_LABEL:-the digest of the artifact this run rebuilt,}"
  RELEASE_RUNNING_BINARY_SHA256="$actual"
  RELEASE_RUNNING_BINARY_VERIFIED=1
}

# Re-verification hook for each benchmark invocation. A no-op outside release
# mode so ordinary captures are unaffected.
verify_binary_before_measurement() {
  [[ "${RELEASE_MODE:-0}" == "1" ]] || return 0
  verify_running_binary "${BINARY}"
}

# A release-mode re-render measures nothing, so it has no rebuild of its own to
# bind to. It inherits the previous run's authority instead — but only after
# proving that authority was earned: the retained pack must itself have been a
# verified release capture, and the binary still on disk must still hash to the
# digest that pack recorded. An unverified or non-release pack is refused
# rather than re-rendered into something that looks citable.
release_render_only_reverify() {
  local recorded=""
  recorded="$(python3 - "${BUILD_METADATA_JSON}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    data = json.load(handle)
if data.get("release_mode") is not True:
    sys.exit("retained pack was not captured in release mode")
if data.get("running_binary_sha256_verified") is not True:
    sys.exit("retained pack never verified its running binary")
digest = data.get("running_binary_sha256") or ""
if not digest:
    sys.exit("retained pack records no running binary digest")
print(digest)
PY
)" || die "cannot re-render in release mode from ${BUILD_METADATA_JSON}: the retained pack does not carry a verified release digest"
  require_lower_hex "$recorded" 64 "retained running binary digest"
  RELEASE_REBUILT_BINARY_SHA256="$recorded"
  # Nothing was rebuilt on this path, so name the actual source of the
  # expectation in any refusal message.
  RELEASE_EXPECTED_DIGEST_LABEL="the digest recorded by the retained release pack,"
  resolve_binary_path
  verify_running_binary "${BINARY}"
}

# Exercise the guard without running any benchmark, building anything, or
# writing into an evidence path. Every case runs in a subshell so `die`'s exit
# is observed rather than terminating the self-test.
if [[ "${FSQLITE_CAPTURE_SELF_TEST:-0}" == "1" ]]; then
  self_test_failures=0
  expect_reject() {
    local label="$1"
    shift
    if ( "$@" ) >/dev/null 2>&1; then
      echo "SELF-TEST FAIL: ${label} was accepted but must be rejected" >&2
      self_test_failures=$((self_test_failures + 1))
    else
      echo "SELF-TEST ok: rejected ${label}"
    fi
  }
  expect_accept() {
    local label="$1"
    shift
    if ( "$@" ) >/dev/null 2>&1; then
      echo "SELF-TEST ok: accepted ${label}"
    else
      echo "SELF-TEST FAIL: ${label} was rejected but must be accepted" >&2
      self_test_failures=$((self_test_failures + 1))
    fi
  }

  # Path states are simulated, never materialised: this self-test creates and
  # removes nothing on disk. `/nonexistent/...` is used purely as a syntactically
  # absolute label; it is never opened, because the simulated state short-circuits
  # the filesystem probe.
  sim_path="/nonexistent/c1-selftest/performance/c1"

  expect_reject "empty path" validate_output_dir ""
  expect_reject "relative path" validate_output_dir "artifacts/relative"
  expect_reject "existing regular file" validate_output_dir "$sim_path" 0 non-directory
  expect_reject "populated directory" validate_output_dir "$sim_path" 0 dir-populated
  expect_accept "absent absolute path" validate_output_dir "$sim_path" 0 absent
  expect_accept "empty absolute directory" validate_output_dir "$sim_path" 0 dir-empty
  expect_accept "populated directory under RENDER_ONLY" \
    validate_output_dir "$sim_path" 1 dir-populated
  expect_reject "relative path under RENDER_ONLY" validate_output_dir "relative" 1 dir-empty
  expect_reject "non-directory under RENDER_ONLY" validate_output_dir "$sim_path" 1 non-directory

  # Release-contract cases. Git state is injected, so no repository mutation,
  # no checkout, and no benchmark occurs.
  good_commit="0123456789abcdef0123456789abcdef01234567"
  other_commit="fedcba9876543210fedcba9876543210fedcba98"

  release_case() {
    local label="$1" head="$2" status="$3" expected="$4" mode="$5"
    if ( C1_TEST_HEAD_COMMIT="$head" C1_TEST_WORKTREE_STATUS="$status" \
         validate_release_contract "$expected" ) >/dev/null 2>&1; then
      if [[ "$mode" == "accept" ]]; then
        echo "SELF-TEST ok: accepted ${label}"
      else
        echo "SELF-TEST FAIL: ${label} was accepted but must be rejected" >&2
        self_test_failures=$((self_test_failures + 1))
      fi
    elif [[ "$mode" == "reject" ]]; then
      echo "SELF-TEST ok: rejected ${label}"
    else
      echo "SELF-TEST FAIL: ${label} was rejected but must be accepted" >&2
      self_test_failures=$((self_test_failures + 1))
    fi
  }

  release_case "clean checkout at the expected commit" \
    "$good_commit" "" "$good_commit" accept
  release_case "empty expected commit" \
    "$good_commit" "" "" reject
  release_case "short (non-40-hex) expected commit" \
    "$good_commit" "" "0123456789abcdef" reject
  release_case "uppercase expected commit" \
    "$good_commit" "" "0123456789ABCDEF0123456789ABCDEF01234567" reject
  release_case "non-hex expected commit" \
    "$good_commit" "" "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz" reject
  release_case "HEAD mismatching the expected commit" \
    "$other_commit" "" "$good_commit" reject
  release_case "unresolvable HEAD" \
    "" "" "$good_commit" reject
  release_case "dirty worktree at the expected commit" \
    "$good_commit" " M README.md" "$good_commit" reject
  release_case "untracked file at the expected commit" \
    "$good_commit" "?? README.md" "$good_commit" reject

  # Execution-environment contract. Every case is pure environment injection in
  # a subshell: no files, no RCH invocation, no benchmark.
  good_nonce="$(printf '0123456789abcdef%.0s' 1 2 3 4)"
  good_binsha="$(printf 'fedcba9876543210%.0s' 1 2 3 4)"

  exec_case() {
    local label="$1" mode="$2"
    shift 2
    if ( export FSQLITE_USE_RCH=1 RCH_REQUIRE_REMOTE=1 RCH_NO_SELF_HEALING=1 \
                RCH_WORKER=ovh-a FSQLITE_BENCH_BUILD_NONCE="$good_nonce" \
                FSQLITE_RELEASE_RUNNING_BINARY_SHA256="$good_binsha"
         while (( $# )); do
           override_name="${1%%=*}"
           override_value="${1#*=}"
           export "${override_name}"="${override_value}"
           shift
         done
         validate_release_execution_contract ) >/dev/null 2>&1; then
      if [[ "$mode" == "accept" ]]; then
        echo "SELF-TEST ok: accepted ${label}"
      else
        echo "SELF-TEST FAIL: ${label} was accepted but must be rejected" >&2
        self_test_failures=$((self_test_failures + 1))
      fi
    elif [[ "$mode" == "reject" ]]; then
      echo "SELF-TEST ok: rejected ${label}"
    else
      echo "SELF-TEST FAIL: ${label} was rejected but must be accepted" >&2
      self_test_failures=$((self_test_failures + 1))
    fi
  }

  exec_case "fully satisfied execution contract" accept
  exec_case "FSQLITE_USE_RCH not 1" reject FSQLITE_USE_RCH=0
  exec_case "RCH_REQUIRE_REMOTE not 1" reject RCH_REQUIRE_REMOTE=
  exec_case "RCH_NO_SELF_HEALING not 1" reject RCH_NO_SELF_HEALING=0
  exec_case "unnamed RCH_WORKER" reject RCH_WORKER=
  exec_case "RCH_WORKER naming a set" reject RCH_WORKER=ovh-a,ovh-b
  exec_case "quarantined worker hz1" reject RCH_WORKER=hz1
  exec_case "quarantined worker hz2" reject RCH_WORKER=hz2
  exec_case "missing build nonce" reject FSQLITE_BENCH_BUILD_NONCE=
  exec_case "short build nonce" reject FSQLITE_BENCH_BUILD_NONCE=0123456789abcdef
  exec_case "uppercase build nonce" \
    reject "FSQLITE_BENCH_BUILD_NONCE=$(printf '0123456789ABCDEF%.0s' 1 2 3 4)"
  # The running-binary digest is an optional pin now that the authoritative
  # value comes from the artifact this run rebuilds; a malformed pin is still
  # refused before a build is spent.
  exec_case "absent optional running-binary pin" accept FSQLITE_RELEASE_RUNNING_BINARY_SHA256=
  exec_case "short running-binary sha256" reject FSQLITE_RELEASE_RUNNING_BINARY_SHA256=deadbeef
  exec_case "non-hex running-binary sha256" \
    reject "FSQLITE_RELEASE_RUNNING_BINARY_SHA256=$(printf 'zzzzzzzzzzzzzzzz%.0s' 1 2 3 4)"

  # Running-binary digest verification. The pure comparator is exercised from
  # fixed strings; the hashing helper is exercised against files that already
  # exist in this repository, so nothing is created or removed.
  expect_accept "matching running-binary digest" \
    assert_running_binary_digest "$good_binsha" "$good_binsha" /some/binary
  expect_reject "mismatching running-binary digest" \
    assert_running_binary_digest "$good_binsha" "$good_nonce" /some/binary
  expect_reject "empty actual running-binary digest" \
    assert_running_binary_digest "$good_binsha" "" /some/binary

  expect_reject "hashing an empty path" sha256_of_regular_file ""
  expect_reject "hashing a missing binary" \
    sha256_of_regular_file "${PROJECT_ROOT}/nonexistent-release-binary"
  expect_reject "hashing a directory" sha256_of_regular_file "${PROJECT_ROOT}/scripts"
  expect_accept "hashing an existing regular file" \
    sha256_of_regular_file "${PROJECT_ROOT}/README.md"

  # End-to-end on a real regular file: the digest this helper computes must be
  # exactly what the comparator accepts, and any other digest must be refused.
  readme_digest="$(sha256_of_regular_file "${PROJECT_ROOT}/README.md")"
  expect_accept "self-consistent digest of a real file" \
    assert_running_binary_digest "$readme_digest" "$readme_digest" "${PROJECT_ROOT}/README.md"
  expect_reject "real file digest against a foreign expectation" \
    assert_running_binary_digest "$good_binsha" "$readme_digest" "${PROJECT_ROOT}/README.md"
  expect_reject "digest comparison with no expectation established" \
    assert_running_binary_digest "" "$readme_digest" "${PROJECT_ROOT}/README.md"

  # Build-provenance parsing. Log text is fed through process substitution, so
  # every case reads a pipe and no log file is created. `RELEASE_WORKER_DENYLIST`
  # is the same list the live guard consults.
  good_log=(
    '   Compiling fsqlite-e2e v0.2.0 (/workspace/crates/fsqlite-e2e)'
    '[*] Job j-7 submitted to ovh-a'
    '[RCH] remote ovh-a accepted job j-7'
  )

  expect_reject "two conflicting worker markers in one log" \
    worker_marker_from_rch_log \
      <(printf '%s\n' '[RCH] remote ovh-a accepted job j-7' 'Selected worker: ovh-b')
  expect_accept "a permitted worker" require_allowed_remote_worker ovh-a
  expect_reject "the quarantined worker hz1" require_allowed_remote_worker hz1
  expect_reject "the quarantined worker hz2" require_allowed_remote_worker hz2
  expect_reject "an unnamed worker" require_allowed_remote_worker ""

  expect_accept "an integral poll interval" require_poll_interval 1
  expect_accept "a fractional poll interval" require_poll_interval 0.25
  expect_reject "a non-numeric poll interval" require_poll_interval abc
  expect_reject "a zero poll interval" require_poll_interval 0
  expect_reject "a zero fractional poll interval" require_poll_interval 0.0
  expect_reject "a negative poll interval" require_poll_interval -1
  expect_reject "an empty poll interval" require_poll_interval ""

  expect_accept "an unchanged artifact identity across hashing" \
    assert_artifact_identity_stable "12:34:56" "12:34:56" /some/binary
  expect_reject "an artifact whose size changed while hashing" \
    assert_artifact_identity_stable "12:34:56" "12:99:56" /some/binary
  expect_reject "an artifact whose inode changed while hashing" \
    assert_artifact_identity_stable "12:34:56" "13:34:56" /some/binary
  expect_reject "an artifact whose mtime changed while hashing" \
    assert_artifact_identity_stable "12:34:56" "12:34:99" /some/binary
  expect_reject "an unreadable artifact identity before hashing" \
    assert_artifact_identity_stable unreadable "12:34:56" /some/binary
  expect_reject "an unreadable artifact identity after hashing" \
    assert_artifact_identity_stable "12:34:56" unreadable /some/binary
  identity_probe="$(artifact_identity "${PROJECT_ROOT}/README.md")"
  if [[ "$identity_probe" =~ ^[0-9]+:[0-9]+:[0-9]+$ ]]; then
    echo "SELF-TEST ok: read a well-formed artifact identity from a real file"
  else
    echo "SELF-TEST FAIL: artifact identity was '${identity_probe}'" >&2
    self_test_failures=$((self_test_failures + 1))
  fi
  if [[ "$(artifact_identity "${PROJECT_ROOT}/nonexistent-release-binary")" == "unreadable" ]]; then
    echo "SELF-TEST ok: reported a missing artifact identity as unreadable"
  else
    echo "SELF-TEST FAIL: a missing artifact did not report an unreadable identity" >&2
    self_test_failures=$((self_test_failures + 1))
  fi

  expect_accept "build log recording a benchmark-crate compile" \
    require_rebuild_evidence <(printf '%s\n' "${good_log[@]}")
  expect_reject "build log with no compile line" \
    require_rebuild_evidence <(printf '%s\n' '[RCH] remote ovh-a accepted job j-7' 'Finished release-perf')
  expect_reject "empty build log" require_rebuild_evidence <(printf '')

  expect_accept "artifact written after the build started" \
    assert_artifact_rebuilt 1700000005 1700000000 /some/binary
  expect_accept "artifact written in the build's opening second" \
    assert_artifact_rebuilt 1700000000 1700000000 /some/binary
  expect_reject "artifact predating the build" \
    assert_artifact_rebuilt 1699999999 1700000000 /some/binary
  expect_reject "unreadable artifact modification time" \
    assert_artifact_rebuilt "" 1700000000 /some/binary

  # Nonce-binding classification, exercised against files that already exist in
  # this repository: one that does not reference the nonce and one that does.
  binding_case() {
    local label="$1" source_path="$2" expected="$3" actual
    if actual="$(release_nonce_binding "$source_path" 2>/dev/null)" \
       && [[ "$actual" == "$expected" ]]; then
      echo "SELF-TEST ok: classified ${label} as ${expected}"
    else
      echo "SELF-TEST FAIL: ${label} classified as '${actual:-<error>}', expected ${expected}" >&2
      self_test_failures=$((self_test_failures + 1))
    fi
  }
  binding_case "a source that never reads the nonce" \
    "${PROJECT_ROOT}/README.md" forces_rebuild_only
  binding_case "a source that references the nonce" \
    "${PROJECT_ROOT}/crates/fsqlite-e2e/build.rs" compiled_into_binary
  expect_reject "nonce binding for a missing source" \
    release_nonce_binding "${PROJECT_ROOT}/nonexistent-benchmark-source.rs"
  live_binding="$(release_nonce_binding 2>/dev/null || true)"
  if [[ "$live_binding" == "forces_rebuild_only" || "$live_binding" == "compiled_into_binary" ]]; then
    echo "SELF-TEST ok: benchmark binary source classifies as ${live_binding}"
  else
    echo "SELF-TEST FAIL: benchmark binary source produced no valid nonce binding" >&2
    self_test_failures=$((self_test_failures + 1))
  fi

  # Scheduler-isolation trace. Snapshots are synthesised as JSON text and fed
  # through process substitution, so the daemon is never contacted, no trace
  # file is written, and no build is launched.
  # Job ids are real 17-digit values so the >2^53 exactness of the comparison is
  # actually exercised: OURS and NEIGHBOUR differ only in the final digit and
  # collapse to the same IEEE-754 double.
  OURS_ID=29960766543102031
  NEAR_ID=29960766543102030
  MARKER="--bin realdb-e2e"
  our_cmd="cargo build --locked --verbose --profile release-perf -p fsqlite-e2e --bin realdb-e2e"
  their_cmd="cargo test -p fsqlite-harness --test phase5_regression_guard"

  qsnap() { # $1=active_builds array json
    printf '{"api_version":"1.0","command":"queue","success":true,"data":{"active_builds":%s,"queued_builds":[]}}\n' "$1"
  }
  wsnap() { # $1=status, $2=circuit_state
    printf '{"api_version":"1.0","command":"status","success":true,"data":{"schema_version":"1.0.0","daemon":{"workers":[{"id":"ovh-a","host":"h","status":"%s","circuit_state":"%s"}],"active_builds":[]}}}\n' "$1" "$2"
  }
  build_entry() { # $1=id, $2=worker, $3=command
    printf '{"id":%s,"worker_id":"%s","project_id":"frankensqlite-df8c83ae","command":"%s"}' "$1" "$2" "$3"
  }
  ours="[$(build_entry $OURS_ID ovh-a "$our_cmd")]"
  neighbour="[$(build_entry $NEAR_ID ovh-a "$our_cmd")]"
  theirs="[$(build_entry $OURS_ID ovh-a "$their_cmd")]"
  coresident="[$(build_entry $OURS_ID ovh-a "$our_cmd"),$(build_entry $NEAR_ID ovh-a "$their_cmd")]"
  elsewhere="[$(build_entry $OURS_ID ovh-a "$our_cmd"),$(build_entry $NEAR_ID ovh-b "$their_cmd")]"
  idle='[]'
  healthy_w="$(wsnap healthy closed)"

  isolation_case() { # label mode queue-text worker-text
    local label="$1" mode="$2" qtext="$3" wtext="$4"
    if ( verify_scheduler_isolation_trace ovh-a "$MARKER" \
           <(printf '%s' "$qtext") <(printf '%s' "$wtext") ) >/dev/null 2>&1; then
      if [[ "$mode" == "accept" ]]; then echo "SELF-TEST ok: accepted ${label}"
      else echo "SELF-TEST FAIL: ${label} was accepted but must be rejected" >&2
           self_test_failures=$((self_test_failures + 1)); fi
    elif [[ "$mode" == "reject" ]]; then echo "SELF-TEST ok: rejected ${label}"
    else echo "SELF-TEST FAIL: ${label} was rejected but must be accepted" >&2
         self_test_failures=$((self_test_failures + 1)); fi
  }

  isolation_case "our job as the sole active build" accept \
    "$(qsnap "$ours")" "$healthy_w"
  isolation_case "our job observed only mid-window" accept \
    "$(qsnap "$idle")$(qsnap "$ours")$(qsnap "$idle")" "$healthy_w$healthy_w$healthy_w"
  isolation_case "a foreign build on a different worker" accept \
    "$(qsnap "$elsewhere")" "$healthy_w"
  isolation_case "our job observed in every sample" accept \
    "$(qsnap "$ours")$(qsnap "$ours")" "$healthy_w$healthy_w"
  isolation_case "a co-resident build alongside ours" reject \
    "$(qsnap "$coresident")" "$healthy_w"
  isolation_case "a foreign build later in the window" reject \
    "$(qsnap "$ours")$(qsnap "$theirs")" "$healthy_w$healthy_w"
  isolation_case "two distinct job ids across the window" reject \
    "$(qsnap "$ours")$(qsnap "$neighbour")" "$healthy_w$healthy_w"
  isolation_case "a build whose command lacks our marker" reject \
    "$(qsnap "$theirs")" "$healthy_w"
  isolation_case "a worker that was never busy" reject \
    "$(qsnap "$idle")$(qsnap "$idle")" "$healthy_w$healthy_w"
  isolation_case "an unhealthy worker" reject \
    "$(qsnap "$ours")" "$(wsnap degraded closed)"
  isolation_case "an open worker circuit" reject \
    "$(qsnap "$ours")" "$(wsnap healthy open)"
  isolation_case "an empty queue trace" reject "" "$healthy_w"
  isolation_case "an empty worker trace" reject "$(qsnap "$ours")" ""

  # Envelope validation: a snapshot that is not a successful answer to the
  # exact expected command is not evidence.
  isolation_case "a queue snapshot answering the wrong command" reject \
    '{"api_version":"1.0","command":"status","success":true,"data":{"active_builds":[]}}' "$healthy_w"
  isolation_case "a queue snapshot reporting success=false" reject \
    "$(printf '{"api_version":"1.0","command":"queue","success":false,"data":{"active_builds":%s}}\n' "$ours")" "$healthy_w"
  isolation_case "a queue snapshot with an unrecognised api_version" reject \
    "$(printf '{"api_version":"2.0","command":"queue","success":true,"data":{"active_builds":%s}}\n' "$ours")" "$healthy_w"
  isolation_case "a worker snapshot answering the wrong command" reject \
    "$(qsnap "$ours")" '{"api_version":"1.0","command":"queue","success":true,"data":{"daemon":{"workers":[]}}}'
  isolation_case "a truncated snapshot stream" reject \
    "$(qsnap "$ours")$(printf '{"api_version":"1.0","command":"queue"')" "$healthy_w"
  isolation_case "a queue snapshot with no active_builds array" reject \
    '{"api_version":"1.0","command":"queue","success":true,"data":{}}' "$healthy_w"
  isolation_case "a build on our worker with a null job id" reject \
    "$(qsnap '[{"id":null,"worker_id":"ovh-a","project_id":"p","command":"cargo build --bin realdb-e2e"}]')" "$healthy_w"
  isolation_case "a build on our worker with a string job id" reject \
    "$(qsnap '[{"id":"29960766543102031","worker_id":"ovh-a","project_id":"p","command":"cargo build --bin realdb-e2e"}]')" "$healthy_w"
  isolation_case "a build on our worker with no id field at all" reject \
    "$(qsnap '[{"worker_id":"ovh-a","project_id":"p","command":"cargo build --bin realdb-e2e"}]')" "$healthy_w"
  isolation_case "a status snapshot missing the selected worker" reject \
    "$(qsnap "$ours")" '{"api_version":"1.0","command":"status","success":true,"data":{"daemon":{"workers":[{"id":"ovh-b","status":"healthy","circuit_state":"closed"}]}}}'

  # The adjudicated identity must be reported exactly, with no float rounding.
  reported="$(verify_scheduler_isolation_trace ovh-a "$MARKER" \
    <(qsnap "$ours"; qsnap "$ours") <(printf '%s%s' "$healthy_w" "$healthy_w") 2>/dev/null || true)"
  reported_id="$(printf '%s' "$reported" | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])' 2>/dev/null || true)"
  if [[ "$reported_id" == "$OURS_ID" ]]; then
    echo "SELF-TEST ok: reported job id ${reported_id} exactly (>2^53, no float rounding)"
  else
    echo "SELF-TEST FAIL: job id reported as '${reported_id}', expected ${OURS_ID}" >&2
    self_test_failures=$((self_test_failures + 1))
  fi
  reported_samples="$(printf '%s' "$reported" | python3 -c 'import json,sys; print(json.load(sys.stdin)["queue_samples"])' 2>/dev/null || true)"
  if [[ "$reported_samples" == "2" ]]; then
    echo "SELF-TEST ok: reported 2 retained queue samples"
  else
    echo "SELF-TEST FAIL: queue sample count was '${reported_samples}', expected 2" >&2
    self_test_failures=$((self_test_failures + 1))
  fi

  # Completion-record keepers. Fixtures carry the real record shape observed
  # from the daemon — 17-digit id, timing block, duration, nullable
  # cancellation — so the big-integer identity is exercised end to end rather
  # than on a toy value.
  OTHER_ID=29960766543102030
  PROJECT_ID="frankensqlite-df8c83ae"
  completion() { # $1=id $2=worker $3=project $4=command $5=exit $6=location $7=cancellation
    printf '{"api_version":"1.0","timestamp":1785809338,"command":"status","success":true,"data":{"schema_version":"1.0.0","daemon":{"workers":[],"active_builds":[],"queued_builds":[],"recent_builds":[{"id":%s,"started_at":"2026-08-04T01:49:55.983611556+00:00","completed_at":"2026-08-04T01:57:30.358532967+00:00","project_id":"%s","worker_id":"%s","command":"%s","exit_code":%s,"duration_ms":454374,"location":"%s","bytes_transferred":null,"timing":{"classify":null,"select":null,"sync_up":964,"exec":447131,"sync_down":628,"cleanup":null,"total":454374},"cancellation":%s}]}}}\n' \
      "$1" "$3" "$2" "$4" "$5" "$6" "$7"
  }
  CANCELLED='{"operation_id":"cancel-29960766543102031","origin":"stuck_detector","reason_code":"stuck_detector","decision_path":["requested","term_sent","remote_kill_sent","completed"],"escalation_stage":"remote_kill","final_state":"completed"}'

  completion_case() { # label mode snapshot-text
    local label="$1" mode="$2" text="$3"
    if ( verify_release_completion ovh-a "$OURS_ID" "$PROJECT_ID" "$our_cmd" \
           <(printf '%s' "$text") ) >/dev/null 2>&1; then
      if [[ "$mode" == "accept" ]]; then echo "SELF-TEST ok: accepted ${label}"
      else echo "SELF-TEST FAIL: ${label} was accepted but must be rejected" >&2
           self_test_failures=$((self_test_failures + 1)); fi
    elif [[ "$mode" == "reject" ]]; then echo "SELF-TEST ok: rejected ${label}"
    else echo "SELF-TEST FAIL: ${label} was rejected but must be accepted" >&2
         self_test_failures=$((self_test_failures + 1)); fi
  }

  completion_case "a clean remote completion record" accept \
    "$(completion "$OURS_ID" ovh-a "$PROJECT_ID" "$our_cmd" 0 remote null)"
  completion_case "a completion record for a neighbouring job id" reject \
    "$(completion "$OTHER_ID" ovh-a "$PROJECT_ID" "$our_cmd" 0 remote null)"
  completion_case "a completion record naming a different worker" reject \
    "$(completion "$OURS_ID" ovh-b "$PROJECT_ID" "$our_cmd" 0 remote null)"
  completion_case "a completion record for a different project" reject \
    "$(completion "$OURS_ID" ovh-a "some-other-project" "$our_cmd" 0 remote null)"
  completion_case "a completion record whose command differs" reject \
    "$(completion "$OURS_ID" ovh-a "$PROJECT_ID" "$their_cmd" 0 remote null)"
  completion_case "a completion record with a non-zero exit code" reject \
    "$(completion "$OURS_ID" ovh-a "$PROJECT_ID" "$our_cmd" 101 remote null)"
  completion_case "a completion record that ran locally" reject \
    "$(completion "$OURS_ID" ovh-a "$PROJECT_ID" "$our_cmd" 0 local null)"
  completion_case "a completion record that was cancelled" reject \
    "$(completion "$OURS_ID" ovh-a "$PROJECT_ID" "$our_cmd" 0 remote "$CANCELLED")"
  completion_case "a history with no record for our job" reject \
    '{"api_version":"1.0","command":"status","success":true,"data":{"daemon":{"recent_builds":[]}}}'
  completion_case "a snapshot with no recent_builds array" reject \
    '{"api_version":"1.0","command":"status","success":true,"data":{"daemon":{}}}'
  completion_case "a completion snapshot answering the wrong command" reject \
    "$(printf '{"api_version":"1.0","command":"queue","success":true,"data":{"daemon":{"recent_builds":[{"id":%s,"worker_id":"ovh-a","project_id":"%s","command":"%s","exit_code":0,"location":"remote","cancellation":null}]}}}\n' "$OURS_ID" "$PROJECT_ID" "$our_cmd")"
  completion_case "a completion snapshot reporting success=false" reject \
    "$(printf '{"api_version":"1.0","command":"status","success":false,"data":{"daemon":{"recent_builds":[{"id":%s,"worker_id":"ovh-a","project_id":"%s","command":"%s","exit_code":0,"location":"remote","cancellation":null}]}}}\n' "$OURS_ID" "$PROJECT_ID" "$our_cmd")"
  completion_case "a completion record whose id is a string" reject \
    "$(printf '{"api_version":"1.0","command":"status","success":true,"data":{"daemon":{"recent_builds":[{"id":"%s","worker_id":"ovh-a","project_id":"%s","command":"%s","exit_code":0,"location":"remote","cancellation":null}]}}}\n' "$OURS_ID" "$PROJECT_ID" "$our_cmd")"
  completion_case "a history carrying two records for the same job" reject \
    "$(printf '{"api_version":"1.0","command":"status","success":true,"data":{"daemon":{"recent_builds":[{"id":%s,"worker_id":"ovh-a","project_id":"%s","command":"%s","exit_code":0,"location":"remote","cancellation":null},{"id":%s,"worker_id":"ovh-a","project_id":"%s","command":"%s","exit_code":0,"location":"remote","cancellation":null}]}}}\n' "$OURS_ID" "$PROJECT_ID" "$our_cmd" "$OURS_ID" "$PROJECT_ID" "$our_cmd")"
  completion_case "an unreadable completion snapshot" reject ""

  # A neighbouring id must not be adopted through float rounding: the accepted
  # record's id is reported back verbatim.
  completion_reported="$(verify_release_completion ovh-a "$OURS_ID" "$PROJECT_ID" "$our_cmd" \
    <(completion "$OURS_ID" ovh-a "$PROJECT_ID" "$our_cmd" 0 remote null) 2>/dev/null || true)"
  completion_id="$(printf '%s' "$completion_reported" | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])' 2>/dev/null || true)"
  if [[ "$completion_id" == "$OURS_ID" ]]; then
    echo "SELF-TEST ok: completion record matched job ${OURS_ID} exactly (>2^53)"
  else
    echo "SELF-TEST FAIL: completion matched '${completion_id}', expected ${OURS_ID}" >&2
    self_test_failures=$((self_test_failures + 1))
  fi

  # Log markers are corroboration only: absent is fine, disagreeing is not.
  expect_accept "absent worker marker in the build log" \
    assert_worker_marker_agrees absent ovh-a
  expect_accept "a log marker agreeing with the daemon" \
    assert_worker_marker_agrees ovh-a ovh-a
  expect_reject "a log marker contradicting the daemon" \
    assert_worker_marker_agrees ovh-b ovh-a
  for marker_case in "[RCH] remote ovh-a accepted job j-7:ovh-a" \
                     "Selected worker: ovh-a:ovh-a" \
                     "nothing to see here:absent"; do
    marker_line="${marker_case%:*}"; marker_want="${marker_case##*:}"
    marker_got="$(worker_marker_from_rch_log <(printf '%s\n' "$marker_line"))"
    if [[ "$marker_got" == "$marker_want" ]]; then
      echo "SELF-TEST ok: parsed worker marker '${marker_want}' from the build log"
    else
      echo "SELF-TEST FAIL: marker parsed as '${marker_got}', expected '${marker_want}'" >&2
      self_test_failures=$((self_test_failures + 1))
    fi
  done
  scheduler_case_note="scheduler isolation (31)"

  if [[ "$self_test_failures" -eq 0 ]]; then
    echo "SELF-TEST PASS: 120 cases — output-directory guard (9) + release source contract (9) + release execution contract (14) + running-binary digest (10) + build provenance (31) + ${scheduler_case_note} + remote completion (16); no filesystem mutation, no benchmark executed"
    exit 0
  fi
  echo "SELF-TEST FAILED: ${self_test_failures} case(s)" >&2
  exit 1
fi

# Release mode is opt-in and must clear before the output directory is created
# or any benchmark starts. Ordinary (non-release) captures leave
# FSQLITE_RELEASE_FROZEN_COMMIT unset and are unaffected.
RELEASE_FROZEN_COMMIT="${FSQLITE_RELEASE_FROZEN_COMMIT:-}"
RELEASE_MODE=0
RELEASE_CLEAN_CHECKOUT=0
RELEASE_RCH_WORKER=""
RELEASE_RCH_WORKER_SELECTED=""
RELEASE_RCH_JOB_ID=""
RELEASE_BUILD_NONCE=""
RELEASE_BUILD_NONCE_BINDING=""
RELEASE_RCH_JOB_COMMAND=""
RELEASE_RCH_JOB_PROJECT_ID=""
RELEASE_RCH_JOB_EXIT_CODE=""
RELEASE_RCH_JOB_DURATION_MS=""
RELEASE_SCHEDULER_COMPLETION_SHA256=""
RELEASE_SCHEDULER_COMPLETION_RELPATH=""
RELEASE_SCHEDULER_QUEUE_SAMPLES=""
RELEASE_SCHEDULER_WORKER_SAMPLES=""
RELEASE_SCHEDULER_JOB_SAMPLES=""
RELEASE_SCHEDULER_QUEUE_SHA256=""
RELEASE_SCHEDULER_WORKERS_SHA256=""
RELEASE_SCHEDULER_QUEUE_RELPATH=""
RELEASE_SCHEDULER_WORKERS_RELPATH=""
# Substring the daemon-reported command must carry for a build to be adopted as
# ours. Derived from the build target rather than restated, so it cannot drift.
RELEASE_BUILD_COMMAND_MARKER="--bin realdb-e2e"
RELEASE_REBUILT_BINARY_SHA256=""
RELEASE_EXPECTED_DIGEST_LABEL=""
RELEASE_RUNNING_BINARY_SHA256=""
RELEASE_RUNNING_BINARY_VERIFIED=0
if [[ -n "$RELEASE_FROZEN_COMMIT" ]]; then
  validate_release_contract "$RELEASE_FROZEN_COMMIT"
  validate_release_execution_contract
  # Only reachable once both contracts have passed, so every marker below is a
  # record of an enforced fact rather than an independent claim that could
  # drift from the guard.
  RELEASE_MODE=1
  RELEASE_CLEAN_CHECKOUT=1
  RELEASE_RCH_WORKER="${RCH_WORKER}"
  RELEASE_BUILD_NONCE="${FSQLITE_BENCH_BUILD_NONCE}"
  RELEASE_BUILD_NONCE_BINDING="$(release_nonce_binding)"
fi

validate_output_dir "$OUTPUT_DIR" "${RENDER_ONLY:-0}"

EVENTS_JSONL="${OUTPUT_DIR}/events.jsonl"
COMMANDS_JSONL="${OUTPUT_DIR}/commands.jsonl"
MANIFEST_JSON="${OUTPUT_DIR}/c1_pack_manifest.json"
SCORECARD_JSON="${OUTPUT_DIR}/c1_scorecard.json"
SUMMARY_MD="${OUTPUT_DIR}/summary.md"
BUILD_METADATA_JSON="${OUTPUT_DIR}/build_metadata.json"
HASHES_TXT="${OUTPUT_DIR}/artifact_hashes.txt"
SCHEDULER_QUEUE_JSONL="${OUTPUT_DIR}/rch_build_queue.jsonl"
SCHEDULER_WORKERS_JSONL="${OUTPUT_DIR}/rch_build_workers.jsonl"
SCHEDULER_COMPLETION_JSON="${OUTPUT_DIR}/rch_build_completion.json"
SCHEDULER_DIAGNOSTICS_LOG="${OUTPUT_DIR}/rch_poll_stderr.log"

mkdir -p "$OUTPUT_DIR"

WORKLOADS="${WORKLOADS:-commutative_inserts_disjoint_keys,hot_page_contention,mixed_read_write}"
HOT_PROFILE_WORKLOAD="${HOT_PROFILE_WORKLOAD:-commutative_inserts_disjoint_keys}"
if [[ "${HOT_PROFILE_WORKLOAD}" == "commutative_inserts_disjoint_keys" ]]; then
  HOT_PROFILE_WORKLOAD_TAG="commutative"
else
  HOT_PROFILE_WORKLOAD_TAG="$(printf '%s' "${HOT_PROFILE_WORKLOAD}" | tr -c '[:alnum:]_' '_')"
fi
CONCURRENCY="${CONCURRENCY:-1}"
REPEAT="${REPEAT:-3}"
DB_FIXTURES="${DB_FIXTURES:-frankensqlite,frankentui,frankensearch}"
HEALTHY_MARGIN_MIN="${HEALTHY_MARGIN_MIN:-1.10}"
SKIP_RUN="${SKIP_RUN:-0}"
RENDER_ONLY="${RENDER_ONLY:-0}"

if [[ "${RENDER_ONLY}" == "1" ]]; then
  touch "$EVENTS_JSONL" "$COMMANDS_JSONL"
else
  : > "$EVENTS_JSONL"
  : > "$COMMANDS_JSONL"
fi

IFS=',' read -ra FIXTURE_ARRAY <<< "$DB_FIXTURES"

emit_event() {
  local phase="$1"
  local event_type="$2"
  local outcome="$3"
  local elapsed_ms="$4"
  local message="$5"
  local fixture_id="${6:-all}"
  local mode_id="${7:-all}"
  local artifact_relpath="${8:-none}"
  local command_line="${9:-none}"

  python3 - "${EVENTS_JSONL}" \
    "${TRACE_ID}" "${SCENARIO_ID}" "${PRIMARY_BEAD_ID}" "${RUN_ID}" "${phase}" \
    "${event_type}" "${outcome}" "${elapsed_ms}" "${message}" \
    "${fixture_id}" "${mode_id}" "${artifact_relpath}" "${command_line}" <<'PY'
import json
import sys
from datetime import datetime, timezone

path = sys.argv[1]
(
    trace_id,
    scenario_id,
    bead_id,
    run_id,
    phase,
    event_type,
    outcome,
    elapsed_ms,
    message,
    fixture_id,
    mode_id,
    artifact_relpath,
    command_line,
) = sys.argv[2:15]

event = {
    "artifact_manifest_key": "c1_evidence_pack",
    "bead_id": bead_id,
    "command_line": None if command_line == "none" else command_line,
    "elapsed_ms": int(elapsed_ms),
    "event_type": event_type,
    "fixture_id": fixture_id,
    "message": message,
    "mode_id": mode_id,
    "outcome": outcome,
    "phase": phase,
    "artifact_relpath": None if artifact_relpath == "none" else artifact_relpath,
    "run_id": run_id,
    "scenario_id": scenario_id,
    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "trace_id": trace_id,
}
with open(path, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(event, sort_keys=True) + "\n")
PY
}

record_command() {
  local stage="$1"
  local fixture_id="$2"
  local mode_id="$3"
  local command_line="$4"

  python3 - "${COMMANDS_JSONL}" \
    "${TRACE_ID}" "${SCENARIO_ID}" "${PRIMARY_BEAD_ID}" "${RUN_ID}" \
    "${stage}" "${fixture_id}" "${mode_id}" "${command_line}" <<'PY'
import json
import sys
from datetime import datetime, timezone

path = sys.argv[1]
(
    trace_id,
    scenario_id,
    bead_id,
    run_id,
    stage,
    fixture_id,
    mode_id,
    command_line,
) = sys.argv[2:10]

record = {
    "bead_id": bead_id,
    "command_line": command_line,
    "fixture_id": fixture_id,
    "mode_id": mode_id,
    "run_id": run_id,
    "scenario_id": scenario_id,
    "stage": stage,
    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "trace_id": trace_id,
}
with open(path, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(record, sort_keys=True) + "\n")
PY
}

write_build_metadata() {
  local beads_hash="unknown"
  local git_sha="unknown"
  local git_dirty_entries="0"
  local cpu_model="unknown"
  if [[ -f "${PROJECT_ROOT}/.beads/issues.jsonl" ]]; then
    beads_hash="$(sha256sum "${PROJECT_ROOT}/.beads/issues.jsonl" | awk '{print $1}')"
  fi
  if git -C "${PROJECT_ROOT}" rev-parse HEAD >/dev/null 2>&1; then
    git_sha="$(git -C "${PROJECT_ROOT}" rev-parse HEAD)"
    # Not masked: a failing `git status` piped into `wc -l` yields 0, which
    # reads in the artifact as "clean tree". `-1` is recorded instead so an
    # unreadable status is visibly not a clean one.
    if ! git_dirty_entries="$(git -C "${PROJECT_ROOT}" status --porcelain | wc -l | tr -d ' ')"; then
      git_dirty_entries="-1"
    fi
  fi
  cpu_model="$(awk -F: '/model name/ {gsub(/^[ \t]+/, "", $2); print $2; exit}' /proc/cpuinfo 2>/dev/null || true)"
  if [[ -z "${cpu_model}" ]]; then
    cpu_model="unknown"
  fi

  python3 - "${BUILD_METADATA_JSON}" \
    "${PRIMARY_BEAD_ID}" \
    "${COVERED_BEADS}" \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${SCENARIO_ID}" \
    "${HEALTHY_MARGIN_MIN}" \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "$(hostname)" \
    "$(uname -r)" \
    "$(rustc --version)" \
    "$(cargo --version)" \
    "${CARGO_TARGET_DIR:-default}" \
    "${git_sha}" \
    "${git_dirty_entries}" \
    "${beads_hash}" \
    "${cpu_model}" \
    "$(nproc 2>/dev/null || echo unknown)" \
    "${RELEASE_MODE}" \
    "${RELEASE_FROZEN_COMMIT:-}" \
    "${RELEASE_CLEAN_CHECKOUT}" \
    "${RELEASE_RCH_WORKER}" \
    "${RELEASE_RCH_WORKER_SELECTED}" \
    "${RELEASE_RCH_JOB_ID}" \
    "${RELEASE_RCH_JOB_COMMAND}" \
    "${RELEASE_RCH_JOB_PROJECT_ID}" \
    "${RELEASE_RCH_JOB_EXIT_CODE}" \
    "${RELEASE_RCH_JOB_DURATION_MS}" \
    "${RELEASE_SCHEDULER_COMPLETION_RELPATH}" \
    "${RELEASE_SCHEDULER_COMPLETION_SHA256}" \
    "${RELEASE_SCHEDULER_QUEUE_SAMPLES}" \
    "${RELEASE_SCHEDULER_WORKER_SAMPLES}" \
    "${RELEASE_SCHEDULER_JOB_SAMPLES}" \
    "${RCH_STATUS_POLL_SECONDS}" \
    "${RELEASE_SCHEDULER_QUEUE_RELPATH}" \
    "${RELEASE_SCHEDULER_QUEUE_SHA256}" \
    "${RELEASE_SCHEDULER_WORKERS_RELPATH}" \
    "${RELEASE_SCHEDULER_WORKERS_SHA256}" \
    "${RELEASE_BUILD_NONCE}" \
    "${RELEASE_BUILD_NONCE_BINDING}" \
    "${RELEASE_REBUILT_BINARY_SHA256}" \
    "${RELEASE_RUNNING_BINARY_SHA256}" \
    "${RELEASE_RUNNING_BINARY_VERIFIED}" <<'PY'
import json
import sys

path = sys.argv[1]
document = {
    "generated_at_utc": sys.argv[8],
    "primary_bead_id": sys.argv[2],
    "covered_beads": [item for item in sys.argv[3].split(",") if item],
    "run_id": sys.argv[4],
    "trace_id": sys.argv[5],
    "scenario_id": sys.argv[6],
    "cargo_profile": "release-perf",
    "healthy_margin_min": float(sys.argv[7]),
    "hostname": sys.argv[9],
    "kernel_release": sys.argv[10],
    "rustc_version": sys.argv[11],
    "cargo_version": sys.argv[12],
    "cargo_target_dir": sys.argv[13],
    "git_sha": sys.argv[14],
    "git_dirty_entries": int(sys.argv[15]),
    "beads_data_hash": sys.argv[16],
    "cpu_model": sys.argv[17],
    # `nproc` falls back to the string "unknown"; recorded as null rather than
    # crashing the provenance writer on int("unknown").
    "cpu_cores": int(sys.argv[18]) if sys.argv[18].isdigit() else None,
    # Citation-grade markers. Without these a consumer cannot tell a
    # release-contracted pack from an ordinary diagnostic one: the guard
    # refuses at capture time, but that refusal leaves no trace in the
    # artifact. `frozen_commit` is null outside release mode; when present it
    # is the exact 40-hex source commit that HEAD was proven equal to, and
    # `clean_checkout` records that `git status --porcelain
    # --untracked-files=all` was empty at that moment.
    "release_mode": sys.argv[19] == "1",
    "frozen_commit": sys.argv[20] or None,
    "clean_checkout": sys.argv[21] == "1",
    # Execution-environment attestations. All null outside release mode, so a
    # verifier can reject an ordinary diagnostic pack on any one of them.
    # `rch_worker` is what was requested. `rch_worker_selected` is that same
    # host CONFIRMED to be where the adopted job actually ran: the queue trace
    # is scoped to it, and if the daemon had relocated the build elsewhere the
    # scan would have found no job there and refused. `rch_job_id` comes from
    # the queue trace, not from build-log text — the log's marker grammar is
    # undocumented, so it is used only as optional corroboration and a
    # contradicting marker is refused.
    "rch_worker": sys.argv[22] or None,
    "rch_worker_selected": sys.argv[23] or None,
    "rch_job_id": sys.argv[24] or None,
    # The daemon-reported command for the adopted job, so a reader can confirm
    # the pack attributed the right build rather than trusting the id alone.
    "rch_job_command": sys.argv[25] or None,
    "rch_job_project_id": sys.argv[26] or None,
    # Completion evidence. The active-sample trace proves the job was alone on
    # the worker; it cannot prove how the job ENDED. These come from exactly one
    # matching `data.daemon.recent_builds` record captured immediately after the
    # build process returned, matched on the full decimal job id, worker,
    # project and exact command, and required to report location=remote,
    # exit_code=0 and cancellation=null. An absent record is a refusal, which is
    # what closes the gap between the last active sample and the build
    # returning.
    "rch_job_exit_code": int(sys.argv[27]) if sys.argv[27] else None,
    "rch_job_duration_ms": int(sys.argv[28]) if sys.argv[28].isdigit() else None,
    "rch_completion_snapshot_relpath": sys.argv[29] or None,
    "rch_completion_snapshot_sha256": sys.argv[30] or None,
    # Daemon-side scheduler isolation, from two retained raw streams:
    # `rch queue --json` (authoritative job ids, worker, command) and
    # `rch status --workers --jobs --json` (live worker health/circuit state;
    # `rch workers list --json` carries static configuration only and cannot
    # support a health claim).
    #
    # HONEST SCOPE: this is isolation SAMPLED at
    # `scheduler_isolation_poll_interval_secs`, not continuous isolation. It
    # establishes that at every recorded sample the worker carried at most one
    # active build and that build was ours. A foreign build contained entirely
    # between two samples is not observable through this interface and is NOT
    # excluded by this evidence. The sample counts and interval are recorded so
    # a reader can size that gap instead of inferring a guarantee.
    "scheduler_isolation_method": "sampled_poll" if sys.argv[19] == "1" else None,
    "scheduler_isolation_continuous": False if sys.argv[19] == "1" else None,
    "scheduler_isolation_queue_samples": int(sys.argv[31]) if sys.argv[31] else None,
    "scheduler_isolation_worker_samples": int(sys.argv[32]) if sys.argv[32] else None,
    "scheduler_isolation_samples_observing_job": int(sys.argv[33]) if sys.argv[33] else None,
    "scheduler_isolation_poll_interval_secs": float(sys.argv[34]) if sys.argv[19] == "1" else None,
    "scheduler_isolation_queue_trace_relpath": sys.argv[35] or None,
    "scheduler_isolation_queue_trace_sha256": sys.argv[36] or None,
    "scheduler_isolation_workers_trace_relpath": sys.argv[37] or None,
    "scheduler_isolation_workers_trace_sha256": sys.argv[38] or None,
    "build_nonce": sys.argv[39] or None,
    # What the nonce actually establishes, probed from the benchmark binary's
    # source rather than assumed. `forces_rebuild_only` means the nonce
    # invalidated the crate and so guaranteed a fresh compile, but is NOT
    # recoverable from the running binary and therefore does not bind the
    # artifact to this run. `compiled_into_binary` would mean it does. Consumers
    # must not treat a `forces_rebuild_only` nonce as an identity attestation;
    # the binding evidence is frozen_commit + the captured build log + the
    # binary digests below.
    "build_nonce_binding": sys.argv[40] or None,
    # Digest of the artifact produced by this run's verified rebuild.
    "rebuilt_binary_sha256": sys.argv[41] or None,
    # Digest re-taken from the binary immediately before each measurement and
    # required to equal `rebuilt_binary_sha256` every time.
    "running_binary_sha256": sys.argv[42] or None,
    # True only when this run hashed the exact binary it executed and matched it
    # against the artifact it had just rebuilt, before any benchmark ran.
    "running_binary_sha256_verified": sys.argv[43] == "1",
}
with open(path, "w", encoding="utf-8") as handle:
    json.dump(document, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

resolve_binary_path() {
  BINARY="${CARGO_TARGET_DIR:-$PROJECT_ROOT/target}/release-perf/realdb-e2e"
}

# Release mode never reuses a binary it did not just build. An artifact that is
# already on disk carries no evidence of which source, which toolchain or which
# host produced it, so accepting it would leave the pack's central claim —
# "these numbers came from this commit" — resting on a filename. Ordinary
# captures keep the cheap reuse path.
ensure_binary() {
  resolve_binary_path
  if [[ "${RELEASE_MODE}" != "1" && -f "${BINARY}" ]]; then
    return
  fi

  local started finished elapsed build_cmd build_started_epoch
  local -a build_argv=(cargo build --profile release-perf -p fsqlite-e2e --bin realdb-e2e)
  if [[ "${RELEASE_MODE}" == "1" ]]; then
    # `--locked` refuses a lockfile edit, keeping the dependency graph identical
    # to the frozen commit. `--verbose` is what makes the log admissible: it is
    # the record the worker and job markers are parsed out of. The offload is
    # explicit rather than hook-dependent so the build cannot quietly run on
    # this machine, and the nonce is passed through as the cache-buster that
    # guarantees a real recompile. Ordinary captures keep the plain build:
    # `--locked` would newly fail them on any in-progress lockfile edit, which
    # is a release concern and not theirs.
    build_argv=(
      "${RCH_BIN:-rch}" exec -- env
      "FSQLITE_BENCH_BUILD_NONCE=${RELEASE_BUILD_NONCE}"
      "FSQLITE_BENCH_PROFILE_NAME=release-perf"
      cargo build --locked --verbose
      --profile release-perf -p fsqlite-e2e --bin realdb-e2e
    )
  fi
  build_cmd="${build_argv[*]}"
  record_command "build" "all" "build" "${build_cmd}"
  emit_event "build" "start" "running" 0 "building release-perf realdb-e2e" "all" "build" "none" "${build_cmd}"
  started="$(date +%s%3N)"
  build_started_epoch="$(date +%s)"

  local build_status=0
  if [[ "${RELEASE_MODE}" == "1" ]]; then
    run_traced_release_build "${OUTPUT_DIR}/build_stdout.log" "${build_argv[@]}" \
      || build_status=$?
  elif (
    cd "${PROJECT_ROOT}"
    "${build_argv[@]}"
  ) 2>&1 | tee "${OUTPUT_DIR}/build_stdout.log"; then
    build_status=0
  else
    build_status=1
  fi

  finished="$(date +%s%3N)"
  elapsed="$((finished - started))"
  if [[ "${build_status}" -ne 0 ]]; then
    emit_event "build" "fail" "fail" "${elapsed}" "failed to build release-perf realdb-e2e" "all" "build" "build_stdout.log" "${build_cmd}"
    return 1
  fi
  emit_event "build" "pass" "pass" "${elapsed}" "built release-perf realdb-e2e" "all" "build" "build_stdout.log" "${build_cmd}"

  if [[ "${RELEASE_MODE}" == "1" ]]; then
    verify_release_build "${OUTPUT_DIR}/build_stdout.log" "${build_started_epoch}"
  fi
}

# Run the offloaded build while sampling daemon status alongside it. The build
# is backgrounded rather than piped so its liveness can drive the poll loop; its
# transcript is replayed to stdout afterwards, so the operator still sees the
# same output and the retained log is byte-identical to what the markers are
# parsed from.
run_traced_release_build() {
  local log_path="$1"
  shift
  local build_pid build_status=0

  require_status_tooling
  : > "$log_path"
  : > "$SCHEDULER_QUEUE_JSONL"
  : > "$SCHEDULER_WORKERS_JSONL"
  : > "$SCHEDULER_DIAGNOSTICS_LOG"
  (
    cd "${PROJECT_ROOT}"
    "$@"
  ) > "$log_path" 2>&1 &
  build_pid=$!
  # Sampling opens before the daemon can have scheduled anything for this build
  # and closes when the build process exits. Both interfaces are sampled in the
  # same iteration so a queue sample and a health sample describe the same
  # moment as closely as two sequential calls allow.
  while kill -0 "$build_pid" 2>/dev/null; do
    capture_queue_snapshot "$SCHEDULER_QUEUE_JSONL" "$SCHEDULER_DIAGNOSTICS_LOG"
    capture_worker_snapshot "$SCHEDULER_WORKERS_JSONL" "$SCHEDULER_DIAGNOSTICS_LOG"
    sleep "$RCH_STATUS_POLL_SECONDS"
  done
  wait "$build_pid" || build_status=$?
  # Taken immediately after the process returns, before anything else, so the
  # completion record is read as close as possible to the moment the job ended.
  # Captured even on a failed build: the snapshot is the evidence for WHY it
  # failed and must not be discarded along with the exit status.
  capture_completion_snapshot "$SCHEDULER_COMPLETION_JSON" "$SCHEDULER_DIAGNOSTICS_LOG"
  cat "$log_path"
  return "$build_status"
}

# Turn the captured build log into the pack's execution provenance, then bind
# it to the artifact. Order matters: the log is checked for evidence that a
# compile happened at all before its worker and job markers are trusted, and
# the artifact's freshness is proven before its digest is taken as the
# authority for every later measurement.
verify_release_build() {
  local log_path="$1"
  local build_started_epoch="$2"
  local artifact_mtime

  require_rebuild_evidence "$log_path"

  # Identity comes from the daemon, not the transcript: the requested worker is
  # the scope, and the queue trace says what actually ran there.
  local isolation_json=""
  isolation_json="$(verify_scheduler_isolation_trace \
    "$RELEASE_RCH_WORKER" "$RELEASE_BUILD_COMMAND_MARKER" \
    "$SCHEDULER_QUEUE_JSONL" "$SCHEDULER_WORKERS_JSONL")" || exit 2

  RELEASE_RCH_WORKER_SELECTED="$RELEASE_RCH_WORKER"
  require_allowed_remote_worker "$RELEASE_RCH_WORKER_SELECTED"
  assert_worker_marker_agrees \
    "$(worker_marker_from_rch_log "$log_path")" "$RELEASE_RCH_WORKER_SELECTED"

  RELEASE_RCH_JOB_ID="$(printf '%s' "$isolation_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')"
  RELEASE_RCH_JOB_COMMAND="$(printf '%s' "$isolation_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["command"])')"
  local job_project_id
  job_project_id="$(printf '%s' "$isolation_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["project_id"])')"
  RELEASE_SCHEDULER_QUEUE_SAMPLES="$(printf '%s' "$isolation_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["queue_samples"])')"
  RELEASE_SCHEDULER_WORKER_SAMPLES="$(printf '%s' "$isolation_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["worker_samples"])')"
  RELEASE_SCHEDULER_JOB_SAMPLES="$(printf '%s' "$isolation_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["samples_observing_job"])')"

  # Binding each trace's own digest into the metadata is what stops a retained
  # trace from being swapped for a friendlier one after the fact: the pack
  # asserts which bytes it was judged against.
  # Completion is adjudicated only after the active trace has fixed the job's
  # identity, and the match is exact on all of id, worker, project and command.
  local completion_json=""
  completion_json="$(verify_release_completion \
    "$RELEASE_RCH_WORKER_SELECTED" "$RELEASE_RCH_JOB_ID" \
    "$job_project_id" "$RELEASE_RCH_JOB_COMMAND" \
    "$SCHEDULER_COMPLETION_JSON")" || exit 2
  RELEASE_RCH_JOB_PROJECT_ID="$job_project_id"
  RELEASE_RCH_JOB_EXIT_CODE="$(printf '%s' "$completion_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["exit_code"])')"
  RELEASE_RCH_JOB_DURATION_MS="$(printf '%s' "$completion_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["duration_ms"])')"

  RELEASE_SCHEDULER_QUEUE_SHA256="$(sha256_of_regular_file "$SCHEDULER_QUEUE_JSONL")" || exit 2
  RELEASE_SCHEDULER_WORKERS_SHA256="$(sha256_of_regular_file "$SCHEDULER_WORKERS_JSONL")" || exit 2
  RELEASE_SCHEDULER_COMPLETION_SHA256="$(sha256_of_regular_file "$SCHEDULER_COMPLETION_JSON")" || exit 2
  RELEASE_SCHEDULER_QUEUE_RELPATH="$(basename "$SCHEDULER_QUEUE_JSONL")"
  RELEASE_SCHEDULER_WORKERS_RELPATH="$(basename "$SCHEDULER_WORKERS_JSONL")"
  RELEASE_SCHEDULER_COMPLETION_RELPATH="$(basename "$SCHEDULER_COMPLETION_JSON")"

  [[ -f "${BINARY}" ]] \
    || die "release build reported success but produced no artifact at ${BINARY}; refusing to cite a build whose output never arrived"
  artifact_mtime="$(stat -c %Y "${BINARY}" 2>/dev/null || echo "")"
  assert_artifact_rebuilt "${artifact_mtime}" "${build_started_epoch}" "${BINARY}"

  # Hashing a large binary is not atomic, so the file identity is pinned on
  # both sides of the digest and required to be unchanged. Without this a
  # replacement landing mid-read yields a digest of neither the old nor the new
  # file, which would then become the authority every later measurement is
  # checked against. This closes the during-hash window; it does NOT close the
  # window between the build exiting and this first stat, which no in-process
  # check can observe — see the honest wording on `verify_running_binary`.
  local identity_before identity_after
  identity_before="$(artifact_identity "${BINARY}")"
  RELEASE_REBUILT_BINARY_SHA256="$(sha256_of_regular_file "${BINARY}")" || exit 2
  identity_after="$(artifact_identity "${BINARY}")"
  assert_artifact_identity_stable "$identity_before" "$identity_after" "${BINARY}"
  # Optional operator pin, asserted against the artifact rather than trusted in
  # place of it.
  if [[ -n "${FSQLITE_RELEASE_RUNNING_BINARY_SHA256:-}" ]]; then
    assert_running_binary_digest \
      "${FSQLITE_RELEASE_RUNNING_BINARY_SHA256}" "${RELEASE_REBUILT_BINARY_SHA256}" \
      "${BINARY}" "the operator-pinned FSQLITE_RELEASE_RUNNING_BINARY_SHA256"
  fi

  emit_event "build" "note" "pass" 0 \
    "release build verified on worker ${RELEASE_RCH_WORKER_SELECTED} job ${RELEASE_RCH_JOB_ID}; sole active build at every one of ${RELEASE_SCHEDULER_QUEUE_SAMPLES} queue sample(s) at ${RCH_STATUS_POLL_SECONDS}s interval (sampled, not continuous)" \
    "all" "build" "${RELEASE_SCHEDULER_QUEUE_RELPATH}" "none"
}

run_bench_mode() {
  local fixture_id="$1"
  local mode_id="$2"
  local mode_label="$3"
  shift 3
  local -a mode_args=("$@")
  local result_relpath="c1_${fixture_id}_${mode_id}.jsonl"
  local log_relpath="c1_${fixture_id}_${mode_id}_stdout.log"
  local started finished elapsed
  local -a cmd=(
    "${BINARY}" bench
    --db "${fixture_id}"
    --preset "${WORKLOADS}"
    --concurrency "${CONCURRENCY}"
    --repeat "${REPEAT}"
    --output-jsonl "${OUTPUT_DIR}/${result_relpath}"
    --pretty
    "${mode_args[@]}"
  )
  local command_line="${cmd[*]}"

  verify_binary_before_measurement
  record_command "bench" "${fixture_id}" "${mode_id}" "${command_line}"
  emit_event "bench" "start" "running" 0 "running ${mode_label}" "${fixture_id}" "${mode_id}" "${result_relpath}" "${command_line}"
  started="$(date +%s%3N)"
  if "${cmd[@]}" 2>&1 | tee "${OUTPUT_DIR}/${log_relpath}"; then
    finished="$(date +%s%3N)"
    elapsed="$((finished - started))"
    emit_event "bench" "pass" "pass" "${elapsed}" "completed ${mode_label}" "${fixture_id}" "${mode_id}" "${result_relpath}" "${command_line}"
  else
    finished="$(date +%s%3N)"
    elapsed="$((finished - started))"
    emit_event "bench" "fail" "fail" "${elapsed}" "failed ${mode_label}" "${fixture_id}" "${mode_id}" "${result_relpath}" "${command_line}"
    return 1
  fi
}

run_hot_profile() {
  local fixture_id="$1"
  local hot_dir_relpath="hotprofile_${fixture_id}_${HOT_PROFILE_WORKLOAD_TAG}_c1"
  local hot_dir="${OUTPUT_DIR}/${hot_dir_relpath}"
  local log_relpath="c1_${fixture_id}_hotprofile_${HOT_PROFILE_WORKLOAD_TAG}.log"
  local started finished elapsed
  local -a cmd=(
    "${BINARY}" hot-profile
    --db "${fixture_id}"
    --preset "${HOT_PROFILE_WORKLOAD}"
    --concurrency "${CONCURRENCY}"
    --mvcc
    --output-dir "${hot_dir}"
    --pretty
  )
  local command_line="${cmd[*]}"

  verify_binary_before_measurement
  mkdir -p "${hot_dir}"
  record_command "hot_profile" "${fixture_id}" "fsqlite_mvcc" "${command_line}"
  emit_event "hot_profile" "start" "running" 0 "running c1 hot profile" "${fixture_id}" "fsqlite_mvcc" "${hot_dir_relpath}" "${command_line}"
  started="$(date +%s%3N)"
  if "${cmd[@]}" 2>&1 | tee "${OUTPUT_DIR}/${log_relpath}"; then
    finished="$(date +%s%3N)"
    elapsed="$((finished - started))"
    emit_event "hot_profile" "pass" "pass" "${elapsed}" "completed c1 hot profile" "${fixture_id}" "fsqlite_mvcc" "${hot_dir_relpath}" "${command_line}"
  else
    finished="$(date +%s%3N)"
    elapsed="$((finished - started))"
    emit_event "hot_profile" "fail" "fail" "${elapsed}" "failed c1 hot profile" "${fixture_id}" "fsqlite_mvcc" "${hot_dir_relpath}" "${command_line}"
    return 1
  fi
}

render_reports() {
  emit_event "render" "start" "running" 0 "rendering c1 scorecard and manifest"
  python3 - \
    "${OUTPUT_DIR}" \
    "${MANIFEST_JSON}" \
    "${SCORECARD_JSON}" \
    "${SUMMARY_MD}" \
    "${BUILD_METADATA_JSON}" \
    "${COMMANDS_JSONL}" \
    "${TRACE_ID}" \
    "${SCENARIO_ID}" \
    "${RUN_ID}" \
    "${PRIMARY_BEAD_ID}" \
    "${HEALTHY_MARGIN_MIN}" \
    "${WORKLOADS}" \
    "${DB_FIXTURES}" \
    "${CONCURRENCY}" \
    "${REPEAT}" \
    "${HOT_PROFILE_WORKLOAD}" \
    "${HOT_PROFILE_WORKLOAD_TAG}" <<'PY'
import json
import math
import os
import sys
from pathlib import Path

output_dir = Path(sys.argv[1])
manifest_path = Path(sys.argv[2])
scorecard_path = Path(sys.argv[3])
summary_path = Path(sys.argv[4])
build_metadata_path = Path(sys.argv[5])
commands_path = Path(sys.argv[6])
trace_id = sys.argv[7]
scenario_id = sys.argv[8]
run_id = sys.argv[9]
bead_id = sys.argv[10]
healthy_margin_min = float(sys.argv[11])
workloads = [w for w in sys.argv[12].split(",") if w]
fixtures = [f for f in sys.argv[13].split(",") if f]
concurrency = int(sys.argv[14])
repeat = int(sys.argv[15])
hot_profile_workload = sys.argv[16]
hot_profile_workload_tag = sys.argv[17]

mode_specs = [
    ("sqlite3", "C SQLite"),
    ("fsqlite_mvcc", "FrankenSQLite MVCC"),
    ("fsqlite_single", "FrankenSQLite Single Writer"),
]

with build_metadata_path.open("r", encoding="utf-8") as handle:
    build_metadata = json.load(handle)

trace_id = build_metadata.get("trace_id", trace_id)
scenario_id = build_metadata.get("scenario_id", scenario_id)
run_id = build_metadata.get("run_id", run_id)
bead_id = build_metadata.get("primary_bead_id", bead_id)
healthy_margin_min = float(build_metadata.get("healthy_margin_min", healthy_margin_min))

commands = []
with commands_path.open("r", encoding="utf-8") as handle:
    for line in handle:
        line = line.strip()
        if line:
            commands.append(json.loads(line))

summaries = {}
artifacts = []
for fixture in fixtures:
    for mode_id, mode_label in mode_specs:
        relpath = f"c1_{fixture}_{mode_id}.jsonl"
        log_relpath = f"c1_{fixture}_{mode_id}_stdout.log"
        path = output_dir / relpath
        if path.exists():
            entries = []
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    record = json.loads(line)
                    if "benchmark_id" not in record or "throughput" not in record:
                        continue
                    entries.append(record)
            summaries[(fixture, mode_id)] = entries
            artifacts.append(
                {
                    "fixture_id": fixture,
                    "mode_id": mode_id,
                    "mode_label": mode_label,
                    "result_jsonl": relpath,
                    "stdout_log": log_relpath,
                }
            )

hot_profiles = []
for fixture in fixtures:
    candidates = [
        (
            f"hotprofile_{fixture}_{hot_profile_workload_tag}_c1",
            f"c1_{fixture}_hotprofile_{hot_profile_workload_tag}.log",
        ),
        (f"hotprofile_{fixture}_commutative_c1", f"c1_{fixture}_hotprofile_commutative.log"),
    ]
    selected = None
    for relpath, log_relpath in candidates:
        if (output_dir / relpath).exists() or (output_dir / log_relpath).exists():
            selected = (relpath, log_relpath)
            break
    if selected is not None:
        relpath, log_relpath = selected
        hot_profiles.append(
            {
                "fixture_id": fixture,
                "workload": hot_profile_workload,
                "directory": relpath,
                "stdout_log": log_relpath,
            }
        )
hot_profile_dirs = {entry["fixture_id"]: entry["directory"] for entry in hot_profiles}

rows = []
ratio_buckets = {
    "fsqlite_mvcc": {"below_parity": 0, "parity_to_margin": 0, "healthy_margin": 0},
    "fsqlite_single": {"below_parity": 0, "parity_to_margin": 0, "healthy_margin": 0},
}
ratio_values = {"fsqlite_mvcc": [], "fsqlite_single": []}

def classify_ratio(ratio: float) -> str:
    if ratio < 1.0:
        return "below_parity"
    if ratio < healthy_margin_min:
        return "parity_to_margin"
    return "healthy_margin"

for fixture in fixtures:
    sqlite_entries = {
        row["workload"]: row
        for row in summaries.get((fixture, "sqlite3"), [])
    }
    for mode_id, mode_label in mode_specs[1:]:
        for row in summaries.get((fixture, mode_id), []):
            workload = row["workload"]
            baseline = sqlite_entries.get(workload)
            median_ops = row["throughput"]["median_ops_per_sec"]
            median_latency = row["latency"]["median_ms"]
            p95_latency = row["latency"]["p95_ms"]
            retries_total = sum(it["retries"] for it in row.get("iterations", []))
            aborts_total = sum(it["aborts"] for it in row.get("iterations", []))
            if baseline is None:
                ratio = None
                classification = "missing_baseline"
                sqlite_median_ops = None
                sqlite_median_latency = None
            else:
                sqlite_median_ops = baseline["throughput"]["median_ops_per_sec"]
                sqlite_median_latency = baseline["latency"]["median_ms"]
                ratio = (median_ops / sqlite_median_ops) if sqlite_median_ops > 0 else None
                if ratio is None:
                    classification = "missing_baseline"
                else:
                    classification = classify_ratio(ratio)
                    ratio_buckets[mode_id][classification] += 1
                    ratio_values[mode_id].append(ratio)

            rows.append(
                {
                    "row_id": f"{fixture}:{workload}:{mode_id}",
                    "fixture_id": fixture,
                    "workload": workload,
                    "mode_id": mode_id,
                    "mode_label": mode_label,
                    "median_ops_per_sec": median_ops,
                    "median_latency_ms": median_latency,
                    "p95_latency_ms": p95_latency,
                    "sqlite_median_ops_per_sec": sqlite_median_ops,
                    "sqlite_median_latency_ms": sqlite_median_latency,
                    "speedup_vs_sqlite": ratio,
                    "classification": classification,
                    "retries_total": retries_total,
                    "aborts_total": aborts_total,
                    "measurement_count": row["measurement_count"],
                    "total_measurement_ms": row["total_measurement_ms"],
                    "hot_profile_dir": hot_profile_dirs.get(fixture),
                }
            )

def geometric_mean(values):
    positives = [value for value in values if value and value > 0]
    if not positives:
        return None
    return math.exp(sum(math.log(value) for value in positives) / len(positives))

below_rows = [row for row in rows if row["classification"] == "below_parity"]
margin_rows = [row for row in rows if row["classification"] == "parity_to_margin"]
healthy_rows = [row for row in rows if row["classification"] == "healthy_margin"]
missing_baseline_rows = [row for row in rows if row["classification"] == "missing_baseline"]
expected_critical_cell_count = len(fixtures) * len(workloads) * 2
comparable_rows = [row for row in rows if row["speedup_vs_sqlite"] is not None]
if not comparable_rows:
    honest_gate_verdict = "no_data"
elif len(comparable_rows) < expected_critical_cell_count or missing_baseline_rows:
    honest_gate_verdict = "incomplete"
elif below_rows:
    honest_gate_verdict = "fail"
elif margin_rows:
    honest_gate_verdict = "warning"
else:
    honest_gate_verdict = "pass"

mode_rollup = []
for mode_id, mode_label in mode_specs[1:]:
    mode_rollup.append(
        {
            "mode_id": mode_id,
            "mode_label": mode_label,
            "comparable_cell_count": len(ratio_values[mode_id]),
            "geometric_mean_speedup": geometric_mean(ratio_values[mode_id]),
            **ratio_buckets[mode_id],
        }
    )

workload_rollup = []
for mode_id, mode_label in mode_specs[1:]:
    for workload in workloads:
        values = [
            row["speedup_vs_sqlite"]
            for row in rows
            if row["mode_id"] == mode_id and row["workload"] == workload and row["speedup_vs_sqlite"]
        ]
        workload_rollup.append(
            {
                "mode_id": mode_id,
                "mode_label": mode_label,
                "workload": workload,
                "geometric_mean_speedup": geometric_mean(values),
                "comparable_cell_count": len(values),
            }
        )

scorecard = {
    "schema_version": "bd-db300.c1_evidence_pack_scorecard.v1",
    "bead_id": bead_id,
    "covered_beads": build_metadata["covered_beads"],
    "run_id": run_id,
    "trace_id": trace_id,
    "scenario_id": scenario_id,
    "pack_role": "honest_gate_scorecard",
    "baseline_comparator": "sqlite3_same_pack",
    "shadow_lineage": "none",
    "critical_scope": "all c1 fixture/workload/mode cells captured by this pack",
    "comparator_contract": {
        "baseline_comparator": "sqlite3_same_pack",
        "comparator_engine": "sqlite3",
        "comparator_scope": "same fixture, same workload, same pack",
        "aggregate_rows_are_secondary": True,
    },
    "causal_attribution_contract": {
        "required_for_claimed_fix": True,
        "required_claim_fields": [
            "code_change_ref",
            "claim_summary",
            "baseline_run_id",
            "baseline_comparator",
            "cells_expected_to_move",
            "cells_expected_not_to_move",
            "negative_findings",
        ],
    },
    "honest_gate_summary": {
        "verdict": honest_gate_verdict,
        "expected_critical_cell_count": expected_critical_cell_count,
        "critical_cell_count": len(rows),
        "comparable_cell_count": len(comparable_rows),
        "missing_baseline_count": len(missing_baseline_rows),
        "below_parity_count": len(below_rows),
        "parity_to_margin_count": len(margin_rows),
        "healthy_margin_count": len(healthy_rows),
        "hard_fail_when_below_parity_present": True,
        "critical_red_cell_ids": [row["row_id"] for row in below_rows],
        "margin_band_cell_ids": [row["row_id"] for row in margin_rows],
        "missing_baseline_row_ids": [row["row_id"] for row in missing_baseline_rows],
    },
    "healthy_margin_min": healthy_margin_min,
    "concurrency": concurrency,
    "repeat": repeat,
    "fixtures": fixtures,
    "workloads": workloads,
    "rows": rows,
    "mode_rollup": mode_rollup,
    "workload_rollup": workload_rollup,
    "below_parity_rows": below_rows,
    "parity_to_margin_rows": margin_rows,
    "healthy_margin_rows": healthy_rows,
    "missing_baseline_rows": missing_baseline_rows,
}
scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n", encoding="utf-8")

manifest = {
    "schema_version": "bd-db300.c1_evidence_pack_manifest.v1",
    "bead_id": bead_id,
    "covered_beads": build_metadata["covered_beads"],
    "run_id": run_id,
    "trace_id": trace_id,
    "scenario_id": scenario_id,
    "output_dir": output_dir.name,
    "entrypoint": "scripts/capture_c1_evidence_pack.sh",
    "pack_role": "honest_gate_evidence_pack",
    "baseline_comparator": "sqlite3_same_pack",
    "shadow_lineage": "none",
    "comparator_contract": scorecard["comparator_contract"],
    "causal_attribution_contract": scorecard["causal_attribution_contract"],
    "honest_gate_summary": scorecard["honest_gate_summary"],
    "build_metadata": build_metadata,
    "fixtures": fixtures,
    "workloads": workloads,
    "concurrency": concurrency,
    "repeat": repeat,
    "healthy_margin_min": healthy_margin_min,
    "build_metadata_json": build_metadata_path.name,
    "build_metadata_relpath": build_metadata_path.name,
    "commands_jsonl": commands_path.name,
    "commands_relpath": commands_path.name,
    "events_jsonl": "events.jsonl",
    "events_relpath": "events.jsonl",
    "scorecard_json": scorecard_path.name,
    "scorecard_relpath": scorecard_path.name,
    "summary_md": summary_path.name,
    "summary_relpath": summary_path.name,
    "hashes_relpath": "artifact_hashes.txt",
    "bench_artifacts": artifacts,
    "hot_profiles": hot_profiles,
    "command_count": len(commands),
}
manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

summary_lines = [
    f"# {bead_id} c1 Evidence Pack",
    "",
    f"- run_id: `{run_id}`",
    f"- trace_id: `{trace_id}`",
    f"- scenario_id: `{scenario_id}`",
    f"- fixtures: `{', '.join(fixtures)}`",
    f"- workloads: `{', '.join(workloads)}`",
    f"- concurrency: `{concurrency}`",
    f"- repeat: `{repeat}`",
    f"- healthy_margin_min: `{healthy_margin_min:.2f}x`",
    "",
    "## Honest Gate Summary",
    "",
    f"- verdict: `{honest_gate_verdict}`",
    "- critical_scope: every c1 fixture/workload/mode cell in this pack is a critical gate cell",
    "- baseline_comparator: same-pack `sqlite3` rows for the matching fixture and workload",
    f"- expected_critical_cell_count: `{expected_critical_cell_count}`",
    f"- comparable_cell_count: `{len(comparable_rows)}`",
    f"- missing_baseline_count: `{len(missing_baseline_rows)}`",
    f"- below_parity_count: `{len(below_rows)}`",
    f"- parity_to_margin_count: `{len(margin_rows)}`",
    f"- healthy_margin_count: `{len(healthy_rows)}`",
    "- aggregate rollups are secondary and must not be used to hide a red c1 cell",
    "",
    "## Mode Rollup",
    "",
    "| Mode | Geometric Mean Speedup | Below 1.0x | 1.0x to Margin | Healthy Margin |",
    "|------|------------------------|------------|----------------|----------------|",
]
for row in mode_rollup:
    gm = row["geometric_mean_speedup"]
    gm_str = "n/a" if gm is None else f"{gm:.3f}x"
    summary_lines.append(
        f"| {row['mode_label']} | {gm_str} | {row['below_parity']} | {row['parity_to_margin']} | {row['healthy_margin']} |"
    )

summary_lines.extend(
    [
        "",
        "## Workload Rollup",
        "",
        "| Mode | Workload | Geometric Mean Speedup | Comparable Cells |",
        "|------|----------|------------------------|------------------|",
    ]
)
for row in workload_rollup:
    gm = row["geometric_mean_speedup"]
    gm_str = "n/a" if gm is None else f"{gm:.3f}x"
    summary_lines.append(
        f"| {row['mode_label']} | {row['workload']} | {gm_str} | {row['comparable_cell_count']} |"
    )

summary_lines.extend(
    [
        "",
        "## Cell Scorecard",
        "",
        "| Fixture | Workload | Mode | Median ops/s | SQLite median ops/s | Speedup | Median latency (ms) | P95 latency (ms) | Retries | Aborts | Verdict |",
        "|---------|----------|------|--------------|---------------------|---------|---------------------|------------------|---------|--------|---------|",
    ]
)
for row in rows:
    speedup = row["speedup_vs_sqlite"]
    speedup_str = "n/a" if speedup is None else f"{speedup:.3f}x"
    sqlite_ops = row["sqlite_median_ops_per_sec"]
    sqlite_ops_str = "n/a" if sqlite_ops is None else f"{sqlite_ops:.2f}"
    summary_lines.append(
        f"| {row['fixture_id']} | {row['workload']} | {row['mode_label']} | {row['median_ops_per_sec']:.2f} | {sqlite_ops_str} | {speedup_str} | {row['median_latency_ms']:.3f} | {row['p95_latency_ms']:.3f} | {row['retries_total']} | {row['aborts_total']} | {row['classification']} |"
    )

summary_lines.extend(["", "## Cells Still Below Parity", ""])
if not comparable_rows:
    summary_lines.append("- no comparable c1 cells were captured in this pack")
elif below_rows:
    for row in below_rows:
        hot_profile_note = row["hot_profile_dir"] or "none"
        summary_lines.append(
            f"- `{row['fixture_id']}:{row['workload']}:{row['mode_id']}` at `{row['speedup_vs_sqlite']:.3f}x`; hot-profile bundle: `{hot_profile_note}`"
        )
else:
    summary_lines.append("- none")

summary_lines.extend(["", "## Comparator and Hot-Profile Bundles", ""])
if hot_profiles:
    for entry in hot_profiles:
        summary_lines.append(
            f"- `{entry['fixture_id']}` hot-profile dir: `{entry['directory']}` for workload `{entry['workload']}`"
        )
else:
    summary_lines.append("- no hot-profile bundles were captured in this pack")

summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
PY
  emit_event "render" "pass" "pass" 0 "rendered c1 scorecard and manifest"
}

hash_artifacts() {
  (
    cd "${OUTPUT_DIR}"
    find . -type f ! -name "$(basename "${HASHES_TXT}")" -print0 \
      | sort -z \
      | xargs -0 sha256sum > "$(basename "${HASHES_TXT}")"
  )
  emit_event "hash" "pass" "pass" 0 "hashed c1 evidence artifacts"
}

main() {
  local display_bead_id="${PRIMARY_BEAD_ID}"
  if [[ "${RENDER_ONLY}" == "1" && -f "${BUILD_METADATA_JSON}" ]]; then
    display_bead_id="$(python3 - "${BUILD_METADATA_JSON}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    data = json.load(handle)
print(data.get("primary_bead_id", "unknown"))
PY
)"
  fi

  echo "=== ${display_bead_id}: c1 Evidence Pack ==="
  echo "Output: ${OUTPUT_DIR}"
  echo "Profile: release-perf"
  echo "Fixtures: ${DB_FIXTURES}"
  echo "Workloads: ${WORKLOADS}"
  echo ""

  if [[ "${RENDER_ONLY}" == "1" ]]; then
    if [[ ! -f "${BUILD_METADATA_JSON}" ]]; then
      echo "RENDER_ONLY=1 requires an existing build_metadata.json at ${BUILD_METADATA_JSON}" >&2
      return 1
    fi
    # A release-mode re-render must still prove it describes the exact binary
    # that produced the retained measurements; it may not inherit an unproven
    # digest from the previous pack.
    if [[ "${RELEASE_MODE}" == "1" ]]; then
      release_render_only_reverify
    fi
    emit_event "render" "note" "pass" 0 "render-only refresh: reusing existing c1 raw artifacts"
  else
    ensure_binary
    verify_binary_before_measurement
    write_build_metadata
  fi

  if [[ "${RENDER_ONLY}" == "1" ]]; then
    :
  elif [[ "${SKIP_RUN}" != "1" ]]; then
    for fixture_id in "${FIXTURE_ARRAY[@]}"; do
      echo "====== Fixture: ${fixture_id} ======"
      run_bench_mode "${fixture_id}" "sqlite3" "C SQLite control (c1)" --engine sqlite3
      echo ""
      run_bench_mode "${fixture_id}" "fsqlite_mvcc" "FrankenSQLite MVCC (c1)" --engine fsqlite --mvcc
      echo ""
      run_bench_mode "${fixture_id}" "fsqlite_single" "FrankenSQLite single-writer (c1)" --engine fsqlite --no-mvcc
      echo ""
      run_hot_profile "${fixture_id}"
      echo ""
    done
  else
    emit_event "bench" "skip" "skipped" 0 "skipping benchmark execution because SKIP_RUN=1"
  fi

  render_reports
  hash_artifacts

  printf '%s\n' \
    "=== Evidence pack complete: ${OUTPUT_DIR} ===" \
    "summary: ${SUMMARY_MD}" \
    "manifest: ${MANIFEST_JSON}" \
    "scorecard: ${SCORECARD_JSON}" \
    "events: ${EVENTS_JSONL}" \
    "commands: ${COMMANDS_JSONL}" \
    "hashes: ${HASHES_TXT}"
}

main "$@"
