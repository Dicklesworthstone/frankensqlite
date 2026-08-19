#!/usr/bin/env bash
# PROPOSED_BEADS_2026-05-19.import.sh
#
# Import the proposed beads from PROPOSED_BEADS_2026-05-19.md into the local
# beads workspace. Run from /dp/frankensqlite after Epic 0 (DB restore) lands
# and `br doctor` reports the workspace healthy.
#
# Strategy: each bead is created with `br create` and its returned ID is
# captured into a name→id map. Dependencies are then wired with `br dep add`
# using the captured IDs (we cannot pin the bd-* hash beforehand). On any
# `br create` failure the script aborts and prints the failing bead name so
# you can re-run after fixing.
#
# Safe to re-run: skips any bead whose declared title already exists open in
# the workspace (matched on exact title).

set -euo pipefail

if ! command -v br >/dev/null 2>&1; then
    echo "fatal: 'br' (beads_rust) not on PATH" >&2
    exit 1
fi

# Pre-flight: refuse to run against an actively corrupted DB. `br doctor
# --quick` exit 1 also fires on WARN-level findings (orphaned write locks,
# preserved recovery artifacts, etc.) — those are operator hygiene, not
# corruption, and shouldn't block bead creation. So inspect the JSON and
# refuse only on ERROR-severity anomalies or a `database_corrupt` code.
preflight=$(br doctor --quick --json 2>/dev/null || true)
if echo "$preflight" \
    | jq -e '
        (.reliability_audit.anomalies // [])
        | map(select(
            (.severity? == "error")
            or (.code? == "database_corrupt")
            or (.code? == "sqlite_integrity_failed")
          ))
        | length > 0' >/dev/null 2>&1; then
    cat >&2 <<'EOF'
fatal: br doctor --quick reports active corruption (error-severity or
       database_corrupt anomaly). Fix the workspace before importing.
       Likely Epic 0 (bd-NEW-0.1..0.3) is still pending.
       See PROPOSED_BEADS_2026-05-19.md, section "Epic 0".
EOF
    exit 1
fi

declare -A ID    # logical_name → bd-* hash

# Helper: idempotent create. Echoes the bd-* id.
new() {
    local name="$1" type="$2" prio="$3" title="$4" desc="$5"
    # `br list --status=open --json` returns {"issues": [...], ...}; older
    # builds may return a bare array — handle both.
    local existing
    existing=$(br list --status=open --json 2>/dev/null \
        | jq -r --arg t "$title" '
            (if type == "array" then .
             else (.issues // .items // []) end)
            | .[]
            | select(.title==$t)
            | .id' \
        | head -n1)
    if [[ -n "$existing" ]]; then
        echo "skip ${name} (already open as ${existing})"
        ID[$name]="$existing"
        return 0
    fi
    # `br create --json` returns {"id":"bd-...", ...}; extract the id.
    local create_json id
    create_json=$(br create --type="$type" --priority="$prio" \
        --title="$title" --description="$desc" --json 2>&1)
    id=$(jq -r '.id // empty' <<<"$create_json" 2>/dev/null)
    if [[ -z "$id" || "$id" == "null" ]]; then
        echo "fatal: br create returned no id for ${name}" >&2
        echo "       output: ${create_json:0:200}" >&2
        exit 2
    fi
    ID[$name]="$id"
    echo "created ${name} → ${id}"
}

dep() {
    local from="$1" to="$2" kind="${3:-blocks}"
    local from_id="${ID[$from]:-}" to_id="${ID[$to]:-}"
    if [[ -z "$from_id" || -z "$to_id" ]]; then
        echo "skip dep ${from} -> ${to} (missing one side)" >&2
        return 0
    fi
    br dep add "$from_id" "$to_id" --type="$kind" 2>/dev/null \
        || echo "warn: dep ${from_id} -> ${to_id} (${kind}) failed (may already exist)" >&2
}

# ---------- Epic 0: Restore the beads database ----------
new 0.1 task 0 \
    "Snapshot the corrupted .beads/ tree for #70 forensics" \
    "The current beads.db has the exact failure mode described in #70 (page double-references that survived checkpoints, with 28 prior failed rebuilds preserved). Before any repair, copy .beads/.br_recovery/ and the current beads.db into ci-artifacts/beads-corruption-2026-05-19/ for forensic analysis. Specifically: \`sqlite3 .beads/beads.db \".dbinfo\"\` output, \`PRAGMA integrity_check;\` full output, page-3552 + page-123 hex dumps (the first observed double-referenced pages), and the youngest .rebuild-failed WAL. Acceptance: artifact tarball >= 10MB lands at ci-artifacts/, plus a forensic-summary.md describing what was captured and why."

new 0.2 bug 1 \
    "br: 50KB content limit blocks JSONL rebuild on legacy oversized records" \
    "br doctor --repair --allow-repeated-repair currently fails with \`Validation failed: content: exceeds 50KB. The JSONL file may be corrupt\`. Affected records (size in bytes): bd-1hi=154704, bd-3t3=258369, bd-wwqen.2=107632, bd-wwqen.3=554061, bd-wwqen.4=164059, bd-3go=93683, bd-2y306.6=80512, bd-197d=71326, bd-202x=67546. These are real beads with long spec content, not corruption. Two valid resolutions: (a) move spec body to a per-id sidecar file (.beads/long_descriptions/<id>.md) referenced by a short pointer in the bead description, or (b) raise the limit (likely in beads_rust). Acceptance: br doctor --repair on this workspace succeeds without --skip flags. Do not delete or truncate the spec content."

new 0.3 task 0 \
    "Rebuild .beads/beads.db from issues.jsonl into a verified canonical DB" \
    "After 0.2 lands, run br sync --import-only --rebuild --db .beads/beads.db.rebuild_<timestamp> --no-auto-import --no-auto-flush, verify with \`br show bd-zppf --db <temp>\` + \`br sync --status --db <temp>\` + \`br doctor --db <temp>\`, then promote: mv .beads/beads.db .beads/beads.db.bad_<timestamp> && mv <temp> .beads/beads.db. Leave the bad_<ts> file in place. Acceptance: PRAGMA integrity_check on the new DB returns \"ok\", \`br list --status=open --json | jq length\` matches the rebuild source count."

new 0.4 task 2 \
    "Add systemd user timer for br doctor --quick on this workspace" \
    "Recurring page-corruption with 28 prior failed rebuilds means we are not catching the corruption early. Add a ~/.config/systemd/user/beads-doctor-fsqlite.timer that runs \`br doctor --quick --json\` every 30 minutes against /dp/frankensqlite, writes the JSON to a rolling log, and ntfy.sh-notifies on \`error\` severity findings. Acceptance: timer is enabled, journalctl shows at least 3 successful runs, manual corruption injection (touch a single byte in beads.db) is detected within 30min and an ntfy notification fires."

dep 0.3 0.1
dep 0.3 0.2

# ---------- Epic 1: Multi-process concurrent durability (#70) ----------
new 1.0 epic 0 \
    "EPIC: Multi-process concurrent durability (issue #70)" \
    "Roll-up bead for the #70 success criterion. Subgoals: (a) swarm reproducer harness, (b) classify each observed corruption mode, (c) one targeted fix per mode, (d) regression test landing per fix, (e) overnight 8-32-process stress that passes stock SQLite integrity_check."

new 1.1 task 0 \
    "Swarm-write reproducer: NxINSERT/UPDATE/SELECT/DDL against shared file" \
    "Add tests/swarm_write_reproducer.rs (and a thin tests/scripts/swarm.sh wrapper) that spawns N caller processes via std::process::Command, each executing a configurable mix of INSERT/UPDATE/SELECT WHERE id=?/schema-stable reads against the SAME .fsqlite file. Knobs: --processes N (default 8), --duration SECS (default 60), --ops-per-txn K, --workload-mix {balanced,write-heavy,read-heavy,ddl-occasional}. Post-run: open with stock C SQLite via rusqlite, run integrity_check + quick_check + WAL inspection. Emit a structured JSON report. Acceptance: harness produces deterministic seeded runs, run on todays main reproduces at least one failure class from #70 within 5 minutes on a quiet machine."

new 1.2 task 0 \
    "Inventory corruption modes from #70 + this repo's beads.db forensics" \
    "Iterate over the harness output from 1.1 plus the bd-NEW-0.1 forensic snapshot. For each distinct PRAGMA integrity_check failure pattern, file a sub-bead under this epic: page-double-reference, WAL frame-order anomaly, short header read, freelist trunk/leaf duplication, missing checkpoint, lost commit, stale plan, etc. Each sub-bead must include: minimal repro from the harness, exact integrity_check fingerprint, suspected code path (file:line), and a hypothesis about why current code admits the bug. Acceptance: one sub-bead per distinct fingerprint, no duplicates, all linked to bd-NEW-1.0 with parent-child."

new 1.3 task 1 \
    "SharedPageLockTable: prove crash-cleanup runs and releases dead-process locks" \
    "Issue #70 success criterion #4 (no stale-plan reads) requires that when a caller dies mid-write, its locks are released by SOMETHING. Section 5.6.3 (per bd-zppf comments) specifies a shared-memory scan release_page_locks_for(txn_id) for the slow-path crash cleanup. Verify: kill -9 a caller mid-transaction, confirm SharedPageLockTable.fsqlite-shm shows the dead pid + birth-time + lease, confirm the next live caller correctly identifies it as dead via process_alive(pid, birth) and CAS-clears the entry, confirm a fresh writer can then acquire the lock. Add a test under tests/crash_recovery/shared_lock_release.rs. Acceptance: test passes 100 iterations under loom (or std-only if loom is too heavy for shm), and the harness from 1.1 cannot keep a lock alive past 1x lease duration after kill."

new 1.4 task 0 \
    "Two-phase commit: WAL fsync MUST happen before CommitIndex publish" \
    "A common failure mode for multi-process MVCC is: writer A appends WAL frames, flips CommitIndex[pgno]=N to publish, then crashes before fsync. Subsequent reader B sees CommitIndex=N, looks up the frame, finds it half-written or absent -> torn read. The fix: enforce a strict two-phase ordering -- (1) write WAL frames + fsync(wal_fd), (2) memory-barrier (Release), (3) update CommitIndex with Release ordering. Audit every CommitIndex publish site to confirm it is preceded by an explicit fdatasync (or fdatasync-equivalent on platforms without it) and a Release barrier. Add a debug_assertions-only invariant check that records the last fsync sequence per page and panics if Publish happens before Fsync. Acceptance: cargo asan + cargo loom both green on tests/wal_commit_ordering.rs."

new 1.5 task 1 \
    "Crash injection: SIGKILL each phase boundary, verify post-restart integrity" \
    "Add tests/crash_injection/ with a child-process harness that opens a connection, executes a write, and stops at each of the following phase boundaries (configurable via env var): before-wal-append, after-wal-append-before-fsync, after-fsync-before-publish, after-publish-before-checkpoint, after-checkpoint. At each boundary, SIGKILL is delivered. Parent then opens stock C SQLite (rusqlite), runs integrity_check, plus opens with fsqlite again and verifies the committed-or-not state is consistent across both engines. Acceptance: 100 iterations at each boundary pass, no integrity failures, no half-committed rows visible."

new 1.6 task 1 \
    "Stale-plan detection: bind queries to schema epoch and invalidate on DDL commit" \
    "Success criterion #4: schema-stable readers must not see stale plans after a DDL commit in another process. Implement: every prepared statement records the schema_epoch it was planned under. Before each step(), check schema_epoch_atomic.load(Acquire) >= plan.epoch; if not, return SQLITE_SCHEMA (the same error C SQLite emits) and let the caller re-prepare. The harness from 1.1 must include a DDL-occasional mix that proves the invariant. Acceptance: test_stale_plan_after_cross_process_ddl in tests/concurrent/schema_epoch.rs passes 1000 iterations."

new 1.7 task 1 \
    "WAL frame index integrity: cross-check against stock SQLite after each test run" \
    "Many of the corruption fingerprints from #70 are WAL anomalies (frame-order, short header read, WAL page-index integrity failure). After every test in tests/concurrent/ and tests/swarm/, programmatically open the resulting .fsqlite file with stock C SQLite via rusqlite, run PRAGMA wal_checkpoint(TRUNCATE), then PRAGMA integrity_check. If either fails, dump the WAL header + first 32 frames + checksum chain into the test failure artifact. Acceptance: a single test helper verify_with_c_sqlite(path) exists, every concurrency test calls it, CI uploads the dump on failure."

new 1.8 task 2 \
    "Assert no raw XOR merge on SQLite structured pages (PageType != Overflow)" \
    "Per README and bd-zppf: raw byte-range XOR merges are forbidden for SQLite structured pages. Add a runtime assertion in the merge path that the page being XOR-merged has PageType::Overflow (the only page family where byte-XOR is safe). Anything else routes through the intent-replay or structured-patch ladder. Acceptance: cargo test --features paranoia covers all page types and panics on any non-Overflow XOR attempt; release builds elide the assertion."

new 1.9 task 1 \
    "Reaper sweep: detect + clear stale serialized_writer_token from dead processes" \
    "A specific stuck state from the harness: process A acquires the serialized writer indicator, sets pid+birth+lease, then is killed before clearing. Process B reads the indicator, sees the lease has not expired, refuses to acquire -- but A is gone. The check_serialized_writer_exclusion CAS-clear loop (bd-zppf) handles this lazily on contention, but a passive reader sees no contention and stays stuck. Add a periodic sweeper (every lease/4) that probes the indicator, CAS-clears it if process_alive(pid, birth) is false, regardless of contention. Acceptance: harness scenario where A is SIGKILL-ed mid-serialized-write resolves within lease/2."

new 1.10 task 1 \
    "48-hour continuous swarm-write soak with periodic stock-SQLite integrity_check" \
    "Final gate for #70. Run the harness from 1.1 with 16 concurrent processes, ops-per-second tuned to saturate one disk, for 48 hours wall clock. Every 10 minutes, cross-check with stock C SQLite (read-only open + integrity_check). Acceptance: zero integrity_check failures over the full run, zero silent lost writes (each caller asserts its own commits readable post-commit), zero stale plans after DDL, peak RSS bounded."

dep 1.1 1.0 parent-child
dep 1.2 1.1
dep 1.3 1.1
dep 1.4 1.2
dep 1.5 1.4
dep 1.6 1.1
dep 1.7 1.1
dep 1.9 1.3
dep 1.10 1.4
dep 1.10 1.6
dep 1.10 1.7
dep 1.10 1.9

# ---------- Epic 2: File-backed time travel (#82) ----------
new 2.0 question 1 \
    "Design decision: minimal durable history for #82 vs full #30 substrate" \
    "The full #30 design covered: durable commit metadata, ref protocol, history walk. #82 only strictly needs: (a) a durable on-disk log of (commit_seq, root_page) tuples, and (b) the ability to open the DB at a historical root_page for read-only queries. Decide whether to: option A -- revive #30 in full (months of work, broader payoff), option B -- land a minimal commit-snapshot log (weeks, scoped to #82). Output: a one-page design note at docs/design/time-travel-file-backed.md justifying the choice. Acceptance: decision committed, this bead closed with the chosen option recorded as close_reason, follow-up beads filed for the chosen path."

new 2.1 task 1 \
    "On-disk commit-snapshot log: append-only (commit_seq, root_page, ts) records" \
    "Define a stable on-disk format for a per-database commit-snapshot log: file alongside .fsqlite at .fsqlite-history, append-only, fixed-size records (commit_seq u64, root_page u32, wall_ts u64, prev_record_crc u32, this_record_crc u32). Writer appends after every successful commit, fsync. Reader uses bisect search to locate a target commit_seq. Acceptance: format documented in docs/design/, fuzz tested with proptest for crash-mid-append safety, byte-for-byte stable across endianness."

new 2.2 task 1 \
    "Open .fsqlite read-only at a historical root_page recovered from .fsqlite-history" \
    "Once 2.1 lands, plumb FOR SYSTEM_TIME AS OF COMMITSEQ N: (1) locate the .fsqlite-history record for N, (2) extract root_page, (3) open a read-only connection that swaps in that root_page in place of the live root. Reuse the existing in-memory snapshot ring machinery (#23) -- only the root_page binding differs. Acceptance: the in-issue repro (insert+update+commit+commit+TT-by-COMMITSEQ-1-and-2-and-3, then close, reopen, retry) returns the historical values; rusqlite oracle agrees on the underlying byte content."

new 2.3 task 2 \
    "FOR SYSTEM_TIME AS OF TIMESTAMP '...' (in addition to COMMITSEQ)" \
    "The spec lets users say AS OF a wall-clock timestamp. With 2.1 capturing wall_ts in each history record, bisect the timestamp dimension and resolve to a commit_seq, then route through 2.2. Acceptance: AS OF TIMESTAMP rounds-down to the most recent commit <= the requested timestamp; conformance tests cover boundary cases (exact match, between commits, before first commit, after last commit)."

new 2.4 task 2 \
    ".fsqlite-history retention: configurable per-database TTL or commit-count cap" \
    "Without a retention policy, .fsqlite-history grows unbounded. Add a PRAGMA history_retention = { ttl_seconds: N | max_commits: M | unlimited } and a background reaper that truncates the oldest records. Reaper must coordinate with active reader snapshots (no in-flight historical read can have its anchor reaped). Acceptance: pragma changes survive close/reopen, reaper passes a property test that no live reader ever observes a missing record."

new 2.5 task 1 \
    "Run #23's time-travel conformance suite against file-backed DBs" \
    "The existing conformance tests for in-memory time-travel were the original closing gate for #23. Parameterize them over backing-store type (memory vs file) and require both pass post-2.2. Acceptance: every test in tests/conformance/time_travel_*.rs runs in both modes and passes, golden output is identical."

dep 2.1 2.0
dep 2.2 2.1
dep 2.3 2.2
dep 2.4 2.2
dep 2.5 2.2

# ---------- Epic 3: RefCell panic (mam_rust #118) ----------
new 3.1 bug 1 \
    "Repro: RefCell already borrowed at fsqlite-core/connection.rs:47590 on send_message path" \
    "The mam_rust reproducer: am doctor archive-normalize --yes --apply-mode quarantine && am doctor reconstruct --yes && am doctor repair --yes; then call send_message via MCP. Strip that down to a frankensqlite-only repro: same prepared-statement shape, same nested-borrow path, same outcome. Acceptance: tests/reentry/refcell_borrow_at_47590.rs panics deterministically on main, and the panic backtrace matches the mam_rust report."

new 3.2 task 1 \
    "Replace RefCell at connection.rs:47590 with re-entrant primitive (or break the nested-borrow chain)" \
    "Two valid resolutions: (a) replace the specific RefCell with a Cell-based single-mutable-state abstraction that supports nested borrows where the inner borrow does not need exclusivity, or (b) refactor the caller so the second borrow happens after the first scope ends. Pick whichever matches the existing connection.rs style. Acceptance: the 3.1 reproducer no longer panics, all existing tests pass, no new unsafe, no new RefCell."

new 3.3 task 2 \
    "Audit every RefCell in fsqlite-core for the same nested-borrow risk" \
    "After 3.2, grep for RefCell across fsqlite-core and assess each site: under what call chain could a second borrow happen? File a sub-bead for any site that has a non-trivial answer. Acceptance: a docs/internal/refcell-audit-2026-Q2.md captures every RefCell site, its borrow lifetime, the highest-level caller that could re-enter, and a verdict (safe / refactor / TODO)."

dep 3.2 3.1
dep 3.3 3.2

# ---------- Cross-cutting test infrastructure ----------
new X.1 task 1 \
    "Test helper: verify a .fsqlite file with stock C SQLite via rusqlite" \
    "Single function verify_with_c_sqlite(path: &Path) -> Result<VerifyReport> that opens read-only via rusqlite, runs PRAGMA quick_check + integrity_check + wal_checkpoint(TRUNCATE), returns structured report. Used by 1.5, 1.7, 1.10, and 2.x."

new X.2 task 2 \
    "Loom model: SharedPageLockTable acquire/release x CommitIndex publish ordering" \
    "Encode the page-lock + CommitIndex publish in a loom test (or shuttle if loom is the wrong granularity). Verify no schedule reaches a state where a reader sees CommitIndex pointing at a WAL frame not yet fsynced. Acceptance: cargo test --features loom passes deterministically, the model rejects an intentionally weakened ordering."

new X.3 task 2 \
    "CI: run verify_with_c_sqlite over every artifact produced by tests/concurrent/" \
    "In .github/workflows/ci.yml, add a step after the concurrent-tests job that walks every .fsqlite file in target/test-artifacts/ and runs verify_with_c_sqlite. CI fails red if any artifact fails. Acceptance: CI shows a new \"Cross-engine integrity check\" step, takes <30s on a passing run, fails clearly when bd-NEW-1.x has a regression."

dep 1.7 X.1
dep 1.10 X.1
dep 1.10 X.3
dep 2.5 X.1

echo
echo "=========================================="
echo "Imported beads:"
for k in "${!ID[@]}"; do
    printf "  %-6s -> %s\n" "$k" "${ID[$k]}"
done | sort
echo "=========================================="
echo
echo "Next steps:"
echo "  br sync --flush-only      # export to JSONL"
echo "  git add .beads/           # stage beads JSONL changes"
echo "  bv --robot-triage         # see what's ready to start"
