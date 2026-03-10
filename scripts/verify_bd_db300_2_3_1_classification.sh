#!/usr/bin/env bash
# Verification artifact for bd-db300.2.3.1:
# classify shared metadata and counter paths by contention pattern.
#
# Deterministic replay:
#   bash scripts/verify_bd_db300_2_3_1_classification.sh

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAD_ID="bd-db300.2.3.1"
SCENARIO_ID="SHARED-METADATA-CLASSIFICATION-B3.1"
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-$$"
TRACE_ID="trace-${RUN_ID}"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
REPLAY_COMMAND="bash scripts/verify_bd_db300_2_3_1_classification.sh"
ARTIFACT_DIR="${OUTPUT_DIR:-${WORKSPACE_ROOT}/artifacts/perf/${BEAD_ID}/${RUN_ID}}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
CLASSIFICATION_JSON="${ARTIFACT_DIR}/classification.json"
MANIFEST_JSON="${ARTIFACT_DIR}/manifest.json"
SUMMARY_MD="${ARTIFACT_DIR}/summary.md"

mkdir -p "${ARTIFACT_DIR}"
: > "${EVENTS_JSONL}"

emit_event() {
    local phase="$1"
    local event_type="$2"
    local outcome="$3"
    local message="$4"
    jq -cn \
        --arg trace_id "${TRACE_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg bead_id "${BEAD_ID}" \
        --arg scenario_id "${SCENARIO_ID}" \
        --arg phase "${phase}" \
        --arg event_type "${event_type}" \
        --arg outcome "${outcome}" \
        --arg message "${message}" \
        --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        '{
            trace_id: $trace_id,
            run_id: $run_id,
            bead_id: $bead_id,
            scenario_id: $scenario_id,
            phase: $phase,
            event_type: $event_type,
            outcome: $outcome,
            timestamp: $timestamp,
            message: $message
        }' >> "${EVENTS_JSONL}"
}

fail() {
    local phase="$1"
    local message="$2"
    emit_event "${phase}" "fail" "fail" "${message}"
    echo "ERROR: ${message}" >&2
    exit 1
}

require_tool() {
    local tool_name="$1"
    command -v "${tool_name}" >/dev/null 2>&1 || fail "bootstrap" "missing required tool: ${tool_name}"
}

require_file() {
    local path="$1"
    [[ -f "${path}" ]] || fail "inputs" "missing required file: ${path}"
}

require_match() {
    local file="$1"
    local pattern="$2"
    local description="$3"
    if rg -n --fixed-strings -- "${pattern}" "${file}" >/dev/null; then
        emit_event "inputs" "symbol_present" "pass" "${description}"
        return
    fi
    fail "inputs" "missing pattern in ${file}: ${description} :: ${pattern}"
}

emit_event "bootstrap" "start" "running" "classification verification started"

require_tool jq
require_tool rg

require_file "${WORKSPACE_ROOT}/.beads/issues.jsonl"
require_file "${WORKSPACE_ROOT}/crates/fsqlite-core/src/connection.rs"
require_file "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/begin_concurrent.rs"
require_file "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/core_types.rs"
require_file "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/shared_lock_table.rs"
require_file "${WORKSPACE_ROOT}/crates/fsqlite-pager/src/pager.rs"
require_file "${WORKSPACE_ROOT}/crates/fsqlite-vdbe/src/engine.rs"

require_match "${WORKSPACE_ROOT}/.beads/issues.jsonl" '"id":"bd-db300.2.1"' "B1 inventory bead is present"
require_match "${WORKSPACE_ROOT}/.beads/issues.jsonl" '"id":"bd-db300.2.3.1"' "B3.1 classification bead is present"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-core/src/connection.rs" 'registry: Arc<Mutex<ConcurrentRegistry>>' "shared MVCC state still uses a global ConcurrentRegistry mutex"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-core/src/connection.rs" 'next_commit_seq: Arc<AtomicU64>' "shared MVCC state still exposes a global commit sequencer"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-core/src/connection.rs" 'commit_write_mutex: Arc<Mutex<()>>' "shared MVCC state still exposes a global commit publication mutex"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-core/src/connection.rs" 'self.next_commit_seq.fetch_add(1, AtomicOrdering::AcqRel)' "commit sequence assignment still happens through a single atomic fetch_add"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/begin_concurrent.rs" 'pub struct ConcurrentRegistry {' "ConcurrentRegistry definition still exists"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/begin_concurrent.rs" 'committed_readers: Vec<CommittedReaderInfo>' "ConcurrentRegistry still stores committed reader history"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/begin_concurrent.rs" 'committed_writers: Vec<CommittedWriterInfo>' "ConcurrentRegistry still stores committed writer history"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-vdbe/src/engine.rs" '.registry' "VDBE page I/O still touches the shared registry on the hot path"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/core_types.rs" 'type CommitShard = CacheAligned<RwLock<HashMap<PageNumber, CommitSeq, PageNumberBuildHasher>>>;' "CommitIndex remains a sharded RwLock-backed map"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/core_types.rs" 'pub fn release_all(&self, txn: TxnId)' "InProcessPageLockTable still has release_all full-shard cleanup"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/core_types.rs" 'pub fn release_set(&self, pages: impl IntoIterator<Item = PageNumber>, txn: TxnId)' "InProcessPageLockTable exposes release_set for held-lock release"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/shared_lock_table.rs" 'struct StripedOccupancyCounter {' "shared lock table still uses a striped occupancy counter"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-pager/src/pager.rs" 'struct StripedCounter64 {' "pager still carries the cache-padded striped counter primitive"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-pager/src/pager.rs" 'struct PublishedPagerState {' "pager still carries the seqlock-style publication primitive"

emit_event "classification" "start" "running" "writing classification report"

jq -n \
    --arg schema_version "frankensqlite.db300.shared_metadata_classification.v1" \
    --arg bead_id "${BEAD_ID}" \
    --arg run_id "${RUN_ID}" \
    --arg trace_id "${TRACE_ID}" \
    --arg generated_at "${GENERATED_AT}" \
    --arg scenario_id "${SCENARIO_ID}" \
    --arg replay_command "${REPLAY_COMMAND}" \
    '
    {
        schema_version: $schema_version,
        bead_id: $bead_id,
        run_id: $run_id,
        trace_id: $trace_id,
        generated_at: $generated_at,
        scenario_id: $scenario_id,
        replay_command: $replay_command,
        source_beads: ["bd-db300.2.1", "bd-db300.2.3.1"],
        classification_axes: {
            write_frequency: [
                "per_page_hot_path",
                "per_commit",
                "per_new_slot_insert",
                "amortized_background_or_finalize"
            ],
            reader_retryability: [
                "high",
                "mixed",
                "low",
                "none"
            ],
            topology_sensitivity: [
                "high",
                "medium",
                "low"
            ]
        },
        open_hot_paths: [
            {
                path_id: "concurrent_registry_global_mutex",
                class: "mixed_read_mostly_metadata",
                component: "MVCC registry",
                current_primitive: "Arc<Mutex<ConcurrentRegistry>>",
                evidence_files: [
                    "crates/fsqlite-core/src/connection.rs",
                    "crates/fsqlite-mvcc/src/begin_concurrent.rs",
                    "crates/fsqlite-vdbe/src/engine.rs"
                ],
                hot_phases: [
                    "BEGIN CONCURRENT registration",
                    "read witness recording",
                    "dirty/write witness tracking",
                    "SSI prepare/finalize scans"
                ],
                write_frequency: "per_page_hot_path",
                reader_retryability: "mixed",
                topology_sensitivity: "high",
                contention_pattern: "One global mutex serializes both retryable metadata reads and per-session witness mutations, so ownership bounces across workers on ordinary page access instead of only on commit publication.",
                primitive_selection_readout: "Split per-session mutable handle state away from globally published active/history views first; after that split, the published side is a seqlock/RCU-style candidate.",
                not_a_good_fit_for: [
                    "blind counter striping",
                    "single giant global mutex"
                ],
                best_follow_on_beads: [
                    "bd-db300.2.3.3",
                    "bd-db300.5.1"
                ]
            },
            {
                path_id: "commit_index_sharded_rwlock",
                class: "read_mostly_page_metadata",
                component: "CommitIndex",
                current_primitive: "64 cache-aligned RwLock<HashMap<PageNumber, CommitSeq>> shards",
                evidence_files: [
                    "crates/fsqlite-mvcc/src/core_types.rs",
                    "crates/fsqlite-mvcc/src/begin_concurrent.rs"
                ],
                hot_phases: [
                    "FCW validation reads",
                    "commit finalize publish"
                ],
                write_frequency: "per_commit",
                reader_retryability: "high",
                topology_sensitivity: "high",
                contention_pattern: "Readers query latest(page) on conflict detection while finalize writes one shard per committed page; lock traffic is already sharded, but every read still pays a lock and cross-core ownership transfer on hot pages.",
                primitive_selection_readout: "This is the cleanest B3 metadata prototype target: the read side can retry cheaply, the write side is bounded to finalize, and the key/value payload is compact enough for sequence-published or copy-on-publish shards.",
                not_a_good_fit_for: [
                    "global mutex publication",
                    "cross-shard full-map rebuild on every commit"
                ],
                best_follow_on_beads: [
                    "bd-db300.2.3.3",
                    "bd-db300.3.2"
                ]
            },
            {
                path_id: "inprocess_page_lock_table",
                class: "write_hot_ownership_map",
                component: "InProcessPageLockTable",
                current_primitive: "64 cache-aligned Mutex<HashMap<PageNumber, TxnId>> shards plus release_all/release_set",
                evidence_files: [
                    "crates/fsqlite-mvcc/src/core_types.rs",
                    "crates/fsqlite-mvcc/src/begin_concurrent.rs"
                ],
                hot_phases: [
                    "page lock acquire",
                    "commit abort/finalize release",
                    "draining-table maintenance"
                ],
                write_frequency: "per_page_hot_path",
                reader_retryability: "low",
                topology_sensitivity: "medium",
                contention_pattern: "The table is already sharded and cache-aligned, but ownership mutation is write-hot and finalize still has an O(all shards) release_all path that bounces every shard even when a txn only touched a small page set.",
                primitive_selection_readout: "Treat this as an ownership-map problem, not a seqlock problem. The immediate leverage is to route finalize through held-lock release_set or an explicit held-lock list, not to make readers spin on a global sequence.",
                not_a_good_fit_for: [
                    "RCU without ownership handoff",
                    "blind reader-retry schemes"
                ],
                best_follow_on_beads: [
                    "bd-db300.3.3",
                    "bd-db300.2.4"
                ]
            },
            {
                path_id: "next_commit_seq_global_sequencer",
                class: "global_ordering_counter",
                component: "next_commit_seq",
                current_primitive: "Arc<AtomicU64>",
                evidence_files: [
                    "crates/fsqlite-core/src/connection.rs"
                ],
                hot_phases: [
                    "snapshot high-water reads",
                    "commit sequence assignment"
                ],
                write_frequency: "per_commit",
                reader_retryability: "high",
                topology_sensitivity: "medium",
                contention_pattern: "The read side is cheap, but the write side carries a total-order guarantee. If Track C shrinks the commit mutex, this single atomic becomes a cache-line ownership baton between committers.",
                primitive_selection_readout: "Do not treat this like a relaxed striped counter. Preserve exact ordering; if it becomes hot after publication shrink, use cache-line isolation or block allocation rather than approximate striping.",
                not_a_good_fit_for: [
                    "lossy striped counters",
                    "eventual-consistency aggregation"
                ],
                best_follow_on_beads: [
                    "bd-db300.3.2",
                    "bd-db300.5.3"
                ]
            },
            {
                path_id: "commit_write_mutex_publication_gate",
                class: "serialized_publication_gate",
                component: "commit_write_mutex",
                current_primitive: "Arc<Mutex<()>>",
                evidence_files: [
                    "crates/fsqlite-core/src/connection.rs"
                ],
                hot_phases: [
                    "concurrent commit planning/finalize critical section"
                ],
                write_frequency: "per_commit",
                reader_retryability: "none",
                topology_sensitivity: "high",
                contention_pattern: "This is a single-file publication gate for successful committers. It is a structural choke point, not a read-mostly metadata path.",
                primitive_selection_readout: "Classify as out-of-family for B3 lock-free metadata work. The right move is to shrink the guarded region to true durable ordering and publish already-prepared state outside the mutex.",
                not_a_good_fit_for: [
                    "reader-optimized seqlock",
                    "counter striping"
                ],
                best_follow_on_beads: [
                    "bd-db300.3.1",
                    "bd-db300.3.2"
                ]
            }
        ],
        reusable_in_tree_primitives: [
            {
                primitive_id: "pager_published_state_seqlock",
                location: "crates/fsqlite-pager/src/pager.rs",
                shape: "sequence-published snapshot with retryable readers",
                why_it_matters: "Demonstrates an existing in-tree template for retryable publication once mutable writer-owned state is separated from published metadata."
            },
            {
                primitive_id: "pager_striped_counter64",
                location: "crates/fsqlite-pager/src/pager.rs",
                shape: "cache-padded striped relaxed counter",
                why_it_matters: "Shows the right primitive for false-sharing-sensitive relaxed counters, which is useful for B3.2 but not for exact commit ordering."
            },
            {
                primitive_id: "shared_lock_table_striped_occupancy_counter",
                location: "crates/fsqlite-mvcc/src/shared_lock_table.rs",
                shape: "hash-key-striped occupancy counter",
                why_it_matters: "Provides an in-tree example of spreading insert-only counter traffic across cache lines without losing exact steady-state totals."
            }
        ],
        follow_on_ranking: [
            {
                rank: 1,
                target: "commit_index_sharded_rwlock",
                next_bead: "bd-db300.2.3.3",
                reason: "Best combination of retryable reads, bounded writes, and compact publication payload."
            },
            {
                rank: 2,
                target: "concurrent_registry_global_mutex",
                next_bead: "bd-db300.2.3.3",
                reason: "Largest apparent mutex hotspot, but only after separating published metadata from per-session witness mutation."
            },
            {
                rank: 3,
                target: "inprocess_page_lock_table",
                next_bead: "bd-db300.3.3",
                reason: "High leverage, but the fix is ownership-aware release topology rather than generic lock-free publication."
            }
        ],
        exclusions_for_b3_counter_work: [
            {
                path_id: "next_commit_seq_global_sequencer",
                exclusion_reason: "Exact total-order sequencer; not a candidate for relaxed striped counter replacement."
            },
            {
                path_id: "commit_write_mutex_publication_gate",
                exclusion_reason: "Serialized gate belongs to publication-window shrink work, not metadata publication primitive selection."
            }
        ]
    }' > "${CLASSIFICATION_JSON}"

jq -n \
    --arg schema_version "frankensqlite.db300.shared_metadata_classification_manifest.v1" \
    --arg bead_id "${BEAD_ID}" \
    --arg run_id "${RUN_ID}" \
    --arg trace_id "${TRACE_ID}" \
    --arg generated_at "${GENERATED_AT}" \
    --arg scenario_id "${SCENARIO_ID}" \
    --arg replay_command "${REPLAY_COMMAND}" \
    --arg artifact_dir "${ARTIFACT_DIR}" \
    --arg events_jsonl "${EVENTS_JSONL}" \
    --arg classification_json "${CLASSIFICATION_JSON}" \
    --arg summary_md "${SUMMARY_MD}" \
    --slurpfile report "${CLASSIFICATION_JSON}" \
    '{
        schema_version: $schema_version,
        bead_id: $bead_id,
        run_id: $run_id,
        trace_id: $trace_id,
        generated_at: $generated_at,
        scenario_id: $scenario_id,
        replay_command: $replay_command,
        artifact_dir: $artifact_dir,
        artifacts: {
            events_jsonl: $events_jsonl,
            classification_json: $classification_json,
            summary_md: $summary_md
        },
        summary: {
            open_hot_paths: ($report[0].open_hot_paths | length),
            reusable_primitives: ($report[0].reusable_in_tree_primitives | length),
            top_metadata_candidate: $report[0].follow_on_ranking[0].target,
            top_metadata_bead: $report[0].follow_on_ranking[0].next_bead,
            top_exclusion: $report[0].exclusions_for_b3_counter_work[0].path_id
        }
    }' > "${MANIFEST_JSON}"

cat > "${SUMMARY_MD}" <<EOF
# ${BEAD_ID} Verification Summary

- run_id: \`${RUN_ID}\`
- trace_id: \`${TRACE_ID}\`
- scenario_id: \`${SCENARIO_ID}\`
- replay_command: \`${REPLAY_COMMAND}\`
- classification_json: \`${CLASSIFICATION_JSON}\`
- manifest_json: \`${MANIFEST_JSON}\`

## Classification Outcomes

- \`commit_index_sharded_rwlock\` is the cleanest next B3 metadata prototype target because the read side is retryable and the write side is naturally bounded to finalize.
- \`concurrent_registry_global_mutex\` is likely the largest mutex hotspot, but it first needs a structural split between published metadata and per-session mutable witness state.
- \`inprocess_page_lock_table\` is already sharded; the next gain is ownership-aware release topology (\`release_set\` or held-lock lists), not a generic seqlock conversion.
- \`next_commit_seq_global_sequencer\` is not a striped-counter candidate because it carries exact total-order semantics.
- \`commit_write_mutex_publication_gate\` is a Track C shrink-the-window problem, not a B3 lock-free publication problem.

## Reusable In-Tree Primitives

- \`PublishedPagerState\` in \`crates/fsqlite-pager/src/pager.rs\` is the in-tree seqlock-style publication template.
- \`StripedCounter64\` in \`crates/fsqlite-pager/src/pager.rs\` is the in-tree cache-padded counter-striping template.
- \`StripedOccupancyCounter\` in \`crates/fsqlite-mvcc/src/shared_lock_table.rs\` is the in-tree exact striped occupancy template.
EOF

emit_event "classification" "report_written" "pass" "classification report emitted"
emit_event "complete" "finish" "pass" "classification verification completed"

echo "classification report: ${CLASSIFICATION_JSON}"
echo "manifest: ${MANIFEST_JSON}"
echo "summary: ${SUMMARY_MD}"
