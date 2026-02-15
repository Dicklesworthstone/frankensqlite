# E2E Scenario Catalog

This document defines the canonical catalog of end-to-end test scenarios for FrankenSQLite. Each scenario has a stable ID, pass/fail criteria, and links to relevant invariants.

## Scenario ID Format

`E2E-{CATEGORY}-{NUMBER}`

Categories:
- **DDL** - Data Definition Language (CREATE, ALTER, DROP)
- **DML** - Data Manipulation Language (INSERT, UPDATE, DELETE, SELECT)
- **TXN** - Transaction lifecycle
- **CNC** - Concurrency and MVCC
- **REC** - Recovery and durability
- **COR** - Corruption detection and repair
- **CMP** - Compatibility mode
- **EXT** - Extensions

---

## DDL Scenarios

### E2E-DDL-001: CREATE TABLE with all column types
- **Description**: Create table with INTEGER, REAL, TEXT, BLOB, NULL-capable columns
- **Pass criteria**: Table created, schema persisted, columns queryable
- **Invariants**: INV-S1 (Schema Epoch)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::create_table_and_insert_select`

### E2E-DDL-002: CREATE INDEX single column
- **Description**: Create index on single column, verify query uses it
- **Pass criteria**: Index created, EXPLAIN shows index usage
- **Invariants**: INV-S1
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::index_insert_single_column`

### E2E-DDL-003: CREATE INDEX multi-column
- **Description**: Create composite index, verify prefix usage
- **Pass criteria**: Index works for leading column queries
- **Invariants**: INV-S1
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::index_insert_multi_column`

### E2E-DDL-004: DROP TABLE with active cursors
- **Description**: Drop table while cursor is open
- **Pass criteria**: Error or deferred drop, no crash
- **Invariants**: INV-S1
- **Evidence**: TBD - gap identified

### E2E-DDL-005: ALTER TABLE ADD COLUMN
- **Description**: Add column to existing table with data
- **Pass criteria**: New column added, existing data preserved
- **Invariants**: INV-S1
- **Evidence**: TBD - gap identified

---

## DML Scenarios

### E2E-DML-001: INSERT single row
- **Description**: Insert single row with explicit values
- **Pass criteria**: Row inserted, rowid returned
- **Invariants**: INV-3 (Version Chain)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::create_table_and_insert_select`

### E2E-DML-002: INSERT multi-row VALUES
- **Description**: Insert multiple rows in single statement
- **Pass criteria**: All rows inserted atomically
- **Invariants**: INV-6 (Commit Atomicity)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::insert_multi_row_values`

### E2E-DML-003: INSERT ... SELECT
- **Description**: Insert rows from subquery
- **Pass criteria**: Rows copied correctly
- **Invariants**: INV-6
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::probe_insert_select`

### E2E-DML-004: UPDATE with WHERE
- **Description**: Update subset of rows matching predicate
- **Pass criteria**: Matching rows updated, others unchanged
- **Invariants**: INV-3, INV-6
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::update_modifies_rows`

### E2E-DML-005: DELETE with WHERE
- **Description**: Delete subset of rows matching predicate
- **Pass criteria**: Matching rows deleted, others preserved
- **Invariants**: INV-3, INV-6
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::delete_removes_rows`

### E2E-DML-006: SELECT with JOIN
- **Description**: Inner/outer joins across tables
- **Pass criteria**: Correct result set
- **Invariants**: None (query correctness)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::full_outer_join_null_extension`

### E2E-DML-007: SELECT with subquery
- **Description**: Scalar and table subqueries in WHERE
- **Pass criteria**: Correct filtering
- **Invariants**: None
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::scalar_subquery_expression`

---

## Transaction Scenarios

### E2E-TXN-001: BEGIN/COMMIT basic
- **Description**: Start transaction, make changes, commit
- **Pass criteria**: Changes visible after commit
- **Invariants**: INV-6 (Commit Atomicity)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::begin_commit_persists_changes`

### E2E-TXN-002: BEGIN/ROLLBACK basic
- **Description**: Start transaction, make changes, rollback
- **Pass criteria**: Changes reverted
- **Invariants**: INV-6
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::rollback_reverts_changes`

### E2E-TXN-003: SAVEPOINT create/release
- **Description**: Nested savepoints within transaction
- **Pass criteria**: Savepoints work correctly
- **Invariants**: INV-5 (Snapshot Stability)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::savepoint_and_rollback_to`

### E2E-TXN-004: SAVEPOINT rollback
- **Description**: Rollback to savepoint
- **Pass criteria**: Changes since savepoint reverted
- **Invariants**: INV-5
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::savepoint_release_commits_changes`

### E2E-TXN-005: Nested transaction error
- **Description**: BEGIN inside active transaction
- **Pass criteria**: Error returned
- **Invariants**: None (constraint)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::nested_begin_errors`

---

## Concurrency Scenarios

### E2E-CNC-001: Concurrent readers
- **Description**: Multiple readers see consistent snapshot
- **Pass criteria**: No dirty reads
- **Invariants**: INV-5 (Snapshot Stability)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::concurrent_readers_consistency`

### E2E-CNC-002: Concurrent writers different pages
- **Description**: Two writers touching different leaf pages
- **Pass criteria**: Both commit successfully
- **Invariants**: INV-2 (Page Lock Exclusivity), INV-C2 (No Deadlock)
- **Evidence**: `crates/fsqlite-harness/tests/bd_2npr_mvcc_concurrent_writer_stress.rs`

### E2E-CNC-003: Concurrent writers same page
- **Description**: Two writers touching same leaf page
- **Pass criteria**: FCW or safe merge, one commits
- **Invariants**: INV-2, INV-4 (Write Set)
- **Evidence**: `crates/fsqlite-mvcc/src/deterministic_rebase.rs` tests

### E2E-CNC-004: Writer vs reader isolation
- **Description**: Reader doesn't see uncommitted writer changes
- **Pass criteria**: Snapshot isolation holds
- **Invariants**: INV-5, INV-6
- **Evidence**: TBD - gap identified

### E2E-CNC-005: SSI write skew prevention
- **Description**: Detect and abort write skew anomaly
- **Pass criteria**: One transaction aborts with SQLITE_BUSY_SNAPSHOT
- **Invariants**: INV-6 (SSI)
- **Evidence**: `crates/fsqlite-harness/tests/bd_2d3i_1_ssi_witness_plane_deterministic_scenarios_compliance.rs`

### E2E-CNC-006: Pointer swizzle protocol pilot (tagged CAS transitions)
- **Description**: Validate deterministic swizzle/unswizzle CAS transitions and HOT/COOLING/COLD state contract for B-tree child references.
- **Pass criteria**: Swizzle prototype tests pass, structured log schema fields validate, replay command reproduces run.
- **Invariants**: INV-C2 (No Deadlock), swizzle protocol design invariants in `docs/design/pointer-swizzle-protocol.md`
- **Evidence**: `e2e/bd_2uza4_1_swizzle_protocol_pilot.sh`, `crates/fsqlite-btree/src/swizzle.rs` tests

---

## Recovery Scenarios

### E2E-REC-001: Clean shutdown and restart
- **Description**: Close connection, reopen, verify data
- **Pass criteria**: All committed data present
- **Invariants**: INV-D1 (WAL Integrity)
- **Evidence**: `crates/fsqlite-core/src/connection.rs` tests

### E2E-REC-002: Crash after commit, before checkpoint
- **Description**: Kill process after WAL commit, recover
- **Pass criteria**: Committed data recovered
- **Invariants**: INV-D2 (Crash Recovery)
- **Evidence**: `crates/fsqlite-wal/tests/wal_fec_recovery.rs::test_recovery_intact_wal`

### E2E-REC-003: Crash during write
- **Description**: Kill process mid-write, recover
- **Pass criteria**: No partial commits visible
- **Invariants**: INV-D2
- **Evidence**: TBD - gap identified

### E2E-REC-004: WAL replay after checkpoint
- **Description**: Verify checkpoint followed by WAL replay
- **Pass criteria**: Correct final state
- **Invariants**: INV-D2
- **Evidence**: `crates/fsqlite-wal/src/checkpoint_executor.rs` tests

---

## Corruption Scenarios

### E2E-COR-001: WAL checksum validation
- **Description**: Detect corrupted WAL frame
- **Pass criteria**: Corruption detected, error returned
- **Invariants**: INV-D1 (WAL Integrity)
- **Evidence**: `crates/fsqlite-wal/tests/wal_fec_recovery.rs::test_raptorq_bitflip_detected`

### E2E-COR-002: RaptorQ repair within budget
- **Description**: Corrupt symbols, recover via repair
- **Pass criteria**: Data recovered successfully
- **Invariants**: INV-D3 (Self-Healing)
- **Evidence**: `crates/fsqlite-wal/tests/wal_fec_recovery.rs::test_raptorq_bitflip_repair`

### E2E-COR-003: RaptorQ repair exceeded
- **Description**: Corruption beyond repair budget
- **Pass criteria**: Error returned, no silent corruption
- **Invariants**: INV-D3
- **Evidence**: `crates/fsqlite-wal/tests/wal_fec_recovery.rs::test_raptorq_symbol_loss_beyond_R`

### E2E-COR-004: Database file corruption detection
- **Description**: Corrupt database page, detect on read
- **Pass criteria**: Corruption detected
- **Invariants**: INV-D1
- **Evidence**: TBD - gap identified

---

## Compatibility Scenarios

### E2E-CMP-001: Open SQLite database
- **Description**: Open database created by C SQLite
- **Pass criteria**: Data readable, schema correct
- **Invariants**: None (compatibility)
- **Evidence**: `crates/fsqlite-core/src/compat_persist.rs` tests

### E2E-CMP-002: Export to SQLite format
- **Description**: Write database readable by C SQLite
- **Pass criteria**: sqlite3 can read the file
- **Invariants**: None (compatibility)
- **Evidence**: TBD - gap identified

### E2E-CMP-003: WAL mode interop
- **Description**: WAL files compatible with C SQLite
- **Pass criteria**: Recovery works in both directions
- **Invariants**: INV-D1
- **Evidence**: TBD - gap identified

---

## Extension Scenarios

### E2E-EXT-001: JSON1 functions
- **Description**: json_extract, json_set, json_array
- **Pass criteria**: Correct JSON manipulation
- **Invariants**: None
- **Evidence**: `crates/fsqlite-ext-json` tests

### E2E-EXT-002: FTS5 full-text search
- **Description**: Create FTS table, query with MATCH
- **Pass criteria**: Correct search results
- **Invariants**: None
- **Evidence**: `crates/fsqlite-ext-fts5` tests

### E2E-EXT-003: R-tree spatial index
- **Description**: Create R-tree, spatial queries
- **Pass criteria**: Correct spatial results
- **Invariants**: None
- **Evidence**: `crates/fsqlite-ext-rtree` tests

---

## Gap Summary

Scenarios marked "TBD - gap identified" require test implementation:

| ID | Scenario | Priority |
|----|----------|----------|
| E2E-DDL-004 | DROP TABLE with active cursors | P1 |
| E2E-DDL-005 | ALTER TABLE ADD COLUMN | P1 |
| E2E-CNC-004 | Writer vs reader isolation | P0 |
| E2E-REC-003 | Crash during write | P0 |
| E2E-COR-004 | Database file corruption | P1 |
| E2E-CMP-002 | Export to SQLite format | P1 |
| E2E-CMP-003 | WAL mode interop | P1 |

---

## Related Documents

- [Critical Invariants Catalog](critical-invariants.md)
- [Coverage Gap Report](coverage-gap-report.md)
- [Test Realism Inventory](test-realism/README.md)
- [Unified E2E Log Schema Contract](e2e_log_schema_contract.md)
- [Shell Script Log Profile](e2e_shell_script_log_profile.json)

---

## Operator Failure Triage Runbook (bd-mblr.5.4.2)

This runbook is the canonical first-response flow for E2E failures.

- Log schema version: `1.0.0` (see `docs/e2e_log_schema_contract.md`)
- Artifact roots:
  - `test-results/<bead>/...` for run-level JSONL reports
  - `test-results/<bead>/logs/<run_id>/...` for phase logs, SQL traces, diagnostics, mismatch extracts

### Fast Triage Workflow

1. Start from the run-level JSONL referenced by CI (`test-results/.../<run_id>.jsonl`).
2. Identify terminal error and divergence markers:
   - `event_type == "fail"` or `status == "fail"`
   - `error_code != null`
   - `first_divergence == true` or `mismatch_digest != "none"`
3. Pivot to artifacts listed in `artifact_paths` and `context.artifact_paths`.
4. Replay deterministically using the mapped command in the table below.
5. Confirm whether failure reproduces, then classify root cause by signature.

### Annotated Failure Trace A: Terminal Failure (`E_TERMINAL_FAILURE`)

Source: `test-results/bd_2ddl/bd-2ddl-20260213T092408Z-109509.jsonl`

```json
{"run_id":"bd-2ddl-20260213T092408Z-109509","phase":"report","event_type":"fail","scenario_id":"INFRA-6","outcome":"fail","error_code":"E_TERMINAL_FAILURE","context":{"case":"terminal_failure","details":"... missing_public_api_test_crates=fsqlite-types ... fsqlite-harness ..."}}
```

What this tells you:

- Failure is terminal, not a warning (`event_type=fail`, `outcome=fail`).
- Classification is explicit (`context.case=terminal_failure`).
- Root-cause hints are already embedded in `context.details` (for this run: missing public API test coverage across listed crates).
- Replay target is the same bead script and scenario (`bd-2ddl`, `INFRA-6`).

### Annotated Failure Trace B: Differential First Divergence

Source: `test-results/bd_1dp9_5_4/bd-1dp9.5.4-20260213T085338Z-3400784.jsonl`

```json
{"run_id":"bd-1dp9.5.4-20260213T085338Z-3400784","scenario_id":"EXT-1","phase":"json_fts_differential_wave","status":"pass","mismatch_digest":"6871f37f9294...","first_divergence":true}
```

What this tells you:

- The phase did not hard-fail (`status=pass`) but still produced divergence evidence.
- `first_divergence=true` means the mismatch stream must be treated as a correctness incident, even if exit code is `0`.
- `mismatch_digest` is the stable handle for correlation across `.mismatch.log`, `.diagnostics.log`, and SQL traces.
- Start with the run-local mismatch artifact:
  - `test-results/bd_1dp9_5_4/logs/bd-1dp9.5.4-20260213T085338Z-3400784/json_fts_differential_wave.mismatch.log`

### Signature to Action Map

| Signature | Detection command | First response | Deterministic replay |
| --- | --- | --- | --- |
| Terminal failure (`E_TERMINAL_FAILURE`) | `jq -rc 'select(.event_type=="fail" and .error_code=="E_TERMINAL_FAILURE")' test-results/bd_2ddl/*.jsonl` | Parse `context.details` for explicit failing dimensions; confirm report SHA and artifact path continuity. | `rch exec -- bash e2e/bd_2ddl_compliance.sh --json` |
| Differential divergence (`first_divergence=true`) | `jq -rc 'select((.first_divergence // false) == true)' test-results/bd_1dp9_5_4/*.jsonl` | Open `.mismatch.log` and `.diagnostics.log`; compare `mismatch_digest` and scenario/phase to isolate first drift point. | `rch exec -- bash e2e/extension_integrated_wave_report.sh --json` |
| Schema/contract drift | `jq -r '.validation_errors, .result' test-results/bd-mblr.5.3-schema-integration-verify.json` | Validate schema version and required fields against `docs/e2e_log_schema_contract.md`; treat nonzero `validation_errors` as gate-breaking. | `rch exec -- bash scripts/verify_e2e_log_schema.sh --json --deterministic --seed 424242` |

### CI-to-Raw-Event Drilldown Commands

```bash
# 1) Show terminal failures and divergence markers in one pass.
jq -rc 'select(.event_type=="fail" or (.first_divergence // false) == true)' \
  test-results/bd_2ddl/*.jsonl test-results/bd_1dp9_5_4/*.jsonl

# 2) For a chosen run, list event timeline with phase/status/error.
jq -r '[.timestamp, .scenario_id, .phase, (.event_type // .marker), (.status // .outcome), (.error_code // "none")] | @tsv' \
  test-results/bd_2ddl/bd-2ddl-20260213T092408Z-109509.jsonl

# 3) Follow artifact pointers for deep inspection.
jq -r '.artifact_paths[]?' test-results/bd_2ddl/bd-2ddl-20260213T092408Z-109509.jsonl | sort -u
```

### Synchronization Notes

- This runbook is pinned to schema version `1.0.0` and current artifact layout under `test-results/`.
- If schema or artifact layout changes, update:
  - `docs/e2e_log_schema_contract.md`
  - `docs/e2e_shell_script_log_profile.json`
  - This runbook section and detection/replay commands in lockstep.
