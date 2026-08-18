# bd-oxw4d — Decomposition Plan for `crates/fsqlite-core/src/connection.rs`

**Status:** planning deliverable (analysis-only; no code moved yet). Produced 2026-08-18.
Acceptance criteria for bd-oxw4d (identify boundaries · name borrow constraints/tests/risk
per extraction · record a focused implementation sequence · preserve concurrent-writer
defaults, no shims/broad rewrites) are all satisfied by this plan. Implementation (Seams 0–4)
is follow-on work.

## Orientation facts (verified against HEAD at authoring time)

- `pub struct Connection` spans `connection.rs:10627–11595` — ~200 fields, almost all
  `RefCell`/`Cell`/`Arc`/`OnceCell`. Public method surface = 180 `pub`/`pub async fn`.
- **The extraction mechanism is already wired:** `connection.rs:34 mod conformal_retry;` and
  `:35 mod pragma_maintenance;` resolve to `connection/*.rs`, each `use super::*;` +
  `impl Connection { pub(super) … }` (see `connection/pragma_maintenance.rs:1–4`). This is
  *impl-block splitting* — the lowest-risk lever.
- The pager does **not** hold a back-reference to `Connection` (`SimplePager` at
  `fsqlite-pager/src/pager.rs:11798`; no `Connection`/`Rc`/`Weak` in `wal_adapter.rs`). The
  "bidirectional borrow" is the **Connection ↔ `Rc<RefCell<MemDatabase>>` mirror ↔ pager-txn
  triangle**, not pager→Connection.
- MVCC concurrency state is **already fully `Arc`-shared** in `SharedMvccState`
  (`connection.rs:96798`); `Connection` holds `Arc` clones + a keep-alive. Grouping those
  `Arc`s cannot change borrow semantics → the safest genuine sub-struct.
- `concurrent_mode_default: RefCell::new(true)` at **both** constructors (`12788`, `13310`) is
  a per-connection **policy** flag, separate from the shared bundle. Promotion sites: `48449,
  48469, 60632, 62431, 67269`; accessor `67427`; inline assertion `177391`. It MUST stay on
  `Connection`, default true, never relocated into a sub-struct.
- Tooling payoff: `AGENTS.md:640–668` excludes `connection.rs` from `ubs` (superlinear hang
  >50k lines). Every file peeled below ~50k lines re-enters `ubs` coverage.

## 1. Ownership map

### 1a. Field clusters (state any extraction must partition)

- **Pager handle:** `pager: PagerBackend` (10634) → `Arc<SimplePager<Vfs>>` (enum at 3005).
- **MemDb mirror (aliased `Rc<RefCell>`):** `db` (10631; `MemDatabase` at
  `fsqlite-vdbe/src/engine.rs:4475`), `memdb_visible_commit_seq` (11252), `memdb_rows_loaded`
  (11255), `memdb_requires_active_txn_reload` (11259), `schema_reload_parse_cache/_count`
  (11268/11273), `memdb_row_hydration_count` (11282), `memdb_storage_count_shortcuts_safe`
  (11286).
- **Txn / savepoint:** `active_txn` (10637; `TransactionKind` at
  `fsqlite-pager/src/traits.rs:1626`), `in_transaction` (10963), `txn_snapshot` (10965),
  `savepoints` (10968), `txn_lifecycle_metrics` (10971), `implicit_txn` (10994),
  `concurrent_txn` (10998), `pending_transaction_cleanup` (11013),
  `internal_statement_savepoint_depth` (10990).
- **Autocommit fast-path caches:** `cached_read_snapshot*` (10648–55),
  `cached_autocommit_publication*` (10658–60), `cached_write_txn*` (10667–73),
  `retained_autocommit_*` (10684–10735), `storage_count_cache` (10713).
- **Prepared/direct-DML scratch + pending runs:** `cached_vdbe_engine` (10742),
  `prepared_direct_insert_*` (10754–72), `prepared_direct_update_row_scratch` (10779),
  `pending_memdb_direct_upserts` (10825), `pending_direct_{insert_page_run,update_leaf_patch_run,delete_leaf_run}*`
  (10834–61), `quotient_filters*` (10791–800), `precomputed_in_sets` (10804),
  `exists_probe_memo` (10815).
- **Schema catalog:** `schema` (10869), `schema_by_name` (10876), `temp_table_names` (10883),
  `shadowed_main_tables` (10892), `views`/`views_by_name` (10894/98),
  `triggers`/`triggers_by_name` (10900/02), `trigger_frame_stack` (10904),
  `rowid_alias_columns` (11060), `without_rowid_pk_desc` (11069), `original_ddl_sql` (11074),
  `pending_ddl_source` (11081), `schema_cookie` (11096), `schema_generation` (11099),
  `force_full_schema_reload_once` (11115), `db_text_encoding` (11121), `change_counter`
  (11124), `next_temp_root_page` (10887).
- **Function/collation registry:** `func_registry` (10907), `scalar_function_overridden`
  (10920), `connection_registry_differs_from_base` (10930), `custom_aggregate_*` (10936–43),
  `custom_window_functions` (10950), `like_or_glob_overridden` (10957),
  `function_registry_generation` (10959), `collation_registry` (10961).
- **Autoincrement / sequence / changes:** `autoincrement_tables` (11084),
  `sqlite_sequence_cache` (11086), `next_master_rowid` (11090), `last_changes` (10979),
  `last_insert_rowid` (10981), `total_changes` (10983).
- **FK enforcement:** `fk_cascade_depth` (11129) … `fk_parent_validation_cache` (11190),
  `deferred_fk_checks` (11140), `statement_fk_*` (11152–66).
- **Live-vtab registry:** `vtab_modules` (11414), `vtab_instances` (11418),
  `dropped_vtab_instances` (11422), `live_vtab_transactions` (11428),
  `live_vtab_registry_undo` (11431), `live_vtab_failed_begin_cleanup` (11435),
  `live_vtab_lifecycle_failure` (11440), `live_vtab_callback_depth` (11455),
  `pending_local_live_vtab_preservation` (11308).
- **Plan / statement caches:** `parse_cache`/`_cookie` (11460/63), `compiled_cache` (11468),
  `bypass_compiled_cache` (11471), `prepared_cache` (11480), `statement_reuse_trace` (11484),
  `planner_directive_cache` (11488), `prepared_indexed_equality_cache` (11491),
  `group_by_bucket_fast_memo` (11506), `table_execution_metadata_cache` (11510).
- **MVCC shared bundle (all `Arc`):** `conflict_observer` (11197), `concurrent_registry`
  (11206), `_shared_mvcc_state` (11209), `runtime_region` (11221), `concurrent_lock_table`
  (11235), `concurrent_commit_index` (11237), `active_commit_seqs` (11239), `next_commit_seq`
  (11241), `stable_commit_seq` (11243), `committed_schema_cookie` (11249),
  `data_version_global` (11304), `version_store` (11372), `gc_*` (11374–78) + per-conn
  `concurrent_session_id` (11224), `cached_concurrent_handle` (11232),
  `memory_concurrent_synced_write_roots` (11229), `data_version_own_commits` (11300).
- **Concurrency POLICY (stays on Connection):** `concurrent_mode_default` (11016),
  `write_merge_mode` (11020), `ssi_e_process_gate` (11040).
- **Attach:** `attached_schemas` (11524), `attach_env` (11527), `attached_connections`
  (11531). **Time-travel:** `time_travel_snapshots` (11535), `time_travel_active` (11539),
  `time_travel_capture_enabled` (11549). **Cx/lifecycle/misc:** `root_cx` (11360),
  `operation_cx_override` (11364), `closed` (11311), `pragma_state` (11054), etc.

### 1b. The pager/WAL/adapter boundary + named bidirectional cycles

The entanglement is a **triangle**, not a back-pointer:

1. `Connection.pager` (10634) vends `TransactionKind` via `self.pager.begin(cx, mode)`
   (e.g. `82275, 82282`).
2. `Connection.db` (10631) is **aliased**: `Rc::clone`d into execution + snapshot/restore
   guards (`39119, 39188, 39260, 39351, 39426, 62039`) and handed to the VDBE via free fn
   `execute_table_program_with_db(… db: &Rc<RefCell<MemDatabase>>, txn: Option<TransactionKind>, …)`
   (`113752–113781`).
3. The sync closing the cycle is `reload_memdb_from_pager*` (`82249, 82290, 82342`): within
   one `&self` call it binds pager publication, `pager.begin`→txn, `borrow_mut` rebuilds
   `self.db` from that txn, rebuilds the schema catalog, and publishes into MVCC atomics
   (`committed_schema_cookie` 11249). Hot entrypoints: `refresh_memdb_if_stale` (21207),
   `refresh_memdb_from_active_txn_if_dirty` (21214).

- **Cycle A (mirror⇄pager):** `db` ⇄ `active_txn` ⇄ `pager` co-borrowed;
  `reload_memdb_from_txn_with_mode` writes `db` from a pager txn.
- **Cycle B (mirror⇄schema⇄mvcc):** reload rebuilds `schema`/`schema_cookie` and pushes into
  `committed_schema_cookie`/`stable_commit_seq`.
- **Cycle C (vtab re-entrancy):** live-vtab callbacks run user SQL that re-enters `Connection`
  (guarded by `live_vtab_callback_depth` 11455; guards `TakenLiveVtabRestoreGuard` 11638 /
  `PendingLiveVtabGuard` 11644 mutate `vtab_instances`/`dropped_vtab_instances` on unwind
  `11731–11766`). Any sub-struct here must drop its `RefCell` borrow before re-entry.

The MVCC bundle is `Arc`-shared/lock-mediated → NOT part of the borrow entanglement → safest
to relocate.

## 2. Ranked extraction seams (easiest / highest value first)

- **Seam 0 — Impl-block splitting (no struct change) — DO FIRST.** Move method *bodies* only,
  grouped by responsibility, into new `connection/<area>.rs` files (`use super::*; impl
  Connection { pub(super) fn … }`, exactly like `connection/pragma_maintenance.rs`). Targets:
  live-vtab (`17637–18053+`), memdb reload/refresh (`21160–21317`, `82249–82385`), txn
  lifecycle (`60604, 61479, 62071/62114, 62417, 62584`), schema-master build (`60266, 40712`),
  registration API (`21897–22172`). *Borrow constraints:* none — fields stay on `Connection`,
  `&self` unchanged, `pub(super)` keeps the surface identical; co-move private helpers + inline
  `#[cfg(test)]` modules. *Tests:* none semantically; `cargo test -p fsqlite-core`; new files
  become `ubs`-eligible. *Rollback risk:* **Very Low** (pure textual move; fails at
  `cargo check`, never runtime).
- **Seam 1 — `LiveVtabRegistry` sub-struct.** Move fields `11414–11455` + `11308`. *Borrow:*
  Cycle C — keep callback-invoking methods (`17882, 17920`) on `Connection`, borrow the
  sub-struct only around take/restore, drop before re-entry; switch guards `11638/11644` to
  `&self.vtab_registry`. *Tests:* vtab lifecycle/rollback e2e + `bd_o01lp_..._gh357.rs` + inline
  live-vtab. *Rollback risk:* **Medium** (`RefCell` re-borrow ordering across callback).
- **Seam 2 — `PlanCaches` sub-struct.** Move `11460–11510`. *Borrow:* caches invalidated
  together keyed on `schema_cookie` (11096) — pass the cookie in, don't borrow the schema
  cluster; leave `cached_vdbe_engine` (10742) on `Connection`. *Tests:*
  `prepared_hit_rate_proof.rs`, `fast_path_separation.rs`, `b4_query_row_indexed_equality.rs`.
  *Rollback risk:* **Low–Medium.**
- **Seam 3 — `SequenceState` sub-struct.** Move `11084, 11086, 11090, 10981, 10979, 10983`.
  *Borrow:* minimal. *Tests:* autoincrement/sqlite_sequence oracles. *Rollback risk:* **Low.**
- **Seam 4 — `MvccShared` sub-struct (the Arc bundle) — low-contention window only.** Move the
  pure-`Arc` MVCC fields (mirroring `SharedMvccState` 96798). *Borrow:* none semantic (grouping
  `Arc`s); cost is a wide mechanical `self.field`→`self.mvcc.field` rename. **`concurrent_mode_default`
  / `write_merge_mode` / `ssi_e_process_gate` MUST NOT move.** *Rollback risk:* **Low**
  correctness / **Medium** merge.

**Deliberately NOT a seam this pass:** the MemDb-mirror ⇄ txn ⇄ pager triangle (Cycles A/B) +
the schema-rebuild path — the entanglement core; partitioning risks concurrency semantics.
Follow-up bead after Seams 0–4 shrink the file.

## 3. Focused implementation sequence (each step compiles + tests independently)

1. **Seam 0a** — split live-vtab methods → `connection/live_vtab.rs`. Guard: `cargo check` +
   vtab e2e. *Concurrency checkpoint:* `concurrent_mode_default: RefCell::new(true)` still at
   both constructors.
2. **Seam 0b** — split memdb reload/refresh → `connection/memdb_reload.rs`. Guard:
   `bd_qteu2_memdb_reload_row_rehydration_scaling.rs`, `bd_420r8_trigger_when_subquery_reparse.rs`.
3. **Seam 0c** — split txn lifecycle → `connection/txn_lifecycle.rs`. Guard: transaction +
   savepoint integration; **run the concurrent-writer guard suite** (nearest promotion sites
   `60632, 62431`).
4. **Seam 0d/0e** — split schema-master build + registration API → `connection/schema_master.rs`,
   `connection/registration.rs`. Guard: sqlite_master + UDF tests.
5. **Seam 1** — `LiveVtabRegistry` sub-struct. Guard: full vtab lifecycle/rollback e2e; watch
   `already borrowed` panics.
6. **Seam 2** — `PlanCaches`. Guard: `prepared_hit_rate_proof.rs`, `fast_path_separation.rs`.
7. **Seam 3** — `SequenceState`. Guard: autoincrement oracles.
8. **Seam 4** — `MvccShared` (low-contention window). Guard: entire concurrent-writer suite;
   assert no call-site moved `concurrent_mode_default`.

**Concurrency-preservation checkpoint (after every step, mandatory before landing):**
- Static: `rg "concurrent_mode_default: RefCell::new\(true\)"` returns both `12788` and
  `13310`; field still a direct `Connection` member; no new `Mutex`/file-lock/serialization in
  `MemoryVfs` (the Feb-10 regression class, `AGENTS.md:359`).
- Dynamic (must stay green, unchanged): `mvcc_concurrent_writers.rs`,
  `concurrent_writer_mvcc_oracle_e2e.rs`, `bd_1r0ha_3_concurrent_writer_e2e.rs`,
  `ssi_conflict_oracle_e2e.rs`, `multi_connection_visibility_oracle_e2e.rs`,
  `swarm_writer_harness.rs`, inline assertion `connection.rs:177391`.

## 4. Risks & non-goals

**Non-goals:** no MVCC semantics change; `concurrent_mode_default` stays a `Connection` field,
default true, promotion logic untouched; no compat shims/wrapper fns/back-compat; no broad
rewrite (Cycles A/B + schema-rebuild left intact for a follow-up); no change to the 180-method
public API (methods keep `&self`; only file location or `self.<cluster>.` receiver changes).

**Top risks & mitigations:**
1. `RefCell` re-borrow panics across re-entrant calls (Cycle C in Seam 1; any method that reads
   a cluster then calls `self.query()`/reload). *Mitigation:* keep re-entrant methods on
   `Connection`, borrow the sub-struct only around the leaf mutation; vtab + reload e2e exercise
   the exact paths.
2. Merge conflicts vs heavy concurrent editing (Seam 4's wide rename is worst). *Mitigation:*
   Seam 0 first (it *reduces* contention by scattering hot method groups); defer Seam 4 to a
   quiet window; reserve `connection.rs` via agent-mail.
3. Silent cache-invalidation drift if PlanCaches loses `schema_cookie` gating. *Mitigation:*
   pass the cookie explicitly; `prepared_hit_rate_proof.rs` + `fast_path_separation.rs` are the
   keepers.

## Critical files
- `crates/fsqlite-core/src/connection.rs` (struct `10627–11595`; reload `82249`, txn
  `60604–62584`, live-vtab `17637–18053`, execute free-fn `113752`)
- `crates/fsqlite-core/src/connection/pragma_maintenance.rs` (the canonical submodule template)
- `crates/fsqlite-vdbe/src/engine.rs` (`MemDatabase` `4475` — the `Rc<RefCell>` mirror consumer)
- `crates/fsqlite-pager/src/traits.rs` (`TransactionKind`/`TransactionHandle` `1626` — pager txn boundary)
- `crates/fsqlite-e2e/tests/mvcc_concurrent_writers.rs` (+ `ssi_conflict_oracle_e2e.rs`,
  `swarm_writer_harness.rs` — the concurrency-preservation guard suite)
