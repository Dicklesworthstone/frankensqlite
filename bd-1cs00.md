Currently, FrankenSQLite fails to fully utilize the core features of the `asupersync` runtime. While it uses it for fountain codes, it completely ignores the formal quiescence and shutdown guarantees, cancel-correctness via two-phase effects, and `Cx` checkpoints.

**Goal:**
Fully integrate `asupersync` throughout the execution, storage, and background processing layers to ensure true structured concurrency, responsive cancellation, and no orphaned tasks. We must do this without compromising the VDBE throughput or leaking memory during panics.

**Background & Justification:**
The VDBE opcode evaluator loop in `fsqlite-vdbe` iterates infinitely over opcodes synchronously without taking a `&mut Cx` or calling `cx.checkpoint()`. This means massive analytical queries cannot be cancelled or preempted by the runtime.
Furthermore, the B-Tree and Pager layers perform mutations synchronously without two-phase reserve/commit permits, meaning cancellation mid-split could leave the graph in an inconsistent state. Finally, `fsqlite-mvcc` spawns background GC threads using raw `std::thread::spawn` instead of `asupersync::Region`, breaking the 'no orphans' guarantee.

**Considerations & Systemic Revisions:**
1. **Execution:** Pushing `&mut Cx` into the VDBE loop requires updating the signature of `Vdbe::execute` and all its upstream callers in `Connection::execute`. It also requires a strategy for yielding without trashing instruction cache (e.g., yielding every 1,024 instructions).
2. **Quiescence:** Replacing `std::thread::spawn` with regions will require designing a root region for the `Connection` or `VersionStore`. Since `Drop` cannot be asynchronous in Rust, we must either expose an explicit `async fn shutdown()` method or use a synchronous `scope.close_blocking()` equivalent to ensure the GC threads actually quiesce before process exit.
3. **Two-Phase B-Tree:** Two-phase commits for B-tree mutations are highly complex. We need an intent log or a `WritePermit` paradigm where `reserve_write_page` allocates and locks everything infallibly, and `.commit()` applies the byte changes.
4. **Testing & Validation:** We must enforce deterministic `asupersync::LabRuntime` tests and end-to-end (e2e) scripts for every sub-feature. We will use Mazurkiewicz trace and invariant monitoring provided by `asupersync` to verify no orphaned tasks and no unbounded cancellation delays.

**Observability Hook Requirements:**
- `tracing::debug!(target: "fsqlite_mvcc::region", ...)` for lifecycle events.
- `tracing::info!(target: "fsqlite_vdbe::cancel", ...)` for query aborts.
- `tracing::trace!(target: "fsqlite_btree::mutation", ...)` for two-phase permit acquisitions.