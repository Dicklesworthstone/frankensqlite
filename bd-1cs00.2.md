Audit the codebase, specifically `fsqlite-mvcc` and `fsqlite-core`, for unstructured `std::thread::spawn` calls. Replace them with `asupersync::Region` and `scope.spawn`.

**Background & Reasoning:**
`asupersync` enforces a 'no orphans' invariant where tasks are owned by regions. Currently, FrankenSQLite spawns raw OS threads for tasks like MVCC garbage collection (`gc_tick`) or pager flushing, escaping the runtime's structured concurrency model. This means that dropping a database connection might leave zombie threads running, breaking shutdown formalisms and causing use-after-free panics in tests.

**Implementation Steps:**
1. **Locate Spawns:** Rip out all `std::thread::spawn` or `std::thread::Builder::new().spawn()` calls in `fsqlite-mvcc/src/lifecycle.rs` and the pager cache.
2. **Establish Root Region via Runtime:** Modify `Connection` to instantiate an internal `asupersync::Runtime` dedicated to managing its background tasks. The connection will hold a handle to a long-lived `Region` on this runtime.
3. **Migrate Thread Spawns:** Migrate the threads to `scope.spawn(|cx| async { ... })` within the connection's runtime. Ensure the GC loops use `cx.checkpoint()?` correctly to respond to shutdown requests.
4. **Shutdown & Quiescence:** Because `Connection::drop` is synchronous, we cannot use `.await` to wait for regions to close. We must implement an explicit `pub fn shutdown(self)` on the `Connection` that gracefully triggers cancellation and uses a blocking wait (e.g. `runtime.block_on(scope.close())`) for clean, formal teardown of the background workers. If `Drop` is called without `shutdown` having been called, we should emit a `WARN` trace and attempt a synchronous block.
5. **Observability:** Emit `tracing::debug!(target: "fsqlite_mvcc::region", ...)` when regions are created, and `tracing::info!(target: "fsqlite_mvcc::quiescence", ...)` during the drain and finalization phases.

**Testing & Validation Requirements:**
- **Explicit Shutdown Test:** Verify that `conn.shutdown()` reliably terminates all GC tasks cleanly without deadlocking.
- **E2E Drop Panic Test:** Create a script to simulate abrupt Drops.
  ```rust
  #[test]
  fn test_mvcc_zombie_prevention() {
      let lab = asupersync::lab::LabRuntime::new(LabConfig::default().seed(42));
      lab.run(|cx| async {
          cx.region(|scope| async {
              let _conn = Connection::open_in_memory().unwrap();
              // Do some writes to trigger GC
              // Drop _conn abruptly without calling shutdown()
          }).await;
      });
      // The obligation leak oracle will strictly prove that the region successfully 
      // waited on the background GC tasks to die before the test exited.
      assert!(lab.obligation_leak_oracle().is_ok());
      assert!(lab.quiescence_oracle().is_ok());
  }
  ```