Update `fsqlite-vdbe/src/engine.rs` to take `&Cx` in `Vdbe::execute`. Add `cx.checkpoint()?` inside the main opcode evaluation loop to allow `asupersync` to cancel or preempt long-running analytical queries.

**Background & Reasoning:**
Without a checkpoint, a large query or Cartesian join will block the thread indefinitely, breaking the responsive cancellation guarantees of `asupersync`. This means queries cannot be timed out by the user or an encompassing budget. However, we cannot simply invoke `cx.checkpoint()?` on *every single opcode*, as that would destroy VDBE throughput via branch prediction penalties and function call overhead.

**Implementation Steps:**
1. **Signature Update:** Modify `Vdbe::execute` signature to accept `cx: &Cx` alongside the `VdbeProgram`.
2. **Execution Loop Integration:** Inside `engine.rs`'s `execute` loop, implement a countdown budget:
   ```rust
   let mut checkpoint_budget = 1024;
   loop {
       checkpoint_budget -= 1;
       if checkpoint_budget == 0 {
           cx.checkpoint().map_err(|e| ExecOutcome::Cancelled(e.to_string()))?;
           checkpoint_budget = 1024;
       }
       // ... existing opcode matching ...
   }
   ```
3. **Connection Integration:** Update the upstream callers in `fsqlite-core/src/connection.rs` (`Connection::execute_program`, etc.) to pass the active `Cx`. Crucially, `Connection` methods are synchronous and generate a per-operation `Cx` using `self.op_cx()`. You must pass the borrowed `&cx` from `self.op_cx()` down the chain to the VDBE.
4. **Error Handling & Observability:** When `cx.checkpoint()` yields a cancellation error, emit an `INFO` or `WARN` level trace using `tracing::info!(target: "fsqlite_vdbe::cancel", reason = %e, opcode_count)` before returning the new `ExecOutcome::Cancelled`.

**Testing & Validation Requirements:**
- **Unit Tests:** Write a unit test that executes a deliberately infinite VDBE loop (`Opcode::Goto 0`) and assert that it correctly aborts via `cx.cancel()` from a parallel thread.
- **E2E / LabRuntime Validation Script:**
  We must prove cancellation is bounded. Create `crates/fsqlite-harness/tests/test_vdbe_cancellation.rs`:
  ```rust
  #[test]
  fn test_vdbe_cancellation_bounded() {
      let lab = asupersync::lab::LabRuntime::new(LabConfig::default().seed(42));
      lab.run(|cx| async {
          cx.region(|scope| async {
              let child = scope.spawn(|cx| async {
                  // Run an infinite Cartesian join query here
                  let mut conn = Connection::open_in_memory().unwrap();
                  // In lab, execute must eventually hit cx.checkpoint() which will fail
                  conn.execute("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt) SELECT * FROM cnt;").unwrap();
                  Outcome::ok(())
              });
              // Yield to allow query to start
              asupersync::runtime::yield_now(cx).await;
              child.cancel(CancelReason::Timeout);
          }).await;
      });
      // The quiescence oracle will fail if the VDBE doesn't respect the checkpoint
      assert!(lab.quiescence_oracle().is_ok());
  }
  ```