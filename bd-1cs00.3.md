Refactor B-Tree balancing (`balance_for_insert`) and page cache mutations to use `asupersync`'s two-phase effect permits (reserve/commit).

**Background & Reasoning:**
Asupersync relies on a "reserve/commit" architecture to ensure cancel-correctness without silent data loss. Currently, if a task is cancelled (via a `Cx` checkpoint exception) or panics mid-mutation in the B-Tree (e.g. during a multi-page interior node split), it can leave the page graph in an inconsistent transient state. By separating the intention (reserve) from the non-fallible mutation (commit), we guarantee atomic transitions.

**Implementation Steps:**
1. **API Design:** Define a robust reserve/commit API model within `TransactionPageIo` and the `PageWriter` traits.
   ```rust
   pub trait PageWriter: PageReader {
       /// Phase 1: Infallible allocation and locking
       fn reserve_write<'a>(&'a mut self, cx: &Cx, page_no: PageNumber) -> Result<WritePermit<'a>>;
   }
   
   pub struct WritePermit<'a> { ... }
   impl<'a> WritePermit<'a> {
       /// Phase 2: Infallible commit
       pub fn commit(self, data: &[u8]);
   }
   ```
2. **Refactor B-Tree Splitting:** Refactor `balance_for_insert` (and `balance_deeper` / `balance_nonroot`) in `fsqlite-btree`. The code must calculate the new structure, allocate new pages, construct the updated data in memory, and acquire `WritePermit`s for **all** affected pages *before* writing to any of them. If `reserve_write` fails for the 3rd page out of 4, the prior 2 permits are dropped and cleanly roll back.
3. **Linear Commit:** Once all permits are acquired (a cancellation-safe point), invoke `.commit()` on them. The commit step MUST NOT contain a `?` operator or a `cx.checkpoint()`.
4. **Abort Safety:** Implement `Drop` for `WritePermit` such that dropping a permit without calling `.commit()` cleanly aborts the operation, releasing locks and deallocating any speculatively allocated pages without polluting the committed pager state.

**Testing & Validation Requirements:**
- **Unit Tests:** Implement tests that forcibly inject a simulated cancellation or panic immediately after permits are acquired but before they are committed, and assert that the original page graph is entirely untouched and uncorrupted.
- **Continuous DPOR Fuzzing:**
  ```rust
  #[test]
  fn test_btree_split_cancel_safety() {
      let mut config = asupersync::lab::ExplorerConfig::default();
      config.inject_random_cancellations = true; // Lab feature
      let explorer = asupersync::lab::DporExplorer::new(config);
      
      explorer.explore(|cx| async {
          // Trigger a complex B-tree split here
          // The DPOR engine will systematically cancel the task at every yield point.
          // We assert the database isn't corrupted upon recovery.
      });
  }
  ```