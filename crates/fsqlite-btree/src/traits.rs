#![allow(clippy::future_not_send)]
//! B-tree cursor operations trait (sealed).
//!
//! This module defines `BtreeCursorOps`, the internal interface used by
//! the VDBE to navigate and mutate B-tree structures. The trait is sealed
//! to enforce MVCC safety invariants — only this crate provides implementations.
//!
//! # Cursor semantics
//!
//! A cursor is bound to a single transaction and a single B-tree (either
//! a table intkey tree or an index blobkey tree). Cursors are NOT `Send`
//! or `Sync` — they are pinned to the creating thread.
//!
//! # Two B-tree types
//!
//! - **Table B-trees (intkey):** Keyed by `i64` rowid, leaves store record payloads.
//! - **Index B-trees (blobkey):** Keyed by arbitrary byte sequences, leaves are key-only.

use std::borrow::Cow;
use std::future::Future;

use fsqlite_error::Result;
use fsqlite_types::cx::Cx;

// ---------------------------------------------------------------------------
// Sealed trait discipline
// ---------------------------------------------------------------------------

pub(crate) mod sealed {
    /// Marker trait restricting implementation to this crate.
    pub trait Sealed {}
}

// ---------------------------------------------------------------------------
// Cursor seek result
// ---------------------------------------------------------------------------

/// Result of a seek operation on a B-tree cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekResult {
    /// The exact key was found; the cursor points to it.
    Found,
    /// The key was not found; the cursor points to the entry that would
    /// follow it in sort order (or is at EOF if no such entry exists).
    NotFound,
}

impl SeekResult {
    /// Whether the seek found an exact match.
    #[must_use]
    pub fn is_found(self) -> bool {
        self == Self::Found
    }
}

// ---------------------------------------------------------------------------
// BtreeCursorOps
// ---------------------------------------------------------------------------

/// Low-level B-tree cursor operations.
///
/// This trait is consumed by the VDBE to implement `OP_SeekRowid`,
/// `OP_Next`, `OP_Insert`, `OP_Delete`, etc. Each cursor is bound to
/// a single transaction and a single B-tree root page.
///
/// # NOT Send/Sync
///
/// Cursors hold mutable references into the page cache and are bound
/// to a single transaction/thread. They must not cross thread boundaries.
///
/// # Cx Everywhere
///
/// Every method that touches I/O or could block accepts `&Cx` for
/// cancellation and deadline propagation.
///
/// # Sealed
///
/// This trait is sealed — only this crate can implement it.
pub trait BtreeCursorOps: sealed::Sealed {
    // -- Seek operations --

    /// Position the cursor at the given key in an index B-tree.
    ///
    /// Returns [`SeekResult::Found`] if the exact key exists, or
    /// [`SeekResult::NotFound`] if the cursor is positioned at the
    /// entry that would follow the key in sort order.
    fn index_move_to<'a>(
        &'a mut self,
        cx: &'a Cx,
        key: &'a [u8],
    ) -> impl Future<Output = Result<SeekResult>> + 'a;

    /// Position the cursor at the strict upper bound of an index key.
    ///
    /// Packed-record implementations treat `key` as a logical prefix and
    /// position after every stored key whose leading fields compare equal to
    /// that prefix. Raw-byte implementations use the ordinary strict byte
    /// upper bound. The cursor is at EOF when no greater key exists.
    fn index_move_to_upper_bound<'a>(
        &'a mut self,
        cx: &'a Cx,
        key: &'a [u8],
    ) -> impl Future<Output = Result<()>> + 'a {
        async move {
            self.index_move_to(cx, key).await?;
            while !self.eof() {
                if self.payload(cx).await?.as_slice() != key {
                    break;
                }
                if !self.next(cx).await? {
                    break;
                }
            }
            Ok(())
        }
    }

    /// Position the cursor at the given rowid in a table B-tree.
    fn table_move_to<'a>(
        &'a mut self,
        cx: &'a Cx,
        rowid: i64,
    ) -> impl Future<Output = Result<SeekResult>> + 'a;

    // -- Navigation --

    /// Move the cursor to the first entry. Returns `false` if the tree
    /// is empty.
    fn first<'a>(&'a mut self, cx: &'a Cx) -> impl Future<Output = Result<bool>> + 'a;

    /// Move the cursor to the last entry. Returns `false` if the tree
    /// is empty.
    fn last<'a>(&'a mut self, cx: &'a Cx) -> impl Future<Output = Result<bool>> + 'a;

    /// Advance the cursor to the next entry. Returns `false` if there
    /// is no next entry (cursor is now at EOF).
    fn next<'a>(&'a mut self, cx: &'a Cx) -> impl Future<Output = Result<bool>> + 'a;

    /// Move the cursor to the previous entry. Returns `false` if there
    /// is no previous entry.
    fn prev<'a>(&'a mut self, cx: &'a Cx) -> impl Future<Output = Result<bool>> + 'a;

    // -- Mutation --

    /// Insert a key into an index B-tree.
    ///
    /// If the key already exists, it is replaced.
    fn index_insert<'a>(
        &'a mut self,
        cx: &'a Cx,
        key: &'a [u8],
    ) -> impl Future<Output = Result<()>> + 'a;

    /// Insert a key into a UNIQUE index B-tree.
    ///
    /// Before inserting, checks whether a key with the same first
    /// `n_unique_cols` fields already exists. If such a key is found
    /// **and** none of those fields are NULL, returns
    /// `FrankenError::UniqueViolation`. Multiple NULL entries are
    /// permitted (SQLite semantics).
    ///
    /// `columns_label` is used in the error message.
    fn index_insert_unique<'a>(
        &'a mut self,
        cx: &'a Cx,
        key: &'a [u8],
        n_unique_cols: usize,
        columns_label: &'a str,
    ) -> impl Future<Output = Result<()>> + 'a {
        async move {
            // Default: fall back to non-unique insert (MockBtreeCursor etc.).
            let _ = (n_unique_cols, columns_label);
            self.index_insert(cx, key).await
        }
    }

    /// Insert a row into a table B-tree.
    ///
    /// `rowid` is the integer key. `data` is the serialized record payload.
    fn table_insert<'a>(
        &'a mut self,
        cx: &'a Cx,
        rowid: i64,
        data: &'a [u8],
    ) -> impl Future<Output = Result<()>> + 'a;

    /// Delete the entry at the current cursor position.
    ///
    /// The cursor is positioned at the next entry after deletion (or EOF
    /// if the deleted entry was the last one).
    fn delete<'a>(&'a mut self, cx: &'a Cx) -> impl Future<Output = Result<()>> + 'a;

    // -- Access --

    /// Read the payload at the current cursor position.
    ///
    /// For table B-trees, this is the serialized record. For index
    /// B-trees, this is the key bytes.
    fn payload<'a>(&'a self, cx: &'a Cx) -> impl Future<Output = Result<Vec<u8>>> + 'a;

    /// Read the payload at the current cursor position into the provided buffer,
    /// clearing it first. This avoids allocations when repeatedly reading payloads.
    fn payload_into<'a>(
        &'a self,
        cx: &'a Cx,
        buf: &'a mut Vec<u8>,
    ) -> impl Future<Output = Result<()>> + 'a;

    /// Read the rowid and payload at the current cursor position.
    ///
    /// The default preserves the historical two-call behavior for mock cursors.
    /// Real table cursors can override this to parse the current cell once.
    fn rowid_and_payload_into<'a>(
        &'a self,
        cx: &'a Cx,
        buf: &'a mut Vec<u8>,
    ) -> impl Future<Output = Result<i64>> + 'a {
        async move {
            let rowid = self.rowid(cx).await?;
            self.payload_into(cx, buf).await?;
            Ok(rowid)
        }
    }

    /// Read the rowid and payload at the current cursor position, returning a
    /// borrowed payload when the implementation can safely expose one.
    ///
    /// The default keeps mock and non-specialized cursors correct by owning the
    /// payload bytes. Real table cursors override this to borrow local page
    /// payloads and allocate only for overflow chains.
    fn rowid_and_payload_cow<'a>(
        &'a self,
        cx: &'a Cx,
    ) -> impl Future<Output = Result<(i64, Cow<'a, [u8]>)>> + 'a {
        async move {
            let rowid = self.rowid(cx).await?;
            Ok((rowid, Cow::Owned(self.payload(cx).await?)))
        }
    }

    /// Read only a prefix of the payload at the current cursor position into
    /// the provided buffer, clearing it first.
    fn payload_prefix_into<'a>(
        &'a self,
        cx: &'a Cx,
        max_prefix_bytes: usize,
        buf: &'a mut Vec<u8>,
    ) -> impl Future<Output = Result<()>> + 'a;

    /// Read the rowid at the current cursor position.
    ///
    /// For table B-trees this is the integer key. For index B-trees this is
    /// extracted from the trailing field of the serialized key record.
    fn rowid<'a>(&'a self, cx: &'a Cx) -> impl Future<Output = Result<i64>> + 'a;

    /// Whether the cursor is at EOF (past the last entry).
    fn eof(&self) -> bool;
}

// ---------------------------------------------------------------------------
// Exported test mock (cross-crate)
// ---------------------------------------------------------------------------

/// Test/mock cursor exported for cross-crate tests.
#[derive(Debug, Default)]
pub struct MockBtreeCursor {
    at_eof: bool,
    current_rowid: i64,
    entries: Vec<(i64, Vec<u8>)>,
    pos: usize,
}

impl MockBtreeCursor {
    /// Create a mock cursor with pre-seeded `(rowid, payload)` entries.
    #[must_use]
    pub fn new(entries: Vec<(i64, Vec<u8>)>) -> Self {
        Self {
            at_eof: entries.is_empty(),
            current_rowid: entries.first().map_or(0, |e| e.0),
            entries,
            pos: 0,
        }
    }
}

impl sealed::Sealed for MockBtreeCursor {}

// This in-memory test backend deliberately returns immediately-ready futures:
// none of its operations perform I/O or need to suspend.
#[allow(clippy::missing_errors_doc, clippy::unused_async_trait_impl)]
impl BtreeCursorOps for MockBtreeCursor {
    async fn index_move_to(&mut self, _cx: &Cx, key: &[u8]) -> Result<SeekResult> {
        // Linear scan for exact match, then find successor position if not found.
        // Per SeekResult::NotFound contract: cursor must point to the entry that
        // would follow the key in sort order (or be at EOF if no such entry exists).
        for (i, (_, data)) in self.entries.iter().enumerate() {
            if data.as_slice() == key {
                self.pos = i;
                self.at_eof = false;
                self.current_rowid = self.entries[i].0;
                return Ok(SeekResult::Found);
            }
        }
        // Not found: find successor position (first entry with key > target).
        // Entries assumed to be sorted by key for proper seek semantics.
        let successor_pos = self
            .entries
            .iter()
            .position(|(_, data)| data.as_slice() > key);
        if let Some(pos) = successor_pos {
            self.pos = pos;
            self.at_eof = false;
            self.current_rowid = self.entries[pos].0;
        } else {
            // No successor exists - position at EOF.
            self.pos = self.entries.len();
            self.at_eof = true;
        }
        Ok(SeekResult::NotFound)
    }

    async fn table_move_to(&mut self, _cx: &Cx, rowid: i64) -> Result<SeekResult> {
        // Linear scan for exact match, then find successor position if not found.
        // Per SeekResult::NotFound contract: cursor must point to the entry that
        // would follow the rowid in sort order (or be at EOF if no such entry exists).
        for (i, (rid, _)) in self.entries.iter().enumerate() {
            if *rid == rowid {
                self.pos = i;
                self.at_eof = false;
                self.current_rowid = rowid;
                return Ok(SeekResult::Found);
            }
        }
        // Not found: find successor position (first entry with rowid > target).
        // Entries assumed to be sorted by rowid for proper seek semantics.
        let successor_pos = self.entries.iter().position(|(rid, _)| *rid > rowid);
        if let Some(pos) = successor_pos {
            self.pos = pos;
            self.at_eof = false;
            self.current_rowid = self.entries[pos].0;
        } else {
            // No successor exists - position at EOF.
            self.pos = self.entries.len();
            self.at_eof = true;
        }
        Ok(SeekResult::NotFound)
    }

    async fn first(&mut self, _cx: &Cx) -> Result<bool> {
        if self.entries.is_empty() {
            self.at_eof = true;
            return Ok(false);
        }
        self.pos = 0;
        self.at_eof = false;
        self.current_rowid = self.entries[0].0;
        Ok(true)
    }

    async fn last(&mut self, _cx: &Cx) -> Result<bool> {
        if self.entries.is_empty() {
            self.at_eof = true;
            return Ok(false);
        }
        self.pos = self.entries.len() - 1;
        self.at_eof = false;
        self.current_rowid = self.entries[self.pos].0;
        Ok(true)
    }

    async fn next(&mut self, _cx: &Cx) -> Result<bool> {
        if self.pos + 1 >= self.entries.len() {
            self.at_eof = true;
            return Ok(false);
        }
        self.pos += 1;
        self.current_rowid = self.entries[self.pos].0;
        Ok(true)
    }

    async fn prev(&mut self, _cx: &Cx) -> Result<bool> {
        if self.entries.is_empty() {
            return Ok(false);
        }
        if self.at_eof {
            // Revive from EOF, pointing to the last entry.
            self.pos = self.entries.len() - 1;
            self.at_eof = false;
            self.current_rowid = self.entries[self.pos].0;
            return Ok(true);
        }
        if self.pos == 0 {
            return Ok(false);
        }
        self.pos -= 1;
        self.current_rowid = self.entries[self.pos].0;
        Ok(true)
    }

    async fn index_insert(&mut self, _cx: &Cx, key: &[u8]) -> Result<()> {
        let next_rowid = self.entries.iter().map(|e| e.0).max().unwrap_or(0) + 1;
        let pos = self
            .entries
            .binary_search_by(|e| e.1.as_slice().cmp(key))
            .unwrap_or_else(|e| e);
        // Replace if exists, otherwise insert
        if pos < self.entries.len() && self.entries[pos].1 == key {
            self.entries[pos] = (next_rowid, key.to_vec());
        } else {
            self.entries.insert(pos, (next_rowid, key.to_vec()));
        }
        self.current_rowid = next_rowid;
        self.pos = pos;
        self.at_eof = false;
        Ok(())
    }

    async fn table_insert(&mut self, _cx: &Cx, rowid: i64, data: &[u8]) -> Result<()> {
        let pos = self
            .entries
            .binary_search_by_key(&rowid, |e| e.0)
            .unwrap_or_else(|e| e);
        if pos < self.entries.len() && self.entries[pos].0 == rowid {
            self.entries[pos] = (rowid, data.to_vec());
        } else {
            self.entries.insert(pos, (rowid, data.to_vec()));
        }
        self.current_rowid = rowid;
        self.pos = pos;
        self.at_eof = false;
        Ok(())
    }

    async fn delete(&mut self, _cx: &Cx) -> Result<()> {
        if !self.at_eof && self.pos < self.entries.len() {
            self.entries.remove(self.pos);
            if self.pos >= self.entries.len() {
                self.at_eof = true;
            } else {
                self.current_rowid = self.entries[self.pos].0;
            }
        }
        Ok(())
    }

    async fn payload(&self, _cx: &Cx) -> Result<Vec<u8>> {
        if self.at_eof {
            return Err(fsqlite_error::FrankenError::internal("cursor at EOF"));
        }
        Ok(self.entries[self.pos].1.clone())
    }

    async fn payload_into(&self, _cx: &Cx, buf: &mut Vec<u8>) -> Result<()> {
        if self.at_eof {
            return Err(fsqlite_error::FrankenError::internal("cursor at EOF"));
        }
        buf.clear();
        buf.extend_from_slice(&self.entries[self.pos].1);
        Ok(())
    }

    async fn payload_prefix_into(
        &self,
        _cx: &Cx,
        max_prefix_bytes: usize,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if self.at_eof {
            return Err(fsqlite_error::FrankenError::internal("cursor at EOF"));
        }
        buf.clear();
        let bytes = &self.entries[self.pos].1;
        buf.extend_from_slice(&bytes[..bytes.len().min(max_prefix_bytes)]);
        Ok(())
    }

    async fn rowid(&self, _cx: &Cx) -> Result<i64> {
        if self.at_eof {
            return Err(fsqlite_error::FrankenError::internal("cursor at EOF"));
        }
        Ok(self.current_rowid)
    }

    fn eof(&self) -> bool {
        self.at_eof
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use asupersync::runtime::RuntimeBuilder;

    use super::*;

    fn run_async<F: Future>(future: F) -> F::Output {
        RuntimeBuilder::current_thread()
            .build()
            .expect("build B-tree traits test runtime")
            .block_on(future)
    }

    #[test]
    fn test_btree_cursor_ops_sealed_mock() -> Result<()> {
        run_async(async {
            let entries = vec![
                (1, b"alice".to_vec()),
                (2, b"bob".to_vec()),
                (3, b"charlie".to_vec()),
            ];
            let mut cursor = MockBtreeCursor::new(entries);
            let cx = Cx::new();

            // Navigate forward.
            assert!(cursor.first(&cx).await?);
            assert_eq!(cursor.rowid(&cx).await?, 1);
            assert_eq!(cursor.payload(&cx).await?, b"alice");
            let mut payload = Vec::new();
            assert_eq!(cursor.rowid_and_payload_into(&cx, &mut payload).await?, 1);
            assert_eq!(payload, b"alice");

            assert!(cursor.next(&cx).await?);
            assert_eq!(cursor.rowid(&cx).await?, 2);

            assert!(cursor.next(&cx).await?);
            assert_eq!(cursor.rowid(&cx).await?, 3);

            assert!(!cursor.next(&cx).await?);
            assert!(cursor.eof());
            Ok(())
        })
    }

    #[test]
    fn test_btree_cursor_seek() {
        run_async(async {
            let entries = vec![
                (10, b"ten".to_vec()),
                (20, b"twenty".to_vec()),
                (30, b"thirty".to_vec()),
            ];
            let mut cursor = MockBtreeCursor::new(entries);
            let cx = Cx::new();

            assert!(cursor.table_move_to(&cx, 20).await.unwrap().is_found());
            assert_eq!(cursor.rowid(&cx).await.unwrap(), 20);
            assert_eq!(cursor.payload(&cx).await.unwrap(), b"twenty");

            assert!(!cursor.table_move_to(&cx, 99).await.unwrap().is_found());
        });
    }

    #[test]
    fn test_btree_cursor_insert_delete() {
        run_async(async {
            let mut cursor = MockBtreeCursor::new(vec![]);
            let cx = Cx::new();

            assert!(!cursor.first(&cx).await.unwrap());
            assert!(cursor.eof());

            cursor.table_insert(&cx, 1, b"hello").await.unwrap();
            cursor.table_insert(&cx, 2, b"world").await.unwrap();

            assert!(cursor.first(&cx).await.unwrap());
            assert_eq!(cursor.payload(&cx).await.unwrap(), b"hello");

            cursor.delete(&cx).await.unwrap();
            assert!(!cursor.eof());
            assert_eq!(cursor.rowid(&cx).await.unwrap(), 2);
        });
    }

    #[test]
    fn test_btree_cursor_navigate_backward() {
        run_async(async {
            let entries = vec![(1, b"a".to_vec()), (2, b"b".to_vec()), (3, b"c".to_vec())];
            let mut cursor = MockBtreeCursor::new(entries);
            let cx = Cx::new();

            assert!(cursor.last(&cx).await.unwrap());
            assert_eq!(cursor.rowid(&cx).await.unwrap(), 3);

            assert!(cursor.prev(&cx).await.unwrap());
            assert_eq!(cursor.rowid(&cx).await.unwrap(), 2);

            assert!(cursor.prev(&cx).await.unwrap());
            assert_eq!(cursor.rowid(&cx).await.unwrap(), 1);

            assert!(!cursor.prev(&cx).await.unwrap());
        });
    }

    #[test]
    fn test_btree_cursor_index_seek() {
        run_async(async {
            let entries = vec![
                (1, b"alpha".to_vec()),
                (2, b"beta".to_vec()),
                (3, b"gamma".to_vec()),
            ];
            let mut cursor = MockBtreeCursor::new(entries);
            let cx = Cx::new();

            assert!(cursor.index_move_to(&cx, b"beta").await.unwrap().is_found());
            assert_eq!(cursor.rowid(&cx).await.unwrap(), 2);

            assert!(
                !cursor
                    .index_move_to(&cx, b"delta")
                    .await
                    .unwrap()
                    .is_found()
            );
        });
    }

    #[test]
    fn test_mock_cursor_index_upper_bound_skips_equal_keys() {
        run_async(async {
            let entries = vec![
                (1, b"alpha".to_vec()),
                (2, b"beta".to_vec()),
                (3, b"beta".to_vec()),
                (4, b"gamma".to_vec()),
            ];
            let mut cursor = MockBtreeCursor::new(entries);
            let cx = Cx::new();

            cursor
                .index_move_to_upper_bound(&cx, b"beta")
                .await
                .unwrap();
            assert!(!cursor.eof());
            assert_eq!(cursor.rowid(&cx).await.unwrap(), 4);

            cursor
                .index_move_to_upper_bound(&cx, b"gamma")
                .await
                .unwrap();
            assert!(cursor.eof());
        });
    }

    #[test]
    fn test_seek_result_is_found() {
        assert!(SeekResult::Found.is_found());
        assert!(!SeekResult::NotFound.is_found());
    }

    #[test]
    fn test_mock_cursor_prev_from_eof_revives() {
        run_async(async {
            let entries = vec![(1, b"one".to_vec()), (2, b"two".to_vec())];
            let mut cursor = MockBtreeCursor::new(entries);
            let cx = Cx::new();

            assert!(cursor.first(&cx).await.unwrap());
            assert!(cursor.next(&cx).await.unwrap());
            assert!(!cursor.next(&cx).await.unwrap());
            assert!(cursor.eof());

            // The real cursor revives from EOF, so the mock should too.
            assert!(cursor.prev(&cx).await.unwrap());
            assert!(!cursor.eof());
            assert_eq!(cursor.rowid(&cx).await.unwrap(), 2);
        });
    }

    /// bd-hpa5: Verify MockBtreeCursor seek positions at successor on NotFound.
    /// This mirrors real cursor semantics per SeekResult::NotFound contract.
    #[test]
    fn test_mock_cursor_seek_positions_at_successor() {
        run_async(async {
            // Entries sorted by rowid.
            let entries = vec![
                (10, b"ten".to_vec()),
                (20, b"twenty".to_vec()),
                (30, b"thirty".to_vec()),
            ];
            let mut cursor = MockBtreeCursor::new(entries);
            let cx = Cx::new();

            // Seek for rowid 15 (not found) should position at successor (rowid 20).
            let result = cursor.table_move_to(&cx, 15).await.unwrap();
            assert!(!result.is_found());
            assert!(
                !cursor.eof(),
                "cursor should not be at EOF when successor exists"
            );
            assert_eq!(
                cursor.rowid(&cx).await.unwrap(),
                20,
                "cursor should be at successor"
            );

            // Seek for rowid 5 (not found) should position at successor (rowid 10).
            let result = cursor.table_move_to(&cx, 5).await.unwrap();
            assert!(!result.is_found());
            assert!(!cursor.eof());
            assert_eq!(cursor.rowid(&cx).await.unwrap(), 10);

            // Seek for rowid 35 (not found, no successor) should position at EOF.
            let result = cursor.table_move_to(&cx, 35).await.unwrap();
            assert!(!result.is_found());
            assert!(
                cursor.eof(),
                "cursor should be at EOF when no successor exists"
            );
        });
    }

    /// bd-hpa5: Verify index_move_to positions at successor on NotFound.
    #[test]
    fn test_mock_cursor_index_seek_positions_at_successor() {
        run_async(async {
            // Entries sorted by key.
            let entries = vec![
                (1, b"alpha".to_vec()),
                (2, b"beta".to_vec()),
                (3, b"gamma".to_vec()),
            ];
            let mut cursor = MockBtreeCursor::new(entries);
            let cx = Cx::new();

            // Seek for "aaa" (before "alpha") should position at "alpha".
            let result = cursor.index_move_to(&cx, b"aaa").await.unwrap();
            assert!(!result.is_found());
            assert!(!cursor.eof());
            assert_eq!(cursor.rowid(&cx).await.unwrap(), 1);

            // Seek for "cat" (between "beta" and "gamma") should position at "gamma".
            let result = cursor.index_move_to(&cx, b"cat").await.unwrap();
            assert!(!result.is_found());
            assert!(!cursor.eof());
            assert_eq!(cursor.rowid(&cx).await.unwrap(), 3);

            // Seek for "zzz" (after all) should position at EOF.
            let result = cursor.index_move_to(&cx, b"zzz").await.unwrap();
            assert!(!result.is_found());
            assert!(cursor.eof());
        });
    }
}
