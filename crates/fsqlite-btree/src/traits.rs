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
    fn index_move_to(&mut self, cx: &Cx, key: &[u8]) -> Result<SeekResult>;

    /// Position the cursor at the given rowid in a table B-tree.
    fn table_move_to(&mut self, cx: &Cx, rowid: i64) -> Result<SeekResult>;

    // -- Navigation --

    /// Move the cursor to the first entry. Returns `false` if the tree
    /// is empty.
    fn first(&mut self, cx: &Cx) -> Result<bool>;

    /// Move the cursor to the last entry. Returns `false` if the tree
    /// is empty.
    fn last(&mut self, cx: &Cx) -> Result<bool>;

    /// Advance the cursor to the next entry. Returns `false` if there
    /// is no next entry (cursor is now at EOF).
    fn next(&mut self, cx: &Cx) -> Result<bool>;

    /// Move the cursor to the previous entry. Returns `false` if there
    /// is no previous entry.
    fn prev(&mut self, cx: &Cx) -> Result<bool>;

    // -- Mutation --

    /// Insert a key into an index B-tree.
    ///
    /// If the key already exists, it is replaced.
    fn index_insert(&mut self, cx: &Cx, key: &[u8]) -> Result<()>;

    /// Insert a key into a UNIQUE index B-tree.
    ///
    /// Before inserting, checks whether a key with the same first
    /// `n_unique_cols` fields already exists. If such a key is found
    /// **and** none of those fields are NULL, returns
    /// `FrankenError::UniqueViolation`. Multiple NULL entries are
    /// permitted (SQLite semantics).
    ///
    /// `columns_label` is used in the error message.
    fn index_insert_unique(
        &mut self,
        cx: &Cx,
        key: &[u8],
        n_unique_cols: usize,
        columns_label: &str,
    ) -> Result<()> {
        // Default: fall back to non-unique insert (MockBtreeCursor etc.).
        let _ = (n_unique_cols, columns_label);
        self.index_insert(cx, key)
    }

    /// Insert a row into a table B-tree.
    ///
    /// `rowid` is the integer key. `data` is the serialized record payload.
    fn table_insert(&mut self, cx: &Cx, rowid: i64, data: &[u8]) -> Result<()>;

    /// Delete the entry at the current cursor position.
    ///
    /// The cursor is positioned at the next entry after deletion (or EOF
    /// if the deleted entry was the last one).
    fn delete(&mut self, cx: &Cx) -> Result<()>;

    // -- Access --

    /// Read the payload at the current cursor position.
    ///
    /// For table B-trees, this is the serialized record. For index
    /// B-trees, this is the key bytes.
    fn payload(&self, cx: &Cx) -> Result<Vec<u8>>;

    /// Read the payload at the current cursor position into the provided buffer,
    /// clearing it first. This avoids allocations when repeatedly reading payloads.
    fn payload_into(&self, cx: &Cx, buf: &mut Vec<u8>) -> Result<()>;

    /// Read the rowid at the current cursor position.
    ///
    /// For table B-trees this is the integer key. For index B-trees this is
    /// extracted from the trailing field of the serialized key record.
    fn rowid(&self, cx: &Cx) -> Result<i64>;

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

#[allow(clippy::missing_errors_doc)]
impl BtreeCursorOps for MockBtreeCursor {
    fn index_move_to(&mut self, _cx: &Cx, key: &[u8]) -> Result<SeekResult> {
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

    fn table_move_to(&mut self, _cx: &Cx, rowid: i64) -> Result<SeekResult> {
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

    fn first(&mut self, _cx: &Cx) -> Result<bool> {
        if self.entries.is_empty() {
            self.at_eof = true;
            return Ok(false);
        }
        self.pos = 0;
        self.at_eof = false;
        self.current_rowid = self.entries[0].0;
        Ok(true)
    }

    fn last(&mut self, _cx: &Cx) -> Result<bool> {
        if self.entries.is_empty() {
            self.at_eof = true;
            return Ok(false);
        }
        self.pos = self.entries.len() - 1;
        self.at_eof = false;
        self.current_rowid = self.entries[self.pos].0;
        Ok(true)
    }

    fn next(&mut self, _cx: &Cx) -> Result<bool> {
        if self.pos + 1 >= self.entries.len() {
            self.at_eof = true;
            return Ok(false);
        }
        self.pos += 1;
        self.current_rowid = self.entries[self.pos].0;
        Ok(true)
    }

    fn prev(&mut self, _cx: &Cx) -> Result<bool> {
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

    fn index_insert(&mut self, _cx: &Cx, key: &[u8]) -> Result<()> {
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

    fn table_insert(&mut self, _cx: &Cx, rowid: i64, data: &[u8]) -> Result<()> {
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

    fn delete(&mut self, _cx: &Cx) -> Result<()> {
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

    fn payload(&self, _cx: &Cx) -> Result<Vec<u8>> {
        if self.at_eof {
            return Err(fsqlite_error::FrankenError::internal("cursor at EOF"));
        }
        Ok(self.entries[self.pos].1.clone())
    }

    fn payload_into(&self, _cx: &Cx, buf: &mut Vec<u8>) -> Result<()> {
        if self.at_eof {
            return Err(fsqlite_error::FrankenError::internal("cursor at EOF"));
        }
        buf.clear();
        buf.extend_from_slice(&self.entries[self.pos].1);
        Ok(())
    }

    fn rowid(&self, _cx: &Cx) -> Result<i64> {
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
    use super::*;

    #[test]
    fn test_btree_cursor_ops_sealed_mock() {
        let entries = vec![
            (1, b"alice".to_vec()),
            (2, b"bob".to_vec()),
            (3, b"charlie".to_vec()),
        ];
        let mut cursor = MockBtreeCursor::new(entries);
        let cx = Cx::new();

        // Navigate forward.
        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert_eq!(cursor.payload(&cx).unwrap(), b"alice");

        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);

        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);

        assert!(!cursor.next(&cx).unwrap());
        assert!(cursor.eof());
    }

    #[test]
    fn test_btree_cursor_seek() {
        let entries = vec![
            (10, b"ten".to_vec()),
            (20, b"twenty".to_vec()),
            (30, b"thirty".to_vec()),
        ];
        let mut cursor = MockBtreeCursor::new(entries);
        let cx = Cx::new();

        assert!(cursor.table_move_to(&cx, 20).unwrap().is_found());
        assert_eq!(cursor.rowid(&cx).unwrap(), 20);
        assert_eq!(cursor.payload(&cx).unwrap(), b"twenty");

        assert!(!cursor.table_move_to(&cx, 99).unwrap().is_found());
    }

    #[test]
    fn test_btree_cursor_insert_delete() {
        let mut cursor = MockBtreeCursor::new(vec![]);
        let cx = Cx::new();

        assert!(!cursor.first(&cx).unwrap());
        assert!(cursor.eof());

        cursor.table_insert(&cx, 1, b"hello").unwrap();
        cursor.table_insert(&cx, 2, b"world").unwrap();

        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.payload(&cx).unwrap(), b"hello");

        cursor.delete(&cx).unwrap();
        assert!(!cursor.eof());
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);
    }

    #[test]
    fn test_btree_cursor_navigate_backward() {
        let entries = vec![(1, b"a".to_vec()), (2, b"b".to_vec()), (3, b"c".to_vec())];
        let mut cursor = MockBtreeCursor::new(entries);
        let cx = Cx::new();

        assert!(cursor.last(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);

        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);

        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);

        assert!(!cursor.prev(&cx).unwrap());
    }

    #[test]
    fn test_btree_cursor_index_seek() {
        let entries = vec![
            (1, b"alpha".to_vec()),
            (2, b"beta".to_vec()),
            (3, b"gamma".to_vec()),
        ];
        let mut cursor = MockBtreeCursor::new(entries);
        let cx = Cx::new();

        assert!(cursor.index_move_to(&cx, b"beta").unwrap().is_found());
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);

        assert!(!cursor.index_move_to(&cx, b"delta").unwrap().is_found());
    }

    #[test]
    fn test_seek_result_is_found() {
        assert!(SeekResult::Found.is_found());
        assert!(!SeekResult::NotFound.is_found());
    }

    #[test]
    fn test_mock_cursor_prev_from_eof_revives() {
        let entries = vec![(1, b"one".to_vec()), (2, b"two".to_vec())];
        let mut cursor = MockBtreeCursor::new(entries);
        let cx = Cx::new();

        assert!(cursor.first(&cx).unwrap());
        assert!(cursor.next(&cx).unwrap());
        assert!(!cursor.next(&cx).unwrap());
        assert!(cursor.eof());

        // The real cursor revives from EOF, so the mock should too.
        assert!(cursor.prev(&cx).unwrap());
        assert!(!cursor.eof());
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);
    }

    /// bd-hpa5: Verify MockBtreeCursor seek positions at successor on NotFound.
    /// This mirrors real cursor semantics per SeekResult::NotFound contract.
    #[test]
    fn test_mock_cursor_seek_positions_at_successor() {
        // Entries sorted by rowid.
        let entries = vec![
            (10, b"ten".to_vec()),
            (20, b"twenty".to_vec()),
            (30, b"thirty".to_vec()),
        ];
        let mut cursor = MockBtreeCursor::new(entries);
        let cx = Cx::new();

        // Seek for rowid 15 (not found) should position at successor (rowid 20).
        let result = cursor.table_move_to(&cx, 15).unwrap();
        assert!(!result.is_found());
        assert!(
            !cursor.eof(),
            "cursor should not be at EOF when successor exists"
        );
        assert_eq!(
            cursor.rowid(&cx).unwrap(),
            20,
            "cursor should be at successor"
        );

        // Seek for rowid 5 (not found) should position at successor (rowid 10).
        let result = cursor.table_move_to(&cx, 5).unwrap();
        assert!(!result.is_found());
        assert!(!cursor.eof());
        assert_eq!(cursor.rowid(&cx).unwrap(), 10);

        // Seek for rowid 35 (not found, no successor) should position at EOF.
        let result = cursor.table_move_to(&cx, 35).unwrap();
        assert!(!result.is_found());
        assert!(
            cursor.eof(),
            "cursor should be at EOF when no successor exists"
        );
    }

    /// bd-hpa5: Verify index_move_to positions at successor on NotFound.
    #[test]
    fn test_mock_cursor_index_seek_positions_at_successor() {
        // Entries sorted by key.
        let entries = vec![
            (1, b"alpha".to_vec()),
            (2, b"beta".to_vec()),
            (3, b"gamma".to_vec()),
        ];
        let mut cursor = MockBtreeCursor::new(entries);
        let cx = Cx::new();

        // Seek for "aaa" (before "alpha") should position at "alpha".
        let result = cursor.index_move_to(&cx, b"aaa").unwrap();
        assert!(!result.is_found());
        assert!(!cursor.eof());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);

        // Seek for "cat" (between "beta" and "gamma") should position at "gamma".
        let result = cursor.index_move_to(&cx, b"cat").unwrap();
        assert!(!result.is_found());
        assert!(!cursor.eof());
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);

        // Seek for "zzz" (after all) should position at EOF.
        let result = cursor.index_move_to(&cx, b"zzz").unwrap();
        assert!(!result.is_found());
        assert!(cursor.eof());
    }
}
