//! Freelist management (§11, bd-2kvo).
//!
//! SQLite's freelist is a linked list of "trunk" pages. Each trunk page
//! stores a pointer to the next trunk page (or 0 for the end) and an
//! array of "leaf" page numbers. Leaf pages are available for reuse.
//!
//! ```text
//! Trunk page layout:
//! ┌─────────────────────────────────────┐
//! │ Next trunk page (4 bytes, BE)       │  0 = end of freelist
//! ├─────────────────────────────────────┤
//! │ Leaf page count (4 bytes, BE)       │  N
//! ├─────────────────────────────────────┤
//! │ Leaf page numbers (N × 4 bytes, BE) │
//! └─────────────────────────────────────┘
//! ```
//!
//! The maximum number of leaf entries per trunk page is
//! `(usable_size / 4) - 2` (the first 8 bytes are the next pointer
//! and count).

use crate::cell::{
    BtreePageHeader, BtreePageType, CellRef, header_offset_for_page, read_cell_pointers,
};
use crate::cursor::{PageReader, PageWriter};
use fsqlite_error::{FrankenError, Result};
use fsqlite_pager::TransactionHandle;
use fsqlite_types::cx::Cx;
use fsqlite_types::limits::MAX_PAGE_COUNT;
pub use fsqlite_types::{
    AutoVacuumMode, PTRMAP_ENTRY_SIZE_BYTES, PtrMapEntry, PtrMapType, is_ptrmap_page,
    ptrmap_data_page_count_up_to, ptrmap_entries_per_page, ptrmap_entry_offset,
    ptrmap_final_db_size, ptrmap_group_size, ptrmap_lock_byte_page, ptrmap_next_data_page,
    ptrmap_page_count_up_to, ptrmap_page_for, ptrmap_prev_data_page,
};
use fsqlite_types::{PageData, PageNumber};
use std::collections::{HashMap, HashSet};

/// Maximum leaf entries that fit on a single trunk page.
#[must_use]
pub const fn max_leaf_entries(usable_size: u32) -> u32 {
    (usable_size / 4).saturating_sub(2)
}

/// Parsed freelist trunk page.
#[derive(Debug, Clone)]
pub struct FreelistTrunk {
    /// Page number of the next trunk page, or `None` if this is the last.
    pub next_trunk: Option<PageNumber>,
    /// Page numbers of free leaf pages stored on this trunk.
    pub leaf_pages: Vec<PageNumber>,
}

impl FreelistTrunk {
    /// Parse a trunk page from raw page data.
    ///
    /// Only a zero next-trunk pointer terminates the chain. Every declared
    /// leaf entry must name a valid non-header page; silently skipping an
    /// invalid entry would lose free-page accounting and conceal corruption.
    pub fn parse(page: &[u8]) -> Result<Self> {
        if page.len() < 8 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "freelist trunk page too small".to_owned(),
            });
        }

        let next_raw = u32::from_be_bytes([page[0], page[1], page[2], page[3]]);
        let next_trunk = if next_raw == 0 {
            None
        } else {
            Some(
                PageNumber::new(next_raw)
                    .filter(|page| *page != PageNumber::ONE)
                    .ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: format!("invalid freelist next-trunk page number {next_raw}"),
                    })?,
            )
        };

        let leaf_count = u32::from_be_bytes([page[4], page[5], page[6], page[7]]);

        #[allow(clippy::cast_possible_truncation)]
        let max_entries = (page.len() as u32 / 4).saturating_sub(2);
        if leaf_count > max_entries {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "freelist trunk claims {} leaf pages but page can hold at most {}",
                    leaf_count, max_entries
                ),
            });
        }

        let mut leaf_pages = Vec::with_capacity(leaf_count as usize);
        for i in 0..leaf_count as usize {
            let offset = 8 + i * 4;
            let pgno = u32::from_be_bytes([
                page[offset],
                page[offset + 1],
                page[offset + 2],
                page[offset + 3],
            ]);
            let pn = PageNumber::new(pgno)
                .filter(|page| *page != PageNumber::ONE)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!("invalid freelist leaf page number {pgno} at entry {i}"),
                })?;
            leaf_pages.push(pn);
        }

        Ok(Self {
            next_trunk,
            leaf_pages,
        })
    }

    /// Serialize this trunk page into a page-sized buffer.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::OutOfRange`] when the destination is too small
    /// for a trunk header or when the leaf list exceeds its capacity. A
    /// serializer must never silently discard free-page accounting.
    pub fn write(&self, page: &mut [u8]) -> Result<()> {
        if page.len() < 8 {
            return Err(FrankenError::OutOfRange {
                what: "freelist trunk page size".to_owned(),
                value: format!("{} bytes (minimum 8)", page.len()),
            });
        }

        let max_entries = page.len() / 4 - 2;
        let count = u32::try_from(self.leaf_pages.len()).map_err(|_| FrankenError::OutOfRange {
            what: "freelist trunk leaf count".to_owned(),
            value: format!("{} entries", self.leaf_pages.len()),
        })?;
        if self.leaf_pages.len() > max_entries {
            return Err(FrankenError::OutOfRange {
                what: "freelist trunk leaf count".to_owned(),
                value: format!("{count} entries (capacity {max_entries})"),
            });
        }

        let next = self.next_trunk.map_or(0u32, PageNumber::get);
        page[0..4].copy_from_slice(&next.to_be_bytes());
        page[4..8].copy_from_slice(&count.to_be_bytes());

        for (i, &pgno) in self.leaf_pages.iter().enumerate() {
            let offset = 8 + i * 4;
            page[offset..offset + 4].copy_from_slice(&pgno.get().to_be_bytes());
        }
        Ok(())
    }
}

/// In-memory freelist manager.
///
/// Tracks free pages and provides allocation/deallocation. The freelist
/// state is read from the database on open and maintained in memory during
/// a transaction.
#[derive(Debug, Clone)]
pub struct Freelist {
    /// All free page numbers available for allocation.
    free_pages: Vec<PageNumber>,
    /// Total number of pages in the database file (for extending).
    db_page_count: u32,
    /// The page size of the database.
    page_size: u32,
}

impl Freelist {
    /// Create a new freelist with no free pages.
    #[must_use]
    pub fn new(db_page_count: u32, page_size: u32) -> Self {
        Self {
            free_pages: Vec::new(),
            db_page_count,
            page_size,
        }
    }

    /// Create a freelist pre-populated with free pages.
    #[must_use]
    pub fn with_pages(pages: Vec<PageNumber>, db_page_count: u32, page_size: u32) -> Self {
        Self {
            free_pages: pages,
            db_page_count,
            page_size,
        }
    }

    /// Number of free pages available.
    #[must_use]
    pub fn free_count(&self) -> usize {
        self.free_pages.len()
    }

    /// Allocate a page from the freelist.
    ///
    /// Prefers pages from the freelist. If the freelist is empty,
    /// extends the database file by one page. Skips any freelist
    /// entries that reference pages beyond the current database size
    /// (defensive against stale/corrupt freelist state).
    pub fn allocate(&mut self) -> Result<PageNumber> {
        while let Some(pgno) = self.free_pages.pop() {
            if pgno.get() <= self.db_page_count {
                return Ok(pgno);
            }
            // Silently skip freelist entries that point beyond the database
            // file — these indicate stale or corrupt freelist state and
            // would cause "freelist leaf page N exceeds db_size M" errors
            // if returned to callers.
        }
        // Extend the database file.
        if self.db_page_count >= MAX_PAGE_COUNT {
            return Err(FrankenError::DatabaseFull);
        }

        let mut next = self.db_page_count + 1;
        let pending_byte_page = (0x4000_0000 / self.page_size) + 1;

        // Skip the pending byte page (§11.14)
        if next == pending_byte_page {
            if next >= MAX_PAGE_COUNT {
                return Err(FrankenError::DatabaseFull);
            }
            next += 1;
        }

        self.db_page_count = next;
        PageNumber::new(self.db_page_count).ok_or(FrankenError::DatabaseFull)
    }

    /// Return a page to the freelist.
    ///
    /// Rejects pages that reference beyond the current database size,
    /// which would corrupt the freelist on flush.
    pub fn deallocate(&mut self, page: PageNumber) {
        if page.get() <= self.db_page_count {
            self.free_pages.push(page);
        }
    }

    /// Current total page count (including allocated pages beyond original).
    #[must_use]
    pub fn db_page_count(&self) -> u32 {
        self.db_page_count
    }

    /// Get a snapshot of all free pages.
    pub fn free_pages(&self) -> &[PageNumber] {
        &self.free_pages
    }
}

// Pointer-map types and layout arithmetic (`PtrMapType`, `PtrMapEntry`,
// `is_ptrmap_page`, `ptrmap_page_for`, `ptrmap_entry_offset`, `ptrmap_final_db_size`,
// etc.) live in `fsqlite_types` and are re-exported above so `fsqlite-pager`,
// `fsqlite-btree`, and `fsqlite-core` share one single source of truth (bd-1q134).

/// Collect every direct child page referenced by a B-tree page (`page_no`):
/// - interior `right_child` and every cell's `left_child` as [`PtrMapType::Btree`],
/// - every cell's first overflow page as [`PtrMapType::Overflow1`].
pub fn ptrmap_collect_btree_page_children(
    page_no: PageNumber,
    page_bytes: &[u8],
    usable_size: u32,
) -> Result<Vec<(PageNumber, PtrMapType)>> {
    let hdr_off = header_offset_for_page(page_no);
    if page_bytes.len() < hdr_off + 8 {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "b-tree page {} too short ({} bytes) for header",
                page_no.get(),
                page_bytes.len()
            ),
        });
    }
    let header = BtreePageHeader::parse(page_bytes, hdr_off)?;
    let cell_ptrs = read_cell_pointers(page_bytes, &header, hdr_off)?;
    let mut children = Vec::new();
    if let Some(right_child) = header.right_child {
        children.push((right_child, PtrMapType::Btree));
    }
    let is_interior = header.page_type.is_interior();
    let has_payload = matches!(
        header.page_type,
        BtreePageType::LeafTable | BtreePageType::LeafIndex | BtreePageType::InteriorIndex
    );
    for cell_ptr in cell_ptrs {
        let cell_off = usize::from(cell_ptr);
        if is_interior {
            if cell_off + 4 > page_bytes.len() {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "interior cell pointer {} out of bounds on page {}",
                        cell_off,
                        page_no.get()
                    ),
                });
            }
            let left_raw = u32::from_be_bytes([
                page_bytes[cell_off],
                page_bytes[cell_off + 1],
                page_bytes[cell_off + 2],
                page_bytes[cell_off + 3],
            ]);
            let left_child =
                PageNumber::new(left_raw).ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "zero left-child pointer in cell at {} on page {}",
                        cell_off,
                        page_no.get()
                    ),
                })?;
            children.push((left_child, PtrMapType::Btree));
        }
        if has_payload {
            let cell = CellRef::parse(page_bytes, cell_off, header.page_type, usable_size)?;
            if let Some(ovf) = cell.overflow_page {
                children.push((ovf, PtrMapType::Overflow1));
            }
        }
    }
    Ok(children)
}

/// Extract the next overflow page pointer (bytes `0..4`) from an overflow page.
#[must_use]
pub fn ptrmap_collect_overflow_next(page_bytes: &[u8]) -> Option<PageNumber> {
    if page_bytes.len() < 4 {
        return None;
    }
    PageNumber::new(u32::from_be_bytes([
        page_bytes[0],
        page_bytes[1],
        page_bytes[2],
        page_bytes[3],
    ]))
}

/// Rewrite a parent page's 4-byte child reference from `old_child` to `new_child`.
///
/// Fails closed with [`FrankenError::DatabaseCorrupt`] if `old_child` is not
/// referenced by `parent_bytes` in the role declared by `child_kind`.
pub fn ptrmap_rewrite_parent_pointer(
    parent_page_no: PageNumber,
    parent_bytes: &mut [u8],
    child_kind: PtrMapType,
    old_child: PageNumber,
    new_child: PageNumber,
    usable_size: u32,
) -> Result<()> {
    let old_raw = old_child.get();
    let new_be = new_child.get().to_be_bytes();
    match child_kind {
        PtrMapType::Overflow2 => {
            if parent_bytes.len() < 4 {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "overflow parent page {} too short ({} bytes)",
                        parent_page_no.get(),
                        parent_bytes.len()
                    ),
                });
            }
            let cur = u32::from_be_bytes([
                parent_bytes[0],
                parent_bytes[1],
                parent_bytes[2],
                parent_bytes[3],
            ]);
            if cur != old_raw {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "overflow parent page {} points to {}, expected {} during relocation",
                        parent_page_no.get(),
                        cur,
                        old_raw
                    ),
                });
            }
            parent_bytes[0..4].copy_from_slice(&new_be);
            Ok(())
        }
        PtrMapType::Btree => {
            let hdr_off = header_offset_for_page(parent_page_no);
            let header = BtreePageHeader::parse(parent_bytes, hdr_off)?;
            if !header.page_type.is_interior() {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "pointer map claims page {} is B-tree parent of {}, but page type is {:?}",
                        parent_page_no.get(),
                        old_raw,
                        header.page_type
                    ),
                });
            }
            if header.right_child == Some(old_child) {
                parent_bytes[hdr_off + 8..hdr_off + 12].copy_from_slice(&new_be);
                return Ok(());
            }
            let cell_ptrs = read_cell_pointers(parent_bytes, &header, hdr_off)?;
            for cell_ptr in cell_ptrs {
                let off = usize::from(cell_ptr);
                if off + 4 <= parent_bytes.len()
                    && u32::from_be_bytes([
                        parent_bytes[off],
                        parent_bytes[off + 1],
                        parent_bytes[off + 2],
                        parent_bytes[off + 3],
                    ]) == old_raw
                {
                    parent_bytes[off..off + 4].copy_from_slice(&new_be);
                    return Ok(());
                }
            }
            Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "parent interior page {} does not reference child {} during relocation",
                    parent_page_no.get(),
                    old_raw
                ),
            })
        }
        PtrMapType::Overflow1 => {
            let hdr_off = header_offset_for_page(parent_page_no);
            let header = BtreePageHeader::parse(parent_bytes, hdr_off)?;
            let cell_ptrs = read_cell_pointers(parent_bytes, &header, hdr_off)?;
            for cell_ptr in cell_ptrs {
                let cell_off = usize::from(cell_ptr);
                let cell = CellRef::parse(parent_bytes, cell_off, header.page_type, usable_size)?;
                if cell.overflow_page == Some(old_child) {
                    let ovf_off = cell.payload_offset + cell.local_size as usize;
                    if ovf_off + 4 > parent_bytes.len() {
                        return Err(FrankenError::DatabaseCorrupt {
                            detail: format!(
                                "overflow pointer offset {} out of bounds on page {}",
                                ovf_off,
                                parent_page_no.get()
                            ),
                        });
                    }
                    parent_bytes[ovf_off..ovf_off + 4].copy_from_slice(&new_be);
                    return Ok(());
                }
            }
            Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "parent b-tree page {} does not reference overflow1 page {} during relocation",
                    parent_page_no.get(),
                    old_raw
                ),
            })
        }
        PtrMapType::RootPage | PtrMapType::FreePage => Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "cannot rewrite parent pointer for non-child pointer-map kind {:?}",
                child_kind
            ),
        }),
    }
}

/// Synchronize pointer-map entries for all direct B-tree children and first
/// overflow pages referenced by a B-tree page just written on `txn`.
pub async fn ptrmap_sync_btree_page_children_on_txn<T: TransactionHandle + ?Sized>(
    cx: &Cx,
    txn: &mut T,
    page_no: PageNumber,
    page_bytes: &[u8],
    usable_size: u32,
) -> Result<()> {
    let hdr_off = header_offset_for_page(page_no);
    if page_bytes.len() < hdr_off + 8 {
        return Ok(());
    }
    if BtreePageType::from_flag(page_bytes[hdr_off]).is_none() {
        return Ok(());
    }
    let children = ptrmap_collect_btree_page_children(page_no, page_bytes, usable_size)?;
    for (child_pg, child_kind) in children {
        txn.write_ptrmap_entry(
            cx,
            child_pg,
            PtrMapEntry {
                kind: child_kind,
                parent: Some(page_no),
            },
        )
        .await?;
    }
    Ok(())
}

/// Relocate a live non-root page (`src`) to destination slot (`dst`).
///
/// Updates the parent pointer, destination pointer-map entry, and all direct
/// child pointer-map backpointers while preserving global pointer-map invariants.
pub async fn ptrmap_relocate_page<P: PageReader + PageWriter>(
    cx: &Cx,
    io: &mut P,
    src: PageNumber,
    dst: PageNumber,
    usable_size: u32,
) -> Result<()> {
    if src == dst {
        return Ok(());
    }
    let entry = io.read_ptrmap_entry(cx, src).await?;
    let parent = match entry.kind {
        PtrMapType::Btree | PtrMapType::Overflow1 | PtrMapType::Overflow2 => {
            entry.parent.ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "pointer-map entry for page {} ({:?}) missing parent",
                    src.get(),
                    entry.kind
                ),
            })?
        }
        PtrMapType::RootPage | PtrMapType::FreePage => {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "refusing to relocate page {} with pointer-map type {:?}",
                    src.get(),
                    entry.kind
                ),
            });
        }
    };

    let src_bytes = io.read_page(cx, src).await?;
    let mut parent_bytes = io.read_page(cx, parent).await?;
    ptrmap_rewrite_parent_pointer(parent, &mut parent_bytes, entry.kind, src, dst, usable_size)?;
    io.write_raw_page_data(cx, parent, PageData::from_vec(parent_bytes))
        .await?;

    // Collect direct children of `src` before moving `src_bytes` into `dst`.
    let child_updates: Vec<(PageNumber, PtrMapType)> = match entry.kind {
        PtrMapType::Btree => ptrmap_collect_btree_page_children(src, &src_bytes, usable_size)?,
        PtrMapType::Overflow1 | PtrMapType::Overflow2 => ptrmap_collect_overflow_next(&src_bytes)
            .map(|next| vec![(next, PtrMapType::Overflow2)])
            .unwrap_or_default(),
        PtrMapType::RootPage | PtrMapType::FreePage => Vec::new(),
    };

    io.write_raw_page_data(cx, dst, PageData::from_vec(src_bytes))
        .await?;
    io.write_ptrmap_entry(cx, dst, entry).await?;
    for (child_pg, child_kind) in child_updates {
        io.write_ptrmap_entry(
            cx,
            child_pg,
            PtrMapEntry {
                kind: child_kind,
                parent: Some(dst),
            },
        )
        .await?;
    }

    tracing::info!(
        from_page = src.get(),
        to_page = dst.get(),
        kind = ?entry.kind,
        parent = parent.get(),
        "auto_vacuum relocated page during truncation"
    );
    Ok(())
}

/// Relocate a B-tree root page (`old_root`) to `new_root`, updating its
/// pointer-map entry to `(RootPage, None)` and updating all direct B-tree
/// children & first overflow pages to point to `new_root`.
///
/// The caller is responsible for updating `sqlite_master` and in-memory schema
/// references from `old_root` to `new_root` and freeing `old_root`.
pub async fn ptrmap_relocate_root_page<P: PageReader + PageWriter>(
    cx: &Cx,
    io: &mut P,
    old_root: PageNumber,
    new_root: PageNumber,
    usable_size: u32,
) -> Result<()> {
    if old_root == new_root {
        return Ok(());
    }
    let root_bytes = io.read_page(cx, old_root).await?;
    let children = ptrmap_collect_btree_page_children(old_root, &root_bytes, usable_size)?;
    io.write_raw_page_data(cx, new_root, PageData::from_vec(root_bytes))
        .await?;
    io.write_ptrmap_entry(
        cx,
        new_root,
        PtrMapEntry {
            kind: PtrMapType::RootPage,
            parent: None,
        },
    )
    .await?;
    for (child_pg, child_kind) in children {
        io.write_ptrmap_entry(
            cx,
            child_pg,
            PtrMapEntry {
                kind: child_kind,
                parent: Some(new_root),
            },
        )
        .await?;
    }
    tracing::info!(
        from_page = old_root.get(),
        to_page = new_root.get(),
        kind = "RootPage",
        "auto_vacuum relocated root page during drop compaction"
    );
    Ok(())
}

/// Outcome of an auto-vacuum (`FULL` or `INCREMENTAL`) compaction pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoVacuumOutcome {
    /// Post-compaction database size in pages (`<= orig_db_size`).
    pub new_db_size: u32,
    /// Number of pages reclaimed/truncated from the tail of the database.
    pub pages_reclaimed: u32,
    /// Remaining un-reclaimed free pages `<= new_db_size`.
    pub remaining_freelist: Vec<PageNumber>,
    /// Source page numbers that were relocated away from the tail.
    pub relocated_src_pages: Vec<PageNumber>,
}

/// Execute SQLite's `auto_vacuum = FULL` compaction pass over `io`, relocating
/// every live page above `final_db_size` into a free slot `<= final_db_size`.
pub async fn ptrmap_compact_full<P: PageReader + PageWriter>(
    cx: &Cx,
    io: &mut P,
    orig_db_size: u32,
    largest_root_page: u32,
    free_pages: &[PageNumber],
    usable_size: u32,
    page_size: u32,
) -> Result<AutoVacuumOutcome> {
    let lock_byte = ptrmap_lock_byte_page(page_size);
    let mut free_set: HashSet<u32> = HashSet::with_capacity(free_pages.len());
    for &pg in free_pages {
        let raw = pg.get();
        if raw >= 3
            && raw <= orig_db_size
            && raw != lock_byte
            && !is_ptrmap_page(pg, usable_size, page_size)
        {
            free_set.insert(raw);
        }
    }
    if free_set.is_empty() || orig_db_size <= 1 {
        return Ok(AutoVacuumOutcome {
            new_db_size: orig_db_size,
            pages_reclaimed: 0,
            remaining_freelist: Vec::new(),
            relocated_src_pages: Vec::new(),
        });
    }

    let final_db_size = ptrmap_final_db_size(
        orig_db_size,
        u32::try_from(free_set.len()).unwrap_or(u32::MAX),
        largest_root_page,
        usable_size,
        page_size,
    );
    if final_db_size >= orig_db_size {
        let mut remaining: Vec<PageNumber> =
            free_set.into_iter().filter_map(PageNumber::new).collect();
        remaining.sort_unstable_by_key(|p| std::cmp::Reverse(p.get()));
        return Ok(AutoVacuumOutcome {
            new_db_size: orig_db_size,
            pages_reclaimed: 0,
            remaining_freelist: remaining,
            relocated_src_pages: Vec::new(),
        });
    }

    // Destination slots: free pages <= final_db_size (lowest first).
    let mut free_slots: Vec<PageNumber> = free_set
        .iter()
        .copied()
        .filter(|&raw| raw <= final_db_size)
        .filter_map(PageNumber::new)
        .collect();
    free_slots.sort_unstable_by_key(|p| p.get());

    // Live pages strictly above final_db_size (excluding ptrmap, lock-byte, and free pages).
    let mut live_tail: Vec<PageNumber> = Vec::new();
    for raw in final_db_size.saturating_add(1)..=orig_db_size {
        if raw == lock_byte || free_set.contains(&raw) {
            continue;
        }
        if let Some(pg) = PageNumber::new(raw)
            && !is_ptrmap_page(pg, usable_size, page_size)
        {
            live_tail.push(pg);
        }
    }

    if live_tail.len() > free_slots.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "auto_vacuum FULL invariant violation: {} live tail pages exceed {} free slots (orig={} final={} lrp={})",
                live_tail.len(),
                free_slots.len(),
                orig_db_size,
                final_db_size,
                largest_root_page
            ),
        });
    }

    let mut relocated_src_pages = Vec::with_capacity(live_tail.len());
    for (src, dst) in live_tail.iter().copied().zip(free_slots.iter().copied()) {
        ptrmap_relocate_page(cx, io, src, dst, usable_size).await?;
        relocated_src_pages.push(src);
    }

    let mut remaining_freelist: Vec<PageNumber> = free_slots[live_tail.len()..].to_vec();
    remaining_freelist.sort_unstable_by_key(|p| std::cmp::Reverse(p.get()));

    Ok(AutoVacuumOutcome {
        new_db_size: final_db_size,
        pages_reclaimed: orig_db_size.saturating_sub(final_db_size),
        remaining_freelist,
        relocated_src_pages,
    })
}

/// Execute up to `max_pages` (`0` = all) steps of SQLite's `PRAGMA incremental_vacuum`.
#[allow(clippy::too_many_arguments)]
pub async fn ptrmap_compact_incremental<P: PageReader + PageWriter>(
    cx: &Cx,
    io: &mut P,
    orig_db_size: u32,
    largest_root_page: u32,
    free_pages: &[PageNumber],
    max_pages: u32,
    usable_size: u32,
    page_size: u32,
) -> Result<AutoVacuumOutcome> {
    let lock_byte = ptrmap_lock_byte_page(page_size);
    let mut free_set: HashSet<u32> = HashSet::with_capacity(free_pages.len());
    for &pg in free_pages {
        let raw = pg.get();
        if raw >= 3
            && raw <= orig_db_size
            && raw != lock_byte
            && !is_ptrmap_page(pg, usable_size, page_size)
        {
            free_set.insert(raw);
        }
    }
    let floor = largest_root_page.max(1);
    let mut cur_db_size = orig_db_size;
    let mut steps_done = 0_u32;
    let mut relocated_src_pages = Vec::new();

    while !free_set.is_empty() && cur_db_size > floor && (max_pages == 0 || steps_done < max_pages)
    {
        // Strip any trailing pointer-map or lock-byte pages cleanly.
        while cur_db_size > floor {
            let is_special = cur_db_size == lock_byte
                || PageNumber::new(cur_db_size)
                    .is_some_and(|pg| is_ptrmap_page(pg, usable_size, page_size));
            if is_special {
                cur_db_size -= 1;
            } else {
                break;
            }
        }
        if cur_db_size <= floor {
            break;
        }
        if !free_set.remove(&cur_db_size) {
            // Find a free slot strictly below `cur_db_size` (lowest first).
            let Some(&dst_raw) = free_set.iter().filter(|&&p| p < cur_db_size).min() else {
                break;
            };
            let Some(src) = PageNumber::new(cur_db_size) else {
                break;
            };
            let Some(dst) = PageNumber::new(dst_raw) else {
                break;
            };
            // Refuse to move a RootPage even if largest_root_page was stale.
            let entry = io.read_ptrmap_entry(cx, src).await?;
            if entry.kind == PtrMapType::RootPage {
                break;
            }
            ptrmap_relocate_page(cx, io, src, dst, usable_size).await?;
            free_set.remove(&dst_raw);
            relocated_src_pages.push(src);
        }
        cur_db_size -= 1;
        steps_done += 1;
    }

    // Strip any trailing pointer-map or lock-byte pages exposed by the final step
    // (for example, shrinking page 3 down to page 1 strips page 2 ptrmap too).
    while cur_db_size > floor {
        let is_special = cur_db_size == lock_byte
            || PageNumber::new(cur_db_size)
                .is_some_and(|pg| is_ptrmap_page(pg, usable_size, page_size));
        if is_special {
            cur_db_size -= 1;
        } else {
            break;
        }
    }

    let mut remaining_freelist: Vec<PageNumber> = free_set
        .into_iter()
        .filter(|&raw| raw <= cur_db_size)
        .filter_map(PageNumber::new)
        .collect();
    remaining_freelist.sort_unstable_by_key(|p| std::cmp::Reverse(p.get()));

    Ok(AutoVacuumOutcome {
        new_db_size: cur_db_size,
        pages_reclaimed: orig_db_size.saturating_sub(cur_db_size),
        remaining_freelist,
        relocated_src_pages,
    })
}

/// Independently verify EVERY pointer-map entry across `3..=db_size` by walking
/// all B-trees (`PageNumber::ONE` plus `root_pages`), all overflow chains, and
/// `freelist_pages`.
pub async fn ptrmap_verify_all_invariants<P: PageReader + PageWriter>(
    cx: &Cx,
    io: &P,
    root_pages: &[PageNumber],
    freelist_pages: &[PageNumber],
    db_size: u32,
    usable_size: u32,
    page_size: u32,
) -> Result<()> {
    let lock_byte = ptrmap_lock_byte_page(page_size);
    let mut expected: HashMap<u32, PtrMapEntry> = HashMap::new();
    let mut btree_stack: Vec<PageNumber> = Vec::new();
    let mut visited_btree: HashSet<u32> = HashSet::new();

    // Page 1 (`sqlite_master`) may exist when verifying full databases.
    if io.read_page(cx, PageNumber::ONE).await.is_ok() {
        btree_stack.push(PageNumber::ONE);
    }
    for &root in root_pages {
        if root != PageNumber::ONE {
            if expected
                .insert(
                    root.get(),
                    PtrMapEntry {
                        kind: PtrMapType::RootPage,
                        parent: None,
                    },
                )
                .is_some()
            {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("duplicate root page {} in ptrmap verification", root.get()),
                });
            }
            btree_stack.push(root);
        }
    }

    while let Some(pgno) = btree_stack.pop() {
        if !visited_btree.insert(pgno.get()) {
            continue;
        }
        let page_bytes = io.read_page(cx, pgno).await?;
        let hdr_off = header_offset_for_page(pgno);
        if page_bytes.len() < hdr_off + 8 || BtreePageType::from_flag(page_bytes[hdr_off]).is_none()
        {
            continue;
        }
        let children = ptrmap_collect_btree_page_children(pgno, &page_bytes, usable_size)?;
        for (child_pg, child_kind) in children {
            let entry = PtrMapEntry {
                kind: child_kind,
                parent: Some(pgno),
            };
            if let Some(prev) = expected.insert(child_pg.get(), entry) {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "page {} claimed twice during ptrmap walk: prev={:?} new={:?}",
                        child_pg.get(),
                        prev,
                        entry
                    ),
                });
            }
            match child_kind {
                PtrMapType::Btree => btree_stack.push(child_pg),
                PtrMapType::Overflow1 => {
                    let mut cur = child_pg;
                    let mut steps = 0_usize;
                    while let Some(next_ovf) = {
                        let ovf_bytes = io.read_page(cx, cur).await?;
                        ptrmap_collect_overflow_next(&ovf_bytes)
                    } {
                        steps += 1;
                        if steps > crate::overflow::MAX_OVERFLOW_CHAIN {
                            return Err(FrankenError::DatabaseCorrupt {
                                detail: format!(
                                    "overflow cycle detected from page {}",
                                    child_pg.get()
                                ),
                            });
                        }
                        let ovf_entry = PtrMapEntry {
                            kind: PtrMapType::Overflow2,
                            parent: Some(cur),
                        };
                        if let Some(prev) = expected.insert(next_ovf.get(), ovf_entry) {
                            return Err(FrankenError::DatabaseCorrupt {
                                detail: format!(
                                    "overflow page {} claimed twice: prev={:?} new={:?}",
                                    next_ovf.get(),
                                    prev,
                                    ovf_entry
                                ),
                            });
                        }
                        cur = next_ovf;
                    }
                }
                _ => {}
            }
        }
    }

    for &free_pg in freelist_pages {
        if free_pg.get() >= 3 && free_pg.get() <= db_size {
            let entry = PtrMapEntry {
                kind: PtrMapType::FreePage,
                parent: None,
            };
            if let Some(prev) = expected.insert(free_pg.get(), entry) {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("free page {} also claimed as {:?}", free_pg.get(), prev),
                });
            }
        }
    }

    for raw in 3..=db_size {
        if raw == lock_byte {
            continue;
        }
        let Some(pg) = PageNumber::new(raw) else {
            continue;
        };
        if is_ptrmap_page(pg, usable_size, page_size) {
            continue;
        }
        let exp = expected
            .get(&raw)
            .copied()
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "page {raw} is unowned during ptrmap verification (db_size={db_size})"
                ),
            })?;
        let actual = io.read_ptrmap_entry(cx, pg).await?;
        if actual != exp {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("Bad ptr map entry key={raw} expected={exp:?} got={actual:?}"),
            });
        }
    }

    Ok(())
}

/// Read the entire freelist from the database, starting from the first
/// trunk page.
///
/// `first_trunk` is the page number of the first freelist trunk page
/// (from the database header, bytes 32-35).
/// `read_page` reads a raw page by page number.
///
/// Returns all free page numbers collected from the freelist.
///
/// Rejects duplicate references across both trunk and leaf pages before a
/// caller can allocate the same physical page twice. Trunk cycles are caught
/// before rereading the repeated page, not merely by the chain-length bound.
pub fn read_freelist<F>(
    first_trunk: Option<PageNumber>,
    read_page: &mut F,
) -> Result<Vec<PageNumber>>
where
    F: FnMut(PageNumber) -> Result<Vec<u8>>,
{
    let mut all_pages = Vec::new();
    let mut seen = HashSet::new();
    let mut current = first_trunk;
    let mut visited = 0usize;

    while let Some(trunk_pgno) = current {
        if trunk_pgno == PageNumber::ONE {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "database header page used as freelist trunk".to_owned(),
            });
        }
        seen.try_reserve(1).map_err(|_| FrankenError::OutOfMemory)?;
        if !seen.insert(trunk_pgno) {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("duplicate page {trunk_pgno} in freelist"),
            });
        }
        visited += 1;
        if visited > 1_000_000 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "freelist trunk chain too long (possible cycle)".to_owned(),
            });
        }

        let page_data = read_page(trunk_pgno)?;
        let trunk = FreelistTrunk::parse(&page_data)?;

        seen.try_reserve(trunk.leaf_pages.len())
            .map_err(|_| FrankenError::OutOfMemory)?;
        all_pages
            .try_reserve(trunk.leaf_pages.len() + 1)
            .map_err(|_| FrankenError::OutOfMemory)?;
        // The trunk page itself is also free (it's part of the freelist).
        all_pages.push(trunk_pgno);
        for &leaf_pgno in &trunk.leaf_pages {
            if !seen.insert(leaf_pgno) {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("duplicate page {leaf_pgno} in freelist"),
                });
            }
            all_pages.push(leaf_pgno);
        }

        current = trunk.next_trunk;
    }

    Ok(all_pages)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::similar_names)]
mod tests {
    use super::*;
    use crate::traits::BtreeCursorOps;
    use std::collections::HashMap;

    const PENDING_BYTE_OFFSET: u32 = 0x4000_0000;

    #[test]
    fn test_max_leaf_entries() {
        // 4096 / 4 - 2 = 1022
        assert_eq!(max_leaf_entries(4096), 1022);
        // 512 / 4 - 2 = 126
        assert_eq!(max_leaf_entries(512), 126);
    }

    #[test]
    fn test_max_leaf_entries_saturates_on_tiny_pages() {
        // The formula is (usable/4) - 2, using saturating_sub so that
        // pathologically small usable sizes yield 0 leaf entries rather than
        // underflowing (which on u32 would wrap to a huge count and later
        // overflow the trunk page). test_max_leaf_entries only covers the
        // ordinary 512/4096 cases.
        assert_eq!(max_leaf_entries(0), 0);
        assert_eq!(max_leaf_entries(4), 0); // 4/4 = 1, 1 - 2 saturates to 0
        assert_eq!(max_leaf_entries(8), 0); // 8/4 = 2, 2 - 2 = 0
        assert_eq!(max_leaf_entries(11), 0); // 11/4 = 2, 2 - 2 = 0
        assert_eq!(max_leaf_entries(12), 1); // 12/4 = 3, 3 - 2 = 1 (first nonzero)
    }

    #[test]
    fn test_trunk_parse_write_roundtrip() {
        let trunk = FreelistTrunk {
            next_trunk: Some(PageNumber::new(10).unwrap()),
            leaf_pages: vec![
                PageNumber::new(20).unwrap(),
                PageNumber::new(30).unwrap(),
                PageNumber::new(40).unwrap(),
            ],
        };

        let mut page = vec![0u8; 4096];
        trunk.write(&mut page).unwrap();

        let parsed = FreelistTrunk::parse(&page).unwrap();
        assert_eq!(parsed.next_trunk.unwrap().get(), 10);
        assert_eq!(parsed.leaf_pages.len(), 3);
        assert_eq!(parsed.leaf_pages[0].get(), 20);
        assert_eq!(parsed.leaf_pages[1].get(), 30);
        assert_eq!(parsed.leaf_pages[2].get(), 40);
    }

    #[test]
    fn test_trunk_parse_last_in_chain() {
        let trunk = FreelistTrunk {
            next_trunk: None,
            leaf_pages: vec![PageNumber::new(5).unwrap()],
        };

        let mut page = vec![0u8; 4096];
        trunk.write(&mut page).unwrap();

        let parsed = FreelistTrunk::parse(&page).unwrap();
        assert!(parsed.next_trunk.is_none());
        assert_eq!(parsed.leaf_pages.len(), 1);
    }

    #[test]
    fn test_trunk_parse_empty() {
        let trunk = FreelistTrunk {
            next_trunk: None,
            leaf_pages: vec![],
        };

        let mut page = vec![0u8; 4096];
        trunk.write(&mut page).unwrap();

        let parsed = FreelistTrunk::parse(&page).unwrap();
        assert!(parsed.next_trunk.is_none());
        assert!(parsed.leaf_pages.is_empty());
    }

    #[test]
    fn test_trunk_parse_truncated() {
        let page = vec![0u8; 4];
        let result = FreelistTrunk::parse(&page);
        assert!(result.is_err());
    }

    #[test]
    fn test_trunk_write_must_not_silently_drop_overflow_leaves() {
        let trunk = FreelistTrunk {
            next_trunk: None,
            leaf_pages: vec![
                PageNumber::new(10).unwrap(),
                PageNumber::new(20).unwrap(),
                PageNumber::new(30).unwrap(),
            ],
        };
        let mut page = vec![0xCC; 16];

        let error = trunk
            .write(&mut page)
            .expect_err("a 16-byte trunk can hold only two leaf entries");
        assert!(
            matches!(error, FrankenError::OutOfRange { .. }),
            "expected a typed capacity error, got {error}"
        );
        assert_eq!(
            page,
            vec![0xCC; 16],
            "a rejected serialization must leave its destination untouched"
        );

        let mut short_page = vec![0xDD; 7];
        assert!(matches!(
            trunk.write(&mut short_page),
            Err(FrankenError::OutOfRange { .. })
        ));
        assert_eq!(short_page, vec![0xDD; 7]);
    }

    #[test]
    fn test_trunk_parse_rejects_over_capacity_and_invalid_leaf_entries() {
        // A trunk page whose declared leaf_count exceeds what the page can hold
        // is corruption. A 16-byte page holds max (16/4 - 2) = 2 leaf entries.
        let mut over = vec![0u8; 16];
        over[4..8].copy_from_slice(&3u32.to_be_bytes()); // claims 3 > 2
        assert!(
            matches!(
                FreelistTrunk::parse(&over),
                Err(FrankenError::DatabaseCorrupt { .. })
            ),
            "leaf_count over capacity must be rejected"
        );

        // Invalid entries inside the declared count are corruption, not
        // padding. In particular, never silently turn three entries into two.
        let mut page = vec![0u8; 4096];
        page[0..4].copy_from_slice(&0u32.to_be_bytes()); // no next trunk
        page[4..8].copy_from_slice(&3u32.to_be_bytes()); // 3 declared entries
        page[8..12].copy_from_slice(&20u32.to_be_bytes());
        page[16..20].copy_from_slice(&40u32.to_be_bytes());
        for invalid in [0u32, 1, u32::MAX] {
            page[12..16].copy_from_slice(&invalid.to_be_bytes());
            let error = FreelistTrunk::parse(&page).unwrap_err();
            assert!(matches!(error, FrankenError::DatabaseCorrupt { .. }));
            assert!(
                error
                    .to_string()
                    .contains("invalid freelist leaf page number")
            );
        }
    }

    #[test]
    fn test_trunk_parse_distinguishes_terminator_from_invalid_next_page() {
        let mut page = [0u8; 8];
        for invalid in [1u32, u32::MAX] {
            page[..4].copy_from_slice(&invalid.to_be_bytes());
            let error = FreelistTrunk::parse(&page).unwrap_err();
            assert!(matches!(error, FrankenError::DatabaseCorrupt { .. }));
            assert!(
                error
                    .to_string()
                    .contains("invalid freelist next-trunk page number")
            );
        }
        for valid in [0u32, 2, u32::MAX - 1] {
            page[..4].copy_from_slice(&valid.to_be_bytes());
            let parsed = FreelistTrunk::parse(&page).unwrap();
            assert_eq!(parsed.next_trunk, PageNumber::new(valid));
            assert!(parsed.leaf_pages.is_empty());
        }
    }

    #[test]
    fn test_trunk_parse_keeps_valid_boundaries_and_ignores_unused_slots() {
        // Bytes beyond leaf_count are unused, even if they resemble invalid
        // page numbers. Only the two declared entries must be validated.
        let mut page = [0xFFu8; 32];
        page[..4].copy_from_slice(&0u32.to_be_bytes());
        page[4..8].copy_from_slice(&2u32.to_be_bytes());
        page[8..12].copy_from_slice(&2u32.to_be_bytes());
        page[12..16].copy_from_slice(&(u32::MAX - 1).to_be_bytes());
        let parsed = FreelistTrunk::parse(&page).unwrap();
        assert!(parsed.next_trunk.is_none());
        assert_eq!(
            parsed.leaf_pages,
            vec![
                PageNumber::new(2).unwrap(),
                PageNumber::new(u32::MAX - 1).unwrap()
            ]
        );
    }

    #[test]
    fn test_freelist_allocate_from_free() {
        let mut fl = Freelist::with_pages(
            vec![PageNumber::new(10).unwrap(), PageNumber::new(20).unwrap()],
            100,
            4096,
        );

        assert_eq!(fl.free_count(), 2);
        let p1 = fl.allocate().unwrap();
        assert_eq!(p1.get(), 20); // LIFO order.
        assert_eq!(fl.free_count(), 1);
        let p2 = fl.allocate().unwrap();
        assert_eq!(p2.get(), 10);
        assert_eq!(fl.free_count(), 0);
    }

    #[test]
    fn test_freelist_allocate_extends_db() {
        let mut fl = Freelist::new(100, 4096);
        assert_eq!(fl.free_count(), 0);

        let p = fl.allocate().unwrap();
        assert_eq!(p.get(), 101);
        assert_eq!(fl.db_page_count(), 101);

        let p2 = fl.allocate().unwrap();
        assert_eq!(p2.get(), 102);
    }

    #[test]
    fn test_freelist_deallocate() {
        let mut fl = Freelist::new(100, 4096);
        fl.deallocate(PageNumber::new(50).unwrap());
        assert_eq!(fl.free_count(), 1);

        let p = fl.allocate().unwrap();
        assert_eq!(p.get(), 50);
    }

    #[test]
    fn test_freelist_reuses_freed_page_before_extending_db() {
        // Reuse must take precedence over file growth, and extension resumes from
        // the high-water mark (not the reused page + 1). The existing tests cover
        // allocate-from-a-prefilled-freelist, pure extension, and a single
        // deallocate+allocate in isolation, but not this interleaving.
        let mut fl = Freelist::new(100, 4096);

        // Empty freelist: the first two allocations extend the db file.
        assert_eq!(fl.allocate().unwrap().get(), 101);
        assert_eq!(fl.allocate().unwrap().get(), 102);
        assert_eq!(fl.db_page_count(), 102);

        // Free page 101; the next allocation must reuse it rather than extend.
        fl.deallocate(PageNumber::new(101).unwrap());
        assert_eq!(fl.free_count(), 1);
        assert_eq!(
            fl.allocate().unwrap().get(),
            101,
            "freed page must be reused before extending the db"
        );
        assert_eq!(fl.free_count(), 0);

        // Freelist empty again: extension resumes from the high-water mark
        // (db_page_count is still 102), yielding 103 -- not the reused 101 + 1.
        assert_eq!(
            fl.allocate().unwrap().get(),
            103,
            "extension must resume from the high-water mark, not the reused page"
        );
        assert_eq!(fl.db_page_count(), 103);
    }

    #[test]
    fn test_freelist_max_page_count() {
        let mut fl = Freelist::new(MAX_PAGE_COUNT, 4096);
        assert!(fl.allocate().is_err());
    }

    #[test]
    fn test_freelist_rejects_and_skips_pages_beyond_db_size() {
        // deallocate must reject pages beyond the current db size — returning
        // them later would corrupt the freelist on flush.
        let mut fl = Freelist::new(10, 4096);
        fl.deallocate(PageNumber::new(5).unwrap()); // in range -> kept
        fl.deallocate(PageNumber::new(100).unwrap()); // beyond db_page_count -> rejected
        assert_eq!(
            fl.free_count(),
            1,
            "out-of-range page must not enter the freelist"
        );
        assert_eq!(fl.allocate().unwrap().get(), 5);

        // allocate must skip stale freelist entries pointing beyond the db, then
        // fall through to extending the file.
        let mut fl = Freelist::with_pages(
            vec![PageNumber::new(99).unwrap(), PageNumber::new(5).unwrap()],
            10,
            4096,
        );
        // Top of the LIFO stack (5) is in range and allocated first.
        assert_eq!(fl.allocate().unwrap().get(), 5);
        // The remaining entry (99) is stale (> db_page_count 10): it is skipped,
        // so the allocator extends the db to page 11 rather than returning 99.
        assert_eq!(
            fl.allocate().unwrap().get(),
            11,
            "stale entry skipped; db extended instead"
        );
        assert_eq!(fl.db_page_count(), 11);
    }

    #[test]
    fn test_btree_freelist_reclamation() {
        let mut freelist = Freelist::new(200, 4096);
        let reclaimed = PageNumber::new(150).unwrap();

        freelist.deallocate(reclaimed);
        assert_eq!(freelist.free_count(), 1);
        assert_eq!(freelist.allocate().unwrap(), reclaimed);
        assert_eq!(freelist.free_count(), 0);
    }

    #[test]
    fn test_read_freelist_single_trunk() {
        let mut pages: HashMap<u32, Vec<u8>> = HashMap::new();

        let trunk = FreelistTrunk {
            next_trunk: None,
            leaf_pages: vec![
                PageNumber::new(5).unwrap(),
                PageNumber::new(6).unwrap(),
                PageNumber::new(7).unwrap(),
            ],
        };
        let mut page = vec![0u8; 4096];
        trunk.write(&mut page).unwrap();
        pages.insert(3, page);

        let result = read_freelist(Some(PageNumber::new(3).unwrap()), &mut |pgno| {
            pages
                .get(&pgno.get())
                .cloned()
                .ok_or_else(|| FrankenError::internal("page not found"))
        })
        .unwrap();

        // Should include trunk page + 3 leaf pages = 4 total.
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].get(), 3); // Trunk page itself.
        assert_eq!(result[1].get(), 5);
        assert_eq!(result[2].get(), 6);
        assert_eq!(result[3].get(), 7);
    }

    #[test]
    fn test_read_freelist_multi_trunk() {
        let mut pages: HashMap<u32, Vec<u8>> = HashMap::new();

        // Trunk 3 → Trunk 8 → end
        let trunk2 = FreelistTrunk {
            next_trunk: None,
            leaf_pages: vec![PageNumber::new(9).unwrap()],
        };
        let mut page2 = vec![0u8; 4096];
        trunk2.write(&mut page2).unwrap();
        pages.insert(8, page2);

        let trunk1 = FreelistTrunk {
            next_trunk: Some(PageNumber::new(8).unwrap()),
            leaf_pages: vec![PageNumber::new(5).unwrap(), PageNumber::new(6).unwrap()],
        };
        let mut page1 = vec![0u8; 4096];
        trunk1.write(&mut page1).unwrap();
        pages.insert(3, page1);

        let result = read_freelist(Some(PageNumber::new(3).unwrap()), &mut |pgno| {
            pages
                .get(&pgno.get())
                .cloned()
                .ok_or_else(|| FrankenError::internal("page not found"))
        })
        .unwrap();

        // Trunk 3 + leaves 5,6 + Trunk 8 + leaf 9 = 5 pages.
        assert_eq!(result.len(), 5);
        assert_eq!(
            result.iter().map(|page| page.get()).collect::<Vec<_>>(),
            vec![3, 5, 6, 8, 9],
            "validation must preserve trunk/leaf order"
        );
        let mut freelist = Freelist::with_pages(result, 9, 4096);
        for expected in [9, 8, 6, 5, 3] {
            assert_eq!(freelist.allocate().unwrap().get(), expected);
        }
        assert_eq!(freelist.free_count(), 0);
        assert_eq!(freelist.allocate().unwrap().get(), 10);
    }

    #[test]
    fn test_read_freelist_rejects_cycles_and_cross_role_aliases() {
        for (trunks, expected_reads) in [
            (vec![(3, 3, vec![])], 1),
            (vec![(3, 8, vec![]), (8, 3, vec![])], 2),
            (vec![(3, 0, vec![5, 5])], 1),
            (vec![(3, 8, vec![5]), (8, 0, vec![5])], 2),
            (vec![(3, 0, vec![3])], 1),
            (vec![(3, 8, vec![8]), (8, 0, vec![])], 1),
            (vec![(3, 8, vec![]), (8, 0, vec![3])], 2),
            (vec![(3, 8, vec![9]), (8, 9, vec![])], 2),
        ] {
            let mut pages = HashMap::new();
            for (page_no, next, leaves) in trunks {
                let trunk = FreelistTrunk {
                    next_trunk: PageNumber::new(next),
                    leaf_pages: leaves
                        .into_iter()
                        .map(|page| PageNumber::new(page).unwrap())
                        .collect(),
                };
                let mut bytes = vec![0u8; 32];
                trunk.write(&mut bytes).unwrap();
                pages.insert(page_no, bytes);
            }
            let mut reads = 0;
            let error = read_freelist(Some(PageNumber::new(3).unwrap()), &mut |page| {
                reads += 1;
                pages.get(&page.get()).cloned().ok_or(FrankenError::Busy)
            })
            .unwrap_err();
            assert!(matches!(error, FrankenError::DatabaseCorrupt { .. }));
            assert!(error.to_string().contains("duplicate page"));
            assert_eq!(reads, expected_reads, "must not reread or follow an alias");
        }
    }

    #[test]
    fn test_read_freelist_rejects_header_before_io() {
        let result = read_freelist(Some(PageNumber::ONE), &mut |_| {
            panic!("database header must not be read as a freelist trunk")
        });
        assert!(matches!(result, Err(FrankenError::DatabaseCorrupt { .. })));
    }

    #[test]
    fn test_read_freelist_propagates_late_backend_error() {
        let mut page = vec![0u8; 8];
        page[..4].copy_from_slice(&8u32.to_be_bytes());
        let mut reads = 0;
        let result = read_freelist(Some(PageNumber::new(3).unwrap()), &mut |page_no| {
            reads += 1;
            if page_no.get() == 3 {
                Ok(page.clone())
            } else {
                Err(FrankenError::Busy)
            }
        });
        assert!(matches!(result, Err(FrankenError::Busy)));
        assert_eq!(reads, 2, "must not return a partial freelist as success");
    }

    #[test]
    fn test_read_freelist_none() {
        let result = read_freelist(None, &mut |_pgno| {
            Err(FrankenError::internal("should not be called"))
        })
        .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_freelist_allocate_skips_pending_byte() {
        let page_size = 4096;
        let pending_byte_page = (0x4000_0000 / page_size) + 1;

        let mut fl = Freelist::new(pending_byte_page - 1, page_size);

        // Next allocation should skip the pending byte page.
        let p = fl.allocate().unwrap();
        assert_eq!(p.get(), pending_byte_page + 1);
        assert_eq!(fl.db_page_count(), pending_byte_page + 1);
    }

    #[test]
    fn test_ptrmap_entries_per_page_4096() {
        assert_eq!(ptrmap_entries_per_page(4096), 819);
        assert_eq!(ptrmap_group_size(4096), 820);
    }

    #[test]
    fn test_ptrmap_page_locations_4096() {
        assert!(is_ptrmap_page(PageNumber::new(2).unwrap(), 4096, 4096));
        assert!(is_ptrmap_page(PageNumber::new(822).unwrap(), 4096, 4096));
        assert!(is_ptrmap_page(PageNumber::new(1642).unwrap(), 4096, 4096));
        assert!(!is_ptrmap_page(PageNumber::new(3).unwrap(), 4096, 4096));
    }

    #[test]
    fn test_ptrmap_page_for_given_pgno_boundaries() {
        let p3 = PageNumber::new(3).unwrap();
        let p821 = PageNumber::new(821).unwrap();
        let p823 = PageNumber::new(823).unwrap();

        assert_eq!(ptrmap_page_for(p3, 4096, 4096).unwrap().get(), 2);
        assert_eq!(ptrmap_entry_offset(p3, 4096, 4096).unwrap(), 0);

        assert_eq!(ptrmap_page_for(p821, 4096, 4096).unwrap().get(), 2);
        assert_eq!(ptrmap_entry_offset(p821, 4096, 4096).unwrap(), 818 * 5);

        assert_eq!(ptrmap_page_for(p823, 4096, 4096).unwrap().get(), 822);
        assert_eq!(ptrmap_entry_offset(p823, 4096, 4096).unwrap(), 0);

        // Pointer-map pages do not have entries in themselves.
        assert!(ptrmap_page_for(PageNumber::new(822).unwrap(), 4096, 4096).is_none());
        assert!(ptrmap_entry_offset(PageNumber::new(822).unwrap(), 4096, 4096).is_none());
    }

    #[test]
    fn test_ptrmap_page_for_header_and_first_ptrmap_page_have_no_entry() {
        // test_ptrmap_page_for_given_pgno_boundaries covers a regular page, a
        // group-final page, a next-group page, and a high pointer-map page (822).
        // The two low-page edges are still unpinned: page 1 (the database header,
        // raw < 3 -- pointer-map entries begin at page 3) and page 2 (which is
        // itself the first pointer-map page) both lack a pointer-map entry, via
        // two different guards.
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();

        // Page 1 is the database header (raw < 3 guard): no pointer-map entry.
        assert!(ptrmap_page_for(p1, 4096, 4096).is_none());
        assert!(ptrmap_entry_offset(p1, 4096, 4096).is_none());

        // Page 2 is the first pointer-map page (is_ptrmap_page guard), so it has
        // no entry of its own.
        assert!(is_ptrmap_page(p2, 4096, 4096));
        assert!(ptrmap_page_for(p2, 4096, 4096).is_none());
        assert!(ptrmap_entry_offset(p2, 4096, 4096).is_none());
    }

    #[test]
    fn test_ptrmap_pending_byte_page_collision() {
        // For 1024 byte pages, the pending byte page is 1_048_577.
        // It falls exactly on what would normally be a pointer map page.
        let pending_byte_pgno = 1_048_577;
        let usable_size = 1024;
        let page_size = 1024;

        // The pointer map page is pushed to the next page.
        assert!(!is_ptrmap_page(
            PageNumber::new(pending_byte_pgno).unwrap(),
            usable_size,
            page_size
        ));
        assert!(is_ptrmap_page(
            PageNumber::new(pending_byte_pgno + 1).unwrap(),
            usable_size,
            page_size
        ));

        // The pending byte page itself has no pointer map entry.
        assert!(
            ptrmap_entry_offset(
                PageNumber::new(pending_byte_pgno).unwrap(),
                usable_size,
                page_size
            )
            .is_none()
        );

        // The page right after the pointer map page has offset 0.
        assert_eq!(
            ptrmap_entry_offset(
                PageNumber::new(pending_byte_pgno + 2).unwrap(),
                usable_size,
                page_size
            ),
            Some(0)
        );
    }

    #[test]
    fn test_ptrmap_entry_offset_skips_pending_byte_inside_group() {
        // For 4096-byte pages, the pending byte page lands in the middle of
        // the pointer-map group that starts at page 261582.
        // The slot corresponding to the pending byte page is simply left unused,
        // and subsequent pages maintain their unshifted index offsets.
        let usable_size = 4096;
        let page_size = 4096;
        let pending_byte_pgno = (PENDING_BYTE_OFFSET / page_size) + 1;

        // The pending byte page itself has no pointer map entry.
        assert!(
            ptrmap_entry_offset(
                PageNumber::new(pending_byte_pgno).unwrap(),
                usable_size,
                page_size
            )
            .is_none()
        );

        let pgno = PageNumber::new(pending_byte_pgno + 1).unwrap();

        assert_eq!(
            ptrmap_page_for(pgno, usable_size, page_size).unwrap().get(),
            261_582
        );
        assert_eq!(
            ptrmap_entry_offset(pgno, usable_size, page_size),
            // No shift occurs. The slot for the pending byte is wasted.
            Some(563 * PTRMAP_ENTRY_SIZE_BYTES)
        );
    }

    #[test]
    fn test_ptrmap_entry_offset_last_page_in_group_with_pending_byte() {
        let usable_size = 4096;
        let page_size = 4096;
        let ptrmap_page = 261582;
        // Group size is 820 physical pages.
        // Group starts at 261582. Next group starts at 261582 + 820 = 262402.
        // Therefore, 262402 is NOT in this group, it IS the next ptrmap page!
        let pgno = PageNumber::new(262402).unwrap();

        assert_eq!(
            ptrmap_page_for(pgno, usable_size, page_size),
            None // It's a ptrmap page itself!
        );
        assert_eq!(ptrmap_entry_offset(pgno, usable_size, page_size), None);

        let last_in_group = PageNumber::new(262401).unwrap();
        assert_eq!(
            ptrmap_page_for(last_in_group, usable_size, page_size)
                .unwrap()
                .get(),
            ptrmap_page
        );
        let offset = ptrmap_entry_offset(last_in_group, usable_size, page_size).unwrap();
        assert!(offset + PTRMAP_ENTRY_SIZE_BYTES <= usable_size);
        assert_eq!(offset, 818 * PTRMAP_ENTRY_SIZE_BYTES);
    }
    #[test]
    fn test_ptrmap_entry_encode_decode() {
        let entry = PtrMapEntry {
            kind: PtrMapType::Overflow1,
            parent: Some(PageNumber::new(123).unwrap()),
        };
        let encoded = entry.encode();
        let decoded = PtrMapEntry::decode(&encoded).unwrap();
        assert_eq!(decoded, entry);
    }

    #[test]
    fn test_ptrmap_type_parent_semantics() {
        let root = PtrMapEntry {
            kind: PtrMapType::RootPage,
            parent: None,
        };
        let free = PtrMapEntry {
            kind: PtrMapType::FreePage,
            parent: None,
        };

        assert_eq!(PtrMapEntry::decode(&root.encode()).unwrap(), root);
        assert_eq!(PtrMapEntry::decode(&free.encode()).unwrap(), free);

        let invalid_root = [1, 0, 0, 0, 7];
        assert!(PtrMapEntry::decode(&invalid_root).is_err());

        let invalid_bt = [5, 0, 0, 0, 0];
        assert!(PtrMapEntry::decode(&invalid_bt).is_err());
    }

    #[test]
    fn test_ptrmap_entry_decode_rejects_short_input_and_invalid_type_code() {
        // test_ptrmap_type_parent_semantics covers the parent-value guards
        // (root/free must be 0, btree/overflow must be nonzero), but not the two
        // structural guards in decode: a buffer shorter than PTRMAP_ENTRY_SIZE_
        // BYTES, and a type code outside the valid 1..=5 range.

        // Fewer than 5 bytes is rejected as "too small".
        assert!(PtrMapEntry::decode(&[]).is_err());
        assert!(PtrMapEntry::decode(&[1, 0, 0, 0]).is_err()); // 4 bytes

        // Type code 0 and 6 are outside the valid 1..=5 range.
        assert!(PtrMapEntry::decode(&[0, 0, 0, 0, 0]).is_err());
        assert!(PtrMapEntry::decode(&[6, 0, 0, 0, 1]).is_err());

        // Sanity: well-formed entries at the type-code range boundaries decode.
        assert!(PtrMapEntry::decode(&[1, 0, 0, 0, 0]).is_ok()); // RootPage, parent 0
        assert!(PtrMapEntry::decode(&[5, 0, 0, 0, 1]).is_ok()); // Btree, parent 1
    }

    #[test]
    fn test_ptrmap_layout_self_consistency_across_a_page_range() {
        // 512-byte pages, no reserved bytes: 102 entries/ptrmap-page, group=103,
        // so ptrmap pages fall at 2, 105, 208, ... The pending-byte page (~2M)
        // is far outside this range, giving a clean periodic layout. Existing
        // ptrmap tests use 4096-byte pages (group=820 -> only page 2 in range),
        // so the multi-group periodicity and cross-function consistency are
        // unexercised. Sweep the range and check the functions agree.
        let usable = 512u32;
        let page_size = 512u32;
        assert_eq!(ptrmap_entries_per_page(usable), 102);
        let group = ptrmap_group_size(usable);
        assert_eq!(group, 103);

        for raw in 2u32..=250 {
            let pg = PageNumber::new(raw).unwrap();
            let is_map = is_ptrmap_page(pg, usable, page_size);
            // Ptrmap pages sit exactly at the periodic positions 2, 2+G, 2+2G...
            assert_eq!(
                is_map,
                (raw - 2) % group == 0,
                "is_ptrmap_page wrong at {raw}"
            );

            match ptrmap_page_for(pg, usable, page_size) {
                None => {
                    // Only ptrmap pages themselves (and pages < 3) lack a parent map.
                    assert!(
                        is_map || raw < 3,
                        "page {raw} unexpectedly has no ptrmap page"
                    );
                }
                Some(q) => {
                    assert!(!is_map, "ptrmap page {raw} must not map to another");
                    assert!(
                        is_ptrmap_page(q, usable, page_size),
                        "page {raw} maps to non-ptrmap {q:?}"
                    );
                    assert!(q.get() < raw, "ptrmap page {q:?} must precede {raw}");
                    assert!(
                        raw - q.get() <= group,
                        "page {raw} lies outside its group {q:?}"
                    );
                    // The entry is densely packed and fits on the ptrmap page.
                    let off = ptrmap_entry_offset(pg, usable, page_size).expect("entry offset");
                    assert_eq!(
                        off,
                        (raw - q.get() - 1) * PTRMAP_ENTRY_SIZE_BYTES,
                        "entry offset wrong at {raw}"
                    );
                    assert!(off < usable, "entry offset {off} past usable {usable}");
                }
            }
        }
    }

    #[test]
    fn test_ptrmap_final_db_size_matches_sqlite_boundaries() {
        // 4096B page: group=820, ptrmap at 2, 822.
        assert_eq!(ptrmap_final_db_size(55, 48, 5, 4096, 4096), 7);
        assert_eq!(ptrmap_final_db_size(3, 1, 1, 4096, 4096), 1);
        assert_eq!(ptrmap_final_db_size(10, 0, 3, 4096, 4096), 10);
        // 512B page: group=103, ptrmap at 2, 105, 208.
        assert_eq!(ptrmap_final_db_size(106, 1, 3, 512, 512), 104);
        assert_eq!(ptrmap_final_db_size(260, 250, 3, 512, 512), 8);
    }

    #[test]
    fn test_ptrmap_full_and_incremental_compaction_and_corrupt_negative_control() {
        let rt = asupersync::runtime::RuntimeBuilder::current_thread()
            .build()
            .expect("build runtime");
        rt.block_on(async {
            let cx = Cx::new();
            let page_size = 512_u32;
            let (mut store, root) =
                crate::cursor::MemPageStore::with_pointer_map_leaf_table(page_size);
            let mut expected_rows: std::collections::BTreeMap<i64, Vec<u8>> =
                std::collections::BTreeMap::new();

            // Grow past page 105 (second pointer-map page!) using both inline and overflowing rows.
            {
                let mut cursor = crate::cursor::BtCursor::new(store, root, page_size, true);
                for id in 1..=200_i64 {
                    let len = if id % 7 == 0 { 1100 } else { 180 };
                    let payload = vec![(id % 251) as u8; len];
                    cursor
                        .table_insert(&cx, id, &payload)
                        .await
                        .expect("insert");
                    expected_rows.insert(id, payload);
                }
                store = cursor.into_pager();
            }
            let grown_db_size = store.max_page_number();
            assert!(
                grown_db_size > 110,
                "512B table must cross second ptrmap page 105 (got {grown_db_size})"
            );
            ptrmap_verify_all_invariants(
                &cx,
                &store,
                &[root],
                store.free_pages_snapshot(),
                grown_db_size,
                page_size,
                page_size,
            )
            .await
            .expect("ptrmap must match full walk after multi-ptrmap growth");

            // Delete 85% of rows (including overflowing rows).
            {
                let mut cursor = crate::cursor::BtCursor::new(store, root, page_size, true);
                for id in 21..=200_i64 {
                    assert!(
                        cursor
                            .table_move_to(&cx, id)
                            .await
                            .expect("seek")
                            .is_found()
                    );
                    cursor.delete(&cx).await.expect("delete");
                    expected_rows.remove(&id);
                }
                store = cursor.into_pager();
            }
            let after_delete_size = store.max_page_number();
            let free_after_delete = store.free_pages_snapshot().to_vec();
            assert!(
                !free_after_delete.is_empty(),
                "deleting 85% of rows must produce freed pages"
            );
            ptrmap_verify_all_invariants(
                &cx,
                &store,
                &[root],
                &free_after_delete,
                after_delete_size,
                page_size,
                page_size,
            )
            .await
            .expect("ptrmap valid after deletes");

            // Incremental vacuum 5 pages first.
            let inc = ptrmap_compact_incremental(
                &cx,
                &mut store,
                after_delete_size,
                root.get(),
                &free_after_delete,
                5,
                page_size,
                page_size,
            )
            .await
            .expect("incremental vacuum 5");
            assert!(inc.new_db_size < after_delete_size);
            store.truncate_to_db_size(inc.new_db_size, inc.remaining_freelist.clone());
            ptrmap_verify_all_invariants(
                &cx,
                &store,
                &[root],
                store.free_pages_snapshot(),
                inc.new_db_size,
                page_size,
                page_size,
            )
            .await
            .expect("ptrmap valid after incremental vacuum(5)");

            // Full vacuum the rest.
            let full = ptrmap_compact_full(
                &cx,
                &mut store,
                inc.new_db_size,
                root.get(),
                &inc.remaining_freelist,
                page_size,
                page_size,
            )
            .await
            .expect("full vacuum");
            assert!(
                full.new_db_size < grown_db_size / 3,
                "full vacuum must dramatically shrink DB: grown={grown_db_size} final={}",
                full.new_db_size
            );
            assert!(full.remaining_freelist.is_empty());
            store.truncate_to_db_size(full.new_db_size, full.remaining_freelist.clone());
            ptrmap_verify_all_invariants(
                &cx,
                &store,
                &[root],
                store.free_pages_snapshot(),
                full.new_db_size,
                page_size,
                page_size,
            )
            .await
            .expect("ptrmap valid after full vacuum");

            // Verify every surviving row's exact payload after all relocations!
            {
                let mut cursor = crate::cursor::BtCursor::new(store, root, page_size, true);
                let mut actual_rows = std::collections::BTreeMap::new();
                if cursor.first(&cx).await.expect("first") {
                    loop {
                        let rid = cursor.rowid(&cx).await.expect("rowid");
                        let pay = cursor.payload(&cx).await.expect("payload");
                        actual_rows.insert(rid, pay);
                        if !cursor.next(&cx).await.expect("next") {
                            break;
                        }
                    }
                }
                assert_eq!(actual_rows, expected_rows);
                store = cursor.into_pager();
            }

            // Planted negative control: corrupt one ptrmap entry -> verifier MUST fail!
            if full.new_db_size >= 4 {
                let victim = PageNumber::new(4).unwrap();
                store
                    .write_ptrmap_entry(
                        &cx,
                        victim,
                        PtrMapEntry {
                            kind: PtrMapType::Btree,
                            parent: Some(PageNumber::new(999).unwrap()),
                        },
                    )
                    .await
                    .expect("plant corrupt ptrmap");
                let err = ptrmap_verify_all_invariants(
                    &cx,
                    &store,
                    &[root],
                    store.free_pages_snapshot(),
                    full.new_db_size,
                    page_size,
                    page_size,
                )
                .await
                .expect_err("corrupted ptrmap entry MUST fail invariant verification");
                assert!(
                    err.to_string().contains("Bad ptr map entry"),
                    "unexpected error: {err}"
                );
            }
        });
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(24))]
        #[test]
        fn proptest_ptrmap_invariants_across_splits_overflows_deletes_and_compaction(
            ops in proptest::collection::vec(
                (0_u8..4, 1_i64..=90, 16_usize..=720),
                12..65,
            )
        ) {
            let rt = asupersync::runtime::RuntimeBuilder::current_thread()
                .build()
                .expect("build runtime");
            rt.block_on(async {
                let cx = Cx::new();
                let page_size = 512_u32;
                let (mut store, root) =
                    crate::cursor::MemPageStore::with_pointer_map_leaf_table(page_size);
                let mut model: std::collections::BTreeMap<i64, Vec<u8>> =
                    std::collections::BTreeMap::new();

                for (step_idx, (op, key, size)) in ops.into_iter().enumerate() {
                    let payload = vec![((key as usize + step_idx) % 251) as u8; size];
                    {
                        let mut cursor =
                            crate::cursor::BtCursor::new(store, root, page_size, true);
                        match op {
                            0..=2 => {
                                // Insert or replace
                                if cursor.table_move_to(&cx, key).await.expect("seek").is_found() {
                                    cursor.delete(&cx).await.expect("delete before replace");
                                }
                                cursor.table_insert(&cx, key, &payload).await.expect("insert");
                                model.insert(key, payload);
                            }
                            _ => {
                                // Delete if present
                                if cursor.table_move_to(&cx, key).await.expect("seek").is_found() {
                                    cursor.delete(&cx).await.expect("delete");
                                    model.remove(&key);
                                }
                            }
                        }
                        store = cursor.into_pager();
                    }

                    // Verify EVERY ptrmap entry after each mutation / split / balance!
                    let db_size = store.max_page_number();
                    ptrmap_verify_all_invariants(
                        &cx,
                        &store,
                        &[root],
                        store.free_pages_snapshot(),
                        db_size,
                        page_size,
                        page_size,
                    )
                    .await
                    .expect("ptrmap invariant broken after b-tree op");

                    // Periodically run full compaction when free pages exist.
                    if step_idx % 9 == 8 && !store.free_pages_snapshot().is_empty() {
                        let free_before = store.free_pages_snapshot().to_vec();
                        let outcome = ptrmap_compact_full(
                            &cx,
                            &mut store,
                            db_size,
                            root.get(),
                            &free_before,
                            page_size,
                            page_size,
                        )
                        .await
                        .expect("full compaction");
                        store.truncate_to_db_size(outcome.new_db_size, outcome.remaining_freelist);
                        ptrmap_verify_all_invariants(
                            &cx,
                            &store,
                            &[root],
                            store.free_pages_snapshot(),
                            outcome.new_db_size,
                            page_size,
                            page_size,
                        )
                        .await
                        .expect("ptrmap invariant broken after full compaction");
                    }
                }

                // Final full data-parity check against reference BTreeMap model.
                let mut cursor = crate::cursor::BtCursor::new(store, root, page_size, true);
                let mut actual = std::collections::BTreeMap::new();
                if cursor.first(&cx).await.expect("first") {
                    loop {
                        let rid = cursor.rowid(&cx).await.expect("rowid");
                        let pay = cursor.payload(&cx).await.expect("payload");
                        actual.insert(rid, pay);
                        if !cursor.next(&cx).await.expect("next") {
                            break;
                        }
                    }
                }
                assert_eq!(actual, model);
            });
        }
    }
}
