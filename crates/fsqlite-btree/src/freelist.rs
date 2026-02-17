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

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::PageNumber;
use fsqlite_types::limits::MAX_PAGE_COUNT;

/// Maximum leaf entries that fit on a single trunk page.
#[must_use]
pub const fn max_leaf_entries(usable_size: u32) -> u32 {
    usable_size / 4 - 2
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
    pub fn parse(page: &[u8]) -> Result<Self> {
        if page.len() < 8 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "freelist trunk page too small".to_owned(),
            });
        }

        let next_raw = u32::from_be_bytes([page[0], page[1], page[2], page[3]]);
        let next_trunk = PageNumber::new(next_raw);

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
            if let Some(pn) = PageNumber::new(pgno) {
                leaf_pages.push(pn);
            }
            // Skip zero entries (shouldn't happen in valid DB, but defensive).
        }

        Ok(Self {
            next_trunk,
            leaf_pages,
        })
    }

    /// Serialize this trunk page into a page-sized buffer.
    #[allow(clippy::cast_possible_truncation)]
    pub fn write(&self, page: &mut [u8]) {
        let next = self.next_trunk.map_or(0u32, PageNumber::get);
        page[0..4].copy_from_slice(&next.to_be_bytes());

        let count = self.leaf_pages.len() as u32;
        page[4..8].copy_from_slice(&count.to_be_bytes());

        for (i, &pgno) in self.leaf_pages.iter().enumerate() {
            let offset = 8 + i * 4;
            page[offset..offset + 4].copy_from_slice(&pgno.get().to_be_bytes());
        }
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
}

impl Freelist {
    /// Create a new freelist with no free pages.
    #[must_use]
    pub fn new(db_page_count: u32) -> Self {
        Self {
            free_pages: Vec::new(),
            db_page_count,
        }
    }

    /// Create a freelist pre-populated with free pages.
    #[must_use]
    pub fn with_pages(pages: Vec<PageNumber>, db_page_count: u32) -> Self {
        Self {
            free_pages: pages,
            db_page_count,
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
    /// extends the database file by one page.
    pub fn allocate(&mut self) -> Result<PageNumber> {
        if let Some(pgno) = self.free_pages.pop() {
            return Ok(pgno);
        }
        // Extend the database file.
        if self.db_page_count >= MAX_PAGE_COUNT {
            return Err(FrankenError::DatabaseFull);
        }
        self.db_page_count += 1;
        PageNumber::new(self.db_page_count).ok_or(FrankenError::DatabaseFull)
    }

    /// Return a page to the freelist.
    pub fn deallocate(&mut self, page: PageNumber) {
        self.free_pages.push(page);
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

/// Pointer-map entry size in bytes (§11.6).
pub const PTRMAP_ENTRY_SIZE_BYTES: u32 = 5;

/// Pointer-map entry type code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PtrMapType {
    /// Root b-tree page, parent must be 0.
    RootPage = 1,
    /// Freelist page, parent must be 0.
    FreePage = 2,
    /// First overflow page, parent is owning b-tree page.
    Overflow1 = 3,
    /// Subsequent overflow page, parent is previous overflow page.
    Overflow2 = 4,
    /// Non-root b-tree page, parent is b-tree parent page.
    Btree = 5,
}

impl PtrMapType {
    fn from_code(code: u8) -> Result<Self> {
        match code {
            1 => Ok(Self::RootPage),
            2 => Ok(Self::FreePage),
            3 => Ok(Self::Overflow1),
            4 => Ok(Self::Overflow2),
            5 => Ok(Self::Btree),
            _ => Err(FrankenError::DatabaseCorrupt {
                detail: format!("invalid pointer-map type code: {code}"),
            }),
        }
    }
}

/// Parsed pointer-map entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PtrMapEntry {
    pub kind: PtrMapType,
    pub parent: Option<PageNumber>,
}

impl PtrMapEntry {
    /// Decode one 5-byte pointer-map entry.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < PTRMAP_ENTRY_SIZE_BYTES as usize {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "pointer-map entry too small".to_owned(),
            });
        }

        let kind = PtrMapType::from_code(bytes[0])?;
        let parent_raw = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        let parent = PageNumber::new(parent_raw);

        match kind {
            PtrMapType::RootPage | PtrMapType::FreePage => {
                if parent_raw != 0 {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "pointer-map type {:?} must have parent 0, got {}",
                            kind, parent_raw
                        ),
                    });
                }
            }
            PtrMapType::Overflow1 | PtrMapType::Overflow2 | PtrMapType::Btree => {
                if parent.is_none() {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "pointer-map type {:?} requires non-zero parent page",
                            kind
                        ),
                    });
                }
            }
        }

        Ok(Self { kind, parent })
    }

    /// Encode one 5-byte pointer-map entry.
    #[must_use]
    pub fn encode(self) -> [u8; PTRMAP_ENTRY_SIZE_BYTES as usize] {
        let mut out = [0u8; PTRMAP_ENTRY_SIZE_BYTES as usize];
        out[0] = self.kind as u8;
        let parent = self.parent.map_or(0u32, PageNumber::get);
        out[1..5].copy_from_slice(&parent.to_be_bytes());
        out
    }
}

/// Number of pointer-map entries per pointer-map page for a usable page size.
#[must_use]
pub const fn ptrmap_entries_per_page(usable_size: u32) -> u32 {
    usable_size / PTRMAP_ENTRY_SIZE_BYTES
}

/// Pointer-map group size = entries per page + the pointer-map page itself.
#[must_use]
pub const fn ptrmap_group_size(usable_size: u32) -> u32 {
    ptrmap_entries_per_page(usable_size) + 1
}

/// Whether `pgno` is itself a pointer-map page.
#[must_use]
pub const fn is_ptrmap_page(pgno: PageNumber, usable_size: u32) -> bool {
    let raw = pgno.get();
    if raw < 2 {
        return false;
    }
    let group = ptrmap_group_size(usable_size);
    if group == 0 {
        return false;
    }
    (raw - 2) % group == 0
}

/// Pointer-map page that stores metadata for `pgno`.
///
/// Returns `None` when `pgno` is itself a pointer-map page.
#[must_use]
pub const fn ptrmap_page_for(pgno: PageNumber, usable_size: u32) -> Option<PageNumber> {
    if is_ptrmap_page(pgno, usable_size) {
        return None;
    }
    let raw = pgno.get();
    if raw < 3 {
        // Page 1 is database header; pointer map entries begin at page 3.
        return None;
    }

    let group = ptrmap_group_size(usable_size);
    if group == 0 {
        return None;
    }
    let base = 2 + ((raw - 2) / group) * group;
    PageNumber::new(base)
}

/// Byte offset of `pgno`'s pointer-map entry within its pointer-map page.
///
/// Returns `None` when `pgno` is itself a pointer-map page.
#[must_use]
pub const fn ptrmap_entry_offset(pgno: PageNumber, usable_size: u32) -> Option<u32> {
    let Some(ptrmap_page) = ptrmap_page_for(pgno, usable_size) else {
        return None;
    };
    let index = pgno.get() - ptrmap_page.get() - 1;
    Some(index * PTRMAP_ENTRY_SIZE_BYTES)
}

/// Read the entire freelist from the database, starting from the first
/// trunk page.
///
/// `first_trunk` is the page number of the first freelist trunk page
/// (from the database header, bytes 32-35).
/// `read_page` reads a raw page by page number.
///
/// Returns all free page numbers collected from the freelist.
pub fn read_freelist<F>(
    first_trunk: Option<PageNumber>,
    read_page: &mut F,
) -> Result<Vec<PageNumber>>
where
    F: FnMut(PageNumber) -> Result<Vec<u8>>,
{
    let mut all_pages = Vec::new();
    let mut current = first_trunk;
    let mut visited = 0usize;

    while let Some(trunk_pgno) = current {
        visited += 1;
        if visited > 1_000_000 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "freelist trunk chain too long (possible cycle)".to_owned(),
            });
        }

        let page_data = read_page(trunk_pgno)?;
        let trunk = FreelistTrunk::parse(&page_data)?;

        // The trunk page itself is also free (it's part of the freelist).
        all_pages.push(trunk_pgno);
        all_pages.extend_from_slice(&trunk.leaf_pages);

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
    use std::collections::HashMap;

    #[test]
    fn test_max_leaf_entries() {
        // 4096 / 4 - 2 = 1022
        assert_eq!(max_leaf_entries(4096), 1022);
        // 512 / 4 - 2 = 126
        assert_eq!(max_leaf_entries(512), 126);
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
        trunk.write(&mut page);

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
        trunk.write(&mut page);

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
        trunk.write(&mut page);

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
    fn test_freelist_allocate_from_free() {
        let mut fl = Freelist::with_pages(
            vec![PageNumber::new(10).unwrap(), PageNumber::new(20).unwrap()],
            100,
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
        let mut fl = Freelist::new(100);
        assert_eq!(fl.free_count(), 0);

        let p = fl.allocate().unwrap();
        assert_eq!(p.get(), 101);
        assert_eq!(fl.db_page_count(), 101);

        let p2 = fl.allocate().unwrap();
        assert_eq!(p2.get(), 102);
    }

    #[test]
    fn test_freelist_deallocate() {
        let mut fl = Freelist::new(100);
        fl.deallocate(PageNumber::new(50).unwrap());
        assert_eq!(fl.free_count(), 1);

        let p = fl.allocate().unwrap();
        assert_eq!(p.get(), 50);
    }

    #[test]
    fn test_freelist_max_page_count() {
        let mut fl = Freelist::new(MAX_PAGE_COUNT);
        assert!(fl.allocate().is_err());
    }

    #[test]
    fn test_btree_freelist_reclamation() {
        let mut freelist = Freelist::new(200);
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
        trunk.write(&mut page);
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
        trunk2.write(&mut page2);
        pages.insert(8, page2);

        let trunk1 = FreelistTrunk {
            next_trunk: Some(PageNumber::new(8).unwrap()),
            leaf_pages: vec![PageNumber::new(5).unwrap(), PageNumber::new(6).unwrap()],
        };
        let mut page1 = vec![0u8; 4096];
        trunk1.write(&mut page1);
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
    fn test_ptrmap_entries_per_page_4096() {
        assert_eq!(ptrmap_entries_per_page(4096), 819);
        assert_eq!(ptrmap_group_size(4096), 820);
    }

    #[test]
    fn test_ptrmap_page_locations_4096() {
        assert!(is_ptrmap_page(PageNumber::new(2).unwrap(), 4096));
        assert!(is_ptrmap_page(PageNumber::new(822).unwrap(), 4096));
        assert!(is_ptrmap_page(PageNumber::new(1642).unwrap(), 4096));
        assert!(!is_ptrmap_page(PageNumber::new(3).unwrap(), 4096));
    }

    #[test]
    fn test_ptrmap_page_for_given_pgno_boundaries() {
        let p3 = PageNumber::new(3).unwrap();
        let p821 = PageNumber::new(821).unwrap();
        let p823 = PageNumber::new(823).unwrap();

        assert_eq!(ptrmap_page_for(p3, 4096).unwrap().get(), 2);
        assert_eq!(ptrmap_entry_offset(p3, 4096).unwrap(), 0);

        assert_eq!(ptrmap_page_for(p821, 4096).unwrap().get(), 2);
        assert_eq!(ptrmap_entry_offset(p821, 4096).unwrap(), 818 * 5);

        assert_eq!(ptrmap_page_for(p823, 4096).unwrap().get(), 822);
        assert_eq!(ptrmap_entry_offset(p823, 4096).unwrap(), 0);

        // Pointer-map pages do not have entries in themselves.
        assert!(ptrmap_page_for(PageNumber::new(822).unwrap(), 4096).is_none());
        assert!(ptrmap_entry_offset(PageNumber::new(822).unwrap(), 4096).is_none());
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
}
