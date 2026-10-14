//! Immutable historical images reconstructed from validated native history.
//!
//! These own page bytes, not storage handles, writer authority or snapshot pins
//! in a live publisher. They cannot be converted into a writable page store.

use std::fmt;
use std::sync::Arc;

use fsqlite_types::{CommitSeq, PageNumber};

struct SnapshotPages {
    sequence: CommitSeq,
    commit_time_unix_ns: Option<u64>,
    page_size: u32,
    extent: u32,
    pages: Vec<(PageNumber, Arc<[u8]>)>,
    payload_bytes: usize,
}

/// A read-only native page image at one exact committed sequence.
///
/// Constructed only by `NativePageRecovery::prepare_snapshot` or
/// `prepare_snapshot_at_time` after the complete recovered history passes the
/// ordinary proof, capsule, read-version, token and resource checks. Clones
/// share the image; later commits, recovery retries and live history reclamation
/// cannot change it. A deleted page is absent, but its address remains in the
/// historical extent. Sparse holes are never expanded into a dense page array.
///
/// This is an in-memory historical image of the native log, not a serialized
/// checkpoint, a SQLite database file, an SSI proof or permission to publish.
/// It does not pin a live transaction or retain file handles or encryption keys.
/// The payload fits the recovery owner's page budget; independently retained
/// images accumulate caller-owned memory. This is not a process-RSS bound.
#[derive(Clone)]
pub struct NativePageSnapshot {
    inner: Arc<SnapshotPages>,
}

impl fmt::Debug for NativePageSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativePageSnapshot")
            .field("sequence", &self.inner.sequence)
            .field("page_size", &self.inner.page_size)
            .field("extent", &self.inner.extent)
            .field("pages", &self.inner.pages.len())
            .field("payload_bytes", &self.inner.payload_bytes)
            .finish_non_exhaustive()
    }
}

impl NativePageSnapshot {
    // Only the enclosing native-page implementation can create an image. The
    // recovery interpreter supplies ordered, validated pages after each atomic
    // capsule; public callers cannot substitute an arbitrary page map.
    pub(super) fn from_validated_pages(
        sequence: CommitSeq,
        commit_time_unix_ns: Option<u64>,
        page_size: u32,
        extent: u32,
        pages: Vec<(PageNumber, Arc<[u8]>)>,
        payload_bytes: usize,
    ) -> Self {
        Self {
            inner: Arc::new(SnapshotPages {
                sequence,
                commit_time_unix_ns,
                page_size,
                extent,
                pages,
                payload_bytes,
            }),
        }
    }

    /// Exact historical commit. Zero denotes the empty genesis image.
    #[must_use]
    pub fn commit_seq(&self) -> CommitSeq { self.inner.sequence }

    /// Actual selected commit time, not the requested timestamp. Genesis has none.
    #[must_use]
    pub fn commit_time_unix_ns(&self) -> Option<u64> { self.inner.commit_time_unix_ns }

    #[must_use]
    pub fn page_size(&self) -> u32 { self.inner.page_size }

    /// Highest address committed by this point, including historical tombstones.
    /// This is an address bound, not a live-page count or physical file length.
    #[must_use]
    pub fn database_size_bound(&self) -> u32 { self.inner.extent }

    /// Present pages only; deletions and never-allocated holes do not contribute.
    #[must_use]
    pub fn page_count(&self) -> usize { self.inner.pages.len() }

    /// Image payload bytes, excluding the sparse index and caller-held clones.
    #[must_use]
    pub fn retained_page_bytes(&self) -> usize { self.inner.payload_bytes }

    /// Read exact immutable bytes in O(log(present pages)) without storage I/O.
    /// An absent result never falls through to a live publisher's newer pages.
    #[must_use]
    pub fn read_page(&self, page: PageNumber) -> Option<Arc<[u8]>> {
        self.inner.pages.binary_search_by_key(&page, |(number, _)| *number)
            .ok().map(|index| Arc::clone(&self.inner.pages[index].1))
    }

    /// Enumerate present addresses in order, without allocating a hole list.
    pub fn page_numbers(&self) -> impl ExactSizeIterator<Item = PageNumber> + '_ {
        self.inner.pages.iter().map(|(page, _)| *page)
    }
}
