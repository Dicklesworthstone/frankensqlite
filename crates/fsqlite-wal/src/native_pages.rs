//! Native page capsules and snapshot validation over durable ECS commits.
//!
//! This profile uses commit-order optimistic validation: every observed page
//! version must still be current when a writing transaction commits. It is
//! deliberately more conservative than SSI, not a replacement for the SQL
//! engine's live SSI policy. Missing pages and tombstones have version stamps
//! too, so an insert/delete cycle cannot erase a conflicting observation.
//!
//! The page set is physical. A B-tree consumer must record the pages used to
//! resolve a predicate (including absence); this module does not infer SQL
//! predicates, schema changes, or row-level conflicts from arbitrary bytes.

mod store;
pub use store::{
    MAX_NATIVE_SAVEPOINTS, NativePageLimits, NativePageSavepoint, NativePageStore,
    NativePageTransaction, NativePageTransactionState,
};

use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{CommitSeq, PageNumber};

use crate::native_commit::durable::NativeCommitCandidate;

/// Maximum canonical capsule size, matching the concrete native object codec.
pub const MAX_PAGE_CAPSULE_BYTES: usize = 4 * 1024 * 1024;
/// Bounds both read-set metadata and the number of page mutations per capsule.
pub const MAX_CAPSULE_PAGES: usize = 4096;
const MAGIC: &[u8; 8] = b"FNPG\x01\0\0\0";
const HEADER_BYTES: usize = 28;

/// A full page replacement, or a versioned deletion when `data` is `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativePageWrite {
    pub page: PageNumber,
    pub data: Option<Arc<[u8]>>,
}

/// Complete physical read observations and mutations for one native commit.
///
/// Collections are strictly page-ordered and contain no duplicates. Each
/// written page must also have an observation, even for a blind write or an
/// insertion into an absent page. Sequence zero means no version has ever been
/// visible at the snapshot; deletion of an existing page has a nonzero stamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativePageCapsule {
    pub page_size: u32,
    pub snapshot: CommitSeq,
    pub reads: Vec<(PageNumber, CommitSeq)>,
    pub writes: Vec<NativePageWrite>,
}

impl NativePageCapsule {
    /// Encode the versioned capsule without relying on a serializer's layout.
    ///
    /// # Errors
    /// Rejects malformed sets, page sizes, versions, and oversized capsules.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let len = self.encoded_len()?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(len).map_err(|_| FrankenError::OutOfMemory)?;
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.page_size.to_le_bytes());
        bytes.extend_from_slice(&self.snapshot.get().to_le_bytes());
        bytes.extend_from_slice(&count_u32(self.reads.len())?.to_le_bytes());
        bytes.extend_from_slice(&count_u32(self.writes.len())?.to_le_bytes());
        for (page, version) in &self.reads {
            bytes.extend_from_slice(&page.get().to_le_bytes());
            bytes.extend_from_slice(&version.get().to_le_bytes());
        }
        for write in &self.writes {
            bytes.extend_from_slice(&write.page.get().to_le_bytes());
            bytes.extend_from_slice(&u32::from(write.data.is_some()).to_le_bytes());
            if let Some(data) = &write.data {
                bytes.extend_from_slice(data);
            }
        }
        Ok(bytes)
    }

    /// Decode a bounded capsule, refusing truncated and trailing data.
    ///
    /// # Errors
    /// Rejects unsupported versions, malformed counts, noncanonical ordering,
    /// missing write observations, and invalid page payloads.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_BYTES || bytes.len() > MAX_PAGE_CAPSULE_BYTES {
            return Err(corrupt("native page capsule length is out of bounds"));
        }
        let mut input = Reader(bytes);
        if input.array::<8>()? != *MAGIC {
            return Err(corrupt("unsupported native page capsule version"));
        }
        let page_size = u32::from_le_bytes(input.array()?);
        validate_page_size(page_size)?;
        let snapshot = CommitSeq::new(u64::from_le_bytes(input.array()?));
        let read_count = usize::try_from(u32::from_le_bytes(input.array()?))
            .map_err(|_| FrankenError::TooBig)?;
        let write_count = usize::try_from(u32::from_le_bytes(input.array()?))
            .map_err(|_| FrankenError::TooBig)?;
        if read_count > MAX_CAPSULE_PAGES || write_count == 0 || write_count > MAX_CAPSULE_PAGES {
            return Err(FrankenError::TooBig);
        }
        let minimum = read_count.checked_mul(12)
            .and_then(|n| write_count.checked_mul(8).and_then(|m| n.checked_add(m)))
            .ok_or(FrankenError::TooBig)?;
        if minimum > input.0.len() {
            return Err(corrupt("native page capsule counts exceed its payload"));
        }
        let mut reads = Vec::new();
        reads.try_reserve_exact(read_count).map_err(|_| FrankenError::OutOfMemory)?;
        for _ in 0..read_count {
            let page = input.page()?;
            let version = CommitSeq::new(u64::from_le_bytes(input.array()?));
            reads.push((page, version));
        }
        let mut writes = Vec::new();
        writes.try_reserve_exact(write_count).map_err(|_| FrankenError::OutOfMemory)?;
        let page_len = usize::try_from(page_size).map_err(|_| FrankenError::TooBig)?;
        for _ in 0..write_count {
            let page = input.page()?;
            let data = match u32::from_le_bytes(input.array()?) {
                0 => None,
                1 => Some(Arc::from(input.take(page_len)?)),
                _ => return Err(corrupt("invalid native page mutation tag")),
            };
            writes.push(NativePageWrite { page, data });
        }
        if !input.0.is_empty() {
            return Err(corrupt("trailing native page capsule bytes"));
        }
        let capsule = Self { page_size, snapshot, reads, writes };
        capsule.encoded_len()?;
        Ok(capsule)
    }

    /// Bind a stored capsule to the exact admission metadata used by the driver.
    ///
    /// This profile stores its read observations inside the capsule, not in
    /// untyped external SSI objects. Nonempty external evidence lists are
    /// refused rather than being accepted without checking their semantics.
    ///
    /// # Errors
    /// Rejects digest, snapshot, write-set, or profile/evidence mismatches.
    pub fn from_candidate(candidate: &NativeCommitCandidate) -> Result<Self> {
        let capsule = Self::from_bytes(&candidate.capsule)?;
        let proof = &candidate.proof;
        let submission = &proof.submission;
        if capsule.snapshot != submission.begin_seq || proof.commit_seq <= capsule.snapshot
            || blake3::hash(&candidate.capsule).as_bytes() != &submission.capsule_digest
            || !submission.write_set_pages.iter().copied().eq(capsule.writes.iter().map(|w| w.page))
            || !submission.witness_refs.is_empty()
            || !submission.edge_ids.is_empty()
            || !submission.merge_witness_ids.is_empty()
        {
            return Err(corrupt("native page capsule does not match its admission proof"));
        }
        Ok(capsule)
    }

    /// Validate every observation against a complete current page-version map.
    ///
    /// `latest` must return the last committed version, including deletion
    /// versions, or zero for a page never committed. A stale absence is a
    /// conflict even when the page is absent again now. The caller must retain
    /// commit-order authority from this check through physical publication.
    ///
    /// # Errors
    /// Returns a snapshot conflict for changed pages; rejects future snapshots.
    pub fn validate_snapshot(
        &self,
        committed_tip: CommitSeq,
        mut latest: impl FnMut(PageNumber) -> CommitSeq,
    ) -> Result<()> {
        self.encoded_len()?;
        if self.snapshot > committed_tip {
            return Err(corrupt("native page transaction has a future snapshot"));
        }
        for &(page, observed) in &self.reads {
            if latest(page) != observed {
                return Err(FrankenError::BusySnapshot { conflicting_pages: page.get().to_string() });
            }
        }
        Ok(())
    }

    fn encoded_len(&self) -> Result<usize> {
        validate_page_size(self.page_size)?;
        if self.reads.len() > MAX_CAPSULE_PAGES || self.writes.is_empty()
            || self.writes.len() > MAX_CAPSULE_PAGES
        {
            return Err(FrankenError::TooBig);
        }
        if self.reads.windows(2).any(|w| w[0].0 >= w[1].0)
            || self.writes.windows(2).any(|w| w[0].page >= w[1].page)
            || self.reads.iter().any(|(_, seq)| *seq > self.snapshot)
        {
            return Err(corrupt("unordered or future native page observations"));
        }
        let mut len = self.reads.len().checked_mul(12)
            .and_then(|n| n.checked_add(HEADER_BYTES)).ok_or(FrankenError::TooBig)?;
        let page_size = usize::try_from(self.page_size).map_err(|_| FrankenError::TooBig)?;
        for write in &self.writes {
            if self.reads.binary_search_by_key(&write.page, |(page, _)| *page).is_err() {
                return Err(corrupt("native page write has no snapshot observation"));
            }
            if write.data.as_ref().is_some_and(|data| data.len() != page_size) {
                return Err(corrupt("native page image has the wrong size"));
            }
            len = len.checked_add(8)
                .and_then(|n| n.checked_add(write.data.as_ref().map_or(0, |data| data.len())))
                .filter(|n| *n <= MAX_PAGE_CAPSULE_BYTES).ok_or(FrankenError::TooBig)?;
        }
        Ok(len)
    }
}

fn validate_page_size(size: u32) -> Result<()> {
    if !(512..=65_536).contains(&size) || !size.is_power_of_two() {
        return Err(corrupt("invalid native capsule page size"));
    }
    Ok(())
}

fn count_u32(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| FrankenError::TooBig)
}

fn corrupt(detail: &str) -> FrankenError {
    FrankenError::WalCorrupt { detail: detail.to_owned() }
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let (head, tail) = self.0.split_at_checked(len)
            .ok_or_else(|| corrupt("truncated native page capsule"))?;
        self.0 = tail;
        Ok(head)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?.try_into().map_err(|_| corrupt("truncated native page field"))
    }
    fn page(&mut self) -> Result<PageNumber> {
        PageNumber::new(u32::from_le_bytes(self.array()?))
            .ok_or_else(|| corrupt("zero native page number"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_commit::CommitSubmission;
    use crate::native_commit::durable::NativeCommitProof;
    use fsqlite_types::{ObjectId, TxnEpoch, TxnId, TxnToken};

    fn page(n: u32) -> PageNumber { PageNumber::new(n).unwrap() }
    fn capsule() -> NativePageCapsule {
        NativePageCapsule {
            page_size: 512,
            snapshot: CommitSeq::new(7),
            reads: vec![(page(1), CommitSeq::new(5)), (page(3), CommitSeq::ZERO)],
            writes: vec![
                NativePageWrite { page: page(1), data: None },
                NativePageWrite { page: page(3), data: Some(Arc::from(vec![0xA5; 512])) },
            ],
        }
    }
    fn candidate() -> NativeCommitCandidate {
        let capsule = capsule();
        let bytes = capsule.to_bytes().unwrap();
        NativeCommitCandidate {
            proof: NativeCommitProof {
                commit_seq: CommitSeq::new(8),
                commit_time_unix_ns: 123,
                submission: CommitSubmission {
                    capsule_object_id: ObjectId::derive_from_canonical_bytes(&bytes),
                    capsule_digest: *blake3::hash(&bytes).as_bytes(),
                    write_set_pages: vec![page(1), page(3)],
                    witness_refs: vec![], edge_ids: vec![], merge_witness_ids: vec![],
                    txn_token: TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(1)),
                    begin_seq: CommitSeq::new(7),
                },
            },
            proof_object_id: ObjectId::from_bytes([1; 16]),
            capsule: Arc::from(bytes),
        }
    }

    #[test]
    fn capsule_wire_has_exact_header_and_retains_tombstones() {
        let bytes = capsule().to_bytes().unwrap();
        assert_eq!(bytes.len(), 28 + 24 + 16 + 512);
        assert_eq!(&bytes[..8], b"FNPG\x01\0\0\0");
        assert_eq!(&bytes[8..12], &512_u32.to_le_bytes());
        assert_eq!(&bytes[12..20], &7_u64.to_le_bytes());
        assert_eq!(&bytes[20..28], &[2, 0, 0, 0, 2, 0, 0, 0]);
        assert_eq!(&bytes[52..60], &[1, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(NativePageCapsule::from_bytes(&bytes).unwrap(), capsule());
        assert_eq!(NativePageCapsule::from_candidate(&candidate()).unwrap(), capsule());
    }

    #[test]
    fn capsule_rejects_every_truncation_and_trailing_data() {
        let bytes = capsule().to_bytes().unwrap();
        for end in 0..bytes.len() {
            assert!(NativePageCapsule::from_bytes(&bytes[..end]).is_err(), "end={end}");
        }
        let mut bytes = bytes;
        bytes.push(0);
        assert!(NativePageCapsule::from_bytes(&bytes).is_err());
    }

    #[test]
    fn capsule_rejects_forged_wire_counts_pages_and_tags() {
        for (offset, value) in [(4, 2), (20, 0xFF), (24, 0xFF), (28, 0), (56, 2)] {
            let mut bytes = capsule().to_bytes().unwrap();
            bytes[offset] = value;
            assert!(NativePageCapsule::from_bytes(&bytes).is_err(), "offset={offset}");
        }
        let mut bytes = capsule().to_bytes().unwrap();
        bytes[20..24].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(NativePageCapsule::from_bytes(&bytes).is_err());
    }

    #[test]
    fn capsule_requires_ordered_complete_write_observations() {
        let mut missing = capsule(); missing.reads.pop();
        assert!(missing.to_bytes().is_err());
        let mut duplicate = capsule(); duplicate.reads.push((page(3), CommitSeq::ZERO));
        assert!(duplicate.to_bytes().is_err());
        let mut duplicate = capsule(); duplicate.writes.push(duplicate.writes[1].clone());
        assert!(duplicate.to_bytes().is_err());
        let mut reversed = capsule(); reversed.writes.reverse();
        assert!(reversed.to_bytes().is_err());
        let mut future = capsule(); future.reads[0].1 = CommitSeq::new(8);
        assert!(future.to_bytes().is_err());
    }

    #[test]
    fn capsule_rejects_invalid_page_images_and_page_sizes() {
        for size in [0, 256, 513, 131_072] {
            let mut value = capsule(); value.page_size = size;
            assert!(value.to_bytes().is_err());
        }
        let mut short = capsule(); short.writes[1].data = Some(Arc::from(vec![1; 511]));
        assert!(short.to_bytes().is_err());
        let mut empty = capsule(); empty.writes.clear();
        assert!(empty.to_bytes().is_err());
    }

    #[test]
    fn capsule_binding_rejects_forged_admission_metadata() {
        for case in 0..6 {
            let mut value = candidate();
            match case {
                0 => value.proof.submission.begin_seq = CommitSeq::new(6),
                1 => value.proof.submission.write_set_pages.pop().map(|_| ()).unwrap(),
                2 => value.proof.submission.capsule_digest[0] ^= 1,
                3 => value.proof.submission.witness_refs.push(ObjectId::from_bytes([9; 16])),
                4 => value.proof.submission.edge_ids.push(ObjectId::from_bytes([9; 16])),
                5 => value.proof.submission.merge_witness_ids.push(ObjectId::from_bytes([9; 16])),
                _ => unreachable!(),
            }
            assert!(NativePageCapsule::from_candidate(&value).is_err(), "case={case}");
        }
    }

    #[test]
    fn snapshot_validation_checks_absence_and_not_only_written_images() {
        let mut value = capsule();
        value.reads.push((page(5), CommitSeq::new(6))); // Read-only dependency.
        let stable = |p: PageNumber| match p.get() { 1 => CommitSeq::new(5), 5 => CommitSeq::new(6), _ => CommitSeq::ZERO };
        value.validate_snapshot(CommitSeq::new(7), stable).unwrap();
        for changed in [1, 3, 5] {
            assert!(matches!(value.validate_snapshot(CommitSeq::new(8), |p| {
                if p.get() == changed { CommitSeq::new(8) } else { stable(p) }
            }), Err(FrankenError::BusySnapshot { .. })));
        }
        assert!(value.validate_snapshot(CommitSeq::new(6), stable).is_err());
    }
}
