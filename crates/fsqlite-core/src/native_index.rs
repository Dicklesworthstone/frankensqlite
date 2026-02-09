//! Native index lookup, repair, and deterministic segment construction
//! (§3.6.4-§3.6.7).
//!
//! This module implements the read-path lookup algorithm used by Native mode:
//! cache -> presence filter -> index scan -> fetch+materialize.

use std::collections::BTreeMap;

use crate::commit_marker::{
    CommitMarkerRecord, MARKER_SEGMENT_HEADER_BYTES, MarkerSegmentHeader, recover_valid_prefix,
};
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::ecs::{PageVersionIndexSegment, PatchKind, VersionPointer};
use fsqlite_types::{ObjectId, PageNumber};
use tracing::{debug, error, info, warn};

const NATIVE_INDEX_BEAD_ID: &str = "bd-1hi.32";
const NATIVE_INDEX_REPAIR_BEAD_ID: &str = "bd-1hi.33";
const NATIVE_INDEX_LOGGING_STANDARD: &str = "bd-1fpm";
const MAX_PATCH_DEPTH: usize = 8;
const DEFAULT_MAX_REPAIR_SYMBOL_LOSS_RATE: f64 = 0.25;

/// Provider for base-page bytes (step 2 fallback / step 4 materialization).
pub trait BasePageProvider {
    /// Load a base page image.
    ///
    /// # Errors
    ///
    /// Returns an I/O or corruption error when the page cannot be read.
    fn load_base_page(&self, page: PageNumber) -> Result<Vec<u8>>;
}

/// Provider for ECS patch object payload bytes.
pub trait PatchObjectStore {
    /// Fetch an ECS object payload by `ObjectId`.
    ///
    /// # Errors
    ///
    /// Returns an error when the object is missing/corrupt/unreadable.
    fn fetch_patch_object(&self, object_id: ObjectId) -> Result<Vec<u8>>;
}

/// Hot-path cache keyed by `(page, snapshot_high)`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NativePageCache {
    entries: BTreeMap<(u32, u64), Vec<u8>>,
}

impl NativePageCache {
    /// Insert a materialized page for a specific visibility bound.
    pub fn insert(&mut self, page: PageNumber, snapshot_high: u64, bytes: Vec<u8>) {
        self.entries.insert((page.get(), snapshot_high), bytes);
    }

    /// Get cached page bytes for `(page, snapshot_high)`.
    #[must_use]
    pub fn get(&self, page: PageNumber, snapshot_high: u64) -> Option<&[u8]> {
        self.entries
            .get(&(page.get(), snapshot_high))
            .map(Vec::as_slice)
    }
}

/// Structured debug trace for a single lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LookupTrace {
    /// True when step 1 returned from cache.
    pub cache_hit: bool,
    /// True when the presence filter indicated a possible version.
    pub filter_hit: bool,
    /// Number of index segments scanned during step 3.
    pub segment_scans: u64,
    /// Resolved commit sequence, if any.
    pub resolved_commit_seq: Option<u64>,
}

/// Result of native lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupResult {
    /// Materialized page bytes visible under the snapshot.
    pub page_bytes: Vec<u8>,
    /// Resolved version pointer (if lookup found one).
    pub resolved_pointer: Option<VersionPointer>,
    /// Structured path telemetry.
    pub trace: LookupTrace,
}

/// Deterministic index segment build output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltIndexSegment {
    /// Built segment payload.
    pub segment: PageVersionIndexSegment,
    /// Content-addressed deterministic object id for the segment.
    pub object_id: ObjectId,
}

/// Input policy for repair/rebuild aggressiveness (§3.6.7).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BoldnessConstraint {
    /// Permit emergency linear scan when the native index is unavailable.
    pub allow_emergency_linear_scan: bool,
    /// Maximum tolerated symbol-loss estimate before aggressive repair is blocked.
    pub max_repair_symbol_loss_rate: f64,
}

impl BoldnessConstraint {
    /// Strict policy used by default: no emergency scans and conservative repair.
    #[must_use]
    pub const fn strict() -> Self {
        Self {
            allow_emergency_linear_scan: false,
            max_repair_symbol_loss_rate: DEFAULT_MAX_REPAIR_SYMBOL_LOSS_RATE,
        }
    }

    /// Emergency policy enabling linear-scan fallback.
    #[must_use]
    pub const fn emergency() -> Self {
        Self {
            allow_emergency_linear_scan: true,
            max_repair_symbol_loss_rate: DEFAULT_MAX_REPAIR_SYMBOL_LOSS_RATE,
        }
    }

    #[must_use]
    fn permits_repair(self, symbol_loss_rate_estimate: f64) -> bool {
        symbol_loss_rate_estimate <= self.max_repair_symbol_loss_rate
    }
}

impl Default for BoldnessConstraint {
    fn default() -> Self {
        Self::strict()
    }
}

/// Manifest entry for one native index segment object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeIndexSegmentRef {
    /// Inclusive start commit sequence covered by the segment.
    pub start_seq: u64,
    /// Inclusive end commit sequence covered by the segment.
    pub end_seq: u64,
    /// Deterministic ECS object id of the segment payload.
    pub object_id: ObjectId,
}

/// Source of persisted index segments.
pub trait NativeIndexSegmentStore {
    /// Load one index segment object by id.
    ///
    /// # Errors
    ///
    /// Returns an error when the segment is unavailable or corrupted.
    fn fetch_index_segment(&self, object_id: ObjectId) -> Result<PageVersionIndexSegment>;
}

/// Source of per-commit page-version updates recoverable from commit capsules.
pub trait CommitCapsuleIndexSource {
    /// Return all page updates encoded by a commit capsule.
    ///
    /// # Errors
    ///
    /// Returns an error when the capsule cannot be decoded or verified.
    fn updates_for_commit(
        &self,
        commit_seq: u64,
        capsule_object_id: ObjectId,
    ) -> Result<Vec<(PageNumber, VersionPointer)>>;
}

/// Summary of native index repair attempts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRepairReport {
    /// Repaired segments ordered by `end_seq`.
    pub segments: Vec<PageVersionIndexSegment>,
    /// Segments recovered from local symbols.
    pub repaired_from_local: u64,
    /// Segments recovered from remote symbols.
    pub repaired_from_remote: u64,
}

/// Summary of deterministic full rebuild results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRebuildReport {
    /// Ordered commit markers used to rebuild the index.
    pub markers: Vec<CommitMarkerRecord>,
    /// Built index segments.
    pub segments: Vec<BuiltIndexSegment>,
}

/// In-memory deterministic builder for `PageVersionIndexSegment`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentBuilder {
    max_entries: usize,
    start_seq: Option<u64>,
    end_seq: Option<u64>,
    pending: BTreeMap<(u32, u64), VersionPointer>,
}

impl SegmentBuilder {
    /// Create a new builder.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::OutOfRange`] when `max_entries == 0`.
    pub fn new(max_entries: usize) -> Result<Self> {
        if max_entries == 0 {
            return Err(FrankenError::OutOfRange {
                what: "segment_builder.max_entries".to_owned(),
                value: "0".to_owned(),
            });
        }
        Ok(Self {
            max_entries,
            start_seq: None,
            end_seq: None,
            pending: BTreeMap::new(),
        })
    }

    /// Ingest one commit worth of page-version updates.
    ///
    /// Returns `Some(segment)` when the builder auto-flushes due to `max_entries`.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::TypeMismatch`] if a pointer commit sequence does
    /// not match `commit_seq`.
    pub fn ingest_commit(
        &mut self,
        commit_seq: u64,
        updates: impl IntoIterator<Item = (PageNumber, VersionPointer)>,
    ) -> Result<Option<BuiltIndexSegment>> {
        debug!(
            bead_id = NATIVE_INDEX_BEAD_ID,
            logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
            commit_seq = commit_seq,
            "segment builder ingesting commit updates"
        );

        for (page, pointer) in updates {
            if pointer.commit_seq != commit_seq {
                return Err(FrankenError::TypeMismatch {
                    expected: format!("pointer.commit_seq == {commit_seq}"),
                    actual: pointer.commit_seq.to_string(),
                });
            }
            self.pending
                .insert((page.get(), pointer.commit_seq), pointer);
        }

        self.start_seq = Some(match self.start_seq {
            Some(start) => start.min(commit_seq),
            None => commit_seq,
        });
        self.end_seq = Some(match self.end_seq {
            Some(end) => end.max(commit_seq),
            None => commit_seq,
        });

        if self.pending.len() >= self.max_entries {
            self.flush()
        } else {
            Ok(None)
        }
    }

    /// Flush pending updates into a deterministic segment.
    ///
    /// # Errors
    ///
    /// Returns corruption errors when internal state is inconsistent.
    pub fn flush(&mut self) -> Result<Option<BuiltIndexSegment>> {
        if self.pending.is_empty() {
            return Ok(None);
        }

        let start_seq = self
            .start_seq
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "segment builder missing start_seq".to_owned(),
            })?;
        let end_seq = self.end_seq.ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "segment builder missing end_seq".to_owned(),
        })?;

        let mut entries = Vec::with_capacity(self.pending.len());
        for ((page_raw, _commit_seq), pointer) in &self.pending {
            let page = PageNumber::new(*page_raw).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!("segment builder produced invalid page number {page_raw}"),
            })?;
            entries.push((page, *pointer));
        }

        let segment = PageVersionIndexSegment::new(start_seq, end_seq, entries);
        let object_id = derive_segment_object_id(&segment);

        info!(
            bead_id = NATIVE_INDEX_BEAD_ID,
            logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
            start_seq = start_seq,
            end_seq = end_seq,
            segments_built = 1_u8,
            entry_count = segment.entries.len(),
            object_id = %object_id,
            "segment builder flush complete"
        );

        self.pending.clear();
        self.start_seq = None;
        self.end_seq = None;

        Ok(Some(BuiltIndexSegment { segment, object_id }))
    }
}

/// Perform native index lookup for one page under `snapshot_high`.
///
/// Implements:
/// 1) cache lookup
/// 2) presence filter check (fast negative path)
/// 3) backward segment scan
/// 4) patch fetch + materialization.
///
/// # Errors
///
/// Returns corruption errors for malformed patches and provider errors for I/O.
pub fn lookup_page_version(
    page: PageNumber,
    snapshot_high: u64,
    segments: &[PageVersionIndexSegment],
    cache: &mut NativePageCache,
    base_provider: &impl BasePageProvider,
    patch_store: &impl PatchObjectStore,
    symbol_loss_rate_estimate: f64,
) -> Result<LookupResult> {
    debug!(
        bead_id = NATIVE_INDEX_BEAD_ID,
        logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
        page = page.get(),
        snapshot_high = snapshot_high,
        "native index lookup started"
    );
    let mut deps = LookupDeps {
        cache,
        base_provider,
        patch_store,
        symbol_loss_rate_estimate,
    };

    if let Some(cached) = deps.cache.get(page, snapshot_high) {
        log_lookup_path(true, false, 0, None);
        return Ok(LookupResult {
            page_bytes: cached.to_vec(),
            resolved_pointer: None,
            trace: LookupTrace {
                cache_hit: true,
                filter_hit: false,
                segment_scans: 0,
                resolved_commit_seq: None,
            },
        });
    }

    let filter_hit = version_maybe_present(page, snapshot_high, segments);
    if !filter_hit {
        return base_fallback_result(
            page,
            snapshot_high,
            deps.cache,
            deps.base_provider,
            false,
            0,
        );
    }

    let (resolved_pointer, segment_scans) =
        lookup_pointer_in_segments(page, snapshot_high, segments);
    let Some(pointer) = resolved_pointer else {
        return base_fallback_result(
            page,
            snapshot_high,
            deps.cache,
            deps.base_provider,
            true,
            segment_scans,
        );
    };

    materialized_result(page, snapshot_high, pointer, segment_scans, &mut deps)
}

struct LookupDeps<'a> {
    cache: &'a mut NativePageCache,
    base_provider: &'a dyn BasePageProvider,
    patch_store: &'a dyn PatchObjectStore,
    symbol_loss_rate_estimate: f64,
}

fn base_fallback_result(
    page: PageNumber,
    snapshot_high: u64,
    cache: &mut NativePageCache,
    base_provider: &(impl BasePageProvider + ?Sized),
    filter_hit: bool,
    segment_scans: u64,
) -> Result<LookupResult> {
    let base = base_provider.load_base_page(page)?;
    cache.insert(page, snapshot_high, base.clone());
    log_lookup_path(false, filter_hit, segment_scans, None);
    Ok(LookupResult {
        page_bytes: base,
        resolved_pointer: None,
        trace: LookupTrace {
            cache_hit: false,
            filter_hit,
            segment_scans,
            resolved_commit_seq: None,
        },
    })
}

fn materialized_result(
    page: PageNumber,
    snapshot_high: u64,
    pointer: VersionPointer,
    segment_scans: u64,
    deps: &mut LookupDeps<'_>,
) -> Result<LookupResult> {
    let patch_bytes = deps.patch_store.fetch_patch_object(pointer.patch_object)?;
    let base_bytes = deps.base_provider.load_base_page(page)?;
    let page_bytes = materialize_patch(pointer, &patch_bytes, &base_bytes, deps.patch_store, 0)?;
    if pointer.patch_kind != PatchKind::FullImage {
        warn!(
            bead_id = NATIVE_INDEX_BEAD_ID,
            logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
            object_id = %pointer.patch_object,
            symbol_loss_rate_estimate = deps.symbol_loss_rate_estimate,
            "native index lookup used repair/materialization path"
        );
    }

    deps.cache.insert(page, snapshot_high, page_bytes.clone());
    log_lookup_path(false, true, segment_scans, Some(pointer.commit_seq));
    info!(
        bead_id = NATIVE_INDEX_BEAD_ID,
        logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
        page = page.get(),
        snapshot_high = snapshot_high,
        resolved_commit_seq = pointer.commit_seq,
        "native index lookup resolved page version"
    );
    Ok(LookupResult {
        page_bytes,
        resolved_pointer: Some(pointer),
        trace: LookupTrace {
            cache_hit: false,
            filter_hit: true,
            segment_scans,
            resolved_commit_seq: Some(pointer.commit_seq),
        },
    })
}

fn log_lookup_path(
    cache_hit: bool,
    filter_hit: bool,
    segment_scans: u64,
    resolved_commit_seq: Option<u64>,
) {
    debug!(
        bead_id = NATIVE_INDEX_BEAD_ID,
        logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
        cache_hit = cache_hit,
        filter_hit = filter_hit,
        segment_scans = segment_scans,
        resolved_commit_seq = ?resolved_commit_seq,
        "lookup path chosen"
    );
}

#[must_use]
fn version_maybe_present(
    page: PageNumber,
    snapshot_high: u64,
    segments: &[PageVersionIndexSegment],
) -> bool {
    segments
        .iter()
        .any(|segment| segment.start_seq <= snapshot_high && segment.bloom.maybe_contains(page))
}

#[must_use]
fn lookup_pointer_in_segments(
    page: PageNumber,
    snapshot_high: u64,
    segments: &[PageVersionIndexSegment],
) -> (Option<VersionPointer>, u64) {
    let mut ordered: Vec<&PageVersionIndexSegment> = segments.iter().collect();
    ordered.sort_by_key(|segment| segment.end_seq);
    ordered.reverse();

    let mut scans = 0_u64;
    for segment in ordered {
        if segment.start_seq > snapshot_high {
            continue;
        }
        scans = scans.saturating_add(1);
        if let Some(pointer) = segment.lookup(page, snapshot_high) {
            return (Some(*pointer), scans);
        }
    }
    (None, scans)
}

fn materialize_patch(
    pointer: VersionPointer,
    patch_bytes: &[u8],
    base_page: &[u8],
    patch_store: &(impl PatchObjectStore + ?Sized),
    depth: usize,
) -> Result<Vec<u8>> {
    if depth > MAX_PATCH_DEPTH {
        error!(
            bead_id = NATIVE_INDEX_BEAD_ID,
            logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
            reason_code = "materialize_depth_exceeded",
            depth = depth,
            "recursive patch materialization depth exceeded"
        );
        return Err(FrankenError::DatabaseCorrupt {
            detail: "patch materialization depth exceeded".to_owned(),
        });
    }

    match pointer.patch_kind {
        PatchKind::FullImage => Ok(patch_bytes.to_vec()),
        PatchKind::IntentLog => {
            let mut out = resolve_base_bytes(pointer, base_page, patch_store)?;
            apply_intent_log_patch(&mut out, patch_bytes)?;
            Ok(out)
        }
        PatchKind::SparseXor => {
            let mut out = resolve_base_bytes(pointer, base_page, patch_store)?;
            apply_sparse_xor_patch(&mut out, patch_bytes)?;
            Ok(out)
        }
    }
}

fn resolve_base_bytes(
    pointer: VersionPointer,
    base_page: &[u8],
    patch_store: &(impl PatchObjectStore + ?Sized),
) -> Result<Vec<u8>> {
    if let Some(base_object) = pointer.base_hint {
        patch_store.fetch_patch_object(base_object)
    } else {
        Ok(base_page.to_vec())
    }
}

fn apply_intent_log_patch(out: &mut [u8], patch_bytes: &[u8]) -> Result<()> {
    let mut cursor = 0_usize;
    let op_count = read_u8(patch_bytes, &mut cursor, "intent.op_count")?;
    for op_index in 0..op_count {
        let offset = usize::from(read_u16_le(patch_bytes, &mut cursor, "intent.op.offset")?);
        let len = usize::from(read_u16_le(patch_bytes, &mut cursor, "intent.op.len")?);
        let data = read_slice(patch_bytes, &mut cursor, len, "intent.op.data")?;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "intent patch offset overflow".to_owned(),
            })?;
        if end > out.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "intent patch op {op_index} out of bounds: end={end}, page_len={}",
                    out.len()
                ),
            });
        }
        out[offset..end].copy_from_slice(data);
    }
    if cursor != patch_bytes.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "intent patch trailing bytes: parsed={cursor}, actual={}",
                patch_bytes.len()
            ),
        });
    }
    Ok(())
}

fn apply_sparse_xor_patch(out: &mut [u8], patch_bytes: &[u8]) -> Result<()> {
    let mut cursor = 0_usize;
    let op_count = read_u8(patch_bytes, &mut cursor, "xor.op_count")?;
    for op_index in 0..op_count {
        let offset = usize::from(read_u16_le(patch_bytes, &mut cursor, "xor.op.offset")?);
        let len = usize::from(read_u16_le(patch_bytes, &mut cursor, "xor.op.len")?);
        let data = read_slice(patch_bytes, &mut cursor, len, "xor.op.data")?;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "sparse-xor patch offset overflow".to_owned(),
            })?;
        if end > out.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "sparse-xor patch op {op_index} out of bounds: end={end}, page_len={}",
                    out.len()
                ),
            });
        }
        for (dst, delta) in out[offset..end].iter_mut().zip(data.iter()) {
            *dst ^= *delta;
        }
    }
    if cursor != patch_bytes.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "sparse-xor patch trailing bytes: parsed={cursor}, actual={}",
                patch_bytes.len()
            ),
        });
    }
    Ok(())
}

fn read_u8(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<u8> {
    let end = cursor
        .checked_add(1)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!("{field} overflow"),
        })?;
    if end > bytes.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("{field} out of bounds: end={end}, len={}", bytes.len()),
        });
    }
    let value = bytes[*cursor];
    *cursor = end;
    Ok(value)
}

fn read_u16_le(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<u16> {
    let end = cursor
        .checked_add(2)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!("{field} overflow"),
        })?;
    if end > bytes.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("{field} out of bounds: end={end}, len={}", bytes.len()),
        });
    }
    let raw = [bytes[*cursor], bytes[*cursor + 1]];
    *cursor = end;
    Ok(u16::from_le_bytes(raw))
}

fn read_slice<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
    field: &str,
) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!("{field} overflow"),
        })?;
    if end > bytes.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("{field} out of bounds: end={end}, len={}", bytes.len()),
        });
    }
    let slice = &bytes[*cursor..end];
    *cursor = end;
    Ok(slice)
}

/// Deterministically derive the ECS object id of an index segment.
#[must_use]
pub fn derive_segment_object_id(segment: &PageVersionIndexSegment) -> ObjectId {
    let canonical = canonical_segment_bytes(segment);
    ObjectId::derive_from_canonical_bytes(&canonical)
}

#[must_use]
fn canonical_segment_bytes(segment: &PageVersionIndexSegment) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&segment.start_seq.to_le_bytes());
    out.extend_from_slice(&segment.end_seq.to_le_bytes());
    let count = u64::try_from(segment.entries.len()).unwrap_or(u64::MAX);
    out.extend_from_slice(&count.to_le_bytes());
    for (page, pointer) in &segment.entries {
        out.extend_from_slice(&page.get().to_le_bytes());
        let vp_bytes = pointer.to_bytes();
        let vp_len = u64::try_from(vp_bytes.len()).unwrap_or(u64::MAX);
        out.extend_from_slice(&vp_len.to_le_bytes());
        out.extend_from_slice(&vp_bytes);
    }
    out
}

/// Critical preflight check before repair/rebuild attempts.
///
/// # Errors
///
/// Returns [`FrankenError::DatabaseCorrupt`] with
/// `reason_code=index_unrebuildable_with_markers` when commit markers are
/// present but neither repair nor rebuild capability is available.
pub fn preflight_native_index_integrity(
    marker_segment_blobs: &[Vec<u8>],
    repair_available: bool,
    rebuild_available: bool,
) -> Result<()> {
    let markers = scan_commit_markers(marker_segment_blobs)?;
    if !markers.is_empty() && !repair_available && !rebuild_available {
        error!(
            bead_id = NATIVE_INDEX_REPAIR_BEAD_ID,
            logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
            reason_code = "index_unrebuildable_with_markers",
            marker_count = markers.len(),
            repair_available = repair_available,
            rebuild_available = rebuild_available,
            "critical integrity failure detected before repair attempt"
        );
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "reason_code=index_unrebuildable_with_markers marker_count={} repair_available={repair_available} rebuild_available={rebuild_available}",
                markers.len()
            ),
        });
    }
    Ok(())
}

/// Repair index segments from surviving symbols without a full rebuild.
///
/// The loader first tries local symbols, then remote symbols.
///
/// # Errors
///
/// Returns:
/// - `reason_code=boldness_violation_blocked_repair` when the boldness policy blocks repair.
/// - `reason_code=index_repair_incomplete` when one or more segments are irrecoverable.
pub fn repair_index_segments_from_ecs(
    segment_refs: &[NativeIndexSegmentRef],
    local_store: &impl NativeIndexSegmentStore,
    remote_store: &impl NativeIndexSegmentStore,
    symbol_loss_rate_estimate: f64,
    boldness: BoldnessConstraint,
) -> Result<IndexRepairReport> {
    if !boldness.permits_repair(symbol_loss_rate_estimate) {
        warn!(
            bead_id = NATIVE_INDEX_REPAIR_BEAD_ID,
            logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
            reason_code = "boldness_violation_blocked_repair",
            symbol_loss_rate_estimate = symbol_loss_rate_estimate,
            max_repair_symbol_loss_rate = boldness.max_repair_symbol_loss_rate,
            "repair blocked by boldness constraint"
        );
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "reason_code=boldness_violation_blocked_repair symbol_loss_rate_estimate={symbol_loss_rate_estimate:.6} max_repair_symbol_loss_rate={:.6}",
                boldness.max_repair_symbol_loss_rate
            ),
        });
    }

    let mut ordered_refs = segment_refs.to_vec();
    ordered_refs.sort_by_key(|entry| (entry.start_seq, entry.end_seq, *entry.object_id.as_bytes()));

    debug!(
        bead_id = NATIVE_INDEX_REPAIR_BEAD_ID,
        logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
        segment_ref_count = ordered_refs.len(),
        "starting native index repair from surviving symbols"
    );

    let mut segments = Vec::with_capacity(ordered_refs.len());
    let mut repaired_from_local = 0_u64;
    let mut repaired_from_remote = 0_u64;
    let mut missing = Vec::new();

    for entry in ordered_refs {
        match try_fetch_valid_segment(entry, local_store) {
            Ok(segment) => {
                repaired_from_local = repaired_from_local.saturating_add(1);
                segments.push(segment);
                continue;
            }
            Err(local_error) => {
                warn!(
                    bead_id = NATIVE_INDEX_REPAIR_BEAD_ID,
                    logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
                    object_id = %entry.object_id,
                    start_seq = entry.start_seq,
                    end_seq = entry.end_seq,
                    error = %local_error,
                    "local segment fetch failed; trying remote recovery path"
                );
            }
        }

        match try_fetch_valid_segment(entry, remote_store) {
            Ok(segment) => {
                repaired_from_remote = repaired_from_remote.saturating_add(1);
                segments.push(segment);
            }
            Err(remote_error) => {
                error!(
                    bead_id = NATIVE_INDEX_REPAIR_BEAD_ID,
                    logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
                    object_id = %entry.object_id,
                    start_seq = entry.start_seq,
                    end_seq = entry.end_seq,
                    error = %remote_error,
                    reason_code = "index_repair_segment_irrecoverable",
                    "segment irrecoverable from both local and remote symbols"
                );
                missing.push(entry.object_id);
            }
        }
    }

    if !missing.is_empty() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "reason_code=index_repair_incomplete irrecoverable_segments={} first_irrecoverable_object={}",
                missing.len(),
                missing
                    .first()
                    .map_or_else(|| "none".to_owned(), ToString::to_string)
            ),
        });
    }

    segments.sort_by_key(|segment| segment.end_seq);

    info!(
        bead_id = NATIVE_INDEX_REPAIR_BEAD_ID,
        logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
        repaired_from_local = repaired_from_local,
        repaired_from_remote = repaired_from_remote,
        segments_repaired = segments.len(),
        "native index repair complete"
    );

    Ok(IndexRepairReport {
        segments,
        repaired_from_local,
        repaired_from_remote,
    })
}

/// Rebuild native index segments by replaying the commit marker stream.
///
/// # Errors
///
/// Returns [`FrankenError::DatabaseCorrupt`] with
/// `reason_code=index_unrebuildable_with_markers` when marker replay cannot
/// reconstruct commit updates.
pub fn rebuild_index_from_marker_stream(
    marker_segment_blobs: &[Vec<u8>],
    capsule_source: &impl CommitCapsuleIndexSource,
    max_entries: usize,
) -> Result<IndexRebuildReport> {
    let markers = scan_commit_markers(marker_segment_blobs)?;
    if markers.is_empty() {
        return Ok(IndexRebuildReport {
            markers,
            segments: Vec::new(),
        });
    }

    let start_seq = markers.first().map_or(0_u64, |record| record.commit_seq);
    let end_seq = markers.last().map_or(0_u64, |record| record.commit_seq);
    info!(
        bead_id = NATIVE_INDEX_REPAIR_BEAD_ID,
        logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
        start_seq = start_seq,
        end_seq = end_seq,
        segments_built = 0_u64,
        "native index rebuild start"
    );

    let mut builder = SegmentBuilder::new(max_entries)?;
    let mut built_segments = Vec::new();

    for marker in &markers {
        let capsule_id = ObjectId::from_bytes(marker.capsule_object_id);
        let updates =
            capsule_source
                .updates_for_commit(marker.commit_seq, capsule_id)
                .map_err(|source_error| {
                    error!(
                        bead_id = NATIVE_INDEX_REPAIR_BEAD_ID,
                        logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
                        reason_code = "index_unrebuildable_with_markers",
                        commit_seq = marker.commit_seq,
                        capsule_object_id = %capsule_id,
                        error = %source_error,
                        "marker stream exists but commit capsule updates are unrecoverable"
                    );
                    FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "reason_code=index_unrebuildable_with_markers commit_seq={} capsule_object_id={} source_error={source_error}",
                            marker.commit_seq, capsule_id
                        ),
                    }
                })?;

        if let Some(segment) = builder.ingest_commit(marker.commit_seq, updates)? {
            built_segments.push(segment);
        }
    }

    if let Some(segment) = builder.flush()? {
        built_segments.push(segment);
    }

    info!(
        bead_id = NATIVE_INDEX_REPAIR_BEAD_ID,
        logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
        start_seq = start_seq,
        end_seq = end_seq,
        segments_built = built_segments.len(),
        "native index rebuild complete"
    );

    Ok(IndexRebuildReport {
        markers,
        segments: built_segments,
    })
}

/// Emergency linear scan over commit markers when index segments are unavailable.
///
/// # Errors
///
/// Returns `reason_code=boldness_violation_blocked_linear_scan` if emergency
/// mode is not explicitly enabled.
pub fn emergency_linear_scan_lookup(
    page: PageNumber,
    snapshot_high: u64,
    marker_segment_blobs: &[Vec<u8>],
    capsule_source: &impl CommitCapsuleIndexSource,
    boldness: BoldnessConstraint,
    evidence_state: &str,
) -> Result<Option<VersionPointer>> {
    if !boldness.allow_emergency_linear_scan {
        warn!(
            bead_id = NATIVE_INDEX_REPAIR_BEAD_ID,
            logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
            reason_code = "boldness_violation_blocked_linear_scan",
            attempted_page = page.get(),
            attempted_snapshot_high = snapshot_high,
            evidence_state = evidence_state,
            "boldness violation attempt blocked"
        );
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "reason_code=boldness_violation_blocked_linear_scan attempted_page={} attempted_snapshot_high={} evidence_state={evidence_state}",
                page.get(),
                snapshot_high
            ),
        });
    }

    let markers = scan_commit_markers(marker_segment_blobs)?;
    for marker in markers.iter().rev() {
        if marker.commit_seq > snapshot_high {
            continue;
        }
        let capsule_id = ObjectId::from_bytes(marker.capsule_object_id);
        let updates = capsule_source.updates_for_commit(marker.commit_seq, capsule_id)?;
        if let Some((_, pointer)) = updates
            .into_iter()
            .find(|(candidate, pointer)| *candidate == page && pointer.commit_seq <= snapshot_high)
        {
            info!(
                bead_id = NATIVE_INDEX_REPAIR_BEAD_ID,
                logging_standard = NATIVE_INDEX_LOGGING_STANDARD,
                page = page.get(),
                snapshot_high = snapshot_high,
                resolved_commit_seq = pointer.commit_seq,
                "native index emergency linear scan resolved version pointer"
            );
            return Ok(Some(pointer));
        }
    }
    Ok(None)
}

fn scan_commit_markers(marker_segment_blobs: &[Vec<u8>]) -> Result<Vec<CommitMarkerRecord>> {
    let mut ordered_segments: Vec<(u64, Vec<CommitMarkerRecord>)> =
        Vec::with_capacity(marker_segment_blobs.len());
    for bytes in marker_segment_blobs {
        if bytes.len() < MARKER_SEGMENT_HEADER_BYTES {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "marker segment shorter than header: bytes={} header_bytes={MARKER_SEGMENT_HEADER_BYTES}",
                    bytes.len()
                ),
            });
        }
        let header =
            MarkerSegmentHeader::decode(&bytes[..MARKER_SEGMENT_HEADER_BYTES]).map_err(|err| {
                FrankenError::DatabaseCorrupt {
                    detail: format!("invalid marker segment header: {err}"),
                }
            })?;
        let records = recover_valid_prefix(bytes).map_err(|err| FrankenError::DatabaseCorrupt {
            detail: format!("marker segment prefix recovery failed: {err}"),
        })?;
        ordered_segments.push((header.start_commit_seq, records));
    }

    ordered_segments.sort_by_key(|(start_seq, _)| *start_seq);
    let mut combined = Vec::new();
    let mut expected_next_seq: Option<u64> = None;
    for (_, records) in ordered_segments {
        for record in records {
            if let Some(expected) = expected_next_seq
                && record.commit_seq != expected
            {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "marker stream commit gap: expected {expected}, found {}",
                        record.commit_seq
                    ),
                });
            }
            expected_next_seq = Some(record.commit_seq.saturating_add(1));
            combined.push(record);
        }
    }
    Ok(combined)
}

fn try_fetch_valid_segment(
    entry: NativeIndexSegmentRef,
    store: &impl NativeIndexSegmentStore,
) -> Result<PageVersionIndexSegment> {
    let segment = store.fetch_index_segment(entry.object_id)?;
    if segment.start_seq != entry.start_seq || segment.end_seq != entry.end_seq {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "segment bounds mismatch for object {}: expected [{}..={}], found [{}..={}]",
                entry.object_id, entry.start_seq, entry.end_seq, segment.start_seq, segment.end_seq
            ),
        });
    }
    let recomputed = derive_segment_object_id(&segment);
    if recomputed != entry.object_id {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "segment object id mismatch: expected {}, recomputed {}",
                entry.object_id, recomputed
            ),
        });
    }
    Ok(segment)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::commit_marker::{
        COMMIT_MARKER_RECORD_BYTES, CommitMarkerRecord, MARKER_SEGMENT_HEADER_BYTES,
        MarkerSegmentHeader,
    };

    type CapsuleKey = (u64, [u8; 16]);
    type CapsuleUpdates = Vec<(PageNumber, VersionPointer)>;

    #[derive(Debug, Clone)]
    struct TestBasePages {
        pages: BTreeMap<u32, Vec<u8>>,
        loads: Cell<u64>,
    }

    impl TestBasePages {
        fn new(entries: impl IntoIterator<Item = (PageNumber, Vec<u8>)>) -> Self {
            let mut pages = BTreeMap::new();
            for (page, bytes) in entries {
                pages.insert(page.get(), bytes);
            }
            Self {
                pages,
                loads: Cell::new(0),
            }
        }

        fn loads(&self) -> u64 {
            self.loads.get()
        }
    }

    impl BasePageProvider for TestBasePages {
        fn load_base_page(&self, page: PageNumber) -> Result<Vec<u8>> {
            self.loads.set(self.loads.get().saturating_add(1));
            self.pages
                .get(&page.get())
                .cloned()
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!("missing base page {}", page.get()),
                })
        }
    }

    #[derive(Debug, Clone)]
    struct TestPatchStore {
        objects: BTreeMap<[u8; 16], Vec<u8>>,
        fetches: Cell<u64>,
    }

    impl TestPatchStore {
        fn new(entries: impl IntoIterator<Item = (ObjectId, Vec<u8>)>) -> Self {
            let mut objects = BTreeMap::new();
            for (oid, payload) in entries {
                objects.insert(*oid.as_bytes(), payload);
            }
            Self {
                objects,
                fetches: Cell::new(0),
            }
        }

        fn fetches(&self) -> u64 {
            self.fetches.get()
        }
    }

    impl PatchObjectStore for TestPatchStore {
        fn fetch_patch_object(&self, object_id: ObjectId) -> Result<Vec<u8>> {
            self.fetches.set(self.fetches.get().saturating_add(1));
            self.objects
                .get(object_id.as_bytes())
                .cloned()
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!("missing patch object {object_id}"),
                })
        }
    }

    #[derive(Debug, Clone, Default)]
    struct TestSegmentStore {
        segments: BTreeMap<[u8; 16], PageVersionIndexSegment>,
    }

    impl TestSegmentStore {
        fn new(entries: impl IntoIterator<Item = (ObjectId, PageVersionIndexSegment)>) -> Self {
            let mut segments = BTreeMap::new();
            for (id, segment) in entries {
                segments.insert(*id.as_bytes(), segment);
            }
            Self { segments }
        }
    }

    impl NativeIndexSegmentStore for TestSegmentStore {
        fn fetch_index_segment(&self, object_id: ObjectId) -> Result<PageVersionIndexSegment> {
            self.segments
                .get(object_id.as_bytes())
                .cloned()
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!("missing index segment object {object_id}"),
                })
        }
    }

    #[derive(Debug, Clone, Default)]
    struct TestCapsuleSource {
        updates: BTreeMap<CapsuleKey, CapsuleUpdates>,
    }

    impl TestCapsuleSource {
        fn with_update(
            mut self,
            commit_seq: u64,
            capsule_seed: u8,
            updates: Vec<(PageNumber, VersionPointer)>,
        ) -> Self {
            self.updates
                .insert((commit_seq, [capsule_seed; 16]), updates);
            self
        }
    }

    impl CommitCapsuleIndexSource for TestCapsuleSource {
        fn updates_for_commit(
            &self,
            commit_seq: u64,
            capsule_object_id: ObjectId,
        ) -> Result<Vec<(PageNumber, VersionPointer)>> {
            self.updates
                .get(&(commit_seq, *capsule_object_id.as_bytes()))
                .cloned()
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "missing capsule updates commit_seq={commit_seq} capsule_object_id={capsule_object_id}"
                    ),
                })
        }
    }

    fn page(n: u32) -> PageNumber {
        PageNumber::new(n).expect("non-zero page number")
    }

    fn oid(seed: u8) -> ObjectId {
        ObjectId::from_bytes([seed; 16])
    }

    fn pointer(
        commit_seq: u64,
        patch_object_seed: u8,
        patch_kind: PatchKind,
        base_hint: Option<u8>,
    ) -> VersionPointer {
        VersionPointer {
            commit_seq,
            patch_object: oid(patch_object_seed),
            patch_kind,
            base_hint: base_hint.map(oid),
        }
    }

    fn encode_intent_patch(ops: &[(u16, &[u8])]) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(u8::try_from(ops.len()).expect("op count fits u8"));
        for (offset, data) in ops {
            out.extend_from_slice(&offset.to_le_bytes());
            out.extend_from_slice(
                &u16::try_from(data.len())
                    .expect("op data len fits u16")
                    .to_le_bytes(),
            );
            out.extend_from_slice(data);
        }
        out
    }

    fn encode_sparse_xor_patch(ops: &[(u16, &[u8])]) -> Vec<u8> {
        encode_intent_patch(ops)
    }

    fn marker_record(
        commit_seq: u64,
        capsule_seed: u8,
        prev_marker: [u8; 16],
    ) -> CommitMarkerRecord {
        CommitMarkerRecord::new(
            commit_seq,
            1_700_000_000_000_000_000_u64.saturating_add(commit_seq),
            [capsule_seed; 16],
            [capsule_seed.wrapping_add(1); 16],
            prev_marker,
        )
    }

    fn marker_segment_blob(start_seq: u64, records: &[CommitMarkerRecord]) -> Vec<u8> {
        let mut out = Vec::new();
        let segment_id = start_seq / 1_000_000;
        let header = MarkerSegmentHeader::new(segment_id, start_seq);
        out.extend_from_slice(&header.encode());
        for record in records {
            out.extend_from_slice(&record.encode());
        }
        let expected_len = MARKER_SEGMENT_HEADER_BYTES + COMMIT_MARKER_RECORD_BYTES * records.len();
        assert_eq!(out.len(), expected_len);
        out
    }

    #[test]
    fn test_lookup_latest_version() {
        let p = page(7);
        let vp10 = pointer(10, 0x10, PatchKind::FullImage, None);
        let vp20 = pointer(20, 0x20, PatchKind::FullImage, None);
        let seg10 = PageVersionIndexSegment::new(10, 10, vec![(p, vp10)]);
        let seg20 = PageVersionIndexSegment::new(20, 20, vec![(p, vp20)]);
        let base = TestBasePages::new([(p, b"base".to_vec())]);
        let store =
            TestPatchStore::new([(oid(0x10), b"v10".to_vec()), (oid(0x20), b"v20".to_vec())]);

        let mut cache = NativePageCache::default();
        let result_high = lookup_page_version(
            p,
            25,
            &[seg10.clone(), seg20.clone()],
            &mut cache,
            &base,
            &store,
            0.0,
        )
        .expect("lookup");
        assert_eq!(result_high.page_bytes, b"v20".to_vec());
        assert_eq!(result_high.resolved_pointer, Some(vp20));

        let mut cache2 = NativePageCache::default();
        let result_mid =
            lookup_page_version(p, 15, &[seg10, seg20], &mut cache2, &base, &store, 0.0)
                .expect("lookup");
        assert_eq!(result_mid.page_bytes, b"v10".to_vec());
        assert_eq!(result_mid.resolved_pointer, Some(vp10));
    }

    #[test]
    fn test_filter_negative_path_has_no_false_negatives() {
        let target = page(5);
        let other = page(9);
        let vp = pointer(12, 0x30, PatchKind::FullImage, None);
        let segment = PageVersionIndexSegment::new(12, 12, vec![(other, vp)]);
        let base = TestBasePages::new([(target, b"base5".to_vec()), (other, b"base9".to_vec())]);
        let store = TestPatchStore::new([(oid(0x30), b"other-version".to_vec())]);

        let mut cache = NativePageCache::default();
        let negative = lookup_page_version(
            target,
            20,
            std::slice::from_ref(&segment),
            &mut cache,
            &base,
            &store,
            0.0,
        )
        .expect("negative path");
        assert!(!negative.trace.filter_hit);
        assert_eq!(negative.page_bytes, b"base5".to_vec());
        assert_eq!(store.fetches(), 0);

        let mut cache2 = NativePageCache::default();
        let positive = lookup_page_version(other, 20, &[segment], &mut cache2, &base, &store, 0.0)
            .expect("positive path");
        assert!(positive.trace.filter_hit);
        assert_eq!(positive.page_bytes, b"other-version".to_vec());
    }

    #[test]
    fn test_materialization_intent_and_sparse_xor() {
        let p = page(3);
        let base = TestBasePages::new([(p, b"aaaa".to_vec())]);
        let intent_ptr = pointer(10, 0x40, PatchKind::IntentLog, None);
        let xor_ptr = pointer(20, 0x50, PatchKind::SparseXor, None);
        let seg_intent = PageVersionIndexSegment::new(10, 10, vec![(p, intent_ptr)]);
        let seg_xor = PageVersionIndexSegment::new(20, 20, vec![(p, xor_ptr)]);

        let intent_patch = encode_intent_patch(&[(1, b"BC")]);
        let xor_patch = encode_sparse_xor_patch(&[(0, &[0x01, 0x02, 0x03, 0x04])]);
        let store = TestPatchStore::new([(oid(0x40), intent_patch), (oid(0x50), xor_patch)]);

        let mut cache = NativePageCache::default();
        let intent = lookup_page_version(
            p,
            10,
            &[seg_intent, seg_xor.clone()],
            &mut cache,
            &base,
            &store,
            0.0,
        )
        .expect("intent materialization");
        assert_eq!(intent.page_bytes, b"aBCa".to_vec());

        let mut cache2 = NativePageCache::default();
        let sparse = lookup_page_version(p, 20, &[seg_xor], &mut cache2, &base, &store, 0.0)
            .expect("xor materialization");
        assert_eq!(sparse.page_bytes, vec![96, 99, 98, 101]);
    }

    #[test]
    fn test_segment_construction_deterministic() {
        let mut builder_a = SegmentBuilder::new(16).expect("builder");
        let mut builder_b = SegmentBuilder::new(16).expect("builder");

        let updates_a = vec![
            (page(7), pointer(100, 0x60, PatchKind::FullImage, None)),
            (
                page(2),
                pointer(100, 0x61, PatchKind::IntentLog, Some(0x62)),
            ),
        ];
        let updates_b = vec![
            (
                page(2),
                pointer(100, 0x61, PatchKind::IntentLog, Some(0x62)),
            ),
            (page(7), pointer(100, 0x60, PatchKind::FullImage, None)),
        ];

        assert!(
            builder_a
                .ingest_commit(100, updates_a)
                .expect("ingest")
                .is_none()
        );
        assert!(
            builder_b
                .ingest_commit(100, updates_b)
                .expect("ingest")
                .is_none()
        );

        let built_a = builder_a.flush().expect("flush").expect("segment");
        let built_b = builder_b.flush().expect("flush").expect("segment");
        assert_eq!(built_a.segment.entries, built_b.segment.entries);
        assert_eq!(built_a.object_id, built_b.object_id);
    }

    fn run_e2e_path_case() {
        let p = page(11);
        let base_bytes = vec![0x10, 0x20, 0x30, 0x40];
        let base = TestBasePages::new([(p, base_bytes.clone())]);
        let pointer = pointer(30, 0x71, PatchKind::SparseXor, Some(0x70));
        let segment = PageVersionIndexSegment::new(30, 30, vec![(p, pointer)]);
        let xor_patch = encode_sparse_xor_patch(&[(2, &[0xFF, 0x0F])]);
        let store = TestPatchStore::new([(oid(0x70), base_bytes), (oid(0x71), xor_patch)]);

        let mut cache = NativePageCache::default();
        let first = lookup_page_version(
            p,
            30,
            std::slice::from_ref(&segment),
            &mut cache,
            &base,
            &store,
            0.02,
        )
        .expect("first lookup");
        assert!(!first.trace.cache_hit);
        assert!(first.trace.filter_hit);
        assert_eq!(first.trace.segment_scans, 1);
        assert_eq!(first.trace.resolved_commit_seq, Some(30));
        assert_eq!(first.page_bytes, vec![0x10, 0x20, 0xCF, 0x4F]);

        let second = lookup_page_version(p, 30, &[segment], &mut cache, &base, &store, 0.02)
            .expect("second lookup");
        assert!(second.trace.cache_hit);
        assert_eq!(second.page_bytes, first.page_bytes);
        assert_eq!(store.fetches(), 2);
        assert_eq!(base.loads(), 1);
    }

    #[test]
    fn test_e2e_cache_miss_filter_hit_index_scan_fetch_materialize() {
        run_e2e_path_case();
    }

    #[test]
    fn test_bd_1hi_32_unit_compliance_gate() {
        assert_eq!(NATIVE_INDEX_BEAD_ID, "bd-1hi.32");
        assert_eq!(NATIVE_INDEX_LOGGING_STANDARD, "bd-1fpm");
        let store = TestPatchStore::new(std::iter::empty::<(ObjectId, Vec<u8>)>());
        let err = materialize_patch(
            pointer(1, 0xAA, PatchKind::FullImage, None),
            b"",
            b"",
            &store,
            MAX_PATCH_DEPTH + 1,
        )
        .expect_err("depth guard");
        assert!(err.to_string().contains("depth exceeded"));
    }

    #[test]
    fn prop_bd_1hi_32_structure_compliance() {
        for seed in 1_u8..=16 {
            let mut builder = SegmentBuilder::new(8).expect("builder");
            let updates = vec![
                (
                    page(u32::from(seed)),
                    pointer(500, seed, PatchKind::FullImage, None),
                ),
                (
                    page(u32::from(seed) + 100),
                    pointer(500, seed.wrapping_add(1), PatchKind::SparseXor, Some(seed)),
                ),
            ];
            assert!(
                builder
                    .ingest_commit(500, updates)
                    .expect("ingest")
                    .is_none()
            );
            let built = builder.flush().expect("flush").expect("segment");
            assert!(!built.segment.entries.is_empty());
            let recomputed = derive_segment_object_id(&built.segment);
            assert_eq!(built.object_id, recomputed);
        }
    }

    #[test]
    fn test_e2e_bd_1hi_32_compliance() {
        run_e2e_path_case();
    }

    #[test]
    fn test_index_rebuild_from_markers() {
        let marker_100 = marker_record(100, 0x10, [0_u8; 16]);
        let marker_101 = marker_record(101, 0x11, marker_100.marker_id);
        let marker_blob = marker_segment_blob(100, &[marker_100, marker_101]);

        let source = TestCapsuleSource::default()
            .with_update(
                100,
                0x10,
                vec![(page(3), pointer(100, 0x80, PatchKind::FullImage, None))],
            )
            .with_update(
                101,
                0x11,
                vec![
                    (
                        page(3),
                        pointer(101, 0x81, PatchKind::IntentLog, Some(0x82)),
                    ),
                    (page(9), pointer(101, 0x83, PatchKind::FullImage, None)),
                ],
            );

        let rebuilt_a =
            rebuild_index_from_marker_stream(std::slice::from_ref(&marker_blob), &source, 2)
                .expect("rebuild from markers");
        let rebuilt_b = rebuild_index_from_marker_stream(&[marker_blob], &source, 2)
            .expect("deterministic rebuild");
        assert_eq!(rebuilt_a.segments, rebuilt_b.segments);

        let segments: Vec<PageVersionIndexSegment> = rebuilt_a
            .segments
            .iter()
            .map(|segment| segment.segment.clone())
            .collect();
        let (resolved, scans) = lookup_pointer_in_segments(page(3), 101, &segments);
        assert_eq!(
            resolved,
            Some(pointer(101, 0x81, PatchKind::IntentLog, Some(0x82)))
        );
        assert!(scans >= 1);
    }

    #[test]
    fn test_index_repair_from_ecs() {
        let p1 = page(4);
        let p2 = page(8);
        let seg_local = PageVersionIndexSegment::new(
            200,
            200,
            vec![(p1, pointer(200, 0x90, PatchKind::FullImage, None))],
        );
        let seg_remote = PageVersionIndexSegment::new(
            201,
            201,
            vec![(p2, pointer(201, 0x91, PatchKind::SparseXor, Some(0x92)))],
        );
        let id_local = derive_segment_object_id(&seg_local);
        let id_remote = derive_segment_object_id(&seg_remote);

        let refs = vec![
            NativeIndexSegmentRef {
                start_seq: 200,
                end_seq: 200,
                object_id: id_local,
            },
            NativeIndexSegmentRef {
                start_seq: 201,
                end_seq: 201,
                object_id: id_remote,
            },
        ];

        let local_store = TestSegmentStore::new([(id_local, seg_local.clone())]);
        let remote_store = TestSegmentStore::new([(id_local, seg_local), (id_remote, seg_remote)]);
        let report = repair_index_segments_from_ecs(
            &refs,
            &local_store,
            &remote_store,
            0.02,
            BoldnessConstraint::strict(),
        )
        .expect("repair from surviving symbols");

        assert_eq!(report.repaired_from_local, 1);
        assert_eq!(report.repaired_from_remote, 1);
        assert_eq!(report.segments.len(), 2);
        let (resolved, _) = lookup_pointer_in_segments(p2, 201, &report.segments);
        assert_eq!(
            resolved,
            Some(pointer(201, 0x91, PatchKind::SparseXor, Some(0x92)))
        );
    }

    #[test]
    fn test_boldness_constraint() {
        let marker = marker_record(300, 0x33, [0_u8; 16]);
        let marker_blob = marker_segment_blob(300, std::slice::from_ref(&marker));
        let source = TestCapsuleSource::default().with_update(
            300,
            0x33,
            vec![(page(11), pointer(300, 0xA0, PatchKind::FullImage, None))],
        );

        let blocked = emergency_linear_scan_lookup(
            page(11),
            300,
            std::slice::from_ref(&marker_blob),
            &source,
            BoldnessConstraint::strict(),
            "index_destroyed",
        )
        .expect_err("strict boldness must block emergency lookup");
        assert!(
            blocked
                .to_string()
                .contains("reason_code=boldness_violation_blocked_linear_scan")
        );

        let resolved = emergency_linear_scan_lookup(
            page(11),
            300,
            &[marker_blob],
            &source,
            BoldnessConstraint::emergency(),
            "index_destroyed",
        )
        .expect("emergency lookup")
        .expect("pointer found");
        assert_eq!(resolved, pointer(300, 0xA0, PatchKind::FullImage, None));
    }

    #[test]
    fn test_critical_integrity_failure_detected_before_repair_attempt() {
        let marker = marker_record(400, 0x44, [0_u8; 16]);
        let blob = marker_segment_blob(400, std::slice::from_ref(&marker));
        let err = preflight_native_index_integrity(std::slice::from_ref(&blob), false, false)
            .expect_err("markers without recovery paths are critical");
        assert!(
            err.to_string()
                .contains("reason_code=index_unrebuildable_with_markers")
        );

        preflight_native_index_integrity(&[blob], true, false).expect("repair path available");
    }

    #[test]
    fn test_bd_1hi_33_unit_compliance_gate() {
        assert_eq!(NATIVE_INDEX_REPAIR_BEAD_ID, "bd-1hi.33");
        assert_eq!(NATIVE_INDEX_LOGGING_STANDARD, "bd-1fpm");

        let marker = marker_record(500, 0x55, [0_u8; 16]);
        let marker_blob = marker_segment_blob(500, std::slice::from_ref(&marker));
        let err = preflight_native_index_integrity(&[marker_blob], false, false)
            .expect_err("critical preflight error");
        assert!(
            err.to_string()
                .contains("reason_code=index_unrebuildable_with_markers")
        );
    }

    #[test]
    fn prop_bd_1hi_33_structure_compliance() {
        for seed in 1_u8..=12 {
            let commit_seq = 10_000_u64 + u64::from(seed);
            let marker = marker_record(commit_seq, seed, [0_u8; 16]);
            let marker_blob = marker_segment_blob(commit_seq, std::slice::from_ref(&marker));
            let source = TestCapsuleSource::default().with_update(
                commit_seq,
                seed,
                vec![(
                    page(u32::from(seed) + 1),
                    pointer(commit_seq, seed.wrapping_add(1), PatchKind::FullImage, None),
                )],
            );

            let rebuilt =
                rebuild_index_from_marker_stream(std::slice::from_ref(&marker_blob), &source, 1)
                    .expect("rebuild");
            assert_eq!(rebuilt.markers.len(), 1);
            assert!(!rebuilt.segments.is_empty());

            let refs: Vec<NativeIndexSegmentRef> = rebuilt
                .segments
                .iter()
                .map(|segment| NativeIndexSegmentRef {
                    start_seq: segment.segment.start_seq,
                    end_seq: segment.segment.end_seq,
                    object_id: segment.object_id,
                })
                .collect();
            let remote_store = TestSegmentStore::new(
                rebuilt
                    .segments
                    .iter()
                    .map(|segment| (segment.object_id, segment.segment.clone())),
            );
            let local_store = TestSegmentStore::default();
            let repaired = repair_index_segments_from_ecs(
                &refs,
                &local_store,
                &remote_store,
                0.05,
                BoldnessConstraint::strict(),
            )
            .expect("repair");
            assert_eq!(repaired.segments.len(), refs.len());

            let looked_up = emergency_linear_scan_lookup(
                page(u32::from(seed) + 1),
                commit_seq,
                &[marker_blob],
                &source,
                BoldnessConstraint::emergency(),
                "index_destroyed",
            )
            .expect("emergency lookup");
            assert!(looked_up.is_some());
        }
    }

    struct Bd1Hi33Fixture {
        p11: PageNumber,
        p12: PageNumber,
        base: TestBasePages,
        patch_store: TestPatchStore,
        marker_blob: Vec<u8>,
        source: TestCapsuleSource,
    }

    fn build_bd_1hi_33_fixture() -> Bd1Hi33Fixture {
        let p11 = page(11);
        let p12 = page(12);
        let base = TestBasePages::new([(p11, b"base11".to_vec()), (p12, b"base12".to_vec())]);
        let patch_store = TestPatchStore::new([
            (oid(0x90), b"v300-p11".to_vec()),
            (oid(0x91), b"v301-p11".to_vec()),
            (oid(0x92), b"v301-p12".to_vec()),
        ]);

        let marker_300 = marker_record(300, 0x30, [0_u8; 16]);
        let marker_301 = marker_record(301, 0x31, marker_300.marker_id);
        let marker_blob = marker_segment_blob(300, &[marker_300, marker_301]);

        let source = TestCapsuleSource::default()
            .with_update(
                300,
                0x30,
                vec![(p11, pointer(300, 0x90, PatchKind::FullImage, None))],
            )
            .with_update(
                301,
                0x31,
                vec![
                    (p11, pointer(301, 0x91, PatchKind::FullImage, None)),
                    (p12, pointer(301, 0x92, PatchKind::FullImage, None)),
                ],
            );

        Bd1Hi33Fixture {
            p11,
            p12,
            base,
            patch_store,
            marker_blob,
            source,
        }
    }

    fn assert_repaired_index_lookups(
        fixture: &Bd1Hi33Fixture,
        segments: &[PageVersionIndexSegment],
    ) {
        let mut cache = NativePageCache::default();
        let looked_up_p11 = lookup_page_version(
            fixture.p11,
            301,
            segments,
            &mut cache,
            &fixture.base,
            &fixture.patch_store,
            0.05,
        )
        .expect("indexed lookup p11");
        assert_eq!(looked_up_p11.page_bytes, b"v301-p11".to_vec());

        let mut cache2 = NativePageCache::default();
        let looked_up_p12 = lookup_page_version(
            fixture.p12,
            301,
            segments,
            &mut cache2,
            &fixture.base,
            &fixture.patch_store,
            0.05,
        )
        .expect("indexed lookup p12");
        assert_eq!(looked_up_p12.page_bytes, b"v301-p12".to_vec());
    }

    fn assert_emergency_scan_behavior(fixture: &Bd1Hi33Fixture) {
        let blocked = emergency_linear_scan_lookup(
            fixture.p11,
            301,
            std::slice::from_ref(&fixture.marker_blob),
            &fixture.source,
            BoldnessConstraint::strict(),
            "index_destroyed",
        )
        .expect_err("strict policy blocks emergency scan");
        assert!(
            blocked
                .to_string()
                .contains("reason_code=boldness_violation_blocked_linear_scan")
        );

        let fallback_pointer = emergency_linear_scan_lookup(
            fixture.p11,
            301,
            std::slice::from_ref(&fixture.marker_blob),
            &fixture.source,
            BoldnessConstraint::emergency(),
            "index_destroyed",
        )
        .expect("emergency linear scan")
        .expect("pointer");
        let fallback_patch = fixture
            .patch_store
            .fetch_patch_object(fallback_pointer.patch_object)
            .expect("patch bytes");
        let fallback_base = fixture.base.load_base_page(fixture.p11).expect("base page");
        let fallback_bytes = materialize_patch(
            fallback_pointer,
            &fallback_patch,
            &fallback_base,
            &fixture.patch_store,
            0,
        )
        .expect("materialize from emergency pointer");
        assert_eq!(fallback_bytes, b"v301-p11".to_vec());
    }

    #[test]
    fn test_e2e_bd_1hi_33_compliance() {
        let fixture = build_bd_1hi_33_fixture();

        let rebuilt_a = rebuild_index_from_marker_stream(
            std::slice::from_ref(&fixture.marker_blob),
            &fixture.source,
            1,
        )
        .expect("rebuild");
        let rebuilt_b = rebuild_index_from_marker_stream(
            std::slice::from_ref(&fixture.marker_blob),
            &fixture.source,
            1,
        )
        .expect("rebuild deterministic");
        let ids_a: Vec<ObjectId> = rebuilt_a
            .segments
            .iter()
            .map(|segment| segment.object_id)
            .collect();
        let ids_b: Vec<ObjectId> = rebuilt_b
            .segments
            .iter()
            .map(|segment| segment.object_id)
            .collect();
        assert_eq!(ids_a, ids_b);

        let refs: Vec<NativeIndexSegmentRef> = rebuilt_a
            .segments
            .iter()
            .map(|segment| NativeIndexSegmentRef {
                start_seq: segment.segment.start_seq,
                end_seq: segment.segment.end_seq,
                object_id: segment.object_id,
            })
            .collect();
        assert!(refs.len() >= 2);

        let local_store = TestSegmentStore::new([(
            rebuilt_a.segments[0].object_id,
            rebuilt_a.segments[0].segment.clone(),
        )]);
        let remote_store = TestSegmentStore::new(
            rebuilt_a
                .segments
                .iter()
                .map(|segment| (segment.object_id, segment.segment.clone())),
        );
        let repaired = repair_index_segments_from_ecs(
            &refs,
            &local_store,
            &remote_store,
            0.05,
            BoldnessConstraint::strict(),
        )
        .expect("repair");

        assert_repaired_index_lookups(&fixture, &repaired.segments);
        assert_emergency_scan_behavior(&fixture);
    }
}
