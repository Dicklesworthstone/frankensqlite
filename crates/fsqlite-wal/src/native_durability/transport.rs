//! Authenticated, transport-independent native catch-up.
//!
//! A plan contains an anchored marker suffix and a bounded object inventory.
//! Object messages carry the EXISTING SymbolRecord envelopes. Neither side
//! needs the other side's file handles. The caller supplies transport, routing,
//! key distribution, namespace admission and the destination append lease.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_vfs::{VfsFile, VfsWriteCompletion};

use crate::native_commit::durable::{NativeCommitCandidate, NativeObjectCodec};

use super::{
    COMMIT_MARKER_RECORD_V1_SIZE, CommitMarker, CommitSeq, NativeDurabilityLog,
    NativeDurabilityReceipt, ObjectId, SYMBOL_HEADER_BYTES, SYMBOL_TRAILER_BYTES,
    SymbolRecord, checkpoint, corrupt, read_exact_at, symbol_wire_len, u64_len,
    validate_marker,
};

const PLAN_MAGIC: &[u8; 8] = b"FNPLAN01";
const OBJECT_MAGIC: &[u8; 8] = b"FNOBJS01";
const REQUEST_MAGIC: &[u8; 8] = b"FNNEED01";
const AUTH_DOMAIN: &[u8] = b"frankensqlite/native-replication/transport/v1\0";
const PLAN_HEADER: usize = 56;
const OBJECT_HEADER: usize = 60;
const REQUEST_HEADER: usize = 44;
const TAG_BYTES: usize = 32;
const MAX_COMMITS: usize = 128;

/// Wire admission bounds, separate from the destination's storage/codec limits.
/// At most one object message is needed at a time; this is not a total RSS bound.
#[derive(Debug, Clone, Copy)]
pub struct NativeReplicationLimits {
    pub max_plan_bytes: usize,
    pub max_objects: usize,
    pub max_object_message_bytes: usize,
}

impl Default for NativeReplicationLimits {
    fn default() -> Self {
        Self {
            max_plan_bytes: 4 * 1024 * 1024,
            max_objects: 131_072,
            max_object_message_bytes: 16 * 1024 * 1024 + OBJECT_HEADER + TAG_BYTES,
        }
    }
}

impl NativeReplicationLimits {
    fn validate(self) -> Result<()> {
        if self.max_plan_bytes < PLAN_HEADER + TAG_BYTES + COMMIT_MARKER_RECORD_V1_SIZE + 16
            || self.max_objects == 0
            || self.max_object_message_bytes < OBJECT_HEADER + TAG_BYTES + SYMBOL_HEADER_BYTES + SYMBOL_TRAILER_BYTES + 1
        {
            return Err(FrankenError::TooBig);
        }
        Ok(())
    }
}

/// A claimed prefix position, not an acknowledgement or writer authority.
/// Every use against a log checks the actual marker chain and accepted tip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeReplicationCursor {
    pub sequence: CommitSeq,
    pub marker: Option<ObjectId>,
}

impl NativeReplicationCursor {
    #[must_use]
    pub const fn genesis() -> Self {
        Self { sequence: CommitSeq::ZERO, marker: None }
    }

    fn validate(self) -> Result<()> {
        if (self.sequence == CommitSeq::ZERO) != self.marker.is_none() {
            return Err(corrupt("invalid native replication cursor"));
        }
        Ok(())
    }
}

/// A nonempty anchored suffix and its source object inventory.
///
/// Remote plans MUST be decoded with `from_bytes` and a separately configured
/// database identity and key. The identity is a routing domain supplied by the
/// caller, not inferred from a filename, VFS identity or this message. Use a
/// dedicated replication key, not an object-encryption key. Anyone holding that
/// key can authorize a source; this is not consensus, election or key rotation.
///
/// The inventory includes intact orphans because native proof formats may
/// reference objects transitively. The source codec and later semantic replay
/// still determine whether those bytes describe valid commits/pages.
#[derive(Debug, Clone)]
pub struct NativeReplicationPlan {
    database: ObjectId,
    base: NativeReplicationCursor,
    markers: Vec<CommitMarker>,
    objects: Vec<ObjectId>,
    binding: [u8; 32],
    signer: Option<blake3::Hash>,
}

impl NativeReplicationPlan {
    /// Authenticate a receiver's missing-object request for this exact plan.
    ///
    /// Pass the configured replication key, not a key obtained from the request.
    /// Only strictly ordered, unique members of this plan's inventory can be
    /// requested. The returned IDs can be passed to `export_replication_object`
    /// one at a time; no source handle is needed to decode this request itself.
    /// An empty request says no payloads are requested, NOT that publication
    /// has succeeded. Replayed requests may resend data; object stages deduplicate.
    ///
    /// Authentication, plan binding, counts and exact length are checked before
    /// allocating the result. The plan's byte/object limits bound the request;
    /// arbitrary paths, physical offsets and caller-provided inventories are
    /// never interpreted. Routing, delivery and peer admission remain external.
    ///
    /// # Errors
    /// Rejects wrong keys/plans, nonmembers, duplicates, ordering, malformed or
    /// excessive lengths, allocation failure and cancellation.
    pub fn requested_object_ids(
        &self,
        cx: &Cx,
        bytes: &[u8],
        key: &[u8; 32],
        limits: NativeReplicationLimits,
    ) -> Result<Vec<ObjectId>> {
        self.validate(cx, limits)?;
        let body = authenticate(cx, bytes, key, limits.max_plan_bytes)?;
        if let Some(signer) = self.signer {
            if signer != mac(cx, key, &self.binding)? {
                return Err(corrupt("native request signing key differs from its plan"));
            }
        }
        let mut input = Reader(body);
        if input.array::<8>()? != *REQUEST_MAGIC || input.array::<32>()? != self.binding {
            return Err(corrupt("native missing-object request belongs to another plan"));
        }
        let count = usize::try_from(u32::from_le_bytes(input.array()?))
            .map_err(|_| FrankenError::TooBig)?;
        if count > self.objects.len() || request_len(count)? != bytes.len() {
            return Err(corrupt("native missing-object request count or length mismatch"));
        }
        let mut requested = Vec::new();
        requested.try_reserve_exact(count).map_err(|_| FrankenError::OutOfMemory)?;
        let mut previous = None;
        for _ in 0..count {
            checkpoint(cx)?;
            let id = ObjectId::from_bytes(input.array()?);
            if previous.is_some_and(|previous| previous >= id)
                || self.objects.binary_search(&id).is_err()
            {
                return Err(corrupt("noncanonical or unlisted native object request"));
            }
            requested.push(id);
            previous = Some(id);
        }
        checkpoint(cx)?;
        Ok(requested)
    }

    #[must_use]
    pub const fn database(&self) -> ObjectId { self.database }
    #[must_use]
    pub const fn base(&self) -> NativeReplicationCursor { self.base }
    #[must_use]
    pub fn target(&self) -> NativeReplicationCursor {
        let marker = self.markers.last().expect("validated nonempty replication plan");
        NativeReplicationCursor {
            sequence: marker.commit_seq,
            marker: Some(ObjectId::derive_from_canonical_bytes(&marker.to_record_bytes())),
        }
    }
    #[must_use]
    pub fn commit_count(&self) -> usize { self.markers.len() }
    pub fn object_ids(&self) -> impl ExactSizeIterator<Item = ObjectId> + '_ {
        self.objects.iter().copied()
    }

    fn validate(&self, cx: &Cx, limits: NativeReplicationLimits) -> Result<usize> {
        limits.validate()?;
        self.base.validate()?;
        if self.markers.is_empty() || self.markers.len() > MAX_COMMITS
            || self.objects.is_empty() || self.objects.len() > limits.max_objects
            || self.objects.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(corrupt("invalid native replication plan inventory"));
        }
        let len = plan_len(self.markers.len(), self.objects.len())?;
        if len > limits.max_plan_bytes { return Err(FrankenError::TooBig); }
        let mut tip = self.base.sequence;
        let mut previous = self.base.marker;
        let mut time = 0;
        for marker in &self.markers {
            checkpoint(cx)?;
            validate_marker(marker, tip, previous, time)?;
            for id in [marker.capsule_object_id, marker.proof_object_id] {
                if self.objects.binary_search(&id).is_err() {
                    return Err(corrupt("native replication plan omits a committed referent"));
                }
            }
            tip = marker.commit_seq;
            previous = Some(ObjectId::derive_from_canonical_bytes(&marker.to_record_bytes()));
            time = marker.commit_time_unix_ns;
        }
        Ok(len)
    }

    fn body(&self, cx: &Cx, limits: NativeReplicationLimits) -> Result<Vec<u8>> {
        let len = self.validate(cx, limits)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(len).map_err(|_| FrankenError::OutOfMemory)?;
        bytes.extend_from_slice(PLAN_MAGIC);
        bytes.extend_from_slice(self.database.as_bytes());
        bytes.extend_from_slice(&self.base.sequence.get().to_le_bytes());
        bytes.extend_from_slice(
            self.base
                .marker
                .as_ref()
                .map_or(&[0u8; 16], ObjectId::as_bytes)
                .as_slice(),
        );
        bytes.extend_from_slice(&count(self.markers.len())?.to_le_bytes());
        bytes.extend_from_slice(&count(self.objects.len())?.to_le_bytes());
        for marker in &self.markers { bytes.extend_from_slice(&marker.to_record_bytes()); }
        for id in &self.objects {
            checkpoint(cx)?;
            bytes.extend_from_slice(id.as_bytes());
        }
        Ok(bytes)
    }

    /// Encode a canonical plan with a domain-separated keyed BLAKE3 tag.
    /// No key is retained in the plan or exposed in Debug output.
    ///
    /// # Errors
    /// Rejects cancellation, invalid metadata, wire limits and allocation failure.
    pub fn to_bytes(&self, cx: &Cx, key: &[u8; 32], limits: NativeReplicationLimits) -> Result<Vec<u8>> {
        seal(cx, self.body(cx, limits)?, key)
    }

    /// Authenticate bytes and routing BEFORE allocating variable-size lists.
    /// Counts, exact length, canonical ordering and every marker are checked.
    /// A valid MAC authenticates a sender, not SQL/SSI semantics or durability.
    ///
    /// # Errors
    /// Rejects wrong keys/domains, malformed/trailing/truncated input and limits.
    pub fn from_bytes(
        cx: &Cx, bytes: &[u8], database: ObjectId, key: &[u8; 32], limits: NativeReplicationLimits,
    ) -> Result<Self> {
        limits.validate()?;
        let body = authenticate(cx, bytes, key, limits.max_plan_bytes)?;
        let mut input = Reader(body);
        if input.array::<8>()? != *PLAN_MAGIC || ObjectId::from_bytes(input.array()?) != database {
            return Err(corrupt("native replication plan version or database mismatch"));
        }
        let sequence = CommitSeq::new(u64::from_le_bytes(input.array()?));
        let anchor = input.array::<16>()?;
        let marker = if sequence == CommitSeq::ZERO {
            if anchor != [0; 16] { return Err(corrupt("noncanonical native genesis anchor")); }
            None
        } else { Some(ObjectId::from_bytes(anchor)) };
        let commits = usize::try_from(u32::from_le_bytes(input.array()?)).map_err(|_| FrankenError::TooBig)?;
        let object_count = usize::try_from(u32::from_le_bytes(input.array()?)).map_err(|_| FrankenError::TooBig)?;
        if commits == 0 || commits > MAX_COMMITS || object_count == 0
            || object_count > limits.max_objects || plan_len(commits, object_count)? != bytes.len()
        {
            return Err(corrupt("native replication plan counts or length mismatch"));
        }
        let mut markers = Vec::new();
        markers.try_reserve_exact(commits).map_err(|_| FrankenError::OutOfMemory)?;
        for _ in 0..commits {
            checkpoint(cx)?;
            let wire = input.array::<COMMIT_MARKER_RECORD_V1_SIZE>()?;
            let marker = CommitMarker::from_record_bytes(&wire)
                .ok_or_else(|| corrupt("malformed transported native marker"))?;
            if marker.to_record_bytes() != wire {
                return Err(corrupt("noncanonical transported native marker"));
            }
            markers.push(marker);
        }
        let mut objects = Vec::new();
        objects.try_reserve_exact(object_count).map_err(|_| FrankenError::OutOfMemory)?;
        for _ in 0..object_count {
            checkpoint(cx)?;
            objects.push(ObjectId::from_bytes(input.array()?));
        }
        let binding = *blake3::hash(body).as_bytes();
        let plan = Self {
            database, base: NativeReplicationCursor { sequence, marker }, markers, objects,
            binding, signer: Some(mac(cx, key, &binding)?),
        };
        plan.validate(cx, limits)?;
        Ok(plan)
    }
}

impl<S: VfsFile, M: VfsFile> NativeDurabilityLog<S, M> {
    /// Read and verify the accepted prefix before advertising its position.
    /// Tails are excluded; a cursor does not authorize appending through them.
    ///
    /// # Errors
    /// Refuses unavailable, changed or corrupt storage and cancellation.
    pub async fn replication_cursor(&mut self, cx: &Cx) -> Result<NativeReplicationCursor> {
        let cursor = NativeReplicationCursor { sequence: self.published_tip, marker: self.previous_marker };
        self.transport_suffix(cx, cursor, 0).await?;
        Ok(cursor)
    }

    /// Export an anchored plan without borrowing a destination or buffering its
    /// payloads. At most 128 markers and `max_objects` IDs are retained. All
    /// accepted source markers are checked, including the unselected suffix.
    /// Use `export_replication_object` for each ID, then close the source only
    /// after all needed messages have been obtained. Transport is caller-owned.
    ///
    /// # Errors
    /// Rejects foreign/ahead cursors, invalid bounds, source corruption and limits.
    pub async fn export_replication_plan(
        &mut self, cx: &Cx, database: ObjectId, base: NativeReplicationCursor,
        max_commits: usize, limits: NativeReplicationLimits,
    ) -> Result<Option<NativeReplicationPlan>> {
        limits.validate()?;
        if !(1..=MAX_COMMITS).contains(&max_commits) { return Err(FrankenError::TooBig); }
        let markers = self.transport_suffix(cx, base, max_commits).await?;
        if markers.is_empty() { return Ok(None); }
        if self.index.len() > limits.max_objects
            || plan_len(markers.len(), self.index.len())? > limits.max_plan_bytes
        {
            return Err(FrankenError::TooBig);
        }
        let mut objects = Vec::new();
        objects.try_reserve_exact(self.index.len()).map_err(|_| FrankenError::OutOfMemory)?;
        for id in self.index.keys() { checkpoint(cx)?; objects.push(*id); }
        let mut plan = NativeReplicationPlan { database, base, markers, objects, binding: [0; 32], signer: None };
        plan.binding = *blake3::hash(&plan.body(cx, limits)?).as_bytes();
        Ok(Some(plan))
    }

    /// Export one complete surviving object as a separately authenticated
    /// message bound to this exact plan. Original source/repair envelopes are
    /// retained; neither RaptorQ symbols nor authentication data are fabricated.
    /// The source may have advanced, but the planned marker must still match.
    ///
    /// One object is buffered, bounded by BOTH storage-read and transport limits.
    /// The caller may stream messages one at a time; retaining all is its budget.
    ///
    /// # Errors
    /// Rejects missing/nonmember objects, changed history, malformed envelopes,
    /// exceeded limits, cancellation and source read errors.
    pub async fn export_replication_object(
        &mut self, cx: &Cx, plan: &NativeReplicationPlan, id: ObjectId,
        key: &[u8; 32], limits: NativeReplicationLimits,
    ) -> Result<Vec<u8>> {
        limits.validate()?;
        self.readable()?;
        self.check_lengths(cx)?;
        checkpoint(cx)?;
        if plan.objects.binary_search(&id).is_err() { return Err(corrupt("unlisted replication object")); }
        let target = plan.target();
        if target.sequence > self.published_tip { return Err(corrupt("replication plan exceeds source")); }
        let mut marker = [0; COMMIT_MARKER_RECORD_V1_SIZE];
        let offset = (target.sequence.get() - 1).checked_mul(u64_len(marker.len())?)
            .ok_or(FrankenError::TooBig)?;
        read_exact_at(&self.markers, cx, &mut marker, offset).await?;
        if target.marker != Some(ObjectId::derive_from_canonical_bytes(&marker)) {
            return Err(corrupt("replication plan source anchor changed"));
        }
        let records = self.read_object(cx, id).await?;
        let mut len = OBJECT_HEADER + TAG_BYTES;
        for record in &records {
            checkpoint(cx)?;
            len = len.checked_add(record.symbol_data.len())
                .and_then(|n| n.checked_add(SYMBOL_HEADER_BYTES + SYMBOL_TRAILER_BYTES))
                .filter(|n| *n <= limits.max_object_message_bytes).ok_or(FrankenError::TooBig)?;
        }
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(len).map_err(|_| FrankenError::OutOfMemory)?;
        bytes.extend_from_slice(OBJECT_MAGIC);
        bytes.extend_from_slice(&plan.binding);
        bytes.extend_from_slice(id.as_bytes());
        bytes.extend_from_slice(&count(records.len())?.to_le_bytes());
        for record in records { checkpoint(cx)?; bytes.extend_from_slice(&record.to_bytes()); }
        self.check_lengths(cx)?;
        seal(cx, bytes, key)
    }

    /// Begin receiving an authenticated plan on caller-owned destination files.
    /// No source handle is needed. A partial committed prefix from an earlier
    /// failed attempt is checked and skipped. An already-applied anchored plan
    /// returns None WITHOUT issuing another durability acknowledgement.
    ///
    /// Keep the receiver alive across object messages. Dropping it before finish
    /// leaves the destination recovery-blocked, with outstanding write ownership
    /// retained by the log. Retry only after original-handle recovery. This is
    /// fail-closed storage publication, not atomicity across multiple commits.
    ///
    /// # Errors
    /// Refuses tails, unavailable storage, forks/gaps and invalid resource bounds.
    pub async fn receive_replication_plan(
        &mut self, cx: &Cx, plan: NativeReplicationPlan, limits: NativeReplicationLimits,
    ) -> Result<Option<NativeReplicationReceiver<'_, S, M>>> {
        self.ready()?;
        plan.validate(cx, limits)?;
        if plan.signer.is_none() {
            return Err(corrupt("native receive requires an authenticated wire plan"));
        }
        let target = plan.target();
        if self.published_tip < plan.base.sequence { return Err(corrupt("native replica is behind plan base")); }
        let through = self.published_tip.min(target.sequence);
        let anchor = if through == plan.base.sequence { plan.base.marker } else {
            let index = usize::try_from(through.get() - plan.base.sequence.get() - 1)
                .map_err(|_| FrankenError::TooBig)?;
            Some(ObjectId::derive_from_canonical_bytes(&plan.markers[index].to_record_bytes()))
        };
        self.transport_suffix(cx, NativeReplicationCursor { sequence: through, marker: anchor }, 0).await?;
        if self.published_tip >= target.sequence { return Ok(None); }
        let first = usize::try_from(self.published_tip.get() - plan.base.sequence.get())
            .map_err(|_| FrankenError::TooBig)?;
        // The wire decoder knows the base digest, not the base timestamp.
        // Bind the first remaining commit to this destination before staging.
        validate_marker(&plan.markers[first], self.published_tip, self.previous_marker, self.last_time_ns)?;
        let pending = plan.markers.len() - first;
        let marker_bytes = pending.checked_mul(COMMIT_MARKER_RECORD_V1_SIZE).ok_or(FrankenError::TooBig)?;
        let end = self.marker_end.checked_add(u64_len(marker_bytes)?).ok_or(FrankenError::TooBig)?;
        if marker_bytes > self.limits.max_batch_bytes || end > self.limits.max_stream_bytes
            || end / u64_len(COMMIT_MARKER_RECORD_V1_SIZE)? > u64_len(self.limits.max_records)?
        {
            return Err(FrankenError::TooBig);
        }
        let mut received = Vec::new();
        received.try_reserve_exact(plan.objects.len()).map_err(|_| FrankenError::OutOfMemory)?;
        received.resize(plan.objects.len(), false);
        let base = NativeReplicationCursor { sequence: self.published_tip, marker: self.previous_marker };
        Ok(Some(NativeReplicationReceiver { destination: self, plan, limits, first, base, received, complete: false }))
    }

    // The existing record decoder and chain validator define the protocol.
    // This scanner additionally binds a portable caller cursor to the accepted
    // local head, without trusting that cursor as a recovered log or receipt.
    async fn transport_suffix(
        &mut self, cx: &Cx, base: NativeReplicationCursor, limit: usize,
    ) -> Result<Vec<CommitMarker>> {
        self.readable()?;
        self.check_lengths(cx)?;
        base.validate()?;
        if base.sequence > self.published_tip { return Err(corrupt("native replication cursor is ahead")); }
        let width = u64_len(COMMIT_MARKER_RECORD_V1_SIZE)?;
        let count = self.marker_end / width;
        if count != self.published_tip.get() {
            self.poisoned = true;
            return Err(corrupt("unaccepted native marker boundary"));
        }
        let capacity = usize::try_from((count - base.sequence.get()).min(u64_len(limit)?))
            .map_err(|_| FrankenError::TooBig)?;
        let mut suffix = Vec::new();
        suffix.try_reserve_exact(capacity).map_err(|_| FrankenError::OutOfMemory)?;
        let mut tip = CommitSeq::ZERO;
        let mut previous = None;
        let mut time = 0;
        let mut base_matches = base.sequence == CommitSeq::ZERO;
        for ordinal in 0..count {
            checkpoint(cx)?;
            let mut wire = [0; COMMIT_MARKER_RECORD_V1_SIZE];
            read_exact_at(&self.markers, cx, &mut wire, ordinal.checked_mul(width).ok_or(FrankenError::TooBig)?).await?;
            let Some(marker) = CommitMarker::from_record_bytes(&wire) else {
                self.poisoned = true;
                return Err(corrupt("malformed native transport marker"));
            };
            if let Err(error) = validate_marker(&marker, tip, previous, time) {
                self.poisoned = true;
                return Err(error);
            }
            tip = marker.commit_seq;
            time = marker.commit_time_unix_ns;
            previous = Some(ObjectId::derive_from_canonical_bytes(&wire));
            if tip == base.sequence { base_matches = previous == base.marker; }
            if tip > base.sequence && suffix.len() < capacity { suffix.push(marker); }
        }
        if tip != self.published_tip || previous != self.previous_marker || time != self.last_time_ns {
            self.poisoned = true;
            return Err(corrupt("native transport source changed accepted history"));
        }
        self.check_lengths(cx)?;
        checkpoint(cx)?;
        if !base_matches { return Err(corrupt("native replication cursor fork")); }
        Ok(suffix)
    }
}

/// Borrowed destination owner spanning authenticated messages and publication.
/// A dropped stage future never loses the log's source-owned completion token.
/// No live page publisher or SQL reader is returned by this storage-only layer.
pub struct NativeReplicationReceiver<'a, S: VfsFile, M: VfsFile> {
    destination: &'a mut NativeDurabilityLog<S, M>,
    plan: NativeReplicationPlan,
    limits: NativeReplicationLimits,
    first: usize,
    base: NativeReplicationCursor,
    received: Vec<bool>,
    complete: bool,
}

impl<S: VfsFile, M: VfsFile> fmt::Debug for NativeReplicationReceiver<'_, S, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeReplicationReceiver").field("base", &self.base)
            .field("missing_objects", &self.received.iter().filter(|seen| !**seen).count())
            .finish_non_exhaustive()
    }
}

impl<S: VfsFile, M: VfsFile> Drop for NativeReplicationReceiver<'_, S, M> {
    fn drop(&mut self) { if !self.complete { self.destination.poisoned = true; } }
}

impl<S: VfsFile, M: VfsFile> NativeReplicationReceiver<'_, S, M> {
    #[must_use]
    pub fn outstanding_write(&self) -> Option<VfsWriteCompletion> { self.destination.outstanding_write() }

    /// Encode the unresolved inventory as an authenticated request to the sender.
    /// Call after optional local reuse; only missing IDs are encoded, using this
    /// plan's binding and signing key. The sender uses `requested_object_ids` on
    /// its retained plan, then exports only those objects. No payloads, storage
    /// writes, synchronization or source handles are needed here.
    ///
    /// An empty request is allowed but is not a durability acknowledgement.
    /// Reusing or receiving more objects after creating a request can make it
    /// redundant; ordinary stage deduplication still handles retransmission.
    /// The encoded request fits `max_plan_bytes` and the existing inventory bound.
    ///
    /// # Errors
    /// Rejects wrong signing keys, unavailable storage, cancellation and limits.
    pub fn missing_object_request(&self, cx: &Cx, key: &[u8; 32]) -> Result<Vec<u8>> {
        checkpoint(cx)?;
        self.destination.ready()?;
        if self.plan.signer != Some(mac(cx, key, &self.plan.binding)?) {
            return Err(corrupt("native request signing key differs from its plan"));
        }
        let count = self.missing_object_ids().count();
        let len = request_len(count)?;
        if len > self.limits.max_plan_bytes { return Err(FrankenError::TooBig); }
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(len).map_err(|_| FrankenError::OutOfMemory)?;
        bytes.extend_from_slice(REQUEST_MAGIC);
        bytes.extend_from_slice(&self.plan.binding);
        bytes.extend_from_slice(&u32::try_from(count).map_err(|_| FrankenError::TooBig)?.to_le_bytes());
        for id in self.missing_object_ids() {
            checkpoint(cx)?;
            bytes.extend_from_slice(id.as_bytes());
        }
        seal(cx, bytes, key)
    }

    /// Objects still requiring a message or successful local verification.
    ///
    /// This borrowed, ordered inventory allocates nothing. An indexed object
    /// is not automatically available: `reuse_object` must decode its actual
    /// stored bytes first. The caller routes requests for these IDs under the
    /// same authenticated plan; this iterator is not a wire message or receipt.
    pub fn missing_object_ids(&self) -> impl Iterator<Item = ObjectId> + '_ {
        self.plan.objects.iter().zip(&self.received)
            .filter_map(|(id, received)| (!*received).then_some(*id))
    }

    /// Satisfy one plan entry using an independently verified local object.
    ///
    /// Use the same identity/authentication codec as final publication. This
    /// re-reads the existing envelopes and requires complete reconstruction;
    /// index membership, a checksum, or a previous stage receipt is not enough.
    /// A recoverable subset of RaptorQ symbols is sufficient, without inventing
    /// missing redundancy. No object message, source handle, append or sync is
    /// needed. Storage/codec limits apply; the network-message size bound does
    /// not apply to data that never crosses the transport.
    ///
    /// `false` means the ID has no indexed symbols and needs a message. Errors
    /// (including insufficient rank, authentication, I/O and cancellation) are
    /// NOT turned into cache misses. The entry stays missing on every failed
    /// or dropped polled attempt, including revalidation of a staged entry.
    /// The receiver can accept a valid message or retry local verification;
    /// an uncertain physical write still requires original-handle recovery.
    ///
    /// This is only inventory admission. Finalizers still re-read referents,
    /// validate the selected history, and perform the usual durability barriers.
    /// The caller must continuously retain the destination namespace/append
    /// lease; successful verification does not freeze external file writers.
    ///
    /// # Errors
    /// Rejects unlisted objects, unavailable storage, changed stream lengths,
    /// malformed/undecodable objects, wrong keys, limits and cancellation.
    pub async fn reuse_object<C: NativeObjectCodec>(
        &mut self,
        cx: &Cx,
        id: ObjectId,
        codec: &C,
    ) -> Result<bool> {
        let index = self.plan.objects.binary_search(&id)
            .map_err(|_| corrupt("unlisted locally reused object"))?;
        // Revalidation cannot leave an older successful bit authorizing finish
        // after the current read/decode fails or its borrowed future is dropped.
        self.received[index] = false;
        checkpoint(cx)?;
        self.destination.ready()?;
        self.destination.check_lengths(cx)?;
        if !self.destination.index.contains_key(&id) {
            return Ok(false);
        }
        let records = self.destination.read_object(cx, id).await?;
        let payload = codec.decode(cx, id, &records)?;
        if payload.is_empty() || payload.len() > self.destination.limits.max_batch_bytes {
            return Err(FrankenError::TooBig);
        }
        // Do not retain decoded payloads across inventory entries. The codec
        // enforces its identity/profile and scratch bounds, as in publication.
        drop(payload);
        self.destination.check_lengths(cx)?;
        checkpoint(cx)?;
        self.received[index] = true;
        Ok(true)
    }

    /// Verify unresolved local entries one object at a time, returning the
    /// number newly reused by this call. Request only `missing_object_ids`
    /// afterward. Completed message stages and previous verified entries are
    /// skipped; a fresh receiver re-verifies them after recovery or a new plan.
    ///
    /// Earlier successes remain recorded on error/cancellation, but the failing
    /// entry remains missing. No error is suppressed as an absent object. This
    /// scans the plan inventory and reads locally present objects; it is not
    /// constant-work catch-up or a reduction in final semantic replay work.
    ///
    /// # Errors
    /// Propagates the first `reuse_object` error without publishing any marker.
    pub async fn reuse_available_objects<C: NativeObjectCodec>(
        &mut self,
        cx: &Cx,
        codec: &C,
    ) -> Result<usize> {
        checkpoint(cx)?;
        self.destination.ready()?;
        let mut reused = 0;
        for index in 0..self.plan.objects.len() {
            checkpoint(cx)?;
            if !self.received[index]
                && self.reuse_object(cx, self.plan.objects[index], codec).await?
            {
                reused += 1;
            }
        }
        Ok(reused)
    }

    /// Authenticate and bound an object message before its first storage read
    /// or write. Messages may arrive out of order or repeat. Exact duplicate
    /// envelopes reuse existing locators; conflicting OTI/ESI bytes are refused.
    /// A successful stage is not a commit or a durability receipt.
    ///
    /// # Errors
    /// Rejects wrong keys/plans, malformed messages, conflicting staged symbols,
    /// storage/transport limits, cancellation and uncertain writes.
    pub async fn stage_object(&mut self, cx: &Cx, bytes: &[u8], key: &[u8; 32]) -> Result<()> {
        if self.plan.signer != Some(mac(cx, key, &self.plan.binding)?) {
            return Err(corrupt("native object signing key differs from its plan"));
        }
        let body = authenticate(cx, bytes, key, self.limits.max_object_message_bytes)?;
        let mut input = Reader(body);
        if input.array::<8>()? != *OBJECT_MAGIC || input.array::<32>()? != self.plan.binding {
            return Err(corrupt("native object message belongs to another plan"));
        }
        let id = ObjectId::from_bytes(input.array()?);
        let index = self.plan.objects.binary_search(&id).map_err(|_| corrupt("unlisted transported object"))?;
        let count = usize::try_from(u32::from_le_bytes(input.array()?)).map_err(|_| FrankenError::TooBig)?;
        if count == 0 || count > self.destination.limits.max_records
            || count > input.0.len() / (SYMBOL_HEADER_BYTES + SYMBOL_TRAILER_BYTES + 1)
            || input.0.len() > self.destination.limits.max_batch_bytes
        {
            return Err(FrankenError::TooBig);
        }
        let mut records = Vec::new();
        records.try_reserve_exact(count).map_err(|_| FrankenError::OutOfMemory)?;
        for _ in 0..count {
            checkpoint(cx)?;
            let len = symbol_wire_len(input.0, self.destination.limits)?;
            let record = SymbolRecord::from_bytes(input.take(len)?).map_err(|error| corrupt(&error.to_string()))?;
            if record.object_id != id { return Err(corrupt("transported object identity mismatch")); }
            records.push(record);
        }
        if !input.0.is_empty() { return Err(corrupt("trailing native object message bytes")); }
        self.destination.ready()?;
        self.destination.check_lengths(cx)?;
        let existing = if self.destination.index.contains_key(&id) {
            self.destination.read_object(cx, id).await?
        } else { Vec::new() };
        let missing = merge_symbols(cx, records, existing)?;
        if !missing.is_empty() { self.destination.append_symbols(cx, &missing).await?; }
        self.received[index] = true;
        Ok(())
    }

    /// Replay the complete destination history and incoming suffix before
    /// publishing any new marker. Stored proofs, capsules and immediate
    /// evidence use the SAME decoder and binding checks as native recovery.
    /// The mandatory callback must enforce the application's historical
    /// semantics and bind each write set to its capsule. Its external effects
    /// are not rolled back. A transport key alone cannot provide that proof.
    ///
    /// This is full replay, not incremental validation: retained markers are
    /// bounded by the destination record limit, decoded evidence is bounded
    /// per commit, and callback-owned state has its own resource obligations.
    /// Every callback finishes before the first durability barrier. Failure
    /// retains staged orphans and the receiver's recovery obligations without
    /// publishing a partial semantic prefix. Physical publication can still
    /// tear: recover the original handles before retrying the portable plan.
    ///
    /// # Errors
    /// Rejects incomplete inventories, invalid proofs or application history,
    /// failed object authentication, cancellation and storage/limit failures.
    pub async fn finish_validated<C, V>(
        mut self,
        cx: &Cx,
        codec: &C,
        validate: V,
    ) -> Result<NativeDurabilityReceipt>
    where
        C: NativeObjectCodec,
        V: FnMut(&NativeCommitCandidate, &BTreeMap<ObjectId, Arc<[u8]>>) -> Result<()>,
    {
        self.validate_candidates(cx, codec, validate).await?;
        self.finish(cx, |id, records| {
            std::future::ready(codec.decode(cx, id, &records).map(|_| ()))
        })
        .await
    }

    // Also used by the native-page endpoint: it allocates its immutable image
    // after this replay but before finish can start mutating marker storage.
    // The pending records stay private; callers cannot substitute a replay list.
    pub(crate) async fn validate_candidates<C, V>(
        &mut self,
        cx: &Cx,
        codec: &C,
        mut validate: V,
    ) -> Result<(CommitSeq, u64)>
    where
        C: NativeObjectCodec,
        V: FnMut(&NativeCommitCandidate, &BTreeMap<ObjectId, Arc<[u8]>>) -> Result<()>,
    {
        checkpoint(cx)?;
        self.destination.ready()?;
        if self.received.iter().any(|received| !received) {
            return Err(corrupt("incomplete native object inventory"));
        }
        let count = usize::try_from(self.base.sequence.get()).map_err(|_| FrankenError::TooBig)?;
        if count > self.destination.limits.max_records {
            return Err(FrankenError::TooBig);
        }
        let prefix = self.destination
            .transport_suffix(cx, NativeReplicationCursor::genesis(), count).await?;
        if prefix.len() != count || self.destination.published_tip != self.base.sequence {
            return Err(corrupt("native receiver validation prefix changed"));
        }
        for marker in prefix.iter().chain(&self.plan.markers[self.first..]) {
            let (candidate, objects) = self.destination.read_commit_candidate(cx, codec, marker).await?;
            validate(&candidate, &objects)?;
        }
        // A codec/callback or read can yield to external work. Rebind the
        // accepted chain, then honor even cancellation by the final callback.
        // Neither this check nor a successful replay is a durability receipt.
        self.destination.transport_suffix(cx, self.base, 0).await?;
        checkpoint(cx)?;
        let last = self.plan.markers.last()
            .ok_or_else(|| corrupt("empty native validation plan"))?;
        Ok((last.commit_seq, last.commit_time_unix_ns))
    }

    /// Verify and publish after every entry is received or locally verified.
    /// Referents are re-read from the destination by the existing two-barrier
    /// publisher. The mandatory verifier must enforce decoding, canonical
    /// identity, object authentication and transitive evidence, as for publish.
    /// Page/SSI replay remains required before exposing a queryable replica.
    ///
    /// Errors or dropped futures return no receipt and retain recovery duties.
    /// No cancellation check follows a successful final durability barrier.
    ///
    /// # Errors
    /// Rejects incomplete inventories, changed prefixes, invalid referents,
    /// cancellation and storage failures. A torn marker batch may recover a
    /// committed prefix; retrying the same plan skips that verified prefix.
    pub async fn finish<V, VF>(mut self, cx: &Cx, verify_object: V) -> Result<NativeDurabilityReceipt>
    where
        V: FnMut(ObjectId, Vec<SymbolRecord>) -> VF,
        VF: std::future::Future<Output = Result<()>>,
    {
        checkpoint(cx)?;
        if self.received.iter().any(|received| !received) { return Err(corrupt("incomplete native object inventory")); }
        self.destination.transport_suffix(cx, self.base, 0).await?;
        let receipt = self.destination.publish(cx, &self.plan.markers[self.first..], verify_object)
            .await?.ok_or_else(|| corrupt("nonempty native transport batch produced no receipt"))?;
        self.complete = true;
        Ok(receipt)
    }
}

fn merge_symbols(cx: &Cx, mut incoming: Vec<SymbolRecord>, mut existing: Vec<SymbolRecord>) -> Result<Vec<SymbolRecord>> {
    let oti = incoming.first().ok_or_else(|| corrupt("empty transported object"))?.oti;
    for records in [&mut incoming, &mut existing] {
        for record in records.iter() {
            checkpoint(cx)?;
            if record.oti != oti { return Err(corrupt("transported native OTI conflict")); }
        }
        records.sort_unstable_by_key(|record| record.esi);
        if records.windows(2).any(|pair| pair[0].esi == pair[1].esi && pair[0] != pair[1]) {
            return Err(corrupt("conflicting transported native symbol"));
        }
        records.dedup();
    }
    let mut missing = Vec::new();
    missing.try_reserve_exact(incoming.len()).map_err(|_| FrankenError::OutOfMemory)?;
    for record in incoming {
        checkpoint(cx)?;
        match existing.binary_search_by_key(&record.esi, |symbol| symbol.esi) {
            Ok(index) if existing[index] != record => return Err(corrupt("native transport conflicts with staged symbol")),
            Ok(_) => {}
            Err(_) => missing.push(record),
        }
    }
    Ok(missing)
}

fn plan_len(commits: usize, objects: usize) -> Result<usize> {
    commits.checked_mul(COMMIT_MARKER_RECORD_V1_SIZE)
        .and_then(|n| objects.checked_mul(16).and_then(|m| n.checked_add(m)))
        .and_then(|n| n.checked_add(PLAN_HEADER + TAG_BYTES)).ok_or(FrankenError::TooBig)
}
fn request_len(objects: usize) -> Result<usize> {
    objects.checked_mul(ObjectId::LEN)
        .and_then(|bytes| bytes.checked_add(REQUEST_HEADER + TAG_BYTES))
        .ok_or(FrankenError::TooBig)
}
fn count(value: usize) -> Result<u32> { u32::try_from(value).map_err(|_| FrankenError::TooBig) }
fn mac(cx: &Cx, key: &[u8; 32], bytes: &[u8]) -> Result<blake3::Hash> {
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(AUTH_DOMAIN);
    for chunk in bytes.chunks(65_536) { checkpoint(cx)?; hasher.update(chunk); }
    checkpoint(cx)?;
    Ok(hasher.finalize())
}
fn seal(cx: &Cx, mut bytes: Vec<u8>, key: &[u8; 32]) -> Result<Vec<u8>> {
    let tag = mac(cx, key, &bytes)?;
    bytes.try_reserve(TAG_BYTES).map_err(|_| FrankenError::OutOfMemory)?;
    bytes.extend_from_slice(tag.as_bytes());
    Ok(bytes)
}
fn authenticate<'a>(cx: &Cx, bytes: &'a [u8], key: &[u8; 32], limit: usize) -> Result<&'a [u8]> {
    checkpoint(cx)?;
    if bytes.len() > limit { return Err(FrankenError::TooBig); }
    let length = bytes.len().checked_sub(TAG_BYTES).ok_or_else(|| corrupt("short native transport tag"))?;
    let (body, tag) = bytes.split_at(length);
    let expected = blake3::Hash::from_bytes(tag.try_into().map_err(|_| corrupt("short native transport tag"))?);
    // Hash equality, unlike slice equality, uses BLAKE3's constant-time comparison.
    if mac(cx, key, body)? != expected { return Err(corrupt("native transport authentication failed")); }
    Ok(body)
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let (head, tail) = self.0.split_at_checked(len).ok_or_else(|| corrupt("truncated native transport message"))?;
        self.0 = tail;
        Ok(head)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?.try_into().map_err(|_| corrupt("truncated native transport field"))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::future::{Future, Ready, pending, ready};
    use std::path::Path;
    use std::task::{Context, Waker};

    use asupersync::runtime::RuntimeBuilder;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_types::{Oti, SymbolRecordFlags, reconstruct_systematic_happy_path};
    use fsqlite_vfs::{MemoryVfs, Vfs};

    use super::*;
    use crate::native_durability::NativeDurabilityLimits;

    const KEY: [u8; 32] = [0x59; 32];
    const DB: ObjectId = ObjectId::from_bytes([0x31; 16]);
    type File = <MemoryVfs as Vfs>::File;
    type Log = NativeDurabilityLog<File, File>;

    fn run(future: impl Future<Output = ()>) {
        RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
    }
    fn file(vfs: &MemoryVfs, cx: &Cx, name: &str) -> File {
        vfs.open(cx, Some(Path::new(name)),
            VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::WAL).unwrap().0
    }
    fn log(vfs: &MemoryVfs, cx: &Cx) -> Log {
        Log::create(cx, file(vfs, cx, "objects"), file(vfs, cx, "markers"), NativeDurabilityLimits::default()).unwrap()
    }
    fn symbol(value: u64) -> SymbolRecord {
        let bytes = value.to_le_bytes().to_vec();
        SymbolRecord::new(ObjectId::derive_from_canonical_bytes(&bytes),
            Oti { f: 8, al: 1, t: 8, z: 1, n: 1 }, 0, bytes, SymbolRecordFlags::SYSTEMATIC_RUN_START)
    }
    fn verify(id: ObjectId, records: Vec<SymbolRecord>) -> Ready<Result<()>> {
        ready(reconstruct_systematic_happy_path(&records)
            .map_err(|error| corrupt(&error.to_string())).and_then(|bytes| {
                if ObjectId::derive_from_canonical_bytes(&bytes) == id { Ok(()) }
                else { Err(corrupt("transport test object identity mismatch")) }
            }))
    }
    async fn append(log: &mut Log, cx: &Cx, time: u64) {
        let seq = log.published_tip.get() + 1;
        let records = [symbol(seq * 2), symbol(seq * 2 + 1)];
        let marker = CommitMarker::new(CommitSeq::new(seq), time,
            records[0].object_id, records[1].object_id, log.previous_marker);
        log.append_symbols(cx, &records).await.unwrap();
        log.publish(cx, &[marker], verify).await.unwrap();
    }
    async fn bytes(file: &File, cx: &Cx) -> Vec<u8> {
        let mut bytes = vec![0; usize::try_from(file.file_size(cx).unwrap()).unwrap()];
        read_exact_at(file, cx, &mut bytes, 0).await.unwrap();
        bytes
    }
    fn decode(cx: &Cx, wire: &[u8]) -> NativeReplicationPlan {
        NativeReplicationPlan::from_bytes(cx, wire, DB, &KEY, NativeReplicationLimits::default()).unwrap()
    }
    async fn messages(log: &mut Log, cx: &Cx, plan: &NativeReplicationPlan) -> Vec<Vec<u8>> {
        let mut messages = Vec::new();
        for id in plan.object_ids() {
            messages.push(log.export_replication_object(cx, plan, id, &KEY, NativeReplicationLimits::default()).await.unwrap());
        }
        messages
    }
    async fn recover(log: Log, cx: &Cx) -> Log {
        let mut recovery = log.into_recovery();
        recovery.prepare(cx, verify).await.unwrap();
        recovery.finish().unwrap().into_log().0
    }

    #[test]
    fn wire_plan_authentication_canonicality_and_counts_fail_closed() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut source = log(&vfs, &cx);
            append(&mut source, &cx, 10).await;
            let limits = NativeReplicationLimits::default();
            let plan = source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 1, limits).await.unwrap().unwrap();
            let wire = plan.to_bytes(&cx, &KEY, limits).unwrap();
            assert_eq!(decode(&cx, &wire).to_bytes(&cx, &KEY, limits).unwrap(), wire);
            for end in 0..wire.len() {
                assert!(NativeReplicationPlan::from_bytes(&cx, &wire[..end], DB, &KEY, limits).is_err(), "end={end}");
            }
            assert!(NativeReplicationPlan::from_bytes(&cx, &wire, DB, &[3; 32], limits).is_err());
            assert!(NativeReplicationPlan::from_bytes(&cx, &wire, ObjectId::from_bytes([8; 16]), &KEY, limits).is_err());
            for offset in [0, 8, 24, 32, 48, 52, PLAN_HEADER, wire.len() - 1] {
                let mut damaged = wire.clone(); damaged[offset] ^= 1;
                assert!(NativeReplicationPlan::from_bytes(&cx, &damaged, DB, &KEY, limits).is_err());
            }
            // Authorized-but-malformed sender: recomputing the transport MAC
            // does not excuse bogus counts, duplicate inventories or padding.
            for case in 0..7 {
                let mut body = wire[..wire.len() - TAG_BYTES].to_vec();
                match case {
                    0 => body[48..52].copy_from_slice(&u32::MAX.to_le_bytes()),
                    1 => body[52..56].copy_from_slice(&u32::MAX.to_le_bytes()),
                    2 => {
                        let start = PLAN_HEADER + COMMIT_MARKER_RECORD_V1_SIZE;
                        let id: [u8; 16] = body[start..start + 16].try_into().unwrap();
                        body[start + 16..start + 32].copy_from_slice(&id);
                    }
                    3 => body.push(0),
                    // The shared marker decoder accepts reserved bytes. The
                    // transport must still reject noncanonical authenticated
                    // records instead of silently changing their chain digest.
                    4 => body[PLAN_HEADER + 1] = 1,
                    5 => body[PLAN_HEADER + 83] = 1,
                    6 => body[PLAN_HEADER + 50] = 1,
                    _ => unreachable!(),
                }
                let signed = seal(&cx, body, &KEY).unwrap();
                assert!(NativeReplicationPlan::from_bytes(&cx, &signed, DB, &KEY, limits).is_err(), "case={case}");
            }
            let too_small = NativeReplicationLimits { max_plan_bytes: wire.len() - 1, ..limits };
            assert!(matches!(NativeReplicationPlan::from_bytes(&cx, &wire, DB, &KEY, too_small), Err(FrankenError::TooBig)));
            source.close(&cx).unwrap();
        });
    }

    #[test]
    fn transported_messages_publish_after_source_close_and_duplicate_delivery_is_idempotent() {
        run(async {
            let cx = Cx::new(); let source_vfs = MemoryVfs::new(); let mut source = log(&source_vfs, &cx);
            for time in 1..=3 { append(&mut source, &cx, time).await; }
            source.append_symbols(&cx, &[symbol(99)]).await.unwrap();
            let limits = NativeReplicationLimits::default();
            let first = source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 2, limits).await.unwrap().unwrap();
            let second = source.export_replication_plan(&cx, DB, first.target(), 2, limits).await.unwrap().unwrap();
            let first_wire = first.to_bytes(&cx, &KEY, limits).unwrap();
            let second_wire = second.to_bytes(&cx, &KEY, limits).unwrap();
            let first_messages = messages(&mut source, &cx, &first).await;
            let second_messages = messages(&mut source, &cx, &second).await;
            let original = bytes(&source.markers, &cx).await;
            source.close(&cx).unwrap(); drop(source);
            // A different VFS and only portable bytes: neither source descriptor
            // nor an in-process source log is available to the destination.
            let replica_vfs = MemoryVfs::new(); let mut replica = log(&replica_vfs, &cx);
            let mut receiver = replica.receive_replication_plan(&cx, decode(&cx, &first_wire), limits).await.unwrap().unwrap();
            for message in first_messages.iter().rev() {
                receiver.stage_object(&cx, message, &KEY).await.unwrap();
                receiver.stage_object(&cx, message, &KEY).await.unwrap();
            }
            let receipt = receiver.finish(&cx, verify).await.unwrap();
            assert_eq!(receipt.first_seq, CommitSeq::new(1)); assert_eq!(receipt.last_seq, CommitSeq::new(2));
            assert_eq!(replica.record_count, 7, "retransmissions must not duplicate physical symbols");
            let staged = bytes(&replica.symbols, &cx).await;
            let mut receiver = replica.receive_replication_plan(&cx, decode(&cx, &second_wire), limits).await.unwrap().unwrap();
            for message in &second_messages { receiver.stage_object(&cx, message, &KEY).await.unwrap(); }
            assert_eq!(receiver.finish(&cx, verify).await.unwrap().commits, 1);
            assert_eq!(bytes(&replica.symbols, &cx).await, staged);
            assert_eq!(bytes(&replica.markers, &cx).await, original);
            for wire in [&first_wire, &second_wire] {
                assert!(replica.receive_replication_plan(&cx, decode(&cx, wire), limits).await.unwrap().is_none());
            }
            assert_eq!(bytes(&replica.markers, &cx).await, original);
            replica = recover(replica, &cx).await;
            assert_eq!(replica.replication_cursor(&cx).await.unwrap(), second.target());
            verify(symbol(99).object_id, replica.read_object(&cx, symbol(99).object_id).await.unwrap()).await.unwrap();
            replica.close(&cx).unwrap();
        });
    }

    #[test]
    fn malformed_object_messages_do_not_mutate_and_wrong_plan_or_signer_cannot_stage() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut source = log(&vfs, &cx);
            append(&mut source, &cx, 1).await; append(&mut source, &cx, 2).await;
            let limits = NativeReplicationLimits::default();
            let plan = source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 1, limits).await.unwrap().unwrap();
            let other = source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 2, limits).await.unwrap().unwrap();
            let id = plan.object_ids().next().unwrap();
            let message = source.export_replication_object(&cx, &plan, id, &KEY, limits).await.unwrap();
            let wrong_plan = source.export_replication_object(&cx, &other, id, &KEY, limits).await.unwrap();
            let other_key = [0x60; 32];
            let wrong_signer = source.export_replication_object(&cx, &plan, id, &other_key, limits).await.unwrap();
            let replica_vfs = MemoryVfs::new(); let mut replica = log(&replica_vfs, &cx);
            let wire = plan.to_bytes(&cx, &KEY, limits).unwrap();
            let mut receiver = replica.receive_replication_plan(&cx, decode(&cx, &wire), limits).await.unwrap().unwrap();
            for end in 0..message.len() {
                assert!(receiver.stage_object(&cx, &message[..end], &KEY).await.is_err());
            }
            assert!(receiver.stage_object(&cx, &wrong_plan, &KEY).await.is_err());
            assert!(receiver.stage_object(&cx, &wrong_signer, &other_key).await.is_err());
            assert!(receiver.stage_object(&cx, &message, &other_key).await.is_err());
            let mut body = message[..message.len() - TAG_BYTES].to_vec();
            body[56..60].copy_from_slice(&u32::MAX.to_le_bytes());
            assert!(receiver.stage_object(&cx, &seal(&cx, body, &KEY).unwrap(), &KEY).await.is_err());
            let cancelled = Cx::new(); cancelled.cancel();
            assert!(matches!(receiver.stage_object(&cancelled, &message, &KEY).await, Err(FrankenError::Interrupt)));
            assert_eq!(receiver.destination.symbols.file_size(&cx).unwrap(), 0);
            assert_eq!(receiver.destination.markers.file_size(&cx).unwrap(), 0);
            assert!(receiver.outstanding_write().is_none());
            for message in messages(&mut source, &cx, &plan).await {
                receiver.stage_object(&cx, &message, &KEY).await.unwrap();
            }
            receiver.finish(&cx, verify).await.unwrap();
            replica.close(&cx).unwrap(); source.close(&cx).unwrap();
        });
    }

    #[test]
    fn incomplete_and_dropped_publication_need_recovery_but_reuse_staged_objects() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut source = log(&vfs, &cx);
            append(&mut source, &cx, 1).await;
            let limits = NativeReplicationLimits::default();
            let plan = source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 1, limits).await.unwrap().unwrap();
            let wire = plan.to_bytes(&cx, &KEY, limits).unwrap();
            let messages = messages(&mut source, &cx, &plan).await;
            let replica_vfs = MemoryVfs::new(); let mut replica = log(&replica_vfs, &cx);
            let mut receiver = replica.receive_replication_plan(&cx, decode(&cx, &wire), limits).await.unwrap().unwrap();
            receiver.stage_object(&cx, &messages[0], &KEY).await.unwrap();
            assert!(receiver.finish(&cx, verify).await.is_err());
            assert!(replica.needs_recovery()); assert_eq!(replica.published_tip(), CommitSeq::ZERO);
            assert!(replica.receive_replication_plan(&cx, decode(&cx, &wire), limits).await.is_err());
            replica = recover(replica, &cx).await;
            let mut receiver = replica.receive_replication_plan(&cx, decode(&cx, &wire), limits).await.unwrap().unwrap();
            for message in &messages { receiver.stage_object(&cx, message, &KEY).await.unwrap(); }
            let mut publishing = Box::pin(receiver.finish(&cx, |_, _| pending::<Result<()>>()));
            assert!(publishing.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
            drop(publishing);
            assert!(replica.needs_recovery()); assert!(replica.outstanding_write().is_none());
            let staged = bytes(&replica.symbols, &cx).await;
            replica = recover(replica, &cx).await;
            let mut receiver = replica.receive_replication_plan(&cx, decode(&cx, &wire), limits).await.unwrap().unwrap();
            for message in &messages { receiver.stage_object(&cx, message, &KEY).await.unwrap(); }
            receiver.finish(&cx, verify).await.unwrap();
            assert_eq!(bytes(&replica.symbols, &cx).await, staged);
            assert_eq!(replica.record_count, 2);
            replica.close(&cx).unwrap(); source.close(&cx).unwrap();
        });
    }

    #[test]
    fn anchored_partial_prefix_resumes_but_forks_gaps_and_clock_regressions_are_rejected() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut source = log(&vfs, &cx);
            for time in 10..=12 { append(&mut source, &cx, time).await; }
            let limits = NativeReplicationLimits::default();
            let plan = source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 3, limits).await.unwrap().unwrap();
            let wire = plan.to_bytes(&cx, &KEY, limits).unwrap();
            let object_messages = messages(&mut source, &cx, &plan).await;
            for fork in [false, true] {
                let replica_vfs = MemoryVfs::new(); let mut replica = log(&replica_vfs, &cx);
                append(&mut replica, &cx, if fork { 9 } else { 10 }).await;
                let before = (bytes(&replica.symbols, &cx).await, bytes(&replica.markers, &cx).await);
                if fork {
                    assert!(replica.receive_replication_plan(&cx, decode(&cx, &wire), limits).await.is_err());
                    assert!(!replica.needs_recovery());
                    assert_eq!(bytes(&replica.symbols, &cx).await, before.0);
                    assert_eq!(bytes(&replica.markers, &cx).await, before.1);
                } else {
                    let base = replica.replication_cursor(&cx).await.unwrap();
                    let suffix = source.export_replication_plan(&cx, DB, base, 1, limits).await.unwrap().unwrap();
                    let replica2_vfs = MemoryVfs::new(); let mut replica2 = log(&replica2_vfs, &cx);
                    assert!(replica2.receive_replication_plan(&cx, decode(&cx, &suffix.to_bytes(&cx, &KEY, limits).unwrap()), limits).await.is_err());
                    // Even a signed, contiguous suffix cannot decrease the base clock.
                    let mut old_clock = decode(&cx, &suffix.to_bytes(&cx, &KEY, limits).unwrap());
                    let old = &old_clock.markers[0];
                    old_clock.markers[0] = CommitMarker::new(old.commit_seq, 1, old.capsule_object_id, old.proof_object_id, old.prev_marker);
                    let old_clock = decode(&cx, &old_clock.to_bytes(&cx, &KEY, limits).unwrap());
                    assert!(replica.receive_replication_plan(&cx, old_clock, limits).await.is_err());
                    assert!(!replica.needs_recovery());
                    let mut receiver = replica.receive_replication_plan(&cx, decode(&cx, &wire), limits).await.unwrap().unwrap();
                    for message in &object_messages { receiver.stage_object(&cx, message, &KEY).await.unwrap(); }
                    let receipt = receiver.finish(&cx, verify).await.unwrap();
                    assert_eq!(receipt.first_seq, CommitSeq::new(2)); assert_eq!(receipt.commits, 2);
                    assert_eq!(replica.record_count, 6);
                    replica2.close(&cx).unwrap();
                }
                replica.close(&cx).unwrap();
            }
            source.close(&cx).unwrap();
        });
    }

    #[test]
    fn conflicting_staged_envelopes_and_source_suffix_corruption_never_publish() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut source = log(&vfs, &cx);
            append(&mut source, &cx, 1).await; append(&mut source, &cx, 2).await;
            let limits = NativeReplicationLimits::default();
            let plan = source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 1, limits).await.unwrap().unwrap();
            let wire = plan.to_bytes(&cx, &KEY, limits).unwrap();
            let original = symbol(2);
            let message = source.export_replication_object(&cx, &plan, original.object_id, &KEY, limits).await.unwrap();
            let replica_vfs = MemoryVfs::new(); let mut replica = log(&replica_vfs, &cx);
            let changed = SymbolRecord::new(original.object_id, original.oti, original.esi,
                vec![0xFF; 8], SymbolRecordFlags::SYSTEMATIC_RUN_START);
            replica.append_symbols(&cx, &[changed]).await.unwrap();
            let before = bytes(&replica.symbols, &cx).await;
            let mut receiver = replica.receive_replication_plan(&cx, decode(&cx, &wire), limits).await.unwrap().unwrap();
            assert!(receiver.stage_object(&cx, &message, &KEY).await.is_err()); drop(receiver);
            assert_eq!(bytes(&replica.symbols, &cx).await, before);
            assert_eq!(replica.markers.file_size(&cx).unwrap(), 0);
            assert!(replica.needs_recovery());
            // The second marker is outside the requested one-commit batch.
            let offset = u64_len(COMMIT_MARKER_RECORD_V1_SIZE).unwrap();
            source.markers.write(&cx, &[0xFF], offset).await.unwrap();
            assert!(source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 1, limits).await.is_err());
            assert!(source.needs_recovery());
            assert!(source.replication_cursor(&cx).await.is_err());
            replica.close(&cx).unwrap(); source.close(&cx).unwrap();
        });
    }

    mod semantic_validation {
        use super::*;
        use crate::native_commit::CommitSubmission;
        use crate::native_commit::durable::NativeCommitProof;
        use fsqlite_types::{PageNumber, TxnEpoch, TxnId, TxnToken};

        struct Codec;
        impl NativeObjectCodec for Codec {
            fn encode(&self, _: &Cx, payload: &[u8]) -> Result<Vec<SymbolRecord>> {
                let size = u32::try_from(payload.len()).map_err(|_| FrankenError::TooBig)?;
                Ok(vec![SymbolRecord::new(
                    ObjectId::derive_from_canonical_bytes(payload),
                    Oti { f: u64::from(size), al: 1, t: size, z: 1, n: 1 },
                    0, payload.to_vec(), SymbolRecordFlags::SYSTEMATIC_RUN_START,
                )])
            }
            fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
                let payload = reconstruct_systematic_happy_path(records)
                    .map_err(|error| corrupt(&error.to_string()))?;
                if ObjectId::derive_from_canonical_bytes(&payload) != id {
                    return Err(corrupt("invalid semantic fixture object"));
                }
                Ok(payload)
            }
        }

        async fn append_proof(log: &mut Log, cx: &Cx, edit: impl FnOnce(&mut NativeCommitProof)) {
            let seq = log.published_tip.get() + 1;
            let payload = seq.to_le_bytes();
            let capsule = Codec.encode(cx, &payload).unwrap();
            let mut proof = NativeCommitProof {
                commit_seq: CommitSeq::new(seq),
                commit_time_unix_ns: seq,
                submission: CommitSubmission {
                    capsule_object_id: capsule[0].object_id,
                    capsule_digest: *blake3::hash(&payload).as_bytes(),
                    write_set_pages: vec![PageNumber::ONE],
                    witness_refs: vec![], edge_ids: vec![], merge_witness_ids: vec![],
                    txn_token: TxnToken::new(TxnId::new(seq).unwrap(), TxnEpoch::new(1)),
                    begin_seq: CommitSeq::new(seq - 1),
                },
            };
            edit(&mut proof);
            let proof_records = Codec.encode(cx, &proof.to_bytes().unwrap()).unwrap();
            let marker = CommitMarker::new(CommitSeq::new(seq), seq,
                capsule[0].object_id, proof_records[0].object_id, log.previous_marker);
            log.append_symbols(cx, &capsule).await.unwrap();
            log.append_symbols(cx, &proof_records).await.unwrap();
            // Deliberately envelope-only: this fixture can represent an
            // authenticated but semantically invalid upstream producer.
            log.publish(cx, &[marker], verify).await.unwrap();
        }

        async fn receive<'a>(
            source: &mut Log, destination: &'a mut Log, cx: &Cx, commits: usize,
        ) -> NativeReplicationReceiver<'a, File, File> {
            let limits = NativeReplicationLimits::default();
            let plan = source.export_replication_plan(cx, DB,
                NativeReplicationCursor::genesis(), commits, limits).await.unwrap().unwrap();
            let wire = plan.to_bytes(cx, &KEY, limits).unwrap();
            let messages = messages(source, cx, &plan).await;
            let mut receiver = destination.receive_replication_plan(cx, decode(cx, &wire), limits)
                .await.unwrap().unwrap();
            for message in messages { receiver.stage_object(cx, &message, &KEY).await.unwrap(); }
            receiver
        }

        #[test]
        fn validation_replays_existing_commits_before_the_missing_suffix() {
            run(async {
                let cx = Cx::new();
                let source_vfs = MemoryVfs::new(); let mut source = log(&source_vfs, &cx);
                for _ in 0..3 { append_proof(&mut source, &cx, |_| {}).await; }
                let dest_vfs = MemoryVfs::new(); let mut destination = log(&dest_vfs, &cx);
                let first = receive(&mut source, &mut destination, &cx, 1).await
                    .finish_validated(&cx, &Codec, |_, _| Ok(())).await.unwrap();
                assert_eq!(first.commits, 1);
                let mut visited = Vec::new();
                let receiver = receive(&mut source, &mut destination, &cx, 3).await;
                source.close(&cx).unwrap();
                let receipt = receiver.finish_validated(&cx, &Codec, |candidate, evidence| {
                    let sequence = candidate.proof.commit_seq.get();
                    assert_eq!(candidate.capsule.as_ref(), &sequence.to_le_bytes());
                    assert_eq!(evidence.len(), 1);
                    assert!(evidence.contains_key(&candidate.proof.submission.capsule_object_id));
                    visited.push(sequence);
                    Ok(())
                }).await.unwrap();
                assert_eq!(visited, vec![1, 2, 3]);
                assert_eq!(receipt.first_seq, CommitSeq::new(2));
                assert_eq!(receipt.last_seq, CommitSeq::new(3));
                destination = recover(destination, &cx).await;
                assert_eq!(destination.published_tip(), CommitSeq::new(3));
                destination.close(&cx).unwrap();
            });
        }

        #[test]
        fn a_late_semantic_rejection_or_cancellation_cannot_publish_an_earlier_marker() {
            run(async {
                for cancel in [false, true] {
                    let cx = Cx::new();
                    let source_vfs = MemoryVfs::new(); let mut source = log(&source_vfs, &cx);
                    for _ in 0..3 { append_proof(&mut source, &cx, |_| {}).await; }
                    let dest_vfs = MemoryVfs::new(); let mut destination = log(&dest_vfs, &cx);
                    let receiver = receive(&mut source, &mut destination, &cx, 3).await;
                    let staged = bytes(&receiver.destination.symbols, &cx).await;
                    let mut visited = 0;
                    let result = receiver.finish_validated(&cx, &Codec, |_, _| {
                        visited += 1;
                        if visited == 3 {
                            if cancel { cx.cancel(); } else { return Err(FrankenError::Abort); }
                        }
                        Ok(())
                    }).await;
                    assert_eq!(visited, 3);
                    if cancel { assert!(matches!(result, Err(FrankenError::Interrupt))); }
                    else { assert!(matches!(result, Err(FrankenError::Abort))); }
                    let clean = Cx::new();
                    assert_eq!(destination.markers.file_size(&clean).unwrap(), 0);
                    assert_eq!(bytes(&destination.symbols, &clean).await, staged);
                    assert_eq!(destination.published_tip(), CommitSeq::ZERO);
                    assert!(destination.needs_recovery());
                    assert!(destination.outstanding_write().is_none());
                    destination = recover(destination, &clean).await;
                    receive(&mut source, &mut destination, &clean, 3).await
                        .finish_validated(&clean, &Codec, |_, _| Ok(())).await.unwrap();
                    assert_eq!(bytes(&destination.symbols, &clean).await, staged);
                    destination.close(&clean).unwrap(); source.close(&clean).unwrap();
                }
            });
        }

        #[test]
        fn transport_authentication_cannot_replace_bound_proof_and_evidence_checks() {
            run(async {
                for invalid in 0..5 {
                    let cx = Cx::new();
                    let source_vfs = MemoryVfs::new(); let mut source = log(&source_vfs, &cx);
                    append_proof(&mut source, &cx, |proof| match invalid {
                        0 => proof.commit_seq = CommitSeq::new(2),
                        1 => proof.commit_time_unix_ns = 99,
                        2 => proof.submission.capsule_object_id = DB,
                        3 => proof.submission.capsule_digest[0] ^= 1,
                        4 => proof.submission.witness_refs.push(DB),
                        _ => unreachable!(),
                    }).await;
                    let dest_vfs = MemoryVfs::new(); let mut destination = log(&dest_vfs, &cx);
                    let receiver = receive(&mut source, &mut destination, &cx, 1).await;
                    let mut called = false;
                    let result = receiver.finish_validated(&cx, &Codec, |_, _| {
                        called = true; Ok(())
                    }).await;
                    assert!(matches!(result, Err(FrankenError::WalCorrupt { .. })), "case={invalid}");
                    assert!(!called, "invalid envelope/binding must not reach semantic code");
                    assert_eq!(destination.markers.file_size(&cx).unwrap(), 0);
                    assert_eq!(destination.published_tip(), CommitSeq::ZERO);
                    assert!(destination.needs_recovery());
                    destination.close(&cx).unwrap(); source.close(&cx).unwrap();
                }
            });
        }

        #[test]
        fn local_reuse_requests_only_new_objects_without_skipping_history_validation() {
            run(async {
                let cx = Cx::new();
                let source_vfs = MemoryVfs::new(); let mut source = log(&source_vfs, &cx);
                let dest_vfs = MemoryVfs::new(); let mut destination = log(&dest_vfs, &cx);
                append_proof(&mut source, &cx, |_| {}).await;
                receive(&mut source, &mut destination, &cx, 1).await
                    .finish_validated(&cx, &Codec, |_, _| Ok(())).await.unwrap();
                append_proof(&mut source, &cx, |_| {}).await;
                let limits = NativeReplicationLimits::default();
                let base = destination.replication_cursor(&cx).await.unwrap();
                let plan = source.export_replication_plan(&cx, DB, base, 1, limits)
                    .await.unwrap().unwrap();
                assert_eq!(plan.object_ids().len(), 4);
                let before = bytes(&destination.symbols, &cx).await;
                let mut receiver = destination.receive_replication_plan(
                    &cx, decode(&cx, &plan.to_bytes(&cx, &KEY, limits).unwrap()), limits,
                ).await.unwrap().unwrap();
                assert_eq!(receiver.missing_object_ids().count(), 4);
                assert_eq!(receiver.reuse_available_objects(&cx, &Codec).await.unwrap(), 2);
                assert_eq!(bytes(&receiver.destination.symbols, &cx).await, before);
                let missing: Vec<_> = receiver.missing_object_ids().collect();
                assert_eq!(missing.len(), 2);
                assert!(missing.windows(2).all(|pair| pair[0] < pair[1]));
                for id in missing {
                    let message = source.export_replication_object(&cx, &plan, id, &KEY, limits)
                        .await.unwrap();
                    receiver.stage_object(&cx, &message, &KEY).await.unwrap();
                }
                assert_eq!(receiver.missing_object_ids().count(), 0);
                assert_eq!(receiver.reuse_available_objects(&cx, &Codec).await.unwrap(), 0);
                let mut visited = Vec::new();
                let receipt = receiver.finish_validated(&cx, &Codec, |candidate, _| {
                    visited.push(candidate.proof.commit_seq.get()); Ok(())
                }).await.unwrap();
                assert_eq!(visited, vec![1, 2]);
                assert_eq!(receipt.first_seq, CommitSeq::new(2));
                assert_eq!(receipt.commits, 1);
                assert_eq!(destination.record_count, 4);
                assert_eq!(bytes(&destination.markers, &cx).await, bytes(&source.markers, &cx).await);
                destination = recover(destination, &cx).await;
                assert_eq!(destination.replication_cursor(&cx).await.unwrap(), plan.target());
                destination.close(&cx).unwrap(); source.close(&cx).unwrap();
            });
        }

        #[test]
        fn local_reuse_resumes_interrupted_inventory_without_redownloading_staged_orphans() {
            run(async {
                for staged_count in [1, 6] {
                    let cx = Cx::new();
                    let source_vfs = MemoryVfs::new(); let mut source = log(&source_vfs, &cx);
                    for _ in 0..3 { append_proof(&mut source, &cx, |_| {}).await; }
                    let limits = NativeReplicationLimits::default();
                    let plan = source.export_replication_plan(
                        &cx, DB, NativeReplicationCursor::genesis(), 3, limits,
                    ).await.unwrap().unwrap();
                    let wire = plan.to_bytes(&cx, &KEY, limits).unwrap();
                    let dest_vfs = MemoryVfs::new(); let mut destination = log(&dest_vfs, &cx);
                    let mut receiver = destination.receive_replication_plan(&cx, decode(&cx, &wire), limits)
                        .await.unwrap().unwrap();
                    for id in plan.object_ids().take(staged_count) {
                        let message = source.export_replication_object(&cx, &plan, id, &KEY, limits)
                            .await.unwrap();
                        receiver.stage_object(&cx, &message, &KEY).await.unwrap();
                    }
                    drop(receiver);
                    assert!(destination.needs_recovery());
                    let staged = bytes(&destination.symbols, &cx).await;
                    destination = recover(destination, &cx).await;
                    let mut receiver = destination.receive_replication_plan(&cx, decode(&cx, &wire), limits)
                        .await.unwrap().unwrap();
                    assert_eq!(receiver.reuse_available_objects(&cx, &Codec).await.unwrap(), staged_count);
                    let missing: Vec<_> = receiver.missing_object_ids().collect();
                    assert_eq!(missing.len(), 6 - staged_count);
                    let mut messages = Vec::new();
                    for id in missing {
                        messages.push(source.export_replication_object(&cx, &plan, id, &KEY, limits)
                            .await.unwrap());
                    }
                    source.close(&cx).unwrap(); drop(source);
                    for message in messages { receiver.stage_object(&cx, &message, &KEY).await.unwrap(); }
                    let receipt = receiver.finish_validated(&cx, &Codec, |_, _| Ok(())).await.unwrap();
                    assert_eq!(receipt.commits, 3);
                    assert_eq!(destination.record_count, 6);
                    assert!(bytes(&destination.symbols, &cx).await.starts_with(&staged));
                    destination.close(&cx).unwrap();
                }
            });
        }

        #[test]
        fn local_reuse_failed_revalidation_revokes_the_entry_without_mutating_storage() {
            struct Reject;
            impl NativeObjectCodec for Reject {
                fn encode(&self, _: &Cx, _: &[u8]) -> Result<Vec<SymbolRecord>> { Err(FrankenError::Abort) }
                fn decode(&self, _: &Cx, _: ObjectId, _: &[SymbolRecord]) -> Result<Vec<u8>> { Err(FrankenError::Abort) }
            }
            struct Cancel;
            impl NativeObjectCodec for Cancel {
                fn encode(&self, cx: &Cx, payload: &[u8]) -> Result<Vec<SymbolRecord>> { Codec.encode(cx, payload) }
                fn decode(&self, cx: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
                    let bytes = Codec.decode(cx, id, records)?; cx.cancel(); Ok(bytes)
                }
            }
            run(async {
                let cx = Cx::new();
                let source_vfs = MemoryVfs::new(); let mut source = log(&source_vfs, &cx);
                append_proof(&mut source, &cx, |_| {}).await;
                let dest_vfs = MemoryVfs::new(); let mut destination = log(&dest_vfs, &cx);
                let mut receiver = receive(&mut source, &mut destination, &cx, 1).await;
                let id = receiver.plan.objects[0];
                let before = bytes(&receiver.destination.symbols, &cx).await;
                assert!(receiver.reuse_object(&cx, id, &Codec).await.unwrap());
                assert!(matches!(receiver.reuse_object(&cx, id, &Reject).await, Err(FrankenError::Abort)));
                assert_eq!(receiver.missing_object_ids().collect::<Vec<_>>(), vec![id]);
                assert!(receiver.reuse_object(&cx, DB, &Codec).await.is_err());
                assert_eq!(receiver.missing_object_ids().collect::<Vec<_>>(), vec![id]);
                assert!(receiver.reuse_object(&cx, id, &Codec).await.unwrap());
                let cancelled = Cx::new();
                assert!(matches!(receiver.reuse_object(&cancelled, id, &Cancel).await, Err(FrankenError::Interrupt)));
                assert_eq!(receiver.missing_object_ids().collect::<Vec<_>>(), vec![id]);
                assert_eq!(bytes(&receiver.destination.symbols, &cx).await, before);
                assert_eq!(receiver.destination.markers.file_size(&cx).unwrap(), 0);
                assert!(receiver.outstanding_write().is_none());
                assert_eq!(receiver.reuse_available_objects(&cx, &Codec).await.unwrap(), 1);
                receiver.finish_validated(&cx, &Codec, |_, _| Ok(())).await.unwrap();
                destination.close(&cx).unwrap(); source.close(&cx).unwrap();
            });
        }

        #[test]
        fn local_reuse_requires_complete_reconstruction_not_index_membership() {
            run(async {
                let cx = Cx::new();
                let source_vfs = MemoryVfs::new(); let mut source = log(&source_vfs, &cx);
                let payload = [0x5A; 8]; let id = ObjectId::derive_from_canonical_bytes(&payload);
                let oti = Oti { f: 8, al: 1, t: 4, z: 1, n: 1 };
                let records = [
                    SymbolRecord::new(id, oti, 0, payload[..4].to_vec(), SymbolRecordFlags::SYSTEMATIC_RUN_START),
                    SymbolRecord::new(id, oti, 1, payload[4..].to_vec(), SymbolRecordFlags::empty()),
                ];
                source.append_symbols(&cx, &records).await.unwrap();
                source.publish(&cx, &[CommitMarker::new(CommitSeq::new(1), 1, id, id, None)], verify)
                    .await.unwrap();
                let limits = NativeReplicationLimits::default();
                let plan = source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 1, limits)
                    .await.unwrap().unwrap();
                let dest_vfs = MemoryVfs::new(); let mut destination = log(&dest_vfs, &cx);
                let wire = plan.to_bytes(&cx, &KEY, limits).unwrap();
                let mut receiver = destination.receive_replication_plan(&cx, decode(&cx, &wire), limits)
                    .await.unwrap().unwrap();
                assert!(!receiver.reuse_object(&cx, id, &Codec).await.unwrap());
                // Actual on-disk, intact but incomplete symbols, not a fake cache bit.
                receiver.destination.append_symbols(&cx, &records[..1]).await.unwrap();
                let before = bytes(&receiver.destination.symbols, &cx).await;
                assert!(receiver.reuse_object(&cx, id, &Codec).await.is_err());
                assert_eq!(receiver.missing_object_ids().collect::<Vec<_>>(), vec![id]);
                assert_eq!(bytes(&receiver.destination.symbols, &cx).await, before);
                let message = source.export_replication_object(&cx, &plan, id, &KEY, limits).await.unwrap();
                receiver.stage_object(&cx, &message, &KEY).await.unwrap();
                assert!(receiver.reuse_object(&cx, id, &Codec).await.unwrap());
                receiver.finish(&cx, verify).await.unwrap();
                assert_eq!(destination.record_count, 2);
                assert_eq!(Codec.decode(&cx, id, &destination.read_object(&cx, id).await.unwrap()).unwrap(), payload);
                destination.close(&cx).unwrap(); source.close(&cx).unwrap();
            });
        }

        #[test]
        fn local_reuse_never_replaces_final_destination_readback() {
            run(async {
                let cx = Cx::new();
                let source_vfs = MemoryVfs::new(); let mut source = log(&source_vfs, &cx);
                append_proof(&mut source, &cx, |_| {}).await;
                let dest_vfs = MemoryVfs::new(); let mut destination = log(&dest_vfs, &cx);
                let mut receiver = receive(&mut source, &mut destination, &cx, 1).await;
                let id = receiver.plan.objects[0];
                assert!(receiver.reuse_object(&cx, id, &Codec).await.unwrap());
                let offset = receiver.destination.index[&id][0].offset + u64_len(SYMBOL_HEADER_BYTES).unwrap();
                receiver.destination.symbols.write(&cx, &[0xFF], offset).await.unwrap();
                assert_eq!(receiver.missing_object_ids().count(), 0);
                assert!(receiver.finish_validated(&cx, &Codec, |_, _| Ok(())).await.is_err());
                assert_eq!(destination.markers.file_size(&cx).unwrap(), 0);
                assert!(destination.needs_recovery());
                destination.close(&cx).unwrap(); source.close(&cx).unwrap();
            });
        }

        #[test]
        fn local_reuse_does_not_require_a_message_large_enough_to_retransmit_the_object() {
            run(async {
                let cx = Cx::new();
                let source_vfs = MemoryVfs::new(); let mut source = log(&source_vfs, &cx);
                append_proof(&mut source, &cx, |_| {}).await;
                let limits = NativeReplicationLimits {
                    max_object_message_bytes: OBJECT_HEADER + TAG_BYTES + SYMBOL_HEADER_BYTES + SYMBOL_TRAILER_BYTES + 1,
                    ..NativeReplicationLimits::default()
                };
                let plan = source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 1, limits)
                    .await.unwrap().unwrap();
                let dest_vfs = MemoryVfs::new(); let mut destination = log(&dest_vfs, &cx);
                for id in plan.object_ids() {
                    destination.append_symbols(&cx, &source.read_object(&cx, id).await.unwrap()).await.unwrap();
                    assert!(matches!(source.export_replication_object(&cx, &plan, id, &KEY, limits).await,
                        Err(FrankenError::TooBig)));
                }
                let wire = plan.to_bytes(&cx, &KEY, limits).unwrap();
                source.close(&cx).unwrap(); drop(source);
                let before = bytes(&destination.symbols, &cx).await;
                let mut receiver = destination.receive_replication_plan(&cx, decode(&cx, &wire), limits)
                    .await.unwrap().unwrap();
                assert_eq!(receiver.reuse_available_objects(&cx, &Codec).await.unwrap(), 2);
                assert_eq!(receiver.missing_object_ids().count(), 0);
                receiver.finish_validated(&cx, &Codec, |_, _| Ok(())).await.unwrap();
                assert_eq!(bytes(&destination.symbols, &cx).await, before);
                assert_eq!(destination.published_tip(), CommitSeq::new(1));
                destination.close(&cx).unwrap();
            });
        }

        #[test]
        fn missing_requests_select_only_unverified_members_and_empty_is_not_an_ack() {
            run(async {
                let cx = Cx::new();
                let source_vfs = MemoryVfs::new(); let mut source = log(&source_vfs, &cx);
                for _ in 0..2 { append_proof(&mut source, &cx, |_| {}).await; }
                let limits = NativeReplicationLimits::default();
                let plan = source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 2, limits)
                    .await.unwrap().unwrap();
                let dest_vfs = MemoryVfs::new(); let mut destination = log(&dest_vfs, &cx);
                let local = plan.object_ids().next().unwrap();
                destination.append_symbols(&cx, &source.read_object(&cx, local).await.unwrap()).await.unwrap();
                let mut receiver = destination.receive_replication_plan(
                    &cx, decode(&cx, &plan.to_bytes(&cx, &KEY, limits).unwrap()), limits,
                ).await.unwrap().unwrap();
                assert_eq!(receiver.reuse_available_objects(&cx, &Codec).await.unwrap(), 1);
                let request = receiver.missing_object_request(&cx, &KEY).unwrap();
                let requested = plan.requested_object_ids(&cx, &request, &KEY, limits).unwrap();
                assert_eq!(requested, receiver.missing_object_ids().collect::<Vec<_>>());
                assert_eq!(requested.len(), 3); assert!(!requested.contains(&local));
                for id in requested {
                    let message = source.export_replication_object(&cx, &plan, id, &KEY, limits).await.unwrap();
                    receiver.stage_object(&cx, &message, &KEY).await.unwrap();
                }
                let empty = receiver.missing_object_request(&cx, &KEY).unwrap();
                assert_eq!(empty.len(), REQUEST_HEADER + TAG_BYTES);
                assert!(plan.requested_object_ids(&cx, &empty, &KEY, limits).unwrap().is_empty());
                assert_eq!(receiver.destination.published_tip(), CommitSeq::ZERO);
                assert_eq!(receiver.destination.markers.file_size(&cx).unwrap(), 0);
                source.close(&cx).unwrap(); drop(source);
                let receipt = receiver.finish_validated(&cx, &Codec, |_, _| Ok(())).await.unwrap();
                assert_eq!(receipt.commits, 2); assert_eq!(destination.record_count, 4);
                destination.close(&cx).unwrap();
            });
        }

        #[test]
        fn missing_requests_reject_wrong_keys_plans_counts_and_noncanonical_inventories() {
            run(async {
                let cx = Cx::new();
                let source_vfs = MemoryVfs::new(); let mut source = log(&source_vfs, &cx);
                for _ in 0..2 { append_proof(&mut source, &cx, |_| {}).await; }
                let limits = NativeReplicationLimits::default();
                let plan = source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 1, limits)
                    .await.unwrap().unwrap();
                let other = source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 2, limits)
                    .await.unwrap().unwrap();
                let dest_vfs = MemoryVfs::new(); let mut destination = log(&dest_vfs, &cx);
                let receiver = destination.receive_replication_plan(
                    &cx, decode(&cx, &plan.to_bytes(&cx, &KEY, limits).unwrap()), limits,
                ).await.unwrap().unwrap();
                let wire = receiver.missing_object_request(&cx, &KEY).unwrap();
                assert!(other.requested_object_ids(&cx, &wire, &KEY, limits).is_err());
                assert!(plan.requested_object_ids(&cx, &wire, &[7; 32], limits).is_err());
                assert!(receiver.missing_object_request(&cx, &[7; 32]).is_err());
                for end in 0..wire.len() {
                    assert!(plan.requested_object_ids(&cx, &wire[..end], &KEY, limits).is_err(), "end={end}");
                }
                for case in 0..7 {
                    let mut body = wire[..wire.len() - TAG_BYTES].to_vec();
                    match case {
                        0 => body[0] ^= 1,
                        1 => body[8] ^= 1,
                        2 => body[40..44].copy_from_slice(&u32::MAX.to_le_bytes()),
                        3 => {
                            let first: [u8; 16] = body[44..60].try_into().unwrap();
                            body[60..76].copy_from_slice(&first);
                        }
                        4 => {
                            let first: [u8; 16] = body[44..60].try_into().unwrap();
                            let second: [u8; 16] = body[60..76].try_into().unwrap();
                            body[44..60].copy_from_slice(&second); body[60..76].copy_from_slice(&first);
                        }
                        5 => body[44..60].copy_from_slice(DB.as_bytes()),
                        6 => body.push(0),
                        _ => unreachable!(),
                    }
                    let malformed = seal(&cx, body, &KEY).unwrap();
                    assert!(plan.requested_object_ids(&cx, &malformed, &KEY, limits).is_err(), "case={case}");
                }
                let cancelled = Cx::new(); cancelled.cancel();
                assert!(matches!(receiver.missing_object_request(&cancelled, &KEY), Err(FrankenError::Interrupt)));
                assert!(matches!(plan.requested_object_ids(&cancelled, &wire, &KEY, limits), Err(FrankenError::Interrupt)));
                let tiny = NativeReplicationLimits { max_plan_bytes: wire.len() - 1, ..limits };
                assert!(plan.requested_object_ids(&cx, &wire, &KEY, tiny).is_err());
                assert_eq!(receiver.destination.symbols.file_size(&cx).unwrap(), 0);
                assert_eq!(receiver.destination.markers.file_size(&cx).unwrap(), 0);
                assert_eq!(receiver.missing_object_ids().count(), 4);
                drop(receiver);
                destination.close(&cx).unwrap(); source.close(&cx).unwrap();
            });
        }

        #[cfg(feature = "native")]
        #[test]
        fn missing_requests_reuse_authenticated_raptorq_survivors_and_publish_exact_pages() {
            use crate::native_commit::durable::codec::RaptorQNativeCodec;
            use crate::native_pages::{NativePageLimits, NativePageRecovery, NativePageStore};

            run(async {
                let cx = Cx::new(); cx.set_native_cx(asupersync::Cx::current().unwrap());
                let source_vfs = MemoryVfs::new();
                let mut store = NativePageStore::new(log(&source_vfs, &cx),
                    RaptorQNativeCodec::new(Some([0x7B; 32])), 512, NativePageLimits::default()).unwrap();
                let mut tx = store.begin(&cx).unwrap();
                for n in 2..=20 {
                    store.write_page(&cx, &mut tx, PageNumber::new(n).unwrap(),
                        Some(&[u8::try_from(n).unwrap(); 512])).unwrap();
                }
                store.commit(&cx, &mut tx, 100).await.unwrap(); drop(tx);
                let (mut physical, codec) = store.try_into_recovery().ok().unwrap();
                physical.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ())))
                    .await.unwrap();
                let (mut source, _) = physical.finish().unwrap().into_log();
                let limits = NativeReplicationLimits::default();
                let plan = source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 1, limits)
                    .await.unwrap().unwrap();
                let dest_vfs = MemoryVfs::new(); let mut destination = log(&dest_vfs, &cx);
                let mut retained = 0;
                for id in plan.object_ids() {
                    let mut records = source.read_object(&cx, id).await.unwrap();
                    let original = codec.decode(&cx, id, &records).unwrap();
                    assert!(records.iter().any(|record| record.esi & (1 << 31) != 0));
                    let systematic = records.iter().position(|record| record.esi & (1 << 31) == 0).unwrap();
                    records.remove(systematic);
                    assert_eq!(codec.decode(&cx, id, &records).unwrap(), original);
                    retained += records.len();
                    // Existing local cache holds real surviving authenticated envelopes,
                    // not a replay bit or a fabricated reconstruction result.
                    destination.append_symbols(&cx, &records).await.unwrap();
                }
                let before = bytes(&destination.symbols, &cx).await;
                let wire = plan.to_bytes(&cx, &KEY, limits).unwrap();
                source.close(&cx).unwrap(); drop(source);
                let mut receiver = destination.receive_replication_plan(&cx, decode(&cx, &wire), limits)
                    .await.unwrap().unwrap();
                let wrong = RaptorQNativeCodec::new(Some([0x7C; 32]));
                assert!(receiver.reuse_available_objects(&cx, &wrong).await.is_err());
                assert_eq!(receiver.missing_object_ids().count(), plan.object_ids().len());
                assert_eq!(receiver.reuse_available_objects(&cx, codec.as_ref()).await.unwrap(), plan.object_ids().len());
                let request = receiver.missing_object_request(&cx, &KEY).unwrap();
                assert!(plan.requested_object_ids(&cx, &request, &KEY, limits).unwrap().is_empty());
                let (receipt, image) = receiver.finish_page_snapshot(&cx, codec.as_ref(), 512, NativePageLimits::default())
                    .await.unwrap();
                assert_eq!(receipt.commits, 1); assert_eq!(image.page_count(), 19);
                for n in 2..=20 {
                    assert_eq!(image.read_page(PageNumber::new(n).unwrap()).unwrap().as_ref(),
                        &[u8::try_from(n).unwrap(); 512]);
                }
                assert_eq!(destination.record_count, retained);
                assert_eq!(bytes(&destination.symbols, &cx).await, before);
                let mut recovery = destination.into_recovery();
                recovery.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ())))
                    .await.unwrap();
                let mut pages = NativePageRecovery::with_shared_codec(recovery.finish().unwrap(),
                    codec, 512, NativePageLimits::default());
                let reopened = pages.prepare_snapshot(&cx, CommitSeq::new(1)).await.unwrap();
                assert_eq!(reopened.page_count(), image.page_count());
                for page in image.page_numbers() { assert_eq!(reopened.read_page(page), image.read_page(page)); }
                pages.close(&cx).unwrap();
            });
        }

        #[test]
        fn previously_stored_invalid_history_is_not_an_authority_for_new_publication() {
            run(async {
                let cx = Cx::new();
                let source_vfs = MemoryVfs::new(); let mut source = log(&source_vfs, &cx);
                append_proof(&mut source, &cx, |proof| proof.submission.capsule_digest[0] ^= 1).await;
                append_proof(&mut source, &cx, |_| {}).await;
                let dest_vfs = MemoryVfs::new(); let mut destination = log(&dest_vfs, &cx);
                receive(&mut source, &mut destination, &cx, 1).await.finish(&cx, verify).await.unwrap();
                let original = bytes(&destination.markers, &cx).await;
                let receiver = receive(&mut source, &mut destination, &cx, 2).await;
                let mut called = false;
                assert!(receiver.finish_validated(&cx, &Codec, |_, _| {
                    called = true; Ok(())
                }).await.is_err());
                assert!(!called);
                assert_eq!(bytes(&destination.markers, &cx).await, original);
                assert_eq!(destination.published_tip(), CommitSeq::new(1));
                destination.close(&cx).unwrap(); source.close(&cx).unwrap();
            });
        }
    }

    #[test]
    fn transport_limits_and_retained_tails_do_not_authorize_work() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut source = log(&vfs, &cx);
            append(&mut source, &cx, 1).await;
            let limits = NativeReplicationLimits::default();
            for max_commits in [0, 129, usize::MAX] {
                assert!(source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), max_commits, limits).await.is_err());
            }
            let small = NativeReplicationLimits { max_objects: 1, ..limits };
            assert!(source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 1, small).await.is_err());
            let malformed = NativeReplicationCursor { sequence: CommitSeq::ZERO, marker: Some(DB) };
            assert!(source.export_replication_plan(&cx, DB, malformed, 1, limits).await.is_err());
            let plan = source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 1, limits).await.unwrap().unwrap();
            let tiny = NativeReplicationLimits { max_object_message_bytes: OBJECT_HEADER + TAG_BYTES + 77, ..limits };
            assert!(source.export_replication_object(&cx, &plan, symbol(2).object_id, &KEY, tiny).await.is_err());
            let source_end = source.symbol_end;
            source.symbols.write(&cx, b"tail", source_end).await.unwrap();
            source = recover(source, &cx).await;
            let plan = source.export_replication_plan(&cx, DB, NativeReplicationCursor::genesis(), 1, limits).await.unwrap().unwrap();
            let wire = plan.to_bytes(&cx, &KEY, limits).unwrap();
            let replica_vfs = MemoryVfs::new(); let mut replica = log(&replica_vfs, &cx);
            // An in-memory source plan must pass authenticated serialization too.
            assert!(replica.receive_replication_plan(&cx, plan, limits).await.is_err());
            assert!(!replica.needs_recovery());
            replica.markers.write(&cx, b"tail", 0).await.unwrap();
            replica = recover(replica, &cx).await;
            assert!(replica.receive_replication_plan(&cx, decode(&cx, &wire), limits).await.is_err());
            assert_eq!(replica.markers.file_size(&cx).unwrap(), 4);
            replica.close(&cx).unwrap(); source.close(&cx).unwrap();
        });
    }
}
