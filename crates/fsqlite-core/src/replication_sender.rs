//! §3.4.2 Fountain-Coded Replication Sender (bd-1hi.13).
//!
//! Implements the sender-side state machine for fountain-coded database
//! replication using RaptorQ encoding over UDP.
//!
//! State machine: IDLE → ENCODING → STREAMING → COMPLETE
//!
//! Changeset encoding is deterministic, self-delimiting, and uses
//! domain-separated BLAKE3 for changeset identity.

use std::fmt;

use fsqlite_error::{FrankenError, Result};
use tracing::{debug, error, info, warn};

use crate::source_block_partition::K_MAX;

const BEAD_ID: &str = "bd-1hi.13";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Changeset header magic bytes.
pub const CHANGESET_MAGIC: [u8; 4] = *b"FSRP";

/// Changeset format version.
pub const CHANGESET_VERSION: u16 = 1;

/// BLAKE3 domain separation context for changeset identity.
pub const CHANGESET_DOMAIN: &str = "fsqlite:replication:changeset:v1";

/// Replication packet header size (bytes).
pub const REPLICATION_HEADER_SIZE: usize = 24;

/// Maximum UDP application payload (IPv4).
pub const MAX_UDP_PAYLOAD: usize = 65_507;

/// Maximum symbol size for replication: `MAX_UDP_PAYLOAD - REPLICATION_HEADER_SIZE`.
pub const MAX_REPLICATION_SYMBOL_SIZE: usize = MAX_UDP_PAYLOAD - REPLICATION_HEADER_SIZE;

/// Recommended MTU-safe symbol size for Ethernet.
/// 1500 MTU - 20 IPv4 - 8 UDP - 24 replication header = 1448.
pub const MTU_SAFE_SYMBOL_SIZE: u16 = 1448;

/// Default maximum ISI multiplier for streaming stop.
pub const DEFAULT_MAX_ISI_MULTIPLIER: u32 = 2;

/// Default hard cap for a single remote message (4 MiB, §4.19.6).
pub const DEFAULT_RPC_MESSAGE_CAP_BYTES: usize = 4 * 1024 * 1024;

/// HTTP/2 default: max concurrent streams.
pub const DEFAULT_HTTP2_MAX_CONCURRENT_STREAMS: u32 = 256;

/// HTTP/2 default: maximum compressed header list size (64 KiB).
pub const DEFAULT_HTTP2_MAX_HEADER_LIST_SIZE: usize = 65_536;

/// HTTP/2 default: CONTINUATION timeout in milliseconds (5s).
pub const DEFAULT_HTTP2_CONTINUATION_TIMEOUT_MS: u64 = 5_000;

/// HTTP/2 default: absolute header fragment cap (256 KiB).
pub const DEFAULT_HTTP2_HEADER_FRAGMENT_CAP: usize = 262_144;

/// Default handshake timeout in milliseconds.
pub const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 500;

/// Changeset header size in bytes.
pub const CHANGESET_HEADER_SIZE: usize = 4 + 2 + 4 + 4 + 8; // magic + version + page_size + n_pages + total_len = 22

// ---------------------------------------------------------------------------
// §4.19.6 Network Policy + Deterministic VirtualTcp
// ---------------------------------------------------------------------------

/// Transport security mode for remote networking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportSecurityMode {
    /// TLS transport via rustls.
    RustlsTls,
    /// Plaintext transport (only for explicit local development opt-in).
    Plaintext,
}

/// Enforced HTTP/2 hard limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http2HardLimits {
    pub max_concurrent_streams: u32,
    pub max_header_list_size: usize,
    pub continuation_timeout_ms: u64,
    pub header_fragment_cap: usize,
}

impl Default for Http2HardLimits {
    fn default() -> Self {
        Self {
            max_concurrent_streams: DEFAULT_HTTP2_MAX_CONCURRENT_STREAMS,
            max_header_list_size: DEFAULT_HTTP2_MAX_HEADER_LIST_SIZE,
            continuation_timeout_ms: DEFAULT_HTTP2_CONTINUATION_TIMEOUT_MS,
            header_fragment_cap: DEFAULT_HTTP2_HEADER_FRAGMENT_CAP,
        }
    }
}

/// Networking stack policy for remote effects and replication transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkStackConfig {
    pub security: TransportSecurityMode,
    pub explicit_plaintext_opt_in: bool,
    pub handshake_timeout_ms: u64,
    pub message_size_cap_bytes: usize,
    pub http2: Http2HardLimits,
}

impl Default for NetworkStackConfig {
    fn default() -> Self {
        Self {
            security: TransportSecurityMode::RustlsTls,
            explicit_plaintext_opt_in: false,
            handshake_timeout_ms: DEFAULT_HANDSHAKE_TIMEOUT_MS,
            message_size_cap_bytes: DEFAULT_RPC_MESSAGE_CAP_BYTES,
            http2: Http2HardLimits::default(),
        }
    }
}

impl NetworkStackConfig {
    /// Build plaintext config for explicit local development.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::Unsupported` when plaintext is requested
    /// without explicit opt-in.
    pub fn plaintext_local_dev(explicit_opt_in: bool) -> Result<Self> {
        if !explicit_opt_in {
            return Err(FrankenError::Unsupported);
        }
        Ok(Self {
            security: TransportSecurityMode::Plaintext,
            explicit_plaintext_opt_in: true,
            ..Self::default()
        })
    }

    /// Validate the transport security policy.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::Unsupported` if plaintext is not explicitly opted in.
    pub fn validate_security(&self) -> Result<()> {
        if self.security == TransportSecurityMode::Plaintext && !self.explicit_plaintext_opt_in {
            return Err(FrankenError::Unsupported);
        }
        Ok(())
    }

    /// Validate stream concurrency against HTTP/2 hard limits.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::Busy` when `streams` exceeds the configured maximum.
    pub fn validate_concurrent_streams(&self, streams: u32) -> Result<()> {
        if streams > self.http2.max_concurrent_streams {
            return Err(FrankenError::Busy);
        }
        Ok(())
    }

    /// Validate HTTP header-list size.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::TooBig` if header bytes exceed configured limit.
    pub fn validate_header_list_size(&self, header_bytes: usize) -> Result<()> {
        if header_bytes > self.http2.max_header_list_size {
            return Err(FrankenError::TooBig);
        }
        Ok(())
    }

    /// Validate elapsed time for HTTP/2 continuation.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::BusyRecovery` when continuation elapsed time
    /// exceeds the configured timeout.
    pub fn validate_continuation_elapsed(&self, elapsed_ms: u64) -> Result<()> {
        if elapsed_ms > self.http2.continuation_timeout_ms {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(())
    }

    /// Validate elapsed handshake time against timeout budget.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::BusyRecovery` when elapsed time exceeds budget.
    pub fn validate_handshake_elapsed(&self, elapsed_ms: u64) -> Result<()> {
        if elapsed_ms > self.handshake_timeout_ms {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(())
    }

    /// Validate message size against the hard cap.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::TooBig` when `message_bytes` exceeds the cap.
    pub fn validate_message_size(&self, message_bytes: usize) -> Result<()> {
        if message_bytes > self.message_size_cap_bytes {
            return Err(FrankenError::TooBig);
        }
        Ok(())
    }
}

/// Fault profile for deterministic in-memory VirtualTcp transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VirtualTcpFaultProfile {
    pub drop_per_million: u32,
    pub reorder_per_million: u32,
    pub corrupt_per_million: u32,
}

impl VirtualTcpFaultProfile {
    /// Validate rates in parts-per-million (`0..=1_000_000`).
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::OutOfRange` when any rate is above 1_000_000.
    pub fn validate(&self) -> Result<()> {
        const PPM_MAX: u32 = 1_000_000;
        if self.drop_per_million > PPM_MAX {
            return Err(FrankenError::OutOfRange {
                what: "drop_per_million".to_owned(),
                value: self.drop_per_million.to_string(),
            });
        }
        if self.reorder_per_million > PPM_MAX {
            return Err(FrankenError::OutOfRange {
                what: "reorder_per_million".to_owned(),
                value: self.reorder_per_million.to_string(),
            });
        }
        if self.corrupt_per_million > PPM_MAX {
            return Err(FrankenError::OutOfRange {
                what: "corrupt_per_million".to_owned(),
                value: self.corrupt_per_million.to_string(),
            });
        }
        Ok(())
    }
}

/// Trace event kind for deterministic VirtualTcp replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VirtualTcpTraceKind {
    Dropped,
    BufferedForReorder,
    Delivered,
    DeliveredCorrupt,
    FlushedReordered,
}

/// Deterministic trace event emitted by VirtualTcp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualTcpTraceEvent {
    pub seq: u64,
    pub kind: VirtualTcpTraceKind,
    pub payload_hash: u64,
}

/// Deterministic in-memory network shim for lab/DPOR.
#[derive(Debug, Clone)]
pub struct VirtualTcp {
    state: u64,
    seq: u64,
    faults: VirtualTcpFaultProfile,
    pending_reorder: Option<Vec<u8>>,
    trace: Vec<VirtualTcpTraceEvent>,
}

impl VirtualTcp {
    /// Construct a new deterministic VirtualTcp instance.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::OutOfRange` when fault probabilities are invalid.
    pub fn new(seed: u64, faults: VirtualTcpFaultProfile) -> Result<Self> {
        faults.validate()?;
        Ok(Self {
            state: seed,
            seq: 0,
            faults,
            pending_reorder: None,
            trace: Vec::new(),
        })
    }

    /// Return deterministic trace events for replay/debugging.
    #[must_use]
    pub fn trace(&self) -> &[VirtualTcpTraceEvent] {
        &self.trace
    }

    /// Transmit one payload through deterministic drop/reorder/corrupt rules.
    ///
    /// Returns zero, one, or two delivered payloads (reorder flush path).
    #[must_use]
    pub fn transmit(&mut self, payload: &[u8]) -> Vec<Vec<u8>> {
        self.seq = self.seq.saturating_add(1);

        if self.coin_flip(self.faults.drop_per_million) {
            self.push_trace(VirtualTcpTraceKind::Dropped, payload);
            return Vec::new();
        }

        let mut wire = payload.to_vec();
        let mut corrupted = false;
        if !wire.is_empty() && self.coin_flip(self.faults.corrupt_per_million) {
            let idx = (self.next_u32() as usize) % wire.len();
            wire[idx] ^= 0x01;
            corrupted = true;
        }

        if self.coin_flip(self.faults.reorder_per_million) && self.pending_reorder.is_none() {
            self.push_trace(VirtualTcpTraceKind::BufferedForReorder, &wire);
            self.pending_reorder = Some(wire);
            return Vec::new();
        }

        let mut out = Vec::with_capacity(2);
        if let Some(previous) = self.pending_reorder.take() {
            let kind = if corrupted {
                VirtualTcpTraceKind::DeliveredCorrupt
            } else {
                VirtualTcpTraceKind::Delivered
            };
            self.push_trace(kind, &wire);
            out.push(wire);
            self.push_trace(VirtualTcpTraceKind::FlushedReordered, &previous);
            out.push(previous);
            return out;
        }

        let kind = if corrupted {
            VirtualTcpTraceKind::DeliveredCorrupt
        } else {
            VirtualTcpTraceKind::Delivered
        };
        self.push_trace(kind, &wire);
        out.push(wire);
        out
    }

    /// Flush any pending reordered payload.
    pub fn flush(&mut self) -> Option<Vec<u8>> {
        let pending = self.pending_reorder.take()?;
        self.seq = self.seq.saturating_add(1);
        self.push_trace(VirtualTcpTraceKind::FlushedReordered, &pending);
        Some(pending)
    }

    fn push_trace(&mut self, kind: VirtualTcpTraceKind, payload: &[u8]) {
        self.trace.push(VirtualTcpTraceEvent {
            seq: self.seq,
            kind,
            payload_hash: xxhash_rust::xxh3::xxh3_64(payload),
        });
    }

    fn coin_flip(&mut self, per_million: u32) -> bool {
        if per_million == 0 {
            return false;
        }
        const PPM_MAX: u32 = 1_000_000;
        if per_million >= PPM_MAX {
            return true;
        }
        self.next_u32() % PPM_MAX < per_million
    }

    fn next_u32(&mut self) -> u32 {
        // Deterministic LCG for lab replay.
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        (self.state >> 32) as u32
    }
}

// ---------------------------------------------------------------------------
// Changeset Encoding
// ---------------------------------------------------------------------------

/// Self-delimiting changeset header (§3.4.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangesetHeader {
    pub magic: [u8; 4],
    pub version: u16,
    pub page_size: u32,
    pub n_pages: u32,
    pub total_len: u64,
}

impl ChangesetHeader {
    /// Encode to little-endian bytes.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; CHANGESET_HEADER_SIZE] {
        let mut buf = [0_u8; CHANGESET_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..10].copy_from_slice(&self.page_size.to_le_bytes());
        buf[10..14].copy_from_slice(&self.n_pages.to_le_bytes());
        buf[14..22].copy_from_slice(&self.total_len.to_le_bytes());
        buf
    }

    /// Decode from little-endian bytes.
    ///
    /// # Errors
    ///
    /// Returns error if magic or version mismatch.
    pub fn from_bytes(buf: &[u8; CHANGESET_HEADER_SIZE]) -> Result<Self> {
        let magic: [u8; 4] = buf[0..4].try_into().expect("4 bytes");
        if magic != CHANGESET_MAGIC {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("changeset magic mismatch: expected FSRP, got {magic:?}"),
            });
        }
        let version = u16::from_le_bytes(buf[4..6].try_into().expect("2 bytes"));
        if version != CHANGESET_VERSION {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "changeset version mismatch: expected {CHANGESET_VERSION}, got {version}"
                ),
            });
        }
        let page_size = u32::from_le_bytes(buf[6..10].try_into().expect("4 bytes"));
        let n_pages = u32::from_le_bytes(buf[10..14].try_into().expect("4 bytes"));
        let total_len = u64::from_le_bytes(buf[14..22].try_into().expect("8 bytes"));
        Ok(Self {
            magic,
            version,
            page_size,
            n_pages,
            total_len,
        })
    }
}

/// A single page entry in the changeset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageEntry {
    pub page_number: u32,
    pub page_xxh3: u64,
    pub page_bytes: Vec<u8>,
}

impl PageEntry {
    /// Create a page entry, computing the xxh3 checksum.
    #[must_use]
    pub fn new(page_number: u32, page_bytes: Vec<u8>) -> Self {
        let page_xxh3 = xxhash_rust::xxh3::xxh3_64(&page_bytes);
        Self {
            page_number,
            page_xxh3,
            page_bytes,
        }
    }

    /// Validate that the stored xxh3 matches the page bytes.
    #[must_use]
    pub fn validate_xxh3(&self) -> bool {
        xxhash_rust::xxh3::xxh3_64(&self.page_bytes) == self.page_xxh3
    }
}

/// 128-bit changeset identifier (truncated BLAKE3).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChangesetId([u8; 16]);

impl ChangesetId {
    /// Bytes of the identifier.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Create from raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

impl fmt::Debug for ChangesetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChangesetId(")?;
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        write!(f, ")")
    }
}

/// Compute the changeset identifier: `Trunc128(BLAKE3(domain || changeset_bytes))`.
#[must_use]
pub fn compute_changeset_id(changeset_bytes: &[u8]) -> ChangesetId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CHANGESET_DOMAIN.as_bytes());
    hasher.update(changeset_bytes);
    let hash = hasher.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    ChangesetId(id)
}

/// Derive the deterministic RaptorQ seed from a changeset identifier.
#[must_use]
pub fn derive_seed_from_changeset_id(id: &ChangesetId) -> u64 {
    xxhash_rust::xxh3::xxh3_64(id.as_bytes())
}

/// Encode pages into a deterministic changeset byte stream.
///
/// Pages are sorted by `page_number` ascending. The result is self-delimiting
/// via the `total_len` field in the header.
///
/// # Errors
///
/// Returns error if `page_size` is 0 or pages are empty.
pub fn encode_changeset(page_size: u32, pages: &mut [PageEntry]) -> Result<Vec<u8>> {
    if pages.is_empty() {
        return Err(FrankenError::OutOfRange {
            what: "pages".to_owned(),
            value: "0".to_owned(),
        });
    }
    if page_size == 0 {
        return Err(FrankenError::OutOfRange {
            what: "page_size".to_owned(),
            value: "0".to_owned(),
        });
    }

    // Sort by page_number ascending (normative requirement).
    pages.sort_by_key(|p| p.page_number);

    let n_pages = u32::try_from(pages.len()).map_err(|_| FrankenError::OutOfRange {
        what: "n_pages".to_owned(),
        value: pages.len().to_string(),
    })?;

    // Per-page entry size: 4 (page_number) + 8 (xxh3) + page_size
    let entry_size = 4_u64 + 8 + u64::from(page_size);
    let total_len = CHANGESET_HEADER_SIZE as u64 + entry_size * u64::from(n_pages);

    let header = ChangesetHeader {
        magic: CHANGESET_MAGIC,
        version: CHANGESET_VERSION,
        page_size,
        n_pages,
        total_len,
    };

    let mut buf = Vec::with_capacity(usize::try_from(total_len).unwrap_or(usize::MAX));
    buf.extend_from_slice(&header.to_bytes());

    for page in pages.iter() {
        buf.extend_from_slice(&page.page_number.to_le_bytes());
        buf.extend_from_slice(&page.page_xxh3.to_le_bytes());
        buf.extend_from_slice(&page.page_bytes);
    }

    debug!(
        bead_id = BEAD_ID,
        n_pages, page_size, total_len, "encoded changeset"
    );

    debug_assert_eq!(buf.len() as u64, total_len);
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Sharding
// ---------------------------------------------------------------------------

/// A shard of a large changeset that fits within a single RaptorQ source block.
#[derive(Debug, Clone)]
pub struct ChangesetShard {
    /// The changeset bytes for this shard.
    pub changeset_bytes: Vec<u8>,
    /// The changeset identifier for this shard.
    pub changeset_id: ChangesetId,
    /// The deterministic seed for RaptorQ encoding.
    pub seed: u64,
    /// Number of source symbols (K_source) for this shard.
    pub k_source: u32,
}

/// Shard a changeset into pieces that each fit within K_MAX source symbols.
///
/// If the changeset fits in one block, returns a single shard.
///
/// # Errors
///
/// Returns error if `symbol_size` is 0.
pub fn shard_changeset(changeset_bytes: Vec<u8>, symbol_size: u16) -> Result<Vec<ChangesetShard>> {
    if symbol_size == 0 {
        return Err(FrankenError::OutOfRange {
            what: "symbol_size".to_owned(),
            value: "0".to_owned(),
        });
    }

    let t = u64::from(symbol_size);
    let f = changeset_bytes.len() as u64;
    let k_source_total = f.div_ceil(t);

    if k_source_total <= u64::from(K_MAX) {
        let id = compute_changeset_id(&changeset_bytes);
        let seed = derive_seed_from_changeset_id(&id);
        let k_source = u32::try_from(k_source_total).expect("checked <= K_MAX");
        info!(
            bead_id = BEAD_ID,
            k_source,
            symbol_size,
            changeset_len = changeset_bytes.len(),
            "single-shard changeset"
        );
        return Ok(vec![ChangesetShard {
            changeset_bytes,
            changeset_id: id,
            seed,
            k_source,
        }]);
    }

    // Need to shard: split the changeset bytes into chunks
    // Each chunk gets its own changeset_id and seed.
    let max_chunk = u64::from(K_MAX) * t;
    let n_shards = f.div_ceil(max_chunk);

    info!(
        bead_id = BEAD_ID,
        n_shards,
        k_source_total,
        symbol_size,
        changeset_len = changeset_bytes.len(),
        "sharding large changeset"
    );

    let mut shards = Vec::with_capacity(usize::try_from(n_shards).unwrap_or(256));
    let max_chunk_usize = usize::try_from(max_chunk).unwrap_or(usize::MAX);

    for (i, chunk) in changeset_bytes.chunks(max_chunk_usize).enumerate() {
        let shard_bytes = chunk.to_vec();
        let id = compute_changeset_id(&shard_bytes);
        let seed = derive_seed_from_changeset_id(&id);
        let k = (chunk.len() as u64).div_ceil(t);
        let k_source = u32::try_from(k).expect("each shard <= K_MAX symbols");

        debug!(
            bead_id = BEAD_ID,
            shard_index = i,
            k_source,
            shard_len = chunk.len(),
            "created changeset shard"
        );

        shards.push(ChangesetShard {
            changeset_bytes: shard_bytes,
            changeset_id: id,
            seed,
            k_source,
        });
    }

    Ok(shards)
}

// ---------------------------------------------------------------------------
// UDP Packet Format
// ---------------------------------------------------------------------------

/// Replication packet: big-endian header + little-endian symbol payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationPacket {
    /// 16-byte changeset identifier for multiplexing.
    pub changeset_id: ChangesetId,
    /// Source block number (MUST be 0 in V1).
    pub sbn: u8,
    /// Encoding Symbol ID (ISI).
    pub esi: u32,
    /// Number of source symbols.
    pub k_source: u32,
    /// Symbol data (T bytes).
    pub symbol_data: Vec<u8>,
}

impl ReplicationPacket {
    /// Validate the symbol size against the hard wire limit.
    ///
    /// # Errors
    ///
    /// Returns error if symbol size exceeds `MAX_REPLICATION_SYMBOL_SIZE`.
    pub fn validate_symbol_size(symbol_size: usize) -> Result<()> {
        if symbol_size > MAX_REPLICATION_SYMBOL_SIZE {
            error!(
                bead_id = BEAD_ID,
                symbol_size,
                max = MAX_REPLICATION_SYMBOL_SIZE,
                "symbol size exceeds UDP hard wire limit"
            );
            return Err(FrankenError::OutOfRange {
                what: "symbol_size".to_owned(),
                value: symbol_size.to_string(),
            });
        }
        Ok(())
    }

    /// Encode to wire format: 24-byte big-endian header + symbol data.
    ///
    /// # Errors
    ///
    /// Returns error if ESI doesn't fit in 24 bits or symbol exceeds wire limit.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        if self.esi > 0x00FF_FFFF {
            return Err(FrankenError::OutOfRange {
                what: "esi".to_owned(),
                value: self.esi.to_string(),
            });
        }
        Self::validate_symbol_size(self.symbol_data.len())?;

        let total = REPLICATION_HEADER_SIZE + self.symbol_data.len();
        let mut buf = Vec::with_capacity(total);

        // Header (big-endian / network byte order)
        buf.extend_from_slice(self.changeset_id.as_bytes()); // 16 bytes
        buf.push(self.sbn); // 1 byte
        // ESI as u24 big-endian (3 bytes)
        let esi_bytes = self.esi.to_be_bytes(); // [0, high, mid, low]
        buf.extend_from_slice(&esi_bytes[1..4]); // 3 bytes
        buf.extend_from_slice(&self.k_source.to_be_bytes()); // 4 bytes

        // Payload (little-endian symbol data)
        buf.extend_from_slice(&self.symbol_data);

        Ok(buf)
    }

    /// Decode from wire format.
    ///
    /// # Errors
    ///
    /// Returns error if buffer is too short.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < REPLICATION_HEADER_SIZE {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "replication packet too short: {} < {REPLICATION_HEADER_SIZE}",
                    buf.len()
                ),
            });
        }

        let mut id_bytes = [0_u8; 16];
        id_bytes.copy_from_slice(&buf[0..16]);
        let changeset_id = ChangesetId::from_bytes(id_bytes);

        let sbn = buf[16];

        // ESI from u24 big-endian
        let esi = u32::from(buf[17]) << 16 | u32::from(buf[18]) << 8 | u32::from(buf[19]);

        let k_source = u32::from_be_bytes(buf[20..24].try_into().expect("4 bytes"));

        let symbol_data = buf[24..].to_vec();

        Ok(Self {
            changeset_id,
            sbn,
            esi,
            k_source,
            symbol_data,
        })
    }

    /// Total packet size on the wire.
    #[must_use]
    pub fn wire_size(&self) -> usize {
        REPLICATION_HEADER_SIZE + self.symbol_data.len()
    }

    /// Whether this packet carries a source symbol (systematic).
    #[must_use]
    pub fn is_source_symbol(&self) -> bool {
        self.esi < self.k_source
    }
}

// ---------------------------------------------------------------------------
// Sender State Machine
// ---------------------------------------------------------------------------

/// Sender state (§3.4.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SenderState {
    /// No active replication session.
    Idle,
    /// Changeset encoded, encoder prepared.
    Encoding,
    /// Streaming symbols to receiver(s).
    Streaming,
    /// Streaming complete, resources released.
    Complete,
}

/// Configuration for the replication sender.
#[derive(Debug, Clone)]
pub struct SenderConfig {
    /// Symbol size for replication transport.
    pub symbol_size: u16,
    /// Maximum ISI = `max_isi_multiplier * k_source`.
    pub max_isi_multiplier: u32,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self {
            symbol_size: MTU_SAFE_SYMBOL_SIZE,
            max_isi_multiplier: DEFAULT_MAX_ISI_MULTIPLIER,
        }
    }
}

/// Prepared encoding session ready for streaming.
#[derive(Debug)]
pub struct EncodingSession {
    /// Shards of the changeset.
    pub shards: Vec<ChangesetShard>,
    /// Current shard index being streamed.
    pub current_shard: usize,
    /// Current ISI within the current shard.
    pub current_isi: u32,
    /// Configuration.
    pub config: SenderConfig,
}

/// Replication sender state machine.
#[derive(Debug)]
pub struct ReplicationSender {
    state: SenderState,
    session: Option<EncodingSession>,
}

impl ReplicationSender {
    /// Create a new sender in IDLE state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: SenderState::Idle,
            session: None,
        }
    }

    /// Current state.
    #[must_use]
    pub const fn state(&self) -> SenderState {
        self.state
    }

    /// Transition from IDLE to ENCODING: prepare a changeset for streaming.
    ///
    /// # Errors
    ///
    /// Returns error if not in IDLE state, pages are empty, or symbol size invalid.
    pub fn prepare(
        &mut self,
        page_size: u32,
        pages: &mut [PageEntry],
        config: SenderConfig,
    ) -> Result<()> {
        if self.state != SenderState::Idle {
            return Err(FrankenError::Internal(format!(
                "sender must be IDLE to prepare, current state: {:?}",
                self.state
            )));
        }

        ReplicationPacket::validate_symbol_size(usize::from(config.symbol_size))?;

        let changeset_bytes = encode_changeset(page_size, pages)?;
        let shards = shard_changeset(changeset_bytes, config.symbol_size)?;

        info!(
            bead_id = BEAD_ID,
            n_shards = shards.len(),
            symbol_size = config.symbol_size,
            "sender prepared for streaming"
        );

        self.session = Some(EncodingSession {
            shards,
            current_shard: 0,
            current_isi: 0,
            config,
        });
        self.state = SenderState::Encoding;
        Ok(())
    }

    /// Transition from ENCODING to STREAMING.
    ///
    /// # Errors
    ///
    /// Returns error if not in ENCODING state.
    pub fn start_streaming(&mut self) -> Result<()> {
        if self.state != SenderState::Encoding {
            return Err(FrankenError::Internal(format!(
                "sender must be ENCODING to start streaming, current state: {:?}",
                self.state
            )));
        }
        self.state = SenderState::Streaming;
        info!(bead_id = BEAD_ID, "sender started streaming");
        Ok(())
    }

    /// Generate the next replication packet in the stream.
    ///
    /// Returns `None` when all shards have been fully streamed (ISI limit reached).
    ///
    /// # Errors
    ///
    /// Returns error if not in STREAMING state.
    #[allow(clippy::too_many_lines)]
    pub fn next_packet(&mut self) -> Result<Option<ReplicationPacket>> {
        if self.state != SenderState::Streaming {
            return Err(FrankenError::Internal(format!(
                "sender must be STREAMING to generate packets, current state: {:?}",
                self.state
            )));
        }

        let session = self
            .session
            .as_mut()
            .expect("session exists in STREAMING state");

        if session.current_shard >= session.shards.len() {
            // All shards complete.
            return Ok(None);
        }

        let shard = &session.shards[session.current_shard];
        let max_isi = shard
            .k_source
            .saturating_mul(session.config.max_isi_multiplier);

        if session.current_isi >= max_isi {
            // Move to next shard.
            session.current_shard += 1;
            session.current_isi = 0;

            if session.current_shard >= session.shards.len() {
                return Ok(None);
            }

            let next_shard = &session.shards[session.current_shard];
            debug!(
                bead_id = BEAD_ID,
                shard_index = session.current_shard,
                k_source = next_shard.k_source,
                "advancing to next shard"
            );
        }

        let shard = &session.shards[session.current_shard];
        let isi = session.current_isi;
        let t = usize::from(session.config.symbol_size);

        // Generate symbol data for current ISI.
        // For source symbols (ISI < K_source): extract from changeset bytes.
        // For repair symbols (ISI >= K_source): would use RaptorQ encoder in production.
        // Here we provide the framework; actual FEC encoding is delegated to asupersync.
        let symbol_data = if u64::from(isi) < u64::from(shard.k_source) {
            // Source symbol: extract T bytes starting at ISI * T.
            let start = isi as usize * t;
            let end = (start + t).min(shard.changeset_bytes.len());
            let mut data = vec![0_u8; t];
            let available = end.saturating_sub(start);
            if available > 0 {
                data[..available].copy_from_slice(&shard.changeset_bytes[start..end]);
            }
            // Remaining bytes are zero-padded (per RFC 6330 symbol alignment).
            data
        } else {
            // Repair symbol: placeholder (production uses RaptorQ intermediate symbols).
            // For now, generate deterministic placeholder from seed + ISI.
            let mut data = vec![0_u8; t];
            #[allow(clippy::cast_possible_truncation)]
            {
                let repair_seed = shard.seed.wrapping_add(u64::from(isi));
                for (i, byte) in data.iter_mut().enumerate() {
                    let mixed = repair_seed
                        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                        .wrapping_add(i as u64);
                    *byte = (mixed >> 32) as u8;
                }
            }
            warn!(
                bead_id = BEAD_ID,
                isi,
                shard_index = session.current_shard,
                "generated placeholder repair symbol (production uses RaptorQ encoder)"
            );
            data
        };

        let packet = ReplicationPacket {
            changeset_id: shard.changeset_id,
            sbn: 0, // MUST be 0 in V1
            esi: isi,
            k_source: shard.k_source,
            symbol_data,
        };

        session.current_isi += 1;
        Ok(Some(packet))
    }

    /// Acknowledge completion from receiver: stop streaming and transition to COMPLETE.
    ///
    /// # Errors
    ///
    /// Returns error if not in STREAMING state.
    pub fn acknowledge_complete(&mut self) -> Result<()> {
        if self.state != SenderState::Streaming {
            return Err(FrankenError::Internal(format!(
                "sender must be STREAMING to acknowledge, current state: {:?}",
                self.state
            )));
        }
        self.state = SenderState::Complete;
        info!(bead_id = BEAD_ID, "sender acknowledged completion");
        Ok(())
    }

    /// Complete streaming: release resources and transition to COMPLETE.
    ///
    /// This is called when ISI limit is reached or explicit stop.
    pub fn complete(&mut self) {
        if self.state == SenderState::Streaming || self.state == SenderState::Encoding {
            self.state = SenderState::Complete;
            info!(bead_id = BEAD_ID, "sender completed");
        }
    }

    /// Reset to IDLE for the next replication session.
    pub fn reset(&mut self) {
        self.state = SenderState::Idle;
        self.session = None;
        debug!(bead_id = BEAD_ID, "sender reset to IDLE");
    }
}

impl Default for ReplicationSender {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_BEAD_ID: &str = "bd-1hi.13";

    #[allow(clippy::cast_possible_truncation)]
    fn make_pages(page_size: u32, page_numbers: &[u32]) -> Vec<PageEntry> {
        page_numbers
            .iter()
            .map(|&pn| {
                let mut data = vec![0_u8; page_size as usize];
                // Fill with deterministic data based on page number.
                for (i, byte) in data.iter_mut().enumerate() {
                    *byte = ((pn as usize * 251 + i * 31) % 256) as u8;
                }
                PageEntry::new(pn, data)
            })
            .collect()
    }

    // -----------------------------------------------------------------------
    // Changeset encoding tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_changeset_header_format() {
        let header = ChangesetHeader {
            magic: CHANGESET_MAGIC,
            version: CHANGESET_VERSION,
            page_size: 4096,
            n_pages: 10,
            total_len: 42_000,
        };
        let bytes = header.to_bytes();
        assert_eq!(
            &bytes[0..4],
            b"FSRP",
            "bead_id={TEST_BEAD_ID} case=header_magic"
        );
        assert_eq!(bytes.len(), CHANGESET_HEADER_SIZE);

        let decoded = ChangesetHeader::from_bytes(&bytes).expect("decode should succeed");
        assert_eq!(
            header, decoded,
            "bead_id={TEST_BEAD_ID} case=header_roundtrip"
        );
    }

    #[test]
    fn test_changeset_encoding_deterministic() {
        let page_size = 512_u32;
        let mut pages_a = make_pages(page_size, &[3, 1, 2]);
        let mut pages_b = make_pages(page_size, &[2, 3, 1]); // different order

        let bytes_a = encode_changeset(page_size, &mut pages_a).expect("encode a");
        let bytes_b = encode_changeset(page_size, &mut pages_b).expect("encode b");

        // Same pages (different input order) → same changeset bytes (sorted).
        assert_eq!(
            bytes_a, bytes_b,
            "bead_id={TEST_BEAD_ID} case=deterministic_encoding"
        );

        // Same bytes → same changeset_id.
        let id_a = compute_changeset_id(&bytes_a);
        let id_b = compute_changeset_id(&bytes_b);
        assert_eq!(
            id_a, id_b,
            "bead_id={TEST_BEAD_ID} case=deterministic_changeset_id"
        );
    }

    #[test]
    fn test_changeset_id_domain_separation() {
        let data = b"test payload";

        // Changeset domain
        let changeset_id = compute_changeset_id(data);

        // Different domain (simulating ECS)
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"fsqlite:ecs:v1");
        hasher.update(data);
        let ecs_hash = hasher.finalize();
        let mut ecs_id = [0_u8; 16];
        ecs_id.copy_from_slice(&ecs_hash.as_bytes()[..16]);

        assert_ne!(
            changeset_id.as_bytes(),
            &ecs_id,
            "bead_id={TEST_BEAD_ID} case=domain_separation"
        );
    }

    #[test]
    fn test_seed_derivation() {
        let id = ChangesetId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let seed = derive_seed_from_changeset_id(&id);

        // Deterministic: same id → same seed.
        let seed2 = derive_seed_from_changeset_id(&id);
        assert_eq!(
            seed, seed2,
            "bead_id={TEST_BEAD_ID} case=seed_deterministic"
        );

        // Non-trivial.
        assert_ne!(seed, 0, "bead_id={TEST_BEAD_ID} case=seed_nonzero");
    }

    #[test]
    fn test_page_entries_sorted() {
        let page_size = 128_u32;
        let mut pages = make_pages(page_size, &[5, 1, 3, 2, 4]);
        let bytes = encode_changeset(page_size, &mut pages).expect("encode");

        // Verify pages are sorted in the output.
        assert_eq!(pages[0].page_number, 1);
        assert_eq!(pages[1].page_number, 2);
        assert_eq!(pages[2].page_number, 3);
        assert_eq!(pages[3].page_number, 4);
        assert_eq!(pages[4].page_number, 5);

        // Verify total_len from header matches actual length.
        let header_bytes: [u8; CHANGESET_HEADER_SIZE] =
            bytes[..CHANGESET_HEADER_SIZE].try_into().unwrap();
        let header = ChangesetHeader::from_bytes(&header_bytes).expect("decode header");
        assert_eq!(
            header.total_len,
            bytes.len() as u64,
            "bead_id={TEST_BEAD_ID} case=total_len_matches"
        );
        assert_eq!(header.n_pages, 5);
    }

    #[test]
    fn test_page_xxh3_validation() {
        let page = PageEntry::new(1, vec![0xAA; 4096]);
        assert!(
            page.validate_xxh3(),
            "bead_id={TEST_BEAD_ID} case=xxh3_valid"
        );

        // Tampered page fails validation.
        let mut tampered = page;
        tampered.page_bytes[0] ^= 0xFF;
        assert!(
            !tampered.validate_xxh3(),
            "bead_id={TEST_BEAD_ID} case=xxh3_tampered"
        );
    }

    // -----------------------------------------------------------------------
    // UDP Packet format tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_udp_packet_format() {
        let id = ChangesetId::from_bytes([0xAA; 16]);
        let packet = ReplicationPacket {
            changeset_id: id,
            sbn: 0,
            esi: 42,
            k_source: 100,
            symbol_data: vec![0x55; 512],
        };

        let wire = packet.to_bytes().expect("encode");
        assert_eq!(
            wire.len(),
            REPLICATION_HEADER_SIZE + 512,
            "bead_id={TEST_BEAD_ID} case=packet_size"
        );

        // Header is 24 bytes.
        assert_eq!(&wire[0..16], &[0xAA; 16], "changeset_id");
        assert_eq!(wire[16], 0, "sbn");
        // ESI 42 as u24 big-endian = [0, 0, 42]
        assert_eq!(&wire[17..20], &[0, 0, 42], "esi u24 big-endian");
        // K_source 100 as u32 big-endian
        assert_eq!(&wire[20..24], &100_u32.to_be_bytes(), "k_source");

        // Roundtrip.
        let decoded = ReplicationPacket::from_bytes(&wire).expect("decode");
        assert_eq!(
            packet, decoded,
            "bead_id={TEST_BEAD_ID} case=packet_roundtrip"
        );
    }

    #[test]
    fn test_udp_packet_mtu_safe() {
        // T=1448 → packet 1472 bytes. With IP(20) + UDP(8) = 1500 = Ethernet MTU.
        let t = usize::from(MTU_SAFE_SYMBOL_SIZE);
        let total = REPLICATION_HEADER_SIZE + t;
        assert_eq!(
            total, 1472,
            "bead_id={TEST_BEAD_ID} case=mtu_safe_packet_size"
        );
        // Plus IP + UDP headers: 1472 + 20 + 8 = 1500.
        assert_eq!(total + 20 + 8, 1500, "fits in Ethernet MTU");
    }

    #[test]
    fn test_hard_wire_limit() {
        // Symbol that exceeds the hard wire limit.
        let oversized = MAX_REPLICATION_SYMBOL_SIZE + 1;
        let result = ReplicationPacket::validate_symbol_size(oversized);
        assert!(
            result.is_err(),
            "bead_id={TEST_BEAD_ID} case=hard_wire_limit_rejected"
        );

        // At the limit: OK.
        let at_limit = MAX_REPLICATION_SYMBOL_SIZE;
        let result = ReplicationPacket::validate_symbol_size(at_limit);
        assert!(
            result.is_ok(),
            "bead_id={TEST_BEAD_ID} case=hard_wire_limit_at_max"
        );
    }

    // -----------------------------------------------------------------------
    // State machine tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_sender_idle_to_encoding() {
        let mut sender = ReplicationSender::new();
        assert_eq!(sender.state(), SenderState::Idle);

        let mut pages = make_pages(512, &[1, 2, 3]);
        sender
            .prepare(512, &mut pages, SenderConfig::default())
            .expect("prepare");
        assert_eq!(
            sender.state(),
            SenderState::Encoding,
            "bead_id={TEST_BEAD_ID} case=idle_to_encoding"
        );
    }

    #[test]
    fn test_streaming_source_then_repair() {
        let mut sender = ReplicationSender::new();
        let mut pages = make_pages(512, &[1, 2]);
        let config = SenderConfig {
            symbol_size: 512,
            max_isi_multiplier: 2,
        };
        sender.prepare(512, &mut pages, config).expect("prepare");
        sender.start_streaming().expect("start");

        let session = sender.session.as_ref().unwrap();
        let k_source = session.shards[0].k_source;

        let mut source_count = 0_u32;
        let mut repair_count = 0_u32;
        let mut last_isi = 0_u32;

        while let Some(packet) = sender.next_packet().expect("next") {
            if packet.is_source_symbol() {
                source_count += 1;
            } else {
                repair_count += 1;
            }
            last_isi = packet.esi;
        }

        assert!(
            source_count > 0,
            "bead_id={TEST_BEAD_ID} case=has_source_symbols"
        );
        assert!(
            repair_count > 0,
            "bead_id={TEST_BEAD_ID} case=has_repair_symbols"
        );
        assert_eq!(
            source_count, k_source,
            "bead_id={TEST_BEAD_ID} case=source_count_matches_k"
        );
        assert_eq!(
            last_isi,
            k_source * 2 - 1,
            "bead_id={TEST_BEAD_ID} case=max_isi_reached"
        );
    }

    #[test]
    fn test_streaming_stop_on_ack() {
        let mut sender = ReplicationSender::new();
        let mut pages = make_pages(512, &[1]);
        sender
            .prepare(512, &mut pages, SenderConfig::default())
            .expect("prepare");
        sender.start_streaming().expect("start");

        // Generate a few packets.
        let _p1 = sender.next_packet().expect("next").expect("packet");

        // Receiver ACKs completion.
        sender.acknowledge_complete().expect("ack");
        assert_eq!(
            sender.state(),
            SenderState::Complete,
            "bead_id={TEST_BEAD_ID} case=stop_on_ack"
        );
    }

    #[test]
    fn test_streaming_stop_on_max_isi() {
        let mut sender = ReplicationSender::new();
        let mut pages = make_pages(128, &[1]);
        let config = SenderConfig {
            symbol_size: 128,
            max_isi_multiplier: 2,
        };
        sender.prepare(128, &mut pages, config).expect("prepare");
        sender.start_streaming().expect("start");

        let mut count = 0_u32;
        while sender.next_packet().expect("next").is_some() {
            count += 1;
        }

        // Should have generated exactly k_source * max_isi_multiplier packets.
        let session = sender.session.as_ref().unwrap();
        let expected = session.shards[0].k_source * 2;
        assert_eq!(
            count, expected,
            "bead_id={TEST_BEAD_ID} case=stop_on_max_isi"
        );
    }

    #[test]
    fn test_block_size_limit_sharding() {
        // Create a changeset that exceeds K_MAX source symbols.
        let symbol_size = 64_u16;
        let bytes_per_max_block = u64::from(K_MAX) * u64::from(symbol_size);
        // Make changeset bytes just over the limit.
        let changeset_bytes = vec![0xAB_u8; usize::try_from(bytes_per_max_block).unwrap() + 1];
        let shards = shard_changeset(changeset_bytes.clone(), symbol_size).expect("shard");

        assert!(
            shards.len() > 1,
            "bead_id={TEST_BEAD_ID} case=sharding_triggered shards={}",
            shards.len()
        );

        // Each shard has k_source <= K_MAX.
        for (i, shard) in shards.iter().enumerate() {
            assert!(
                shard.k_source <= K_MAX,
                "bead_id={TEST_BEAD_ID} case=shard_k_max shard={i} k_source={}",
                shard.k_source
            );
        }

        // All bytes covered.
        let total_bytes: usize = shards.iter().map(|s| s.changeset_bytes.len()).sum();
        assert_eq!(
            total_bytes,
            changeset_bytes.len(),
            "bead_id={TEST_BEAD_ID} case=sharding_coverage"
        );
    }

    // -----------------------------------------------------------------------
    // Property tests
    // -----------------------------------------------------------------------

    #[test]
    fn prop_changeset_id_unique() {
        let page_size = 128_u32;
        let mut ids = Vec::new();
        for seed in 0_u32..20 {
            let mut pages = vec![PageEntry::new(
                1,
                vec![u8::try_from(seed).unwrap(); page_size as usize],
            )];
            let bytes = encode_changeset(page_size, &mut pages).expect("encode");
            ids.push(compute_changeset_id(&bytes));
        }

        // All IDs should be unique.
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                assert_ne!(
                    ids[i], ids[j],
                    "bead_id={TEST_BEAD_ID} case=prop_id_unique i={i} j={j}"
                );
            }
        }
    }

    #[test]
    fn prop_sharding_covers_all_pages() {
        let symbol_size = 64_u16;
        for size_multiplier in [1_u64, 2, 5] {
            let total = u64::from(K_MAX) * u64::from(symbol_size) * size_multiplier + 7;
            let changeset = vec![0xCC_u8; usize::try_from(total).unwrap()];
            let shards = shard_changeset(changeset.clone(), symbol_size).expect("shard");

            let reassembled: Vec<u8> = shards
                .iter()
                .flat_map(|s| s.changeset_bytes.iter().copied())
                .collect();

            assert_eq!(
                reassembled, changeset,
                "bead_id={TEST_BEAD_ID} case=prop_sharding_coverage multiplier={size_multiplier}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Compliance tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_bd_1hi_13_unit_compliance_gate() {
        assert_eq!(CHANGESET_MAGIC, *b"FSRP");
        assert_eq!(CHANGESET_VERSION, 1);
        assert_eq!(CHANGESET_HEADER_SIZE, 22);
        assert_eq!(REPLICATION_HEADER_SIZE, 24);
        assert_eq!(MAX_UDP_PAYLOAD, 65_507);
        const { assert!(MAX_REPLICATION_SYMBOL_SIZE < MAX_UDP_PAYLOAD) };

        // Verify core functions exist.
        let _ = ChangesetId::from_bytes([0; 16]);
        let _ = compute_changeset_id(b"test");
        let _ = derive_seed_from_changeset_id(&ChangesetId::from_bytes([0; 16]));
    }

    #[test]
    fn prop_bd_1hi_13_structure_compliance() {
        // State machine transitions are correct.
        let mut sender = ReplicationSender::new();
        assert_eq!(sender.state(), SenderState::Idle);

        let mut pages = make_pages(256, &[1, 2]);
        sender
            .prepare(256, &mut pages, SenderConfig::default())
            .expect("prepare");
        assert_eq!(sender.state(), SenderState::Encoding);

        sender.start_streaming().expect("start");
        assert_eq!(sender.state(), SenderState::Streaming);

        sender.complete();
        assert_eq!(sender.state(), SenderState::Complete);

        sender.reset();
        assert_eq!(sender.state(), SenderState::Idle);
    }

    // -----------------------------------------------------------------------
    // §4.19.6 networking policy tests (bd-i0m5)
    // -----------------------------------------------------------------------

    #[test]
    fn test_tls_by_default() {
        let cfg = NetworkStackConfig::default();
        assert_eq!(cfg.security, TransportSecurityMode::RustlsTls);
        assert!(cfg.validate_security().is_ok());
    }

    #[test]
    fn test_plaintext_requires_explicit_opt_in() {
        let cfg = NetworkStackConfig {
            security: TransportSecurityMode::Plaintext,
            explicit_plaintext_opt_in: false,
            ..NetworkStackConfig::default()
        };
        let err = cfg.validate_security().unwrap_err();
        assert!(matches!(err, FrankenError::Unsupported));

        let opted_in = NetworkStackConfig::plaintext_local_dev(true).unwrap();
        assert_eq!(opted_in.security, TransportSecurityMode::Plaintext);
        assert!(opted_in.validate_security().is_ok());
    }

    #[test]
    fn test_http2_max_concurrent_streams() {
        let cfg = NetworkStackConfig::default();
        assert!(
            cfg.validate_concurrent_streams(DEFAULT_HTTP2_MAX_CONCURRENT_STREAMS)
                .is_ok()
        );
        let err = cfg
            .validate_concurrent_streams(DEFAULT_HTTP2_MAX_CONCURRENT_STREAMS + 1)
            .unwrap_err();
        assert!(matches!(err, FrankenError::Busy));
    }

    #[test]
    fn test_http2_max_header_list_size() {
        let cfg = NetworkStackConfig::default();
        assert!(
            cfg.validate_header_list_size(DEFAULT_HTTP2_MAX_HEADER_LIST_SIZE)
                .is_ok()
        );
        let err = cfg
            .validate_header_list_size(DEFAULT_HTTP2_MAX_HEADER_LIST_SIZE + 1)
            .unwrap_err();
        assert!(matches!(err, FrankenError::TooBig));
    }

    #[test]
    fn test_http2_continuation_timeout() {
        let cfg = NetworkStackConfig::default();
        assert!(
            cfg.validate_continuation_elapsed(DEFAULT_HTTP2_CONTINUATION_TIMEOUT_MS)
                .is_ok()
        );
        let err = cfg
            .validate_continuation_elapsed(DEFAULT_HTTP2_CONTINUATION_TIMEOUT_MS + 1)
            .unwrap_err();
        assert!(matches!(err, FrankenError::BusyRecovery));
    }

    #[test]
    fn test_message_size_cap_enforced() {
        let cfg = NetworkStackConfig::default();
        assert!(
            cfg.validate_message_size(DEFAULT_RPC_MESSAGE_CAP_BYTES)
                .is_ok()
        );
        let err = cfg
            .validate_message_size(DEFAULT_RPC_MESSAGE_CAP_BYTES + 1)
            .unwrap_err();
        assert!(matches!(err, FrankenError::TooBig));
    }

    #[test]
    fn test_handshake_timeout_bounded() {
        let cfg = NetworkStackConfig {
            handshake_timeout_ms: DEFAULT_HANDSHAKE_TIMEOUT_MS,
            ..NetworkStackConfig::default()
        };
        assert!(
            cfg.validate_handshake_elapsed(DEFAULT_HANDSHAKE_TIMEOUT_MS)
                .is_ok()
        );
        let err = cfg
            .validate_handshake_elapsed(DEFAULT_HANDSHAKE_TIMEOUT_MS + 500)
            .unwrap_err();
        assert!(matches!(err, FrankenError::BusyRecovery));
    }

    #[test]
    fn test_virtual_tcp_deterministic() {
        let faults = VirtualTcpFaultProfile {
            drop_per_million: 150_000,
            reorder_per_million: 200_000,
            corrupt_per_million: 125_000,
        };
        let payloads = vec![
            b"alpha".to_vec(),
            b"beta".to_vec(),
            b"gamma".to_vec(),
            b"delta".to_vec(),
            b"epsilon".to_vec(),
        ];

        let mut left = VirtualTcp::new(42, faults).unwrap();
        let mut left_out = Vec::new();
        for payload in &payloads {
            left_out.extend(left.transmit(payload));
        }
        if let Some(flush) = left.flush() {
            left_out.push(flush);
        }
        let left_trace = left.trace().to_vec();

        let mut right = VirtualTcp::new(42, faults).unwrap();
        let mut right_out = Vec::new();
        for payload in &payloads {
            right_out.extend(right.transmit(payload));
        }
        if let Some(flush) = right.flush() {
            right_out.push(flush);
        }
        let right_trace = right.trace().to_vec();

        assert_eq!(left_out, right_out);
        assert_eq!(left_trace, right_trace);
    }

    #[test]
    fn test_virtual_tcp_fault_injection() {
        let mut vtcp = VirtualTcp::new(
            7,
            VirtualTcpFaultProfile {
                drop_per_million: 0,
                reorder_per_million: 1_000_000,
                corrupt_per_million: 1_000_000,
            },
        )
        .unwrap();

        let out_first = vtcp.transmit(b"packet-a");
        assert!(out_first.is_empty(), "first packet must be buffered");

        let out_second = vtcp.transmit(b"packet-b");
        assert_eq!(out_second.len(), 2, "second transmit flushes reorder queue");
        assert_ne!(
            out_second[0],
            b"packet-b".to_vec(),
            "corruption must alter delivered payload"
        );

        let has_buffer = vtcp
            .trace()
            .iter()
            .any(|event| event.kind == VirtualTcpTraceKind::BufferedForReorder);
        let has_corrupt_delivery = vtcp
            .trace()
            .iter()
            .any(|event| event.kind == VirtualTcpTraceKind::DeliveredCorrupt);
        let has_flush = vtcp
            .trace()
            .iter()
            .any(|event| event.kind == VirtualTcpTraceKind::FlushedReordered);

        assert!(has_buffer);
        assert!(has_corrupt_delivery);
        assert!(has_flush);
    }
}
