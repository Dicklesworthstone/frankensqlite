//! §3.4.3 Fountain-Coded Snapshot Shipping (bd-1hi.15).
//!
//! Implements snapshot transfer for initializing new replicas using
//! fountain coding. The entire database is partitioned into source blocks
//! and streamed as rateless-coded symbols over UDP.
//!
//! Key advantages:
//! - No handshake or acknowledgment needed
//! - Receiver can start receiving from any point in the stream
//! - Inherently resumable with zero protocol overhead
//! - Natural multicast: initialize many replicas simultaneously
//! - Progressive receive: partial queries after first block decoded

use std::collections::{HashMap, HashSet};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use tracing::{debug, error, info, warn};

use crate::replication_sender::{
    CHANGESET_HEADER_SIZE, ChangesetId, PageEntry, RepairEncoder, ReplicationPacket,
    ReplicationPacketV2Header, SenderConfig, compute_changeset_id, derive_seed_from_changeset_id,
    encode_replication_blocks, symbol_schedule_end, validate_codec_esi,
};
use crate::source_block_partition::{K_MAX, SourceBlock};

const BEAD_ID: &str = "bd-1hi.15";

// ---------------------------------------------------------------------------
// Resume State (persistent across connection losses)
// ---------------------------------------------------------------------------

/// Per-block resume state: tracks which ISIs have been received.
#[derive(Debug, Clone)]
pub struct BlockResumeState {
    /// Source block index (SBN).
    pub block_id: u32,
    /// Number of unique symbols received.
    pub num_received: u32,
    /// Set of received ISIs (for O(1) dedup).
    pub received_isis: HashSet<u32>,
    /// Whether this block has been fully decoded.
    pub decoded: bool,
}

impl BlockResumeState {
    /// Create a new empty resume state for a block.
    #[must_use]
    fn new(block_id: u32) -> Self {
        Self {
            block_id,
            num_received: 0,
            received_isis: HashSet::new(),
            decoded: false,
        }
    }

    /// Record a received ISI. Returns true if new (accepted).
    fn record_isi(&mut self, isi: u32) -> bool {
        if self.received_isis.insert(isi) {
            self.num_received += 1;
            true
        } else {
            false
        }
    }

    /// Serialize to a compact binary format for persistence.
    ///
    /// Format: `block_id(4 LE) | num_received(4 LE) | decoded(1) | n_isis(4 LE) | isis(4 LE each)`
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let n = self.received_isis.len();
        let mut buf = Vec::with_capacity(13 + n * 4);
        buf.extend_from_slice(&self.block_id.to_le_bytes());
        buf.extend_from_slice(&self.num_received.to_le_bytes());
        buf.push(u8::from(self.decoded));
        let n_u32 = u32::try_from(n).unwrap_or(u32::MAX);
        buf.extend_from_slice(&n_u32.to_le_bytes());
        let mut sorted_isis: Vec<u32> = self.received_isis.iter().copied().collect();
        sorted_isis.sort_unstable();
        for isi in sorted_isis {
            buf.extend_from_slice(&isi.to_le_bytes());
        }
        buf
    }

    /// Deserialize from bytes.
    ///
    /// # Errors
    ///
    /// Returns error if buffer is too short or malformed.
    pub fn from_bytes(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 13 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("BlockResumeState too short: {} < 13", buf.len()),
            });
        }
        let block_id = u32::from_le_bytes(buf[0..4].try_into().expect("4 bytes"));
        let num_received = u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes"));
        let decoded = buf[8] != 0;
        let n_isis = u32::from_le_bytes(buf[9..13].try_into().expect("4 bytes"));
        let n = n_isis as usize;
        let expected = n
            .checked_mul(4)
            .and_then(|v| v.checked_add(13))
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!("BlockResumeState n_isis ({n_isis}) causes size overflow"),
            })?;
        if buf.len() < expected {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("BlockResumeState truncated: {} < {expected}", buf.len()),
            });
        }
        let mut received_isis = HashSet::with_capacity(n);
        for i in 0..n {
            let offset = 13 + i * 4;
            let isi = u32::from_le_bytes(buf[offset..offset + 4].try_into().expect("4 bytes"));
            received_isis.insert(isi);
        }
        Ok((
            Self {
                block_id,
                num_received,
                received_isis,
                decoded,
            },
            expected,
        ))
    }
}

/// Full resume state for a snapshot transfer.
#[derive(Debug, Clone)]
pub struct ResumeState {
    /// Per-block resume states.
    pub blocks: Vec<BlockResumeState>,
    /// Total number of source blocks expected.
    pub total_blocks: u32,
}

impl ResumeState {
    /// Create a new resume state for a snapshot with `total_blocks` blocks.
    #[must_use]
    pub fn new(total_blocks: u32) -> Self {
        let blocks = (0..total_blocks).map(BlockResumeState::new).collect();
        Self {
            blocks,
            total_blocks,
        }
    }

    /// Number of blocks fully decoded.
    #[must_use]
    pub fn decoded_count(&self) -> u32 {
        u32::try_from(self.blocks.iter().filter(|b| b.decoded).count()).unwrap_or(u32::MAX)
    }

    /// Whether all blocks are decoded.
    #[must_use]
    pub fn all_decoded(&self) -> bool {
        self.blocks.iter().all(|b| b.decoded)
    }

    /// Serialize to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.total_blocks.to_le_bytes());
        for block in &self.blocks {
            buf.extend_from_slice(&block.to_bytes());
        }
        buf
    }

    /// Deserialize from bytes.
    ///
    /// # Errors
    ///
    /// Returns error if buffer is malformed.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < 4 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("ResumeState too short: {} < 4", buf.len()),
            });
        }
        let total_blocks = u32::from_le_bytes(buf[0..4].try_into().expect("4 bytes"));
        let mut blocks = Vec::with_capacity(total_blocks as usize);
        let mut offset = 4;
        for _ in 0..total_blocks {
            let (block, consumed) = BlockResumeState::from_bytes(&buf[offset..])?;
            blocks.push(block);
            offset += consumed;
        }
        Ok(Self {
            blocks,
            total_blocks,
        })
    }
}

// ---------------------------------------------------------------------------
// Snapshot Sender
// ---------------------------------------------------------------------------

/// Snapshot sender: partitions a database into source blocks and streams symbols.
#[derive(Debug)]
pub struct SnapshotSender {
    /// Source blocks from the partition algorithm.
    pub source_blocks: Vec<SourceBlock>,
    /// Page size of the database.
    pub page_size: u32,
    /// Current block being streamed.
    current_block: usize,
    /// Current ISI within the current block.
    current_isi: u32,
    /// Per-block changeset IDs (computed during prepare).
    block_changeset_ids: Vec<ChangesetId>,
    /// Per-block K_source values.
    block_k_sources: Vec<u32>,
    /// Per-block changeset bytes.
    block_changesets: Vec<Vec<u8>>,
    /// Sender config.
    config: SenderConfig,
    /// Whether we're done.
    done: bool,
    repair_encoder: RepairEncoder,
}

impl SnapshotSender {
    /// Prepare a snapshot sender for the given database pages.
    ///
    /// `all_pages` must be sorted by page number and cover the entire database.
    ///
    /// # Errors
    ///
    /// Returns error if partitioning fails or pages are empty.
    #[allow(clippy::too_many_lines)]
    pub fn prepare(
        page_size: u32,
        all_pages: &mut [PageEntry],
        config: SenderConfig,
    ) -> Result<Self> {
        config.validate()?;
        let shards = encode_replication_blocks(page_size, all_pages, config.symbol_size, 256)?;
        let mut source_blocks = Vec::with_capacity(shards.len());
        let mut block_changeset_ids = Vec::with_capacity(shards.len());
        let mut block_k_sources = Vec::with_capacity(shards.len());
        let mut block_changesets = Vec::with_capacity(shards.len());
        let mut page_idx = 0;
        for (index, shard) in shards.into_iter().enumerate() {
            let num_pages = u32::from_le_bytes(
                shard.changeset_bytes[10..14]
                    .try_into()
                    .expect("encoded page count"),
            );
            source_blocks.push(SourceBlock {
                index: u8::try_from(index).expect("block count admitted before encoding"),
                start_page: all_pages[page_idx].page_number,
                num_pages,
            });
            page_idx += num_pages as usize;
            block_changeset_ids.push(shard.changeset_id);
            block_k_sources.push(shard.k_source);
            block_changesets.push(shard.changeset_bytes);
        }

        Ok(Self {
            source_blocks,
            page_size,
            current_block: 0,
            current_isi: 0,
            block_changeset_ids,
            block_k_sources,
            block_changesets,
            config,
            done: false,
            repair_encoder: RepairEncoder::default(),
        })
    }

    /// Generate the next snapshot packet.
    ///
    /// Returns `None` when the current streaming pass is complete.
    /// Caller can restart from block 0 for continuous streaming.
    ///
    /// # Errors
    /// Returns cancellation, invalid codec dimensions or repair encoding errors.
    pub fn next_packet(&mut self, cx: &Cx) -> Result<Option<ReplicationPacket>> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        if self.done || self.current_block >= self.source_blocks.len() {
            self.done = true;
            return Ok(None);
        }

        let k_source = self.block_k_sources[self.current_block];
        let max_isi = symbol_schedule_end(k_source, self.config.max_isi_multiplier);

        if self.current_isi >= max_isi {
            self.current_block += 1;
            self.current_isi = 0;
            self.repair_encoder = RepairEncoder::default();
            if self.current_block >= self.source_blocks.len() {
                self.done = true;
                return Ok(None);
            }
        }

        let changeset = &self.block_changesets[self.current_block];
        let changeset_id = self.block_changeset_ids[self.current_block];
        let k_source = self.block_k_sources[self.current_block];
        let isi = self.current_isi;
        let t = usize::from(self.config.symbol_size);

        // Extract or generate symbol data.
        let symbol_data = if u64::from(isi) < u64::from(k_source) {
            let start = isi as usize * t;
            let end = (start + t).min(changeset.len());
            let mut data = vec![0_u8; t];
            let available = end.saturating_sub(start);
            if available > 0 {
                data[..available].copy_from_slice(&changeset[start..end]);
            }
            data
        } else {
            self.repair_encoder
                .symbol(cx, changeset, k_source, self.config.symbol_size, isi)?
        };

        let seed = derive_seed_from_changeset_id(&changeset_id);
        let r_repair =
            symbol_schedule_end(k_source, self.config.max_isi_multiplier).saturating_sub(k_source);
        let packet = ReplicationPacket::new_v2(
            ReplicationPacketV2Header {
                changeset_id,
                sbn: 0,
                esi: isi,
                k_source,
                r_repair,
                symbol_size_t: self.config.symbol_size,
                seed,
            },
            symbol_data,
        );

        self.current_isi += 1;
        Ok(Some(packet))
    }

    /// Number of source blocks.
    #[must_use]
    pub fn num_blocks(&self) -> usize {
        self.source_blocks.len()
    }

    /// Total source symbols across all blocks.
    #[must_use]
    pub fn total_source_symbols(&self) -> u64 {
        self.block_k_sources.iter().map(|&k| u64::from(k)).sum()
    }

    /// Reset to re-stream from the beginning (for continuous multicast).
    pub fn restart(&mut self) {
        self.current_block = 0;
        self.current_isi = 0;
        self.done = false;
        self.repair_encoder = RepairEncoder::default();
        debug!(bead_id = BEAD_ID, "snapshot sender restarted for next pass");
    }
}

// ---------------------------------------------------------------------------
// Snapshot Receiver
// ---------------------------------------------------------------------------

/// Snapshot receiver state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotReceiverState {
    /// Waiting for first packet.
    Waiting,
    /// Actively collecting symbols.
    Receiving,
    /// All blocks decoded, snapshot complete.
    Complete,
}

/// A decoded source block's pages.
#[derive(Debug, Clone)]
pub struct DecodedBlock {
    /// Block index.
    pub block_index: u32,
    /// Decoded pages sorted by page number.
    pub pages: Vec<DecodedBlockPage>,
}

/// A single page from a decoded block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedBlockPage {
    /// Page number.
    pub page_number: u32,
    /// Page data.
    pub page_data: Vec<u8>,
}

/// Per-block decoder used by the snapshot receiver.
#[derive(Debug)]
struct BlockDecoder {
    /// The changeset_id for this block (determined from first packet).
    changeset_id: Option<ChangesetId>,
    /// K_source for this block.
    k_source: u32,
    /// Symbol size.
    symbol_size: u32,
    /// Seed for RaptorQ.
    seed: u64,
    /// Symbols collected by ISI.
    symbols: HashMap<u32, Vec<u8>>,
    /// ISI dedup set.
    received_isis: HashSet<u32>,
    /// Whether decoded.
    decoded: bool,
}

impl BlockDecoder {
    fn new() -> Self {
        Self {
            changeset_id: None,
            k_source: 0,
            symbol_size: 0,
            seed: 0,
            symbols: HashMap::new(),
            received_isis: HashSet::new(),
            decoded: false,
        }
    }

    fn initialize(&mut self, changeset_id: ChangesetId, k_source: u32, symbol_size: u32) {
        self.changeset_id = Some(changeset_id);
        self.k_source = k_source;
        self.symbol_size = symbol_size;
        self.seed = derive_seed_from_changeset_id(&changeset_id);
    }

    fn add_symbol(&mut self, isi: u32, data: Vec<u8>) -> bool {
        if self.received_isis.insert(isi) {
            self.symbols.insert(isi, data);
            true
        } else {
            false
        }
    }

    fn received_count(&self) -> u32 {
        u32::try_from(self.received_isis.len()).unwrap_or(u32::MAX)
    }

    fn ready_to_decode(&self) -> bool {
        self.received_count() >= self.k_source && self.k_source > 0
    }

    fn try_decode(&self, cx: &Cx) -> Result<Option<Vec<u8>>> {
        crate::replication_receiver::decode_symbols(
            cx,
            &self.symbols,
            self.k_source,
            self.symbol_size,
            self.seed,
            crate::replication_sender::MAX_REPAIR_WORK_BYTES,
            false,
        )
        .map(|decoded| decoded.data)
    }
}

/// Snapshot receiver: collects symbols per source block, decodes progressively.
#[derive(Debug)]
pub struct SnapshotReceiver {
    state: SnapshotReceiverState,
    /// Per-changeset_id → block index mapping.
    changeset_to_block: HashMap<ChangesetId, usize>,
    /// Per-block decoders.
    block_decoders: Vec<BlockDecoder>,
    /// Number of blocks expected (set after first packet or from resume state).
    num_blocks: usize,
    /// Decoded blocks ready for application.
    decoded_blocks: Vec<DecodedBlock>,
    /// Resume state.
    resume: ResumeState,
    /// Page size.
    page_size: u32,
    buffered_symbol_bytes: usize,
    auth_key: Option<[u8; 32]>,
}

impl SnapshotReceiver {
    /// Create a new snapshot receiver.
    ///
    /// `num_blocks` is the expected number of source blocks (from partitioning).
    /// `page_size` is the database page size.
    #[must_use]
    pub fn new(num_blocks: usize, page_size: u32) -> Self {
        let block_decoders = (0..num_blocks).map(|_| BlockDecoder::new()).collect();
        Self {
            state: SnapshotReceiverState::Waiting,
            changeset_to_block: HashMap::new(),
            block_decoders,
            num_blocks,
            decoded_blocks: Vec::new(),
            resume: ResumeState::new(u32::try_from(num_blocks).unwrap_or(u32::MAX)),
            page_size,
            buffered_symbol_bytes: 0,
            auth_key: None,
        }
    }

    /// Create from a resume state (after crash/reconnect).
    #[must_use]
    pub fn from_resume(resume: ResumeState, page_size: u32) -> Self {
        let num_blocks = resume.total_blocks as usize;
        let block_decoders = (0..num_blocks).map(|_| BlockDecoder::new()).collect();
        Self {
            state: if resume.all_decoded() {
                SnapshotReceiverState::Complete
            } else {
                SnapshotReceiverState::Waiting
            },
            changeset_to_block: HashMap::new(),
            block_decoders,
            num_blocks,
            decoded_blocks: Vec::new(),
            resume,
            page_size,
            buffered_symbol_bytes: 0,
            auth_key: None,
        }
    }

    /// Current state.
    #[must_use]
    pub const fn state(&self) -> SnapshotReceiverState {
        self.state
    }

    /// Require authenticated packets before starting this receive session.
    ///
    /// # Errors
    /// Returns `Busy` if packets have already been accepted.
    pub fn set_auth_key(&mut self, auth_key: [u8; 32]) -> Result<()> {
        if self.state != SnapshotReceiverState::Waiting {
            return Err(FrankenError::Busy);
        }
        self.auth_key = Some(auth_key);
        Ok(())
    }

    /// Number of blocks decoded so far.
    #[must_use]
    pub fn blocks_decoded(&self) -> usize {
        self.decoded_blocks.len()
    }

    /// Get the resume state for persistence.
    #[must_use]
    pub fn resume_state(&self) -> &ResumeState {
        &self.resume
    }

    /// Take decoded blocks (for application to local database).
    pub fn take_decoded_blocks(&mut self) -> Vec<DecodedBlock> {
        std::mem::take(&mut self.decoded_blocks)
    }

    /// Process a snapshot packet.
    ///
    /// The receiver maps packets to blocks by changeset_id. The first packet
    /// for a new changeset_id establishes the mapping to the next unmapped block.
    ///
    /// # Errors
    ///
    /// Returns error if the packet is malformed or validation fails.
    #[allow(clippy::too_many_lines)]
    pub fn process_packet(
        &mut self,
        cx: &Cx,
        packet: &ReplicationPacket,
    ) -> Result<SnapshotPacketResult> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        if !packet.verify_integrity(self.auth_key.as_ref()) {
            return Ok(SnapshotPacketResult::Rejected);
        }
        validate_codec_esi(packet.esi)?;
        if usize::from(packet.symbol_size_t) != packet.symbol_data.len()
            || packet.seed != derive_seed_from_changeset_id(&packet.changeset_id)
        {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "snapshot packet size or seed mismatch".to_owned(),
            });
        }
        if self.state == SnapshotReceiverState::Complete {
            return Ok(SnapshotPacketResult::AlreadyComplete);
        }

        // V1 rule.
        if packet.sbn != 0 {
            return Err(FrankenError::Internal(format!(
                "V1: SBN must be 0, got {}",
                packet.sbn
            )));
        }
        if packet.k_source == 0 || packet.k_source > K_MAX {
            return Err(FrankenError::OutOfRange {
                what: "k_source".to_owned(),
                value: packet.k_source.to_string(),
            });
        }
        let symbol_size =
            u32::try_from(packet.symbol_data.len()).map_err(|_| FrankenError::OutOfRange {
                what: "symbol_size".to_owned(),
                value: packet.symbol_data.len().to_string(),
            })?;
        if symbol_size == 0 {
            return Err(FrankenError::OutOfRange {
                what: "symbol_size".to_owned(),
                value: "0".to_owned(),
            });
        }
        let padded_len = usize::try_from(packet.k_source)
            .ok()
            .and_then(|k| k.checked_mul(packet.symbol_data.len()))
            .ok_or(FrankenError::TooBig)?;
        if padded_len > crate::replication_sender::MAX_REPAIR_WORK_BYTES {
            return Err(FrankenError::TooBig);
        }

        let changeset_id = packet.changeset_id;

        if self
            .changeset_to_block
            .get(&changeset_id)
            .is_some_and(|&idx| self.block_decoders[idx].decoded)
        {
            return Ok(SnapshotPacketResult::BlockAlreadyDecoded);
        }

        let new_mapping = !self.changeset_to_block.contains_key(&changeset_id);
        let duplicate = self
            .changeset_to_block
            .get(&changeset_id)
            .is_some_and(|&idx| self.block_decoders[idx].received_isis.contains(&packet.esi));
        if !duplicate
            && self
                .buffered_symbol_bytes
                .checked_add(packet.symbol_data.len())
                .is_none_or(|bytes| bytes > crate::replication_sender::MAX_REPAIR_WORK_BYTES)
        {
            return Err(FrankenError::TooBig);
        }

        // Map changeset_id to block index.
        let block_idx = if let Some(&idx) = self.changeset_to_block.get(&changeset_id) {
            idx
        } else {
            // Find the next unmapped, undecoded block.
            let next_idx = self
                .block_decoders
                .iter()
                .position(|d| d.changeset_id.is_none() && !d.decoded);
            if let Some(idx) = next_idx {
                self.changeset_to_block.insert(changeset_id, idx);
                self.block_decoders[idx].initialize(changeset_id, packet.k_source, symbol_size);
                debug!(
                    bead_id = BEAD_ID,
                    block_index = idx,
                    k_source = packet.k_source,
                    "mapped new changeset to block"
                );
                idx
            } else {
                warn!(
                    bead_id = BEAD_ID,
                    "no available block slot for new changeset_id"
                );
                return Ok(SnapshotPacketResult::Rejected);
            }
        };

        if block_idx >= self.block_decoders.len() {
            return Ok(SnapshotPacketResult::Rejected);
        }

        let decoder = &mut self.block_decoders[block_idx];
        if decoder.decoded {
            return Ok(SnapshotPacketResult::BlockAlreadyDecoded);
        }

        // Validate consistency.
        if decoder.k_source != packet.k_source {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "k_source mismatch for block {block_idx}: {} vs {}",
                    decoder.k_source, packet.k_source
                ),
            });
        }
        if decoder.symbol_size != symbol_size {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "symbol_size mismatch for block {block_idx}: {} vs {symbol_size}",
                    decoder.symbol_size
                ),
            });
        }

        // Add symbol.
        let accepted = decoder.add_symbol(packet.esi, packet.symbol_data.clone());
        if !accepted {
            return Ok(SnapshotPacketResult::Duplicate);
        }
        self.buffered_symbol_bytes += packet.symbol_data.len();

        let padded = if decoder.ready_to_decode() {
            match decoder.try_decode(cx) {
                Ok(padded) => padded,
                Err(error) => {
                    decoder.symbols.remove(&packet.esi);
                    decoder.received_isis.remove(&packet.esi);
                    self.buffered_symbol_bytes -= packet.symbol_data.len();
                    if new_mapping {
                        self.changeset_to_block.remove(&changeset_id);
                        self.block_decoders[block_idx] = BlockDecoder::new();
                    }
                    return Err(error);
                }
            }
        } else {
            None
        };

        if self.state == SnapshotReceiverState::Waiting {
            self.state = SnapshotReceiverState::Receiving;
            info!(bead_id = BEAD_ID, "snapshot receiving started");
        }

        // Update resume state.
        if block_idx < self.resume.blocks.len() {
            self.resume.blocks[block_idx].record_isi(packet.esi);
        }

        // Check if ready to decode this block.
        if let Some(padded) = padded {
            match parse_decoded_snapshot_block(&padded, self.page_size, changeset_id) {
                Ok(pages) => {
                    let block_id = u32::try_from(block_idx).unwrap_or(u32::MAX);
                    decoder.decoded = true;
                    self.buffered_symbol_bytes -=
                        decoder.symbols.values().map(Vec::len).sum::<usize>();
                    decoder.symbols.clear();
                    decoder.received_isis.clear();
                    if block_idx < self.resume.blocks.len() {
                        self.resume.blocks[block_idx].decoded = true;
                    }
                    let n_pages = pages.len();
                    self.decoded_blocks.push(DecodedBlock {
                        block_index: block_id,
                        pages,
                    });
                    info!(
                        bead_id = BEAD_ID,
                        block_index = block_idx,
                        n_pages,
                        decoded_so_far = self.decoded_blocks.len(),
                        total_blocks = self.num_blocks,
                        "source block decoded (progressive)"
                    );

                    // Check if all blocks are done.
                    if self.block_decoders.iter().all(|d| d.decoded) {
                        self.state = SnapshotReceiverState::Complete;
                        info!(
                            bead_id = BEAD_ID,
                            total_blocks = self.num_blocks,
                            "snapshot fully received"
                        );
                    }
                    return Ok(SnapshotPacketResult::BlockDecoded(block_id));
                }
                Err(e) => {
                    error!(
                        bead_id = BEAD_ID,
                        block_index = block_idx,
                        error = %e,
                        "snapshot block validation failed"
                    );
                    self.buffered_symbol_bytes -=
                        decoder.symbols.values().map(Vec::len).sum::<usize>();
                    self.block_decoders[block_idx] = BlockDecoder::new();
                    self.changeset_to_block.remove(&changeset_id);
                    self.resume.blocks[block_idx] =
                        BlockResumeState::new(u32::try_from(block_idx).unwrap_or(u32::MAX));
                    if self.changeset_to_block.is_empty() {
                        self.state = SnapshotReceiverState::Waiting;
                    }
                    return Err(e);
                }
            }
        }

        Ok(SnapshotPacketResult::Accepted)
    }
}

/// Result of processing a snapshot packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotPacketResult {
    /// Symbol accepted, need more.
    Accepted,
    /// Duplicate ISI, ignored.
    Duplicate,
    /// A source block was fully decoded (progressive).
    BlockDecoded(u32),
    /// This block was already decoded.
    BlockAlreadyDecoded,
    /// Packet rejected (no available block slot or already complete).
    Rejected,
    /// Snapshot already complete.
    AlreadyComplete,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse decoded snapshot block bytes into pages with xxh3 validation.
fn parse_decoded_snapshot_block(
    padded_bytes: &[u8],
    page_size: u32,
    changeset_id: ChangesetId,
) -> Result<Vec<DecodedBlockPage>> {
    use crate::replication_sender::ChangesetHeader;

    if padded_bytes.len() < CHANGESET_HEADER_SIZE {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "decoded block too short for header: {} < {CHANGESET_HEADER_SIZE}",
                padded_bytes.len()
            ),
        });
    }

    let header_bytes: [u8; CHANGESET_HEADER_SIZE] = padded_bytes[..CHANGESET_HEADER_SIZE]
        .try_into()
        .expect("checked length");
    let header = ChangesetHeader::from_bytes(&header_bytes)?;

    let total_len = usize::try_from(header.total_len).map_err(|_| FrankenError::OutOfRange {
        what: "total_len".to_owned(),
        value: header.total_len.to_string(),
    })?;
    if total_len < CHANGESET_HEADER_SIZE || total_len > padded_bytes.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "total_len ({total_len}) exceeds decoded bytes ({})",
                padded_bytes.len()
            ),
        });
    }
    let changeset_bytes = &padded_bytes[..total_len];
    if header.page_size != page_size || compute_changeset_id(changeset_bytes) != changeset_id {
        return Err(FrankenError::DatabaseCorrupt {
            detail: "snapshot page size or changeset identity mismatch".to_owned(),
        });
    }

    let entry_size = 4_usize + 8 + header.page_size as usize;
    let data_bytes = &changeset_bytes[CHANGESET_HEADER_SIZE..];

    let required_data_len = (header.n_pages as usize)
        .checked_mul(entry_size)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "n_pages causes size overflow".to_owned(),
        })?;

    if data_bytes.len() != required_data_len {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "changeset truncated: expected {} data bytes, got {}",
                required_data_len,
                data_bytes.len()
            ),
        });
    }

    let mut pages = Vec::with_capacity(header.n_pages as usize);
    for i in 0..header.n_pages as usize {
        let offset = i * entry_size;
        let page_number =
            u32::from_le_bytes(data_bytes[offset..offset + 4].try_into().expect("4 bytes"));
        let page_xxh3 = u64::from_le_bytes(
            data_bytes[offset + 4..offset + 12]
                .try_into()
                .expect("8 bytes"),
        );
        let page_data = data_bytes[offset + 12..offset + 12 + header.page_size as usize].to_vec();

        let computed_xxh3 = xxhash_rust::xxh3::xxh3_64(&page_data);
        if computed_xxh3 != page_xxh3 {
            error!(
                bead_id = BEAD_ID,
                page_number,
                expected_xxh3 = page_xxh3,
                computed_xxh3,
                "snapshot page xxh3 mismatch"
            );
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "snapshot page {page_number} xxh3 mismatch: {page_xxh3:#x} vs {computed_xxh3:#x}"
                ),
            });
        }

        pages.push(DecodedBlockPage {
            page_number,
            page_data,
        });
    }

    if pages
        .windows(2)
        .any(|pair| pair[0].page_number > pair[1].page_number)
    {
        return Err(FrankenError::DatabaseCorrupt {
            detail: "snapshot pages are not ordered by page number".to_owned(),
        });
    }

    Ok(pages)
}

#[cfg(test)]
mod tests {
    use crate::source_block_partition::partition_source_blocks;
    use fsqlite_types::cx::Cx;

    use super::*;
    use crate::replication_sender::PageEntry;

    const TEST_BEAD_ID: &str = "bd-1hi.15";

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn test_snapshot_repairs_permanent_source_erasure() {
        let cx = Cx::new();
        let key = [0x91; 32];
        let mut pages = make_pages(256, &[1, 2, 3]);
        let original = pages.clone();
        let mut sender = SnapshotSender::prepare(
            256,
            &mut pages,
            SenderConfig {
                symbol_size: 256,
                max_isi_multiplier: 4,
            },
        )
        .expect("prepare");
        let mut receiver = SnapshotReceiver::new(sender.num_blocks(), 256);
        receiver
            .set_auth_key(key)
            .expect("set key before receiving");
        let mut repairs = 0;
        let mut dropped_source = false;
        while let Some(mut packet) = sender.next_packet(&cx).expect("real packet") {
            if packet.esi == 0 {
                dropped_source = true;
                continue;
            }
            if !packet.is_source_symbol() {
                if repairs == 0 {
                    assert_eq!(packet.esi, packet.k_source);
                }
                repairs += 1;
            }
            packet.attach_auth_tag(&key);
            let mut forged = packet.clone();
            forged.symbol_data[0] ^= 1;
            assert_eq!(
                receiver.process_packet(&cx, &forged).expect("erasure"),
                SnapshotPacketResult::Rejected
            );
            let result = receiver.process_packet(&cx, &packet).expect("receive");
            if matches!(result, SnapshotPacketResult::BlockDecoded(_)) {
                break;
            }
        }
        assert!(dropped_source && repairs > 0);
        assert_eq!(receiver.state(), SnapshotReceiverState::Complete);
        assert!(receiver.resume_state().all_decoded());
        assert_eq!(receiver.buffered_symbol_bytes, 0);
        let blocks = receiver.take_decoded_blocks();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].pages.len(), original.len());
        for (decoded, expected) in blocks[0].pages.iter().zip(&original) {
            assert_eq!(decoded.page_number, expected.page_number);
            assert_eq!(decoded.page_data, expected.page_bytes);
        }
        eprintln!(
            "bead_id=bd-3mgq5.2 phase=snapshot_decode erased_source_esi=0 repair_packets={repairs} exact_pages={}",
            original.len()
        );
    }

    #[test]
    fn test_snapshot_rejects_wrong_object_identity() {
        let cx = Cx::new();
        let mut pages = make_pages(128, &[1]);
        let mut sender = SnapshotSender::prepare(
            128,
            &mut pages,
            SenderConfig {
                symbol_size: 128,
                max_isi_multiplier: 1,
            },
        )
        .expect("prepare");
        let mut receiver = SnapshotReceiver::new(1, 128);
        let mut rejected = false;
        while let Some(mut packet) = sender.next_packet(&cx).expect("packet") {
            packet.changeset_id = ChangesetId::from_bytes([0xAB; 16]);
            packet.seed = derive_seed_from_changeset_id(&packet.changeset_id);
            match receiver.process_packet(&cx, &packet) {
                Err(FrankenError::DatabaseCorrupt { .. }) => {
                    rejected = true;
                    break;
                }
                Ok(SnapshotPacketResult::Accepted) => {}
                other => panic!("unexpected result: {other:?}"),
            }
        }
        assert!(rejected);
        assert!(!receiver.resume_state().all_decoded());
        assert!(receiver.take_decoded_blocks().is_empty());
        assert_eq!(receiver.buffered_symbol_bytes, 0);
        assert!(receiver.changeset_to_block.is_empty());
        assert_eq!(
            receiver.resume_state().to_bytes(),
            ResumeState::new(1).to_bytes()
        );
        sender.restart();
        while let Some(packet) = sender.next_packet(&cx).expect("valid packet") {
            receiver
                .process_packet(&cx, &packet)
                .expect("valid retry after rejection");
        }
        assert_eq!(receiver.state(), SnapshotReceiverState::Complete);
        assert_eq!(
            receiver.take_decoded_blocks()[0].pages[0].page_data,
            pages[0].page_bytes
        );
    }

    #[test]
    fn test_snapshot_rejected_admission_and_cancel_preserve_receiver() {
        let cx = Cx::new();
        let mut pages = make_pages(128, &[1]);
        let mut sender = SnapshotSender::prepare(
            128,
            &mut pages,
            SenderConfig {
                symbol_size: 128,
                max_isi_multiplier: 1,
            },
        )
        .expect("prepare");
        let first = sender.next_packet(&cx).expect("packet").expect("first");
        let mut receiver = SnapshotReceiver::new(1, 128);
        let empty_resume = receiver.resume_state().to_bytes();
        for esi in [crate::replication_sender::MAX_REPLICATION_ESI + 1, u32::MAX] {
            let mut invalid = first.clone();
            invalid.esi = esi;
            assert!(matches!(
                receiver.process_packet(&cx, &invalid),
                Err(FrankenError::OutOfRange { .. })
            ));
        }
        let cancelled = Cx::new();
        cancelled.cancel();
        assert!(matches!(
            receiver.process_packet(&cancelled, &first),
            Err(FrankenError::Abort)
        ));
        let mut oversized = first.clone();
        oversized.k_source = K_MAX;
        oversized.symbol_data = vec![0; 2048];
        oversized.symbol_size_t = 2048;
        oversized.payload_xxh3 = ReplicationPacket::compute_payload_xxh3(&oversized.symbol_data);
        assert!(matches!(
            receiver.process_packet(&cx, &oversized),
            Err(FrankenError::TooBig)
        ));
        assert_eq!(receiver.state(), SnapshotReceiverState::Waiting);
        assert!(receiver.changeset_to_block.is_empty());
        assert_eq!(receiver.buffered_symbol_bytes, 0);
        assert_eq!(receiver.resume_state().to_bytes(), empty_resume);
        receiver
            .process_packet(&cx, &first)
            .expect("first admitted");
        while let Some(packet) = sender.next_packet(&cx).expect("next packet") {
            receiver
                .process_packet(&cx, &packet)
                .expect("valid receive");
        }
        assert_eq!(receiver.state(), SnapshotReceiverState::Complete);
        assert_eq!(
            receiver.take_decoded_blocks()[0].pages[0].page_data,
            pages[0].page_bytes
        );
    }

    #[test]
    fn test_snapshot_failed_prepare_preserves_unsorted_input() {
        for invalid_checksum in [false, true] {
            let mut pages = make_pages(128, &[3, 1, 2]);
            if invalid_checksum {
                pages[1].page_xxh3 ^= 1;
            } else {
                pages[1].page_bytes.push(9);
            }
            let original = pages.clone();
            assert!(SnapshotSender::prepare(128, &mut pages, SenderConfig::default()).is_err());
            assert_eq!(pages, original);
        }
        let mut pages = make_pages(128, &[3, 1, 2]);
        let original = pages.clone();
        assert!(matches!(
            SnapshotSender::prepare(
                128,
                &mut pages,
                SenderConfig {
                    symbol_size: 128,
                    max_isi_multiplier: 0,
                }
            ),
            Err(FrankenError::OutOfRange { .. })
        ));
        assert_eq!(pages, original);
    }

    #[test]
    fn test_snapshot_zero_symbol_size_is_error() {
        let mut pages = make_pages(128, &[1]);
        assert!(matches!(
            SnapshotSender::prepare(
                128,
                &mut pages,
                SenderConfig {
                    symbol_size: 0,
                    max_isi_multiplier: 1,
                }
            ),
            Err(FrankenError::OutOfRange { .. })
        ));
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn test_snapshot_interleaved_objects_repair_independently() {
        let cx = Cx::new();
        let mut pages_a = make_pages(128, &[1, 2]);
        let mut pages_b = make_pages(128, &[11, 12]);
        let config = SenderConfig {
            symbol_size: 128,
            max_isi_multiplier: 8,
        };
        let mut senders = [
            SnapshotSender::prepare(128, &mut pages_a, config.clone()).expect("A"),
            SnapshotSender::prepare(128, &mut pages_b, config).expect("B"),
        ];
        let mut receiver = SnapshotReceiver::new(2, 128);
        let mut completed = [false; 2];
        let mut object_ids = [None; 2];
        while !completed.iter().all(|done| *done) {
            for (index, sender) in senders.iter_mut().enumerate() {
                if completed[index] {
                    continue;
                }
                let packet = sender
                    .next_packet(&cx)
                    .expect("encoding")
                    .expect("fixed repair schedule must suffice");
                object_ids[index] = Some(packet.changeset_id);
                if packet.esi == 0 {
                    continue;
                }
                if matches!(
                    receiver.process_packet(&cx, &packet).expect("receive"),
                    SnapshotPacketResult::BlockDecoded(_)
                ) {
                    completed[index] = true;
                }
            }
        }
        assert_ne!(object_ids[0], object_ids[1]);
        assert_eq!(receiver.state(), SnapshotReceiverState::Complete);
        assert_eq!(receiver.changeset_to_block.len(), 2);
        assert_eq!(receiver.buffered_symbol_bytes, 0);
        let mut blocks = receiver.take_decoded_blocks();
        assert_eq!(blocks.len(), 2);
        blocks.sort_by_key(|block| block.block_index);
        for (block, expected) in blocks.iter().zip([pages_a, pages_b]) {
            assert_eq!(block.pages.len(), expected.len());
            for (page, original) in block.pages.iter().zip(expected) {
                assert_eq!(page.page_number, original.page_number);
                assert_eq!(page.page_data, original.page_bytes);
            }
        }
        eprintln!(
            "bead_id=bd-3mgq5.2 event=interleaved_snapshot_objects blocks=2 erased_source_esi=0 exact_pages=4"
        );
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn test_snapshot_sender_repairs_every_block_of_one_large_input() {
        use std::collections::{BTreeMap, HashSet};

        use crate::replication_sender::{
            MAX_REPLICATION_SYMBOL_SIZE, compute_changeset_id, encode_changeset,
            max_pages_per_repair_block,
        };

        let cx = Cx::new();
        let page_size = 4096;
        let symbol_size = u16::try_from(MAX_REPLICATION_SYMBOL_SIZE).expect("wire symbol size");
        let block_pages = max_pages_per_repair_block(page_size, symbol_size).expect("block budget");
        assert!(block_pages > 1);
        let page_count = block_pages
            .checked_mul(2)
            .and_then(|n| n.checked_add(1))
            .expect("page count");
        let page_numbers: Vec<_> =
            (1..=u32::try_from(page_count).expect("page count fits")).collect();
        let mut pages = make_pages(page_size, &page_numbers);
        let expected: BTreeMap<_, _> = pages
            .iter()
            .map(|page| (page.page_number, page.page_bytes.clone()))
            .collect();
        let mut expected_objects = BTreeMap::new();
        for chunk in pages.chunks(block_pages) {
            let mut originals = chunk.to_vec();
            let bytes =
                encode_changeset(page_size, &mut originals).expect("complete block changeset");
            assert!(
                expected_objects
                    .insert(*compute_changeset_id(&bytes).as_bytes(), originals)
                    .is_none()
            );
        }
        let mut sender = SnapshotSender::prepare(
            page_size,
            &mut pages,
            SenderConfig {
                symbol_size,
                max_isi_multiplier: 8,
            },
        )
        .expect("one large snapshot input");
        assert_eq!(sender.num_blocks(), 3);
        let mut receiver = SnapshotReceiver::new(sender.num_blocks(), page_size);
        let mut packets_by_object: BTreeMap<[u8; 16], Vec<ReplicationPacket>> = BTreeMap::new();
        while let Some(packet) = sender.next_packet(&cx).expect("bounded block encoding") {
            packets_by_object
                .entry(*packet.changeset_id.as_bytes())
                .or_default()
                .push(packet);
        }
        assert_eq!(
            packets_by_object.keys().collect::<Vec<_>>(),
            expected_objects.keys().collect::<Vec<_>>()
        );
        for packets in packets_by_object.values_mut() {
            let k = packets[0].k_source;
            assert_eq!(packets.len(), usize::try_from(k).expect("K fits") * 8);
            assert_eq!(packets.iter().filter(|packet| packet.esi == 0).count(), 1);
            assert!(
                packets
                    .iter()
                    .all(|packet| packet.k_source == k && packet.sbn == 0)
            );
            packets.retain(|packet| packet.esi != 0);
            for pair in packets.chunks_mut(2) {
                if pair.len() == 2 {
                    pair.swap(0, 1);
                }
            }
        }

        let mut completed = HashSet::new();
        let mut completed_slots = HashSet::new();
        let mut recovered = BTreeMap::new();
        let mut block_sizes = Vec::new();
        let rounds = packets_by_object
            .values()
            .map(Vec::len)
            .max()
            .expect("three streams");
        for round in 0..rounds {
            for (id, packets) in packets_by_object.iter().rev() {
                if completed.contains(id) {
                    continue;
                }
                let Some(packet) = packets.get(round) else {
                    continue;
                };
                assert_ne!(packet.esi, 0);
                if let SnapshotPacketResult::BlockDecoded(slot) = receiver
                    .process_packet(&cx, packet)
                    .expect("snapshot receive")
                {
                    assert!(completed.insert(*id), "each object is decoded once");
                    assert!(completed_slots.insert(slot), "one object per receiver slot");
                    let blocks = receiver.take_decoded_blocks();
                    assert_eq!(blocks.len(), 1);
                    let block = &blocks[0];
                    assert_eq!(block.block_index, slot);
                    let originals = &expected_objects[id];
                    assert_eq!(block.pages.len(), originals.len());
                    let original_by_page: BTreeMap<_, _> = originals
                        .iter()
                        .map(|page| (page.page_number, &page.page_bytes))
                        .collect();
                    for page in &block.pages {
                        assert_eq!(&page.page_data, original_by_page[&page.page_number]);
                        assert!(
                            recovered
                                .insert(page.page_number, page.page_data.clone())
                                .is_none(),
                            "page repeated across objects"
                        );
                    }
                    let resume =
                        &receiver.resume_state().blocks[usize::try_from(slot).expect("slot fits")];
                    assert!(resume.decoded);
                    assert!(!resume.received_isis.contains(&0));
                    assert!(
                        resume
                            .received_isis
                            .iter()
                            .any(|esi| *esi >= packet.k_source)
                    );
                    block_sizes.push(block.pages.len());
                    eprintln!(
                        "bead_id=bd-3mgq5.2 event=single_input_snapshot_block object={id:?} receiver_slot={slot} round={round} k={} erased_source_esi=0 recovered_pages={}",
                        packet.k_source,
                        block.pages.len()
                    );
                }
            }
            if round == 0 {
                assert!(
                    receiver
                        .block_decoders
                        .iter()
                        .filter(|decoder| !decoder.decoded && !decoder.received_isis.is_empty())
                        .count()
                        >= 2,
                    "the full objects must overlap in collection"
                );
            }
        }
        assert_eq!(
            completed.len(),
            3,
            "both full blocks and short tail must recover"
        );
        block_sizes.sort_unstable();
        assert_eq!(block_sizes, [1, block_pages, block_pages]);
        assert_eq!(recovered, expected);
        assert_eq!(receiver.state(), SnapshotReceiverState::Complete);
        assert!(receiver.resume_state().all_decoded());
        assert_eq!(receiver.changeset_to_block.len(), 3);
        assert_eq!(receiver.buffered_symbol_bytes, 0);
        assert!(receiver.take_decoded_blocks().is_empty());
    }

    #[allow(clippy::cast_possible_truncation)]
    fn make_pages(page_size: u32, page_numbers: &[u32]) -> Vec<PageEntry> {
        page_numbers
            .iter()
            .map(|&pn| {
                let mut data = vec![0_u8; page_size as usize];
                for (i, byte) in data.iter_mut().enumerate() {
                    *byte = ((pn as usize * 251 + i * 31) % 256) as u8;
                }
                PageEntry::new(pn, data)
            })
            .collect()
    }

    // -----------------------------------------------------------------------
    // Resume state tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_resume_state_persistence() {
        let mut resume = ResumeState::new(3);
        resume.blocks[0].record_isi(0);
        resume.blocks[0].record_isi(5);
        resume.blocks[0].record_isi(10);
        resume.blocks[1].decoded = true;

        let bytes = resume.to_bytes();
        let restored = ResumeState::from_bytes(&bytes).expect("deserialize");

        assert_eq!(
            restored.total_blocks, 3,
            "bead_id={TEST_BEAD_ID} case=resume_total_blocks"
        );
        assert_eq!(
            restored.blocks[0].num_received, 3,
            "bead_id={TEST_BEAD_ID} case=resume_block0_received"
        );
        assert!(
            restored.blocks[0].received_isis.contains(&5),
            "bead_id={TEST_BEAD_ID} case=resume_block0_isi_5"
        );
        assert!(
            restored.blocks[1].decoded,
            "bead_id={TEST_BEAD_ID} case=resume_block1_decoded"
        );
        assert!(
            !restored.blocks[2].decoded,
            "bead_id={TEST_BEAD_ID} case=resume_block2_not_decoded"
        );
    }

    #[test]
    fn test_resume_no_protocol_negotiation() {
        // Resume state works without any sender-side coordination.
        let mut resume = ResumeState::new(2);
        resume.blocks[0].record_isi(0);
        resume.blocks[0].record_isi(1);

        // Persist and restore.
        let bytes = resume.to_bytes();
        let restored = ResumeState::from_bytes(&bytes).expect("deserialize");
        assert_eq!(
            restored.blocks[0].num_received, 2,
            "bead_id={TEST_BEAD_ID} case=resume_no_negotiation"
        );
        assert!(!restored.all_decoded());
    }

    // -----------------------------------------------------------------------
    // Snapshot sender/receiver integration
    // -----------------------------------------------------------------------

    #[test]
    fn test_snapshot_single_block() {
        let page_size = 256_u32;
        let page_numbers: Vec<u32> = (1..=10).collect();
        let mut pages = make_pages(page_size, &page_numbers);

        let config = SenderConfig {
            symbol_size: 256,
            max_isi_multiplier: 1,
        };
        let mut sender = SnapshotSender::prepare(page_size, &mut pages, config).expect("prepare");
        assert_eq!(
            sender.num_blocks(),
            1,
            "bead_id={TEST_BEAD_ID} case=single_block"
        );

        // Collect all packets.
        let mut packets = Vec::new();
        while let Some(pkt) = sender.next_packet(&Cx::new()).expect("next packet") {
            packets.push(pkt);
        }
        assert!(
            !packets.is_empty(),
            "bead_id={TEST_BEAD_ID} case=has_packets"
        );

        // Feed to receiver.
        let mut receiver = SnapshotReceiver::new(1, page_size);
        for pkt in &packets {
            let _ = receiver.process_packet(&Cx::new(), pkt);
        }

        assert_eq!(
            receiver.state(),
            SnapshotReceiverState::Complete,
            "bead_id={TEST_BEAD_ID} case=single_block_complete"
        );

        let blocks = receiver.take_decoded_blocks();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].pages.len(), 10);
    }

    #[test]
    fn test_snapshot_multi_block_small() {
        // Force multi-block by using many pages.
        // Use smaller page count that still creates multiple blocks
        // by using the sender's internal sharding mechanism.
        let page_size = 64_u32;
        let n_pages = 200_u32;
        let page_numbers: Vec<u32> = (1..=n_pages).collect();
        let mut pages = make_pages(page_size, &page_numbers);

        let config = SenderConfig {
            symbol_size: 64,
            max_isi_multiplier: 1,
        };
        let mut sender = SnapshotSender::prepare(page_size, &mut pages, config).expect("prepare");

        // Should be 1 block (200 < K_MAX).
        assert_eq!(
            sender.num_blocks(),
            1,
            "bead_id={TEST_BEAD_ID} case=multi_block_small_count"
        );

        let mut packets = Vec::new();
        while let Some(pkt) = sender.next_packet(&Cx::new()).expect("next packet") {
            packets.push(pkt);
        }

        let mut receiver = SnapshotReceiver::new(sender.num_blocks(), page_size);
        for pkt in &packets {
            let _ = receiver.process_packet(&Cx::new(), pkt);
        }

        assert_eq!(
            receiver.state(),
            SnapshotReceiverState::Complete,
            "bead_id={TEST_BEAD_ID} case=multi_block_small_complete"
        );

        let blocks = receiver.take_decoded_blocks();
        let total_pages: usize = blocks.iter().map(|b| b.pages.len()).sum();
        assert_eq!(
            total_pages, n_pages as usize,
            "bead_id={TEST_BEAD_ID} case=multi_block_all_pages"
        );
    }

    #[test]
    fn test_duplicate_isi_discarded() {
        let page_size = 128_u32;
        let mut pages = make_pages(page_size, &[1, 2, 3]);
        let config = SenderConfig {
            symbol_size: 128,
            max_isi_multiplier: 1,
        };
        let mut sender = SnapshotSender::prepare(page_size, &mut pages, config).expect("prepare");

        let mut packets = Vec::new();
        while let Some(pkt) = sender.next_packet(&Cx::new()).expect("next packet") {
            packets.push(pkt);
        }

        let mut receiver = SnapshotReceiver::new(1, page_size);

        // Feed first packet twice.
        let r1 = receiver
            .process_packet(&Cx::new(), &packets[0])
            .expect("first");
        assert_ne!(
            r1,
            SnapshotPacketResult::Duplicate,
            "bead_id={TEST_BEAD_ID} case=first_not_dup"
        );
        let r2 = receiver
            .process_packet(&Cx::new(), &packets[0])
            .expect("duplicate");
        assert_eq!(
            r2,
            SnapshotPacketResult::Duplicate,
            "bead_id={TEST_BEAD_ID} case=dup_discarded"
        );
    }

    #[test]
    fn test_snapshot_progressive_receive() {
        // With a single block, after decode the receiver is complete.
        // Progressive receive means we can query pages from decoded blocks
        // while other blocks are still being received.
        let page_size = 128_u32;
        let mut pages = make_pages(page_size, &[1, 2, 3, 4, 5]);
        let config = SenderConfig {
            symbol_size: 128,
            max_isi_multiplier: 1,
        };
        let mut sender = SnapshotSender::prepare(page_size, &mut pages, config).expect("prepare");

        let mut packets = Vec::new();
        while let Some(pkt) = sender.next_packet(&Cx::new()).expect("next packet") {
            packets.push(pkt);
        }

        let mut receiver = SnapshotReceiver::new(1, page_size);
        let mut block_decoded_at = None;

        for (i, pkt) in packets.iter().enumerate() {
            if let Ok(SnapshotPacketResult::BlockDecoded(_)) =
                receiver.process_packet(&Cx::new(), pkt)
            {
                block_decoded_at = Some(i);
                break;
            }
        }

        assert!(
            block_decoded_at.is_some(),
            "bead_id={TEST_BEAD_ID} case=progressive_block_decoded"
        );

        // After decoding, pages are available.
        let blocks = receiver.take_decoded_blocks();
        assert!(
            !blocks.is_empty(),
            "bead_id={TEST_BEAD_ID} case=progressive_has_pages"
        );
    }

    // -----------------------------------------------------------------------
    // E2E tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_e2e_sender_receiver_roundtrip() {
        let page_size = 512_u32;
        let n_pages = 50_u32;
        let page_numbers: Vec<u32> = (1..=n_pages).collect();
        let original_pages = make_pages(page_size, &page_numbers);
        let mut pages = original_pages.clone();

        let config = SenderConfig {
            symbol_size: 512,
            max_isi_multiplier: 1,
        };
        let mut sender = SnapshotSender::prepare(page_size, &mut pages, config).expect("prepare");

        let mut packets = Vec::new();
        while let Some(pkt) = sender.next_packet(&Cx::new()).expect("next packet") {
            packets.push(pkt);
        }

        let mut receiver = SnapshotReceiver::new(sender.num_blocks(), page_size);
        for pkt in &packets {
            let _ = receiver.process_packet(&Cx::new(), pkt);
        }

        assert_eq!(
            receiver.state(),
            SnapshotReceiverState::Complete,
            "bead_id={TEST_BEAD_ID} case=e2e_roundtrip_complete"
        );

        let blocks = receiver.take_decoded_blocks();
        let mut all_decoded_pages: Vec<&DecodedBlockPage> =
            blocks.iter().flat_map(|b| b.pages.iter()).collect();
        all_decoded_pages.sort_by_key(|p| p.page_number);

        assert_eq!(
            all_decoded_pages.len(),
            original_pages.len(),
            "bead_id={TEST_BEAD_ID} case=e2e_page_count"
        );

        for (decoded, original) in all_decoded_pages.iter().zip(original_pages.iter()) {
            assert_eq!(
                decoded.page_number, original.page_number,
                "bead_id={TEST_BEAD_ID} case=e2e_page_number"
            );
            assert_eq!(
                decoded.page_data, original.page_bytes,
                "bead_id={TEST_BEAD_ID} case=e2e_page_data pn={}",
                original.page_number
            );
        }
    }

    #[test]
    fn test_e2e_resume_after_partial() {
        let page_size = 128_u32;
        let n_pages = 20_u32;
        let mut pages = make_pages(page_size, &(1..=n_pages).collect::<Vec<_>>());

        let config = SenderConfig {
            symbol_size: 128,
            max_isi_multiplier: 1,
        };
        let mut sender = SnapshotSender::prepare(page_size, &mut pages, config).expect("prepare");

        let mut packets = Vec::new();
        while let Some(pkt) = sender.next_packet(&Cx::new()).expect("next packet") {
            packets.push(pkt);
        }

        // First receiver: receive only half the packets.
        let half = packets.len() / 2;
        let mut receiver1 = SnapshotReceiver::new(sender.num_blocks(), page_size);
        for pkt in &packets[..half] {
            let _ = receiver1.process_packet(&Cx::new(), pkt);
        }

        // Persist resume state.
        let resume_bytes = receiver1.resume_state().to_bytes();

        // "Crash" — create new receiver from resume state.
        let resume = ResumeState::from_bytes(&resume_bytes).expect("restore");
        let mut receiver2 = SnapshotReceiver::from_resume(resume, page_size);

        // Continue with remaining packets (and possibly some overlap).
        for pkt in &packets {
            let _ = receiver2.process_packet(&Cx::new(), pkt);
        }

        // Should be complete now.
        assert_eq!(
            receiver2.state(),
            SnapshotReceiverState::Complete,
            "bead_id={TEST_BEAD_ID} case=e2e_resume_complete"
        );
    }

    #[test]
    fn test_e2e_bd_1hi_15_compliance() {
        // Full compliance test.
        let page_size = 256_u32;
        let n_pages = 30_u32;
        let original_pages = make_pages(page_size, &(1..=n_pages).collect::<Vec<_>>());
        let mut pages = original_pages;

        let config = SenderConfig {
            symbol_size: 256,
            max_isi_multiplier: 1,
        };
        let mut sender = SnapshotSender::prepare(page_size, &mut pages, config).expect("prepare");

        // Verify sender state.
        assert!(
            sender.num_blocks() >= 1,
            "bead_id={TEST_BEAD_ID} case=compliance_has_blocks"
        );
        assert!(
            sender.total_source_symbols() > 0,
            "bead_id={TEST_BEAD_ID} case=compliance_has_symbols"
        );

        let mut packets = Vec::new();
        while let Some(pkt) = sender.next_packet(&Cx::new()).expect("next packet") {
            packets.push(pkt);
        }

        let mut receiver = SnapshotReceiver::new(sender.num_blocks(), page_size);
        assert_eq!(receiver.state(), SnapshotReceiverState::Waiting);

        for pkt in &packets {
            let _ = receiver.process_packet(&Cx::new(), pkt);
        }
        assert_eq!(receiver.state(), SnapshotReceiverState::Complete);

        let blocks = receiver.take_decoded_blocks();
        let total_decoded: usize = blocks.iter().map(|b| b.pages.len()).sum();
        assert_eq!(
            total_decoded, n_pages as usize,
            "bead_id={TEST_BEAD_ID} case=compliance_all_pages_decoded"
        );

        // Verify resume state.
        assert!(
            receiver.resume_state().all_decoded(),
            "bead_id={TEST_BEAD_ID} case=compliance_resume_all_decoded"
        );
    }

    // -----------------------------------------------------------------------
    // Property tests
    // -----------------------------------------------------------------------

    #[test]
    fn prop_partition_covers_all_pages() {
        for p in [1_u32, 10, 100, 1000, 56_403, 56_404, 100_000] {
            let blocks = partition_source_blocks(p).expect("partition");
            let total: u32 = blocks.iter().map(|b| b.num_pages).sum();
            assert_eq!(
                total, p,
                "bead_id={TEST_BEAD_ID} case=prop_partition_covers p={p}"
            );
        }
    }

    #[test]
    fn prop_partition_block_sizes_valid() {
        for p in [1_u32, 56_403, 56_404, 200_000] {
            let blocks = partition_source_blocks(p).expect("partition");
            for block in &blocks {
                assert!(
                    block.num_pages <= K_MAX,
                    "bead_id={TEST_BEAD_ID} case=prop_block_size p={p} block={} num_pages={}",
                    block.index,
                    block.num_pages
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Compliance gate tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_bd_1hi_15_unit_compliance_gate() {
        // Verify all required types exist.
        let _ = SnapshotReceiverState::Waiting;
        let _ = SnapshotReceiverState::Receiving;
        let _ = SnapshotReceiverState::Complete;

        let _ = SnapshotPacketResult::Accepted;
        let _ = SnapshotPacketResult::Duplicate;
        let _ = SnapshotPacketResult::Rejected;
        let _ = SnapshotPacketResult::AlreadyComplete;

        let resume = ResumeState::new(3);
        assert_eq!(resume.total_blocks, 3);
        assert!(!resume.all_decoded());
        assert_eq!(resume.decoded_count(), 0);

        // Verify BlockResumeState serialization.
        let block = BlockResumeState::new(0);
        let bytes = block.to_bytes();
        let (restored, _) = BlockResumeState::from_bytes(&bytes).expect("deser");
        assert_eq!(restored.block_id, 0);
    }

    #[test]
    fn prop_bd_1hi_15_structure_compliance() {
        // Verify snapshot sender + receiver integration.
        let page_size = 128_u32;
        let mut pages = make_pages(page_size, &[1, 2]);
        let config = SenderConfig {
            symbol_size: 128,
            max_isi_multiplier: 1,
        };
        let mut sender = SnapshotSender::prepare(page_size, &mut pages, config).expect("prepare");
        assert!(sender.num_blocks() >= 1);

        let mut packets = Vec::new();
        while let Some(pkt) = sender.next_packet(&Cx::new()).expect("next packet") {
            packets.push(pkt);
        }

        let mut receiver = SnapshotReceiver::new(sender.num_blocks(), page_size);
        for pkt in &packets {
            let _ = receiver.process_packet(&Cx::new(), pkt);
        }
        assert_eq!(receiver.state(), SnapshotReceiverState::Complete);
    }
}
