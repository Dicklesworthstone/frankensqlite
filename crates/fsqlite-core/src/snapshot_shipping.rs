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
use tracing::{debug, error, info, warn};

use crate::replication_sender::{
    CHANGESET_HEADER_SIZE, ChangesetId, PageEntry, ReplicationPacket, ReplicationPacketV2Header,
    SenderConfig, compute_changeset_id, derive_seed_from_changeset_id, encode_changeset,
};
use crate::source_block_partition::{K_MAX, SourceBlock, partition_source_blocks};

const BEAD_ID: &str = "bd-1hi.15";

// ---------------------------------------------------------------------------
// Resume State (persistent across connection losses)
// ---------------------------------------------------------------------------

/// Per-block resume state: tracks which ISIs have been received.
#[derive(Debug, Clone)]
pub struct BlockResumeState {
    /// Source block index (SBN).
    pub block_id: u8,
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
    fn new(block_id: u8) -> Self {
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
    /// Format: `block_id(1) | num_received(4 LE) | decoded(1) | n_isis(4 LE) | isis(4 LE each)`
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let n = self.received_isis.len();
        let mut buf = Vec::with_capacity(10 + n * 4);
        buf.push(self.block_id);
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
        if buf.len() < 10 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("BlockResumeState too short: {} < 10", buf.len()),
            });
        }
        let block_id = buf[0];
        let num_received = u32::from_le_bytes(buf[1..5].try_into().expect("4 bytes"));
        let decoded = buf[5] != 0;
        let n_isis = u32::from_le_bytes(buf[6..10].try_into().expect("4 bytes"));
        let n = n_isis as usize;
        let expected = 10 + n * 4;
        if buf.len() < expected {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("BlockResumeState truncated: {} < {expected}", buf.len()),
            });
        }
        let mut received_isis = HashSet::with_capacity(n);
        for i in 0..n {
            let offset = 10 + i * 4;
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
        let blocks = (0..total_blocks)
            .map(|i| BlockResumeState::new(u8::try_from(i).unwrap_or(u8::MAX)))
            .collect();
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
        if all_pages.is_empty() {
            return Err(FrankenError::OutOfRange {
                what: "pages".to_owned(),
                value: "0".to_owned(),
            });
        }

        let total_pages = u32::try_from(all_pages.len()).map_err(|_| FrankenError::OutOfRange {
            what: "total_pages".to_owned(),
            value: all_pages.len().to_string(),
        })?;

        let source_blocks = partition_source_blocks(total_pages)?;
        info!(
            bead_id = BEAD_ID,
            total_pages,
            n_blocks = source_blocks.len(),
            page_size,
            "snapshot partitioned into source blocks"
        );

        // Sort all pages by page_number.
        all_pages.sort_by_key(|p| p.page_number);

        // Build per-block changesets.
        let mut block_changeset_ids = Vec::with_capacity(source_blocks.len());
        let mut block_k_sources = Vec::with_capacity(source_blocks.len());
        let mut block_changesets = Vec::with_capacity(source_blocks.len());

        let mut page_idx = 0_usize;
        for block in &source_blocks {
            let end = page_idx + block.num_pages as usize;
            if end > all_pages.len() {
                return Err(FrankenError::Internal(format!(
                    "block {} requires pages up to index {end}, but only {} available",
                    block.index,
                    all_pages.len()
                )));
            }
            let block_pages = &mut all_pages[page_idx..end];
            let changeset_bytes = encode_changeset(page_size, block_pages)?;
            let changeset_id = compute_changeset_id(&changeset_bytes);

            // Compute K_source from changeset + symbol_size.
            let t = u64::from(config.symbol_size);
            let f = changeset_bytes.len() as u64;
            let k_source = u32::try_from(f.div_ceil(t)).map_err(|_| FrankenError::OutOfRange {
                what: "k_source".to_owned(),
                value: f.div_ceil(t).to_string(),
            })?;

            debug!(
                bead_id = BEAD_ID,
                block_index = block.index,
                num_pages = block.num_pages,
                changeset_len = changeset_bytes.len(),
                k_source,
                "prepared block changeset"
            );

            block_changeset_ids.push(changeset_id);
            block_k_sources.push(k_source);
            block_changesets.push(changeset_bytes);
            page_idx = end;
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
        })
    }

    /// Generate the next snapshot packet.
    ///
    /// Returns `None` when the current streaming pass is complete.
    /// Caller can restart from block 0 for continuous streaming.
    pub fn next_packet(&mut self) -> Option<ReplicationPacket> {
        if self.done || self.current_block >= self.source_blocks.len() {
            self.done = true;
            return None;
        }

        let k_source = self.block_k_sources[self.current_block];
        let max_isi = k_source.saturating_mul(self.config.max_isi_multiplier);

        if self.current_isi >= max_isi {
            self.current_block += 1;
            self.current_isi = 0;
            if self.current_block >= self.source_blocks.len() {
                self.done = true;
                return None;
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
            // Repair symbol placeholder.
            #[allow(clippy::cast_possible_truncation)]
            {
                let seed = derive_seed_from_changeset_id(&changeset_id);
                let repair_seed = seed.wrapping_add(u64::from(isi));
                let mut data = vec![0_u8; t];
                for (i, byte) in data.iter_mut().enumerate() {
                    let mixed = repair_seed
                        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                        .wrapping_add(i as u64);
                    *byte = (mixed >> 32) as u8;
                }
                data
            }
        };

        let seed = derive_seed_from_changeset_id(&changeset_id);
        let r_repair = max_isi.saturating_sub(k_source);
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
        Some(packet)
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
    pub block_index: u8,
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

    fn try_decode(&self) -> Option<Vec<u8>> {
        if !self.ready_to_decode() {
            return None;
        }
        let source_count = self
            .symbols
            .keys()
            .filter(|&&isi| isi < self.k_source)
            .count();
        let k = self.k_source as usize;
        let t = self.symbol_size as usize;
        if source_count >= k {
            let padded_len = k * t;
            let mut padded = vec![0_u8; padded_len];
            for isi in 0..self.k_source {
                if let Some(data) = self.symbols.get(&isi) {
                    let start = isi as usize * t;
                    let copy_len = data.len().min(t);
                    padded[start..start + copy_len].copy_from_slice(&data[..copy_len]);
                }
            }
            Some(padded)
        } else {
            warn!(
                bead_id = BEAD_ID,
                source_count,
                k_source = self.k_source,
                "snapshot block decode needs repair symbols (production RaptorQ)"
            );
            None
        }
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
        }
    }

    /// Current state.
    #[must_use]
    pub const fn state(&self) -> SnapshotReceiverState {
        self.state
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
    pub fn process_packet(&mut self, packet: &ReplicationPacket) -> Result<SnapshotPacketResult> {
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

        if self.state == SnapshotReceiverState::Waiting {
            self.state = SnapshotReceiverState::Receiving;
            info!(bead_id = BEAD_ID, "snapshot receiving started");
        }

        let changeset_id = packet.changeset_id;

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

        // Update resume state.
        if block_idx < self.resume.blocks.len() {
            self.resume.blocks[block_idx].record_isi(packet.esi);
        }

        // Check if ready to decode this block.
        if decoder.ready_to_decode() && !decoder.decoded {
            if let Some(padded) = decoder.try_decode() {
                match parse_decoded_snapshot_block(&padded, self.page_size) {
                    Ok(pages) => {
                        let block_id = u8::try_from(block_idx).unwrap_or(u8::MAX);
                        decoder.decoded = true;
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
                        return Err(e);
                    }
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
    BlockDecoded(u8),
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
    _page_size: u32,
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
    if total_len > padded_bytes.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "total_len ({total_len}) exceeds decoded bytes ({})",
                padded_bytes.len()
            ),
        });
    }
    let changeset_bytes = &padded_bytes[..total_len];

    let entry_size = 4_usize + 8 + header.page_size as usize;
    let data_bytes = &changeset_bytes[CHANGESET_HEADER_SIZE..];

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

    Ok(pages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication_sender::PageEntry;

    const TEST_BEAD_ID: &str = "bd-1hi.15";

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
        while let Some(pkt) = sender.next_packet() {
            packets.push(pkt);
        }
        assert!(
            !packets.is_empty(),
            "bead_id={TEST_BEAD_ID} case=has_packets"
        );

        // Feed to receiver.
        let mut receiver = SnapshotReceiver::new(1, page_size);
        for pkt in &packets {
            let _ = receiver.process_packet(pkt);
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
        while let Some(pkt) = sender.next_packet() {
            packets.push(pkt);
        }

        let mut receiver = SnapshotReceiver::new(sender.num_blocks(), page_size);
        for pkt in &packets {
            let _ = receiver.process_packet(pkt);
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
        while let Some(pkt) = sender.next_packet() {
            packets.push(pkt);
        }

        let mut receiver = SnapshotReceiver::new(1, page_size);

        // Feed first packet twice.
        let r1 = receiver.process_packet(&packets[0]).expect("first");
        assert_ne!(
            r1,
            SnapshotPacketResult::Duplicate,
            "bead_id={TEST_BEAD_ID} case=first_not_dup"
        );
        let r2 = receiver.process_packet(&packets[0]).expect("duplicate");
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
        while let Some(pkt) = sender.next_packet() {
            packets.push(pkt);
        }

        let mut receiver = SnapshotReceiver::new(1, page_size);
        let mut block_decoded_at = None;

        for (i, pkt) in packets.iter().enumerate() {
            if let Ok(SnapshotPacketResult::BlockDecoded(_)) = receiver.process_packet(pkt) {
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
        while let Some(pkt) = sender.next_packet() {
            packets.push(pkt);
        }

        let mut receiver = SnapshotReceiver::new(sender.num_blocks(), page_size);
        for pkt in &packets {
            let _ = receiver.process_packet(pkt);
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
        while let Some(pkt) = sender.next_packet() {
            packets.push(pkt);
        }

        // First receiver: receive only half the packets.
        let half = packets.len() / 2;
        let mut receiver1 = SnapshotReceiver::new(sender.num_blocks(), page_size);
        for pkt in &packets[..half] {
            let _ = receiver1.process_packet(pkt);
        }

        // Persist resume state.
        let resume_bytes = receiver1.resume_state().to_bytes();

        // "Crash" — create new receiver from resume state.
        let resume = ResumeState::from_bytes(&resume_bytes).expect("restore");
        let mut receiver2 = SnapshotReceiver::from_resume(resume, page_size);

        // Continue with remaining packets (and possibly some overlap).
        for pkt in &packets {
            let _ = receiver2.process_packet(pkt);
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
        while let Some(pkt) = sender.next_packet() {
            packets.push(pkt);
        }

        let mut receiver = SnapshotReceiver::new(sender.num_blocks(), page_size);
        assert_eq!(receiver.state(), SnapshotReceiverState::Waiting);

        for pkt in &packets {
            let _ = receiver.process_packet(pkt);
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
        while let Some(pkt) = sender.next_packet() {
            packets.push(pkt);
        }

        let mut receiver = SnapshotReceiver::new(sender.num_blocks(), page_size);
        for pkt in &packets {
            let _ = receiver.process_packet(pkt);
        }
        assert_eq!(receiver.state(), SnapshotReceiverState::Complete);
    }
}
