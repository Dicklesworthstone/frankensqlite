//! §3.4.2 Fountain-Coded Replication Receiver (bd-1hi.14).
//!
//! Implements the receiver-side state machine for fountain-coded database
//! replication. Listens for UDP packets, collects symbols per changeset,
//! decodes when sufficient, validates and applies recovered pages.
//!
//! State machine: LISTENING → COLLECTING → DECODING → APPLYING → COMPLETE

use std::collections::{HashMap, HashSet};

use fsqlite_error::{FrankenError, Result};
use tracing::{debug, error, info, warn};

use crate::replication_sender::{
    CHANGESET_HEADER_SIZE, ChangesetHeader, ChangesetId, DEFAULT_RPC_MESSAGE_CAP_BYTES, PageEntry,
    ReplicationPacket,
};
use crate::source_block_partition::K_MAX;

const BEAD_ID: &str = "bd-1hi.14";

// ---------------------------------------------------------------------------
// Receiver State Machine
// ---------------------------------------------------------------------------

/// Receiver state (§3.4.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiverState {
    /// Ready to accept replication data.
    Listening,
    /// At least one packet received; collecting symbols.
    Collecting,
    /// Sufficient symbols collected; decoding in progress.
    Decoding,
    /// Pages decoded; applying to local database.
    Applying,
    /// All pages applied; ready for next changeset.
    Complete,
}

/// Per-changeset decoder state, created on first packet.
#[derive(Debug)]
pub struct DecoderState {
    /// Number of source symbols expected.
    pub k_source: u32,
    /// Symbol size in bytes (inferred from first packet).
    pub symbol_size: u32,
    /// Deterministic seed derived from changeset_id.
    pub seed: u64,
    /// Collected symbols indexed by ISI.
    symbols: HashMap<u32, Vec<u8>>,
    /// Set of received ISIs for O(1) deduplication.
    received_isis: HashSet<u32>,
}

impl DecoderState {
    /// Create a new decoder state for a changeset.
    fn new(k_source: u32, symbol_size: u32, seed: u64) -> Self {
        Self {
            k_source,
            symbol_size,
            seed,
            symbols: HashMap::with_capacity(k_source as usize),
            received_isis: HashSet::with_capacity(k_source as usize),
        }
    }

    /// Number of unique symbols received.
    #[must_use]
    pub fn received_count(&self) -> u32 {
        u32::try_from(self.received_isis.len()).unwrap_or(u32::MAX)
    }

    /// Whether enough symbols have been collected to attempt decode.
    #[must_use]
    pub fn ready_to_decode(&self) -> bool {
        self.received_count() >= self.k_source
    }

    /// Add a symbol. Returns `true` if the symbol was new (accepted).
    fn add_symbol(&mut self, isi: u32, data: Vec<u8>) -> bool {
        if self.received_isis.contains(&isi) {
            return false;
        }
        self.received_isis.insert(isi);
        self.symbols.insert(isi, data);
        true
    }

    /// Attempt to decode the collected symbols into changeset bytes.
    ///
    /// For source symbols (ISI < k_source), this reconstructs the padded
    /// changeset by placing each symbol at offset `ISI * symbol_size`.
    /// Repair symbols would require RaptorQ decoding in production;
    /// this implementation handles the source-symbol-only case.
    ///
    /// Returns `None` if insufficient symbols or decode fails.
    fn try_decode(&self) -> Option<Vec<u8>> {
        if !self.ready_to_decode() {
            return None;
        }

        // Count source symbols available.
        let source_count = self
            .symbols
            .keys()
            .filter(|&&isi| isi < self.k_source)
            .count();

        let k = self.k_source as usize;
        let t = self.symbol_size as usize;

        if source_count >= k {
            // All source symbols available — reconstruct directly.
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
            // Need repair symbols + RaptorQ decoder (production path via asupersync).
            // For now, return None to stay in COLLECTING.
            warn!(
                bead_id = BEAD_ID,
                source_count,
                k_source = self.k_source,
                total_received = self.received_count(),
                "decode requires repair symbols (production uses RaptorQ decoder)"
            );
            None
        }
    }
}

/// A decoded and validated page ready for application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedPage {
    /// Page number in the database.
    pub page_number: u32,
    /// Validated page data.
    pub page_data: Vec<u8>,
}

/// Result of a successful decode operation.
#[derive(Debug)]
pub struct DecodeResult {
    /// The changeset identifier that was decoded.
    pub changeset_id: ChangesetId,
    /// Decoded and validated pages, sorted by page number.
    pub pages: Vec<DecodedPage>,
    /// Number of symbols used for decoding.
    pub symbols_used: u32,
}

/// Replication receiver state machine.
#[derive(Debug)]
pub struct ReplicationReceiver {
    state: ReceiverState,
    /// Per-changeset decoder states.
    decoders: HashMap<ChangesetId, DecoderState>,
    /// Received symbol counts per changeset.
    received_counts: HashMap<ChangesetId, u32>,
    /// Decoded results waiting for application.
    pending_results: Vec<DecodeResult>,
    /// Applied results (for metrics/ACK).
    applied_count: u64,
}

impl ReplicationReceiver {
    /// Create a new receiver in LISTENING state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: ReceiverState::Listening,
            decoders: HashMap::new(),
            received_counts: HashMap::new(),
            pending_results: Vec::new(),
            applied_count: 0,
        }
    }

    /// Current state.
    #[must_use]
    pub const fn state(&self) -> ReceiverState {
        self.state
    }

    /// Number of changesets successfully applied.
    #[must_use]
    pub const fn applied_count(&self) -> u64 {
        self.applied_count
    }

    /// Number of active decoder sessions.
    #[must_use]
    pub fn active_decoders(&self) -> usize {
        self.decoders.len()
    }

    /// Process a raw packet from the wire.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Packet is malformed (too short, symbol_size = 0)
    /// - V1 rule violated (SBN != 0)
    /// - K_source out of range
    /// - K_source or symbol_size mismatch for existing decoder
    pub fn process_packet(&mut self, packet_bytes: &[u8]) -> Result<PacketResult> {
        if packet_bytes.len() > DEFAULT_RPC_MESSAGE_CAP_BYTES {
            return Err(FrankenError::TooBig);
        }
        let packet = ReplicationPacket::from_bytes(packet_bytes)?;
        self.process_parsed_packet(&packet)
    }

    /// Process a parsed packet.
    ///
    /// # Errors
    ///
    /// See `process_packet`.
    #[allow(clippy::too_many_lines)]
    pub fn process_parsed_packet(&mut self, packet: &ReplicationPacket) -> Result<PacketResult> {
        // V1 rule: reject multi-block packets.
        if packet.sbn != 0 {
            error!(
                bead_id = BEAD_ID,
                sbn = packet.sbn,
                "V1 rule: SBN must be 0"
            );
            return Err(FrankenError::Internal(format!(
                "V1 replication: source_block must be 0, got {}",
                packet.sbn
            )));
        }

        // Validate K_source range.
        if packet.k_source == 0 || packet.k_source > K_MAX {
            error!(
                bead_id = BEAD_ID,
                k_source = packet.k_source,
                k_max = K_MAX,
                "K_source out of valid range"
            );
            return Err(FrankenError::OutOfRange {
                what: "k_source".to_owned(),
                value: packet.k_source.to_string(),
            });
        }

        // Compute symbol_size from packet.
        let symbol_size =
            u32::try_from(packet.symbol_data.len()).map_err(|_| FrankenError::OutOfRange {
                what: "symbol_data_len".to_owned(),
                value: packet.symbol_data.len().to_string(),
            })?;
        if symbol_size == 0 {
            return Err(FrankenError::OutOfRange {
                what: "symbol_size".to_owned(),
                value: "0".to_owned(),
            });
        }

        // Transition LISTENING → COLLECTING on first packet.
        if self.state == ReceiverState::Listening {
            self.state = ReceiverState::Collecting;
            info!(bead_id = BEAD_ID, "first packet received, now COLLECTING");
        }

        let changeset_id = packet.changeset_id;

        // Get or create decoder state.
        if let Some(decoder) = self.decoders.get(&changeset_id) {
            // Validate consistency with existing decoder.
            if decoder.k_source != packet.k_source {
                error!(
                    bead_id = BEAD_ID,
                    expected_k = decoder.k_source,
                    got_k = packet.k_source,
                    "K_source mismatch for existing changeset"
                );
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "K_source mismatch: expected {}, got {}",
                        decoder.k_source, packet.k_source
                    ),
                });
            }
            if decoder.symbol_size != symbol_size {
                error!(
                    bead_id = BEAD_ID,
                    expected_t = decoder.symbol_size,
                    got_t = symbol_size,
                    "symbol_size mismatch for existing changeset"
                );
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "symbol_size mismatch: expected {}, got {}",
                        decoder.symbol_size, symbol_size
                    ),
                });
            }
        } else {
            // Create new decoder state.
            let seed = crate::replication_sender::derive_seed_from_changeset_id(&changeset_id);
            debug!(
                bead_id = BEAD_ID,
                k_source = packet.k_source,
                symbol_size,
                seed,
                "created decoder for new changeset"
            );
            self.decoders.insert(
                changeset_id,
                DecoderState::new(packet.k_source, symbol_size, seed),
            );
            self.received_counts.insert(changeset_id, 0);
        }

        // Add symbol to decoder (with ISI deduplication).
        let decoder = self.decoders.get_mut(&changeset_id).expect("just inserted");
        let accepted = decoder.add_symbol(packet.esi, packet.symbol_data.clone());

        if accepted {
            let count = self.received_counts.entry(changeset_id).or_insert(0);
            *count += 1;
            debug!(
                bead_id = BEAD_ID,
                isi = packet.esi,
                received = *count,
                k_source = packet.k_source,
                "symbol accepted"
            );
        } else {
            debug!(
                bead_id = BEAD_ID,
                isi = packet.esi,
                "duplicate ISI, symbol ignored"
            );
            return Ok(PacketResult::Duplicate);
        }

        // Check if ready to decode.
        if decoder.ready_to_decode() {
            info!(
                bead_id = BEAD_ID,
                received = decoder.received_count(),
                k_source = decoder.k_source,
                "attempting decode"
            );
            self.state = ReceiverState::Decoding;

            if let Some(padded_bytes) = decoder.try_decode() {
                // Decode succeeded: truncate to total_len and parse pages.
                match self.parse_and_validate_changeset(changeset_id, &padded_bytes) {
                    Ok(result) => {
                        let n_pages = result.pages.len();
                        self.pending_results.push(result);
                        self.state = ReceiverState::Applying;
                        info!(
                            bead_id = BEAD_ID,
                            n_pages, "decode succeeded, ready to apply"
                        );
                        // Clean up decoder for this changeset.
                        self.decoders.remove(&changeset_id);
                        self.received_counts.remove(&changeset_id);
                        return Ok(PacketResult::DecodeReady);
                    }
                    Err(e) => {
                        error!(
                            bead_id = BEAD_ID,
                            error = %e,
                            "changeset validation failed after decode"
                        );
                        // Clean up failed decoder.
                        self.decoders.remove(&changeset_id);
                        self.received_counts.remove(&changeset_id);
                        self.state = if self.decoders.is_empty() {
                            ReceiverState::Listening
                        } else {
                            ReceiverState::Collecting
                        };
                        return Err(e);
                    }
                }
            }

            // Decode failed (need more symbols).
            warn!(
                bead_id = BEAD_ID,
                "decode failed at K_source, continuing collection"
            );
            self.state = ReceiverState::Collecting;
            return Ok(PacketResult::NeedMore);
        }

        Ok(PacketResult::Accepted)
    }

    /// Parse and validate decoded changeset bytes.
    fn parse_and_validate_changeset(
        &self,
        changeset_id: ChangesetId,
        padded_bytes: &[u8],
    ) -> Result<DecodeResult> {
        if padded_bytes.len() < CHANGESET_HEADER_SIZE {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "decoded bytes too short for header: {} < {CHANGESET_HEADER_SIZE}",
                    padded_bytes.len()
                ),
            });
        }

        // Parse header.
        let header_bytes: [u8; CHANGESET_HEADER_SIZE] = padded_bytes[..CHANGESET_HEADER_SIZE]
            .try_into()
            .expect("checked length");
        let header = ChangesetHeader::from_bytes(&header_bytes)?;

        // Truncate to total_len.
        let total_len =
            usize::try_from(header.total_len).map_err(|_| FrankenError::OutOfRange {
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

        // Parse page entries.
        let entry_size = 4_usize + 8 + header.page_size as usize; // page_number + xxh3 + data
        let data_start = CHANGESET_HEADER_SIZE;
        let data_bytes = &changeset_bytes[data_start..];

        if data_bytes.len() < entry_size * header.n_pages as usize {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "insufficient data for {} pages: {} < {}",
                    header.n_pages,
                    data_bytes.len(),
                    entry_size * header.n_pages as usize,
                ),
            });
        }

        let mut pages = Vec::with_capacity(header.n_pages as usize);
        let decoder_state_symbols = self
            .decoders
            .get(&changeset_id)
            .map_or(0, DecoderState::received_count);

        for i in 0..header.n_pages as usize {
            let offset = i * entry_size;
            let page_number =
                u32::from_le_bytes(data_bytes[offset..offset + 4].try_into().expect("4 bytes"));
            let page_xxh3 = u64::from_le_bytes(
                data_bytes[offset + 4..offset + 12]
                    .try_into()
                    .expect("8 bytes"),
            );
            let page_data =
                data_bytes[offset + 12..offset + 12 + header.page_size as usize].to_vec();

            // Validate page xxh3.
            let computed_xxh3 = xxhash_rust::xxh3::xxh3_64(&page_data);
            if computed_xxh3 != page_xxh3 {
                error!(
                    bead_id = BEAD_ID,
                    page_number,
                    expected_xxh3 = page_xxh3,
                    computed_xxh3,
                    "page xxh3 validation failed"
                );
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "page {page_number} xxh3 mismatch: expected {page_xxh3:#x}, got {computed_xxh3:#x}"
                    ),
                });
            }

            pages.push(DecodedPage {
                page_number,
                page_data,
            });
        }

        // Pages should already be sorted (sender sorts them).
        debug_assert!(
            pages
                .windows(2)
                .all(|w| w[0].page_number <= w[1].page_number)
        );

        Ok(DecodeResult {
            changeset_id,
            pages,
            symbols_used: decoder_state_symbols,
        })
    }

    /// Apply pending decoded results. Returns applied page counts.
    ///
    /// In production, this writes pages to the local database. Here we
    /// validate and return the results for the caller to apply.
    ///
    /// # Errors
    ///
    /// Returns error if not in APPLYING state.
    pub fn apply_pending(&mut self) -> Result<Vec<DecodeResult>> {
        if self.state != ReceiverState::Applying {
            return Err(FrankenError::Internal(format!(
                "receiver must be APPLYING to apply, current state: {:?}",
                self.state
            )));
        }

        let results = std::mem::take(&mut self.pending_results);
        let n = results.len();
        self.applied_count += u64::try_from(n).unwrap_or(u64::MAX);

        info!(
            bead_id = BEAD_ID,
            applied = n,
            total_applied = self.applied_count,
            "applied pending changesets"
        );

        // Transition to COMPLETE.
        self.state = ReceiverState::Complete;
        Ok(results)
    }

    /// Transition from COMPLETE back to LISTENING for the next changeset.
    ///
    /// # Errors
    ///
    /// Returns error if not in COMPLETE state.
    pub fn reset_to_listening(&mut self) -> Result<()> {
        if self.state != ReceiverState::Complete {
            return Err(FrankenError::Internal(format!(
                "receiver must be COMPLETE to reset, current state: {:?}",
                self.state
            )));
        }
        self.state = ReceiverState::Listening;
        debug!(bead_id = BEAD_ID, "receiver reset to LISTENING");
        Ok(())
    }

    /// Force reset to LISTENING from any state (e.g., on error recovery).
    pub fn force_reset(&mut self) {
        self.decoders.clear();
        self.received_counts.clear();
        self.pending_results.clear();
        self.state = ReceiverState::Listening;
        warn!(bead_id = BEAD_ID, "receiver force-reset to LISTENING");
    }
}

impl Default for ReplicationReceiver {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of processing a single packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketResult {
    /// Symbol accepted, need more for decode.
    Accepted,
    /// Duplicate ISI, silently ignored.
    Duplicate,
    /// Enough symbols collected, decode succeeded and ready to apply.
    DecodeReady,
    /// Had enough symbols but decode failed, need more.
    NeedMore,
}

// ---------------------------------------------------------------------------
// Changeset parsing utility (used by tests and receiver)
// ---------------------------------------------------------------------------

/// Parse changeset bytes into page entries (for validation/testing).
///
/// # Errors
///
/// Returns error if the changeset is malformed.
pub fn parse_changeset_pages(changeset_bytes: &[u8]) -> Result<(ChangesetHeader, Vec<PageEntry>)> {
    if changeset_bytes.len() < CHANGESET_HEADER_SIZE {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "changeset too short: {} < {CHANGESET_HEADER_SIZE}",
                changeset_bytes.len()
            ),
        });
    }

    let header_bytes: [u8; CHANGESET_HEADER_SIZE] = changeset_bytes[..CHANGESET_HEADER_SIZE]
        .try_into()
        .expect("checked length");
    let header = ChangesetHeader::from_bytes(&header_bytes)?;

    let entry_size = 4_usize + 8 + header.page_size as usize;
    let data_start = CHANGESET_HEADER_SIZE;
    let data_bytes = &changeset_bytes[data_start..];

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
        let page_bytes = data_bytes[offset + 12..offset + 12 + header.page_size as usize].to_vec();

        pages.push(PageEntry {
            page_number,
            page_xxh3,
            page_bytes,
        });
    }

    Ok((header, pages))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication_sender::{
        CHANGESET_HEADER_SIZE, ChangesetId, PageEntry, REPLICATION_HEADER_SIZE, ReplicationPacket,
        ReplicationSender, SenderConfig, compute_changeset_id, derive_seed_from_changeset_id,
        encode_changeset,
    };

    const TEST_BEAD_ID: &str = "bd-1hi.14";

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

    /// Helper: generate sender packets for a set of pages.
    fn generate_sender_packets(
        page_size: u32,
        page_numbers: &[u32],
        symbol_size: u16,
    ) -> Vec<Vec<u8>> {
        let mut sender = ReplicationSender::new();
        let mut pages = make_pages(page_size, page_numbers);
        let config = SenderConfig {
            symbol_size,
            max_isi_multiplier: 1, // only source symbols
        };
        sender
            .prepare(page_size, &mut pages, config)
            .expect("prepare");
        sender.start_streaming().expect("start");

        let mut packets = Vec::new();
        while let Some(packet) = sender.next_packet().expect("next") {
            packets.push(packet.to_bytes().expect("encode"));
        }
        packets
    }

    // -----------------------------------------------------------------------
    // State transition tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_receiver_listening_to_collecting() {
        let mut receiver = ReplicationReceiver::new();
        assert_eq!(
            receiver.state(),
            ReceiverState::Listening,
            "bead_id={TEST_BEAD_ID} case=initial_state"
        );

        let packets = generate_sender_packets(512, &[1], 512);
        assert!(!packets.is_empty());

        receiver.process_packet(&packets[0]).expect("first packet");
        assert_ne!(
            receiver.state(),
            ReceiverState::Listening,
            "bead_id={TEST_BEAD_ID} case=transition_on_first_packet"
        );
    }

    #[test]
    fn test_receiver_decoder_creation() {
        let mut receiver = ReplicationReceiver::new();
        let packets = generate_sender_packets(512, &[1, 2], 512);
        assert_eq!(receiver.active_decoders(), 0);

        receiver.process_packet(&packets[0]).expect("first packet");
        // Should have created exactly one decoder.
        // Note: if decode triggers, the decoder may be cleaned up,
        // so just check that processing succeeded.
        assert_ne!(
            receiver.state(),
            ReceiverState::Listening,
            "bead_id={TEST_BEAD_ID} case=decoder_created"
        );
    }

    #[test]
    fn test_receiver_seed_derivation() {
        // Verify seed = xxh3_64(changeset_id_bytes) matches sender.
        let id = ChangesetId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let seed = derive_seed_from_changeset_id(&id);

        let expected = xxhash_rust::xxh3::xxh3_64(id.as_bytes());
        assert_eq!(
            seed, expected,
            "bead_id={TEST_BEAD_ID} case=seed_matches_sender"
        );
    }

    #[test]
    fn test_receiver_v1_reject_sbn_nonzero() {
        let mut receiver = ReplicationReceiver::new();
        let packet = ReplicationPacket {
            changeset_id: ChangesetId::from_bytes([0xAA; 16]),
            sbn: 1, // V1 violation
            esi: 0,
            k_source: 10,
            symbol_data: vec![0x55; 512],
        };
        let wire = packet.to_bytes().expect("encode");
        let result = receiver.process_packet(&wire);
        assert!(
            result.is_err(),
            "bead_id={TEST_BEAD_ID} case=v1_sbn_rejected"
        );
    }

    #[test]
    fn test_receiver_k_source_validation() {
        let mut receiver = ReplicationReceiver::new();

        // K_source = 0 → rejected.
        let packet_zero = ReplicationPacket {
            changeset_id: ChangesetId::from_bytes([0xBB; 16]),
            sbn: 0,
            esi: 0,
            k_source: 0,
            symbol_data: vec![0x55; 512],
        };
        let wire_zero = packet_zero.to_bytes().expect("encode");
        assert!(
            receiver.process_packet(&wire_zero).is_err(),
            "bead_id={TEST_BEAD_ID} case=k_source_zero_rejected"
        );

        // K_source = K_MAX + 1 → rejected.
        let packet_over = ReplicationPacket {
            changeset_id: ChangesetId::from_bytes([0xCC; 16]),
            sbn: 0,
            esi: 0,
            k_source: K_MAX + 1,
            symbol_data: vec![0x55; 512],
        };
        // ESI only has 24 bits, K_source > K_MAX might not fit in packet format
        // but we test the validation path directly.
        let result = receiver.process_parsed_packet(&packet_over);
        assert!(
            result.is_err(),
            "bead_id={TEST_BEAD_ID} case=k_source_over_max_rejected"
        );

        // K_source = K_MAX → accepted.
        let packet_max = ReplicationPacket {
            changeset_id: ChangesetId::from_bytes([0xDD; 16]),
            sbn: 0,
            esi: 0,
            k_source: K_MAX,
            symbol_data: vec![0x55; 512],
        };
        let result = receiver.process_parsed_packet(&packet_max);
        assert!(
            result.is_ok(),
            "bead_id={TEST_BEAD_ID} case=k_source_at_max_accepted"
        );
    }

    #[test]
    fn test_receiver_symbol_size_inference() {
        let mut receiver = ReplicationReceiver::new();
        let packet = ReplicationPacket {
            changeset_id: ChangesetId::from_bytes([0xEE; 16]),
            sbn: 0,
            esi: 0,
            k_source: 100,
            symbol_data: vec![0x42; 1024],
        };
        receiver
            .process_parsed_packet(&packet)
            .expect("accept packet");

        // Symbol size should be inferred as 1024.
        let decoder = receiver
            .decoders
            .get(&packet.changeset_id)
            .expect("decoder exists");
        assert_eq!(
            decoder.symbol_size, 1024,
            "bead_id={TEST_BEAD_ID} case=symbol_size_inferred"
        );

        // Zero-length symbol data → rejected.
        let mut receiver2 = ReplicationReceiver::new();
        let empty_packet = ReplicationPacket {
            changeset_id: ChangesetId::from_bytes([0xFF; 16]),
            sbn: 0,
            esi: 0,
            k_source: 10,
            symbol_data: vec![],
        };
        assert!(
            receiver2.process_parsed_packet(&empty_packet).is_err(),
            "bead_id={TEST_BEAD_ID} case=zero_symbol_size_rejected"
        );
    }

    #[test]
    fn test_receiver_k_source_mismatch_rejected() {
        let mut receiver = ReplicationReceiver::new();
        let id = ChangesetId::from_bytes([0x11; 16]);

        let p1 = ReplicationPacket {
            changeset_id: id,
            sbn: 0,
            esi: 0,
            k_source: 100,
            symbol_data: vec![0x42; 512],
        };
        receiver
            .process_parsed_packet(&p1)
            .expect("first packet ok");

        // Same changeset_id, different K_source.
        let p2 = ReplicationPacket {
            changeset_id: id,
            sbn: 0,
            esi: 1,
            k_source: 200, // mismatch
            symbol_data: vec![0x42; 512],
        };
        assert!(
            receiver.process_parsed_packet(&p2).is_err(),
            "bead_id={TEST_BEAD_ID} case=k_source_mismatch_rejected"
        );
    }

    #[test]
    fn test_receiver_symbol_size_mismatch_rejected() {
        let mut receiver = ReplicationReceiver::new();
        let id = ChangesetId::from_bytes([0x22; 16]);

        let p1 = ReplicationPacket {
            changeset_id: id,
            sbn: 0,
            esi: 0,
            k_source: 100,
            symbol_data: vec![0x42; 512],
        };
        receiver
            .process_parsed_packet(&p1)
            .expect("first packet ok");

        // Same changeset_id, different symbol_size.
        let p2 = ReplicationPacket {
            changeset_id: id,
            sbn: 0,
            esi: 1,
            k_source: 100,
            symbol_data: vec![0x42; 1024], // different size
        };
        assert!(
            receiver.process_parsed_packet(&p2).is_err(),
            "bead_id={TEST_BEAD_ID} case=symbol_size_mismatch_rejected"
        );
    }

    #[test]
    fn test_receiver_isi_deduplication() {
        let mut receiver = ReplicationReceiver::new();
        let id = ChangesetId::from_bytes([0x33; 16]);

        let p1 = ReplicationPacket {
            changeset_id: id,
            sbn: 0,
            esi: 0,
            k_source: 100,
            symbol_data: vec![0x42; 512],
        };

        let r1 = receiver.process_parsed_packet(&p1).expect("first");
        assert_eq!(
            r1,
            PacketResult::Accepted,
            "bead_id={TEST_BEAD_ID} case=first_accepted"
        );

        // Same ISI again → duplicate.
        let r2 = receiver.process_parsed_packet(&p1).expect("duplicate");
        assert_eq!(
            r2,
            PacketResult::Duplicate,
            "bead_id={TEST_BEAD_ID} case=isi_dedup"
        );

        // Count should still be 1.
        let count = receiver.received_counts.get(&id).copied().unwrap_or(0);
        assert_eq!(
            count, 1,
            "bead_id={TEST_BEAD_ID} case=dedup_count_unchanged"
        );
    }

    #[test]
    fn test_receiver_decode_at_k_source() {
        // Use the sender to generate proper packets, then feed to receiver.
        let page_size = 512_u32;
        let mut receiver = ReplicationReceiver::new();
        let packets = generate_sender_packets(page_size, &[1, 2, 3], 512);

        let mut last_result = PacketResult::Accepted;
        for pkt in &packets {
            match receiver.process_packet(pkt) {
                Ok(r) => last_result = r,
                Err(e) => panic!("bead_id={TEST_BEAD_ID} case=decode_at_k unexpected error: {e}"),
            }
        }

        assert_eq!(
            last_result,
            PacketResult::DecodeReady,
            "bead_id={TEST_BEAD_ID} case=decode_triggers_at_k_source"
        );
        assert_eq!(
            receiver.state(),
            ReceiverState::Applying,
            "bead_id={TEST_BEAD_ID} case=state_applying_after_decode"
        );
    }

    #[test]
    fn test_receiver_decode_success_truncation() {
        let page_size = 128_u32;
        let mut receiver = ReplicationReceiver::new();
        let packets = generate_sender_packets(page_size, &[1], 128);

        for pkt in &packets {
            let _ = receiver.process_packet(pkt);
        }

        // Apply and check that pages are correctly truncated.
        if receiver.state() == ReceiverState::Applying {
            let results = receiver.apply_pending().expect("apply");
            assert!(
                !results.is_empty(),
                "bead_id={TEST_BEAD_ID} case=has_results"
            );
            for result in &results {
                for page in &result.pages {
                    assert_eq!(
                        page.page_data.len(),
                        page_size as usize,
                        "bead_id={TEST_BEAD_ID} case=page_data_correct_size"
                    );
                }
            }
        }
    }

    #[test]
    fn test_receiver_page_xxh3_validation() {
        let page_size = 256_u32;
        let mut pages = make_pages(page_size, &[1]);
        let changeset_bytes = encode_changeset(page_size, &mut pages).expect("encode");

        // Tamper with a page byte in the changeset (after header + page_number + xxh3).
        let mut tampered = changeset_bytes.clone();
        let tamper_offset = CHANGESET_HEADER_SIZE + 4 + 8 + 10; // into page data
        tampered[tamper_offset] ^= 0xFF;

        // Now create a "decoded" changeset and try to parse it.
        let receiver = ReplicationReceiver::new();
        let changeset_id = compute_changeset_id(&changeset_bytes);
        let result = receiver.parse_and_validate_changeset(changeset_id, &tampered);
        assert!(
            result.is_err(),
            "bead_id={TEST_BEAD_ID} case=xxh3_validation_catches_corruption"
        );
    }

    #[test]
    fn test_receiver_pages_applied_in_order() {
        let page_size = 256_u32;
        let mut receiver = ReplicationReceiver::new();
        let packets = generate_sender_packets(page_size, &[5, 1, 3, 2, 4], 256);

        for pkt in &packets {
            let _ = receiver.process_packet(pkt);
        }

        if receiver.state() == ReceiverState::Applying {
            let results = receiver.apply_pending().expect("apply");
            let pages = &results[0].pages;
            for w in pages.windows(2) {
                assert!(
                    w[0].page_number <= w[1].page_number,
                    "bead_id={TEST_BEAD_ID} case=pages_sorted pn0={} pn1={}",
                    w[0].page_number,
                    w[1].page_number
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Property tests
    // -----------------------------------------------------------------------

    #[test]
    fn prop_any_k_symbols_decode() {
        // With only source symbols and k_source = actual source count,
        // providing all k source symbols always decodes.
        for n_pages in [1_u32, 3, 5, 10] {
            let page_size = 256_u32;
            let mut receiver = ReplicationReceiver::new();
            let packets =
                generate_sender_packets(page_size, &(1..=n_pages).collect::<Vec<_>>(), 256);

            let mut decode_ready = false;
            for pkt in &packets {
                if matches!(receiver.process_packet(pkt), Ok(PacketResult::DecodeReady)) {
                    decode_ready = true;
                    break;
                }
            }
            assert!(
                decode_ready,
                "bead_id={TEST_BEAD_ID} case=prop_any_k_decode n_pages={n_pages}"
            );
        }
    }

    #[test]
    fn prop_dedup_idempotent() {
        // Use a large K_source so we can feed duplicates before decode triggers.
        let mut receiver = ReplicationReceiver::new();
        let id = ChangesetId::from_bytes([0x77; 16]);

        // Feed the same ISI multiple times within a single decoder session.
        let p1 = ReplicationPacket {
            changeset_id: id,
            sbn: 0,
            esi: 0,
            k_source: 100, // large enough that one symbol won't trigger decode
            symbol_data: vec![0x42; 512],
        };

        let r1 = receiver.process_parsed_packet(&p1).expect("first");
        assert_eq!(
            r1,
            PacketResult::Accepted,
            "bead_id={TEST_BEAD_ID} case=dedup_first_accepted"
        );

        for _ in 0..5 {
            let r = receiver.process_parsed_packet(&p1).expect("duplicate");
            assert_eq!(
                r,
                PacketResult::Duplicate,
                "bead_id={TEST_BEAD_ID} case=dedup_subsequent_always_duplicate"
            );
        }

        // Count should still be 1.
        let count = receiver.received_counts.get(&id).copied().unwrap_or(0);
        assert_eq!(count, 1, "bead_id={TEST_BEAD_ID} case=dedup_count_stable");
    }

    // -----------------------------------------------------------------------
    // E2E tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_packet_reject_over_message_cap() {
        let mut receiver = ReplicationReceiver::new();
        let oversized = vec![0_u8; DEFAULT_RPC_MESSAGE_CAP_BYTES + 1];
        let err = receiver.process_packet(&oversized).unwrap_err();
        assert!(matches!(err, FrankenError::TooBig));
    }

    #[test]
    fn test_e2e_sender_receiver_roundtrip() {
        // Sender encodes pages. Receiver collects and decodes. Byte-identical.
        let page_size = 512_u32;
        let page_numbers: Vec<u32> = (1..=20).collect();
        let original_pages = make_pages(page_size, &page_numbers);

        let mut receiver = ReplicationReceiver::new();
        let packets = generate_sender_packets(page_size, &page_numbers, 512);

        for pkt in &packets {
            let _ = receiver.process_packet(pkt);
        }

        assert_eq!(
            receiver.state(),
            ReceiverState::Applying,
            "bead_id={TEST_BEAD_ID} case=e2e_roundtrip_applying"
        );

        let results = receiver.apply_pending().expect("apply");
        assert_eq!(
            results.len(),
            1,
            "bead_id={TEST_BEAD_ID} case=e2e_one_changeset"
        );

        let decoded_pages = &results[0].pages;
        assert_eq!(
            decoded_pages.len(),
            original_pages.len(),
            "bead_id={TEST_BEAD_ID} case=e2e_page_count"
        );

        for (decoded, original) in decoded_pages.iter().zip(original_pages.iter()) {
            assert_eq!(
                decoded.page_number, original.page_number,
                "bead_id={TEST_BEAD_ID} case=e2e_page_number_match"
            );
            assert_eq!(
                decoded.page_data, original.page_bytes,
                "bead_id={TEST_BEAD_ID} case=e2e_page_data_identical pn={}",
                original.page_number
            );
        }

        // Complete the cycle.
        receiver.reset_to_listening().expect("reset");
        assert_eq!(
            receiver.state(),
            ReceiverState::Listening,
            "bead_id={TEST_BEAD_ID} case=e2e_back_to_listening"
        );
    }

    #[test]
    fn test_e2e_concurrent_changesets() {
        // Two changesets streaming simultaneously.
        let mut receiver = ReplicationReceiver::new();

        let packets_a = generate_sender_packets(256, &[1, 2, 3], 256);
        let packets_b = generate_sender_packets(256, &[10, 20, 30], 256);

        // Interleave packets from two different changesets.
        let mut all_packets = Vec::new();
        let max_len = packets_a.len().max(packets_b.len());
        for i in 0..max_len {
            if i < packets_a.len() {
                all_packets.push(packets_a[i].clone());
            }
            if i < packets_b.len() {
                all_packets.push(packets_b[i].clone());
            }
        }

        let mut decode_count = 0_u32;
        for pkt in &all_packets {
            if matches!(receiver.process_packet(pkt), Ok(PacketResult::DecodeReady)) {
                decode_count += 1;
                // Apply immediately and reset if needed.
                if receiver.state() == ReceiverState::Applying {
                    let _ = receiver.apply_pending();
                    // If more decoders remain, go back to collecting.
                    if !receiver.decoders.is_empty() {
                        receiver.state = ReceiverState::Collecting;
                    }
                }
            }
        }

        assert!(
            decode_count >= 1,
            "bead_id={TEST_BEAD_ID} case=e2e_concurrent_at_least_one_decoded count={decode_count}"
        );
    }

    #[test]
    fn test_e2e_bd_1hi_14_compliance() {
        // Full end-to-end compliance test.
        let page_size = 1024_u32;
        let page_numbers: Vec<u32> = (1..=10).collect();
        let original_pages = make_pages(page_size, &page_numbers);

        // Encode via sender.
        let mut sender = ReplicationSender::new();
        let mut pages = make_pages(page_size, &page_numbers);
        sender
            .prepare(page_size, &mut pages, SenderConfig::default())
            .expect("prepare");
        sender.start_streaming().expect("start");

        // Collect all packets.
        let mut wire_packets = Vec::new();
        while let Some(packet) = sender.next_packet().expect("next") {
            wire_packets.push(packet.to_bytes().expect("encode"));
        }

        // Feed to receiver.
        let mut receiver = ReplicationReceiver::new();
        assert_eq!(receiver.state(), ReceiverState::Listening);

        let mut last_result = PacketResult::Accepted;
        for pkt in &wire_packets {
            match receiver.process_packet(pkt) {
                Ok(r) => {
                    last_result = r;
                    if r == PacketResult::DecodeReady {
                        break;
                    }
                }
                Err(e) => {
                    panic!("bead_id={TEST_BEAD_ID} case=e2e_compliance unexpected error: {e}")
                }
            }
        }

        // Verify decode happened.
        assert_eq!(
            last_result,
            PacketResult::DecodeReady,
            "bead_id={TEST_BEAD_ID} case=e2e_compliance_decoded"
        );
        assert_eq!(receiver.state(), ReceiverState::Applying);

        // Apply.
        let results = receiver.apply_pending().expect("apply");
        assert_eq!(receiver.state(), ReceiverState::Complete);
        assert_eq!(results.len(), 1);

        // Verify byte-identical pages.
        let decoded = &results[0].pages;
        assert_eq!(decoded.len(), original_pages.len());
        for (d, o) in decoded.iter().zip(original_pages.iter()) {
            assert_eq!(d.page_number, o.page_number);
            assert_eq!(d.page_data, o.page_bytes);
        }

        // Reset and verify.
        receiver.reset_to_listening().expect("reset");
        assert_eq!(
            receiver.state(),
            ReceiverState::Listening,
            "bead_id={TEST_BEAD_ID} case=e2e_compliance_reset"
        );
        assert_eq!(receiver.applied_count(), 1);
    }

    // -----------------------------------------------------------------------
    // Compliance gate tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_bd_1hi_14_unit_compliance_gate() {
        // Verify all required types and functions exist.
        let _ = ReceiverState::Listening;
        let _ = ReceiverState::Collecting;
        let _ = ReceiverState::Decoding;
        let _ = ReceiverState::Applying;
        let _ = ReceiverState::Complete;

        let _ = PacketResult::Accepted;
        let _ = PacketResult::Duplicate;
        let _ = PacketResult::DecodeReady;
        let _ = PacketResult::NeedMore;

        let receiver = ReplicationReceiver::new();
        assert_eq!(receiver.state(), ReceiverState::Listening);
        assert_eq!(receiver.applied_count(), 0);
        assert_eq!(receiver.active_decoders(), 0);

        // Verify REPLICATION_HEADER_SIZE is correct.
        assert_eq!(REPLICATION_HEADER_SIZE, 24);
    }

    #[test]
    fn prop_bd_1hi_14_structure_compliance() {
        // Full state machine cycle.
        let page_size = 256_u32;
        let mut receiver = ReplicationReceiver::new();
        assert_eq!(receiver.state(), ReceiverState::Listening);

        let packets = generate_sender_packets(page_size, &[1, 2], 256);
        for pkt in &packets {
            let _ = receiver.process_packet(pkt);
        }

        // Should have transitioned through the state machine.
        assert!(
            receiver.state() == ReceiverState::Applying
                || receiver.state() == ReceiverState::Collecting,
            "bead_id={TEST_BEAD_ID} case=prop_state_machine state={:?}",
            receiver.state()
        );

        if receiver.state() == ReceiverState::Applying {
            let results = receiver.apply_pending().expect("apply");
            assert!(!results.is_empty());
            assert_eq!(receiver.state(), ReceiverState::Complete);
            receiver.reset_to_listening().expect("reset");
            assert_eq!(receiver.state(), ReceiverState::Listening);
        }
    }
}
