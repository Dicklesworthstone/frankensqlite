//! §3.4.2 Fountain-Coded Replication Receiver (bd-1hi.14).
//!
//! Implements the receiver-side state machine for fountain-coded database
//! replication. Listens for UDP packets, collects symbols per changeset,
//! decodes when sufficient, validates and applies recovered pages.
//!
//! State machine: LISTENING → COLLECTING → DECODING → APPLYING → COMPLETE

use std::collections::{HashMap, HashSet};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::ObjectId;
use tracing::{debug, error, info, warn};

use crate::decode_proofs::{DecodeAuditEntry, EcsDecodeProof};
use crate::replication_sender::{
    CHANGESET_HEADER_SIZE, ChangesetHeader, ChangesetId, DEFAULT_RPC_MESSAGE_CAP_BYTES, PageEntry,
    ReplicationPacket, ReplicationWireVersion, compute_changeset_id,
};
use crate::source_block_partition::K_MAX;

const BEAD_ID: &str = "bd-1hi.14";
const DEFAULT_MAX_INFLIGHT_DECODERS: usize = 128;
const DEFAULT_MAX_BUFFERED_SYMBOL_BYTES: usize = 64 * 1024 * 1024;

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

    /// Number of collected source symbols (`isi < k_source`).
    #[must_use]
    pub fn source_symbol_count(&self) -> u32 {
        let count = self
            .symbols
            .keys()
            .filter(|&&isi| isi < self.k_source)
            .count();
        u32::try_from(count).unwrap_or(u32::MAX)
    }

    /// Whether any collected symbol is a repair symbol (`isi >= k_source`).
    #[must_use]
    pub fn has_repair_symbols(&self) -> bool {
        self.symbols.keys().any(|&isi| isi >= self.k_source)
    }

    /// Sorted unique ISIs of all collected symbols.
    #[must_use]
    pub fn sorted_isis(&self) -> Vec<u32> {
        let mut isis: Vec<u32> = self.symbols.keys().copied().collect();
        isis.sort_unstable();
        isis.dedup();
        isis
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

    #[must_use]
    fn has_symbol(&self, isi: u32) -> bool {
        self.received_isis.contains(&isi)
    }

    #[must_use]
    fn buffered_bytes(&self) -> usize {
        self.symbols.values().map(Vec::len).sum()
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
        let source_count = usize::try_from(self.source_symbol_count()).unwrap_or(usize::MAX);

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
    /// Optional decode proof emitted under policy control.
    pub decode_proof: Option<EcsDecodeProof>,
}

#[derive(Debug, Clone, Copy)]
struct DecodeProofBuildInput<'a> {
    changeset_id: ChangesetId,
    k_source: u32,
    symbol_size: u32,
    seed: u64,
    received_isis: &'a [u32],
    decode_success: bool,
    intermediate_rank: Option<u32>,
    symbols_used: u32,
}

/// Replication receiver state machine.
#[derive(Debug)]
pub struct ReplicationReceiver {
    config: ReceiverConfig,
    state: ReceiverState,
    /// Per-changeset decoder states.
    decoders: HashMap<ChangesetId, DecoderState>,
    /// Received symbol counts per changeset.
    received_counts: HashMap<ChangesetId, u32>,
    /// Total bytes currently buffered across all decoder symbol sets.
    buffered_symbol_bytes: usize,
    /// Decoded results waiting for application.
    pending_results: Vec<DecodeResult>,
    /// Applied results (for metrics/ACK).
    applied_count: u64,
    /// Decode-proof audit entries emitted by this receiver.
    decode_audit: Vec<DecodeAuditEntry>,
    /// Monotonic audit sequence.
    decode_audit_seq: u64,
}

/// Receiver policy knobs for packet integrity/auth enforcement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeProofEmissionPolicy {
    /// Emit proofs on decode failure (durability-critical requirement).
    pub emit_on_decode_failure: bool,
    /// Emit proofs on successful decode that included repair symbols.
    pub emit_on_repair_success: bool,
}

impl DecodeProofEmissionPolicy {
    /// Default production posture: disabled.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            emit_on_decode_failure: false,
            emit_on_repair_success: false,
        }
    }

    /// Durability-critical posture for replication apply paths.
    #[must_use]
    pub const fn durability_critical() -> Self {
        Self {
            emit_on_decode_failure: true,
            emit_on_repair_success: true,
        }
    }
}

impl Default for DecodeProofEmissionPolicy {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Receiver policy knobs for packet integrity/auth enforcement.
#[derive(Debug, Clone)]
pub struct ReceiverConfig {
    /// Optional auth key for validating packet auth tags.
    pub auth_key: Option<[u8; 32]>,
    /// Decode proof emission hooks.
    pub decode_proof_policy: DecodeProofEmissionPolicy,
    /// Maximum number of concurrent in-flight changeset decoders.
    pub max_inflight_decoders: usize,
    /// Maximum total bytes buffered across all decoder symbol maps.
    pub max_buffered_symbol_bytes: usize,
}

impl ReceiverConfig {
    /// Build a receiver config with authenticated transport enabled.
    #[must_use]
    pub const fn with_auth_key(auth_key: [u8; 32]) -> Self {
        Self {
            auth_key: Some(auth_key),
            decode_proof_policy: DecodeProofEmissionPolicy::disabled(),
            max_inflight_decoders: DEFAULT_MAX_INFLIGHT_DECODERS,
            max_buffered_symbol_bytes: DEFAULT_MAX_BUFFERED_SYMBOL_BYTES,
        }
    }
}

impl Default for ReceiverConfig {
    fn default() -> Self {
        Self {
            auth_key: None,
            decode_proof_policy: DecodeProofEmissionPolicy::disabled(),
            max_inflight_decoders: DEFAULT_MAX_INFLIGHT_DECODERS,
            max_buffered_symbol_bytes: DEFAULT_MAX_BUFFERED_SYMBOL_BYTES,
        }
    }
}

impl ReplicationReceiver {
    fn remove_decoder(&mut self, changeset_id: ChangesetId) {
        if let Some(decoder) = self.decoders.remove(&changeset_id) {
            self.buffered_symbol_bytes = self
                .buffered_symbol_bytes
                .saturating_sub(decoder.buffered_bytes());
        }
        self.received_counts.remove(&changeset_id);
    }

    /// Create a new receiver with explicit configuration.
    #[must_use]
    pub fn with_config(config: ReceiverConfig) -> Self {
        Self {
            config,
            state: ReceiverState::Listening,
            decoders: HashMap::new(),
            received_counts: HashMap::new(),
            buffered_symbol_bytes: 0,
            pending_results: Vec::new(),
            applied_count: 0,
            decode_audit: Vec::new(),
            decode_audit_seq: 0,
        }
    }

    /// Create a new receiver in LISTENING state.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(ReceiverConfig::default())
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

    /// View decode-proof audit entries emitted so far.
    #[must_use]
    pub fn decode_audit_entries(&self) -> &[DecodeAuditEntry] {
        &self.decode_audit
    }

    /// Drain decode-proof audit entries.
    pub fn take_decode_audit_entries(&mut self) -> Vec<DecodeAuditEntry> {
        std::mem::take(&mut self.decode_audit)
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
        if !packet.verify_integrity(self.config.auth_key.as_ref()) {
            warn!(
                bead_id = BEAD_ID,
                wire_version = ?packet.wire_version,
                has_auth = packet.auth_tag.is_some(),
                "packet integrity/auth verification failed; treating as erasure"
            );
            return Ok(PacketResult::Erasure);
        }
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

        // Compute symbol_size from packet header and validate payload consistency.
        if usize::from(packet.symbol_size_t) != packet.symbol_data.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "symbol_size_t mismatch: header={}, payload={}",
                    packet.symbol_size_t,
                    packet.symbol_data.len()
                ),
            });
        }
        let symbol_size = u32::from(packet.symbol_size_t);
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
        let mut created_decoder = false;

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
            if packet.wire_version == ReplicationWireVersion::FramedV2
                && decoder.seed != packet.seed
            {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "seed mismatch: expected {}, got {}",
                        decoder.seed, packet.seed
                    ),
                });
            }
        } else {
            if self.decoders.len() >= self.config.max_inflight_decoders {
                warn!(
                    bead_id = BEAD_ID,
                    active_decoders = self.decoders.len(),
                    max_inflight_decoders = self.config.max_inflight_decoders,
                    "decoder cap reached; rejecting new changeset"
                );
                return Err(FrankenError::Busy);
            }
            // Create new decoder state.
            let expected_seed =
                crate::replication_sender::derive_seed_from_changeset_id(&changeset_id);
            if packet.wire_version == ReplicationWireVersion::FramedV2
                && packet.seed != expected_seed
            {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "seed does not match deterministic derivation for changeset: expected {expected_seed}, got {}",
                        packet.seed
                    ),
                });
            }
            let seed = expected_seed;
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
            created_decoder = true;
        }

        // Enforce global buffered-symbol bound before accepting a new symbol.
        if let Some(decoder) = self.decoders.get(&changeset_id)
            && !decoder.has_symbol(packet.esi)
        {
            let next_total = self
                .buffered_symbol_bytes
                .saturating_add(packet.symbol_data.len());
            if next_total > self.config.max_buffered_symbol_bytes {
                warn!(
                    bead_id = BEAD_ID,
                    buffered_symbol_bytes = self.buffered_symbol_bytes,
                    incoming_symbol_bytes = packet.symbol_data.len(),
                    max_buffered_symbol_bytes = self.config.max_buffered_symbol_bytes,
                    "buffered symbol budget exceeded"
                );
                if created_decoder {
                    self.remove_decoder(changeset_id);
                    self.state = if self.decoders.is_empty() {
                        ReceiverState::Listening
                    } else {
                        ReceiverState::Collecting
                    };
                }
                return Err(FrankenError::TooBig);
            }
        }

        // Add symbol to decoder (with ISI deduplication) and capture decode context.
        let (
            ready_to_decode,
            k_source_ctx,
            symbol_size_ctx,
            seed_ctx,
            received_isis_ctx,
            received_count_ctx,
            source_count_ctx,
            has_repair_ctx,
            decoded_padded,
        ) = {
            let decoder = self.decoders.get_mut(&changeset_id).expect("just inserted");
            let accepted = decoder.add_symbol(packet.esi, packet.symbol_data.clone());

            if !accepted {
                debug!(
                    bead_id = BEAD_ID,
                    isi = packet.esi,
                    "duplicate ISI, symbol ignored"
                );
                return Ok(PacketResult::Duplicate);
            }

            self.buffered_symbol_bytes = self
                .buffered_symbol_bytes
                .saturating_add(packet.symbol_data.len());
            let count = self.received_counts.entry(changeset_id).or_insert(0);
            *count += 1;
            debug!(
                bead_id = BEAD_ID,
                isi = packet.esi,
                received = *count,
                k_source = packet.k_source,
                "symbol accepted"
            );

            let ready = decoder.ready_to_decode();
            let padded = if ready { decoder.try_decode() } else { None };
            (
                ready,
                decoder.k_source,
                decoder.symbol_size,
                decoder.seed,
                decoder.sorted_isis(),
                decoder.received_count(),
                decoder.source_symbol_count(),
                decoder.has_repair_symbols(),
                padded,
            )
        };

        if ready_to_decode {
            info!(
                bead_id = BEAD_ID,
                received = received_count_ctx,
                k_source = k_source_ctx,
                "attempting decode"
            );
            self.state = ReceiverState::Decoding;

            if let Some(padded_bytes) = decoded_padded {
                let success_proof =
                    if self.config.decode_proof_policy.emit_on_repair_success && has_repair_ctx {
                        Some(Self::build_decode_proof(DecodeProofBuildInput {
                            changeset_id,
                            k_source: k_source_ctx,
                            symbol_size: symbol_size_ctx,
                            seed: seed_ctx,
                            received_isis: &received_isis_ctx,
                            decode_success: true,
                            intermediate_rank: Some(k_source_ctx),
                            symbols_used: received_count_ctx,
                        }))
                    } else {
                        None
                    };

                // Decode succeeded: truncate to total_len and parse pages.
                match self.parse_and_validate_changeset(changeset_id, &padded_bytes) {
                    Ok(mut result) => {
                        let n_pages = result.pages.len();
                        if let Some(proof) = success_proof {
                            self.record_decode_proof(proof.clone());
                            result.decode_proof = Some(proof);
                        }
                        self.pending_results.push(result);
                        self.state = ReceiverState::Applying;
                        info!(
                            bead_id = BEAD_ID,
                            n_pages, "decode succeeded, ready to apply"
                        );
                        // Clean up decoder for this changeset.
                        self.remove_decoder(changeset_id);
                        return Ok(PacketResult::DecodeReady);
                    }
                    Err(e) => {
                        error!(
                            bead_id = BEAD_ID,
                            error = %e,
                            "changeset validation failed after decode"
                        );
                        // Clean up failed decoder.
                        self.remove_decoder(changeset_id);
                        self.state = if self.decoders.is_empty() {
                            ReceiverState::Listening
                        } else {
                            ReceiverState::Collecting
                        };
                        return Err(e);
                    }
                }
            }

            if self.config.decode_proof_policy.emit_on_decode_failure {
                let failure_proof = Self::build_decode_proof(DecodeProofBuildInput {
                    changeset_id,
                    k_source: k_source_ctx,
                    symbol_size: symbol_size_ctx,
                    seed: seed_ctx,
                    received_isis: &received_isis_ctx,
                    decode_success: false,
                    intermediate_rank: Some(source_count_ctx),
                    symbols_used: received_count_ctx,
                });
                self.record_decode_proof(failure_proof);
            }

            // Decode failed (need more symbols).
            warn!(
                bead_id = BEAD_ID,
                source_count = source_count_ctx,
                k_source = k_source_ctx,
                "decode failed at K_source, continuing collection"
            );
            self.state = ReceiverState::Collecting;
            return Ok(PacketResult::NeedMore);
        }

        Ok(PacketResult::Accepted)
    }

    /// Parse and validate decoded changeset bytes.
    #[allow(clippy::too_many_lines)]
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
        if total_len < CHANGESET_HEADER_SIZE {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "total_len ({total_len}) smaller than changeset header size ({CHANGESET_HEADER_SIZE})"
                ),
            });
        }
        if total_len > padded_bytes.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "total_len ({total_len}) exceeds decoded bytes ({})",
                    padded_bytes.len()
                ),
            });
        }
        let changeset_bytes = &padded_bytes[..total_len];
        let computed_id = compute_changeset_id(changeset_bytes);
        if computed_id != changeset_id {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "changeset id mismatch: expected {changeset_id:?}, computed {computed_id:?}"
                ),
            });
        }

        // Parse page entries.
        let page_size =
            usize::try_from(header.page_size).map_err(|_| FrankenError::OutOfRange {
                what: "page_size".to_owned(),
                value: header.page_size.to_string(),
            })?;
        if page_size == 0 {
            return Err(FrankenError::OutOfRange {
                what: "page_size".to_owned(),
                value: "0".to_owned(),
            });
        }
        let entry_size = 4_usize
            .checked_add(8)
            .and_then(|value| value.checked_add(page_size))
            .ok_or_else(|| FrankenError::OutOfRange {
                what: "entry_size".to_owned(),
                value: format!("page_size={}", header.page_size),
            })?; // page_number + xxh3 + data
        let n_pages = usize::try_from(header.n_pages).map_err(|_| FrankenError::OutOfRange {
            what: "n_pages".to_owned(),
            value: header.n_pages.to_string(),
        })?;
        let data_start = CHANGESET_HEADER_SIZE;
        let data_bytes = &changeset_bytes[data_start..];
        let required_bytes =
            entry_size
                .checked_mul(n_pages)
                .ok_or_else(|| FrankenError::OutOfRange {
                    what: "changeset payload size".to_owned(),
                    value: format!("entry_size={entry_size}, n_pages={}", header.n_pages),
                })?;

        if data_bytes.len() != required_bytes {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "changeset payload length mismatch for {} pages: {} != {}",
                    header.n_pages,
                    data_bytes.len(),
                    required_bytes,
                ),
            });
        }

        let mut pages = Vec::with_capacity(n_pages);
        let decoder_state_symbols = self
            .decoders
            .get(&changeset_id)
            .map_or(0, DecoderState::received_count);

        for i in 0..n_pages {
            let offset = i
                .checked_mul(entry_size)
                .ok_or_else(|| FrankenError::OutOfRange {
                    what: "page entry offset".to_owned(),
                    value: format!("index={i}, entry_size={entry_size}"),
                })?;
            let page_number =
                u32::from_le_bytes(data_bytes[offset..offset + 4].try_into().expect("4 bytes"));
            let page_xxh3 = u64::from_le_bytes(
                data_bytes[offset + 4..offset + 12]
                    .try_into()
                    .expect("8 bytes"),
            );
            let page_data = data_bytes[offset + 12..offset + 12 + page_size].to_vec();

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
            decode_proof: None,
        })
    }

    fn build_decode_proof(input: DecodeProofBuildInput<'_>) -> EcsDecodeProof {
        let object_id = ObjectId::from_bytes(*input.changeset_id.as_bytes());
        let timing_ns =
            deterministic_timing_ns(input.k_source, input.symbol_size, input.symbols_used);
        EcsDecodeProof::from_esis(
            object_id,
            input.k_source,
            input.received_isis,
            input.decode_success,
            input.intermediate_rank,
            timing_ns,
            input.seed,
        )
        .with_changeset_id(*input.changeset_id.as_bytes())
    }

    fn record_decode_proof(&mut self, proof: EcsDecodeProof) {
        self.decode_audit_seq = self.decode_audit_seq.saturating_add(1);
        self.decode_audit.push(DecodeAuditEntry {
            proof,
            seq: self.decode_audit_seq,
            lab_mode: false,
        });
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
        self.buffered_symbol_bytes = 0;
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

fn deterministic_timing_ns(k_source: u32, symbol_size: u32, symbols_used: u32) -> u64 {
    let mut material = [0_u8; 12];
    material[..4].copy_from_slice(&k_source.to_le_bytes());
    material[4..8].copy_from_slice(&symbol_size.to_le_bytes());
    material[8..12].copy_from_slice(&symbols_used.to_le_bytes());
    xxhash_rust::xxh3::xxh3_64(&material)
}

/// Result of processing a single packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketResult {
    /// Symbol accepted, need more for decode.
    Accepted,
    /// Integrity/auth invalid; packet ignored as erasure.
    Erasure,
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

    let total_len = usize::try_from(header.total_len).map_err(|_| FrankenError::OutOfRange {
        what: "total_len".to_owned(),
        value: header.total_len.to_string(),
    })?;
    if total_len < CHANGESET_HEADER_SIZE {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "total_len ({total_len}) smaller than changeset header size ({CHANGESET_HEADER_SIZE})"
            ),
        });
    }
    if total_len > changeset_bytes.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "total_len ({total_len}) exceeds available bytes ({})",
                changeset_bytes.len()
            ),
        });
    }
    let changeset_bytes = &changeset_bytes[..total_len];

    let page_size = usize::try_from(header.page_size).map_err(|_| FrankenError::OutOfRange {
        what: "page_size".to_owned(),
        value: header.page_size.to_string(),
    })?;
    if page_size == 0 {
        return Err(FrankenError::OutOfRange {
            what: "page_size".to_owned(),
            value: "0".to_owned(),
        });
    }
    let entry_size = 4_usize
        .checked_add(8)
        .and_then(|value| value.checked_add(page_size))
        .ok_or_else(|| FrankenError::OutOfRange {
            what: "entry_size".to_owned(),
            value: format!("page_size={}", header.page_size),
        })?;
    let n_pages = usize::try_from(header.n_pages).map_err(|_| FrankenError::OutOfRange {
        what: "n_pages".to_owned(),
        value: header.n_pages.to_string(),
    })?;
    let data_start = CHANGESET_HEADER_SIZE;
    let data_bytes = &changeset_bytes[data_start..];
    let required_bytes =
        entry_size
            .checked_mul(n_pages)
            .ok_or_else(|| FrankenError::OutOfRange {
                what: "changeset payload size".to_owned(),
                value: format!("entry_size={entry_size}, n_pages={}", header.n_pages),
            })?;
    if data_bytes.len() != required_bytes {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "changeset payload length mismatch for {} pages: {} != {}",
                header.n_pages,
                data_bytes.len(),
                required_bytes
            ),
        });
    }

    let mut pages = Vec::with_capacity(n_pages);
    for i in 0..n_pages {
        let offset = i
            .checked_mul(entry_size)
            .ok_or_else(|| FrankenError::OutOfRange {
                what: "page entry offset".to_owned(),
                value: format!("index={i}, entry_size={entry_size}"),
            })?;
        let page_number =
            u32::from_le_bytes(data_bytes[offset..offset + 4].try_into().expect("4 bytes"));
        let page_xxh3 = u64::from_le_bytes(
            data_bytes[offset + 4..offset + 12]
                .try_into()
                .expect("8 bytes"),
        );
        let page_bytes = data_bytes[offset + 12..offset + 12 + page_size].to_vec();

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
    use asupersync::runtime::RuntimeBuilder;
    use asupersync::security::authenticated::AuthenticatedSymbol;
    use asupersync::security::tag::AuthenticationTag;
    use asupersync::transport::{
        SimNetwork, SimTransportConfig, SymbolSinkExt as _, SymbolStreamExt as _,
    };
    use asupersync::types::{Symbol, SymbolId, SymbolKind};
    use std::collections::HashSet;

    use super::*;
    use crate::replication_sender::{
        CHANGESET_HEADER_SIZE, ChangesetId, PageEntry, REPLICATION_HEADER_SIZE, ReplicationPacket,
        ReplicationPacketV2Header, ReplicationSender, ReplicationWireVersion, SenderConfig,
        compute_changeset_id, derive_seed_from_changeset_id, encode_changeset,
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
        generate_sender_packets_with_multiplier(page_size, page_numbers, symbol_size, 1)
    }

    fn generate_sender_packets_with_multiplier(
        page_size: u32,
        page_numbers: &[u32],
        symbol_size: u16,
        max_isi_multiplier: u32,
    ) -> Vec<Vec<u8>> {
        let mut sender = ReplicationSender::new();
        let mut pages = make_pages(page_size, page_numbers);
        let config = SenderConfig {
            symbol_size,
            max_isi_multiplier,
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

    #[derive(Debug)]
    struct SimNetworkDelivery {
        sent_count: usize,
        delivered: Vec<(u32, Vec<u8>)>,
    }

    fn packet_symbol(esi: u32, wire_bytes: Vec<u8>) -> AuthenticatedSymbol {
        let symbol_id = SymbolId::new_for_test(0xBEEF, 0, esi);
        let symbol = Symbol::new(symbol_id, wire_bytes, SymbolKind::Source);
        AuthenticatedSymbol::from_parts(symbol, AuthenticationTag::zero())
    }

    fn transmit_packets_simnetwork(
        config: SimTransportConfig,
        packet_bytes: &[Vec<u8>],
    ) -> SimNetworkDelivery {
        let network = SimNetwork::fully_connected(2, config);
        let (mut sink, mut stream) = network.transport(0, 1);
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .expect("runtime build");

        runtime.block_on(async {
            for (index, bytes) in packet_bytes.iter().enumerate() {
                let esi = u32::try_from(index).expect("test packet index fits u32");
                sink.send(packet_symbol(esi, bytes.clone()))
                    .await
                    .expect("send simulated symbol");
            }
            sink.close().await.expect("close simulated sink");

            let mut delivered = Vec::new();
            while let Some(item) = stream.next().await {
                let auth = item.expect("sim stream item");
                delivered.push((auth.symbol().id().esi(), auth.symbol().data().to_vec()));
            }

            SimNetworkDelivery {
                sent_count: packet_bytes.len(),
                delivered,
            }
        })
    }

    fn has_duplicate_esies(delivery: &SimNetworkDelivery) -> bool {
        let mut seen = HashSet::new();
        delivery.delivered.iter().any(|(esi, _)| !seen.insert(*esi))
    }

    fn has_reordered_esies(delivery: &SimNetworkDelivery) -> bool {
        delivery
            .delivered
            .windows(2)
            .any(|window| window[0].0 > window[1].0)
    }

    fn has_corrupted_wire_bytes(delivery: &SimNetworkDelivery, original: &[Vec<u8>]) -> bool {
        delivery.delivered.iter().any(|(esi, bytes)| {
            usize::try_from(*esi)
                .ok()
                .and_then(|index| original.get(index))
                .is_some_and(|expected| expected.as_slice() != bytes.as_slice())
        })
    }

    fn decode_from_wire_packets(
        delivered: &[(u32, Vec<u8>)],
    ) -> (Option<Vec<DecodedPage>>, usize, usize) {
        let mut receiver = ReplicationReceiver::new();
        let mut erasures = 0_usize;
        let mut parse_errors = 0_usize;

        for (_, wire) in delivered {
            match receiver.process_packet(wire) {
                Ok(PacketResult::DecodeReady) => {
                    let mut applied = receiver.apply_pending().expect("apply decoded changeset");
                    let pages = applied.pop().expect("decode result pages").pages;
                    return (Some(pages), erasures, parse_errors);
                }
                Ok(PacketResult::Erasure) => erasures += 1,
                Ok(PacketResult::Accepted | PacketResult::Duplicate | PacketResult::NeedMore) => {}
                Err(_) => parse_errors += 1,
            }
        }

        (None, erasures, parse_errors)
    }

    fn decoded_matches_original(decoded: &[DecodedPage], original: &[PageEntry]) -> bool {
        if decoded.len() != original.len() {
            return false;
        }
        for (decoded, original) in decoded.iter().zip(original.iter()) {
            if decoded.page_number != original.page_number {
                return false;
            }
            if decoded.page_data != original.page_bytes {
                return false;
            }
        }
        true
    }

    fn make_packet(
        changeset_id: ChangesetId,
        sbn: u8,
        esi: u32,
        k_source: u32,
        symbol_data: Vec<u8>,
    ) -> ReplicationPacket {
        let symbol_size_t =
            u16::try_from(symbol_data.len()).expect("test symbol payload must fit u16");
        let seed = derive_seed_from_changeset_id(&changeset_id);
        ReplicationPacket::new_v2(
            ReplicationPacketV2Header {
                changeset_id,
                sbn,
                esi,
                k_source,
                r_repair: 0,
                symbol_size_t,
                seed,
            },
            symbol_data,
        )
    }

    fn receiver_with_decode_proofs() -> ReplicationReceiver {
        ReplicationReceiver::with_config(ReceiverConfig {
            auth_key: None,
            decode_proof_policy: DecodeProofEmissionPolicy::durability_critical(),
            ..ReceiverConfig::default()
        })
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
    fn test_receiver_rejects_new_changeset_when_decoder_limit_hit() {
        let mut receiver = ReplicationReceiver::with_config(ReceiverConfig {
            max_inflight_decoders: 1,
            ..ReceiverConfig::default()
        });

        let first = make_packet(
            ChangesetId::from_bytes([0x31; 16]),
            0,
            0,
            100,
            vec![0x11; 256],
        );
        receiver
            .process_parsed_packet(&first)
            .expect("first decoder");
        assert_eq!(receiver.active_decoders(), 1);

        let second = make_packet(
            ChangesetId::from_bytes([0x32; 16]),
            0,
            0,
            100,
            vec![0x22; 256],
        );
        let err = receiver.process_parsed_packet(&second).unwrap_err();
        assert!(matches!(err, FrankenError::Busy));
        assert_eq!(receiver.active_decoders(), 1);
    }

    #[test]
    fn test_receiver_enforces_buffered_symbol_budget() {
        let mut receiver = ReplicationReceiver::with_config(ReceiverConfig {
            max_buffered_symbol_bytes: 512,
            ..ReceiverConfig::default()
        });

        let first = make_packet(
            ChangesetId::from_bytes([0x41; 16]),
            0,
            0,
            100,
            vec![0x55; 400],
        );
        receiver
            .process_parsed_packet(&first)
            .expect("first packet");
        assert_eq!(receiver.active_decoders(), 1);

        // New changeset would exceed budget and should be rejected/cleaned up.
        let second = make_packet(
            ChangesetId::from_bytes([0x42; 16]),
            0,
            0,
            100,
            vec![0x77; 200],
        );
        let err = receiver.process_parsed_packet(&second).unwrap_err();
        assert!(matches!(err, FrankenError::TooBig));
        assert_eq!(receiver.active_decoders(), 1);
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
        let packet = make_packet(
            ChangesetId::from_bytes([0xAA; 16]),
            1, // V1 violation
            0,
            10,
            vec![0x55; 512],
        );
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
        let packet_zero = make_packet(
            ChangesetId::from_bytes([0xBB; 16]),
            0,
            0,
            0,
            vec![0x55; 512],
        );
        let wire_zero = packet_zero.to_bytes().expect("encode");
        assert!(
            receiver.process_packet(&wire_zero).is_err(),
            "bead_id={TEST_BEAD_ID} case=k_source_zero_rejected"
        );

        // K_source = K_MAX + 1 → rejected.
        let packet_over = make_packet(
            ChangesetId::from_bytes([0xCC; 16]),
            0,
            0,
            K_MAX + 1,
            vec![0x55; 512],
        );
        // ESI only has 24 bits, K_source > K_MAX might not fit in packet format
        // but we test the validation path directly.
        let result = receiver.process_parsed_packet(&packet_over);
        assert!(
            result.is_err(),
            "bead_id={TEST_BEAD_ID} case=k_source_over_max_rejected"
        );

        // K_source = K_MAX → accepted.
        let packet_max = make_packet(
            ChangesetId::from_bytes([0xDD; 16]),
            0,
            0,
            K_MAX,
            vec![0x55; 512],
        );
        let result = receiver.process_parsed_packet(&packet_max);
        assert!(
            result.is_ok(),
            "bead_id={TEST_BEAD_ID} case=k_source_at_max_accepted"
        );
    }

    #[test]
    fn test_receiver_symbol_size_inference() {
        let mut receiver = ReplicationReceiver::new();
        let packet = make_packet(
            ChangesetId::from_bytes([0xEE; 16]),
            0,
            0,
            100,
            vec![0x42; 1024],
        );
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
        let empty_packet = make_packet(ChangesetId::from_bytes([0xFF; 16]), 0, 0, 10, vec![]);
        assert!(
            receiver2.process_parsed_packet(&empty_packet).is_err(),
            "bead_id={TEST_BEAD_ID} case=zero_symbol_size_rejected"
        );
    }

    #[test]
    fn test_receiver_k_source_mismatch_rejected() {
        let mut receiver = ReplicationReceiver::new();
        let id = ChangesetId::from_bytes([0x11; 16]);

        let p1 = make_packet(id, 0, 0, 100, vec![0x42; 512]);
        receiver
            .process_parsed_packet(&p1)
            .expect("first packet ok");

        // Same changeset_id, different K_source.
        let p2 = make_packet(id, 0, 1, 200, vec![0x42; 512]); // mismatch
        assert!(
            receiver.process_parsed_packet(&p2).is_err(),
            "bead_id={TEST_BEAD_ID} case=k_source_mismatch_rejected"
        );
    }

    #[test]
    fn test_receiver_symbol_size_mismatch_rejected() {
        let mut receiver = ReplicationReceiver::new();
        let id = ChangesetId::from_bytes([0x22; 16]);

        let p1 = make_packet(id, 0, 0, 100, vec![0x42; 512]);
        receiver
            .process_parsed_packet(&p1)
            .expect("first packet ok");

        // Same changeset_id, different symbol_size.
        let p2 = make_packet(id, 0, 1, 100, vec![0x42; 1024]); // different size
        assert!(
            receiver.process_parsed_packet(&p2).is_err(),
            "bead_id={TEST_BEAD_ID} case=symbol_size_mismatch_rejected"
        );
    }

    #[test]
    fn test_receiver_isi_deduplication() {
        let mut receiver = ReplicationReceiver::new();
        let id = ChangesetId::from_bytes([0x33; 16]);

        let p1 = make_packet(id, 0, 0, 100, vec![0x42; 512]);

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
    fn test_receiver_treats_payload_hash_mismatch_as_erasure() {
        let mut receiver = ReplicationReceiver::new();
        let packet = make_packet(
            ChangesetId::from_bytes([0x44; 16]),
            0,
            0,
            100,
            vec![0x42; 512],
        );
        let mut wire = packet.to_bytes().expect("encode packet");
        wire[48] ^= 0xFF;
        let result = receiver.process_packet(&wire).expect("process packet");
        assert_eq!(result, PacketResult::Erasure);
    }

    #[test]
    fn test_receiver_treats_invalid_auth_tag_as_erasure() {
        let receiver_key = [0x11_u8; 32];
        let sender_key = [0x22_u8; 32];
        let mut receiver =
            ReplicationReceiver::with_config(ReceiverConfig::with_auth_key(receiver_key));
        let mut packet = make_packet(
            ChangesetId::from_bytes([0x45; 16]),
            0,
            0,
            100,
            vec![0x24; 512],
        );
        packet.attach_auth_tag(&sender_key);
        let wire = packet.to_bytes().expect("encode auth packet");
        let result = receiver.process_packet(&wire).expect("process packet");
        assert_eq!(result, PacketResult::Erasure);
    }

    #[test]
    fn test_receiver_accepts_legacy_v1_packets() {
        let mut receiver = ReplicationReceiver::new();
        let id = ChangesetId::from_bytes([0x46; 16]);
        let symbol_data = vec![0x5A; 512];
        let legacy = ReplicationPacket {
            wire_version: ReplicationWireVersion::LegacyV1,
            changeset_id: id,
            sbn: 0,
            esi: 0,
            k_source: 100,
            r_repair: 0,
            symbol_size_t: 512,
            seed: derive_seed_from_changeset_id(&id),
            payload_xxh3: ReplicationPacket::compute_payload_xxh3(&symbol_data),
            auth_tag: None,
            symbol_data,
        };
        let wire = legacy.to_bytes().expect("encode legacy packet");
        let parsed = ReplicationPacket::from_bytes(&wire).expect("decode legacy packet");
        assert_eq!(parsed.wire_version, ReplicationWireVersion::LegacyV1);
        let result = receiver
            .process_packet(&wire)
            .expect("process legacy packet");
        assert_eq!(result, PacketResult::Accepted);
    }

    #[test]
    fn test_receiver_decode_at_k_source() {
        // Use the sender to generate proper packets, then feed to receiver.
        let page_size = 512_u32;
        let mut receiver = ReplicationReceiver::new();
        let packets = generate_sender_packets(page_size, &[1, 2, 3], 512);

        let mut last_result = PacketResult::Accepted;
        for pkt in &packets {
            let result = receiver
                .process_packet(pkt)
                .expect("bead_id={TEST_BEAD_ID} case=decode_at_k unexpected error");
            last_result = result;
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
    fn test_receiver_decode_failure_emits_proof_when_enabled() {
        let mut receiver = receiver_with_decode_proofs();
        let changeset_id = ChangesetId::from_bytes([0x5A; 16]);

        // Two repair-only symbols at K=2: ready_to_decode => true, but decode fails.
        let p1 = make_packet(changeset_id, 0, 2, 2, vec![0xA1; 64]);
        let p2 = make_packet(changeset_id, 0, 3, 2, vec![0xA2; 64]);

        let r1 = receiver.process_parsed_packet(&p1).expect("first packet");
        assert_eq!(r1, PacketResult::Accepted);
        let r2 = receiver.process_parsed_packet(&p2).expect("second packet");
        assert_eq!(r2, PacketResult::NeedMore);

        let audit = receiver.take_decode_audit_entries();
        assert_eq!(audit.len(), 1, "bead_id=bd-faz4 case=failure_proof_emitted");
        let proof = &audit[0].proof;
        assert!(
            !proof.decode_success,
            "bead_id=bd-faz4 case=failure_proof_decode_success_false"
        );
        assert_eq!(proof.changeset_id, Some(*changeset_id.as_bytes()));
        assert!(
            proof.is_consistent(),
            "bead_id=bd-faz4 case=failure_proof_consistent"
        );
    }

    #[test]
    fn test_receiver_decode_success_with_repair_emits_proof_when_enabled() {
        let mut receiver = ReplicationReceiver::with_config(ReceiverConfig {
            auth_key: None,
            decode_proof_policy: DecodeProofEmissionPolicy {
                emit_on_decode_failure: false,
                emit_on_repair_success: true,
            },
            ..ReceiverConfig::default()
        });
        let page_size = 64_u32;
        let mut pages = make_pages(page_size, &[7]);
        let changeset_bytes = encode_changeset(page_size, &mut pages).expect("encode changeset");
        let changeset_id = compute_changeset_id(&changeset_bytes);

        // Build K=2 source symbols from encoded bytes.
        let symbol_size = 64_usize;
        let mut s0 = vec![0_u8; symbol_size];
        let mut s1 = vec![0_u8; symbol_size];
        let split = changeset_bytes.len().min(symbol_size);
        s0[..split].copy_from_slice(&changeset_bytes[..split]);
        if changeset_bytes.len() > symbol_size {
            let rem = changeset_bytes.len() - symbol_size;
            s1[..rem].copy_from_slice(&changeset_bytes[symbol_size..]);
        }

        // Interleave source+repair so K is reached with at least one repair symbol present.
        let p0 = make_packet(changeset_id, 0, 0, 2, s0);
        let p_repair = make_packet(changeset_id, 0, 2, 2, vec![0xCC; symbol_size]);
        let p1 = make_packet(changeset_id, 0, 1, 2, s1);

        assert_eq!(
            receiver.process_parsed_packet(&p0).expect("p0"),
            PacketResult::Accepted
        );
        assert_eq!(
            receiver.process_parsed_packet(&p_repair).expect("repair"),
            PacketResult::NeedMore
        );
        assert_eq!(
            receiver.process_parsed_packet(&p1).expect("p1"),
            PacketResult::DecodeReady
        );
        assert_eq!(receiver.state(), ReceiverState::Applying);

        let results = receiver.apply_pending().expect("apply");
        assert_eq!(results.len(), 1);
        let decode_proof = results[0]
            .decode_proof
            .as_ref()
            .expect("bead_id=bd-faz4 case=success_proof_attached_to_result");
        assert!(decode_proof.decode_success);
        assert!(decode_proof.is_repair());
        assert!(
            decode_proof.is_consistent(),
            "bead_id=bd-faz4 case=success_proof_consistent"
        );

        let audit = receiver.take_decode_audit_entries();
        assert_eq!(audit.len(), 1, "bead_id=bd-faz4 case=success_proof_emitted");
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
    fn test_parse_and_validate_rejects_changeset_id_mismatch() {
        let page_size = 128_u32;
        let mut pages = make_pages(page_size, &[1]);
        let changeset_bytes = encode_changeset(page_size, &mut pages).expect("encode");
        let wrong_changeset_id = ChangesetId::from_bytes([0x42; 16]);

        let receiver = ReplicationReceiver::new();
        let result = receiver.parse_and_validate_changeset(wrong_changeset_id, &changeset_bytes);
        assert!(
            matches!(result, Err(FrankenError::DatabaseCorrupt { .. })),
            "bead_id={TEST_BEAD_ID} case=changeset_id_mismatch_rejected"
        );
    }

    #[test]
    fn test_parse_and_validate_rejects_total_len_smaller_than_header() {
        let receiver = ReplicationReceiver::new();
        let changeset_id = ChangesetId::from_bytes([0xA5; 16]);

        let mut malformed = vec![0_u8; CHANGESET_HEADER_SIZE];
        malformed[0..4].copy_from_slice(b"FSRP");
        malformed[4..6].copy_from_slice(&1_u16.to_le_bytes());
        malformed[6..10].copy_from_slice(&4096_u32.to_le_bytes());
        malformed[10..14].copy_from_slice(&1_u32.to_le_bytes());
        malformed[14..22].copy_from_slice(&1_u64.to_le_bytes());

        let result = receiver.parse_and_validate_changeset(changeset_id, &malformed);
        assert!(matches!(result, Err(FrankenError::DatabaseCorrupt { .. })));
    }

    #[test]
    fn test_parse_and_validate_rejects_trailing_payload_bytes() {
        let page_size = 128_u32;
        let mut pages = make_pages(page_size, &[1]);
        let mut malformed = encode_changeset(page_size, &mut pages).expect("encode");
        malformed.push(0x99);
        let total_len = u64::try_from(malformed.len()).expect("test total_len fits u64");
        malformed[14..22].copy_from_slice(&total_len.to_le_bytes());
        let changeset_id = compute_changeset_id(&malformed);

        let receiver = ReplicationReceiver::new();
        let result = receiver.parse_and_validate_changeset(changeset_id, &malformed);
        assert!(
            matches!(result, Err(FrankenError::DatabaseCorrupt { .. })),
            "bead_id={TEST_BEAD_ID} case=parse_rejects_trailing_payload"
        );
    }

    #[test]
    fn test_parse_changeset_pages_rejects_truncated_payload() {
        let total_len = CHANGESET_HEADER_SIZE + 8;
        let mut malformed = vec![0_u8; total_len];
        malformed[0..4].copy_from_slice(b"FSRP");
        malformed[4..6].copy_from_slice(&1_u16.to_le_bytes());
        malformed[6..10].copy_from_slice(&4096_u32.to_le_bytes());
        malformed[10..14].copy_from_slice(&1_u32.to_le_bytes());
        malformed[14..22].copy_from_slice(
            &u64::try_from(total_len)
                .expect("test total_len fits into u64")
                .to_le_bytes(),
        );

        let result = parse_changeset_pages(&malformed);
        assert!(matches!(result, Err(FrankenError::DatabaseCorrupt { .. })));
    }

    #[test]
    fn test_parse_changeset_pages_rejects_trailing_payload() {
        let page_size = 128_u32;
        let mut pages = make_pages(page_size, &[1]);
        let mut malformed = encode_changeset(page_size, &mut pages).expect("encode");
        malformed.push(0xA5);
        let total_len = u64::try_from(malformed.len()).expect("test total_len fits u64");
        malformed[14..22].copy_from_slice(&total_len.to_le_bytes());

        let result = parse_changeset_pages(&malformed);
        assert!(
            matches!(result, Err(FrankenError::DatabaseCorrupt { .. })),
            "bead_id={TEST_BEAD_ID} case=parse_pages_rejects_trailing_payload"
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
        let p1 = make_packet(id, 0, 0, 100, vec![0x42; 512]); // large enough that one symbol won't trigger decode

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
            let result = receiver
                .process_packet(pkt)
                .expect("bead_id={TEST_BEAD_ID} case=e2e_compliance unexpected error");
            last_result = result;
            if result == PacketResult::DecodeReady {
                break;
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

    #[test]
    fn test_simnetwork_loss_profiles_converge_with_repair_symbols() {
        let page_size = 128_u32;
        let page_numbers = [1_u32, 2];
        let original_pages = make_pages(page_size, &page_numbers);
        let packets = generate_sender_packets_with_multiplier(page_size, &page_numbers, 128, 2);
        let loss_packets: Vec<Vec<u8>> = packets
            .iter()
            .flat_map(|packet| [packet.clone(), packet.clone()])
            .collect();

        for (loss_rate, require_observed_drop) in [(0.05_f64, false), (0.30_f64, true)] {
            let mut found_seed = None;
            for seed in 1_u64..=20_000 {
                let mut config = SimTransportConfig::deterministic(seed);
                config.loss_rate = loss_rate;
                config.preserve_order = true;

                let delivery = transmit_packets_simnetwork(config, &loss_packets);
                let observed_drop = delivery.delivered.len() < delivery.sent_count;
                if require_observed_drop && !observed_drop {
                    continue;
                }
                let saw_repair_symbol = delivery.delivered.iter().any(|(_, wire)| {
                    ReplicationPacket::from_bytes(wire)
                        .is_ok_and(|packet| !packet.is_source_symbol())
                });
                if !saw_repair_symbol {
                    continue;
                }

                let (decoded, _erasures, _parse_errors) =
                    decode_from_wire_packets(&delivery.delivered);
                if decoded
                    .as_ref()
                    .is_some_and(|pages| decoded_matches_original(pages, &original_pages))
                {
                    found_seed = Some(seed);
                    break;
                }
            }

            assert!(
                found_seed.is_some(),
                "bead_id=bd-xgoe case=loss_profile_convergence loss_rate={loss_rate} require_drop={require_observed_drop} did not find deterministic convergent seed"
            );
        }
    }

    #[test]
    fn test_simnetwork_reorder_and_dup_converge() {
        let page_size = 128_u32;
        let page_numbers = [7_u32, 11];
        let original_pages = make_pages(page_size, &page_numbers);
        let packets = generate_sender_packets_with_multiplier(page_size, &page_numbers, 128, 2);

        let mut found_seed = None;
        for seed in 1_u64..=2_000 {
            let mut config = SimTransportConfig::deterministic(seed);
            config.preserve_order = false;
            config.duplication_rate = 0.35;

            let delivery = transmit_packets_simnetwork(config, &packets);
            if !has_duplicate_esies(&delivery) || !has_reordered_esies(&delivery) {
                continue;
            }

            let (decoded, _erasures, _parse_errors) = decode_from_wire_packets(&delivery.delivered);
            if decoded
                .as_ref()
                .is_some_and(|pages| decoded_matches_original(pages, &original_pages))
            {
                found_seed = Some(seed);
                break;
            }
        }

        assert!(
            found_seed.is_some(),
            "bead_id=bd-xgoe case=reorder_dup_convergence no deterministic seed achieved reorder+dup convergence"
        );
    }

    #[test]
    fn test_simnetwork_corruption_is_rejected_and_recovered() {
        let page_size = 128_u32;
        let page_numbers = [21_u32, 34];
        let original_pages = make_pages(page_size, &page_numbers);
        let packets = generate_sender_packets_with_multiplier(page_size, &page_numbers, 128, 2);

        let mut found_seed = None;
        for seed in 1_u64..=20_000 {
            let mut config = SimTransportConfig::deterministic(seed);
            config.corruption_rate = 0.20;
            config.preserve_order = false;

            let delivery = transmit_packets_simnetwork(config, &packets);
            if !has_corrupted_wire_bytes(&delivery, &packets) {
                continue;
            }

            let (decoded, erasures, parse_errors) = decode_from_wire_packets(&delivery.delivered);
            if erasures + parse_errors == 0 {
                continue;
            }
            if decoded
                .as_ref()
                .is_some_and(|pages| decoded_matches_original(pages, &original_pages))
            {
                found_seed = Some(seed);
                break;
            }
        }

        assert!(
            found_seed.is_some(),
            "bead_id=bd-xgoe case=corruption_recovery no deterministic seed achieved corruption rejection + convergence"
        );
    }

    #[test]
    fn test_simnetwork_stop_early_reduces_traffic() {
        let page_size = 256_u32;
        let page_numbers = [1_u32, 2, 3];
        let packets = generate_sender_packets_with_multiplier(page_size, &page_numbers, 256, 2);

        let full_delivery = transmit_packets_simnetwork(SimTransportConfig::reliable(), &packets);
        let full_sent = full_delivery.sent_count;

        let network = SimNetwork::fully_connected(2, SimTransportConfig::reliable());
        let (mut sink, mut stream) = network.transport(0, 1);
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .expect("runtime build");

        let mut receiver = ReplicationReceiver::new();
        let mut stop_early_sent = 0_usize;
        let mut decoded = false;

        runtime.block_on(async {
            for (index, bytes) in packets.iter().enumerate() {
                let esi = u32::try_from(index).expect("test packet index fits u32");
                sink.send(packet_symbol(esi, bytes.clone()))
                    .await
                    .expect("send simulated symbol");
                stop_early_sent += 1;

                let delivered = stream
                    .next()
                    .await
                    .expect("delivered packet")
                    .expect("stream item");
                let wire = delivered.symbol().data().to_vec();
                if matches!(
                    receiver.process_packet(&wire).expect("receiver process"),
                    PacketResult::DecodeReady
                ) {
                    decoded = true;
                    break;
                }
            }
            sink.close().await.expect("close simulated sink");
        });

        assert!(
            decoded,
            "bead_id=bd-xgoe case=stop_early_decode_not_reached"
        );
        assert!(
            stop_early_sent < full_sent,
            "bead_id=bd-xgoe case=stop_early_not_reduced stop_early_sent={stop_early_sent} full_sent={full_sent}"
        );
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
        let _ = PacketResult::Erasure;
        let _ = PacketResult::Duplicate;
        let _ = PacketResult::DecodeReady;
        let _ = PacketResult::NeedMore;

        let receiver = ReplicationReceiver::new();
        assert_eq!(receiver.state(), ReceiverState::Listening);
        assert_eq!(receiver.applied_count(), 0);
        assert_eq!(receiver.active_decoders(), 0);

        // Verify REPLICATION_HEADER_SIZE is correct.
        assert_eq!(REPLICATION_HEADER_SIZE, 72);
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
