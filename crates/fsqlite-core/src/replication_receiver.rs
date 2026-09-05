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
use fsqlite_types::cx::Cx;
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

#[derive(Debug)]
pub(crate) struct SymbolDecode {
    pub data: Option<Vec<u8>>,
    pub rank: Option<u32>,
    pub repairs_used: bool,
}

/// Decode one admitted source block, preserving canonical wire ESIs.
pub(crate) fn decode_symbols(
    cx: &Cx,
    symbols: &HashMap<u32, Vec<u8>>,
    k_source: u32,
    symbol_size: u32,
    seed: u64,
    max_bytes: usize,
    capture_rank: bool,
) -> Result<SymbolDecode> {
    cx.checkpoint().map_err(|_| FrankenError::Abort)?;
    let k = usize::try_from(k_source).map_err(|_| FrankenError::TooBig)?;
    let t = usize::try_from(symbol_size).map_err(|_| FrankenError::TooBig)?;
    let padded_len = k.checked_mul(t).ok_or(FrankenError::TooBig)?;
    if k == 0 || t == 0 || padded_len > max_bytes {
        return Err(FrankenError::TooBig);
    }
    if symbols.len() < k {
        return Ok(SymbolDecode {
            data: None,
            rank: None,
            repairs_used: false,
        });
    }
    if (0..k_source).all(|esi| symbols.contains_key(&esi)) {
        let mut padded = Vec::with_capacity(padded_len);
        for esi in 0..k_source {
            cx.checkpoint().map_err(|_| FrankenError::Abort)?;
            let data = &symbols[&esi];
            if data.len() != t {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "source symbol size mismatch".to_owned(),
                });
            }
            padded.extend_from_slice(data);
        }
        // Repairs may have arrived, but none contributed to direct assembly.
        return Ok(SymbolDecode {
            data: Some(padded),
            rank: None,
            repairs_used: false,
        });
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        use crate::replication_sender::{MAX_REPAIR_WORK_BYTES, repair_parameters};
        use asupersync::raptorq::decoder::{DecodeError, InactivationDecoder, ReceivedSymbol};

        repair_parameters(k, t, symbols.len(), max_bytes.min(MAX_REPAIR_WORK_BYTES))?;
        let decoder =
            InactivationDecoder::try_new(k, t, seed).map_err(|error| FrankenError::OutOfRange {
                what: "RaptorQ source block".to_owned(),
                value: format!("{error:?}"),
            })?;
        let mut received = decoder.constraint_symbols();
        let mut ordered: Vec<_> = symbols.iter().collect();
        ordered.sort_unstable_by_key(|(esi, _)| **esi);
        for (&esi, data) in ordered {
            cx.checkpoint().map_err(|_| FrankenError::Abort)?;
            if data.len() != t {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "repair symbol size mismatch".to_owned(),
                });
            }
            if esi < k_source {
                received.push(ReceivedSymbol::source(esi, data.clone()));
            } else {
                let (columns, coefficients) =
                    decoder
                        .repair_equation(esi)
                        .map_err(|error| FrankenError::OutOfRange {
                            what: "repair ESI".to_owned(),
                            value: format!("{error:?}"),
                        })?;
                received.push(ReceivedSymbol::repair(
                    esi,
                    columns,
                    coefficients,
                    data.clone(),
                ));
            }
        }
        let decoded = decoder.decode(&received);
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        match decoded {
            Ok(decoded) => {
                let rank =
                    u32::try_from(decoded.intermediate.len()).map_err(|_| FrankenError::TooBig)?;
                Ok(SymbolDecode {
                    data: Some(decoded.source.concat()),
                    rank: Some(rank),
                    repairs_used: true,
                })
            }
            Err(error) if error.is_recoverable() => {
                let rank = if capture_rank {
                    let status = decoder.rank_status(&received).map_err(|error| {
                        FrankenError::DatabaseCorrupt {
                            detail: format!("RaptorQ rank validation failed: {error:?}"),
                        }
                    })?;
                    Some(u32::try_from(status.rank).map_err(|_| FrankenError::TooBig)?)
                } else {
                    None
                };
                cx.checkpoint().map_err(|_| FrankenError::Abort)?;
                Ok(SymbolDecode {
                    data: None,
                    rank,
                    repairs_used: false,
                })
            }
            Err(
                DecodeError::ComputeBudgetExhausted { .. }
                | DecodeError::EsiRateLimitExceeded { .. },
            ) => Err(FrankenError::TooBig),
            Err(error) => Err(FrankenError::DatabaseCorrupt {
                detail: format!("RaptorQ decode failed: {error:?}"),
            }),
        }
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = (seed, capture_rank);
        Err(FrankenError::NotImplemented(
            "RaptorQ repair decoding on wasm32".to_owned(),
        ))
    }
}

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
            // A claimed K is not permission to allocate K slots before any
            // payload has passed admission. Grow with accepted symbols.
            symbols: HashMap::new(),
            received_isis: HashSet::new(),
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
    fn try_decode(&self, cx: &Cx, max_bytes: usize, capture_rank: bool) -> Result<SymbolDecode> {
        decode_symbols(
            cx,
            &self.symbols,
            self.k_source,
            self.symbol_size,
            self.seed,
            max_bytes,
            capture_rank,
        )
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
    seed: u64,
    received_isis: &'a [u32],
    decode_success: bool,
    intermediate_rank: Option<u32>,
    timing_ns: u64,
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
    pub fn process_packet(&mut self, cx: &Cx, packet_bytes: &[u8]) -> Result<PacketResult> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
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
        self.process_parsed_packet(cx, &packet)
    }

    /// Process a parsed packet.
    ///
    /// # Errors
    ///
    /// See `process_packet`.
    #[allow(clippy::too_many_lines)]
    pub fn process_parsed_packet(
        &mut self,
        cx: &Cx,
        packet: &ReplicationPacket,
    ) -> Result<PacketResult> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        if !packet.verify_integrity(self.config.auth_key.as_ref()) {
            return Ok(PacketResult::Erasure);
        }
        crate::replication_sender::validate_codec_esi(packet.esi)?;
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
            decode_timing_ns,
            seed_ctx,
            received_isis_ctx,
            received_count_ctx,
            source_count_ctx,
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
            #[cfg(not(target_arch = "wasm32"))]
            let started = std::time::Instant::now();
            let padded = if ready {
                decoder.try_decode(
                    cx,
                    self.config.max_buffered_symbol_bytes,
                    self.config.decode_proof_policy.emit_on_decode_failure,
                )
            } else {
                Ok(SymbolDecode {
                    data: None,
                    rank: None,
                    repairs_used: false,
                })
            };
            #[cfg(not(target_arch = "wasm32"))]
            let timing_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            // The browser path cannot emit a repair decode proof until it has
            // a supported codec. Zero here denotes unavailable timing.
            #[cfg(target_arch = "wasm32")]
            let timing_ns = 0;
            (
                ready,
                decoder.k_source,
                timing_ns,
                decoder.seed,
                decoder.sorted_isis(),
                decoder.received_count(),
                decoder.source_symbol_count(),
                padded,
            )
        };

        let decoded = match decoded_padded {
            Ok(decoded) => decoded,
            Err(error) => {
                let decoder = self
                    .decoders
                    .get_mut(&changeset_id)
                    .expect("active decoder");
                decoder.symbols.remove(&packet.esi);
                decoder.received_isis.remove(&packet.esi);
                self.buffered_symbol_bytes -= packet.symbol_data.len();
                *self
                    .received_counts
                    .get_mut(&changeset_id)
                    .expect("active count") -= 1;
                if created_decoder {
                    self.remove_decoder(changeset_id);
                }
                self.state = if self.decoders.is_empty() {
                    ReceiverState::Listening
                } else {
                    ReceiverState::Collecting
                };
                return Err(error);
            }
        };

        if ready_to_decode {
            info!(
                bead_id = BEAD_ID,
                received = received_count_ctx,
                k_source = k_source_ctx,
                "attempting decode"
            );
            self.state = ReceiverState::Decoding;

            if let Some(padded_bytes) = decoded.data {
                let success_proof = if self.config.decode_proof_policy.emit_on_repair_success
                    && decoded.repairs_used
                {
                    Some(Self::build_decode_proof(DecodeProofBuildInput {
                        changeset_id,
                        k_source: k_source_ctx,
                        seed: seed_ctx,
                        received_isis: &received_isis_ctx,
                        decode_success: true,
                        intermediate_rank: decoded.rank,
                        timing_ns: decode_timing_ns,
                    }))
                } else {
                    None
                };

                // Decode succeeded: truncate to total_len and parse pages.
                match self.parse_and_validate_changeset(changeset_id, &padded_bytes) {
                    Ok(mut result) => {
                        result.symbols_used = if decoded.repairs_used {
                            received_count_ctx
                        } else {
                            k_source_ctx
                        };
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
                    seed: seed_ctx,
                    received_isis: &received_isis_ctx,
                    decode_success: false,
                    intermediate_rank: decoded.rank,
                    timing_ns: decode_timing_ns,
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

        if pages
            .windows(2)
            .any(|pair| pair[0].page_number > pair[1].page_number)
        {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "changeset pages are not ordered by page number".to_owned(),
            });
        }

        Ok(DecodeResult {
            changeset_id,
            pages,
            symbols_used: decoder_state_symbols,
            decode_proof: None,
        })
    }

    fn build_decode_proof(input: DecodeProofBuildInput<'_>) -> EcsDecodeProof {
        let object_id = ObjectId::from_bytes(*input.changeset_id.as_bytes());
        EcsDecodeProof::from_esis(
            object_id,
            input.k_source,
            input.received_isis,
            input.decode_success,
            input.intermediate_rank,
            input.timing_ns,
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

    /// Drain validated changesets for the caller to apply to its database.
    ///
    /// # Errors
    ///
    /// Returns error if there are no pending results.
    pub fn apply_pending(&mut self) -> Result<Vec<DecodeResult>> {
        if self.pending_results.is_empty() {
            return Err(FrankenError::Internal(format!(
                "receiver has no pending changesets, current state: {:?}",
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

        self.state = if self.decoders.is_empty() {
            ReceiverState::Complete
        } else {
            ReceiverState::Collecting
        };
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
    use asupersync::raptorq::decoder::{InactivationDecoder, ReceivedSymbol};
    use asupersync::raptorq::systematic::SystematicEncoder;
    use asupersync::runtime::RuntimeBuilder;
    use asupersync::security::authenticated::AuthenticatedSymbol;
    use asupersync::security::tag::AuthenticationTag;
    use asupersync::transport::{
        SimNetwork, SimTransportConfig, SymbolSinkExt as _, SymbolStreamExt as _,
    };
    use asupersync::types::{Symbol, SymbolId, SymbolKind};
    use fsqlite_types::cx::Cx;
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
        while let Some(packet) = sender.next_packet(&Cx::new()).expect("next") {
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
        auth_key: Option<[u8; 32]>,
    ) -> (Option<DecodeResult>, usize, usize) {
        let mut receiver = receiver_with_decode_proofs();
        receiver.config.auth_key = auth_key;
        let cx = Cx::new();
        let mut erasures = 0_usize;
        let mut parse_errors = 0_usize;

        for (_, wire) in delivered {
            match receiver.process_packet(&cx, wire) {
                Ok(PacketResult::DecodeReady) => {
                    let mut applied = receiver.apply_pending().expect("apply decoded changeset");
                    assert_eq!(applied.len(), 1);
                    assert_eq!(receiver.buffered_symbol_bytes, 0);
                    assert_eq!(receiver.active_decoders(), 0);
                    let decoded = applied.pop().expect("decode result pages");
                    assert_eq!(
                        receiver
                            .decode_audit_entries()
                            .iter()
                            .filter(|entry| entry.proof.decode_success)
                            .count(),
                        usize::from(decoded.decode_proof.is_some())
                    );
                    return (Some(decoded), erasures, parse_errors);
                }
                Ok(PacketResult::Erasure) => erasures += 1,
                Ok(PacketResult::Accepted | PacketResult::Duplicate | PacketResult::NeedMore) => {}
                Err(error) => {
                    eprintln!("bead_id=bd-3mgq5.2 event=packet_error error={error}");
                    parse_errors += 1;
                }
            }
        }

        (None, erasures, parse_errors)
    }

    fn assert_genuine_repair(result: &DecodeResult, original: &[PageEntry]) {
        assert!(decoded_matches_original(&result.pages, original));
        let proof = result.decode_proof.as_ref().expect("repair proof");
        assert!(proof.decode_success && proof.is_repair() && proof.is_consistent());
        assert!(
            !proof.source_esis.contains(&0),
            "source ESI 0 was permanently erased"
        );
        assert!(!proof.symbols_received.contains(&0));
        assert!(!proof.repair_esis.is_empty());
        let decoder = InactivationDecoder::new(
            usize::try_from(proof.k_source).expect("K fits usize"),
            128,
            proof.seed,
        );
        assert_eq!(
            proof.intermediate_rank,
            Some(u32::try_from(decoder.params().l).expect("rank fits u32"))
        );
        assert_eq!(proof.changeset_id, Some(*result.changeset_id.as_bytes()));
        eprintln!(
            "bead_id=bd-3mgq5.2 event=repair_decoded k={} rank={:?} source_esis={:?} repair_esis={:?} pages={}",
            proof.k_source,
            proof.intermediate_rank,
            proof.source_esis,
            proof.repair_esis,
            result.pages.len()
        );
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

        receiver
            .process_packet(&Cx::new(), &packets[0])
            .expect("first packet");
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

        receiver
            .process_packet(&Cx::new(), &packets[0])
            .expect("first packet");
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
            .process_parsed_packet(&Cx::new(), &first)
            .expect("first decoder");
        assert_eq!(receiver.active_decoders(), 1);

        let second = make_packet(
            ChangesetId::from_bytes([0x32; 16]),
            0,
            0,
            100,
            vec![0x22; 256],
        );
        let err = receiver
            .process_parsed_packet(&Cx::new(), &second)
            .unwrap_err();
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
            .process_parsed_packet(&Cx::new(), &first)
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
        let err = receiver
            .process_parsed_packet(&Cx::new(), &second)
            .unwrap_err();
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
        let result = receiver.process_packet(&Cx::new(), &wire);
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
            receiver.process_packet(&Cx::new(), &wire_zero).is_err(),
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
        let result = receiver.process_parsed_packet(&Cx::new(), &packet_over);
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
        let result = receiver.process_parsed_packet(&Cx::new(), &packet_max);
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
            .process_parsed_packet(&Cx::new(), &packet)
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
            receiver2
                .process_parsed_packet(&Cx::new(), &empty_packet)
                .is_err(),
            "bead_id={TEST_BEAD_ID} case=zero_symbol_size_rejected"
        );
    }

    #[test]
    fn test_receiver_k_source_mismatch_rejected() {
        let mut receiver = ReplicationReceiver::new();
        let id = ChangesetId::from_bytes([0x11; 16]);

        let p1 = make_packet(id, 0, 0, 100, vec![0x42; 512]);
        receiver
            .process_parsed_packet(&Cx::new(), &p1)
            .expect("first packet ok");

        // Same changeset_id, different K_source.
        let p2 = make_packet(id, 0, 1, 200, vec![0x42; 512]); // mismatch
        assert!(
            receiver.process_parsed_packet(&Cx::new(), &p2).is_err(),
            "bead_id={TEST_BEAD_ID} case=k_source_mismatch_rejected"
        );
    }

    #[test]
    fn test_receiver_symbol_size_mismatch_rejected() {
        let mut receiver = ReplicationReceiver::new();
        let id = ChangesetId::from_bytes([0x22; 16]);

        let p1 = make_packet(id, 0, 0, 100, vec![0x42; 512]);
        receiver
            .process_parsed_packet(&Cx::new(), &p1)
            .expect("first packet ok");

        // Same changeset_id, different symbol_size.
        let p2 = make_packet(id, 0, 1, 100, vec![0x42; 1024]); // different size
        assert!(
            receiver.process_parsed_packet(&Cx::new(), &p2).is_err(),
            "bead_id={TEST_BEAD_ID} case=symbol_size_mismatch_rejected"
        );
    }

    #[test]
    fn test_receiver_isi_deduplication() {
        let mut receiver = ReplicationReceiver::new();
        let id = ChangesetId::from_bytes([0x33; 16]);

        let p1 = make_packet(id, 0, 0, 100, vec![0x42; 512]);

        let r1 = receiver
            .process_parsed_packet(&Cx::new(), &p1)
            .expect("first");
        assert_eq!(
            r1,
            PacketResult::Accepted,
            "bead_id={TEST_BEAD_ID} case=first_accepted"
        );

        // Same ISI again → duplicate.
        let r2 = receiver
            .process_parsed_packet(&Cx::new(), &p1)
            .expect("duplicate");
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
        let result = receiver
            .process_packet(&Cx::new(), &wire)
            .expect("process packet");
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
        let result = receiver
            .process_packet(&Cx::new(), &wire)
            .expect("process packet");
        assert_eq!(result, PacketResult::Erasure);
    }

    #[test]
    fn test_receiver_rejects_corrupt_and_unauthenticated_repairs_on_both_entrypoints() {
        let cx = Cx::new();
        let key = [0x11_u8; 32];
        let packets = generate_sender_packets_with_multiplier(128, &[1, 2], 128, 2);
        let repair = packets
            .iter()
            .map(|wire| ReplicationPacket::from_bytes(wire).expect("packet"))
            .find(|packet| !packet.is_source_symbol())
            .expect("real repair");
        let mut valid = repair.clone();
        valid.attach_auth_tag(&key);
        let mut corrupted_wire = valid.to_bytes().expect("valid authenticated wire");
        let payload_start = corrupted_wire.len() - valid.symbol_data.len();
        corrupted_wire[payload_start] ^= 0x80;
        let mut corrupted = valid.clone();
        corrupted.symbol_data[0] ^= 0x80;
        let mut wrong_key = repair.clone();
        wrong_key.attach_auth_tag(&[0x22; 32]);
        let wrong_key_wire = wrong_key.to_bytes().expect("wrong-key wire");
        let missing_tag_wire = repair.to_bytes().expect("missing-tag wire");
        for (case, packet, wire) in [
            ("payload", corrupted, corrupted_wire),
            ("wrong_key", wrong_key, wrong_key_wire),
            ("missing_tag", repair, missing_tag_wire),
        ] {
            for parsed in [false, true] {
                let mut receiver =
                    ReplicationReceiver::with_config(ReceiverConfig::with_auth_key(key));
                let outcome = if parsed {
                    receiver.process_parsed_packet(&cx, &packet)
                } else {
                    receiver.process_packet(&cx, &wire)
                };
                assert_eq!(
                    outcome.expect("invalid integrity is an erasure"),
                    PacketResult::Erasure,
                    "case={case} parsed={parsed}"
                );
                assert_eq!(receiver.state(), ReceiverState::Listening);
                assert_eq!(receiver.active_decoders(), 0);
                assert_eq!(receiver.buffered_symbol_bytes, 0);
                assert!(receiver.pending_results.is_empty());
                assert!(receiver.decode_audit_entries().is_empty());
                assert_eq!(
                    receiver
                        .process_parsed_packet(&cx, &valid)
                        .expect("valid authenticated repair"),
                    PacketResult::Accepted
                );
                assert_eq!(receiver.buffered_symbol_bytes, 128);
                receiver.force_reset();
                assert_eq!(receiver.buffered_symbol_bytes, 0);
                eprintln!(
                    "bead_id=bd-3mgq5.2 event=repair_integrity case={case} parsed={parsed} rejected=true valid_retry=accepted"
                );
            }
        }
    }

    #[test]
    fn test_receiver_cancelled_packet_preserves_admitted_symbols() {
        let cx = Cx::new();
        let cancelled = Cx::new();
        cancelled.cancel();
        let packets = generate_sender_packets(128, &[1, 2], 128);
        let first = ReplicationPacket::from_bytes(&packets[0]).expect("first");
        let second = ReplicationPacket::from_bytes(&packets[1]).expect("second");
        let mut receiver = receiver_with_decode_proofs();
        assert!(matches!(
            receiver.process_packet(&cancelled, &packets[0]),
            Err(FrankenError::Abort)
        ));
        assert_eq!(receiver.state(), ReceiverState::Listening);
        assert_eq!(receiver.active_decoders(), 0);
        receiver
            .process_packet(&cx, &packets[0])
            .expect("first admitted");
        let original = receiver.decoders[&first.changeset_id].symbols.clone();
        let counts = receiver.received_counts.clone();
        for parsed in [false, true] {
            let outcome = if parsed {
                receiver.process_parsed_packet(&cancelled, &second)
            } else {
                receiver.process_packet(&cancelled, &packets[1])
            };
            assert!(matches!(outcome, Err(FrankenError::Abort)));
            assert_eq!(receiver.decoders[&first.changeset_id].symbols, original);
            assert_eq!(receiver.received_counts, counts);
            assert_eq!(receiver.buffered_symbol_bytes, 128);
            assert_eq!(receiver.state(), ReceiverState::Collecting);
            assert!(receiver.pending_results.is_empty());
            assert!(receiver.decode_audit_entries().is_empty());
        }
        for wire in &packets[1..] {
            receiver
                .process_packet(&cx, wire)
                .expect("fresh caller continues");
        }
        let results = receiver
            .apply_pending()
            .expect("exact source-only recovery");
        assert_eq!(results.len(), 1);
        assert!(decoded_matches_original(
            &results[0].pages,
            &make_pages(128, &[1, 2])
        ));
        assert!(results[0].decode_proof.is_none());
        assert_eq!(receiver.buffered_symbol_bytes, 0);
        assert_eq!(receiver.active_decoders(), 0);
        eprintln!(
            "bead_id=bd-3mgq5.2 event=cancel_retry preserved_bytes=128 final_buffered_bytes=0 decoded_pages=2"
        );
    }

    #[test]
    fn test_receiver_repair_work_budget_rolls_back_only_new_symbol() {
        let cx = Cx::new();
        let packets = generate_sender_packets_with_multiplier(128, &[1, 2], 128, 2);
        let first = ReplicationPacket::from_bytes(&packets[0]).expect("source 0");
        assert_eq!(first.k_source, 3);
        let mut receiver = ReplicationReceiver::with_config(ReceiverConfig {
            max_buffered_symbol_bytes: 384,
            ..ReceiverConfig::default()
        });
        for wire in &packets[1..3] {
            receiver
                .process_packet(&cx, wire)
                .expect("admit surviving sources");
        }
        let original = receiver.decoders[&first.changeset_id].symbols.clone();
        assert!(matches!(
            receiver.process_packet(&cx, &packets[3]),
            Err(FrankenError::TooBig)
        ));
        assert_eq!(receiver.decoders[&first.changeset_id].symbols, original);
        assert_eq!(receiver.buffered_symbol_bytes, 256);
        assert_eq!(receiver.received_counts[&first.changeset_id], 2);
        assert!(receiver.pending_results.is_empty());
        assert_eq!(receiver.state(), ReceiverState::Collecting);
        assert_eq!(
            receiver
                .process_packet(&cx, &packets[0])
                .expect("bounded direct source assembly"),
            PacketResult::DecodeReady
        );
        let results = receiver.apply_pending().expect("source assembly");
        assert_eq!(results.len(), 1);
        assert!(decoded_matches_original(
            &results[0].pages,
            &make_pages(128, &[1, 2])
        ));
        assert_eq!(receiver.buffered_symbol_bytes, 0);
        assert_eq!(receiver.active_decoders(), 0);
        eprintln!(
            "bead_id=bd-3mgq5.2 event=repair_budget rejected_symbol=3 preserved_bytes=256 source_retry=decoded"
        );
    }

    #[test]
    fn test_receiver_oversized_esi_cannot_poison_valid_transfer() {
        let cx = Cx::new();
        let packets = generate_sender_packets(128, &[1, 2], 128);
        let first = ReplicationPacket::from_bytes(&packets[0]).expect("source packet");
        for (esi, parsed) in [(1_000_001, false), (u32::MAX, true)] {
            let mut receiver = receiver_with_decode_proofs();
            let mut invalid = first.clone();
            invalid.esi = esi;
            let result = if parsed {
                receiver.process_parsed_packet(&cx, &invalid)
            } else {
                receiver.process_packet(&cx, &invalid.to_bytes().expect("ESI fits wire encoding"))
            };
            assert!(
                matches!(result, Err(FrankenError::OutOfRange { .. })),
                "esi={esi} parsed={parsed}"
            );
            assert_eq!(receiver.state(), ReceiverState::Listening);
            assert_eq!(receiver.active_decoders(), 0);
            assert_eq!(receiver.buffered_symbol_bytes, 0);
            assert!(receiver.received_counts.is_empty());
            assert!(receiver.pending_results.is_empty());
            assert!(receiver.decode_audit_entries().is_empty());
            for wire in &packets {
                receiver
                    .process_packet(&cx, wire)
                    .expect("same receiver accepts valid transfer");
            }
            let results = receiver
                .apply_pending()
                .expect("valid pages after invalid ESI");
            assert_eq!(results.len(), 1);
            assert!(decoded_matches_original(
                &results[0].pages,
                &make_pages(128, &[1, 2])
            ));
            assert_eq!(receiver.buffered_symbol_bytes, 0);
            assert_eq!(receiver.active_decoders(), 0);
            eprintln!(
                "bead_id=bd-3mgq5.2 event=esi_admission esi={esi} parsed={parsed} invalid_admitted=false valid_retry=decoded"
            );
        }
    }

    #[test]
    fn test_receiver_unordered_changeset_is_rejected_before_valid_retry() {
        let cx = Cx::new();
        let mut pages = make_pages(64, &[1, 2]);
        let mut changeset = encode_changeset(64, &mut pages).expect("changeset");
        let entry_size = 12 + 64;
        let split = CHANGESET_HEADER_SIZE + entry_size;
        assert_eq!(changeset.len(), CHANGESET_HEADER_SIZE + 2 * entry_size);
        let first_entry = changeset[CHANGESET_HEADER_SIZE..split].to_vec();
        changeset.copy_within(split..split + entry_size, CHANGESET_HEADER_SIZE);
        changeset[split..].copy_from_slice(&first_entry);
        // Hash the malformed order itself: this is not a transport/hash rejection.
        let id = compute_changeset_id(&changeset);
        let k = u32::try_from(changeset.len().div_ceil(64)).expect("K fits");
        let mut receiver = receiver_with_decode_proofs();
        for (index, chunk) in changeset.chunks(64).enumerate() {
            let mut payload = vec![0; 64];
            payload[..chunk.len()].copy_from_slice(chunk);
            let esi = u32::try_from(index).expect("ESI fits");
            let packet = make_packet(id, 0, esi, k, payload);
            let result = receiver.process_packet(&cx, &packet.to_bytes().expect("wire"));
            if esi + 1 == k {
                assert!(
                    matches!(result, Err(FrankenError::DatabaseCorrupt { .. })),
                    "unordered self-consistent changeset must fail"
                );
            } else {
                assert_eq!(result.expect("partial source"), PacketResult::Accepted);
            }
        }
        assert_eq!(receiver.state(), ReceiverState::Listening);
        assert_eq!(receiver.buffered_symbol_bytes, 0);
        assert_eq!(receiver.active_decoders(), 0);
        assert!(receiver.pending_results.is_empty());
        assert!(receiver.decode_audit_entries().is_empty());
        for wire in generate_sender_packets(64, &[1, 2], 64) {
            receiver
                .process_packet(&cx, &wire)
                .expect("valid transfer after unordered data");
        }
        let results = receiver.apply_pending().expect("valid ordered transfer");
        assert_eq!(results.len(), 1);
        assert!(decoded_matches_original(&results[0].pages, &pages));
        assert_eq!(receiver.buffered_symbol_bytes, 0);
        assert_eq!(receiver.active_decoders(), 0);
        eprintln!(
            "bead_id=bd-3mgq5.2 event=unordered_changeset invalid_applied=false valid_retry=decoded"
        );
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn test_replication_sender_repairs_every_shard_of_one_large_input() {
        use std::collections::BTreeMap;

        use crate::replication_sender::{MAX_REPLICATION_SYMBOL_SIZE, max_pages_per_repair_block};

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
                encode_changeset(page_size, &mut originals).expect("complete shard changeset");
            assert!(
                expected_objects
                    .insert(*compute_changeset_id(&bytes).as_bytes(), originals)
                    .is_none()
            );
        }
        assert_eq!(expected_objects.len(), 3);

        let mut sender = ReplicationSender::new();
        sender
            .prepare(
                page_size,
                &mut pages,
                SenderConfig {
                    symbol_size,
                    max_isi_multiplier: 8,
                },
            )
            .expect("one large sender input");
        sender.start_streaming().expect("stream shards");
        let mut packets_by_object: BTreeMap<[u8; 16], Vec<ReplicationPacket>> = BTreeMap::new();
        while let Some(packet) = sender.next_packet(&cx).expect("bounded shard encoding") {
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
            // Fixed adjacent reversal, followed by round-robin object delivery.
            for pair in packets.chunks_mut(2) {
                if pair.len() == 2 {
                    pair.swap(0, 1);
                }
            }
        }

        let mut receiver = receiver_with_decode_proofs();
        let mut completed = HashSet::new();
        let mut recovered = BTreeMap::new();
        let rounds = packets_by_object
            .values()
            .map(Vec::len)
            .max()
            .expect("three streams");
        for round in 0..rounds {
            for (id, packets) in packets_by_object.iter().rev() {
                // DecodeReady is the object's ACK boundary; stop forwarding its surplus repairs.
                if completed.contains(id) {
                    continue;
                }
                let Some(packet) = packets.get(round) else {
                    continue;
                };
                assert_ne!(packet.esi, 0);
                let outcome = receiver
                    .process_packet(&cx, &packet.to_bytes().expect("wire packet"))
                    .expect("shard receive");
                if outcome == PacketResult::DecodeReady {
                    let results = receiver.apply_pending().expect("drain one repaired object");
                    assert_eq!(results.len(), 1);
                    let result = &results[0];
                    assert_eq!(result.changeset_id.as_bytes(), id);
                    assert!(completed.insert(*id), "each object is drained once");
                    assert_genuine_repair(result, &expected_objects[id]);
                    for page in &result.pages {
                        assert!(
                            recovered
                                .insert(page.page_number, page.page_data.clone())
                                .is_none(),
                            "page repeated across shards"
                        );
                    }
                    eprintln!(
                        "bead_id=bd-3mgq5.2 event=single_input_replication_shard object={id:?} round={round} k={} erased_source_esi=0 recovered_pages={}",
                        packet.k_source,
                        result.pages.len()
                    );
                }
            }
            if round == 0 {
                assert!(
                    receiver.active_decoders() >= 2,
                    "the full objects must overlap in collection"
                );
            }
        }
        assert_eq!(
            completed.len(),
            3,
            "both full blocks and short tail must recover"
        );
        assert_eq!(recovered, expected);
        let mut block_sizes: Vec<_> = expected_objects.values().map(Vec::len).collect();
        block_sizes.sort_unstable();
        assert_eq!(block_sizes, [1, block_pages, block_pages]);
        assert_eq!(receiver.applied_count(), 3);
        assert_eq!(receiver.active_decoders(), 0);
        assert_eq!(receiver.buffered_symbol_bytes, 0);
        assert!(receiver.pending_results.is_empty());
    }

    #[test]
    fn test_receiver_pending_repaired_changeset_survives_incomplete_peer() {
        let cx = Cx::new();
        let mut receiver = receiver_with_decode_proofs();
        let expected_a = make_pages(128, &[7, 11]);
        for wire in generate_sender_packets_with_multiplier(128, &[7, 11], 128, 32) {
            let packet = ReplicationPacket::from_bytes(&wire).expect("A packet");
            if packet.esi == 0 {
                continue;
            }
            if receiver.process_packet(&cx, &wire).expect("repair A") == PacketResult::DecodeReady {
                break;
            }
        }
        assert_eq!(receiver.pending_results.len(), 1);
        assert_genuine_repair(&receiver.pending_results[0], &expected_a);
        let proof_a = receiver.pending_results[0].decode_proof.clone();
        let id_a = receiver.pending_results[0].changeset_id;
        let packets_b = generate_sender_packets(128, &[21, 34], 128);
        let id_b = ReplicationPacket::from_bytes(&packets_b[0])
            .expect("B packet")
            .changeset_id;
        assert_ne!(id_a, id_b);
        assert_eq!(
            receiver
                .process_packet(&cx, &packets_b[0])
                .expect("incomplete B"),
            PacketResult::Accepted
        );
        assert_eq!(receiver.active_decoders(), 1);
        assert_eq!(receiver.buffered_symbol_bytes, 128);
        let results_a = receiver
            .apply_pending()
            .expect("A remains drainable while B collects");
        assert_eq!(results_a.len(), 1);
        assert_eq!(results_a[0].changeset_id, id_a);
        assert_eq!(results_a[0].decode_proof, proof_a);
        assert!(decoded_matches_original(&results_a[0].pages, &expected_a));
        assert_eq!(receiver.applied_count(), 1);
        assert_eq!(receiver.state(), ReceiverState::Collecting);
        assert_eq!(receiver.buffered_symbol_bytes, 128);
        assert!(
            receiver.apply_pending().is_err(),
            "A must drain exactly once"
        );
        assert_eq!(receiver.applied_count(), 1);
        for wire in &packets_b[1..] {
            receiver
                .process_packet(&cx, wire)
                .expect("continue B after draining A");
        }
        let results_b = receiver.apply_pending().expect("drain B");
        assert_eq!(results_b.len(), 1);
        assert_eq!(results_b[0].changeset_id, id_b);
        assert!(decoded_matches_original(
            &results_b[0].pages,
            &make_pages(128, &[21, 34])
        ));
        assert!(results_b[0].decode_proof.is_none());
        assert_eq!(receiver.applied_count(), 2);
        assert_eq!(receiver.buffered_symbol_bytes, 0);
        assert_eq!(receiver.active_decoders(), 0);
        assert!(receiver.pending_results.is_empty());
        eprintln!(
            "bead_id=bd-3mgq5.2 event=interleaved_drain a_repaired=true a_drains=1 b_drains=1 final_buffered_bytes=0"
        );
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
            .process_packet(&Cx::new(), &wire)
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
                .process_packet(&Cx::new(), pkt)
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
        let packets = generate_sender_packets_with_multiplier(64, &[7], 64, 161);
        let p1 = ReplicationPacket::from_bytes(&packets[311]).expect("repair 311");
        let p2 = ReplicationPacket::from_bytes(&packets[320]).expect("repair 320");
        let changeset_id = p1.changeset_id;
        assert_eq!(p1.k_source, 2);
        assert_eq!((p1.esi, p2.esi), (311, 320));
        let decoder = InactivationDecoder::new(2, 64, p1.seed);
        let mut equations = decoder.constraint_symbols();
        // Frozen dependent RFC equations, not arbitrary bytes or a successful seed search.
        for packet in [&p1, &p2] {
            let (mut columns, coefficients) =
                decoder.repair_equation(packet.esi).expect("equation");
            assert!(coefficients.iter().all(|coefficient| coefficient.0 == 1));
            columns.sort_unstable();
            assert_eq!(columns, [9, 13, 19, 20, 25]);
            equations.push(ReceivedSymbol::repair(
                packet.esi,
                columns,
                coefficients,
                packet.symbol_data.clone(),
            ));
        }
        assert_eq!(p1.symbol_data, p2.symbol_data);
        let rank = decoder.rank_status(&equations).expect("rank status");
        assert!(rank.rank < rank.columns && rank.deficit > 0);

        let r1 = receiver
            .process_parsed_packet(&Cx::new(), &p1)
            .expect("first packet");
        assert_eq!(r1, PacketResult::Accepted);
        let r2 = receiver
            .process_parsed_packet(&Cx::new(), &p2)
            .expect("second packet");
        assert_eq!(r2, PacketResult::NeedMore);

        let audit = receiver.take_decode_audit_entries();
        assert_eq!(audit.len(), 1, "bead_id=bd-faz4 case=failure_proof_emitted");
        let proof = &audit[0].proof;
        assert!(
            !proof.decode_success,
            "bead_id=bd-faz4 case=failure_proof_decode_success_false"
        );
        assert_eq!(proof.changeset_id, Some(*changeset_id.as_bytes()));
        assert_eq!(
            proof.intermediate_rank,
            Some(u32::try_from(rank.rank).expect("rank fits"))
        );
        assert_eq!(proof.symbols_received, [311, 320]);
        assert!(proof.source_esis.is_empty());
        assert!(receiver.pending_results.is_empty());
        assert_eq!(receiver.buffered_symbol_bytes, 128);
        assert_eq!(receiver.active_decoders(), 1);
        assert_eq!(receiver.state(), ReceiverState::Collecting);
        assert!(
            proof.is_consistent(),
            "bead_id=bd-faz4 case=failure_proof_consistent"
        );
        eprintln!(
            "bead_id=bd-3mgq5.2 event=rank_deficient esis=311,320 rank={} columns={} deficit={}",
            rank.rank, rank.columns, rank.deficit
        );
        receiver.force_reset();
        assert_eq!(receiver.buffered_symbol_bytes, 0);
        assert_eq!(receiver.active_decoders(), 0);
    }

    #[test]
    fn test_receiver_decode_success_with_repair_emits_proof_when_enabled() {
        let original = make_pages(128, &[7, 11]);
        let packets = generate_sender_packets_with_multiplier(128, &[7, 11], 128, 32);
        let surviving: Vec<_> = packets
            .into_iter()
            .enumerate()
            .filter_map(|(index, wire)| {
                let packet = ReplicationPacket::from_bytes(&wire).expect("sender packet");
                (packet.esi != 0).then_some((u32::try_from(index).expect("index fits"), wire))
            })
            .collect();
        let source_only: Vec<_> = surviving
            .iter()
            .filter(|(_, wire)| {
                ReplicationPacket::from_bytes(wire)
                    .expect("sender packet")
                    .is_source_symbol()
            })
            .cloned()
            .collect();
        let (without_repair, erasures, errors) = decode_from_wire_packets(&source_only, None);
        assert!(
            without_repair.is_none(),
            "permanent erasure cannot decode without repairs"
        );
        assert_eq!((erasures, errors), (0, 0));
        let (decoded, erasures, errors) = decode_from_wire_packets(&surviving, None);
        assert_eq!((erasures, errors), (0, 0));
        assert_genuine_repair(
            &decoded.expect("repair must recover absent source ESI 0"),
            &original,
        );
    }

    #[test]
    fn test_receiver_source_assembly_does_not_claim_unused_repair() {
        let cx = Cx::new();
        let packets = generate_sender_packets(64, &[7], 64);
        let source: Vec<_> = packets
            .iter()
            .map(|wire| ReplicationPacket::from_bytes(wire).expect("source"))
            .collect();
        assert_eq!(source.len(), 2);
        let payloads: Vec<_> = source
            .iter()
            .map(|packet| packet.symbol_data.clone())
            .collect();
        let encoder = SystematicEncoder::new(&payloads, 64, source[0].seed).expect("real encoder");
        let decoder = InactivationDecoder::new(2, 64, source[0].seed);
        // This fixed repair equation equals source ESI 0. It cannot recover source ESI 1.
        let mut source_columns = decoder.source_equation(0).0;
        let mut repair_columns = decoder.repair_equation(26_345).expect("repair equation").0;
        source_columns.sort_unstable();
        repair_columns.sort_unstable();
        assert_eq!(source_columns, [9, 13, 18, 23]);
        assert_eq!(source_columns, repair_columns);
        let redundant = make_packet(
            source[0].changeset_id,
            0,
            26_345,
            2,
            encoder.repair_symbol(26_345),
        );
        assert_eq!(redundant.symbol_data, source[0].symbol_data);
        let mut receiver = receiver_with_decode_proofs();
        assert_eq!(
            receiver.process_packet(&cx, &packets[0]).expect("source 0"),
            PacketResult::Accepted
        );
        assert_eq!(
            receiver
                .process_parsed_packet(&cx, &redundant)
                .expect("dependent repair"),
            PacketResult::NeedMore
        );
        assert_eq!(
            receiver.process_packet(&cx, &packets[1]).expect("source 1"),
            PacketResult::DecodeReady
        );
        let result = receiver.apply_pending().expect("source assembly");
        assert_eq!(result.len(), 1);
        assert!(decoded_matches_original(
            &result[0].pages,
            &make_pages(64, &[7])
        ));
        assert!(
            result[0].decode_proof.is_none(),
            "unused repair is not repair success"
        );
        assert!(
            receiver
                .decode_audit_entries()
                .iter()
                .all(|entry| !entry.proof.decode_success)
        );
        assert_eq!(receiver.buffered_symbol_bytes, 0);
        assert_eq!(receiver.active_decoders(), 0);
    }

    #[test]
    fn test_receiver_decode_success_truncation() {
        let page_size = 128_u32;
        let mut receiver = ReplicationReceiver::new();
        let packets = generate_sender_packets(page_size, &[1], 128);

        for pkt in &packets {
            let _ = receiver.process_packet(&Cx::new(), pkt);
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
            let _ = receiver.process_packet(&Cx::new(), pkt);
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
                if matches!(
                    receiver.process_packet(&Cx::new(), pkt),
                    Ok(PacketResult::DecodeReady)
                ) {
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

        let r1 = receiver
            .process_parsed_packet(&Cx::new(), &p1)
            .expect("first");
        assert_eq!(
            r1,
            PacketResult::Accepted,
            "bead_id={TEST_BEAD_ID} case=dedup_first_accepted"
        );

        for _ in 0..5 {
            let r = receiver
                .process_parsed_packet(&Cx::new(), &p1)
                .expect("duplicate");
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
        let err = receiver.process_packet(&Cx::new(), &oversized).unwrap_err();
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
            let _ = receiver.process_packet(&Cx::new(), pkt);
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
            if matches!(
                receiver.process_packet(&Cx::new(), pkt),
                Ok(PacketResult::DecodeReady)
            ) {
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
        while let Some(packet) = sender.next_packet(&Cx::new()).expect("next") {
            wire_packets.push(packet.to_bytes().expect("encode"));
        }

        // Feed to receiver.
        let mut receiver = ReplicationReceiver::new();
        assert_eq!(receiver.state(), ReceiverState::Listening);

        let mut last_result = PacketResult::Accepted;
        for pkt in &wire_packets {
            let result = receiver
                .process_packet(&Cx::new(), pkt)
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
        let packets = generate_sender_packets_with_multiplier(page_size, &page_numbers, 128, 32);
        let loss_packets: Vec<Vec<u8>> = packets
            .iter()
            .filter(|wire| ReplicationPacket::from_bytes(wire).expect("packet").esi != 0)
            .cloned()
            .collect();

        for loss_rate in [0.05_f64, 0.30_f64] {
            for seed in [1_u64, 7, 42] {
                let mut config = SimTransportConfig::deterministic(seed);
                config.loss_rate = loss_rate;
                config.preserve_order = true;

                let delivery = transmit_packets_simnetwork(config, &loss_packets);
                assert!(
                    delivery.delivered.len() < delivery.sent_count,
                    "seed={seed} loss_rate={loss_rate} must exercise transport loss"
                );
                let (decoded, erasures, parse_errors) =
                    decode_from_wire_packets(&delivery.delivered, None);
                assert_eq!((erasures, parse_errors), (0, 0));
                eprintln!(
                    "bead_id=bd-xgoe event=fixed_loss seed={seed} loss_rate={loss_rate} sent={} delivered={} source_0=permanently_erased",
                    delivery.sent_count,
                    delivery.delivered.len()
                );
                assert_genuine_repair(
                    &decoded.expect("fixed loss schedule must recover"),
                    &original_pages,
                );
            }
        }
    }

    #[test]
    fn test_simnetwork_reorder_and_dup_converge() {
        let page_size = 128_u32;
        let page_numbers = [7_u32, 11];
        let original_pages = make_pages(page_size, &page_numbers);
        let packets: Vec<_> =
            generate_sender_packets_with_multiplier(page_size, &page_numbers, 128, 32)
                .into_iter()
                .filter(|wire| ReplicationPacket::from_bytes(wire).expect("packet").esi != 0)
                .collect();

        for seed in [1_u64, 7, 42] {
            let mut config = SimTransportConfig::deterministic(seed);
            config.preserve_order = false;
            config.duplication_rate = 1.0;

            let delivery = transmit_packets_simnetwork(config, &packets);
            assert!(
                has_duplicate_esies(&delivery),
                "seed={seed}: no duplication exercised"
            );
            assert!(
                has_reordered_esies(&delivery),
                "seed={seed}: no reordering exercised"
            );
            let (decoded, erasures, parse_errors) =
                decode_from_wire_packets(&delivery.delivered, None);
            assert_eq!((erasures, parse_errors), (0, 0));
            eprintln!(
                "bead_id=bd-xgoe event=fixed_reorder_dup seed={seed} sent={} delivered={} source_0=permanently_erased",
                delivery.sent_count,
                delivery.delivered.len()
            );
            assert_genuine_repair(
                &decoded.expect("fixed reorder/dup schedule must recover"),
                &original_pages,
            );
        }
    }

    #[test]
    fn test_simnetwork_corruption_is_rejected_and_recovered() {
        let page_size = 128_u32;
        let page_numbers = [21_u32, 34];
        let original_pages = make_pages(page_size, &page_numbers);
        let auth_key = [0xA5; 32];
        let packets: Vec<_> =
            generate_sender_packets_with_multiplier(page_size, &page_numbers, 128, 32)
                .into_iter()
                .filter_map(|wire| {
                    let mut packet = ReplicationPacket::from_bytes(&wire).expect("packet");
                    if packet.esi == 0 {
                        return None;
                    }
                    packet.attach_auth_tag(&auth_key);
                    Some(packet.to_bytes().expect("authenticated packet"))
                })
                .collect();
        let mut corrupt_repair = packets
            .iter()
            .find(|wire| {
                !ReplicationPacket::from_bytes(wire)
                    .expect("packet")
                    .is_source_symbol()
            })
            .expect("repair packet")
            .clone();
        *corrupt_repair.last_mut().expect("payload byte") ^= 0x80;

        for seed in [1_u64, 7, 42] {
            let mut config = SimTransportConfig::deterministic(seed);
            config.corruption_rate = 0.20;
            config.preserve_order = false;

            let mut delivery = transmit_packets_simnetwork(config, &packets);
            assert!(
                has_corrupted_wire_bytes(&delivery, &packets),
                "seed={seed}: no simulated corruption"
            );
            // Deterministic first rejection, even if decode finishes before later corrupt packets.
            delivery
                .delivered
                .insert(0, (u32::MAX, corrupt_repair.clone()));
            let (decoded, erasures, parse_errors) =
                decode_from_wire_packets(&delivery.delivered, Some(auth_key));
            assert!(
                erasures > 0,
                "corrupt repair must be rejected before decode"
            );
            eprintln!(
                "bead_id=bd-xgoe event=fixed_corruption seed={seed} sent={} delivered={} erasures={erasures} parse_errors={parse_errors} source_0=permanently_erased",
                delivery.sent_count,
                delivery.delivered.len()
            );
            assert_genuine_repair(
                &decoded.expect("fixed corruption schedule must recover"),
                &original_pages,
            );
        }
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
                    receiver
                        .process_packet(&Cx::new(), &wire)
                        .expect("receiver process"),
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
            let _ = receiver.process_packet(&Cx::new(), pkt);
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
