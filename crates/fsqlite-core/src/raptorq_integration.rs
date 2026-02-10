//! §3.3 Asupersync RaptorQ Pipeline Integration (bd-1hi.5).
//!
//! This module provides the FrankenSQLite-side wrapper types for the
//! asupersync RaptorQ pipeline.  Production code uses abstract traits
//! (`PageSymbolSink`, `PageSymbolSource`, `SymbolCodec`) so that the
//! actual asupersync dependency remains dev-only.
//!
//! # Cx Cancellation
//!
//! All long-running encode/decode loops call `cx.checkpoint()` every
//! `checkpoint_interval` symbols (§4.12.1).  If the context is cancelled
//! the operation returns `FrankenError::Abort`.

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{ObjectId, cx::Cx};
use tracing::{debug, error, info, warn};
use xxhash_rust::xxh3::xxh3_64;

use crate::decode_proofs::EcsDecodeProof;

const BEAD_ID: &str = "bd-1hi.5";

// ---------------------------------------------------------------------------
// Pipeline Configuration (§3.3)
// ---------------------------------------------------------------------------

/// Minimum allowed symbol size (bytes).
pub const MIN_PIPELINE_SYMBOL_SIZE: u32 = 512;

/// Maximum allowed symbol size (bytes).
pub const MAX_PIPELINE_SYMBOL_SIZE: u32 = 65_536;

/// Default Cx checkpoint interval (symbols between cancellation checks).
pub const DEFAULT_CHECKPOINT_INTERVAL: u32 = 64;

/// Policy surface for decode-proof emission hooks.
///
/// This keeps proof generation optional in production while allowing
/// durability paths and tests to request deterministic proof artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeProofEmissionPolicy {
    /// Emit proof records for decode failures.
    pub emit_on_decode_failure: bool,
    /// Emit proof records for successful decodes that required repair symbols.
    pub emit_on_repair_success: bool,
}

impl DecodeProofEmissionPolicy {
    /// Default production posture: proof emission disabled.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            emit_on_decode_failure: false,
            emit_on_repair_success: false,
        }
    }

    /// Durability-focused posture for replication/WAL-style decode paths.
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

/// FrankenSQLite-side RaptorQ pipeline configuration (§3.3).
///
/// Mirrors the needed subset of asupersync's `RaptorQConfig` so that
/// production code does not depend on asupersync directly.
#[derive(Debug, Clone, PartialEq)]
pub struct PipelineConfig {
    /// Symbol size T in bytes.  Must be a power of two in
    /// `[MIN_PIPELINE_SYMBOL_SIZE, MAX_PIPELINE_SYMBOL_SIZE]`.
    pub symbol_size: u32,
    /// Maximum source block size (max K per source block) in bytes.
    pub max_block_size: u32,
    /// Repair overhead factor.  E.g. `1.25` means 25 % extra repair symbols.
    pub repair_overhead: f64,
    /// Symbols between `Cx::checkpoint()` calls (§4.12.1).
    pub checkpoint_interval: u32,
    /// Decode-proof emission policy hooks (§3.5.8 / bd-faz4).
    pub decode_proof_policy: DecodeProofEmissionPolicy,
}

impl PipelineConfig {
    /// Create a configuration for page-sized symbols (T = page_size).
    #[must_use]
    pub fn for_page_size(page_size: u32) -> Self {
        Self {
            symbol_size: page_size,
            max_block_size: 64 * 1024,
            repair_overhead: 1.25,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            decode_proof_policy: DecodeProofEmissionPolicy::default(),
        }
    }

    /// Validate this configuration.
    ///
    /// Rejects:
    /// - `symbol_size == 0`
    /// - `symbol_size` not a power of two
    /// - `symbol_size` outside `[MIN, MAX]`
    /// - `max_block_size == 0`
    /// - `repair_overhead < 1.0`
    /// - `checkpoint_interval == 0`
    pub fn validate(&self) -> Result<()> {
        if self.symbol_size == 0 {
            return Err(FrankenError::OutOfRange {
                what: "pipeline symbol_size".to_owned(),
                value: "0".to_owned(),
            });
        }
        if !self.symbol_size.is_power_of_two() {
            return Err(FrankenError::OutOfRange {
                what: "pipeline symbol_size (must be power of 2)".to_owned(),
                value: self.symbol_size.to_string(),
            });
        }
        if self.symbol_size < MIN_PIPELINE_SYMBOL_SIZE
            || self.symbol_size > MAX_PIPELINE_SYMBOL_SIZE
        {
            return Err(FrankenError::OutOfRange {
                what: format!(
                    "pipeline symbol_size (must be in [{MIN_PIPELINE_SYMBOL_SIZE}, {MAX_PIPELINE_SYMBOL_SIZE}])"
                ),
                value: self.symbol_size.to_string(),
            });
        }
        if self.max_block_size == 0 {
            return Err(FrankenError::OutOfRange {
                what: "pipeline max_block_size".to_owned(),
                value: "0".to_owned(),
            });
        }
        if self.repair_overhead < 1.0 {
            return Err(FrankenError::OutOfRange {
                what: "pipeline repair_overhead (must be >= 1.0)".to_owned(),
                value: self.repair_overhead.to_string(),
            });
        }
        if self.checkpoint_interval == 0 {
            return Err(FrankenError::OutOfRange {
                what: "pipeline checkpoint_interval".to_owned(),
                value: "0".to_owned(),
            });
        }
        Ok(())
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self::for_page_size(4096)
    }
}

// ---------------------------------------------------------------------------
// Page Symbol Sink / Source Traits (§3.3)
// ---------------------------------------------------------------------------

/// Writes encoded page symbols to WAL/ECS storage.
pub trait PageSymbolSink {
    /// Write a single encoded symbol.
    fn write_symbol(&mut self, esi: u32, data: &[u8]) -> Result<()>;

    /// Flush all buffered symbols to durable storage.
    fn flush(&mut self) -> Result<()>;

    /// Number of symbols written so far.
    fn written_count(&self) -> u32;
}

/// Reads symbols from WAL/ECS storage for decoding.
pub trait PageSymbolSource {
    /// Read a symbol by its ESI.  Returns `None` if unavailable (erased).
    fn read_symbol(&mut self, esi: u32) -> Result<Option<Vec<u8>>>;

    /// All available ESIs in this source.
    fn available_esis(&self) -> Vec<u32>;

    /// Number of available symbols.
    fn available_count(&self) -> u32;
}

// ---------------------------------------------------------------------------
// Symbol Codec Trait (§3.3)
// ---------------------------------------------------------------------------

/// Abstraction over the actual RaptorQ encode/decode engine.
///
/// In production, this wraps asupersync's `RaptorQSenderBuilder` /
/// `RaptorQReceiverBuilder`.  In tests, it may be a mock.
pub trait SymbolCodec: Send + Sync {
    /// Encode source data into source + repair symbols.
    fn encode(
        &self,
        source_data: &[u8],
        symbol_size: u32,
        repair_overhead: f64,
    ) -> Result<CodecEncodeResult>;

    /// Decode from received symbols.
    fn decode(
        &self,
        symbols: &[(u32, Vec<u8>)],
        k_source: u32,
        symbol_size: u32,
    ) -> Result<CodecDecodeResult>;
}

/// Raw encode result from the codec.
#[derive(Debug, Clone)]
pub struct CodecEncodeResult {
    /// Source symbols: `(esi, data)`.
    pub source_symbols: Vec<(u32, Vec<u8>)>,
    /// Repair symbols: `(esi, data)`.
    pub repair_symbols: Vec<(u32, Vec<u8>)>,
    /// Number of source symbols K.
    pub k_source: u32,
}

/// Raw decode result from the codec.
#[derive(Debug, Clone)]
pub enum CodecDecodeResult {
    /// Decode succeeded.
    Success {
        /// Recovered source data.
        data: Vec<u8>,
        /// Number of symbols consumed.
        symbols_used: u32,
        /// Symbols resolved by peeling.
        peeled_count: u32,
        /// Symbols resolved by Gaussian elimination (inactive subsystem).
        inactivated_count: u32,
    },
    /// Decode failed.
    Failure {
        /// Reason for failure.
        reason: DecodeFailureReason,
        /// Number of symbols that were received.
        symbols_received: u32,
        /// Source symbols required.
        k_required: u32,
    },
}

// ---------------------------------------------------------------------------
// Outcome Types (§3.3)
// ---------------------------------------------------------------------------

/// Result of a pipeline encode operation.
#[derive(Debug, Clone)]
pub struct EncodeOutcome {
    /// Number of source symbols produced.
    pub source_count: u32,
    /// Number of repair symbols produced.
    pub repair_count: u32,
    /// Symbol size in bytes.
    pub symbol_size: u32,
}

/// Result of a pipeline decode operation.
#[derive(Debug, Clone)]
pub enum DecodeOutcome {
    /// Successful decode with recovered pages.
    Success(DecodeSuccess),
    /// Failed decode with diagnostic information.
    Failure(DecodeFailure),
}

/// Successful decode metadata.
#[derive(Debug, Clone)]
pub struct DecodeSuccess {
    /// Recovered page data, concatenated.
    pub data: Vec<u8>,
    /// Number of symbols used for decoding.
    pub symbols_used: u32,
    /// Symbols resolved during the peeling phase.
    pub peeled_count: u32,
    /// Symbols resolved during the Gaussian elimination phase.
    pub inactivated_count: u32,
    /// Optional decode proof emitted under policy control.
    pub decode_proof: Option<EcsDecodeProof>,
}

/// Failed decode metadata.
#[derive(Debug, Clone)]
pub struct DecodeFailure {
    /// Why the decode failed.
    pub reason: DecodeFailureReason,
    /// Number of symbols that were available.
    pub symbols_received: u32,
    /// Source symbols required (K).
    pub k_required: u32,
    /// Optional decode proof emitted under policy control.
    pub decode_proof: Option<EcsDecodeProof>,
}

/// Reasons a decode can fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeFailureReason {
    /// Fewer symbols than K available.
    InsufficientSymbols,
    /// The decoding matrix is singular (rank deficient).
    SingularMatrix,
    /// Symbol sizes do not match the expected T.
    SymbolSizeMismatch,
    /// Cancelled via `Cx::checkpoint()`.
    Cancelled,
}

// ---------------------------------------------------------------------------
// Pipeline Encoder (§3.3)
// ---------------------------------------------------------------------------

/// RaptorQ page encoder that wraps a [`SymbolCodec`] and writes through
/// a [`PageSymbolSink`] with Cx cancellation checkpoints.
pub struct RaptorQPageEncoder<C: SymbolCodec> {
    config: PipelineConfig,
    codec: C,
}

impl<C: SymbolCodec> RaptorQPageEncoder<C> {
    /// Create a new encoder.  Validates the config eagerly.
    pub fn new(config: PipelineConfig, codec: C) -> Result<Self> {
        config.validate()?;
        info!(
            bead_id = BEAD_ID,
            symbol_size = config.symbol_size,
            max_block_size = config.max_block_size,
            repair_overhead = config.repair_overhead,
            "RaptorQ page encoder created"
        );
        Ok(Self { config, codec })
    }

    /// Encode page data and write symbols through the sink.
    ///
    /// `cx.checkpoint()` is called every `checkpoint_interval` symbols.
    #[allow(clippy::cast_possible_truncation)]
    pub fn encode_pages(
        &self,
        cx: &Cx,
        page_data: &[u8],
        sink: &mut dyn PageSymbolSink,
    ) -> Result<EncodeOutcome> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;

        let symbol_size = self.config.symbol_size;
        debug!(
            bead_id = BEAD_ID,
            data_len = page_data.len(),
            symbol_size,
            "starting page encode"
        );

        let result = self
            .codec
            .encode(page_data, symbol_size, self.config.repair_overhead)?;

        // Write source symbols with checkpoints.
        let interval = self.config.checkpoint_interval as usize;
        for (idx, (esi, data)) in result.source_symbols.iter().enumerate() {
            if idx > 0 && idx % interval == 0 {
                cx.checkpoint().map_err(|_| FrankenError::Abort)?;
            }
            sink.write_symbol(*esi, data)?;
        }

        // Write repair symbols with checkpoints.
        for (idx, (esi, data)) in result.repair_symbols.iter().enumerate() {
            if idx > 0 && idx % interval == 0 {
                cx.checkpoint().map_err(|_| FrankenError::Abort)?;
            }
            sink.write_symbol(*esi, data)?;
        }

        sink.flush()?;

        let outcome = EncodeOutcome {
            source_count: result.k_source,
            repair_count: result.repair_symbols.len() as u32,
            symbol_size,
        };

        info!(
            bead_id = BEAD_ID,
            source_count = outcome.source_count,
            repair_count = outcome.repair_count,
            symbol_size = outcome.symbol_size,
            "page encode complete"
        );

        Ok(outcome)
    }

    /// Reference to the pipeline config.
    #[must_use]
    pub const fn config(&self) -> &PipelineConfig {
        &self.config
    }
}

// ---------------------------------------------------------------------------
// Pipeline Decoder (§3.3)
// ---------------------------------------------------------------------------

/// RaptorQ page decoder that wraps a [`SymbolCodec`] and reads from
/// a [`PageSymbolSource`] with Cx cancellation checkpoints.
pub struct RaptorQPageDecoder<C: SymbolCodec> {
    config: PipelineConfig,
    codec: C,
}

impl<C: SymbolCodec> RaptorQPageDecoder<C> {
    /// Create a new decoder.  Validates the config eagerly.
    pub fn new(config: PipelineConfig, codec: C) -> Result<Self> {
        config.validate()?;
        info!(
            bead_id = BEAD_ID,
            symbol_size = config.symbol_size,
            "RaptorQ page decoder created"
        );
        Ok(Self { config, codec })
    }

    /// Decode pages from the source.
    ///
    /// Reads available symbols, delegates to the codec, and returns the
    /// outcome.  Cx checkpoint is called at read boundaries.
    #[allow(clippy::cast_possible_truncation, clippy::too_many_lines)]
    pub fn decode_pages(
        &self,
        cx: &Cx,
        source: &mut dyn PageSymbolSource,
        k_source: u32,
    ) -> Result<DecodeOutcome> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;

        let available = source.available_count();
        debug!(
            bead_id = BEAD_ID,
            k_source, available, "starting page decode"
        );

        if available < k_source {
            warn!(
                bead_id = BEAD_ID,
                k_source, available, "fewer symbols than K_source — decode likely to fail"
            );
        }

        // Collect symbols from source with checkpoints.
        let esis = source.available_esis();
        let interval = self.config.checkpoint_interval as usize;
        let mut symbols = Vec::with_capacity(esis.len());
        for (idx, esi) in esis.iter().enumerate() {
            if idx > 0 && idx % interval == 0 {
                cx.checkpoint().map_err(|_| FrankenError::Abort)?;
            }
            if let Some(data) = source.read_symbol(*esi)? {
                symbols.push((*esi, data));
            }
        }

        // Delegate to codec.
        let codec_result = self
            .codec
            .decode(&symbols, k_source, self.config.symbol_size)?;
        let all_esis = canonical_esis(&symbols);
        let proof_object_id =
            derive_decode_proof_object_id(k_source, self.config.symbol_size, &all_esis);
        let proof_seed = xxh3_64(proof_object_id.as_bytes());

        match codec_result {
            CodecDecodeResult::Success {
                data,
                symbols_used,
                peeled_count,
                inactivated_count,
            } => {
                info!(
                    bead_id = BEAD_ID,
                    k_source,
                    symbols_used,
                    peeled_count,
                    inactivated_count,
                    "page decode succeeded"
                );
                let decode_proof = if self.config.decode_proof_policy.emit_on_repair_success
                    && contains_repair_esi(&all_esis, k_source)
                {
                    let proof = EcsDecodeProof::from_esis(
                        proof_object_id,
                        k_source,
                        &all_esis,
                        true,
                        Some(symbols_used),
                        deterministic_timing_ns(k_source, self.config.symbol_size, symbols_used),
                        proof_seed,
                    );
                    debug!(
                        bead_id = "bd-faz4",
                        symbols_used, k_source, "emitted repair-success decode proof"
                    );
                    Some(proof)
                } else {
                    None
                };
                if symbols_used == k_source {
                    warn!(
                        bead_id = BEAD_ID,
                        k_source,
                        symbols_used,
                        "fragile recovery: decoded with minimum symbol count"
                    );
                }
                Ok(DecodeOutcome::Success(DecodeSuccess {
                    data,
                    symbols_used,
                    peeled_count,
                    inactivated_count,
                    decode_proof,
                }))
            }
            CodecDecodeResult::Failure {
                reason,
                symbols_received,
                k_required,
            } => {
                let decode_proof = if self.config.decode_proof_policy.emit_on_decode_failure {
                    let intermediate_rank = Some(symbols_received.min(k_required));
                    let proof = EcsDecodeProof::from_esis(
                        proof_object_id,
                        k_source,
                        &all_esis,
                        false,
                        intermediate_rank,
                        deterministic_timing_ns(
                            k_source,
                            self.config.symbol_size,
                            symbols_received,
                        ),
                        proof_seed,
                    );
                    debug!(
                        bead_id = "bd-faz4",
                        symbols_received, k_required, "emitted decode-failure proof"
                    );
                    Some(proof)
                } else {
                    None
                };
                error!(
                    bead_id = BEAD_ID,
                    k_source,
                    symbols_received,
                    k_required,
                    reason = ?reason,
                    "page decode failed"
                );
                Ok(DecodeOutcome::Failure(DecodeFailure {
                    reason,
                    symbols_received,
                    k_required,
                    decode_proof,
                }))
            }
        }
    }

    /// Reference to the pipeline config.
    #[must_use]
    pub const fn config(&self) -> &PipelineConfig {
        &self.config
    }
}

fn canonical_esis(symbols: &[(u32, Vec<u8>)]) -> Vec<u32> {
    let mut esis: Vec<u32> = symbols.iter().map(|(esi, _)| *esi).collect();
    esis.sort_unstable();
    esis.dedup();
    esis
}

fn contains_repair_esi(esis: &[u32], k_source: u32) -> bool {
    esis.iter().any(|&esi| esi >= k_source)
}

fn derive_decode_proof_object_id(k_source: u32, symbol_size: u32, esis: &[u32]) -> ObjectId {
    let mut material = Vec::with_capacity(40 + esis.len() * 4);
    material.extend_from_slice(b"fsqlite:raptorq:decode-proof:v1");
    material.extend_from_slice(&k_source.to_le_bytes());
    material.extend_from_slice(&symbol_size.to_le_bytes());
    for esi in esis {
        material.extend_from_slice(&esi.to_le_bytes());
    }
    ObjectId::derive_from_canonical_bytes(&material)
}

fn deterministic_timing_ns(k_source: u32, symbol_size: u32, symbols_used: u32) -> u64 {
    let mut material = [0_u8; 12];
    material[..4].copy_from_slice(&k_source.to_le_bytes());
    material[4..8].copy_from_slice(&symbol_size.to_le_bytes());
    material[8..12].copy_from_slice(&symbols_used.to_le_bytes());
    xxh3_64(&material)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use asupersync::error::ErrorKind as AsErrorKind;
    use asupersync::raptorq::RaptorQReceiverBuilder;
    use asupersync::raptorq::RaptorQSenderBuilder;
    use asupersync::security::AuthenticationTag;
    use asupersync::security::authenticated::AuthenticatedSymbol;
    use asupersync::transport::error::{SinkError, StreamError};
    use asupersync::transport::sink::SymbolSink;
    use asupersync::transport::stream::SymbolStream;
    use asupersync::types::{ObjectId as AsObjectId, ObjectParams, Symbol, SymbolId, SymbolKind};
    use asupersync::{Cx as AsCx, RaptorQConfig};

    use super::*;

    // -----------------------------------------------------------------------
    // Mock PageSymbolSink / PageSymbolSource
    // -----------------------------------------------------------------------

    struct VecPageSink {
        symbols: BTreeMap<u32, Vec<u8>>,
        flushed: bool,
    }

    impl VecPageSink {
        fn new() -> Self {
            Self {
                symbols: BTreeMap::new(),
                flushed: false,
            }
        }
    }

    impl PageSymbolSink for VecPageSink {
        fn write_symbol(&mut self, esi: u32, data: &[u8]) -> Result<()> {
            self.symbols.insert(esi, data.to_vec());
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            self.flushed = true;
            Ok(())
        }

        fn written_count(&self) -> u32 {
            self.symbols.len() as u32
        }
    }

    struct VecPageSource {
        symbols: BTreeMap<u32, Vec<u8>>,
    }

    impl VecPageSource {
        fn from_sink(sink: &VecPageSink) -> Self {
            Self {
                symbols: sink.symbols.clone(),
            }
        }

        fn from_map(symbols: BTreeMap<u32, Vec<u8>>) -> Self {
            Self { symbols }
        }
    }

    impl PageSymbolSource for VecPageSource {
        fn read_symbol(&mut self, esi: u32) -> Result<Option<Vec<u8>>> {
            Ok(self.symbols.get(&esi).cloned())
        }

        fn available_esis(&self) -> Vec<u32> {
            self.symbols.keys().copied().collect()
        }

        fn available_count(&self) -> u32 {
            self.symbols.len() as u32
        }
    }

    // -----------------------------------------------------------------------
    // Asupersync-backed SymbolCodec implementation
    // -----------------------------------------------------------------------

    #[derive(Debug)]
    struct VecTransportSink {
        symbols: Vec<Symbol>,
    }

    impl VecTransportSink {
        fn new() -> Self {
            Self {
                symbols: Vec::new(),
            }
        }
    }

    #[derive(Debug)]
    struct VecTransportStream {
        symbols: VecDeque<AuthenticatedSymbol>,
    }

    impl VecTransportStream {
        fn new(symbols: Vec<Symbol>) -> Self {
            let symbols = symbols
                .into_iter()
                .map(|symbol| AuthenticatedSymbol::new_verified(symbol, AuthenticationTag::zero()))
                .collect();
            Self { symbols }
        }
    }

    impl SymbolStream for VecTransportStream {
        fn poll_next(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<std::result::Result<AuthenticatedSymbol, StreamError>>> {
            match self.symbols.pop_front() {
                Some(symbol) => Poll::Ready(Some(Ok(symbol))),
                None => Poll::Ready(None),
            }
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.symbols.len(), Some(self.symbols.len()))
        }

        fn is_exhausted(&self) -> bool {
            self.symbols.is_empty()
        }
    }

    const TEST_OBJECT_ID: u64 = 0xBD_1A15;
    const TEST_MAX_BLOCK_SIZE: usize = 64 * 1024;
    const PACKED_KIND_REPAIR_BIT: u32 = 1_u32 << 31;
    const PACKED_SBN_SHIFT: u32 = 23;
    const PACKED_SBN_MASK: u32 = 0xFF;
    const PACKED_ESI_MASK: u32 = 0x7F_FFFF;

    fn pack_symbol_key(kind: SymbolKind, sbn: u8, esi: u32) -> Result<u32> {
        if esi > PACKED_ESI_MASK {
            return Err(FrankenError::OutOfRange {
                what: "packed symbol esi (must fit 23 bits)".to_owned(),
                value: esi.to_string(),
            });
        }

        let kind_bit = if kind.is_repair() {
            PACKED_KIND_REPAIR_BIT
        } else {
            0
        };
        Ok(kind_bit | (u32::from(sbn) << PACKED_SBN_SHIFT) | esi)
    }

    fn unpack_symbol_key(packed: u32) -> (SymbolKind, u8, u32) {
        let kind = if packed & PACKED_KIND_REPAIR_BIT == 0 {
            SymbolKind::Source
        } else {
            SymbolKind::Repair
        };
        let sbn = ((packed >> PACKED_SBN_SHIFT) & PACKED_SBN_MASK) as u8;
        let esi = packed & PACKED_ESI_MASK;
        (kind, sbn, esi)
    }

    impl SymbolSink for VecTransportSink {
        fn poll_send(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            symbol: AuthenticatedSymbol,
        ) -> Poll<std::result::Result<(), SinkError>> {
            self.symbols.push(symbol.into_symbol());
            Poll::Ready(Ok(()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), SinkError>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), SinkError>> {
            Poll::Ready(Ok(()))
        }

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), SinkError>> {
            Poll::Ready(Ok(()))
        }
    }

    /// SymbolCodec backed by asupersync.
    struct AsupersyncCodec;

    impl SymbolCodec for AsupersyncCodec {
        fn encode(
            &self,
            source_data: &[u8],
            symbol_size: u32,
            repair_overhead: f64,
        ) -> Result<CodecEncodeResult> {
            let mut config = RaptorQConfig::default();
            config.encoding.symbol_size = symbol_size as u16;
            config.encoding.max_block_size = TEST_MAX_BLOCK_SIZE;
            config.encoding.repair_overhead = repair_overhead;

            let cx = AsCx::for_testing();
            let object_id = AsObjectId::new_for_test(TEST_OBJECT_ID);
            let mut sender = RaptorQSenderBuilder::new()
                .config(config)
                .transport(VecTransportSink::new())
                .build()
                .map_err(|e| FrankenError::Internal(format!("sender build: {e}")))?;

            let outcome = sender
                .send_object(&cx, object_id, source_data)
                .map_err(|e| FrankenError::Internal(format!("send_object: {e}")))?;

            let symbols = std::mem::take(&mut sender.transport_mut().symbols);
            let k = outcome.source_symbols as u32;

            let mut source_symbols = Vec::new();
            let mut repair_symbols = Vec::new();
            for s in &symbols {
                let packed_key = pack_symbol_key(s.kind(), s.sbn(), s.esi())?;
                if s.kind().is_source() {
                    source_symbols.push((packed_key, s.data().to_vec()));
                } else {
                    repair_symbols.push((packed_key, s.data().to_vec()));
                }
            }

            Ok(CodecEncodeResult {
                source_symbols,
                repair_symbols,
                k_source: k,
            })
        }

        fn decode(
            &self,
            symbols: &[(u32, Vec<u8>)],
            k_source: u32,
            symbol_size: u32,
        ) -> Result<CodecDecodeResult> {
            if symbols.is_empty() {
                return Ok(CodecDecodeResult::Failure {
                    reason: DecodeFailureReason::InsufficientSymbols,
                    symbols_received: 0,
                    k_required: k_source,
                });
            }

            let object_id = AsObjectId::new_for_test(TEST_OBJECT_ID);
            let mut config = RaptorQConfig::default();
            config.encoding.symbol_size = symbol_size as u16;
            config.encoding.max_block_size = TEST_MAX_BLOCK_SIZE;

            let symbol_size_usize =
                usize::try_from(symbol_size).map_err(|_| FrankenError::OutOfRange {
                    what: "symbol_size as usize".to_owned(),
                    value: symbol_size.to_string(),
                })?;
            let symbols_per_block = u32::try_from((TEST_MAX_BLOCK_SIZE / symbol_size_usize).max(1))
                .map_err(|_| FrankenError::OutOfRange {
                    what: "symbols_per_block as u32".to_owned(),
                    value: (TEST_MAX_BLOCK_SIZE / symbol_size_usize).to_string(),
                })?;
            let source_blocks = k_source.div_ceil(symbols_per_block).max(1);
            let object_size = u64::from(k_source)
                .checked_mul(u64::from(symbol_size))
                .ok_or_else(|| FrankenError::OutOfRange {
                    what: "object_size for decode params".to_owned(),
                    value: format!("{k_source}*{symbol_size}"),
                })?;
            let params = ObjectParams::new(
                object_id,
                object_size,
                u16::try_from(symbol_size).map_err(|_| FrankenError::OutOfRange {
                    what: "symbol_size as u16".to_owned(),
                    value: symbol_size.to_string(),
                })?,
                u8::try_from(source_blocks).map_err(|_| FrankenError::OutOfRange {
                    what: "source_blocks as u8".to_owned(),
                    value: source_blocks.to_string(),
                })?,
                u16::try_from(symbols_per_block).map_err(|_| FrankenError::OutOfRange {
                    what: "symbols_per_block as u16".to_owned(),
                    value: symbols_per_block.to_string(),
                })?,
            );

            let mut rebuilt = Vec::with_capacity(symbols.len());
            for (packed, data) in symbols {
                let (kind, sbn, esi) = unpack_symbol_key(*packed);
                rebuilt.push(Symbol::new(
                    SymbolId::new(object_id, sbn, esi),
                    data.clone(),
                    kind,
                ));
            }

            let cx = AsCx::for_testing();
            let mut receiver = RaptorQReceiverBuilder::new()
                .config(config)
                .source(VecTransportStream::new(rebuilt))
                .build()
                .map_err(|e| FrankenError::Internal(format!("receiver build: {e}")))?;

            match receiver.receive_object(&cx, &params) {
                Ok(outcome) => Ok(CodecDecodeResult::Success {
                    data: outcome.data,
                    symbols_used: outcome.symbols_received as u32,
                    peeled_count: 0,
                    inactivated_count: 0,
                }),
                Err(err) => {
                    let reason = match err.kind() {
                        AsErrorKind::InsufficientSymbols => {
                            DecodeFailureReason::InsufficientSymbols
                        }
                        _ => DecodeFailureReason::SingularMatrix,
                    };
                    Ok(CodecDecodeResult::Failure {
                        reason,
                        symbols_received: symbols.len() as u32,
                        k_required: k_source,
                    })
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn deterministic_page_data(k: usize, symbol_size: usize, seed: u64) -> Vec<u8> {
        let mut state = seed ^ 0x9E37_79B9_7F4A_7C15;
        let total = k * symbol_size;
        let mut out = Vec::with_capacity(total);
        for idx in 0..total {
            state ^= state << 7;
            state ^= state >> 9;
            state = state.wrapping_mul(0xA24B_AED4_963E_E407);
            let idx_byte = (idx % 251) as u8;
            out.push((state & 0xFF) as u8 ^ idx_byte);
        }
        out
    }

    fn test_cx() -> fsqlite_types::cx::Cx {
        fsqlite_types::cx::Cx::new()
    }

    fn default_codec() -> AsupersyncCodec {
        AsupersyncCodec
    }

    fn default_config() -> PipelineConfig {
        PipelineConfig::for_page_size(512)
    }

    // -----------------------------------------------------------------------
    // §3.3 Test 12: Pipeline encode (test_pipeline_encode_async)
    // -----------------------------------------------------------------------

    #[test]
    fn test_pipeline_encode_produces_source_and_repair() {
        let config = default_config();
        let encoder =
            RaptorQPageEncoder::new(config.clone(), default_codec()).expect("encoder build");
        let cx = test_cx();
        let k = 10_usize;
        let data = deterministic_page_data(k, config.symbol_size as usize, 0x1234);

        let mut sink = VecPageSink::new();
        let outcome = encoder
            .encode_pages(&cx, &data, &mut sink)
            .expect("encode must succeed");

        assert_eq!(
            outcome.source_count as usize, k,
            "bead_id={BEAD_ID} case=encode_source_count"
        );
        assert!(
            outcome.repair_count > 0,
            "bead_id={BEAD_ID} case=encode_repair_present"
        );
        assert_eq!(
            outcome.symbol_size, config.symbol_size,
            "bead_id={BEAD_ID} case=encode_symbol_size"
        );
        assert!(sink.flushed, "bead_id={BEAD_ID} case=encode_sink_flushed");

        // Verify source symbols contain original page data.
        let sym_size = config.symbol_size as usize;
        for i in 0..k {
            let esi = i as u32;
            let expected = &data[i * sym_size..(i + 1) * sym_size];
            let actual = sink
                .symbols
                .get(&esi)
                .unwrap_or_else(|| panic!("source symbol ESI {esi} missing"));
            assert_eq!(
                actual, expected,
                "bead_id={BEAD_ID} case=encode_source_symbol_matches esi={esi}"
            );
        }

        info!(
            bead_id = BEAD_ID,
            source_count = outcome.source_count,
            repair_count = outcome.repair_count,
            total_written = sink.written_count(),
            "test_pipeline_encode complete"
        );
    }

    // -----------------------------------------------------------------------
    // §3.3 Test 13: Pipeline decode (test_pipeline_decode_async)
    // -----------------------------------------------------------------------

    #[test]
    fn test_pipeline_decode_with_extra_symbols() {
        let config = default_config();
        let encoder =
            RaptorQPageEncoder::new(config.clone(), default_codec()).expect("encoder build");
        let decoder =
            RaptorQPageDecoder::new(config.clone(), default_codec()).expect("decoder build");
        let cx = test_cx();
        let k = 10_usize;
        let data = deterministic_page_data(k, config.symbol_size as usize, 0x5678);

        // Encode.
        let mut sink = VecPageSink::new();
        let outcome = encoder
            .encode_pages(&cx, &data, &mut sink)
            .expect("encode must succeed");

        // Decode from all symbols (K + repair).
        let mut source = VecPageSource::from_sink(&sink);
        let decode_outcome = decoder
            .decode_pages(&cx, &mut source, outcome.source_count)
            .expect("decode must succeed");

        match decode_outcome {
            DecodeOutcome::Success(success) => {
                assert_eq!(
                    success.data, data,
                    "bead_id={BEAD_ID} case=decode_roundtrip_bytes"
                );
                assert!(
                    success.symbols_used >= outcome.source_count,
                    "bead_id={BEAD_ID} case=decode_symbols_used"
                );
                info!(
                    bead_id = BEAD_ID,
                    symbols_used = success.symbols_used,
                    peeled = success.peeled_count,
                    inactivated = success.inactivated_count,
                    "test_pipeline_decode complete"
                );
            }
            DecodeOutcome::Failure(failure) => {
                panic!(
                    "bead_id={BEAD_ID} case=decode_unexpected_failure reason={:?}",
                    failure.reason
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // §3.3 Test 14: Cancel-safety (test_pipeline_cancel_safe)
    // -----------------------------------------------------------------------

    #[test]
    fn test_pipeline_cancel_safe_encode() {
        let config = PipelineConfig {
            checkpoint_interval: 2, // checkpoint every 2 symbols
            ..default_config()
        };
        let encoder =
            RaptorQPageEncoder::new(config.clone(), default_codec()).expect("encoder build");

        // Create a Cx that is already cancelled.
        let cx = fsqlite_types::cx::Cx::new();
        cx.cancel_with_reason(fsqlite_types::cx::CancelReason::UserInterrupt);

        let k = 10_usize;
        let data = deterministic_page_data(k, config.symbol_size as usize, 0xABCD);
        let mut sink = VecPageSink::new();

        let result = encoder.encode_pages(&cx, &data, &mut sink);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=cancel_safe_encode_aborts"
        );
        assert!(
            matches!(result.unwrap_err(), FrankenError::Abort),
            "bead_id={BEAD_ID} case=cancel_safe_encode_error_type"
        );
        // Sink should not have been flushed.
        assert!(!sink.flushed, "bead_id={BEAD_ID} case=cancel_safe_no_flush");
    }

    #[test]
    fn test_pipeline_cancel_safe_decode() {
        let config = PipelineConfig {
            checkpoint_interval: 2,
            ..default_config()
        };
        let decoder =
            RaptorQPageDecoder::new(config.clone(), default_codec()).expect("decoder build");

        // Create a Cx that is already cancelled.
        let cx = fsqlite_types::cx::Cx::new();
        cx.cancel_with_reason(fsqlite_types::cx::CancelReason::UserInterrupt);

        // Feed some symbols.
        let mut symbols = BTreeMap::new();
        for esi in 0..10_u32 {
            symbols.insert(esi, vec![0xAA; config.symbol_size as usize]);
        }
        let mut source = VecPageSource::from_map(symbols);

        let result = decoder.decode_pages(&cx, &mut source, 10);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=cancel_safe_decode_aborts"
        );
        assert!(
            matches!(result.unwrap_err(), FrankenError::Abort),
            "bead_id={BEAD_ID} case=cancel_safe_decode_error_type"
        );
    }

    // -----------------------------------------------------------------------
    // §3.3 Test 15: Backpressure (test_pipeline_backpressure)
    // -----------------------------------------------------------------------

    /// Sink that fails after N writes, simulating a full output buffer.
    struct BackpressureSink {
        limit: u32,
        count: u32,
    }

    impl BackpressureSink {
        fn new(limit: u32) -> Self {
            Self { limit, count: 0 }
        }
    }

    impl PageSymbolSink for BackpressureSink {
        fn write_symbol(&mut self, _esi: u32, _data: &[u8]) -> Result<()> {
            if self.count >= self.limit {
                return Err(FrankenError::Busy);
            }
            self.count += 1;
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        fn written_count(&self) -> u32 {
            self.count
        }
    }

    #[test]
    fn test_pipeline_backpressure_sink_full() {
        let config = default_config();
        let encoder =
            RaptorQPageEncoder::new(config.clone(), default_codec()).expect("encoder build");
        let cx = test_cx();
        let k = 10_usize;
        let data = deterministic_page_data(k, config.symbol_size as usize, 0xEEFF);

        // Sink that only accepts 3 symbols then returns Busy.
        let mut sink = BackpressureSink::new(3);
        let result = encoder.encode_pages(&cx, &data, &mut sink);

        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=backpressure_propagated"
        );
        assert!(
            matches!(result.unwrap_err(), FrankenError::Busy),
            "bead_id={BEAD_ID} case=backpressure_error_type"
        );
        assert_eq!(
            sink.written_count(),
            3,
            "bead_id={BEAD_ID} case=backpressure_partial_write"
        );
    }

    // -----------------------------------------------------------------------
    // Config Validation Tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_config_validation_zero_symbol_size() {
        let config = PipelineConfig {
            symbol_size: 0,
            ..default_config()
        };
        assert!(
            config.validate().is_err(),
            "bead_id={BEAD_ID} case=config_reject_zero_symbol_size"
        );
    }

    #[test]
    fn test_config_validation_non_power_of_two() {
        let config = PipelineConfig {
            symbol_size: 1000,
            ..default_config()
        };
        assert!(
            config.validate().is_err(),
            "bead_id={BEAD_ID} case=config_reject_non_power_of_two"
        );
    }

    #[test]
    fn test_config_validation_below_min() {
        let config = PipelineConfig {
            symbol_size: 256,
            ..default_config()
        };
        assert!(
            config.validate().is_err(),
            "bead_id={BEAD_ID} case=config_reject_below_min"
        );
    }

    #[test]
    fn test_config_validation_above_max() {
        let config = PipelineConfig {
            symbol_size: 128 * 1024,
            ..default_config()
        };
        assert!(
            config.validate().is_err(),
            "bead_id={BEAD_ID} case=config_reject_above_max"
        );
    }

    #[test]
    fn test_config_validation_zero_max_block_size() {
        let config = PipelineConfig {
            max_block_size: 0,
            ..default_config()
        };
        assert!(
            config.validate().is_err(),
            "bead_id={BEAD_ID} case=config_reject_zero_max_block"
        );
    }

    #[test]
    fn test_config_validation_repair_overhead_below_one() {
        let config = PipelineConfig {
            repair_overhead: 0.5,
            ..default_config()
        };
        assert!(
            config.validate().is_err(),
            "bead_id={BEAD_ID} case=config_reject_repair_overhead_below_one"
        );
    }

    #[test]
    fn test_config_validation_zero_checkpoint_interval() {
        let config = PipelineConfig {
            checkpoint_interval: 0,
            ..default_config()
        };
        assert!(
            config.validate().is_err(),
            "bead_id={BEAD_ID} case=config_reject_zero_checkpoint_interval"
        );
    }

    #[test]
    fn test_config_validation_valid_configs() {
        for symbol_size in [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
            let config = PipelineConfig::for_page_size(symbol_size);
            assert!(
                config.validate().is_ok(),
                "bead_id={BEAD_ID} case=config_valid symbol_size={symbol_size}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Decode Proof on Failure
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_failure_insufficient_symbols() {
        let config = default_config();
        let encoder =
            RaptorQPageEncoder::new(config.clone(), default_codec()).expect("encoder build");
        let decoder =
            RaptorQPageDecoder::new(config.clone(), default_codec()).expect("decoder build");
        let cx = test_cx();
        let k = 10_usize;
        let data = deterministic_page_data(k, config.symbol_size as usize, 0xDEAD);

        // Encode.
        let mut sink = VecPageSink::new();
        let outcome = encoder
            .encode_pages(&cx, &data, &mut sink)
            .expect("encode must succeed");

        // Keep only K-3 source symbols (insufficient).
        let mut partial = BTreeMap::new();
        for esi in 0..((k - 3) as u32) {
            if let Some(sym) = sink.symbols.get(&esi) {
                partial.insert(esi, sym.clone());
            }
        }
        let mut source = VecPageSource::from_map(partial);

        let decode_outcome = decoder
            .decode_pages(&cx, &mut source, outcome.source_count)
            .expect("decode call itself should not error");

        match decode_outcome {
            DecodeOutcome::Failure(failure) => {
                assert_eq!(
                    failure.reason,
                    DecodeFailureReason::InsufficientSymbols,
                    "bead_id={BEAD_ID} case=decode_failure_reason"
                );
                assert!(
                    failure.symbols_received < outcome.source_count,
                    "bead_id={BEAD_ID} case=decode_failure_symbol_count"
                );
                assert_eq!(
                    failure.k_required, outcome.source_count,
                    "bead_id={BEAD_ID} case=decode_failure_k_required"
                );
                assert!(
                    failure.decode_proof.is_none(),
                    "bead_id={BEAD_ID} case=decode_failure_proof_disabled_by_default"
                );
            }
            DecodeOutcome::Success(_) => {
                panic!("bead_id={BEAD_ID} case=decode_should_have_failed");
            }
        }
    }

    #[test]
    fn test_decode_failure_emits_proof_when_enabled() {
        let mut config = default_config();
        config.decode_proof_policy = DecodeProofEmissionPolicy {
            emit_on_decode_failure: true,
            emit_on_repair_success: false,
        };
        let encoder =
            RaptorQPageEncoder::new(config.clone(), default_codec()).expect("encoder build");
        let decoder =
            RaptorQPageDecoder::new(config.clone(), default_codec()).expect("decoder build");
        let cx = test_cx();
        let k = 10_usize;
        let data = deterministic_page_data(k, config.symbol_size as usize, 0xFA24);

        let mut sink = VecPageSink::new();
        let outcome = encoder
            .encode_pages(&cx, &data, &mut sink)
            .expect("encode must succeed");

        let mut partial = BTreeMap::new();
        for esi in 0..((k - 2) as u32) {
            if let Some(sym) = sink.symbols.get(&esi) {
                partial.insert(esi, sym.clone());
            }
        }
        let mut source = VecPageSource::from_map(partial);
        let decode_outcome = decoder
            .decode_pages(&cx, &mut source, outcome.source_count)
            .expect("decode call itself should not error");

        match decode_outcome {
            DecodeOutcome::Failure(failure) => {
                let proof = failure
                    .decode_proof
                    .expect("bead_id=bd-faz4 case=decode_failure_proof_emitted");
                assert!(
                    !proof.decode_success,
                    "bead_id=bd-faz4 case=decode_failure_proof_flag"
                );
                assert!(
                    proof.is_consistent(),
                    "bead_id=bd-faz4 case=decode_failure_proof_consistent"
                );
            }
            DecodeOutcome::Success(_) => {
                panic!("bead_id=bd-faz4 case=decode_failure_expected");
            }
        }
    }

    #[test]
    fn test_decode_success_with_repair_emits_proof_when_enabled() {
        let mut config = default_config();
        config.decode_proof_policy = DecodeProofEmissionPolicy {
            emit_on_decode_failure: false,
            emit_on_repair_success: true,
        };
        let encoder =
            RaptorQPageEncoder::new(config.clone(), default_codec()).expect("encoder build");
        let decoder =
            RaptorQPageDecoder::new(config.clone(), default_codec()).expect("decoder build");
        let cx = test_cx();
        let k = 10_usize;
        let data = deterministic_page_data(k, config.symbol_size as usize, 0xF0AA);

        let mut sink = VecPageSink::new();
        let outcome = encoder
            .encode_pages(&cx, &data, &mut sink)
            .expect("encode must succeed");
        let mut source = VecPageSource::from_sink(&sink);
        let decode_outcome = decoder
            .decode_pages(&cx, &mut source, outcome.source_count)
            .expect("decode must succeed");

        match decode_outcome {
            DecodeOutcome::Success(success) => {
                let proof = success
                    .decode_proof
                    .expect("bead_id=bd-faz4 case=repair_success_proof_emitted");
                assert!(proof.decode_success);
                assert!(proof.is_repair());
                assert!(
                    proof.is_consistent(),
                    "bead_id=bd-faz4 case=repair_success_proof_consistent"
                );
            }
            DecodeOutcome::Failure(failure) => {
                panic!(
                    "bead_id=bd-faz4 case=repair_success_should_decode reason={:?}",
                    failure.reason
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // E2E Round-trip: encode → store → read → decode → verify
    // -----------------------------------------------------------------------

    #[test]
    fn test_e2e_roundtrip_multiple_page_sizes() {
        for &symbol_size in &[512_u32, 1024, 4096] {
            let config = PipelineConfig::for_page_size(symbol_size);
            let encoder =
                RaptorQPageEncoder::new(config.clone(), default_codec()).expect("encoder build");
            let decoder =
                RaptorQPageDecoder::new(config.clone(), default_codec()).expect("decoder build");
            let cx = test_cx();

            let k = 8_usize;
            let data = deterministic_page_data(k, symbol_size as usize, u64::from(symbol_size));

            // Encode → store.
            let mut sink = VecPageSink::new();
            let outcome = encoder
                .encode_pages(&cx, &data, &mut sink)
                .expect("encode must succeed");

            // Read → decode.
            let mut source = VecPageSource::from_sink(&sink);
            let decode_result = decoder
                .decode_pages(&cx, &mut source, outcome.source_count)
                .expect("decode must succeed");

            match decode_result {
                DecodeOutcome::Success(success) => {
                    assert_eq!(
                        success.data, data,
                        "bead_id={BEAD_ID} case=e2e_roundtrip symbol_size={symbol_size}"
                    );
                }
                DecodeOutcome::Failure(f) => {
                    panic!(
                        "bead_id={BEAD_ID} case=e2e_roundtrip_failure symbol_size={symbol_size} reason={:?}",
                        f.reason
                    );
                }
            }
        }
    }

    #[test]
    fn test_e2e_roundtrip_64_pages() {
        let config = PipelineConfig::for_page_size(4096);
        let encoder =
            RaptorQPageEncoder::new(config.clone(), default_codec()).expect("encoder build");
        let decoder =
            RaptorQPageDecoder::new(config.clone(), default_codec()).expect("decoder build");
        let cx = test_cx();

        let k = 64_usize;
        let data = deterministic_page_data(k, config.symbol_size as usize, 0xE2E6_4000);

        let mut sink = VecPageSink::new();
        let outcome = encoder
            .encode_pages(&cx, &data, &mut sink)
            .expect("encode must succeed");

        assert_eq!(
            outcome.source_count as usize, k,
            "bead_id={BEAD_ID} case=e2e_64_source_count"
        );

        let mut source = VecPageSource::from_sink(&sink);
        let decode_result = decoder
            .decode_pages(&cx, &mut source, outcome.source_count)
            .expect("decode must succeed");

        match decode_result {
            DecodeOutcome::Success(success) => {
                assert_eq!(
                    success.data, data,
                    "bead_id={BEAD_ID} case=e2e_64_roundtrip_bytes"
                );
                info!(
                    bead_id = BEAD_ID,
                    k,
                    peeled = success.peeled_count,
                    inactivated = success.inactivated_count,
                    "E2E 64-page roundtrip complete"
                );
            }
            DecodeOutcome::Failure(f) => {
                panic!(
                    "bead_id={BEAD_ID} case=e2e_64_failure reason={:?}",
                    f.reason
                );
            }
        }
    }

    #[test]
    fn test_e2e_bd_1hi_5() {
        let config = PipelineConfig::for_page_size(4096);
        let encoder =
            RaptorQPageEncoder::new(config.clone(), default_codec()).expect("encoder build");
        let decoder =
            RaptorQPageDecoder::new(config.clone(), default_codec()).expect("decoder build");
        let cx = test_cx();

        // Realistic load for this lane: 64 pages (256 KiB) with symbol loss.
        let k = 64_usize;
        let data = deterministic_page_data(k, config.symbol_size as usize, 0xB1D1_5005);
        let mut sink = VecPageSink::new();
        let outcome = encoder
            .encode_pages(&cx, &data, &mut sink)
            .expect("encode must succeed");

        // Drop one source symbol per source block; keep all repair symbols.
        let mut dropped = 0_u32;
        let mut degraded = BTreeMap::new();
        for (packed_key, symbol_bytes) in &sink.symbols {
            let (kind, _sbn, esi) = unpack_symbol_key(*packed_key);
            if kind.is_source() && esi == 0 {
                dropped += 1;
                continue;
            }
            degraded.insert(*packed_key, symbol_bytes.clone());
        }
        assert!(dropped > 0, "bead_id={BEAD_ID} case=e2e_named_dropped_some");

        let mut source = VecPageSource::from_map(degraded);
        let decode_result = decoder
            .decode_pages(&cx, &mut source, outcome.source_count)
            .expect("decode must complete");

        match decode_result {
            DecodeOutcome::Success(success) => {
                assert_eq!(
                    success.data, data,
                    "bead_id={BEAD_ID} case=e2e_named_byte_perfect_recovery"
                );
            }
            DecodeOutcome::Failure(f) => {
                panic!(
                    "bead_id={BEAD_ID} case=e2e_named_unexpected_failure reason={:?}",
                    f.reason
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // E2E: Retry after failure
    // -----------------------------------------------------------------------

    #[test]
    fn test_e2e_retry_after_failure() {
        let config = default_config();
        let encoder =
            RaptorQPageEncoder::new(config.clone(), default_codec()).expect("encoder build");
        let decoder =
            RaptorQPageDecoder::new(config.clone(), default_codec()).expect("decoder build");
        let cx = test_cx();
        let k = 10_usize;
        let data = deterministic_page_data(k, config.symbol_size as usize, 0xAE_7121);

        // Encode.
        let mut sink = VecPageSink::new();
        let outcome = encoder
            .encode_pages(&cx, &data, &mut sink)
            .expect("encode must succeed");

        // First attempt: K-2 source symbols only → should fail.
        let mut partial = BTreeMap::new();
        for esi in 0..((k - 2) as u32) {
            if let Some(sym) = sink.symbols.get(&esi) {
                partial.insert(esi, sym.clone());
            }
        }
        let mut source_attempt1 = VecPageSource::from_map(partial.clone());
        let result1 = decoder
            .decode_pages(&cx, &mut source_attempt1, outcome.source_count)
            .expect("decode call should not error");
        assert!(
            matches!(result1, DecodeOutcome::Failure(_)),
            "bead_id={BEAD_ID} case=retry_first_attempt_fails"
        );

        // Second attempt: add all remaining symbols → should succeed.
        let full = sink.symbols.clone();
        let mut source_attempt2 = VecPageSource::from_map(full);
        let result2 = decoder
            .decode_pages(&cx, &mut source_attempt2, outcome.source_count)
            .expect("decode call should not error");
        match result2 {
            DecodeOutcome::Success(success) => {
                assert_eq!(
                    success.data, data,
                    "bead_id={BEAD_ID} case=retry_second_attempt_succeeds"
                );
            }
            DecodeOutcome::Failure(f) => {
                panic!(
                    "bead_id={BEAD_ID} case=retry_second_should_succeed reason={:?}",
                    f.reason
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Decode with exact K symbols (fragile recovery)
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_source_only_exact_k() {
        let config = default_config();
        let encoder =
            RaptorQPageEncoder::new(config.clone(), default_codec()).expect("encoder build");
        let decoder =
            RaptorQPageDecoder::new(config.clone(), default_codec()).expect("decoder build");
        let cx = test_cx();
        let k = 8_usize;
        let data = deterministic_page_data(k, config.symbol_size as usize, 0xE4AC7);

        let mut sink = VecPageSink::new();
        let outcome = encoder
            .encode_pages(&cx, &data, &mut sink)
            .expect("encode must succeed");

        // Keep only K source symbols (no repair).
        let mut source_only = BTreeMap::new();
        for esi in 0..(k as u32) {
            if let Some(sym) = sink.symbols.get(&esi) {
                source_only.insert(esi, sym.clone());
            }
        }

        let mut source = VecPageSource::from_map(source_only);
        let decode_result = decoder
            .decode_pages(&cx, &mut source, outcome.source_count)
            .expect("decode must not error");

        match decode_result {
            DecodeOutcome::Success(success) => {
                assert_eq!(
                    success.data, data,
                    "bead_id={BEAD_ID} case=exact_k_roundtrip"
                );
                assert_eq!(
                    success.symbols_used, k as u32,
                    "bead_id={BEAD_ID} case=exact_k_symbols_used"
                );
            }
            DecodeOutcome::Failure(f) => {
                panic!(
                    "bead_id={BEAD_ID} case=exact_k_should_succeed reason={:?}",
                    f.reason
                );
            }
        }
    }
}
