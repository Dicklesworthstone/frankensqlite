//! WAL FEC adapter: wires the `PageSymbolSink` / `PageSymbolSource` traits
//! to the WAL commit and read paths via the production `AsupersyncCodec`
//! (bd-3sj9w).
//!
//! # Architecture
//!
//! ```text
//! WalBackendAdapter
//!   ├─ append_frame() ──► FecCommitHook::on_frame()
//!   │                      └─ on commit ──► RaptorQPageEncoder::encode_pages()
//!   │                                        └─ WalFecPageSink (buffers symbols)
//!   │                                             └─ flush ──► sidecar persist
//!   └─ read_page() ──────► on checksum mismatch ──► FEC recovery
//!                            └─ WalFecPageSource (reads sidecar)
//!                                 └─ RaptorQPageDecoder::decode_pages()
//! ```

use std::collections::BTreeMap;

use fsqlite_error::{FrankenError, Result};
use tracing::info;

use crate::raptorq_codec::AsupersyncCodec;
use crate::raptorq_integration::{
    DecodeOutcome, PageSymbolSink, PageSymbolSource, PipelineConfig, RaptorQPageDecoder,
    RaptorQPageEncoder,
};

// ---------------------------------------------------------------------------
// WalFecPageSink: collects encoded symbols in memory
// ---------------------------------------------------------------------------

/// In-memory [`PageSymbolSink`] that collects symbols for a WAL commit group.
///
/// After encoding, the collected symbols can be persisted to the FEC sidecar
/// via [`WalFecPageSink::take_symbols`].
#[derive(Debug)]
pub struct WalFecPageSink {
    symbols: BTreeMap<u32, Vec<u8>>,
    flushed: bool,
}

impl WalFecPageSink {
    /// Create an empty sink.
    #[must_use]
    pub fn new() -> Self {
        Self {
            symbols: BTreeMap::new(),
            flushed: false,
        }
    }

    /// Take all collected symbols, consuming the sink's buffer.
    ///
    /// Returns `(esi, data)` pairs sorted by ESI.
    #[must_use]
    pub fn take_symbols(&mut self) -> Vec<(u32, Vec<u8>)> {
        std::mem::take(&mut self.symbols).into_iter().collect()
    }

    /// Whether [`PageSymbolSink::flush`] has been called.
    #[must_use]
    pub const fn is_flushed(&self) -> bool {
        self.flushed
    }
}

impl Default for WalFecPageSink {
    fn default() -> Self {
        Self::new()
    }
}

impl PageSymbolSink for WalFecPageSink {
    fn write_symbol(&mut self, esi: u32, data: &[u8]) -> Result<()> {
        self.symbols.insert(esi, data.to_vec());
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.flushed = true;
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    fn written_count(&self) -> u32 {
        self.symbols.len() as u32
    }
}

// ---------------------------------------------------------------------------
// WalFecPageSource: reads symbols from a pre-loaded map
// ---------------------------------------------------------------------------

/// [`PageSymbolSource`] backed by an in-memory symbol map.
///
/// Typically populated from FEC sidecar data read during recovery.
#[derive(Debug)]
pub struct WalFecPageSource {
    symbols: BTreeMap<u32, Vec<u8>>,
}

impl WalFecPageSource {
    /// Create a source from a pre-loaded symbol map.
    #[must_use]
    pub fn new(symbols: BTreeMap<u32, Vec<u8>>) -> Self {
        Self { symbols }
    }

    /// Create a source from a vector of `(esi, data)` pairs.
    #[must_use]
    pub fn from_pairs(pairs: Vec<(u32, Vec<u8>)>) -> Self {
        Self {
            symbols: pairs.into_iter().collect(),
        }
    }
}

impl PageSymbolSource for WalFecPageSource {
    fn read_symbol(&mut self, esi: u32) -> Result<Option<Vec<u8>>> {
        Ok(self.symbols.get(&esi).cloned())
    }

    fn available_esis(&self) -> Vec<u32> {
        self.symbols.keys().copied().collect()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn available_count(&self) -> u32 {
        self.symbols.len() as u32
    }
}

// ---------------------------------------------------------------------------
// FecCommitHook: orchestrates encode-on-commit
// ---------------------------------------------------------------------------

/// Buffers pages during a WAL transaction and encodes FEC symbols on commit.
///
/// Attach to `WalBackendAdapter` to enable automatic FEC generation.
/// When `on_frame()` receives a commit frame (`db_size_if_commit > 0`),
/// the hook encodes all buffered pages via `RaptorQPageEncoder` and
/// collects the symbols in a `WalFecPageSink`.
pub struct FecCommitHook {
    /// Pipeline encoder (codec + config).
    encoder: RaptorQPageEncoder<AsupersyncCodec>,
    /// Pages buffered since the last commit.
    buffered_pages: Vec<BufferedPage>,
    /// Whether FEC encoding is enabled.
    enabled: bool,
}

/// A page buffered for FEC encoding.
#[derive(Debug, Clone)]
struct BufferedPage {
    page_number: u32,
    data: Vec<u8>,
}

impl FecCommitHook {
    /// Create a new hook with the given pipeline config.
    pub fn new(config: PipelineConfig) -> Result<Self> {
        let codec = AsupersyncCodec::new(config.max_block_size as usize);
        let encoder = RaptorQPageEncoder::new(config, codec)?;
        Ok(Self {
            encoder,
            buffered_pages: Vec::new(),
            enabled: true,
        })
    }

    /// Create a disabled hook (no-op on all operations).
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            // Use default config — won't be used since enabled=false.
            encoder: RaptorQPageEncoder::new(PipelineConfig::default(), AsupersyncCodec::default())
                .expect("default config is valid"),
            buffered_pages: Vec::new(),
            enabled: false,
        }
    }

    /// Create a hook configured from environment variables.
    ///
    /// Reads:
    /// - `FSQLITE_RAPTORQ_ENABLED`: `"1"` or `"true"` to enable (default: disabled)
    /// - `FSQLITE_RAPTORQ_OVERHEAD`: repair overhead factor (default: 1.25)
    /// - `FSQLITE_RAPTORQ_SEGMENT_SIZE`: max block size in bytes (default: 65536)
    ///
    /// `page_size` is the database page size to use as the symbol size.
    pub fn from_env(page_size: u32) -> Result<Self> {
        let enabled = std::env::var("FSQLITE_RAPTORQ_ENABLED")
            .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));

        if !enabled {
            return Ok(Self::disabled());
        }

        let overhead: f64 = std::env::var("FSQLITE_RAPTORQ_OVERHEAD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1.25);

        let segment_size: u32 = std::env::var("FSQLITE_RAPTORQ_SEGMENT_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(65_536);

        if overhead < 1.0 {
            return Err(FrankenError::OutOfRange {
                what: "FSQLITE_RAPTORQ_OVERHEAD (must be >= 1.0)".to_owned(),
                value: overhead.to_string(),
            });
        }

        let config = PipelineConfig {
            symbol_size: page_size,
            max_block_size: segment_size,
            repair_overhead: overhead,
            ..PipelineConfig::default()
        };

        info!(
            page_size,
            overhead, segment_size, "RaptorQ FEC enabled via environment"
        );

        Self::new(config)
    }

    /// Whether FEC encoding is currently enabled.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Enable or disable FEC encoding.
    pub fn set_enabled(&mut self, enabled: bool) {
        if !enabled {
            self.discard_buffered();
        }
        self.enabled = enabled;
    }

    /// Buffer a page for FEC encoding.
    ///
    /// If `db_size_if_commit > 0`, this triggers encoding of all buffered
    /// pages and returns the collected symbols.  Otherwise returns `None`.
    pub fn on_frame(
        &mut self,
        cx: &fsqlite_types::cx::Cx,
        page_number: u32,
        page_data: &[u8],
        db_size_if_commit: u32,
    ) -> Result<Option<FecCommitResult>> {
        if !self.enabled {
            return Ok(None);
        }

        self.buffered_pages.push(BufferedPage {
            page_number,
            data: page_data.to_vec(),
        });

        if db_size_if_commit == 0 {
            return Ok(None);
        }

        // Commit frame — encode all buffered pages.
        let result = self.encode_buffered(cx);
        self.buffered_pages.clear();
        result.map(Some)
    }

    /// Discard buffered pages (e.g. on rollback).
    pub fn discard_buffered(&mut self) {
        self.buffered_pages.clear();
    }

    /// Encode all buffered pages and return the FEC symbols.
    fn encode_buffered(&self, cx: &fsqlite_types::cx::Cx) -> Result<FecCommitResult> {
        if self.buffered_pages.is_empty() {
            return Ok(FecCommitResult {
                page_numbers: Vec::new(),
                symbols: Vec::new(),
                k_source: 0,
                symbol_size: self.encoder.config().symbol_size,
            });
        }

        // Concatenate all page data for encoding.
        let total_size: usize = self.buffered_pages.iter().map(|p| p.data.len()).sum();
        let mut combined = Vec::with_capacity(total_size);
        for page in &self.buffered_pages {
            combined.extend_from_slice(&page.data);
        }

        let mut sink = WalFecPageSink::new();
        let outcome = self.encoder.encode_pages(cx, &combined, &mut sink)?;

        let page_numbers: Vec<u32> = self.buffered_pages.iter().map(|p| p.page_number).collect();

        Ok(FecCommitResult {
            page_numbers,
            symbols: sink.take_symbols(),
            k_source: outcome.source_count,
            symbol_size: outcome.symbol_size,
        })
    }
}

/// Result of FEC encoding a commit group.
#[derive(Debug)]
pub struct FecCommitResult {
    /// Page numbers in this commit group.
    pub page_numbers: Vec<u32>,
    /// Encoded symbols `(esi, data)`.
    pub symbols: Vec<(u32, Vec<u8>)>,
    /// Number of source symbols K.
    pub k_source: u32,
    /// Symbol size T in bytes.
    pub symbol_size: u32,
}

// ---------------------------------------------------------------------------
// FEC Recovery Helper
// ---------------------------------------------------------------------------

/// Attempt to decode page data from FEC symbols using the production codec.
///
/// Returns the recovered data on success, or a decode failure diagnostic.
pub fn attempt_fec_recovery(
    cx: &fsqlite_types::cx::Cx,
    config: &PipelineConfig,
    symbols: BTreeMap<u32, Vec<u8>>,
    k_source: u32,
) -> Result<DecodeOutcome> {
    let codec = AsupersyncCodec::new(config.max_block_size as usize);
    let decoder = RaptorQPageDecoder::new(config.clone(), codec)?;
    let mut source = WalFecPageSource::new(symbols);
    decoder.decode_pages(cx, &mut source, k_source)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
mod tests {
    use super::*;
    use asupersync::Cx as AsCx;
    use asupersync::types::CancelReason as AsCancelReason;
    use fsqlite_types::cx::Cx;

    /// Build a `Cx` with an attached native asupersync `Cx`.
    ///
    /// The production `AsupersyncCodec` (bd-3sj9w) requires either an attached
    /// native `Cx` or an ambient asupersync runtime `Cx` to encode/decode — a
    /// bare `Cx::default()` makes every `encode_pages`/`decode_pages` call fail
    /// with `Internal("bd-3sj9w: ...requires an attached native Cx...")`.  Tests
    /// run outside an asupersync runtime, so they must attach one explicitly,
    /// exactly as `raptorq_codec::tests::test_cx` does.
    fn test_cx() -> Cx {
        let cx = Cx::new();
        cx.set_native_cx(AsCx::for_testing());
        cx
    }

    fn sample_page(seed: u8, size: usize) -> Vec<u8> {
        let mut data = vec![0u8; size];
        for (i, byte) in data.iter_mut().enumerate() {
            let reduced = (i % 251) as u8;
            *byte = reduced ^ seed;
        }
        data
    }

    fn default_config() -> PipelineConfig {
        PipelineConfig::for_page_size(512)
    }

    // -- WalFecPageSink tests --

    #[test]
    fn test_sink_write_and_count() {
        let mut sink = WalFecPageSink::new();
        assert_eq!(sink.written_count(), 0);
        assert!(!sink.is_flushed());

        sink.write_symbol(0, &[1, 2, 3]).unwrap();
        sink.write_symbol(1, &[4, 5, 6]).unwrap();
        assert_eq!(sink.written_count(), 2);

        sink.flush().unwrap();
        assert!(sink.is_flushed());
    }

    #[test]
    fn test_sink_take_symbols() {
        let mut sink = WalFecPageSink::new();
        sink.write_symbol(2, &[0xAA]).unwrap();
        sink.write_symbol(0, &[0xBB]).unwrap();
        sink.write_symbol(1, &[0xCC]).unwrap();
        sink.flush().unwrap();

        let symbols = sink.take_symbols();
        assert_eq!(symbols.len(), 3);
        // BTreeMap order — sorted by ESI.
        assert_eq!(symbols[0].0, 0);
        assert_eq!(symbols[1].0, 1);
        assert_eq!(symbols[2].0, 2);

        // After take, sink is empty.
        assert_eq!(sink.written_count(), 0);
    }

    // -- WalFecPageSource tests --

    #[test]
    fn test_source_read_symbol() {
        let mut map = BTreeMap::new();
        map.insert(0, vec![0x11]);
        map.insert(5, vec![0x22]);
        let mut source = WalFecPageSource::new(map);

        assert_eq!(source.available_count(), 2);
        assert_eq!(source.available_esis(), vec![0, 5]);

        assert_eq!(source.read_symbol(0).unwrap(), Some(vec![0x11]));
        assert_eq!(source.read_symbol(5).unwrap(), Some(vec![0x22]));
        assert_eq!(source.read_symbol(99).unwrap(), None);
    }

    #[test]
    fn test_source_from_pairs() {
        let pairs = vec![(3, vec![0xAA]), (1, vec![0xBB])];
        let mut source = WalFecPageSource::from_pairs(pairs);
        assert_eq!(source.available_count(), 2);
        assert_eq!(source.read_symbol(1).unwrap(), Some(vec![0xBB]));
        assert_eq!(source.read_symbol(3).unwrap(), Some(vec![0xAA]));
    }

    // -- FecCommitHook tests --

    #[test]
    fn test_hook_disabled_returns_none() {
        let cx = test_cx();
        let mut hook = FecCommitHook::disabled();
        assert!(!hook.is_enabled());

        let result = hook.on_frame(&cx, 1, &sample_page(0x42, 512), 1).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_hook_non_commit_returns_none() {
        let cx = test_cx();
        let mut hook = FecCommitHook::new(default_config()).unwrap();

        // db_size_if_commit == 0 means non-commit frame.
        let result = hook.on_frame(&cx, 1, &sample_page(0x42, 512), 0).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_hook_commit_produces_symbols() {
        let cx = test_cx();
        let mut hook = FecCommitHook::new(default_config()).unwrap();

        // Buffer two pages, then commit.
        hook.on_frame(&cx, 1, &sample_page(0x10, 512), 0).unwrap();
        let result = hook.on_frame(&cx, 2, &sample_page(0x20, 512), 2).unwrap();

        let commit = result.expect("commit should produce FEC result");
        assert_eq!(commit.page_numbers, vec![1, 2]);
        assert!(commit.k_source > 0);
        assert!(!commit.symbols.is_empty());
    }

    #[test]
    fn test_hook_single_page_commit() {
        let cx = test_cx();
        let mut hook = FecCommitHook::new(default_config()).unwrap();

        let result = hook.on_frame(&cx, 5, &sample_page(0x55, 512), 5).unwrap();

        let commit = result.expect("single-page commit should produce FEC");
        assert_eq!(commit.page_numbers, vec![5]);
        assert!(commit.k_source > 0);
    }

    #[test]
    fn test_hook_clears_buffer_after_commit() {
        let cx = test_cx();
        let mut hook = FecCommitHook::new(default_config()).unwrap();

        // First commit group.
        hook.on_frame(&cx, 1, &sample_page(0x10, 512), 0).unwrap();
        let r1 = hook.on_frame(&cx, 2, &sample_page(0x20, 512), 2).unwrap();
        assert!(r1.is_some());

        // Second commit group — should only contain page 3.
        let r2 = hook.on_frame(&cx, 3, &sample_page(0x30, 512), 3).unwrap();
        let commit = r2.expect("second commit");
        assert_eq!(commit.page_numbers, vec![3]);
    }

    #[test]
    fn test_hook_discard_buffered() {
        let cx = test_cx();
        let mut hook = FecCommitHook::new(default_config()).unwrap();

        hook.on_frame(&cx, 1, &sample_page(0x10, 512), 0).unwrap();
        hook.on_frame(&cx, 2, &sample_page(0x20, 512), 0).unwrap();
        hook.discard_buffered();

        // Commit with just page 3.
        let result = hook.on_frame(&cx, 3, &sample_page(0x30, 512), 3).unwrap();
        let commit = result.expect("commit after discard");
        assert_eq!(commit.page_numbers, vec![3]);
    }

    #[test]
    fn test_hook_clears_buffer_after_failed_commit_encode() {
        let mut hook = FecCommitHook::new(default_config()).unwrap();

        let aborted_cx = Cx::new();
        // Attach a native Cx so the codec reaches its cancellation path (the
        // production codec requires one); `set_native_cx` mirrors the local
        // cancel onto the native, so the encode aborts rather than failing the
        // bd-3sj9w "missing native Cx" precondition.
        aborted_cx.set_native_cx(AsCx::for_testing());
        aborted_cx.cancel_with_reason(fsqlite_types::cx::CancelReason::UserInterrupt);

        hook.on_frame(&aborted_cx, 1, &sample_page(0x10, 512), 0)
            .unwrap();
        let err = hook.on_frame(&aborted_cx, 2, &sample_page(0x20, 512), 2);
        assert!(
            matches!(err, Err(FrankenError::Abort)),
            "cancelled encode should fail with Abort"
        );

        let cx = test_cx();
        let result = hook.on_frame(&cx, 3, &sample_page(0x30, 512), 3).unwrap();
        let commit = result.expect("next commit should only include new frames");
        assert_eq!(commit.page_numbers, vec![3]);
    }

    #[test]
    fn test_hook_clears_buffer_after_attached_native_cancelled_commit_encode() {
        let mut hook = FecCommitHook::new(default_config()).unwrap();

        let aborted_cx = test_cx();
        let native = AsCx::for_testing();
        aborted_cx.set_native_cx(native.clone());
        native.set_cancel_reason(AsCancelReason::timeout());

        hook.on_frame(&aborted_cx, 1, &sample_page(0x10, 512), 0)
            .unwrap();
        let err = hook.on_frame(&aborted_cx, 2, &sample_page(0x20, 512), 2);
        assert!(
            matches!(err, Err(FrankenError::Abort)),
            "attached native cancellation should abort commit-group encode"
        );

        let cx = test_cx();
        let result = hook.on_frame(&cx, 3, &sample_page(0x30, 512), 3).unwrap();
        let commit = result.expect("next commit should only include new frames");
        assert_eq!(commit.page_numbers, vec![3]);
    }

    #[test]
    fn test_hook_enable_disable() {
        let cx = test_cx();
        let mut hook = FecCommitHook::new(default_config()).unwrap();
        assert!(hook.is_enabled());

        hook.set_enabled(false);
        assert!(!hook.is_enabled());

        let result = hook.on_frame(&cx, 1, &sample_page(0x42, 512), 1).unwrap();
        assert!(result.is_none(), "disabled hook should return None");

        hook.set_enabled(true);
        let result = hook.on_frame(&cx, 1, &sample_page(0x42, 512), 1).unwrap();
        assert!(result.is_some(), "re-enabled hook should produce symbols");
    }

    #[test]
    fn test_hook_disable_discards_buffered_pages() {
        let cx = test_cx();
        let mut hook = FecCommitHook::new(default_config()).unwrap();

        hook.on_frame(&cx, 1, &sample_page(0x10, 512), 0).unwrap();
        hook.set_enabled(false);
        hook.set_enabled(true);

        let result = hook.on_frame(&cx, 2, &sample_page(0x20, 512), 2).unwrap();
        let commit = result.expect("re-enabled hook should encode only new frames");
        assert_eq!(commit.page_numbers, vec![2]);
    }

    // -- FEC recovery round-trip test --

    #[test]
    fn test_fec_encode_then_recover() {
        let cx = test_cx();
        let config = default_config();
        let mut hook = FecCommitHook::new(config.clone()).unwrap();

        // Encode a 2-page commit.
        let page1 = sample_page(0xAA, 512);
        let page2 = sample_page(0xBB, 512);
        hook.on_frame(&cx, 1, &page1, 0).unwrap();
        let result = hook.on_frame(&cx, 2, &page2, 2).unwrap();
        let commit = result.expect("commit");

        // Recover from the encoded symbols.
        let symbol_map: BTreeMap<u32, Vec<u8>> = commit.symbols.into_iter().collect();
        let outcome = attempt_fec_recovery(&cx, &config, symbol_map, commit.k_source).unwrap();

        match outcome {
            DecodeOutcome::Success(success) => {
                let mut expected = Vec::new();
                expected.extend_from_slice(&page1);
                expected.extend_from_slice(&page2);
                assert_eq!(success.data, expected);
            }
            DecodeOutcome::Failure(fail) => {
                panic!("FEC recovery failed: {:?}", fail.reason);
            }
        }
    }

    #[test]
    fn test_fec_encode_then_recover_multiple_source_blocks() {
        let cx = test_cx();
        let config = PipelineConfig {
            max_block_size: 1024,
            ..default_config()
        };
        let mut hook = FecCommitHook::new(config.clone()).unwrap();

        let page1 = sample_page(0xA1, 512);
        let page2 = sample_page(0xB2, 512);
        let page3 = sample_page(0xC3, 512);
        let page4 = sample_page(0xD4, 512);
        hook.on_frame(&cx, 1, &page1, 0).unwrap();
        hook.on_frame(&cx, 2, &page2, 0).unwrap();
        hook.on_frame(&cx, 3, &page3, 0).unwrap();
        let result = hook.on_frame(&cx, 4, &page4, 4).unwrap();
        let commit = result.expect("commit");

        assert!(
            commit.symbols.iter().any(|(packed, _)| {
                let (_, sbn, _) = crate::raptorq_codec::unpack_symbol_key(*packed);
                sbn > 0
            }),
            "encoded commit should span multiple source blocks"
        );

        let symbol_map: BTreeMap<u32, Vec<u8>> = commit.symbols.into_iter().collect();
        let outcome = attempt_fec_recovery(&cx, &config, symbol_map, commit.k_source).unwrap();

        match outcome {
            DecodeOutcome::Success(success) => {
                let mut expected = Vec::new();
                expected.extend_from_slice(&page1);
                expected.extend_from_slice(&page2);
                expected.extend_from_slice(&page3);
                expected.extend_from_slice(&page4);
                assert_eq!(success.data, expected);
            }
            DecodeOutcome::Failure(fail) => {
                panic!("multi-block FEC recovery failed: {:?}", fail.reason);
            }
        }
    }

    #[test]
    fn test_fec_recovery_with_erasures() {
        let cx = test_cx();
        let config = PipelineConfig {
            repair_overhead: 1.5,
            ..PipelineConfig::for_page_size(512)
        };
        let mut hook = FecCommitHook::new(config.clone()).unwrap();

        // Use multiple pages so we get k > 1 source symbols.
        let page1 = sample_page(0xCC, 512);
        let page2 = sample_page(0xDD, 512);
        let page3 = sample_page(0xEE, 512);
        let page4 = sample_page(0xFF, 512);
        hook.on_frame(&cx, 1, &page1, 0).unwrap();
        hook.on_frame(&cx, 2, &page2, 0).unwrap();
        hook.on_frame(&cx, 3, &page3, 0).unwrap();
        let result = hook.on_frame(&cx, 4, &page4, 4).unwrap();
        let commit = result.expect("commit");

        // Find the first source symbol key and drop it.
        let first_source_key = commit
            .symbols
            .iter()
            .find(|(k, _)| {
                let (kind, _, _) = crate::raptorq_codec::unpack_symbol_key(*k);
                kind == asupersync::types::SymbolKind::Source
            })
            .map(|(k, _)| *k)
            .expect("must have source symbols");

        let symbol_map: BTreeMap<u32, Vec<u8>> = commit
            .symbols
            .into_iter()
            .filter(|(k, _)| *k != first_source_key)
            .collect();

        // Should still decode with repair symbols covering the gap.
        let outcome = attempt_fec_recovery(&cx, &config, symbol_map, commit.k_source).unwrap();
        match outcome {
            DecodeOutcome::Success(success) => {
                let mut expected = Vec::new();
                expected.extend_from_slice(&page1);
                expected.extend_from_slice(&page2);
                expected.extend_from_slice(&page3);
                expected.extend_from_slice(&page4);
                assert_eq!(success.data, expected);
            }
            DecodeOutcome::Failure(fail) => {
                panic!("FEC recovery with erasures failed: {:?}", fail.reason);
            }
        }
    }

    #[test]
    fn test_fec_recovery_respects_attached_native_cancellation() {
        let encode_cx = test_cx();
        let config = default_config();
        let mut hook = FecCommitHook::new(config.clone()).unwrap();

        let page1 = sample_page(0xAA, 512);
        let page2 = sample_page(0xBB, 512);
        hook.on_frame(&encode_cx, 1, &page1, 0).unwrap();
        let result = hook.on_frame(&encode_cx, 2, &page2, 2).unwrap();
        let commit = result.expect("commit");

        let decode_cx = test_cx();
        let native = AsCx::for_testing();
        decode_cx.set_native_cx(native.clone());
        native.set_cancel_reason(AsCancelReason::timeout());

        let symbol_map: BTreeMap<u32, Vec<u8>> = commit.symbols.into_iter().collect();
        let err =
            attempt_fec_recovery(&decode_cx, &config, symbol_map, commit.k_source).unwrap_err();
        assert!(matches!(err, FrankenError::Abort));
    }

    // -- Configuration tests --

    #[test]
    fn test_hook_new_with_valid_config() {
        let config = PipelineConfig {
            symbol_size: 4096,
            max_block_size: 65_536,
            repair_overhead: 1.25,
            ..PipelineConfig::default()
        };
        let hook = FecCommitHook::new(config).unwrap();
        assert!(hook.is_enabled());
    }

    #[test]
    fn test_hook_new_with_high_overhead() {
        let config = PipelineConfig {
            repair_overhead: 2.0,
            ..PipelineConfig::for_page_size(512)
        };
        let hook = FecCommitHook::new(config).unwrap();
        assert!(hook.is_enabled());
    }

    #[test]
    fn test_hook_new_with_invalid_symbol_size() {
        let config = PipelineConfig {
            symbol_size: 0,
            ..PipelineConfig::for_page_size(512)
        };
        let result = FecCommitHook::new(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_hook_new_with_invalid_overhead() {
        let config = PipelineConfig {
            repair_overhead: 0.5,
            ..PipelineConfig::for_page_size(512)
        };
        let result = FecCommitHook::new(config);
        assert!(result.is_err());
    }

    // -- Multi-commit-group tests --

    #[test]
    fn test_multiple_commit_groups_independent() {
        let cx = test_cx();
        let mut hook = FecCommitHook::new(default_config()).unwrap();

        // Commit group 1: pages 1-2.
        hook.on_frame(&cx, 1, &sample_page(0x10, 512), 0).unwrap();
        let r1 = hook.on_frame(&cx, 2, &sample_page(0x20, 512), 2).unwrap();
        let g1 = r1.expect("group 1");

        // Commit group 2: pages 3-5.
        hook.on_frame(&cx, 3, &sample_page(0x30, 512), 0).unwrap();
        hook.on_frame(&cx, 4, &sample_page(0x40, 512), 0).unwrap();
        let r2 = hook.on_frame(&cx, 5, &sample_page(0x50, 512), 5).unwrap();
        let g2 = r2.expect("group 2");

        // Groups should have different page sets.
        assert_eq!(g1.page_numbers, vec![1, 2]);
        assert_eq!(g2.page_numbers, vec![3, 4, 5]);

        // Both should produce valid symbols.
        assert!(!g1.symbols.is_empty());
        assert!(!g2.symbols.is_empty());
    }

    // -- Sink edge cases --

    #[test]
    fn test_sink_overwrite_same_esi() {
        let mut sink = WalFecPageSink::new();
        sink.write_symbol(0, &[0x11]).unwrap();
        sink.write_symbol(0, &[0x22]).unwrap();

        // Last write wins (BTreeMap semantics).
        assert_eq!(sink.written_count(), 1);
        let symbols = sink.take_symbols();
        assert_eq!(symbols[0].1, vec![0x22]);
    }

    #[test]
    fn test_sink_default_is_new() {
        let sink = WalFecPageSink::default();
        assert_eq!(sink.written_count(), 0);
        assert!(!sink.is_flushed());
    }

    // -- Recovery edge cases --

    #[test]
    fn test_recovery_insufficient_symbols() {
        let cx = test_cx();
        let config = default_config();

        // Only 1 repair symbol, k_source = 4 — should fail.
        let mut map = BTreeMap::new();
        map.insert(100, vec![0xAA; 512]);

        let outcome = attempt_fec_recovery(&cx, &config, map, 4).unwrap();
        assert!(matches!(outcome, DecodeOutcome::Failure(_)));
    }
}
