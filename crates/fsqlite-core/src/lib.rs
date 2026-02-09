//! Core bounded-parallelism primitives (§1.5, bd-22n.4).
//!
//! This module provides a small bulkhead framework for internal background work.
//! It is intentionally non-blocking: overflow is rejected with `SQLITE_BUSY`
//! (`FrankenError::Busy`) instead of queue-and-wait semantics.

pub mod commit_marker;
pub mod commit_repair;
pub mod db_fec;
pub mod decode_proofs;
pub mod epoch;
pub mod raptorq_integration;
pub mod region;
pub mod repair_symbols;
pub mod replication_receiver;
pub mod replication_sender;
pub mod snapshot_shipping;
pub mod source_block_partition;
pub mod symbol_size_policy;

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{
    ObjectId, Oti, PayloadHash, Region, SymbolRecord, SymbolRecordFlags, gf256_mul_byte,
};
use tracing::{debug, error};

const MAX_BALANCED_BG_CPU: usize = 16;

/// Policy used when the bulkhead admission budget is exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Reject overflow immediately with `SQLITE_BUSY`.
    DropBusy,
}

/// Runtime profile for conservative parallelism defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParallelismProfile {
    /// Conservative profile used by default.
    Balanced,
}

/// Bounded parallelism configuration for a work class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BulkheadConfig {
    /// Number of tasks allowed to execute concurrently.
    pub max_concurrent: usize,
    /// Additional bounded admission slots (not unbounded queueing).
    pub queue_depth: usize,
    /// Overflow behavior when capacity is exhausted.
    pub overflow_policy: OverflowPolicy,
}

impl BulkheadConfig {
    /// Create an explicit configuration.
    ///
    /// Returns `None` when `max_concurrent` is zero.
    #[must_use]
    pub const fn new(
        max_concurrent: usize,
        queue_depth: usize,
        overflow_policy: OverflowPolicy,
    ) -> Option<Self> {
        if max_concurrent == 0 {
            None
        } else {
            Some(Self {
                max_concurrent,
                queue_depth,
                overflow_policy,
            })
        }
    }

    /// Conservative default derived from available CPU parallelism.
    ///
    /// Uses the "balanced profile" formula from bd-22n.4:
    /// `clamp(P / 8, 1, 16)` where `P = available_parallelism`.
    #[must_use]
    pub fn for_profile(profile: ParallelismProfile) -> Self {
        let p = available_parallelism_or_one();
        match profile {
            ParallelismProfile::Balanced => Self {
                max_concurrent: conservative_bg_cpu_max(p),
                queue_depth: 0,
                overflow_policy: OverflowPolicy::DropBusy,
            },
        }
    }

    /// Maximum admitted work units at once.
    #[must_use]
    pub const fn admission_limit(self) -> usize {
        self.max_concurrent.saturating_add(self.queue_depth)
    }
}

impl Default for BulkheadConfig {
    fn default() -> Self {
        Self::for_profile(ParallelismProfile::Balanced)
    }
}

/// Compute conservative default background CPU parallelism from `P`.
#[must_use]
pub const fn conservative_bg_cpu_max(p: usize) -> usize {
    let base = p / 8;
    if base == 0 {
        1
    } else if base > MAX_BALANCED_BG_CPU {
        MAX_BALANCED_BG_CPU
    } else {
        base
    }
}

/// Return `std::thread::available_parallelism()` with a safe floor of 1.
#[must_use]
pub fn available_parallelism_or_one() -> usize {
    std::thread::available_parallelism().map_or(1, NonZeroUsize::get)
}

/// Chunking plan for SIMD-friendly wide-word loops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WideChunkLayout {
    /// Number of `u128` chunks processed.
    pub u128_chunks: usize,
    /// Number of `u64` chunks processed after `u128` chunks.
    pub u64_chunks: usize,
    /// Remaining tail bytes processed scalar.
    pub tail_bytes: usize,
}

impl WideChunkLayout {
    /// Compute the wide-chunk layout for a byte length.
    #[must_use]
    pub const fn for_len(len: usize) -> Self {
        let u128_chunks = len / 16;
        let rem_after_u128 = len % 16;
        let u64_chunks = rem_after_u128 / 8;
        let tail_bytes = rem_after_u128 % 8;
        Self {
            u128_chunks,
            u64_chunks,
            tail_bytes,
        }
    }
}

/// XOR patch application using `u128` + `u64` + tail loops.
///
/// This is the SIMD-friendly primitive for hot patch paths. LLVM can
/// auto-vectorize the wide integer loops.
#[allow(clippy::incompatible_msrv)]
pub fn xor_patch_wide_chunks(dst: &mut [u8], patch: &[u8]) -> Result<WideChunkLayout> {
    if dst.len() != patch.len() {
        return Err(FrankenError::TypeMismatch {
            expected: format!("equal lengths (dst == patch), got {}", dst.len()),
            actual: patch.len().to_string(),
        });
    }

    let layout = WideChunkLayout::for_len(dst.len());

    let (dst_128, dst_rem) = dst.as_chunks_mut::<16>();
    let (patch_128, patch_rem) = patch.as_chunks::<16>();
    for (d, p) in dst_128.iter_mut().zip(patch_128.iter()) {
        let d_word = u128::from_ne_bytes(*d);
        let p_word = u128::from_ne_bytes(*p);
        *d = (d_word ^ p_word).to_ne_bytes();
    }

    let (dst_64, dst_tail) = dst_rem.as_chunks_mut::<8>();
    let (patch_64, patch_tail) = patch_rem.as_chunks::<8>();
    for (d, p) in dst_64.iter_mut().zip(patch_64.iter()) {
        let d_word = u64::from_ne_bytes(*d);
        let p_word = u64::from_ne_bytes(*p);
        *d = (d_word ^ p_word).to_ne_bytes();
    }

    for (d, p) in dst_tail.iter_mut().zip(patch_tail.iter()) {
        *d ^= *p;
    }

    Ok(layout)
}

/// GF(256) addition (`+`) using wide XOR chunk loops.
///
/// In GF(256), addition is XOR, so this uses the same SIMD-friendly chunking
/// strategy as [`xor_patch_wide_chunks`].
pub fn gf256_add_assign_chunked(dst: &mut [u8], src: &[u8]) -> Result<WideChunkLayout> {
    xor_patch_wide_chunks(dst, src)
}

/// RaptorQ symbol add (`dst ^= src`) using chunked XOR.
///
/// This is the core symbol-add primitive from §3.2.2.
pub fn symbol_add_assign(dst: &mut [u8], src: &[u8]) -> Result<WideChunkLayout> {
    debug!(
        bead_id = "bd-1hi.2",
        op = "symbol_add_assign",
        symbol_len = dst.len(),
        "applying in-place XOR over symbol bytes"
    );
    gf256_add_assign_chunked(dst, src)
}

/// RaptorQ symbol scalar multiply (`out = c * src`) in GF(256).
///
/// Special cases:
/// - `c == 0`: zero output
/// - `c == 1`: copy input
pub fn symbol_mul_into(coeff: u8, src: &[u8], out: &mut [u8]) -> Result<()> {
    if src.len() != out.len() {
        error!(
            bead_id = "bd-1hi.2",
            op = "symbol_mul_into",
            coeff,
            src_len = src.len(),
            out_len = out.len(),
            "symbol length mismatch"
        );
        return Err(FrankenError::TypeMismatch {
            expected: format!("equal lengths (src == out), got {}", src.len()),
            actual: out.len().to_string(),
        });
    }

    debug!(
        bead_id = "bd-1hi.2",
        op = "symbol_mul_into",
        coeff,
        symbol_len = src.len(),
        "applying GF(256) scalar multiplication"
    );

    match coeff {
        0 => {
            out.fill(0);
            Ok(())
        }
        1 => {
            out.copy_from_slice(src);
            Ok(())
        }
        _ => {
            let mut out_chunks = out.chunks_exact_mut(16);
            let mut src_chunks = src.chunks_exact(16);

            for (dst_chunk, src_chunk) in out_chunks.by_ref().zip(src_chunks.by_ref()) {
                for (dst_byte, src_byte) in dst_chunk.iter_mut().zip(src_chunk.iter()) {
                    *dst_byte = gf256_mul_byte(coeff, *src_byte);
                }
            }
            for (dst_byte, src_byte) in out_chunks
                .into_remainder()
                .iter_mut()
                .zip(src_chunks.remainder().iter())
            {
                *dst_byte = gf256_mul_byte(coeff, *src_byte);
            }
            Ok(())
        }
    }
}

/// RaptorQ fused multiply-add (`dst ^= c * src`) in GF(256).
///
/// Special cases:
/// - `c == 0`: no-op
/// - `c == 1`: pure XOR path
pub fn symbol_addmul_assign(dst: &mut [u8], coeff: u8, src: &[u8]) -> Result<WideChunkLayout> {
    if dst.len() != src.len() {
        error!(
            bead_id = "bd-1hi.2",
            op = "symbol_addmul_assign",
            coeff,
            dst_len = dst.len(),
            src_len = src.len(),
            "symbol length mismatch"
        );
        return Err(FrankenError::TypeMismatch {
            expected: format!("equal lengths (dst == src), got {}", dst.len()),
            actual: src.len().to_string(),
        });
    }

    debug!(
        bead_id = "bd-1hi.2",
        op = "symbol_addmul_assign",
        coeff,
        symbol_len = dst.len(),
        "applying fused multiply-and-add over symbol bytes"
    );

    match coeff {
        0 => Ok(WideChunkLayout::for_len(dst.len())),
        1 => symbol_add_assign(dst, src),
        _ => {
            let mut dst_chunks = dst.chunks_exact_mut(16);
            let mut src_chunks = src.chunks_exact(16);

            for (dst_chunk, src_chunk) in dst_chunks.by_ref().zip(src_chunks.by_ref()) {
                for (dst_byte, src_byte) in dst_chunk.iter_mut().zip(src_chunk.iter()) {
                    *dst_byte ^= gf256_mul_byte(coeff, *src_byte);
                }
            }
            for (dst_byte, src_byte) in dst_chunks
                .into_remainder()
                .iter_mut()
                .zip(src_chunks.remainder().iter())
            {
                *dst_byte ^= gf256_mul_byte(coeff, *src_byte);
            }
            Ok(WideChunkLayout::for_len(dst.len()))
        }
    }
}

/// Compute xxhash3 + blake3 on a contiguous input buffer.
///
/// - `xxhash3` path comes from `SymbolRecord::new` (`frame_xxh3`).
/// - `blake3` path comes from `PayloadHash::blake3`.
pub fn simd_friendly_checksum_pair(buffer: &[u8]) -> Result<(u64, [u8; 32])> {
    let symbol_size = u32::try_from(buffer.len()).map_err(|_| FrankenError::OutOfRange {
        what: "symbol_size".to_owned(),
        value: buffer.len().to_string(),
    })?;

    let symbol_record = SymbolRecord::new(
        ObjectId::from_bytes([0_u8; ObjectId::LEN]),
        Oti {
            f: u64::from(symbol_size),
            al: 1,
            t: symbol_size,
            z: 1,
            n: 1,
        },
        0,
        buffer.to_vec(),
        SymbolRecordFlags::empty(),
    );
    let blake = PayloadHash::blake3(buffer);

    Ok((symbol_record.frame_xxh3, *blake.as_bytes()))
}

/// Non-blocking bulkhead admission gate.
#[derive(Debug)]
pub struct Bulkhead {
    config: BulkheadConfig,
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
    busy_rejections: AtomicUsize,
}

impl Bulkhead {
    #[must_use]
    pub fn new(config: BulkheadConfig) -> Self {
        Self {
            config,
            in_flight: AtomicUsize::new(0),
            peak_in_flight: AtomicUsize::new(0),
            busy_rejections: AtomicUsize::new(0),
        }
    }

    #[must_use]
    pub const fn config(&self) -> BulkheadConfig {
        self.config
    }

    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn peak_in_flight(&self) -> usize {
        self.peak_in_flight.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn busy_rejections(&self) -> usize {
        self.busy_rejections.load(Ordering::Acquire)
    }

    /// Try to admit one work item.
    ///
    /// Never blocks. If the admission budget is exhausted, this returns
    /// `FrankenError::Busy`.
    pub fn try_acquire(&self) -> Result<BulkheadPermit<'_>> {
        let limit = self.config.admission_limit();
        loop {
            let current = self.in_flight.load(Ordering::Acquire);
            if current >= limit {
                self.busy_rejections.fetch_add(1, Ordering::AcqRel);
                return Err(match self.config.overflow_policy {
                    OverflowPolicy::DropBusy => FrankenError::Busy,
                });
            }

            let next = current.saturating_add(1);
            if self
                .in_flight
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.peak_in_flight.fetch_max(next, Ordering::AcqRel);
                return Ok(BulkheadPermit {
                    bulkhead: self,
                    released: false,
                });
            }
        }
    }

    /// Run work within a bulkhead permit.
    pub fn run<T>(&self, work: impl FnOnce() -> T) -> Result<T> {
        let _permit = self.try_acquire()?;
        Ok(work())
    }
}

/// RAII permit for a single admitted work item.
#[derive(Debug)]
pub struct BulkheadPermit<'a> {
    bulkhead: &'a Bulkhead,
    released: bool,
}

impl BulkheadPermit<'_> {
    /// Explicitly release the permit.
    pub fn release(mut self) {
        if !self.released {
            self.bulkhead.in_flight.fetch_sub(1, Ordering::AcqRel);
            self.released = true;
        }
    }
}

impl Drop for BulkheadPermit<'_> {
    fn drop(&mut self) {
        if !self.released {
            self.bulkhead.in_flight.fetch_sub(1, Ordering::AcqRel);
            self.released = true;
        }
    }
}

/// Region-owned wrapper used for structured-concurrency integration.
#[derive(Debug)]
pub struct RegionBulkhead {
    region: Region,
    bulkhead: Bulkhead,
    closing: AtomicBool,
}

impl RegionBulkhead {
    #[must_use]
    pub fn new(region: Region, config: BulkheadConfig) -> Self {
        Self {
            region,
            bulkhead: Bulkhead::new(config),
            closing: AtomicBool::new(false),
        }
    }

    #[must_use]
    pub const fn region(&self) -> Region {
        self.region
    }

    #[must_use]
    pub fn bulkhead(&self) -> &Bulkhead {
        &self.bulkhead
    }

    pub fn try_acquire(&self) -> Result<BulkheadPermit<'_>> {
        if self.closing.load(Ordering::Acquire) {
            return Err(FrankenError::Busy);
        }
        self.bulkhead.try_acquire()
    }

    /// Begin region close: no new admissions are allowed after this point.
    pub fn begin_close(&self) {
        self.closing.store(true, Ordering::Release);
    }

    /// Whether all region-owned work has quiesced.
    #[must_use]
    pub fn is_quiescent(&self) -> bool {
        self.bulkhead.in_flight() == 0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::thread;
    use std::time::{Duration, Instant};

    use asupersync::raptorq::decoder::{InactivationDecoder, ReceivedSymbol};
    use asupersync::raptorq::gf256::{Gf256, gf256_add_slice, gf256_addmul_slice, gf256_mul_slice};
    use asupersync::raptorq::systematic::{ConstraintMatrix, SystematicEncoder};
    use asupersync::raptorq::{RaptorQReceiverBuilder, RaptorQSenderBuilder};
    use asupersync::security::AuthenticationTag;
    use asupersync::security::authenticated::AuthenticatedSymbol;
    use asupersync::transport::error::{SinkError, StreamError};
    use asupersync::transport::sink::SymbolSink;
    use asupersync::transport::stream::SymbolStream;
    use asupersync::types::{ObjectId as AsObjectId, ObjectParams, Symbol};
    use asupersync::{Cx, RaptorQConfig};
    use fsqlite_btree::compare_key_bytes_contiguous;

    use super::*;

    const BEAD_ID: &str = "bd-22n.4";
    const SIMD_BEAD_ID: &str = "bd-22n.6";
    const RAPTORQ_BEAD_ID: &str = "bd-1hi.2";

    #[derive(Debug)]
    struct VecSink {
        symbols: Vec<Symbol>,
    }

    impl VecSink {
        fn new() -> Self {
            Self {
                symbols: Vec::new(),
            }
        }
    }

    impl SymbolSink for VecSink {
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

    #[derive(Debug)]
    struct VecStream {
        q: VecDeque<AuthenticatedSymbol>,
    }

    impl VecStream {
        fn new(symbols: Vec<Symbol>) -> Self {
            let q = symbols
                .into_iter()
                .map(|symbol| AuthenticatedSymbol::new_verified(symbol, AuthenticationTag::zero()))
                .collect();
            Self { q }
        }
    }

    impl SymbolStream for VecStream {
        fn poll_next(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<std::result::Result<AuthenticatedSymbol, StreamError>>> {
            match self.q.pop_front() {
                Some(symbol) => Poll::Ready(Some(Ok(symbol))),
                None => Poll::Ready(None),
            }
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.q.len(), Some(self.q.len()))
        }

        fn is_exhausted(&self) -> bool {
            self.q.is_empty()
        }
    }

    fn raptorq_config(symbol_size: u16, repair_overhead: f64) -> RaptorQConfig {
        let mut config = RaptorQConfig::default();
        config.encoding.symbol_size = symbol_size;
        config.encoding.max_block_size = 64 * 1024;
        config.encoding.repair_overhead = repair_overhead;
        config
    }

    fn deterministic_payload(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed ^ 0x9E37_79B9_7F4A_7C15;
        let mut out = Vec::with_capacity(len);
        for idx in 0..len {
            state ^= state << 7;
            state ^= state >> 9;
            state = state.wrapping_mul(0xA24B_AED4_963E_E407);
            let idx_byte = u8::try_from(idx % 251).expect("modulo fits in u8");
            out.push(u8::try_from(state & 0xFF).expect("masked to u8") ^ idx_byte);
        }
        out
    }

    fn xor_patch_bytewise(dst: &mut [u8], patch: &[u8]) {
        for (dst_byte, patch_byte) in dst.iter_mut().zip(patch.iter()) {
            *dst_byte ^= *patch_byte;
        }
    }

    fn gf256_mul_bytewise(coeff: u8, src: &[u8], out: &mut [u8]) {
        for (dst_byte, src_byte) in out.iter_mut().zip(src.iter()) {
            *dst_byte = gf256_mul_byte(coeff, *src_byte);
        }
    }

    fn collect_rs_files(root: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let entries = std::fs::read_dir(root).expect("read_dir should succeed");
        for entry in entries {
            let path = entry.expect("read_dir entry should be readable").path();
            if path.is_dir() {
                collect_rs_files(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }

    fn encode_symbols(
        config: RaptorQConfig,
        object_id: AsObjectId,
        data: &[u8],
    ) -> (Vec<Symbol>, usize) {
        let cx = Cx::for_testing();
        let mut sender = RaptorQSenderBuilder::new()
            .config(config)
            .transport(VecSink::new())
            .build()
            .expect("sender build");
        let outcome = sender
            .send_object(&cx, object_id, data)
            .expect("send_object must succeed");
        let symbols = std::mem::take(&mut sender.transport_mut().symbols);
        tracing::debug!(
            bead_id = RAPTORQ_BEAD_ID,
            case = "encode_symbols",
            source_symbols = outcome.source_symbols,
            emitted_symbols = symbols.len(),
            object_size = data.len(),
            "encoded object into source+repair symbol stream"
        );
        (symbols, outcome.source_symbols)
    }

    #[allow(clippy::result_large_err)]
    fn decode_symbols(
        config: RaptorQConfig,
        object_id: AsObjectId,
        object_size: usize,
        source_symbols: usize,
        symbols: Vec<Symbol>,
    ) -> std::result::Result<Vec<u8>, asupersync::Error> {
        let cx = Cx::for_testing();
        let params = ObjectParams::new(
            object_id,
            u64::try_from(object_size).expect("object size fits u64"),
            config.encoding.symbol_size,
            1,
            u16::try_from(source_symbols).expect("source symbol count fits u16"),
        );
        let mut receiver = RaptorQReceiverBuilder::new()
            .config(config)
            .source(VecStream::new(symbols))
            .build()
            .expect("receiver build");

        receiver
            .receive_object(&cx, &params)
            .map(|outcome| outcome.data)
    }

    fn split_source_and_repair(
        symbols: &[Symbol],
        source_symbols: usize,
    ) -> (Vec<Symbol>, Vec<Symbol>) {
        let source_symbols_u32 =
            u32::try_from(source_symbols).expect("source symbol count fits u32");
        let mut sources = Vec::new();
        let mut repairs = Vec::new();
        for symbol in symbols {
            if symbol.esi() < source_symbols_u32 {
                sources.push(symbol.clone());
            } else {
                repairs.push(symbol.clone());
            }
        }
        (sources, repairs)
    }

    fn low_level_source_block(k: usize, symbol_size: usize, seed: u64) -> Vec<Vec<u8>> {
        (0..k)
            .map(|source_index| {
                deterministic_payload(
                    symbol_size,
                    seed + u64::try_from(source_index).expect("source index fits u64"),
                )
            })
            .collect()
    }

    fn append_source_received_symbols(
        received: &mut Vec<ReceivedSymbol>,
        constraints: &ConstraintMatrix,
        base_rows: usize,
        source: &[Vec<u8>],
        source_indexes: &[usize],
    ) {
        for &source_index in source_indexes {
            let row = base_rows + source_index;
            let mut columns = Vec::new();
            let mut coefficients = Vec::new();
            for col in 0..constraints.cols {
                let coeff = constraints.get(row, col);
                if !coeff.is_zero() {
                    columns.push(col);
                    coefficients.push(coeff);
                }
            }

            received.push(ReceivedSymbol {
                esi: u32::try_from(source_index).expect("source index fits u32"),
                is_source: true,
                columns,
                coefficients,
                data: source[source_index].clone(),
            });
        }
    }

    #[test]
    fn test_parallelism_defaults_conservative() {
        assert_eq!(
            conservative_bg_cpu_max(16),
            2,
            "bead_id={BEAD_ID} case=balanced_profile_formula_p16"
        );
        assert_eq!(
            conservative_bg_cpu_max(1),
            1,
            "bead_id={BEAD_ID} case=balanced_profile_min_floor"
        );
        assert_eq!(
            conservative_bg_cpu_max(512),
            16,
            "bead_id={BEAD_ID} case=balanced_profile_max_cap"
        );
    }

    #[test]
    fn test_parallelism_bounded_by_available() {
        let cfg = BulkheadConfig::default();
        let p = available_parallelism_or_one();
        assert!(
            cfg.max_concurrent <= p,
            "bead_id={BEAD_ID} case=default_exceeds_available_parallelism cfg={cfg:?} p={p}"
        );

        let bulkhead = Bulkhead::new(cfg);
        let mut permits = Vec::new();
        for _ in 0..cfg.admission_limit() {
            permits.push(
                bulkhead
                    .try_acquire()
                    .expect("admission under configured limit should succeed"),
            );
        }

        let overflow = bulkhead.try_acquire();
        assert!(
            matches!(overflow, Err(FrankenError::Busy)),
            "bead_id={BEAD_ID} case=bounded_admission_overflow_must_be_busy overflow={overflow:?}"
        );

        drop(permits);
        assert_eq!(
            bulkhead.in_flight(),
            0,
            "bead_id={BEAD_ID} case=permits_drop_to_zero"
        );
    }

    #[test]
    fn test_bulkhead_config_max_concurrent() {
        let cfg = BulkheadConfig::new(3, 0, OverflowPolicy::DropBusy)
            .expect("non-zero max_concurrent must be valid");
        let bulkhead = Bulkhead::new(cfg);

        let p1 = bulkhead.try_acquire().expect("slot 1");
        let p2 = bulkhead.try_acquire().expect("slot 2");
        let p3 = bulkhead.try_acquire().expect("slot 3");
        let overflow = bulkhead.try_acquire();

        assert!(
            matches!(overflow, Err(FrankenError::Busy)),
            "bead_id={BEAD_ID} case=max_concurrent_enforced overflow={overflow:?}"
        );
        drop((p1, p2, p3));
    }

    #[test]
    fn test_overflow_policy_drop_with_busy() {
        let cfg = BulkheadConfig::new(1, 0, OverflowPolicy::DropBusy)
            .expect("non-zero max_concurrent must be valid");
        let bulkhead = Bulkhead::new(cfg);
        let _permit = bulkhead.try_acquire().expect("first permit must succeed");

        let overflow = bulkhead.try_acquire();
        assert!(
            matches!(overflow, Err(FrankenError::Busy)),
            "bead_id={BEAD_ID} case=overflow_policy_drop_busy overflow={overflow:?}"
        );
    }

    #[test]
    fn test_background_work_degrades_gracefully() {
        let cfg = BulkheadConfig::new(2, 0, OverflowPolicy::DropBusy)
            .expect("non-zero max_concurrent must be valid");
        let bulkhead = Bulkhead::new(cfg);

        let _a = bulkhead.try_acquire().expect("permit a");
        let _b = bulkhead.try_acquire().expect("permit b");

        for _ in 0..8 {
            let result = bulkhead.try_acquire();
            assert!(
                matches!(result, Err(FrankenError::Busy)),
                "bead_id={BEAD_ID} case=overflow_must_reject_not_wait result={result:?}"
            );
        }

        assert_eq!(
            bulkhead.busy_rejections(),
            8,
            "bead_id={BEAD_ID} case=busy_rejection_counter"
        );
    }

    #[test]
    fn test_region_integration() {
        let cfg = BulkheadConfig::new(1, 0, OverflowPolicy::DropBusy)
            .expect("non-zero max_concurrent must be valid");
        let region_bulkhead = RegionBulkhead::new(Region::new(7), cfg);
        assert_eq!(
            region_bulkhead.region().get(),
            7,
            "bead_id={BEAD_ID} case=region_id_plumbed"
        );

        let permit = region_bulkhead.try_acquire().expect("first permit");
        assert!(
            !region_bulkhead.is_quiescent(),
            "bead_id={BEAD_ID} case=region_non_quiescent_with_active_work"
        );

        region_bulkhead.begin_close();
        let after_close = region_bulkhead.try_acquire();
        assert!(
            matches!(after_close, Err(FrankenError::Busy)),
            "bead_id={BEAD_ID} case=region_close_blocks_new_work result={after_close:?}"
        );

        drop(permit);
        assert!(
            region_bulkhead.is_quiescent(),
            "bead_id={BEAD_ID} case=region_quiescent_after_permit_drop"
        );
    }

    #[test]
    fn test_gf256_ops_chunked() {
        let mut dst = vec![0xAA_u8; 40];
        let src = vec![0x55_u8; 40];
        let expected: Vec<u8> = dst.iter().zip(src.iter()).map(|(d, s)| *d ^ *s).collect();

        let layout = gf256_add_assign_chunked(&mut dst, &src)
            .expect("equal-length buffers should be accepted");
        assert!(
            layout.u128_chunks > 0 || layout.u64_chunks > 0,
            "bead_id={SIMD_BEAD_ID} case=wide_chunks_expected layout={layout:?}"
        );
        assert_eq!(
            dst, expected,
            "bead_id={SIMD_BEAD_ID} case=gf256_addition_xor_equivalence"
        );
    }

    #[test]
    fn test_xor_patch_wide_chunks() {
        let mut dst = vec![0xF0_u8; 37];
        let patch = vec![0x0F_u8; 37];
        let expected: Vec<u8> = dst.iter().zip(patch.iter()).map(|(d, p)| *d ^ *p).collect();

        let layout =
            xor_patch_wide_chunks(&mut dst, &patch).expect("equal-length buffers should be valid");
        assert_eq!(
            layout,
            WideChunkLayout {
                u128_chunks: 2,
                u64_chunks: 0,
                tail_bytes: 5,
            },
            "bead_id={SIMD_BEAD_ID} case=chunk_layout_expected"
        );
        assert_eq!(
            dst, expected,
            "bead_id={SIMD_BEAD_ID} case=xor_patch_matches_scalar_reference"
        );
    }

    #[test]
    fn test_xor_symbols_u64_chunks() {
        // Length 24 exercises both the u128 and u64 lanes.
        let mut dst = vec![0xAB_u8; 24];
        let src = vec![0xCD_u8; 24];
        let mut expected = vec![0xAB_u8; 24];
        xor_patch_bytewise(&mut expected, &src);

        let layout = xor_patch_wide_chunks(&mut dst, &src).expect("equal-length buffers");
        assert_eq!(
            layout,
            WideChunkLayout {
                u128_chunks: 1,
                u64_chunks: 1,
                tail_bytes: 0,
            },
            "bead_id=bd-2ddc case=u64_chunk_lane_exercised"
        );
        assert_eq!(
            dst, expected,
            "bead_id=bd-2ddc case=chunked_xor_matches_bytewise_reference"
        );
    }

    #[test]
    fn test_gf256_multiply_chunks() {
        let coeff = 0xA7_u8;
        let src = deterministic_payload(4096, 0xDDCC_BBAA_1122_3344);
        let mut chunked = vec![0_u8; src.len()];
        let mut scalar = vec![0_u8; src.len()];

        symbol_mul_into(coeff, &src, &mut chunked).expect("chunked symbol_mul_into");
        gf256_mul_bytewise(coeff, &src, &mut scalar);

        assert_eq!(
            chunked, scalar,
            "bead_id=bd-2ddc case=chunked_mul_matches_scalar_reference"
        );
    }

    #[test]
    fn test_u128_chunk_alignment() {
        // Non-multiple of 16 exercises the u128 path + tail handling.
        let mut via_wide = deterministic_payload(4099, 0x1234_5678_9ABC_DEF0);
        let mut via_u64_only = via_wide.clone();
        let patch = deterministic_payload(4099, 0x0F0E_0D0C_0B0A_0908);

        xor_patch_wide_chunks(&mut via_wide, &patch).expect("wide chunk xor");

        let mut dst_u64_chunks = via_u64_only.chunks_exact_mut(8);
        let mut patch_u64_chunks = patch.chunks_exact(8);
        for (dst_chunk, patch_chunk) in dst_u64_chunks.by_ref().zip(patch_u64_chunks.by_ref()) {
            let dst_word = u64::from_ne_bytes(
                dst_chunk
                    .try_into()
                    .expect("chunks_exact(8) must yield 8-byte chunk"),
            );
            let patch_word = u64::from_ne_bytes(
                patch_chunk
                    .try_into()
                    .expect("chunks_exact(8) must yield 8-byte chunk"),
            );
            dst_chunk.copy_from_slice(&(dst_word ^ patch_word).to_ne_bytes());
        }
        for (dst_byte, patch_byte) in dst_u64_chunks
            .into_remainder()
            .iter_mut()
            .zip(patch_u64_chunks.remainder().iter())
        {
            *dst_byte ^= *patch_byte;
        }

        assert_eq!(
            via_wide, via_u64_only,
            "bead_id=bd-2ddc case=u128_lane_matches_u64_plus_tail"
        );
    }

    #[test]
    fn test_benchmark_chunk_vs_byte() {
        // Meaningful performance checks require optimized codegen.
        if cfg!(debug_assertions) {
            return;
        }

        let iterations = 32_000_usize;
        let src = deterministic_payload(4096, 0xDEAD_BEEF_F00D_CAFE);
        let base = deterministic_payload(4096, 0x0123_4567_89AB_CDEF);

        let mut chunked = base.clone();
        let chunked_start = Instant::now();
        for _ in 0..iterations {
            xor_patch_wide_chunks(&mut chunked, &src).expect("chunked xor");
            std::hint::black_box(&chunked);
        }
        let chunked_elapsed = chunked_start.elapsed();

        let mut bytewise = base;
        let bytewise_start = Instant::now();
        for _ in 0..iterations {
            xor_patch_bytewise(&mut bytewise, &src);
            std::hint::black_box(&bytewise);
        }
        let bytewise_elapsed = bytewise_start.elapsed();

        let speedup = bytewise_elapsed.as_secs_f64() / chunked_elapsed.as_secs_f64();
        assert!(
            speedup >= 4.0,
            "bead_id=bd-2ddc case=chunk_vs_byte_speedup speedup={speedup:.2}x \
             chunked_ns={} bytewise_ns={} iterations={iterations}",
            chunked_elapsed.as_nanos(),
            bytewise_elapsed.as_nanos()
        );
    }

    #[test]
    fn test_no_unsafe_simd() {
        let manifest = include_str!("../../../Cargo.toml");
        assert!(
            manifest.contains(r#"unsafe_code = "forbid""#),
            "bead_id=bd-2ddc case=workspace_forbids_unsafe"
        );

        let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crate dir has parent")
            .parent()
            .expect("workspace root exists")
            .to_path_buf();
        let crates_dir = workspace_root.join("crates");

        let mut rs_files = Vec::new();
        collect_rs_files(&crates_dir, &mut rs_files);

        let simd_needles = [
            "_mm_",
            "std::arch::",
            "core::arch::",
            "__m128",
            "__m256",
            "__m512",
            "simd_shuffle",
            "vpxor",
            "vxorq",
        ];

        let mut offenders = Vec::new();
        for file in rs_files {
            let Ok(content) = std::fs::read_to_string(&file) else {
                continue;
            };

            let lines = content.lines().collect::<Vec<_>>();
            for (idx, line) in lines.iter().enumerate() {
                let has_intrinsic = simd_needles.iter().any(|needle| line.contains(needle));
                if !has_intrinsic {
                    continue;
                }

                let window_start = idx.saturating_sub(3);
                let window_end = (idx + 3).min(lines.len().saturating_sub(1));
                let mut found_unsafe_nearby = false;
                for nearby in &lines[window_start..=window_end] {
                    let trimmed = nearby.trim_start();
                    let is_comment = trimmed.starts_with("//");
                    if !is_comment && trimmed.contains("unsafe") {
                        found_unsafe_nearby = true;
                        break;
                    }
                }

                if found_unsafe_nearby {
                    offenders.push(format!("{}:{}", file.display(), idx + 1));
                }
            }
        }

        assert!(
            offenders.is_empty(),
            "bead_id=bd-2ddc case=no_unsafe_simd_intrinsics offenders={offenders:?}"
        );
    }

    #[test]
    fn test_checksum_simd_friendly() {
        let buffer = vec![0x11_u8; 256];
        let (xx_a, blake_a) =
            simd_friendly_checksum_pair(&buffer).expect("checksum pair must succeed");

        let mut modified = buffer;
        modified[255] ^= 0x01;
        let (xx_b, blake_b) =
            simd_friendly_checksum_pair(&modified).expect("checksum pair must succeed");

        assert_ne!(
            xx_a, xx_b,
            "bead_id={SIMD_BEAD_ID} case=xxhash3_changes_on_byte_flip"
        );
        assert_ne!(
            blake_a, blake_b,
            "bead_id={SIMD_BEAD_ID} case=blake3_changes_on_byte_flip"
        );
    }

    #[test]
    fn test_e2e_bounded_parallelism_under_background_load() {
        let cfg = BulkheadConfig::new(4, 0, OverflowPolicy::DropBusy)
            .expect("non-zero max_concurrent must be valid");
        let bulkhead = Arc::new(Bulkhead::new(cfg));

        let handles: Vec<_> = (0..48)
            .map(|_| {
                let bulkhead = Arc::clone(&bulkhead);
                thread::spawn(move || {
                    bulkhead.run(|| {
                        thread::sleep(Duration::from_millis(10));
                    })
                })
            })
            .collect();

        let mut busy = 0_usize;
        for handle in handles {
            match handle.join().expect("worker thread should not panic") {
                Ok(()) => {}
                Err(FrankenError::Busy) => busy = busy.saturating_add(1),
                Err(err) => {
                    assert_eq!(
                        err.error_code(),
                        fsqlite_error::ErrorCode::Busy,
                        "bead_id={BEAD_ID} case=e2e_unexpected_bulkhead_error err={err}"
                    );
                    busy = busy.saturating_add(1);
                }
            }
        }

        assert!(
            busy > 0,
            "bead_id={BEAD_ID} case=e2e_should_observe_overflow_rejections"
        );
        assert!(
            bulkhead.peak_in_flight() <= cfg.admission_limit(),
            "bead_id={BEAD_ID} case=e2e_peak_parallelism_exceeded peak={} limit={}",
            bulkhead.peak_in_flight(),
            cfg.admission_limit()
        );
    }

    #[test]
    fn test_e2e_simd_hot_path_correctness() {
        // 1) B-tree hot comparison over contiguous slices.
        let contiguous = b"key-0001key-0002".to_vec();
        let left = &contiguous[0..8];
        let right = &contiguous[8..16];
        let compare_start = Instant::now();
        assert_eq!(
            compare_key_bytes_contiguous(left, right),
            left.cmp(right),
            "bead_id={SIMD_BEAD_ID} case=btree_contiguous_compare_correct"
        );
        let compare_elapsed = compare_start.elapsed();

        // 2) GF(256) add (XOR) and XOR patch helpers.
        let mut symbol_a = (0_u8..64).collect::<Vec<u8>>();
        let symbol_b = (64_u8..128).collect::<Vec<u8>>();
        let expected_add: Vec<u8> = symbol_a
            .iter()
            .zip(symbol_b.iter())
            .map(|(a, b)| *a ^ *b)
            .collect();
        let gf256_start = Instant::now();
        gf256_add_assign_chunked(&mut symbol_a, &symbol_b).expect("gf256 add should succeed");
        let gf256_elapsed = gf256_start.elapsed();
        assert_eq!(
            symbol_a, expected_add,
            "bead_id={SIMD_BEAD_ID} case=gf256_chunked_add_correct"
        );

        let mut patch_target = vec![0x33_u8; 64];
        let patch = vec![0xCC_u8; 64];
        let xor_start = Instant::now();
        xor_patch_wide_chunks(&mut patch_target, &patch).expect("xor patch should succeed");
        let xor_elapsed = xor_start.elapsed();
        assert!(
            patch_target.iter().all(|&byte| byte == (0x33_u8 ^ 0xCC_u8)),
            "bead_id={SIMD_BEAD_ID} case=xor_patch_chunked_correct"
        );

        // 3) SIMD-friendly checksum feed.
        let checksum_start = Instant::now();
        let (xx, blake) =
            simd_friendly_checksum_pair(&patch_target).expect("checksum pair should succeed");
        let checksum_elapsed = checksum_start.elapsed();
        assert_ne!(
            xx, 0,
            "bead_id={SIMD_BEAD_ID} case=xxhash3_nonzero_for_nonempty_payload"
        );
        assert!(
            blake.iter().any(|&b| b != 0),
            "bead_id={SIMD_BEAD_ID} case=blake3_digest_nonzero"
        );

        eprintln!(
            "bead_id={SIMD_BEAD_ID} metric=simd_hot_path_ns compare={} gf256_add={} xor_patch={} checksum={}",
            compare_elapsed.as_nanos(),
            gf256_elapsed.as_nanos(),
            xor_elapsed.as_nanos(),
            checksum_elapsed.as_nanos()
        );
    }

    #[test]
    fn test_symbol_add_self_inverse() {
        let src = (0_u16..512)
            .map(|idx| u8::try_from(idx % 251).expect("modulo fits in u8"))
            .collect::<Vec<_>>();
        let mut dst = src.clone();
        symbol_add_assign(&mut dst, &src).expect("symbol_add should succeed");
        assert!(
            dst.iter().all(|byte| *byte == 0),
            "bead_id={RAPTORQ_BEAD_ID} case=symbol_add_self_inverse"
        );
    }

    #[test]
    fn test_symbol_add_commutative_and_associative() {
        let a = (0_u16..128)
            .map(|idx| u8::try_from((idx * 3) % 251).expect("modulo fits"))
            .collect::<Vec<_>>();
        let b = (0_u16..128)
            .map(|idx| u8::try_from((idx * 5 + 7) % 251).expect("modulo fits"))
            .collect::<Vec<_>>();
        let c = (0_u16..128)
            .map(|idx| u8::try_from((idx * 11 + 13) % 251).expect("modulo fits"))
            .collect::<Vec<_>>();

        let mut ab = a.clone();
        symbol_add_assign(&mut ab, &b).expect("a+b");
        let mut ba = b.clone();
        symbol_add_assign(&mut ba, &a).expect("b+a");
        assert_eq!(
            ab, ba,
            "bead_id={RAPTORQ_BEAD_ID} case=symbol_add_commutative"
        );

        let mut lhs = a.clone();
        symbol_add_assign(&mut lhs, &b).expect("(a+b)");
        symbol_add_assign(&mut lhs, &c).expect("(a+b)+c");

        let mut rhs = b;
        symbol_add_assign(&mut rhs, &c).expect("(b+c)");
        let mut rhs2 = a;
        symbol_add_assign(&mut rhs2, &rhs).expect("a+(b+c)");

        assert_eq!(
            lhs, rhs2,
            "bead_id={RAPTORQ_BEAD_ID} case=symbol_add_associative"
        );
    }

    #[test]
    fn test_symbol_mul_special_cases() {
        let src = (0_u16..256)
            .map(|idx| u8::try_from(idx).expect("idx fits"))
            .collect::<Vec<_>>();

        let mut out_zero = vec![0_u8; src.len()];
        symbol_mul_into(0, &src, &mut out_zero).expect("mul by zero");
        assert!(
            out_zero.iter().all(|byte| *byte == 0),
            "bead_id={RAPTORQ_BEAD_ID} case=symbol_mul_zero"
        );

        let mut out_one = vec![0_u8; src.len()];
        symbol_mul_into(1, &src, &mut out_one).expect("mul by one");
        assert_eq!(
            out_one, src,
            "bead_id={RAPTORQ_BEAD_ID} case=symbol_mul_identity"
        );
    }

    #[test]
    fn test_symbol_mul_matches_scalar_reference() {
        let src = (0_u16..512)
            .map(|idx| u8::try_from((idx * 7 + 17) % 251).expect("modulo fits"))
            .collect::<Vec<_>>();
        let coeff = 0xA7_u8;

        let mut out = vec![0_u8; src.len()];
        symbol_mul_into(coeff, &src, &mut out).expect("symbol mul");
        for (actual, input) in out.iter().zip(src.iter()) {
            let expected = gf256_mul_byte(coeff, *input);
            assert_eq!(
                *actual, expected,
                "bead_id={RAPTORQ_BEAD_ID} case=symbol_mul_scalar_match"
            );
        }
    }

    #[test]
    fn test_symbol_addmul_special_cases_and_equivalence() {
        let src = (0_u16..512)
            .map(|idx| u8::try_from((idx * 13 + 19) % 251).expect("modulo fits"))
            .collect::<Vec<_>>();
        let original = (0_u16..512)
            .map(|idx| u8::try_from((idx * 9 + 3) % 251).expect("modulo fits"))
            .collect::<Vec<_>>();

        let mut no_op = original.clone();
        symbol_addmul_assign(&mut no_op, 0, &src).expect("c=0");
        assert_eq!(
            no_op, original,
            "bead_id={RAPTORQ_BEAD_ID} case=symbol_addmul_c0_noop"
        );

        let mut xor_path = original.clone();
        symbol_addmul_assign(&mut xor_path, 1, &src).expect("c=1");
        let mut expected_xor = original.clone();
        symbol_add_assign(&mut expected_xor, &src).expect("xor reference");
        assert_eq!(
            xor_path, expected_xor,
            "bead_id={RAPTORQ_BEAD_ID} case=symbol_addmul_c1_equals_xor"
        );

        let coeff = 0x53_u8;
        let mut fused = original.clone();
        symbol_addmul_assign(&mut fused, coeff, &src).expect("fused");
        let mut mul = vec![0_u8; src.len()];
        symbol_mul_into(coeff, &src, &mut mul).expect("mul");
        let mut separate = original;
        symbol_add_assign(&mut separate, &mul).expect("add");
        assert_eq!(
            fused, separate,
            "bead_id={RAPTORQ_BEAD_ID} case=symbol_addmul_fused_equals_mul_plus_add"
        );
    }

    #[test]
    fn test_symbol_operations_4096_and_512() {
        for symbol_len in [4096_usize, 1024_usize, 512_usize] {
            let a = vec![0xAA_u8; symbol_len];
            let b = vec![0x55_u8; symbol_len];
            let mut sum = a.clone();
            let layout = symbol_add_assign(&mut sum, &b).expect("symbol add");
            assert_eq!(
                layout,
                WideChunkLayout::for_len(symbol_len),
                "bead_id={RAPTORQ_BEAD_ID} case=symbol_len_layout_consistency len={symbol_len}"
            );
            assert!(
                sum.iter().all(|byte| *byte == (0xAA_u8 ^ 0x55_u8)),
                "bead_id={RAPTORQ_BEAD_ID} case=symbol_add_expected_xor len={symbol_len}"
            );
        }
    }

    #[test]
    fn test_gf256_arithmetic_matches_asupersync() {
        for a in 0_u8..=u8::MAX {
            for b in 0_u8..=u8::MAX {
                assert_eq!(
                    gf256_mul_byte(a, b),
                    (Gf256(a) * Gf256(b)).raw(),
                    "bead_id={RAPTORQ_BEAD_ID} case=gf256_mul_parity a=0x{a:02X} b=0x{b:02X}"
                );
            }
        }
    }

    #[test]
    fn test_symbol_ops_match_asupersync_gf256_slices() {
        for symbol_len in [512_usize, 1024_usize, 4096_usize] {
            let src = deterministic_payload(symbol_len, 0xA5A5_0101);
            let dst = deterministic_payload(symbol_len, 0x5A5A_0202);

            let mut ours_add = dst.clone();
            symbol_add_assign(&mut ours_add, &src).expect("symbol add");
            let mut as_add = dst.clone();
            gf256_add_slice(&mut as_add, &src);
            assert_eq!(
                ours_add, as_add,
                "bead_id={RAPTORQ_BEAD_ID} case=asupersync_parity_add len={symbol_len}"
            );

            for coeff in [0_u8, 1_u8, 0x53_u8, 0xA7_u8] {
                let mut ours_mul = vec![0_u8; symbol_len];
                symbol_mul_into(coeff, &src, &mut ours_mul).expect("symbol mul");
                let mut as_mul = src.clone();
                gf256_mul_slice(&mut as_mul, Gf256(coeff));
                assert_eq!(
                    ours_mul, as_mul,
                    "bead_id={RAPTORQ_BEAD_ID} case=asupersync_parity_mul len={symbol_len} coeff=0x{coeff:02X}"
                );

                let mut ours_addmul = dst.clone();
                symbol_addmul_assign(&mut ours_addmul, coeff, &src).expect("symbol addmul");
                let mut as_addmul = dst.clone();
                gf256_addmul_slice(&mut as_addmul, &src, Gf256(coeff));
                assert_eq!(
                    ours_addmul, as_addmul,
                    "bead_id={RAPTORQ_BEAD_ID} case=asupersync_parity_addmul len={symbol_len} coeff=0x{coeff:02X}"
                );
            }
        }
    }

    #[test]
    fn test_encode_single_source_block() {
        let config = raptorq_config(512, 1.25);
        let symbol_size = usize::from(config.encoding.symbol_size);
        let k = 8_usize;
        let data = deterministic_payload(k * symbol_size, 0x0102_0304);
        let object_id = AsObjectId::new_for_test(1201);
        tracing::info!(
            bead_id = RAPTORQ_BEAD_ID,
            case = "test_encode_single_source_block",
            symbol_size,
            requested_k = k,
            "encoding source block"
        );
        let (symbols, source_symbols) = encode_symbols(config, object_id, &data);
        let (sources, repairs) = split_source_and_repair(&symbols, source_symbols);

        assert_eq!(
            source_symbols, k,
            "bead_id={RAPTORQ_BEAD_ID} case=encode_single_block_source_count"
        );
        assert_eq!(
            sources.len(),
            source_symbols,
            "bead_id={RAPTORQ_BEAD_ID} case=encode_single_block_source_partition"
        );
        assert!(
            !repairs.is_empty(),
            "bead_id={RAPTORQ_BEAD_ID} case=encode_single_block_repair_present"
        );
        assert!(
            symbols.iter().all(|symbol| symbol.len() == symbol_size),
            "bead_id={RAPTORQ_BEAD_ID} case=encode_single_block_symbol_size_consistent"
        );
    }

    #[test]
    fn test_decode_exact_k_symbols() {
        let symbol_size = 512_usize;
        let k = 16_usize;
        let seed = 0x0BAD_CAFE_u64;
        let source = low_level_source_block(k, symbol_size, seed);
        let encoder =
            SystematicEncoder::new(&source, symbol_size, seed).expect("systematic encoder");
        let decoder = InactivationDecoder::new(k, symbol_size, seed);
        let params = decoder.params();
        let base_rows = params.s + params.h;
        let constraints = ConstraintMatrix::build(params, seed);
        let mut received = decoder.constraint_symbols();
        let source_indexes = (0..k).collect::<Vec<_>>();
        append_source_received_symbols(
            &mut received,
            &constraints,
            base_rows,
            &source,
            &source_indexes,
        );

        tracing::warn!(
            bead_id = RAPTORQ_BEAD_ID,
            case = "test_decode_exact_k_symbols",
            source_symbols = k,
            "decoding with minimum symbol count (fragile recovery threshold)"
        );
        let decode_outcome = decoder
            .decode(&received)
            .expect("decode exact-k must succeed");
        assert_eq!(
            decode_outcome.source, source,
            "bead_id={RAPTORQ_BEAD_ID} case=decode_exact_k_symbols_roundtrip"
        );
        assert_eq!(
            decode_outcome.intermediate[0].len(),
            symbol_size,
            "bead_id={RAPTORQ_BEAD_ID} case=decode_exact_k_symbol_size"
        );
        assert_eq!(
            encoder.intermediate_symbol(0),
            decode_outcome.intermediate[0],
            "bead_id={RAPTORQ_BEAD_ID} case=decode_exact_k_intermediate_consistency"
        );
    }

    #[test]
    fn test_decode_with_repair_symbols() {
        let symbol_size = 512_usize;
        let k = 16_usize;
        let seed = 0xABC0_FED1_u64;
        let source = low_level_source_block(k, symbol_size, seed);
        let encoder =
            SystematicEncoder::new(&source, symbol_size, seed).expect("systematic encoder");
        let decoder = InactivationDecoder::new(k, symbol_size, seed);
        let params = decoder.params();
        let base_rows = params.s + params.h;
        let constraints = ConstraintMatrix::build(params, seed);

        let mut received = decoder.constraint_symbols();
        let source_indexes = (1..k).collect::<Vec<_>>();
        append_source_received_symbols(
            &mut received,
            &constraints,
            base_rows,
            &source,
            &source_indexes,
        );

        let repair_esi = u32::try_from(k).expect("k fits u32");
        let (columns, coefficients) = decoder.repair_equation(repair_esi);
        let repair_data = encoder.repair_symbol(repair_esi);
        received.push(ReceivedSymbol::repair(
            repair_esi,
            columns,
            coefficients,
            repair_data,
        ));

        let decode_outcome = decoder
            .decode(&received)
            .expect("decode with one repair must succeed");
        assert_eq!(
            decode_outcome.source, source,
            "bead_id={RAPTORQ_BEAD_ID} case=decode_with_repair_roundtrip"
        );
    }

    #[test]
    fn test_decode_insufficient_symbols() {
        let symbol_size = 512_usize;
        let k = 8_usize;
        let seed = 0xDEAD_BEEF_u64;
        let source = low_level_source_block(k, symbol_size, seed);
        let decoder = InactivationDecoder::new(k, symbol_size, seed);
        let params = decoder.params();
        let base_rows = params.s + params.h;
        let constraints = ConstraintMatrix::build(params, seed);
        let mut received = decoder.constraint_symbols();
        let source_indexes = (0..k.saturating_sub(1)).collect::<Vec<_>>();
        append_source_received_symbols(
            &mut received,
            &constraints,
            base_rows,
            &source,
            &source_indexes,
        );

        let decode = decoder.decode(&received);
        assert!(
            decode.is_err(),
            "bead_id={RAPTORQ_BEAD_ID} case=decode_insufficient_symbols unexpectedly succeeded"
        );
        if let Err(err) = decode {
            tracing::error!(
                bead_id = RAPTORQ_BEAD_ID,
                case = "test_decode_insufficient_symbols",
                error = ?err,
                "decode failed as expected due to insufficient symbols"
            );
        }
    }

    #[test]
    fn test_symbol_size_alignment() {
        let config = raptorq_config(4096, 1.20);
        let symbol_size = usize::from(config.encoding.symbol_size);
        let data = deterministic_payload(symbol_size * 3, 0x600D_1111);
        let object_id = AsObjectId::new_for_test(1205);
        let (symbols, source_symbols) = encode_symbols(config, object_id, &data);

        assert_eq!(
            source_symbols, 3,
            "bead_id={RAPTORQ_BEAD_ID} case=symbol_size_alignment_source_count"
        );
        assert!(
            symbol_size.is_power_of_two(),
            "bead_id={RAPTORQ_BEAD_ID} case=symbol_size_alignment_power_of_two"
        );
        assert_eq!(
            symbol_size % 512,
            0,
            "bead_id={RAPTORQ_BEAD_ID} case=symbol_size_alignment_sector_multiple"
        );
        assert!(
            symbols.iter().all(|symbol| symbol.len() == symbol_size),
            "bead_id={RAPTORQ_BEAD_ID} case=symbol_size_alignment_symbol_lengths"
        );
    }

    #[test]
    fn prop_encode_decode_roundtrip() {
        for seed in [11_u64, 29_u64, 43_u64, 71_u64] {
            for k in [8_usize, 16_usize] {
                let config = raptorq_config(512, 1.30);
                let symbol_size = usize::from(config.encoding.symbol_size);
                let data = deterministic_payload(k * symbol_size - 17, seed);
                let object_id = AsObjectId::new_for_test(2000 + seed);
                let (symbols, source_symbols) = encode_symbols(config.clone(), object_id, &data);
                let (sources, _) = split_source_and_repair(&symbols, source_symbols);
                let subset = sources
                    .iter()
                    .take(source_symbols)
                    .cloned()
                    .collect::<Vec<_>>();
                let decoded = decode_symbols(config, object_id, data.len(), source_symbols, subset)
                    .expect("property roundtrip decode");
                assert_eq!(
                    decoded, data,
                    "bead_id={RAPTORQ_BEAD_ID} case=prop_encode_decode_roundtrip seed={seed} k={k}"
                );
            }
        }
    }

    #[test]
    fn prop_any_k_of_n_suffices() {
        let symbol_size = 512_usize;
        let k = 16_usize;
        let sources = (0..k)
            .map(|symbol_idx| {
                deterministic_payload(
                    symbol_size,
                    0x5000_0000 + u64::try_from(symbol_idx).expect("index fits u64"),
                )
            })
            .collect::<Vec<_>>();

        let mut parity = vec![0_u8; symbol_size];
        for source in &sources {
            symbol_add_assign(&mut parity, source).expect("parity construction");
        }

        for omitted in 0..=k {
            let rebuilt = if omitted == k {
                sources.clone()
            } else {
                let mut recovered = parity.clone();
                for (index, source) in sources.iter().enumerate() {
                    if index != omitted {
                        symbol_add_assign(&mut recovered, source).expect("single-erasure recovery");
                    }
                }
                let mut rebuilt = sources.clone();
                rebuilt[omitted] = recovered;
                rebuilt
            };

            assert_eq!(
                rebuilt.len(),
                k,
                "bead_id={RAPTORQ_BEAD_ID} case=prop_any_k_of_n_subset_size omitted_index={omitted}"
            );
            assert_eq!(
                rebuilt, sources,
                "bead_id={RAPTORQ_BEAD_ID} case=prop_any_k_of_n_suffices omitted_index={omitted}"
            );
        }
    }

    #[test]
    fn prop_symbol_size_consistent() {
        for symbol_size in [512_u16, 1024_u16, 4096_u16] {
            for k in [4_usize, 8_usize] {
                let config = raptorq_config(symbol_size, 1.25);
                let size = usize::from(symbol_size);
                let object_id = AsObjectId::new_for_test(
                    u64::from(symbol_size) * 100 + u64::try_from(k).expect("k fits u64"),
                );
                let data = deterministic_payload(k * size - 3, u64::from(symbol_size));
                let (symbols, _) = encode_symbols(config, object_id, &data);
                assert!(
                    symbols.iter().all(|symbol| symbol.len() == size),
                    "bead_id={RAPTORQ_BEAD_ID} case=prop_symbol_size_consistent symbol_size={symbol_size} k={k}"
                );
            }
        }
    }

    #[test]
    fn test_e2e_symbol_ops_in_encode_decode_roundtrip() {
        for (run, k) in [8_usize, 16_usize, 64_usize].iter().copied().enumerate() {
            let config = raptorq_config(512, 1.30);
            let symbol_size = usize::from(config.encoding.symbol_size);
            let data = deterministic_payload(
                k * symbol_size,
                0x4455_6677 + u64::try_from(run).expect("run fits u64"),
            );
            let object_id = AsObjectId::new_for_test(3000 + u64::try_from(k).expect("k fits u64"));

            tracing::info!(
                bead_id = RAPTORQ_BEAD_ID,
                case = "test_e2e_symbol_ops_in_encode_decode_roundtrip",
                k,
                symbol_size,
                "starting encode/decode roundtrip"
            );
            let (symbols, source_symbols) = encode_symbols(config.clone(), object_id, &data);
            let (sources, _) = split_source_and_repair(&symbols, source_symbols);
            let subset = sources
                .iter()
                .take(source_symbols)
                .cloned()
                .collect::<Vec<_>>();
            let decoded = decode_symbols(config, object_id, data.len(), source_symbols, subset)
                .expect("e2e decode must succeed");

            assert_eq!(
                decoded, data,
                "bead_id={RAPTORQ_BEAD_ID} case=e2e_roundtrip_bytes k={k}"
            );

            let mut source_parity = vec![0_u8; symbol_size];
            for chunk in data.chunks_exact(symbol_size) {
                symbol_add_assign(&mut source_parity, chunk).expect("source parity xor");
            }

            let mut decoded_parity = vec![0_u8; symbol_size];
            for chunk in decoded.chunks_exact(symbol_size) {
                symbol_add_assign(&mut decoded_parity, chunk).expect("decoded parity xor");
            }

            assert_eq!(
                decoded_parity, source_parity,
                "bead_id={RAPTORQ_BEAD_ID} case=e2e_symbol_ops_parity k={k}"
            );
        }
    }
}
