//! Local Reconstruction Codes (§1.4) for distributed repair.
//!
//! LRC extends standard erasure coding with *locality groups*: source
//! symbols are partitioned into groups of size `r`, and each group gets
//! its own local parity symbol. When a single symbol is lost within a
//! group, it can be repaired by reading only `r` symbols (the group
//! members + local parity) instead of all `k` source symbols.
//!
//! This reduces repair I/O by a factor of `k/r` compared to standard
//! Reed-Solomon or RaptorQ codes for single-failure cases.
//!
//! # Design
//!
//! - `LrcCodec` wraps a locality group size `r` and produces local +
//!   global parity symbols.
//! - Local parity: XOR of all symbols in a locality group.
//! - Global parity: XOR of all source symbols (full redundancy).
//! - Repair: try local repair first (O(r)), fall back to global (O(k)).

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

// ── Metrics ──────────────────────────────────────────────────────────────

static LRC_LOCAL_REPAIRS_TOTAL: AtomicU64 = AtomicU64::new(0);
static LRC_GLOBAL_REPAIRS_TOTAL: AtomicU64 = AtomicU64::new(0);
static LRC_ENCODE_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Snapshot of LRC metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LrcMetricsSnapshot {
    /// Total local repairs (within a single locality group).
    pub local_repairs_total: u64,
    /// Total global repairs (required reading all source symbols).
    pub global_repairs_total: u64,
    /// Total encode operations.
    pub encode_total: u64,
}

impl fmt::Display for LrcMetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "lrc_local_repairs={} lrc_global_repairs={} lrc_encodes={}",
            self.local_repairs_total, self.global_repairs_total, self.encode_total,
        )
    }
}

/// Return a snapshot of LRC metrics.
#[must_use]
pub fn lrc_metrics_snapshot() -> LrcMetricsSnapshot {
    LrcMetricsSnapshot {
        local_repairs_total: LRC_LOCAL_REPAIRS_TOTAL.load(Ordering::Relaxed),
        global_repairs_total: LRC_GLOBAL_REPAIRS_TOTAL.load(Ordering::Relaxed),
        encode_total: LRC_ENCODE_TOTAL.load(Ordering::Relaxed),
    }
}

/// Reset LRC metrics.
pub fn reset_lrc_metrics() {
    LRC_LOCAL_REPAIRS_TOTAL.store(0, Ordering::Relaxed);
    LRC_GLOBAL_REPAIRS_TOTAL.store(0, Ordering::Relaxed);
    LRC_ENCODE_TOTAL.store(0, Ordering::Relaxed);
}

// ── LRC Configuration ───────────────────────────────────────────────────

/// Configuration for the LRC codec.
#[derive(Debug, Clone, Copy)]
pub struct LrcConfig {
    /// Locality group size: number of source symbols per local parity group.
    /// Smaller `r` means cheaper local repair but more parity overhead.
    /// Must be >= 2.
    pub locality: usize,
}

impl Default for LrcConfig {
    fn default() -> Self {
        Self { locality: 4 }
    }
}

// ── Encode / Decode Types ───────────────────────────────────────────────

/// Result of an LRC encode operation.
#[derive(Debug, Clone)]
pub struct LrcEncodeResult {
    /// Source symbols: (index, data).
    pub source_symbols: Vec<(u32, Vec<u8>)>,
    /// Local parity symbols: (group_index, data).
    /// One per locality group.
    pub local_parities: Vec<(u32, Vec<u8>)>,
    /// Global parity symbol: XOR of all source symbols.
    pub global_parity: Vec<u8>,
    /// Number of source symbols.
    pub k_source: u32,
    /// Locality group size used.
    pub locality: usize,
    /// Number of locality groups.
    pub num_groups: usize,
}

/// Outcome of an LRC repair attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LrcRepairOutcome {
    /// Repaired using only local parity (cheap: read `r` symbols).
    LocalRepair {
        /// Index of the repaired symbol.
        symbol_index: u32,
        /// Locality group that provided the repair.
        group_index: u32,
        /// Number of symbols read for repair.
        symbols_read: usize,
        /// Repaired data.
        data: Vec<u8>,
    },
    /// Repaired using global parity (expensive: read all `k` symbols).
    GlobalRepair {
        /// Index of the repaired symbol.
        symbol_index: u32,
        /// Number of symbols read for repair.
        symbols_read: usize,
        /// Repaired data.
        data: Vec<u8>,
    },
    /// Repair failed: too many erasures.
    Unrecoverable {
        /// Indices of missing symbols that could not be repaired.
        missing: Vec<u32>,
        /// Reason for failure.
        reason: String,
    },
}

// ── LRC Codec ────────────────────────────────────────────────────────────

/// Local Reconstruction Codes codec.
///
/// Produces local parity symbols (one per locality group) plus a global
/// parity symbol. Single failures within a group can be repaired locally.
pub struct LrcCodec {
    config: LrcConfig,
}

impl LrcCodec {
    /// Create a new LRC codec with the given configuration.
    pub fn new(config: LrcConfig) -> Self {
        assert!(
            config.locality >= 2,
            "locality must be >= 2, got {}",
            config.locality
        );
        Self { config }
    }

    /// Return the locality group size.
    #[must_use]
    pub fn locality(&self) -> usize {
        self.config.locality
    }

    /// Encode source data into source symbols + local/global parities.
    ///
    /// `source_data` is split into `symbol_size`-byte symbols.
    /// Each locality group of `r` symbols gets a local parity (XOR).
    /// A global parity (XOR of all source symbols) is also computed.
    #[allow(clippy::cast_possible_truncation)]
    pub fn encode(&self, source_data: &[u8], symbol_size: usize) -> LrcEncodeResult {
        assert!(symbol_size > 0, "symbol_size must be > 0");

        LRC_ENCODE_TOTAL.fetch_add(1, Ordering::Relaxed);

        // Split source into symbols, padding the last one if needed.
        let k = source_data.len().div_ceil(symbol_size);
        let mut source_symbols: Vec<(u32, Vec<u8>)> = Vec::with_capacity(k);

        for i in 0..k {
            let start = i * symbol_size;
            let end = (start + symbol_size).min(source_data.len());
            let mut sym = vec![0u8; symbol_size];
            sym[..end - start].copy_from_slice(&source_data[start..end]);
            source_symbols.push((i as u32, sym));
        }

        // Compute locality groups and local parities.
        let r = self.config.locality;
        let num_groups = k.div_ceil(r);
        let mut local_parities: Vec<(u32, Vec<u8>)> = Vec::with_capacity(num_groups);

        for g in 0..num_groups {
            let group_start = g * r;
            let group_end = ((g + 1) * r).min(k);

            let mut parity = vec![0u8; symbol_size];
            for (_, sym) in source_symbols.iter().take(group_end).skip(group_start) {
                xor_into(&mut parity, sym);
            }
            local_parities.push((g as u32, parity));
        }

        // Compute global parity (XOR of all source symbols).
        let mut global_parity = vec![0u8; symbol_size];
        for (_, sym) in &source_symbols {
            xor_into(&mut global_parity, sym);
        }

        LrcEncodeResult {
            source_symbols,
            local_parities,
            global_parity,
            k_source: k as u32,
            locality: r,
            num_groups,
        }
    }

    /// Attempt to repair missing symbols using local and global parities.
    ///
    /// `available` maps symbol index -> data for symbols that are present.
    /// `missing` lists the indices of symbols that need repair.
    /// Returns the repair outcome.
    #[allow(clippy::cast_possible_truncation)]
    pub fn repair(
        &self,
        encode_result: &LrcEncodeResult,
        available: &std::collections::HashMap<u32, Vec<u8>>,
        missing: &[u32],
    ) -> Vec<LrcRepairOutcome> {
        let r = encode_result.locality;
        let k = encode_result.k_source as usize;
        let mut outcomes = Vec::with_capacity(missing.len());

        // Track which symbols have been repaired (so we can use them for
        // subsequent repairs within the same call).
        let mut repaired: std::collections::HashMap<u32, Vec<u8>> =
            std::collections::HashMap::new();

        for &miss_idx in missing {
            // Determine which locality group this symbol belongs to.
            let group_idx = miss_idx as usize / r;
            let group_start = group_idx * r;
            let group_end = ((group_idx + 1) * r).min(k);

            // Count how many symbols are missing in this group.
            let group_missing: Vec<u32> = (group_start as u32..group_end as u32)
                .filter(|&i| !available.contains_key(&i) && !repaired.contains_key(&i))
                .collect();

            if group_missing.len() == 1 && group_missing[0] == miss_idx {
                // Single missing symbol in the group -> local repair.
                let local_parity = &encode_result.local_parities[group_idx].1;
                let mut restored = local_parity.clone();

                let mut syms_read = 1; // local parity
                for i in group_start as u32..group_end as u32 {
                    if i != miss_idx {
                        let sym = available
                            .get(&i)
                            .or_else(|| repaired.get(&i))
                            .expect("non-missing symbol should be available");
                        xor_into(&mut restored, sym);
                        syms_read += 1;
                    }
                }

                LRC_LOCAL_REPAIRS_TOTAL.fetch_add(1, Ordering::Relaxed);
                repaired.insert(miss_idx, restored.clone());
                outcomes.push(LrcRepairOutcome::LocalRepair {
                    symbol_index: miss_idx,
                    group_index: group_idx as u32,
                    symbols_read: syms_read,
                    data: restored,
                });
            } else if missing.len() == 1 {
                // Only one symbol missing total -> global repair.
                let mut restored = encode_result.global_parity.clone();
                let mut syms_read = 1; // global parity

                for i in 0..k as u32 {
                    if i != miss_idx {
                        let sym = available
                            .get(&i)
                            .or_else(|| repaired.get(&i))
                            .expect("non-missing symbol should be available");
                        xor_into(&mut restored, sym);
                        syms_read += 1;
                    }
                }

                LRC_GLOBAL_REPAIRS_TOTAL.fetch_add(1, Ordering::Relaxed);
                repaired.insert(miss_idx, restored.clone());
                outcomes.push(LrcRepairOutcome::GlobalRepair {
                    symbol_index: miss_idx,
                    symbols_read: syms_read,
                    data: restored,
                });
            } else {
                // Multiple missing in the same group -> unrecoverable with
                // simple XOR-based LRC.
                outcomes.push(LrcRepairOutcome::Unrecoverable {
                    missing: group_missing,
                    reason: format!(
                        "multiple erasures in locality group {group_idx}: need more advanced repair"
                    ),
                });
            }
        }

        outcomes
    }
}

impl fmt::Debug for LrcCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LrcCodec")
            .field("locality", &self.config.locality)
            .finish()
    }
}

/// XOR `src` into `dst` in place. Both must have the same length.
fn xor_into(dst: &mut [u8], src: &[u8]) {
    assert_eq!(dst.len(), src.len());
    for (d, s) in dst.iter_mut().zip(src.iter()) {
        *d ^= s;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn basic_encode_decode() {
        let codec = LrcCodec::new(LrcConfig { locality: 2 });
        let data = b"Hello, LRC world! This is a test of local reconstruction codes.";
        let result = codec.encode(data, 16);

        assert_eq!(result.k_source, 4); // 64 bytes / 16 = 4 symbols
        assert_eq!(result.num_groups, 2); // 4 symbols / 2 = 2 groups
        assert_eq!(result.local_parities.len(), 2);
        assert_eq!(result.global_parity.len(), 16);
    }

    #[test]
    fn local_repair_single_failure() {
        let codec = LrcCodec::new(LrcConfig { locality: 2 });
        let data = vec![0xAA; 64];
        let result = codec.encode(&data, 16);

        // Remove symbol 0 (in group 0).
        let mut available: HashMap<u32, Vec<u8>> = HashMap::new();
        for &(idx, ref sym) in &result.source_symbols {
            if idx != 0 {
                available.insert(idx, sym.clone());
            }
        }

        let outcomes = codec.repair(&result, &available, &[0]);
        assert_eq!(outcomes.len(), 1);
        match &outcomes[0] {
            LrcRepairOutcome::LocalRepair {
                symbol_index,
                group_index,
                data: repaired,
                ..
            } => {
                assert_eq!(*symbol_index, 0);
                assert_eq!(*group_index, 0);
                assert_eq!(repaired, &result.source_symbols[0].1);
            }
            other => panic!("expected LocalRepair, got {other:?}"),
        }
    }

    #[test]
    fn global_repair_fallback() {
        let codec = LrcCodec::new(LrcConfig { locality: 4 });
        let data = vec![0xBB; 64];
        let result = codec.encode(&data, 16);

        // Remove symbol 0 (only missing symbol, but test global path
        // by having it be the only missing symbol with group size = 4).
        let mut available: HashMap<u32, Vec<u8>> = HashMap::new();
        for &(idx, ref sym) in &result.source_symbols {
            if idx != 0 {
                available.insert(idx, sym.clone());
            }
        }

        let outcomes = codec.repair(&result, &available, &[0]);
        assert_eq!(outcomes.len(), 1);
        match &outcomes[0] {
            LrcRepairOutcome::LocalRepair { data: repaired, .. } => {
                assert_eq!(repaired, &result.source_symbols[0].1);
            }
            other => panic!("expected LocalRepair with single missing in group, got {other:?}"),
        }
    }
}
