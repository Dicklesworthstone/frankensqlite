use std::fmt;

use fsqlite_types::{RangeKey, WitnessKey};
use xxhash_rust::xxh3::xxh3_64;

// ---------------------------------------------------------------------------
// §5.6.4.4 RangeKey: Hierarchical Buckets Over WitnessKey Hash Space
// ---------------------------------------------------------------------------

/// Canonical-encode a `WitnessKey` to deterministic bytes for hashing (§5.6.4.4 step 1).
///
/// The encoding is domain-separated per variant so different key types cannot
/// collide even when their payloads happen to be identical.
#[must_use]
pub fn witness_key_canonical_bytes(key: &WitnessKey) -> Vec<u8> {
    match key {
        WitnessKey::Page(pgno) => {
            let mut buf = Vec::with_capacity(5);
            buf.push(0x01); // discriminant
            buf.extend_from_slice(&pgno.get().to_le_bytes());
            buf
        }
        WitnessKey::Cell { btree_root, tag } => {
            let mut buf = Vec::with_capacity(13);
            buf.push(0x02);
            buf.extend_from_slice(&btree_root.get().to_le_bytes());
            buf.extend_from_slice(&tag.to_le_bytes());
            buf
        }
        WitnessKey::ByteRange { page, start, len } => {
            let mut buf = Vec::with_capacity(13);
            buf.push(0x03);
            buf.extend_from_slice(&page.get().to_le_bytes());
            buf.extend_from_slice(&start.to_le_bytes());
            buf.extend_from_slice(&len.to_le_bytes());
            buf
        }
        WitnessKey::KeyRange { btree_root, lo, hi } => {
            let mut buf = Vec::with_capacity(13 + lo.len() + hi.len());
            buf.push(0x04);
            buf.extend_from_slice(&btree_root.get().to_le_bytes());
            buf.extend_from_slice(&u32::try_from(lo.len()).unwrap_or(u32::MAX).to_le_bytes());
            buf.extend_from_slice(lo);
            buf.extend_from_slice(&u32::try_from(hi.len()).unwrap_or(u32::MAX).to_le_bytes());
            buf.extend_from_slice(hi);
            buf
        }
        WitnessKey::Custom { namespace, bytes } => {
            let mut buf = Vec::with_capacity(5 + bytes.len());
            buf.push(0x05);
            buf.extend_from_slice(&namespace.to_le_bytes());
            buf.extend_from_slice(bytes);
            buf
        }
    }
}

/// Compute `KeyHash := xxh3_64(WitnessKeyBytes)` (§5.6.4.4 step 2).
#[must_use]
pub fn witness_key_hash(key: &WitnessKey) -> u64 {
    let bytes = witness_key_canonical_bytes(key);
    xxh3_64(&bytes)
}

/// Extract the top `prefix_bits` of a 64-bit hash as a u32 prefix (§5.6.4.4 step 3).
///
/// Returns the top `prefix_bits` bits of `hash` as a right-aligned u32.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn extract_prefix(hash: u64, prefix_bits: u8) -> u32 {
    if prefix_bits == 0 || prefix_bits > 32 {
        return 0;
    }
    (hash >> (64 - u32::from(prefix_bits))) as u32
}

/// Derive `RangeKey`s at all configured hierarchy levels for a witness key (§5.6.4.4).
///
/// Returns one `RangeKey` per level (L0, L1, L2).
#[must_use]
pub fn derive_range_keys(key: &WitnessKey, config: &WitnessHierarchyConfigV1) -> Vec<RangeKey> {
    let hash = witness_key_hash(key);
    config
        .levels()
        .iter()
        .enumerate()
        .map(|(level_idx, &prefix_bits)| {
            let hash_prefix = extract_prefix(hash, prefix_bits);
            RangeKey {
                level: u8::try_from(level_idx).unwrap_or(u8::MAX),
                hash_prefix,
            }
        })
        .collect()
}

/// Compute a bucket index for a `RangeKey` within a power-of-2 hash table.
///
/// Uses Fibonacci hashing for good distribution under linear probing.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn range_key_bucket_index(range_key: RangeKey, mask: u32) -> u32 {
    if mask == 0 {
        return 0;
    }
    // Combine level + prefix into a single key for hashing.
    let combined = u64::from(range_key.level) << 32 | u64::from(range_key.hash_prefix);
    // Fibonacci hash: multiply by golden ratio constant, take top bits.
    let fib = combined.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let shift = mask.count_ones(); // log2(capacity)
    ((fib >> (64 - shift)) as u32) & mask
}

/// Default V1 witness hierarchy prefix sizes, in bits (Section 5.6.4.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WitnessHierarchyConfigV1 {
    /// Level 0 prefix bits (default: 12).
    pub p0: u8,
    /// Level 1 prefix bits (default: 20).
    pub p1: u8,
    /// Level 2 prefix bits (default: 28).
    pub p2: u8,
}

impl WitnessHierarchyConfigV1 {
    pub const DEFAULT_P0: u8 = 12;
    pub const DEFAULT_P1: u8 = 20;
    pub const DEFAULT_P2: u8 = 28;

    /// Validate hierarchy invariants required for consistent interpretation across replicas.
    pub fn validate(self) -> Result<(), WitnessSizingError> {
        let ok = self.p0 > 0 && self.p0 < self.p1 && self.p1 < self.p2 && self.p2 <= 64;
        if ok {
            Ok(())
        } else {
            Err(WitnessSizingError::InvalidHierarchy {
                p0: self.p0,
                p1: self.p1,
                p2: self.p2,
            })
        }
    }

    #[must_use]
    pub const fn levels(self) -> [u8; 3] {
        [self.p0, self.p1, self.p2]
    }
}

impl Default for WitnessHierarchyConfigV1 {
    fn default() -> Self {
        Self {
            p0: Self::DEFAULT_P0,
            p1: Self::DEFAULT_P1,
            p2: Self::DEFAULT_P2,
        }
    }
}

/// Inputs for sizing the hot witness index region in shared memory.
///
/// This is a planning/manifest type: it records the knobs that determine how
/// much shared memory is reserved, and therefore how many hot buckets can be
/// tracked without falling back to the overflow bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HotWitnessIndexSizingV1 {
    /// Maximum number of TxnSlots in the shared memory header (default in spec: 256).
    pub max_txn_slots: u32,
    /// Byte budget reserved for the *entire* hot witness index region, including header and overflow entry.
    pub budget_bytes: u64,
}

impl HotWitnessIndexSizingV1 {
    pub const DEFAULT_MAX_TXN_SLOTS: u32 = 256;

    /// Cache-line alignment for per-bucket entries (required by spec; 64B is the assumed line size).
    pub const ENTRY_ALIGNMENT_BYTES: u32 = 64;
    /// HotWitnessIndex header bytes (capacity + epoch), aligned up to cache line for stable layout.
    pub const HEADER_BYTES_ALIGNED: u64 = 64;

    #[must_use]
    pub const fn new(max_txn_slots: u32, budget_bytes: u64) -> Self {
        Self {
            max_txn_slots,
            budget_bytes,
        }
    }
}

/// Derived layout and capacity information for the hot witness index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HotWitnessIndexDerivationV1 {
    /// Number of 64-bit words needed to cover `max_txn_slots` bits.
    pub words: u32,
    /// Bytes in a single readers/writers bitset array (`words * 8`).
    pub bytes_per_bitset: u64,
    /// Bytes in the 4 bitsets (readers/writers for epochs A/B).
    pub bytes_bitsets_total: u64,
    /// Bytes for the fixed metadata prefix (before u64 bitset arrays), aligned to 8 bytes.
    pub bytes_meta_aligned: u64,
    /// Bytes per bucket entry aligned to cache line.
    pub bytes_per_entry: u64,
    /// Capacity of the hash table (power-of-two). Does not include the overflow entry.
    pub capacity: u32,
    /// Total bytes required for the hot index region with this capacity (header + entries + overflow).
    pub total_bytes: u64,
}

/// Manifest record tying the witness hierarchy and hot index sizing together.
///
/// Replicas MUST interpret witness evidence using the same hierarchy, and the
/// hot index parameters influence false positive rate and fallback behavior.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WitnessHotIndexManifestV1 {
    pub version: u32,
    pub hierarchy: WitnessHierarchyConfigV1,
    pub sizing: HotWitnessIndexSizingV1,
    pub derivation: HotWitnessIndexDerivationV1,
}

impl WitnessHotIndexManifestV1 {
    pub const VERSION: u32 = 1;

    /// Derive a concrete hot-index layout from the requested budget.
    pub fn derive(
        hierarchy: WitnessHierarchyConfigV1,
        sizing: HotWitnessIndexSizingV1,
    ) -> Result<Self, WitnessSizingError> {
        hierarchy.validate()?;

        if sizing.max_txn_slots == 0 {
            return Err(WitnessSizingError::InvalidMaxTxnSlots {
                max_txn_slots: sizing.max_txn_slots,
            });
        }

        let words = words_for_slots(sizing.max_txn_slots);
        let bytes_per_bitset = u64::from(words) * 8;
        let bytes_bitsets_total = bytes_per_bitset
            .checked_mul(4)
            .ok_or(WitnessSizingError::ArithmeticOverflow)?;

        // Metadata layout per spec (conceptual):
        // level (u8) + prefix (u32) + epoch_lock (u32) + epoch_a (u32) + epoch_b (u32).
        let bytes_meta = 1_u64 + 4_u64 + 4_u64 + 4_u64 + 4_u64;
        let bytes_meta_aligned = align_up(bytes_meta, 8)?;

        let bytes_unaligned = bytes_meta_aligned
            .checked_add(bytes_bitsets_total)
            .ok_or(WitnessSizingError::ArithmeticOverflow)?;
        let bytes_per_entry = align_up(
            bytes_unaligned,
            u64::from(HotWitnessIndexSizingV1::ENTRY_ALIGNMENT_BYTES),
        )?;

        let max_total_entries = max_entries_for_budget(
            sizing.budget_bytes,
            HotWitnessIndexSizingV1::HEADER_BYTES_ALIGNED,
            bytes_per_entry,
        )?;
        if max_total_entries < 2 {
            // Need at least 1 overflow entry + 1 table slot, otherwise the "capacity"
            // table becomes pointless and everything falls into overflow.
            let required = HotWitnessIndexSizingV1::HEADER_BYTES_ALIGNED
                .saturating_add(bytes_per_entry.saturating_mul(2));
            return Err(WitnessSizingError::BudgetTooSmall {
                budget_bytes: sizing.budget_bytes,
                required_bytes: required,
            });
        }

        let max_capacity = max_total_entries
            .saturating_sub(1)
            .try_into()
            .map_err(|_| WitnessSizingError::ArithmeticOverflow)?;
        let capacity = floor_power_of_two_u32(max_capacity).ok_or_else(|| {
            WitnessSizingError::BudgetTooSmall {
                budget_bytes: sizing.budget_bytes,
                required_bytes: HotWitnessIndexSizingV1::HEADER_BYTES_ALIGNED
                    .saturating_add(bytes_per_entry.saturating_mul(2)),
            }
        })?;

        let entries_plus_overflow = u64::from(capacity)
            .checked_add(1)
            .ok_or(WitnessSizingError::ArithmeticOverflow)?;
        let entries_bytes = bytes_per_entry
            .checked_mul(entries_plus_overflow)
            .ok_or(WitnessSizingError::ArithmeticOverflow)?;
        let total_bytes = HotWitnessIndexSizingV1::HEADER_BYTES_ALIGNED
            .checked_add(entries_bytes)
            .ok_or(WitnessSizingError::ArithmeticOverflow)?;

        Ok(Self {
            version: Self::VERSION,
            hierarchy,
            sizing,
            derivation: HotWitnessIndexDerivationV1 {
                words,
                bytes_per_bitset,
                bytes_bitsets_total,
                bytes_meta_aligned,
                bytes_per_entry,
                capacity,
                total_bytes,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WitnessSizingError {
    InvalidHierarchy {
        p0: u8,
        p1: u8,
        p2: u8,
    },
    InvalidMaxTxnSlots {
        max_txn_slots: u32,
    },
    InvalidAlignment {
        alignment: u64,
    },
    BudgetTooSmall {
        budget_bytes: u64,
        required_bytes: u64,
    },
    ArithmeticOverflow,
}

impl fmt::Display for WitnessSizingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidHierarchy { p0, p1, p2 } => write!(
                f,
                "invalid witness hierarchy (must satisfy 0 < p0 < p1 < p2 <= 64): p0={p0} p1={p1} p2={p2}"
            ),
            Self::InvalidMaxTxnSlots { max_txn_slots } => {
                write!(
                    f,
                    "invalid max_txn_slots (must be non-zero): {max_txn_slots}"
                )
            }
            Self::InvalidAlignment { alignment } => {
                write!(f, "invalid alignment (must be non-zero): {alignment}")
            }
            Self::BudgetTooSmall {
                budget_bytes,
                required_bytes,
            } => write!(
                f,
                "budget too small for requested hot witness index: budget={budget_bytes} required>={required_bytes}"
            ),
            Self::ArithmeticOverflow => f.write_str("arithmetic overflow while deriving sizing"),
        }
    }
}

impl std::error::Error for WitnessSizingError {}

fn words_for_slots(max_txn_slots: u32) -> u32 {
    let max = u64::from(max_txn_slots);
    let w = max.div_ceil(64);
    w.try_into().unwrap_or(u32::MAX)
}

fn align_up(value: u64, alignment: u64) -> Result<u64, WitnessSizingError> {
    if alignment == 0 {
        return Err(WitnessSizingError::InvalidAlignment { alignment });
    }
    let rem = value % alignment;
    if rem == 0 {
        Ok(value)
    } else {
        value
            .checked_add(alignment - rem)
            .ok_or(WitnessSizingError::ArithmeticOverflow)
    }
}

fn max_entries_for_budget(
    budget_bytes: u64,
    header_bytes_aligned: u64,
    entry_bytes: u64,
) -> Result<u64, WitnessSizingError> {
    if budget_bytes <= header_bytes_aligned {
        return Ok(0);
    }
    if entry_bytes == 0 {
        return Err(WitnessSizingError::ArithmeticOverflow);
    }
    Ok((budget_bytes - header_bytes_aligned) / entry_bytes)
}

fn floor_power_of_two_u32(n: u32) -> Option<u32> {
    if n == 0 {
        return None;
    }
    let shift = 31_u32.saturating_sub(n.leading_zeros());
    Some(1_u32 << shift)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_hierarchy_is_valid() {
        let ok = WitnessHierarchyConfigV1::default().validate();
        assert!(ok.is_ok(), "default hierarchy must validate: {ok:?}");
    }

    #[test]
    fn derive_hot_index_manifest_is_deterministic() {
        let hierarchy = WitnessHierarchyConfigV1::default();
        let sizing = HotWitnessIndexSizingV1::new(256, 1792);
        let m1 = WitnessHotIndexManifestV1::derive(hierarchy, sizing);
        assert!(m1.is_ok(), "derive failed: {m1:?}");
        let Ok(m1) = m1 else {
            return;
        };

        let m2 = WitnessHotIndexManifestV1::derive(hierarchy, sizing);
        assert!(m2.is_ok(), "derive failed: {m2:?}");
        let Ok(m2) = m2 else {
            return;
        };
        assert_eq!(m1, m2);
        assert_eq!(m1.derivation.words, 4);
        assert_eq!(m1.derivation.bytes_per_bitset, 32);
        assert_eq!(m1.derivation.bytes_bitsets_total, 128);
        assert_eq!(m1.derivation.bytes_meta_aligned, 24);
        assert_eq!(m1.derivation.bytes_per_entry, 192);
        assert_eq!(m1.derivation.capacity, 8);
        assert_eq!(m1.derivation.total_bytes, 64 + (192 * 9));
    }
}
