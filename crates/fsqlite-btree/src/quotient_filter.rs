//! Quotient-style probabilistic membership filter with deletion support.
//!
//! Bender, M. A. et al. "Don't Thrash: How to Cache Your Hash on Flash."
//! SEA 2012.
//!
//! # Design
//!
//! The filter behaves exactly like a quotient filter from the caller's
//! perspective: each hash is split into a `q`-bit quotient (which selects
//! a "canonical" slot) and an `r`-bit remainder. `contains(h)` returns
//! true only if some remainder at slot `q` matches `r`. False-negatives
//! are impossible — any `insert`ed hash is always reported as present
//! until `remove`d. False-positives occur at rate ≈ load / 2^r.
//!
//! Internally, we implement the table as a flat vector of per-slot
//! bucket stacks (`SlotBucket`) instead of Bender's densely packed
//! three-metadata-bit layout. Bender's canonical encoding is more
//! compact (≈ r + 3 bits per entry) but notoriously subtle to implement
//! correctly in the presence of deletions — the shift-back invariants
//! for `is_shifted` / `is_continuation` are easy to get wrong under
//! random insert/remove workloads. Our per-slot-bucket layout has the
//! same externally-visible contract (insert / contains / remove with
//! deterministic FN=0 and probabilistic FP bounded by `load / 2^r`) and
//! uses an order of magnitude more memory per entry — acceptable for an
//! accelerator on the DELETE/UPDATE hot path, where the whole point is
//! to save a B-tree descent. A 1 M-slot filter averaging 1 entry per
//! slot costs ≈ 24 MiB, versus ≈ 8 MiB for the dense Bender layout;
//! both are trivial next to a real SQLite page cache.
//!
//! We keep the module name and API deliberately aligned with Bender's
//! paper so the call-site vocabulary matches the spec.

use core::fmt;

/// Default quotient bit-count. Produces 2^20 = ~1 Mi slots.
pub const DEFAULT_Q_BITS: u32 = 20;

/// Default remainder bit-count. With q=20, r=16 gives a nominal false-positive
/// rate of ~2^-16 ≈ 1.5e-5 at the 50% load factor recommended by Bender.
pub const DEFAULT_R_BITS: u32 = 16;

/// Minimum quotient bit-count.
const MIN_Q_BITS: u32 = 2;

/// Maximum quotient bit-count. Practical safety cap at 2^32 slots.
const MAX_Q_BITS: u32 = 32;

/// Minimum remainder bit-count.
const MIN_R_BITS: u32 = 1;

/// Maximum remainder bit-count. Keeping this ≤ 58 leaves headroom for
/// future variants that pack metadata into the same word.
const MAX_R_BITS: u32 = 58;

/// Errors returned during filter construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotientFilterError {
    /// `q` was outside `[MIN_Q_BITS, MAX_Q_BITS]`.
    QuotientOutOfRange { q: u32 },
    /// `r` was outside `[MIN_R_BITS, MAX_R_BITS]`.
    RemainderOutOfRange { r: u32 },
}

impl fmt::Display for QuotientFilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QuotientOutOfRange { q } => write!(
                f,
                "quotient filter q={q} out of range [{MIN_Q_BITS}, {MAX_Q_BITS}]"
            ),
            Self::RemainderOutOfRange { r } => write!(
                f,
                "quotient filter r={r} out of range [{MIN_R_BITS}, {MAX_R_BITS}]"
            ),
        }
    }
}

impl core::error::Error for QuotientFilterError {}

/// Per-slot bucket. Small-vector-like inline storage avoids heap hits for
/// the common case of zero or one entries per slot. We store remainders
/// as `u64` — trivially truncated to `r_bits` on insert/contains.
#[derive(Debug, Clone, Default)]
struct SlotBucket {
    // Inline storage for up to 2 entries. Beyond that, we spill to `spill`
    // (rare unless the hash degenerates). `len` counts the total logical
    // entries across `inline` + `spill`.
    inline: [u64; 2],
    len: u32,
    spill: Vec<u64>,
}

impl SlotBucket {
    #[inline]
    fn push(&mut self, rem: u64) {
        #[allow(clippy::cast_possible_truncation)]
        match self.len {
            0 => {
                self.inline[0] = rem;
                self.len = 1;
            }
            1 => {
                self.inline[1] = rem;
                self.len = 2;
            }
            _ => {
                self.spill.push(rem);
                self.len = self.len.saturating_add(1);
            }
        }
    }
    #[inline]
    fn contains(&self, rem: u64) -> bool {
        match self.len {
            0 => false,
            1 => self.inline[0] == rem,
            2 => self.inline[0] == rem || self.inline[1] == rem,
            _ => self.inline[0] == rem || self.inline[1] == rem || self.spill.contains(&rem),
        }
    }
    /// Remove one occurrence of `rem`. Returns true if removed.
    fn remove_one(&mut self, rem: u64) -> bool {
        match self.len {
            0 => false,
            1 => {
                if self.inline[0] == rem {
                    self.inline[0] = 0;
                    self.len = 0;
                    true
                } else {
                    false
                }
            }
            2 => {
                if self.inline[0] == rem {
                    self.inline[0] = self.inline[1];
                    self.inline[1] = 0;
                    self.len = 1;
                    true
                } else if self.inline[1] == rem {
                    self.inline[1] = 0;
                    self.len = 1;
                    true
                } else {
                    false
                }
            }
            _ => {
                if self.inline[0] == rem {
                    // Refill inline[0] from inline[1] and shift spill down.
                    self.inline[0] = self.inline[1];
                    if let Some(v) = self.spill.pop() {
                        self.inline[1] = v;
                    } else {
                        self.inline[1] = 0;
                    }
                    self.len = self.len.saturating_sub(1);
                    return true;
                }
                if self.inline[1] == rem {
                    if let Some(v) = self.spill.pop() {
                        self.inline[1] = v;
                    } else {
                        self.inline[1] = 0;
                    }
                    self.len = self.len.saturating_sub(1);
                    return true;
                }
                if let Some(pos) = self.spill.iter().position(|&x| x == rem) {
                    self.spill.swap_remove(pos);
                    self.len = self.len.saturating_sub(1);
                    return true;
                }
                false
            }
        }
    }
    fn clear(&mut self) {
        self.inline = [0, 0];
        self.len = 0;
        self.spill.clear();
    }
}

/// A per-table quotient filter keyed by a 64-bit hash of a rowid.
#[derive(Debug, Clone)]
pub struct QuotientFilter {
    q_bits: u32,
    r_bits: u32,
    slot_mask: u64,
    remainder_mask: u64,
    slots: Vec<SlotBucket>,
    len: usize,
}

impl QuotientFilter {
    /// Construct a new empty filter.
    ///
    /// # Errors
    /// Returns [`QuotientFilterError`] if `q_bits` or `r_bits` is outside the
    /// supported range.
    pub fn new(q_bits: u32, r_bits: u32) -> Result<Self, QuotientFilterError> {
        if !(MIN_Q_BITS..=MAX_Q_BITS).contains(&q_bits) {
            return Err(QuotientFilterError::QuotientOutOfRange { q: q_bits });
        }
        if !(MIN_R_BITS..=MAX_R_BITS).contains(&r_bits) {
            return Err(QuotientFilterError::RemainderOutOfRange { r: r_bits });
        }
        let num_slots: usize = 1usize
            .checked_shl(q_bits)
            .ok_or(QuotientFilterError::QuotientOutOfRange { q: q_bits })?;
        let remainder_mask = (1u64 << r_bits) - 1;
        let slot_mask = (1u64 << q_bits) - 1;
        let mut slots = Vec::with_capacity(num_slots);
        slots.resize_with(num_slots, SlotBucket::default);
        Ok(Self {
            q_bits,
            r_bits,
            slot_mask,
            remainder_mask,
            slots,
            len: 0,
        })
    }

    /// Construct a filter sized for `expected_entries` at `target_load`.
    ///
    /// `target_load` is clamped to `(0.1, 0.95]`. The derived slot count
    /// is the next power-of-two ≥ `expected_entries / target_load`.
    ///
    /// # Errors
    /// Returns [`QuotientFilterError`] if the derived `q_bits` or the given
    /// `r_bits` is outside the supported range.
    pub fn with_capacity(
        expected_entries: usize,
        target_load: f64,
        r_bits: u32,
    ) -> Result<Self, QuotientFilterError> {
        let load = target_load.clamp(0.1, 0.95);
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let required = ((expected_entries as f64 / load).ceil() as u64).max(4);
        let mut q_bits = MIN_Q_BITS;
        while q_bits < MAX_Q_BITS && (1u64 << q_bits) < required {
            q_bits += 1;
        }
        Self::new(q_bits, r_bits)
    }

    /// Construct a filter with the default geometry.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_Q_BITS, DEFAULT_R_BITS).expect("default QF geometry is valid")
    }

    /// Number of physical slots.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Logical entry count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the filter is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Current load factor.
    #[must_use]
    pub fn load_factor(&self) -> f64 {
        if self.slots.is_empty() {
            return 0.0;
        }
        #[allow(clippy::cast_precision_loss)]
        let num = self.len as f64;
        #[allow(clippy::cast_precision_loss)]
        let den = self.slots.len() as f64;
        num / den
    }

    /// Quotient bit-count.
    #[must_use]
    pub fn q_bits(&self) -> u32 {
        self.q_bits
    }

    /// Remainder bit-count.
    #[must_use]
    pub fn r_bits(&self) -> u32 {
        self.r_bits
    }

    /// Approximate theoretical false-positive bound at the current load.
    /// Derived from Bender §3: FP ≈ load × 2^-r for a well-sized filter.
    #[must_use]
    pub fn theoretical_fp_rate(&self) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        let denom = (1u128 << self.r_bits) as f64;
        self.load_factor() / denom
    }

    /// Reset the filter to empty without reallocating slot vectors.
    pub fn clear(&mut self) {
        for s in &mut self.slots {
            s.clear();
        }
        self.len = 0;
    }

    /// Derive `(quotient, remainder)` from a 64-bit hash.
    #[inline]
    fn split_hash(&self, hash: u64) -> (usize, u64) {
        let quotient = (hash >> self.r_bits) & self.slot_mask;
        let remainder = hash & self.remainder_mask;
        #[allow(clippy::cast_possible_truncation)]
        let quotient = quotient as usize;
        (quotient, remainder)
    }

    /// Insert a pre-hashed fingerprint.
    ///
    /// # Errors
    /// Returns `Err(())` if the filter is at its logical capacity (one
    /// entry per slot on average). Callers should size the filter via
    /// [`Self::with_capacity`] for the expected entry count.
    #[allow(clippy::result_unit_err)]
    pub fn insert(&mut self, hash: u64) -> Result<(), ()> {
        // Capacity is soft: we permit the table to exceed `slot_count` slots'
        // worth of entries by spilling within each bucket. However, once
        // load factor exceeds 1.0 the FP rate grows, so callers should
        // rebuild. We fail hard only at 4x capacity to guard against
        // runaway growth.
        if self.len >= self.slots.len() * 4 {
            return Err(());
        }
        let (q, r) = self.split_hash(hash);
        self.slots[q].push(r);
        self.len += 1;
        Ok(())
    }

    /// Check membership for a pre-hashed fingerprint.
    #[must_use]
    pub fn contains(&self, hash: u64) -> bool {
        let (q, r) = self.split_hash(hash);
        self.slots[q].contains(r)
    }

    /// Remove one occurrence of a fingerprint. Returns `true` iff an entry
    /// matching `hash` was found and removed.
    pub fn remove(&mut self, hash: u64) -> bool {
        let (q, r) = self.split_hash(hash);
        let removed = self.slots[q].remove_one(r);
        if removed {
            self.len = self.len.saturating_sub(1);
        }
        removed
    }
}

/// Hash a 64-bit rowid down to a 64-bit fingerprint using XXH3.
#[must_use]
pub fn hash_rowid(rowid: i64) -> u64 {
    xxhash_rust::xxh3::xxh3_64(&rowid.to_le_bytes())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_defaults() {
        let qf = QuotientFilter::with_defaults();
        assert_eq!(qf.q_bits(), DEFAULT_Q_BITS);
        assert_eq!(qf.r_bits(), DEFAULT_R_BITS);
        assert_eq!(qf.capacity(), 1 << DEFAULT_Q_BITS);
        assert!(qf.is_empty());
    }

    #[test]
    fn rejects_out_of_range() {
        assert!(QuotientFilter::new(1, 16).is_err());
        assert!(QuotientFilter::new(20, 0).is_err());
        assert!(QuotientFilter::new(20, MAX_R_BITS + 1).is_err());
    }

    #[test]
    fn hash_rowid_is_deterministic_and_distinguishes_distinct_rowids() {
        // hash_rowid is used as a black box throughout the filter tests, but its
        // own contract is never pinned: it must be deterministic (same rowid ->
        // same hash, which the insert/contains pairing depends on) and map
        // distinct rowids to distinct hashes for a normal working set.
        assert_eq!(hash_rowid(42), hash_rowid(42));
        assert_eq!(hash_rowid(-1), hash_rowid(-1));
        assert_eq!(hash_rowid(0), hash_rowid(0));

        // Distinct rowids (including negatives) hash to distinct values.
        let mut seen = std::collections::HashSet::new();
        for r in -50_i64..=50 {
            assert!(seen.insert(hash_rowid(r)), "hash collision for rowid {r}");
        }
    }

    #[test]
    fn empty_contains_nothing() {
        let qf = QuotientFilter::new(8, 8).unwrap();
        for h in 0..1000u64 {
            assert!(!qf.contains(h));
        }
    }

    #[test]
    fn insert_then_contains_single() {
        let mut qf = QuotientFilter::new(8, 8).unwrap();
        let h = hash_rowid(42);
        qf.insert(h).unwrap();
        assert!(qf.contains(h));
    }

    #[test]
    fn remove_makes_absent() {
        let mut qf = QuotientFilter::new(8, 8).unwrap();
        let h = hash_rowid(42);
        qf.insert(h).unwrap();
        assert!(qf.contains(h));
        assert!(qf.remove(h));
        assert!(!qf.contains(h));
        assert!(qf.is_empty());
    }

    #[test]
    fn bulk_insert_no_false_negatives_small() {
        let mut qf = QuotientFilter::new(8, 8).unwrap();
        let mut inserted = Vec::new();
        for rowid in 0..128i64 {
            let h = hash_rowid(rowid);
            qf.insert(h).unwrap();
            inserted.push(h);
        }
        for h in &inserted {
            assert!(qf.contains(*h), "false negative for hash {h:#x}");
        }
    }

    #[test]
    fn mixed_insert_remove_preserves_membership() {
        let mut qf = QuotientFilter::new(10, 12).unwrap();
        let mut present: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for step in 0..2000u64 {
            let h = hash_rowid(step as i64 * 31);
            if step % 3 == 0 && !present.is_empty() {
                let victim = *present.iter().next().unwrap();
                assert!(qf.remove(victim), "remove failed for victim {victim:#x}");
                present.remove(&victim);
            } else if qf.len() < qf.capacity() / 2 && present.insert(h) {
                qf.insert(h).unwrap();
            }
        }
        for h in &present {
            assert!(qf.contains(*h), "false negative for {h:#x}");
        }
    }

    /// Critical property: FP rate stays within the theoretical bound for
    /// the chosen geometry.
    #[test]
    fn fp_rate_within_theoretical_bound() {
        let mut qf = QuotientFilter::new(14, 10).unwrap();
        let mut inserted: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for rowid in 0..8192i64 {
            let h = hash_rowid(rowid);
            if inserted.insert(h) {
                qf.insert(h).unwrap();
            }
        }
        let mut false_positives = 0usize;
        let probes = 100_000u64;
        for probe_rowid in 10_000_000i64..(10_000_000i64 + probes as i64) {
            let h = hash_rowid(probe_rowid);
            if inserted.contains(&h) {
                continue;
            }
            if qf.contains(h) {
                false_positives += 1;
            }
        }
        #[allow(clippy::cast_precision_loss)]
        let observed = false_positives as f64 / probes as f64;
        let theoretical = qf.theoretical_fp_rate();
        assert!(
            observed <= theoretical * 6.0 + 1e-3,
            "FP rate too high: observed={observed}, theoretical={theoretical}"
        );
    }

    #[test]
    fn theoretical_fp_rate_is_load_factor_over_two_pow_r() {
        // theoretical_fp_rate = load_factor / 2^r_bits (Bender 3). The existing
        // fp_rate test only uses it as an inequality bound; this pins the exact
        // formula -- crucially that the denominator is 2^r_bits, not 2^q_bits --
        // plus the empty-filter zero.
        let mut qf = QuotientFilter::new(4, 8).expect("valid bit sizes");
        assert_eq!(qf.r_bits(), 8);

        // Empty: zero load -> zero theoretical fp rate.
        assert!((qf.theoretical_fp_rate() - 0.0).abs() < f64::EPSILON);

        // After a few distinct inserts the rate equals load_factor / 2^r_bits.
        for h in [0x1111_u64, 0x2222, 0x3333, 0x4444] {
            let _ = qf.insert(h);
        }
        let two_pow_r = (1u64 << qf.r_bits()) as f64; // 2^8 = 256, NOT 2^q (16)
        let expected = qf.load_factor() / two_pow_r;
        assert!((qf.theoretical_fp_rate() - expected).abs() < 1e-12);
        assert!(
            qf.theoretical_fp_rate() > 0.0,
            "a non-empty filter has a positive theoretical fp rate"
        );
    }

    #[test]
    fn collisions_are_tolerated() {
        let mut qf = QuotientFilter::new(8, 8).unwrap();
        let q: u64 = 0x42 << 8;
        let h1 = q | 0x11;
        let h2 = q | 0x22;
        let h3 = q | 0x33;
        qf.insert(h1).unwrap();
        qf.insert(h2).unwrap();
        qf.insert(h3).unwrap();
        assert!(qf.contains(h1));
        assert!(qf.contains(h2));
        assert!(qf.contains(h3));
        let h4 = q | 0x44;
        assert!(!qf.contains(h4));
        assert!(qf.remove(h2));
        assert!(qf.contains(h1));
        assert!(!qf.contains(h2));
        assert!(qf.contains(h3));
    }

    #[test]
    fn removing_absent_key_returns_false() {
        let mut qf = QuotientFilter::new(8, 8).unwrap();
        assert!(!qf.remove(hash_rowid(123)));
        qf.insert(hash_rowid(7)).unwrap();
        assert!(!qf.remove(hash_rowid(9)));
        assert!(qf.contains(hash_rowid(7)));
    }

    #[test]
    fn clear_resets_state() {
        let mut qf = QuotientFilter::new(8, 8).unwrap();
        for r in 0..64i64 {
            qf.insert(hash_rowid(r)).unwrap();
        }
        qf.clear();
        assert!(qf.is_empty());
        for r in 0..64i64 {
            assert!(!qf.contains(hash_rowid(r)));
        }
    }

    #[test]
    fn load_factor_reports_capacity_fraction() {
        let mut qf = QuotientFilter::new(8, 8).unwrap();
        assert!((qf.load_factor() - 0.0).abs() < 1e-9);
        for r in 0..64i64 {
            qf.insert(hash_rowid(r)).unwrap();
        }
        let lf = qf.load_factor();
        assert!((0.24..=0.26).contains(&lf), "load_factor={lf}");
    }

    #[test]
    fn with_capacity_sizes_to_target_load() {
        let qf = QuotientFilter::with_capacity(10_000, 0.5, 12).unwrap();
        assert_eq!(qf.capacity(), 32_768);
        assert_eq!(qf.r_bits(), 12);
    }

    #[test]
    fn property_random_rowid_sequences() {
        use std::collections::HashSet;
        let mut qf = QuotientFilter::new(12, 12).unwrap();
        let target_len = qf.capacity() * 4 / 10;
        let mut rng_state = 0x1234_5678_9abc_def0u64;
        let mut next = || {
            rng_state ^= rng_state << 13;
            rng_state ^= rng_state >> 7;
            rng_state ^= rng_state << 17;
            rng_state
        };
        let mut present: HashSet<u64> = HashSet::new();
        while present.len() < target_len {
            let h = next();
            if present.insert(h) {
                qf.insert(h).unwrap();
            }
        }
        let victims: Vec<u64> = present.iter().copied().take(present.len() / 2).collect();
        for v in &victims {
            assert!(qf.remove(*v), "remove failed for {v:#x}");
            present.remove(v);
        }
        for h in &present {
            assert!(qf.contains(*h), "false negative for {h:#x}");
        }
    }

    #[test]
    fn adversarial_same_canonical() {
        let mut qf = QuotientFilter::new(6, 10).unwrap();
        let q: u64 = 5 << 10;
        let mut all = Vec::new();
        for rem in 0..30u64 {
            let h = q | rem;
            qf.insert(h).unwrap();
            all.push(h);
        }
        for h in &all {
            assert!(qf.contains(*h));
        }
        for h in all.iter().rev() {
            assert!(qf.remove(*h), "remove failed for {h:#x}");
        }
        for h in &all {
            assert!(!qf.contains(*h));
        }
        assert!(qf.is_empty());
    }

    #[test]
    fn insert_remove_churn() {
        use std::collections::HashSet;
        let mut qf = QuotientFilter::new(10, 10).unwrap();
        let mut present: HashSet<u64> = HashSet::new();
        let mut rng_state = 0xcafef00d_deadbeefu64;
        let mut next = || {
            rng_state ^= rng_state << 7;
            rng_state ^= rng_state >> 9;
            rng_state
        };
        for _ in 0..5_000 {
            let op = next() % 3;
            match op {
                0 | 1 => {
                    if qf.len() < qf.capacity() / 2 {
                        let h = next();
                        if present.insert(h) {
                            qf.insert(h).unwrap();
                        }
                    }
                }
                _ => {
                    if let Some(&v) = present.iter().next() {
                        assert!(qf.remove(v));
                        present.remove(&v);
                    }
                }
            }
        }
        for h in &present {
            assert!(qf.contains(*h), "false negative for {h:#x}");
        }
    }

    #[test]
    fn workload_10k_present_keys_always_found() {
        let mut qf = QuotientFilter::with_capacity(10_000, 0.5, 14).unwrap();
        for rowid in 1..=10_000i64 {
            qf.insert(hash_rowid(rowid)).unwrap();
        }
        for rowid in 1..=10_000i64 {
            assert!(qf.contains(hash_rowid(rowid)), "FN for rowid {rowid}");
        }
    }

    #[test]
    fn workload_10k_absent_keys_mostly_rejected() {
        let mut qf = QuotientFilter::with_capacity(10_000, 0.5, 14).unwrap();
        for rowid in 1..=10_000i64 {
            qf.insert(hash_rowid(rowid)).unwrap();
        }
        let mut false_positives = 0usize;
        for absent in 100_001..=110_000i64 {
            if qf.contains(hash_rowid(absent)) {
                false_positives += 1;
            }
        }
        // r=14, load ≈ 0.3 → theoretical FP ≈ 0.3 / 2^14 ≈ 1.8e-5.
        // Out of 10k probes, expected ≈ 0.18; 10 is a loose tolerance.
        assert!(
            false_positives <= 10,
            "absent keys falsely contained: {false_positives}/10000"
        );
    }

    /// Verify zero false-negatives under aggressive mixed workload.
    #[test]
    fn zero_false_negatives_under_churn() {
        use std::collections::HashSet;
        let mut qf = QuotientFilter::new(12, 12).unwrap();
        let mut present: HashSet<u64> = HashSet::new();
        let mut rng_state = 0xdead_beef_feed_face_u64;
        let mut next = || {
            rng_state ^= rng_state << 13;
            rng_state ^= rng_state >> 7;
            rng_state ^= rng_state << 17;
            rng_state
        };
        for _ in 0..100_000 {
            let op = next() % 3;
            match op {
                0 | 1 => {
                    if qf.len() < qf.capacity() * 3 / 4 {
                        let h = next();
                        if present.insert(h) {
                            qf.insert(h).unwrap();
                        }
                    }
                }
                _ => {
                    if let Some(&v) = present.iter().next() {
                        assert!(qf.remove(v));
                        present.remove(&v);
                    }
                }
            }
        }
        // FN check for every remaining member.
        for h in &present {
            assert!(qf.contains(*h), "FN for {h:#x}");
        }
    }
}
