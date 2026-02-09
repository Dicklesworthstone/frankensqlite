//! Seed taxonomy for deterministic concurrency testing (`bd-1xds`, spec §17.3).
//!
//! Derives distinct purpose-specific seeds from a root test seed using
//! `H(test_seed || purpose_tag || scope_id)` where `H` is xxh3_64.
//!
//! # Seed Categories
//!
//! - **schedule_seed**: Controls task scheduling order in the lab runtime.
//! - **entropy_seed**: Controls random decisions inside business logic (e.g., retry delays).
//! - **fault_seed**: Controls fault injection timing and selection.
//! - **fuzz_seed**: Controls fuzz input generation.

use xxhash_rust::xxh3::xxh3_64;

/// Purpose tags for seed derivation (INV-DET-5).
const TAG_SCHEDULE: &[u8] = b"schedule";
const TAG_ENTROPY: &[u8] = b"entropy";
const TAG_FAULT: &[u8] = b"fault";
const TAG_FUZZ: &[u8] = b"fuzz";

/// A set of deterministically derived seeds from a single root test seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeedTaxonomy {
    /// The root test seed.
    pub root: u64,
    /// Seed for deterministic task scheduling.
    pub schedule: u64,
    /// Seed for capability entropy sources.
    pub entropy: u64,
    /// Seed for fault injection timing.
    pub fault: u64,
    /// Seed for fuzz input generation.
    pub fuzz: u64,
}

impl SeedTaxonomy {
    /// Derive a full seed taxonomy from a root test seed and scope identifier.
    ///
    /// The derivation follows `H(test_seed || purpose_tag || scope_id)` (INV-DET-5).
    #[must_use]
    pub fn derive(root_seed: u64, scope_id: &str) -> Self {
        Self {
            root: root_seed,
            schedule: derive_seed(root_seed, TAG_SCHEDULE, scope_id),
            entropy: derive_seed(root_seed, TAG_ENTROPY, scope_id),
            fault: derive_seed(root_seed, TAG_FAULT, scope_id),
            fuzz: derive_seed(root_seed, TAG_FUZZ, scope_id),
        }
    }
}

/// Derive a purpose-specific seed: `H(test_seed_bytes || purpose_tag || scope_id)`.
fn derive_seed(test_seed: u64, purpose_tag: &[u8], scope_id: &str) -> u64 {
    let mut buf = Vec::with_capacity(8 + purpose_tag.len() + scope_id.len());
    buf.extend_from_slice(&test_seed.to_le_bytes());
    buf.extend_from_slice(purpose_tag);
    buf.extend_from_slice(scope_id.as_bytes());
    xxh3_64(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seed_taxonomy_derivation_deterministic() {
        let t1 = SeedTaxonomy::derive(0xDEAD_BEEF, "test_mvcc_two_writers");
        let t2 = SeedTaxonomy::derive(0xDEAD_BEEF, "test_mvcc_two_writers");
        assert_eq!(t1, t2, "same inputs must produce identical taxonomy");
    }

    #[test]
    fn test_seed_taxonomy_all_distinct() {
        let t = SeedTaxonomy::derive(0xCAFE_BABE, "scope1");
        // All derived seeds must be distinct from each other and from the root.
        let seeds = [t.root, t.schedule, t.entropy, t.fault, t.fuzz];
        for i in 0..seeds.len() {
            for j in (i + 1)..seeds.len() {
                assert_ne!(
                    seeds[i], seeds[j],
                    "seeds[{i}]={} must differ from seeds[{j}]={}",
                    seeds[i], seeds[j]
                );
            }
        }
    }

    #[test]
    fn test_seed_taxonomy_different_scopes_differ() {
        let a = SeedTaxonomy::derive(42, "scope_a");
        let b = SeedTaxonomy::derive(42, "scope_b");
        assert_ne!(a.schedule, b.schedule);
        assert_ne!(a.fault, b.fault);
    }

    #[test]
    fn test_seed_taxonomy_different_roots_differ() {
        let a = SeedTaxonomy::derive(1, "scope");
        let b = SeedTaxonomy::derive(2, "scope");
        assert_ne!(a.schedule, b.schedule);
        assert_ne!(a.entropy, b.entropy);
    }
}
