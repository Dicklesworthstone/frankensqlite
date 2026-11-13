//! §3.5.10 Symbol Size Policy (Object-Type-Aware, Measured).
//!
//! Symbol size is a major performance lever. We choose symbol size per
//! ECS object type, with sane defaults and benchmark-driven tuning.
//!
//! All sizing is versioned in `RootManifest` so replicas decode correctly.

use tracing::debug;

// ---------------------------------------------------------------------------
// Object Types
// ---------------------------------------------------------------------------

/// ECS object type for symbol size policy selection (§3.5.10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EcsObjectType {
    /// Commit capsule: contains committed page data.
    CommitCapsule,
    /// Index segment: metadata-heavy witness/index data.
    IndexSegment,
    /// Checkpoint chunk: checkpoint data, MTU-aware.
    CheckpointChunk,
    /// Page history: per-page version chain.
    PageHistory,
}

impl EcsObjectType {
    /// All known object types, useful for exhaustive iteration.
    pub const ALL: [Self; 4] = [
        Self::CommitCapsule,
        Self::IndexSegment,
        Self::CheckpointChunk,
        Self::PageHistory,
    ];
}

impl std::fmt::Display for EcsObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CommitCapsule => write!(f, "CommitCapsule"),
            Self::IndexSegment => write!(f, "IndexSegment"),
            Self::CheckpointChunk => write!(f, "CheckpointChunk"),
            Self::PageHistory => write!(f, "PageHistory"),
        }
    }
}

// ---------------------------------------------------------------------------
// Symbol Size Policy
// ---------------------------------------------------------------------------

/// Minimum symbol size (bytes). Below this, metadata overhead dominates.
pub const MIN_SYMBOL_SIZE: u32 = 512;

/// Maximum symbol size (bytes). Above this, per-symbol loss impact is too high.
pub const MAX_SYMBOL_SIZE: u32 = 65_536;

/// Default symbol size for `IndexSegment` objects.
pub const DEFAULT_INDEX_SEGMENT_SYMBOL_SIZE: u32 = 4096;

/// Default symbol size for `CheckpointChunk` objects.
pub const DEFAULT_CHECKPOINT_CHUNK_SYMBOL_SIZE: u32 = 4096;

/// Symbol size policy: per-object-type sizing with sane defaults (§3.5.10).
///
/// The policy is versioned and stored in `RootManifest` so all replicas
/// use the same sizing for a given manifest version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SymbolSizePolicy {
    /// Policy version (bumped on any change).
    pub version: u32,
    /// Override for `CommitCapsule` symbol size. If `None`, uses default.
    pub commit_capsule_override: Option<u32>,
    /// Override for `IndexSegment` symbol size. If `None`, uses default.
    pub index_segment_override: Option<u32>,
    /// Override for `CheckpointChunk` symbol size. If `None`, uses default.
    pub checkpoint_chunk_override: Option<u32>,
    /// Override for `PageHistory` symbol size. If `None`, uses default.
    pub page_history_override: Option<u32>,
}

impl SymbolSizePolicy {
    /// Create the V1 default policy.
    #[must_use]
    pub const fn v1() -> Self {
        Self {
            version: 1,
            commit_capsule_override: None,
            index_segment_override: None,
            checkpoint_chunk_override: None,
            page_history_override: None,
        }
    }

    /// Compute the symbol size for an object type given the database page size.
    ///
    /// See §3.5.10 policy table for rationale.
    #[must_use]
    pub fn symbol_size_for(&self, object_type: EcsObjectType, page_size: u32) -> u32 {
        let computed = match object_type {
            EcsObjectType::CommitCapsule => self
                .commit_capsule_override
                .unwrap_or_else(|| page_size.min(4096)),
            EcsObjectType::IndexSegment => self
                .index_segment_override
                .unwrap_or(DEFAULT_INDEX_SEGMENT_SYMBOL_SIZE),
            EcsObjectType::CheckpointChunk => self
                .checkpoint_chunk_override
                .unwrap_or(DEFAULT_CHECKPOINT_CHUNK_SYMBOL_SIZE),
            EcsObjectType::PageHistory => self.page_history_override.unwrap_or(page_size),
        };

        let clamped = computed.clamp(MIN_SYMBOL_SIZE, MAX_SYMBOL_SIZE);
        debug!(
            bead_id = "bd-1hi.28",
            object_type = %object_type,
            page_size,
            computed,
            clamped,
            "symbol size policy lookup"
        );
        clamped
    }
}

impl Default for SymbolSizePolicy {
    fn default() -> Self {
        Self::v1()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- §3.5.10 test 10: CommitCapsule symbol size --

    #[test]
    fn test_symbol_size_commit_capsule() {
        let policy = SymbolSizePolicy::v1();

        // page_size=4096: T = min(4096, 4096) = 4096
        assert_eq!(
            policy.symbol_size_for(EcsObjectType::CommitCapsule, 4096),
            4096
        );

        // page_size=65536: T = min(65536, 4096) = 4096
        assert_eq!(
            policy.symbol_size_for(EcsObjectType::CommitCapsule, 65536),
            4096
        );

        // page_size=1024: T = min(1024, 4096) = 1024
        assert_eq!(
            policy.symbol_size_for(EcsObjectType::CommitCapsule, 1024),
            1024
        );

        // page_size=512: T = min(512, 4096) = 512 (at minimum)
        assert_eq!(
            policy.symbol_size_for(EcsObjectType::CommitCapsule, 512),
            512
        );
    }

    // -- §3.5.10 test 11: PageHistory symbol size --

    #[test]
    fn test_symbol_size_page_history() {
        let policy = SymbolSizePolicy::v1();

        // page_size=4096: T = 4096
        assert_eq!(
            policy.symbol_size_for(EcsObjectType::PageHistory, 4096),
            4096
        );

        // page_size=65536: T = 65536
        assert_eq!(
            policy.symbol_size_for(EcsObjectType::PageHistory, 65536),
            65536
        );

        // page_size=512: T = 512
        assert_eq!(policy.symbol_size_for(EcsObjectType::PageHistory, 512), 512);
    }

    // -- §3.5.10 test 12: Versioned in manifest --

    #[test]
    fn test_symbol_size_versioned_in_manifest() {
        let policy = SymbolSizePolicy::v1();
        assert_eq!(policy.version, 1, "V1 policy must have version 1");

        // A "V2" policy with different overrides would have a different version.
        let policy_v2 = SymbolSizePolicy {
            version: 2,
            commit_capsule_override: Some(2048),
            ..SymbolSizePolicy::v1()
        };
        assert_eq!(policy_v2.version, 2);
        assert_ne!(policy.version, policy_v2.version);

        // Replicas using different versions would produce different symbol sizes.
        assert_ne!(
            policy.symbol_size_for(EcsObjectType::CommitCapsule, 4096),
            policy_v2.symbol_size_for(EcsObjectType::CommitCapsule, 4096),
        );
    }

    // -- §3.5.10 test 13: Symbol size consistency --

    #[test]
    fn test_symbol_size_consistency() {
        let policy = SymbolSizePolicy::v1();
        let page_size = 4096;

        // All invocations for the same object type and page_size must return
        // the same T. This is trivially true for pure functions, but we verify
        // it explicitly as the spec demands.
        for obj_type in EcsObjectType::ALL {
            let t1 = policy.symbol_size_for(obj_type, page_size);
            let t2 = policy.symbol_size_for(obj_type, page_size);
            assert_eq!(
                t1, t2,
                "symbol size must be consistent for {obj_type:?} at page_size={page_size}"
            );
        }
    }

    // -- §3.5.10 property: symbol size within bounds --

    #[test]
    fn prop_symbol_size_within_bounds() {
        let policy = SymbolSizePolicy::v1();

        for &page_size in &[512_u32, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
            for obj_type in EcsObjectType::ALL {
                let t = policy.symbol_size_for(obj_type, page_size);
                assert!(
                    t >= MIN_SYMBOL_SIZE,
                    "{obj_type:?} at page_size={page_size}: T={t} < MIN={MIN_SYMBOL_SIZE}"
                );
                assert!(
                    t <= MAX_SYMBOL_SIZE,
                    "{obj_type:?} at page_size={page_size}: T={t} > MAX={MAX_SYMBOL_SIZE}"
                );
            }
        }
    }

    // -- Override tests --

    #[test]
    fn test_symbol_size_overrides() {
        let policy = SymbolSizePolicy {
            version: 2,
            commit_capsule_override: Some(2048),
            index_segment_override: Some(1280),
            checkpoint_chunk_override: Some(1024),
            page_history_override: Some(8192),
        };

        assert_eq!(
            policy.symbol_size_for(EcsObjectType::CommitCapsule, 4096),
            2048
        );
        assert_eq!(
            policy.symbol_size_for(EcsObjectType::IndexSegment, 4096),
            1280
        );
        assert_eq!(
            policy.symbol_size_for(EcsObjectType::CheckpointChunk, 4096),
            1024
        );
        assert_eq!(
            policy.symbol_size_for(EcsObjectType::PageHistory, 4096),
            8192
        );
    }

    // -- Clamping tests --

    #[test]
    fn test_symbol_size_clamping() {
        // Override below minimum → clamped to MIN_SYMBOL_SIZE
        let policy = SymbolSizePolicy {
            version: 3,
            commit_capsule_override: Some(100),
            ..SymbolSizePolicy::v1()
        };
        assert_eq!(
            policy.symbol_size_for(EcsObjectType::CommitCapsule, 4096),
            MIN_SYMBOL_SIZE
        );

        // Override above maximum → clamped to MAX_SYMBOL_SIZE
        let policy = SymbolSizePolicy {
            version: 4,
            page_history_override: Some(100_000),
            ..SymbolSizePolicy::v1()
        };
        assert_eq!(
            policy.symbol_size_for(EcsObjectType::PageHistory, 4096),
            MAX_SYMBOL_SIZE
        );
    }
}
