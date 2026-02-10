//! §3.5.8 Decode Proofs (Auditable Repair) + §3.5.9 Deterministic Encoding.
//!
//! Every decode that repairs corruption MUST produce a proof artifact
//! (a mathematical witness that the fix is correct). In replication,
//! a replica MAY demand proof artifacts for suspicious objects.
//!
//! This module provides the FrankenSQLite-side `EcsDecodeProof` type that
//! wraps asupersync's `DecodeProof` with ECS-specific metadata.

use std::fmt;

use fsqlite_types::ObjectId;
use tracing::{debug, info, warn};
use xxhash_rust::xxh3::xxh3_64;

// ---------------------------------------------------------------------------
// ECS Decode Proof (§3.5.8)
// ---------------------------------------------------------------------------

/// Stable schema version for `EcsDecodeProof`.
pub const DECODE_PROOF_SCHEMA_VERSION_V1: u16 = 1;

/// Default policy identifier for deterministic decode proof emission.
pub const DEFAULT_DECODE_PROOF_POLICY_ID: u32 = 1;
/// Default slack requirement used when verifying successful decode proofs.
pub const DEFAULT_DECODE_PROOF_SLACK: u32 = 2;

/// Why a symbol was rejected before decode.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum SymbolRejectionReason {
    HashMismatch,
    InvalidAuthTag,
    DuplicateEsi,
    FormatViolation,
}

impl fmt::Display for SymbolRejectionReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HashMismatch => write!(f, "hash_mismatch"),
            Self::InvalidAuthTag => write!(f, "invalid_auth_tag"),
            Self::DuplicateEsi => write!(f, "duplicate_esi"),
            Self::FormatViolation => write!(f, "format_violation"),
        }
    }
}

/// Rejected-symbol evidence item.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct RejectedSymbol {
    pub esi: u32,
    pub reason: SymbolRejectionReason,
}

/// Reason for decode failure when `decode_success == false`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum DecodeFailureReason {
    InsufficientSymbols,
    RankDeficiency,
    IntegrityMismatch,
    Unknown,
}

impl fmt::Display for DecodeFailureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientSymbols => write!(f, "insufficient_symbols"),
            Self::RankDeficiency => write!(f, "rank_deficiency"),
            Self::IntegrityMismatch => write!(f, "integrity_mismatch"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Redaction policy for proof payload material.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DecodeProofPayloadMode {
    /// Only metadata + hashes are persisted.
    HashesOnly,
    /// Lab/debug mode may include raw payload bytes.
    IncludeBytesLabOnly,
}

/// Hash of an accepted symbol input (replay-verification artifact).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct SymbolDigest {
    pub esi: u32,
    pub digest_xxh3: u64,
}

/// Deterministic digest set for replay verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProofInputHashes {
    pub metadata_xxh3: u64,
    pub source_esis_xxh3: u64,
    pub repair_esis_xxh3: u64,
    pub rejected_symbols_xxh3: u64,
    pub symbol_digests_xxh3: u64,
}

/// A decode proof recording the outcome and metadata of an ECS decode
/// operation (§3.5.8).
///
/// This is the FrankenSQLite-side proof artifact. It captures the fields
/// specified by the spec (`object_id`, `k_source`, received ESIs, source
/// vs repair partitions, success/failure, intermediate rank, timing, seed).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EcsDecodeProof {
    /// Stable schema version (load-bearing for replay tooling).
    pub schema_version: u16,
    /// Policy identifier used when this proof was emitted.
    pub policy_id: u32,
    /// The object being decoded.
    pub object_id: ObjectId,
    /// Optional changeset identifier (replication path).
    pub changeset_id: Option<[u8; 16]>,
    /// Number of source symbols (K).
    pub k_source: u32,
    /// Number of repair symbols configured for the decode budget (R).
    pub repair_count: u32,
    /// Symbol size (T) in bytes (0 when unavailable in this layer).
    pub symbol_size: u32,
    /// RaptorQ OTI/codec metadata hash (if available).
    pub oti: Option<u64>,
    /// ESIs of all symbols fed to the decoder.
    pub symbols_received: Vec<u32>,
    /// Subset of received ESIs that were source symbols.
    pub source_esis: Vec<u32>,
    /// Subset of received ESIs that were repair symbols.
    pub repair_esis: Vec<u32>,
    /// Rejected symbols and reasons (integrity/auth/format/dup).
    pub rejected_symbols: Vec<RejectedSymbol>,
    /// Hashes of accepted symbols for replay verification.
    pub symbol_digests: Vec<SymbolDigest>,
    /// Whether the decode succeeded.
    pub decode_success: bool,
    /// Failure reason when decode did not succeed.
    pub failure_reason: Option<DecodeFailureReason>,
    /// Decoder matrix rank at success/failure (if available).
    pub intermediate_rank: Option<u32>,
    /// Timing: wall-clock nanoseconds or virtual time under `LabRuntime`.
    pub timing_ns: u64,
    /// RaptorQ seed used for encoding.
    pub seed: u64,
    /// Redaction mode for payload material in this proof.
    pub payload_mode: DecodeProofPayloadMode,
    /// Optional symbol payload bytes (lab/debug only).
    pub debug_symbol_payloads: Option<Vec<Vec<u8>>>,
    /// Deterministic digest summary for replay verification.
    pub input_hashes: ProofInputHashes,
}

/// Inputs controlling proof verification behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DecodeProofVerificationConfig {
    pub expected_schema_version: u16,
    pub expected_policy_id: u32,
    pub decode_success_slack: u32,
}

impl Default for DecodeProofVerificationConfig {
    fn default() -> Self {
        Self {
            expected_schema_version: DECODE_PROOF_SCHEMA_VERSION_V1,
            expected_policy_id: DEFAULT_DECODE_PROOF_POLICY_ID,
            decode_success_slack: DEFAULT_DECODE_PROOF_SLACK,
        }
    }
}

/// Deterministic verifier finding item.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DecodeProofVerificationIssue {
    pub code: String,
    pub detail: String,
}

/// Deterministic, structured report emitted by proof verification tooling.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct DecodeProofVerificationReport {
    pub ok: bool,
    pub expected_schema_version: u16,
    pub expected_policy_id: u32,
    pub decode_success_slack: u32,
    pub schema_version_ok: bool,
    pub policy_id_ok: bool,
    pub internal_consistency_ok: bool,
    pub metadata_hash_ok: bool,
    pub source_hash_ok: bool,
    pub repair_hash_ok: bool,
    pub rejected_hash_ok: bool,
    pub symbol_digests_hash_ok: bool,
    pub replay_verifies: bool,
    pub decode_success_budget_ok: bool,
    pub decode_success_expected_min_symbols: u32,
    pub decode_success_observed_symbols: u32,
    pub rejected_reasons_hash_or_auth_only: bool,
    pub issues: Vec<DecodeProofVerificationIssue>,
}

impl EcsDecodeProof {
    /// Create a proof for a successful decode operation.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn success(
        object_id: ObjectId,
        k_source: u32,
        symbols_received: Vec<u32>,
        source_esis: Vec<u32>,
        repair_esis: Vec<u32>,
        intermediate_rank: Option<u32>,
        timing_ns: u64,
        seed: u64,
    ) -> Self {
        let proof = Self::from_parts(
            object_id,
            None,
            k_source,
            symbols_received,
            source_esis,
            repair_esis,
            Vec::new(),
            Vec::new(),
            true,
            None,
            intermediate_rank,
            timing_ns,
            seed,
        );
        info!(
            bead_id = "bd-awqq",
            object_id = ?proof.object_id,
            k_source = proof.k_source,
            received = proof.symbols_received.len(),
            source = proof.source_esis.len(),
            repair = proof.repair_esis.len(),
            timing_ns = proof.timing_ns,
            schema_version = proof.schema_version,
            policy_id = proof.policy_id,
            "decode proof: SUCCESS"
        );
        proof
    }

    /// Create a proof for a failed decode operation.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn failure(
        object_id: ObjectId,
        k_source: u32,
        symbols_received: Vec<u32>,
        source_esis: Vec<u32>,
        repair_esis: Vec<u32>,
        intermediate_rank: Option<u32>,
        timing_ns: u64,
        seed: u64,
    ) -> Self {
        let proof = Self::from_parts(
            object_id,
            None,
            k_source,
            symbols_received,
            source_esis,
            repair_esis,
            Vec::new(),
            Vec::new(),
            false,
            Some(DecodeFailureReason::Unknown),
            intermediate_rank,
            timing_ns,
            seed,
        );
        warn!(
            bead_id = "bd-awqq",
            object_id = ?proof.object_id,
            k_source = proof.k_source,
            received = proof.symbols_received.len(),
            intermediate_rank = proof.intermediate_rank,
            timing_ns = proof.timing_ns,
            failure_reason = ?proof.failure_reason,
            "decode proof: FAILURE"
        );
        proof
    }

    /// Build an `EcsDecodeProof` from raw received-symbol ESIs.
    ///
    /// Partitions received ESIs into source (< k_source) and repair (>= k_source).
    #[must_use]
    pub fn from_esis(
        object_id: ObjectId,
        k_source: u32,
        all_esis: &[u32],
        decode_success: bool,
        intermediate_rank: Option<u32>,
        timing_ns: u64,
        seed: u64,
    ) -> Self {
        let mut source_partition = Vec::new();
        let mut repair_partition = Vec::new();
        for &esi in all_esis {
            if esi < k_source {
                source_partition.push(esi);
            } else {
                repair_partition.push(esi);
            }
        }
        let symbols_received = canonicalize_esis(all_esis.to_vec());
        source_partition = canonicalize_esis(source_partition);
        repair_partition = canonicalize_esis(repair_partition);

        debug!(
            bead_id = "bd-awqq",
            source_count = source_partition.len(),
            repair_count = repair_partition.len(),
            "partitioned received ESIs into source/repair"
        );

        Self::from_parts(
            object_id,
            None,
            k_source,
            symbols_received,
            source_partition,
            repair_partition,
            Vec::new(),
            Vec::new(),
            decode_success,
            (!decode_success).then_some(DecodeFailureReason::Unknown),
            intermediate_rank,
            timing_ns,
            seed,
        )
    }

    /// Whether this proof records a repair operation (i.e., repair symbols used).
    #[must_use]
    pub fn is_repair(&self) -> bool {
        !self.repair_esis.is_empty()
    }

    /// Whether the decode used the minimum possible symbols (fragile recovery).
    #[must_use]
    pub fn is_minimum_decode(&self) -> bool {
        #[allow(clippy::cast_possible_truncation)]
        let received = self.symbols_received.len() as u32;
        received == self.k_source
    }

    /// Verify that this proof is internally consistent.
    ///
    /// Returns `true` if source_esis + repair_esis == symbols_received and
    /// all ESI partitions are correct.
    #[must_use]
    pub fn is_consistent(&self) -> bool {
        if self.schema_version != DECODE_PROOF_SCHEMA_VERSION_V1 {
            return false;
        }
        if self.decode_success && self.failure_reason.is_some() {
            return false;
        }
        if !self.decode_success && self.failure_reason.is_none() {
            return false;
        }
        if self.payload_mode == DecodeProofPayloadMode::HashesOnly
            && self.debug_symbol_payloads.is_some()
        {
            return false;
        }
        if self.repair_count != u32::try_from(self.repair_esis.len()).unwrap_or(u32::MAX) {
            return false;
        }

        if !is_sorted_unique(&self.symbols_received)
            || !is_sorted_unique(&self.source_esis)
            || !is_sorted_unique(&self.repair_esis)
        {
            return false;
        }
        if !is_sorted_unique(&self.rejected_symbols) || !is_sorted_unique(&self.symbol_digests) {
            return false;
        }

        let mut union = self.source_esis.clone();
        union.extend(self.repair_esis.iter().copied());
        union = canonicalize_esis(union);
        if union != self.symbols_received {
            return false;
        }

        if self.source_esis.iter().any(|&e| e >= self.k_source) {
            return false;
        }
        if self.repair_esis.iter().any(|&e| e < self.k_source) {
            return false;
        }

        if self
            .symbol_digests
            .iter()
            .any(|digest| !self.symbols_received.contains(&digest.esi))
        {
            return false;
        }

        self.input_hashes == self.compute_input_hashes()
    }

    /// Attach changeset identity metadata and recompute integrity hashes.
    #[must_use]
    pub fn with_changeset_id(mut self, changeset_id: [u8; 16]) -> Self {
        self.changeset_id = Some(changeset_id);
        self.input_hashes = self.compute_input_hashes();
        self
    }

    /// Attach rejected-symbol evidence and recompute integrity hashes.
    #[must_use]
    pub fn with_rejected_symbols(mut self, rejected_symbols: Vec<RejectedSymbol>) -> Self {
        self.rejected_symbols = canonicalize_rejected_symbols(rejected_symbols);
        self.input_hashes = self.compute_input_hashes();
        self
    }

    /// Attach accepted-symbol digests and recompute integrity hashes.
    #[must_use]
    pub fn with_symbol_digests(mut self, symbol_digests: Vec<SymbolDigest>) -> Self {
        self.symbol_digests = canonicalize_symbol_digests(symbol_digests);
        self.input_hashes = self.compute_input_hashes();
        self
    }

    /// Switch proof to debug payload mode and embed symbol payload bytes.
    #[must_use]
    pub fn with_debug_symbol_payloads(mut self, payloads: Vec<Vec<u8>>) -> Self {
        self.payload_mode = DecodeProofPayloadMode::IncludeBytesLabOnly;
        self.debug_symbol_payloads = Some(payloads);
        self.input_hashes = self.compute_input_hashes();
        self
    }

    /// Replay verification: ensure digest evidence matches this proof.
    #[must_use]
    pub fn replay_verifies(
        &self,
        symbol_digests: &[SymbolDigest],
        rejected_symbols: &[RejectedSymbol],
    ) -> bool {
        let expected_symbol_digests = canonicalize_symbol_digests(symbol_digests.to_vec());
        let expected_rejected = canonicalize_rejected_symbols(rejected_symbols.to_vec());
        if self.symbol_digests != expected_symbol_digests {
            return false;
        }
        if self.rejected_symbols != expected_rejected {
            return false;
        }
        self.input_hashes.symbol_digests_xxh3 == hash_symbol_digests(&expected_symbol_digests)
            && self.input_hashes.rejected_symbols_xxh3 == hash_rejected_symbols(&expected_rejected)
    }

    /// Verify proof integrity and emit a deterministic structured report.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn verification_report(
        &self,
        config: DecodeProofVerificationConfig,
        symbol_digests: &[SymbolDigest],
        rejected_symbols: &[RejectedSymbol],
    ) -> DecodeProofVerificationReport {
        let expected_symbol_digests = canonicalize_symbol_digests(symbol_digests.to_vec());
        let expected_rejected = canonicalize_rejected_symbols(rejected_symbols.to_vec());

        let schema_version_ok = self.schema_version == config.expected_schema_version;
        let policy_id_ok = self.policy_id == config.expected_policy_id;
        let internal_consistency_ok = self.is_consistent();
        let metadata_hash_ok = self.input_hashes.metadata_xxh3 == hash_metadata(self);
        let source_hash_ok =
            self.input_hashes.source_esis_xxh3 == hash_u32_list("source_esis", &self.source_esis);
        let repair_hash_ok =
            self.input_hashes.repair_esis_xxh3 == hash_u32_list("repair_esis", &self.repair_esis);
        let rejected_hash_ok =
            self.input_hashes.rejected_symbols_xxh3 == hash_rejected_symbols(&expected_rejected);
        let symbol_digests_hash_ok =
            self.input_hashes.symbol_digests_xxh3 == hash_symbol_digests(&expected_symbol_digests);
        let replay_verifies = self.replay_verifies(&expected_symbol_digests, &expected_rejected);

        let decode_success_expected_min_symbols =
            self.k_source.saturating_add(config.decode_success_slack);
        let decode_success_observed_symbols =
            u32::try_from(self.symbols_received.len()).unwrap_or(u32::MAX);
        let decode_success_budget_ok = !self.decode_success
            || decode_success_observed_symbols >= decode_success_expected_min_symbols;
        let rejected_reasons_hash_or_auth_only = self.rejected_symbols.iter().all(|entry| {
            matches!(
                entry.reason,
                SymbolRejectionReason::HashMismatch | SymbolRejectionReason::InvalidAuthTag
            )
        });

        let mut issues = Vec::new();
        if !schema_version_ok {
            issues.push(DecodeProofVerificationIssue {
                code: String::from("schema_version_mismatch"),
                detail: format!(
                    "expected {}, got {}",
                    config.expected_schema_version, self.schema_version
                ),
            });
        }
        if !policy_id_ok {
            issues.push(DecodeProofVerificationIssue {
                code: String::from("policy_id_mismatch"),
                detail: format!(
                    "expected {}, got {}",
                    config.expected_policy_id, self.policy_id
                ),
            });
        }
        if !internal_consistency_ok {
            issues.push(DecodeProofVerificationIssue {
                code: String::from("internal_consistency_failed"),
                detail: String::from("proof failed internal consistency checks"),
            });
        }
        if !metadata_hash_ok
            || !source_hash_ok
            || !repair_hash_ok
            || !rejected_hash_ok
            || !symbol_digests_hash_ok
        {
            issues.push(DecodeProofVerificationIssue {
                code: String::from("hash_mismatch"),
                detail: format!(
                    "metadata={metadata_hash_ok} source={source_hash_ok} repair={repair_hash_ok} rejected={rejected_hash_ok} symbol_digests={symbol_digests_hash_ok}"
                ),
            });
        }
        if !replay_verifies {
            issues.push(DecodeProofVerificationIssue {
                code: String::from("replay_verification_failed"),
                detail: String::from("provided digest/rejection evidence did not match proof"),
            });
        }
        if !decode_success_budget_ok {
            issues.push(DecodeProofVerificationIssue {
                code: String::from("decode_success_budget_failed"),
                detail: format!(
                    "success proof had {decode_success_observed_symbols} symbols, required >= {decode_success_expected_min_symbols}",
                ),
            });
        }
        if !rejected_reasons_hash_or_auth_only {
            issues.push(DecodeProofVerificationIssue {
                code: String::from("rejected_reason_unsupported"),
                detail: String::from(
                    "rejected-symbol reasons must be hash/auth mismatch for this verifier",
                ),
            });
        }

        let ok = issues.is_empty();
        DecodeProofVerificationReport {
            ok,
            expected_schema_version: config.expected_schema_version,
            expected_policy_id: config.expected_policy_id,
            decode_success_slack: config.decode_success_slack,
            schema_version_ok,
            policy_id_ok,
            internal_consistency_ok,
            metadata_hash_ok,
            source_hash_ok,
            repair_hash_ok,
            rejected_hash_ok,
            symbol_digests_hash_ok,
            replay_verifies,
            decode_success_budget_ok,
            decode_success_expected_min_symbols,
            decode_success_observed_symbols,
            rejected_reasons_hash_or_auth_only,
            issues,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        object_id: ObjectId,
        changeset_id: Option<[u8; 16]>,
        k_source: u32,
        symbols_received: Vec<u32>,
        source_esis: Vec<u32>,
        repair_esis: Vec<u32>,
        rejected_symbols: Vec<RejectedSymbol>,
        symbol_digests: Vec<SymbolDigest>,
        decode_success: bool,
        failure_reason: Option<DecodeFailureReason>,
        intermediate_rank: Option<u32>,
        timing_ns: u64,
        seed: u64,
    ) -> Self {
        let mut proof = Self {
            schema_version: DECODE_PROOF_SCHEMA_VERSION_V1,
            policy_id: DEFAULT_DECODE_PROOF_POLICY_ID,
            object_id,
            changeset_id,
            k_source,
            repair_count: u32::try_from(repair_esis.len()).unwrap_or(u32::MAX),
            symbol_size: 0,
            oti: None,
            symbols_received: canonicalize_esis(symbols_received),
            source_esis: canonicalize_esis(source_esis),
            repair_esis: canonicalize_esis(repair_esis),
            rejected_symbols: canonicalize_rejected_symbols(rejected_symbols),
            symbol_digests: canonicalize_symbol_digests(symbol_digests),
            decode_success,
            failure_reason,
            intermediate_rank,
            timing_ns,
            seed,
            payload_mode: DecodeProofPayloadMode::HashesOnly,
            debug_symbol_payloads: None,
            input_hashes: ProofInputHashes {
                metadata_xxh3: 0,
                source_esis_xxh3: 0,
                repair_esis_xxh3: 0,
                rejected_symbols_xxh3: 0,
                symbol_digests_xxh3: 0,
            },
        };
        proof.input_hashes = proof.compute_input_hashes();
        proof
    }

    fn compute_input_hashes(&self) -> ProofInputHashes {
        ProofInputHashes {
            metadata_xxh3: hash_metadata(self),
            source_esis_xxh3: hash_u32_list("source_esis", &self.source_esis),
            repair_esis_xxh3: hash_u32_list("repair_esis", &self.repair_esis),
            rejected_symbols_xxh3: hash_rejected_symbols(&self.rejected_symbols),
            symbol_digests_xxh3: hash_symbol_digests(&self.symbol_digests),
        }
    }
}

fn canonicalize_esis(mut values: Vec<u32>) -> Vec<u32> {
    values.sort_unstable();
    values.dedup();
    values
}

fn canonicalize_rejected_symbols(mut values: Vec<RejectedSymbol>) -> Vec<RejectedSymbol> {
    values.sort();
    values.dedup();
    values
}

fn canonicalize_symbol_digests(mut values: Vec<SymbolDigest>) -> Vec<SymbolDigest> {
    values.sort();
    values.dedup();
    values
}

fn is_sorted_unique<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn hash_u32_list(domain: &str, values: &[u32]) -> u64 {
    let mut bytes = Vec::with_capacity(domain.len() + values.len() * 4);
    bytes.extend_from_slice(domain.as_bytes());
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    xxh3_64(&bytes)
}

fn rejection_reason_code(reason: SymbolRejectionReason) -> u8 {
    match reason {
        SymbolRejectionReason::HashMismatch => 1,
        SymbolRejectionReason::InvalidAuthTag => 2,
        SymbolRejectionReason::DuplicateEsi => 3,
        SymbolRejectionReason::FormatViolation => 4,
    }
}

fn failure_reason_code(reason: DecodeFailureReason) -> u8 {
    match reason {
        DecodeFailureReason::InsufficientSymbols => 1,
        DecodeFailureReason::RankDeficiency => 2,
        DecodeFailureReason::IntegrityMismatch => 3,
        DecodeFailureReason::Unknown => 255,
    }
}

fn hash_rejected_symbols(values: &[RejectedSymbol]) -> u64 {
    let mut bytes = Vec::with_capacity("rejected".len() + values.len() * 5);
    bytes.extend_from_slice(b"rejected");
    for value in values {
        bytes.extend_from_slice(&value.esi.to_le_bytes());
        bytes.push(rejection_reason_code(value.reason));
    }
    xxh3_64(&bytes)
}

fn hash_symbol_digests(values: &[SymbolDigest]) -> u64 {
    let mut bytes = Vec::with_capacity("symbol_digests".len() + values.len() * 12);
    bytes.extend_from_slice(b"symbol_digests");
    for value in values {
        bytes.extend_from_slice(&value.esi.to_le_bytes());
        bytes.extend_from_slice(&value.digest_xxh3.to_le_bytes());
    }
    xxh3_64(&bytes)
}

fn hash_debug_payloads(payloads: Option<&[Vec<u8>]>) -> u64 {
    let Some(payloads) = payloads else {
        return 0;
    };
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"debug_payloads");
    for payload in payloads {
        let len = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(&xxh3_64(payload).to_le_bytes());
    }
    xxh3_64(&bytes)
}

fn hash_metadata(proof: &EcsDecodeProof) -> u64 {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"decode_proof_metadata");
    bytes.extend_from_slice(&proof.schema_version.to_le_bytes());
    bytes.extend_from_slice(&proof.policy_id.to_le_bytes());
    bytes.extend_from_slice(proof.object_id.as_bytes());
    if let Some(changeset_id) = proof.changeset_id {
        bytes.push(1);
        bytes.extend_from_slice(&changeset_id);
    } else {
        bytes.push(0);
    }
    bytes.extend_from_slice(&proof.k_source.to_le_bytes());
    bytes.extend_from_slice(&proof.repair_count.to_le_bytes());
    bytes.extend_from_slice(&proof.symbol_size.to_le_bytes());
    bytes.extend_from_slice(&proof.seed.to_le_bytes());
    if let Some(oti) = proof.oti {
        bytes.push(1);
        bytes.extend_from_slice(&oti.to_le_bytes());
    } else {
        bytes.push(0);
    }
    bytes.push(u8::from(proof.decode_success));
    if let Some(reason) = proof.failure_reason {
        bytes.push(1);
        bytes.push(failure_reason_code(reason));
    } else {
        bytes.push(0);
    }
    if let Some(rank) = proof.intermediate_rank {
        bytes.push(1);
        bytes.extend_from_slice(&rank.to_le_bytes());
    } else {
        bytes.push(0);
    }
    bytes.extend_from_slice(&proof.timing_ns.to_le_bytes());
    bytes.push(match proof.payload_mode {
        DecodeProofPayloadMode::HashesOnly => 0,
        DecodeProofPayloadMode::IncludeBytesLabOnly => 1,
    });
    bytes.extend_from_slice(
        &hash_debug_payloads(proof.debug_symbol_payloads.as_deref()).to_le_bytes(),
    );
    xxh3_64(&bytes)
}
// ---------------------------------------------------------------------------
// Decode Audit Trail
// ---------------------------------------------------------------------------

/// An entry in the decode audit trail (§3.5.8, lab runtime integration).
///
/// In lab runtime, every repair decode produces a proof attached to the
/// test trace. This struct groups the proof with its trace context.
#[derive(Debug, Clone)]
pub struct DecodeAuditEntry {
    /// The decode proof artifact.
    pub proof: EcsDecodeProof,
    /// Monotonic sequence number within the audit trail.
    pub seq: u64,
    /// Whether this was produced under lab runtime (deterministic virtual time).
    pub lab_mode: bool,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_object_id(seed: u64) -> ObjectId {
        ObjectId::derive_from_canonical_bytes(&seed.to_le_bytes())
    }

    fn stable_proof_bytes_for_test(proof: &EcsDecodeProof) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&proof.schema_version.to_le_bytes());
        bytes.extend_from_slice(&proof.policy_id.to_le_bytes());
        bytes.extend_from_slice(proof.object_id.as_bytes());
        bytes.extend_from_slice(
            &proof
                .changeset_id
                .map_or([0_u8; 16], |changeset_id| changeset_id),
        );
        bytes.extend_from_slice(&proof.k_source.to_le_bytes());
        bytes.extend_from_slice(&proof.repair_count.to_le_bytes());
        bytes.extend_from_slice(&proof.symbol_size.to_le_bytes());
        bytes.extend_from_slice(&proof.oti.unwrap_or(0).to_le_bytes());
        bytes.extend_from_slice(&proof.seed.to_le_bytes());
        bytes.extend_from_slice(&proof.timing_ns.to_le_bytes());
        bytes.push(u8::from(proof.decode_success));
        bytes.extend_from_slice(
            &proof
                .failure_reason
                .map_or(255_u8, failure_reason_code)
                .to_le_bytes(),
        );
        bytes.extend_from_slice(&proof.intermediate_rank.unwrap_or(u32::MAX).to_le_bytes());
        bytes.push(match proof.payload_mode {
            DecodeProofPayloadMode::HashesOnly => 0,
            DecodeProofPayloadMode::IncludeBytesLabOnly => 1,
        });

        bytes.extend_from_slice(
            &u32::try_from(proof.symbols_received.len())
                .expect("symbols_received length fits u32")
                .to_le_bytes(),
        );
        for esi in &proof.symbols_received {
            bytes.extend_from_slice(&esi.to_le_bytes());
        }

        bytes.extend_from_slice(
            &u32::try_from(proof.source_esis.len())
                .expect("source_esis length fits u32")
                .to_le_bytes(),
        );
        for esi in &proof.source_esis {
            bytes.extend_from_slice(&esi.to_le_bytes());
        }

        bytes.extend_from_slice(
            &u32::try_from(proof.repair_esis.len())
                .expect("repair_esis length fits u32")
                .to_le_bytes(),
        );
        for esi in &proof.repair_esis {
            bytes.extend_from_slice(&esi.to_le_bytes());
        }

        bytes.extend_from_slice(
            &u32::try_from(proof.rejected_symbols.len())
                .expect("rejected_symbols length fits u32")
                .to_le_bytes(),
        );
        for rejected in &proof.rejected_symbols {
            bytes.extend_from_slice(&rejected.esi.to_le_bytes());
            bytes.push(rejection_reason_code(rejected.reason));
        }

        bytes.extend_from_slice(
            &u32::try_from(proof.symbol_digests.len())
                .expect("symbol_digests length fits u32")
                .to_le_bytes(),
        );
        for digest in &proof.symbol_digests {
            bytes.extend_from_slice(&digest.esi.to_le_bytes());
            bytes.extend_from_slice(&digest.digest_xxh3.to_le_bytes());
        }

        if let Some(payloads) = &proof.debug_symbol_payloads {
            bytes.extend_from_slice(
                &u32::try_from(payloads.len())
                    .expect("debug payload count fits u32")
                    .to_le_bytes(),
            );
            for payload in payloads {
                bytes.extend_from_slice(
                    &u32::try_from(payload.len())
                        .expect("debug payload length fits u32")
                        .to_le_bytes(),
                );
                bytes.extend_from_slice(payload);
            }
        } else {
            bytes.extend_from_slice(&0_u32.to_le_bytes());
        }

        bytes
    }

    // -- §3.5.8 test 1: Decode proof creation --

    #[test]
    fn test_decode_proof_creation() {
        let oid = test_object_id(0x1234);
        let k_source = 10;
        let all_esis: Vec<u32> = (0..12).collect(); // 10 source + 2 repair

        let proof = EcsDecodeProof::from_esis(oid, k_source, &all_esis, true, Some(10), 5000, 42);

        assert!(proof.decode_success);
        assert_eq!(proof.source_esis, (0..10).collect::<Vec<u32>>());
        assert_eq!(proof.repair_esis, vec![10, 11]);
        assert_eq!(proof.symbols_received.len(), 12);
        assert!(proof.is_repair());
        assert!(proof.is_consistent());
    }

    // -- §3.5.8 test 2: Lab mode timing --

    #[test]
    fn test_decode_proof_lab_mode() {
        let oid = test_object_id(0x5678);
        let lab_timing_ns = 1_000_000; // deterministic virtual time

        let proof = EcsDecodeProof::success(
            oid,
            8,
            (0..10).collect(),
            (0..8).collect(),
            vec![8, 9],
            Some(8),
            lab_timing_ns,
            99,
        );

        let entry = DecodeAuditEntry {
            proof,
            seq: 1,
            lab_mode: true,
        };

        assert!(entry.lab_mode);
        assert_eq!(entry.proof.timing_ns, lab_timing_ns);
        assert_eq!(entry.seq, 1);
    }

    // -- §3.5.8 test 3: Failure case --

    #[test]
    fn test_decode_proof_failure_case() {
        let oid = test_object_id(0xABCD);
        let k_source = 16;
        // Only 10 symbols received (insufficient for K=16)
        let all_esis: Vec<u32> = (0..10).collect();

        let proof = EcsDecodeProof::from_esis(oid, k_source, &all_esis, false, Some(10), 3000, 77);

        assert!(!proof.decode_success);
        assert_eq!(proof.intermediate_rank, Some(10));
        assert_eq!(proof.source_esis.len(), 10);
        assert!(proof.repair_esis.is_empty());
        assert!(proof.is_consistent());
    }

    // -- §3.5.8 test 4: Auditable (reproducible given same seed and ESIs) --

    #[test]
    fn test_decode_proof_auditable() {
        let oid = test_object_id(0xFEED);
        let k_source = 8;
        let esis: Vec<u32> = (0..10).collect();

        let proof_a = EcsDecodeProof::from_esis(oid, k_source, &esis, true, Some(8), 100, 42);
        let proof_b = EcsDecodeProof::from_esis(oid, k_source, &esis, true, Some(8), 100, 42);

        assert_eq!(
            proof_a, proof_b,
            "same inputs must produce identical proofs"
        );
    }

    // -- §3.5.8 test 5: Attached to trace --

    #[test]
    fn test_decode_proof_attached_to_trace() {
        let oid = test_object_id(0xCAFE);
        let proof = EcsDecodeProof::success(
            oid,
            8,
            (0..10).collect(),
            (0..8).collect(),
            vec![8, 9],
            Some(8),
            500,
            42,
        );

        // In lab runtime, every repair decode produces a proof attached to trace.
        let mut trace: Vec<DecodeAuditEntry> = Vec::new();
        if proof.is_repair() {
            trace.push(DecodeAuditEntry {
                proof,
                seq: 0,
                lab_mode: true,
            });
        }

        assert_eq!(trace.len(), 1, "repair decode must produce audit entry");
        assert!(trace[0].lab_mode);
    }

    // -- §3.5.9 test 8: Deterministic repair generation --

    #[test]
    fn test_deterministic_repair_generation() {
        // Same ObjectId + same config → same seed → same repair symbols.
        // Different ObjectId → different seed.
        let oid_a = test_object_id(0x1111);
        let oid_b = test_object_id(0x2222);

        let seed_a = crate::repair_symbols::derive_repair_seed(&oid_a);
        let seed_b = crate::repair_symbols::derive_repair_seed(&oid_a);
        let seed_c = crate::repair_symbols::derive_repair_seed(&oid_b);

        assert_eq!(
            seed_a, seed_b,
            "same ObjectId must produce same repair seed"
        );
        assert_ne!(
            seed_a, seed_c,
            "different ObjectIds must produce different seeds"
        );
    }

    // -- §3.5.9 test 9: Cross-replica determinism --

    #[test]
    fn test_cross_replica_determinism() {
        // Two independent "replicas" derive seeds from the same ObjectId.
        // Both must get the same seed, proving any replica can regenerate
        // repair symbols independently.
        let payload = b"commit_capsule_payload_12345";
        let oid = ObjectId::derive_from_canonical_bytes(payload);

        // Replica 1
        let seed_r1 = crate::repair_symbols::derive_repair_seed(&oid);
        let budget_r1 = crate::repair_symbols::compute_repair_budget(
            100,
            &crate::repair_symbols::RepairConfig::new(),
        );

        // Replica 2 (same inputs, independent derivation)
        let seed_r2 = crate::repair_symbols::derive_repair_seed(&oid);
        let budget_r2 = crate::repair_symbols::compute_repair_budget(
            100,
            &crate::repair_symbols::RepairConfig::new(),
        );

        assert_eq!(seed_r1, seed_r2, "cross-replica seed derivation must match");
        assert_eq!(
            budget_r1, budget_r2,
            "cross-replica repair budgets must match"
        );
    }

    // -- §3.5.8 property: consistency invariant --

    #[test]
    fn prop_proof_consistency_invariant() {
        for k in [1_u32, 4, 8, 16, 100] {
            for extra in [0_u32, 1, 2, 5, 10] {
                let total = k + extra;
                let esis: Vec<u32> = (0..total).collect();
                let oid = test_object_id(u64::from(k) * 1000 + u64::from(extra));
                let proof = EcsDecodeProof::from_esis(oid, k, &esis, true, None, 0, 0);
                assert!(
                    proof.is_consistent(),
                    "proof must be consistent for k={k}, extra={extra}"
                );
                assert_eq!(
                    proof.source_esis.len(),
                    k as usize,
                    "source count must equal k"
                );
                assert_eq!(
                    proof.repair_esis.len(),
                    extra as usize,
                    "repair count must equal extra"
                );
            }
        }
    }

    // -- §3.5.9 property: seed no collision --

    #[test]
    fn prop_seed_no_collision() {
        use std::collections::HashSet;

        let mut seeds = HashSet::new();
        for i in 0..100_000_u64 {
            let oid = ObjectId::derive_from_canonical_bytes(&i.to_le_bytes());
            let seed = crate::repair_symbols::derive_repair_seed(&oid);
            seeds.insert(seed);
        }

        // With 64-bit seeds and 100k entries, collision probability is ~2.7e-10.
        // We allow at most 1 collision for robustness.
        assert!(
            seeds.len() >= 99_999,
            "expected at most 1 collision in 100k seeds, got {} unique out of 100000",
            seeds.len()
        );
    }

    // -- §3.5.8 test: minimum decode detection --

    #[test]
    fn test_minimum_decode_detection() {
        let oid = test_object_id(0xBEEF);
        let k_source = 10;

        // Exactly K symbols (minimum decode)
        let esis_min: Vec<u32> = (0..10).collect();
        let proof_min =
            EcsDecodeProof::from_esis(oid, k_source, &esis_min, true, Some(10), 100, 42);
        assert!(
            proof_min.is_minimum_decode(),
            "K=10 received=10 should be minimum decode"
        );

        // K+2 symbols (not minimum)
        let esis_extra: Vec<u32> = (0..12).collect();
        let proof_extra =
            EcsDecodeProof::from_esis(oid, k_source, &esis_extra, true, Some(10), 100, 42);
        assert!(
            !proof_extra.is_minimum_decode(),
            "K=10 received=12 should not be minimum decode"
        );
    }

    // -- bd-awqq schema tests --

    #[test]
    fn test_decode_proof_schema_versioned_defaults() {
        let oid = test_object_id(0xAAAA);
        let proof = EcsDecodeProof::from_esis(oid, 4, &[0, 1, 2, 3], true, Some(4), 42, 99);
        assert_eq!(proof.schema_version, DECODE_PROOF_SCHEMA_VERSION_V1);
        assert_eq!(proof.policy_id, DEFAULT_DECODE_PROOF_POLICY_ID);
        assert_eq!(proof.payload_mode, DecodeProofPayloadMode::HashesOnly);
        assert!(proof.debug_symbol_payloads.is_none());
        assert!(proof.is_consistent());
    }

    #[test]
    fn test_decode_proof_replay_verification_with_digests_and_rejections() {
        let oid = test_object_id(0xBBBB);
        let symbol_digests = vec![
            SymbolDigest {
                esi: 0,
                digest_xxh3: 11,
            },
            SymbolDigest {
                esi: 1,
                digest_xxh3: 22,
            },
        ];
        let rejected = vec![RejectedSymbol {
            esi: 9,
            reason: SymbolRejectionReason::InvalidAuthTag,
        }];
        let proof = EcsDecodeProof::from_esis(oid, 2, &[0, 1, 2], true, Some(2), 100, 17)
            .with_symbol_digests(symbol_digests.clone())
            .with_rejected_symbols(rejected.clone());

        assert!(proof.replay_verifies(&symbol_digests, &rejected));
        assert!(!proof.replay_verifies(
            &[SymbolDigest {
                esi: 0,
                digest_xxh3: 999
            }],
            &rejected
        ));
    }

    #[test]
    fn test_decode_proof_canonicalization_is_deterministic() {
        let oid = test_object_id(0xCCCC);
        let a = EcsDecodeProof::from_esis(oid, 4, &[3, 0, 1, 3, 2, 4, 4], false, Some(3), 77, 5)
            .with_rejected_symbols(vec![
                RejectedSymbol {
                    esi: 8,
                    reason: SymbolRejectionReason::HashMismatch,
                },
                RejectedSymbol {
                    esi: 8,
                    reason: SymbolRejectionReason::HashMismatch,
                },
            ]);
        let b = EcsDecodeProof::from_esis(oid, 4, &[0, 1, 2, 3, 4], false, Some(3), 77, 5)
            .with_rejected_symbols(vec![RejectedSymbol {
                esi: 8,
                reason: SymbolRejectionReason::HashMismatch,
            }]);
        assert_eq!(a, b, "canonicalization must make output deterministic");
        assert!(a.is_consistent());
    }

    #[test]
    fn test_decode_proof_failure_reason_consistency() {
        let oid = test_object_id(0xDDDD);
        let proof = EcsDecodeProof::failure(
            oid,
            8,
            vec![0, 1, 2],
            vec![0, 1, 2],
            vec![],
            Some(3),
            900,
            33,
        );
        assert_eq!(proof.failure_reason, Some(DecodeFailureReason::Unknown));
        assert!(!proof.decode_success);
        assert!(proof.is_consistent());
    }

    #[test]
    fn test_decode_proof_verification_report_success() {
        let oid = test_object_id(0xEEEE);
        let symbol_digests = vec![
            SymbolDigest {
                esi: 0,
                digest_xxh3: 10,
            },
            SymbolDigest {
                esi: 1,
                digest_xxh3: 20,
            },
        ];
        let rejected = vec![RejectedSymbol {
            esi: 9,
            reason: SymbolRejectionReason::HashMismatch,
        }];
        let proof = EcsDecodeProof::from_esis(oid, 4, &[0, 1, 2, 3, 4, 5], true, Some(4), 50, 7)
            .with_symbol_digests(symbol_digests.clone())
            .with_rejected_symbols(rejected.clone());

        let report = proof.verification_report(
            DecodeProofVerificationConfig::default(),
            &symbol_digests,
            &rejected,
        );
        assert!(report.ok, "report should pass: {report:?}");
        assert!(report.replay_verifies);
        assert!(report.decode_success_budget_ok);
        assert!(report.issues.is_empty());
    }

    #[test]
    fn test_decode_proof_verification_report_detects_mismatch() {
        let oid = test_object_id(0xFFFF);
        let proof = EcsDecodeProof::from_esis(oid, 4, &[0, 1, 2, 3], true, Some(4), 90, 17)
            .with_rejected_symbols(vec![RejectedSymbol {
                esi: 7,
                reason: SymbolRejectionReason::DuplicateEsi,
            }]);

        let config = DecodeProofVerificationConfig {
            expected_schema_version: DECODE_PROOF_SCHEMA_VERSION_V1,
            expected_policy_id: DEFAULT_DECODE_PROOF_POLICY_ID + 1,
            decode_success_slack: DEFAULT_DECODE_PROOF_SLACK,
        };
        let report = proof.verification_report(config, &[], &[]);
        let issue_codes: Vec<&str> = report
            .issues
            .iter()
            .map(|issue| issue.code.as_str())
            .collect();

        assert!(!report.ok);
        assert!(!report.policy_id_ok);
        assert!(!report.decode_success_budget_ok);
        assert!(!report.replay_verifies);
        assert!(!report.rejected_reasons_hash_or_auth_only);
        assert!(
            issue_codes.contains(&"policy_id_mismatch"),
            "expected policy mismatch in {issue_codes:?}"
        );
        assert!(
            issue_codes.contains(&"decode_success_budget_failed"),
            "expected decode budget mismatch in {issue_codes:?}"
        );
        assert!(
            issue_codes.contains(&"replay_verification_failed"),
            "expected replay verification mismatch in {issue_codes:?}"
        );
        assert!(
            issue_codes.contains(&"rejected_reason_unsupported"),
            "expected rejected reason mismatch in {issue_codes:?}"
        );
    }

    // -- bd-221l proof stability + replay-verifier hardening tests --

    #[test]
    fn test_decode_proof_serialized_stability_fixed_inputs() {
        let oid = test_object_id(0x2210);
        let symbol_digests = vec![
            SymbolDigest {
                esi: 0,
                digest_xxh3: 0xAA01,
            },
            SymbolDigest {
                esi: 1,
                digest_xxh3: 0xAA02,
            },
            SymbolDigest {
                esi: 2,
                digest_xxh3: 0xAA03,
            },
        ];
        let rejected = vec![RejectedSymbol {
            esi: 6,
            reason: SymbolRejectionReason::HashMismatch,
        }];

        let proof_a = EcsDecodeProof::from_esis(oid, 4, &[0, 1, 2, 3, 4, 5], true, Some(4), 88, 55)
            .with_symbol_digests(symbol_digests.clone())
            .with_rejected_symbols(rejected.clone());
        let proof_b = EcsDecodeProof::from_esis(oid, 4, &[0, 1, 2, 3, 4, 5], true, Some(4), 88, 55)
            .with_symbol_digests(symbol_digests)
            .with_rejected_symbols(rejected);

        let json_a = stable_proof_bytes_for_test(&proof_a);
        let json_b = stable_proof_bytes_for_test(&proof_b);
        assert_eq!(
            json_a, json_b,
            "fixed inputs must produce byte-identical serialized proof artifacts"
        );
        assert!(
            proof_a.debug_symbol_payloads.is_none(),
            "default proof must not embed raw symbol payload bytes"
        );
    }

    #[test]
    fn test_decode_proof_verifier_rejects_altered_esi_list() {
        let oid = test_object_id(0x2211);
        let symbol_digests = vec![SymbolDigest {
            esi: 0,
            digest_xxh3: 0x10,
        }];
        let rejected = vec![RejectedSymbol {
            esi: 9,
            reason: SymbolRejectionReason::InvalidAuthTag,
        }];
        let mut tampered = EcsDecodeProof::from_esis(oid, 2, &[0, 1, 2], true, Some(2), 13, 99)
            .with_symbol_digests(symbol_digests.clone())
            .with_rejected_symbols(rejected.clone());
        tampered.symbols_received.push(99);
        let report = tampered.verification_report(
            DecodeProofVerificationConfig::default(),
            &symbol_digests,
            &rejected,
        );

        let issue_codes: Vec<&str> = report
            .issues
            .iter()
            .map(|issue| issue.code.as_str())
            .collect();
        assert!(!report.ok, "tampered ESI list must fail verification");
        assert!(!report.internal_consistency_ok);
        assert!(
            issue_codes.contains(&"internal_consistency_failed"),
            "expected consistency failure in {issue_codes:?}"
        );
    }

    #[test]
    fn test_decode_proof_verifier_rejects_altered_hashes() {
        let oid = test_object_id(0x2212);
        let symbol_digests = vec![SymbolDigest {
            esi: 0,
            digest_xxh3: 0x20,
        }];
        let rejected = vec![RejectedSymbol {
            esi: 7,
            reason: SymbolRejectionReason::HashMismatch,
        }];
        let mut tampered = EcsDecodeProof::from_esis(oid, 2, &[0, 1, 2], false, Some(2), 21, 123)
            .with_symbol_digests(symbol_digests.clone())
            .with_rejected_symbols(rejected.clone());
        tampered.input_hashes.metadata_xxh3 ^= 1;
        let report = tampered.verification_report(
            DecodeProofVerificationConfig::default(),
            &symbol_digests,
            &rejected,
        );

        let issue_codes: Vec<&str> = report
            .issues
            .iter()
            .map(|issue| issue.code.as_str())
            .collect();
        assert!(!report.ok, "tampered hash evidence must fail verification");
        assert!(!report.metadata_hash_ok);
        assert!(
            issue_codes.contains(&"hash_mismatch"),
            "expected hash mismatch issue in {issue_codes:?}"
        );
    }

    #[test]
    fn test_decode_proof_verifier_rejects_wrong_schema_version() {
        let oid = test_object_id(0x2213);
        let symbol_digests = vec![SymbolDigest {
            esi: 0,
            digest_xxh3: 0x30,
        }];
        let rejected = vec![RejectedSymbol {
            esi: 8,
            reason: SymbolRejectionReason::InvalidAuthTag,
        }];
        let mut tampered = EcsDecodeProof::from_esis(oid, 2, &[0, 1, 2], false, Some(2), 34, 77)
            .with_symbol_digests(symbol_digests.clone())
            .with_rejected_symbols(rejected.clone());
        tampered.schema_version = DECODE_PROOF_SCHEMA_VERSION_V1 + 1;
        let report = tampered.verification_report(
            DecodeProofVerificationConfig::default(),
            &symbol_digests,
            &rejected,
        );

        let issue_codes: Vec<&str> = report
            .issues
            .iter()
            .map(|issue| issue.code.as_str())
            .collect();
        assert!(!report.ok);
        assert!(!report.schema_version_ok);
        assert!(
            issue_codes.contains(&"schema_version_mismatch"),
            "expected schema mismatch issue in {issue_codes:?}"
        );
    }

    #[test]
    fn test_decode_proof_hashes_only_artifact_is_compact() {
        let oid = test_object_id(0x2214);
        let proof =
            EcsDecodeProof::from_esis(oid, 8, &[0, 1, 2, 3, 4, 8, 9], true, Some(8), 55, 100);
        let serialized = stable_proof_bytes_for_test(&proof);

        assert_eq!(proof.payload_mode, DecodeProofPayloadMode::HashesOnly);
        assert!(
            proof.debug_symbol_payloads.is_none(),
            "hashes-only mode must not include raw symbol payloads"
        );
        assert!(
            serialized.len() < 1024,
            "proof artifact unexpectedly large: {} bytes",
            serialized.len()
        );
    }
}
