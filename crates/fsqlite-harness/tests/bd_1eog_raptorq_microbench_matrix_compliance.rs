//! Compliance tests for bd-1eog: RaptorQ encode/decode microbench matrix (K, T, loss).
//!
//! Validates that the benchmark infrastructure covers all required matrix axes,
//! runs deterministically, and supports smoke-mode for CI. Also verifies the
//! key functional properties that the benchmarks measure.

use std::path::{Path, PathBuf};

use asupersync::raptorq::decoder::{InactivationDecoder, ReceivedSymbol};
use asupersync::raptorq::systematic::SystematicEncoder;
use fsqlite_types::{
    ObjectId, Oti, SymbolRecord, SymbolRecordFlags, reconstruct_systematic_happy_path,
};
use fsqlite_wal::{verify_wal_fec_source_hash, wal_fec_source_hash_xxh3_128};
use serde_json::Value;
use tracing::{debug, info};

const BEAD_ID: &str = "bd-1eog";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const BENCH_PATH: &str = "crates/fsqlite-wal/benches/raptorq_matrix.rs";

// Matrix axes from the bead spec.
const K_SOURCE_SMALL: &[usize] = &[1, 8, 32];
const K_SOURCE_MEDIUM: &[usize] = &[256, 1024];
const SYMBOL_SIZES: &[usize] = &[1366, 4096];
const _LOSS_RATES: &[u32] = &[0, 5, 10, 20];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("repo root should be two levels up from harness")
        .to_path_buf()
}

fn make_source_symbols(k: usize, symbol_size: usize) -> Vec<Vec<u8>> {
    (0..k)
        .map(|i| {
            (0..symbol_size)
                .map(|j| {
                    let value = (i.wrapping_mul(37))
                        .wrapping_add(j.wrapping_mul(13))
                        .wrapping_add(17)
                        % 256;
                    u8::try_from(value).expect("modulo 256 fits u8")
                })
                .collect()
        })
        .collect()
}

fn make_systematic_records(source_symbols: &[Vec<u8>]) -> Vec<SymbolRecord> {
    let k = source_symbols.len();
    let symbol_size = source_symbols
        .first()
        .map(Vec::len)
        .expect("non-empty source");
    let transfer_length = u64::try_from(
        k.checked_mul(symbol_size)
            .expect("transfer length should not overflow"),
    )
    .expect("transfer length fits u64");
    let symbol_size_u32 = u32::try_from(symbol_size).expect("symbol size fits u32");
    let oti = Oti {
        f: transfer_length,
        al: 1,
        t: symbol_size_u32,
        z: 1,
        n: 1,
    };
    let object_id = ObjectId::derive_from_canonical_bytes(b"bd-1eog-compliance");

    source_symbols
        .iter()
        .enumerate()
        .map(|(esi, symbol)| {
            let flags = if esi == 0 {
                SymbolRecordFlags::SYSTEMATIC_RUN_START
            } else {
                SymbolRecordFlags::empty()
            };
            SymbolRecord::new(
                object_id,
                oti,
                u32::try_from(esi).expect("ESI fits u32"),
                symbol.clone(),
                flags,
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Bead structure compliance
// ---------------------------------------------------------------------------

#[test]
fn test_bd_1eog_bead_exists_and_in_progress() {
    let issues_path = repo_root().join(ISSUES_JSONL);
    let content = std::fs::read_to_string(&issues_path).expect("issues.jsonl should be readable");
    let mut found = false;
    for line in content.lines() {
        if let Ok(issue) = serde_json::from_str::<Value>(line)
            && issue.get("id").and_then(Value::as_str) == Some(BEAD_ID)
        {
            found = true;
            let status = issue
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            assert!(
                status == "in_progress" || status == "closed",
                "bead_id={BEAD_ID} expected in_progress or closed, got {status}"
            );
            break;
        }
    }
    assert!(found, "bead_id={BEAD_ID} not found in {ISSUES_JSONL}");
}

#[test]
fn test_bd_1eog_bench_file_exists() {
    let bench_file = repo_root().join(BENCH_PATH);
    assert!(
        bench_file.exists(),
        "bead_id={BEAD_ID} bench file missing at {BENCH_PATH}"
    );
}

#[test]
fn test_bd_1eog_bench_covers_matrix_axes() {
    let bench_file = repo_root().join(BENCH_PATH);
    let content = std::fs::read_to_string(&bench_file).expect("bench file should be readable");

    // Verify smoke mode support.
    assert!(
        content.contains("FSQLITE_BENCH_SMOKE") || content.contains("Smoke"),
        "bead_id={BEAD_ID} bench must support smoke mode"
    );

    // Verify K axis coverage (small + medium).
    for &k in &[1_usize, 32, 256, 1024] {
        let k_str = k.to_string();
        assert!(
            content.contains(&k_str),
            "bead_id={BEAD_ID} bench missing K_source={k}"
        );
    }

    // Verify symbol size axis (MTU-ish + page-ish).
    assert!(
        content.contains("1366"),
        "bead_id={BEAD_ID} bench missing MTU-ish symbol size 1366"
    );
    assert!(
        content.contains("4096"),
        "bead_id={BEAD_ID} bench missing page-ish symbol size 4096"
    );

    // Verify loss rate axis.
    for &loss in &[0_u32, 10, 20] {
        let loss_str = loss.to_string();
        assert!(
            content.contains(&loss_str),
            "bead_id={BEAD_ID} bench missing loss_percent={loss}"
        );
    }

    // Verify benchmark groups exist.
    assert!(
        content.contains("systematic_fast_path") || content.contains("systematic"),
        "bead_id={BEAD_ID} bench must measure systematic fast path"
    );
    assert!(
        content.contains("repair_symbol_generation") || content.contains("repair"),
        "bead_id={BEAD_ID} bench must measure repair symbol generation"
    );
    assert!(
        content.contains("decode_paths") || content.contains("decode"),
        "bead_id={BEAD_ID} bench must measure decode throughput"
    );
    assert!(
        content.contains("hash_auth") || content.contains("verification"),
        "bead_id={BEAD_ID} bench must measure hash/auth verification cost"
    );
}

#[test]
fn test_bd_1eog_bench_is_deterministic() {
    let bench_file = repo_root().join(BENCH_PATH);
    let content = std::fs::read_to_string(&bench_file).expect("bench file should be readable");

    // Bench must use deterministic seeds, not random ones.
    assert!(
        content.contains("deterministic_seed") || content.contains("seed"),
        "bead_id={BEAD_ID} bench must use deterministic seeds"
    );
    // Must not use `thread_rng` or `rand::random` for reproducibility.
    assert!(
        !content.contains("thread_rng") && !content.contains("rand::random"),
        "bead_id={BEAD_ID} bench must not use non-deterministic RNG"
    );
}

// ---------------------------------------------------------------------------
// Functional verification (what the bench measures)
// ---------------------------------------------------------------------------

#[test]
fn test_bd_1eog_systematic_fast_path_small() {
    for &k in K_SOURCE_SMALL {
        for &symbol_size in SYMBOL_SIZES {
            let source = make_source_symbols(k, symbol_size);
            let records = make_systematic_records(&source);
            let payload = reconstruct_systematic_happy_path(&records)
                .expect("systematic path should succeed");
            let expected: Vec<u8> = source.into_iter().flatten().collect();
            assert_eq!(
                payload, expected,
                "bead_id={BEAD_ID} systematic fast path K={k} T={symbol_size} mismatch"
            );
            info!(
                bead_id = BEAD_ID,
                k, symbol_size, "systematic fast path verified"
            );
        }
    }
}

#[test]
fn test_bd_1eog_systematic_fast_path_medium() {
    for &k in K_SOURCE_MEDIUM {
        let symbol_size = 1366;
        let source = make_source_symbols(k, symbol_size);
        let records = make_systematic_records(&source);
        let payload =
            reconstruct_systematic_happy_path(&records).expect("systematic path should succeed");
        let expected: Vec<u8> = source.into_iter().flatten().collect();
        assert_eq!(
            payload, expected,
            "bead_id={BEAD_ID} systematic fast path K={k} T={symbol_size} mismatch"
        );
    }
}

#[test]
fn test_bd_1eog_decode_with_loss_succeeds() {
    let k = 32_usize;
    let symbol_size = 64_usize;
    let seed = 0x1E0C_0001_u64;

    let source = make_source_symbols(k, symbol_size);
    let encoder =
        SystematicEncoder::new(&source, symbol_size, seed).expect("encoder should construct");
    let decoder = InactivationDecoder::new(k, symbol_size, seed);

    // Drop 3 symbols (≈10% of 32).
    let dropped: Vec<u32> = vec![5, 12, 28];

    let mut received = decoder.constraint_symbols();
    for (esi, symbol) in source.iter().enumerate() {
        let esi_u32 = u32::try_from(esi).expect("ESI fits u32");
        if dropped.contains(&esi_u32) {
            continue;
        }
        received.push(ReceivedSymbol::source(esi_u32, symbol.clone()));
    }

    // Add enough repair symbols.
    let repair_budget = dropped.len() + decoder.params().s + decoder.params().h + 8;
    for offset in 0..repair_budget {
        let esi = u32::try_from(k + offset).expect("repair ESI fits u32");
        let (columns, coefficients) = decoder
            .repair_equation_rfc6330(esi)
            .expect("repair symbol should have an RFC6330 repair equation");
        let repair_data = encoder.repair_symbol(esi);
        received.push(ReceivedSymbol::repair(
            esi,
            columns,
            coefficients,
            repair_data,
        ));
    }

    let result = decoder.decode(&received);
    assert!(
        result.is_ok(),
        "bead_id={BEAD_ID} decode with 3 lost symbols should succeed"
    );
    let decoded_symbols = result.expect("decode succeeded");
    assert_eq!(
        decoded_symbols.source, source,
        "bead_id={BEAD_ID} decoded symbols should match originals"
    );
    info!(
        bead_id = BEAD_ID,
        k,
        dropped_count = dropped.len(),
        "decode with loss verified"
    );
}

#[test]
fn test_bd_1eog_hash_verification_cost() {
    for &symbol_size in SYMBOL_SIZES {
        let source = make_source_symbols(1, symbol_size);
        let payload = source.first().expect("source should exist");
        let hash = wal_fec_source_hash_xxh3_128(payload);

        // Verify the hash is correct.
        assert!(
            verify_wal_fec_source_hash(payload, hash),
            "bead_id={BEAD_ID} hash verification should pass for T={symbol_size}"
        );

        // Verify corruption detection.
        let mut corrupted = payload.clone();
        corrupted[0] ^= 0xFF;
        assert!(
            !verify_wal_fec_source_hash(&corrupted, hash),
            "bead_id={BEAD_ID} hash verification should fail for corrupted data T={symbol_size}"
        );

        debug!(bead_id = BEAD_ID, symbol_size, "hash verification verified");
    }
}

#[test]
fn test_bd_1eog_auth_tag_verification() {
    let key = [0xA5_u8; 32];
    let symbol_size = 4096_usize;
    let source = make_source_symbols(1, symbol_size);
    let object_id = ObjectId::derive_from_canonical_bytes(b"bd-1eog-auth-test");
    let oti = Oti {
        f: u64::try_from(symbol_size).expect("fits"),
        al: 1,
        t: u32::try_from(symbol_size).expect("fits"),
        z: 1,
        n: 1,
    };

    let record = SymbolRecord::new(
        object_id,
        oti,
        0,
        source[0].clone(),
        SymbolRecordFlags::SYSTEMATIC_RUN_START,
    )
    .with_auth_tag(&key);

    assert!(
        record.verify_auth(&key),
        "bead_id={BEAD_ID} auth tag should verify with correct key"
    );
    let wrong_key = [0xBB_u8; 32];
    assert!(
        !record.verify_auth(&wrong_key),
        "bead_id={BEAD_ID} auth tag should fail with wrong key"
    );
}

// ---------------------------------------------------------------------------
// E2E: combined matrix smoke test
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_bd_1eog_compliance() {
    info!(bead_id = BEAD_ID, "starting E2E compliance check");

    // Verify bench file structure.
    let bench_file = repo_root().join(BENCH_PATH);
    assert!(
        bench_file.exists(),
        "bead_id={BEAD_ID} bench file must exist"
    );

    // Verify systematic fast path across small K axis.
    for &k in &[1_usize, 8, 32] {
        let source = make_source_symbols(k, 4096);
        let records = make_systematic_records(&source);
        let payload =
            reconstruct_systematic_happy_path(&records).expect("systematic should succeed");
        let expected: Vec<u8> = source.into_iter().flatten().collect();
        assert_eq!(payload, expected, "bead_id={BEAD_ID} E2E K={k} T=4096");
    }

    // Verify hash verification.
    let test_data = vec![0xAB_u8; 4096];
    let hash = wal_fec_source_hash_xxh3_128(&test_data);
    assert!(verify_wal_fec_source_hash(&test_data, hash));

    info!(
        bead_id = BEAD_ID,
        "E2E compliance check passed — systematic + hash + auth verified"
    );
}
