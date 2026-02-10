//! Compliance coverage for `bd-9nbw`: RaptorQ tests + bounded performance checks.
//!
//! Scope:
//! - Symbol loss up to `R` succeeds.
//! - Symbol loss beyond `R` fails with explainable decode proof.
//! - Bit flips are detected via integrity checks.
//! - Corrupted symbols can be treated as erasures and repaired via FEC.
//! - Systematic path remains cheap and decode path remains bounded.

use std::time::Instant;

use asupersync::raptorq::decoder::{InactivationDecoder, ReceivedSymbol};
use asupersync::raptorq::systematic::SystematicEncoder;
use fsqlite_core::decode_proofs::EcsDecodeProof;
use fsqlite_types::{
    ObjectId, Oti, SymbolRecord, SymbolRecordFlags, reconstruct_systematic_happy_path,
};
use tracing::{debug, error, info, warn};
use xxhash_rust::xxh3::xxh3_128;

const BEAD_ID: &str = "bd-9nbw";
const SEED_LOSS: u64 = 0x9B0B_0001;
const SEED_REPAIR: u64 = 0x9B0B_5EED;
const SEED_PROOF: u64 = 0x9B0B_F00F;
const SEED_PERF: u64 = 0x9B0B_CAFE;
const SEED_E2E: u64 = 0x9B0B_E2E0;

#[derive(Debug)]
struct DecodeFailureExplanation {
    missing_symbols: usize,
    k_required: usize,
    symbols_received: usize,
    proof: EcsDecodeProof,
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
                    u8::try_from(value).expect("value is < 256")
                })
                .collect()
        })
        .collect()
}

fn source_symbol_count(records: &[ReceivedSymbol]) -> usize {
    records.iter().filter(|symbol| symbol.is_source).count()
}

fn append_repair_symbols(
    decoder: &InactivationDecoder,
    encoder: &SystematicEncoder,
    received: &mut Vec<ReceivedSymbol>,
    start_esi: u32,
    count: usize,
) {
    for offset in 0..count {
        let esi = start_esi + u32::try_from(offset).expect("offset must fit u32");
        let (columns, coefficients) = decoder.repair_equation(esi);
        let repair_data = encoder.repair_symbol(esi);
        received.push(ReceivedSymbol::repair(
            esi,
            columns,
            coefficients,
            repair_data,
        ));
    }
}

fn decode_with_explanation(
    decoder: &InactivationDecoder,
    payload_esis: &[u32],
    received: &[ReceivedSymbol],
    object_id: ObjectId,
    seed: u64,
) -> Result<Vec<Vec<u8>>, Box<DecodeFailureExplanation>> {
    if let Ok(result) = decoder.decode(received) {
        Ok(result.source)
    } else {
        let k_required = decoder.params().k;
        let symbols_received = payload_esis.len();
        let missing_symbols = k_required.saturating_sub(symbols_received);
        let proof = EcsDecodeProof::from_esis(
            object_id,
            u32::try_from(k_required).expect("K must fit u32"),
            payload_esis,
            false,
            Some(u32::try_from(symbols_received).expect("received count must fit u32")),
            0,
            seed,
        );
        Err(Box::new(DecodeFailureExplanation {
            missing_symbols,
            k_required,
            symbols_received,
            proof,
        }))
    }
}

fn make_systematic_symbol_records(
    source_symbols: &[Vec<u8>],
    object_id: ObjectId,
) -> Vec<SymbolRecord> {
    let k = source_symbols.len();
    let symbol_size = source_symbols
        .first()
        .map(Vec::len)
        .expect("source symbols must be non-empty");
    let transfer_length = k
        .checked_mul(symbol_size)
        .expect("transfer length must not overflow");
    let oti = Oti {
        f: u64::try_from(transfer_length).expect("transfer length must fit u64"),
        al: 1,
        t: u32::try_from(symbol_size).expect("symbol_size must fit u32"),
        z: 1,
        n: 1,
    };

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
                u32::try_from(esi).expect("ESI must fit u32"),
                symbol.clone(),
                flags,
            )
        })
        .collect()
}

#[test]
#[allow(non_snake_case)]
fn test_raptorq_symbol_loss_within_R() {
    let k = 24usize;
    let symbol_size = 64usize;
    let seed = SEED_LOSS;
    let r_budget = 3usize;
    let source = make_source_symbols(k, symbol_size);

    let encoder =
        SystematicEncoder::new(&source, symbol_size, seed).expect("encoder must be constructible");
    let decoder = InactivationDecoder::new(k, symbol_size, seed);

    let dropped = [2_u32, 7_u32, 19_u32];
    assert_eq!(dropped.len(), r_budget);
    let repair_count = dropped.len() + decoder.params().s + decoder.params().h;
    let mut received = decoder.constraint_symbols();
    for (esi, symbol) in source.iter().enumerate() {
        let esi_u32 = u32::try_from(esi).expect("ESI must fit u32");
        if !dropped.contains(&esi_u32) {
            received.push(ReceivedSymbol::source(esi_u32, symbol.clone()));
        }
    }
    append_repair_symbols(
        &decoder,
        &encoder,
        &mut received,
        u32::try_from(k).expect("K must fit u32"),
        repair_count,
    );

    let result = decoder
        .decode(&received)
        .expect("decode should succeed with losses <= R");
    for (idx, expected) in source.iter().enumerate() {
        assert_eq!(
            result.source[idx], *expected,
            "bead_id={BEAD_ID} case=loss_within_r idx={idx}"
        );
    }
}

#[test]
#[allow(non_snake_case)]
fn test_raptorq_symbol_loss_beyond_R() {
    let k = 24usize;
    let symbol_size = 64usize;
    let seed = SEED_LOSS;
    let r_budget = 3usize;
    let source = make_source_symbols(k, symbol_size);
    let object_id = ObjectId::derive_from_canonical_bytes(b"bd-9nbw-loss-beyond-r");

    let encoder =
        SystematicEncoder::new(&source, symbol_size, seed).expect("encoder must be constructible");
    let decoder = InactivationDecoder::new(k, symbol_size, seed);

    let dropped = [1_u32, 4_u32, 7_u32, 11_u32]; // R+1 losses
    let mut received = decoder.constraint_symbols();
    let mut payload_esis = Vec::new();
    for (esi, symbol) in source.iter().enumerate() {
        let esi_u32 = u32::try_from(esi).expect("ESI must fit u32");
        if !dropped.contains(&esi_u32) {
            payload_esis.push(esi_u32);
            received.push(ReceivedSymbol::source(esi_u32, symbol.clone()));
        }
    }
    append_repair_symbols(
        &decoder,
        &encoder,
        &mut received,
        u32::try_from(k).expect("K must fit u32"),
        r_budget,
    );
    payload_esis
        .extend((0..r_budget).map(|offset| u32::try_from(k + offset).expect("ESI must fit u32")));

    let explanation = decode_with_explanation(&decoder, &payload_esis, &received, object_id, seed)
        .expect_err("decode should fail with losses > R");

    assert_eq!(explanation.k_required, k);
    assert_eq!(explanation.symbols_received, k - dropped.len() + r_budget);
    assert_eq!(explanation.missing_symbols, 1);
    assert!(
        !explanation.proof.decode_success && explanation.proof.is_consistent(),
        "bead_id={BEAD_ID} case=loss_beyond_r_proof_consistency"
    );
}

#[test]
fn test_raptorq_bitflip_detected() {
    let object_id = ObjectId::derive_from_canonical_bytes(b"bd-9nbw-bitflip-detected");
    let symbol_data = vec![0xAB; 256];
    let oti = Oti {
        f: 256,
        al: 1,
        t: 256,
        z: 1,
        n: 1,
    };
    let record = SymbolRecord::new(
        object_id,
        oti,
        0,
        symbol_data,
        SymbolRecordFlags::SYSTEMATIC_RUN_START,
    );
    let mut bytes = record.to_bytes();
    let flip_index = bytes.len() / 2;
    bytes[flip_index] ^= 0x40;

    let parsed = SymbolRecord::from_bytes(&bytes);
    assert!(
        parsed.is_err(),
        "bead_id={BEAD_ID} case=bitflip_detected expected parse failure"
    );
}

#[test]
fn test_raptorq_bitflip_repair() {
    let k = 18usize;
    let symbol_size = 64usize;
    let seed = SEED_REPAIR;
    let corrupt_esi = 5usize;
    let source = make_source_symbols(k, symbol_size);
    let expected_hashes: Vec<u128> = source.iter().map(|symbol| xxh3_128(symbol)).collect();

    let encoder =
        SystematicEncoder::new(&source, symbol_size, seed).expect("encoder must be constructible");
    let decoder = InactivationDecoder::new(k, symbol_size, seed);

    let mut received = decoder.constraint_symbols();
    for (esi, symbol) in source.iter().enumerate() {
        let mut candidate = symbol.clone();
        if esi == corrupt_esi {
            candidate[0] ^= 0xFF;
        }
        let observed_hash = xxh3_128(&candidate);
        if observed_hash == expected_hashes[esi] {
            received.push(ReceivedSymbol::source(
                u32::try_from(esi).expect("ESI must fit u32"),
                candidate,
            ));
        } else {
            warn!(
                bead_id = BEAD_ID,
                esi, "bit-flipped source symbol rejected and treated as erasure"
            );
        }
    }
    append_repair_symbols(
        &decoder,
        &encoder,
        &mut received,
        u32::try_from(k).expect("K must fit u32"),
        1,
    );

    let result = decoder
        .decode(&received)
        .expect("decode should recover from one detected corruption via repair symbol");
    for (idx, expected) in source.iter().enumerate() {
        assert_eq!(
            result.source[idx], *expected,
            "bead_id={BEAD_ID} case=bitflip_repair idx={idx}"
        );
    }
}

#[test]
fn test_raptorq_decode_proof() {
    let k = 12usize;
    let symbol_size = 64usize;
    let seed = SEED_PROOF;
    let source = make_source_symbols(k, symbol_size);
    let object_id = ObjectId::derive_from_canonical_bytes(b"bd-9nbw-decode-proof");
    let decoder = InactivationDecoder::new(k, symbol_size, seed);

    let mut received = decoder.constraint_symbols();
    let payload_esis: Vec<u32> = (0..(k - 3))
        .map(|esi| u32::try_from(esi).expect("ESI must fit u32"))
        .collect();
    for &esi in &payload_esis {
        received.push(ReceivedSymbol::source(
            esi,
            source[usize::try_from(esi).expect("ESI must fit usize")].clone(),
        ));
    }

    let explanation = decode_with_explanation(&decoder, &payload_esis, &received, object_id, seed)
        .expect_err("decode must fail when not enough payload equations are present");

    assert_eq!(explanation.missing_symbols, 3);
    assert_eq!(explanation.k_required, k);
    assert_eq!(explanation.symbols_received, k - 3);
    assert!(explanation.proof.is_consistent());
    assert!(!explanation.proof.decode_success);
}

#[test]
fn test_raptorq_performance_systematic_vs_decode_bounded() {
    let k = 64usize;
    let symbol_size = 512usize;
    let seed = SEED_PERF;
    let source = make_source_symbols(k, symbol_size);
    let object_id = ObjectId::derive_from_canonical_bytes(b"bd-9nbw-performance");

    let records = make_systematic_symbol_records(&source, object_id);
    let decoder = InactivationDecoder::new(k, symbol_size, seed);
    let encoder =
        SystematicEncoder::new(&source, symbol_size, seed).expect("encoder must be constructible");

    let systematic_runs = 60usize;
    let systematic_start = Instant::now();
    for _ in 0..systematic_runs {
        let reconstructed =
            reconstruct_systematic_happy_path(&records).expect("systematic fast path must succeed");
        assert_eq!(
            reconstructed.len(),
            k * symbol_size,
            "bead_id={BEAD_ID} case=systematic_length"
        );
    }
    let systematic_elapsed = systematic_start.elapsed();
    let avg_systematic_ns =
        systematic_elapsed.as_nanos() / u128::try_from(systematic_runs).expect("runs fit u128");

    let decode_runs = 20usize;
    let dropped = 4usize;
    let repair_count = dropped + decoder.params().s + decoder.params().h;
    let decode_start = Instant::now();
    for _ in 0..decode_runs {
        let mut received = decoder.constraint_symbols();
        for (esi, symbol) in source.iter().enumerate().skip(dropped) {
            received.push(ReceivedSymbol::source(
                u32::try_from(esi).expect("ESI must fit u32"),
                symbol.clone(),
            ));
        }
        append_repair_symbols(
            &decoder,
            &encoder,
            &mut received,
            u32::try_from(k).expect("K must fit u32"),
            repair_count,
        );
        let decode_result = decoder.decode(&received).expect("decode path must succeed");
        assert_eq!(decode_result.source.len(), k);
    }
    let decode_elapsed = decode_start.elapsed();
    let avg_decode_ns =
        decode_elapsed.as_nanos() / u128::try_from(decode_runs).expect("runs fit u128");

    let avg_systematic_ns_u64 = u64::try_from(avg_systematic_ns).unwrap_or(u64::MAX);
    let avg_decode_ns_u64 = u64::try_from(avg_decode_ns).unwrap_or(u64::MAX);
    info!(
        bead_id = BEAD_ID,
        avg_systematic_ns = avg_systematic_ns_u64,
        avg_decode_ns = avg_decode_ns_u64,
        "raptorq perf summary"
    );

    let decode_bound_ns =
        avg_systematic_ns.saturating_mul(250) + std::time::Duration::from_millis(50).as_nanos();
    assert!(
        avg_decode_ns <= decode_bound_ns,
        "bead_id={BEAD_ID} case=decode_bounded avg_decode_ns={avg_decode_ns} bound={decode_bound_ns}"
    );
}

#[test]
fn test_e2e_raptorq_harness() {
    let k = 32usize;
    let symbol_size = 256usize;
    let seed = SEED_E2E;
    let source = make_source_symbols(k, symbol_size);
    let object_id = ObjectId::derive_from_canonical_bytes(b"bd-9nbw-e2e");
    let decoder = InactivationDecoder::new(k, symbol_size, seed);
    let encoder =
        SystematicEncoder::new(&source, symbol_size, seed).expect("encoder must be constructible");
    let erasure_count = 3usize;
    let repair_count = erasure_count + decoder.params().s + decoder.params().h;

    let mut received = decoder.constraint_symbols();
    let mut payload_esis = Vec::new();
    for (esi, symbol) in source.iter().enumerate() {
        let esi_u32 = u32::try_from(esi).expect("ESI must fit u32");
        let mut candidate = symbol.clone();
        if esi == 3 {
            candidate[0] ^= 0xFF;
        }
        if esi == 9 || esi == 17 {
            continue;
        }
        if xxh3_128(&candidate) == xxh3_128(symbol) {
            payload_esis.push(esi_u32);
            received.push(ReceivedSymbol::source(esi_u32, candidate));
        } else {
            warn!(
                bead_id = BEAD_ID,
                esi, "detected corruption and dropped source symbol"
            );
        }
    }
    append_repair_symbols(
        &decoder,
        &encoder,
        &mut received,
        u32::try_from(k).expect("K must fit u32"),
        repair_count,
    );
    payload_esis.extend(
        (0..repair_count).map(|offset| u32::try_from(k + offset).expect("ESI must fit u32")),
    );

    let decode_started = Instant::now();
    let recovered = decode_with_explanation(&decoder, &payload_esis, &received, object_id, seed)
        .expect("e2e decode should succeed");
    let decode_elapsed_ns = decode_started.elapsed().as_nanos();
    let decode_elapsed_ns_u64 = u64::try_from(decode_elapsed_ns).unwrap_or(u64::MAX);
    debug!(
        bead_id = BEAD_ID,
        decoded_symbol_count = recovered.len(),
        decode_elapsed_ns = decode_elapsed_ns_u64,
        "e2e decode completed"
    );

    let proof = EcsDecodeProof::from_esis(
        object_id,
        u32::try_from(k).expect("K must fit u32"),
        &payload_esis,
        true,
        Some(u32::try_from(source_symbol_count(&received)).expect("count must fit u32")),
        u64::try_from(decode_elapsed_ns).unwrap_or(u64::MAX),
        seed,
    );
    assert!(proof.is_consistent());
    assert!(proof.is_repair());
    assert!(proof.decode_success);

    for (idx, expected) in source.iter().enumerate() {
        assert_eq!(
            recovered[idx], *expected,
            "bead_id={BEAD_ID} case=e2e idx={idx} payload mismatch"
        );
    }

    let records = make_systematic_symbol_records(&source, object_id);
    let systematic_bytes = reconstruct_systematic_happy_path(&records)
        .expect("systematic reconstruction must succeed");
    assert_eq!(systematic_bytes.len(), k * symbol_size);

    // Ensure the bitflip-detection mechanism remains active in the integrated path.
    let mut serialized = records[0].to_bytes();
    let midpoint = serialized.len() / 2;
    serialized[midpoint] ^= 0x01;
    if SymbolRecord::from_bytes(&serialized).is_ok() {
        error!(
            bead_id = BEAD_ID,
            "bit-flipped symbol record unexpectedly validated"
        );
        panic!("bead_id={BEAD_ID} e2e integrity check failed");
    }
}
