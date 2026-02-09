use std::fs;
use std::path::Path;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{ObjectId, Oti};
use fsqlite_wal::{
    WalFecGroupMeta, WalFecGroupMetaInit, WalFecGroupRecord, WalFecRecoveryFallbackReason,
    WalFecRecoveryOutcome, WalFrameCandidate, WalSalts, append_wal_fec_group,
    build_source_page_hashes, generate_wal_fec_repair_symbols, identify_damaged_commit_group,
    recover_wal_fec_group_with_decoder, scan_wal_fec,
};
use tempfile::tempdir;

const PAGE_SIZE: u32 = 4096;
const BEAD_ID: &str = "bd-1hi.11";

#[derive(Clone)]
struct GroupFixture {
    meta: WalFecGroupMeta,
    source_pages: Vec<Vec<u8>>,
    record: WalFecGroupRecord,
}

fn sample_payload(seed: u8) -> Vec<u8> {
    let page_len = usize::try_from(PAGE_SIZE).expect("PAGE_SIZE fits in usize");
    let mut payload = vec![0_u8; page_len];
    for (index, byte) in payload.iter_mut().enumerate() {
        let index_mod = u8::try_from(index % 251).expect("modulo result fits u8");
        *byte = seed.wrapping_add(index_mod.rotate_left(1));
    }
    payload
}

fn sample_source_pages(k_source: u32, seed_base: u8) -> Vec<Vec<u8>> {
    (0..k_source)
        .map(|index| {
            let seed = seed_base.wrapping_add(u8::try_from(index).expect("small index fits u8"));
            sample_payload(seed)
        })
        .collect()
}

fn build_fixture(
    start_frame_no: u32,
    k_source: u32,
    r_repair: u32,
    salts: WalSalts,
    object_tag: &[u8],
    seed_base: u8,
    db_size_pages: u32,
) -> GroupFixture {
    let source_pages = sample_source_pages(k_source, seed_base);
    let source_hashes = build_source_page_hashes(&source_pages);
    let page_numbers = (0..k_source).map(|offset| offset + 200).collect::<Vec<_>>();
    let object_id = ObjectId::derive_from_canonical_bytes(object_tag);
    let oti = Oti {
        f: u64::from(k_source) * u64::from(PAGE_SIZE),
        al: 1,
        t: PAGE_SIZE,
        z: 1,
        n: 1,
    };
    let meta = WalFecGroupMeta::from_init(WalFecGroupMetaInit {
        wal_salt1: salts.salt1,
        wal_salt2: salts.salt2,
        start_frame_no,
        end_frame_no: start_frame_no + (k_source - 1),
        db_size_pages,
        page_size: PAGE_SIZE,
        k_source,
        r_repair,
        oti,
        object_id,
        page_numbers,
        source_page_xxh3_128: source_hashes,
    })
    .expect("fixture metadata should be valid");
    let repair_symbols =
        generate_wal_fec_repair_symbols(&meta, &source_pages).expect("repair symbols should build");
    let record =
        WalFecGroupRecord::new(meta.clone(), repair_symbols).expect("fixture record should validate");
    GroupFixture {
        meta,
        source_pages,
        record,
    }
}

fn append_fixture(sidecar_path: &Path, fixture: &GroupFixture) {
    append_wal_fec_group(sidecar_path, &fixture.record).expect("append should succeed");
}

fn frame_candidates(fixture: &GroupFixture) -> Vec<WalFrameCandidate> {
    fixture
        .source_pages
        .iter()
        .enumerate()
        .map(|(index, page)| WalFrameCandidate {
            frame_no: fixture.meta.start_frame_no + u32::try_from(index).expect("small index"),
            page_data: page.clone(),
        })
        .collect()
}

fn corrupt_frame(candidates: &mut [WalFrameCandidate], frame_no: u32) {
    let target = candidates
        .iter_mut()
        .find(|candidate| candidate.frame_no == frame_no)
        .expect("target frame should exist");
    target.page_data[0] ^= 0x7A;
}

fn mutate_sidecar_meta_payload(sidecar_path: &Path, payload_offset: usize) {
    let mut bytes = fs::read(sidecar_path).expect("sidecar should be readable");
    let meta_len = u32::from_le_bytes(bytes[0..4].try_into().expect("meta len prefix"));
    let meta_len_usize = usize::try_from(meta_len).expect("meta len fits usize");
    let offset = 4 + payload_offset;
    assert!(offset < 4 + meta_len_usize);
    bytes[offset] ^= 0x40;
    fs::write(sidecar_path, bytes).expect("sidecar write should succeed");
}

fn corrupt_first_repair_symbol_record(sidecar_path: &Path) {
    let mut bytes = fs::read(sidecar_path).expect("sidecar should be readable");
    let meta_len = u32::from_le_bytes(bytes[0..4].try_into().expect("meta len prefix"));
    let meta_len_usize = usize::try_from(meta_len).expect("meta len fits usize");
    let repair_len_offset = 4 + meta_len_usize;
    let repair_len =
        u32::from_le_bytes(bytes[repair_len_offset..repair_len_offset + 4].try_into().expect("repair prefix"));
    let repair_len_usize = usize::try_from(repair_len).expect("repair len fits usize");
    let repair_payload_offset = repair_len_offset + 4;
    assert!(repair_payload_offset + repair_len_usize <= bytes.len());
    bytes[repair_payload_offset] ^= 0x55; // corrupt SymbolRecord magic/version bytes
    fs::write(sidecar_path, bytes).expect("sidecar write should succeed");
}

fn decoder_from_expected(expected_pages: Vec<Vec<u8>>) -> impl FnMut(&WalFecGroupMeta, &[(u32, Vec<u8>)]) -> Result<Vec<Vec<u8>>> {
    move |meta, available| {
        if available.len() < usize::try_from(meta.k_source).expect("k_source fits usize") {
            return Err(FrankenError::WalCorrupt {
                detail: "decoder invoked with insufficient symbols".to_owned(),
            });
        }
        Ok(expected_pages.clone())
    }
}

#[test]
fn test_bd_1hi_11_unit_compliance_gate() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("unit.wal-fec");
    let salts = WalSalts {
        salt1: 0xAA11_BB22,
        salt2: 0xCC33_DD44,
    };
    let fixture = build_fixture(1, 5, 2, salts, b"bd-1hi.11-unit", 9, 512);
    append_fixture(&sidecar_path, &fixture);

    let mut candidates = frame_candidates(&fixture);
    corrupt_frame(&mut candidates, fixture.meta.start_frame_no + 2);

    let outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        fixture.meta.group_id(),
        salts,
        fixture.meta.start_frame_no + 2,
        &candidates,
        decoder_from_expected(fixture.source_pages.clone()),
    )
    .expect("recovery should not error");

    let WalFecRecoveryOutcome::Recovered(recovered) = outcome else {
        panic!("expected successful recovery");
    };
    assert_eq!(recovered.db_size_pages, fixture.meta.db_size_pages);
    assert_eq!(recovered.recovered_pages, fixture.source_pages);
    assert!(
        recovered
            .decode_proof
            .recovered_frame_nos
            .contains(&(fixture.meta.start_frame_no + 2))
    );
}

#[test]
fn prop_bd_1hi_11_structure_compliance() {
    let salts = WalSalts {
        salt1: 0x1234_ABCD,
        salt2: 0x5678_EF01,
    };
    for k_source in 3..=6 {
        for r_repair in 1..=3 {
            let fixture = build_fixture(
                10,
                k_source,
                r_repair,
                salts,
                format!("prop-{k_source}-{r_repair}").as_bytes(),
                u8::try_from(k_source + r_repair).expect("small sum fits u8"),
                200,
            );
            let mut candidates = frame_candidates(&fixture);
            let frame_to_corrupt = fixture.meta.start_frame_no + (k_source / 2);
            corrupt_frame(&mut candidates, frame_to_corrupt);

            let temp_dir = tempdir().expect("tempdir should be created");
            let sidecar_path = temp_dir.path().join("prop.wal-fec");
            append_fixture(&sidecar_path, &fixture);

            let outcome = recover_wal_fec_group_with_decoder(
                &sidecar_path,
                fixture.meta.group_id(),
                salts,
                frame_to_corrupt,
                &candidates,
                decoder_from_expected(fixture.source_pages.clone()),
            )
            .expect("recovery should not error");

            match outcome {
                WalFecRecoveryOutcome::Recovered(recovered) => {
                    assert_eq!(recovered.recovered_pages, fixture.source_pages);
                }
                WalFecRecoveryOutcome::TruncateBeforeGroup { decode_proof, .. } => {
                    assert_eq!(
                        decode_proof.fallback_reason,
                        Some(WalFecRecoveryFallbackReason::InsufficientSymbols)
                    );
                }
            }
        }
    }
}

#[test]
fn test_recovery_intact_wal() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("intact.wal-fec");
    let salts = WalSalts {
        salt1: 0x1111_0001,
        salt2: 0x2222_0002,
    };
    let fixture = build_fixture(5, 5, 2, salts, b"intact", 3, 900);
    append_fixture(&sidecar_path, &fixture);
    let candidates = frame_candidates(&fixture);

    let outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        fixture.meta.group_id(),
        salts,
        fixture.meta.end_frame_no + 1,
        &candidates,
        decoder_from_expected(fixture.source_pages.clone()),
    )
    .expect("recovery should not error");

    let WalFecRecoveryOutcome::Recovered(recovered) = outcome else {
        panic!("expected fast-path recovery");
    };
    assert!(!recovered.decode_proof.decode_attempted);
    assert!(recovered.decode_proof.recovered_frame_nos.is_empty());
}

#[test]
fn test_recovery_single_and_boundary_corruption() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("boundary.wal-fec");
    let salts = WalSalts {
        salt1: 0xDEAD_BEEF,
        salt2: 0xA5A5_5A5A,
    };
    let fixture = build_fixture(1, 5, 2, salts, b"boundary", 12, 1200);
    append_fixture(&sidecar_path, &fixture);

    let mut one_corrupt = frame_candidates(&fixture);
    corrupt_frame(&mut one_corrupt, fixture.meta.start_frame_no + 1);
    let one_outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        fixture.meta.group_id(),
        salts,
        fixture.meta.start_frame_no + 1,
        &one_corrupt,
        decoder_from_expected(fixture.source_pages.clone()),
    )
    .expect("single-corruption recovery should run");
    assert!(matches!(one_outcome, WalFecRecoveryOutcome::Recovered(_)));

    let mut two_corrupt = frame_candidates(&fixture);
    corrupt_frame(&mut two_corrupt, fixture.meta.start_frame_no + 1);
    corrupt_frame(&mut two_corrupt, fixture.meta.start_frame_no + 3);
    let two_outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        fixture.meta.group_id(),
        salts,
        fixture.meta.start_frame_no + 1,
        &two_corrupt,
        decoder_from_expected(fixture.source_pages.clone()),
    )
    .expect("max-corruption recovery should run");
    assert!(matches!(two_outcome, WalFecRecoveryOutcome::Recovered(_)));
}

#[test]
fn test_recovery_exceed_corruption_falls_back() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("exceed.wal-fec");
    let salts = WalSalts {
        salt1: 0x0BAD_C0DE,
        salt2: 0xFEED_FACE,
    };
    let fixture = build_fixture(1, 5, 2, salts, b"exceed", 5, 1500);
    append_fixture(&sidecar_path, &fixture);

    let mut candidates = frame_candidates(&fixture);
    corrupt_frame(&mut candidates, fixture.meta.start_frame_no);
    corrupt_frame(&mut candidates, fixture.meta.start_frame_no + 1);
    corrupt_frame(&mut candidates, fixture.meta.start_frame_no + 2);

    let outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        fixture.meta.group_id(),
        salts,
        fixture.meta.start_frame_no,
        &candidates,
        decoder_from_expected(fixture.source_pages.clone()),
    )
    .expect("recovery should not hard-error");

    let WalFecRecoveryOutcome::TruncateBeforeGroup {
        truncate_before_frame_no,
        decode_proof,
    } = outcome
    else {
        panic!("expected truncate fallback when corruption exceeds R");
    };
    assert_eq!(truncate_before_frame_no, fixture.meta.start_frame_no);
    assert_eq!(
        decode_proof.fallback_reason,
        Some(WalFecRecoveryFallbackReason::InsufficientSymbols)
    );
}

#[test]
fn test_recovery_missing_or_corrupt_sidecar_fallback() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_missing = temp_dir.path().join("missing.wal-fec");
    let salts = WalSalts {
        salt1: 0x1010_2020,
        salt2: 0x3030_4040,
    };
    let fixture = build_fixture(1, 5, 2, salts, b"missing", 7, 300);
    let candidates = frame_candidates(&fixture);

    let missing_outcome = recover_wal_fec_group_with_decoder(
        &sidecar_missing,
        fixture.meta.group_id(),
        salts,
        fixture.meta.start_frame_no,
        &candidates,
        decoder_from_expected(fixture.source_pages.clone()),
    )
    .expect("missing sidecar should return fallback outcome");
    assert!(matches!(
        missing_outcome,
        WalFecRecoveryOutcome::TruncateBeforeGroup { .. }
    ));

    let sidecar_corrupt = temp_dir.path().join("corrupt.wal-fec");
    append_fixture(&sidecar_corrupt, &fixture);
    // Corrupt metadata payload so checksum validation fails.
    let payload_offset = 8 + 4 + (8 * 4) + 22 + 16;
    mutate_sidecar_meta_payload(&sidecar_corrupt, payload_offset);

    let corrupt_outcome = recover_wal_fec_group_with_decoder(
        &sidecar_corrupt,
        fixture.meta.group_id(),
        salts,
        fixture.meta.start_frame_no,
        &candidates,
        decoder_from_expected(fixture.source_pages.clone()),
    )
    .expect("corrupt sidecar should return fallback outcome");
    let WalFecRecoveryOutcome::TruncateBeforeGroup { decode_proof, .. } = corrupt_outcome else {
        panic!("expected fallback for corrupt sidecar");
    };
    assert_eq!(
        decode_proof.fallback_reason,
        Some(WalFecRecoveryFallbackReason::SidecarUnreadable)
    );
}

#[test]
fn test_recovery_source_hash_and_repair_symbol_filtering() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("filtering.wal-fec");
    let salts = WalSalts {
        salt1: 0xABAB_ABAB,
        salt2: 0xCDCD_CDCD,
    };
    let fixture = build_fixture(1, 5, 2, salts, b"filtering", 17, 411);
    append_fixture(&sidecar_path, &fixture);

    // Corrupt one repair SymbolRecord so recovery parser excludes it (instead of aborting scan).
    corrupt_first_repair_symbol_record(&sidecar_path);

    let mut candidates = frame_candidates(&fixture);
    // Corrupt one frame at/after mismatch to force source hash filtering.
    corrupt_frame(&mut candidates, fixture.meta.start_frame_no + 2);

    let outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        fixture.meta.group_id(),
        salts,
        fixture.meta.start_frame_no + 1,
        &candidates,
        decoder_from_expected(fixture.source_pages.clone()),
    )
    .expect("recovery should run");

    let WalFecRecoveryOutcome::Recovered(recovered) = outcome else {
        panic!("expected recovery with filtered symbols still sufficient");
    };
    assert_eq!(recovered.decode_proof.validated_repair_symbols, 1);
    assert_eq!(recovered.decode_proof.validated_source_symbols, 4);
}

#[test]
fn test_recovery_multiple_groups_and_chain_break_behavior() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("groups.wal-fec");
    let salts = WalSalts {
        salt1: 0xAA00_AA00,
        salt2: 0xBB00_BB00,
    };
    let group_a = build_fixture(1, 3, 2, salts, b"group-a", 1, 100);
    let group_b = build_fixture(4, 3, 2, salts, b"group-b", 11, 103);
    let group_c = build_fixture(7, 3, 2, salts, b"group-c", 21, 106);
    append_fixture(&sidecar_path, &group_a);
    append_fixture(&sidecar_path, &group_b);
    append_fixture(&sidecar_path, &group_c);

    let strict_scan = scan_wal_fec(&sidecar_path).expect("strict scan should parse all groups");
    assert_eq!(
        identify_damaged_commit_group(&strict_scan.groups, salts, group_b.meta.start_frame_no + 1),
        Some(group_b.meta.group_id())
    );

    let mut candidates = frame_candidates(&group_b);
    // Chain break starts at frame 5; frame 6 must be independently hash-validated and excluded.
    let mismatch_frame = group_b.meta.start_frame_no + 1;
    corrupt_frame(&mut candidates, mismatch_frame + 1);
    let outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        group_b.meta.group_id(),
        salts,
        mismatch_frame,
        &candidates,
        decoder_from_expected(group_b.source_pages.clone()),
    )
    .expect("group-b recovery should run");
    assert!(matches!(outcome, WalFecRecoveryOutcome::Recovered(_)));
}

#[test]
fn test_e2e_bd_1hi_11_compliance() {
    assert_eq!(BEAD_ID, "bd-1hi.11");

    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("e2e.wal-fec");
    let salts = WalSalts {
        salt1: 0xF0F0_1234,
        salt2: 0x0F0F_5678,
    };

    let mut fixtures = Vec::new();
    for group_index in 0_u32..10 {
        let start = (group_index * 5) + 1;
        let fixture = build_fixture(
            start,
            5,
            2,
            salts,
            format!("e2e-{group_index}").as_bytes(),
            u8::try_from(group_index * 3 + 7).expect("small index fits u8"),
            10_000 + group_index,
        );
        append_fixture(&sidecar_path, &fixture);
        fixtures.push(fixture);
    }

    let target = fixtures[4].clone(); // group 5
    let mut candidates = frame_candidates(&target);
    corrupt_frame(&mut candidates, target.meta.start_frame_no + 1);
    corrupt_frame(&mut candidates, target.meta.start_frame_no + 3);

    let outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        target.meta.group_id(),
        salts,
        target.meta.start_frame_no + 1,
        &candidates,
        decoder_from_expected(target.source_pages.clone()),
    )
    .expect("e2e recovery should run");

    let WalFecRecoveryOutcome::Recovered(recovered) = outcome else {
        panic!("expected successful e2e recovery");
    };
    assert_eq!(recovered.recovered_pages, target.source_pages);
    assert_eq!(recovered.db_size_pages, target.meta.db_size_pages);
    assert!(recovered.decode_proof.decode_attempted);
    assert!(recovered.decode_proof.decode_succeeded);
    assert_eq!(recovered.decode_proof.fallback_reason, None);
}
