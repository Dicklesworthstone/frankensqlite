use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{ObjectId, Oti};
use fsqlite_wal::{
    WalFecGroupMeta, WalFecGroupMetaInit, WalFecGroupRecord, WalFecRecoveryFallbackReason,
    WalFecRecoveryOutcome, WalFecRepairEvidenceQuery, WalFrameCandidate, WalSalts,
    append_wal_fec_group, build_source_page_hashes, generate_wal_fec_repair_symbols,
    identify_damaged_commit_group, query_raptorq_repair_evidence, raptorq_repair_evidence_snapshot,
    recover_wal_fec_group_with_config, recover_wal_fec_group_with_decoder,
    reset_raptorq_repair_telemetry, scan_wal_fec,
};
use tempfile::tempdir;

const PAGE_SIZE: u32 = 4096;
const BEAD_ID: &str = "bd-1hi.11";
const BEAD_1OHZ_ID: &str = "bd-1ohz";
const BEAD_9NBW_ID: &str = "bd-9nbw";
type TestDecoder = Box<dyn FnMut(&WalFecGroupMeta, &[(u32, Vec<u8>)]) -> Result<Vec<Vec<u8>>>>;

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
    let record = WalFecGroupRecord::new(meta.clone(), repair_symbols)
        .expect("fixture record should validate");
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

#[derive(Clone, Copy)]
struct DeterministicFaultRng {
    state: u64,
}

impl DeterministicFaultRng {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u32(&mut self) -> u32 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        let upper = (self.state >> 32) & u64::from(u32::MAX);
        u32::try_from(upper).expect("upper 32 bits always fit")
    }

    fn unique_offsets(&mut self, upper_exclusive: u32, count: usize) -> Vec<u32> {
        if upper_exclusive == 0 || count == 0 {
            return Vec::new();
        }
        let mut picked = BTreeSet::new();
        let target = count.min(usize::try_from(upper_exclusive).expect("small range fits usize"));
        while picked.len() < target {
            picked.insert(self.next_u32() % upper_exclusive);
        }
        picked.into_iter().collect()
    }
}

fn drop_frame(candidates: &mut Vec<WalFrameCandidate>, frame_no: u32) {
    candidates.retain(|candidate| candidate.frame_no != frame_no);
}

fn split_fault_offsets(offsets: &[u32], drop_count: usize) -> (Vec<u32>, Vec<u32>) {
    let split = drop_count.min(offsets.len());
    (offsets[..split].to_vec(), offsets[split..].to_vec())
}

fn build_faulted_candidates(
    fixture: &GroupFixture,
    drop_offsets: &[u32],
    corrupt_offsets: &[u32],
) -> (Vec<WalFrameCandidate>, u32) {
    let mut candidates = frame_candidates(fixture);
    let mut mismatch_frame = fixture.meta.end_frame_no.saturating_add(1);

    for offset in drop_offsets {
        let frame_no = fixture.meta.start_frame_no.saturating_add(*offset);
        mismatch_frame = mismatch_frame.min(frame_no);
        drop_frame(&mut candidates, frame_no);
    }
    for offset in corrupt_offsets {
        let frame_no = fixture.meta.start_frame_no.saturating_add(*offset);
        mismatch_frame = mismatch_frame.min(frame_no);
        if candidates
            .iter()
            .any(|candidate| candidate.frame_no == frame_no)
        {
            corrupt_frame(&mut candidates, frame_no);
        }
    }

    (candidates, mismatch_frame)
}

fn seeded_fault_scenario(
    rng: &mut DeterministicFaultRng,
    fixture: &GroupFixture,
    total_faults: usize,
    require_drop: bool,
) -> (Vec<WalFrameCandidate>, u32) {
    let raw_drop_count =
        usize::try_from(rng.next_u32()).expect("u32 fits usize") % (total_faults + 1);
    let drop_count = if require_drop {
        raw_drop_count.clamp(1, total_faults)
    } else {
        raw_drop_count
    };
    let offsets = rng.unique_offsets(fixture.meta.k_source, total_faults);
    let (drop_offsets, corrupt_offsets) = split_fault_offsets(&offsets, drop_count);
    build_faulted_candidates(fixture, &drop_offsets, &corrupt_offsets)
}

fn recover_with_expected_pages(
    sidecar_path: &Path,
    fixture: &GroupFixture,
    salts: WalSalts,
    mismatch_frame: u32,
    candidates: &[WalFrameCandidate],
) -> WalFecRecoveryOutcome {
    recover_wal_fec_group_with_decoder(
        sidecar_path,
        fixture.meta.group_id(),
        salts,
        mismatch_frame,
        candidates,
        decoder_from_expected(fixture.source_pages.clone()),
    )
    .expect("recovery should run")
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
    let repair_len = u32::from_le_bytes(
        bytes[repair_len_offset..repair_len_offset + 4]
            .try_into()
            .expect("repair prefix"),
    );
    let repair_len_usize = usize::try_from(repair_len).expect("repair len fits usize");
    let repair_payload_offset = repair_len_offset + 4;
    assert!(repair_payload_offset + repair_len_usize <= bytes.len());
    bytes[repair_payload_offset] ^= 0x55; // corrupt SymbolRecord magic/version bytes
    fs::write(sidecar_path, bytes).expect("sidecar write should succeed");
}

fn decoder_from_expected(expected_pages: Vec<Vec<u8>>) -> TestDecoder {
    Box::new(move |meta, available| {
        if available.len() < usize::try_from(meta.k_source).expect("k_source fits usize") {
            return Err(FrankenError::WalCorrupt {
                detail: "decoder invoked with insufficient symbols".to_owned(),
            });
        }
        Ok(expected_pages.clone())
    })
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
        decoder_from_expected(fixture.source_pages),
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
        decoder_from_expected(fixture.source_pages),
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
        decoder_from_expected(fixture.source_pages),
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
        decoder_from_expected(fixture.source_pages),
    )
    .expect("recovery should run");

    let WalFecRecoveryOutcome::Recovered(recovered) = outcome else {
        panic!("expected recovery with filtered symbols still sufficient");
    };
    assert_eq!(recovered.decode_proof.validated_repair_symbols, 1);
    assert_eq!(recovered.decode_proof.validated_source_symbols, 4);
    assert_eq!(recovered.decode_proof.corruption_observations, 1);
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
        decoder_from_expected(group_b.source_pages),
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

#[test]
fn test_bd_1ohz_deterministic_e2e_self_healing_with_restart_consistency() {
    assert_eq!(BEAD_1OHZ_ID, "bd-1ohz");

    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("bd_1ohz_e2e.wal-fec");
    let salts = WalSalts {
        salt1: 0x1234_5678,
        salt2: 0x90AB_CDEF,
    };

    let mut fixtures = Vec::new();
    for group_index in 0_u32..12 {
        let fixture = build_fixture(
            (group_index * 6) + 1,
            6,
            2,
            salts,
            format!("bd-1ohz-{group_index}").as_bytes(),
            u8::try_from(17 + group_index).expect("small index fits u8"),
            50_000 + group_index,
        );
        append_fixture(&sidecar_path, &fixture);
        fixtures.push(fixture);
    }

    let mut rng = DeterministicFaultRng::new(0x1A2B_3C4D_5E6F_7788_u64);
    let target_index = usize::try_from(rng.next_u32() % 12).expect("index fits usize");
    let target = fixtures
        .get(target_index)
        .cloned()
        .expect("target fixture must exist");

    let repair_budget = usize::try_from(target.meta.r_repair).expect("repair budget fits usize");
    let within_total_faults = repair_budget;
    let (within_candidates, within_mismatch_frame) =
        seeded_fault_scenario(&mut rng, &target, within_total_faults, false);
    let within_outcome = recover_with_expected_pages(
        &sidecar_path,
        &target,
        salts,
        within_mismatch_frame,
        &within_candidates,
    );
    let WalFecRecoveryOutcome::Recovered(within_recovered) = within_outcome else {
        panic!("bead_id={BEAD_1OHZ_ID} case=within_budget_expected_recovery");
    };
    assert_eq!(within_recovered.recovered_pages, target.source_pages);
    assert!(within_recovered.decode_proof.decode_attempted);
    assert!(within_recovered.decode_proof.decode_succeeded);
    assert_eq!(within_recovered.decode_proof.fallback_reason, None);

    let beyond_total_faults = repair_budget.saturating_add(1);
    let (beyond_candidates, beyond_mismatch_frame) =
        seeded_fault_scenario(&mut rng, &target, beyond_total_faults, true);
    let beyond_outcome = recover_with_expected_pages(
        &sidecar_path,
        &target,
        salts,
        beyond_mismatch_frame,
        &beyond_candidates,
    );
    let WalFecRecoveryOutcome::TruncateBeforeGroup {
        truncate_before_frame_no,
        decode_proof,
    } = &beyond_outcome
    else {
        panic!("bead_id={BEAD_1OHZ_ID} case=beyond_budget_expected_truncate");
    };
    assert_eq!(*truncate_before_frame_no, target.meta.start_frame_no);
    assert_eq!(
        decode_proof.fallback_reason,
        Some(WalFecRecoveryFallbackReason::InsufficientSymbols)
    );
    assert!(
        decode_proof.available_symbols < decode_proof.required_symbols,
        "bead_id={BEAD_1OHZ_ID} case=beyond_budget_expected_insufficient_symbols decode_proof={decode_proof:?}"
    );

    let scan = scan_wal_fec(&sidecar_path).expect("restart scan should parse sidecar");
    assert_eq!(
        identify_damaged_commit_group(&scan.groups, salts, beyond_mismatch_frame),
        Some(target.meta.group_id()),
        "bead_id={BEAD_1OHZ_ID} case=restart_group_identification_mismatch"
    );

    let replay_outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        target.meta.group_id(),
        salts,
        beyond_mismatch_frame,
        &beyond_candidates,
        decoder_from_expected(target.source_pages),
    )
    .expect("restart replay should run");
    assert_eq!(
        replay_outcome, beyond_outcome,
        "bead_id={BEAD_1OHZ_ID} case=restart_replay_outcome_mismatch"
    );
}

#[test]
#[allow(non_snake_case)]
fn test_raptorq_symbol_loss_within_R() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("loss_within_r.wal-fec");
    let salts = WalSalts {
        salt1: 0xAAAA_0101,
        salt2: 0xBBBB_0202,
    };
    let fixture = build_fixture(1, 6, 2, salts, b"bd-9nbw-loss-within-r", 23, 1_111);
    append_fixture(&sidecar_path, &fixture);

    let mut candidates = frame_candidates(&fixture);
    let first_corrupt = fixture.meta.start_frame_no + 1;
    let second_corrupt = fixture.meta.start_frame_no + 4;
    corrupt_frame(&mut candidates, first_corrupt);
    corrupt_frame(&mut candidates, second_corrupt);

    let outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        fixture.meta.group_id(),
        salts,
        first_corrupt,
        &candidates,
        decoder_from_expected(fixture.source_pages.clone()),
    )
    .expect("recovery should run");

    let WalFecRecoveryOutcome::Recovered(recovered) = outcome else {
        panic!("bead_id={BEAD_9NBW_ID} expected successful recovery when losses <= R");
    };
    assert!(recovered.decode_proof.decode_attempted);
    assert!(recovered.decode_proof.decode_succeeded);
    assert_eq!(recovered.decode_proof.fallback_reason, None);
    assert_eq!(recovered.recovered_pages, fixture.source_pages);
    assert!(
        recovered
            .decode_proof
            .recovered_frame_nos
            .contains(&first_corrupt),
        "bead_id={BEAD_9NBW_ID} case=within_r_first_corrupt_missing"
    );
    assert!(
        recovered
            .decode_proof
            .recovered_frame_nos
            .contains(&second_corrupt),
        "bead_id={BEAD_9NBW_ID} case=within_r_second_corrupt_missing"
    );
    eprintln!(
        "WARN bead_id={BEAD_9NBW_ID} case=loss_boundary_within_r recovered_frames={:?}",
        recovered.decode_proof.recovered_frame_nos
    );
}

#[test]
#[allow(non_snake_case)]
fn test_raptorq_symbol_loss_half_R() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("loss_half_r.wal-fec");
    let salts = WalSalts {
        salt1: 0xAB12_CD34,
        salt2: 0xEF56_7890,
    };
    let fixture = build_fixture(1, 8, 4, salts, b"bd-9nbw-loss-half-r", 29, 1_515);
    append_fixture(&sidecar_path, &fixture);

    let mut candidates = frame_candidates(&fixture);
    let first_corrupt = fixture.meta.start_frame_no + 2;
    let second_corrupt = fixture.meta.start_frame_no + 6;
    corrupt_frame(&mut candidates, first_corrupt);
    corrupt_frame(&mut candidates, second_corrupt);

    let outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        fixture.meta.group_id(),
        salts,
        first_corrupt,
        &candidates,
        decoder_from_expected(fixture.source_pages.clone()),
    )
    .expect("recovery should run");

    let WalFecRecoveryOutcome::Recovered(recovered) = outcome else {
        panic!("bead_id={BEAD_9NBW_ID} expected successful recovery when losses <= R/2");
    };
    assert!(recovered.decode_proof.decode_attempted);
    assert!(recovered.decode_proof.decode_succeeded);
    assert_eq!(recovered.decode_proof.fallback_reason, None);
    assert_eq!(recovered.recovered_pages, fixture.source_pages);
    assert!(
        recovered
            .decode_proof
            .recovered_frame_nos
            .contains(&first_corrupt),
        "bead_id={BEAD_9NBW_ID} case=half_r_first_corrupt_missing"
    );
    assert!(
        recovered
            .decode_proof
            .recovered_frame_nos
            .contains(&second_corrupt),
        "bead_id={BEAD_9NBW_ID} case=half_r_second_corrupt_missing"
    );
}

#[test]
#[allow(non_snake_case)]
fn test_raptorq_symbol_loss_beyond_R() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("loss_beyond_r.wal-fec");
    let salts = WalSalts {
        salt1: 0xCCCC_0303,
        salt2: 0xDDDD_0404,
    };
    let fixture = build_fixture(1, 6, 2, salts, b"bd-9nbw-loss-beyond-r", 31, 2_222);
    append_fixture(&sidecar_path, &fixture);

    let mut candidates = frame_candidates(&fixture);
    let first_corrupt = fixture.meta.start_frame_no;
    corrupt_frame(&mut candidates, first_corrupt);
    corrupt_frame(&mut candidates, fixture.meta.start_frame_no + 2);
    corrupt_frame(&mut candidates, fixture.meta.start_frame_no + 5);

    let outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        fixture.meta.group_id(),
        salts,
        first_corrupt,
        &candidates,
        decoder_from_expected(fixture.source_pages),
    )
    .expect("recovery should return fallback outcome");

    let WalFecRecoveryOutcome::TruncateBeforeGroup {
        truncate_before_frame_no,
        decode_proof,
    } = outcome
    else {
        panic!("bead_id={BEAD_9NBW_ID} expected truncate fallback when losses > R");
    };
    assert_eq!(truncate_before_frame_no, fixture.meta.start_frame_no);
    assert_eq!(
        decode_proof.fallback_reason,
        Some(WalFecRecoveryFallbackReason::InsufficientSymbols)
    );
    assert!(
        decode_proof.available_symbols < decode_proof.required_symbols,
        "bead_id={BEAD_9NBW_ID} case=beyond_r_expected_insufficient_symbols decode_proof={decode_proof:?}"
    );
}

#[test]
fn test_raptorq_bitflip_detected() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("bitflip_detected.wal-fec");
    let salts = WalSalts {
        salt1: 0xABCD_1111,
        salt2: 0xEF01_2222,
    };
    let fixture = build_fixture(1, 5, 2, salts, b"bd-9nbw-bitflip-detected", 41, 3_333);
    append_fixture(&sidecar_path, &fixture);

    let mut candidates = frame_candidates(&fixture);
    let corrupted_frame = fixture.meta.start_frame_no + 2;
    corrupt_frame(&mut candidates, corrupted_frame);

    let outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        fixture.meta.group_id(),
        salts,
        corrupted_frame,
        &candidates,
        decoder_from_expected(fixture.source_pages),
    )
    .expect("recovery should run");

    let WalFecRecoveryOutcome::Recovered(recovered) = outcome else {
        panic!("bead_id={BEAD_9NBW_ID} expected recovery for single bitflip");
    };
    assert!(recovered.decode_proof.decode_attempted);
    assert_eq!(
        recovered.decode_proof.validated_source_symbols,
        fixture.meta.k_source - 1,
        "bead_id={BEAD_9NBW_ID} case=bitflip_must_reduce_validated_sources"
    );
    assert!(
        recovered
            .decode_proof
            .recovered_frame_nos
            .contains(&corrupted_frame),
        "bead_id={BEAD_9NBW_ID} case=bitflip_frame_not_recorded"
    );
}

#[test]
fn test_raptorq_bitflip_repair() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("bitflip_repair.wal-fec");
    let salts = WalSalts {
        salt1: 0xABCD_3333,
        salt2: 0xEF01_4444,
    };
    let fixture = build_fixture(1, 5, 2, salts, b"bd-9nbw-bitflip-repair", 47, 4_444);
    append_fixture(&sidecar_path, &fixture);

    let mut candidates = frame_candidates(&fixture);
    let corrupted_frame = fixture.meta.start_frame_no + 1;
    corrupt_frame(&mut candidates, corrupted_frame);

    let outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        fixture.meta.group_id(),
        salts,
        corrupted_frame,
        &candidates,
        decoder_from_expected(fixture.source_pages.clone()),
    )
    .expect("recovery should run");

    let WalFecRecoveryOutcome::Recovered(recovered) = outcome else {
        panic!("bead_id={BEAD_9NBW_ID} expected successful repair");
    };
    assert_eq!(
        recovered.recovered_pages, fixture.source_pages,
        "bead_id={BEAD_9NBW_ID} case=bitflip_repair_pages_mismatch"
    );
    assert!(recovered.decode_proof.decode_succeeded);
    assert_eq!(recovered.decode_proof.fallback_reason, None);
}

#[test]
fn test_raptorq_decode_proof() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("decode_proof.wal-fec");
    let salts = WalSalts {
        salt1: 0xFACE_5555,
        salt2: 0xC0DE_6666,
    };
    let fixture = build_fixture(1, 5, 2, salts, b"bd-9nbw-decode-proof", 53, 5_555);
    append_fixture(&sidecar_path, &fixture);

    let mut candidates = frame_candidates(&fixture);
    corrupt_frame(&mut candidates, fixture.meta.start_frame_no);
    corrupt_frame(&mut candidates, fixture.meta.start_frame_no + 2);
    corrupt_frame(&mut candidates, fixture.meta.start_frame_no + 4);

    let outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        fixture.meta.group_id(),
        salts,
        fixture.meta.start_frame_no,
        &candidates,
        decoder_from_expected(fixture.source_pages),
    )
    .expect("recovery should return decode proof");

    let WalFecRecoveryOutcome::TruncateBeforeGroup { decode_proof, .. } = outcome else {
        panic!("bead_id={BEAD_9NBW_ID} expected decode proof fallback");
    };
    assert_eq!(decode_proof.group_id, fixture.meta.group_id());
    assert_eq!(decode_proof.required_symbols, fixture.meta.k_source);
    assert!(
        decode_proof.available_symbols < decode_proof.required_symbols,
        "bead_id={BEAD_9NBW_ID} case=decode_proof_availability_invalid decode_proof={decode_proof:?}"
    );
    assert_eq!(
        decode_proof.fallback_reason,
        Some(WalFecRecoveryFallbackReason::InsufficientSymbols)
    );
}

#[test]
fn test_recovery_with_config_records_evidence_card() {
    reset_raptorq_repair_telemetry();

    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("config_evidence.wal-fec");
    let salts = WalSalts {
        salt1: 0xABCD_AAAA,
        salt2: 0xEF01_BBBB,
    };
    let fixture = build_fixture(1, 5, 2, salts, b"bd-n0g4q.4-evidence", 63, 7_777);
    append_fixture(&sidecar_path, &fixture);

    let mut candidates = frame_candidates(&fixture);
    let mismatch_frame = fixture.meta.start_frame_no + 2;
    corrupt_frame(&mut candidates, mismatch_frame);

    let (outcome, _log) = recover_wal_fec_group_with_config(
        &sidecar_path,
        fixture.meta.group_id(),
        salts,
        mismatch_frame,
        &candidates,
        &fsqlite_wal::WalFecRecoveryConfig::default(),
        decoder_from_expected(fixture.source_pages.clone()),
    )
    .expect("config-aware recovery should run");

    let WalFecRecoveryOutcome::Recovered(recovered) = outcome else {
        panic!("expected successful recovery with evidence card");
    };
    assert!(recovered.decode_proof.decode_attempted);

    let cards = raptorq_repair_evidence_snapshot(0);
    assert_eq!(cards.len(), 1);
    let card = &cards[0];
    assert_eq!(card.frame_id, fixture.meta.group_id().end_frame_no);
    assert!(card.repair_latency_ns > 0);
    assert_ne!(card.chain_hash, [0_u8; 32]);

    let by_frame = query_raptorq_repair_evidence(&WalFecRepairEvidenceQuery {
        frame_id: Some(card.frame_id),
        ..WalFecRepairEvidenceQuery::default()
    });
    assert_eq!(by_frame.len(), 1);
    assert_eq!(by_frame[0].chain_hash, card.chain_hash);
}

#[test]
fn test_e2e_raptorq_harness() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("bd_9nbw_e2e.wal-fec");
    let salts = WalSalts {
        salt1: 0x9090_A0A0,
        salt2: 0xB0B0_C0C0,
    };
    let fixture = build_fixture(1, 8, 2, salts, b"bd-9nbw-e2e", 61, 6_666);
    append_fixture(&sidecar_path, &fixture);

    let scenarios: [(&str, &[u32], bool); 4] = [
        ("intact", &[], true),
        ("single_loss", &[1], true),
        ("boundary_loss", &[0, 5], true),
        ("beyond_budget_loss", &[0, 3, 6], false),
    ];

    let mut recovered_scenarios = 0_u32;
    let mut truncated_scenarios = 0_u32;
    let mut fast_path_elapsed = Duration::ZERO;
    let mut decode_path_elapsed = Duration::ZERO;

    for (label, offsets, should_recover) in scenarios {
        let mut candidates = frame_candidates(&fixture);
        for offset in offsets {
            let frame_no = fixture.meta.start_frame_no + *offset;
            corrupt_frame(&mut candidates, frame_no);
        }
        let mismatch_frame_no = offsets
            .first()
            .map_or(fixture.meta.end_frame_no + 1, |offset| {
                fixture.meta.start_frame_no + *offset
            });

        let started = Instant::now();
        let outcome = recover_wal_fec_group_with_decoder(
            &sidecar_path,
            fixture.meta.group_id(),
            salts,
            mismatch_frame_no,
            &candidates,
            decoder_from_expected(fixture.source_pages.clone()),
        )
        .expect("scenario recovery should run");
        let elapsed = started.elapsed();

        match (outcome, should_recover) {
            (WalFecRecoveryOutcome::Recovered(recovered), true) => {
                recovered_scenarios = recovered_scenarios.saturating_add(1);
                if offsets.is_empty() {
                    fast_path_elapsed = fast_path_elapsed.saturating_add(elapsed);
                } else {
                    decode_path_elapsed = decode_path_elapsed.saturating_add(elapsed);
                }
                assert_eq!(recovered.recovered_pages, fixture.source_pages);
                eprintln!(
                    "DEBUG bead_id={BEAD_9NBW_ID} case=scenario_recovered label={label} elapsed_ms={}",
                    elapsed.as_millis()
                );
            }
            (WalFecRecoveryOutcome::TruncateBeforeGroup { decode_proof, .. }, false) => {
                truncated_scenarios = truncated_scenarios.saturating_add(1);
                assert_eq!(
                    decode_proof.fallback_reason,
                    Some(WalFecRecoveryFallbackReason::InsufficientSymbols)
                );
                eprintln!(
                    "ERROR bead_id={BEAD_9NBW_ID} case=scenario_truncated label={label} decode_proof={decode_proof:?}"
                );
            }
            (unexpected, expected_recover) => {
                panic!(
                    "bead_id={BEAD_9NBW_ID} case=scenario_outcome_mismatch label={label} expected_recover={expected_recover} outcome={unexpected:?}"
                );
            }
        }
    }

    assert_eq!(recovered_scenarios, 3);
    assert_eq!(truncated_scenarios, 1);
    assert!(fast_path_elapsed > Duration::ZERO);
    assert!(decode_path_elapsed > Duration::ZERO);

    let decode_budget = Duration::from_millis(500);
    assert!(
        decode_path_elapsed < decode_budget,
        "bead_id={BEAD_9NBW_ID} case=decode_path_exceeded_budget decode_ms={} budget_ms={}",
        decode_path_elapsed.as_millis(),
        decode_budget.as_millis()
    );

    eprintln!(
        "INFO bead_id={BEAD_9NBW_ID} case=e2e_summary recovered_scenarios={recovered_scenarios} truncated_scenarios={truncated_scenarios} fast_path_ms={} decode_path_ms={}",
        fast_path_elapsed.as_millis(),
        decode_path_elapsed.as_millis()
    );
}
