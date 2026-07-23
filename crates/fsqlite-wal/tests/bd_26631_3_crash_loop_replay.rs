use std::future::Future;
use std::path::Path;

use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_vfs::MemoryVfs;
use fsqlite_vfs::traits::{Vfs, VfsFile};
use fsqlite_wal::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalFile, WalSalts};
use xxhash_rust::xxh3::xxh3_64;

const PAGE_SIZE: u32 = 4096;
const RESTART_ATTEMPTS: usize = 6;
const REPLAY_COMMAND: &str =
    "cargo test -p fsqlite-wal --test bd_26631_3_crash_loop_replay -- --nocapture --test-threads=1";

const FRAME_LAYOUT: [(u32, u32, u8); 6] = [
    (1, 0, 1),
    (2, 0, 2),
    (3, 3, 3),
    (4, 0, 4),
    (5, 0, 5),
    (6, 6, 6),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FaultKind {
    None,
    TornDataMissingFec,
    TornCommitMissingFec,
}

impl FaultKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::TornDataMissingFec => "torn_data_missing_fec",
            Self::TornCommitMissingFec => "torn_commit_missing_fec",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScenarioSpec {
    scenario_id: &'static str,
    seed: u64,
    fault: FaultKind,
    expected_committed_frames: usize,
    expected_last_commit: Option<usize>,
    recovery_state: &'static str,
    repaired_frames: u32,
    unrecoverable_frames: u32,
}

const SCENARIOS: [ScenarioSpec; 3] = [
    ScenarioSpec {
        scenario_id: "WAL-R3-CRASH-LOOP-CONTROL",
        seed: 26631031,
        fault: FaultKind::None,
        expected_committed_frames: 6,
        expected_last_commit: Some(5),
        recovery_state: "stable_replay",
        repaired_frames: 0,
        unrecoverable_frames: 0,
    },
    ScenarioSpec {
        scenario_id: "WAL-R3-TORN-DATA-MISSING-FEC",
        seed: 26631032,
        fault: FaultKind::TornDataMissingFec,
        expected_committed_frames: 3,
        expected_last_commit: Some(2),
        recovery_state: "truncated_without_fec",
        repaired_frames: 0,
        unrecoverable_frames: 3,
    },
    ScenarioSpec {
        scenario_id: "WAL-R3-TORN-COMMIT-MISSING-FEC",
        seed: 26631033,
        fault: FaultKind::TornCommitMissingFec,
        expected_committed_frames: 3,
        expected_last_commit: Some(2),
        recovery_state: "truncated_without_fec",
        repaired_frames: 0,
        unrecoverable_frames: 3,
    },
];

fn test_cx() -> Cx {
    Cx::default()
}

fn block_on_test<F: Future>(future: F) -> F::Output {
    let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .expect("test runtime should build");
    runtime.block_on(future)
}

fn page_size_usize() -> usize {
    usize::try_from(PAGE_SIZE).expect("PAGE_SIZE fits usize")
}

fn frame_size() -> usize {
    WAL_FRAME_HEADER_SIZE + page_size_usize()
}

fn test_salts(seed: u64) -> WalSalts {
    let low = u32::try_from(seed & u64::from(u32::MAX)).expect("masked seed fits u32");
    let shifted =
        u32::try_from((seed >> 16) & u64::from(u32::MAX)).expect("shifted masked seed fits u32");
    WalSalts {
        salt1: 0xB300_0000 ^ low,
        salt2: 0xC600_0000 ^ shifted,
    }
}

fn sample_page(seed: u64, page_number: u32, marker: u8) -> Vec<u8> {
    let seed_byte = u8::try_from(seed % 251).expect("seed modulo fits u8");
    let page_byte = u8::try_from(page_number % 251).expect("page modulo fits u8");
    let mut page = vec![0_u8; page_size_usize()];
    for (idx, byte) in page.iter_mut().enumerate() {
        let idx_byte = u8::try_from(idx % 251).expect("index modulo fits u8");
        *byte = idx_byte ^ seed_byte ^ page_byte ^ marker;
    }
    page
}

fn open_wal_file(vfs: &MemoryVfs, cx: &Cx) -> <MemoryVfs as Vfs>::File {
    let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
    let (file, _) = vfs
        .open(cx, Some(Path::new("bd_26631_3.db-wal")), flags)
        .expect("open wal file");
    file
}

fn write_fixture_wal(vfs: &MemoryVfs, cx: &Cx, seed: u64) {
    let file = open_wal_file(vfs, cx);
    let mut wal = block_on_test(WalFile::create(cx, file, PAGE_SIZE, 0, test_salts(seed)))
        .expect("create wal");
    for (page_number, db_size, marker) in FRAME_LAYOUT {
        block_on_test(wal.append_frame(
            cx,
            page_number,
            &sample_page(seed, page_number, marker),
            db_size,
        ))
        .expect("append frame");
    }
    wal.close(cx).expect("close wal");
}

fn truncate_wal(vfs: &MemoryVfs, cx: &Cx, cut_at: usize) {
    let mut file = open_wal_file(vfs, cx);
    let cut_at_u64 = u64::try_from(cut_at).expect("cut_at fits u64");
    file.truncate(cx, cut_at_u64).expect("truncate wal");
}

fn apply_fault(spec: ScenarioSpec, vfs: &MemoryVfs, cx: &Cx) {
    let fs = frame_size();
    match spec.fault {
        FaultKind::None => {}
        FaultKind::TornDataMissingFec => {
            let cut_at = WAL_HEADER_SIZE + 3 * fs + (fs / 3);
            truncate_wal(vfs, cx, cut_at);
        }
        FaultKind::TornCommitMissingFec => {
            let cut_at = WAL_HEADER_SIZE + 5 * fs + (fs / 2);
            truncate_wal(vfs, cx, cut_at);
        }
    }
}

fn replay_token(spec: ScenarioSpec) -> String {
    let material = format!("{}:{}:{}", spec.scenario_id, spec.seed, spec.fault.as_str());
    format!("{:016x}", xxh3_64(material.as_bytes()))
}

fn artifact_path(spec: ScenarioSpec, token: &str) -> String {
    format!(
        "artifacts/wal-recovery/{}/seed-{}/{}.json",
        spec.scenario_id.to_ascii_lowercase(),
        spec.seed,
        token
    )
}

fn scenario_by_fault(fault: FaultKind) -> ScenarioSpec {
    SCENARIOS
        .iter()
        .copied()
        .find(|scenario| scenario.fault == fault)
        .expect("scenario exists for fault")
}

fn validate_recovered_prefix(
    spec: ScenarioSpec,
    wal: &mut WalFile<<MemoryVfs as Vfs>::File>,
    cx: &Cx,
) -> (usize, Option<usize>, u64) {
    let frame_count = wal.frame_count();
    let last_commit = wal.last_commit_frame(cx).expect("last commit frame");
    assert_eq!(
        frame_count, spec.expected_committed_frames,
        "scenario {} committed frame mismatch",
        spec.scenario_id
    );
    assert_eq!(
        last_commit, spec.expected_last_commit,
        "scenario {} commit boundary mismatch",
        spec.scenario_id
    );

    let mut digest_input = Vec::new();
    for (index, frame_layout) in FRAME_LAYOUT.iter().enumerate().take(frame_count) {
        let (header, data) =
            block_on_test(wal.read_frame(cx, index)).expect("read recovered frame");
        assert_eq!(
            header.page_number, frame_layout.0,
            "scenario {} page number mismatch at frame {}",
            spec.scenario_id, index
        );
        assert_eq!(
            header.db_size, frame_layout.1,
            "scenario {} db_size mismatch at frame {}",
            spec.scenario_id, index
        );
        assert_eq!(
            data,
            sample_page(spec.seed, frame_layout.0, frame_layout.2),
            "scenario {} page payload mismatch at frame {}",
            spec.scenario_id,
            index
        );
        digest_input.extend_from_slice(&header.page_number.to_le_bytes());
        digest_input.extend_from_slice(&header.db_size.to_le_bytes());
        digest_input.extend_from_slice(&(u8::from(header.is_commit())).to_le_bytes());
        digest_input.extend_from_slice(&data);
    }

    (frame_count, last_commit, xxh3_64(&digest_input))
}

fn run_scenario(spec: ScenarioSpec) {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    write_fixture_wal(&vfs, &cx, spec.seed);
    apply_fault(spec, &vfs, &cx);

    let token = replay_token(spec);
    let artifact = artifact_path(spec, &token);
    let mut baseline: Option<(usize, Option<usize>, u64)> = None;

    for attempt_index in 0..RESTART_ATTEMPTS {
        let file = open_wal_file(&vfs, &cx);
        let mut wal =
            block_on_test(WalFile::open(&cx, file)).expect("open wal for crash-loop replay");
        let (committed_frames, last_commit, digest) =
            validate_recovered_prefix(spec, &mut wal, &cx);
        wal.close(&cx).expect("close wal after replay validation");

        if let Some((base_count, base_commit, base_digest)) = baseline {
            if base_count != committed_frames || base_commit != last_commit || base_digest != digest
            {
                println!(
                    "FIRST_FAILURE:{{\"scenario_id\":\"{}\",\"attempt_index\":{},\"reason\":\"non_deterministic_replay\",\"baseline_digest\":\"{:016x}\",\"observed_digest\":\"{:016x}\",\"replay_token\":\"{}\",\"artifact_path\":\"{}\",\"replay_command\":\"{}\"}}",
                    spec.scenario_id,
                    attempt_index,
                    base_digest,
                    digest,
                    token,
                    artifact,
                    REPLAY_COMMAND
                );
            }
            assert_eq!(
                (committed_frames, last_commit, digest),
                (base_count, base_commit, base_digest),
                "scenario {} replay divergence at attempt {}",
                spec.scenario_id,
                attempt_index
            );
        } else {
            baseline = Some((committed_frames, last_commit, digest));
        }

        println!(
            "RECOVERY_ATTEMPT:{{\"scenario_id\":\"{}\",\"seed\":{},\"attempt_index\":{},\"recovery_state\":\"{}\",\"repaired_frames\":{},\"unrecoverable_frames\":{},\"outcome\":\"ok\",\"committed_frames\":{},\"last_commit_frame\":{},\"replay_token\":\"{}\",\"artifact_path\":\"{}\",\"replay_command\":\"{}\"}}",
            spec.scenario_id,
            spec.seed,
            attempt_index,
            spec.recovery_state,
            spec.repaired_frames,
            spec.unrecoverable_frames,
            committed_frames,
            last_commit
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_owned()),
            token,
            artifact,
            REPLAY_COMMAND
        );
    }

    let (committed_frames, last_commit, digest) = baseline.expect("baseline set");
    println!(
        "SCENARIO_OUTCOME:{{\"scenario_id\":\"{}\",\"seed\":{},\"fault_kind\":\"{}\",\"expected_committed_frames\":{},\"observed_committed_frames\":{},\"restart_attempts\":{},\"recovery_state\":\"{}\",\"repaired_frames\":{},\"unrecoverable_frames\":{},\"outcome\":\"pass\",\"final_digest\":\"{:016x}\",\"last_commit_frame\":{},\"replay_token\":\"{}\",\"artifact_path\":\"{}\",\"replay_command\":\"{}\"}}",
        spec.scenario_id,
        spec.seed,
        spec.fault.as_str(),
        spec.expected_committed_frames,
        committed_frames,
        RESTART_ATTEMPTS,
        spec.recovery_state,
        spec.repaired_frames,
        spec.unrecoverable_frames,
        digest,
        last_commit
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_owned()),
        token,
        artifact,
        REPLAY_COMMAND
    );
}

#[test]
fn unit_crash_loop_catalog_has_required_coverage() {
    assert_eq!(SCENARIOS.len(), 3, "expected three R3 scenarios");
    assert!(
        SCENARIOS
            .iter()
            .any(|scenario| scenario.fault == FaultKind::None),
        "control restart-loop scenario missing"
    );
    assert!(
        SCENARIOS
            .iter()
            .any(|scenario| scenario.fault == FaultKind::TornDataMissingFec),
        "torn data + missing fec scenario missing"
    );
    assert!(
        SCENARIOS
            .iter()
            .any(|scenario| scenario.fault == FaultKind::TornCommitMissingFec),
        "torn commit + missing fec scenario missing"
    );
}

#[test]
fn unit_replay_token_generation_is_deterministic() {
    let control = scenario_by_fault(FaultKind::None);
    let control_token_a = replay_token(control);
    let control_token_b = replay_token(control);
    let torn_token = replay_token(scenario_by_fault(FaultKind::TornDataMissingFec));

    assert_eq!(
        control_token_a, control_token_b,
        "replay token must be deterministic"
    );
    assert_ne!(
        control_token_a, torn_token,
        "different scenarios must produce different replay tokens"
    );
}

#[test]
fn scenario_control_restart_loop_is_deterministic() {
    run_scenario(scenario_by_fault(FaultKind::None));
}

#[test]
fn scenario_torn_data_missing_fec_has_deterministic_recovery() {
    run_scenario(scenario_by_fault(FaultKind::TornDataMissingFec));
}

#[test]
fn scenario_torn_commit_missing_fec_has_deterministic_recovery() {
    run_scenario(scenario_by_fault(FaultKind::TornCommitMissingFec));
}
