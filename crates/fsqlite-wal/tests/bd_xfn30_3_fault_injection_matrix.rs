use std::path::Path;

use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_vfs::traits::{Vfs, VfsFile};
use fsqlite_vfs::MemoryVfs;
use fsqlite_wal::{WalFile, WalSalts, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE};

const PAGE_SIZE: u32 = 4096;
const REPLAY_COMMAND: &str =
    "cargo test -p fsqlite-wal --test bd_xfn30_3_fault_injection_matrix -- --nocapture --test-threads=1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FaultKind {
    MidCommitKill,
    RestartLoop,
    TornWrite,
    ChecksumCorruption,
    PartialFsync,
}

impl FaultKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::MidCommitKill => "mid_commit_kill",
            Self::RestartLoop => "restart_loop",
            Self::TornWrite => "torn_write",
            Self::ChecksumCorruption => "checksum_corruption",
            Self::PartialFsync => "partial_fsync",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExpectedOutcome {
    committed_frames: usize,
    reason_code: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScenarioSpec {
    scenario_id: &'static str,
    seed: u64,
    kind: FaultKind,
    expected: ExpectedOutcome,
    restart_iterations: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObservedOutcome {
    committed_frames: usize,
    restart_iterations: usize,
}

const SCENARIOS: [ScenarioSpec; 5] = [
    ScenarioSpec {
        scenario_id: "WAL-DURABILITY-MID-COMMIT-KILL",
        seed: 40101,
        kind: FaultKind::MidCommitKill,
        expected: ExpectedOutcome {
            committed_frames: 3,
            reason_code: "incomplete_commit_dropped",
        },
        restart_iterations: 3,
    },
    ScenarioSpec {
        scenario_id: "WAL-DURABILITY-RESTART-LOOP",
        seed: 40102,
        kind: FaultKind::RestartLoop,
        expected: ExpectedOutcome {
            committed_frames: 6,
            reason_code: "restart_idempotent",
        },
        restart_iterations: 6,
    },
    ScenarioSpec {
        scenario_id: "WAL-DURABILITY-TORN-WRITE",
        seed: 40103,
        kind: FaultKind::TornWrite,
        expected: ExpectedOutcome {
            committed_frames: 3,
            reason_code: "torn_tail_truncated",
        },
        restart_iterations: 3,
    },
    ScenarioSpec {
        scenario_id: "WAL-DURABILITY-CHECKSUM-CORRUPTION",
        seed: 40104,
        kind: FaultKind::ChecksumCorruption,
        expected: ExpectedOutcome {
            committed_frames: 3,
            reason_code: "checksum_mismatch_truncated",
        },
        restart_iterations: 3,
    },
    ScenarioSpec {
        scenario_id: "WAL-DURABILITY-PARTIAL-FSYNC",
        seed: 40105,
        kind: FaultKind::PartialFsync,
        expected: ExpectedOutcome {
            committed_frames: 3,
            reason_code: "partial_fsync_tail_dropped",
        },
        restart_iterations: 3,
    },
];

fn test_cx() -> Cx {
    Cx::default()
}

fn page_size_usize() -> usize {
    usize::try_from(PAGE_SIZE).expect("PAGE_SIZE fits usize")
}

fn test_salts(seed: u64) -> WalSalts {
    let low = u32::try_from(seed & u64::from(u32::MAX)).expect("masked seed fits u32");
    let shifted =
        u32::try_from((seed >> 16) & u64::from(u32::MAX)).expect("shifted masked seed fits u32");
    WalSalts {
        salt1: 0xDEAD_0000 ^ low,
        salt2: 0xCAFE_0000 ^ shifted,
    }
}

fn sample_page(seed: u64, page_number: u32) -> Vec<u8> {
    let seed_byte = u8::try_from(seed % 251).expect("seed modulo fits u8");
    let page_byte = u8::try_from(page_number % 251).expect("page modulo fits u8");
    let mut page = vec![0_u8; page_size_usize()];
    for (idx, byte) in page.iter_mut().enumerate() {
        let idx_byte = u8::try_from(idx % 251).expect("index modulo fits u8");
        *byte = idx_byte ^ seed_byte ^ page_byte;
    }
    page
}

fn open_wal_file(vfs: &MemoryVfs, cx: &Cx) -> <MemoryVfs as Vfs>::File {
    let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
    let (file, _) = vfs
        .open(cx, Some(Path::new("bd_xfn30_3.db-wal")), flags)
        .expect("open wal file");
    file
}

fn build_two_transaction_wal(seed: u64) -> MemoryVfs {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let file = open_wal_file(&vfs, &cx);
    let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts(seed)).expect("create wal");

    let frame_specs = [
        (1_u32, 0_u32),
        (2_u32, 0_u32),
        (3_u32, 3_u32),
        (4_u32, 0_u32),
        (5_u32, 0_u32),
        (6_u32, 6_u32),
    ];
    for (page_number, db_size) in frame_specs {
        wal.append_frame(&cx, page_number, &sample_page(seed, page_number), db_size)
            .expect("append frame");
    }
    wal.close(&cx).expect("close wal");

    vfs
}

fn frame_size() -> usize {
    WAL_FRAME_HEADER_SIZE + page_size_usize()
}

fn flip_one_byte(vfs: &MemoryVfs, cx: &Cx, offset: usize, xor_mask: u8) {
    let mut file = open_wal_file(vfs, cx);
    let mut byte = [0_u8; 1];
    let offset_u64 = u64::try_from(offset).expect("offset fits u64");
    file.read(cx, &mut byte, offset_u64).expect("read byte");
    byte[0] ^= xor_mask;
    file.write(cx, &byte, offset_u64)
        .expect("write flipped byte");
}

fn write_zeroes(vfs: &MemoryVfs, cx: &Cx, start: usize, len: usize) {
    let mut file = open_wal_file(vfs, cx);
    let start_u64 = u64::try_from(start).expect("start fits u64");
    file.write(cx, &vec![0_u8; len], start_u64)
        .expect("write zeroes");
}

fn truncate_wal(vfs: &MemoryVfs, cx: &Cx, cut_at: usize) {
    let mut file = open_wal_file(vfs, cx);
    let cut_at_u64 = u64::try_from(cut_at).expect("cut_at fits u64");
    file.truncate(cx, cut_at_u64).expect("truncate wal");
}

fn apply_fault(spec: ScenarioSpec, vfs: &MemoryVfs, cx: &Cx) {
    let fs = frame_size();
    match spec.kind {
        FaultKind::MidCommitKill => {
            // Partial commit-frame write in txn2.
            let cut_at = WAL_HEADER_SIZE + 5 * fs + (fs / 2);
            truncate_wal(vfs, cx, cut_at);
        }
        FaultKind::RestartLoop => {
            // Control scenario: no mutation.
        }
        FaultKind::TornWrite => {
            // Partial non-commit frame in txn2.
            let cut_at = WAL_HEADER_SIZE + 3 * fs + (fs / 3);
            truncate_wal(vfs, cx, cut_at);
        }
        FaultKind::ChecksumCorruption => {
            // Single-byte corruption in frame 5 page payload.
            let offset = WAL_HEADER_SIZE + 4 * fs + WAL_FRAME_HEADER_SIZE + 42;
            flip_one_byte(vfs, cx, offset, 0x01);
        }
        FaultKind::PartialFsync => {
            // Simulate partial sector persistence in the txn2 commit frame tail.
            let page_tail = 512_usize;
            let start =
                WAL_HEADER_SIZE + 5 * fs + WAL_FRAME_HEADER_SIZE + page_size_usize() - page_tail;
            write_zeroes(vfs, cx, start, page_tail);
        }
    }
}

fn expected_for_kind(kind: FaultKind) -> ExpectedOutcome {
    match kind {
        FaultKind::MidCommitKill => ExpectedOutcome {
            committed_frames: 3,
            reason_code: "incomplete_commit_dropped",
        },
        FaultKind::RestartLoop => ExpectedOutcome {
            committed_frames: 6,
            reason_code: "restart_idempotent",
        },
        FaultKind::TornWrite => ExpectedOutcome {
            committed_frames: 3,
            reason_code: "torn_tail_truncated",
        },
        FaultKind::ChecksumCorruption => ExpectedOutcome {
            committed_frames: 3,
            reason_code: "checksum_mismatch_truncated",
        },
        FaultKind::PartialFsync => ExpectedOutcome {
            committed_frames: 3,
            reason_code: "partial_fsync_tail_dropped",
        },
    }
}

fn validate_recovered_prefix(
    spec: ScenarioSpec,
    wal: &mut WalFile<<MemoryVfs as Vfs>::File>,
    cx: &Cx,
) {
    assert_eq!(
        wal.frame_count(),
        spec.expected.committed_frames,
        "scenario {} committed frame count mismatch",
        spec.scenario_id
    );

    let mut commit_indices = Vec::new();
    for idx in 0..wal.frame_count() {
        let (header, data) = wal.read_frame(cx, idx).expect("read recovered frame");
        let page_number = u32::try_from(idx + 1).expect("frame index fits u32");
        assert_eq!(
            header.page_number, page_number,
            "scenario {} recovered page number mismatch",
            spec.scenario_id
        );
        assert_eq!(
            data,
            sample_page(spec.seed, page_number),
            "scenario {} recovered page payload mismatch",
            spec.scenario_id
        );
        if header.is_commit() {
            commit_indices.push(idx);
        }
    }

    let expected_commits = if spec.expected.committed_frames == 6 {
        vec![2_usize, 5_usize]
    } else {
        vec![2_usize]
    };
    assert_eq!(
        commit_indices, expected_commits,
        "scenario {} commit boundary mismatch",
        spec.scenario_id
    );
}

fn run_scenario(spec: ScenarioSpec) -> ObservedOutcome {
    let vfs = build_two_transaction_wal(spec.seed);
    let cx = test_cx();
    apply_fault(spec, &vfs, &cx);

    let mut observed_frames = Vec::with_capacity(spec.restart_iterations);
    for _ in 0..spec.restart_iterations {
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::open(&cx, file).expect("open wal after fault");
        validate_recovered_prefix(spec, &mut wal, &cx);
        observed_frames.push(wal.frame_count());
        wal.close(&cx).expect("close wal after verification");
    }

    let first = observed_frames[0];
    assert!(
        observed_frames.iter().all(|count| *count == first),
        "scenario {} restart loop produced non-deterministic frame counts: {:?}",
        spec.scenario_id,
        observed_frames
    );

    ObservedOutcome {
        committed_frames: first,
        restart_iterations: spec.restart_iterations,
    }
}

fn scenario_by_kind(kind: FaultKind) -> ScenarioSpec {
    SCENARIOS
        .iter()
        .copied()
        .find(|scenario| scenario.kind == kind)
        .expect("scenario exists for fault kind")
}

fn emit_scenario_outcome(spec: ScenarioSpec, observed: ObservedOutcome) {
    println!(
        "SCENARIO_OUTCOME:{{\"scenario_id\":\"{}\",\"seed\":{},\"fault_kind\":\"{}\",\"decision\":\"keep_committed_prefix\",\"reason_code\":\"{}\",\"expected_committed_frames\":{},\"observed_committed_frames\":{},\"restart_iterations\":{},\"replay_command\":\"{}\"}}",
        spec.scenario_id,
        spec.seed,
        spec.kind.as_str(),
        spec.expected.reason_code,
        spec.expected.committed_frames,
        observed.committed_frames,
        observed.restart_iterations,
        REPLAY_COMMAND
    );
}

#[test]
fn unit_fault_scenario_catalog_is_complete() {
    assert_eq!(SCENARIOS.len(), 5, "exactly five required fault classes");

    let kinds = [
        FaultKind::MidCommitKill,
        FaultKind::RestartLoop,
        FaultKind::TornWrite,
        FaultKind::ChecksumCorruption,
        FaultKind::PartialFsync,
    ];
    for kind in kinds {
        assert!(
            SCENARIOS.iter().any(|scenario| scenario.kind == kind),
            "missing required scenario kind {}",
            kind.as_str()
        );
    }
}

#[test]
fn unit_expected_outcome_mapping_is_explicit() {
    for scenario in SCENARIOS {
        assert_eq!(
            scenario.expected,
            expected_for_kind(scenario.kind),
            "scenario {} must map to explicit expected policy",
            scenario.scenario_id
        );
        assert!(
            !scenario.expected.reason_code.is_empty(),
            "scenario {} must have non-empty reason_code",
            scenario.scenario_id
        );
    }
}

#[test]
fn scenario_mid_commit_kill_recovery() {
    let spec = scenario_by_kind(FaultKind::MidCommitKill);
    let observed = run_scenario(spec);
    assert_eq!(observed.committed_frames, spec.expected.committed_frames);
    emit_scenario_outcome(spec, observed);
}

#[test]
fn scenario_restart_loop_recovery() {
    let spec = scenario_by_kind(FaultKind::RestartLoop);
    let observed = run_scenario(spec);
    assert_eq!(observed.committed_frames, spec.expected.committed_frames);
    emit_scenario_outcome(spec, observed);
}

#[test]
fn scenario_torn_write_recovery() {
    let spec = scenario_by_kind(FaultKind::TornWrite);
    let observed = run_scenario(spec);
    assert_eq!(observed.committed_frames, spec.expected.committed_frames);
    emit_scenario_outcome(spec, observed);
}

#[test]
fn scenario_checksum_corruption_recovery() {
    let spec = scenario_by_kind(FaultKind::ChecksumCorruption);
    let observed = run_scenario(spec);
    assert_eq!(observed.committed_frames, spec.expected.committed_frames);
    emit_scenario_outcome(spec, observed);
}

#[test]
fn scenario_partial_fsync_recovery() {
    let spec = scenario_by_kind(FaultKind::PartialFsync);
    let observed = run_scenario(spec);
    assert_eq!(observed.committed_frames, spec.expected.committed_frames);
    emit_scenario_outcome(spec, observed);
}
