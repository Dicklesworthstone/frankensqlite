use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;

use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_vfs::MemoryVfs;
use fsqlite_vfs::traits::{Vfs, VfsFile};
use fsqlite_wal::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalFile, WalSalts};

const PAGE_SIZE: u32 = 4096;
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-wal --test bd_26631_1_deterministic_replay_startup -- --nocapture --test-threads=1";
const RESTART_ITERATIONS: usize = 5;

const TWO_TXN_FRAMES: [(u32, u32, u8); 6] = [
    (1, 0, 1),
    (2, 0, 2),
    (3, 3, 3),
    (4, 0, 4),
    (5, 0, 5),
    (6, 6, 6),
];

const DUPLICATE_FRAMES: [(u32, u32, u8); 6] = [
    (7, 0, 1),
    (7, 0, 2),
    (7, 3, 3),
    (7, 0, 4),
    (7, 0, 5),
    (7, 6, 6),
];

const UNCOMMITTED_TAIL_FRAMES: [(u32, u32, u8); 5] =
    [(1, 0, 1), (2, 0, 2), (3, 3, 3), (4, 0, 4), (5, 0, 5)];

#[derive(Debug, Clone, PartialEq, Eq)]
struct FrameSnapshot {
    page_number: u32,
    db_size: u32,
    is_commit: bool,
    data: Vec<u8>,
}

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
        salt1: 0xDA7A_0000 ^ low,
        salt2: 0xC0DE_0000 ^ shifted,
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
        .open(cx, Some(Path::new("bd_26631_1.db-wal")), flags)
        .expect("open wal file");
    file
}

fn write_frames(vfs: &MemoryVfs, cx: &Cx, seed: u64, frames: &[(u32, u32, u8)]) {
    let file = open_wal_file(vfs, cx);
    let mut wal = WalFile::create(cx, file, PAGE_SIZE, 0, test_salts(seed)).expect("create wal");
    for (page_number, db_size, marker) in frames {
        wal.append_frame(
            cx,
            *page_number,
            &sample_page(seed, *page_number, *marker),
            *db_size,
        )
        .expect("append frame");
    }
    wal.close(cx).expect("close wal");
}

fn frame_size() -> usize {
    WAL_FRAME_HEADER_SIZE + page_size_usize()
}

fn truncate_wal(vfs: &MemoryVfs, cx: &Cx, cut_at: usize) {
    let mut file = open_wal_file(vfs, cx);
    let cut_at_u64 = u64::try_from(cut_at).expect("cut_at fits u64");
    file.truncate(cx, cut_at_u64).expect("truncate wal");
}

fn flip_byte(vfs: &MemoryVfs, cx: &Cx, offset: usize, xor_mask: u8) {
    let mut file = open_wal_file(vfs, cx);
    let offset_u64 = u64::try_from(offset).expect("offset fits u64");
    let mut byte = [0_u8; 1];
    file.read(cx, &mut byte, offset_u64).expect("read byte");
    byte[0] ^= xor_mask;
    file.write(cx, &byte, offset_u64)
        .expect("write flipped byte");
}

fn collect_snapshot(vfs: &MemoryVfs, cx: &Cx) -> (usize, Option<usize>, Vec<FrameSnapshot>) {
    let file = open_wal_file(vfs, cx);
    let mut wal = WalFile::open(cx, file).expect("open wal");
    let frame_count = wal.frame_count();
    let last_commit = wal.last_commit_frame(cx).expect("last commit");
    let mut frames = Vec::with_capacity(frame_count);
    for idx in 0..frame_count {
        let (header, data) = wal.read_frame(cx, idx).expect("read frame");
        frames.push(FrameSnapshot {
            page_number: header.page_number,
            db_size: header.db_size,
            is_commit: header.is_commit(),
            data,
        });
    }
    wal.close(cx).expect("close wal");
    (frame_count, last_commit, frames)
}

fn assert_snapshot_matches(seed: u64, observed: &[FrameSnapshot], expected: &[(u32, u32, u8)]) {
    assert_eq!(observed.len(), expected.len(), "snapshot length mismatch");
    for (idx, frame) in observed.iter().enumerate() {
        let (page_number, db_size, marker) = expected[idx];
        assert_eq!(frame.page_number, page_number, "frame {idx} page number");
        assert_eq!(frame.db_size, db_size, "frame {idx} db_size");
        assert_eq!(frame.is_commit, db_size > 0, "frame {idx} commit flag");
        assert_eq!(
            frame.data,
            sample_page(seed, page_number, marker),
            "frame {idx} page payload",
        );
    }
}

fn snapshot_digest(frames: &[FrameSnapshot]) -> u64 {
    let mut hasher = DefaultHasher::new();
    frames.len().hash(&mut hasher);
    for frame in frames {
        frame.page_number.hash(&mut hasher);
        frame.db_size.hash(&mut hasher);
        frame.is_commit.hash(&mut hasher);
        frame.data.hash(&mut hasher);
    }
    hasher.finish()
}

fn assert_restart_determinism(
    vfs: &MemoryVfs,
    cx: &Cx,
    expected_frame_count: usize,
    expected_last_commit: Option<usize>,
) -> (Vec<FrameSnapshot>, u64) {
    let (count0, commit0, frames0) = collect_snapshot(vfs, cx);
    assert_eq!(count0, expected_frame_count, "initial frame count");
    assert_eq!(commit0, expected_last_commit, "initial last commit");

    for attempt in 1..RESTART_ITERATIONS {
        let (count, commit, frames) = collect_snapshot(vfs, cx);
        assert_eq!(count, count0, "restart attempt {attempt} frame count");
        assert_eq!(commit, commit0, "restart attempt {attempt} last commit");
        assert_eq!(frames, frames0, "restart attempt {attempt} snapshot");
    }

    let digest = snapshot_digest(&frames0);
    (frames0, digest)
}

fn emit_outcome(
    scenario_id: &str,
    seed: u64,
    reason_code: &str,
    expected_committed_frames: usize,
    observed_committed_frames: usize,
    last_commit_frame: Option<usize>,
    replay_digest: u64,
) {
    println!(
        "SCENARIO_OUTCOME:{{\"scenario_id\":\"{scenario_id}\",\"seed\":{seed},\"reason_code\":\"{reason_code}\",\"expected_committed_frames\":{expected_committed_frames},\"observed_committed_frames\":{observed_committed_frames},\"last_commit_frame\":{},\"restart_iterations\":{RESTART_ITERATIONS},\"replay_digest\":\"{replay_digest:016x}\",\"replay_command\":\"{REPLAY_COMMAND}\"}}",
        last_commit_frame
            .map(|idx| idx.to_string())
            .unwrap_or_else(|| "null".to_owned()),
    );
}

#[test]
fn scenario_truncated_tail_recovers_last_valid_commit() {
    let scenario_id = "WAL-REPLAY-STARTUP-TRUNCATED-TAIL";
    let seed = 2663101_u64;
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    write_frames(&vfs, &cx, seed, &TWO_TXN_FRAMES);

    let cut_at = WAL_HEADER_SIZE + 5 * frame_size() + (frame_size() / 2);
    truncate_wal(&vfs, &cx, cut_at);

    let (frames, digest) = assert_restart_determinism(&vfs, &cx, 3, Some(2));
    assert_snapshot_matches(seed, &frames, &TWO_TXN_FRAMES[..3]);
    emit_outcome(
        scenario_id,
        seed,
        "truncated_tail_stop",
        3,
        frames.len(),
        Some(2),
        digest,
    );
}

#[test]
fn scenario_duplicate_frames_remain_deterministic() {
    let scenario_id = "WAL-REPLAY-STARTUP-DUPLICATE-FRAMES";
    let seed = 2663102_u64;
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    write_frames(&vfs, &cx, seed, &DUPLICATE_FRAMES);

    let (frames, digest) = assert_restart_determinism(&vfs, &cx, 6, Some(5));
    assert_snapshot_matches(seed, &frames, &DUPLICATE_FRAMES);
    assert!(
        frames.iter().all(|frame| frame.page_number == 7),
        "duplicate page-number frames should all be preserved",
    );
    emit_outcome(
        scenario_id,
        seed,
        "accept_commit",
        6,
        frames.len(),
        Some(5),
        digest,
    );
}

#[test]
fn scenario_commit_boundary_drops_uncommitted_tail() {
    let scenario_id = "WAL-REPLAY-STARTUP-COMMIT-BOUNDARY";
    let seed = 2663103_u64;
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    write_frames(&vfs, &cx, seed, &UNCOMMITTED_TAIL_FRAMES);

    let (frames, digest) = assert_restart_determinism(&vfs, &cx, 3, Some(2));
    assert_snapshot_matches(seed, &frames, &UNCOMMITTED_TAIL_FRAMES[..3]);
    assert_eq!(
        frames
            .iter()
            .map(|frame| frame.is_commit)
            .collect::<Vec<_>>(),
        vec![false, false, true],
        "commit boundary map",
    );
    emit_outcome(
        scenario_id,
        seed,
        "commit_boundary",
        3,
        frames.len(),
        Some(2),
        digest,
    );
}

#[test]
fn scenario_restart_loop_with_corruption_is_deterministic() {
    let scenario_id = "WAL-REPLAY-STARTUP-RESTART-LOOP";
    let seed = 2663104_u64;
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    write_frames(&vfs, &cx, seed, &TWO_TXN_FRAMES);

    let corruption_offset = WAL_HEADER_SIZE + 4 * frame_size() + WAL_FRAME_HEADER_SIZE + 17;
    flip_byte(&vfs, &cx, corruption_offset, 0x01);

    let (frames, digest) = assert_restart_determinism(&vfs, &cx, 3, Some(2));
    assert_snapshot_matches(seed, &frames, &TWO_TXN_FRAMES[..3]);
    emit_outcome(
        scenario_id,
        seed,
        "checksum_mismatch_stop",
        3,
        frames.len(),
        Some(2),
        digest,
    );
}

#[test]
fn unit_scenario_contract_is_stable() {
    assert!(REPLAY_COMMAND.contains("bd_26631_1_deterministic_replay_startup"));
    assert!(REPLAY_COMMAND.contains("--test-threads=1"));
    assert_eq!(RESTART_ITERATIONS, 5);
}
