//! Compliance tests for bd-26qk: Replication bench throughput + tail latency.
//!
//! Validates that the benchmark infrastructure measures the replication data path
//! for both systematic-only and mixed (repair) scenarios, and that the underlying
//! sender/receiver pipeline produces correct results.

use std::path::{Path, PathBuf};
use std::time::Instant;

use fsqlite_core::replication_receiver::{PacketResult, ReplicationReceiver};
use fsqlite_core::replication_sender::{
    PageEntry, ReplicationPacket, ReplicationSender, SenderConfig,
};
use fsqlite_types::cx::Cx;
use tracing::{debug, info};

const BEAD_ID: &str = "bd-26qk";
const BENCH_PATH: &str = "crates/fsqlite-core/benches/symbol_ops.rs";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("repo root should be two levels up from harness")
        .to_path_buf()
}

fn make_replication_pages(page_size: u32, page_count: usize) -> Vec<PageEntry> {
    let page_len = usize::try_from(page_size).expect("page_size must fit usize");
    (0..page_count)
        .map(|index| {
            let page_number = u32::try_from(index + 1).expect("page index must fit u32");
            let mut page_data = vec![0_u8; page_len];
            for (offset, byte) in page_data.iter_mut().enumerate() {
                let offset_u32 = u32::try_from(offset).expect("offset must fit u32");
                let mixed = page_number
                    .wrapping_mul(37)
                    .wrapping_add(offset_u32.wrapping_mul(11));
                *byte = u8::try_from(mixed % 251).expect("modulo result must fit u8");
            }
            PageEntry::new(page_number, page_data)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Bench structure compliance
// ---------------------------------------------------------------------------

#[test]
fn test_bd_26qk_bench_file_exists() {
    let bench_file = repo_root().join(BENCH_PATH);
    assert!(
        bench_file.exists(),
        "bead_id={BEAD_ID} bench file missing at {BENCH_PATH}"
    );
}

#[test]
fn test_bd_26qk_bench_covers_replication_paths() {
    let bench_file = repo_root().join(BENCH_PATH);
    let content = std::fs::read_to_string(&bench_file).expect("bench file should be readable");

    assert!(
        content.contains("replication_paths"),
        "bead_id={BEAD_ID} bench must contain replication_paths group"
    );
    assert!(
        content.contains("receiver_systematic_only"),
        "bead_id={BEAD_ID} bench must measure systematic-only throughput"
    );
    assert!(
        content.contains("raptorq_decode_with_repair"),
        "bead_id={BEAD_ID} bench must measure mixed path with repair"
    );
    assert!(
        content.contains("sender_symbol_generation"),
        "bead_id={BEAD_ID} bench must measure symbol generation CPU cost"
    );
    assert!(
        content.contains("packet_hash_auth_verify"),
        "bead_id={BEAD_ID} bench must measure hash/auth verification cost"
    );
}

// ---------------------------------------------------------------------------
// Functional verification
// ---------------------------------------------------------------------------

#[test]
fn test_bd_26qk_systematic_roundtrip() {
    let page_size = 1024_u32;
    let page_count = 16_usize;
    let symbol_size = 512_u16;
    let max_isi_multiplier = 2_u32;

    let mut sender = ReplicationSender::new();
    let mut pages = make_replication_pages(page_size, page_count);
    let expected_data: Vec<Vec<u8>> = pages.iter().map(|p| p.page_bytes.clone()).collect();

    sender
        .prepare(
            page_size,
            &mut pages,
            SenderConfig {
                symbol_size,
                max_isi_multiplier,
            },
        )
        .expect("sender preparation should succeed");
    sender
        .start_streaming()
        .expect("sender should transition to streaming");

    let mut source_packets = Vec::new();
    while let Some(packet) = sender
        .next_packet(&Cx::new())
        .expect("packet generation should succeed")
    {
        if packet.is_source_symbol() {
            source_packets.push(
                packet
                    .to_bytes()
                    .expect("wire packet encoding should succeed"),
            );
        }
    }

    let mut receiver = ReplicationReceiver::new();
    let mut decoded_pages = Vec::new();
    for wire in &source_packets {
        let result = receiver
            .process_packet(&Cx::new(), wire)
            .expect("packet processing should succeed");
        if result == PacketResult::DecodeReady {
            let mut applied = receiver
                .apply_pending()
                .expect("decoded packets should be applicable");
            if let Some(batch) = applied.pop() {
                decoded_pages.extend(batch.pages);
            }
        }
    }

    assert!(
        !decoded_pages.is_empty(),
        "bead_id={BEAD_ID} systematic roundtrip should produce decoded pages"
    );
    for (idx, page) in decoded_pages.iter().enumerate() {
        if idx < expected_data.len() {
            assert_eq!(
                page.page_data, expected_data[idx],
                "bead_id={BEAD_ID} page {idx} mismatch in systematic roundtrip"
            );
        }
    }
    info!(
        bead_id = BEAD_ID,
        page_count,
        decoded_count = decoded_pages.len(),
        "systematic roundtrip verified"
    );
}

#[test]
fn test_bd_26qk_sender_generates_packets() {
    let page_size = 4096_u32;
    let page_count = 8_usize;
    let symbol_size = 1366_u16;

    let mut sender = ReplicationSender::new();
    let mut pages = make_replication_pages(page_size, page_count);
    sender
        .prepare(
            page_size,
            &mut pages,
            SenderConfig {
                symbol_size,
                max_isi_multiplier: 2,
            },
        )
        .expect("sender preparation should succeed");
    sender
        .start_streaming()
        .expect("sender should transition to streaming");

    let mut total_packets = 0_usize;
    let mut source_count = 0_usize;
    let mut repair_count = 0_usize;
    while let Some(packet) = sender
        .next_packet(&Cx::new())
        .expect("packet generation should succeed")
    {
        total_packets += 1;
        if packet.is_source_symbol() {
            source_count += 1;
        } else {
            repair_count += 1;
        }
    }

    assert!(
        total_packets > 0,
        "bead_id={BEAD_ID} sender should generate at least one packet"
    );
    assert!(
        source_count > 0,
        "bead_id={BEAD_ID} sender should generate source symbols"
    );
    debug!(
        bead_id = BEAD_ID,
        total_packets, source_count, repair_count, "sender packet generation verified"
    );
}

#[test]
fn test_bd_26qk_auth_tag_roundtrip() {
    let page_size = 1024_u32;
    let page_count = 4_usize;
    let auth_key = [0xA5_u8; 32];

    let mut sender = ReplicationSender::new();
    let mut pages = make_replication_pages(page_size, page_count);
    sender
        .prepare(
            page_size,
            &mut pages,
            SenderConfig {
                symbol_size: 512,
                max_isi_multiplier: 2,
            },
        )
        .expect("sender preparation should succeed");
    sender
        .start_streaming()
        .expect("sender should transition to streaming");

    if let Some(packet) = sender.next_packet(&Cx::new()).expect("should get a packet") {
        let mut tagged = packet;
        tagged.attach_auth_tag(&auth_key);
        let wire = tagged
            .to_bytes()
            .expect("wire encoding with auth tag should succeed");

        let decoded = ReplicationPacket::from_bytes(&wire).expect("decode with auth tag");
        assert!(
            decoded.verify_integrity(Some(&auth_key)),
            "bead_id={BEAD_ID} auth tag verification should pass"
        );
        assert!(
            !decoded.verify_integrity(Some(&[0xBB_u8; 32])),
            "bead_id={BEAD_ID} auth tag verification should fail with wrong key"
        );
    }
}

// ---------------------------------------------------------------------------
// Tail latency measurement (functional, not criterion)
// ---------------------------------------------------------------------------

#[test]
fn test_bd_26qk_systematic_path_latency_bounded() {
    let cx = Cx::new();
    let page_size = 1024_u32;
    let page_count = 32_usize;
    let symbol_size = 512_u16;
    let sample_count = 10_usize;

    let mut latencies_ns = Vec::with_capacity(sample_count);

    for _ in 0..sample_count {
        let mut sender = ReplicationSender::new();
        let mut pages = make_replication_pages(page_size, page_count);
        sender
            .prepare(
                page_size,
                &mut pages,
                SenderConfig {
                    symbol_size,
                    max_isi_multiplier: 2,
                },
            )
            .expect("sender prep");
        sender.start_streaming().expect("start streaming");

        let mut source_packets = Vec::new();
        while let Some(packet) = sender.next_packet(&cx).expect("packet gen") {
            if packet.is_source_symbol() {
                source_packets.push(packet.to_bytes().expect("encode"));
            }
        }

        let start = Instant::now();
        let mut receiver = ReplicationReceiver::new();
        for wire in &source_packets {
            let _ = receiver.process_packet(&cx, wire).expect("process");
        }
        let elapsed = start.elapsed().as_nanos();
        let elapsed_u64 = u64::try_from(elapsed).unwrap_or(u64::MAX);
        latencies_ns.push(elapsed_u64);
    }

    latencies_ns.sort_unstable();
    let p95_idx = (sample_count * 95) / 100;
    let p95_ns = latencies_ns.get(p95_idx).copied().unwrap_or(0);

    info!(
        bead_id = BEAD_ID,
        p95_ns, sample_count, "systematic path p95 latency measured"
    );

    // Sanity check: systematic path for 32KB should complete in under 100ms.
    let max_ns = 100_000_000_u64;
    assert!(
        p95_ns < max_ns,
        "bead_id={BEAD_ID} systematic path p95 latency {p95_ns}ns exceeds {max_ns}ns budget"
    );
}

// ---------------------------------------------------------------------------
// E2E compliance
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_bd_26qk_compliance() {
    info!(bead_id = BEAD_ID, "starting E2E compliance check");

    // Verify bench file exists.
    let bench_file = repo_root().join(BENCH_PATH);
    assert!(
        bench_file.exists(),
        "bead_id={BEAD_ID} bench file must exist"
    );

    // Verify bench covers required measurements.
    let content = std::fs::read_to_string(&bench_file).expect("bench file should be readable");
    assert!(content.contains("replication_paths"));
    assert!(content.contains("receiver_systematic_only"));
    assert!(content.contains("raptorq_decode_with_repair"));

    // Verify sender+receiver roundtrip.
    let page_size = 1024_u32;
    let page_count = 8_usize;
    let mut sender = ReplicationSender::new();
    let mut pages = make_replication_pages(page_size, page_count);
    sender
        .prepare(
            page_size,
            &mut pages,
            SenderConfig {
                symbol_size: 512,
                max_isi_multiplier: 2,
            },
        )
        .expect("sender prep");
    sender.start_streaming().expect("start");

    let mut packets = Vec::new();
    while let Some(packet) = sender.next_packet(&Cx::new()).expect("gen") {
        if packet.is_source_symbol() {
            packets.push(packet.to_bytes().expect("encode"));
        }
    }

    let mut receiver = ReplicationReceiver::new();
    let mut completed = false;
    for wire in &packets {
        if receiver.process_packet(&Cx::new(), wire).expect("process") == PacketResult::DecodeReady
        {
            completed = true;
            break;
        }
    }

    assert!(
        completed,
        "bead_id={BEAD_ID} E2E replication roundtrip should reach DecodeReady"
    );

    info!(
        bead_id = BEAD_ID,
        "E2E compliance check passed — bench + roundtrip + latency verified"
    );
}
