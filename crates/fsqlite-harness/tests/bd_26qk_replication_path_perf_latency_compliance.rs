use std::time::Instant;

use asupersync::raptorq::decoder::{InactivationDecoder, ReceivedSymbol};
use asupersync::raptorq::systematic::SystematicEncoder;
use fsqlite_core::replication_receiver::{PacketResult, ReplicationReceiver};
use fsqlite_core::replication_sender::{
    PageEntry, ReplicationPacket, ReplicationSender, SenderConfig,
};
use tracing::info;

const BEAD_ID: &str = "bd-26qk";
const PAGE_SIZE: u32 = 1024;
const PAGE_COUNT: usize = 48;
const SYMBOL_SIZE: u16 = 512;
const MAX_ISI_MULTIPLIER: u32 = 2;
const DROPPED_SOURCE_SYMBOLS: usize = 4;
const TAIL_RUNS: usize = 80;
const COMPONENT_RUNS: usize = 40;
const AUTH_VERIFY_ITERATIONS: usize = 512;
const RAPTORQ_K_SOURCE: usize = 64;
const RAPTORQ_SYMBOL_SIZE: usize = 512;
const RAPTORQ_SEED: u64 = 0x2600_0002;

fn make_pages(page_size: u32, page_count: usize) -> Vec<PageEntry> {
    let page_len = usize::try_from(page_size).expect("page_size must fit usize");
    (0..page_count)
        .map(|index| {
            let page_number = u32::try_from(index + 1).expect("page index must fit u32");
            let mut page_bytes = vec![0_u8; page_len];
            for (offset, byte) in page_bytes.iter_mut().enumerate() {
                let offset_u32 = u32::try_from(offset).expect("offset must fit u32");
                let mixed = page_number
                    .wrapping_mul(53)
                    .wrapping_add(offset_u32.wrapping_mul(19));
                *byte = u8::try_from(mixed % 251).expect("modulo result must fit u8");
            }
            PageEntry::new(page_number, page_bytes)
        })
        .collect()
}

fn generate_all_packets() -> Vec<Vec<u8>> {
    let mut sender = ReplicationSender::new();
    let mut pages = make_pages(PAGE_SIZE, PAGE_COUNT);
    sender
        .prepare(
            PAGE_SIZE,
            &mut pages,
            SenderConfig {
                symbol_size: SYMBOL_SIZE,
                max_isi_multiplier: MAX_ISI_MULTIPLIER,
            },
        )
        .expect("sender preparation should succeed");
    sender
        .start_streaming()
        .expect("sender should enter streaming state");

    let mut packets = Vec::new();
    while let Some(packet) = sender
        .next_packet()
        .expect("packet generation should succeed")
    {
        packets.push(packet.to_bytes().expect("packet encoding should succeed"));
    }
    packets
}

fn build_systematic_packets(all_packets: &[Vec<u8>]) -> Vec<Vec<u8>> {
    all_packets
        .iter()
        .filter_map(|wire| {
            let packet = ReplicationPacket::from_bytes(wire).expect("wire packet should decode");
            packet.is_source_symbol().then(|| wire.clone())
        })
        .collect()
}

fn run_receiver_decode_latency_ns(packet_bytes: &[Vec<u8>]) -> (u128, usize) {
    let mut receiver = ReplicationReceiver::new();
    let started = Instant::now();
    for wire in packet_bytes {
        let result = receiver
            .process_packet(wire)
            .expect("packet processing should succeed");
        match result {
            PacketResult::DecodeReady => {
                let mut applied = receiver
                    .apply_pending()
                    .expect("decode-ready path should apply");
                let decoded = applied
                    .pop()
                    .expect("apply_pending should return one result");
                let decoded_bytes = decoded
                    .pages
                    .iter()
                    .map(|page| page.page_data.len())
                    .sum::<usize>();
                return (started.elapsed().as_nanos(), decoded_bytes);
            }
            PacketResult::Accepted
            | PacketResult::Duplicate
            | PacketResult::NeedMore
            | PacketResult::Erasure => {}
        }
    }
    panic!("decode path never reached PacketResult::DecodeReady");
}

struct RaptorqDecodeFixture {
    source_symbols: Vec<Vec<u8>>,
    encoder: SystematicEncoder,
    decoder: InactivationDecoder,
    dropped_source_symbols: usize,
}

fn build_raptorq_decode_fixture() -> RaptorqDecodeFixture {
    let source_symbols = (0..RAPTORQ_K_SOURCE)
        .map(|index| {
            let a = u8::try_from((index + 3) % 251).expect("coefficient must fit u8");
            let b = u8::try_from((index + 17) % 251).expect("coefficient must fit u8");
            (0..RAPTORQ_SYMBOL_SIZE)
                .map(|offset| {
                    let offset_u8 = u8::try_from(offset % 251).expect("offset must fit u8");
                    offset_u8.wrapping_mul(a).wrapping_add(b)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let encoder = SystematicEncoder::new(&source_symbols, RAPTORQ_SYMBOL_SIZE, RAPTORQ_SEED)
        .expect("encoder initialization should succeed");
    let decoder = InactivationDecoder::new(RAPTORQ_K_SOURCE, RAPTORQ_SYMBOL_SIZE, RAPTORQ_SEED);
    RaptorqDecodeFixture {
        source_symbols,
        encoder,
        decoder,
        dropped_source_symbols: DROPPED_SOURCE_SYMBOLS,
    }
}

fn run_raptorq_decode_latency_ns(fixture: &RaptorqDecodeFixture) -> (u128, usize) {
    let started = Instant::now();
    let mut received = fixture.decoder.constraint_symbols();
    for (index, symbol) in fixture
        .source_symbols
        .iter()
        .enumerate()
        .skip(fixture.dropped_source_symbols)
    {
        let esi = u32::try_from(index).expect("source ESI must fit u32");
        received.push(ReceivedSymbol::source(esi, symbol.clone()));
    }

    let repair_symbols_needed =
        fixture.dropped_source_symbols + fixture.decoder.params().s + fixture.decoder.params().h;
    for offset in 0..repair_symbols_needed {
        let esi = u32::try_from(RAPTORQ_K_SOURCE + offset).expect("repair ESI must fit u32");
        let (columns, coefficients) = fixture
            .decoder
            .repair_equation_rfc6330(esi)
            .expect("repair symbol should have an RFC6330 repair equation");
        let repair_data = fixture.encoder.repair_symbol(esi);
        received.push(ReceivedSymbol::repair(
            esi,
            columns,
            coefficients,
            repair_data,
        ));
    }

    let decoded = fixture
        .decoder
        .decode(&received)
        .expect("repair decode should succeed");
    let decoded_bytes = decoded.source.iter().map(Vec::len).sum::<usize>();
    (started.elapsed().as_nanos(), decoded_bytes)
}

fn percentile_ns(samples: &[u128], percentile: usize) -> u128 {
    assert!(!samples.is_empty(), "percentile requires non-empty samples");
    assert!(percentile <= 100, "percentile must be <= 100");
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let len = sorted.len();
    let index = (len.saturating_sub(1) * percentile).div_ceil(100);
    sorted[index]
}

fn throughput_mib_per_s(total_bytes: usize, total_elapsed_ns: u128) -> f64 {
    if total_elapsed_ns == 0 {
        return 0.0;
    }
    let seconds = total_elapsed_ns as f64 / 1_000_000_000.0;
    let mib = total_bytes as f64 / (1024.0 * 1024.0);
    mib / seconds
}

#[test]
fn test_replication_tail_latency_and_throughput_paths() {
    let all_packets = generate_all_packets();
    let systematic_packets = build_systematic_packets(&all_packets);
    assert!(
        !systematic_packets.is_empty(),
        "bead_id={BEAD_ID} case=systematic_packets_empty"
    );

    let decode_fixture = build_raptorq_decode_fixture();

    let mut systematic_latencies_ns = Vec::with_capacity(TAIL_RUNS);
    let mut decode_latencies_ns = Vec::with_capacity(TAIL_RUNS);
    let mut systematic_payload_bytes = 0_usize;
    let mut decode_payload_bytes = 0_usize;

    for _ in 0..TAIL_RUNS {
        let (latency_ns, decoded_bytes) = run_receiver_decode_latency_ns(&systematic_packets);
        systematic_latencies_ns.push(latency_ns);
        systematic_payload_bytes = decoded_bytes;
    }
    for _ in 0..TAIL_RUNS {
        let (latency_ns, decoded_bytes) = run_raptorq_decode_latency_ns(&decode_fixture);
        decode_latencies_ns.push(latency_ns);
        decode_payload_bytes = decoded_bytes;
    }

    assert!(
        systematic_payload_bytes > 0 && decode_payload_bytes > 0,
        "bead_id={BEAD_ID} case=payload_nonzero"
    );

    let systematic_p95_ns = percentile_ns(&systematic_latencies_ns, 95);
    let systematic_p99_ns = percentile_ns(&systematic_latencies_ns, 99);
    let decode_p95_ns = percentile_ns(&decode_latencies_ns, 95);
    let decode_p99_ns = percentile_ns(&decode_latencies_ns, 99);

    let systematic_total_ns = systematic_latencies_ns.iter().copied().sum::<u128>();
    let decode_total_ns = decode_latencies_ns.iter().copied().sum::<u128>();
    let systematic_throughput_mib_per_s = throughput_mib_per_s(
        systematic_payload_bytes.saturating_mul(TAIL_RUNS),
        systematic_total_ns,
    );
    let decode_throughput_mib_per_s = throughput_mib_per_s(
        decode_payload_bytes.saturating_mul(TAIL_RUNS),
        decode_total_ns,
    );

    info!(
        bead_id = BEAD_ID,
        page_size = PAGE_SIZE,
        page_count = PAGE_COUNT,
        symbol_size = SYMBOL_SIZE,
        dropped_source_symbols = DROPPED_SOURCE_SYMBOLS,
        systematic_p95_ns = u64::try_from(systematic_p95_ns).unwrap_or(u64::MAX),
        systematic_p99_ns = u64::try_from(systematic_p99_ns).unwrap_or(u64::MAX),
        decode_p95_ns = u64::try_from(decode_p95_ns).unwrap_or(u64::MAX),
        decode_p99_ns = u64::try_from(decode_p99_ns).unwrap_or(u64::MAX),
        systematic_throughput_mib_per_s,
        decode_throughput_mib_per_s,
        "replication path throughput + tail latency summary"
    );

    assert!(
        systematic_p99_ns > 0 && decode_p99_ns > 0,
        "bead_id={BEAD_ID} case=tail_latency_nonzero"
    );
    assert!(
        systematic_throughput_mib_per_s > 0.0 && decode_throughput_mib_per_s > 0.0,
        "bead_id={BEAD_ID} case=throughput_nonzero"
    );

    let decode_p99_bound_ns = systematic_p99_ns
        .saturating_mul(900)
        .saturating_add(u128::from(100_000_000_u64));
    assert!(
        decode_p99_ns <= decode_p99_bound_ns,
        "bead_id={BEAD_ID} case=decode_tail_latency_unbounded decode_p99_ns={decode_p99_ns} bound_ns={decode_p99_bound_ns}"
    );
}

#[test]
fn test_replication_component_cost_reporting() {
    let all_packets = generate_all_packets();
    let systematic_packets = build_systematic_packets(&all_packets);
    assert!(
        !systematic_packets.is_empty(),
        "bead_id={BEAD_ID} case=systematic_packets_empty_component_test"
    );
    let decode_fixture = build_raptorq_decode_fixture();

    let mut sender_generation_samples_ns = Vec::with_capacity(COMPONENT_RUNS);
    for _ in 0..COMPONENT_RUNS {
        let started = Instant::now();
        let generated_packets = generate_all_packets();
        sender_generation_samples_ns.push(started.elapsed().as_nanos());
        assert!(
            !generated_packets.is_empty(),
            "bead_id={BEAD_ID} case=sender_generation_packets_empty"
        );
    }

    let auth_key = [0x5A_u8; 32];
    let mut packet = ReplicationPacket::from_bytes(&systematic_packets[0])
        .expect("packet decode should succeed");
    packet.attach_auth_tag(&auth_key);
    let tagged_wire = packet.to_bytes().expect("packet encoding should succeed");

    let mut hash_auth_samples_ns = Vec::with_capacity(COMPONENT_RUNS);
    for _ in 0..COMPONENT_RUNS {
        let started = Instant::now();
        for _ in 0..AUTH_VERIFY_ITERATIONS {
            let parsed =
                ReplicationPacket::from_bytes(&tagged_wire).expect("packet decode should succeed");
            assert!(
                parsed.verify_integrity(Some(&auth_key)),
                "bead_id={BEAD_ID} case=auth_integrity_check_failed"
            );
        }
        hash_auth_samples_ns.push(started.elapsed().as_nanos());
    }

    let mut decode_samples_ns = Vec::with_capacity(COMPONENT_RUNS);
    for _ in 0..COMPONENT_RUNS {
        let (latency_ns, decoded_bytes) = run_raptorq_decode_latency_ns(&decode_fixture);
        assert!(
            decoded_bytes > 0,
            "bead_id={BEAD_ID} case=decode_component_zero_bytes"
        );
        decode_samples_ns.push(latency_ns);
    }

    let sender_p95_ns = percentile_ns(&sender_generation_samples_ns, 95);
    let hash_auth_p95_ns = percentile_ns(&hash_auth_samples_ns, 95);
    let decode_p95_ns = percentile_ns(&decode_samples_ns, 95);

    info!(
        bead_id = BEAD_ID,
        sender_generation_p95_ns = u64::try_from(sender_p95_ns).unwrap_or(u64::MAX),
        hash_auth_p95_ns = u64::try_from(hash_auth_p95_ns).unwrap_or(u64::MAX),
        decode_p95_ns = u64::try_from(decode_p95_ns).unwrap_or(u64::MAX),
        auth_verify_iterations = AUTH_VERIFY_ITERATIONS,
        "replication component cost summary (generation/hash+auth/decode)"
    );

    assert!(
        sender_p95_ns > 0 && hash_auth_p95_ns > 0 && decode_p95_ns > 0,
        "bead_id={BEAD_ID} case=component_costs_nonzero"
    );
}
