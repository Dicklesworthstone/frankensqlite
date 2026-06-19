use std::hint::black_box;

use asupersync::raptorq::decoder::{InactivationDecoder, ReceivedSymbol};
use asupersync::raptorq::systematic::SystematicEncoder;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fsqlite_core::replication_receiver::{PacketResult, ReplicationReceiver};
use fsqlite_core::replication_sender::{
    PageEntry, ReplicationPacket, ReplicationSender, SenderConfig,
};
use fsqlite_core::{symbol_add_assign, symbol_addmul_assign, xor_patch_wide_chunks};

fn fill_pattern(len: usize, a: u8, b: u8) -> Vec<u8> {
    (0..len)
        .map(|idx| {
            let idx_byte = u8::try_from(idx % 251).expect("modulo fits u8");
            idx_byte.wrapping_mul(a).wrapping_add(b)
        })
        .collect()
}

fn xor_patch_bytewise(dst: &mut [u8], patch: &[u8]) {
    for (dst_byte, patch_byte) in dst.iter_mut().zip(patch.iter()) {
        *dst_byte ^= *patch_byte;
    }
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

fn run_replication_roundtrip(packet_bytes: &[Vec<u8>]) -> usize {
    let mut receiver = ReplicationReceiver::new();
    for wire in packet_bytes {
        let result = receiver
            .process_packet(wire)
            .expect("replication packet processing should succeed");
        match result {
            PacketResult::DecodeReady => {
                let mut applied = receiver
                    .apply_pending()
                    .expect("decoded packets should be applicable");
                let decoded = applied
                    .pop()
                    .expect("decode-ready state should yield pages");
                return decoded
                    .pages
                    .iter()
                    .map(|page| page.page_data.len())
                    .sum::<usize>();
            }
            PacketResult::Accepted
            | PacketResult::Duplicate
            | PacketResult::NeedMore
            | PacketResult::Erasure => {}
        }
    }
    unreachable!("replication packet stream did not reach decode-ready state");
}

fn build_replication_packet_path(
    page_size: u32,
    page_count: usize,
    symbol_size: u16,
    max_isi_multiplier: u32,
) -> (Vec<Vec<u8>>, usize) {
    let mut sender = ReplicationSender::new();
    let mut pages = make_replication_pages(page_size, page_count);
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

    let mut all_packets = Vec::new();
    while let Some(packet) = sender
        .next_packet()
        .expect("packet generation should succeed")
    {
        all_packets.push(
            packet
                .to_bytes()
                .expect("wire packet encoding should succeed"),
        );
    }

    let mut source_packets = Vec::new();
    for wire in &all_packets {
        let packet = ReplicationPacket::from_bytes(wire).expect("wire packet should decode");
        if packet.is_source_symbol() {
            source_packets.push(wire.clone());
        }
    }

    let systematic_packets = source_packets.clone();
    let _decoded_systematic = run_replication_roundtrip(&systematic_packets);

    let payload_bytes = usize::try_from(page_size)
        .expect("page_size must fit usize")
        .checked_mul(page_count)
        .expect("payload byte count should fit");

    (systematic_packets, payload_bytes)
}

fn run_raptorq_decode_with_repair(
    symbol_size: usize,
    k_source: usize,
    dropped_source_symbols: usize,
) -> usize {
    let seed = 0x2600_0001_u64;
    let source_symbols = (0..k_source)
        .map(|index| {
            let a = u8::try_from((index + 3) % 251).expect("coefficient must fit u8");
            let b = u8::try_from((index + 17) % 251).expect("coefficient must fit u8");
            fill_pattern(symbol_size, a, b)
        })
        .collect::<Vec<_>>();

    let encoder = SystematicEncoder::new(&source_symbols, symbol_size, seed)
        .expect("encoder initialization should succeed");
    let decoder = InactivationDecoder::new(k_source, symbol_size, seed);

    let repair_symbols_needed = dropped_source_symbols + decoder.params().s + decoder.params().h;
    let mut received = decoder.constraint_symbols();
    for (index, symbol) in source_symbols
        .iter()
        .enumerate()
        .skip(dropped_source_symbols)
    {
        let esi = u32::try_from(index).expect("source ESI must fit u32");
        received.push(ReceivedSymbol::source(esi, symbol.clone()));
    }
    for offset in 0..repair_symbols_needed {
        let esi = u32::try_from(k_source + offset).expect("repair ESI must fit u32");
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

    let decoded_output = decoder
        .decode(&received)
        .expect("decode with repair symbols should succeed");
    decoded_output.source.iter().map(Vec::len).sum()
}

fn bench_symbol_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("symbol_ops");

    for symbol_len in [512_usize, 4096_usize] {
        let src = fill_pattern(symbol_len, 17, 29);
        let patch = fill_pattern(symbol_len, 7, 19);
        let mut dst = fill_pattern(symbol_len, 23, 31);

        let bytes = u64::try_from(symbol_len).expect("symbol length fits u64");
        group.throughput(Throughput::Bytes(bytes));

        group.bench_with_input(
            BenchmarkId::new("memcpy_baseline", symbol_len),
            &symbol_len,
            |b, _| {
                b.iter(|| {
                    dst.copy_from_slice(black_box(&src));
                    black_box(&dst);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("symbol_add", symbol_len),
            &symbol_len,
            |b, _| {
                b.iter(|| {
                    symbol_add_assign(&mut dst, black_box(&src)).expect("symbol_add_assign");
                    black_box(&dst);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("symbol_addmul_c53", symbol_len),
            &symbol_len,
            |b, _| {
                b.iter(|| {
                    symbol_addmul_assign(&mut dst, 0x53, black_box(&patch))
                        .expect("symbol_addmul_assign");
                    black_box(&dst);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("xor_patch_chunked", symbol_len),
            &symbol_len,
            |b, _| {
                b.iter(|| {
                    xor_patch_wide_chunks(&mut dst, black_box(&patch)).expect("xor_patch_chunked");
                    black_box(&dst);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("xor_patch_bytewise", symbol_len),
            &symbol_len,
            |b, _| {
                b.iter(|| {
                    xor_patch_bytewise(&mut dst, black_box(&patch));
                    black_box(&dst);
                });
            },
        );
    }

    group.finish();
}

fn bench_raptorq_paths(c: &mut Criterion) {
    let mut group = c.benchmark_group("raptorq_paths");

    let symbol_size = 4096_usize;
    let k_source = 8_usize;
    let payload_len = symbol_size
        .checked_mul(k_source)
        .expect("payload length should fit");
    let payload_len_u64 = u64::try_from(payload_len).expect("payload length should fit in u64");
    group.throughput(Throughput::Bytes(payload_len_u64));

    let source_symbols = (0..k_source)
        .map(|idx| {
            let seed = u8::try_from(idx + 1).expect("small index fits in u8");
            fill_pattern(symbol_size, seed.wrapping_mul(17), seed.wrapping_mul(29))
        })
        .collect::<Vec<_>>();
    let canonical_payload = source_symbols.concat();

    let mut page_io_buffer = vec![0_u8; payload_len];
    let mut systematic_out = vec![0_u8; payload_len];
    let mut decode_acc = vec![0_u8; symbol_size];

    group.bench_function("page_io_copy_baseline", |b| {
        b.iter(|| {
            page_io_buffer.copy_from_slice(black_box(&canonical_payload));
            black_box(&page_io_buffer);
        });
    });

    group.bench_function("systematic_fast_path_concat", |b| {
        b.iter(|| {
            for (index, symbol) in source_symbols.iter().enumerate() {
                let start = index
                    .checked_mul(symbol_size)
                    .expect("start offset should fit");
                let end = start
                    .checked_add(symbol_size)
                    .expect("end offset should fit");
                systematic_out[start..end].copy_from_slice(black_box(symbol));
            }
            black_box(&systematic_out);
        });
    });

    group.bench_function("decode_fallback_addmul", |b| {
        b.iter(|| {
            decode_acc.fill(0);
            for (index, symbol) in source_symbols.iter().enumerate() {
                let coeff = u8::try_from(index + 1).expect("small coefficient fits in u8");
                symbol_addmul_assign(&mut decode_acc, coeff, black_box(symbol))
                    .expect("symbol_addmul_assign");
            }
            black_box(&decode_acc);
        });
    });

    group.finish();
}

fn bench_replication_paths(c: &mut Criterion) {
    let page_size = 1024_u32;
    let page_count = 48_usize;
    let symbol_size = 512_u16;
    let max_isi_multiplier = 2_u32;
    let dropped_source_symbols = 4_usize;

    let (systematic_packets, payload_bytes) =
        build_replication_packet_path(page_size, page_count, symbol_size, max_isi_multiplier);
    let k_source = usize::try_from(
        ReplicationPacket::from_bytes(&systematic_packets[0])
            .expect("packet decode should succeed")
            .k_source,
    )
    .expect("k_source must fit usize");
    assert!(
        k_source > dropped_source_symbols,
        "k_source must exceed dropped source symbol count"
    );

    let mut group = c.benchmark_group("replication_paths");
    group.throughput(Throughput::Bytes(
        u64::try_from(payload_bytes).expect("payload bytes must fit u64"),
    ));

    group.bench_function("receiver_systematic_only", |b| {
        b.iter(|| {
            let decoded_bytes = run_replication_roundtrip(black_box(&systematic_packets));
            black_box(decoded_bytes);
        });
    });

    group.bench_function("raptorq_decode_with_repair", |b| {
        b.iter(|| {
            let decoded_bytes = run_raptorq_decode_with_repair(
                usize::from(symbol_size),
                k_source,
                dropped_source_symbols,
            );
            black_box(decoded_bytes);
        });
    });

    group.bench_function("sender_symbol_generation", |b| {
        b.iter(|| {
            let mut sender = ReplicationSender::new();
            let mut pages = make_replication_pages(page_size, page_count);
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

            let mut generated_bytes = 0_usize;
            while let Some(packet) = sender.next_packet().expect("packet generation should work") {
                generated_bytes = generated_bytes.saturating_add(packet.symbol_data.len());
            }
            black_box(generated_bytes);
        });
    });

    let auth_key = [0xA5_u8; 32];
    let mut tagged_packet =
        ReplicationPacket::from_bytes(&systematic_packets[0]).expect("packet decode");
    tagged_packet.attach_auth_tag(&auth_key);
    let tagged_wire = tagged_packet
        .to_bytes()
        .expect("packet encoding with auth tag");

    group.bench_function("packet_hash_auth_verify", |b| {
        b.iter(|| {
            let packet = ReplicationPacket::from_bytes(black_box(&tagged_wire))
                .expect("packet decode should succeed");
            let ok = packet.verify_integrity(Some(&auth_key));
            black_box(ok);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_symbol_ops,
    bench_raptorq_paths,
    bench_replication_paths
);
criterion_main!(benches);
