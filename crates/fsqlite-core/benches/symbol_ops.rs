use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
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

criterion_group!(benches, bench_symbol_ops);
criterion_main!(benches);
