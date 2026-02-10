use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use asupersync::raptorq::decoder::{InactivationDecoder, ReceivedSymbol};
use asupersync::raptorq::systematic::SystematicEncoder;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use fsqlite_types::{
    ObjectId, Oti, SymbolRecord, SymbolRecordFlags, reconstruct_systematic_happy_path,
};
use fsqlite_wal::{verify_wal_fec_source_hash, wal_fec_source_hash_xxh3_128};

const BEAD_ID: &str = "bd-1eog";
const MTUISH_SYMBOL_SIZE: usize = 1366;
const PAGEISH_SYMBOL_SIZE: usize = 4096;
const SYMBOL_SIZES: [usize; 2] = [MTUISH_SYMBOL_SIZE, PAGEISH_SYMBOL_SIZE];
const FULL_K_SOURCE_AXIS: [usize; 6] = [1, 8, 32, 256, 1024, 4096];
const SMOKE_K_SOURCE_AXIS: [usize; 3] = [32, 256, 1024];
const FULL_LOSS_AXIS: [u32; 4] = [0, 5, 10, 20];
const SMOKE_LOSS_AXIS: [u32; 3] = [0, 10, 20];
const AUTH_KEY: [u8; 32] = [0xA5; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchMode {
    Smoke,
    Full,
}

impl BenchMode {
    fn detect() -> Self {
        let smoke_env = std::env::var("FSQLITE_BENCH_SMOKE")
            .ok()
            .is_some_and(|value| value != "0");
        if smoke_env || std::env::var("CI").is_ok() {
            Self::Smoke
        } else {
            Self::Full
        }
    }

    const fn k_axis(self) -> &'static [usize] {
        match self {
            Self::Smoke => &SMOKE_K_SOURCE_AXIS,
            Self::Full => &FULL_K_SOURCE_AXIS,
        }
    }

    const fn loss_axis(self) -> &'static [u32] {
        match self {
            Self::Smoke => &SMOKE_LOSS_AXIS,
            Self::Full => &FULL_LOSS_AXIS,
        }
    }

    const fn decode_latency_samples(self) -> usize {
        match self {
            Self::Smoke => 16,
            Self::Full => 64,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct MatrixCase {
    k_source: usize,
    symbol_size: usize,
    loss_percent: u32,
}

struct DecodeFixture {
    decoder: InactivationDecoder,
    received_symbols: Vec<ReceivedSymbol>,
    payload_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct LatencySummary {
    p95_ns: u64,
    p99_ns: u64,
}

fn criterion_config() -> Criterion {
    let mode = BenchMode::detect();
    let mut criterion = Criterion::default().configure_from_args();
    criterion = match mode {
        BenchMode::Smoke => criterion
            .sample_size(10)
            .warm_up_time(Duration::from_millis(100))
            .measurement_time(Duration::from_millis(250)),
        BenchMode::Full => criterion
            .sample_size(20)
            .warm_up_time(Duration::from_millis(400))
            .measurement_time(Duration::from_secs(2)),
    };
    criterion
}

fn deterministic_seed(case: MatrixCase) -> u64 {
    let mut seed = 0x9E37_79B9_7F4A_7C15_u64;
    seed ^= u64::try_from(case.k_source).expect("k_source fits in u64");
    seed = seed.rotate_left(17);
    seed ^= u64::try_from(case.symbol_size).expect("symbol_size fits in u64");
    seed = seed.rotate_left(9);
    seed ^= u64::from(case.loss_percent);
    seed
}

fn case_id(case: MatrixCase) -> String {
    format!(
        "K{}_T{}_L{}",
        case.k_source, case.symbol_size, case.loss_percent
    )
}

fn make_source_symbols(k_source: usize, symbol_size: usize) -> Vec<Vec<u8>> {
    (0..k_source)
        .map(|symbol_idx| {
            (0..symbol_size)
                .map(|byte_idx| {
                    let mixed = symbol_idx
                        .wrapping_mul(37)
                        .wrapping_add(byte_idx.wrapping_mul(13))
                        .wrapping_add(17)
                        % 256;
                    u8::try_from(mixed).expect("modulo 256 always fits in u8")
                })
                .collect()
        })
        .collect()
}

fn make_systematic_records(source_symbols: &[Vec<u8>], symbol_size: usize) -> Vec<SymbolRecord> {
    let payload_len = source_symbols
        .len()
        .checked_mul(symbol_size)
        .expect("payload length should not overflow");
    let payload_len_u64 = u64::try_from(payload_len).expect("payload length fits in u64");
    let symbol_size_u32 = u32::try_from(symbol_size).expect("symbol size fits in u32");
    let oti = Oti {
        f: payload_len_u64,
        al: 1,
        t: symbol_size_u32,
        z: 1,
        n: 1,
    };
    let object_id = ObjectId::derive_from_canonical_bytes(b"bd-1eog-systematic-records");

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
                u32::try_from(esi).expect("ESI fits in u32"),
                symbol.clone(),
                flags,
            )
        })
        .collect()
}

fn choose_loss_indices(k_source: usize, loss_percent: u32) -> BTreeSet<u32> {
    if k_source == 0 || loss_percent == 0 {
        return BTreeSet::new();
    }

    let numerator = k_source
        .checked_mul(usize::try_from(loss_percent).expect("loss percentage fits in usize"))
        .expect("loss numerator should not overflow");
    let mut target = numerator.div_ceil(100);
    target = target.clamp(1, k_source);

    let mut indices = BTreeSet::new();
    let k_source_u64 = u64::try_from(k_source).expect("k_source fits in u64");
    let mut state = 0xD1B5_4A32_u64;
    while indices.len() < target {
        state = state.wrapping_mul(1_103_515_245).wrapping_add(12_345);
        let idx_u64 = state % k_source_u64;
        let idx_u32 = u32::try_from(idx_u64).expect("index fits in u32");
        indices.insert(idx_u32);
    }
    indices
}

fn repair_symbol_count(k_source: usize, loss_percent: u32) -> usize {
    let drop_count = choose_loss_indices(k_source, loss_percent).len();
    if drop_count == 0 {
        2
    } else {
        drop_count.max(2)
    }
}

fn make_decode_fixture(case: MatrixCase) -> DecodeFixture {
    let source_symbols = make_source_symbols(case.k_source, case.symbol_size);
    let seed = deterministic_seed(case);
    let encoder = SystematicEncoder::new(&source_symbols, case.symbol_size, seed)
        .expect("encoder should be constructible for decode fixture");
    let decoder = InactivationDecoder::new(case.k_source, case.symbol_size, seed);
    let dropped = choose_loss_indices(case.k_source, case.loss_percent);

    let mut received_symbols = decoder.constraint_symbols();
    for (esi, symbol) in source_symbols.iter().enumerate() {
        let esi_u32 = u32::try_from(esi).expect("ESI fits in u32");
        if dropped.contains(&esi_u32) {
            continue;
        }
        received_symbols.push(ReceivedSymbol::source(esi_u32, symbol.clone()));
    }

    let drop_count = dropped.len();
    if drop_count > 0 {
        let mut offset = 0usize;
        let max_repair_count = case
            .k_source
            .saturating_add(decoder.params().s)
            .saturating_add(decoder.params().h)
            .saturating_add(drop_count)
            .saturating_add(16);

        while offset < max_repair_count {
            let esi = u32::try_from(case.k_source + offset).expect("repair ESI fits in u32");
            let (columns, coefficients) = decoder.repair_equation(esi);
            let repair_data = encoder.repair_symbol(esi);
            received_symbols.push(ReceivedSymbol::repair(
                esi,
                columns,
                coefficients,
                repair_data,
            ));
            if decoder.decode(&received_symbols).is_ok() {
                break;
            }
            offset = offset.saturating_add(1);
        }

        assert!(
            decoder.decode(&received_symbols).is_ok(),
            "unable to build decodable fixture for K={} T={} loss={} after {max_repair_count} repairs",
            case.k_source,
            case.symbol_size,
            case.loss_percent
        );
    }

    let payload_bytes = u64::try_from(
        case.k_source
            .checked_mul(case.symbol_size)
            .expect("payload size should not overflow"),
    )
    .expect("payload size fits in u64");

    DecodeFixture {
        decoder,
        received_symbols,
        payload_bytes,
    }
}

fn percentile(samples: &[u64], percentile_value: u32) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let rank = samples
        .len()
        .saturating_sub(1)
        .saturating_mul(usize::try_from(percentile_value).expect("percentile fits in usize"))
        .div_ceil(100);
    samples[rank]
}

fn sample_decode_latencies(
    decoder: &InactivationDecoder,
    received_symbols: &[ReceivedSymbol],
    sample_count: usize,
) -> LatencySummary {
    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let start = Instant::now();
        let decode_result = decoder
            .decode(received_symbols)
            .expect("decode fixture should always complete");
        black_box(&decode_result.source);
        let elapsed_ns_u128 = start.elapsed().as_nanos();
        let elapsed_ns = u64::try_from(elapsed_ns_u128).unwrap_or(u64::MAX);
        samples.push(elapsed_ns);
    }
    samples.sort_unstable();
    LatencySummary {
        p95_ns: percentile(&samples, 95),
        p99_ns: percentile(&samples, 99),
    }
}

#[allow(clippy::too_many_lines, clippy::significant_drop_tightening)]
fn bench_raptorq_matrix(c: &mut Criterion) {
    let mode = BenchMode::detect();

    let mut systematic_group = c.benchmark_group("bd-1eog/systematic_fast_path");
    for &k_source in mode.k_axis() {
        for &symbol_size in &SYMBOL_SIZES {
            let case = MatrixCase {
                k_source,
                symbol_size,
                loss_percent: 0,
            };
            let case_name = case_id(case);
            let source_symbols = make_source_symbols(k_source, symbol_size);
            let records = make_systematic_records(&source_symbols, symbol_size);
            let payload_bytes =
                u64::try_from(k_source.checked_mul(symbol_size).expect("payload overflow"))
                    .expect("payload fits in u64");
            systematic_group.throughput(Throughput::Bytes(payload_bytes));
            systematic_group.bench_with_input(
                BenchmarkId::new("reconstruct_systematic", case_name),
                &records,
                |b, records_ref| {
                    b.iter(|| {
                        let payload = reconstruct_systematic_happy_path(black_box(records_ref))
                            .expect("systematic reconstruction should succeed");
                        black_box(payload);
                    });
                },
            );
        }
    }
    systematic_group.finish();

    let mut repair_group = c.benchmark_group("bd-1eog/repair_symbol_generation");
    for &k_source in mode.k_axis() {
        for &symbol_size in &SYMBOL_SIZES {
            let source_symbols = make_source_symbols(k_source, symbol_size);
            for &loss_percent in mode.loss_axis() {
                let case = MatrixCase {
                    k_source,
                    symbol_size,
                    loss_percent,
                };
                let case_name = case_id(case);
                let seed = deterministic_seed(case);
                let encoder = SystematicEncoder::new(&source_symbols, symbol_size, seed)
                    .expect("encoder should construct for repair benchmark");
                let repair_count = repair_symbol_count(k_source, loss_percent);
                let generated_bytes = u64::try_from(
                    repair_count
                        .checked_mul(symbol_size)
                        .expect("generated byte count should not overflow"),
                )
                .expect("generated byte count fits in u64");
                repair_group.throughput(Throughput::Bytes(generated_bytes));
                repair_group.bench_with_input(
                    BenchmarkId::new("repair_symbols", case_name),
                    &repair_count,
                    |b, repair_count_ref| {
                        b.iter(|| {
                            let mut xor_acc = 0_u8;
                            for offset in 0..*repair_count_ref {
                                let esi = u32::try_from(k_source + offset)
                                    .expect("repair ESI fits in u32");
                                let symbol = encoder.repair_symbol(esi);
                                xor_acc ^= symbol[offset % symbol.len()];
                                black_box(symbol);
                            }
                            black_box(xor_acc);
                        });
                    },
                );
            }
        }
    }
    repair_group.finish();

    let mut decode_group = c.benchmark_group("bd-1eog/decode_paths");
    for &k_source in mode.k_axis() {
        for &symbol_size in &SYMBOL_SIZES {
            for &loss_percent in mode.loss_axis() {
                let case = MatrixCase {
                    k_source,
                    symbol_size,
                    loss_percent,
                };
                let case_name = case_id(case);
                let fixture = make_decode_fixture(case);
                let latency = sample_decode_latencies(
                    &fixture.decoder,
                    &fixture.received_symbols,
                    mode.decode_latency_samples(),
                );
                eprintln!(
                    "INFO bead_id={BEAD_ID} case=decode_latency matrix={} p95_ns={} p99_ns={} samples={}",
                    case_name,
                    latency.p95_ns,
                    latency.p99_ns,
                    mode.decode_latency_samples()
                );
                decode_group.throughput(Throughput::Bytes(fixture.payload_bytes));
                decode_group.bench_with_input(
                    BenchmarkId::new("decode", case_name),
                    &fixture,
                    |b, fixture_ref| {
                        b.iter(|| {
                            let decoded = fixture_ref
                                .decoder
                                .decode(black_box(&fixture_ref.received_symbols))
                                .expect("decode benchmark fixture should always succeed");
                            black_box(decoded.source);
                        });
                    },
                );
            }
        }
    }
    decode_group.finish();
}

fn bench_hash_and_auth_verification(c: &mut Criterion) {
    let mut group = c.benchmark_group("bd-1eog/hash_auth_verification");

    for &symbol_size in &SYMBOL_SIZES {
        let source_symbols = make_source_symbols(1, symbol_size);
        let payload = source_symbols
            .first()
            .expect("source symbol should exist")
            .clone();
        let expected_hash = wal_fec_source_hash_xxh3_128(&payload);
        let payload_bytes = u64::try_from(symbol_size).expect("symbol size fits in u64");
        group.throughput(Throughput::Bytes(payload_bytes));

        group.bench_with_input(
            BenchmarkId::new("verify_wal_fec_source_hash", symbol_size),
            &payload,
            |b, payload_ref| {
                b.iter(|| {
                    let verified = verify_wal_fec_source_hash(
                        black_box(payload_ref),
                        black_box(expected_hash),
                    );
                    black_box(verified);
                });
            },
        );

        let object_id = ObjectId::derive_from_canonical_bytes(b"bd-1eog-auth-bench");
        let symbol_size_u32 = u32::try_from(symbol_size).expect("symbol size fits in u32");
        let oti = Oti {
            f: payload_bytes,
            al: 1,
            t: symbol_size_u32,
            z: 1,
            n: 1,
        };
        let auth_record = SymbolRecord::new(
            object_id,
            oti,
            0,
            payload.clone(),
            SymbolRecordFlags::SYSTEMATIC_RUN_START,
        )
        .with_auth_tag(&AUTH_KEY);

        group.bench_with_input(
            BenchmarkId::new("verify_symbol_auth_tag", symbol_size),
            &auth_record,
            |b, record_ref| {
                b.iter(|| {
                    let verified = record_ref.verify_auth(&AUTH_KEY);
                    black_box(verified);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = criterion_config();
    targets = bench_raptorq_matrix, bench_hash_and_auth_verification
);
criterion_main!(benches);
