//! Single-connection fast page array: the flat `Vec<Option<CachedPageEntry>>`
//! slot index (legacy) against the head-plus-blocks index (production), in
//! one binary, through identical operation streams.
//!
//! Every read-only file connection runs the fast path, and the flat index
//! held one inline 88-byte slot for every page number below the highest page
//! the connection ever cached, writing all of them on growth. One read near
//! the end of a 54,020,823-page (221 GB) database therefore made 4.75 GB
//! resident per connection: nine of them (cass's lexical-rebuild packet
//! producer and eight page-prep workers) held 42.8 GB before the first
//! document was indexed.
//!
//! Cells:
//! - `memory`: one read of page N, the RSS it adds and the index bytes held.
//! - `scan`: the rebuild reader's shape, from connection open. One read of
//!   the last page, then runs of consecutive pages at seeded offsets over a
//!   wide file, a bounded resident budget, and buffer reuse through
//!   `take_any_clean_from_pool` once the budget is full.
//! - `dense-head` / `dense-blocks`: seeded random hits over a dense resident
//!   set inside the inline head (the `:memory:` shape) and past it (a large
//!   file's hot set). Arms are built before timing; only reads are timed.
//! - `txn-head` / `txn-blocks`: the DML shape. Admit a few pages, read them,
//!   `clear`, repeat; inside the head and past it.
//!
//! Before anything is timed, both arms must return the same checksum (bytes
//! read, victims chosen, residents counted) for the same stream. Timed cells
//! report each arm's p50 [min, max] and the paired median ratio
//! `sparse / legacy` (`< 1.0` is faster), beside an A/A null
//! (`legacy / legacy`, two separately built legacy arms).
//!
//! ```bash
//! RCH_REQUIRE_REMOTE=1 rch exec -- env FAST_AB_SCALE=full FAST_AB_ROUNDS=31 \
//!   FAST_AB_SCAN_ROUNDS=7 cargo bench -p fsqlite-pager --features bench-internals \
//!     --profile release --bench fast_page_array_sparse_ab
//! ```

use std::hint::black_box;
use std::time::Instant;

use fsqlite_pager::page_buf::PageBufPool;
use fsqlite_pager::page_cache::fast_page_array_ab::{
    FastPageArrayArm, LegacyFlatFastPageArray, Sparse,
};
use fsqlite_types::{PageNumber, PageSize};

/// The canonical cass archive's page count (221,269,291,008 bytes / 4096).
const INCIDENT_LAST_PAGE: u32 = 54_020_823;

/// First page of the `*-blocks` cells: far past the 16Ki-slot inline head.
const BLOCKS_FIRST_PAGE: u32 = 1_000_001;

struct Scale {
    name: &'static str,
    memory_page: u32,
    scan_range: u32,
    scan_accesses: usize,
    scan_budget: usize,
    dense_pages: u32,
    dense_gets: usize,
    txn_pages: u32,
    txn_count: usize,
}

const FULL: Scale = Scale {
    name: "full",
    memory_page: INCIDENT_LAST_PAGE,
    scan_range: 8 << 20,
    scan_accesses: 200_000,
    scan_budget: 4_096,
    dense_pages: 2_000,
    dense_gets: 4_000_000,
    txn_pages: 8,
    txn_count: 200_000,
};

const SMOKE: Scale = Scale {
    name: "smoke",
    memory_page: 1 << 20,
    scan_range: 1 << 20,
    scan_accesses: 100_000,
    scan_budget: 1_024,
    dense_pages: 500,
    dense_gets: 200_000,
    txn_pages: 8,
    txn_count: 10_000,
};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

fn page(raw: u32) -> PageNumber {
    PageNumber::new(raw).expect("bench page numbers are non-zero")
}

/// Resident set size of this process, from `/proc/self/statm` (Linux).
fn rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|statm| statm.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map_or(0, |pages| pages * 4096)
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / f64::from(1 << 20)
}

fn memory_cell<A: FastPageArrayArm>(label: &str, last_page: u32) {
    let pool = PageBufPool::new(PageSize::DEFAULT, 4);
    let before = rss_bytes();
    let mut arm = A::new_empty();
    let mut buf = pool.acquire().expect("bench pool has room");
    buf.as_mut_slice()[0] = 0x5A;
    assert!(arm.insert(page(last_page), buf));
    assert_eq!(arm.read_first_byte(page(last_page)), Some(0x5A));
    let after = rss_bytes();
    println!(
        "[memory] {label:7} page={last_page} index_len={} index_bytes={:.1} MiB rss_added={:.1} MiB",
        arm.index_len(),
        mib(arm.index_bytes() as u64),
        mib(after.saturating_sub(before)),
    );
    drop(arm);
}

/// The rebuild reader's stream: runs of 1..=64 consecutive pages at seeded
/// offsets across `1..=range`, opened by one read of the last page.
fn scan_stream(range: u32, accesses: usize) -> Vec<u32> {
    let mut rng = Rng(0x5CA1_AB1E);
    let mut stream = Vec::with_capacity(accesses + 1);
    stream.push(range);
    while stream.len() <= accesses {
        let start = 1 + (rng.next() % u64::from(range)) as u32;
        let run = 1 + (rng.next() % 64) as u32;
        for offset in 0..run {
            stream.push((start + offset).min(range));
        }
    }
    stream.truncate(accesses + 1);
    stream
}

/// Returns `(checksum, index bytes at the end)`.
fn scan<A: FastPageArrayArm>(stream: &[u32], budget: usize) -> (u64, usize) {
    let pool = PageBufPool::new(PageSize::DEFAULT, budget + 1);
    let mut arm = A::new_empty();
    let mut checksum = 0_u64;
    for (step, &raw) in stream.iter().enumerate() {
        let page_no = page(raw);
        if let Some(byte) = arm.read_first_byte(page_no) {
            checksum = checksum.wrapping_add(u64::from(byte));
            continue;
        }
        let mut buf = if arm.resident_count() >= budget {
            let (victim, buf) = arm
                .take_any_clean_from_pool(&pool)
                .expect("a full clean cache yields a victim");
            checksum = checksum
                .wrapping_mul(31)
                .wrapping_add(u64::from(victim.get()));
            buf
        } else {
            pool.acquire().expect("pool sized to the budget")
        };
        buf.as_mut_slice()[0] = step as u8;
        arm.insert(page_no, buf);
    }
    checksum = checksum.wrapping_add(arm.resident_count() as u64);
    (checksum, arm.index_bytes())
}

/// An arm holding pages `first .. first + pages`, each tagged with its low
/// byte.
fn dense_arm<A: FastPageArrayArm>(first: u32, pages: u32) -> A {
    let pool = PageBufPool::new(PageSize::DEFAULT, pages as usize);
    let mut arm = A::new_empty();
    for raw in first..first + pages {
        let mut buf = pool.acquire().expect("pool sized to the page count");
        buf.as_mut_slice()[0] = raw as u8;
        arm.insert(page(raw), buf);
    }
    arm
}

fn dense_reads<A: FastPageArrayArm>(arm: &mut A, gets: &[u32]) -> u64 {
    let mut checksum = 0_u64;
    for &raw in gets {
        checksum = checksum.wrapping_add(u64::from(
            arm.read_first_byte(page(raw))
                .expect("dense pages are resident"),
        ));
    }
    checksum
}

/// An empty arm whose index already spans pages `first .. first + pages`,
/// with the buffer pool its transactions draw from.
fn txn_arm<A: FastPageArrayArm>(first: u32, pages: u32) -> (A, PageBufPool) {
    let pool = PageBufPool::new(PageSize::DEFAULT, pages as usize);
    let mut arm = A::new_empty();
    txn(&mut arm, &pool, first, pages, 1);
    (arm, pool)
}

fn txn<A: FastPageArrayArm>(
    arm: &mut A,
    pool: &PageBufPool,
    first: u32,
    pages: u32,
    count: usize,
) -> u64 {
    let mut checksum = 0_u64;
    for round in 0..count {
        for raw in first..first + pages {
            let mut buf = pool.acquire().expect("pages are released by clear");
            buf.as_mut_slice()[0] = (round as u32 + raw) as u8;
            arm.insert(page(raw), buf);
        }
        for _ in 0..4 {
            for raw in first..first + pages {
                checksum = checksum.wrapping_add(u64::from(
                    arm.read_first_byte(page(raw)).expect("admitted this round"),
                ));
            }
        }
        checksum = checksum.wrapping_add(arm.clear() as u64);
    }
    checksum
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
    sorted[index]
}

fn summary(mut values: Vec<f64>) -> String {
    values.sort_by(f64::total_cmp);
    format!(
        "{:.3} [{:.3}, {:.3}]",
        percentile(&values, 0.5),
        values[0],
        values[values.len() - 1]
    )
}

/// Time `incumbent` and `candidate` in alternating order for `rounds`, in ms.
fn paired(
    rounds: usize,
    mut incumbent: impl FnMut() -> u64,
    mut candidate: impl FnMut() -> u64,
) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let time = |run: &mut dyn FnMut() -> u64| {
        let started = Instant::now();
        black_box(run());
        started.elapsed().as_secs_f64() * 1e3
    };
    let (mut a, mut b, mut ratio) = (Vec::new(), Vec::new(), Vec::new());
    for round in 0..rounds {
        let (ta, tb) = if round % 2 == 0 {
            let ta = time(&mut incumbent);
            (ta, time(&mut candidate))
        } else {
            let tb = time(&mut candidate);
            (time(&mut incumbent), tb)
        };
        a.push(ta);
        b.push(tb);
        ratio.push(tb / ta);
    }
    (a, b, ratio)
}

/// `legacy` and `legacy_twin` are two independent legacy arms (the A/A
/// null); `sparse` is the production arm.
fn timed_cell(
    name: &str,
    ops: usize,
    rounds: usize,
    mut legacy: impl FnMut() -> u64,
    mut legacy_twin: impl FnMut() -> u64,
    mut sparse: impl FnMut() -> u64,
) {
    let expected = legacy();
    assert_eq!(expected, legacy_twin(), "[{name}] legacy arms disagree");
    assert_eq!(expected, sparse(), "[{name}] arms disagree");
    let (_, _, null) = paired(rounds, &mut legacy, &mut legacy_twin);
    let (legacy_ms, sparse_ms, ratio) = paired(rounds, &mut legacy, &mut sparse);
    let per_op = |ms: &[f64]| {
        ms.iter()
            .map(|ms| ms * 1e6 / ops as f64)
            .collect::<Vec<_>>()
    };
    println!(
        "[{name}] ops={ops} legacy ns/op p50 [min,max] {} | sparse {} | ratio sparse/legacy {} | A/A null {}",
        summary(per_op(&legacy_ms)),
        summary(per_op(&sparse_ms)),
        summary(ratio),
        summary(null),
    );
}

fn dense_cell(name: &str, first: u32, scale: &Scale, rounds: usize) {
    let mut rng = Rng(0xD3A5_E000);
    let gets: Vec<u32> = (0..scale.dense_gets)
        .map(|_| first + (rng.next() % u64::from(scale.dense_pages)) as u32)
        .collect();
    let mut legacy: LegacyFlatFastPageArray = dense_arm(first, scale.dense_pages);
    let mut legacy_twin: LegacyFlatFastPageArray = dense_arm(first, scale.dense_pages);
    let mut sparse: Sparse = dense_arm(first, scale.dense_pages);
    timed_cell(
        name,
        gets.len(),
        rounds,
        || dense_reads(&mut legacy, &gets),
        || dense_reads(&mut legacy_twin, &gets),
        || dense_reads(&mut sparse, &gets),
    );
}

fn txn_cell(name: &str, first: u32, scale: &Scale, rounds: usize) {
    let (pages, count) = (scale.txn_pages, scale.txn_count);
    let (mut legacy, legacy_pool) = txn_arm::<LegacyFlatFastPageArray>(first, pages);
    let (mut legacy_twin, twin_pool) = txn_arm::<LegacyFlatFastPageArray>(first, pages);
    let (mut sparse, sparse_pool) = txn_arm::<Sparse>(first, pages);
    timed_cell(
        name,
        count,
        rounds,
        || txn(&mut legacy, &legacy_pool, first, pages, count),
        || txn(&mut legacy_twin, &twin_pool, first, pages, count),
        || txn(&mut sparse, &sparse_pool, first, pages, count),
    );
}

fn main() {
    let scale = match std::env::var("FAST_AB_SCALE").as_deref() {
        Ok("full") => &FULL,
        _ => &SMOKE,
    };
    let rounds_from = |name: &str, default: usize| -> usize {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    };
    let rounds = rounds_from("FAST_AB_ROUNDS", 5);
    // A legacy `scan` round walks the flat array for seconds; it may need fewer.
    let scan_rounds = rounds_from("FAST_AB_SCAN_ROUNDS", rounds);
    eprintln!(
        "[harness] scale={} rounds={rounds} scan_rounds={scan_rounds}",
        scale.name
    );

    memory_cell::<LegacyFlatFastPageArray>("legacy", scale.memory_page);
    memory_cell::<Sparse>("sparse", scale.memory_page);

    dense_cell("dense-head", 1, scale, rounds);
    dense_cell("dense-blocks", BLOCKS_FIRST_PAGE, scale, rounds);
    txn_cell("txn-head", 1, scale, rounds);
    txn_cell("txn-blocks", BLOCKS_FIRST_PAGE, scale, rounds);

    let stream = scan_stream(scale.scan_range, scale.scan_accesses);
    let (legacy_sum, legacy_index) = scan::<LegacyFlatFastPageArray>(&stream, scale.scan_budget);
    let (sparse_sum, sparse_index) = scan::<Sparse>(&stream, scale.scan_budget);
    assert_eq!(legacy_sum, sparse_sum, "[scan] arms disagree");
    println!(
        "[scan] range={} budget={} index_bytes legacy={:.1} MiB sparse={:.3} MiB",
        scale.scan_range,
        scale.scan_budget,
        mib(legacy_index as u64),
        mib(sparse_index as u64),
    );
    timed_cell(
        "scan",
        stream.len(),
        scan_rounds,
        || scan::<LegacyFlatFastPageArray>(&stream, scale.scan_budget).0,
        || scan::<LegacyFlatFastPageArray>(&stream, scale.scan_budget).0,
        || scan::<Sparse>(&stream, scale.scan_budget).0,
    );
}
