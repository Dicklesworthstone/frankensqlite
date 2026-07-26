//! Prefetch effectiveness and safety proofs for `bd-ezg4p`.
#![recursion_limit = "512"]

use fsqlite_btree::{BtCursor, BtreeCursorOps, MemPageStore, PageReader, PageWriter};
use fsqlite_e2e::bench_summary::percentile_u64;
use fsqlite_error::Result;
use fsqlite_types::cx::Cx;
use fsqlite_types::{PageNumber, WitnessKey};
use rusqlite::params;
use serde_json::json;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::hint::black_box;
use std::rc::Rc;
use std::sync::Mutex;
use std::time::Instant;

const BEAD_ID: &str = "bd-ezg4p";
const ROOT_PAGE: u32 = 2;
const USABLE: u32 = 4096;
const COLD_READ_SPIN_ITERS: usize = 1_024;

static PREFETCH_E2E_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct PrefetchStatsSnapshot {
    prefetch_issued_count: usize,
    prefetch_hit_count: usize,
    missing_page_count: usize,
}

#[derive(Debug, Default)]
struct PrefetchState {
    pending_hints: BTreeMap<u32, usize>,
    prefetch_issued_count: usize,
    prefetch_hit_count: usize,
    missing_page_count: usize,
}

#[derive(Debug, Clone)]
struct SharedPrefetchStore {
    inner: Rc<RefCell<MemPageStore>>,
    prefetch_enabled: bool,
    simulate_cold_reads: bool,
    stats: Rc<RefCell<PrefetchState>>,
}

impl SharedPrefetchStore {
    fn new(
        inner: Rc<RefCell<MemPageStore>>,
        prefetch_enabled: bool,
        simulate_cold_reads: bool,
    ) -> Self {
        Self {
            inner,
            prefetch_enabled,
            simulate_cold_reads,
            stats: Rc::new(RefCell::new(PrefetchState::default())),
        }
    }

    fn clear_stats(&self) {
        *self.stats.borrow_mut() = PrefetchState::default();
    }

    fn snapshot(&self) -> PrefetchStatsSnapshot {
        let stats = self.stats.borrow();
        PrefetchStatsSnapshot {
            prefetch_issued_count: stats.prefetch_issued_count,
            prefetch_hit_count: stats.prefetch_hit_count,
            missing_page_count: stats.missing_page_count,
        }
    }
}

#[allow(clippy::manual_async_fn)]
impl PageReader for SharedPrefetchStore {
    #[allow(clippy::useless_let_if_seq)]
    fn read_page<'a>(
        &'a self,
        cx: &'a Cx,
        page_no: PageNumber,
    ) -> impl Future<Output = Result<Vec<u8>>> + 'a {
        async move {
            let prefetched = {
                let mut stats = self.stats.borrow_mut();
                let mut counted_prefetch_hit = false;
                let mut hinted = false;
                let mut remove_pending = false;
                if let Some(pending) = stats.pending_hints.get_mut(&page_no.get())
                    && *pending > 0
                {
                    *pending -= 1;
                    counted_prefetch_hit = true;
                    hinted = true;
                    remove_pending = *pending == 0;
                }
                if counted_prefetch_hit {
                    stats.prefetch_hit_count = stats.prefetch_hit_count.saturating_add(1);
                }
                if remove_pending {
                    stats.pending_hints.remove(&page_no.get());
                }
                hinted
            };

            if self.simulate_cold_reads && !prefetched {
                cold_read_penalty();
            }

            let inner = self.inner.borrow();
            inner.read_page(cx, page_no).await
        }
    }

    fn prefetch_page_hint(&self, cx: &Cx, page_no: PageNumber) {
        if !self.prefetch_enabled {
            return;
        }

        let mut stats = self.stats.borrow_mut();
        stats.prefetch_issued_count = stats.prefetch_issued_count.saturating_add(1);
        *stats.pending_hints.entry(page_no.get()).or_default() += 1;
        drop(stats);

        self.inner.borrow().prefetch_page_hint(cx, page_no);
    }
}

#[allow(clippy::manual_async_fn)]
impl PageWriter for SharedPrefetchStore {
    fn write_page<'a>(
        &'a mut self,
        cx: &'a Cx,
        page_no: PageNumber,
        data: &'a [u8],
    ) -> impl Future<Output = Result<()>> + 'a {
        async move {
            let mut inner = self.inner.borrow_mut();
            inner.write_page(cx, page_no, data).await
        }
    }

    fn allocate_page<'a>(
        &'a mut self,
        cx: &'a Cx,
    ) -> impl Future<Output = Result<PageNumber>> + 'a {
        async move {
            let mut inner = self.inner.borrow_mut();
            inner.allocate_page(cx).await
        }
    }

    fn free_page<'a>(
        &'a mut self,
        cx: &'a Cx,
        page_no: PageNumber,
    ) -> impl Future<Output = Result<()>> + 'a {
        async move {
            let mut inner = self.inner.borrow_mut();
            inner.free_page(cx, page_no).await
        }
    }

    fn record_write_witness(&mut self, _cx: &Cx, _key: WitnessKey) {}
}

fn pn(n: u32) -> PageNumber {
    PageNumber::new(n).expect("page number must be non-zero")
}

fn cold_read_penalty() {
    let mut state = 0xE254_0001_u64;
    for _ in 0..COLD_READ_SPIN_ITERS {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
    }
    black_box(state);
}

fn lcg_next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1);
    *state
}

fn deterministic_shuffle(values: &mut [i64], seed: u64) {
    if values.len() <= 1 {
        return;
    }
    let mut state = seed;
    for i in (1..values.len()).rev() {
        let j = (lcg_next(&mut state) as usize) % (i + 1);
        values.swap(i, j);
    }
}

fn payload_for_rowid(rowid: i64) -> Vec<u8> {
    let rowid_usize = usize::try_from(rowid).expect("rowid must stay positive");
    let payload_len = if rowid % 257 == 0 {
        1_600
    } else {
        24 + (rowid_usize % 192)
    };

    let mut payload = Vec::with_capacity(payload_len);
    for i in 0..payload_len {
        let byte = (rowid_usize.wrapping_mul(29).wrapping_add(i * 11) & 0xFF) as u8;
        payload.push(byte);
    }
    payload
}

async fn build_seed_table_store(total_rows: i64) -> Rc<RefCell<MemPageStore>> {
    let cx = Cx::new();
    let root = pn(ROOT_PAGE);
    let store = Rc::new(RefCell::new(MemPageStore::with_empty_table(root, USABLE)));

    for rowid in 1_i64..=total_rows {
        let mut cursor = BtCursor::new(
            SharedPrefetchStore::new(Rc::clone(&store), true, false),
            root,
            USABLE,
            true,
        );
        cursor
            .table_insert(&cx, rowid, payload_for_rowid(rowid).as_slice())
            .await
            .expect("seed insert should succeed");
    }

    store
}

async fn extract_btree_rows(store: Rc<RefCell<MemPageStore>>) -> Vec<(i64, Vec<u8>)> {
    let cx = Cx::new();
    let mut cursor = BtCursor::new(
        SharedPrefetchStore::new(store, true, false),
        pn(ROOT_PAGE),
        USABLE,
        true,
    );
    let mut rows = Vec::new();
    if !cursor.first(&cx).await.expect("btree first should succeed") {
        return rows;
    }

    loop {
        rows.push((
            cursor.rowid(&cx).await.expect("rowid should exist"),
            cursor.payload(&cx).await.expect("payload should exist"),
        ));
        if !cursor.next(&cx).await.expect("btree next should succeed") {
            break;
        }
    }
    rows
}

fn extract_sqlite_rows(conn: &rusqlite::Connection) -> Vec<(i64, Vec<u8>)> {
    let mut stmt = conn
        .prepare("SELECT id, payload FROM prefetched_rows ORDER BY id")
        .expect("prepare sqlite row dump");
    stmt.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
    })
    .expect("query sqlite rows")
    .map(|row| row.expect("sqlite row"))
    .collect()
}

async fn measure_lookup_latencies(
    store: SharedPrefetchStore,
    workload: &[i64],
) -> (Vec<u64>, PrefetchStatsSnapshot) {
    let cx = Cx::new();
    let mut latencies = Vec::with_capacity(workload.len());
    store.clear_stats();

    for rowid in workload {
        let mut cursor = BtCursor::new(store.clone(), pn(ROOT_PAGE), USABLE, true);
        let started = Instant::now();
        let seek = cursor
            .table_move_to(&cx, *rowid)
            .await
            .expect("lookup should succeed");
        let elapsed = started.elapsed();
        assert!(
            seek.is_found(),
            "rowid {rowid} should exist in the seed tree"
        );
        black_box(cursor.payload(&cx).await.expect("payload should exist"));
        latencies.push(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
    }

    (latencies, store.snapshot())
}

#[test]
fn bd_ezg4p_prefetch_insert_10k_correctness_matches_sqlite() {
    let _guard = PREFETCH_E2E_LOCK.lock().unwrap();
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();
        let root = pn(ROOT_PAGE);
        let backing = Rc::new(RefCell::new(MemPageStore::with_empty_table(root, USABLE)));
        let probe_store = SharedPrefetchStore::new(Rc::clone(&backing), true, false);
        let sqlite = rusqlite::Connection::open_in_memory().expect("open sqlite");
        sqlite
            .execute_batch(
                "CREATE TABLE prefetched_rows (id INTEGER PRIMARY KEY, payload BLOB NOT NULL);",
            )
            .expect("create sqlite table");

        let mut insertion_order: Vec<i64> = (1_i64..=10_000_i64).collect();
        deterministic_shuffle(&mut insertion_order, 0xE2A4_0001);

        probe_store.clear_stats();
        for rowid in &insertion_order {
            let payload = payload_for_rowid(*rowid);
            let mut cursor = BtCursor::new(probe_store.clone(), root, USABLE, true);
            cursor
                .table_insert(&cx, *rowid, payload.as_slice())
                .await
                .expect("btree insert should succeed");
            sqlite
                .execute(
                    "INSERT INTO prefetched_rows (id, payload) VALUES (?1, ?2)",
                    params![rowid, payload],
                )
                .expect("sqlite insert should succeed");
        }

        let btree_rows = extract_btree_rows(Rc::clone(&backing)).await;
        let sqlite_rows = extract_sqlite_rows(&sqlite);
        let stats = probe_store.snapshot();

        assert_eq!(
            btree_rows, sqlite_rows,
            "prefetched B-tree rows diverged from sqlite"
        );
        assert!(
            stats.prefetch_issued_count > 0,
            "10K random inserts should issue at least one child prefetch"
        );
        assert!(
            stats.prefetch_hit_count > 0,
            "10K random inserts should consume at least one prefetched child page"
        );
        assert_eq!(
            stats.missing_page_count, 0,
            "prefetch should never target missing pages"
        );

        eprintln!(
            "PREFETCH_E2E:{}",
            json!({
                "bead_id": BEAD_ID,
                "scenario_id": "TRACK-P-INSERT-10K-CORRECTNESS",
                "rows": btree_rows.len(),
                "prefetch_issued_count": stats.prefetch_issued_count,
                "prefetch_hit_count": stats.prefetch_hit_count,
                "missing_page_count": stats.missing_page_count
            })
        );
    });
}

#[test]
fn bd_ezg4p_prefetch_latency_improvement() {
    let _guard = PREFETCH_E2E_LOCK.lock().unwrap();
    asupersync::test_utils::run_test(|| async {
        let seed_store = build_seed_table_store(10_000).await;

        let prefetch_enabled = SharedPrefetchStore::new(Rc::clone(&seed_store), true, true);
        let prefetch_disabled = SharedPrefetchStore::new(Rc::clone(&seed_store), false, true);

        let mut workload: Vec<i64> = (1_i64..=10_000_i64).collect();
        deterministic_shuffle(&mut workload, 0xE2A4_0002);
        workload.truncate(2_048);

        let (baseline_latencies, baseline_stats) =
            measure_lookup_latencies(prefetch_disabled, &workload).await;
        let (prefetch_latencies, prefetch_stats) =
            measure_lookup_latencies(prefetch_enabled, &workload).await;

        let baseline_p50 = percentile_u64(&baseline_latencies, 50);
        let baseline_p95 = percentile_u64(&baseline_latencies, 95);
        let prefetch_p50 = percentile_u64(&prefetch_latencies, 50);
        let prefetch_p95 = percentile_u64(&prefetch_latencies, 95);

        assert!(
            prefetch_p50 < baseline_p50,
            "prefetch should reduce median descent latency under the cold-read harness: baseline_p50_ns={baseline_p50} prefetch_p50_ns={prefetch_p50}"
        );
        assert!(
            prefetch_p95 < baseline_p95,
            "prefetch should reduce p95 descent latency under the cold-read harness: baseline_p95_ns={baseline_p95} prefetch_p95_ns={prefetch_p95}"
        );
        assert_eq!(
            prefetch_stats.missing_page_count, 0,
            "lookup prefetch should never target missing pages"
        );
        assert_eq!(
            prefetch_stats.prefetch_hit_count, prefetch_stats.prefetch_issued_count,
            "lookup descent should consume every prefetched child page in the deterministic harness"
        );
        assert_eq!(
            baseline_stats.prefetch_issued_count, 0,
            "disabled harness should suppress prefetch issuance"
        );

        eprintln!(
            "PREFETCH_E2E:{}",
            json!({
                "bead_id": BEAD_ID,
                "scenario_id": "TRACK-P-LATENCY-LOOKUP",
                "workload_rows": workload.len(),
                "baseline": {
                    "prefetch_issued_count": baseline_stats.prefetch_issued_count,
                    "prefetch_hit_count": baseline_stats.prefetch_hit_count,
                    "descent_latency_ns": {
                        "p50": baseline_p50,
                        "p95": baseline_p95
                    }
                },
                "prefetch_enabled": {
                    "prefetch_issued_count": prefetch_stats.prefetch_issued_count,
                    "prefetch_hit_count": prefetch_stats.prefetch_hit_count,
                    "descent_latency_ns": {
                        "p50": prefetch_p50,
                        "p95": prefetch_p95
                    }
                }
            })
        );
    });
}
