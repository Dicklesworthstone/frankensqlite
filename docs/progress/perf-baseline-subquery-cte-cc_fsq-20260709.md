# Honest baseline: Subquery & CTE section vs C SQLite (cc_fsq, 2026-07-09)

Status: measurement only. No engine source was modified.

## Summary

`comprehensive-bench --quick --filter subquery` reports FrankenSQLite faster on
all 13 rows (average ratio 0.07x, "72.43x faster" on `10000 rows / IN subquery`,
"27.61x faster" on `Recursive CTE (1..1000 SUM)`).

Two of those headline rows do not measure what their names say. Re-measured
against the same query on a real DB file with a fresh prepare, FrankenSQLite is
**~89x slower** than C SQLite on the IN-subquery row.

## Method

Hardware: 64 cores. Benchmark processes pinned with `taskset` to dedicated
cores. Machine load was elevated (~20) from a concurrent agent's builds, so the
`comprehensive-bench` `fs_cv_pct` of 8.1-15.4% exceeds the 5% honesty bar; the
CLI marginal-cost numbers below are medians of 5 and are robust to that noise
because the effect size is ~90x.

Binaries: `--profile release-perf` with `CARGO_PROFILE_RELEASE_PERF_DEBUG=2`
and `CARGO_PROFILE_RELEASE_PERF_STRIP=none` (the checked-in profile sets
`debug=false, strip=true`, which makes it unprofilable).

Marginal cost isolates the query from process startup and DB open:

    marginal = (T(script with 1000 queries) - T(script with 1 query)) / 999

Both engines read the identical `prod.db` (built once by `sqlite3`) and return
the identical answer (100).

## Results

| measurement | fsqlite | sqlite3 | ratio |
|---|---|---|---|
| `IN (subquery)` marginal, per query | 2188 us | 24.5 us | **89x slower** |
| `SELECT 1;` marginal, per statement | 65 us | 1 us | **65x slower** |
| execution alone (marginal minus per-stmt overhead) | ~2123 us | ~23.5 us | ~90x slower |
| 10505-stmt setup script (10k INSERTs in one txn) | 11.5 s | 39 ms | ~295x slower |

## Root cause: no index seek for `IN (subquery)`

Same `prod.db`, `EXPLAIN QUERY PLAN` of

    SELECT COUNT(*) FROM products WHERE category_id IN (SELECT id FROM categories WHERE id <= 5);

    sqlite3 : SEARCH products USING COVERING INDEX idx_prod_cat (category_id=?)
              LIST SUBQUERY 1 -> SEARCH categories USING INTEGER PRIMARY KEY (rowid<?)

    fsqlite : SCAN INDEX idx_prod_cat USING COVERING INDEX idx_prod_cat
              SCAN categories

C SQLite seeks 5 index ranges yielding 100 rows. FrankenSQLite walks all 10000
index entries, and full-scans `categories` instead of using a rowid range for
`id <= 5`. 2188 us is consistent with the full walk.

Tracked as **bd-2dgf5** (P0).

## Why the benchmark reported a win instead

1. **Retained count/sum cache hit.** `measure()` prepares once and re-executes
   the *identical* statement against a *static* in-memory DB. FrankenSQLite has
   a retained autocommit count/sum cache (`retained_autocommit_count_star_sum_row_in_txn`,
   and the `retained_autocommit_count_sum_cache_row` tests in `connection.rs`),
   so after warmup it returns a cached row in ~70 ns. 70 ns cannot visit the 100
   matching rows. C SQLite has no such cache. The row measures
   *cache hit vs real execution*.

2. **Results are discarded, so correctness is never checked.** The C side does
   `let _: i64 = stmt.query_row([], |r| r.get(0)).unwrap();` (extracts the
   value). The fsqlite side does `let _ = fs_stmt.query_row();` (drops the Row).
   A wrong answer on the fsqlite side would be recorded as a speed win, and the
   C side pays a column extraction the fsqlite side skips.

3. **The recursive-CTE row is pattern-matched.**
   `recursive_cte_integer_series_sum_plan` recognizes exactly
   `WITH RECURSIVE cnt(x) AS (SELECT a UNION ALL SELECT x+s FROM cnt WHERE x < b) SELECT SUM(x) FROM cnt`
   and replaces the engine with a native `checked_add` loop. It requires exactly
   one column, no FROM/WHERE in the base case, one `UNION ALL`, and a recursive
   core selecting only `x+step` with `WHERE x < bound`. `COUNT` instead of
   `SUM`, a second carried column, or an outer `WHERE` all fall back to the
   general path. The 2026-05-05 ledger entry measured that general path at
   1.16x-1.24x *slower* than C SQLite; this benchmark row never exercises it.

Tracked as **bd-czzlp** (P0) and **bd-5310l** (P1).

## Reproduction

    # build prod.db: products(10000) + categories(500) + idx_prod_cat
    sqlite3 prod.db < setup.sql

    Q='SELECT COUNT(*) FROM products WHERE category_id IN (SELECT id FROM categories WHERE id <= 5);'
    echo "$Q" > q1.sql; for i in $(seq 1000); do echo "$Q"; done > q1000.sql

    # marginal = (T(q1000) - T(q1)) / 999, median of 5, taskset-pinned
    taskset -c 50 fsqlite prod.db < q1000.sql
    taskset -c 51 sqlite3 prod.db < q1000.sql

## Incidental: allocator is the top global hotspot

`perf record -F 999 -g` over the subquery section (603 samples; dominated by the
10k-row INSERT setup, not the queries) ranks `_int_malloc` at **16.71%** self
time, reached via `fsqlite_btree::balance::balance_nonroot` and
`<fsqlite_types::PageData as Clone>::clone`. That is a setup-phase signal, not a
query-phase one, and is recorded here only so the next profiling pass does not
re-derive it. A query-phase profile needs a workload without the INSERT setup.

## Note on bd-1bzmw (Track Q lock-free flat hash page cache)

Not started, deliberately. Its premise is already satisfied:
`ShardedPageCache::get_shared()` is three tiers - a direct-indexed
`FastPageArray` (`Vec<Option<CachedPageEntry>>` keyed by `pgno-1`, strictly
better than pcache1's `iKey % nHash` + chain walk), then a lock-free
open-addressing `FlatPageSlots::find_slot()` (`AtomicU32` slots, Acquire loads,
32-probe, tombstones), then the 128 shards as an overflow tier only. Its own
dependency `bd-agozb` was closed "Deferred" recording that an uncontended
`parking_lot::Mutex` costs ~2-3 ns and that the remaining win is diminishing
returns. Implementing the bead as written would replace direct indexing with
modulo hash + chain traversal - a regression. See the bd-1bzmw comment thread.
