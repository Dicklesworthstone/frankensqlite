#![recursion_limit = "512"]

//! GH#419: the SQLite documentation's Sudoku solver (a recursive CTE whose
//! recursive arm filters candidates with a correlated `NOT EXISTS` over a
//! 9-row CTE) ran ~100x slower than stock sqlite3.
//!
//! Root cause: the `NOT EXISTS` probe is evaluated once per candidate row
//! (~42k times for this puzzle) and, because its predicate is an `OR` of
//! expression comparisons rather than one correlated equality, every probe
//! was validated and compiled into a fresh VDBE program from scratch. The fix
//! scans the small probe table directly.
//!
//! This keeper pins the complexity property rather than a wall-clock time:
//! the number of statement compilations for one execution must not grow with
//! the number of probes. It lives in its own test binary because the hot-path
//! profile counters are process-global.

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

const SUDOKU: &str = r"
WITH RECURSIVE
  input(sud) AS (
    VALUES('53..7....6..195....98....6.8...6...34..8.3..17...2...6.6....28....419..5....8..79')
  ),
  digits(z, lp) AS (
    VALUES('1', 1)
    UNION ALL SELECT
    CAST(lp+1 AS TEXT), lp+1 FROM digits WHERE lp<9
  ),
  x(s, ind) AS (
    SELECT sud, instr(sud, '.') FROM input
    UNION ALL
    SELECT
      substr(s, 1, ind-1) || z || substr(s, ind+1),
      instr( substr(s, 1, ind-1) || z || substr(s, ind+1), '.' )
     FROM x, digits AS z
    WHERE ind>0
      AND NOT EXISTS (
            SELECT 1
              FROM digits AS lp
             WHERE z.z = substr(s, ((ind-1)/9)*9 + lp, 1)
                OR z.z = substr(s, ((ind-1)%9) + (lp-1)*9 + 1, 1)
                OR z.z = substr(s, (((ind-1)/3) % 3) * 3
                        + ((ind-1)/27) * 27 + lp
                        + ((lp-1) / 3) * 6, 1)
         )
  )
SELECT s FROM x WHERE ind=0;
";

const SOLUTION: &str =
    "534678912672195348198342567859761423426853791713924856961537284287419635345286179";

/// Stock sqlite3 evaluates 4,632 rows of `x` for this puzzle, so the
/// correlated probe runs tens of thousands of times. A handful of
/// compilations for the CTE bodies and the consumer is expected; anything
/// proportional to the probe count is the GH#419 regression.
const MAX_COMPILATIONS_PER_EXECUTION: u64 = 200;

#[test]
fn gh419_sudoku_correlated_not_exists_does_not_compile_per_probe() {
    asupersync::test_utils::run_test(|| async {
        let connection = Connection::open(":memory:").await.expect("open");
        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();

        let rows = connection.query(SUDOKU).await.expect("sudoku query");

        let compilations = hot_path_profile_snapshot().parser.compiled_cache_misses;
        set_hot_path_profile_enabled(false);
        eprintln!("GH#419 Sudoku: {compilations} statement compilations");

        assert_eq!(rows.len(), 1, "exactly one solved grid");
        assert_eq!(rows[0].values(), &[SqliteValue::Text(SOLUTION.into())]);
        assert!(
            compilations <= MAX_COMPILATIONS_PER_EXECUTION,
            "GH#419: {compilations} statement compilations for one Sudoku execution; \
             the correlated NOT EXISTS probe is being compiled once per candidate row"
        );
    });
}
