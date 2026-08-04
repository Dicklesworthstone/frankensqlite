//! bd-agg-composite-prefix-range: `SELECT COUNT(*)/SUM(...) FROM t WHERE a = v AND b <range>` on a
//! composite `index(a, b)` now seeks the `a = v` block bounded by the range on `b` instead of
//! full-scanning — for SUM (via `codegen_select_aggregate`) and COUNT(*) (which yields out of
//! `codegen_select_count_star`). HARD GATE: byte-identical to C SQLite (rusqlite) across all range
//! operators on `b`, both aggregates, NULL `b` (excluded), empty ranges (→ COUNT=0 / SUM=NULL),
//! COALESCE, mixed storage in `b`, and a residual-predicate DECLINE (`... AND c = k` must not be
//! dropped — the bd-zqkrp-residual-drop guard). Opcode gate: the seek emits SeekGE + IdxGT; COUNT(*)
//! and SUM(a) are covering (no SeekRowid), SUM(b) is not.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f:?}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

async fn has_op(conn: &Connection, sql: &str, want: &str) -> bool {
    conn.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .any(
            |row| matches!(row.values().get(1), Some(SqliteValue::Text(op)) if op.to_string() == want),
        )
}

#[test]
fn agg_composite_prefix_range_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("frank");
        let r = rusqlite::Connection::open_in_memory().expect("sqlite");
        for stmt in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);",
            "CREATE INDEX idx_ab ON t(a, b);",
        ] {
            f.execute(stmt).await.unwrap();
            r.execute_batch(stmt).unwrap();
        }
        for i in 1..=3000_i64 {
            let a = i % 12;
            // b spread over a range with some NULLs.
            let b = if i % 13 == 0 {
                "NULL".to_owned()
            } else {
                format!("{}", ((i.wrapping_mul(7)) % 200) - 50)
            };
            let c = i % 5;
            let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {b}, {c});");
            f.execute(&stmt).await.unwrap();
            r.execute_batch(&stmt).unwrap();
        }
        // Mixed storage in b under INTEGER affinity.
        for (id, aval, bval) in [(9001, 5, "7.5"), (9002, 5, "'apple'")] {
            let stmt = format!("INSERT INTO t VALUES ({id}, {aval}, {bval}, 0);");
            f.execute(&stmt).await.unwrap();
            r.execute_batch(&stmt).unwrap();
        }

        for sql in [
            // All range operators on b within a = v, both aggregates.
            "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10",
            "SELECT SUM(c) FROM t WHERE a = 5 AND b > 10",
            "SELECT COUNT(*) FROM t WHERE a = 5 AND b >= 10",
            "SELECT COUNT(*) FROM t WHERE a = 5 AND b < 10",
            "SELECT SUM(c) FROM t WHERE a = 5 AND b <= 10",
            "SELECT COUNT(*) FROM t WHERE a = 5 AND b BETWEEN -20 AND 40",
            "SELECT SUM(c) FROM t WHERE a = 5 AND b > -20 AND b < 40",
            // Reversed operand order in the range.
            "SELECT COUNT(*) FROM t WHERE a = 5 AND 10 < b",
            // Absent prefix / empty range -> COUNT=0 / SUM=NULL, and COALESCE.
            "SELECT COUNT(*) FROM t WHERE a = 999 AND b > 0",
            "SELECT SUM(c) FROM t WHERE a = 5 AND b > 100000",
            "SELECT COALESCE(SUM(c), -1) FROM t WHERE a = 5 AND b > 100000",
            // Covering SUM of the pinned leading column, and of the range column (both are key columns).
            "SELECT SUM(a) FROM t WHERE a = 5 AND b > 10",
            "SELECT SUM(b) FROM t WHERE a = 5 AND b > 10",
            "SELECT MIN(b) FROM t WHERE a = 5 AND b > 10",
            "SELECT MAX(b) FROM t WHERE a = 5 AND b BETWEEN 0 AND 40",
            // Residual on a NON-key column: `c = 1` must NOT be dropped (declines to a correct scan).
            "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10 AND c = 1",
            "SELECT SUM(c) FROM t WHERE a = 3 AND b BETWEEN 0 AND 50 AND c = 2",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await,
                sqlite_rows(&r, sql),
                "diverged: `{sql}`"
            );
        }

        // Opcode gate: the composite prefix+range seek fires (SeekGE anchor + IdxGT prefix stop).
        assert!(
            has_op(
                &f,
                "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10",
                "SeekGE"
            )
            .await
                && has_op(&f, "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10", "IdxGT").await,
            "COUNT(*) WHERE a=v AND b>c must seek (SeekGE + IdxGT)"
        );
        assert!(
            has_op(&f, "SELECT SUM(c) FROM t WHERE a = 5 AND b > 10", "IdxGT").await,
            "SUM WHERE a=v AND b>c must seek the composite block"
        );
        // Covering gate: COUNT(*)/SUM(leading col a) read straight from the index; SUM(b) needs the table.
        assert!(
            !has_op(
                &f,
                "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10",
                "SeekRowid"
            )
            .await,
            "COUNT(*) composite prefix+range must be covering (no SeekRowid)"
        );
        assert!(
            !has_op(
                &f,
                "SELECT SUM(a) FROM t WHERE a = 5 AND b > 10",
                "SeekRowid"
            )
            .await,
            "SUM(leading col) composite prefix+range must be covering (no SeekRowid)"
        );
        assert!(
            !has_op(
                &f,
                "SELECT SUM(b) FROM t WHERE a = 5 AND b > 10",
                "SeekRowid"
            )
            .await,
            "SUM(range col b) is a KEY column -> covering (no SeekRowid)"
        );
        // A SUM of a NON-key column still needs the table lookup.
        assert!(
            has_op(
                &f,
                "SELECT SUM(c) FROM t WHERE a = 5 AND b > 10",
                "SeekRowid"
            )
            .await,
            "SUM(non-key col c) must open the table (SeekRowid)"
        );
        // Residual declines the seek (no IdxGT), falling to a scan that enforces c.
        assert!(
            !has_op(
                &f,
                "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10 AND c = 1",
                "IdxGT"
            )
            .await,
            "residual `c = 1` must decline the composite prefix+range seek"
        );
    });
}

/// bd-bn45n follow-up: aggregates over a PURE range on the LEADING key term of a composite-only index
/// (`SELECT COUNT(*)/SUM(...) FROM t WHERE a <range>`, no equality prefix) reuse the same
/// `composite_index_prefix_range_target` (now empty-prefix-capable) and the aggregate composite-range
/// emitter. The emitter must SKIP the prefix `IdxGT` when `prefix_len==0` (the range bounds terminate
/// the walk); this asserts byte-identical results and that the seek fires with no prefix IdxGT.
#[test]
fn agg_composite_pure_leading_range_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("frank");
        let r = rusqlite::Connection::open_in_memory().expect("sqlite");
        for stmt in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);",
            "CREATE INDEX idx_ab ON t(a, b);",
        ] {
            f.execute(stmt).await.unwrap();
            r.execute_batch(stmt).unwrap();
        }
        for i in 1..=3000_i64 {
            // NULL run in the LEADING term `a` (excluded by any `a <range>`, matching C SQLite).
            let a = if i % 47 == 0 {
                "NULL".to_owned()
            } else {
                format!("{}", i % 12)
            };
            let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {}, {});", i % 25, i % 5);
            f.execute(&stmt).await.unwrap();
            r.execute_batch(&stmt).unwrap();
        }
        for sql in [
            "SELECT COUNT(*) FROM t WHERE a < 5",
            "SELECT COUNT(*) FROM t WHERE a <= 4",
            "SELECT COUNT(*) FROM t WHERE a > 7",
            "SELECT COUNT(*) FROM t WHERE a >= 6",
            "SELECT COUNT(*) FROM t WHERE a BETWEEN 2 AND 8",
            "SELECT SUM(c) FROM t WHERE a < 5",
            "SELECT SUM(a) FROM t WHERE a < 5",
            "SELECT MIN(b) FROM t WHERE a < 5",
            "SELECT MAX(c) FROM t WHERE a >= 6",
            "SELECT COALESCE(SUM(c), -1) FROM t WHERE a > 100000",
            // Residual on a non-key column must NOT be dropped (declines to a scan enforcing c).
            "SELECT COUNT(*) FROM t WHERE a < 5 AND c = 1",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await,
                sqlite_rows(&r, sql),
                "diverged: `{sql}`"
            );
        }

        // Pure leading range seeks (SeekGE) with NO prefix IdxGT (empty prefix); residual declines.
        assert!(
            has_op(&f, "SELECT COUNT(*) FROM t WHERE a < 5", "SeekGE").await,
            "COUNT(*) pure leading range must seek idx_ab (SeekGE)"
        );
        assert!(
            !has_op(&f, "SELECT COUNT(*) FROM t WHERE a < 5", "IdxGT").await,
            "empty-prefix aggregate leading-range seek must not emit a prefix IdxGT"
        );
        assert!(
            !has_op(&f, "SELECT COUNT(*) FROM t WHERE a < 5 AND c = 1", "SeekGE").await,
            "residual `c = 1` must decline the aggregate leading-range seek"
        );
    });
}
