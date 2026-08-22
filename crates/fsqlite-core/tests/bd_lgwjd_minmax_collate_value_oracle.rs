//! bd-lgwjd (REVIEW3-P2): `min()/max()` must compute the extremum VALUE under a
//! column's DECLARED collation (`name TEXT COLLATE NOCASE`), not BINARY, in the
//! multi-aggregate and explicit-GROUP BY lanes — and `max` must break
//! collation-equal ties FIRST-wins like stock `minmaxStep`, not last-wins.
//!
//! (a) The collated-extremum VALUE path only fired when a `JoinEvalCollationContext`
//!     was installed; multi-aggregate expressions (`max(name)||min(name)`) and the
//!     explicit-GROUP BY lane did not install it, so they fell back to BINARY.
//! (b) `compute_aggregate_ext_collated` used `Iterator::max_by` (last-wins on ties);
//!     stock is first-wins.
//!
//! Differential against rusqlite (bundled C SQLite) — the oracle.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Flatten a one-row / one-or-two-column result to a `|`-joined text form so
/// frank and stock can be compared without per-type plumbing.
fn frank_row_text(rows: &[fsqlite_core::connection::Row]) -> String {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|v| match v {
                    SqliteValue::Null => "NULL".to_owned(),
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Float(f) => format!("{f}"),
                    SqliteValue::Text(s) => s.to_string(),
                    SqliteValue::Blob(b) => format!("blob:{}", b.len()),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn stock_row_text(conn: &rusqlite::Connection, sql: &str) -> String {
    let mut stmt = conn.prepare(sql).unwrap();
    let ncols = stmt.column_count();
    let rows: Vec<String> = stmt
        .query_map([], |r| {
            let cells: Vec<String> = (0..ncols)
                .map(|i| match r.get_ref(i).unwrap() {
                    rusqlite::types::ValueRef::Null => "NULL".to_owned(),
                    rusqlite::types::ValueRef::Integer(n) => n.to_string(),
                    rusqlite::types::ValueRef::Real(f) => format!("{f}"),
                    rusqlite::types::ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned(),
                    rusqlite::types::ValueRef::Blob(b) => format!("blob:{}", b.len()),
                })
                .collect();
            Ok(cells.join("|"))
        })
        .unwrap()
        .map(std::result::Result::unwrap)
        .collect();
    rows.join("\n")
}

/// Build the same schema/rows in stock and frank, then assert every query agrees.
async fn assert_frank_matches_stock(schema: &[&str], rows: &[&str], queries: &[&str]) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("bd_lgwjd.db");

    // Stock builds the fixture (so the on-disk collation metadata is stock's).
    {
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        for stmt in schema {
            stock.execute_batch(stmt).unwrap();
        }
        for stmt in rows {
            stock.execute_batch(stmt).unwrap();
        }
    }
    let stock = rusqlite::Connection::open(&db_path).unwrap();

    let conn = Connection::open(db_path.to_str().unwrap()).await.unwrap();
    for q in queries {
        let frank = frank_row_text(&conn.query(q).await.unwrap());
        let want = stock_row_text(&stock, q);
        assert_eq!(
            frank, want,
            "bd-lgwjd: frank diverged from stock on `{q}` (frank={frank:?} stock={want:?})"
        );
    }
}

/// (a) declared-collation VALUE in multi-aggregate + GROUP BY lanes.
/// NOCASE order: 'a3' < 'q1' < 'Z1'; BINARY order: 'Z1' < 'a3' < 'q1'. So the
/// NOCASE extremum VALUE differs from BINARY, exposing the fallback.
#[test]
fn bd_lgwjd_declared_collation_minmax_value_multi_agg_and_group_by() {
    asupersync::test_utils::run_test(|| async {
        assert_frank_matches_stock(
            &["CREATE TABLE prod(name TEXT COLLATE NOCASE);"],
            &["INSERT INTO prod(name) VALUES ('q1'), ('Z1'), ('a3');"],
            &[
                // single bare aggregate — already correct, guards against regression
                "SELECT max(name) FROM prod;",
                "SELECT min(name) FROM prod;",
                // multi-aggregate expression (no GROUP BY)
                "SELECT max(name)||min(name) FROM prod;",
                // single aggregate wrapped in an expression (single-agg wrapper lane)
                "SELECT max(name)||'' FROM prod;",
                // count + aggregate expression
                "SELECT count(*), max(name)||'' FROM prod;",
                // direct aggregate under explicit GROUP BY (substrate path)
                "SELECT max(name) FROM prod GROUP BY 1=1;",
                "SELECT min(name) FROM prod GROUP BY 1=1;",
                // aggregate expression under explicit GROUP BY (interpreter path)
                "SELECT max(name)||min(name) FROM prod GROUP BY 1=1;",
            ],
        )
        .await;
    });
}

/// (b) `max` tie-break on collation-equal values must be FIRST-wins (stock
/// minmaxStep), not last-wins (`Iterator::max_by`). Under NOCASE, 'z' and 'Z' are
/// equal; inserted 'z' first, so stock keeps 'z'. `min` is coincidentally
/// first-wins already — included as a control. The `max(v), ord` shape exercises
/// the minmax-cache VALUE lane (`compute_aggregate_ext_collated`, the reported
/// site); the bare/`||` shapes exercise the VDBE step (already first-wins).
#[test]
fn bd_lgwjd_max_collation_tie_break_is_first_wins() {
    asupersync::test_utils::run_test(|| async {
        assert_frank_matches_stock(
            &["CREATE TABLE t(v TEXT COLLATE NOCASE, ord INTEGER);"],
            &["INSERT INTO t(v, ord) VALUES ('z', 1), ('Z', 2);"],
            &[
                "SELECT max(v) FROM t;",
                "SELECT min(v) FROM t;",
                "SELECT max(v)||'' FROM t;",
                // single min/max + bare column -> minmax-cache lane
                "SELECT max(v), ord FROM t;",
                "SELECT min(v), ord FROM t;",
            ],
        )
        .await;
    });
}
