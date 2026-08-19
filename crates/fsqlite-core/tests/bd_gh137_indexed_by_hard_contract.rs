//! GH #137 (bd-gh-indexed-by-hard-contract-qd1cs): `INDEXED BY <name>` is a hard
//! planning contract — the named index must be usable, or prepare must fail with
//! `no query solution`. The bead's evidence predates the bd-wlo29 / bd-0fz9d
//! partial-index cover work; this is a census-vs-HEAD keeper for the two
//! confirmed sub-issues (oracle: sqlite3 3.46.1).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Sub-issue 1: a forced PARTIAL index that the query cannot use (no WHERE that
/// implies the partial predicate) must ERROR like stock — not silently
/// table-scan and return rows the index can't represent.
#[test]
fn gh137_sub1_unusable_partial_index_errors() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX p ON t(k) WHERE k > 2;")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1,1),(2,3);").await.unwrap();

        // Stock: "no query solution". frank must also refuse (not return 1,2).
        let res = conn.query("SELECT id FROM t INDEXED BY p;").await;
        assert!(
            res.is_err(),
            "forced unusable partial index must error like stock (no query solution); got rows {res:?}"
        );
    });
}

/// Sub-issue 2: an aggregate over `INDEXED BY <idx>` must scan in the INDEX
/// order, which changes SUM overflow semantics. Rows summed in `ord` order are
/// MAX + (-1) + 1 = MAX (no overflow); in rowid order MAX + 1 overflows. Stock
/// returns 9223372036854775807; frank must honor the hard hint, not scan rowid
/// order and raise "integer overflow".
///
/// STILL RED at HEAD (bd-gh-indexed-by-hard-contract-qd1cs sub-issue 2): an
/// aggregate does not take an index-ordered full-scan path for a forced index
/// (`aggregate_index_eq_seek_allowed` only fires with no hint), so it scans in
/// rowid order and overflows. Un-ignore when the aggregate honors INDEXED BY
/// scan order.
#[ignore = "GH#137 sub-issue 2: aggregate must honor INDEXED BY scan order (pending codegen)"]
#[test]
fn gh137_sub2_aggregate_honors_indexed_by_scan_order() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, ord INTEGER, x INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX idx_ord ON t(ord);").await.unwrap();
        conn.execute(
            "INSERT INTO t VALUES (1,1,9223372036854775807),(2,3,1),(3,2,-1);",
        )
        .await
        .unwrap();

        let res = conn.query("SELECT sum(x) FROM t INDEXED BY idx_ord;").await;
        let got = res.expect("sum honoring INDEXED BY must not overflow");
        assert_eq!(
            got[0].values()[0],
            SqliteValue::Integer(9_223_372_036_854_775_807),
            "aggregate must scan in idx_ord order (MAX-1+1=MAX), matching stock"
        );
    });
}
