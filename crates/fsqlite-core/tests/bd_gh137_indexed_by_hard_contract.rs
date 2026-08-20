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
/// FIXED (bd-gh-indexed-by-hard-contract-qd1cs sub-issue 2): codegen_select_aggregate
/// now takes an index-ORDERED full-scan path for a forced `INDEXED BY` (the
/// `forced_index_ordered` branch), so the aggregate accumulates in index order.
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

/// Sub-2 order-INSENSITIVE aggregates over a forced index stay correct: the
/// COVERING path (count(*) + sum(ord), both readable from idx_ord) and the
/// non-covering path (min/max(x), x looked up in the table by rowid).
#[test]
fn gh137_sub2_covering_and_noncovering_unchanged() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, ord INTEGER, x INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX idx_ord ON t(ord);").await.unwrap();
        conn.execute("INSERT INTO t VALUES (1,3,10),(2,1,20),(3,2,30);")
            .await
            .unwrap();

        // Covering: count(*) needs no column; sum(ord) reads ord from the index.
        let cov = conn
            .query("SELECT count(*), sum(ord) FROM t INDEXED BY idx_ord;")
            .await
            .expect("covering forced-index aggregate");
        assert_eq!(cov[0].values()[0], SqliteValue::Integer(3));
        assert_eq!(cov[0].values()[1], SqliteValue::Integer(6));

        // Non-covering: min/max(x) require a table lookup by rowid.
        let mm = conn
            .query("SELECT min(x), max(x) FROM t INDEXED BY idx_ord;")
            .await
            .expect("non-covering forced-index aggregate");
        assert_eq!(mm[0].values()[0], SqliteValue::Integer(10));
        assert_eq!(mm[0].values()[1], SqliteValue::Integer(30));
    });
}

/// Sub-2 with a residual WHERE: the forced full index scan enforces no
/// predicate, so the whole WHERE is re-applied per row (non-covering).
#[test]
fn gh137_sub2_where_residual() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, ord INTEGER, x INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX idx_ord ON t(ord);").await.unwrap();
        conn.execute("INSERT INTO t VALUES (1,3,10),(2,1,20),(3,2,30);")
            .await
            .unwrap();

        let res = conn
            .query("SELECT sum(x) FROM t INDEXED BY idx_ord WHERE x > 15;")
            .await
            .expect("forced-index aggregate with residual WHERE");
        // x in {20,30} pass the filter -> 50.
        assert_eq!(res[0].values()[0], SqliteValue::Integer(50));
    });
}

/// Sub-2 × sub-4: a WITHOUT ROWID table has no rowid, so the forced-index
/// aggregate must fetch the row via the PK-suffix seek. Same index-order overflow
/// semantics as the rowid case (MAX + -1 + 1 = MAX, no intermediate overflow).
#[test]
fn gh137_sub2_without_rowid() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE w(k TEXT PRIMARY KEY, ord INTEGER, x INTEGER) WITHOUT ROWID;")
            .await
            .unwrap();
        conn.execute("CREATE INDEX wi ON w(ord);").await.unwrap();
        conn.execute(
            "INSERT INTO w VALUES ('a',1,9223372036854775807),('b',3,1),('c',2,-1);",
        )
        .await
        .unwrap();

        let res = conn
            .query("SELECT sum(x) FROM w INDEXED BY wi;")
            .await
            .expect("WITHOUT ROWID forced-index aggregate must not overflow");
        assert_eq!(
            res[0].values()[0],
            SqliteValue::Integer(9_223_372_036_854_775_807),
            "WITHOUT ROWID aggregate must scan wi order (PK-suffix seek), matching stock"
        );
    });
}

/// Sub-2 empty-table edge: the forced index is empty, so Rewind jumps straight to
/// finalize with still-Null accumulators — sum -> NULL, count(*) -> 0 (stock).
#[test]
fn gh137_sub2_empty_table() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, ord INTEGER, x INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX idx_ord ON t(ord);").await.unwrap();

        let res = conn
            .query("SELECT sum(x), count(*) FROM t INDEXED BY idx_ord;")
            .await
            .expect("empty forced-index aggregate");
        assert_eq!(res[0].values()[0], SqliteValue::Null);
        assert_eq!(res[0].values()[1], SqliteValue::Integer(0));
    });
}

/// Sub-4 facet (a) — WITHOUT ROWID + INDEXED BY on a NON-aggregate query (the
/// codegen_select_index_ordered_scan path): covering (b in the index) and
/// non-covering (k via the PK-suffix table seek) both scan in wi (index) order.
/// Appears resolved at HEAD (bd-rjaff PK-suffix seek); this is a regression guard.
#[test]
fn gh137_sub4_without_rowid_nonaggregate() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE w(k TEXT PRIMARY KEY, b INTEGER) WITHOUT ROWID;")
            .await
            .unwrap();
        conn.execute("CREATE INDEX wi ON w(b);").await.unwrap();
        conn.execute("INSERT INTO w VALUES ('c',30),('a',10),('b',20);")
            .await
            .unwrap();

        // Covering (b is the index key): forced index wi -> ascending b order.
        let cov = conn
            .query("SELECT b FROM w INDEXED BY wi;")
            .await
            .expect("WITHOUT ROWID covering forced index");
        let bs: Vec<SqliteValue> = cov.iter().map(|r| r.values()[0].clone()).collect();
        assert_eq!(
            bs,
            vec![
                SqliteValue::Integer(10),
                SqliteValue::Integer(20),
                SqliteValue::Integer(30)
            ],
            "WITHOUT ROWID covering forced index must scan wi order"
        );

        // Non-covering (k needs the PK-suffix table seek), same wi order.
        let non = conn
            .query("SELECT k, b FROM w INDEXED BY wi;")
            .await
            .expect("WITHOUT ROWID non-covering forced index");
        let ks: Vec<SqliteValue> = non.iter().map(|r| r.values()[0].clone()).collect();
        assert_eq!(
            ks,
            vec![
                SqliteValue::Text("a".into()),
                SqliteValue::Text("b".into()),
                SqliteValue::Text("c".into())
            ],
            "WITHOUT ROWID non-covering forced index must fetch k via PK-suffix seek in wi order"
        );
    });
}

/// Sub-4 facet (b) — parameter lowering across a forced-index seek + residual: a
/// bound `?` in the seek key and another in the residual WHERE must map to the
/// right parameter (the base-reset in codegen_select_index_ordered_scan). Appears
/// resolved at HEAD; this is a regression guard.
#[test]
fn gh137_sub4_param_lowering() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX ik ON t(k);").await.unwrap();
        conn.execute("INSERT INTO t VALUES (1,5,100),(2,5,200),(3,9,300);")
            .await
            .unwrap();

        // k = ?1 (seek key), v > ?2 (residual). k=5 -> v in {100,200}; v>150 -> 200.
        let res = conn
            .query_with_params(
                "SELECT v FROM t INDEXED BY ik WHERE k = ? AND v > ?;",
                &[SqliteValue::Integer(5), SqliteValue::Integer(150)],
            )
            .await
            .expect("forced-index param lowering");
        let vs: Vec<SqliteValue> = res.iter().map(|r| r.values()[0].clone()).collect();
        assert_eq!(
            vs,
            vec![SqliteValue::Integer(200)],
            "both bound params must map correctly across the forced-index seek + residual"
        );
    });
}
