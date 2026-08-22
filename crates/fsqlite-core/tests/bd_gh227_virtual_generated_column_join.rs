#![recursion_limit = "512"]

//! GH #227 (bd-gh-virtual-generated-columns-5e0u1): a VIRTUAL generated column
//! works for base-table reads but produced EMPTY results when used as a JOIN
//! predicate on a FILE-BACKED database.
//!
//! Root cause: the file-backed read/inflate path materialized only the
//! physically stored columns and left a NULL placeholder in a VIRTUAL generated
//! column's slot. The streaming JOIN source (`try_scan_join_source_from_pager`)
//! then compared NULL where the real generated value was expected, silently
//! dropping every row. A secondary defect (issue 2) keyed pre-existing rows
//! backfilled at `CREATE INDEX` time on the same NULL placeholder.
//!
//! This keeper reproduces the bug FILE-BACKED and pins Franken differentially
//! against rusqlite across: (a) an INNER JOIN on the VIRTUAL column, (b) rows
//! inserted BEFORE and AFTER index creation (the index-backfill path), (c) a
//! STORED generated-column control (already materialized on disk), and (d) a
//! base-table SELECT of the VIRTUAL column.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => {
            format!(
                "X'{}'",
                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
            )
        }
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => {
            format!(
                "X'{}'",
                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
            )
        }
    }
}

/// Assert Franken and rusqlite return identical rows for `sql`, and return the
/// shared row count so callers can additionally assert non-emptiness.
async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) -> usize {
    let fr: Vec<Vec<String>> = fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect();
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    let rr: Vec<Vec<String>> = st
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(fr, rr, "GH#227 mismatch on `{sql}`");
    fr.len()
}

/// Run the identical DDL/DML script against both engines. The insert order is
/// deliberate: two rows land BEFORE the index on the VIRTUAL column is created
/// (exercising the backfill path) and two land AFTER (the ordinary DML path).
async fn seed(fconn: &Connection, rconn: &rusqlite::Connection) {
    let pre = [
        // t: a is the physical JSON source; b is VIRTUAL; s is STORED.
        "CREATE TABLE t (\
            a TEXT, \
            b TEXT AS (json_extract(a, '$.k')) VIRTUAL, \
            s TEXT AS (json_extract(a, '$.k') || '!') STORED\
        )",
        // u joins on the VIRTUAL column; u2 joins on the STORED column.
        "CREATE TABLE u (x TEXT, y INTEGER)",
        "CREATE TABLE u2 (x TEXT, y INTEGER)",
        // Two rows BEFORE the index — these are the ones CREATE INDEX backfills.
        "INSERT INTO t(a) VALUES ('{\"k\":\"apple\"}')",
        "INSERT INTO t(a) VALUES ('{\"k\":\"banana\"}')",
        // Now build the index on the VIRTUAL generated column.
        "CREATE INDEX idx_t_b ON t(b)",
        // Two rows AFTER the index — ordinary post-creation DML.
        "INSERT INTO t(a) VALUES ('{\"k\":\"cherry\"}')",
        "INSERT INTO t(a) VALUES ('{\"k\":\"apple\"}')",
        // Join targets.
        "INSERT INTO u VALUES ('apple', 1)",
        "INSERT INTO u VALUES ('cherry', 2)",
        "INSERT INTO u VALUES ('durian', 3)",
        "INSERT INTO u2 VALUES ('banana!', 10)",
        "INSERT INTO u2 VALUES ('apple!', 20)",
        // bd-3radn H1: a COALESCE VIRTUAL gen-col that yields a NON-NULL value
        // for a NULL base — so a null-extended OUTER JOIN row is distinguishable
        // from a real row whose base is NULL. r(1) matches; r(3) is a real row
        // with base NULL (g = 99); l(2) has no match (null-extended, g = NULL).
        "CREATE TABLE r (id INTEGER, base INTEGER, g INTEGER AS (COALESCE(base, 99)) VIRTUAL)",
        "CREATE TABLE l (id INTEGER)",
        "INSERT INTO r(id, base) VALUES (1, 7), (3, NULL)",
        "INSERT INTO l VALUES (1), (2), (3)",
    ];
    for sql in pre {
        fconn
            .execute(sql)
            .await
            .unwrap_or_else(|e| panic!("franken exec `{sql}`: {e:?}"));
        rconn
            .execute_batch(sql)
            .unwrap_or_else(|e| panic!("rusqlite exec `{sql}`: {e:?}"));
    }
}

#[test]
fn gh227_virtual_generated_column_join_file_backed() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("gh227.db").to_string_lossy().into_owned();

        let fconn = Connection::open(&db).await.expect("open franken file db");
        let rconn = rusqlite::Connection::open(dir.path().join("gh227_oracle.db"))
            .expect("open rusqlite oracle file db");

        seed(&fconn, &rconn).await;

        // (d) Base-table SELECT of the VIRTUAL column still works.
        assert_agree(&fconn, &rconn, "SELECT a, b FROM t ORDER BY a, rowid").await;

        // (c) STORED generated-column control: physically materialized, so it
        // was already correct; assert it stays correct on read and in a JOIN.
        assert_agree(&fconn, &rconn, "SELECT a, s FROM t ORDER BY a, rowid").await;
        assert_agree(
            &fconn,
            &rconn,
            "SELECT t.a, t.s, u2.y FROM t JOIN u2 ON t.s = u2.x ORDER BY u2.y, t.rowid",
        )
        .await;

        // (a) The regression itself: INNER JOIN ON t.<virtual> = u.x. Before the
        // fix this returned NO rows on a file-backed db. There are matches
        // (apple x2, cherry x1), so the count must be non-zero AND equal to the
        // oracle.
        let joined = assert_agree(
            &fconn,
            &rconn,
            "SELECT t.a, t.b, u.y FROM t JOIN u ON t.b = u.x ORDER BY u.y, t.rowid",
        )
        .await;
        assert!(
            joined >= 3,
            "GH#227: VIRTUAL-column JOIN returned {joined} rows; expected the \
             apple/apple/cherry matches (regression: file-backed join saw NULL)"
        );

        // (b) Filter on the VIRTUAL column covering rows inserted BOTH before
        // and after the index. `apple` was inserted once pre-index (backfilled)
        // and once post-index (ordinary DML); both must be found. This is where
        // the index-backfill defect (issue 2) would otherwise surface as
        // mismatched on-disk index keys.
        let apples = assert_agree(
            &fconn,
            &rconn,
            "SELECT a, b FROM t WHERE b = 'apple' ORDER BY rowid",
        )
        .await;
        assert_eq!(
            apples, 2,
            "GH#227: expected both the pre-index (backfilled) and post-index \
             'apple' rows to match on the VIRTUAL column"
        );

        // A VIRTUAL-column filter that spans a pre-index row only.
        assert_agree(
            &fconn,
            &rconn,
            "SELECT a, b FROM t WHERE b = 'banana' ORDER BY rowid",
        )
        .await;

        // Symmetric join with the generated column on the right-hand side.
        assert_agree(
            &fconn,
            &rconn,
            "SELECT u.y, t.b FROM u JOIN t ON u.x = t.b ORDER BY u.y, t.rowid",
        )
        .await;

        // bd-3radn H1: a COALESCE VIRTUAL gen-col on the RIGHT (nullable) side of
        // a LEFT JOIN. For an UNMATCHED (null-extended) row the generated column
        // must be NULL, NOT COALESCE(NULL, 99) = 99. A real row whose base is
        // NULL still yields 99. Result must be (1,7), (2,NULL), (3,99).
        assert_agree(
            &fconn,
            &rconn,
            "SELECT l.id, r.g FROM l LEFT JOIN r ON l.id = r.id ORDER BY l.id",
        )
        .await;
        // The anti-join must find exactly the null-extended row (l.id = 2). The
        // pre-fix code computed g = 99 for it and dropped it, returning zero.
        let anti = assert_agree(
            &fconn,
            &rconn,
            "SELECT l.id FROM l LEFT JOIN r ON l.id = r.id WHERE r.g IS NULL ORDER BY l.id",
        )
        .await;
        assert_eq!(
            anti, 1,
            "bd-3radn H1: LEFT JOIN anti-join on a COALESCE gen-col must return \
             exactly the null-extended row (regression returned zero)"
        );

        fconn.close().await.expect("close franken");
    });
}
