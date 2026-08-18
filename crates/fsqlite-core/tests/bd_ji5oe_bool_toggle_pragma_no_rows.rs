#![recursion_limit = "512"]

//! bd-ji5oe: a bool-toggle PRAGMA *assignment* (`PRAGMA x = ON`) must emit
//! ZERO rows, matching C SQLite 3.46.1, while a bare readback (`PRAGMA x`)
//! still emits its single 0/1 row. The five pragmas that route through
//! `apply_bool_toggle` are trusted_schema, read_uncommitted, cell_size_check,
//! checkpoint_fullfsync, and automatic_index (GH #282/#278/#262/#281/#283).
//!
//! rusqlite (real SQLite) is the oracle for both the row count and the value.
//! Setting a known value first sidesteps the trusted_schema compile-time
//! default divergence (SQLITE_TRUSTED_SCHEMA build flag) noted in the bead —
//! we never rely on the *default* readback, only on post-assignment behavior.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => {
            format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>())
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
            format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>())
        }
    }
}

async fn fq(fconn: &Connection, sql: &str) -> Vec<Vec<String>> {
    fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}
fn rq(rconn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// Assert both engines return byte-identical result sets (row count AND values)
/// for `sql`.
async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let mut fr = fq(fconn, sql).await;
    fr.sort();
    let mut rr = rq(rconn, sql);
    rr.sort();
    assert_eq!(fr, rr, "row-set mismatch on `{sql}`");
}

const BOOL_TOGGLE_PRAGMAS: [&str; 5] = [
    "trusted_schema",
    "read_uncommitted",
    "cell_size_check",
    "checkpoint_fullfsync",
    "automatic_index",
];

#[test]
fn bool_toggle_assignment_emits_no_rows_bare_emits_one_gh282() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        for p in BOOL_TOGGLE_PRAGMAS {
            // Assignment: ZERO rows on both engines (the core bd-ji5oe claim).
            let on = format!("PRAGMA {p} = ON");
            assert_agree(&f, &r, &on).await;
            assert!(
                fq(&f, &on).await.is_empty(),
                "`{on}` must emit no rows (bd-ji5oe)"
            );

            // Bare readback: one 0/1 row, agreeing with the oracle (value = 1 now).
            let bare = format!("PRAGMA {p}");
            assert_agree(&f, &r, &bare).await;
            assert_eq!(fq(&f, &bare).await.len(), 1, "`{bare}` must emit one row");

            // Flip OFF: still zero rows, then bare reads 0 on both.
            let off = format!("PRAGMA {p} = OFF");
            assert_agree(&f, &r, &off).await;
            assert!(
                fq(&f, &off).await.is_empty(),
                "`{off}` must emit no rows (bd-ji5oe)"
            );
            assert_agree(&f, &r, &bare).await;
        }
    });
}
