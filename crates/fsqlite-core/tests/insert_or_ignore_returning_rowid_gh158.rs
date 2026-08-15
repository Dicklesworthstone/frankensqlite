#![recursion_limit = "512"]

//! GH #158 (bd-gh-insert-or-ignore-returning): `INSERT OR IGNORE ... RETURNING`
//! (and `ON CONFLICT DO NOTHING ... RETURNING`) must emit NO row for a row that
//! is skipped on a rowid / INTEGER-PRIMARY-KEY conflict. Before the fix,
//! fsqlite's Opcode::Insert suppressed the write on conflict but fell through to
//! emit_returning, which re-seeked the rowid and wrongly emitted the
//! PRE-EXISTING row. rusqlite is the oracle for the RETURNING output.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}

/// Run a (possibly RETURNING) statement on both engines and return the sorted
/// row tags plus whether each engine errored, so a caller can assert agreement.
async fn both(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    sql: &str,
) -> (Result<Vec<Vec<String>>, ()>, Result<Vec<Vec<String>>, ()>) {
    let f = match fconn.query(sql).await {
        Ok(rows) => Ok({
            let mut v: Vec<Vec<String>> =
                rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect();
            v.sort();
            v
        }),
        Err(_) => Err(()),
    };
    let r = (|| {
        let mut stmt = rconn.prepare(sql).map_err(|_| ())?;
        let n = stmt.column_count();
        let mut rows: Vec<Vec<String>> = stmt
            .query_map([], |row| {
                Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())
            })
            .map_err(|_| ())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ())?;
        rows.sort();
        Ok(rows)
    })();
    (f, r)
}

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let (f, r) = both(fconn, rconn, sql).await;
    assert_eq!(f, r, "GH#158 divergence on `{sql}`\n  frank: {f:?}\n  csql:  {r:?}");
}

async fn seed(fconn: &Connection, rconn: &rusqlite::Connection) {
    for s in [
        "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER)",
        "INSERT INTO a VALUES (1, 10)",
    ] {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn insert_or_ignore_returning_skips_conflicting_rowid_values() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;

        // Conflict on the INTEGER PK: RETURNING must emit NO rows on both engines.
        assert_agree(&f, &r, "INSERT OR IGNORE INTO a VALUES (1, 99) RETURNING id, v").await;
        // Non-conflicting: RETURNING emits the newly inserted row.
        assert_agree(&f, &r, "INSERT OR IGNORE INTO a VALUES (2, 20) RETURNING id, v").await;
        // Mixed multi-row: only the non-conflicting rows are returned.
        assert_agree(
            &f,
            &r,
            "INSERT OR IGNORE INTO a VALUES (1, 111), (3, 30), (2, 222) RETURNING id, v",
        )
        .await;
        // The pre-existing rows were left untouched; only new ids landed.
        assert_agree(&f, &r, "SELECT id, v FROM a ORDER BY id").await;
    });
}

#[test]
fn insert_or_ignore_returning_skips_conflicting_rowid_select() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // INSERT ... SELECT ... RETURNING on a conflicting rowid: no row emitted.
        assert_agree(
            &f,
            &r,
            "INSERT OR IGNORE INTO a SELECT 1, 999 UNION ALL SELECT 4, 40 RETURNING id, v",
        )
        .await;
        assert_agree(&f, &r, "SELECT id, v FROM a ORDER BY id").await;
    });
}

#[test]
fn on_conflict_do_nothing_returning_skips_conflicting_rowid() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // ON CONFLICT DO NOTHING ... RETURNING on a conflicting rowid: no row.
        assert_agree(
            &f,
            &r,
            "INSERT INTO a VALUES (1, 99) ON CONFLICT DO NOTHING RETURNING id, v",
        )
        .await;
        assert_agree(&f, &r, "SELECT id, v FROM a ORDER BY id").await;
    });
}
