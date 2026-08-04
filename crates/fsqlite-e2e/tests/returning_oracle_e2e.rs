//! bd-7vqtc — Oracle-parity e2e: the RETURNING clause vs rusqlite (SQLite 3.35+).
//!
//! INSERT/UPDATE/DELETE ... RETURNING returns the affected rows. Covered edges:
//! RETURNING a column / `*` / an expression / an alias, multi-row INSERT
//! RETURNING, UPDATE RETURNING (sees NEW values), DELETE RETURNING (sees the
//! deleted rows), INSERT...SELECT RETURNING, and UPSERT (ON CONFLICT DO UPDATE)
//! RETURNING. RETURNING does not support ORDER BY and its row order is not
//! guaranteed for multi-row UPDATE/DELETE, so comparisons are order-insensitive
//! (the multiset of returned rows must match). All data is fixed.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

/// Run a RETURNING statement on FrankenSQLite and collect the returned rows.
async fn frank_returning(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_returning(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn setup() -> (Connection, rusqlite::Connection, &'static [&'static str]) {
    let ddl: &[&str] = &[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
        "INSERT INTO t VALUES (1,10,'x'),(2,20,'y'),(3,30,'z')",
    ];
    let fconn = Connection::open(":memory:").await.expect("open frank");
    let rconn = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in ddl {
        fconn
            .execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        rconn
            .execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (fconn, rconn, ddl)
}

/// Run the same RETURNING statement on both engines and assert the returned
/// rows match as a multiset (order-insensitive), then assert the post-state of
/// the table matches via a follow-up ORDER BY query.
async fn assert_returning(stmt: &str, post_query: &str, label: &str) {
    let (f, r, _) = setup().await;
    let mut fe = frank_returning(&f, stmt).await;
    let mut re = sqlite_returning(&r, stmt);
    if let (Ok(fr), Ok(rr)) = (fe.as_mut(), re.as_mut()) {
        fr.sort();
        rr.sort();
    }
    assert!(
        fe == re,
        "{label}: RETURNING rows differ for `{stmt}`\n  frank: {fe:?}\n  csql:  {re:?}"
    );
    // Post-mutation table state must also agree.
    let fp = frank_returning(&f, post_query).await;
    let rp = sqlite_returning(&r, post_query);
    assert!(
        fp == rp,
        "{label}: post-state differs for `{post_query}`\n  frank: {fp:?}\n  csql:  {rp:?}"
    );
}

#[test]
fn returning_insert_column_and_star() {
    asupersync::test_utils::run_test(|| async {
        assert_returning(
            "INSERT INTO t VALUES (4,40,'w') RETURNING id",
            "SELECT id, a, b FROM t ORDER BY id",
            "returning_insert_column",
        )
        .await;
        assert_returning(
            "INSERT INTO t VALUES (5,50,'v') RETURNING *",
            "SELECT id, a, b FROM t ORDER BY id",
            "returning_insert_star",
        )
        .await;
    });
}

#[test]
fn returning_insert_expression_and_alias() {
    asupersync::test_utils::run_test(|| async {
        assert_returning(
            "INSERT INTO t VALUES (6,60,'u') RETURNING id, a * 2 AS doubled, b || '!' ",
            "SELECT id FROM t ORDER BY id",
            "returning_insert_expression",
        )
        .await;
    });
}

#[test]
fn returning_multi_row_insert() {
    asupersync::test_utils::run_test(|| async {
        assert_returning(
            "INSERT INTO t VALUES (7,70,'g'),(8,80,'h'),(9,90,'i') RETURNING id, a",
            "SELECT id, a FROM t ORDER BY id",
            "returning_multi_row_insert",
        )
        .await;
    });
}

#[test]
fn returning_update_sees_new_values() {
    asupersync::test_utils::run_test(|| async {
        assert_returning(
            "UPDATE t SET a = a + 100 WHERE id IN (1,2) RETURNING id, a",
            "SELECT id, a FROM t ORDER BY id",
            "returning_update_multi",
        )
        .await;
        assert_returning(
            "UPDATE t SET b = 'updated' WHERE id = 3 RETURNING *",
            "SELECT id, a, b FROM t ORDER BY id",
            "returning_update_star",
        )
        .await;
    });
}

#[test]
fn returning_delete_sees_deleted_rows() {
    asupersync::test_utils::run_test(|| async {
        assert_returning(
            "DELETE FROM t WHERE a >= 20 RETURNING id, a, b",
            "SELECT id FROM t ORDER BY id",
            "returning_delete_multi",
        )
        .await;
        assert_returning(
            "DELETE FROM t WHERE id = 1 RETURNING *",
            "SELECT count(*) FROM t",
            "returning_delete_star",
        )
        .await;
    });
}

#[test]
fn returning_insert_select() {
    asupersync::test_utils::run_test(|| async {
        // INSERT ... SELECT ... RETURNING returns the inserted rows.
        let (f, r, _) = setup().await;
        for s in [
            "CREATE TABLE src (id INTEGER, a INTEGER, b TEXT)",
            "INSERT INTO src VALUES (100,1,'p'),(200,2,'q')",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        let stmt = "INSERT INTO t SELECT id, a, b FROM src RETURNING id, a";
        let mut fe = frank_returning(&f, stmt).await;
        let mut re = sqlite_returning(&r, stmt);
        if let (Ok(fr), Ok(rr)) = (fe.as_mut(), re.as_mut()) {
            fr.sort();
            rr.sort();
        }
        assert!(
            fe == re,
            "returning_insert_select: frank {fe:?} vs csql {re:?}"
        );
        let fp = frank_returning(&f, "SELECT id, a, b FROM t ORDER BY id").await;
        let rp = sqlite_returning(&r, "SELECT id, a, b FROM t ORDER BY id");
        assert!(
            fp == rp,
            "returning_insert_select post: frank {fp:?} vs csql {rp:?}"
        );
    });
}

#[test]
fn returning_upsert_do_update() {
    asupersync::test_utils::run_test(|| async {
        let (f, r, _) = setup().await;
        let stmt = "INSERT INTO t(id, a, b) VALUES (2, 999, 'z2') \
                    ON CONFLICT(id) DO UPDATE SET a = excluded.a RETURNING id, a, b";
        let mut fe = frank_returning(&f, stmt).await;
        let mut re = sqlite_returning(&r, stmt);
        if let (Ok(fr), Ok(rr)) = (fe.as_mut(), re.as_mut()) {
            fr.sort();
            rr.sort();
        }
        assert!(fe == re, "returning_upsert: frank {fe:?} vs csql {re:?}");
        let fp = frank_returning(&f, "SELECT id, a, b FROM t ORDER BY id").await;
        let rp = sqlite_returning(&r, "SELECT id, a, b FROM t ORDER BY id");
        assert!(
            fp == rp,
            "returning_upsert post: frank {fp:?} vs csql {rp:?}"
        );
    });
}
