#![recursion_limit = "512"]

//! Differential oracle sweep (pane af49, 2026-08-20): frank vs rusqlite over DML
//! conflict resolution and RETURNING — INSERT OR REPLACE/IGNORE/ABORT on PK &
//! UNIQUE conflicts, RETURNING on INSERT/UPDATE/DELETE, multi-row UPDATE,
//! DELETE with subquery, INSERT ... SELECT. Distinct from UPSERT (ON CONFLICT
//! DO UPDATE). Pass = coverage keeper; a mismatch is a leaf. Each op is applied
//! to BOTH engines and the resulting table state (+ any RETURNING rows) compared.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

async fn fq(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = conn.prepare(sql).unwrap();
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

/// Run a mutating statement on both engines (via query, to capture RETURNING
/// rows) and compare the returned rows.
async fn both(f: &Connection, r: &rusqlite::Connection, sql: &str) -> Option<String> {
    let fr = fq(f, sql).await;
    let rr = rq(r, sql);
    if fr != rr {
        Some(format!("  `{sql}`\n     frank= {fr:?}\n     stock= {rr:?}"))
    } else {
        None
    }
}

#[test]
fn dml_conflict_and_returning_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();

        for s in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, u TEXT UNIQUE, v INTEGER)",
            "INSERT INTO t VALUES (1,'a',10),(2,'b',20),(3,'c',30)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        // Conflict resolution + RETURNING, applied to both engines.
        for sql in [
            "INSERT OR IGNORE INTO t VALUES (1,'x',999)",              // PK conflict -> ignored
            "INSERT OR REPLACE INTO t VALUES (2,'b2',222)",            // PK conflict -> replace
            "INSERT OR REPLACE INTO t(id,u,v) VALUES (99,'a',111)",    // UNIQUE(u) conflict -> replace row 1
            "INSERT INTO t VALUES (5,'e',50) RETURNING id, v, v*2",    // RETURNING
            "UPDATE t SET v = v + 1 WHERE v >= 20 RETURNING id, v",    // multi-row UPDATE RETURNING
            "DELETE FROM t WHERE id IN (SELECT id FROM t WHERE v > 100) RETURNING id",
            "INSERT INTO t(id,u,v) SELECT id+100, u||'_c', v FROM t WHERE v < 60 RETURNING id",
            "UPDATE OR IGNORE t SET u='b2' WHERE id=5",               // would violate UNIQUE -> ignore
        ] {
            if let Some(d) = both(&f, &r, sql).await {
                diffs.push(d);
            }
        }

        // Final table state must match exactly.
        if let Some(d) = both(&f, &r, "SELECT id,u,v FROM t ORDER BY id").await {
            diffs.push(d);
        }
        if let Some(d) = both(
            &f,
            &r,
            "SELECT count(*), sum(v), group_concat(u,'|') FROM t",
        )
        .await
        {
            diffs.push(d);
        }

        assert!(
            diffs.is_empty(),
            "{} dml/conflict/returning divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
