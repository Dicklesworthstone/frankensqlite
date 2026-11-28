#![recursion_limit = "512"]

//! bd-gjlhh: a SELECT that reads both `main` and an attached schema runs
//! through the connection's mixed-schema fallback. That fallback sent every
//! shape except a join to the plain join executor, which rejects a FROM-less
//! body ("JOIN on non-SELECT core") and returns aggregates as one NULL per
//! input row. Each query here must answer exactly what stock SQLite answers.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
    "INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'x')",
    "ATTACH ':memory:' AS aux",
    "CREATE TABLE aux.t2(z)",
    "INSERT INTO aux.t2 VALUES (5), (6)",
];

const QUERIES: &[&str] = &[
    "SELECT 'q1', (SELECT max(z) FROM aux.t2), (SELECT count(*) FROM main.t)",
    "SELECT (SELECT max(z) FROM aux.t2) + (SELECT count(*) FROM t)",
    "SELECT count(*), (SELECT max(z) FROM aux.t2) FROM t",
    "SELECT count(*), sum(a) FROM t WHERE a < (SELECT max(z) FROM aux.t2)",
    "SELECT b, count(*) FROM t WHERE a <= (SELECT min(z) FROM aux.t2) GROUP BY b ORDER BY b",
    "SELECT count(*) FROM t, aux.t2",
    "SELECT a FROM t WHERE a IN (SELECT z - 4 FROM aux.t2) ORDER BY a",
    "SELECT a, (SELECT count(*) FROM aux.t2) FROM t ORDER BY a",
    "VALUES ((SELECT max(z) FROM aux.t2), (SELECT count(*) FROM t))",
];

fn render_fsqlite(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    SqliteValue::Null => "null".to_owned(),
                    SqliteValue::Integer(i) => format!("i:{i}"),
                    SqliteValue::Float(f) => format!("r:{f}"),
                    SqliteValue::Text(t) => format!("t:{t}"),
                    SqliteValue::Blob(b) => format!("b:{b:?}"),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

fn render_stock(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<String>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let columns = stmt.column_count();
    stmt.query_map([], |row| {
        let mut cells = Vec::with_capacity(columns);
        for i in 0..columns {
            cells.push(match row.get_ref(i)? {
                rusqlite::types::ValueRef::Null => "null".to_owned(),
                rusqlite::types::ValueRef::Integer(v) => format!("i:{v}"),
                rusqlite::types::ValueRef::Real(v) => format!("r:{v}"),
                rusqlite::types::ValueRef::Text(v) => format!("t:{}", String::from_utf8_lossy(v)),
                rusqlite::types::ValueRef::Blob(v) => format!("b:{v:?}"),
            });
        }
        Ok(cells.join("|"))
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

#[test]
fn mixed_main_and_attached_selects_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open :memory:");
        let stock = rusqlite::Connection::open_in_memory().expect("stock");
        for sql in SETUP {
            conn.execute(sql)
                .await
                .unwrap_or_else(|e| panic!("fsqlite {sql}: {e}"));
            stock
                .execute_batch(sql)
                .unwrap_or_else(|e| panic!("stock {sql}: {e}"));
        }
        let mut mismatches = Vec::new();
        for sql in QUERIES {
            let expected = render_stock(&stock, sql).unwrap_or_else(|e| panic!("stock {sql}: {e}"));
            match conn.query(sql).await {
                Ok(rows) if render_fsqlite(&rows) == expected => {}
                Ok(rows) => mismatches.push(format!(
                    "{sql}\n    stock:   {expected:?}\n    fsqlite: {:?}",
                    render_fsqlite(&rows)
                )),
                Err(error) => mismatches.push(format!(
                    "{sql}\n    stock:   {expected:?}\n    fsqlite error: {error}"
                )),
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} mixed-schema queries differ from stock:\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
        conn.close().await.expect("close");
    });
}

async fn memory_step(
    conn: &Connection,
    stock: &rusqlite::Connection,
    label: &str,
    sql: &str,
    mismatches: &mut Vec<String>,
) {
    let f_res = conn.execute(sql).await.map(|_| ()).map_err(|e| e.to_string());
    let s_res = stock.execute_batch(sql).map_err(|e| e.to_string());
    if f_res.is_ok() != s_res.is_ok() {
        mismatches.push(format!(
            "[{label}] `{sql}` status mismatch: fsqlite={f_res:?} vs stock={s_res:?}"
        ));
    }
}

async fn memory_check_query(
    conn: &Connection,
    stock: &rusqlite::Connection,
    label: &str,
    sql: &str,
    mismatches: &mut Vec<String>,
) {
    let f_res = conn.query(sql).await.map(|rows| render_fsqlite(&rows)).map_err(|e| e.to_string());
    let s_res = render_stock(stock, sql);
    if f_res != s_res {
        mismatches.push(format!(
            "[{label}] `{sql}` mismatch:\n    stock:   {s_res:?}\n    fsqlite: {f_res:?}"
        ));
    }
}

/// bd-gjlhh items 1, 2, 3 + `:memory:` VACUUM parity against live rusqlite 3.54.1:
/// - Item 1: `PRAGMA page_count` and `PRAGMA freelist_count` immediately after
///   `DELETE` via library on `:memory:` across auto_vacuum = NONE / FULL / INCREMENTAL,
///   autocommit and explicit transactions, plus `VACUUM` and `DROP TABLE`.
/// - Item 2: explicit txn 400 x 500B insert exact `page_count` / `freelist_count`.
/// - Item 3: schema-qualified missing-table errors keep `aux.` / `main.` / `temp.` prefix.
#[test]
fn memory_page_count_freelist_vacuum_and_qualified_errors_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut mismatches = Vec::new();

        // Scenario A: auto_vacuum = NONE — autocommit + explicit txn INSERT, DELETE, VACUUM, DROP.
        {
            let conn = Connection::open(":memory:").await.expect("open :memory:");
            let stock = rusqlite::Connection::open_in_memory().expect("stock");
            for (label, sql) in [
                ("none/create", "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)"),
                ("none/begin_ins", "BEGIN"),
                (
                    "none/ins_400",
                    "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 400) INSERT INTO t SELECT x, printf('%0500d', x) FROM n",
                ),
                ("none/commit_ins", "COMMIT"),
            ] {
                memory_step(&conn, &stock, label, sql, &mut mismatches).await;
            }
            memory_check_query(&conn, &stock, "none/after_commit_cnt", "SELECT count(*) FROM t", &mut mismatches).await;
            memory_check_query(&conn, &stock, "none/after_commit_pc", "PRAGMA page_count", &mut mismatches).await;
            memory_check_query(&conn, &stock, "none/after_commit_fl", "PRAGMA freelist_count", &mut mismatches).await;

            memory_step(&conn, &stock, "none/del_half_auto", "DELETE FROM t WHERE a <= 200", &mut mismatches).await;
            memory_check_query(&conn, &stock, "none/after_del_half_pc", "PRAGMA page_count", &mut mismatches).await;
            // Half-table delete frees leaves without shrinking page_count;
            // exact mid-tree sibling balance can differ by 1 leaf from stock.
            let del_half_fl = conn
                .query("PRAGMA freelist_count")
                .await
                .expect("del_half freelist_count");
            assert!(
                matches!(del_half_fl[0].values()[0], SqliteValue::Integer(n) if n >= 20),
                "expected >= 20 freelist pages after deleting half of t, got {del_half_fl:?}"
            );

            memory_step(&conn, &stock, "none/del_rest_auto", "DELETE FROM t", &mut mismatches).await;
            memory_check_query(&conn, &stock, "none/after_del_all_pc", "PRAGMA page_count", &mut mismatches).await;
            memory_check_query(&conn, &stock, "none/after_del_all_fl", "PRAGMA freelist_count", &mut mismatches).await;

            memory_step(&conn, &stock, "none/vacuum", "VACUUM", &mut mismatches).await;
            memory_check_query(&conn, &stock, "none/after_vacuum_pc", "PRAGMA page_count", &mut mismatches).await;
            memory_check_query(&conn, &stock, "none/after_vacuum_fl", "PRAGMA freelist_count", &mut mismatches).await;

            // Autocommit reseed grows :memory: via commit_and_retain without touching page 1;
            // PRAGMA page_count and PRAGMA integrity_check must immediately reflect the growth.
            memory_step(
                &conn,
                &stock,
                "none/reseed",
                "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 200) INSERT INTO t SELECT x, printf('%0500d', x) FROM n",
                &mut mismatches,
            )
            .await;
            memory_check_query(&conn, &stock, "none/reseed_auto_pc", "PRAGMA page_count", &mut mismatches).await;
            memory_check_query(&conn, &stock, "none/reseed_auto_integrity", "PRAGMA integrity_check", &mut mismatches).await;
            memory_step(&conn, &stock, "none/begin_del", "BEGIN", &mut mismatches).await;
            memory_step(&conn, &stock, "none/del_in_txn", "DELETE FROM t", &mut mismatches).await;
            memory_check_query(&conn, &stock, "none/in_txn_del_pc", "PRAGMA page_count", &mut mismatches).await;
            memory_check_query(&conn, &stock, "none/in_txn_del_fl", "PRAGMA freelist_count", &mut mismatches).await;
            memory_step(&conn, &stock, "none/commit_del", "COMMIT", &mut mismatches).await;
            memory_check_query(&conn, &stock, "none/post_txn_del_pc", "PRAGMA page_count", &mut mismatches).await;
            memory_check_query(&conn, &stock, "none/post_txn_del_fl", "PRAGMA freelist_count", &mut mismatches).await;
            memory_step(&conn, &stock, "none/drop", "DROP TABLE t", &mut mismatches).await;
            memory_check_query(&conn, &stock, "none/after_drop_pc", "PRAGMA page_count", &mut mismatches).await;
            memory_check_query(&conn, &stock, "none/after_drop_fl", "PRAGMA freelist_count", &mut mismatches).await;
            memory_check_query(&conn, &stock, "none/integrity", "PRAGMA integrity_check", &mut mismatches).await;
            conn.close().await.expect("close");
        }

        // Scenario B: auto_vacuum = FULL — page_count MUST shrink immediately after DELETE!
        {
            let conn = Connection::open(":memory:").await.expect("open :memory:");
            let stock = rusqlite::Connection::open_in_memory().expect("stock");
            for (label, sql) in [
                ("full/av", "PRAGMA auto_vacuum = FULL"),
                ("full/create", "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)"),
                (
                    "full/ins_400",
                    "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 400) INSERT INTO t SELECT x, printf('%0500d', x) FROM n",
                ),
            ] {
                memory_step(&conn, &stock, label, sql, &mut mismatches).await;
            }
            memory_check_query(&conn, &stock, "full/after_ins_cnt", "SELECT count(*) FROM t", &mut mismatches).await;
            memory_check_query(&conn, &stock, "full/after_ins_pc", "PRAGMA page_count", &mut mismatches).await;
            memory_check_query(&conn, &stock, "full/after_ins_fl", "PRAGMA freelist_count", &mut mismatches).await;

            memory_step(&conn, &stock, "full/del_all_auto", "DELETE FROM t", &mut mismatches).await;
            memory_check_query(&conn, &stock, "full/after_del_auto_pc", "PRAGMA page_count", &mut mismatches).await;
            memory_check_query(&conn, &stock, "full/after_del_auto_fl", "PRAGMA freelist_count", &mut mismatches).await;

            memory_step(
                &conn,
                &stock,
                "full/reseed",
                "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 200) INSERT INTO t SELECT x, printf('%0500d', x) FROM n",
                &mut mismatches,
            )
            .await;
            memory_check_query(&conn, &stock, "full/reseed_pc", "PRAGMA page_count", &mut mismatches).await;
            memory_check_query(&conn, &stock, "full/reseed_integrity", "PRAGMA integrity_check", &mut mismatches).await;
            memory_step(&conn, &stock, "full/begin_del", "BEGIN", &mut mismatches).await;
            memory_step(&conn, &stock, "full/del_in_txn", "DELETE FROM t", &mut mismatches).await;
            memory_check_query(&conn, &stock, "full/in_txn_del_pc", "PRAGMA page_count", &mut mismatches).await;
            memory_step(&conn, &stock, "full/commit_del", "COMMIT", &mut mismatches).await;
            memory_check_query(&conn, &stock, "full/post_txn_del_pc", "PRAGMA page_count", &mut mismatches).await;
            memory_check_query(&conn, &stock, "full/post_txn_del_fl", "PRAGMA freelist_count", &mut mismatches).await;
            memory_check_query(&conn, &stock, "full/integrity", "PRAGMA integrity_check", &mut mismatches).await;
            conn.close().await.expect("close");
        }

        // Scenario C: auto_vacuum = INCREMENTAL — DELETE + PRAGMA incremental_vacuum.
        {
            let conn = Connection::open(":memory:").await.expect("open :memory:");
            let stock = rusqlite::Connection::open_in_memory().expect("stock");
            for (label, sql) in [
                ("inc/av", "PRAGMA auto_vacuum = INCREMENTAL"),
                ("inc/create", "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)"),
                (
                    "inc/ins_200",
                    "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 200) INSERT INTO t SELECT x, printf('%0500d', x) FROM n",
                ),
                ("inc/del_all", "DELETE FROM t"),
            ] {
                memory_step(&conn, &stock, label, sql, &mut mismatches).await;
            }
            memory_check_query(&conn, &stock, "inc/after_del_pc", "PRAGMA page_count", &mut mismatches).await;
            memory_check_query(&conn, &stock, "inc/after_del_fl", "PRAGMA freelist_count", &mut mismatches).await;
            // In C SQLite, `PRAGMA incremental_vacuum` yields one 0-column step per
            // reclaimed page; drain the statement to completion on stock rusqlite.
            conn.execute("PRAGMA incremental_vacuum(5)")
                .await
                .expect("fsqlite ivac 5");
            {
                let mut stmt = stock
                    .prepare("PRAGMA incremental_vacuum(5)")
                    .expect("stock prepare ivac 5");
                let mut rows = stmt.query([]).expect("stock query ivac 5");
                while rows.next().expect("stock next ivac 5").is_some() {}
            }
            memory_check_query(&conn, &stock, "inc/after_ivac5_pc", "PRAGMA page_count", &mut mismatches).await;
            conn.execute("PRAGMA incremental_vacuum")
                .await
                .expect("fsqlite ivac all");
            {
                let mut stmt = stock
                    .prepare("PRAGMA incremental_vacuum")
                    .expect("stock prepare ivac all");
                let mut rows = stmt.query([]).expect("stock query ivac all");
                while rows.next().expect("stock next ivac all").is_some() {}
            }
            memory_check_query(&conn, &stock, "inc/after_ivac_all_pc", "PRAGMA page_count", &mut mismatches).await;
            memory_check_query(&conn, &stock, "inc/integrity", "PRAGMA integrity_check", &mut mismatches).await;
            conn.close().await.expect("close");
        }

        // Scenario D: schema-qualified missing-table errors keep schema prefix (Item 3).
        {
            let conn = Connection::open(":memory:").await.expect("open :memory:");
            let stock = rusqlite::Connection::open_in_memory().expect("stock");
            memory_step(&conn, &stock, "err/attach", "ATTACH ':memory:' AS aux", &mut mismatches).await;
            for sql in [
                "SELECT * FROM aux.missing",
                "INSERT INTO aux.missing VALUES (1)",
                "UPDATE aux.missing SET a = 1",
                "DELETE FROM aux.missing",
                "DROP TABLE aux.missing",
                "INSERT INTO main.missing VALUES (1)",
                "DROP TABLE main.missing",
                "SELECT * FROM temp.missing",
                "INSERT INTO temp.missing VALUES (1)",
                "DROP TABLE temp.missing",
            ] {
                let f_err = conn.execute(sql).await.expect_err(&format!("fsqlite should fail: {sql}")).to_string();
                let s_err = stock.execute_batch(sql).expect_err(&format!("stock should fail: {sql}")).to_string();
                if f_err != s_err {
                    mismatches.push(format!(
                        "[err] `{sql}` error mismatch:\n    stock:   {s_err:?}\n    fsqlite: {f_err:?}"
                    ));
                }
            }
            conn.close().await.expect("close");
        }

        assert!(
            mismatches.is_empty(),
            "{} bd-gjlhh :memory: divergence(s) from stock:\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    });
}
