#![recursion_limit = "512"]

//! bd-pud7p: Resolve unquoted `TRUE` and `FALSE` as visible columns before
//! treating them as boolean literals, matching stock SQLite 3.54.1 across
//! direct and prepared execution, `:memory:` and file-backed databases,
//! aliases, ambiguous joins, no-FROM literal fallbacks, CTE/view/derived/outer
//! scopes, DML (`UPDATE`, `DELETE`, `UPSERT`, `RETURNING`), triggers, partial
//! and expression indexes, `CHECK`/`GENERATED`/`DEFAULT` clauses, and on-disk
//! cross-engine `PRAGMA integrity_check`.

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

fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap_or_else(|e| panic!("stock prepare `{sql}`: {e}"));
    let cols = stmt.column_count();
    stmt.query_map([], |row| {
        Ok((0..cols)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect::<Vec<_>>())
    })
    .unwrap_or_else(|e| panic!("stock query `{sql}`: {e}"))
    .collect::<Result<Vec<_>, _>>()
    .unwrap_or_else(|e| panic!("stock collect `{sql}`: {e}"))
}

async fn fq_direct(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank direct `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

async fn fq_prepared(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    let stmt = conn
        .prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("frank prepare `{sql}`: {e}"));
    stmt.query()
        .await
        .unwrap_or_else(|e| panic!("frank prepared query `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

async fn assert_query_parity(
    f: &Connection,
    r: &rusqlite::Connection,
    label: &str,
    sql: &str,
) {
    let stock = rq(r, sql);
    let direct = fq_direct(f, sql).await;
    let prepared = fq_prepared(f, sql).await;
    assert_eq!(direct, stock, "[{label}] direct mismatch for `{sql}`");
    assert_eq!(prepared, stock, "[{label}] prepared mismatch for `{sql}`");
}

async fn assert_both_error_contains(
    f: &Connection,
    r: &rusqlite::Connection,
    label: &str,
    sql: &str,
    needle: &str,
) {
    let stock_err = match r.prepare(sql) {
        Ok(_) => panic!("[{label}] stock must reject `{sql}`"),
        Err(err) => err.to_string(),
    };
    assert!(
        stock_err.to_ascii_lowercase().contains(needle),
        "[{label}] stock error `{stock_err}` missing `{needle}` for `{sql}`"
    );
    let direct_err = match f.query(sql).await {
        Ok(rows) => panic!("[{label}] frank direct must reject `{sql}`, got {rows:?}"),
        Err(err) => err.to_string(),
    };
    assert!(
        direct_err.to_ascii_lowercase().contains(needle),
        "[{label}] frank direct error `{direct_err}` missing `{needle}` for `{sql}`"
    );
    let prepared_err = match f.prepare(sql).await {
        Ok(_) => panic!("[{label}] frank prepare must reject `{sql}`"),
        Err(err) => err.to_string(),
    };
    assert!(
        prepared_err.to_ascii_lowercase().contains(needle),
        "[{label}] frank prepare error `{prepared_err}` missing `{needle}` for `{sql}`"
    );
}

async fn exec_both(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    f.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("frank execute `{sql}`: {e}"));
    r.execute_batch(sql)
        .unwrap_or_else(|e| panic!("stock execute `{sql}`: {e}"));
}

#[test]
#[allow(clippy::too_many_lines)]
fn true_false_column_resolution_scopes_aliases_and_dml_oracle() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("scopes.db");

        for (mode, path_str) in [
            ("memory", ":memory:".to_owned()),
            ("file", db_path.to_str().unwrap().to_owned()),
        ] {
            let f = Connection::open(&path_str).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            // Disable double-quoted string fallback in stock so we pin exact
            // identifier resolution semantics cleanly.
            r.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DQS_DML, false)
                .unwrap();
            r.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DQS_DDL, false)
                .unwrap();

            for ddl in [
                r#"CREATE TABLE t(a, "true", "false")"#,
                "INSERT INTO t VALUES (2, 1, 0), (2, 2, 2)",
                "CREATE TABLE t_plain(a)",
                "INSERT INTO t_plain VALUES (2), (5)",
                r#"CREATE TABLE t1(a, "true", "false")"#,
                "INSERT INTO t1 VALUES (2, 1, 0), (5, 2, 9)",
                r#"CREATE TABLE t2(b, "true", "false")"#,
                "INSERT INTO t2 VALUES (2, 2, 9), (9, 3, 4)",
            ] {
                exec_both(&f, &r, ddl).await;
            }

            // 1. Exact bead repro + FALSE + mixed case + IS NOT + COLLATE.
            let queries = [
                r#"SELECT "true", true, a IS true, a IS (true COLLATE nocase) FROM t ORDER BY "true""#,
                r#"SELECT "false", false, a IS false, a IS (false COLLATE nocase) FROM t ORDER BY "true""#,
                r#"SELECT True, FALSE, a IS NOT true, a IS NOT (true COLLATE rtrim), a IS NOT false FROM t ORDER BY "true""#,
                // Qualified column & table-alias-named-true/false.
                "SELECT t.true, t.false, main.t.true FROM t ORDER BY t.true",
                r#"SELECT true.a, true.true, true.false FROM t AS "true" ORDER BY true.true"#,
                r#"SELECT true.* FROM t AS "true" ORDER BY true.true"#,
                r#"SELECT false.a, false.false FROM t AS "false" ORDER BY false.true"#,
                // No-FROM literal fallback & tables without "true"/"false" columns keep truth-test semantics.
                "SELECT true, false, 2 IS true, 2 IS (true COLLATE nocase), 2 IS (true COLLATE missing_coll), NULL IS true, NULL IS NOT false",
                "SELECT a, true, false, a IS true, a IS (true COLLATE nocase), a IS NOT false FROM t_plain ORDER BY a",
                // Result-set AS aliases in WHERE, GROUP BY, HAVING, ORDER BY, no-FROM, and nested WHERE subquery.
                r#"SELECT a, a AS "true" FROM t_plain WHERE 2 IS true ORDER BY a"#,
                r#"SELECT a, a AS "false" FROM t_plain WHERE 5 IS false ORDER BY a"#,
                r#"SELECT a, a AS "true" FROM t_plain ORDER BY true"#,
                r#"SELECT a, a AS "true" FROM t_plain ORDER BY 2 IS true, a"#,
                r#"SELECT a, a AS "true" FROM t_plain GROUP BY true ORDER BY a"#,
                r#"SELECT a, a AS "true" FROM t_plain GROUP BY a HAVING 2 IS true ORDER BY a"#,
                r#"SELECT a, "true", a AS "true" FROM t1 ORDER BY true"#,
                r#"SELECT a, "true", a AS "true" FROM t1 ORDER BY true + 0"#,
                r#"SELECT a AS "true", true, a IS true FROM t_plain ORDER BY a"#,
                r#"SELECT a, true AS "true" FROM t_plain WHERE 2 IS true ORDER BY a"#,
                r#"SELECT a, false AS "true" FROM t_plain WHERE 2 IS true ORDER BY a"#,
                r#"SELECT x.a, x.a AS "true" FROM t_plain x LEFT JOIN t_plain y ON 2 IS true AND x.a = y.a ORDER BY x.a"#,
                r#"SELECT a, a AS "true" FROM t_plain WHERE EXISTS (SELECT 1 WHERE 2 IS true) ORDER BY a"#,
                r#"SELECT 2 AS a, 5 AS "true" WHERE a IS true"#,
                r#"SELECT 5 AS a, 5 AS "true" WHERE a IS true"#,
                // JOIN USING coalesces "true" without ambiguity.
                r#"SELECT t1.a, true, t1.a IS true FROM t1 JOIN t2 USING("true") ORDER BY t1.a"#,
                // Correlated outer scopes & inner shadowing.
                r#"SELECT a, (SELECT true), (SELECT 2 IS true) FROM t ORDER BY "true""#,
                r#"SELECT a, (SELECT true FROM t_plain LIMIT 1), (SELECT 2 IS true FROM t_plain LIMIT 1) FROM t ORDER BY "true""#,
                "SELECT t1.a, (SELECT true FROM t2 ORDER BY b LIMIT 1) FROM t1 ORDER BY t1.a",
                // CTEs & derived tables (inner resolves table col; outer follows sqlite3ColumnsFromExprList).
                r#"WITH c AS (SELECT a, true, a IS true AS eq FROM t ORDER BY "true") SELECT * FROM c"#,
                r#"WITH c("true") AS (SELECT 99) SELECT column1, true, 2 IS true FROM c"#,
                r#"SELECT a, (WITH c("true") AS (SELECT 99) SELECT true FROM c) FROM t ORDER BY "true""#,
                r#"SELECT a, (SELECT true FROM (SELECT 99 AS "true")) FROM t ORDER BY "true""#,
                // Compound SELECT ORDER BY with AS "true".
                r#"SELECT 10 AS "true" UNION ALL SELECT 5 ORDER BY true"#,
            ];
            for sql in queries {
                assert_query_parity(&f, &r, mode, sql).await;
            }

            // Verify exact expected values on the bead's canonical repro.
            assert_eq!(
                fq_direct(
                    &f,
                    r#"SELECT "true", true, a IS true, a IS (true COLLATE nocase) FROM t ORDER BY "true""#,
                )
                .await,
                vec![
                    vec!["int:1", "int:1", "int:0", "int:0"],
                    vec!["int:2", "int:2", "int:1", "int:1"],
                ],
                "[{mode}] exact canonical bead repro mismatch"
            );

            // 2. Ambiguous column errors (direct + prepared) & unknown function true().
            for (sql, needle) in [
                ("SELECT true FROM t1, t2", "ambiguous column name"),
                ("SELECT false FROM t1, t2", "ambiguous column name"),
                ("SELECT a IS true FROM t1 JOIN t2 ON t1.a = t2.b", "ambiguous column name"),
                ("UPDATE t1 SET a = true FROM t2 WHERE t1.a = t2.b", "ambiguous column name"),
                ("SELECT true()", "no such function"),
            ] {
                assert_both_error_contains(&f, &r, mode, sql, needle).await;
            }

            // 3. Views and CTAS parity.
            exec_both(
                &f,
                &r,
                r#"CREATE VIEW v_true AS SELECT a, true, a IS true AS eq FROM t ORDER BY "true""#,
            )
            .await;
            assert_query_parity(&f, &r, mode, "SELECT * FROM v_true").await;
            assert_query_parity(
                &f,
                &r,
                mode,
                "SELECT cid, name FROM pragma_table_info('v_true') ORDER BY cid",
            )
            .await;

            exec_both(
                &f,
                &r,
                r#"CREATE TABLE ctas AS SELECT a, "true", true, a IS true AS eq FROM t ORDER BY "true""#,
            )
            .await;
            assert_query_parity(&f, &r, mode, "SELECT * FROM ctas").await;
            assert_query_parity(&f, &r, mode, "PRAGMA table_info(ctas)").await;

            // 4. INSERT VALUES stays literal; UPDATE / UPDATE ... FROM / DELETE / UPSERT / RETURNING resolve columns.
            exec_both(&f, &r, r#"CREATE TABLE u(id INTEGER PRIMARY KEY, a, "true", "false")"#).await;
            exec_both(&f, &r, "INSERT INTO u(id, a, \"true\", \"false\") VALUES (1, true, false, true)").await;
            assert_query_parity(&f, &r, mode, r#"SELECT id, a, "true", "false" FROM u"#).await;

            exec_both(&f, &r, "INSERT INTO u VALUES (2, 2, 1, 0), (3, 2, 2, 5)").await;
            // Direct UPDATE with `WHERE a IS true` only touches id=3 (a=2, "true"=2), NOT id=2 (a=2, "true"=1).
            exec_both(&f, &r, "UPDATE u SET a = a + 100 WHERE a IS true").await;
            assert_query_parity(&f, &r, mode, r#"SELECT id, a, "true", "false" FROM u ORDER BY id"#).await;

            // Prepared UPDATE with table alias and `RETURNING`.
            let prep_upd_sql =
                "UPDATE u AS x SET a = x.true + true WHERE x.id = 2 AND a IS NOT true RETURNING id, a, true, a IS true";
            let stock_ret = rq(&r, prep_upd_sql);
            let f_prep_upd = f.prepare(prep_upd_sql).await.unwrap();
            let frank_ret: Vec<Vec<String>> = f_prep_upd
                .query()
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().iter().map(tag_f).collect())
                .collect();
            assert_eq!(frank_ret, stock_ret, "[{mode}] prepared UPDATE RETURNING mismatch");

            // UPDATE ... FROM where target lacks "true" and FROM table supplies "true".
            exec_both(&f, &r, "CREATE TABLE dst(id INTEGER PRIMARY KEY, v)").await;
            exec_both(&f, &r, "INSERT INTO dst VALUES (1, 0), (2, 0)").await;
            exec_both(&f, &r, r#"UPDATE dst SET v = true FROM t WHERE dst.id = t."true""#).await;
            assert_query_parity(&f, &r, mode, "SELECT id, v FROM dst ORDER BY id").await;

            // UPSERT: ON CONFLICT DO UPDATE resolves `true` to target column and `excluded.true` to excluded.
            exec_both(&f, &r, r#"CREATE TABLE up(id INTEGER PRIMARY KEY, a, "true")"#).await;
            exec_both(&f, &r, "INSERT INTO up VALUES (1, 10, 2), (2, 10, 3)").await;
            let upsert_sql = "INSERT INTO up VALUES (1, 99, 5), (2, 99, 7)
                ON CONFLICT(id) DO UPDATE SET a = true * 10 + excluded.true WHERE 2 IS true
                RETURNING id, a, true";
            let stock_up = rq(&r, upsert_sql);
            let frank_up = fq_direct(&f, upsert_sql).await;
            assert_eq!(frank_up, stock_up, "[{mode}] UPSERT RETURNING mismatch");
            assert_query_parity(&f, &r, mode, r#"SELECT id, a, "true" FROM up ORDER BY id"#).await;

            // DELETE with `WHERE a IS false`.
            exec_both(&f, &r, "DELETE FROM u WHERE id = 2 AND a IS true").await;
            assert_query_parity(&f, &r, mode, r#"SELECT id, a, "true", "false" FROM u ORDER BY id"#).await;

            // 5. Triggers: WHEN uses literal for bare `true`, body SELECT resolves `t."true"`.
            exec_both(&f, &r, "CREATE TABLE trig_log(a, b, eq)").await;
            exec_both(
                &f,
                &r,
                r#"CREATE TRIGGER tr_t AFTER INSERT ON t WHEN NEW."true" IS true BEGIN
                    INSERT INTO trig_log SELECT a, true, a IS true FROM t WHERE a IS true;
                END"#,
            )
            .await;
            exec_both(&f, &r, "INSERT INTO t VALUES (7, 7, 0)").await;
            assert_query_parity(&f, &r, mode, "SELECT a, b, eq FROM trig_log ORDER BY a, b").await;

            f.close().await.unwrap();
        }
    });
}

#[test]
#[allow(clippy::too_many_lines)]
fn true_false_indexes_constraints_and_file_format_cross_engine_oracle() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let frank_db = dir.path().join("frank_written.db");
        let stock_db = dir.path().join("stock_written.db");

        // Phase A: Write schema + rows + partial/expression indexes + CHECK + GENERATED + DEFAULT
        // through FrankenSQLite AND stock SQLite 3.54.1, compare every query (direct & prepared),
        // then cross-open BOTH files in the opposite engine and verify `PRAGMA integrity_check`!
        let f = Connection::open(frank_db.to_str().unwrap()).await.unwrap();
        let r = rusqlite::Connection::open(&stock_db).unwrap();
        r.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DQS_DML, false)
            .unwrap();
        r.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DQS_DDL, false)
            .unwrap();

        for sql in [
            r#"CREATE TABLE tp(id INTEGER PRIMARY KEY, a, "true", "false")"#,
            "INSERT INTO tp VALUES (1, 2, 1, 0), (2, 2, 2, 2), (3, 0, 0, 5), (4, NULL, NULL, 1)",
            // Partial indexes whose predicate references column "true" / "false" (NOT truth test!).
            "CREATE INDEX i_part_eq ON tp(id) WHERE a IS true",
            "CREATE INDEX i_part_coll ON tp(id) WHERE a IS (true COLLATE nocase)",
            "CREATE INDEX i_part_false ON tp(id) WHERE a IS false",
            // Simple column index on "true" and expression index on `true + 10`.
            "CREATE INDEX i_col_true ON tp(true)",
            "CREATE INDEX i_expr_true ON tp(true + 10)",
            // Table WITHOUT "true"/"false" column: partial index `WHERE a IS true` IS a truth test!
            "CREATE TABLE tp_plain(id INTEGER PRIMARY KEY, a)",
            "INSERT INTO tp_plain VALUES (1, NULL), (2, 0), (3, 1), (4, 2)",
            "CREATE INDEX i_plain_truth ON tp_plain(id) WHERE a IS true",
            // GENERATED (VIRTUAL & STORED) and CHECK with column "true", plus DEFAULT staying literal.
            r#"CREATE TABLE tg(
                id INTEGER PRIMARY KEY,
                a,
                "true",
                gv GENERATED ALWAYS AS (true * 10) VIRTUAL,
                gs GENERATED ALWAYS AS (true * 100) STORED,
                CHECK (a IS true OR "true" = 1)
            )"#,
            r#"INSERT INTO tg(id, a, "true") VALUES (1, 2, 1), (2, 2, 2)"#,
            r#"CREATE TABLE td(id INTEGER PRIMARY KEY, a DEFAULT true, "true" DEFAULT false)"#,
            "INSERT INTO td(id) VALUES (1)",
        ] {
            exec_both(&f, &r, sql).await;
        }

        // Violating CHECK(a IS true OR "true" = 1): a=2, "true"=3 => `a IS true` is 0, `"true"=1` is 0 => MUST FAIL!
        let bad_check = r#"INSERT INTO tg(id, a, "true") VALUES (3, 2, 3)"#;
        assert!(r.execute(bad_check, []).is_err(), "stock must reject bad CHECK");
        assert!(f.execute(bad_check).await.is_err(), "frank must reject bad CHECK");

        // Post-CREATE-INDEX DML churning rows in/out of partial & expression indexes.
        for sql in [
            "INSERT INTO tp VALUES (5, 9, 9, 9), (6, 9, 1, 9)",
            r#"UPDATE tp SET "true" = 2 WHERE id = 1"#,
            "DELETE FROM tp WHERE id = 3",
        ] {
            exec_both(&f, &r, sql).await;
        }

        let index_queries = [
            "SELECT id FROM tp INDEXED BY i_part_eq WHERE a IS true ORDER BY id",
            "SELECT id FROM tp INDEXED BY i_part_coll WHERE a IS (true COLLATE nocase) ORDER BY id",
            "SELECT id FROM tp INDEXED BY i_part_false WHERE a IS false ORDER BY id",
            "SELECT id FROM tp INDEXED BY i_col_true WHERE true = 2 ORDER BY id",
            "SELECT id FROM tp INDEXED BY i_expr_true WHERE true + 10 = 12 ORDER BY id",
            "SELECT id FROM tp_plain INDEXED BY i_plain_truth WHERE a IS true ORDER BY id",
            r#"SELECT id, a, "true", gv, gs FROM tg ORDER BY id"#,
            r#"SELECT id, a, "true" FROM td"#,
            "PRAGMA integrity_check",
        ];
        for sql in index_queries {
            assert_query_parity(&f, &r, "file-index", sql).await;
        }

        // Verify partial index on `tp` with missing collation in `a IS (true COLLATE missing)` FAILS
        // because `"true"` is a column (NOT a truth test!), whereas on `tp_plain` it SUCCEEDS!
        assert!(
            r.execute("CREATE INDEX i_bad_coll ON tp(id) WHERE a IS (true COLLATE missing_coll)", [])
                .is_err(),
            "stock rejects missing collation on column equality partial index"
        );
        assert!(
            f.execute("CREATE INDEX i_bad_coll ON tp(id) WHERE a IS (true COLLATE missing_coll)")
                .await
                .is_err(),
            "frank rejects missing collation on column equality partial index"
        );

        f.close().await.unwrap();
        drop(r);

        // Phase B: Cross-engine file format verification!
        // 1) Stock SQLite 3.54.1 opens the FrankenSQLite-written file -> PRAGMA integrity_check == ok
        //    and every indexed query matches!
        let r_on_frank = rusqlite::Connection::open(&frank_db).unwrap();
        assert_eq!(
            rq(&r_on_frank, "PRAGMA integrity_check"),
            vec![vec!["text:ok"]],
            "stock SQLite 3.54.1 PRAGMA integrity_check failed on FrankenSQLite-written file"
        );
        let r_on_stock = rusqlite::Connection::open(&stock_db).unwrap();
        for sql in index_queries {
            assert_eq!(
                rq(&r_on_frank, sql),
                rq(&r_on_stock, sql),
                "stock reading frank file diverged on `{sql}`"
            );
        }
        drop(r_on_frank);

        // 2) FrankenSQLite opens the stock-SQLite-written file -> PRAGMA integrity_check == ok
        //    and every indexed query (direct + prepared) matches!
        let f_on_stock = Connection::open(stock_db.to_str().unwrap()).await.unwrap();
        assert_eq!(
            fq_direct(&f_on_stock, "PRAGMA integrity_check").await,
            vec![vec!["text:ok"]],
            "FrankenSQLite PRAGMA integrity_check failed on stock-written file"
        );
        for sql in index_queries {
            assert_query_parity(&f_on_stock, &r_on_stock, "frank-reads-stock", sql).await;
        }
        f_on_stock.close().await.unwrap();
    });
}
