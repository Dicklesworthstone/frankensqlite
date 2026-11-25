#![recursion_limit = "512"]

//! bd-fts5-lazy-shadow-reads-itcc4.3 special commands: `'integrity-check'` and
//! `'flush'`.
//!
//! `INSERT INTO t(t) VALUES('integrity-check')` validates the persisted index by
//! scanning every segment's leaves through the page-boundary reader (no hydration);
//! a readable index returns Ok. `'flush'` is a no-op (FrankenSQLite persists every
//! statement). A cross-engine case runs integrity-check over a stock-written index
//! whose doclist spills across leaf pages, exercising the GH#360 stitcher.

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

fn rowids(rows: &[fsqlite_core::connection::Row]) -> Vec<i64> {
    rows.iter()
        .map(|row| match row.values()[0] {
            SqliteValue::Integer(n) => n,
            ref other => panic!("expected rowid integer, got {other:?}"),
        })
        .collect()
}

#[test]
fn bd_fts5_integrity_check_and_flush_accept_valid_index() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_integ.db");
        let db_str = db_path.to_string_lossy().into_owned();

        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(
                "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1);",
            )
            .await
            .unwrap();
            for id in 1..=5 {
                conn.execute(&format!(
                    "INSERT INTO t(rowid, body) VALUES ({id}, 'common term{id}');"
                ))
                .await
                .unwrap();
            }
            conn.close().await.unwrap();
        }

        let conn = Connection::open(&db_str).await.unwrap();
        // integrity-check on a valid index succeeds (no error).
        conn.execute("INSERT INTO t(t) VALUES('integrity-check');")
            .await
            .expect("integrity-check must pass on a valid index");
        // flush is accepted as a no-op.
        conn.execute("INSERT INTO t(t) VALUES('flush');")
            .await
            .expect("flush must be accepted");
        // Neither command disturbed the corpus.
        let matched = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(rowids(&matched), vec![1, 2, 3, 4, 5], "corpus intact");
        conn.close().await.unwrap();
    });
}

#[test]
fn bd_fts5_integrity_check_passes_on_stock_spilled_index() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_integ_spill.db");

        // Stock writes a corpus whose 'common' doclist spills across leaf pages
        // (pgsz=64, doc 1 with an 80-position poslist), optimized to one segment.
        {
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            stock
                .execute_batch(
                    "CREATE VIRTUAL TABLE t USING fts5(x, content='', contentless_delete=1);\n\
                     INSERT INTO t(t, rank) VALUES('pgsz', 64);",
                )
                .unwrap();
            for id in 1..=40 {
                stock
                    .execute(
                        "INSERT INTO t(rowid, x) VALUES (?1, ?2)",
                        rusqlite::params![id, format!("common w{id}")],
                    )
                    .unwrap();
            }
            let big = vec!["common"; 80].join(" ");
            stock
                .execute(
                    "UPDATE t SET x = ?1 WHERE rowid = 1",
                    rusqlite::params![big],
                )
                .unwrap();
            stock
                .execute_batch("INSERT INTO t(t) VALUES('optimize');")
                .unwrap();
            let leaf_pages: i64 = stock
                .query_row("SELECT count(*) FROM t_data WHERE id > 100", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert!(leaf_pages > 1, "fixture must spill, got {leaf_pages}");
        }

        // integrity-check scans the stock-split index through the stitcher: Ok.
        let conn = Connection::open(db_path.to_str().unwrap()).await.unwrap();
        conn.execute("INSERT INTO t(t) VALUES('integrity-check');")
            .await
            .expect("integrity-check must read the stock-spilled index without error");
        let matched = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(rowids(&matched), (1..=40).collect::<Vec<i64>>());
        conn.close().await.unwrap();

        // Keep the structure record valid so a schema-only open succeeds, but
        // corrupt one persisted leaf. The read-only maintenance command must
        // scan far enough to discover that planted negative itself.
        {
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            let leaf_id: i64 = stock
                .query_row("SELECT min(id) FROM t_data WHERE id > 100", [], |row| {
                    row.get(0)
                })
                .unwrap();
            stock
                .execute(
                    "UPDATE t_data SET block = X'FFFFFFFFFFFFFFFFFFFFFFFF' WHERE id = ?1",
                    [leaf_id],
                )
                .unwrap();
        }

        let conn = Connection::open_schema_only(db_path.to_str().unwrap())
            .await
            .expect("leaf corruption must not prevent the read-only schema open");
        let err = conn
            .execute("INSERT INTO t(t) VALUES('integrity-check');")
            .await
            .expect_err("the read-only integrity command must reject a corrupt leaf");
        assert!(
            !matches!(err, FrankenError::ReadOnly),
            "the semantic integrity result must not be hidden by the SQL write guard"
        );
        assert!(
            err.to_string().contains("malformed") || err.to_string().contains("corrupt"),
            "expected a corruption diagnostic, got {err:?}"
        );
        conn.close().await.unwrap();
    });
}

#[test]
fn bd_fts5_integrity_check_is_the_only_maintenance_insert_allowed_read_only() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_read_only_integrity.db");
        let db_str = db_path.to_string_lossy().into_owned();

        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(
                "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1);",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO t(rowid, body) VALUES (1, 'read only sentinel');")
                .await
                .unwrap();
            conn.close().await.unwrap();
        }

        let conn = Connection::open_schema_only(&db_str).await.unwrap();
        conn.execute("PRAGMA query_only = 1;")
            .await
            .expect("enable the application-level read-only guard");
        conn.execute("INSERT INTO t(t) VALUES('integrity-check');")
            .await
            .expect(
                "the FTS5 integrity decoder must run through a read-only pager with query_only enabled",
            );

        for command in ["flush", "optimize", "rebuild", "delete-all", "merge"] {
            let err = conn
                .execute(&format!("INSERT INTO t(t) VALUES('{command}');"))
                .await
                .expect_err("mutating FTS5 maintenance must stay read-only rejected");
            assert!(
                matches!(err, FrankenError::ReadOnly),
                "{command} must require a writable pager, got {err:?}"
            );
        }
        let err = conn
            .execute("INSERT INTO t(t, rank) VALUES('merge', 4);")
            .await
            .expect_err("parameterized merge must stay read-only rejected");
        assert!(
            matches!(err, FrankenError::ReadOnly),
            "merge(4) must require a writable pager, got {err:?}"
        );

        let err = conn
            .execute("INSERT INTO t(rowid, body) VALUES (2, 'forbidden write');")
            .await
            .expect_err("ordinary inserts must stay read-only rejected");
        assert!(matches!(err, FrankenError::ReadOnly));

        let matched = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'sentinel';")
            .await
            .unwrap();
        assert_eq!(rowids(&matched), vec![1]);
        conn.close().await.unwrap();
    });
}

fn stock_change_counts(conn: &rusqlite::Connection) -> (i64, i64) {
    conn.query_row("SELECT changes(), total_changes()", [], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
    })
    .unwrap()
}

async fn fsqlite_change_counts(conn: &Connection) -> (i64, i64) {
    let rows = conn
        .query("SELECT changes(), total_changes()")
        .await
        .unwrap();
    let vals = rows[0].values();
    let c = match vals[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected integer changes(), got {other:?}"),
    };
    let tc = match vals[1] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected integer total_changes(), got {other:?}"),
    };
    (c, tc)
}

async fn exec_fsqlite(conn: &Connection, sql: &str, prepared: bool) -> Result<(), String> {
    if prepared && !sql.trim_start().to_ascii_uppercase().starts_with("CREATE ") {
        let stmt = conn.prepare(sql).await.map_err(|e| e.to_string())?;
        stmt.execute().await.map(|_| ()).map_err(|e| e.to_string())
    } else {
        conn.execute(sql)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

#[test]
fn bd_w4pr1_36_fts5_maintenance_insert_changes_and_total_changes_match_stock() {
    #[derive(Clone, Copy)]
    enum StepKind {
        /// DDL setup (`CREATE VIRTUAL TABLE` / `CREATE TABLE`).
        Ddl,
        /// Non-mutating / no-op FTS5 special insert (`integrity-check`, `flush`,
        /// no-op `optimize`/`merge`): both engines must return `changes() == 1`
        /// and identical `total_changes()` delta (`+1`).
        ExactDeltaSpecialInsert,
        /// Mutating FTS5 special insert (`rebuild`, mutating `optimize`, `delete-all`)
        /// or ordinary FTS5 `INSERT`: `changes()` must match stock verbatim,
        /// FrankenSQLite's `total_changes()` must advance by `changes()` (never 0),
        /// and stock's `total_changes()` advances by at least `changes()` (stock
        /// also adds its internal recursive SQL shadow-table writes).
        ShadowMutatingDml,
        /// Ordinary non-vtab table DML: both `changes()` and `total_changes()`
        /// delta must match stock verbatim.
        OrdinaryTableDml,
    }

    const STEPS: &[(&str, StepKind)] = &[
        // Stored-content FTS5 table: ordinary INSERTs + rebuild, optimize, merge,
        // integrity-check, flush, and a trailing ordinary INSERT negative control.
        ("CREATE VIRTUAL TABLE f USING fts5(x)", StepKind::Ddl),
        ("INSERT INTO f VALUES ('a b')", StepKind::ShadowMutatingDml),
        (
            "INSERT INTO f VALUES ('c d'), ('e f')",
            StepKind::ShadowMutatingDml,
        ),
        (
            "INSERT INTO f(f) VALUES ('rebuild')",
            StepKind::ShadowMutatingDml,
        ),
        (
            "INSERT INTO f(f) VALUES ('optimize')",
            StepKind::ExactDeltaSpecialInsert,
        ),
        (
            "INSERT INTO f(f, rank) VALUES ('merge', 4)",
            StepKind::ExactDeltaSpecialInsert,
        ),
        (
            "INSERT INTO f(f) VALUES ('merge')",
            StepKind::ExactDeltaSpecialInsert,
        ),
        (
            "INSERT INTO f(f) VALUES ('integrity-check')",
            StepKind::ExactDeltaSpecialInsert,
        ),
        (
            "INSERT INTO f(f, rank) VALUES ('integrity-check', 0)",
            StepKind::ExactDeltaSpecialInsert,
        ),
        (
            "INSERT INTO f(f) VALUES ('flush')",
            StepKind::ExactDeltaSpecialInsert,
        ),
        ("INSERT INTO f VALUES ('g h')", StepKind::ShadowMutatingDml),
        // Contentless FTS5 table: supports delete-all in addition to optimize,
        // merge, integrity-check, flush, and ordinary inserts.
        (
            "CREATE VIRTUAL TABLE c USING fts5(x, content='', contentless_delete=1)",
            StepKind::Ddl,
        ),
        (
            "INSERT INTO c(rowid, x) VALUES (1, 'alpha beta')",
            StepKind::ShadowMutatingDml,
        ),
        (
            "INSERT INTO c(rowid, x) VALUES (2, 'beta gamma')",
            StepKind::ShadowMutatingDml,
        ),
        (
            "INSERT INTO c(c) VALUES ('optimize')",
            StepKind::ShadowMutatingDml,
        ),
        (
            "INSERT INTO c(c, rank) VALUES ('merge', 4)",
            StepKind::ExactDeltaSpecialInsert,
        ),
        (
            "INSERT INTO c(c) VALUES ('integrity-check')",
            StepKind::ExactDeltaSpecialInsert,
        ),
        (
            "INSERT INTO c(c) VALUES ('flush')",
            StepKind::ExactDeltaSpecialInsert,
        ),
        (
            "INSERT INTO c(c) VALUES ('delete-all')",
            StepKind::ShadowMutatingDml,
        ),
        (
            "INSERT INTO c(rowid, x) VALUES (3, 'post reset')",
            StepKind::ShadowMutatingDml,
        ),
        // External-content FTS5 table: rebuild + delete-all + rebuild.
        (
            "CREATE TABLE src(id INTEGER PRIMARY KEY, x TEXT)",
            StepKind::Ddl,
        ),
        (
            "INSERT INTO src VALUES (1, 'hello world'), (2, 'rust sqlite')",
            StepKind::OrdinaryTableDml,
        ),
        (
            "CREATE VIRTUAL TABLE e USING fts5(x, content='src', content_rowid='id')",
            StepKind::Ddl,
        ),
        (
            "INSERT INTO e(e) VALUES ('rebuild')",
            StepKind::ShadowMutatingDml,
        ),
        (
            "INSERT INTO e(e) VALUES ('delete-all')",
            StepKind::ShadowMutatingDml,
        ),
        (
            "INSERT INTO e(e) VALUES ('rebuild')",
            StepKind::ShadowMutatingDml,
        ),
    ];

    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let f_path = dir.path().join("frank.db");
                let r_path = dir.path().join("stock.db");
                let (f, r) = if file_backed {
                    (
                        Connection::open(f_path.to_str().unwrap()).await.unwrap(),
                        rusqlite::Connection::open(&r_path).unwrap(),
                    )
                } else {
                    (
                        Connection::open(":memory:").await.unwrap(),
                        rusqlite::Connection::open_in_memory().unwrap(),
                    )
                };

                let mode = format!(
                    "{}/{}",
                    if file_backed { "file" } else { ":memory:" },
                    if prepared { "prepared" } else { "direct" }
                );
                let mut prev_f_tc = 0_i64;
                let mut prev_r_tc = 0_i64;
                for &(sql, kind) in STEPS {
                    let r_res = r.execute(sql, []).map(|_| ()).map_err(|e| e.to_string());
                    let f_res = exec_fsqlite(&f, sql, prepared).await;
                    let (r_changes, r_tc) = stock_change_counts(&r);
                    let (f_changes, f_tc) = fsqlite_change_counts(&f).await;
                    let r_delta = r_tc - prev_r_tc;
                    let f_delta = f_tc - prev_f_tc;
                    prev_r_tc = r_tc;
                    prev_f_tc = f_tc;

                    if f_res.is_ok() != r_res.is_ok() {
                        diffs.push(format!(
                            "  [{mode}] `{sql}` outcome mismatch\n     frank= {f_res:?}\n     stock= {r_res:?}"
                        ));
                        continue;
                    }
                    match kind {
                        StepKind::Ddl => {}
                        StepKind::ExactDeltaSpecialInsert => {
                            if f_changes != r_changes || f_changes != 1 || f_delta != r_delta {
                                diffs.push(format!(
                                    "  [{mode}] `{sql}`\n     frank= changes={f_changes} total_delta={f_delta}\n     stock= changes={r_changes} total_delta={r_delta}"
                                ));
                            }
                        }
                        StepKind::ShadowMutatingDml => {
                            if f_changes != r_changes
                                || f_delta != f_changes
                                || r_delta < r_changes
                            {
                                diffs.push(format!(
                                    "  [{mode}] `{sql}`\n     frank= changes={f_changes} total_delta={f_delta}\n     stock= changes={r_changes} total_delta={r_delta}"
                                ));
                            }
                        }
                        StepKind::OrdinaryTableDml => {
                            if f_changes != r_changes || f_delta != r_delta {
                                diffs.push(format!(
                                    "  [{mode}] `{sql}`\n     frank= changes={f_changes} total_delta={f_delta}\n     stock= changes={r_changes} total_delta={r_delta}"
                                ));
                            }
                        }
                    }
                }

                // Corpus queries must also match stock after rebuild/optimize/merge/delete-all.
                for query in [
                    "SELECT rowid FROM f WHERE f MATCH 'a OR g' ORDER BY rowid",
                    "SELECT rowid FROM c WHERE c MATCH 'post' ORDER BY rowid",
                    "SELECT rowid FROM c WHERE c MATCH 'alpha' ORDER BY rowid",
                    "SELECT rowid FROM e WHERE e MATCH 'rust' ORDER BY rowid",
                ] {
                    let f_rows = rowids(&f.query(query).await.unwrap());
                    let r_rows: Vec<i64> = r
                        .prepare(query)
                        .unwrap()
                        .query_map([], |row| row.get(0))
                        .unwrap()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    if f_rows != r_rows {
                        diffs.push(format!(
                            "  [{mode}] corpus `{query}`\n     frank= {f_rows:?}\n     stock= {r_rows:?}"
                        ));
                    }
                }
                f.close().await.unwrap();
            }
        }

        assert!(
            diffs.is_empty(),
            "{} divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
