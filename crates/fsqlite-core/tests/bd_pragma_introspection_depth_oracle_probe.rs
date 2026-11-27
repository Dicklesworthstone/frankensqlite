#![recursion_limit = "512"]

//! PRAGMA schema-introspection depth leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over the introspection PRAGMAs exposed as table-valued functions —
//! pragma_table_info (cid,name,type,notnull,dflt_value,pk), pragma_table_xinfo
//! (adds hidden + generated columns), pragma_index_list (seq,name,unique,origin,
//! partial), pragma_index_info / pragma_index_xinfo, and pragma_foreign_key_list
//! (id,seq,table,from,to,on_update,on_delete,match) — the last exercises the
//! `to`=NULL-for-implicit-PK case. Selected by explicit stable columns and
//! ordered, so opaque/version-specific ordering can't cause false diffs.
//! Pass = coverage keeper; a mismatch is a leaf divergence.

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
    match conn.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let Ok(mut st) = conn.prepare(sql) else {
        return vec![vec!["ERR".to_owned()]];
    };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect::<Vec<_>>())
    }) {
        Ok(rows) => rows
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}

#[test]
fn pragma_introspection_depth_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE parent(pid INTEGER PRIMARY KEY, code TEXT UNIQUE)",
            "CREATE TABLE child(\
               id INTEGER PRIMARY KEY, \
               name TEXT NOT NULL DEFAULT 'anon', \
               qty INTEGER DEFAULT 0, \
               ppid INTEGER REFERENCES parent(pid) ON DELETE CASCADE ON UPDATE SET NULL, \
               pcode TEXT REFERENCES parent(code), \
               total INTEGER GENERATED ALWAYS AS (qty * 2) VIRTUAL)",
            "CREATE INDEX ix_child_name ON child(name)",
            "CREATE UNIQUE INDEX ux_child_ppid ON child(ppid) WHERE ppid IS NOT NULL",
            "CREATE INDEX ix_child_expr ON child(qty + 1)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }
        let mut diffs = Vec::new();
        let check =
            |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
                if fr != rr {
                    d.push(format!(
                        "  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            };

        // table_info: columns with type/notnull/default/pk
        check("table_info child",
            fq(&f, "SELECT cid,name,type,\"notnull\",dflt_value,pk FROM pragma_table_info('child') ORDER BY cid").await,
            rq(&r, "SELECT cid,name,type,\"notnull\",dflt_value,pk FROM pragma_table_info('child') ORDER BY cid"), &mut diffs);
        check("table_info parent",
            fq(&f, "SELECT cid,name,type,\"notnull\",dflt_value,pk FROM pragma_table_info('parent') ORDER BY cid").await,
            rq(&r, "SELECT cid,name,type,\"notnull\",dflt_value,pk FROM pragma_table_info('parent') ORDER BY cid"), &mut diffs);
        // table_xinfo: includes generated column (hidden flag distinguishes)
        check("table_xinfo child",
            fq(&f, "SELECT cid,name,type,\"notnull\",pk,hidden FROM pragma_table_xinfo('child') ORDER BY cid").await,
            rq(&r, "SELECT cid,name,type,\"notnull\",pk,hidden FROM pragma_table_xinfo('child') ORDER BY cid"), &mut diffs);

        // index_list: user + auto indexes, unique/origin/partial
        check("index_list child",
            fq(&f, "SELECT name,\"unique\",origin,partial FROM pragma_index_list('child') ORDER BY name").await,
            rq(&r, "SELECT name,\"unique\",origin,partial FROM pragma_index_list('child') ORDER BY name"), &mut diffs);
        check("index_list parent",
            fq(&f, "SELECT name,\"unique\",origin,partial FROM pragma_index_list('parent') ORDER BY name").await,
            rq(&r, "SELECT name,\"unique\",origin,partial FROM pragma_index_list('parent') ORDER BY name"), &mut diffs);

        // index_info: columns of a named index (seqno, cid, name)
        check(
            "index_info ix_child_name",
            fq(
                &f,
                "SELECT seqno,cid,name FROM pragma_index_info('ix_child_name') ORDER BY seqno",
            )
            .await,
            rq(
                &r,
                "SELECT seqno,cid,name FROM pragma_index_info('ix_child_name') ORDER BY seqno",
            ),
            &mut diffs,
        );
        // index_xinfo: adds desc, coll, key flag
        check("index_xinfo ux_child_ppid",
            fq(&f, "SELECT seqno,cid,name,\"desc\",coll,key FROM pragma_index_xinfo('ux_child_ppid') ORDER BY seqno").await,
            rq(&r, "SELECT seqno,cid,name,\"desc\",coll,key FROM pragma_index_xinfo('ux_child_ppid') ORDER BY seqno"), &mut diffs);
        // expression index: keyed column has cid = -2
        check(
            "index_xinfo expr",
            fq(
                &f,
                "SELECT seqno,cid,key FROM pragma_index_xinfo('ix_child_expr') ORDER BY seqno",
            )
            .await,
            rq(
                &r,
                "SELECT seqno,cid,key FROM pragma_index_xinfo('ix_child_expr') ORDER BY seqno",
            ),
            &mut diffs,
        );

        // foreign_key_list: the `to` = NULL for implicit-PK reference case + named-col case
        check("foreign_key_list child",
            fq(&f, "SELECT id,seq,\"table\",\"from\",\"to\",on_update,on_delete,match FROM pragma_foreign_key_list('child') ORDER BY id,seq").await,
            rq(&r, "SELECT id,seq,\"table\",\"from\",\"to\",on_update,on_delete,match FROM pragma_foreign_key_list('child') ORDER BY id,seq"), &mut diffs);

        // counts via the introspection tables
        check(
            "column count",
            fq(&f, "SELECT count(*) FROM pragma_table_info('child')").await,
            rq(&r, "SELECT count(*) FROM pragma_table_info('child')"),
            &mut diffs,
        );

        assert!(
            diffs.is_empty(),
            "{} PRAGMA introspection divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

#[test]
fn pragma_tvf_positional_schema_selects_the_named_catalog() {
    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.path().join("frank-pragma-schema.db");
            let f = Connection::open(if file_backed {
                fpath.to_str().unwrap()
            } else {
                ":memory:"
            })
            .await
            .unwrap();
            let r = if file_backed {
                rusqlite::Connection::open(dir.path().join("stock-pragma-schema.db")).unwrap()
            } else {
                rusqlite::Connection::open_in_memory().unwrap()
            };
            for sql in [
                "CREATE TABLE t(m INTEGER)",
                "CREATE TEMP TABLE t(p TEXT)",
                "ATTACH ':memory:' AS aux",
                "CREATE TABLE aux.t(a INTEGER,b TEXT GENERATED ALWAYS AS (a || 'x'))",
                "CREATE INDEX aux.ai ON t(a)",
                "CREATE INDEX temp.ai ON t(p)",
                "CREATE TABLE aux.child(pid INTEGER REFERENCES t(a))",
            ] {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in [
                "SELECT name FROM pragma_table_info('t')",
                "SELECT name FROM pragma_table_info('t',NULL)",
                "SELECT name FROM pragma_table_info('t','main')",
                "SELECT name FROM pragma_table_info('t','TEMP')",
                "SELECT name FROM pragma_table_info('t','AuX')",
                "SELECT name FROM pragma_table_info(x'74',x'617578')",
                "SELECT name,hidden FROM pragma_table_xinfo('t','aux') ORDER BY cid",
                "SELECT name FROM pragma_index_list('t','aux')",
                "SELECT name FROM pragma_index_list('t','main')",
                "SELECT name FROM pragma_index_info('ai','aux')",
                "SELECT name FROM pragma_index_info('ai','temp')",
                "SELECT name FROM pragma_index_xinfo('ai','aux') WHERE key=1",
                "SELECT \"from\" FROM pragma_foreign_key_list('child','aux')",
                "SELECT name FROM pragma_table_info(NULL,'aux')",
            ] {
                let expected = rq(&r, sql);
                assert_ne!(expected, vec![vec!["ERR"]], "stock control: {sql}");
                let direct = fq(&f, sql).await;
                if direct != expected {
                    diffs.push(format!("direct on file={file_backed}, {sql}: {direct:?} vs {expected:?}"));
                }
                let prepared = match f.prepare(sql).await {
                    Ok(statement) => statement.query().await.map(|rows| {
                        rows.iter().map(|row| row.values().iter().map(tag_f).collect()).collect::<Vec<Vec<String>>>()
                    }),
                    Err(error) => Err(error),
                };
                if prepared.as_ref().ok() != Some(&expected) {
                    diffs.push(format!("prepared on file={file_backed}, {sql}: {prepared:?} vs {expected:?}"));
                }
            }
            let sql = "SELECT name FROM pragma_table_info(?1,?2)";
            let prepared = f.prepare(sql).await.unwrap();
            let mut stock = r.prepare(sql).unwrap();
            for schema in [Some("main"), Some("AuX"), None, Some("TEMP")] {
                let expected = stock
                    .query_row(("t", schema), |row| row.get::<_, String>(0))
                    .unwrap();
                let params = [
                    SqliteValue::from("t"),
                    schema.map_or(SqliteValue::Null, SqliteValue::from),
                ];
                let direct = f.query_with_params(sql, &params).await.unwrap();
                let rows = prepared.query_with_params(&params).await.unwrap();
                let expected = [SqliteValue::from(expected.as_str())];
                assert_eq!(rows[0].values(), expected);
                assert_eq!(direct[0].values(), expected);
            }
            for sql in [
                "SELECT name FROM pragma_table_info('t','')",
                "SELECT name FROM pragma_table_info('t',1)",
                "SELECT name FROM pragma_table_info(NULL,'x''y')",
                "SELECT name FROM pragma_table_info('t','fsqlite')",
            ] {
                let rusqlite::Error::SqliteFailure(_, Some(expected)) =
                    r.query_row(sql, [], |row| row.get::<_, String>(0)).unwrap_err()
                else {
                    panic!("unexpected stock result for {sql}");
                };
                assert!(expected.starts_with("unknown database '"));
                assert_eq!(f.query(sql).await.unwrap_err().to_string(), expected);
                let stmt = f.prepare(sql).await.unwrap();
                assert_eq!(stmt.query().await.unwrap_err().to_string(), expected);
            }
            drop(prepared);
            f.close().await.unwrap();
        }
        assert!(diffs.is_empty(), "{}", diffs.join("\n"));
    });
}

#[test]
fn pragma_tvf_rejects_extra_arguments_at_prepare() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        f.execute("CREATE TABLE t(k)").await.unwrap();
        r.execute("CREATE TABLE t(k)", []).unwrap();
        let mut diffs = Vec::new();
        for sql in [
            "SELECT * FROM pragma_table_info('t','main',NULL) WHERE 0",
            "SELECT * FROM pragma_table_list('t',NULL)",
            "SELECT * FROM pragma_compile_options(NULL)",
            "SELECT * FROM PRAGMA_TABLE_XINFO('t','main',NULL)",
            "SELECT * FROM (SELECT * FROM pragma_index_info('i','main',NULL))",
            "WITH c AS (SELECT * FROM pragma_foreign_key_list('t','main',NULL)) SELECT * FROM c",
            "SELECT 1 FROM t CROSS JOIN pragma_database_list(NULL) LIMIT 0",
        ] {
            let Err(error) = r.prepare(sql) else {
                panic!("stock prepared an over-applied PRAGMA table function: {sql}");
            };
            let expected = match error {
                rusqlite::Error::SqlInputError { msg, .. }
                | rusqlite::Error::SqliteFailure(_, Some(msg)) => msg,
                other => panic!("unexpected stock error for {sql}: {other}"),
            };
            assert!(expected.starts_with("too many arguments on "));
            let prepared = f.prepare(sql).await.err().map(|error| error.to_string());
            let direct = f.query(sql).await.err().map(|error| error.to_string());
            if prepared.as_deref() != Some(expected.as_str())
                || direct.as_deref() != Some(expected.as_str())
            {
                diffs.push(format!("{sql}: prepare={prepared:?}, direct={direct:?}, stock={expected}"));
            }
        }
        for sql in [
            "SELECT name FROM pragma_table_info('t','main')",
            "SELECT name FROM pragma_table_list('t')",
            "SELECT count(*) > 0 FROM pragma_compile_options()",
        ] {
            assert_eq!(fq(&f, sql).await, rq(&r, sql), "{sql}");
        }
        f.close().await.unwrap();
        assert!(diffs.is_empty(), "{}", diffs.join("\n"));
    });
}

#[test]
fn pragma_tvf_identifiers_stop_at_the_first_nul() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(m)",
            "CREATE TEMP TABLE t(p)",
            "ATTACH ':memory:' AS aux",
            "CREATE TABLE aux.t(a)",
        ] {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }
        for (sql, name) in [
            ("SELECT name FROM pragma_table_info('t' || char(0) || 'tail','main')", "m"),
            ("SELECT name FROM pragma_table_info('t','temp' || char(0) || 'tail')", "p"),
            ("SELECT name FROM pragma_table_info(x'74007461696c',x'617578007461696c')", "a"),
        ] {
            let expected = rq(&r, sql);
            assert_eq!(expected, vec![vec![format!("text:{name}")]], "stock: {sql}");
            assert_eq!(fq(&f, sql).await, expected, "direct: {sql}");
            let statement = f.prepare(sql).await.unwrap();
            let rows = statement.query().await.unwrap();
            assert_eq!(rows.len(), 1, "prepared: {sql}");
            assert_eq!(rows[0].values(), &[SqliteValue::from(name)], "prepared: {sql}");
        }
        f.execute("DETACH aux").await.unwrap();
        f.close().await.unwrap();
    });
}

#[test]
fn unqualified_metadata_pragmas_follow_catalog_precedence() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "PRAGMA foreign_keys=OFF",
            "ATTACH ':memory:' AS first_db",
            "ATTACH ':memory:' AS second_db",
            "CREATE TABLE first_db.only_attached(first_col INTEGER)",
            "CREATE TABLE second_db.only_attached(second_col TEXT)",
            "CREATE INDEX first_db.attached_idx ON only_attached(first_col)",
            "CREATE INDEX second_db.attached_idx ON only_attached(second_col)",
            "CREATE TABLE first_db.parent(id INTEGER PRIMARY KEY)",
            "CREATE TABLE first_db.orphan(fid INTEGER REFERENCES parent(id))",
            "INSERT INTO first_db.orphan VALUES (123)",
            "CREATE TABLE main.same_name(local_pk INTEGER PRIMARY KEY) WITHOUT ROWID",
            "CREATE INDEX first_db.same_name ON orphan(fid)",
            "CREATE TABLE first_db.wr(attached_pk INTEGER PRIMARY KEY, spare) WITHOUT ROWID",
            "CREATE TABLE main.wr(local_col INTEGER)",
            "CREATE TABLE main.view_shadow(main_col TEXT)",
            "CREATE TEMP VIEW view_shadow AS SELECT 1 AS temp_col",
            "CREATE TEMP VIEW temp_type AS SELECT local_col FROM main.wr",
        ] {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }
        let mut diffs = Vec::new();
        for (setup, queries) in [
            (None, &[
                "PRAGMA table_info('only_attached')",
                "PRAGMA index_info('attached_idx')",
                "PRAGMA foreign_key_check('orphan')",
                "SELECT name FROM pragma_table_info('only_attached',NULL)",
                "SELECT name FROM pragma_table_xinfo('only_attached')",
                "SELECT name FROM pragma_table_info('only_attached','second_db')",
                "SELECT name FROM pragma_index_list('only_attached')",
                "SELECT name FROM pragma_index_xinfo('attached_idx') WHERE key=1",
                "SELECT name FROM pragma_index_info('same_name')",
                "SELECT count(*) FROM pragma_index_info('wr')",
                "SELECT name FROM pragma_index_info('wr','first_db')",
                "SELECT name FROM pragma_index_info('sqlite_autoindex_wr_1')",
                "SELECT \"from\" FROM pragma_foreign_key_list('orphan')",
                "SELECT \"table\" FROM pragma_foreign_key_check('orphan')",
                "SELECT count(*) FROM pragma_foreign_key_check()",
                "SELECT name FROM pragma_table_info('view_shadow')",
                "SELECT count(*) FROM pragma_foreign_key_check('view_shadow')",
                    "SELECT type FROM pragma_table_info('temp_type')",
            ][..]),
            (Some("CREATE TEMP TABLE only_attached(temp_col INTEGER)"), &[
                "SELECT name FROM pragma_table_info('only_attached')",
                "SELECT count(*) FROM pragma_index_list('only_attached')",
                "SELECT name FROM pragma_index_info('attached_idx')",
                "SELECT name FROM pragma_table_info('only_attached','first_db')",
            ][..]),
            (Some("CREATE TEMP TABLE orphan(other_col INTEGER)"), &[
                "SELECT count(*) FROM pragma_foreign_key_list('orphan')",
                "SELECT count(*) FROM pragma_foreign_key_check('orphan')",
                "SELECT \"table\" FROM pragma_foreign_key_check('orphan','first_db')",
            ][..]),
        ] {
            if let Some(sql) = setup {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in queries {
                let expected = rq(&r, sql);
                assert_ne!(expected, vec![vec!["ERR"]], "stock: {sql}");
                let direct = fq(&f, sql).await;
                let prepared = match f.prepare(sql).await {
                    Ok(statement) => statement.query().await.map(|rows| {
                        rows.iter().map(|row| row.values().iter().map(tag_f).collect()).collect::<Vec<Vec<String>>>()
                    }),
                    Err(error) => Err(error),
                };
                if direct != expected || prepared.as_ref().ok() != Some(&expected) {
                    diffs.push(format!("{sql}: direct={direct:?}, prepared={prepared:?}, stock={expected:?}"));
                }
            }
        }
        f.execute("DETACH second_db").await.unwrap();
        f.execute("DETACH first_db").await.unwrap();
        f.close().await.unwrap();
        assert!(diffs.is_empty(), "{}", diffs.join("\n"));
    });
}

#[test]
fn metadata_pragma_lookup_refreshes_an_attached_file_catalog() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let frank_path = dir.path().join("frank-catalog.db");
        let stock_path = dir.path().join("stock-catalog.db");
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let attach = "ATTACH ?1 AS peer";
        f.execute_with_params(attach, &[SqliteValue::from(frank_path.to_str().unwrap())])
            .await.unwrap();
        r.execute(attach, [stock_path.to_str().unwrap()]).unwrap();
        let sql = "SELECT name FROM pragma_table_info('new_table')";
        let held = f.prepare(sql).await.unwrap();
        let mut stock_held = r.prepare(sql).unwrap();
        assert!(held.query().await.unwrap().is_empty());
        assert!(!stock_held.exists([]).unwrap());

        let frank_peer = Connection::open(frank_path.to_str().unwrap()).await.unwrap();
        let stock_peer = rusqlite::Connection::open(&stock_path).unwrap();
        let ddl = "CREATE TABLE new_table(external_column INTEGER)";
        frank_peer.execute(ddl).await.unwrap();
        stock_peer.execute(ddl, []).unwrap();
        frank_peer.close().await.unwrap();
        let stock = stock_held.query_row([], |row| row.get::<_, String>(0)).unwrap();
        assert_eq!(stock, "external_column");
        assert_eq!(fq(&f, sql).await, vec![vec![format!("text:{stock}")]]);
        let rows = held.query().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values(), &[SqliteValue::from(stock.as_str())]);
        drop(held);
        f.execute("DETACH peer").await.unwrap();
        f.close().await.unwrap();
    });
}

#[test]
fn pragma_view_metadata_uses_the_source_catalog() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(v INTEGER, main_only VARCHAR(20))",
            "CREATE VIEW leaf AS SELECT v FROM t",
            "CREATE VIEW nested AS SELECT v FROM leaf",
            "CREATE VIEW wide AS SELECT * FROM t",
            "CREATE TEMP TABLE t(v TEXT)",
            "CREATE TEMP TABLE c(v TEXT)",
            "CREATE TEMP VIEW leaf AS SELECT v FROM temp.t",
            "CREATE TEMP VIEW explicit_main AS SELECT v FROM main.t",
            "CREATE TEMP VIEW explicit_nested AS SELECT v FROM main.leaf",
            "CREATE TEMP VIEW implicit_temp AS SELECT v FROM t",
            "CREATE VIEW typed_cte AS WITH c AS (SELECT v FROM t) SELECT v FROM c",
            "CREATE TABLE other_type(v TEXT)",
            "CREATE VIEW shadowed_cte AS WITH b AS (SELECT v FROM t), c AS (SELECT v FROM b) \
             SELECT q.v FROM (WITH b AS (SELECT v FROM other_type) SELECT v FROM c) AS q",
            "ATTACH ':memory:' AS aux",
            "CREATE TABLE aux.t(v REAL, attached_only BLOB)",
            "CREATE VIEW aux.leaf AS SELECT * FROM t",
            "CREATE TEMP VIEW explicit_aux AS SELECT * FROM aux.t",
        ] {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }
        let mut diffs = Vec::new();
        for sql in [
            "PRAGMA main.table_info('nested')",
            "PRAGMA table_xinfo('explicit_main')",
            "PRAGMA aux.table_info('leaf')",
            "SELECT name,type FROM pragma_table_info('wide','main') ORDER BY cid",
            "SELECT name,type FROM pragma_table_xinfo('explicit_nested')",
            "SELECT name,type FROM pragma_table_info('implicit_temp')",
            "SELECT name,type FROM pragma_table_info('typed_cte','main')",
            "SELECT name,type FROM pragma_table_xinfo('shadowed_cte','main')",
            "SELECT name,type FROM pragma_table_info('explicit_aux') ORDER BY cid",
            "SELECT name,type FROM pragma_table_info('leaf')",
            "SELECT name,type FROM pragma_table_info('leaf','main')",
            "SELECT name,type FROM pragma_table_info('leaf','aux') ORDER BY cid",
        ] {
            let expected = rq(&r, sql);
            assert!(!expected.is_empty() && expected != vec![vec!["ERR"]], "stock: {sql}");
            let direct = fq(&f, sql).await;
            let prepared = match f.prepare(sql).await {
                Ok(statement) => statement.query().await.map(|rows| {
                    rows.iter().map(|row| row.values().iter().map(tag_f).collect()).collect::<Vec<Vec<String>>>()
                }),
                Err(error) => Err(error),
            };
            if direct != expected || prepared.as_ref().ok() != Some(&expected) {
                diffs.push(format!("{sql}: direct={direct:?}, prepared={prepared:?}, stock={expected:?}"));
            }
        }
        f.execute("DETACH aux").await.unwrap();
        f.close().await.unwrap();
        assert!(diffs.is_empty(), "{}", diffs.join("\n"));
    });
}

#[test]
fn pragma_introspects_schema_catalog_aliases_in_their_namespace() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let attach = "ATTACH ':memory:' AS aux";
        f.execute(attach).await.unwrap();
        r.execute(attach, []).unwrap();
        let mut diffs = Vec::new();
        for sql in [
            "PRAGMA table_info('sqlite_schema')",
            "PRAGMA table_xinfo('sqlite_temp_master')",
            "PRAGMA aux.table_info('sqlite_master')",
            "SELECT name,type FROM pragma_table_info('sqlite_schema','temp') ORDER BY cid",
            "SELECT name,type,hidden FROM pragma_table_xinfo('sqlite_temp_schema','temp') ORDER BY cid",
            "SELECT name,type FROM pragma_table_info('sqlite_master','aux') ORDER BY cid",
            "SELECT count(*) FROM pragma_table_info('sqlite_temp_schema','main')",
            "SELECT count(*) FROM pragma_table_xinfo('sqlite_temp_master','aux')",
        ] {
            let expected = rq(&r, sql);
            assert!(!expected.is_empty() && expected != vec![vec!["ERR"]], "stock: {sql}");
            let direct = fq(&f, sql).await;
            let prepared = match f.prepare(sql).await {
                Ok(statement) => statement.query().await.map(|rows| {
                    rows.iter().map(|row| row.values().iter().map(tag_f).collect()).collect::<Vec<Vec<String>>>()
                }),
                Err(error) => Err(error),
            };
            if direct != expected || prepared.as_ref().ok() != Some(&expected) {
                diffs.push(format!("{sql}: direct={direct:?}, prepared={prepared:?}, stock={expected:?}"));
            }
        }
        f.execute("DETACH aux").await.unwrap();
        f.close().await.unwrap();
        assert!(diffs.is_empty(), "{}", diffs.join("\n"));
    });
}

#[test]
fn view_cte_wildcard_uses_its_definition_scope() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE origin(outer_key INTEGER)",
            "INSERT INTO origin VALUES (1)",
            "CREATE VIEW cte_star AS
             WITH b AS (SELECT * FROM origin), c AS (SELECT * FROM b)
             SELECT q.* FROM (
                 WITH b AS (SELECT 'shadow' AS inner_key, 2 AS extra)
                 SELECT * FROM c
             ) AS q",
            "CREATE VIEW cte_star_same_name AS WITH c AS (
                 WITH c AS (SELECT * FROM origin) SELECT * FROM c
             ) SELECT * FROM c",
        ] {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }

        let mut diffs = Vec::new();
        for sql in [
            "PRAGMA table_info('cte_star')",
            "SELECT name, type FROM pragma_table_xinfo('cte_star')",
            "SELECT * FROM cte_star",
            "PRAGMA table_info('cte_star_same_name')",
            "SELECT * FROM cte_star_same_name",
        ] {
            let expected = rq(&r, sql);
            assert_eq!(expected.len(), 1, "stock: {sql}");
            assert_ne!(expected, vec![vec!["ERR"]], "stock: {sql}");
            let direct = fq(&f, sql).await;
            let prepared = match f.prepare(sql).await {
                Ok(statement) => statement.query().await.map(|rows| {
                    rows.iter()
                        .map(|row| row.values().iter().map(tag_f).collect())
                        .collect::<Vec<Vec<String>>>()
                }),
                Err(error) => Err(error),
            };
            if direct != expected || prepared.as_ref().ok() != Some(&expected) {
                diffs.push(format!(
                    "{sql}: direct={direct:?}, prepared={prepared:?}, stock={expected:?}"
                ));
            }
        }
        f.close().await.unwrap();
        assert!(diffs.is_empty(), "{}", diffs.join("\n"));
    });
}

#[test]
fn pragma_view_star_preserves_decltypes_and_catalog_order() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "ATTACH ':memory:' AS aux",
            "ATTACH ':memory:' AS later",
            "CREATE TABLE aux.remote_only(a INTEGER,b BLOB)",
            "CREATE TABLE later.remote_only(ignored TEXT)",
            "CREATE TEMP VIEW implicit_aux AS SELECT * FROM remote_only",
            "CREATE TABLE lhs(a TEXT,b VARCHAR(4))",
            "CREATE TABLE rhs(a INT,c BLOB)",
            "CREATE VIEW v_using AS SELECT *,a AS merged,lhs.a AS lhs_a,rhs.a AS rhs_a \
             FROM lhs LEFT JOIN rhs USING(a)",
            "CREATE VIEW v_natural AS SELECT *,a AS merged FROM lhs NATURAL JOIN rhs",
            "CREATE VIEW v_right AS SELECT *,a AS merged FROM lhs RIGHT JOIN rhs USING(a)",
            "CREATE VIEW v_full AS SELECT *,a AS merged FROM lhs FULL JOIN rhs USING(a)",
            "CREATE TABLE affinities(a VARCHAR(11), r DOUBLE, n DECIMAL(6,2), i BIGINT, b)",
            "CREATE TABLE other_affinities(a INT,r INT,n INT,i INT,b INT)",
            "CREATE VIEW v_full_affinities AS SELECT * FROM affinities FULL JOIN other_affinities \
             USING(a,r,n,i,b)",
            "CREATE TABLE strict_left(a ANY) STRICT",
            "CREATE VIEW v_full_strict_any AS SELECT * FROM strict_left FULL JOIN rhs USING(a)",
            "CREATE TABLE source_affinity(a, b VARCHAR(11), n DECIMAL(6, 2))",
            "CREATE VIEW v_expr_affinity AS SELECT a,b,b COLLATE NOCASE AS folded,\
             n COLLATE BINARY AS numeric_collate, CAST(1 AS INTEGER) AS i,\
             CAST(1 AS DOUBLE) AS r, CAST(1 AS BLOB) AS bytes,\
             CAST(1 AS VARCHAR(4)) COLLATE NOCASE AS cast_collate, +b AS no_affinity \
             FROM source_affinity",
            "CREATE VIEW v_nested_affinity AS SELECT inner_view.* FROM \
             (SELECT * FROM v_expr_affinity) AS inner_view",
            "CREATE VIEW v_strict_any AS SELECT a, a COLLATE BINARY AS collated FROM strict_left",
        ] {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }
        let mut diffs = Vec::new();
        for sql in [
            "PRAGMA table_info('implicit_aux')",
            "SELECT name,type FROM pragma_table_xinfo('implicit_aux','temp') ORDER BY cid",
            "PRAGMA table_info('v_using')",
            "SELECT name,type FROM pragma_table_xinfo('v_natural') ORDER BY cid",
            "SELECT name,type FROM pragma_table_info('v_right') ORDER BY cid",
            "SELECT name,type FROM pragma_table_info('v_full') ORDER BY cid",
            "SELECT name,type FROM pragma_table_info('v_full_affinities') ORDER BY cid",
            "SELECT name,type FROM pragma_table_xinfo('v_full_strict_any') ORDER BY cid",
            "PRAGMA table_info('source_affinity')",
            "PRAGMA table_info('v_expr_affinity')",
            "SELECT name,type FROM pragma_table_xinfo('v_nested_affinity') ORDER BY cid",
            "PRAGMA table_xinfo('v_strict_any')",
        ] {
            let stock = rq(&r, sql);
            assert!(!stock.is_empty() && stock != vec![vec!["ERR"]], "stock: {sql}");
            let direct = fq(&f, sql).await;
            let prepared = match f.prepare(sql).await {
                Ok(statement) => statement.query().await.map(|rows| {
                    rows.iter()
                        .map(|row| row.values().iter().map(tag_f).collect())
                        .collect::<Vec<Vec<String>>>()
                }),
                Err(error) => Err(error),
            };
            if direct != stock || prepared.as_ref().ok() != Some(&stock) {
                diffs.push(format!(
                    "{sql}: direct={direct:?}, prepared={prepared:?}, stock={stock:?}"
                ));
            }
        }
        f.execute("DETACH later").await.unwrap();
        f.execute("DETACH aux").await.unwrap();
        f.close().await.unwrap();
        assert!(diffs.is_empty(), "{}", diffs.join("\n"));
    });
}

#[test]
fn gh512_pragma_table_info_quoted_and_parenthesized_pk_ordinals() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("f_gh512_pk.db");
        let r_path = dir.path().join("r_gh512_pk.db");

        let ddls = [
            "CREATE TABLE t_quoted(\"b\" TEXT, \"a\" INTEGER, \"c\" BLOB, PRIMARY KEY (\"b\", \"A\"))",
            "CREATE TABLE t_parens(a INTEGER, b TEXT, c REAL, PRIMARY KEY ((\"B\"), (\"a\")))",
            "CREATE TABLE t_brackets([x] TEXT, [y] INTEGER, PRIMARY KEY ([Y], [x]))",
        ];

        {
            let f = Connection::open(f_path.to_str().unwrap()).await.unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            for ddl in ddls {
                f.execute(ddl).await.unwrap();
                r.execute(ddl, []).unwrap();
            }
            for tbl in ["t_quoted", "t_parens", "t_brackets"] {
                let q = format!(
                    "SELECT cid, name, type, \"notnull\", pk FROM pragma_table_info('{tbl}') ORDER BY cid"
                );
                assert_eq!(fq(&f, &q).await, rq(&r, &q), "live GH#512 pk ordinal on {tbl}");
                let p = format!("PRAGMA table_info({tbl})");
                assert_eq!(fq(&f, &p).await, rq(&r, &p), "live PRAGMA table_info({tbl})");
            }
        }

        // Reopen from disk to exercise schema reload from sqlite_master DDL as well.
        let f = Connection::open(f_path.to_str().unwrap()).await.unwrap();
        let r = rusqlite::Connection::open(&r_path).unwrap();
        for tbl in ["t_quoted", "t_parens", "t_brackets"] {
            let q = format!(
                "SELECT cid, name, type, \"notnull\", pk FROM pragma_table_info('{tbl}') ORDER BY cid"
            );
            assert_eq!(
                fq(&f, &q).await,
                rq(&r, &q),
                "reopened GH#512 pk ordinal on {tbl}"
            );
            let p = format!("PRAGMA table_info({tbl})");
            assert_eq!(
                fq(&f, &p).await,
                rq(&r, &p),
                "reopened PRAGMA table_info({tbl})"
            );
        }
    });
}

#[test]
fn bd_c1q6u_json_each_tree_and_pragma_hidden_columns_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let f_path = dir.path().join("frank_c1q6u.db");
            let r_path = dir.path().join("stock_c1q6u.db");
            let f = Connection::open(if file_backed {
                f_path.to_str().unwrap()
            } else {
                ":memory:"
            })
            .await
            .unwrap();
            let r = if file_backed {
                rusqlite::Connection::open(&r_path).unwrap()
            } else {
                rusqlite::Connection::open_in_memory().unwrap()
            };
            let db = if file_backed { "file" } else { "memory" };

            for sql in [
                "PRAGMA foreign_keys = OFF",
                "CREATE TABLE t(a INTEGER, b TEXT)",
                "CREATE INDEX idx_t_a ON t(a)",
                "CREATE TABLE t2(c REAL)",
                "CREATE TEMP TABLE t(p TEXT)",
                "CREATE INDEX temp.idx_temp_t ON t(p)",
                "ATTACH ':memory:' AS aux",
                "CREATE TABLE aux.t(k INTEGER, z TEXT GENERATED ALWAYS AS (k || '!') VIRTUAL)",
                "CREATE INDEX aux.idx_aux_t ON t(k)",
                "CREATE TABLE aux.at(v INTEGER)",
                "CREATE TABLE fk_parent(id INTEGER PRIMARY KEY)",
                "CREATE TABLE fk_child(pid INTEGER REFERENCES fk_parent(id))",
                "INSERT INTO fk_child VALUES (99)",
                "CREATE TABLE tbl_names(tname TEXT)",
                "INSERT INTO tbl_names VALUES ('t'), ('t2')",
                "CREATE TABLE jdocs(id INTEGER PRIMARY KEY, raw TEXT, bin BLOB)",
                "INSERT INTO jdocs VALUES (1, '{\"a\":[10,{\"b\":20}],\"u\":\"\\u0041\"}', jsonb('{\"a\":[10,{\"b\":20}],\"u\":\"\\u0041\"}'))",
            ] {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }

            let queries = [
                // json_each / json_tree: all 9 visible+rowid columns + hidden json & root
                "SELECT rowid, key, value, type, atom, id, parent, fullkey, path, json, typeof(json), root, typeof(root) FROM json_each('[10,20,30]')",
                "SELECT rowid, key, value, type, atom, id, parent, fullkey, path, json, root FROM json_each('{\"a\":[10,{\"b\":20}]}', '$.a')",
                "SELECT rowid, key, value, type, atom, id, parent, fullkey, path, json, root FROM json_tree('{\"a\":[10,{\"b\":20}]}', '$.a')",
                "SELECT d.id, j.rowid, j.key, j.value, j.id, j.parent, j.fullkey, j.path, typeof(j.json), j.root FROM jdocs d, json_tree(d.raw, '$.a') j",
                "SELECT d.id, j.rowid, j.key, j.value, j.id, j.parent, j.fullkey, j.path, typeof(j.json), j.root FROM jdocs d, json_tree(d.bin, '$.a') j",
                "SELECT key, value, root FROM json_each('{\"a\":[1,2]}', '$.a') WHERE root = '$.a'",
                "SELECT key, value FROM json_each('[10,20]') WHERE json = '[10,20]'",
                "SELECT key, value FROM json_each('[10,20]') WHERE json = '[99]'",
                "SELECT * FROM json_each('[10,20]')",
                // pragma_table_info / pragma_table_xinfo / pragma_index_list / pragma_index_info / pragma_index_xinfo / pragma_foreign_key_list / pragma_foreign_key_check / pragma_table_list hidden arg & schema
                "SELECT * FROM pragma_table_info('t') ORDER BY cid",
                "SELECT cid, name, arg, typeof(arg), schema, typeof(schema) FROM pragma_table_info('t') ORDER BY cid",
                "SELECT cid, name, arg, typeof(arg), schema, typeof(schema) FROM pragma_table_info('t', 'main') ORDER BY cid",
                "SELECT cid, name, arg, typeof(arg), schema, typeof(schema) FROM pragma_table_info('t', 'TEMP') ORDER BY cid",
                "SELECT cid, name, arg, typeof(arg), schema, typeof(schema) FROM pragma_table_info('t', 'aux') ORDER BY cid",
                "SELECT cid, name, arg, typeof(arg), schema, typeof(schema) FROM pragma_table_info('t', NULL) ORDER BY cid",
                "SELECT cid, name, arg, typeof(arg), schema, typeof(schema) FROM pragma_table_info(x'74', x'617578') ORDER BY cid",
                "SELECT cid, name, arg, length(arg), schema, length(schema) FROM pragma_table_info('t' || char(0) || 'x', 'main' || char(0) || 'y') ORDER BY cid",
                "SELECT cid, name, arg, schema FROM pragma_table_info('t') WHERE arg = 't' ORDER BY cid",
                "SELECT cid, name, arg, schema FROM pragma_table_info('t') WHERE arg = 'other' ORDER BY cid",
                "SELECT cid, name, arg, schema FROM pragma_table_info('t') WHERE schema IS NULL ORDER BY cid",
                "SELECT cid, name, arg, schema FROM pragma_table_info('t') WHERE schema = 'main' ORDER BY cid",
                "SELECT cid, name, arg, schema FROM pragma_table_info('t') WHERE schema = 'aux' ORDER BY cid",
                "SELECT cid, name, arg, schema FROM pragma_table_info('t', 'main') WHERE schema = 'main' ORDER BY cid",
                "SELECT cid, name, arg, schema FROM pragma_table_info('t', 'main') WHERE schema = 'temp' ORDER BY cid",
                "SELECT cid, name, arg, schema FROM pragma_table_info('T') WHERE arg = 't' ORDER BY cid",
                "SELECT cid, name, arg, schema FROM pragma_table_info('T') WHERE arg = 't' COLLATE NOCASE ORDER BY cid",
                // Bare parenless pragma_* forms with WHERE / aliased / reversed / IS constraints
                "SELECT count(*) FROM pragma_table_info",
                "SELECT count(*) FROM pragma_table_info()",
                "SELECT cid, name, arg, schema FROM pragma_table_info WHERE arg = 't' ORDER BY cid",
                "SELECT cid, name, arg, schema FROM pragma_table_info WHERE 't' = arg AND 'aux' = schema ORDER BY cid",
                "SELECT cid, name, arg, schema FROM pragma_table_info WHERE arg = 't' AND schema = 'main' ORDER BY cid",
                "SELECT p.cid, p.name, p.arg, p.schema FROM pragma_table_info p WHERE p.arg = 't' AND p.schema = 'aux' ORDER BY p.cid",
                "SELECT cid, name, arg, schema FROM pragma_table_info WHERE arg IS 't' ORDER BY cid",
                "SELECT cid, name, arg, schema FROM pragma_table_info WHERE arg = 't' AND arg = 't2' ORDER BY cid",
                "SELECT cid, name, hidden, arg, schema FROM pragma_table_xinfo WHERE arg = 't' AND schema = 'aux' ORDER BY cid",
                // pragma_index_list / pragma_index_info / pragma_index_xinfo / pragma_foreign_key_list / pragma_foreign_key_check / pragma_table_list
                "SELECT * FROM pragma_index_list('t', 'main') ORDER BY name",
                "SELECT name, arg, typeof(arg), schema, typeof(schema) FROM pragma_index_list('t') ORDER BY name",
                "SELECT name, arg, schema FROM pragma_index_list('t', 'aux') ORDER BY name",
                "SELECT name, arg, schema FROM pragma_index_list WHERE arg = 't' AND schema = 'main' ORDER BY name",
                "SELECT name, arg, schema FROM pragma_index_list WHERE arg = 't' AND schema = 'aux' ORDER BY name",
                "SELECT name, arg, schema FROM pragma_index_info WHERE arg = 'idx_aux_t' AND schema = 'aux' ORDER BY seqno",
                "SELECT name, key, arg, schema FROM pragma_index_xinfo WHERE arg = 'idx_aux_t' AND schema = 'aux' AND key = 1 ORDER BY seqno",
                "SELECT \"from\", arg, schema FROM pragma_foreign_key_list WHERE arg = 'fk_child' AND schema = 'main' ORDER BY id, seq",
                "SELECT \"table\", rowid, parent, fkid, arg, typeof(arg), schema, typeof(schema) FROM pragma_foreign_key_check",
                "SELECT \"table\", rowid, parent, fkid, arg, schema FROM pragma_foreign_key_check('fk_child', 'main')",
                "SELECT \"table\", rowid, parent, fkid, arg, schema FROM pragma_foreign_key_check WHERE arg = 'fk_child' AND schema = 'main'",
                "SELECT name, arg, typeof(arg), schema FROM pragma_table_list WHERE name = 'at'",
                "SELECT name, arg, typeof(arg), schema FROM pragma_table_list('at')",
                "SELECT name, arg, schema FROM pragma_table_list WHERE arg = 't' AND schema = 'aux'",
                // JOIN ON / USING over pragma_* hidden columns + star width control
                "SELECT n.tname, p.name, p.arg, p.schema FROM tbl_names n JOIN pragma_table_info p ON p.arg = n.tname ORDER BY n.tname, p.cid",
                "SELECT * FROM pragma_table_info('t2') a JOIN (SELECT 't2' AS arg, 'ok' AS tag) b USING(arg) ORDER BY cid",
                "SELECT * FROM (SELECT 't2' AS arg, 'ok' AS tag) b JOIN pragma_table_info a USING(arg) ORDER BY cid",
                // Error parity: invalid schema via WHERE, missing hidden columns on no-arg pragmas
                "SELECT name FROM pragma_table_info('t') WHERE schema = 'nosuchdb'",
                "SELECT name FROM pragma_table_info WHERE arg = 't' AND schema = 'nosuchdb'",
                "SELECT arg FROM pragma_database_list",
                "SELECT schema FROM pragma_database_list",
                "SELECT arg FROM pragma_collation_list",
                "SELECT schema FROM pragma_collation_list",
            ];

            for sql in queries {
                let stock = rq(&r, sql);
                let direct = fq(&f, sql).await;
                let prepared = match f.prepare(sql).await {
                    Ok(stmt) => match stmt.query().await {
                        Ok(rows) => rows
                            .iter()
                            .map(|row| row.values().iter().map(tag_f).collect())
                            .collect(),
                        Err(_) => vec![vec!["ERR".to_owned()]],
                    },
                    Err(_) => vec![vec!["ERR".to_owned()]],
                };
                if direct != stock || prepared != stock {
                    diffs.push(format!(
                        "[{db}] `{sql}`\n   direct=   {direct:?}\n   prepared= {prepared:?}\n   stock=    {stock:?}"
                    ));
                }
            }

            // Parameterized bare pragma_table_info with anonymous `?` AND numbered `?1`/`?2`
            for sql in [
                "SELECT name, arg, schema FROM pragma_table_info WHERE arg = ? AND schema = ? ORDER BY cid",
                "SELECT name, arg, schema FROM pragma_table_info WHERE arg = ?1 AND schema = ?2 ORDER BY cid",
                "SELECT name, arg, schema FROM pragma_table_info(?) WHERE schema = ? ORDER BY cid",
            ] {
                let prepared_f = f.prepare(sql).await.unwrap();
                let mut prepared_r = r.prepare(sql).unwrap();
                for (arg_val, schema_val) in [("t", "main"), ("t", "aux"), ("t", "temp")] {
                    let stock_rows: Vec<Vec<String>> = prepared_r
                        .query_map([arg_val, schema_val], |row| {
                            Ok(vec![
                                tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(0)),
                                tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(1)),
                                tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(2)),
                            ])
                        })
                        .unwrap()
                        .map(Result::unwrap)
                        .collect();
                    let params = [SqliteValue::from(arg_val), SqliteValue::from(schema_val)];
                    let direct_rows: Vec<Vec<String>> = f
                        .query_with_params(sql, &params)
                        .await
                        .unwrap()
                        .iter()
                        .map(|row| row.values().iter().map(tag_f).collect())
                        .collect();
                    let prep_rows: Vec<Vec<String>> = prepared_f
                        .query_with_params(&params)
                        .await
                        .unwrap()
                        .iter()
                        .map(|row| row.values().iter().map(tag_f).collect())
                        .collect();
                    if direct_rows != stock_rows || prep_rows != stock_rows {
                        diffs.push(format!(
                            "[{db}] param `{sql}` ({arg_val}, {schema_val})\n   direct=   {direct_rows:?}\n   prepared= {prep_rows:?}\n   stock=    {stock_rows:?}"
                        ));
                    }
                }
            }

            f.execute("DETACH aux").await.unwrap();
            f.close().await.unwrap();
        }
        assert!(
            diffs.is_empty(),
            "{} bd-c1q6u hidden-column divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

