#![recursion_limit = "512"]

//! GROUP BY depth leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite over
//! GROUP BY corners — grouping by an EXPRESSION, the SQLite-specific rule that a
//! bare (non-aggregated) column in a query with a single min()/max() takes its
//! value from the row that produced that extremum, HAVING referencing an
//! aggregate and an output alias, GROUP BY by output ordinal, GROUP BY with a
//! NOCASE collation, multi-column grouping, and grouping with NULLs (all NULLs
//! form one group). Only the min/max-associated bare column is asserted (a bare
//! column with NO min/max is implementation-defined, so it is NOT probed).
//! Ordered result sets compared. Pass = coverage keeper; a mismatch is a leaf.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use fsqlite_vdbe::engine::{reset_vdbe_metrics, set_vdbe_metrics_enabled, vdbe_metrics_snapshot};

// VDBE metrics are process-global: keep the other oracle tests out of the
// measured region too, rather than depending on the runner's thread count.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

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
fn group_by_depth_match_rusqlite_oracle() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, g TEXT, v INTEGER, tag TEXT)",
            "INSERT INTO t VALUES \
              (1,'a',30,'a-hi'),(2,'a',10,'a-lo'),(3,'a',20,'a-mid'),\
              (4,'b',50,'b-hi'),(5,'b',40,'b-mid'),\
              (6,'c',NULL,'c-null'),(7,'c',5,'c-lo')",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // min/max bare-column rule: `tag` and `id` come from the max(v) row per group
            "SELECT g, max(v), id, tag FROM t GROUP BY g ORDER BY g",
            "SELECT g, min(v), id, tag FROM t GROUP BY g ORDER BY g",
            // the extremum row selection across the whole table (no GROUP BY)
            "SELECT max(v), id, tag FROM t",
            "SELECT min(v), id, tag FROM t",
            // grouping by an expression (first letter — here g already is, so use substr on tag)
            "SELECT substr(tag,1,1) AS k, count(*) FROM t GROUP BY substr(tag,1,1) ORDER BY k",
            // group by output ordinal
            "SELECT g, count(*), sum(v) FROM t GROUP BY 1 ORDER BY 1",
            // group by expression that is a computed bucket
            "SELECT v/10 AS bucket, count(*) FROM t WHERE v IS NOT NULL GROUP BY v/10 ORDER BY bucket",
            // HAVING referencing an aggregate
            "SELECT g, count(*) AS n FROM t GROUP BY g HAVING count(*) >= 2 ORDER BY g",
            // HAVING referencing an aggregate alias
            "SELECT g, sum(v) AS s FROM t GROUP BY g HAVING sum(v) > 25 ORDER BY g",
            // HAVING with a non-aggregate group condition
            "SELECT g, count(*) FROM t GROUP BY g HAVING g <> 'a' ORDER BY g",
            // NULLs form a single group
            "SELECT v, count(*) FROM t GROUP BY v ORDER BY v",
            // multi-column grouping
            "SELECT g, (v IS NULL) AS isnull, count(*) FROM t GROUP BY g, (v IS NULL) ORDER BY g, isnull",
            // GROUP BY with a NOCASE collation folds case
            "SELECT count(DISTINCT g) FROM (SELECT 'A' AS g UNION ALL SELECT 'a' UNION ALL SELECT 'B')",
            // min(g) is deterministic (BINARY) per NOCASE group; bare `g` would be implementation-defined
            "SELECT min(g) AS mg, count(*) FROM (SELECT 'A' AS g UNION ALL SELECT 'a' UNION ALL SELECT 'B') GROUP BY g COLLATE NOCASE ORDER BY mg",
            // aggregate over an empty group set (WHERE excludes all) -> one all-NULL/zero row
            "SELECT count(*), sum(v), max(v) FROM t WHERE g='zzz'",
            // GROUP BY over empty -> no rows
            "SELECT g, count(*) FROM t WHERE g='zzz' GROUP BY g",
        ];

        let mut diffs = Vec::new();
        for q in queries {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} GROUP BY-depth divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// GH#531: selective grouping keeps its indexed row source. Check both the
/// stock row values and the actual bytecode, independently of EQP text.
#[test]
#[allow(clippy::too_many_lines)]
fn indexed_group_by_predicates_match_sqlite_without_table_scans() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("grouped.db");
            let f = Connection::open(if file_backed {
                path.to_str().unwrap()
            } else {
                ":memory:"
            })
            .await
            .unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in [
                "CREATE TABLE likes(id INTEGER PRIMARY KEY, post_id INTEGER, n INTEGER, tag TEXT)",
                "CREATE INDEX likes_post_id_idx ON likes(post_id)",
                "INSERT INTO likes VALUES (1,2,10,'a'),(2,2,NULL,'b'),(3,2,30,'c'),\
                 (4,3,7,'d'),(5,4,0,'e'),(6,NULL,9,'f'),(7,2.5,11,'g'),\
                 (8,'abc',13,'h'),(9,-1,17,'i')",
                "CREATE TABLE composite(id INTEGER PRIMARY KEY, k INTEGER, suffix TEXT, n INTEGER)",
                "CREATE INDEX composite_k_suffix ON composite(k,suffix DESC)",
                "INSERT INTO composite VALUES (1,2,NULL,5),(2,2,'a',7),(3,2,'z',11),(4,3,'x',13)",
                "CREATE TABLE repeated(id INTEGER PRIMARY KEY, k INTEGER, n INTEGER)",
                "CREATE INDEX repeated_k_k ON repeated(k,k)",
                "INSERT INTO repeated VALUES (1,2,10),(2,2,20),(3,3,10)",
                "CREATE TABLE text_keys(id INTEGER PRIMARY KEY, k TEXT, n INTEGER)",
                "CREATE INDEX text_keys_k ON text_keys(k)",
                "INSERT INTO text_keys VALUES (1,'2',10),(2,'2',20),(3,'02',30),(4,NULL,40),\
                 (5,'A',50),(6,'a',60),(7,'a ',70)",
                "CREATE TABLE names(id INTEGER PRIMARY KEY, k TEXT COLLATE NOCASE, n INTEGER)",
                "CREATE INDEX names_k ON names(k)",
                "INSERT INTO names VALUES (1,'A',2),(2,'a',3),(3,'B',5),(4,NULL,7)",
            ] {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in [
                "SELECT post_id,count(*) FROM likes WHERE post_id=2 GROUP BY post_id",
                "SELECT post_id,count(*),sum(n),count(n),max(n) FROM likes WHERE post_id='2' GROUP BY post_id",
                "SELECT post_id,count(*) FROM likes WHERE post_id=2.5 GROUP BY post_id",
                "SELECT post_id,count(*) FROM likes WHERE post_id='abc' GROUP BY post_id",
                "SELECT post_id,count(*) FROM likes WHERE post_id=NULL GROUP BY post_id",
                "SELECT post_id,count(*) FROM likes WHERE post_id=99 GROUP BY post_id",
                "SELECT post_id,count(*) FROM likes WHERE post_id=2 AND n>5 GROUP BY post_id",
                "SELECT post_id,count(*) FROM likes WHERE post_id=2 AND post_id=3 GROUP BY post_id",
                "SELECT post_id,sum(n),max(n) FROM likes WHERE post_id='2' AND (n>20 OR n IS NULL) GROUP BY post_id",
                "SELECT post_id,count(*) FROM likes WHERE post_id=NULL AND n>5 GROUP BY post_id",
                "SELECT post_id,count(*) FROM likes WHERE post_id=99 AND n>5 GROUP BY post_id",
                "SELECT post_id,count(*) FROM likes WHERE post_id IN (2,3,2,NULL,99) GROUP BY post_id",
                "SELECT post_id,count(*),sum(n) FROM likes WHERE post_id IN (2,3,4) AND n>5 GROUP BY post_id HAVING sum(n)>10",
                "SELECT post_id,count(*) FROM likes WHERE post_id IN (2,3,4) GROUP BY post_id LIMIT 1 OFFSET 1",
                "SELECT post_id,count(*) FROM likes NOT INDEXED WHERE post_id=2 GROUP BY post_id",
                "SELECT k,count(*),sum(n) FROM composite WHERE k=2 GROUP BY k",
                "SELECT k,count(*),sum(n) FROM composite WHERE k IN (2,3,2) GROUP BY k",
                "SELECT k,suffix,count(*) FROM composite WHERE k=2 GROUP BY k,suffix",
                "SELECT k,count(*) FROM composite WHERE suffix='a' AND k=2 GROUP BY k",
                "SELECT k,sum(n) FROM composite WHERE suffix='a' AND k=2 AND n>5 GROUP BY k",
                "SELECT k,suffix,count(*) FROM composite WHERE k IN (2,3,2) GROUP BY k,suffix",
                // Repeated index columns must not hide the n=10 residual.
                "SELECT k,count(*) FROM repeated WHERE k=2 AND n=10 GROUP BY k",
                "SELECT k,count(*) FROM repeated WHERE k IN (2,3,2) AND n=10 GROUP BY k",
                "SELECT k,count(*) FROM text_keys WHERE k=2 GROUP BY k",
                "SELECT k,count(*) FROM text_keys WHERE k=2 AND n>10 GROUP BY k",
                "SELECT k,count(*) FROM text_keys WHERE k='a' COLLATE NOCASE AND n>40 GROUP BY k",
                "SELECT k,count(*) FROM text_keys WHERE k IN (2,'2','02',NULL) GROUP BY k",
                // A constant singleton RHS COLLATE affects membership; a
                // multi-element list uses only the column's BINARY collation.
                "SELECT k,count(*) FROM text_keys WHERE k IN ('a' COLLATE NOCASE) GROUP BY k",
                "SELECT k,count(*) FROM text_keys WHERE k IN ('a' COLLATE RTRIM) GROUP BY k",
                "SELECT k,count(*) FROM text_keys WHERE k IN ('a' COLLATE BINARY) GROUP BY k",
                "SELECT k,count(*) FROM text_keys WHERE k IN ('a' COLLATE NOCASE,'02') GROUP BY k",
                "SELECT k,sum(n) FROM text_keys WHERE k IN ('a' COLLATE NOCASE) AND n>40 GROUP BY k",
                // FILTER, aggregate arguments and expression group keys keep
                // their normal table reads and evaluation semantics.
                "SELECT post_id,count(*) FILTER (WHERE n>10) FROM likes WHERE post_id=2 GROUP BY post_id",
                "SELECT post_id,count(n) FROM likes WHERE post_id=2 GROUP BY post_id",
                "SELECT post_id+1,count(*) FROM likes WHERE post_id=2 GROUP BY post_id+1",
                "SELECT min(k),count(*),sum(n) FROM names WHERE k='a' GROUP BY k",
                "SELECT min(k),count(*),sum(n) FROM names WHERE k COLLATE BINARY='a' GROUP BY k",
            ] {
                let expected = rq(&r, sql);
                assert_eq!(fq(&f, sql).await, expected, "file={file_backed}, {sql}");
                let prepared = f.prepare(sql).await.unwrap();
                let actual: Vec<Vec<String>> = prepared
                    .query()
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().iter().map(tag_f).collect())
                    .collect();
                assert_eq!(actual, expected, "prepared, file={file_backed}, {sql}");
            }

            let eq = "SELECT post_id,count(*) FROM likes WHERE post_id=?1 GROUP BY post_id";
            let members = "SELECT post_id,count(*) FROM likes WHERE post_id IN (?1,?2,?3,?4) GROUP BY post_id";
            let members_twenty = "SELECT post_id,count(*) FROM likes WHERE post_id IN (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20) GROUP BY post_id";
            if file_backed {
                f.set_reject_mem_fallback(true);
                f.set_strict_mem_fallback_rejection(true);
            }
            for (sql, bindings) in [
                (eq, vec![rusqlite::types::Value::Integer(2)]),
                (eq, vec![rusqlite::types::Value::Text("2".to_owned())]),
                (eq, vec![rusqlite::types::Value::Integer(99)]),
                (eq, vec![rusqlite::types::Value::Null]),
                (
                    "SELECT post_id,count(*) FROM likes WHERE n>? AND post_id=? AND id>? GROUP BY post_id",
                    [15, 2, 1]
                        .into_iter()
                        .map(rusqlite::types::Value::Integer)
                        .collect(),
                ),
                (
                    "SELECT k,count(*) FROM text_keys WHERE k IN (?1 COLLATE NOCASE) GROUP BY k",
                    vec![rusqlite::types::Value::Text("a".to_owned())],
                ),
                (
                    members,
                    vec![
                        rusqlite::types::Value::Integer(2),
                        rusqlite::types::Value::Text("2".to_owned()),
                        rusqlite::types::Value::Null,
                        rusqlite::types::Value::Integer(3),
                    ],
                ),
                (
                    members_twenty,
                    (1..=20).map(rusqlite::types::Value::Integer).collect(),
                ),
                (
                    "SELECT post_id,count(*) FROM likes WHERE n>? AND post_id IN (?,?,?) AND id>? GROUP BY post_id",
                    [5, 2, 2, 3, 0]
                        .into_iter()
                        .map(rusqlite::types::Value::Integer)
                        .collect(),
                ),
            ] {
                let mut stock = r.prepare(sql).unwrap();
                let expected: Vec<Vec<String>> = stock
                    .query_map(rusqlite::params_from_iter(&bindings), |row| {
                        Ok((0..2).map(|i| tag_r(&row.get_unwrap(i))).collect())
                    })
                    .unwrap()
                    .collect::<Result<_, _>>()
                    .unwrap();
                let params: Vec<SqliteValue> = bindings
                    .iter()
                    .map(|value| match value {
                        rusqlite::types::Value::Null => SqliteValue::Null,
                        rusqlite::types::Value::Integer(value) => SqliteValue::Integer(*value),
                        rusqlite::types::Value::Text(value) => {
                            SqliteValue::Text(value.clone().into())
                        }
                        _ => unreachable!("fixture uses null, integer, and text bindings"),
                    })
                    .collect();
                let prepared = f.prepare(sql).await.unwrap();
                for rows in [
                    f.query_with_params(sql, &params).await.unwrap(),
                    prepared.query_with_params(&params).await.unwrap(),
                ] {
                    let actual: Vec<Vec<String>> = rows
                        .iter()
                        .map(|row| row.values().iter().map(tag_f).collect())
                        .collect();
                    assert_eq!(
                        actual, expected,
                        "binds={bindings:?}, file={file_backed}, {sql}"
                    );
                }
            }
            if file_backed {
                f.set_strict_mem_fallback_rejection(false);
                f.set_reject_mem_fallback(false);
            }

            let transaction_query =
                "SELECT post_id,count(*),sum(n) FROM likes WHERE post_id=2 GROUP BY post_id";
            f.execute("BEGIN").await.unwrap();
            r.execute_batch("BEGIN").unwrap();
            let insert = "INSERT INTO likes VALUES (10,2,19,'pending')";
            f.execute(insert).await.unwrap();
            r.execute(insert, []).unwrap();
            assert_eq!(fq(&f, transaction_query).await, rq(&r, transaction_query));
            f.execute("ROLLBACK").await.unwrap();
            r.execute_batch("ROLLBACK").unwrap();
            assert_eq!(fq(&f, transaction_query).await, rq(&r, transaction_query));

            for sql in [
                eq,
                members,
                members_twenty,
                "SELECT k,count(*) FROM composite WHERE k=2 GROUP BY k",
                "SELECT k,count(*) FROM composite WHERE k IN (2,3) GROUP BY k",
            ] {
                let program = f.query(&format!("EXPLAIN {sql}")).await.unwrap();
                let has_opcode = |row: &fsqlite_core::connection::Row, name: &str| {
                    row.values()[1].as_text() == Some(name)
                };
                assert!(
                    program.iter().any(|row| has_opcode(row, "SeekGE")),
                    "missing seek: {sql}"
                );
                assert!(
                    !program.iter().any(|row| has_opcode(row, "Rewind")
                        && row.values()[2] == SqliteValue::Integer(0)),
                    "GROUP BY regressed to a full source-table scan: {sql}"
                );
                assert!(
                    !program.iter().any(|row| has_opcode(row, "OpenAutoindex")),
                    "GROUP BY storage source unexpectedly requires MemDatabase: {sql}"
                );
                assert!(
                    !program.iter().any(|row| has_opcode(row, "OpenRead")
                        && row.values()[2] == SqliteValue::Integer(0)),
                    "covered grouped count unnecessarily opened its source table: {sql}"
                );
                assert!(
                    !program.iter().any(|row| has_opcode(row, "SeekRowid")),
                    "covered grouped count unnecessarily looked up source rows: {sql}"
                );
            }
            for sql in [
                "SELECT post_id,count(*) FROM likes WHERE post_id IN (2,3) AND n>5 GROUP BY post_id",
                "SELECT post_id,count(*) FROM likes WHERE post_id=2 AND n>5 GROUP BY post_id",
                "SELECT post_id,count(*) FROM likes WHERE post_id=?1 AND post_id=?2 GROUP BY post_id",
                "SELECT post_id,count(n) FROM likes WHERE post_id=2 GROUP BY post_id",
                "SELECT k,count(*) FROM repeated WHERE k=2 AND n=10 GROUP BY k",
            ] {
                let program = f.query(&format!("EXPLAIN {sql}")).await.unwrap();
                assert!(
                    program
                        .iter()
                        .any(|row| row.values()[1].as_text() == Some("SeekRowid")),
                    "noncovering input lost the table lookup required by its residual or payload: {sql}"
                );
            }
        }
    });
}

/// Retaining one statement must not retain its earlier IN members, group
/// payloads, or transaction rows. Bind slots preserve source SQL order.
#[test]
fn indexed_group_by_rebinds_all_clauses_and_observes_rollback() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("grouped-bindings.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let setup = "\
            CREATE TABLE likes(id INTEGER PRIMARY KEY, post_id INTEGER, n INTEGER, suffix TEXT);\
            CREATE INDEX likes_post ON likes(post_id);\
            CREATE INDEX likes_post_suffix ON likes(post_id, suffix DESC);\
            INSERT INTO likes VALUES (1,2,10,'a'),(2,2,NULL,'b'),(3,2,30,'a'),\
              (4,3,7,'a'),(5,4,0,'b'),(6,NULL,9,'a');";
        f.execute_batch(setup).await.unwrap();
        r.execute_batch(setup).unwrap();
        f.set_reject_mem_fallback(true);
        f.set_strict_mem_fallback_rejection(true);
        for (sql, bindings) in [
            (
                "SELECT post_id,sum(n+?) AS total FROM likes \
                 WHERE post_id IN (?,?,?) AND n>? GROUP BY post_id \
                 HAVING total>=? LIMIT ? OFFSET ?",
                vec![
                    vec![2, 2, 3, 4, -1, 9, 2, 0],
                    vec![100, 4, 4, 99, -1, 0, 1, 0],
                ],
            ),
            (
                "SELECT post_id,count(*) FROM likes \
                 WHERE post_id IN (:one,:one,:two) GROUP BY post_id \
                 HAVING count(*)>=:minimum LIMIT :maximum OFFSET :skip",
                vec![
                    vec![2, 3, 1, 2, 0],
                    vec![3, 4, 1, 1, 1],
                    vec![99, 98, 0, -1, 0],
                ],
            ),
            (
                "SELECT post_id,sum(n) FROM likes \
                 WHERE suffix=? AND post_id=? GROUP BY post_id",
                vec![vec![1, 2], vec![2, 3]],
            ),
            (
                "SELECT post_id,count(*) FROM likes \
                 WHERE n>? AND post_id=? AND id>? GROUP BY post_id",
                vec![vec![12, 2, 0], vec![5, 3, 0], vec![0, 99, 0]],
            ),
            (
                "SELECT post_id,count(*) FROM likes \
                 WHERE suffix=:suffix AND post_id=:post GROUP BY post_id",
                vec![vec![1, 2], vec![2, 3]],
            ),
        ] {
            let prepared = f.prepare(sql).await.unwrap();
            let mut stock = r.prepare(sql).unwrap();
            for stage in 0..3 {
                match stage {
                    1 => {
                        let change = "BEGIN; UPDATE likes SET n=n+5,suffix='1' WHERE post_id=2;";
                        f.execute_batch(change).await.unwrap();
                        r.execute_batch(change).unwrap();
                    }
                    2 => {
                        f.execute("ROLLBACK").await.unwrap();
                        r.execute_batch("ROLLBACK").unwrap();
                    }
                    _ => {}
                }
                for values in bindings.iter().chain(bindings.first()) {
                    let expected: Vec<Vec<String>> = stock
                        .query_map(rusqlite::params_from_iter(values), |row| {
                            Ok((0..2).map(|i| tag_r(&row.get_unwrap(i))).collect())
                        })
                        .unwrap()
                        .collect::<Result<_, _>>()
                        .unwrap();
                    let params: Vec<_> = values.iter().copied().map(SqliteValue::Integer).collect();
                    let actual: Vec<Vec<String>> = prepared
                        .query_with_params(&params)
                        .await
                        .unwrap()
                        .iter()
                        .map(|row| row.values().iter().map(tag_f).collect())
                        .collect();
                    assert_eq!(actual, expected, "stage={stage}, binds={values:?}: {sql}");
                }
            }
        }
        f.close().await.unwrap();
    });
}

/// The work budget observes actual execution with a forced full-scan control;
/// no elapsed-time threshold or inactive zero counter can make it pass.
#[test]
fn indexed_group_by_work_tracks_matches_instead_of_table_size() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        const ROWS: u64 = 20_000;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("grouped-work.db");
        let stock = rusqlite::Connection::open(&path).unwrap();
        stock
            .execute_batch(&format!(
                "CREATE TABLE likes(id INTEGER PRIMARY KEY,post_id INTEGER,n INTEGER);\
             CREATE INDEX likes_post ON likes(post_id);\
             WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<{ROWS})\
             INSERT INTO likes SELECT i,(i-1)/4,i%17 FROM s;"
            ))
            .unwrap();
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        f.set_reject_mem_fallback(true);
        f.set_strict_mem_fallback_rejection(true);
        for (sql, params, is_scan, covering) in [
            (
                "SELECT post_id,count(*) FROM likes WHERE post_id=?1 GROUP BY post_id",
                vec![2374],
                false,
                true,
            ),
            (
                "SELECT post_id,count(*) FROM likes WHERE post_id=?1 GROUP BY post_id",
                vec![99_999],
                false,
                true,
            ),
            (
                "SELECT post_id,count(*) FROM likes WHERE post_id IN (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20) GROUP BY post_id",
                (1..=20).map(|n| n * 37).collect(),
                false,
                true,
            ),
            (
                "SELECT post_id,count(*) FROM likes WHERE n>?1 AND post_id=?2 GROUP BY post_id",
                vec![5, 2374],
                false,
                false,
            ),
            (
                "SELECT post_id,count(*) FROM likes NOT INDEXED WHERE n>?1 AND post_id=?2 GROUP BY post_id",
                vec![5, 2374],
                true,
                false,
            ),
            (
                "SELECT post_id,count(*) FROM likes NOT INDEXED WHERE post_id=?1 GROUP BY post_id",
                vec![2374],
                true,
                false,
            ),
        ] {
            let mut oracle = stock.prepare(sql).unwrap();
            let expected: Vec<Vec<String>> = oracle
                .query_map(rusqlite::params_from_iter(&params), |row| {
                    Ok((0..2).map(|i| tag_r(&row.get_unwrap(i))).collect())
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            let prepared = f.prepare(sql).await.unwrap();
            let values: Vec<_> = params.iter().copied().map(SqliteValue::Integer).collect();
            prepared.query_with_params(&values).await.unwrap();
            set_vdbe_metrics_enabled(true);
            reset_vdbe_metrics();
            let result = prepared.query_with_params(&values).await;
            let metrics = vdbe_metrics_snapshot();
            set_vdbe_metrics_enabled(false);
            let actual: Vec<Vec<String>> = result
                .unwrap()
                .iter()
                .map(|row| row.values().iter().map(tag_f).collect())
                .collect();
            assert_eq!(actual, expected, "{sql}: {params:?}");
            let steps: u64 = metrics
                .opcode_execution_totals
                .iter()
                .filter(|entry| matches!(entry.opcode.as_str(), "Next" | "Prev"))
                .map(|entry| entry.total)
                .sum();
            eprintln!(
                "GH#531 grouped scan={is_scan}, opcodes={}, cursor_steps={steps}, sorter_rows={}: {sql}",
                metrics.opcodes_executed_total, metrics.sort_rows_total
            );
            assert!(
                metrics.opcodes_executed_total > 0,
                "execution counters were inactive: {sql}"
            );
            if is_scan {
                assert!(
                    steps >= ROWS,
                    "scan control did not visit {ROWS} rows: {metrics:?}"
                );
            } else {
                let table_lookups: u64 = metrics
                    .opcode_execution_totals
                    .iter()
                    .filter(|entry| entry.opcode == "SeekRowid")
                    .map(|entry| entry.total)
                    .sum();
                if covering {
                    assert_eq!(
                        table_lookups, 0,
                        "covered grouping fetched source-table rows: {metrics:?}"
                    );
                } else {
                    assert_eq!(
                        table_lookups, 4,
                        "residual evaluation must fetch only the four prefix matches: {metrics:?}"
                    );
                }
                assert!(
                    steps < 1_000,
                    "selective grouping scanned the table: {metrics:?}"
                );
                assert!(
                    metrics.opcodes_executed_total < 25_000,
                    "selective grouping exceeded its work budget: {metrics:?}"
                );
                assert!(
                    metrics.sort_rows_total <= 100,
                    "grouping retained unrelated rows: {metrics:?}"
                );
            }
        }
        f.close().await.unwrap();
    });
}
