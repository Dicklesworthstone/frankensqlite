#![recursion_limit = "512"]

//! GH#497 (bd-5qu8n): `col IN ('a', 'b', …)` on a TEXT column with an ordinary index seeks the
//! index once per distinct value, as `col = 'a'` does, instead of scanning the whole table.
//! Results are compared with stock SQLite (rusqlite, bundled) as sorted row sets (no ORDER BY, so
//! row order is unspecified), on `:memory:` and on a file, through `query()` and
//! `prepare().query()`; the EXPLAIN of the BINARY TEXT shape must open and seek the index.
//! Also covered: an integer IN list over a composite index whose trailing key column holds NULL
//! (the probe is a one-field prefix there), and shapes that must keep the full scan (a NOCASE
//! column) yet still return stock's rows.
//!
//! The issue's impact case, a parameter list `key IN (?1, ?2, ...)`, seeks too: members take the
//! column's affinity (an integer 5 finds TEXT '5', REAL 2.5 and text '1' on an INTEGER column
//! behave as stock), NULLs and duplicates drop out, and a residual conjunct still filters.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE r9_t (id TEXT PRIMARY KEY, k TEXT NOT NULL, v TEXT)",
    "CREATE INDEX r9_t_k ON r9_t (k)",
    "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 199) \
     INSERT INTO r9_t SELECT 'id' || i, 'k' || (i % 20), 'v' || i FROM n",
    "INSERT INTO r9_t VALUES ('e1', '', 'empty'), ('u1', 'ключ', 'unicode')",
    "INSERT INTO r9_t VALUES ('e5', 5, 'five'), ('e6', '5.0', 'five point zero')",
    "CREATE TABLE c (a INT, b INT, v TEXT)",
    "CREATE INDEX c_ab ON c (a, b)",
    "INSERT INTO c VALUES (1, NULL, 'n1'), (1, 2, 'x'), (2, NULL, 'n2'), (3, 4, 'y'), (1, NULL, 'n3')",
    "INSERT INTO c VALUES (2.5, 7, 'real'), (-1, 0, 'neg'), ('txt', 1, 'text')",
    "CREATE TABLE nc (k TEXT COLLATE NOCASE, v TEXT)",
    "CREATE INDEX nc_k ON nc (k)",
    "INSERT INTO nc VALUES ('A', 'upper'), ('a', 'lower'), ('b', 'other')",
];

const QUERIES: &[&str] = &[
    "SELECT v FROM r9_t WHERE k IN ('k1')",
    "SELECT v FROM r9_t WHERE k IN ('k1', 'k2')",
    "SELECT v FROM r9_t WHERE k IN ('k2', 'k1', 'k2')",
    "SELECT v FROM r9_t WHERE k IN ('zz')",
    "SELECT v FROM r9_t WHERE k IN ('', 'ключ')",
    "SELECT id, v FROM r9_t WHERE k IN ('k3', 'k4') AND v LIKE 'v1%'",
    "SELECT * FROM r9_t WHERE k IN ('k5')",
    "SELECT v FROM c WHERE a IN (1, 2)",
    "SELECT v FROM c WHERE a IN (1) AND b IS NULL",
    "SELECT v FROM nc WHERE k IN ('A')",
    "SELECT v FROM nc WHERE k IN ('a', 'B')",
    // Lists the literal paths decline: mixed classes, numbers on TEXT, text on INTEGER.
    "SELECT v FROM r9_t WHERE k IN ('k1', 5)",
    "SELECT v FROM r9_t WHERE k IN (5, 5.0, '5')",
    "SELECT v FROM c WHERE a IN ('1', 2.0)",
    "SELECT v FROM c WHERE a IN (-1, 1)",
];

/// GH#497's impact case: `key IN (?1, ?2, ...)`, a batched key lookup. Each member takes the
/// column's affinity; NULLs and duplicates are dropped.
const PARAM_QUERIES: &[(&str, &[SqliteValue])] = &[
    (
        "SELECT v FROM r9_t WHERE k IN (?1)",
        &[SqliteValue::Integer(5)],
    ),
    (
        "SELECT v FROM r9_t WHERE k IN (?1, ?2)",
        &[SqliteValue::Integer(5), SqliteValue::Float(5.0)],
    ),
    (
        "SELECT v FROM r9_t WHERE k IN (?1, ?2)",
        &[SqliteValue::Null, SqliteValue::Integer(5)],
    ),
    (
        "SELECT v FROM r9_t WHERE k IN (?1, ?1, ?2)",
        &[SqliteValue::Null, SqliteValue::Null],
    ),
    (
        "SELECT id, v FROM r9_t WHERE k IN ('k3', ?1) AND v LIKE 'v1%'",
        &[SqliteValue::Integer(5)],
    ),
    (
        "SELECT v FROM r9_t WHERE v <> ?1 AND k IN (?2, ?3)",
        &[
            SqliteValue::Integer(0),
            SqliteValue::Integer(5),
            SqliteValue::Float(5.0),
        ],
    ),
    (
        "SELECT v FROM c WHERE a IN (?1, ?2)",
        &[SqliteValue::Integer(1), SqliteValue::Integer(3)],
    ),
    (
        "SELECT v FROM c WHERE a IN (?1, ?2)",
        &[SqliteValue::Float(1.0), SqliteValue::Float(2.5)],
    ),
    (
        "SELECT v FROM c WHERE a IN (?1) AND b IS NULL",
        &[SqliteValue::Integer(1)],
    ),
    ("SELECT v FROM nc WHERE k IN (?1)", &[SqliteValue::Null]),
];

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

fn to_rusqlite(v: &SqliteValue) -> rusqlite::types::Value {
    match v {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
        SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
        SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
        SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
    }
}

fn stock_rows(r: &rusqlite::Connection, sql: &str, params: &[SqliteValue]) -> Vec<Vec<String>> {
    let mut statement = r.prepare(sql).expect("stock prepare");
    let n = statement.column_count();
    let params: Vec<rusqlite::types::Value> = params.iter().map(to_rusqlite).collect();
    let rows = statement
        .query_map(rusqlite::params_from_iter(params), |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .expect("stock query");
    sorted(rows)
}

fn frank_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    sorted(
        rows.iter()
            .map(|row| row.values().iter().map(tag_f).collect())
            .collect(),
    )
}

async fn run(file_backed: bool) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = if file_backed {
        dir.path()
            .join("gh497.db")
            .to_str()
            .expect("utf-8 path")
            .to_owned()
    } else {
        ":memory:".to_owned()
    };
    let f = Connection::open(&path).await.expect("open");
    let r = rusqlite::Connection::open_in_memory().expect("stock open");
    for sql in SETUP {
        f.execute(sql).await.expect("frank setup");
        r.execute(sql, []).expect("stock setup");
    }
    let mut failures = Vec::new();
    for sql in QUERIES {
        let stock = stock_rows(&r, sql, &[]);
        let direct = f.query(sql).await.map(|rows| frank_rows(&rows));
        if direct.as_ref().ok() != Some(&stock) {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` (query): frank {direct:?} vs stock {stock:?}"
            ));
        }
        let prepared = match f.prepare(sql).await {
            Ok(statement) => statement.query().await.map(|rows| frank_rows(&rows)),
            Err(e) => Err(e),
        };
        if prepared.as_ref().ok() != Some(&stock) {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` (prepare): frank {prepared:?} vs stock {stock:?}"
            ));
        }
    }
    for (sql, params) in PARAM_QUERIES {
        let stock = stock_rows(&r, sql, params);
        let direct = f
            .query_with_params(sql, params)
            .await
            .map(|rows| frank_rows(&rows));
        if direct.as_ref().ok() != Some(&stock) {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` {params:?} (query_with_params): frank \
                 {direct:?} vs stock {stock:?}"
            ));
        }
        let prepared = match f.prepare(sql).await {
            Ok(statement) => statement
                .query_with_params(params)
                .await
                .map(|rows| frank_rows(&rows)),
            Err(e) => Err(e),
        };
        if prepared.as_ref().ok() != Some(&stock) {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` {params:?} (prepare): frank {prepared:?} vs \
                 stock {stock:?}"
            ));
        }
    }
    // The index is opened and sought, as for `k = 'k1'`: the table (cursor 0) is never
    // rewound. A parameter list rewinds only its probe set.
    for sql in [
        "EXPLAIN SELECT v FROM r9_t WHERE k IN ('k1')",
        "EXPLAIN SELECT v FROM r9_t WHERE k IN ('k1', 'k2')",
        "EXPLAIN SELECT v FROM r9_t WHERE k IN (?1)",
        "EXPLAIN SELECT v FROM r9_t WHERE k IN (?1, ?2, ?3)",
        "EXPLAIN SELECT v FROM c WHERE a IN (?1, ?2)",
    ] {
        let program: Vec<(String, i64)> = f
            .query(sql)
            .await
            .expect("explain")
            .iter()
            .filter_map(|row| match (row.values().get(1), row.values().get(2)) {
                (Some(SqliteValue::Text(op)), Some(SqliteValue::Integer(p1))) => {
                    Some((op.to_string(), *p1))
                }
                _ => None,
            })
            .collect();
        let seeks = program.iter().any(|(op, _)| op == "SeekGE");
        let rewinds_table = program.iter().any(|(op, p1)| op == "Rewind" && *p1 == 0);
        if !seeks || rewinds_table {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` does not seek the index: {program:?}"
            ));
        }
    }
    f.close().await.expect("close");
    failures
}

#[test]
fn text_in_list_seeks_the_index_and_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for file_backed in [false, true] {
            failures.extend(run(file_backed).await);
        }
        assert!(
            failures.is_empty(),
            "{} failures:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

/// bd-in-or-index-scan-fs7nd: selective IN/OR predicates must seek REAL,
/// NUMERIC and TEXT indexes for both row-producing and aggregate queries.
/// Mixed storage classes, numeric aliases and NULLs exercise the probe set's
/// affinity and deduplication semantics against stock, not just its plan.
#[test]
fn typed_index_membership_seeks_and_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().expect("typed membership temp dir");
            let path = if file_backed {
                dir.path().join("typed.db").to_str().unwrap().to_owned()
            } else {
                ":memory:".to_owned()
            };
            let frank = Connection::open(&path).await.unwrap();
            let stock = rusqlite::Connection::open_in_memory().unwrap();
            for sql in [
                "CREATE TABLE typed_keys(id INTEGER PRIMARY KEY, r REAL, n NUMERIC, t TEXT, suffix, payload TEXT)",
                "CREATE INDEX typed_r ON typed_keys(r)",
                "CREATE INDEX typed_n ON typed_keys(n, suffix DESC)",
                "CREATE INDEX typed_t ON typed_keys(t)",
                "INSERT INTO typed_keys VALUES \
                 (1,1,1,'1',NULL,'one'), \
                 (2,1.0,1.0,'01',4,'duplicate'), \
                 (3,2.5,2.5,'2.5',NULL,'fraction'), \
                 (4,-1,-1,'-1',8,'negative'), \
                 (5,NULL,NULL,NULL,NULL,'null'), \
                 (6,'word','word','word',2,'text'), \
                 (7,X'3132',X'3132',X'3132',NULL,'blob'), \
                 (8,9007199254740992,9007199254740992,'9007199254740992',NULL,'even'), \
                 (9,9007199254740993,9007199254740993,'9007199254740993',7,'odd'), \
                 (10,0,0,'',NULL,'zero')",
                "CREATE TABLE collated(k TEXT COLLATE NOCASE, v INTEGER)",
                "CREATE INDEX collated_k ON collated(k)",
                "INSERT INTO collated VALUES('A',1),('a',2),('b',3)",
                "CREATE INDEX typed_t_nocase ON typed_keys(t COLLATE NOCASE)",
                "CREATE TABLE untyped(k, payload TEXT)",
                "CREATE INDEX untyped_k ON untyped(k)",
                "INSERT INTO untyped VALUES(1,'integer'),(1.0,'real'),('1','text'),('01','leading'),(X'31','blob'),(NULL,'null')",
                "CREATE TABLE partial_only(k NUMERIC)",
                "CREATE INDEX partial_positive ON partial_only(k) WHERE k > 0",
                "INSERT INTO partial_only VALUES(-1),(1.5),(4),(NULL)",
                "CREATE TABLE mismatched(k TEXT)",
                "CREATE INDEX mismatched_nocase ON mismatched(k COLLATE NOCASE)",
                "INSERT INTO mismatched VALUES('A'),('a'),('b')",
            ] {
                frank.execute(sql).await.unwrap();
                stock.execute_batch(sql).unwrap();
            }

            let mut queries = Vec::new();
            for column in ["r", "n", "t"] {
                for predicate in [
                    format!("{column} IN (1, 2.5, 'word')"),
                    format!("{column} IN (1, 1.0, '1', NULL, 1)"),
                    format!("{column} IN (NULL, NULL)"),
                    format!("{column} IN (-1, X'3132', 'absent')"),
                    format!("{column} IN (9007199254740993)"),
                    format!("{column} IN (9007199254740992, 9007199254740993)"),
                    format!("{column} = 1 OR {column} = 2.5 OR 'word' = {column}"),
                    format!("{column} = 1 OR {column} = 1.0 OR {column} = '1'"),
                ] {
                    for projection in ["id, payload", "count(*)", "count(id), sum(id)"] {
                        queries.push(format!(
                            "SELECT {projection} FROM typed_keys WHERE {predicate}"
                        ));
                    }
                }
                queries.push(format!(
                    "SELECT count(*), min({column}), max({column}) FROM typed_keys \
                     WHERE {column} IN (1, 1.0, '1', 2.5)"
                ));
                queries.push(format!(
                    "SELECT count(*), sum(id) FROM typed_keys \
                     WHERE id > 1 AND {column} IN (1, 2.5, 'word') AND suffix IS NULL"
                ));
                queries.push(format!(
                    "SELECT {column}, count(*) FROM typed_keys \
                     WHERE {column} IN (1, 1.0, '1', 2.5) GROUP BY {column}"
                ));
            }
            queries.extend([
                "SELECT typeof(sum(r)), sum(r), min(r), max(r) FROM typed_keys WHERE r IN (1, 2.5)".to_owned(),
                "SELECT count(DISTINCT r), sum(DISTINCT r) FROM typed_keys WHERE r IN (1,1.0,2.5)".to_owned(),
                "SELECT count(*) FILTER (WHERE id > 1), sum(id) FILTER (WHERE suffix IS NULL) FROM typed_keys WHERE n IN (1,2.5,'word')".to_owned(),
                "SELECT payload, min(r) FROM typed_keys WHERE r IN (1,2.5)".to_owned(),
                "SELECT group_concat(id) FROM typed_keys WHERE t IN ('word','1','2.5')".to_owned(),
                "SELECT count(*) FROM typed_keys WHERE t IN ('WORD' COLLATE NOCASE)".to_owned(),
                "SELECT count(*) FROM typed_keys WHERE t IN ('WORD' COLLATE NOCASE,'absent')".to_owned(),
                "SELECT count(*) FROM typed_keys WHERE t = 'WORD' COLLATE NOCASE OR t = 'word'".to_owned(),
                "SELECT count(*) FROM collated WHERE k IN ('A','a','B',NULL)".to_owned(),
                "SELECT count(*) FROM typed_keys NOT INDEXED WHERE r IN (1,2.5)".to_owned(),
                "SELECT count(*) FROM typed_keys WHERE r NOT IN (1,NULL)".to_owned(),
                "SELECT count(*) FROM typed_keys WHERE r = 1 OR t = 'word'".to_owned(),
                "SELECT id FROM typed_keys WHERE t = CAST(1 AS NUMERIC) OR t = 'word'".to_owned(),
                "SELECT id FROM typed_keys WHERE t IN (CAST(1 AS NUMERIC),'word')".to_owned(),
                "SELECT count(*) FROM typed_keys WHERE n = CAST('1' AS TEXT) OR n = 2.5".to_owned(),
                "SELECT payload FROM untyped WHERE k IN (1,1.0,'1',NULL,X'31')".to_owned(),
                "SELECT count(*), sum(rowid) FROM untyped WHERE k = 1 OR k = 1.0 OR k = '1'".to_owned(),
                "SELECT payload FROM untyped WHERE k = CAST(1 AS NUMERIC) OR k = 'missing'".to_owned(),
                "SELECT count(*) FROM partial_only WHERE k IN (-1,1.5)".to_owned(),
                "SELECT count(*) FROM partial_only WHERE k = -1 OR k = 1.5".to_owned(),
                "SELECT k FROM mismatched WHERE k IN ('A','b')".to_owned(),
                "SELECT count(*) FROM mismatched WHERE k = 'A' OR k = 'b'".to_owned(),
            ]);
            for sql in &queries {
                let expected = stock_rows(&stock, sql, &[]);
                assert_eq!(
                    frank_rows(&frank.query(sql).await.unwrap()),
                    expected,
                    "direct file_backed={file_backed}: {sql}"
                );
                assert_eq!(
                    frank_rows(&frank.prepare(sql).await.unwrap().query().await.unwrap()),
                    expected,
                    "prepared file_backed={file_backed}: {sql}"
                );
            }

            for (sql, params) in [
                (
                    "SELECT count(*), sum(id) FROM typed_keys WHERE r IN (?1,?2,?3)",
                    vec![
                        SqliteValue::Integer(1),
                        SqliteValue::Float(1.0),
                        SqliteValue::Null,
                    ],
                ),
                (
                    "SELECT count(*), sum(id) FROM typed_keys WHERE r IN (?1,?2)",
                    vec![
                        SqliteValue::Integer(9_007_199_254_740_993),
                        SqliteValue::Text("9007199254740993".into()),
                    ],
                ),
                (
                    "SELECT count(*), sum(id) FROM typed_keys WHERE id > ?1 AND n IN (?2,?3)",
                    vec![
                        SqliteValue::Integer(1),
                        SqliteValue::Text("1".into()),
                        SqliteValue::Float(2.5),
                    ],
                ),
                (
                    "SELECT sum(id + ?1) FROM typed_keys WHERE r IN (?2,?3)",
                    vec![
                        SqliteValue::Integer(10),
                        SqliteValue::Integer(1),
                        SqliteValue::Float(2.5),
                    ],
                ),
                (
                    "SELECT id FROM typed_keys WHERE ?1 = t OR t = ?2 OR t = ?3",
                    vec![
                        SqliteValue::Integer(1),
                        SqliteValue::Text("1".into()),
                        SqliteValue::Null,
                    ],
                ),
                (
                    "SELECT count(*), sum(id) FROM typed_keys WHERE payload <> ? AND n IN (?,?) AND payload <> ?",
                    vec![
                        SqliteValue::Text("duplicate".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Float(2.5),
                        SqliteValue::Text("absent".into()),
                    ],
                ),
            ] {
                let expected = stock_rows(&stock, sql, &params);
                assert_eq!(
                    frank_rows(&frank.query_with_params(sql, &params).await.unwrap()),
                    expected,
                    "direct parameters file_backed={file_backed}: {sql}"
                );
                assert_eq!(
                    frank_rows(
                        &frank
                            .prepare(sql)
                            .await
                            .unwrap()
                            .query_with_params(&params)
                            .await
                            .unwrap()
                    ),
                    expected,
                    "prepared parameters file_backed={file_backed}: {sql}"
                );
            }

            for column in ["r", "n", "t"] {
                for predicate in [
                    format!("{column} IN (1,2.5)"),
                    format!("{column} IN (?1,?2)"),
                    format!("{column} = 1 OR {column} = 2.5"),
                ] {
                    for projection in ["id", "count(*)", "sum(id)"] {
                        let sql = format!(
                            "EXPLAIN SELECT {projection} FROM typed_keys WHERE {predicate}"
                        );
                        let rows = frank.query(&sql).await.unwrap();
                        let operations: Vec<_> = rows.iter().map(|row| row.values()).collect();
                        assert!(
                            operations
                                .iter()
                                .any(|op| op[1] == SqliteValue::Text("SeekGE".into())),
                            "must seek the membership index, file_backed={file_backed}: {sql}"
                        );
                        assert!(
                            !operations.iter().any(|op| {
                                op[1] == SqliteValue::Text("Rewind".into())
                                    && op[2] == SqliteValue::Integer(0)
                            }),
                            "must not scan the table, file_backed={file_backed}: {sql}"
                        );
                    }
                }
            }
            frank.close().await.unwrap();
        }
    });
}
