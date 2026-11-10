#![recursion_limit = "512"]

//! Differential oracle: window functions vs rusqlite (bundled SQLite 3.53). A
//! probe sweep found this surface stock-correct across 20 cases; this keeper
//! locks it in. Deterministic ORDER BY keys avoid tie-ordering ambiguity.
//!
//! Covers ranking (ROW_NUMBER/RANK/DENSE_RANK/NTILE), running aggregates over
//! ROWS and RANGE frames (including RANGE ties and UNBOUNDED FOLLOWING),
//! navigation (LAG/LEAD with offset+default, FIRST_VALUE/LAST_VALUE), PARTITION
//! BY, named WINDOW clauses, and a window function over a grouped aggregate.
//! The bd-row-number-join-kmqdn cases also cover unordered numbering and frame
//! clauses through the join and grouped-window fallbacks, in memory and on
//! disk, through both direct and prepared queries.
//! bd-3oyv2 covers subqueries over attached tables in window query filters,
//! projections, wrapped window expressions, and the final ordering.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
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
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e:?}>")]],
    }
}
fn rq(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
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

async fn agree(sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in SETUP {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(
        fr, rr,
        "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}"
    );
}

const SETUP: &[&str] = &[
    "CREATE TABLE s(g TEXT, id INT, v INT)",
    "INSERT INTO s VALUES ('a',1,10),('a',2,20),('a',3,20),('a',4,40),('b',5,5),('b',6,15),('b',7,25)",
];

#[test]
fn ranking_functions() {
    asupersync::test_utils::run_test(|| async {
        agree(
            "SELECT id, row_number() OVER (ORDER BY id) FROM s ORDER BY id",
            "ROW_NUMBER",
        )
        .await;
        agree(
            "SELECT id, v, rank() OVER (ORDER BY v) FROM s ORDER BY id",
            "RANK with ties",
        )
        .await;
        agree(
            "SELECT id, v, dense_rank() OVER (ORDER BY v) FROM s ORDER BY id",
            "DENSE_RANK with ties",
        )
        .await;
        agree(
            "SELECT id, ntile(3) OVER (ORDER BY id) FROM s ORDER BY id",
            "NTILE(3)",
        )
        .await;
        agree(
            "SELECT g, id, v, rank() OVER (PARTITION BY g ORDER BY v DESC) FROM s ORDER BY g, id",
            "RANK partitioned",
        )
        .await;
    });
}

async fn row_number_queries_match_stock(queries: &[&str]) {
    let mut differences = Vec::new();
    for file_backed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (f, r) = if file_backed {
            (
                Connection::open(dir.path().join("frank.db").to_str().unwrap())
                    .await
                    .unwrap(),
                rusqlite::Connection::open(dir.path().join("stock.db")).unwrap(),
            )
        } else {
            (
                Connection::open(":memory:").await.unwrap(),
                rusqlite::Connection::open_in_memory().unwrap(),
            )
        };
        for sql in SETUP {
            f.execute(sql).await.unwrap();
            r.execute_batch(sql).unwrap();
        }
        for sql in queries {
            let expected = rq(&r, sql);
            for prepared in [false, true] {
                let rows = if prepared {
                    f.prepare(sql).await.unwrap().query().await
                } else {
                    f.query(sql).await
                }
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
                let actual = rows
                    .iter()
                    .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                if actual != expected {
                    differences.push(format!(
                        "file_backed={file_backed} prepared={prepared}: {sql}\n\
                         frank={actual:?}\nstock={expected:?}"
                    ));
                }
            }
        }
        f.close().await.unwrap();
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
}

#[test]
fn row_number_without_order_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        // Numbering within an unordered partition may follow any input order.
        // Sort by the assigned number so these comparisons do not prescribe a
        // join plan or a tie order; the numbers must still be 1 through N.
        row_number_queries_match_stock(&[
            "SELECT row_number() OVER () AS n FROM s ORDER BY n",
            "SELECT row_number() OVER () AS n FROM s a JOIN s b ON a.id = b.id ORDER BY n",
            "SELECT row_number() OVER () AS n FROM json_each('[10,20]') a JOIN s b ON a.key + 1 = b.id ORDER BY n",
            "SELECT 100 + row_number() OVER () AS n FROM s a JOIN s b ON a.id = b.id ORDER BY n",
            "SELECT a.g, row_number() OVER (PARTITION BY a.g) AS n FROM s a JOIN s b ON a.id = b.id ORDER BY a.g, n",
            "SELECT row_number() OVER () AS n FROM s a JOIN s b ON a.id = b.id GROUP BY a.g ORDER BY n",
            "SELECT 100 + row_number() OVER () AS n FROM s GROUP BY g ORDER BY n",
            "SELECT a.g, row_number() OVER (PARTITION BY a.g) AS n FROM s a JOIN s b ON a.id = b.id GROUP BY a.g, a.v ORDER BY a.g, n",
            "SELECT a.g, row_number() OVER w AS n FROM s a JOIN s b ON a.id = b.id WINDOW w AS (PARTITION BY a.g) ORDER BY a.g, n",
            "SELECT row_number() OVER () AS n FROM s a JOIN s b ON a.id = b.id WHERE a.id < 0 ORDER BY n",
            "SELECT row_number() OVER () AS n, count(*) OVER () AS total FROM s a JOIN s b ON a.id = b.id ORDER BY n",
        ])
        .await;
    });
}

#[test]
fn row_number_ignores_frame_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        row_number_queries_match_stock(&[
            "SELECT row_number() OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS n FROM s a JOIN s b ON a.id = b.id ORDER BY n",
            "SELECT row_number() OVER (ROWS CURRENT ROW) AS n FROM s a JOIN s b ON a.id = b.id ORDER BY n",
            "SELECT row_number() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS n FROM s a JOIN s b ON a.id = b.id ORDER BY n",
            "SELECT a.id, row_number() OVER (ORDER BY a.id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS n FROM s a JOIN s b ON a.id = b.id ORDER BY a.id",
            "SELECT a.id, row_number() OVER (ORDER BY a.id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS n FROM s a JOIN s b ON a.id = b.id ORDER BY a.id",
            "SELECT a.id, row_number() OVER (ORDER BY a.id DESC ROWS CURRENT ROW) AS n FROM s a JOIN s b ON a.id = b.id ORDER BY a.id",
            "SELECT a.id, row_number() OVER (ORDER BY a.id RANGE CURRENT ROW EXCLUDE CURRENT ROW) AS n FROM s a JOIN s b ON a.id = b.id ORDER BY a.id",
            "SELECT row_number() OVER (ORDER BY a.v GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP) AS n FROM s a JOIN s b ON a.id = b.id ORDER BY n",
            "SELECT a.g, row_number() OVER (PARTITION BY a.g ROWS CURRENT ROW) AS n FROM s a JOIN s b ON a.id = b.id GROUP BY a.g, a.v ORDER BY a.g, n",
            "SELECT a.g, 100 + row_number() OVER (PARTITION BY a.g ORDER BY a.v ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING EXCLUDE TIES) AS n FROM s a JOIN s b ON a.id = b.id GROUP BY a.g, a.v ORDER BY a.g, n",
            "SELECT g, row_number() OVER (ORDER BY g ROWS CURRENT ROW) AS n FROM s GROUP BY g ORDER BY g",
        ])
        .await;
    });
}

struct RowNumberOverride {
    num_args: i32,
}

impl fsqlite_func::WindowFunction for RowNumberOverride {
    type State = i64;

    fn initial_state(&self) -> Self::State {
        0
    }

    fn step(&self, state: &mut Self::State, _args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        *state += 1;
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, _args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        *state -= 1;
        Ok(())
    }

    fn value(&self, state: &Self::State) -> fsqlite_error::Result<SqliteValue> {
        Ok(SqliteValue::Integer(100 + *state))
    }

    fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
        Ok(SqliteValue::Integer(100 + state))
    }

    fn num_args(&self) -> i32 {
        self.num_args
    }

    fn name(&self) -> &'static str {
        "row_number"
    }
}

#[test]
fn row_number_custom_override_preserved() {
    asupersync::test_utils::run_test(|| async {
        for num_args in [0, -1] {
            let f = Connection::open(":memory:").await.unwrap();
            for sql in SETUP {
                f.execute(sql).await.unwrap();
            }
            f.register_window_function(RowNumberOverride { num_args });
            // An application function named row_number still uses its own
            // step/inverse/value callbacks and the SQL window frame.
            for (sql, value, count) in [
                (
                    "SELECT row_number() OVER () FROM s a JOIN s b ON a.id = b.id",
                    107,
                    7,
                ),
                (
                    "SELECT 1000 + row_number() OVER () FROM s a JOIN s b ON a.id = b.id",
                    1107,
                    7,
                ),
                (
                    "SELECT row_number() OVER (ORDER BY a.id ROWS CURRENT ROW) FROM s a JOIN s b ON a.id = b.id",
                    101,
                    7,
                ),
                (
                    "SELECT row_number() OVER () FROM s a JOIN s b ON a.id = b.id GROUP BY a.g",
                    102,
                    2,
                ),
                (
                    "SELECT 1000 + row_number() OVER (ORDER BY a.g ROWS CURRENT ROW) FROM s a JOIN s b ON a.id = b.id GROUP BY a.g",
                    1101,
                    2,
                ),
                (
                    "SELECT row_number() FILTER (WHERE a.v > 20) OVER () FROM s a JOIN s b ON a.id = b.id",
                    102,
                    7,
                ),
            ] {
                for prepared in [false, true] {
                    let rows = if prepared {
                        f.prepare(sql).await.unwrap().query().await
                    } else {
                        f.query(sql).await
                    }
                    .unwrap_or_else(|error| panic!("{sql}: {error}"));
                    let actual = rows
                        .iter()
                        .map(|row| row.values().to_vec())
                        .collect::<Vec<_>>();
                    assert_eq!(
                        actual,
                        vec![vec![SqliteValue::Integer(value)]; count],
                        "num_args={num_args} prepared={prepared}: {sql}"
                    );
                }
            }
            f.close().await.unwrap();
        }
    });
}

#[test]
fn running_frames() {
    asupersync::test_utils::run_test(|| async {
        agree(
            "SELECT id, sum(v) OVER (ORDER BY id) FROM s ORDER BY id",
            "default running sum",
        )
        .await;
        agree("SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM s ORDER BY id", "ROWS 1 preceding").await;
        agree("SELECT id, v, sum(v) OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM s ORDER BY id", "RANGE with ties").await;
        agree("SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM s ORDER BY id", "ROWS unbounded following").await;
    });
}

#[test]
fn navigation_functions() {
    asupersync::test_utils::run_test(|| async {
        agree(
            "SELECT id, lag(v) OVER (ORDER BY id) FROM s ORDER BY id",
            "LAG",
        )
        .await;
        agree(
            "SELECT id, lag(v, 1, -1) OVER (ORDER BY id) FROM s ORDER BY id",
            "LAG with offset+default",
        )
        .await;
        agree(
            "SELECT id, lead(v) OVER (ORDER BY id) FROM s ORDER BY id",
            "LEAD",
        )
        .await;
        agree(
            "SELECT id, lead(v, 2) OVER (ORDER BY id) FROM s ORDER BY id",
            "LEAD offset 2",
        )
        .await;
        agree(
            "SELECT id, first_value(v) OVER (PARTITION BY g ORDER BY id) FROM s ORDER BY id",
            "FIRST_VALUE",
        )
        .await;
        agree("SELECT id, last_value(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM s ORDER BY id", "LAST_VALUE full frame").await;
    });
}

#[test]
fn partition_named_and_over_group() {
    asupersync::test_utils::run_test(|| async {
        agree(
            "SELECT g, id, row_number() OVER (PARTITION BY g ORDER BY id) FROM s ORDER BY g, id",
            "ROW_NUMBER partitioned",
        )
        .await;
        agree(
            "SELECT g, id, avg(v) OVER (PARTITION BY g ORDER BY id) FROM s ORDER BY g, id",
            "partitioned running avg",
        )
        .await;
        agree(
            "SELECT id, count(*) OVER (PARTITION BY g) FROM s ORDER BY id",
            "count(*) over partition",
        )
        .await;
        agree(
            "SELECT id, sum(v) OVER w, avg(v) OVER w FROM s WINDOW w AS (ORDER BY id) ORDER BY id",
            "named window",
        )
        .await;
        agree(
            "SELECT g, sum(v) tot, rank() OVER (ORDER BY sum(v) DESC) FROM s GROUP BY g ORDER BY g",
            "window over grouped aggregate",
        )
        .await;
    });
}

#[test]
fn attached_subqueries_with_windows_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut differences = Vec::new();
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = if file_backed {
                (
                    Connection::open(dir.path().join("frank.db").to_str().unwrap())
                        .await
                        .unwrap(),
                    rusqlite::Connection::open(dir.path().join("stock.db")).unwrap(),
                )
            } else {
                (
                    Connection::open(":memory:").await.unwrap(),
                    rusqlite::Connection::open_in_memory().unwrap(),
                )
            };
            let frank_aux = if file_backed {
                dir.path().join("frank_aux.db").to_str().unwrap().to_owned()
            } else {
                ":memory:".to_owned()
            };
            let stock_aux = if file_backed {
                dir.path().join("stock_aux.db").to_str().unwrap().to_owned()
            } else {
                ":memory:".to_owned()
            };
            f.execute_with_params("ATTACH ?1 AS aux", &[SqliteValue::Text(frank_aux.into())])
                .await
                .unwrap();
            r.execute("ATTACH ?1 AS aux", [stock_aux]).unwrap();
            for sql in SETUP.iter().copied().chain([
                "CREATE TABLE aux.u(z INTEGER)",
                "INSERT INTO aux.u VALUES (10),(20),(NULL)",
                "CREATE TABLE main.u(z INTEGER)",
                "INSERT INTO main.u VALUES (999)",
            ]) {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            for sql in [
                "SELECT id, row_number() OVER (ORDER BY id) FROM s WHERE v IN (SELECT z FROM aux.u) ORDER BY id",
                "SELECT id, row_number() OVER (ORDER BY id) FROM s WHERE v IN (SELECT z FROM aux.u WHERE z = s.v) ORDER BY id",
                "SELECT id, (SELECT max(z) FROM aux.u), row_number() OVER (ORDER BY id) FROM s ORDER BY id",
                "SELECT id, (SELECT max(z) FROM aux.u WHERE z <= s.v), row_number() OVER (ORDER BY id) FROM s ORDER BY id",
                "SELECT id, EXISTS (SELECT 1 FROM aux.u WHERE z = s.v), row_number() OVER (ORDER BY id) FROM s ORDER BY id",
                "SELECT id, v NOT IN (SELECT z FROM aux.u), row_number() OVER (ORDER BY id) FROM s ORDER BY id",
                "SELECT id, row_number() OVER (ORDER BY id) + (SELECT max(z) FROM aux.u WHERE z <= s.v) FROM s ORDER BY id",
                "SELECT id, row_number() OVER (ORDER BY id) FROM s ORDER BY (SELECT max(z) FROM aux.u WHERE z <= s.v), id",
                "SELECT id, row_number() OVER (ORDER BY id) FROM s WHERE EXISTS (SELECT 1 FROM aux.u WHERE z = s.v) ORDER BY id",
                "SELECT id, (SELECT max(z) FROM aux.u WHERE z > 1000), row_number() OVER (ORDER BY id) FROM s ORDER BY id",
                "SELECT u.z, (SELECT count(*) FROM main.s WHERE s.v = u.z), row_number() OVER (ORDER BY u.z) FROM aux.u AS u ORDER BY u.z",
                "SELECT u.z, row_number() OVER (ORDER BY u.z) FROM aux.u AS u WHERE EXISTS (SELECT 1 FROM main.s WHERE s.v = u.z) ORDER BY u.z",
            ] {
                let expected = rq(&r, sql);
                for prepared in [false, true] {
                    let result = if prepared {
                        match f.prepare(sql).await {
                            Ok(statement) => statement.query().await,
                            Err(error) => Err(error),
                        }
                    } else {
                        f.query(sql).await
                    };
                    match result {
                        Ok(rows) => {
                            let actual = rows
                                .iter()
                                .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                                .collect::<Vec<_>>();
                            if actual != expected {
                                differences.push(format!(
                                    "file_backed={file_backed} prepared={prepared}: {sql}\nfrank={actual:?}\nstock={expected:?}"
                                ));
                            }
                        }
                        Err(error) => differences.push(format!(
                            "file_backed={file_backed} prepared={prepared}: {sql}\n{error}"
                        )),
                    }
                }
            }
            f.close().await.unwrap();
        }
        assert!(differences.is_empty(), "{}", differences.join("\n"));
    });
}

#[test]
fn attached_window_collations_use_the_source_schema() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "ATTACH ':memory:' AS aux;",
            "CREATE TABLE main.u(z TEXT COLLATE BINARY);",
            "INSERT INTO main.u VALUES('unrelated');",
            "CREATE TABLE aux.u(z TEXT COLLATE NOCASE);",
            "INSERT INTO aux.u VALUES('b'),('a'),('A'),('c');",
        ] {
            frank.execute(sql).await.unwrap();
            stock.execute_batch(sql).unwrap();
        }
        // The scalar probe keeps execution on the mixed-schema path. The
        // attached column's NOCASE must win over same-named main.u metadata
        // for window peers, partitions, and the final ORDER BY.
        for sql in [
            "SELECT u.z,dense_rank() OVER(ORDER BY u.z),(SELECT count(*) FROM main.u) FROM aux.u AS u ORDER BY u.z COLLATE BINARY;",
            "SELECT u.z,count(*) OVER(PARTITION BY u.z),(SELECT count(*) FROM main.u) FROM aux.u AS u ORDER BY u.z COLLATE BINARY;",
            "SELECT u.z,rank() OVER(),(SELECT count(*) FROM main.u) FROM aux.u AS u ORDER BY u.z,u.rowid;",
        ] {
            let expected = rq(&stock, sql);
            assert_eq!(fq(&frank, sql).await, expected, "direct: {sql}");
            let prepared = frank.prepare(sql).await.unwrap();
            let actual = prepared
                .query()
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "prepared: {sql}");
        }
    });
}
