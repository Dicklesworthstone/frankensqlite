#![recursion_limit = "512"]

//! bd-reality-check-1022-llk6g.3: R-tree tables use stock SQLite's file
//! format. A table FrankenSQLite creates keeps its entries in the stock
//! `_node`, `_rowid` and `_parent` shadow tables, whose rows are byte for
//! byte what stock writes for the same statements (only the shadow tables'
//! root page numbers can differ), so stock opens, queries and checks it; a
//! table stock created opens, queries and keeps growing in FrankenSQLite.
//!
//! Requires `--features ext-rtree`.

#![cfg(feature = "ext-rtree")]

use std::path::Path;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use rusqlite::types::Value;

/// xorshift64*: deterministic and dependency-free.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

#[derive(Clone, Copy, Debug)]
struct Shape {
    module: &'static str,
    dimensions: usize,
    aux: usize,
}

const SHAPES: [Shape; 3] = [
    Shape {
        module: "rtree",
        dimensions: 2,
        aux: 0,
    },
    Shape {
        module: "rtree",
        dimensions: 3,
        aux: 1,
    },
    Shape {
        module: "rtree_i32",
        dimensions: 2,
        aux: 0,
    },
];

impl Shape {
    fn create_sql(self) -> String {
        let mut columns = vec!["id".to_owned()];
        columns.extend((0..self.dimensions * 2).map(|coord| format!("c{coord}")));
        columns.extend((0..self.aux).map(|aux| format!("+tag{aux}")));
        format!(
            "CREATE VIRTUAL TABLE rt USING {}({})",
            self.module,
            columns.join(", ")
        )
    }

    /// A coordinate pair: exact binary fractions, plus decimals that stock
    /// rounds outward to `f32`, or plain integers for `rtree_i32`.
    fn pair(self, rng: &mut Rng) -> (String, String) {
        if self.module == "rtree_i32" {
            let lo = rng.below(20_000) as i64 - 10_000;
            return (lo.to_string(), (lo + rng.below(300) as i64).to_string());
        }
        if rng.below(5) == 0 {
            let lo = rng.below(20_000) as i64 - 10_000;
            return (format!("{lo}.1"), format!("{}.7", lo + 2));
        }
        let lo = (rng.below(1_000_000) as f64 - 500_000.0) / 64.0;
        let hi = lo + rng.below(20_000) as f64 / 64.0;
        (format!("{lo:?}"), format!("{hi:?}"))
    }

    fn row(self, rng: &mut Rng, id: &str) -> String {
        let mut values = vec![id.to_owned()];
        for _ in 0..self.dimensions {
            let (lo, hi) = self.pair(rng);
            values.push(lo);
            values.push(hi);
        }
        for _ in 0..self.aux {
            values.push(match rng.below(3) {
                0 => "NULL".to_owned(),
                1 => rng.below(1_000).to_string(),
                _ => format!("'tag-{}'", rng.below(1_000)),
            });
        }
        format!("({})", values.join(", "))
    }

    /// Statements that grow the tree past one level and exercise every
    /// write path: multi-row and NULL-id inserts, single and multi-row
    /// deletes, updates that keep or change the id, and INSERT OR REPLACE.
    fn script(self, seed: u64, first_id: i64, batches: usize) -> Vec<String> {
        let mut rng = Rng(seed);
        let mut statements = Vec::new();
        let mut next_id = first_id;
        for batch in 0..batches {
            let rows: Vec<String> = (0..40)
                .map(|row| {
                    let id = if row % 13 == 7 {
                        // A NULL id takes max(rowid) + 1, so the next explicit
                        // id must skip it.
                        next_id += 1;
                        "NULL".to_owned()
                    } else {
                        next_id += 1 + rng.below(3) as i64;
                        next_id.to_string()
                    };
                    self.row(&mut rng, &id)
                })
                .collect();
            statements.push(format!("INSERT INTO rt VALUES {}", rows.join(", ")));
            let victim = first_id + rng.below((next_id - first_id).max(1) as u64) as i64;
            statements.push(format!("DELETE FROM rt WHERE id = {victim}"));
            let (lo, hi) = self.pair(&mut rng);
            let target = first_id + rng.below((next_id - first_id).max(1) as u64) as i64;
            statements.push(format!(
                "UPDATE rt SET c0 = {lo}, c1 = {hi} WHERE id = {target}"
            ));
            if batch % 3 == 2 {
                next_id += 1;
                statements.push(format!(
                    "UPDATE rt SET id = {next_id} WHERE id = {}",
                    target + 1
                ));
                let row = self.row(&mut rng, &(target + 2).to_string());
                statements.push(format!("INSERT OR REPLACE INTO rt VALUES {row}"));
            }
            if batch % 4 == 3 {
                statements.push(format!(
                    "DELETE FROM rt WHERE id > {} AND id < {}",
                    next_id - 30,
                    next_id - 20
                ));
            }
        }
        statements
    }
}

fn to_stock(value: &SqliteValue) -> Value {
    match value {
        SqliteValue::Null => Value::Null,
        SqliteValue::Integer(integer) => Value::Integer(*integer),
        SqliteValue::Float(float) => Value::Real(*float),
        SqliteValue::Text(_) => Value::Text(value.as_text().unwrap_or_default().to_owned()),
        SqliteValue::Blob(blob) => Value::Blob(blob.to_vec()),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<Value>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|error| panic!("fsqlite `{sql}`: {error}"))
        .iter()
        .map(|row| row.values().iter().map(to_stock).collect())
        .collect()
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<Value>> {
    let mut statement = conn
        .prepare(sql)
        .unwrap_or_else(|error| panic!("stock `{sql}`: {error}"));
    let width = statement.column_count();
    statement
        .query_map([], |row| {
            (0..width)
                .map(|index| row.get::<_, Value>(index))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .expect("stock query")
        .collect::<rusqlite::Result<_>>()
        .expect("stock rows")
}

fn stock_scalar(conn: &rusqlite::Connection, sql: &str) -> String {
    conn.query_row(sql, [], |row| row.get::<_, String>(0))
        .unwrap_or_else(|error| panic!("stock `{sql}`: {error}"))
}

/// The shadow tables' rows and catalog entries, as stock reads them.
fn stock_image(path: &Path) -> Vec<Vec<Vec<Value>>> {
    let conn = rusqlite::Connection::open(path).expect("stock open");
    assert_eq!(
        stock_scalar(&conn, "PRAGMA integrity_check"),
        "ok",
        "{}",
        path.display()
    );
    assert_eq!(
        stock_scalar(&conn, "SELECT rtreecheck('rt')"),
        "ok",
        "{}",
        path.display()
    );
    vec![
        stock_rows(&conn, "SELECT * FROM rt_node ORDER BY nodeno"),
        stock_rows(&conn, "SELECT * FROM rt_rowid ORDER BY rowid"),
        stock_rows(&conn, "SELECT * FROM rt_parent ORDER BY nodeno"),
        // Not the shadow tables' root page numbers: from a transaction's
        // third page on, FrankenSQLite allocates from an extent it hands out
        // top down, so the three CREATE TABLEs get pages 2, 3 and 10 where
        // stock uses 2, 3 and 4, and pages 4-9 go on the freelist. Stock's
        // integrity_check above accepts both layouts.
        stock_rows(
            &conn,
            "SELECT type, name, tbl_name, sql FROM sqlite_schema \
             WHERE name LIKE 'rt\\_%' ESCAPE '\\' ORDER BY name",
        ),
        stock_rows(
            &conn,
            "SELECT type, name, rootpage FROM sqlite_schema WHERE name = 'rt'",
        ),
        stock_rows(&conn, "SELECT * FROM rt"),
    ]
}

/// Window queries over the first dimension, in the engine's natural order.
fn window_queries(shape: Shape) -> Vec<String> {
    let mut rng = Rng(0x00C0_FFEE ^ shape.dimensions as u64);
    (0..6)
        .map(|_| {
            let (lo, hi) = shape.pair(&mut rng);
            format!("SELECT * FROM rt WHERE c0 <= {hi} AND c1 >= {lo}")
        })
        .collect()
}

fn run_stock(path: &Path, statements: &[String]) {
    let conn = rusqlite::Connection::open(path).expect("stock open");
    for sql in statements {
        conn.execute_batch(sql)
            .unwrap_or_else(|error| panic!("stock `{sql}`: {error}"));
    }
}

async fn run_frank(path: &Path, statements: &[String]) {
    let conn = Connection::open(path.to_str().expect("utf-8 path"))
        .await
        .expect("fsqlite open");
    for sql in statements {
        conn.execute(sql)
            .await
            .unwrap_or_else(|error| panic!("fsqlite `{sql}`: {error}"));
    }
    conn.close().await.expect("fsqlite close");
}

#[test]
fn fsqlite_writes_the_bytes_stock_writes() {
    asupersync::test_utils::run_test(|| async {
        for (index, shape) in SHAPES.into_iter().enumerate() {
            let directory = tempfile::tempdir().unwrap();
            let ours = directory.path().join("frank.db");
            let theirs = directory.path().join("stock.db");
            let mut statements = vec![shape.create_sql()];
            statements.extend(shape.script(0x51ED_0000 + index as u64, 0, 30));

            run_frank(&ours, &statements).await;
            run_stock(&theirs, &statements);

            let (frank_image, stock_image) = (stock_image(&ours), stock_image(&theirs));
            for (part, (frank, stock)) in frank_image.iter().zip(&stock_image).enumerate() {
                assert_eq!(frank, stock, "{shape:?}: image part {part} differs");
            }
            assert!(
                frank_image[0].len() > 1,
                "{shape:?}: the tree must split ({} nodes)",
                frank_image[0].len()
            );
            eprintln!(
                "{shape:?}: {} entries, {} nodes byte-identical to stock",
                frank_image[1].len(),
                frank_image[0].len()
            );

            // Stock's queries over the FrankenSQLite file match ours.
            let stock_conn = rusqlite::Connection::open(&theirs).unwrap();
            let frank_conn = Connection::open(ours.to_str().unwrap()).await.unwrap();
            for sql in window_queries(shape) {
                assert_eq!(
                    frank_rows(&frank_conn, &sql).await,
                    stock_rows(&stock_conn, &sql),
                    "{shape:?}: {sql}"
                );
            }
            frank_conn.close().await.unwrap();
        }
    });
}

#[test]
fn fsqlite_reads_and_extends_stock_files() {
    asupersync::test_utils::run_test(|| async {
        for (index, shape) in SHAPES.into_iter().enumerate() {
            let directory = tempfile::tempdir().unwrap();
            let ours = directory.path().join("frank.db");
            let theirs = directory.path().join("stock.db");
            let mut setup = vec![shape.create_sql()];
            setup.extend(shape.script(0xA11C_E000 + index as u64, 0, 20));
            run_stock(&theirs, &setup);
            std::fs::copy(&theirs, &ours).unwrap();

            let stock_conn = rusqlite::Connection::open(&theirs).unwrap();
            let frank_conn = Connection::open(ours.to_str().unwrap()).await.unwrap();
            let mut queries = window_queries(shape);
            queries.push("SELECT * FROM rt".to_owned());
            queries.push("SELECT count(*) FROM rt".to_owned());
            queries.push("SELECT * FROM rt WHERE id = 25".to_owned());
            for sql in &queries {
                assert_eq!(
                    frank_rows(&frank_conn, sql).await,
                    stock_rows(&stock_conn, sql),
                    "{shape:?}: {sql} on a stock-written file"
                );
            }
            frank_conn.close().await.unwrap();
            drop(stock_conn);

            let more = shape.script(0xB0B0_0000 + index as u64, 10_000, 12);
            run_frank(&ours, &more).await;
            run_stock(&theirs, &more);
            assert_eq!(
                stock_image(&ours),
                stock_image(&theirs),
                "{shape:?}: extending a stock file"
            );
        }
    });
}

async fn frank_error(conn: &Connection, sql: &str) -> String {
    match conn.execute(sql).await {
        Ok(_) => panic!("fsqlite `{sql}` should fail"),
        Err(error) => error.to_string(),
    }
}

fn stock_error(conn: &rusqlite::Connection, sql: &str) -> String {
    match conn.execute_batch(sql).unwrap_err() {
        rusqlite::Error::SqliteFailure(_, Some(message)) => message,
        other => other.to_string(),
    }
}

#[test]
fn constraint_errors_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE VIRTUAL TABLE rt USING rtree(id, x0, x1, y0, y1)",
            "INSERT INTO rt VALUES (1, 0, 1, 0, 1)",
            "CREATE TABLE grid_node(x)",
        ] {
            frank.execute(sql).await.unwrap();
            stock.execute_batch(sql).unwrap();
        }
        for sql in [
            "INSERT INTO rt VALUES (2, 5, 1, 0, 1)",
            "INSERT INTO rt VALUES (2, 0, 1, 9, 1)",
            "INSERT INTO rt VALUES (1, 3, 4, 3, 4)",
            "UPDATE rt SET x0 = 7 WHERE id = 1",
            "CREATE VIRTUAL TABLE grid USING rtree_i32(id, a, b)",
        ] {
            assert_eq!(
                frank_error(&frank, sql).await,
                stock_error(&stock, sql),
                "{sql}"
            );
        }
        assert_eq!(
            frank_rows(&frank, "SELECT * FROM rt").await,
            stock_rows(&stock, "SELECT * FROM rt")
        );
        assert!(
            frank_rows(
                &frank,
                "SELECT name FROM sqlite_schema WHERE name LIKE 'grid%' AND name <> 'grid_node'"
            )
            .await
            .is_empty(),
            "a failed CREATE leaves no shadow tables"
        );
    });
}

async fn frank_count(conn: &Connection, sql: &str) -> i64 {
    match frank_rows(conn, sql).await.as_slice() {
        [row] => match row.as_slice() {
            [Value::Integer(count)] => *count,
            other => panic!("`{sql}` returned {other:?}"),
        },
        other => panic!("`{sql}` returned {other:?}"),
    }
}

#[test]
fn statements_and_transactions_roll_back_the_shadow_tables() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("txn.db");
        let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE rt USING rtree(id, x0, x1)")
            .await
            .unwrap();
        let rows: Vec<String> = (1..=200)
            .map(|id| format!("({id}, {id}, {})", id + 2))
            .collect();
        conn.execute(&format!("INSERT INTO rt VALUES {}", rows.join(", ")))
            .await
            .unwrap();
        let node_count = frank_count(&conn, "SELECT count(*) FROM rt_node").await;
        assert!(node_count > 1, "200 entries split the root");
        assert_eq!(frank_count(&conn, "SELECT count(*) FROM rt_rowid").await, 200);

        // A statement that fails on its second row keeps neither row, and the
        // transaction's earlier statement survives.
        conn.execute("BEGIN").await.unwrap();
        conn.execute("INSERT INTO rt VALUES (500, 1, 2)").await.unwrap();
        frank_error(&conn, "INSERT INTO rt VALUES (501, 1, 2), (502, 9, 1)").await;
        assert_eq!(frank_count(&conn, "SELECT count(*) FROM rt").await, 201);
        assert_eq!(frank_count(&conn, "SELECT count(*) FROM rt_rowid").await, 201);
        conn.execute("COMMIT").await.unwrap();

        // ROLLBACK and ROLLBACK TO restore the tree and its storage.
        conn.execute("BEGIN").await.unwrap();
        conn.execute("DELETE FROM rt WHERE id <= 150").await.unwrap();
        conn.execute("SAVEPOINT s").await.unwrap();
        conn.execute("DELETE FROM rt").await.unwrap();
        assert_eq!(frank_count(&conn, "SELECT count(*) FROM rt").await, 0);
        conn.execute("ROLLBACK TO s").await.unwrap();
        assert_eq!(frank_count(&conn, "SELECT count(*) FROM rt").await, 51);
        conn.execute("ROLLBACK").await.unwrap();
        assert_eq!(frank_count(&conn, "SELECT count(*) FROM rt").await, 201);
        assert_eq!(frank_count(&conn, "SELECT count(*) FROM rt_rowid").await, 201);
        // Ids 9 and 10 cover 10.5.
        let window = "SELECT count(*) FROM rt WHERE x0 <= 10.5 AND x1 >= 10.5";
        assert_eq!(frank_count(&conn, window).await, 2);
        conn.close().await.unwrap();

        let stock = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(stock_scalar(&stock, "PRAGMA integrity_check"), "ok");
        assert_eq!(stock_scalar(&stock, "SELECT rtreecheck('rt')"), "ok");
        assert_eq!(
            stock_rows(&stock, "SELECT count(*) FROM rt"),
            vec![vec![Value::Integer(201)]]
        );
        assert_eq!(stock_rows(&stock, window), vec![vec![Value::Integer(2)]]);
        drop(stock);

        let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
        conn.execute("DROP TABLE rt").await.unwrap();
        assert!(
            frank_rows(&conn, "SELECT name FROM sqlite_schema WHERE name LIKE 'rt%'")
                .await
                .is_empty(),
            "DROP TABLE removes the shadow tables"
        );
        conn.close().await.unwrap();
    });
}

#[test]
fn peer_connections_see_each_others_commits() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("peers.db");
        let path = path.to_str().unwrap();
        let first = Connection::open(path).await.unwrap();
        first
            .execute("CREATE VIRTUAL TABLE rt USING rtree(id, x0, x1)")
            .await
            .unwrap();
        first
            .execute("INSERT INTO rt VALUES (1, 0, 1), (2, 5, 6)")
            .await
            .unwrap();
        let second = Connection::open(path).await.unwrap();
        assert_eq!(frank_count(&second, "SELECT count(*) FROM rt").await, 2);

        let rows: Vec<String> = (10..120)
            .map(|id| format!("({id}, {id}, {})", id + 1))
            .collect();
        first
            .execute(&format!("INSERT INTO rt VALUES {}", rows.join(", ")))
            .await
            .unwrap();
        assert_eq!(frank_count(&second, "SELECT count(*) FROM rt").await, 112);
        second.execute("DELETE FROM rt WHERE id = 2").await.unwrap();
        second
            .execute("INSERT INTO rt VALUES (500, 50, 51)")
            .await
            .unwrap();
        // Id 50 and the second connection's id 500 cover 50.5.
        let window = "SELECT count(*) FROM rt WHERE x0 <= 50.5 AND x1 >= 50.5";
        assert_eq!(frank_count(&first, window).await, 2);
        assert_eq!(frank_count(&first, "SELECT count(*) FROM rt").await, 112);
        assert_eq!(
            frank_count(&first, "SELECT count(*) FROM rt WHERE id = 500").await,
            1
        );
        first.close().await.unwrap();
        second.close().await.unwrap();

        let stock = rusqlite::Connection::open(path).unwrap();
        assert_eq!(stock_scalar(&stock, "PRAGMA integrity_check"), "ok");
        assert_eq!(stock_scalar(&stock, "SELECT rtreecheck('rt')"), "ok");
        assert_eq!(stock_rows(&stock, window), vec![vec![Value::Integer(2)]]);
    });
}
