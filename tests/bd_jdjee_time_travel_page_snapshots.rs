#![recursion_limit = "512"]

//! bd-jdjee: `:memory:` time-travel snapshots are committed page images,
//! shared page-by-page between snapshots and decoded only when a historical
//! query reads them.
//!
//! Capture used to reload the whole row mirror from the pager and deep-clone
//! the MemDatabase at every DDL and explicit COMMIT, so one commit on a
//! database of a few million rows cost 0.4-1.4 s. These keepers check that
//! historical reads still match a shadow stock SQLite stopped at each commit
//! boundary, across DML, DDL, ROLLBACK, ALTER TABLE, WITHOUT ROWID and TEMP
//! tables. They also check that commits do not re-hydrate rows they did not
//! touch, counted with `memdb_row_hydration_count`, not timed.
//!
//! Historical queries here list rows rather than aggregate them: aggregates
//! over a time-travel snapshot are wrong independently of how snapshots are
//! captured (`count(*)` over an empty snapshot table returns no row, and over
//! a TEMP table one NULL per row; filed separately), so they cannot serve as
//! this keeper's oracle. Neither can a TEMP table that shadows a main table:
//! the executor returns no rows for it, from the old capture and the new one
//! alike (filed separately).

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

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

fn render_stock(conn: &rusqlite::Connection, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).expect("stock prepare");
    let columns = stmt.column_count();
    stmt.query_map([], |row| {
        let mut cells = Vec::with_capacity(columns);
        for i in 0..columns {
            cells.push(match row.get_ref(i)? {
                rusqlite::types::ValueRef::Null => "null".to_owned(),
                rusqlite::types::ValueRef::Integer(v) => format!("i:{v}"),
                rusqlite::types::ValueRef::Real(v) => format!("r:{v}"),
                rusqlite::types::ValueRef::Text(v) => {
                    format!("t:{}", String::from_utf8_lossy(v))
                }
                rusqlite::types::ValueRef::Blob(v) => format!("b:{v:?}"),
            });
        }
        Ok(cells.join("|"))
    })
    .expect("stock query")
    .collect::<Result<Vec<_>, _>>()
    .expect("stock rows")
}

/// Commit seqs that currently resolve to a snapshot, in order. Probing
/// avoids assuming how many sequence numbers each statement consumes.
async fn snapshot_seqs(conn: &Connection, probe_table: &str, upto: u64) -> Vec<u64> {
    let mut seqs = Vec::new();
    for seq in 1..=upto {
        let sql = format!("SELECT count(*) FROM {probe_table} FOR SYSTEM_TIME AS OF COMMITSEQ {seq}");
        if conn.query(&sql).await.is_ok() {
            seqs.push(seq);
        }
    }
    seqs
}

const BIG_ROWS: u32 = 3000;

fn fill_big_sql(rows: u32) -> String {
    format!(
        "INSERT INTO big WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {rows}) \
         SELECT x, 'row-' || x || '-padding-padding-padding-padding', x % 97 FROM c"
    )
}

/// One commit boundary: the statements that end in a snapshot, and the
/// historical queries valid against it (stock answers them from its shadow).
struct Boundary {
    statements: Vec<String>,
    queries: Vec<&'static str>,
}

const BIG_QUERIES: &[&str] = &[
    "SELECT id, a, b FROM big ORDER BY id",
    "SELECT id, a, b FROM big WHERE id % 499 = 0 ORDER BY id",
];

fn boundaries() -> Vec<Boundary> {
    let s = |sql: &str| sql.to_owned();
    vec![
        Boundary {
            statements: vec![s("CREATE TABLE big (id INTEGER PRIMARY KEY, a TEXT, b INT)")],
            queries: vec!["SELECT id FROM big"],
        },
        Boundary {
            statements: vec![s("BEGIN"), fill_big_sql(BIG_ROWS), s("COMMIT")],
            queries: BIG_QUERIES.to_vec(),
        },
        Boundary {
            statements: vec![s("CREATE TABLE small (k TEXT PRIMARY KEY, v) WITHOUT ROWID")],
            queries: BIG_QUERIES.to_vec(),
        },
        Boundary {
            statements: vec![
                s("BEGIN"),
                s("INSERT INTO small VALUES ('a', 1), ('b', 2)"),
                s("UPDATE big SET b = b + 1000 WHERE id % 10 = 0"),
                s("COMMIT"),
            ],
            queries: [BIG_QUERIES, &["SELECT k, v FROM small ORDER BY k"]].concat(),
        },
        Boundary {
            statements: vec![
                s("BEGIN"),
                s("DELETE FROM big WHERE id > 2500"),
                s("INSERT INTO big VALUES (99999, 'tail', 7)"),
                s("COMMIT"),
                // A rolled-back transaction leaves no snapshot and no trace.
                s("BEGIN"),
                s("DELETE FROM big"),
                s("INSERT INTO small VALUES ('z', 26)"),
                s("ROLLBACK"),
            ],
            queries: [BIG_QUERIES, &["SELECT k, v FROM small ORDER BY k"]].concat(),
        },
        Boundary {
            statements: vec![
                s("BEGIN"),
                s("UPDATE small SET v = v * 10"),
                s("COMMIT"),
            ],
            queries: [BIG_QUERIES, &["SELECT k, v FROM small ORDER BY k"]].concat(),
        },
        Boundary {
            statements: vec![s("ALTER TABLE big ADD COLUMN c TEXT DEFAULT 'dflt'")],
            queries: [
                BIG_QUERIES,
                &["SELECT id, c FROM big WHERE id IN (1, 5, 6, 99999) ORDER BY id"],
            ]
            .concat(),
        },
        Boundary {
            statements: vec![
                s("BEGIN"),
                s("UPDATE big SET c = 'set' WHERE id <= 5"),
                s("COMMIT"),
            ],
            queries: [
                BIG_QUERIES,
                &["SELECT id, c FROM big WHERE id IN (1, 5, 6, 99999) ORDER BY id"],
            ]
            .concat(),
        },
    ]
}

#[test]
fn historical_reads_match_stock_at_every_commit_boundary() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        let plan = boundaries();
        let mut shadows = Vec::new();
        for (index, boundary) in plan.iter().enumerate() {
            for sql in &boundary.statements {
                conn.execute(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
            }
            // Stock frozen at this boundary: replay everything up to it.
            shadows.push(stock_through(&plan[..=index]));
        }
        let stock = stock_through(&plan);

        let seqs = snapshot_seqs(&conn, "big", 64).await;
        assert_eq!(
            seqs.len(),
            plan.len(),
            "one snapshot per DDL/COMMIT boundary, none for the ROLLBACK (seqs {seqs:?})"
        );
        for ((boundary, shadow), seq) in plan.iter().zip(&shadows).zip(&seqs) {
            for query in &boundary.queries {
                let historical = historical_sql(query, *seq);
                // Ask twice: the second read is served from the decoded cache.
                for attempt in 0..2 {
                    let got = render_fsqlite(
                        &conn
                            .query(&historical)
                            .await
                            .unwrap_or_else(|e| panic!("{historical}: {e}")),
                    );
                    assert_eq!(
                        got,
                        render_stock(shadow, query),
                        "{historical} (attempt {attempt}) must match stock frozen at that commit"
                    );
                }
            }
        }

        // Historical reads leave the live state alone.
        for query in BIG_QUERIES {
            let got = render_fsqlite(&conn.query(query).await.expect("live query"));
            assert_eq!(got, render_stock(&stock, query), "live {query}");
        }
    });
}

/// Stock SQLite after running every statement of `boundaries`.
fn stock_through(boundaries: &[Boundary]) -> rusqlite::Connection {
    let stock = rusqlite::Connection::open_in_memory().expect("stock");
    for sql in boundaries.iter().flat_map(|boundary| &boundary.statements) {
        stock
            .execute_batch(sql)
            .unwrap_or_else(|e| panic!("stock {sql}: {e}"));
    }
    stock
}

/// Attach `FOR SYSTEM_TIME AS OF COMMITSEQ seq` to the single table a query
/// reads.
fn historical_sql(query: &str, seq: u64) -> String {
    let clause = format!(" FOR SYSTEM_TIME AS OF COMMITSEQ {seq}");
    for table in ["big", "small"] {
        let needle = format!("FROM {table}");
        if let Some(pos) = query.find(&needle) {
            let end = pos + needle.len();
            return format!("{}{clause}{}", &query[..end], &query[end..]);
        }
    }
    panic!("query reads no known table: {query}");
}

#[test]
fn temp_table_history_is_kept_with_the_snapshot() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, a TEXT, b INT)")
            .await
            .expect("create");
        conn.execute("CREATE TEMP TABLE scratch (x)").await.expect("temp");
        conn.execute("BEGIN").await.expect("begin");
        conn.execute("INSERT INTO scratch VALUES (1), (2), (3)")
            .await
            .expect("temp rows");
        conn.execute(&fill_big_sql(500)).await.expect("fill");
        conn.execute("COMMIT").await.expect("commit");
        conn.execute("BEGIN").await.expect("begin");
        conn.execute("DELETE FROM scratch WHERE x > 1")
            .await
            .expect("temp delete");
        conn.execute("DELETE FROM big WHERE id > 100").await.expect("delete");
        conn.execute("COMMIT").await.expect("commit");

        let seqs = snapshot_seqs(&conn, "big", 32).await;
        let filled = seqs[seqs.len() - 2];
        let trimmed = seqs[seqs.len() - 1];
        let rows = |sql: String| {
            let conn = &conn;
            async move {
                render_fsqlite(&conn.query(&sql).await.unwrap_or_else(|e| panic!("{sql}: {e}")))
            }
        };
        assert_eq!(
            rows(format!("SELECT x FROM scratch FOR SYSTEM_TIME AS OF COMMITSEQ {filled} ORDER BY x")).await,
            vec!["i:1", "i:2", "i:3"]
        );
        assert_eq!(
            rows(format!("SELECT x FROM scratch FOR SYSTEM_TIME AS OF COMMITSEQ {trimmed} ORDER BY x")).await,
            vec!["i:1"]
        );
        assert_eq!(
            rows(format!("SELECT id FROM big FOR SYSTEM_TIME AS OF COMMITSEQ {filled}")).await.len(),
            500
        );
        assert_eq!(
            rows(format!("SELECT id FROM big FOR SYSTEM_TIME AS OF COMMITSEQ {trimmed}")).await.len(),
            100
        );
        assert_eq!(rows("SELECT x FROM scratch ORDER BY x".to_owned()).await, vec!["i:1"]);
    });
}

#[test]
fn commits_do_not_rehydrate_rows_they_did_not_touch() {
    asupersync::test_utils::run_test(|| async {
        const BIG: u32 = 5000;
        const COMMITS: i64 = 20;
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, a TEXT, b INT)")
            .await
            .expect("create big");
        conn.execute("CREATE TABLE small (x INTEGER)").await.expect("create small");
        conn.execute("BEGIN").await.expect("begin");
        conn.execute(&fill_big_sql(BIG)).await.expect("fill");
        conn.execute("COMMIT").await.expect("commit");
        // Settle the mirror once so only the loop below is measured.
        conn.query("SELECT count(*) FROM big").await.expect("settle");

        let before = conn.memdb_row_hydration_count();
        for i in 0..COMMITS {
            conn.execute("BEGIN").await.expect("begin");
            conn.execute(&format!("INSERT INTO small VALUES ({i})"))
                .await
                .expect("insert");
            conn.execute("COMMIT").await.expect("commit");
        }
        let hydrated = conn.memdb_row_hydration_count() - before;
        // The old capture re-hydrated all of `big` at every one of these
        // commits (20 x 5000 rows). Capture now reads pages, not rows.
        assert!(
            hydrated < u64::from(BIG),
            "{COMMITS} commits into `small` hydrated {hydrated} rows; capture must not re-decode `big`"
        );

        // And every one of those snapshots is still exact.
        let seqs = snapshot_seqs(&conn, "small", 64).await;
        let loop_seqs = &seqs[seqs.len() - usize::try_from(COMMITS).unwrap()..];
        for (i, seq) in loop_seqs.iter().enumerate() {
            let sql = format!("SELECT x FROM small FOR SYSTEM_TIME AS OF COMMITSEQ {seq} ORDER BY x");
            let got = render_fsqlite(&conn.query(&sql).await.expect("historical"));
            let expected: Vec<String> = (0..=i).map(|x| format!("i:{x}")).collect();
            assert_eq!(got, expected, "{sql}");
            let sql = format!("SELECT id FROM big FOR SYSTEM_TIME AS OF COMMITSEQ {seq}");
            let got = conn.query(&sql).await.expect("historical big");
            assert_eq!(got.len(), usize::try_from(BIG).unwrap(), "{sql}");
        }
    });
}
