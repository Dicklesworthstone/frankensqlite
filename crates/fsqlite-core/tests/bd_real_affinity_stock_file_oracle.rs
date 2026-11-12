#![recursion_limit = "512"]

//! bd-real-affinity-on-read-lsjei: stock stores an integral value of a
//! REAL-affinity column as an integer in the record, which is smaller, and
//! turns it back into a real when the column is read (`OP_RealAffinity`).
//! FrankenSQLite must do the same when it reads a file stock wrote, or
//! `291.0 / 2` answers 145.
//!
//! Stock (bundled rusqlite) builds the file. FrankenSQLite reads one copy and
//! stock reads another, and every answer must be equal. The reads cover
//! `typeof`, arithmetic, text conversion, comparisons, ORDER BY, GROUP BY,
//! aggregates, index scans, index-only reads, a WITHOUT ROWID table, an ALTER
//! TABLE default, joins, subqueries, views, windows, compounds and prepared
//! parameters. The writes cover UPDATE, INSERT ... SELECT, a trigger's `old.*`
//! and an index-driven DELETE, after which stock checks the FrankenSQLite copy
//! with `PRAGMA integrity_check`.
//!
//! The negatives are what a naive conversion gets wrong. A NUMERIC column and
//! a `FLOATING POINT` column, whose name gives it INTEGER affinity, keep their
//! integers. TEXT and BLOB values in a REAL column stay as they are. The
//! integer primary key stays an integer.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
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
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
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

async fn frank_rows(f: &Connection, sql: &str, params: &[SqliteValue]) -> Vec<Vec<String>> {
    let result = if params.is_empty() {
        f.query(sql).await
    } else {
        f.query_with_params(sql, params).await
    };
    match result {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e}>")]],
    }
}

fn stock_rows(r: &rusqlite::Connection, sql: &str, params: &[SqliteValue]) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    let params: Vec<rusqlite::types::Value> = params.iter().map(to_rusqlite).collect();
    st.query_map(rusqlite::params_from_iter(params), |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

/// Built by stock. `c`, `f` and `d` have REAL affinity; `n` NUMERIC and `fp`
/// INTEGER (its type name contains "INT"). Integral reals within 2^47 are
/// stored as integers, larger ones as 8-byte reals; '12' becomes 12.0 and is
/// stored as the integer 12, 'abc' and x'01' stay TEXT and BLOB.
const SETUP: &str = "
CREATE TABLE r(id INTEGER PRIMARY KEY, c REAL, n NUMERIC, f FLOAT, d DOUBLE PRECISION,
               fp FLOATING POINT, t TEXT);
INSERT INTO r VALUES
  (1, 291.0, 5.0, 2.0, 3.0, 4.0, 'a'),
  (2, 1.5, 2.5, 0.5, -1.0, 2.5, 'b'),
  (3, 7, 7, 7, 7, 7, 'c'),
  (4, -3.0, -3.0, -3, 0, -3.0, 'd'),
  (5, 0.0, 0, 0.0, 0, 0.0, 'e'),
  (6, 140737488355327.0, 140737488355327.0, -140737488355328.0, 1e3, 9.0, 'f'),
  (7, 140737488355328.0, 1e300, 9007199254740993, 2.5e-3, 1e18, 'g'),
  (8, NULL, NULL, NULL, NULL, NULL, 'h'),
  (9, '12', '12', '12.0', ' 13 ', '14', 'i'),
  (10, 'abc', 'abc', x'01', '', 'xyz', 'j'),
  (11, 7.0, 6.0, 7.5, 8, 9, 'k'),
  (12, 1000000, 1000000, 99, -42, 1e15, 'l');
CREATE INDEX r_c ON r(c);
CREATE INDEX r_fc ON r(f, c);
CREATE TABLE w(k REAL PRIMARY KEY, v REAL, s TEXT) WITHOUT ROWID;
INSERT INTO w VALUES (3, 1.5, 'x'), (4.0, 8, 'y'), (2.5, 2, 'z'), (7, 291, 'q'), (-3, 0, 'm');
ALTER TABLE r ADD COLUMN e REAL DEFAULT 4;
ALTER TABLE r ADD COLUMN g NUMERIC DEFAULT 4.0;
INSERT INTO r(id, c, n, f, d, fp, t, e, g) VALUES (13, 13, 13, 13, 13, 13, 'm', 6, 6.0);
CREATE TABLE log(id INTEGER, half, kind TEXT);
CREATE VIEW rv AS SELECT id, c, c / 2 AS half FROM r;
";

const QUERIES: [&str; 32] = [
    "SELECT id, c, typeof(c), n, typeof(n), f, typeof(f), d, typeof(d), fp, typeof(fp) FROM r ORDER BY id",
    "SELECT id, c / 2, typeof(c + 0), f * 3, d - 1, n / 2, fp / 2 FROM r WHERE id <= 7 OR id >= 11 ORDER BY id",
    "SELECT * FROM r ORDER BY id",
    "SELECT id, c || '', printf('%s', c), quote(c), CAST(c AS TEXT), json_quote(c) FROM r ORDER BY id",
    "SELECT json_group_array(c), json_group_array(f), json_group_array(d) FROM r",
    "SELECT id, typeof(c) FROM r WHERE c > 6 ORDER BY c",
    "SELECT c, typeof(c) FROM r WHERE c > 0 ORDER BY c",
    "SELECT c FROM r WHERE c BETWEEN -5 AND 300 ORDER BY c DESC",
    "SELECT f, c, typeof(f), typeof(c) FROM r WHERE f >= 0 ORDER BY f, c",
    "SELECT count(*) FROM r WHERE c = 7",
    "SELECT id FROM r WHERE c = 7.0 ORDER BY id",
    "SELECT id, c / 2 FROM r WHERE c = '7' ORDER BY id",
    "SELECT sum(c), total(c), avg(c), min(c), max(c), typeof(sum(c)), typeof(min(c)), count(c) FROM r WHERE id <> 10",
    "SELECT min(c), max(c), typeof(min(c)), typeof(max(c)) FROM r",
    "SELECT sum(n), typeof(sum(n)), sum(fp), typeof(sum(fp)), sum(f), typeof(sum(f)) FROM r WHERE id IN (1, 3, 4, 5, 11, 12)",
    "SELECT c, count(*), typeof(c) FROM r GROUP BY c ORDER BY c",
    "SELECT typeof(c), count(*), sum(c) FROM r GROUP BY typeof(c) ORDER BY 1",
    "SELECT DISTINCT typeof(c), typeof(f) FROM r ORDER BY 1, 2",
    "SELECT id, e, typeof(e), e / 3, g, typeof(g) FROM r ORDER BY id",
    "SELECT k, typeof(k), v, typeof(v), v / 2, s FROM w ORDER BY k",
    "SELECT k, v / 4 FROM w WHERE k = 3",
    "SELECT r.id, r.c / 2, w.v / 2, typeof(w.k) FROM r JOIN w ON w.k = r.c ORDER BY r.id",
    "SELECT id, (SELECT v / 2 FROM w WHERE w.k = r.c) FROM r ORDER BY id",
    "SELECT id FROM r WHERE c IN (SELECT k FROM w) ORDER BY id",
    "SELECT (SELECT c FROM r WHERE id = 1) / 2, (SELECT max(c) FROM r WHERE id < 9) / 2",
    "SELECT c / 2, typeof(c) FROM r WHERE id = 3",
    "SELECT id, half, typeof(c) FROM rv WHERE id < 9 ORDER BY id",
    "WITH x AS (SELECT id, c FROM r WHERE id IN (1, 3, 11)) SELECT id, c / 2, typeof(c) FROM x ORDER BY id",
    "SELECT id, c / 2, sum(c) OVER (ORDER BY id), typeof(sum(c) OVER (ORDER BY id)) FROM r WHERE id < 8 ORDER BY id",
    "SELECT c FROM r WHERE id IN (1, 3) UNION ALL SELECT v FROM w ORDER BY 1",
    "SELECT c FROM r WHERE id IN (1, 3) UNION SELECT k FROM w ORDER BY 1",
    "SELECT id, c / 2 FROM r ORDER BY c / 2 DESC, id LIMIT 4",
];

/// Prepared reads with parameters: a rowid point lookup, an index seek and a
/// WITHOUT ROWID primary-key seek.
const PARAM_QUERIES: [(&str, i64); 6] = [
    ("SELECT c / 2, typeof(c), f / 2 FROM r WHERE id = ?1", 1),
    ("SELECT c / 2, typeof(c), f / 2 FROM r WHERE id = ?1", 3),
    ("SELECT id, c / 2 FROM r WHERE c = ?1 ORDER BY id", 7),
    ("SELECT id, c FROM r WHERE c >= ?1 ORDER BY c", 291),
    ("SELECT v / 2, typeof(k) FROM w WHERE k = ?1", 4),
    ("SELECT count(*), sum(c) FROM r WHERE c < ?1", 300),
];

/// Writes that read a REAL column as they go. Each runs on both copies.
const MUTATIONS: [&str; 6] = [
    "UPDATE r SET t = c / 2 WHERE id <= 4",
    "INSERT INTO log SELECT id, c / 2, typeof(c) FROM r WHERE id < 9",
    "UPDATE r SET n = n + 1 WHERE id = 3",
    "CREATE TRIGGER r_t AFTER UPDATE OF t ON r BEGIN \
       INSERT INTO log VALUES (old.id, old.c / 2, typeof(old.c) || '/' || typeof(new.f)); \
     END; \
     UPDATE r SET t = 'z' WHERE id IN (1, 3, 11, 12)",
    "DELETE FROM r WHERE c = 7",
    "UPDATE w SET s = v / 2 WHERE k > 2; DELETE FROM w WHERE k = 4",
];

const AFTER_QUERIES: [&str; 5] = [
    "SELECT id, c, typeof(c), t, n, typeof(n) FROM r ORDER BY id",
    "SELECT id, half, kind FROM log ORDER BY rowid",
    "SELECT k, v, s, typeof(k) FROM w ORDER BY k",
    "SELECT c, typeof(c) FROM r WHERE c > 0 ORDER BY c",
    "SELECT f, c FROM r WHERE f >= 0 ORDER BY f, c",
];

async fn compare(
    label: &str,
    frank: &Connection,
    stock: &rusqlite::Connection,
    query: &str,
    params: &[SqliteValue],
    failures: &mut Vec<String>,
) {
    let (f, s) = (
        frank_rows(frank, query, params).await,
        stock_rows(stock, query, params),
    );
    if f != s {
        failures.push(format!(
            "{label}: `{query}` {params:?}:\n  frank {f:?}\n  stock {s:?}"
        ));
    }
}

#[test]
fn real_columns_of_a_stock_written_file_read_back_as_reals() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let seed = dir.path().join("seed.db");
        rusqlite::Connection::open(&seed)
            .expect("stock seed open")
            .execute_batch(SETUP)
            .expect("stock seed setup");
        let frank_path = dir.path().join("frank.db");
        let stock_path = dir.path().join("stock.db");
        std::fs::copy(&seed, &frank_path).expect("copy seed for frank");
        std::fs::copy(&seed, &stock_path).expect("copy seed for stock");

        let stock = rusqlite::Connection::open(&stock_path).expect("stock open");
        let frank = Connection::open(frank_path.to_str().expect("utf-8 path"))
            .await
            .expect("open stock-written file");

        let mut failures = Vec::new();
        for query in QUERIES {
            compare("loaded", &frank, &stock, query, &[], &mut failures).await;
        }
        for (query, param) in PARAM_QUERIES {
            let params = [SqliteValue::Integer(param)];
            compare("prepared", &frank, &stock, query, &params, &mut failures).await;
        }
        for (step, mutation) in MUTATIONS.iter().enumerate() {
            if let Err(error) = frank.execute_batch(mutation).await {
                failures.push(format!("mutation {step} `{mutation}` failed on frank: {error}"));
            }
            stock.execute_batch(mutation).expect("stock mutation");
            let label = format!("after mutation {step}");
            for query in AFTER_QUERIES {
                compare(&label, &frank, &stock, query, &[], &mut failures).await;
            }
        }
        for query in QUERIES {
            compare("after all mutations", &frank, &stock, query, &[], &mut failures).await;
        }
        frank.close().await.expect("close");

        let check: String = rusqlite::Connection::open(&frank_path)
            .expect("stock opens the FrankenSQLite copy")
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("integrity_check");
        if check != "ok" {
            failures.push(format!("stock integrity_check of the FrankenSQLite copy: {check}"));
        }

        assert!(
            failures.is_empty(),
            "{} answers differ from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}
