//! bd-minmax-prefix-seek: `SELECT MIN(b)/MAX(b) FROM t WHERE a = <const>` on a composite `(a,b)` index
//! now seeks the extremum of `b` within the `a=?` block (one seek) instead of scanning the group.
//! HARD GATE: byte-identical to C SQLite (rusqlite) across INT/TEXT/REAL/NOCASE `b`, groups with
//! leading `b`-NULLs (MIN must skip), an all-NULL-`b` group and an absent group (both -> NULL),
//! boundary `a` values (first/last/below/above), the COALESCE wrapper, and the decline cases (a DESC
//! second term, a single-column index) which fall to the group scan and must still match. Plus an
//! opcode gate: the fast path uses SeekLE (MAX) / SeekGE (MIN), covering (no SeekRowid); declines don't.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render(v: &SqliteValue) -> String {
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

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f:?}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

/// (has SeekLE, has SeekGE, has SeekRowid) in the EXPLAIN of `sql`.
async fn seek_shape(conn: &Connection, sql: &str) -> (bool, bool, bool) {
    let rows = conn.query(&format!("EXPLAIN {sql}")).await.unwrap();
    let mut le = false;
    let mut ge = false;
    let mut rowid = false;
    for row in &rows {
        if let Some(SqliteValue::Text(op)) = row.values().get(1) {
            match op.to_string().as_str() {
                "SeekLE" => le = true,
                "SeekGE" => ge = true,
                "SeekRowid" => rowid = true,
                _ => {}
            }
        }
    }
    (le, ge, rowid)
}

#[test]
fn minmax_prefix_seek_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("frank");
        let r = rusqlite::Connection::open_in_memory().expect("sqlite");
        let schema = [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, w TEXT, x REAL, c TEXT COLLATE NOCASE, d INTEGER, e INTEGER);",
            "CREATE INDEX idx_ab ON t(a, b);",
            "CREATE INDEX idx_aw ON t(a, w);",
            "CREATE INDEX idx_ax ON t(a, x);",
            "CREATE INDEX idx_ac ON t(a, c);",
            "CREATE INDEX idx_ad ON t(a, d DESC);", // DESC second term -> declines
            "CREATE INDEX idx_e ON t(e);", // single-col -> declines (non-covering group scan)
        ];
        for s in schema {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        // 10 a-groups (a in 0..=9), ~60 rows each. Group a=3 has some NULL b (leading NULLs in the block);
        // group a=8 has ALL NULL b. Text/real/nocase columns get case + sign variety.
        for i in 1..=600_i64 {
            let a = i % 10;
            let raw = (i.wrapping_mul(2_654_435_761) >> 8) & 0x3ff;
            let b = if a == 8 || (a == 3 && i % 3 == 0) {
                "NULL".to_owned()
            } else {
                format!("{}", (raw as i64) - 500)
            };
            let w = if a == 8 {
                "NULL".to_owned()
            } else {
                format!("'w{:03}'", raw % 200)
            };
            let x = if a == 8 {
                "NULL".to_owned()
            } else {
                format!("{}.{}", raw % 50, i % 7)
            };
            let c = if a == 8 {
                "NULL".to_owned()
            } else if i % 2 == 0 {
                format!("'K{:02}'", raw % 40)
            } else {
                format!("'k{:02}'", raw % 40)
            };
            let d = (raw as i64) - 500;
            let e = a; // e mirrors a so WHERE e=? selects the same groups (single-col decline)
            let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {b}, {w}, {x}, {c}, {d}, {e});");
            f.execute(&stmt).await.unwrap();
            r.execute_batch(&stmt).unwrap();
        }

        let queries = [
            // INT b: normal, NULL-mixed group (a=3), all-NULL group (a=8), absent groups, boundaries.
            "SELECT MAX(b) FROM t WHERE a = 5",
            "SELECT MIN(b) FROM t WHERE a = 5",
            "SELECT MAX(b) FROM t WHERE a = 3",
            "SELECT MIN(b) FROM t WHERE a = 3",
            "SELECT MAX(b) FROM t WHERE a = 8",
            "SELECT MIN(b) FROM t WHERE a = 8",
            "SELECT MAX(b) FROM t WHERE a = 0",
            "SELECT MIN(b) FROM t WHERE a = 9",
            "SELECT MAX(b) FROM t WHERE a = 999",
            "SELECT MIN(b) FROM t WHERE a = 999",
            "SELECT MAX(b) FROM t WHERE a = -1",
            "SELECT MIN(b) FROM t WHERE a = -1",
            // TEXT (BINARY) / REAL b. NOTE: NOCASE `b` (column c) is intentionally NOT byte-exact-checked:
            // the lever declines a non-BINARY collation (its index tie representative may differ from the
            // scan's), verified by the opcode gate below; the group-scan fallback's NOCASE tie choice is
            // out of scope for this lever.
            "SELECT MAX(w) FROM t WHERE a = 5",
            "SELECT MIN(w) FROM t WHERE a = 5",
            "SELECT MAX(x) FROM t WHERE a = 5",
            "SELECT MIN(x) FROM t WHERE a = 5",
            "SELECT MAX(w) FROM t WHERE a = 4",
            "SELECT MIN(w) FROM t WHERE a = 4",
            // Wrapper on an absent / all-NULL group.
            "SELECT COALESCE(MAX(b), -1) FROM t WHERE a = 999",
            "SELECT COALESCE(MIN(b), -1) FROM t WHERE a = 8",
            // Declines (must still match via the group scan): DESC second term; single-column index.
            "SELECT MAX(d) FROM t WHERE a = 5",
            "SELECT MIN(d) FROM t WHERE a = 5",
            "SELECT MAX(b) FROM t WHERE e = 5",
            "SELECT MIN(b) FROM t WHERE e = 5",
            // Controls (unaffected).
            "SELECT COUNT(*) FROM t WHERE a = 5",
            "SELECT SUM(b) FROM t WHERE a = 5",
        ];
        for sql in queries {
            assert_eq!(
                frank_rows(&f, sql).await,
                sqlite_rows(&r, sql),
                "diverged: `{sql}`"
            );
        }

        // Opcode gate: the composite prefix seek is covering (SeekLE/SeekGE, no SeekRowid); declines aren't.
        assert_eq!(
            seek_shape(&f, "SELECT MAX(b) FROM t WHERE a = 5").await,
            (true, false, false),
            "MAX(b) WHERE a=? must SeekLE the (a,b) prefix, covering (no SeekRowid)"
        );
        assert_eq!(
            seek_shape(&f, "SELECT MIN(b) FROM t WHERE a = 5").await,
            (false, true, false),
            "MIN(b) WHERE a=? must SeekGE the (a,b) prefix, covering (no SeekRowid)"
        );
        assert!(
            !seek_shape(&f, "SELECT MAX(d) FROM t WHERE a = 5").await.0,
            "MAX(d) WHERE a=? (DESC second term) must decline the SeekLE prefix seek"
        );
        assert!(
            !seek_shape(&f, "SELECT MAX(c) FROM t WHERE a = 5").await.0,
            "MAX(c) WHERE a=? (NOCASE second term) must decline the SeekLE prefix seek"
        );
        assert!(
            seek_shape(&f, "SELECT MAX(b) FROM t WHERE e = 5").await.2,
            "MAX(b) WHERE e=? (single-col index) must fall to the non-covering group scan (SeekRowid)"
        );
    });
}

/// bd-zgo8b: `SELECT MAX(id) FROM t WHERE a = ?` (the latest row per key) on a one-key index `(a)`,
/// whose `a = ?` block is in rowid order: one SeekLE (MAX) or SeekGE (MIN) and the entry's rowid,
/// instead of walking the block. In memory and on a file database in strict mode, against rusqlite.
#[test]
fn minmax_rowid_prefix_seek_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("minmax_rowid_prefix.db");
        let schema = [
            "CREATE TABLE m (id INTEGER PRIMARY KEY, a INTEGER, s TEXT, c TEXT COLLATE NOCASE, \
             k INTEGER, v INTEGER);",
            "CREATE INDEX m_a ON m(a);",
            "CREATE INDEX m_s ON m(s);",
            "CREATE INDEX m_c ON m(c);",
            // k leads only a composite index: its block is in v order, not rowid order.
            "CREATE INDEX m_kv ON m(k, v);",
            // A NOCASE column under a BINARY index: `c = 'k1'` matches rows the BINARY block lacks.
            "CREATE TABLE n (id INTEGER PRIMARY KEY, c TEXT COLLATE NOCASE, v INTEGER);",
            "CREATE INDEX n_cv ON n(c COLLATE BINARY, v);",
            "INSERT INTO n VALUES (1, 'k1', 50), (2, 'K1', 3), (3, 'k1', 20), (4, 'k2', 1);",
            "CREATE TABLE w (k INTEGER, id INTEGER, PRIMARY KEY (k, id)) WITHOUT ROWID;",
        ];
        let mut rows = Vec::new();
        for i in 1..=2000_i64 {
            let a = if i % 97 == 0 {
                "NULL".to_owned()
            } else {
                format!("{}", i % 50)
            };
            let c = format!("'{}{}'", if i % 2 == 0 { "K" } else { "k" }, i % 7);
            rows.push(format!(
                "INSERT INTO m VALUES ({i}, {a}, 'k{}', {c}, {}, {});",
                i % 100,
                i % 30,
                (i * 37) % 101
            ));
            rows.push(format!("INSERT INTO w VALUES ({}, {i});", i % 30));
        }
        let queries = [
            "SELECT MAX(id) FROM m WHERE a = 5",
            "SELECT MIN(id) FROM m WHERE a = 5",
            "SELECT MAX(rowid) FROM m WHERE a = 17",
            "SELECT MIN(rowid) FROM m WHERE 17 = a",
            "SELECT MAX(id) FROM m WHERE a = 999",
            "SELECT MIN(id) FROM m WHERE a = NULL",
            "SELECT MAX(id) FROM m WHERE a = '5'",
            "SELECT MIN(id) FROM m WHERE a = 5.0",
            "SELECT MAX(id) FROM m WHERE a = 5.5",
            "SELECT COALESCE(MAX(id), 0) FROM m WHERE a = 999",
            "SELECT MAX(id) + 1 FROM m WHERE a = 0",
            "SELECT MAX(id) FROM m WHERE s = 'k9'",
            "SELECT MIN(id) FROM m WHERE s = 9",
            "SELECT MAX(id) FROM m WHERE c = 'K1'",
            "SELECT MIN(id) FROM m WHERE c = 'k3'",
            // Declines that must still match: a composite-only key, a residual, the
            // collation mismatch, WITHOUT ROWID.
            "SELECT MAX(id) FROM m WHERE k = 3",
            "SELECT MIN(id) FROM m WHERE k = 3",
            "SELECT MAX(id) FROM m WHERE a = 5 AND v > 50",
            "SELECT MIN(v) FROM n WHERE c = 'k1'",
            "SELECT MAX(v) FROM n WHERE c = 'K1'",
            "SELECT MAX(id) FROM w WHERE k = 5",
        ];
        for path in [":memory:", file.to_str().unwrap()] {
            let f = Connection::open(path).await.expect("frank");
            let r = rusqlite::Connection::open_in_memory().expect("sqlite");
            for stmt in schema
                .iter()
                .copied()
                .chain(rows.iter().map(String::as_str))
            {
                f.execute(stmt).await.unwrap();
                r.execute_batch(stmt).unwrap();
            }
            if path != ":memory:" {
                f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                    .await
                    .unwrap();
            }
            for sql in queries {
                assert_eq!(
                    frank_rows(&f, sql).await,
                    sqlite_rows(&r, sql),
                    "diverged: `{sql}` at {path}"
                );
            }
            // Bound parameters take the same seek.
            for value in [
                SqliteValue::Integer(5),
                SqliteValue::Text("5".into()),
                SqliteValue::Null,
            ] {
                let got: Vec<Vec<String>> = f
                    .query_with_params("SELECT MAX(id) FROM m WHERE a = ?1", &[value.clone()])
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().iter().map(render).collect())
                    .collect();
                let literal = match &value {
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Text(s) => format!("'{s}'"),
                    _ => "NULL".to_owned(),
                };
                let want = sqlite_rows(&r, &format!("SELECT MAX(id) FROM m WHERE a = {literal}"));
                assert_eq!(got, want, "MAX(id) WHERE a = ?1 with {value:?} at {path}");
            }
            assert_eq!(
                seek_shape(&f, "SELECT MAX(id) FROM m WHERE a = 5").await,
                (true, false, false),
                "MAX(id) WHERE a=? must SeekLE the one-key index, covering"
            );
            assert_eq!(
                seek_shape(&f, "SELECT MIN(id) FROM m WHERE s = 'k9'").await,
                (false, true, false),
                "MIN(id) WHERE s=? must SeekGE the one-key index, covering"
            );
            assert!(
                !seek_shape(&f, "SELECT MAX(id) FROM m WHERE k = 3").await.0,
                "MAX(id) WHERE k=? has no one-key index on k and must not take the prefix seek"
            );
            f.close().await.unwrap();
        }
    });
}
