//! bd-agg-leading-eq-residual: `SELECT COUNT(*)/SUM(...) FROM t WHERE a = <literal> AND <residual>`
//! seeks a storage-class-exact equality-prefix block on an index and applies the FULL WHERE as a
//! residual filter per row, instead of full-scanning. HARD GATE: byte-identical to C SQLite across
//! residual equality/range/OR/multi-term predicates (including a rowid residual), a multi-column
//! equality prefix, NULL trailing composite-index terms, absent keys
//! (→ COUNT=0 / SUM=NULL), COALESCE, and both aggregates. The residual filter enforces the WHOLE
//! predicate, so nothing is dropped. Opcode gate: an all-literal WHERE SeekGEs the index; a WHERE with
//! a bound parameter in the literal prefix declines, while a parameterized residual remains seekable.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(_) => "blob".to_owned(),
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
                rusqlite::types::Value::Blob(_) => "blob".to_owned(),
            });
        }
        Ok(out)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

async fn has_op(conn: &Connection, sql: &str, want: &str) -> bool {
    conn.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .any(
            |row| matches!(row.values().get(1), Some(SqliteValue::Text(op)) if op.to_string() == want),
        )
}

#[test]
fn agg_leading_eq_residual_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("frank");
        let r = rusqlite::Connection::open_in_memory().expect("sqlite");
        for stmt in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, x INTEGER, y INTEGER, s TEXT);",
            "CREATE INDEX idx_a_x5 ON t(a) WHERE x = 5;",
            "CREATE INDEX idx_ab ON t(a, b);",
            "CREATE INDEX idx_a ON t(a);",
            "CREATE INDEX idx_s ON t(s);", // BINARY collation (default)
        ] {
            f.execute(stmt).await.unwrap();
            r.execute_batch(stmt).unwrap();
        }
        for i in 1..=3000_i64 {
            let a = i % 12;
            // Exercise the prefix-record floor on idx_ab. A synthetic
            // `(a, i64::MIN)` probe sorts after these NULL trailing keys and
            // would silently omit them.
            let b = if i % 23 == 0 {
                "NULL".to_owned()
            } else {
                (i % 7).to_string()
            };
            let x = i % 10;
            let y = i % 3;
            let stmt = format!(
                "INSERT INTO t VALUES ({i}, {a}, {b}, {x}, {y}, 'k{}');",
                i % 6
            );
            f.execute(&stmt).await.unwrap();
            r.execute_batch(&stmt).unwrap();
        }

        for sql in [
            // Leading eq + residual equality on a non-key column, both aggregates.
            "SELECT COUNT(*) FROM t WHERE a = 7 AND x = 5",
            "SELECT COUNT(*) FROM t AS q WHERE q.a = 7 AND q.x = 5",
            "SELECT SUM(x) FROM t WHERE a = 7 AND x = 5",
            "SELECT SUM(b) FROM t WHERE a = 7 AND x = 5",
            // Residual range / inequality / OR.
            "SELECT COUNT(*) FROM t WHERE a = 7 AND x > 5",
            "SELECT COUNT(*) FROM t WHERE a = 3 AND x <> 5",
            "SELECT COUNT(*) FROM t WHERE a = 7 AND (x = 5 OR x = 6)",
            // Multiple residuals.
            "SELECT COUNT(*) FROM t WHERE a = 7 AND x = 5 AND y = 1",
            // Rowid residual: matching, reversed-conjunct, and rejection cases.  The
            // rejection is load-bearing proof that the full WHERE is re-applied after
            // the index probe instead of silently dropping the residual.
            "SELECT COUNT(*) FROM t WHERE a = 7 AND id = 7",
            "SELECT SUM(x) FROM t WHERE id = 7 AND a = 7",
            "SELECT COUNT(*) FROM t WHERE a = 7 AND id = 8",
            "SELECT SUM(x) FROM t WHERE a = 7 AND id = 8",
            // Multi-column equality prefix (a, b) + residual on x.
            "SELECT COUNT(*) FROM t WHERE a = 7 AND b = 3 AND x = 5",
            "SELECT SUM(x) FROM t WHERE a = 7 AND b = 3 AND x > 2",
            // Absent prefix -> COUNT=0 / SUM=NULL, and COALESCE.
            "SELECT COUNT(*) FROM t WHERE a = 999 AND x = 5",
            "SELECT SUM(x) FROM t WHERE a = 999 AND x = 5",
            "SELECT COALESCE(SUM(x), -1) FROM t WHERE a = 999 AND x = 5",
            // MIN/MAX riding the same seek.
            "SELECT MIN(x), MAX(x) FROM t WHERE a = 7 AND x > 2",
            // TEXT prefix (BINARY-indexed) + residual — text literal vs TEXT column seeks exactly.
            "SELECT COUNT(*) FROM t WHERE s = 'k3' AND x = 5",
            "SELECT SUM(x) FROM t WHERE s = 'k3' AND x > 2",
            "SELECT COUNT(*) FROM t WHERE s = 'nope' AND x = 5",
            // Residual with IN / BETWEEN / NOT (all literal) — now recognized as placeholder-free.
            "SELECT COUNT(*) FROM t WHERE a = 7 AND x IN (2, 5, 8)",
            "SELECT SUM(x) FROM t WHERE a = 7 AND b IN (1, 3)",
            "SELECT COUNT(*) FROM t WHERE a = 7 AND x BETWEEN 2 AND 6",
            "SELECT COUNT(*) FROM t WHERE a = 7 AND x NOT IN (0, 1)",
            "SELECT COUNT(*) FROM t WHERE s = 'k3' AND x IN (2, 5)",
            // TEXT RANGE + residual on a BINARY-indexed text column (one- and two-sided).
            "SELECT COUNT(*) FROM t WHERE s > 'k1' AND x = 3",
            "SELECT SUM(x) FROM t WHERE s BETWEEN 'k1' AND 'k4' AND x > 2",
            "SELECT COUNT(*) FROM t WHERE s < 'k3' AND x IN (2, 5)",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await,
                sqlite_rows(&r, sql),
                "diverged: `{sql}`"
            );
        }

        // Opcode gate: an all-literal WHERE seeks the prefix (integer and text prefixes both).
        assert!(
            has_op(&f, "SELECT COUNT(*) FROM t WHERE a = 7 AND x = 5", "SeekGE").await,
            "leading eq + literal residual must seek the prefix (SeekGE)"
        );
        let rowid_residual_sql = "SELECT COUNT(*) FROM t WHERE a = 7 AND id = 7";
        assert!(
            has_op(&f, rowid_residual_sql, "SeekGE").await,
            "leading eq + rowid residual must seek the index prefix"
        );
        assert!(
            has_op(&f, rowid_residual_sql, "SeekRowid").await,
            "leading eq + rowid residual must visit the candidate table row"
        );
        assert!(
            !has_op(&f, rowid_residual_sql, "Rewind").await,
            "leading eq + rowid residual must not full-scan the table"
        );
        assert!(
            has_op(
                &f,
                "SELECT COUNT(*) FROM t WHERE s = 'k3' AND x = 5",
                "SeekGE"
            )
            .await,
            "TEXT leading eq + literal residual must seek the prefix (SeekGE)"
        );
        assert!(
            has_op(
                &f,
                "SELECT COUNT(*) FROM t WHERE a = 7 AND x IN (2, 5, 8)",
                "SeekGE"
            )
            .await,
            "leading eq + IN-list residual (all literal) must seek the prefix (SeekGE)"
        );
        // A bound parameter in the RESIDUAL is fine: the literal prefix probe consumes no anonymous
        // placeholders, so the residual filter numbers `?` exactly as the scan path does. The seek fires.
        assert!(
            has_op(&f, "SELECT COUNT(*) FROM t WHERE a = 7 AND x = ?", "SeekGE").await,
            "leading eq (literal) + parameterized residual must still seek the prefix"
        );
        // A bound parameter in the PREFIX / range bound (the probe) declines — the probe must be a literal.
        assert!(
            !has_op(&f, "SELECT COUNT(*) FROM t WHERE a = ? AND x = 5", "SeekGE").await,
            "a parameterized PREFIX must decline (probe must be a literal)"
        );
        assert!(
            !has_op(&f, "SELECT COUNT(*) FROM t WHERE a > ? AND x = 5", "SeekGE").await,
            "a parameterized range bound must decline (bound must be a literal)"
        );
        // The single-column index range seek anchors with SeekGE (exclusive lower handled by a Le skip).
        assert!(
            has_op(
                &f,
                "SELECT COUNT(*) FROM t WHERE s > 'k1' AND x = 3",
                "SeekGE"
            )
            .await,
            "TEXT range + residual on a BINARY-indexed column must seek the range (SeekGE)"
        );
    });
}

#[test]
fn agg_leading_eq_residual_rejects_binary_index_for_nocase_column() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("frank");
        let r = rusqlite::Connection::open_in_memory().expect("sqlite");
        for stmt in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE, x INTEGER);",
            "CREATE INDEX idx_s_binary ON t(s COLLATE BINARY);",
            "INSERT INTO t VALUES (1, 'k3', 5);",
            "INSERT INTO t VALUES (2, 'K3', 5);",
            "INSERT INTO t VALUES (3, 'k3', 4);",
            "INSERT INTO t VALUES (4, 'other', 5);",
        ] {
            f.execute(stmt).await.unwrap();
            r.execute_batch(stmt).unwrap();
        }

        for sql in [
            "SELECT COUNT(*) FROM t WHERE s = 'k3' AND x = 5",
            "SELECT SUM(x) FROM t WHERE s = 'k3' AND x = 5",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await,
                sqlite_rows(&r, sql),
                "NOCASE-column/BINARY-index aggregate diverged: `{sql}`"
            );
            assert!(
                !has_op(&f, sql, "SeekGE").await,
                "a BINARY index cannot serve a bare equality whose NOCASE column \
                 semantics also match differently-cased keys: `{sql}`"
            );
        }
    });
}
