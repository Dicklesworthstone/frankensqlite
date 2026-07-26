//! bd-agg-rowid-eq-residual: `SELECT SUM(v)/MIN(v)/MAX(v)/COUNT(v) FROM t WHERE <rowid> = <int> AND
//! <residual>` seeks the single row and re-applies the residual before accumulating, instead of
//! full-scanning. Reached only for a NON-indexed residual (an indexed-literal residual routes to an
//! earlier index seek). Aggregate results are compared against C SQLite; the optimization is confirmed by
//! the ABSENCE of a `Rewind`. Also regresses the bare `rowid = <int>` aggregate (rowid_eq_seek).
use fsqlite::Connection;
use fsqlite_types::SqliteValue;

async fn val_f(c: &Connection, sql: &str) -> SqliteValue {
    c.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .first()
        .and_then(|r| r.values().first().cloned())
        .unwrap_or(SqliteValue::Null)
}

fn val_r(c: &rusqlite::Connection, sql: &str) -> rusqlite::types::Value {
    c.query_row(sql, [], |row| row.get::<_, rusqlite::types::Value>(0))
        .unwrap()
}

fn same(f: &SqliteValue, r: &rusqlite::types::Value) -> bool {
    use rusqlite::types::Value as RV;
    match (f, r) {
        (SqliteValue::Null, RV::Null) => true,
        (SqliteValue::Integer(a), RV::Integer(b)) => a == b,
        (SqliteValue::Float(a), RV::Real(b)) => (a - b).abs() < 1e-9,
        (SqliteValue::Integer(a), RV::Real(b)) => (*a as f64 - b).abs() < 1e-9,
        (SqliteValue::Float(a), RV::Integer(b)) => (a - *b as f64).abs() < 1e-9,
        _ => false,
    }
}

async fn has_op(c: &Connection, sql: &str, prefix: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .any(|row| matches!(row.values().get(1), Some(SqliteValue::Text(o)) if o.to_string().starts_with(prefix)))
}

async fn cmp(f: &Connection, r: &rusqlite::Connection, sql: &str, no_rewind: Option<bool>) {
    match no_rewind {
        Some(true) => assert!(
            !has_op(f, sql, "Rewind").await,
            "agg rowid-eq-residual must not full-scan (Rewind): `{sql}`"
        ),
        Some(false) => assert!(
            has_op(f, sql, "Rewind").await,
            "control should full-scan (Rewind): `{sql}`"
        ),
        None => {}
    }
    let (vf, vr) = (val_f(f, sql).await, val_r(r, sql));
    assert!(
        same(&vf, &vr),
        "value diverged for `{sql}`: frank {vf:?} vs sqlite {vr:?}"
    );
}

#[test]
fn agg_rowid_eq_residual_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let schema = "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, x INTEGER);";
        f.execute(schema).await.unwrap();
        r.execute_batch(schema).unwrap();
        for i in 1..=300_i64 {
            let vv = if i % 41 == 0 {
                "NULL".to_string()
            } else {
                (i * 2).to_string()
            };
            let s = format!("INSERT INTO t VALUES ({i}, {vv}, {});", i % 7); // x = i % 7 (x[5]=5)
            f.execute(&s).await.unwrap();
            r.execute_batch(&s).unwrap();
        }

        // Bare rowid = <int> aggregate still seeks (regression: rowid_eq_seek unchanged, new gate declines).
        cmp(&f, &r, "SELECT SUM(v) FROM t WHERE id = 5", Some(true)).await; // -> 10

        // rowid = <int> AND <non-indexed residual>: seek the single row + residual, no Rewind, value byte-exact.
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE id = 5 AND x = 5",
            Some(true),
        )
        .await; // x[5]=5 -> 10
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE id = 5 AND x = 3",
            Some(true),
        )
        .await; // x[5]=5 != 3 -> NULL
        cmp(
            &f,
            &r,
            "SELECT COUNT(v) FROM t WHERE id = 5 AND x = 5",
            Some(true),
        )
        .await; // -> 1
        cmp(
            &f,
            &r,
            "SELECT COUNT(v) FROM t WHERE id = 5 AND x = 3",
            Some(true),
        )
        .await; // no match -> 0
        cmp(
            &f,
            &r,
            "SELECT MAX(v) FROM t WHERE id = 5 AND x >= 0",
            Some(true),
        )
        .await; // matches -> 10
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE id = 99999 AND x = 0",
            Some(true),
        )
        .await; // absent rowid -> NULL
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE id = 41 AND x >= 0",
            Some(true),
        )
        .await; // v NULL at id=41 -> NULL
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE 5 = id AND x = 5",
            Some(true),
        )
        .await; // reversed operand -> 10
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE id = 5 AND x > 0 AND x < 10",
            Some(true),
        )
        .await; // multi-conjunct -> 10

        // Placeholder residual routes to the seek too.
        assert!(
            !has_op(&f, "SELECT SUM(v) FROM t WHERE id = 5 AND x = ?", "Rewind").await,
            "param agg rowid-eq-residual should seek (no Rewind)"
        );

        // Control: a non-rowid, non-indexed predicate still full-scans.
        cmp(&f, &r, "SELECT SUM(v) FROM t WHERE x = 3", Some(false)).await;
    });
}
