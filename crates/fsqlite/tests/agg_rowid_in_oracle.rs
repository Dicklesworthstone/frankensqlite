//! bd-agg-rowid-in: `SELECT SUM(v)/AVG(v)/MIN(v)/MAX(v)/COUNT(v) FROM t WHERE <rowid> IN (<int literals>)`
//! seeks each listed rowid and accumulates it, instead of full-scanning. (COUNT(*) is served separately by
//! count_star's own rowid-IN path.) Aggregate results are compared against C SQLite; the optimization is
//! confirmed by the ABSENCE of a `Rewind` in the plan. A bare output column, an all-miss (Null result),
//! and a non-rowid predicate decline / stay correct.
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
            "agg-rowid-IN must not full-scan (Rewind): `{sql}`"
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
fn agg_rowid_in_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let schema = "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, w REAL, x INTEGER);";
        f.execute(schema).await.unwrap();
        r.execute_batch(schema).unwrap();
        for i in 1..=300_i64 {
            // v = 2*id; some NULL v (COUNT(v)/SUM skip); w real; x non-indexed for control.
            let vv = if i % 41 == 0 {
                "NULL".to_string()
            } else {
                (i * 2).to_string()
            };
            let s = format!("INSERT INTO t VALUES ({i}, {vv}, {i}.5, {});", i % 7);
            f.execute(&s).await.unwrap();
            r.execute_batch(&s).unwrap();
        }

        // SUM / AVG / MIN / MAX / COUNT(col) over rowid IN: seek per value, no Rewind, value byte-exact.
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE id IN (5, 25, 45)",
            Some(true),
        )
        .await;
        cmp(
            &f,
            &r,
            "SELECT AVG(v) FROM t WHERE id IN (5, 25, 45)",
            Some(true),
        )
        .await;
        cmp(
            &f,
            &r,
            "SELECT MIN(v) FROM t WHERE id IN (5, 25, 45)",
            Some(true),
        )
        .await;
        cmp(
            &f,
            &r,
            "SELECT MAX(v) FROM t WHERE id IN (5, 25, 45)",
            Some(true),
        )
        .await;
        cmp(
            &f,
            &r,
            "SELECT COUNT(v) FROM t WHERE id IN (5, 25, 45)",
            Some(true),
        )
        .await;
        cmp(
            &f,
            &r,
            "SELECT SUM(w) FROM t WHERE id IN (5, 25, 45)",
            Some(true),
        )
        .await; // real sum
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE id IN (5, 5, 25)",
            Some(true),
        )
        .await; // dedup: 5 counted once
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE id IN (5, 99999)",
            Some(true),
        )
        .await; // one absent
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE id IN (99999, 88888)",
            Some(true),
        )
        .await; // all absent -> NULL
        cmp(
            &f,
            &r,
            "SELECT COUNT(v) FROM t WHERE id IN (99999, 88888)",
            Some(true),
        )
        .await; // all absent -> 0
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE id IN (41, 82)",
            Some(true),
        )
        .await; // v is NULL at those rows -> NULL
        cmp(
            &f,
            &r,
            "SELECT COUNT(v) FROM t WHERE id IN (41, 82, 5)",
            Some(true),
        )
        .await; // 2 NULL + 1 non-null -> 1
        cmp(
            &f,
            &r,
            "SELECT MAX(v) FROM t WHERE 7 = id OR id = 8",
            Some(true),
        )
        .await; // OR-of-rowid-eq (an IN)

        // bd-agg-rowid-in-residual: rowid IN (<ints>) AND <non-indexed residual> — seek per value + re-apply
        // the WHERE per hit, no Rewind, value byte-exact. `x = i % 7` (x[5]=5, x[25]=4, x[45]=3).
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE id IN (5, 25, 45) AND x > 0",
            Some(true),
        )
        .await; // all 3 -> 150
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE id IN (5, 25, 45) AND x = 5",
            Some(true),
        )
        .await; // only id=5 -> 10
        cmp(
            &f,
            &r,
            "SELECT COUNT(v) FROM t WHERE id IN (5, 25, 45) AND x = 5",
            Some(true),
        )
        .await; // -> 1
        cmp(
            &f,
            &r,
            "SELECT MAX(v) FROM t WHERE id IN (5, 25, 45) AND x < 5",
            Some(true),
        )
        .await; // id=25,45 -> 90
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE id IN (5, 25) AND x = 999",
            Some(true),
        )
        .await; // none -> NULL
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE id IN (5, 99999) AND x = 5",
            Some(true),
        )
        .await; // residual + absent -> 10
        cmp(
            &f,
            &r,
            "SELECT SUM(v) FROM t WHERE id IN (41, 5) AND x >= 0",
            Some(true),
        )
        .await; // NULL v row + real -> 10
        assert!(
            !has_op(
                &f,
                "SELECT SUM(v) FROM t WHERE id IN (5, 25) AND x = ?",
                "Rewind"
            )
            .await,
            "param agg rowid-IN residual should seek (no Rewind)"
        );

        // Control: a non-rowid, non-indexed predicate still full-scans.
        cmp(&f, &r, "SELECT SUM(v) FROM t WHERE x = 3", Some(false)).await;
    });
}
