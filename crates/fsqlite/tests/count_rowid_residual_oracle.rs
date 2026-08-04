//! bd-count-rowid-in-residual / bd-count-rowid-eq-residual: `SELECT COUNT(*) FROM t WHERE <rowid> IN
//! (<ints>) AND <residual>` and `<rowid> = <int> AND <residual>` seek per rowid value and re-apply the
//! full WHERE per hit, instead of full-scanning. Reached only when no `simple_count_star` diverter
//! matches — a residual on an INDEXED column may route to the aggregate seek instead, so those cases are
//! checked for COUNT PARITY ONLY (plan not asserted). Counts are compared against C SQLite; for the
//! non-indexed-residual cases the optimization is confirmed by the ABSENCE of a `Rewind`. Also regresses
//! the bare rowid IN/eq cases (the shared emitter gained a residual path that must stay byte-identical
//! when off).
use fsqlite::Connection;
use fsqlite_types::SqliteValue;

async fn count_f(c: &Connection, sql: &str) -> i64 {
    let rows = c
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"));
    match rows.first().and_then(|r| r.values().first()) {
        Some(SqliteValue::Integer(n)) => *n,
        other => panic!("expected integer count, got {other:?} for `{sql}`"),
    }
}

fn count_r(c: &rusqlite::Connection, sql: &str) -> i64 {
    c.query_row(sql, [], |row| row.get::<_, i64>(0)).unwrap()
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
            "rowid-residual COUNT must not full-scan (Rewind): `{sql}`"
        ),
        Some(false) => assert!(
            has_op(f, sql, "Rewind").await,
            "control COUNT should full-scan (Rewind): `{sql}`"
        ),
        None => {}
    }
    assert_eq!(
        count_f(f, sql).await,
        count_r(r, sql),
        "count diverged: `{sql}`"
    );
}

#[test]
fn count_rowid_residual_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, y INTEGER, c INTEGER);", // y non-indexed, c indexed
            "CREATE INDEX idx_c ON t(c);",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        for i in 1..=300_i64 {
            let s = format!("INSERT INTO t VALUES ({i}, {}, {});", i % 20, i % 10);
            f.execute(&s).await.unwrap();
            r.execute_batch(&s).unwrap();
        }

        // Bare cases still seek (regression: the shared emitter's residual path must be off / byte-identical).
        cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE id = 5", Some(true)).await;
        cmp(
            &f,
            &r,
            "SELECT COUNT(*) FROM t WHERE id IN (5, 25, 45)",
            Some(true),
        )
        .await;

        // rowid = <int> AND <non-indexed residual>: reaches count_star (no diverter) -> seek, no Rewind.
        cmp(
            &f,
            &r,
            "SELECT COUNT(*) FROM t WHERE id = 5 AND y = 5",
            Some(true),
        )
        .await; // y[5]=5 -> 1
        cmp(
            &f,
            &r,
            "SELECT COUNT(*) FROM t WHERE id = 5 AND y = 7",
            Some(true),
        )
        .await; // y[5]=5 != 7 -> 0
        cmp(
            &f,
            &r,
            "SELECT COUNT(*) FROM t WHERE id = 99999 AND y = 5",
            Some(true),
        )
        .await; // absent rowid -> 0
        cmp(
            &f,
            &r,
            "SELECT COUNT(*) FROM t WHERE id = 5 AND y > 0 AND y < 10",
            Some(true),
        )
        .await; // multi-conjunct -> 1

        // rowid IN (<ints>) AND <non-indexed residual>: SeekRowid per value + residual, no Rewind.
        cmp(
            &f,
            &r,
            "SELECT COUNT(*) FROM t WHERE id IN (5, 25, 45) AND y = 5",
            Some(true),
        )
        .await; // all y=5 -> 3
        cmp(
            &f,
            &r,
            "SELECT COUNT(*) FROM t WHERE id IN (5, 26, 47) AND y = 5",
            Some(true),
        )
        .await; // only id=5 -> 1
        cmp(
            &f,
            &r,
            "SELECT COUNT(*) FROM t WHERE id IN (5, 25, 45) AND y = 6",
            Some(true),
        )
        .await; // none -> 0
        cmp(
            &f,
            &r,
            "SELECT COUNT(*) FROM t WHERE id IN (5, 99999) AND y = 5",
            Some(true),
        )
        .await; // one absent -> 1

        // Placeholder residual routes to the seek too (plan asserted on the unbound `?`).
        assert!(
            !has_op(
                &f,
                "SELECT COUNT(*) FROM t WHERE id = 5 AND y = ?",
                "Rewind"
            )
            .await,
            "param eq-residual COUNT should seek (no Rewind)"
        );
        assert!(
            !has_op(
                &f,
                "SELECT COUNT(*) FROM t WHERE id IN (5, 25) AND y = ?",
                "Rewind"
            )
            .await,
            "param in-residual COUNT should seek (no Rewind)"
        );

        // Indexed residual may route to the aggregate seek (bd-2dgf5) instead of count_star — count parity only.
        cmp(
            &f,
            &r,
            "SELECT COUNT(*) FROM t WHERE id = 5 AND c = 5",
            None,
        )
        .await; // c[5]=5 -> 1
        cmp(
            &f,
            &r,
            "SELECT COUNT(*) FROM t WHERE id IN (5, 25, 45) AND c = 5",
            None,
        )
        .await;

        // Control: a bare non-rowid, non-indexed predicate still full-scans.
        cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE y = 5", Some(false)).await;
    });
}
