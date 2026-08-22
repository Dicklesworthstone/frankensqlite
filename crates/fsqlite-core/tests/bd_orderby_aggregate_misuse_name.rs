// Keeper (bd-errmsg-parity-batch2 (a)): a bare aggregate in ORDER BY of a
// non-aggregate SELECT reports stock's "misuse of aggregate: <name>()" naming
// the rightmost bare aggregate, not the old nameless stub "misuse of aggregate: ".
// Windowed calls (OVER) are skipped. Oracle: sqlite3 3.46.1 + rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn err_of(sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute("CREATE TABLE t(a, b)").await.unwrap();
    c.execute(sql)
        .await
        .expect_err("aggregate in ORDER BY is a misuse")
        .to_string()
}

#[test]
fn orderby_aggregate_misuse_names_aggregate() {
    asupersync::test_utils::run_test(|| async {
        assert_eq!(
            err_of("SELECT a FROM t ORDER BY sum(a)").await,
            "misuse of aggregate: sum()"
        );
        assert_eq!(
            err_of("SELECT a FROM t ORDER BY max(a)").await,
            "misuse of aggregate: max()"
        );
        assert_eq!(
            err_of("SELECT a FROM t ORDER BY count(*)").await,
            "misuse of aggregate: count()"
        );
        assert_eq!(
            err_of("SELECT a FROM t ORDER BY total(b)").await,
            "misuse of aggregate: total()"
        );
        // Windowed call is skipped; the bare aggregate is named.
        assert_eq!(
            err_of("SELECT a FROM t ORDER BY count(*) OVER () + sum(a)").await,
            "misuse of aggregate: sum()",
        );
        // Multiple bare aggregates: the RIGHTMOST is named.
        assert_eq!(
            err_of("SELECT a FROM t ORDER BY sum(a) + total(b)").await,
            "misuse of aggregate: total()",
        );
        assert_eq!(
            err_of("SELECT a FROM t ORDER BY total(b) + sum(a)").await,
            "misuse of aggregate: sum()",
        );
    });
}
