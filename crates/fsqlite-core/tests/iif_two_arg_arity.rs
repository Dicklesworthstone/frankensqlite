//! bd-gh-iif-two-arg-arity-a8tm9 (GH #183): two-argument `iif(X,Y)` (shorthand
//! for `iif(X,Y,NULL)`, SQLite 3.48+). A truthy X returns Y; a falsy/NULL X
//! returns NULL. Covers both the constant const-fold path and the runtime
//! IifFunc path, plus the `if` alias and the 3-arg form. Expected values are
//! the documented SQLite 3.48+ semantics (rusqlite's bundled SQLite may predate
//! 2-arg iif, so no oracle here).

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

fn cell_text(v: &SqliteValue) -> Option<String> {
    match v {
        SqliteValue::Text(s) => Some(s.as_ref().to_owned()),
        SqliteValue::Null => None,
        other => panic!("expected text/null, got {other:?}"),
    }
}

#[test]
fn iif_two_argument_form() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        // All-constant args -> the interpreter const-fold path (the bug: 2-arg
        // truthy folded to NULL instead of args[1]).
        let scalar = |sql: &'static str, want: Option<&'static str>| {
            let conn = &conn;
            async move {
                let rows = conn.query(sql).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
                assert_eq!(
                    cell_text(&rows[0].values()[0]).as_deref(),
                    want,
                    "`{sql}` mismatch"
                );
            }
        };
        scalar("SELECT iif(1, 'a')", Some("a")).await; // truthy -> Y (was NULL)
        scalar("SELECT iif(0, 'a')", None).await; // falsy -> NULL
        scalar("SELECT iif(NULL, 'a')", None).await; // NULL cond -> NULL
        scalar("SELECT iif(0.5, 'a')", Some("a")).await; // non-zero real is truthy
        scalar("SELECT iif(1, 'a', 'b')", Some("a")).await; // 3-arg still works
        scalar("SELECT iif(0, 'a', 'b')", Some("b")).await;
        scalar("SELECT if(1, 'a')", Some("a")).await; // `if` alias, 2-arg
        scalar("SELECT if(0, 'a')", None).await;

        // Runtime (column-driven) -> IifFunc path.
        conn.execute("CREATE TABLE t(c)").await.unwrap();
        conn.execute("INSERT INTO t VALUES (0),(1),(NULL)").await.unwrap();
        let rows = conn
            .query("SELECT iif(c, 'a') FROM t ORDER BY rowid")
            .await
            .expect("runtime iif");
        let got: Vec<Option<String>> = rows.iter().map(|r| cell_text(&r.values()[0])).collect();
        assert_eq!(
            got,
            vec![None, Some("a".to_owned()), None],
            "runtime 2-arg iif over column values"
        );
    });
}
