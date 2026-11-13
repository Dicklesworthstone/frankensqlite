//! Re-execute the same prepared VDBE lookup across peer commits and DDL.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[test]
fn retained_prepared_lookup_matches_stock_after_peer_rows_and_ddl() {
    asupersync::test_utils::run_test(|| async {
        for (name, ddl, lookup) in [
            (
                "composite_pk",
                "CREATE TABLE t(tenant TEXT, id TEXT, party TEXT, v TEXT, PRIMARY KEY(tenant,id))",
                "SELECT v FROM t WHERE tenant='a' AND id=?1",
            ),
            (
                "without_rowid",
                "CREATE TABLE t(tenant TEXT, id TEXT, party TEXT, v TEXT, PRIMARY KEY(tenant,id)) WITHOUT ROWID",
                "SELECT v FROM t WHERE tenant='a' AND id=?1",
            ),
            (
                "secondary_index",
                "CREATE TABLE t(tenant TEXT, id TEXT, party TEXT, v TEXT); CREATE INDEX t_party ON t(tenant,party)",
                "SELECT v FROM t WHERE tenant='a' AND party=?1",
            ),
        ] {
            let dir = tempfile::tempdir()
                .expect("retained oracle directory")
                .keep();
            let frank_path = dir.join("frank.db");
            let stock_path = dir.join("stock.db");
            let frank = Connection::open(frank_path.to_str().expect("UTF-8 path"))
                .await
                .expect("open FrankenSQLite");
            let stock = rusqlite::Connection::open(&stock_path).expect("open stock");
            let setup = format!("{ddl}; INSERT INTO t VALUES('a','one','one','before')");
            frank
                .execute_batch(&setup)
                .await
                .expect("FrankenSQLite setup");
            stock.execute_batch(&setup).expect("stock setup");
            let peer = Connection::open(frank_path.to_str().expect("UTF-8 path"))
                .await
                .expect("open FrankenSQLite peer");
            let stock_peer = rusqlite::Connection::open(&stock_path).expect("open stock peer");
            // These exact statement objects survive every write and schema change.
            let stmt = frank.prepare(lookup).await.expect("prepare lookup");
            let mut stock_stmt = stock.prepare(lookup).expect("prepare stock lookup");
            for (change, keys) in [
                ("", &["one", "two"][..]),
                ("UPDATE t SET v='updated' WHERE id='one'", &["one"][..]),
                (
                    "INSERT INTO t VALUES('a','two','two','inserted')",
                    &["one", "two"][..],
                ),
                ("DELETE FROM t WHERE id='one'", &["one", "two"][..]),
                (
                    "ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'default-value'",
                    &["two"][..],
                ),
                (
                    "UPDATE t SET v='after-ddl' WHERE id='two'",
                    &["one", "two"][..],
                ),
            ] {
                if !change.is_empty() {
                    peer.execute_batch(change).await.expect("peer write");
                    stock_peer.execute_batch(change).expect("stock peer write");
                }
                for key in keys {
                    let actual = stmt
                        .query_with_params(&[SqliteValue::Text((*key).into())])
                        .await
                        .unwrap_or_else(|error| {
                            panic!("{name} after `{change}`, key {key}: {error:?}")
                        });
                    let expected: Vec<String> = stock_stmt
                        .query_map([key], |row| row.get(0))
                        .expect("stock retained lookup")
                        .collect::<rusqlite::Result<_>>()
                        .expect("stock rows");
                    let actual: Vec<String> = actual
                        .iter()
                        .map(|row| match &row.values()[0] {
                            SqliteValue::Text(value) => value.to_string(),
                            other => panic!("{name}: expected stock TEXT, got {other:?}"),
                        })
                        .collect();
                    assert_eq!(actual, expected, "{name} after `{change}`, key {key}");
                }
            }
        }
    });
}
