//! bd-76k72 pin: does fsqlite apply comparison numeric affinity to a PLAIN
//! (non-FTS5) `TEXT = INTEGER PRIMARY KEY` comparison, in WHERE and in JOIN
//! ON-conditions? Stock sqlite3 3.46.1 matches all three (verified):
//!   WHERE id='7'                      -> 'seven'
//!   child LEFT JOIN parent ON txt=id  -> ('7','seven')
//!   child JOIN parent ON txt=id       -> ('7','seven')
//! Whichever of these fsqlite gets wrong localizes the FTS5-join red's real
//! gap (general affinity vs join-specific vs FTS5-vtab-specific).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[test]
fn probe_plain_text_int_pk_affinity_where_and_join() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO parent VALUES (7,'seven');")
            .await
            .unwrap();
        conn.execute("CREATE TABLE child(txt TEXT);").await.unwrap();
        conn.execute("INSERT INTO child VALUES ('7');").await.unwrap();

        let where_rows = conn
            .query("SELECT v FROM parent WHERE id = '7';")
            .await
            .unwrap();
        eprintln!(
            "PROBE WHERE id='7' (expect [\"seven\"]): {:?}",
            where_rows
                .iter()
                .map(|r| r.values().to_vec())
                .collect::<Vec<_>>()
        );

        let left = conn
            .query("SELECT child.txt, parent.v FROM child LEFT JOIN parent ON child.txt = parent.id;")
            .await
            .unwrap();
        eprintln!(
            "PROBE LEFT JOIN txt=id (expect [(\"7\",\"seven\")]): {:?}",
            left.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>()
        );

        let inner = conn
            .query("SELECT child.txt, parent.v FROM child JOIN parent ON child.txt = parent.id;")
            .await
            .unwrap();
        eprintln!(
            "PROBE INNER JOIN txt=id (expect [(\"7\",\"seven\")]): {:?}",
            inner.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>()
        );

        // Stock-parity assertions.
        assert_eq!(where_rows.len(), 1, "WHERE id='7' must match via numeric affinity");
        assert_eq!(where_rows[0].values()[0], SqliteValue::Text("seven".into()));
        assert_eq!(left.len(), 1);
        assert_eq!(
            left[0].get(1),
            Some(&SqliteValue::Text("seven".into())),
            "LEFT JOIN text=int-PK must match via affinity"
        );
        assert_eq!(inner.len(), 1, "INNER JOIN text=int-PK must match via affinity");
    });
}
