//! bd-gh-pragma-tvf-surface-0ryxd (GH #213): `pragma_database_list()` must be
//! usable as a table-valued function, not error "table-valued function
//! pragma_database_list is not supported". Its rows mirror `PRAGMA
//! database_list` (seq, name, file), and it supports projection/WHERE like any
//! TVF. This keeper checks self-consistency with the PRAGMA form plus ATTACH
//! visibility (no cross-engine path comparison, which is environment-specific).

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

fn seq_name(rows: &[fsqlite_core::connection::Row]) -> Vec<(i64, String)> {
    rows.iter()
        .map(|r| {
            let seq = match r.values()[0] {
                SqliteValue::Integer(n) => n,
                ref o => panic!("seq not int: {o:?}"),
            };
            let name = match r.values()[1] {
                SqliteValue::Text(ref s) => s.as_ref().to_owned(),
                ref o => panic!("name not text: {o:?}"),
            };
            (seq, name)
        })
        .collect()
}

#[test]
fn pragma_database_list_table_valued_function() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        // The TVF must not error (the bug) and must mirror the PRAGMA form.
        let tvf = conn
            .query("SELECT seq, name FROM pragma_database_list()")
            .await
            .expect("pragma_database_list() TVF must be supported");
        let pragma = conn
            .query("PRAGMA database_list")
            .await
            .expect("PRAGMA database_list");
        assert_eq!(
            seq_name(&tvf),
            seq_name(&pragma),
            "pragma_database_list() must mirror PRAGMA database_list"
        );
        assert_eq!(seq_name(&tvf).first(), Some(&(0, "main".to_owned())));

        // Projection + WHERE (real TVF behaviour).
        let names = conn
            .query("SELECT name FROM pragma_database_list() WHERE seq = 0")
            .await
            .expect("projection/WHERE on the TVF");
        assert_eq!(names.len(), 1);
        assert!(matches!(names[0].values()[0], SqliteValue::Text(ref s) if s.as_ref() == "main"));

        // ATTACH becomes visible through the TVF.
        let dir = tempfile::tempdir().expect("temp dir");
        let aux = dir.path().join("aux.db");
        conn.execute(&format!("ATTACH '{}' AS aux2", aux.display()))
            .await
            .expect("attach");
        let after = conn
            .query("SELECT name FROM pragma_database_list() ORDER BY seq")
            .await
            .expect("TVF after attach");
        let attached: Vec<String> = after
            .iter()
            .map(|r| match r.values()[0] {
                SqliteValue::Text(ref s) => s.as_ref().to_owned(),
                ref o => panic!("name not text: {o:?}"),
            })
            .collect();
        assert!(
            attached.contains(&"main".to_owned()),
            "main missing: {attached:?}"
        );
        assert!(
            attached.contains(&"aux2".to_owned()),
            "attached db missing: {attached:?}"
        );
    });
}
