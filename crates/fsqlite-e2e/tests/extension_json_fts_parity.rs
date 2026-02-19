//! E2E extension parity checks for JSON1 + FTS scalar surface.
//!
//! Bead: bd-1dp9.5.2

use fsqlite_e2e::comparison::{ComparisonRunner, SqlBackend, SqlValue};

#[test]
fn json1_contract_rows_match_csqlite() {
    let stmts = vec![
        r#"SELECT json_valid('{"a":1}');"#.to_owned(),
        r#"SELECT json_extract('{"a":1,"b":[2,3]}', '$.a');"#.to_owned(),
        r#"SELECT json_extract('{"a":1,"b":[2,3]}', '$.b[1]');"#.to_owned(),
        r#"SELECT json_type('{"a":[1,2]}', '$.a');"#.to_owned(),
        r#"SELECT json_set('{"a":1}', '$.b', 2);"#.to_owned(),
        r#"SELECT json_remove('{"a":1,"b":2}', '$.b');"#.to_owned(),
        r"SELECT json_array(1,'x',NULL);".to_owned(),
        r"SELECT json_object('a',1,'b',2);".to_owned(),
    ];

    eprintln!(
        "{{\"bead\":\"bd-1dp9.5.2\",\"phase\":\"json1_contract_rows\",\"statements\":{}}}",
        stmts.len()
    );

    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    let result = runner.run_and_compare(&stmts);

    assert_eq!(
        result.operations_mismatched, 0,
        "json1 contract mismatches detected: {:?}",
        result.mismatches
    );
}

#[test]
fn fts5_source_id_available_in_frankensqlite() {
    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");

    let frank_rows = runner
        .frank()
        .query("SELECT fts5_source_id();")
        .expect("FrankenSQLite should expose fts5_source_id()");
    assert_eq!(frank_rows.len(), 1);
    match &frank_rows[0][0] {
        SqlValue::Text(source) => {
            assert!(
                source.to_ascii_lowercase().contains("fts5"),
                "unexpected fts5_source_id payload: {source}"
            );
        }
        other => panic!("fts5_source_id() must return text, got {other:?}"),
    }

    // Best-effort parity check: some SQLite bundled builds can omit FTS5.
    // When available, ensure C SQLite also returns one row.
    if let Ok(c_rows) = runner.csqlite().query("SELECT fts5_source_id();") {
        assert_eq!(c_rows.len(), 1);
    }
}
