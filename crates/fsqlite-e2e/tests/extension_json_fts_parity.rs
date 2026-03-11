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

#[test]
fn fts5_highlight_and_snippet_available_in_frankensqlite() {
    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");

    runner
        .frank()
        .execute("CREATE VIRTUAL TABLE docs USING fts5(subject, body);")
        .expect("FrankenSQLite should create FTS5 virtual tables");
    runner
        .frank()
        .execute(
            "INSERT INTO docs(rowid, subject, body) VALUES \
             (1, 'Intro', 'Rust systems language safety and speed');",
        )
        .expect("first FTS5 insert should succeed");
    runner
        .frank()
        .execute(
            "INSERT INTO docs(rowid, subject, body) VALUES \
             (2, 'Other', 'Nothing relevant lives here');",
        )
        .expect("second FTS5 insert should succeed");

    let highlight_rows = runner
        .frank()
        .query(
            "SELECT highlight(body, 'rust AND safety', '<b>', '</b>') \
             FROM docs WHERE rowid = 1;",
        )
        .expect("highlight() should be callable from SQL");
    assert_eq!(highlight_rows.len(), 1);
    match &highlight_rows[0][0] {
        SqlValue::Text(text) => {
            assert!(text.contains("<b>Rust</b>"));
            assert!(text.contains("<b>safety</b>"));
        }
        other => panic!("highlight() must return text, got {other:?}"),
    }

    let snippet_rows = runner
        .frank()
        .query(
            "SELECT snippet(body, 'rust AND safety', '[', ']', '...', 4) \
             FROM docs WHERE rowid = 1;",
        )
        .expect("snippet() should be callable from SQL");
    assert_eq!(snippet_rows.len(), 1);
    match &snippet_rows[0][0] {
        SqlValue::Text(text) => {
            assert!(text.contains("[Rust]") || text.contains("[safety]"));
        }
        other => panic!("snippet() must return text, got {other:?}"),
    }
}

#[test]
fn fts5_match_column_filters_work_in_frankensqlite() {
    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");

    runner
        .frank()
        .execute("CREATE VIRTUAL TABLE docs USING fts5(subject, body);")
        .expect("FrankenSQLite should create FTS5 virtual tables");
    runner
        .frank()
        .execute(
            "INSERT INTO docs(rowid, subject, body) VALUES \
             (1, 'Rust title', 'plain body'), \
             (2, 'Plain title', 'rust body');",
        )
        .expect("FrankenSQLite FTS5 inserts should succeed");

    let frank_subject_rows = runner
        .frank()
        .query("SELECT rowid FROM docs WHERE docs MATCH 'subject:rust';")
        .expect("subject column filter should succeed");
    assert_eq!(frank_subject_rows, vec![vec![SqlValue::Integer(1)]]);

    let frank_body_rows = runner
        .frank()
        .query("SELECT rowid FROM docs WHERE docs MATCH 'body:rust';")
        .expect("body column filter should succeed");
    assert_eq!(frank_body_rows, vec![vec![SqlValue::Integer(2)]]);

    if runner
        .csqlite()
        .execute("CREATE VIRTUAL TABLE docs USING fts5(subject, body);")
        .is_ok()
    {
        runner
            .csqlite()
            .execute(
                "INSERT INTO docs(rowid, subject, body) VALUES \
                 (1, 'Rust title', 'plain body'), \
                 (2, 'Plain title', 'rust body');",
            )
            .expect("C SQLite FTS5 inserts should succeed when FTS5 is available");

        let c_subject_rows = runner
            .csqlite()
            .query("SELECT rowid FROM docs WHERE docs MATCH 'subject:rust';")
            .expect("C SQLite subject column filter should succeed");
        assert_eq!(c_subject_rows, frank_subject_rows);

        let c_body_rows = runner
            .csqlite()
            .query("SELECT rowid FROM docs WHERE docs MATCH 'body:rust';")
            .expect("C SQLite body column filter should succeed");
        assert_eq!(c_body_rows, frank_body_rows);
    }
}
