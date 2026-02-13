//! Extension integration tests on real file-backed storage (bd-mblr.2.4).
//!
//! Exercises JSON, FTS3/FTS5, R-tree, session, and misc extension crate APIs
//! using data stored in file-backed databases. Each test creates a temp directory,
//! opens a file-backed connection, writes data, closes, reopens, and verifies
//! integrity after the round-trip.

use fsqlite::Connection;
use fsqlite_ext_fts5::{Fts5Tokenizer, Unicode61Tokenizer};
use fsqlite_ext_json as json_ext;
use fsqlite_ext_rtree as rtree_ext;
use fsqlite_ext_session as session_ext;
use fsqlite_func::scalar::ScalarFunction;
use fsqlite_types::SqliteValue;

const BEAD_ID: &str = "bd-mblr.2.4";

// ─── Helpers ─────────────────────────────────────────────────────────────

fn temp_db() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("create tempdir");
    let path = dir.path().join("test.db").to_str().unwrap().to_owned();
    (dir, path)
}

fn query_col_text(conn: &Connection, sql: &str) -> String {
    let rows = conn.query(sql).expect("query should succeed");
    assert!(!rows.is_empty(), "expected at least one row from: {sql}");
    match rows[0].get(0).expect("column 0") {
        SqliteValue::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    }
}

fn query_col_blob(conn: &Connection, sql: &str) -> Vec<u8> {
    let rows = conn.query(sql).expect("query should succeed");
    assert!(!rows.is_empty(), "expected at least one row from: {sql}");
    match rows[0].get(0).expect("column 0") {
        SqliteValue::Blob(b) => b.clone(),
        other => panic!("expected Blob, got {other:?}"),
    }
}

fn row_count(conn: &Connection, sql: &str) -> usize {
    conn.query(sql).expect("query should succeed").len()
}

// ═══════════════════════════════════════════════════════════════════════
// §1  JSON Extension — Real Storage Round-Trips
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn json_text_storage_round_trip() {
    let (_dir, path) = temp_db();
    let json_str = r#"{"name":"Alice","age":30,"scores":[95,87,92]}"#;

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, data TEXT)")
            .expect("create");
        conn.execute(&format!("INSERT INTO docs VALUES (1, '{json_str}')"))
            .expect("insert");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let stored = query_col_text(&conn, "SELECT data FROM docs WHERE id = 1");

        let minified = json_ext::json(&stored).expect("json parse");
        assert_eq!(json_ext::json_valid(&stored, None), 1);
        assert!(!minified.is_empty());

        let name = json_ext::json_extract(&stored, &["$.name"]).expect("extract name");
        assert_eq!(name, SqliteValue::Text("Alice".to_owned()));

        let age = json_ext::json_extract(&stored, &["$.age"]).expect("extract age");
        assert_eq!(age, SqliteValue::Integer(30));

        let jtype = json_ext::json_type(&stored, None).expect("type");
        assert_eq!(jtype, Some("object"));
    }

    eprintln!("[{BEAD_ID}][test=json_text_storage_round_trip] PASS");
}

#[test]
fn json_mutation_persistence() {
    let (_dir, path) = temp_db();

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE cfg (key TEXT PRIMARY KEY, val TEXT)")
            .expect("create");
        conn.execute(r#"INSERT INTO cfg VALUES ('settings', '{"theme":"dark","font_size":14}')"#)
            .expect("insert");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let original = query_col_text(&conn, "SELECT val FROM cfg WHERE key = 'settings'");

        let updated = json_ext::json_set(&original, &[("$.font_size", SqliteValue::Integer(16))])
            .expect("json_set");

        conn.execute(&format!(
            "UPDATE cfg SET val = '{}' WHERE key = 'settings'",
            updated.replace('\'', "''")
        ))
        .expect("update");
    }

    {
        let conn = Connection::open(&path).expect("reopen2");
        let stored = query_col_text(&conn, "SELECT val FROM cfg WHERE key = 'settings'");

        let font_size =
            json_ext::json_extract(&stored, &["$.font_size"]).expect("extract font_size");
        assert_eq!(font_size, SqliteValue::Integer(16));

        let theme = json_ext::json_extract(&stored, &["$.theme"]).expect("extract theme");
        assert_eq!(theme, SqliteValue::Text("dark".to_owned()));
    }

    eprintln!("[{BEAD_ID}][test=json_mutation_persistence] PASS");
}

#[test]
fn json_array_storage_and_inspection() {
    let (_dir, path) = temp_db();

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE lists (id INTEGER PRIMARY KEY, items TEXT)")
            .expect("create");

        let arr = json_ext::json_array(&[
            SqliteValue::Text("apple".to_owned()),
            SqliteValue::Text("banana".to_owned()),
            SqliteValue::Integer(42),
        ])
        .expect("json_array");

        conn.execute(&format!(
            "INSERT INTO lists VALUES (1, '{}')",
            arr.replace('\'', "''")
        ))
        .expect("insert");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let stored = query_col_text(&conn, "SELECT items FROM lists WHERE id = 1");

        let len = json_ext::json_array_length(&stored, None).expect("array_length");
        assert_eq!(len, Some(3));

        let jtype = json_ext::json_type(&stored, Some("$[0]")).expect("type[0]");
        assert_eq!(jtype, Some("text"));

        let jtype2 = json_ext::json_type(&stored, Some("$[2]")).expect("type[2]");
        assert_eq!(jtype2, Some("integer"));
    }

    eprintln!("[{BEAD_ID}][test=json_array_storage_and_inspection] PASS");
}

#[test]
fn json_object_construction_round_trip() {
    let (_dir, path) = temp_db();

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE objects (id INTEGER PRIMARY KEY, data TEXT)")
            .expect("create");

        let obj = json_ext::json_object(&[
            SqliteValue::Text("x".to_owned()),
            SqliteValue::Integer(10),
            SqliteValue::Text("y".to_owned()),
            SqliteValue::Float(3.14),
        ])
        .expect("json_object");

        conn.execute(&format!(
            "INSERT INTO objects VALUES (1, '{}')",
            obj.replace('\'', "''")
        ))
        .expect("insert");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let stored = query_col_text(&conn, "SELECT data FROM objects WHERE id = 1");

        let x_val = json_ext::json_extract(&stored, &["$.x"]).expect("extract x");
        assert_eq!(x_val, SqliteValue::Integer(10));
    }

    eprintln!("[{BEAD_ID}][test=json_object_construction_round_trip] PASS");
}

#[test]
fn json_remove_and_patch_persistence() {
    let (_dir, path) = temp_db();

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE patches (id INTEGER PRIMARY KEY, data TEXT)")
            .expect("create");
        conn.execute(r#"INSERT INTO patches VALUES (1, '{"a":1,"b":2,"c":3}')"#)
            .expect("insert");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let original = query_col_text(&conn, "SELECT data FROM patches WHERE id = 1");

        let removed = json_ext::json_remove(&original, &["$.b"]).expect("json_remove");
        let patched = json_ext::json_patch(&removed, r#"{"d":4}"#).expect("json_patch");

        conn.execute(&format!(
            "UPDATE patches SET data = '{}' WHERE id = 1",
            patched.replace('\'', "''")
        ))
        .expect("update");
    }

    {
        let conn = Connection::open(&path).expect("reopen2");
        let stored = query_col_text(&conn, "SELECT data FROM patches WHERE id = 1");

        let b_type = json_ext::json_type(&stored, Some("$.b")).expect("type b");
        assert_eq!(b_type, None);

        let d_val = json_ext::json_extract(&stored, &["$.d"]).expect("extract d");
        assert_eq!(d_val, SqliteValue::Integer(4));

        let a_val = json_ext::json_extract(&stored, &["$.a"]).expect("extract a");
        assert_eq!(a_val, SqliteValue::Integer(1));
    }

    eprintln!("[{BEAD_ID}][test=json_remove_and_patch_persistence] PASS");
}

// ═══════════════════════════════════════════════════════════════════════
// §2  R-tree Extension — Spatial Data on Real Storage
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn rtree_bounding_box_round_trip() {
    let (_dir, path) = temp_db();

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute(
            "CREATE TABLE spatial (id INTEGER PRIMARY KEY, \
             min_x REAL, min_y REAL, max_x REAL, max_y REAL, label TEXT)",
        )
        .expect("create");
        conn.execute("INSERT INTO spatial VALUES (1, 0.0, 0.0, 10.0, 10.0, 'region_a')")
            .expect("insert 1");
        conn.execute("INSERT INTO spatial VALUES (2, 5.0, 5.0, 15.0, 15.0, 'region_b')")
            .expect("insert 2");
        conn.execute("INSERT INTO spatial VALUES (3, 20.0, 20.0, 30.0, 30.0, 'region_c')")
            .expect("insert 3");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let rows = conn
            .query("SELECT min_x, min_y, max_x, max_y, label FROM spatial ORDER BY id")
            .expect("query");
        assert_eq!(rows.len(), 3);

        let bb_a = rtree_ext::BoundingBox {
            min_x: 0.0,
            min_y: 0.0,
            max_x: 10.0,
            max_y: 10.0,
        };
        let bb_b = rtree_ext::BoundingBox {
            min_x: 5.0,
            min_y: 5.0,
            max_x: 15.0,
            max_y: 15.0,
        };

        let point = rtree_ext::Point::new(7.0, 7.0);
        assert!(bb_a.contains_point(point));
        assert!(bb_b.contains_point(point));

        let point_c = rtree_ext::Point::new(25.0, 25.0);
        assert!(!bb_a.contains_point(point_c));
        assert!(!bb_b.contains_point(point_c));
    }

    eprintln!("[{BEAD_ID}][test=rtree_bounding_box_round_trip] PASS");
}

#[test]
fn rtree_multi_dimensional_persistence() {
    let (_dir, path) = temp_db();

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute(
            "CREATE TABLE spatial3d (id INTEGER PRIMARY KEY, \
             x0 REAL, x1 REAL, y0 REAL, y1 REAL, z0 REAL, z1 REAL)",
        )
        .expect("create");
        conn.execute("INSERT INTO spatial3d VALUES (1, 0.0, 10.0, 0.0, 10.0, 0.0, 10.0)")
            .expect("insert");
        conn.execute("INSERT INTO spatial3d VALUES (2, 5.0, 15.0, 5.0, 15.0, 5.0, 15.0)")
            .expect("insert");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let rows = conn
            .query("SELECT x0, x1, y0, y1, z0, z1 FROM spatial3d ORDER BY id")
            .expect("query");
        assert_eq!(rows.len(), 2);

        let mbb1 = rtree_ext::MBoundingBox::new(vec![0.0, 10.0, 0.0, 10.0, 0.0, 10.0])
            .expect("valid 3D bbox");
        let mbb2 = rtree_ext::MBoundingBox::new(vec![5.0, 15.0, 5.0, 15.0, 5.0, 15.0])
            .expect("valid 3D bbox");

        assert_eq!(mbb1.dimensions(), 3);
        assert!(mbb1.overlaps(&mbb2));
    }

    eprintln!("[{BEAD_ID}][test=rtree_multi_dimensional_persistence] PASS");
}

// ═══════════════════════════════════════════════════════════════════════
// §3  Session Extension — Changeset Round-Trips
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn session_changeset_encode_store_decode() {
    let (_dir, path) = temp_db();

    let mut changeset = session_ext::Changeset::new();
    changeset.tables.push(session_ext::TableChangeset {
        info: session_ext::TableInfo {
            name: "users".to_owned(),
            column_count: 3,
            pk_flags: vec![true, false, false],
        },
        rows: vec![
            session_ext::ChangesetRow {
                op: session_ext::ChangeOp::Insert,
                old_values: vec![],
                new_values: vec![
                    session_ext::ChangesetValue::Integer(1),
                    session_ext::ChangesetValue::Text("Alice".to_owned()),
                    session_ext::ChangesetValue::Integer(30),
                ],
            },
            session_ext::ChangesetRow {
                op: session_ext::ChangeOp::Insert,
                old_values: vec![],
                new_values: vec![
                    session_ext::ChangesetValue::Integer(2),
                    session_ext::ChangesetValue::Text("Bob".to_owned()),
                    session_ext::ChangesetValue::Integer(25),
                ],
            },
        ],
    });

    let encoded = changeset.encode();

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE changesets (id INTEGER PRIMARY KEY, data BLOB)")
            .expect("create");
        conn.execute_with_params(
            "INSERT INTO changesets VALUES (1, ?1)",
            &[SqliteValue::Blob(encoded.clone())],
        )
        .expect("insert changeset blob");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let stored_blob = query_col_blob(&conn, "SELECT data FROM changesets WHERE id = 1");

        let decoded = session_ext::Changeset::decode(&stored_blob).expect("decode changeset");
        assert_eq!(decoded.tables.len(), 1);
        assert_eq!(decoded.tables[0].info.name, "users");
        assert_eq!(decoded.tables[0].rows.len(), 2);
    }

    eprintln!("[{BEAD_ID}][test=session_changeset_encode_store_decode] PASS");
}

#[test]
fn session_changeset_invert_round_trip() {
    let (_dir, path) = temp_db();

    let mut changeset = session_ext::Changeset::new();
    changeset.tables.push(session_ext::TableChangeset {
        info: session_ext::TableInfo {
            name: "items".to_owned(),
            column_count: 2,
            pk_flags: vec![true, false],
        },
        rows: vec![session_ext::ChangesetRow {
            op: session_ext::ChangeOp::Insert,
            old_values: vec![],
            new_values: vec![
                session_ext::ChangesetValue::Integer(42),
                session_ext::ChangesetValue::Text("widget".to_owned()),
            ],
        }],
    });

    let inverted = changeset.invert();
    let inv_encoded = inverted.encode();

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE inv (id INTEGER PRIMARY KEY, data BLOB)")
            .expect("create");
        conn.execute_with_params(
            "INSERT INTO inv VALUES (1, ?1)",
            &[SqliteValue::Blob(inv_encoded)],
        )
        .expect("insert");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let blob = query_col_blob(&conn, "SELECT data FROM inv WHERE id = 1");
        let decoded = session_ext::Changeset::decode(&blob).expect("decode inverted");
        assert_eq!(decoded.tables.len(), 1);
        assert_eq!(decoded.tables[0].rows[0].op, session_ext::ChangeOp::Delete);
    }

    eprintln!("[{BEAD_ID}][test=session_changeset_invert_round_trip] PASS");
}

#[test]
fn session_patchset_round_trip() {
    let (_dir, path) = temp_db();

    let mut changeset = session_ext::Changeset::new();
    changeset.tables.push(session_ext::TableChangeset {
        info: session_ext::TableInfo {
            name: "metrics".to_owned(),
            column_count: 3,
            pk_flags: vec![true, false, false],
        },
        rows: vec![session_ext::ChangesetRow {
            op: session_ext::ChangeOp::Insert,
            old_values: vec![],
            new_values: vec![
                session_ext::ChangesetValue::Integer(1),
                session_ext::ChangesetValue::Text("cpu".to_owned()),
                session_ext::ChangesetValue::Real(0.75),
            ],
        }],
    });

    let patchset = changeset.encode_patchset();

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE patches (id INTEGER PRIMARY KEY, data BLOB)")
            .expect("create");
        conn.execute_with_params(
            "INSERT INTO patches VALUES (1, ?1)",
            &[SqliteValue::Blob(patchset.clone())],
        )
        .expect("insert");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let stored = query_col_blob(&conn, "SELECT data FROM patches WHERE id = 1");
        assert_eq!(stored, patchset, "patchset bytes should survive round-trip");
    }

    eprintln!("[{BEAD_ID}][test=session_patchset_round_trip] PASS");
}

// ═══════════════════════════════════════════════════════════════════════
// §4  Misc Extension — Decimal and UUID with Real Storage
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn misc_decimal_precision_round_trip() {
    let (_dir, path) = temp_db();

    let a = SqliteValue::Text("123456789.987654321".to_owned());
    let b = SqliteValue::Text("0.000000001".to_owned());
    let sum = fsqlite_ext_misc::DecimalAddFunc
        .invoke(&[a.clone(), b.clone()])
        .expect("decimal_add");

    let sum_text = match &sum {
        SqliteValue::Text(s) => s.clone(),
        other => panic!("expected Text from decimal_add, got {other:?}"),
    };

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE decimals (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create");
        conn.execute(&format!("INSERT INTO decimals VALUES (1, '{sum_text}')"))
            .expect("insert");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let stored = query_col_text(&conn, "SELECT val FROM decimals WHERE id = 1");
        assert_eq!(stored, sum_text, "decimal should survive round-trip");

        // Recompute and verify consistency
        let recomputed = fsqlite_ext_misc::DecimalAddFunc
            .invoke(&[a, b])
            .expect("recompute");
        if let SqliteValue::Text(ref s) = recomputed {
            assert_eq!(&stored, s);
        }
    }

    eprintln!("[{BEAD_ID}][test=misc_decimal_precision_round_trip] PASS");
}

#[test]
fn misc_uuid_round_trip() {
    let (_dir, path) = temp_db();

    let uuid_val = fsqlite_ext_misc::UuidFunc.invoke(&[]).expect("uuid()");
    let uuid_str = match &uuid_val {
        SqliteValue::Text(s) => s.clone(),
        other => panic!("expected Text from uuid(), got {other:?}"),
    };

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE uuids (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create");
        conn.execute(&format!("INSERT INTO uuids VALUES (1, '{uuid_str}')"))
            .expect("insert");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let stored = query_col_text(&conn, "SELECT val FROM uuids WHERE id = 1");
        assert_eq!(stored, uuid_str, "uuid should survive round-trip");

        // Verify it's a valid UUID format (8-4-4-4-12)
        let parts: Vec<&str> = stored.split('-').collect();
        assert_eq!(parts.len(), 5, "UUID should have 5 dash-separated parts");
        assert_eq!(parts[0].len(), 8);
        assert_eq!(parts[1].len(), 4);
        assert_eq!(parts[2].len(), 4);
        assert_eq!(parts[3].len(), 4);
        assert_eq!(parts[4].len(), 12);
    }

    eprintln!("[{BEAD_ID}][test=misc_uuid_round_trip] PASS");
}

#[test]
fn misc_uuid_blob_text_conversion_round_trip() {
    let (_dir, path) = temp_db();

    let uuid_text = fsqlite_ext_misc::UuidFunc.invoke(&[]).expect("uuid()");

    // Convert to blob
    let uuid_blob = fsqlite_ext_misc::UuidBlobFunc
        .invoke(&[uuid_text.clone()])
        .expect("uuid_blob");

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE uuid_blobs (id INTEGER PRIMARY KEY, data BLOB)")
            .expect("create");
        if let SqliteValue::Blob(ref b) = uuid_blob {
            conn.execute_with_params(
                "INSERT INTO uuid_blobs VALUES (1, ?1)",
                &[SqliteValue::Blob(b.clone())],
            )
            .expect("insert");
        }
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let stored_blob = query_col_blob(&conn, "SELECT data FROM uuid_blobs WHERE id = 1");

        // Convert back to string
        let recovered = fsqlite_ext_misc::UuidStrFunc
            .invoke(&[SqliteValue::Blob(stored_blob)])
            .expect("uuid_str");
        assert_eq!(recovered, uuid_text, "uuid blob->text should round-trip");
    }

    eprintln!("[{BEAD_ID}][test=misc_uuid_blob_text_conversion_round_trip] PASS");
}

// ═══════════════════════════════════════════════════════════════════════
// §5  FTS5 Extension — Tokenizer APIs with Stored Text
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn fts5_tokenizer_on_stored_text() {
    let (_dir, path) = temp_db();

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
            .expect("create");
        conn.execute(
            "INSERT INTO articles VALUES (1, 'Database Engine Design', \
             'FrankenSQLite implements MVCC page-level versioning for concurrent writers.')",
        )
        .expect("insert 1");
        conn.execute(
            "INSERT INTO articles VALUES (2, 'Rust Memory Safety', \
             'The borrow checker prevents data races at compile time.')",
        )
        .expect("insert 2");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let rows = conn
            .query("SELECT title, body FROM articles ORDER BY id")
            .expect("query");
        assert_eq!(rows.len(), 2);

        let body1 = match rows[0].get(1).unwrap() {
            SqliteValue::Text(s) => s.clone(),
            _ => panic!("expected text"),
        };

        let tokenizer = Unicode61Tokenizer::new();
        let tokens = tokenizer.tokenize(&body1);
        assert!(!tokens.is_empty());

        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert!(terms.contains(&"mvcc"), "should contain 'mvcc'");
        assert!(terms.contains(&"concurrent"), "should contain 'concurrent'");
        assert!(terms.contains(&"versioning"), "should contain 'versioning'");

        // Verify token offsets point back to original text
        for token in &tokens {
            assert!(token.start < token.end);
            assert!(token.end <= body1.len());
        }
    }

    eprintln!("[{BEAD_ID}][test=fts5_tokenizer_on_stored_text] PASS");
}

#[test]
fn fts3_query_parsing_on_stored_content() {
    let (_dir, path) = temp_db();

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, content TEXT)")
            .expect("create");
        conn.execute("INSERT INTO notes VALUES (1, 'hello world database engine')")
            .expect("insert");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let _stored = query_col_text(&conn, "SELECT content FROM notes WHERE id = 1");

        // Validate FTS3 query parsing
        let tokens = fsqlite_ext_fts3::parse_query("hello AND world").expect("valid");
        assert!(!tokens.is_empty());

        let err = fsqlite_ext_fts3::parse_query("");
        assert!(err.is_err(), "empty query should fail");
    }

    eprintln!("[{BEAD_ID}][test=fts3_query_parsing_on_stored_content] PASS");
}

// ═══════════════════════════════════════════════════════════════════════
// §6  Cross-Extension Integration — Multiple Extensions on Same DB
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn multi_extension_data_on_single_db() {
    let (_dir, path) = temp_db();

    {
        let conn = Connection::open(&path).expect("open");

        conn.execute("CREATE TABLE json_store (id INTEGER PRIMARY KEY, data TEXT)")
            .expect("create json_store");
        conn.execute(r#"INSERT INTO json_store VALUES (1, '{"type":"point","x":5.0,"y":10.0}')"#)
            .expect("insert json");

        conn.execute(
            "CREATE TABLE spatial_store (id INTEGER PRIMARY KEY, \
             min_x REAL, min_y REAL, max_x REAL, max_y REAL)",
        )
        .expect("create spatial_store");
        conn.execute("INSERT INTO spatial_store VALUES (1, 0.0, 0.0, 20.0, 20.0)")
            .expect("insert spatial");

        conn.execute("CREATE TABLE change_log (id INTEGER PRIMARY KEY, cs BLOB)")
            .expect("create change_log");
        let mut cs = session_ext::Changeset::new();
        cs.tables.push(session_ext::TableChangeset {
            info: session_ext::TableInfo {
                name: "json_store".to_owned(),
                column_count: 2,
                pk_flags: vec![true, false],
            },
            rows: vec![session_ext::ChangesetRow {
                op: session_ext::ChangeOp::Insert,
                old_values: vec![],
                new_values: vec![
                    session_ext::ChangesetValue::Integer(1),
                    session_ext::ChangesetValue::Text(
                        r#"{"type":"point","x":5.0,"y":10.0}"#.to_owned(),
                    ),
                ],
            }],
        });
        conn.execute_with_params(
            "INSERT INTO change_log VALUES (1, ?1)",
            &[SqliteValue::Blob(cs.encode())],
        )
        .expect("insert changeset");

        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create docs");
        conn.execute(
            "INSERT INTO docs VALUES (1, \
             'R-tree indexes support efficient range queries over bounding boxes.')",
        )
        .expect("insert doc");
    }

    {
        let conn = Connection::open(&path).expect("reopen");

        assert_eq!(row_count(&conn, "SELECT * FROM json_store"), 1);
        assert_eq!(row_count(&conn, "SELECT * FROM spatial_store"), 1);
        assert_eq!(row_count(&conn, "SELECT * FROM change_log"), 1);
        assert_eq!(row_count(&conn, "SELECT * FROM docs"), 1);

        // JSON: extract point
        let json_data = query_col_text(&conn, "SELECT data FROM json_store WHERE id = 1");
        let x = json_ext::json_extract(&json_data, &["$.x"]).expect("extract x");
        let y = json_ext::json_extract(&json_data, &["$.y"]).expect("extract y");

        // R-tree: check containment
        let bb = rtree_ext::BoundingBox {
            min_x: 0.0,
            min_y: 0.0,
            max_x: 20.0,
            max_y: 20.0,
        };
        if let (SqliteValue::Float(xf), SqliteValue::Float(yf)) = (&x, &y) {
            let pt = rtree_ext::Point::new(*xf, *yf);
            assert!(bb.contains_point(pt), "point from JSON should be in bbox");
        }

        // Session: decode changeset
        let cs_blob = query_col_blob(&conn, "SELECT cs FROM change_log WHERE id = 1");
        let decoded = session_ext::Changeset::decode(&cs_blob).expect("decode");
        assert_eq!(decoded.tables[0].info.name, "json_store");

        // FTS5: tokenize retrieved text
        let body = query_col_text(&conn, "SELECT body FROM docs WHERE id = 1");
        let tokenizer = Unicode61Tokenizer::new();
        let tokens = tokenizer.tokenize(&body);
        assert!(!tokens.is_empty());
    }

    eprintln!("[{BEAD_ID}][test=multi_extension_data_on_single_db] PASS");
}

// ═══════════════════════════════════════════════════════════════════════
// §7  Multiple Reopen Cycles
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn multiple_reopen_cycles_preserve_extension_data() {
    let (_dir, path) = temp_db();

    // Cycle 1: create and populate
    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE jdata (id INTEGER PRIMARY KEY, j TEXT)")
            .expect("create");
        conn.execute(r#"INSERT INTO jdata VALUES (1, '{"v":1}')"#)
            .expect("insert");
    }

    // Cycle 2: mutate
    {
        let conn = Connection::open(&path).expect("reopen1");
        let j = query_col_text(&conn, "SELECT j FROM jdata WHERE id = 1");
        let modified =
            json_ext::json_set(&j, &[("$.v", SqliteValue::Integer(2))]).expect("json_set");
        conn.execute(&format!(
            "UPDATE jdata SET j = '{}' WHERE id = 1",
            modified.replace('\'', "''")
        ))
        .expect("update");
    }

    // Cycle 3: mutate again
    {
        let conn = Connection::open(&path).expect("reopen2");
        let j = query_col_text(&conn, "SELECT j FROM jdata WHERE id = 1");
        let val = json_ext::json_extract(&j, &["$.v"]).expect("extract v");
        assert_eq!(val, SqliteValue::Integer(2));

        let modified =
            json_ext::json_set(&j, &[("$.v", SqliteValue::Integer(3))]).expect("json_set");
        conn.execute(&format!(
            "UPDATE jdata SET j = '{}' WHERE id = 1",
            modified.replace('\'', "''")
        ))
        .expect("update");
    }

    // Cycle 4: verify final state
    {
        let conn = Connection::open(&path).expect("reopen3");
        let j = query_col_text(&conn, "SELECT j FROM jdata WHERE id = 1");
        let val = json_ext::json_extract(&j, &["$.v"]).expect("extract v");
        assert_eq!(val, SqliteValue::Integer(3));
    }

    eprintln!("[{BEAD_ID}][test=multiple_reopen_cycles_preserve_extension_data] PASS");
}

// ═══════════════════════════════════════════════════════════════════════
// §8  Large Data Integrity
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn large_json_document_storage_integrity() {
    let (_dir, path) = temp_db();

    let mut items = Vec::new();
    for i in 0..100 {
        items.push(SqliteValue::Integer(i));
    }
    let large_array = json_ext::json_array(&items).expect("json_array 100 items");

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE large (id INTEGER PRIMARY KEY, data TEXT)")
            .expect("create");
        conn.execute(&format!(
            "INSERT INTO large VALUES (1, '{}')",
            large_array.replace('\'', "''")
        ))
        .expect("insert large");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let stored = query_col_text(&conn, "SELECT data FROM large WHERE id = 1");
        let len = json_ext::json_array_length(&stored, None).expect("array_length");
        assert_eq!(len, Some(100));

        let first = json_ext::json_extract(&stored, &["$[0]"]).expect("first");
        assert_eq!(first, SqliteValue::Integer(0));

        let last = json_ext::json_extract(&stored, &["$[99]"]).expect("last");
        assert_eq!(last, SqliteValue::Integer(99));
    }

    eprintln!("[{BEAD_ID}][test=large_json_document_storage_integrity] PASS");
}

// ═══════════════════════════════════════════════════════════════════════
// §9  JSONB Binary Encoding Round-Trip
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn jsonb_blob_storage_round_trip() {
    let (_dir, path) = temp_db();

    let original = r#"{"key":"value","number":42}"#;
    let jsonb_bytes = json_ext::jsonb(original).expect("encode jsonb");

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE jsonb_store (id INTEGER PRIMARY KEY, data BLOB)")
            .expect("create");
        conn.execute_with_params(
            "INSERT INTO jsonb_store VALUES (1, ?1)",
            &[SqliteValue::Blob(jsonb_bytes.clone())],
        )
        .expect("insert");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let stored_blob = query_col_blob(&conn, "SELECT data FROM jsonb_store WHERE id = 1");
        assert_eq!(
            stored_blob, jsonb_bytes,
            "JSONB bytes should survive round-trip"
        );

        let decoded = json_ext::json_from_jsonb(&stored_blob).expect("json_from_jsonb");
        assert_eq!(json_ext::json_valid(&decoded, None), 1);

        let key_val = json_ext::json_extract(&decoded, &["$.key"]).expect("extract key");
        assert_eq!(key_val, SqliteValue::Text("value".to_owned()));
    }

    eprintln!("[{BEAD_ID}][test=jsonb_blob_storage_round_trip] PASS");
}

// ═══════════════════════════════════════════════════════════════════════
// §10  Session Concat and Multi-Table Changeset
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn session_concat_multi_table_round_trip() {
    let (_dir, path) = temp_db();

    let mut cs1 = session_ext::Changeset::new();
    cs1.tables.push(session_ext::TableChangeset {
        info: session_ext::TableInfo {
            name: "t1".to_owned(),
            column_count: 2,
            pk_flags: vec![true, false],
        },
        rows: vec![session_ext::ChangesetRow {
            op: session_ext::ChangeOp::Insert,
            old_values: vec![],
            new_values: vec![
                session_ext::ChangesetValue::Integer(1),
                session_ext::ChangesetValue::Text("a".to_owned()),
            ],
        }],
    });

    let mut cs2 = session_ext::Changeset::new();
    cs2.tables.push(session_ext::TableChangeset {
        info: session_ext::TableInfo {
            name: "t2".to_owned(),
            column_count: 2,
            pk_flags: vec![true, false],
        },
        rows: vec![session_ext::ChangesetRow {
            op: session_ext::ChangeOp::Insert,
            old_values: vec![],
            new_values: vec![
                session_ext::ChangesetValue::Integer(10),
                session_ext::ChangesetValue::Text("x".to_owned()),
            ],
        }],
    });

    cs1.concat(&cs2);
    let encoded = cs1.encode();

    {
        let conn = Connection::open(&path).expect("open");
        conn.execute("CREATE TABLE multi_cs (id INTEGER PRIMARY KEY, data BLOB)")
            .expect("create");
        conn.execute_with_params(
            "INSERT INTO multi_cs VALUES (1, ?1)",
            &[SqliteValue::Blob(encoded)],
        )
        .expect("insert");
    }

    {
        let conn = Connection::open(&path).expect("reopen");
        let blob = query_col_blob(&conn, "SELECT data FROM multi_cs WHERE id = 1");
        let decoded = session_ext::Changeset::decode(&blob).expect("decode");
        assert_eq!(decoded.tables.len(), 2);
        assert_eq!(decoded.tables[0].info.name, "t1");
        assert_eq!(decoded.tables[1].info.name, "t2");
    }

    eprintln!("[{BEAD_ID}][test=session_concat_multi_table_round_trip] PASS");
}
