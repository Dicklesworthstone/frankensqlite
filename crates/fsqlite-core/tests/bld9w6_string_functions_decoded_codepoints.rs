#![recursion_limit = "512"]

//! bld9w-6 (VDBE/STRING FUNCTIONS): verifies that `length`, `substr`, `instr`,
//! `replace`, and `typeof` operate on the DECODED code-point form of TEXT, not
//! on raw bytes. On a UTF-16 database the record decoder (bd-bld9w.2,
//! `SqliteValue::from_record_text_bytes`) yields canonical UTF-8 `SmallText`, so
//! a decoded UTF-16 value is byte-identical to its UTF-8 twin and the string
//! functions return identical results. This battery pins the code-point
//! semantics on multibyte UTF-8 text (2-/3-/4-byte scalars), which is the
//! strongest oracle possible before the UTF-16 admission-gate lift (bd-bld9w.3);
//! the end-to-end UTF-16-database oracle is folded into that slice. rusqlite is
//! the oracle.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let mut fr: Vec<Vec<String>> = fconn.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}")).iter().map(|r| r.values().iter().map(tag_f).collect()).collect();
    fr.sort();
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    let mut rr: Vec<Vec<String>> = st.query_map([], |row| Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    rr.sort();
    assert_eq!(fr, rr, "string-function code-point mismatch on `{sql}`");
}

async fn seed(fconn: &Connection, rconn: &rusqlite::Connection) {
    // Multibyte scalars: 'é'=2 bytes, '€'=3 bytes, '𐍈'/'🦀'=4 bytes (surrogate
    // pair in UTF-16 — the exact case the UTF-16 decoder must round-trip).
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)",
        "INSERT INTO t VALUES (1,'café'),(2,'€uro'),(3,'a🦀b𐍈c'),(4,'naïve'),(5,'')",
    ] {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn bld9w6_length_counts_code_points() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // length() = code-point count, NOT byte count.
        assert_agree(&f, &r, "SELECT id, length(s) FROM t ORDER BY id").await;
        // A BLOB length stays a byte count (stock parity).
        assert_agree(&f, &r, "SELECT length(CAST('café' AS BLOB))").await;
    });
}

#[test]
fn bld9w6_substr_indexes_code_points() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // substr with positive/negative/zero start and multibyte content.
        assert_agree(&f, &r, "SELECT id, substr(s,2) FROM t ORDER BY id").await;
        assert_agree(&f, &r, "SELECT id, substr(s,2,2) FROM t ORDER BY id").await;
        assert_agree(&f, &r, "SELECT id, substr(s,-2) FROM t ORDER BY id").await;
        assert_agree(&f, &r, "SELECT id, substr(s,-2,1) FROM t ORDER BY id").await;
        assert_agree(&f, &r, "SELECT id, substr(s,0,3) FROM t ORDER BY id").await;
    });
}

#[test]
fn bld9w6_instr_and_replace_code_points() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // instr() returns a 1-based code-point position, not a byte offset.
        assert_agree(&f, &r, "SELECT id, instr(s,'🦀') FROM t ORDER BY id").await;
        assert_agree(&f, &r, "SELECT id, instr(s,'é') FROM t ORDER BY id").await;
        assert_agree(&f, &r, "SELECT instr('a🦀b𐍈c','b')").await;
        // replace() operates on decoded text and preserves multibyte scalars.
        assert_agree(&f, &r, "SELECT id, replace(s,'é','E') FROM t ORDER BY id").await;
        assert_agree(&f, &r, "SELECT replace('a🦀b🦀c','🦀','__')").await;
    });
}

#[test]
fn bld9w6_typeof_and_upper_lower_ascii_scope() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // typeof stays 'text' for multibyte content.
        assert_agree(&f, &r, "SELECT id, typeof(s) FROM t ORDER BY id").await;
        // upper()/lower() only fold ASCII in stock SQLite (no ICU) — multibyte
        // scalars pass through unchanged on both engines.
        assert_agree(&f, &r, "SELECT id, upper(s), lower(s) FROM t ORDER BY id").await;
    });
}
