//! File-compatibility matrix (bd-w4pr1.15): every stock database variant opens
//! in FrankenSQLite, takes its writes, and stays stock-valid.
//!
//! For each variant stock SQLite (bundled rusqlite) builds a base database. Two
//! copies take the same deterministic writes, one through FrankenSQLite and one
//! through stock. Stock then gives the verdict on the FrankenSQLite copy:
//! `PRAGMA integrity_check` is `ok`, the schema text equals the stock replay's,
//! and every table holds exactly the rows of the stock replay. Virtual tables
//! are compared through their query surface (and FTS5's own integrity-check),
//! not their shadow-table bytes, whose layout is an implementation detail.
//! Variants FrankenSQLite refuses to write (auto-vacuum, bd-w4pr1.1) must fail
//! with the file left byte-identical.
#![recursion_limit = "512"]

use std::path::{Path, PathBuf};

use rusqlite::config::DbConfig;
use rusqlite::types::Value;
use tempfile::TempDir;

const SUFFIXES: [&str; 4] = ["", "-wal", "-shm", "-journal"];

fn rows(n: u32) -> String {
    format!("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<{n}) ")
}

fn base_table(n: u32, blob: &str) -> String {
    format!(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b BLOB); CREATE INDEX t_a ON t(a);
         {}INSERT INTO t(a, b) SELECT printf('v%05d', x), {blob} FROM c;",
        rows(n)
    )
}

const DML: &str = "INSERT INTO t(a, b) VALUES ('new-1', x'aa'), ('new-2', x'bbbb');
UPDATE t SET a = a || '-u' WHERE id % 7 = 0;
DELETE FROM t WHERE id % 11 = 0;
CREATE INDEX IF NOT EXISTS t_new ON t(b);";

struct Case {
    name: &'static str,
    /// Run by stock before `schema` (page size, journal mode, encoding...).
    setup: &'static str,
    schema: String,
    writes: String,
    /// Copy the files while stock still holds the database open after
    /// committing, so un-checkpointed WAL frames come along.
    keep_wal_frames: bool,
    /// FrankenSQLite must refuse the writes and leave the file unchanged.
    expect_refusal: bool,
    /// Bytes reserved at the end of every page (checksum and encryption VFSes).
    reserved_bytes: u8,
}

impl Case {
    fn new(name: &'static str, setup: &'static str, schema: String, writes: &str) -> Self {
        Self {
            name,
            setup,
            schema,
            writes: writes.to_owned(),
            keep_wal_frames: false,
            expect_refusal: false,
            reserved_bytes: 0,
        }
    }
}

/// Start `path` as an empty stock database whose pages reserve `reserved`
/// bytes, the file `SQLITE_FCNTL_RESERVE_BYTES` produces before the first
/// table: header byte 20 holds the reservation and the empty page 1 starts its
/// cell content area at the usable size.
fn start_with_reserved_bytes(path: &Path, reserved: u8) {
    rusqlite::Connection::open(path)
        .unwrap()
        .execute_batch("PRAGMA user_version = 7;")
        .unwrap();
    let mut bytes = std::fs::read(path).unwrap();
    let page_size = u16::from_be_bytes([bytes[16], bytes[17]]);
    assert!(page_size >= 512, "default page size expected");
    assert_eq!(
        u16::from_be_bytes([bytes[103], bytes[104]]),
        0,
        "page 1 must be empty"
    );
    bytes[20] = reserved;
    let content_start = page_size - u16::from(reserved);
    bytes[105..107].copy_from_slice(&content_start.to_be_bytes());
    std::fs::write(path, bytes).unwrap();
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

fn copy_database(from: &Path, to: &Path) {
    for suffix in SUFFIXES {
        let source = sidecar(from, suffix);
        if source.exists() {
            std::fs::copy(&source, sidecar(to, suffix)).expect("copy database file");
        }
    }
}

/// Every row of `sql`, rendered and sorted.
fn sorted_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).unwrap();
    let width = stmt.column_count();
    let mut rendered: Vec<String> = stmt
        .query_map([], |row| {
            (0..width)
                .map(|i| row.get::<_, Value>(i))
                .collect::<Result<Vec<_>, _>>()
                .map(|values| format!("{values:?}"))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    rendered.sort();
    rendered
}

/// Stock's view of a database: the schema (without root page numbers, which
/// are allocation choices), then every table's rows (sorted), with each
/// virtual table read through its query surface instead of its shadow tables.
fn stock_contents(path: &Path) -> Vec<(String, Vec<String>)> {
    let conn = rusqlite::Connection::open(path).expect("stock opens the database");
    let mut contents = vec![(
        "sqlite_master".to_owned(),
        sorted_rows(&conn, "SELECT type, name, tbl_name, sql FROM sqlite_master"),
    )];
    let mut objects: Vec<(String, String)> = conn
        .prepare(
            "SELECT name, coalesce(sql, '') FROM sqlite_master WHERE type = 'table'
             AND (substr(name, 1, 7) <> 'sqlite_' OR name = 'sqlite_sequence') ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let virtual_tables: Vec<String> = objects
        .iter()
        .filter(|(_, sql)| sql.to_ascii_uppercase().starts_with("CREATE VIRTUAL TABLE"))
        .map(|(name, _)| name.clone())
        .collect();
    // Shadow tables (`f_data`, `r_node`, ...) are the virtual table's storage
    // layout, not its contents.
    objects.retain(|(name, _)| {
        !virtual_tables
            .iter()
            .any(|vt| name.len() > vt.len() && name.starts_with(&format!("{vt}_")))
    });
    for (name, _) in objects {
        // A virtual table's `*` leaves out its rowid; ordinary tables either
        // alias it (INTEGER PRIMARY KEY) or have none (WITHOUT ROWID).
        let rowid = if virtual_tables.contains(&name) {
            "rowid, "
        } else {
            ""
        };
        let table_rows = sorted_rows(&conn, &format!("SELECT {rowid}* FROM \"{name}\""));
        contents.push((name, table_rows));
    }
    contents
}

/// A FrankenSQLite value in the form [`sorted_rows`] renders stock's.
fn stock_value(value: &fsqlite::SqliteValue) -> Value {
    match value {
        fsqlite::SqliteValue::Null => Value::Null,
        fsqlite::SqliteValue::Integer(v) => Value::Integer(*v),
        fsqlite::SqliteValue::Float(v) => Value::Real(*v),
        fsqlite::SqliteValue::Text(v) => Value::Text(v.as_str().to_owned()),
        fsqlite::SqliteValue::Blob(v) => Value::Blob(v.to_vec()),
    }
}

async fn fsqlite_query(path: &Path, sql: &str) -> Result<Vec<Vec<Value>>, String> {
    let conn = fsqlite::Connection::open(path.to_str().unwrap())
        .await
        .map_err(|error| format!("open: {error}"))?;
    let rows = conn
        .query(sql)
        .await
        .map_err(|error| format!("query: {error}"))?;
    conn.close()
        .await
        .map_err(|error| format!("close: {error}"))?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(stock_value).collect())
        .collect())
}

/// Every row FrankenSQLite reads for `sql`, rendered and sorted as
/// [`sorted_rows`] renders stock's.
fn fsqlite_rows(path: &Path, sql: &str) -> Vec<String> {
    let mut outcome = Err("the runtime did not run the query".to_owned());
    let slot = &mut outcome;
    asupersync::test_utils::run_test(|| async move {
        *slot = fsqlite_query(path, sql).await;
    });
    let mut rendered: Vec<String> = outcome
        .unwrap_or_else(|error| panic!("FrankenSQLite `{sql}`: {error}"))
        .iter()
        .map(|values| format!("{values:?}"))
        .collect();
    rendered.sort();
    rendered
}

fn stock_integrity(path: &Path) -> String {
    let conn = rusqlite::Connection::open(path).expect("stock opens the database");
    conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check runs")
}

async fn fsqlite_write(path: &Path, sql: &str) -> Result<(), String> {
    let conn = fsqlite::Connection::open(path.to_str().unwrap())
        .await
        .map_err(|error| format!("open: {error}"))?;
    conn.execute_batch(sql)
        .await
        .map_err(|error| format!("write: {error}"))?;
    conn.close()
        .await
        .map_err(|error| format!("close: {error}"))
}

fn fsqlite_execute(path: &Path, sql: &str) -> Result<(), String> {
    let mut outcome = Err("the runtime did not run the writes".to_owned());
    let slot = &mut outcome;
    asupersync::test_utils::run_test(|| async move {
        *slot = fsqlite_write(path, sql).await;
    });
    outcome
}

fn run_case(case: &Case) {
    let dir = TempDir::new().unwrap();
    let base = dir.path().join("base.db");
    let fsq = dir.path().join("fsqlite.db");
    let stock = dir.path().join("stock.db");
    if case.reserved_bytes > 0 {
        start_with_reserved_bytes(&base, case.reserved_bytes);
    }
    {
        let conn = rusqlite::Connection::open(&base).unwrap();
        conn.execute_batch(case.setup).unwrap();
        conn.execute_batch(&case.schema).unwrap();
        if case.keep_wal_frames {
            // Copy while stock holds the database open: closing would
            // checkpoint the WAL away.
            copy_database(&base, &fsq);
            copy_database(&base, &stock);
            assert!(
                sidecar(&fsq, "-wal").exists(),
                "[{}] the copy must carry a WAL",
                case.name
            );
        }
    }
    if !case.keep_wal_frames {
        copy_database(&base, &fsq);
        copy_database(&base, &stock);
    }
    assert_eq!(
        stock_integrity(&stock),
        "ok",
        "[{}] the stock-built base must be valid",
        case.name
    );
    let before = std::fs::read(&fsq).unwrap();
    let result = fsqlite_execute(&fsq, &case.writes);
    if case.expect_refusal {
        assert!(
            result.is_err(),
            "[{}] FrankenSQLite must refuse these writes",
            case.name
        );
        assert_eq!(
            std::fs::read(&fsq).unwrap(),
            before,
            "[{}] a refused write must leave the file byte-identical",
            case.name
        );
        return;
    }
    result.unwrap_or_else(|error| panic!("[{}] FrankenSQLite writes failed: {error}", case.name));
    rusqlite::Connection::open(&stock)
        .unwrap()
        .execute_batch(&case.writes)
        .unwrap_or_else(|error| panic!("[{}] stock replay failed: {error}", case.name));
    assert_eq!(
        stock_integrity(&fsq),
        "ok",
        "[{}] stock integrity_check of the FrankenSQLite-written file",
        case.name
    );
    assert_eq!(
        std::fs::read(&fsq).unwrap()[20],
        case.reserved_bytes,
        "[{}] the per-page reservation must survive",
        case.name
    );
    let ours = stock_contents(&fsq);
    let theirs = stock_contents(&stock);
    assert_eq!(
        ours.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        theirs.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        "[{}] same tables",
        case.name
    );
    for ((name, our_rows), (_, their_rows)) in ours.iter().zip(&theirs) {
        assert_eq!(
            our_rows.len(),
            their_rows.len(),
            "[{}] row count of {name}",
            case.name
        );
        let first_difference = our_rows
            .iter()
            .zip(their_rows)
            .find(|(ours, theirs)| ours != theirs);
        assert!(
            first_difference.is_none(),
            "[{}] {name} differs from the stock replay: FrankenSQLite {:?} vs stock {:?}",
            case.name,
            first_difference.map(|(ours, _)| ours),
            first_difference.map(|(_, theirs)| theirs)
        );
    }
}

/// Control: the comparison sees one changed value and a schema-only change,
/// so a matrix pass is not vacuous.
#[test]
fn comparison_detects_one_changed_value_and_a_schema_change() {
    let dir = TempDir::new().unwrap();
    let original = dir.path().join("original.db");
    let changed = dir.path().join("changed.db");
    rusqlite::Connection::open(&original)
        .unwrap()
        .execute_batch(&base_table(50, "x'00'"))
        .unwrap();
    copy_database(&original, &changed);
    let reference = stock_contents(&original);
    assert_eq!(reference, stock_contents(&changed));
    assert!(
        reference
            .iter()
            .any(|(name, rows)| name == "t" && rows.len() == 50)
    );
    rusqlite::Connection::open(&changed)
        .unwrap()
        .execute_batch("UPDATE t SET a = 'changed' WHERE id = 17;")
        .unwrap();
    assert_ne!(reference, stock_contents(&changed));
    copy_database(&original, &changed);
    rusqlite::Connection::open(&changed)
        .unwrap()
        .execute_batch("CREATE INDEX t_extra ON t(b);")
        .unwrap();
    assert_ne!(reference, stock_contents(&changed));
}

#[test]
fn page_sizes_512_to_65536() {
    for (name, setup) in [
        ("page_size_512", "PRAGMA page_size = 512;"),
        ("page_size_1024", "PRAGMA page_size = 1024;"),
        ("page_size_2048", "PRAGMA page_size = 2048;"),
        ("page_size_4096", "PRAGMA page_size = 4096;"),
        ("page_size_8192", "PRAGMA page_size = 8192;"),
        ("page_size_16384", "PRAGMA page_size = 16384;"),
        ("page_size_32768", "PRAGMA page_size = 32768;"),
        ("page_size_65536", "PRAGMA page_size = 65536;"),
    ] {
        run_case(&Case::new(
            name,
            setup,
            base_table(300, "randomblob(x % 900)"),
            DML,
        ));
    }
}

#[test]
fn journal_modes() {
    for (name, setup) in [
        ("journal_delete", "PRAGMA journal_mode = DELETE;"),
        ("journal_truncate", "PRAGMA journal_mode = TRUNCATE;"),
        ("journal_persist", "PRAGMA journal_mode = PERSIST;"),
        ("journal_wal", "PRAGMA journal_mode = WAL;"),
    ] {
        run_case(&Case::new(name, setup, base_table(300, "zeroblob(x)"), DML));
    }
}

#[test]
fn un_checkpointed_wal_left_by_stock() {
    let mut case = Case::new(
        "wal_uncheckpointed",
        "PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0;",
        base_table(300, "randomblob(x % 700)") + "UPDATE t SET a = a || '-w' WHERE id < 50;",
        DML,
    );
    case.keep_wal_frames = true;
    run_case(&case);
}

#[test]
fn reserved_bytes_at_page_end() {
    for (name, reserved) in [("reserved_bytes_8", 8), ("reserved_bytes_32", 32)] {
        let mut case = Case::new(name, "", base_table(300, "randomblob(x % 500)"), DML);
        case.reserved_bytes = reserved;
        run_case(&case);
    }
}

#[test]
fn utf16_encodings() {
    let writes = "INSERT INTO t(a, b) VALUES ('new-1', 'ü'), ('new-2', 'é');
UPDATE t SET a = a || '-ü' WHERE id % 7 = 0;
DELETE FROM t WHERE id % 11 = 0;";
    for (name, setup) in [
        ("utf16le", "PRAGMA encoding = 'UTF-16le';"),
        ("utf16be", "PRAGMA encoding = 'UTF-16be';"),
    ] {
        run_case(&Case::new(
            name,
            setup,
            format!(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
                 {}INSERT INTO t(a, b) SELECT printf('v%05d', x), 'ünïcödé ' || x FROM c;",
                rows(300)
            ),
            writes,
        ));
    }
}

#[test]
fn table_kinds_and_schema_features() {
    let cases = [
        Case::new(
            "without_rowid",
            "",
            format!(
                "CREATE TABLE t(k TEXT PRIMARY KEY, a INTEGER, b TEXT) WITHOUT ROWID;
                 {}INSERT INTO t SELECT printf('k%05d', x), x, printf('b%d', x) FROM c;",
                rows(300)
            ),
            "INSERT INTO t VALUES ('n1', 1, 'x'), ('n2', 2, 'y');
UPDATE t SET b = b || '-u' WHERE a % 7 = 0;
DELETE FROM t WHERE a % 11 = 0;",
        ),
        Case::new(
            "strict",
            "",
            format!(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER) STRICT;
                 {}INSERT INTO t(a, b) SELECT printf('v%05d', x), x FROM c;",
                rows(300)
            ),
            "INSERT INTO t(a, b) VALUES ('new-1', 1), ('new-2', 2);
UPDATE t SET a = a || '-u' WHERE id % 7 = 0;
DELETE FROM t WHERE id % 11 = 0;",
        ),
        Case::new(
            "generated_columns",
            "",
            format!(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b BLOB,
                   g1 INTEGER GENERATED ALWAYS AS (length(a)) VIRTUAL,
                   g2 TEXT GENERATED ALWAYS AS (upper(a)) STORED);
                 {}INSERT INTO t(a, b) SELECT printf('v%05d', x), x'00' FROM c;",
                rows(300)
            ),
            DML,
        ),
        Case::new(
            "autoincrement",
            "",
            format!(
                "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, a TEXT, b BLOB);
                 {}INSERT INTO t(a, b) SELECT printf('v%05d', x), x'01' FROM c;
                 DELETE FROM t WHERE id > 290;",
                rows(300)
            ),
            DML,
        ),
        Case::new(
            "partial_and_expression_indexes",
            "",
            format!(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b BLOB);
                 CREATE INDEX t_p ON t(a) WHERE id % 2 = 0;
                 CREATE INDEX t_e ON t(lower(a), id);
                 {}INSERT INTO t(a, b) SELECT printf('V%05d', x), x'02' FROM c;",
                rows(300)
            ),
            DML,
        ),
        Case::new(
            "triggers_and_views",
            "",
            format!(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b BLOB);
                 CREATE TABLE log(n INTEGER);
                 CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES (new.id); END;
                 CREATE VIEW v AS SELECT id, a FROM t WHERE id % 3 = 0;
                 {}INSERT INTO t(a, b) SELECT printf('v%05d', x), x'03' FROM c;",
                rows(300)
            ),
            DML,
        ),
    ];
    for case in &cases {
        run_case(case);
    }
}

/// bd-w4pr1.31: a VIRTUAL generated column before ordinary and STORED
/// columns. Stock's record leaves the VIRTUAL column out, so every later
/// column sits one field earlier than its declared position.
fn virtual_before_stored_table(n: u32) -> String {
    format!(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT,
           v INTEGER GENERATED ALWAYS AS (length(a)) VIRTUAL,
           c TEXT, s TEXT GENERATED ALWAYS AS (a || c) STORED, d TEXT UNIQUE);
         CREATE INDEX t_c ON t(c);
         CREATE INDEX t_v ON t(v);
         {}INSERT INTO t(a, c, d) SELECT printf('v%d', x), printf('c%d', x % 9), printf('d%05d', x)
           FROM c;",
        rows(n)
    )
}

/// A WITHOUT ROWID table with a non-leading PRIMARY KEY and a VIRTUAL column:
/// stock stores `[k, a, d, s]`.
fn without_rowid_virtual_table(n: u32) -> String {
    format!(
        "CREATE TABLE w(a TEXT, v GENERATED ALWAYS AS (length(a)) VIRTUAL, k TEXT PRIMARY KEY,
           d TEXT, s GENERATED ALWAYS AS (a || d) STORED) WITHOUT ROWID;
         CREATE INDEX w_d ON w(d);
         {}INSERT INTO w(a, k, d) SELECT printf('a%d', x), printf('k%05d', x), printf('d%d', x % 7)
           FROM c;",
        rows(n)
    )
}

/// The INTEGER PRIMARY KEY after a VIRTUAL column: stock stores `[NULL, b]`.
fn ipk_after_virtual_table(n: u32) -> String {
    format!(
        "CREATE TABLE p(v INTEGER GENERATED ALWAYS AS (id * 2) VIRTUAL, id INTEGER PRIMARY KEY,
           b TEXT);
         {}INSERT INTO p(b) SELECT printf('b%d', x) FROM c;",
        rows(n)
    )
}

/// Stock adds a column after the VIRTUAL one, so the older records are short.
/// The CREATE text is one line: DROP COLUMN rewrites it, and FrankenSQLite's
/// rewrite does not keep a newline between column definitions the way stock's
/// text edit does, which is a schema-text matter apart from the record layout.
fn alter_after_virtual_table(n: u32) -> String {
    format!(
        "CREATE TABLE q(id INTEGER PRIMARY KEY, a TEXT, \
           v INTEGER GENERATED ALWAYS AS (length(a)) VIRTUAL, b TEXT, c TEXT);
         {}INSERT INTO q(a, b, c) SELECT printf('v%d', x), printf('b%d', x), printf('c%d', x)
           FROM c;
         ALTER TABLE q ADD COLUMN e TEXT DEFAULT 'e0';
         INSERT INTO q(a, b, c, e) VALUES ('late', 'lb', 'lc', 'le');",
        rows(n)
    )
}

/// bd-w4pr1.31, the read side: FrankenSQLite reads every stock-written row
/// of a table whose VIRTUAL generated column comes before stored ones exactly
/// as stock does. Reading record field i as declared column i (the defect)
/// returns the following column's value for `c`, `s`, `d`, `k` and `b`.
#[test]
fn frankensqlite_reads_stock_rows_past_virtual_generated_columns() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("stock.db");
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(&format!(
            "{}{}{}{}",
            virtual_before_stored_table(300),
            without_rowid_virtual_table(300),
            ipk_after_virtual_table(300),
            alter_after_virtual_table(300)
        ))
        .unwrap();
    assert_eq!(stock_integrity(&path), "ok");
    let stock = rusqlite::Connection::open(&path).unwrap();
    for sql in [
        "SELECT id, a, v, c, s, d FROM t",
        "SELECT id, a, v, c, s, d FROM t WHERE c = 'c3'",
        "SELECT id, c, s FROM t WHERE v = 3",
        "SELECT a, v, c, s, d FROM t WHERE id = 42",
        "SELECT x.id, y.id, x.s, y.d FROM t AS x JOIN t AS y ON x.c = y.c
         WHERE x.id < 15 AND y.id < 40",
        "SELECT a, v, k, d, s FROM w",
        "SELECT a, v, k, d, s FROM w WHERE k = 'k00042'",
        "SELECT k, s FROM w WHERE d = 'd3'",
        "SELECT v, id, b FROM p",
        "SELECT v, b FROM p WHERE id = 42",
        "SELECT id, a, v, b, c, e FROM q",
    ] {
        assert_eq!(
            fsqlite_rows(&path, sql),
            sorted_rows(&stock, sql),
            "FrankenSQLite must read `{sql}` from the stock-built file as stock does"
        );
    }
    assert_eq!(
        fsqlite_rows(&path, "PRAGMA integrity_check"),
        vec![format!("{:?}", vec![Value::Text("ok".to_owned())])],
        "FrankenSQLite's own integrity_check of the stock-built file"
    );
}

/// bd-w4pr1.31, the write side: FrankenSQLite's rows in tables with VIRTUAL
/// generated columns read back exactly by stock, through every row writer.
#[test]
fn virtual_generated_column_record_layouts() {
    let cases = [
        Case::new(
            "virtual_before_stored",
            "",
            virtual_before_stored_table(300),
            "INSERT INTO t(a, c, d) VALUES ('new-1', 'c1', 'd-new-1'), ('new-22', 'c2', 'd-new-2');
INSERT INTO t(a, c, d) SELECT a || '-s', c, d || '-s' FROM t WHERE id % 13 = 0;
UPDATE t SET c = c || '-u', d = c || d WHERE id % 7 = 0;
DELETE FROM t WHERE id % 11 = 0;
INSERT OR REPLACE INTO t(id, a, c, d)
  VALUES (5, 'rep-5', 'rc', 'rd-5'), (1000, 'rep-new', 'rc2', 'd00003');
INSERT INTO t(a, c, d) VALUES ('up', 'uc', 'd00004')
  ON CONFLICT(d) DO UPDATE SET a = excluded.a || '-x', c = c || '-x';
ALTER TABLE t ADD COLUMN e TEXT DEFAULT 'e0';
UPDATE t SET e = 'e-' || id WHERE id % 5 = 0;
INSERT INTO t(a, c, d, e) VALUES ('after', 'ac', 'd-after', 'ee');
VACUUM;",
        ),
        Case::new(
            "without_rowid_virtual",
            "",
            without_rowid_virtual_table(300),
            "INSERT INTO w(a, k, d) VALUES ('n1', 'kn1', 'dn1'), ('n22', 'kn2', 'dn2');
UPDATE w SET d = d || '-u', a = a || 'u' WHERE k < 'k00050';
DELETE FROM w WHERE k > 'k00280';
INSERT OR REPLACE INTO w(a, k, d) VALUES ('rep', 'k00010', 'drep');
INSERT INTO w(a, k, d) VALUES ('ups', 'k00020', 'dups')
  ON CONFLICT(k) DO UPDATE SET d = excluded.d, a = a || '-x';",
        ),
        Case::new(
            "ipk_after_virtual",
            "",
            ipk_after_virtual_table(300),
            "INSERT INTO p(id, b) VALUES (1000, 'explicit');
INSERT INTO p(b) VALUES ('auto');
UPDATE p SET b = b || '-u' WHERE id % 7 = 0;
UPDATE p SET id = id + 2000 WHERE id % 50 = 0;
DELETE FROM p WHERE id % 11 = 0;",
        ),
        Case::new(
            "alter_drop_stored_after_virtual",
            "",
            alter_after_virtual_table(300),
            "ALTER TABLE q DROP COLUMN b;
INSERT INTO q(a, c, e) VALUES ('x', 'xc', 'xe');
UPDATE q SET c = c || '-u' WHERE id % 7 = 0;",
        ),
        Case::new(
            "alter_drop_virtual",
            "",
            alter_after_virtual_table(300),
            "ALTER TABLE q DROP COLUMN v;
INSERT INTO q(a, b, c, e) VALUES ('y', 'yb', 'yc', 'ye');
UPDATE q SET b = b || '-u' WHERE id % 7 = 0;",
        ),
        Case::new(
            "trailing_virtual",
            "",
            format!(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT,
                   v TEXT GENERATED ALWAYS AS (a || b) VIRTUAL);
                 CREATE INDEX t_v ON t(v);
                 {}INSERT INTO t(a, b) SELECT printf('a%d', x), printf('b%d', x) FROM c;",
                rows(300)
            ),
            "INSERT INTO t(a, b) VALUES ('new-1', 'x'), ('new-2', 'y');
UPDATE t SET b = b || '-u' WHERE id % 7 = 0;
DELETE FROM t WHERE id % 11 = 0;
INSERT OR REPLACE INTO t(id, a, b) VALUES (3, 'rep', 'r');",
        ),
    ];
    for case in &cases {
        run_case(case);
    }
}

#[test]
fn virtual_tables_created_by_stock() {
    run_case(&Case::new(
        "fts5",
        "",
        format!(
            "CREATE VIRTUAL TABLE f USING fts5(body);
             CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b BLOB);
             {r}INSERT INTO f SELECT printf('word%d alpha beta %d', x % 17, x) FROM c;
             {r}INSERT INTO t(a, b) SELECT printf('v%05d', x), x'04' FROM c;",
            r = rows(300)
        ),
        &format!(
            "{DML}
INSERT INTO f VALUES ('gamma delta new');
DELETE FROM f WHERE rowid % 13 = 0;"
        ),
    ));
    run_case(&Case::new(
        "rtree",
        "",
        format!(
            "CREATE VIRTUAL TABLE r USING rtree(id, x0, x1, y0, y1);
             CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b BLOB);
             {r}INSERT INTO r SELECT x, x, x + 1, x, x + 2 FROM c;
             {r}INSERT INTO t(a, b) SELECT printf('v%05d', x), x'05' FROM c;",
            r = rows(300)
        ),
        &format!(
            "{DML}
INSERT INTO r VALUES (1000, 1, 2, 3, 4);
DELETE FROM r WHERE id % 13 = 0;"
        ),
    ));
}

/// FTS5's own check of the index FrankenSQLite wrote, and stock continuing
/// to write it.
#[test]
fn stock_fts5_index_checks_after_frankensqlite_writes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("fts.db");
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(&format!(
            "CREATE VIRTUAL TABLE f USING fts5(body);
             {}INSERT INTO f SELECT printf('word%d alpha beta %d', x % 17, x) FROM c;",
            rows(300)
        ))
        .unwrap();
    fsqlite_execute(
        &path,
        "INSERT INTO f VALUES ('gamma delta new'); DELETE FROM f WHERE rowid % 13 = 0;",
    )
    .unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("INSERT INTO f(f) VALUES ('integrity-check');")
        .expect("stock FTS5 integrity-check of the FrankenSQLite-written index");
    let gamma: i64 = conn
        .query_row("SELECT count(*) FROM f WHERE f MATCH 'gamma'", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(gamma, 1);
    conn.execute_batch(
        "INSERT INTO f VALUES ('stock writes after'); INSERT INTO f(f) VALUES ('integrity-check');",
    )
    .expect("stock keeps writing the index");
}

#[test]
fn operations_on_stock_files() {
    let base = base_table(300, "randomblob(x % 700)");
    let cases = [
        Case::new("vacuum", "", base.clone(), &format!("{DML}\nVACUUM;")),
        Case::new(
            "alter_table_columns",
            "",
            base.clone(),
            "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 5;
ALTER TABLE t RENAME COLUMN a TO a2;
UPDATE t SET c = id WHERE id < 10;",
        ),
        Case::new(
            "drop_objects",
            "",
            base.clone(),
            &format!("CREATE TABLE z AS SELECT * FROM t;\nDROP INDEX t_a;\nDROP TABLE z;\n{DML}"),
        ),
        Case::new(
            "mass_delete_then_refill",
            "",
            base,
            "DELETE FROM t WHERE id > 20;
INSERT INTO t(a, b) SELECT a || '-r', b FROM t;",
        ),
        Case::new(
            "large_overflow_chains",
            "",
            format!(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b BLOB);
                 {}INSERT INTO t(a, b) SELECT printf('v%05d', x), randomblob(200000) FROM c;",
                rows(60)
            ),
            "UPDATE t SET b = CAST(printf('%.5000c', char(65 + id % 26)) AS BLOB) WHERE id % 3 = 0;
UPDATE t SET b = zeroblob(300000) WHERE id % 5 = 0;
DELETE FROM t WHERE id % 7 = 0;
INSERT INTO t(a, b) VALUES ('big', zeroblob(1000000));",
        ),
    ];
    for case in &cases {
        run_case(case);
    }
}

#[test]
fn auto_vacuum_databases_are_refused_and_left_intact() {
    for (name, setup) in [
        ("auto_vacuum_full", "PRAGMA auto_vacuum = FULL;"),
        (
            "auto_vacuum_incremental",
            "PRAGMA auto_vacuum = INCREMENTAL;",
        ),
    ] {
        let mut case = Case::new(name, setup, base_table(300, "randomblob(x % 700)"), DML);
        case.expect_refusal = true;
        run_case(&case);
    }
}

/// Legacy schema formats (1..=3) are excluded by design. Such a file is valid
/// stock, so the refusal must say how to upgrade it rather than call it
/// malformed, and the file must be left as it was.
#[test]
fn legacy_schema_format_is_refused_with_the_upgrade_path() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("legacy.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.set_db_config(DbConfig::SQLITE_DBCONFIG_LEGACY_FILE_FORMAT, true)
            .unwrap();
        conn.execute_batch(&base_table(50, "x'00'")).unwrap();
    }
    let before = std::fs::read(&path).unwrap();
    assert_eq!(
        &before[44..48],
        &1u32.to_be_bytes(),
        "stock wrote schema format 1"
    );
    let error = fsqlite_execute(&path, DML).expect_err("a legacy-format file must be refused");
    assert!(
        error.contains("legacy schema format 1") && error.contains("VACUUM"),
        "the refusal names the cause and the upgrade: {error}"
    );
    assert!(
        !error.contains("malformed"),
        "a valid file is not corrupt: {error}"
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);

    // The named upgrade works: after a stock VACUUM the file takes writes.
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("VACUUM;")
        .unwrap();
    fsqlite_execute(&path, DML).expect("the upgraded file takes writes");
    assert_eq!(stock_integrity(&path), "ok");
}

/// A file past 1 GiB holds SQLite's lock-byte page, which must never carry
/// data. Writes about 3 GB of temporary files, so run it explicitly:
/// `cargo test -p fsqlite-e2e --test stock_file_variant_matrix -- --ignored`.
#[test]
#[ignore = "writes ~3 GB of temporary files; run explicitly"]
fn database_past_the_lock_byte_page() {
    run_case(&Case::new(
        "lock_byte_page_1gib",
        "PRAGMA page_size = 65536;",
        format!(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b BLOB);
             {}INSERT INTO t(a, b) SELECT printf('v%05d', x), zeroblob(1000000) FROM c;",
            rows(1100)
        ),
        "INSERT INTO t(a, b) SELECT a || '-n', zeroblob(500000) FROM t WHERE id % 50 = 0;
DELETE FROM t WHERE id % 13 = 0;
UPDATE t SET b = x'ff' WHERE id % 17 = 0;",
    ));
}
