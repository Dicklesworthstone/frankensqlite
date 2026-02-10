//! Database profiler — generates JSON metadata for each golden database file.
//!
//! For each `.db` file in the golden directory, this tool queries SQLite
//! PRAGMAs and `sqlite_master` to extract:
//! - File size, page size, page count, freelist count, schema version
//! - Journal mode, user version, application ID
//! - Table list with columns (name, type, primary key) and row counts
//! - Indexes, triggers, and views
//!
//! Output is one JSON file per database, written to the metadata directory.

use std::ffi::OsString;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags};
use serde::Serialize;

fn main() {
    let exit_code = run_cli(std::env::args_os());
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

#[allow(clippy::too_many_lines)]
fn run_cli<I>(os_args: I) -> i32
where
    I: IntoIterator<Item = OsString>,
{
    let raw: Vec<String> = os_args
        .into_iter()
        .map(|a| a.to_string_lossy().into_owned())
        .collect();

    let tail = if raw.len() > 1 { &raw[1..] } else { &[] };

    if tail.is_empty() || tail.iter().any(|a| a == "-h" || a == "--help") {
        print_help();
        return 0;
    }

    let mut golden_dir = PathBuf::from("sample_sqlite_db_files/golden");
    let mut output_dir = PathBuf::from("sample_sqlite_db_files/metadata");
    let mut single_db: Option<String> = None;
    let mut pretty = false;

    let mut i = 0;
    while i < tail.len() {
        match tail[i].as_str() {
            "--golden-dir" => {
                i += 1;
                if i >= tail.len() {
                    eprintln!("error: --golden-dir requires a directory argument");
                    return 2;
                }
                golden_dir = PathBuf::from(&tail[i]);
            }
            "--output-dir" => {
                i += 1;
                if i >= tail.len() {
                    eprintln!("error: --output-dir requires a directory argument");
                    return 2;
                }
                output_dir = PathBuf::from(&tail[i]);
            }
            "--db" => {
                i += 1;
                if i >= tail.len() {
                    eprintln!("error: --db requires a database filename");
                    return 2;
                }
                single_db = Some(tail[i].clone());
            }
            "--pretty" => pretty = true,
            other => {
                eprintln!("error: unknown option `{other}`");
                return 2;
            }
        }
        i += 1;
    }

    if !golden_dir.is_dir() {
        eprintln!(
            "error: golden directory does not exist: {}",
            golden_dir.display()
        );
        return 1;
    }

    if !output_dir.is_dir() {
        eprintln!(
            "error: output directory does not exist: {}",
            output_dir.display()
        );
        return 1;
    }

    let db_files = match collect_db_files(&golden_dir, single_db.as_deref()) {
        Ok(files) => files,
        Err(e) => {
            eprintln!("error: failed to list golden directory: {e}");
            return 1;
        }
    };

    if db_files.is_empty() {
        eprintln!("warning: no .db files found in {}", golden_dir.display());
        return 0;
    }

    let mut success_count = 0u32;
    let mut fail_count = 0u32;

    for db_path in &db_files {
        let db_name = db_path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();

        match profile_database(db_path) {
            Ok(profile) => {
                let json_result = if pretty {
                    serde_json::to_string_pretty(&profile)
                } else {
                    serde_json::to_string(&profile)
                };
                match json_result {
                    Ok(json) => {
                        let out_path = output_dir.join(format!("{db_name}.json"));
                        match std::fs::write(&out_path, json.as_bytes()) {
                            Ok(()) => {
                                println!("  OK  {db_name} -> {}", out_path.display());
                                success_count += 1;
                            }
                            Err(e) => {
                                eprintln!("FAIL  {db_name}: write error: {e}");
                                fail_count += 1;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("FAIL  {db_name}: JSON serialization error: {e}");
                        fail_count += 1;
                    }
                }
            }
            Err(e) => {
                eprintln!("FAIL  {db_name}: {e}");
                fail_count += 1;
            }
        }
    }

    println!(
        "\nProfiled {success_count}/{} databases ({fail_count} failed)",
        db_files.len()
    );

    i32::from(fail_count > 0)
}

fn print_help() {
    let text = "\
profile-db — Generate JSON metadata for golden database files

USAGE:
    profile-db [OPTIONS]

OPTIONS:
    --golden-dir <DIR>    Directory containing golden .db files
                          (default: sample_sqlite_db_files/golden)
    --output-dir <DIR>    Directory for JSON output files
                          (default: sample_sqlite_db_files/metadata)
    --db <NAME>           Profile only this database file (e.g. beads_viewer.db)
    --pretty              Pretty-print JSON output
    -h, --help            Show this help message

EXAMPLES:
    profile-db
    profile-db --pretty
    profile-db --db frankensqlite.db --pretty
    profile-db --golden-dir /tmp/dbs --output-dir /tmp/meta
";
    let _ = io::stdout().write_all(text.as_bytes());
}

// ── Data structures ──────────────────────────────────────────────────────

/// Full profile of a single SQLite database.
#[derive(Debug, Serialize)]
struct DbProfile {
    name: String,
    file_size_bytes: u64,
    page_size: u32,
    page_count: u32,
    freelist_count: u32,
    schema_version: u32,
    journal_mode: String,
    user_version: u32,
    application_id: u32,
    tables: Vec<TableProfile>,
    indices: Vec<String>,
    triggers: Vec<String>,
    views: Vec<String>,
}

/// Profile of a single table within a database.
#[derive(Debug, Serialize)]
struct TableProfile {
    name: String,
    row_count: u64,
    columns: Vec<ColumnProfile>,
}

/// Profile of a single column within a table.
#[derive(Debug, Serialize)]
struct ColumnProfile {
    name: String,
    #[serde(rename = "type")]
    col_type: String,
    primary_key: bool,
    not_null: bool,
    default_value: Option<String>,
}

// ── Core profiling logic ─────────────────────────────────────────────────

fn collect_db_files(golden_dir: &Path, single_db: Option<&str>) -> Result<Vec<PathBuf>, io::Error> {
    if let Some(name) = single_db {
        let path = golden_dir.join(name);
        if path.is_file() {
            return Ok(vec![path]);
        }
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("database file not found: {}", path.display()),
        ));
    }

    let mut files: Vec<PathBuf> = std::fs::read_dir(golden_dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("db") {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    files.sort();
    Ok(files)
}

#[allow(clippy::cast_possible_truncation)]
fn profile_database(db_path: &Path) -> Result<DbProfile, String> {
    let name = db_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();

    let file_size_bytes = std::fs::metadata(db_path)
        .map_err(|e| format!("cannot stat file: {e}"))?
        .len();

    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn =
        Connection::open_with_flags(db_path, flags).map_err(|e| format!("cannot open: {e}"))?;

    let page_size = pragma_u32(&conn, "page_size")?;
    let page_count = pragma_u32(&conn, "page_count")?;
    let freelist_count = pragma_u32(&conn, "freelist_count")?;
    let schema_version = pragma_u32(&conn, "schema_version")?;
    let user_version = pragma_u32(&conn, "user_version")?;
    let application_id = pragma_u32(&conn, "application_id")?;
    let journal_mode = pragma_string(&conn, "journal_mode")?;

    let tables = query_tables(&conn)?;
    let indices = query_names(&conn, "index")?;
    let triggers = query_names(&conn, "trigger")?;
    let views = query_names(&conn, "view")?;

    Ok(DbProfile {
        name,
        file_size_bytes,
        page_size,
        page_count,
        freelist_count,
        schema_version,
        journal_mode,
        user_version,
        application_id,
        tables,
        indices,
        triggers,
        views,
    })
}

fn pragma_u32(conn: &Connection, name: &str) -> Result<u32, String> {
    let sql = format!("PRAGMA {name}");
    conn.query_row(&sql, [], |row| row.get::<_, u32>(0))
        .map_err(|e| format!("PRAGMA {name}: {e}"))
}

fn pragma_string(conn: &Connection, name: &str) -> Result<String, String> {
    let sql = format!("PRAGMA {name}");
    conn.query_row(&sql, [], |row| row.get::<_, String>(0))
        .map_err(|e| format!("PRAGMA {name}: {e}"))
}

fn query_names(conn: &Connection, obj_type: &str) -> Result<Vec<String>, String> {
    let sql =
        "SELECT name FROM sqlite_master WHERE type = ?1 AND name NOT LIKE 'sqlite_%' ORDER BY name";
    let mut stmt = conn.prepare(sql).map_err(|e| format!("prepare: {e}"))?;
    let rows = stmt
        .query_map([obj_type], |row| row.get::<_, String>(0))
        .map_err(|e| format!("query sqlite_master for {obj_type}: {e}"))?;

    let mut names = Vec::new();
    for row in rows {
        names.push(row.map_err(|e| format!("row read: {e}"))?);
    }
    Ok(names)
}

fn query_tables(conn: &Connection) -> Result<Vec<TableProfile>, String> {
    let table_names = {
        let sql = "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name";
        let mut stmt = conn.prepare(sql).map_err(|e| format!("prepare: {e}"))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| format!("query tables: {e}"))?;

        let mut names = Vec::new();
        for row in rows {
            names.push(row.map_err(|e| format!("row read: {e}"))?);
        }
        names
    };

    let mut tables = Vec::with_capacity(table_names.len());
    for tname in &table_names {
        let columns = query_columns(conn, tname)?;
        let row_count = query_row_count(conn, tname)?;
        tables.push(TableProfile {
            name: tname.clone(),
            row_count,
            columns,
        });
    }
    Ok(tables)
}

fn query_columns(conn: &Connection, table_name: &str) -> Result<Vec<ColumnProfile>, String> {
    // table_info returns: cid, name, type, notnull, dflt_value, pk
    let sql = format!("PRAGMA table_info('{table_name}')");
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("prepare table_info: {e}"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok(ColumnProfile {
                name: row.get::<_, String>(1)?,
                col_type: row.get::<_, String>(2)?,
                not_null: row.get::<_, bool>(3)?,
                default_value: row.get::<_, Option<String>>(4)?,
                primary_key: row.get::<_, i32>(5)? != 0,
            })
        })
        .map_err(|e| format!("query table_info({table_name}): {e}"))?;

    let mut columns = Vec::new();
    for row in rows {
        columns.push(row.map_err(|e| format!("column read: {e}"))?);
    }
    Ok(columns)
}

fn query_row_count(conn: &Connection, table_name: &str) -> Result<u64, String> {
    // Use a quoted identifier to handle table names with special characters.
    let sql = format!("SELECT count(*) FROM \"{table_name}\"");
    conn.query_row(&sql, [], |row| row.get::<_, u64>(0))
        .map_err(|e| format!("count(*) from {table_name}: {e}"))
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn run_with(args: &[&str]) -> i32 {
        let os_args: Vec<OsString> = args.iter().map(OsString::from).collect();
        run_cli(os_args)
    }

    #[test]
    fn test_help_flag_exits_zero() {
        assert_eq!(run_with(&["profile-db", "--help"]), 0);
        assert_eq!(run_with(&["profile-db", "-h"]), 0);
    }

    #[test]
    fn test_no_args_shows_help() {
        assert_eq!(run_with(&["profile-db"]), 0);
    }

    #[test]
    fn test_unknown_option_exits_two() {
        assert_eq!(run_with(&["profile-db", "--bogus"]), 2);
    }

    #[test]
    fn test_missing_golden_dir_exits_one() {
        assert_eq!(
            run_with(&["profile-db", "--golden-dir", "/nonexistent/path/xyz"]),
            1
        );
    }

    #[test]
    fn test_profile_tempdb() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create a small test database.
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL);
             INSERT INTO items VALUES (1, 'widget', 9.99);
             INSERT INTO items VALUES (2, 'gadget', 19.99);
             CREATE INDEX idx_items_name ON items(name);
             CREATE VIEW item_names AS SELECT name FROM items;",
        )
        .unwrap();
        drop(conn);

        let profile = profile_database(&db_path).unwrap();
        assert_eq!(profile.name, "test");
        assert!(profile.page_size > 0);
        assert!(profile.page_count > 0);
        assert_eq!(profile.tables.len(), 1);
        assert_eq!(profile.tables[0].name, "items");
        assert_eq!(profile.tables[0].row_count, 2);
        assert_eq!(profile.tables[0].columns.len(), 3);
        assert_eq!(profile.tables[0].columns[0].name, "id");
        assert!(profile.tables[0].columns[0].primary_key);
        assert_eq!(profile.tables[0].columns[1].name, "name");
        assert!(profile.tables[0].columns[1].not_null);
        assert_eq!(profile.indices, vec!["idx_items_name"]);
        assert_eq!(profile.views, vec!["item_names"]);
    }

    #[test]
    fn test_profile_outputs_valid_json() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("json_test.db");

        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch("CREATE TABLE t1 (a INTEGER, b TEXT);")
            .unwrap();
        drop(conn);

        let profile = profile_database(&db_path).unwrap();
        let json = serde_json::to_string_pretty(&profile).unwrap();

        // Round-trip: deserialize back into a generic value.
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["name"], "json_test");
        assert_eq!(parsed["tables"][0]["name"], "t1");
        assert_eq!(parsed["tables"][0]["columns"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_full_cli_with_tempdb() {
        let golden = tempfile::tempdir().unwrap();
        let meta = tempfile::tempdir().unwrap();

        let db_path = golden.path().join("sample.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch("CREATE TABLE x (id INTEGER PRIMARY KEY);")
            .unwrap();
        drop(conn);

        let exit_code = run_with(&[
            "profile-db",
            "--golden-dir",
            golden.path().to_str().unwrap(),
            "--output-dir",
            meta.path().to_str().unwrap(),
            "--pretty",
        ]);
        assert_eq!(exit_code, 0);

        let out_path = meta.path().join("sample.json");
        assert!(out_path.exists(), "JSON output file should exist");

        let content = std::fs::read_to_string(&out_path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed["name"], "sample");
    }

    #[test]
    fn test_single_db_filter() {
        let golden = tempfile::tempdir().unwrap();
        let meta = tempfile::tempdir().unwrap();

        // Create two databases.
        for name in &["a.db", "b.db"] {
            let db_path = golden.path().join(name);
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch("CREATE TABLE t (id INTEGER);").unwrap();
            drop(conn);
        }

        let exit_code = run_with(&[
            "profile-db",
            "--golden-dir",
            golden.path().to_str().unwrap(),
            "--output-dir",
            meta.path().to_str().unwrap(),
            "--db",
            "a.db",
        ]);
        assert_eq!(exit_code, 0);

        // Only a.json should exist.
        assert!(meta.path().join("a.json").exists());
        assert!(!meta.path().join("b.json").exists());
    }

    #[test]
    fn test_empty_golden_dir() {
        let golden = tempfile::tempdir().unwrap();
        let meta = tempfile::tempdir().unwrap();

        let exit_code = run_with(&[
            "profile-db",
            "--golden-dir",
            golden.path().to_str().unwrap(),
            "--output-dir",
            meta.path().to_str().unwrap(),
        ]);
        assert_eq!(exit_code, 0);
    }

    #[test]
    fn test_pragma_values() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("pragmas.db");

        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA page_size = 8192;
             CREATE TABLE t (x INTEGER);",
        )
        .unwrap();
        drop(conn);

        let profile = profile_database(&db_path).unwrap();
        assert_eq!(profile.page_size, 8192);
        // freelist_count is always non-negative (u32), just verify it's accessible.
        let _ = profile.freelist_count;
        assert!(profile.schema_version > 0);
    }

    #[test]
    fn test_table_with_defaults_and_notnull() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("defaults.db");

        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE config (
                key TEXT NOT NULL PRIMARY KEY,
                value TEXT DEFAULT 'unknown',
                priority INTEGER NOT NULL DEFAULT 0
            );
            INSERT INTO config (key) VALUES ('test_key');",
        )
        .unwrap();
        drop(conn);

        let profile = profile_database(&db_path).unwrap();
        assert_eq!(profile.tables.len(), 1);
        let t = &profile.tables[0];
        assert_eq!(t.row_count, 1);

        let key_col = &t.columns[0];
        assert_eq!(key_col.name, "key");
        assert!(key_col.primary_key);
        assert!(key_col.not_null);

        let val_col = &t.columns[1];
        assert_eq!(val_col.default_value.as_deref(), Some("'unknown'"));
        assert!(!val_col.not_null);

        let pri_col = &t.columns[2];
        assert_eq!(pri_col.default_value.as_deref(), Some("0"));
        assert!(pri_col.not_null);
    }
}
