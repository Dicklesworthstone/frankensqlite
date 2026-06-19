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
use sha2::{Digest, Sha256};

use fsqlite_e2e::fixture_metadata::{
    ColumnProfileV1, FIXTURE_METADATA_SCHEMA_VERSION_V1, FixtureFeaturesV1, FixtureMetadataV1,
    FixtureSafetyV1, RiskLevel, SqliteMetaV1, TableProfileV1, normalize_tags, size_bucket_tag,
};

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
    let mut checksums_path = PathBuf::from("sample_sqlite_db_files/checksums.sha256");
    let mut manifest_path = PathBuf::from("sample_sqlite_db_files/manifests/manifest.v1.json");
    let mut single_db: Option<String> = None;
    let mut pretty = false;
    let mut write_manifest = false;
    let mut manifest_only = false;

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
            "--checksums" => {
                i += 1;
                if i >= tail.len() {
                    eprintln!("error: --checksums requires a file path");
                    return 2;
                }
                checksums_path = PathBuf::from(&tail[i]);
            }
            "--manifest-path" => {
                i += 1;
                if i >= tail.len() {
                    eprintln!("error: --manifest-path requires a file path");
                    return 2;
                }
                manifest_path = PathBuf::from(&tail[i]);
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
            "--write-manifest" => write_manifest = true,
            "--manifest-only" => manifest_only = true,
            other => {
                eprintln!("error: unknown option `{other}`");
                return 2;
            }
        }
        i += 1;
    }

    if !manifest_only && !golden_dir.is_dir() {
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

    if !manifest_only {
        let db_files = match collect_db_files(&golden_dir, single_db.as_deref()) {
            Ok(files) => files,
            Err(e) => {
                eprintln!("error: failed to list golden directory: {e}");
                return 1;
            }
        };

        if db_files.is_empty() {
            eprintln!("warning: no .db files found in {}", golden_dir.display());
        } else {
            let mut success_count = 0u32;
            let mut fail_count = 0u32;

            for db_path in &db_files {
                let db_stem = db_path
                    .file_stem()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned();

                let db_id = match validate_db_id(&db_stem) {
                    Ok(id) => id,
                    Err(e) => {
                        eprintln!("FAIL  {db_stem}: invalid db_id: {e}");
                        fail_count += 1;
                        continue;
                    }
                };

                let Some(golden_filename) = db_path.file_name().and_then(|n| n.to_str()) else {
                    eprintln!("FAIL  {db_id}: invalid golden filename");
                    fail_count += 1;
                    continue;
                };

                let sha256_golden = match sha256_file(db_path) {
                    Ok(h) => h,
                    Err(e) => {
                        eprintln!("FAIL  {db_id}: sha256 error: {e}");
                        fail_count += 1;
                        continue;
                    }
                };

                match profile_database(db_path, &db_id, golden_filename, &sha256_golden) {
                    Ok(profile) => {
                        let json_result = if pretty {
                            serde_json::to_string_pretty(&profile)
                        } else {
                            serde_json::to_string(&profile)
                        };
                        match json_result {
                            Ok(json) => {
                                let out_path = output_dir.join(format!("{db_id}.json"));
                                match std::fs::write(&out_path, json.as_bytes()) {
                                    Ok(()) => {
                                        println!("  OK  {db_id} -> {}", out_path.display());
                                        success_count += 1;
                                    }
                                    Err(e) => {
                                        eprintln!("FAIL  {db_id}: write error: {e}");
                                        fail_count += 1;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("FAIL  {db_id}: JSON serialization error: {e}");
                                fail_count += 1;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("FAIL  {db_id}: {e}");
                        fail_count += 1;
                    }
                }
            }

            println!(
                "\nProfiled {success_count}/{} databases ({fail_count} failed)",
                db_files.len()
            );

            if fail_count > 0 {
                return 1;
            }
        }
    }

    if write_manifest || manifest_only {
        match write_manifest_v1(&checksums_path, &output_dir, &manifest_path) {
            Ok(()) => {
                println!("Wrote manifest: {}", manifest_path.display());
            }
            Err(e) => {
                eprintln!("error: failed to write manifest: {e}");
                return 1;
            }
        }
    }

    0
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
    --checksums <PATH>    Checksums file (sha256sum format) used for manifest generation
                          (default: sample_sqlite_db_files/checksums.sha256)
    --manifest-path <PATH> Output path for manifest.v1.json
                          (default: sample_sqlite_db_files/manifests/manifest.v1.json)
    --db <NAME>           Profile only this database file (e.g. beads_viewer.db)
    --pretty              Pretty-print JSON output
    --write-manifest      Generate sample_sqlite_db_files/manifests/manifest.v1.json
                          from checksums + metadata (stable, deterministic output)
    --manifest-only       Skip profiling; only generate manifest from checksums + metadata
    -h, --help            Show this help message

EXAMPLES:
    profile-db
    profile-db --pretty
    profile-db --db frankensqlite.db --pretty
    profile-db --write-manifest
    profile-db --manifest-only
    profile-db --golden-dir /tmp/dbs --output-dir /tmp/meta
";
    let _ = io::stdout().write_all(text.as_bytes());
}

// ── Data structures ──────────────────────────────────────────────────────
//
// Fixture metadata is emitted using `fsqlite_e2e::fixture_metadata::FixtureMetadataV1`.

// ── Manifest (v1) ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct ManifestV1 {
    manifest_version: u32,
    entries: Vec<ManifestEntryV1>,
}

#[derive(Debug, Serialize)]
struct ManifestEntryV1 {
    db_id: String,
    golden_filename: String,
    sha256_golden: String,
    size_bytes: u64,
    sqlite_meta: ManifestSqliteMetaV1,
}

#[derive(Debug, Serialize)]
struct ManifestSqliteMetaV1 {
    page_size: u32,
    journal_mode: Option<String>,
    user_version: Option<u32>,
    application_id: Option<u32>,
}

fn write_manifest_v1(
    checksums_path: &Path,
    metadata_dir: &Path,
    manifest_path: &Path,
) -> Result<(), String> {
    let manifest = build_manifest_v1(checksums_path, metadata_dir)?;

    let json = serde_json::to_string_pretty(&manifest)
        .map_err(|e| format!("manifest JSON serialization failed: {e}"))?;

    let parent = manifest_path
        .parent()
        .ok_or_else(|| format!("invalid manifest path: {}", manifest_path.display()))?;
    std::fs::create_dir_all(parent).map_err(|e| {
        format!(
            "failed to create manifest directory {}: {e}",
            parent.display()
        )
    })?;

    std::fs::write(manifest_path, json.as_bytes())
        .map_err(|e| format!("failed to write manifest {}: {e}", manifest_path.display()))?;

    Ok(())
}

fn build_manifest_v1(checksums_path: &Path, metadata_dir: &Path) -> Result<ManifestV1, String> {
    let checksums = read_checksums_sha256(checksums_path)?;

    let mut entries: Vec<ManifestEntryV1> = Vec::with_capacity(checksums.len());
    for (golden_filename, sha256_golden) in &checksums {
        let stem = Path::new(golden_filename)
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| format!("invalid golden filename: {golden_filename}"))?;

        let db_id = validate_db_id(stem)?;
        let meta_path = metadata_dir.join(format!("{db_id}.json"));
        let (size_bytes, page_size, journal_mode, user_version, application_id) =
            read_metadata_minimal(&meta_path)?;

        entries.push(ManifestEntryV1 {
            db_id,
            golden_filename: golden_filename.to_owned(),
            sha256_golden: sha256_golden.to_owned(),
            size_bytes,
            sqlite_meta: ManifestSqliteMetaV1 {
                page_size,
                journal_mode,
                user_version,
                application_id,
            },
        });
    }

    entries.sort_by(|a, b| a.db_id.cmp(&b.db_id));

    Ok(ManifestV1 {
        manifest_version: 1,
        entries,
    })
}

fn read_checksums_sha256(path: &Path) -> Result<Vec<(String, String)>, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;

    let mut pairs = Vec::new();
    for (line_no, raw) in content.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let Some((hash, filename)) = line.split_once("  ") else {
            return Err(format!(
                "malformed checksums line {} (expected '<sha256>  <filename>'): {raw}",
                line_no + 1
            ));
        };
        let hash = hash.trim();
        let filename = filename.trim();

        if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(format!(
                "line {}: invalid sha256 (expected 64 lowercase hex chars): {hash}",
                line_no + 1
            ));
        }
        let ext_ok = Path::new(filename)
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("db"));
        if !ext_ok {
            return Err(format!(
                "line {}: filename must end with .db: {filename}",
                line_no + 1
            ));
        }

        pairs.push((filename.to_owned(), hash.to_ascii_lowercase()));
    }

    if pairs.is_empty() {
        return Err(format!("checksums file {} is empty", path.display()));
    }

    // Stable ordering by filename.
    pairs.sort_by(|a, b| a.0.cmp(&b.0));

    // Reject duplicate filenames.
    for w in pairs.windows(2) {
        if w[0].0 == w[1].0 {
            return Err(format!("duplicate filename in checksums file: {}", w[0].0));
        }
    }

    Ok(pairs)
}

fn validate_db_id(s: &str) -> Result<String, String> {
    let id = s.to_ascii_lowercase();
    if id.len() < 2 || id.len() > 64 {
        return Err(format!("db_id length must be 2..=64: {id}"));
    }
    let mut chars = id.chars();
    let Some(first) = chars.next() else {
        return Err("empty db_id".to_owned());
    };
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return Err(format!("db_id must start with [a-z0-9]: {id}"));
    }
    if !chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-') {
        return Err(format!(
            "db_id must match ^[a-z0-9][a-z0-9_\\-]{{1,63}}$: {id}"
        ));
    }
    Ok(id)
}

#[allow(clippy::type_complexity)]
fn read_metadata_minimal(
    meta_path: &Path,
) -> Result<(u64, u32, Option<String>, Option<u32>, Option<u32>), String> {
    let content = std::fs::read_to_string(meta_path)
        .map_err(|e| format!("cannot read metadata {}: {e}", meta_path.display()))?;
    let meta: FixtureMetadataV1 = serde_json::from_str(&content)
        .map_err(|e| format!("metadata JSON parse failed (expected v1): {e}"))?;

    Ok((
        meta.size_bytes,
        meta.sqlite_meta.page_size,
        Some(meta.sqlite_meta.journal_mode),
        Some(meta.sqlite_meta.user_version),
        Some(meta.sqlite_meta.application_id),
    ))
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

fn quote_ident(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn detect_sidecars(db_path: &Path) -> Vec<String> {
    const SIDECARS: [&str; 3] = ["-wal", "-shm", "-journal"];
    let mut present = Vec::new();

    for suffix in SIDECARS {
        let mut os = db_path.as_os_str().to_os_string();
        os.push(suffix);
        let path = PathBuf::from(os);
        if path.exists() {
            present.push(suffix.to_owned());
        }
    }

    present
}

fn sha256_file(path: &Path) -> Result<String, String> {
    use std::fmt::Write as _;

    let data = std::fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(&data);
    let digest = hasher.finalize();

    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    Ok(hex)
}

fn sqlite_master_sql_contains(conn: &Connection, needle_lower: &str) -> Result<bool, String> {
    let pattern = format!("%{needle_lower}%");
    let mut stmt = conn
        .prepare(
            "SELECT 1 FROM sqlite_master \
             WHERE sql IS NOT NULL AND lower(sql) LIKE ?1 \
             LIMIT 1",
        )
        .map_err(|e| format!("sqlite_master sql prepare: {e}"))?;
    let mut rows = stmt
        .query([pattern])
        .map_err(|e| format!("sqlite_master sql query: {e}"))?;
    Ok(rows
        .next()
        .map_err(|e| format!("sqlite_master sql next: {e}"))?
        .is_some())
}

fn has_foreign_keys(conn: &Connection, tables: &[TableProfileV1]) -> Result<bool, String> {
    for t in tables {
        let sql = format!("PRAGMA foreign_key_list({})", quote_ident(&t.name));
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("PRAGMA foreign_key_list({}) prepare: {e}", t.name))?;
        let mut rows = stmt
            .query([])
            .map_err(|e| format!("PRAGMA foreign_key_list({}) query: {e}", t.name))?;
        if rows
            .next()
            .map_err(|e| format!("PRAGMA foreign_key_list({}) next: {e}", t.name))?
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[allow(clippy::cast_possible_truncation, clippy::too_many_lines)]
fn profile_database(
    db_path: &Path,
    db_id: &str,
    golden_filename: &str,
    sha256_golden: &str,
) -> Result<FixtureMetadataV1, String> {
    let size_bytes = std::fs::metadata(db_path)
        .map_err(|e| format!("cannot stat file: {e}"))?
        .len();
    let sidecars_present = detect_sidecars(db_path);

    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn =
        Connection::open_with_flags(db_path, flags).map_err(|e| format!("cannot open: {e}"))?;

    let page_size = pragma_u32(&conn, "page_size")?;
    let page_count = pragma_u32(&conn, "page_count")?;
    let freelist_count = pragma_u32(&conn, "freelist_count")?;
    let schema_version = pragma_u32(&conn, "schema_version")?;
    let encoding = pragma_string(&conn, "encoding")?;
    let user_version = pragma_u32(&conn, "user_version")?;
    let application_id = pragma_u32(&conn, "application_id")?;
    let journal_mode = pragma_string(&conn, "journal_mode")?;
    let auto_vacuum = pragma_u32(&conn, "auto_vacuum")?;

    let tables = query_tables(&conn)?;
    let indices = query_names(&conn, "index")?;
    let triggers = query_names(&conn, "trigger")?;
    let views = query_names(&conn, "view")?;

    let has_fts = sqlite_master_sql_contains(&conn, "using fts")?;
    let has_rtree = sqlite_master_sql_contains(&conn, "using rtree")?;
    let has_foreign_keys = has_foreign_keys(&conn, &tables)?;

    let features = FixtureFeaturesV1 {
        has_wal_sidecars_observed: sidecars_present.iter().any(|s| s == "-wal" || s == "-shm"),
        has_fts,
        has_rtree,
        has_triggers: !triggers.is_empty(),
        has_views: !views.is_empty(),
        has_foreign_keys,
    };

    let norm_id = db_id.replace('_', "-");
    let mut tags: Vec<String> = Vec::new();
    for stable in fsqlite_harness::fixture_discovery::STABLE_CORPUS_TAGS {
        if *stable == "misc" {
            continue;
        }
        if norm_id.contains(stable) {
            tags.push((*stable).to_owned());
        }
    }
    if tags.is_empty() {
        tags.push("misc".to_owned());
    }

    tags.push(size_bucket_tag(size_bytes).to_owned());
    if journal_mode.eq_ignore_ascii_case("wal") {
        tags.push("wal".to_owned());
    }
    if features.has_fts {
        tags.push("fts".to_owned());
    }
    if features.has_rtree {
        tags.push("rtree".to_owned());
    }
    if indices.len() > 20 {
        tags.push("many-indexes".to_owned());
    }
    if tables.len() > 20 {
        tags.push("many-tables".to_owned());
    }

    let pii_risk = if tags.iter().any(|t| {
        matches!(
            t.as_str(),
            "asupersync" | "frankentui" | "flywheel" | "frankensqlite" | "agent-mail" | "beads"
        )
    }) {
        RiskLevel::Unlikely
    } else {
        RiskLevel::Unknown
    };
    let secrets_risk = pii_risk;
    let allowed_for_ci = pii_risk == RiskLevel::Unlikely && secrets_risk == RiskLevel::Unlikely;

    Ok(FixtureMetadataV1 {
        schema_version: FIXTURE_METADATA_SCHEMA_VERSION_V1,
        db_id: db_id.to_owned(),
        source_path: None,
        golden_filename: golden_filename.to_owned(),
        sha256_golden: sha256_golden.to_owned(),
        size_bytes,
        sidecars_present,
        sqlite_meta: SqliteMetaV1 {
            page_size,
            page_count,
            freelist_count,
            schema_version,
            encoding,
            user_version,
            application_id,
            journal_mode,
            auto_vacuum,
        },
        features,
        tags: normalize_tags(tags),
        safety: FixtureSafetyV1 {
            pii_risk,
            secrets_risk,
            allowed_for_ci,
        },
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

fn query_tables(conn: &Connection) -> Result<Vec<TableProfileV1>, String> {
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
        tables.push(TableProfileV1 {
            name: tname.clone(),
            row_count,
            columns,
        });
    }
    Ok(tables)
}

fn query_columns(conn: &Connection, table_name: &str) -> Result<Vec<ColumnProfileV1>, String> {
    // table_info returns: cid, name, type, notnull, dflt_value, pk
    let sql = format!("PRAGMA table_info('{table_name}')");
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("prepare table_info: {e}"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok(ColumnProfileV1 {
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
    let count = conn
        .query_row(&sql, [], |row| row.get::<_, i64>(0))
        .map_err(|e| format!("count(*) from {table_name}: {e}"))?;
    u64::try_from(count).map_err(|e| format!("count(*) from {table_name}: {e}"))
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn run_with(args: &[&str]) -> i32 {
        let os_args: Vec<OsString> = args.iter().map(OsString::from).collect();
        run_cli(os_args)
    }

    fn profile_for_test(db_path: &Path) -> FixtureMetadataV1 {
        let stem = db_path
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("test db path must have UTF-8 stem");
        let db_id = validate_db_id(stem).expect("db_id must be valid in tests");
        let golden_filename = db_path
            .file_name()
            .and_then(|n| n.to_str())
            .expect("test db path must have UTF-8 filename");
        let sha256_golden = sha256_file(db_path).expect("sha256 must be computed for test db");
        profile_database(db_path, &db_id, golden_filename, &sha256_golden)
            .expect("profile_database must succeed for test db")
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

        let profile = profile_for_test(&db_path);
        assert_eq!(profile.db_id, "test");
        assert_eq!(profile.golden_filename, "test.db");
        assert!(profile.size_bytes > 0);
        assert!(profile.sqlite_meta.page_size > 0);
        assert!(profile.sqlite_meta.page_count > 0);
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

        let profile = profile_for_test(&db_path);
        let json = serde_json::to_string_pretty(&profile).unwrap();

        // Round-trip: deserialize back into a generic value.
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["db_id"], "json_test");
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
        assert_eq!(parsed["db_id"], "sample");
        assert_eq!(parsed["golden_filename"], "sample.db");
    }

    #[test]
    fn test_single_db_filter() {
        let golden = tempfile::tempdir().unwrap();
        let meta = tempfile::tempdir().unwrap();

        // Create two databases.
        for name in &["aa.db", "bb.db"] {
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
            "aa.db",
        ]);
        assert_eq!(exit_code, 0);

        // Only a.json should exist.
        assert!(meta.path().join("aa.json").exists());
        assert!(!meta.path().join("bb.json").exists());
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
    fn test_manifest_only_generates_manifest_from_checksums_and_metadata() {
        let meta = tempfile::tempdir().unwrap();
        let checksums = meta.path().join("checksums.sha256");
        let manifest = meta.path().join("manifest.v1.json");

        // Minimal metadata file matching the stable corpus metadata schema.
        let meta_path = meta.path().join("alpha.json");
        let meta_record = FixtureMetadataV1 {
            schema_version: FIXTURE_METADATA_SCHEMA_VERSION_V1,
            db_id: "alpha".to_owned(),
            source_path: None,
            golden_filename: "alpha.db".to_owned(),
            sha256_golden: "00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff"
                .to_owned(),
            size_bytes: 123,
            sidecars_present: Vec::new(),
            sqlite_meta: SqliteMetaV1 {
                page_size: 4096,
                page_count: 1,
                freelist_count: 0,
                schema_version: 1,
                encoding: "UTF-8".to_owned(),
                user_version: 0,
                application_id: 0,
                journal_mode: "wal".to_owned(),
                auto_vacuum: 0,
            },
            features: FixtureFeaturesV1 {
                has_wal_sidecars_observed: false,
                has_fts: false,
                has_rtree: false,
                has_triggers: false,
                has_views: false,
                has_foreign_keys: false,
            },
            tags: normalize_tags([String::from("misc"), String::from("small")]),
            safety: FixtureSafetyV1 {
                pii_risk: RiskLevel::Unlikely,
                secrets_risk: RiskLevel::Unlikely,
                allowed_for_ci: true,
            },
            tables: Vec::new(),
            indices: Vec::new(),
            triggers: Vec::new(),
            views: Vec::new(),
        };
        let meta_json = serde_json::to_string_pretty(&meta_record).unwrap();
        std::fs::write(&meta_path, meta_json.as_bytes()).unwrap();

        std::fs::write(
            &checksums,
            "00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff  alpha.db\n",
        )
        .unwrap();

        let exit_code = run_with(&[
            "profile-db",
            "--manifest-only",
            "--output-dir",
            meta.path().to_str().unwrap(),
            "--checksums",
            checksums.to_str().unwrap(),
            "--manifest-path",
            manifest.to_str().unwrap(),
        ]);
        assert_eq!(exit_code, 0);
        assert!(manifest.exists(), "manifest file should be written");

        let parsed: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
        assert_eq!(parsed["manifest_version"], 1);
        assert_eq!(parsed["entries"][0]["db_id"], "alpha");
        assert_eq!(parsed["entries"][0]["golden_filename"], "alpha.db");
        assert_eq!(
            parsed["entries"][0]["sha256_golden"],
            "00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff"
        );
        assert_eq!(parsed["entries"][0]["size_bytes"], 123);
        assert_eq!(parsed["entries"][0]["sqlite_meta"]["page_size"], 4096);
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

        let profile = profile_for_test(&db_path);
        assert_eq!(profile.sqlite_meta.page_size, 8192);
        // freelist_count is always non-negative (u32), just verify it's accessible.
        let _ = profile.sqlite_meta.freelist_count;
        assert!(profile.sqlite_meta.schema_version > 0);
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

        let profile = profile_for_test(&db_path);
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
