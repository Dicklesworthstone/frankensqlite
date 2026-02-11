//! Mismatch artifact bundles for rapid debugging of correctness divergences.
//!
//! Bead: bd-2als.3.4
//!
//! When the batch runner detects a mismatch between C SQLite and FrankenSQLite,
//! this module produces a self-contained diagnostic bundle that enables rapid
//! reproduction and root-cause analysis.
//!
//! # Bundle contents
//!
//! - `metadata.json` — fixture id, golden SHA-256, workload, seed, concurrency,
//!   engine settings, comparison results
//! - `REPRO.md` — copy-paste commands to reproduce the mismatch
//! - `schema_diff.txt` — side-by-side schema comparison
//! - `dump_diff.txt` — row-level data differences (capped for size)
//! - `pragma_diff.txt` — PRAGMA setting differences between engines
//! - `sqlite3_dump.sql` / `fsqlite_dump.sql` — deterministic SQL dumps
//! - Canonicalized DB files (optional, controlled by config)
//!
//! # Size discipline
//!
//! Row-level diffs are capped at [`MAX_DIFF_ROWS`] per table to prevent
//! multi-gigabyte bundles.  Full working copies are omitted by default but
//! can be included via [`BundleConfig::include_working_copies`].

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::batch_runner::{CellResult, CellVerdict};
use crate::{E2eResult, HarnessSettings};

/// Maximum rows to include in per-table dump diffs before truncating.
const MAX_DIFF_ROWS: usize = 200;

/// Maximum tables to include full diffs for (to bound bundle size).
const MAX_DIFF_TABLES: usize = 50;

// ── Configuration ──────────────────────────────────────────────────────

/// Controls what goes into a mismatch bundle.
#[derive(Debug, Clone)]
pub struct BundleConfig {
    /// Base directory for writing bundles.  Each mismatch gets a subdirectory.
    pub output_base: PathBuf,
    /// Include full SQL dumps of both databases.
    pub include_dumps: bool,
    /// Include canonicalized `.db` files in the bundle.
    pub include_canonical_dbs: bool,
    /// Include the raw working-copy `.db` files (can be large).
    pub include_working_copies: bool,
    /// Maximum rows per table in diff output.
    pub max_diff_rows: usize,
}

impl Default for BundleConfig {
    fn default() -> Self {
        Self {
            output_base: PathBuf::from("reports/mismatches"),
            include_dumps: true,
            include_canonical_dbs: false,
            include_working_copies: false,
            max_diff_rows: MAX_DIFF_ROWS,
        }
    }
}

// ── Bundle metadata ────────────────────────────────────────────────────

/// Serializable metadata for a mismatch bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleMetadata {
    /// Schema version for forward compatibility.
    pub schema_version: String,
    /// Fixture identifier (golden DB stem).
    pub fixture_id: String,
    /// SHA-256 of the golden input DB, if known.
    pub golden_sha256: Option<String>,
    /// Workload preset name.
    pub preset_name: String,
    /// Deterministic seed used for this run.
    pub seed: u64,
    /// Number of concurrent workers.
    pub concurrency: u16,
    /// Engine settings applied to both engines.
    pub settings: SettingsSnapshot,
    /// Expected equivalence tier.
    pub expected_tier: String,
    /// Achieved comparison tier.
    pub achieved_tier: Option<String>,
    /// Human-readable mismatch detail.
    pub mismatch_detail: String,
    /// SHA-256 of sqlite3 canonical output, if computed.
    pub sqlite3_canonical_sha256: Option<String>,
    /// SHA-256 of fsqlite canonical output, if computed.
    pub fsqlite_canonical_sha256: Option<String>,
    /// Wall time in milliseconds for the cell execution.
    pub wall_time_ms: u64,
}

/// Snapshot of engine settings for reproducibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsSnapshot {
    pub journal_mode: String,
    pub synchronous: String,
    pub cache_size: i64,
    pub page_size: u32,
    pub busy_timeout_ms: u32,
    pub concurrent_mode: bool,
}

impl From<&HarnessSettings> for SettingsSnapshot {
    fn from(s: &HarnessSettings) -> Self {
        Self {
            journal_mode: s.journal_mode.clone(),
            synchronous: s.synchronous.clone(),
            cache_size: s.cache_size,
            page_size: s.page_size,
            busy_timeout_ms: s.busy_timeout_ms,
            concurrent_mode: s.concurrent_mode,
        }
    }
}

// ── Schema / dump / pragma diff types ──────────────────────────────────

/// Schema difference between two databases.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDiff {
    /// Tables only in database A (sqlite3).
    pub only_in_a: Vec<String>,
    /// Tables only in database B (fsqlite).
    pub only_in_b: Vec<String>,
    /// Tables present in both but with different CREATE SQL.
    pub sql_differs: Vec<TableSqlDiff>,
    /// Tables that match exactly.
    pub matching_count: usize,
}

/// A single table whose schema SQL differs between engines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSqlDiff {
    pub table: String,
    pub sql_a: String,
    pub sql_b: String,
}

/// Per-table row-level diff summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDumpDiff {
    pub table: String,
    pub rows_a: usize,
    pub rows_b: usize,
    /// First N differing rows (capped by config).
    pub sample_diffs: Vec<RowDiff>,
    /// Whether the diff was truncated due to size limits.
    pub truncated: bool,
}

/// A single differing row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowDiff {
    /// 0-based row index where the difference occurs.
    pub row_index: usize,
    /// Row values from database A (sqlite3), if present.
    pub values_a: Option<Vec<String>>,
    /// Row values from database B (fsqlite), if present.
    pub values_b: Option<Vec<String>>,
}

/// PRAGMA value difference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PragmaDiff {
    pub pragma: String,
    pub value_a: String,
    pub value_b: String,
}

// ── Bundle generation ──────────────────────────────────────────────────

/// Stable directory name for a mismatch bundle.
#[must_use]
pub fn bundle_dir_name(cell: &CellResult) -> String {
    format!(
        "{}__{}__c{}__s{}",
        cell.fixture_id, cell.preset_name, cell.concurrency, cell.seed
    )
}

/// Generate and write a mismatch artifact bundle to disk.
///
/// Returns the path to the bundle directory on success.
///
/// # Errors
///
/// Returns `E2eError` on I/O failures or database access errors.
pub fn write_mismatch_bundle(
    cell: &CellResult,
    sqlite_db: &Path,
    fsqlite_db: &Path,
    golden_db: Option<&Path>,
    settings: &HarnessSettings,
    config: &BundleConfig,
) -> E2eResult<PathBuf> {
    let dir_name = bundle_dir_name(cell);
    let bundle_dir = config.output_base.join(&dir_name);
    std::fs::create_dir_all(&bundle_dir)?;

    // 1. Compute golden SHA-256 if path provided.
    let golden_sha256 = golden_db.and_then(|p| sha256_file(p).ok());

    // 2. Build metadata.
    let (expected_tier, achieved_tier, mismatch_detail) = match &cell.verdict {
        CellVerdict::Mismatch {
            expected_tier,
            achieved_tier,
            detail,
        } => (expected_tier.clone(), achieved_tier.clone(), detail.clone()),
        CellVerdict::Error(msg) => ("N/A".to_owned(), None, msg.clone()),
        CellVerdict::Pass { achieved_tier } => {
            ("N/A".to_owned(), Some(achieved_tier.clone()), String::new())
        }
    };

    let metadata = BundleMetadata {
        schema_version: "fsqlite-e2e.mismatch_bundle.v1".to_owned(),
        fixture_id: cell.fixture_id.clone(),
        golden_sha256,
        preset_name: cell.preset_name.clone(),
        seed: cell.seed,
        concurrency: cell.concurrency,
        settings: SettingsSnapshot::from(settings),
        expected_tier,
        achieved_tier,
        mismatch_detail,
        sqlite3_canonical_sha256: cell
            .tiered_comparison
            .as_ref()
            .and_then(|tc| tc.sha256_a.clone()),
        fsqlite_canonical_sha256: cell
            .tiered_comparison
            .as_ref()
            .and_then(|tc| tc.sha256_b.clone()),
        wall_time_ms: cell.wall_time_ms,
    };

    // 3. Write metadata.json.
    let metadata_json = serde_json::to_string_pretty(&metadata)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    std::fs::write(bundle_dir.join("metadata.json"), &metadata_json)?;

    // 4. Compute and write diffs (best-effort — don't fail the whole bundle).
    let schema_diff = compute_schema_diff(sqlite_db, fsqlite_db).ok();
    if let Some(ref diff) = schema_diff {
        std::fs::write(bundle_dir.join("schema_diff.txt"), render_schema_diff(diff))?;
    }

    let dump_diffs = compute_dump_diffs(sqlite_db, fsqlite_db, config.max_diff_rows).ok();
    if let Some(ref diffs) = dump_diffs {
        std::fs::write(bundle_dir.join("dump_diff.txt"), render_dump_diffs(diffs))?;
    }

    let pragma_diffs = compute_pragma_diffs(sqlite_db, fsqlite_db).ok();
    if let Some(ref diffs) = pragma_diffs {
        std::fs::write(
            bundle_dir.join("pragma_diff.txt"),
            render_pragma_diffs(diffs),
        )?;
    }

    // 5. SQL dumps (best-effort).
    if config.include_dumps {
        if let Ok(dump) = dump_database(sqlite_db) {
            std::fs::write(bundle_dir.join("sqlite3_dump.sql"), dump)?;
        }
        if let Ok(dump) = dump_database(fsqlite_db) {
            std::fs::write(bundle_dir.join("fsqlite_dump.sql"), dump)?;
        }
    }

    // 6. Canonical DB copies (best-effort).
    if config.include_canonical_dbs {
        let _ =
            crate::canonicalize::canonicalize(sqlite_db, &bundle_dir.join("sqlite3_canonical.db"));
        let _ =
            crate::canonicalize::canonicalize(fsqlite_db, &bundle_dir.join("fsqlite_canonical.db"));
    }

    // 7. Working copies (optional, can be large).
    if config.include_working_copies {
        let _ = std::fs::copy(sqlite_db, bundle_dir.join("sqlite3_working.db"));
        let _ = std::fs::copy(fsqlite_db, bundle_dir.join("fsqlite_working.db"));
    }

    // 8. REPRO.md.
    let repro = render_repro_md(&metadata, &dir_name);
    std::fs::write(bundle_dir.join("REPRO.md"), repro)?;

    tracing::info!(
        bundle_dir = %bundle_dir.display(),
        fixture = %cell.fixture_id,
        preset = %cell.preset_name,
        "wrote mismatch artifact bundle"
    );

    Ok(bundle_dir)
}

/// Write schema, dump, and PRAGMA diff files into a run directory.
///
/// Best-effort: any individual diff failure is silently ignored so that
/// partial results are still available.
pub fn write_diff_files(run_dir: &Path, sqlite_db: &Path, fsqlite_db: &Path) {
    if let Ok(diff) = compute_schema_diff(sqlite_db, fsqlite_db) {
        let _ = std::fs::write(run_dir.join("schema_diff.txt"), render_schema_diff(&diff));
    }
    if let Ok(diffs) = compute_dump_diffs(sqlite_db, fsqlite_db, MAX_DIFF_ROWS) {
        let _ = std::fs::write(run_dir.join("dump_diff.txt"), render_dump_diffs(&diffs));
    }
    if let Ok(diffs) = compute_pragma_diffs(sqlite_db, fsqlite_db) {
        let _ = std::fs::write(run_dir.join("pragma_diff.txt"), render_pragma_diffs(&diffs));
    }
}

// ── Diff computation ───────────────────────────────────────────────────

/// Compare schemas between two databases.
fn compute_schema_diff(db_a: &Path, db_b: &Path) -> E2eResult<SchemaDiff> {
    let conn_a = open_readonly(db_a)?;
    let conn_b = open_readonly(db_b)?;

    let schema_a = schema_sql_map(&conn_a)?;
    let schema_b = schema_sql_map(&conn_b)?;

    let mut only_in_a = Vec::new();
    let mut only_in_b = Vec::new();
    let mut sql_differs = Vec::new();
    let mut matching_count = 0usize;

    for (name, sql_a) in &schema_a {
        if let Some(sql_b) = schema_b.get(name) {
            if sql_a == sql_b {
                matching_count += 1;
            } else {
                sql_differs.push(TableSqlDiff {
                    table: name.clone(),
                    sql_a: sql_a.clone(),
                    sql_b: sql_b.clone(),
                });
            }
        } else {
            only_in_a.push(name.clone());
        }
    }

    for name in schema_b.keys() {
        if !schema_a.contains_key(name) {
            only_in_b.push(name.clone());
        }
    }

    only_in_a.sort();
    only_in_b.sort();

    Ok(SchemaDiff {
        only_in_a,
        only_in_b,
        sql_differs,
        matching_count,
    })
}

/// Compute row-level diffs for all tables, capped by `max_rows`.
fn compute_dump_diffs(db_a: &Path, db_b: &Path, max_rows: usize) -> E2eResult<Vec<TableDumpDiff>> {
    let conn_a = open_readonly(db_a)?;
    let conn_b = open_readonly(db_b)?;

    let tables_a = list_user_tables(&conn_a)?;
    let tables_b = list_user_tables(&conn_b)?;

    // Union of all table names.
    let mut all_tables: Vec<String> = tables_a.clone();
    for t in &tables_b {
        if !all_tables.contains(t) {
            all_tables.push(t.clone());
        }
    }
    all_tables.sort();
    all_tables.truncate(MAX_DIFF_TABLES);

    let mut diffs = Vec::new();

    for table in &all_tables {
        let rows_a = if tables_a.contains(table) {
            fetch_all_rows_sorted(&conn_a, table)?
        } else {
            Vec::new()
        };
        let rows_b = if tables_b.contains(table) {
            fetch_all_rows_sorted(&conn_b, table)?
        } else {
            Vec::new()
        };

        if rows_a == rows_b {
            continue; // Skip matching tables.
        }

        let mut sample_diffs = Vec::new();
        let max_len = rows_a.len().max(rows_b.len());
        let mut diff_count = 0usize;

        for i in 0..max_len {
            let a = rows_a.get(i);
            let b = rows_b.get(i);
            if a != b {
                if diff_count < max_rows {
                    sample_diffs.push(RowDiff {
                        row_index: i,
                        values_a: a.cloned(),
                        values_b: b.cloned(),
                    });
                }
                diff_count += 1;
            }
        }

        diffs.push(TableDumpDiff {
            table: table.clone(),
            rows_a: rows_a.len(),
            rows_b: rows_b.len(),
            truncated: diff_count > max_rows,
            sample_diffs,
        });
    }

    Ok(diffs)
}

/// Compare key PRAGMAs between two databases.
fn compute_pragma_diffs(db_a: &Path, db_b: &Path) -> E2eResult<Vec<PragmaDiff>> {
    let conn_a = open_readonly(db_a)?;
    let conn_b = open_readonly(db_b)?;

    let pragmas = [
        "journal_mode",
        "page_size",
        "page_count",
        "freelist_count",
        "auto_vacuum",
        "cache_size",
        "synchronous",
        "encoding",
        "schema_version",
        "user_version",
        "application_id",
    ];

    let mut diffs = Vec::new();

    for pragma in pragmas {
        let val_a = query_pragma(&conn_a, pragma);
        let val_b = query_pragma(&conn_b, pragma);
        if val_a != val_b {
            diffs.push(PragmaDiff {
                pragma: pragma.to_owned(),
                value_a: val_a,
                value_b: val_b,
            });
        }
    }

    Ok(diffs)
}

// ── Rendering ──────────────────────────────────────────────────────────

/// Render schema diff as human-readable text.
#[must_use]
fn render_schema_diff(diff: &SchemaDiff) -> String {
    let mut out = String::new();
    out.push_str("=== Schema Diff ===\n\n");

    let _ = writeln!(out, "Matching tables: {}", diff.matching_count);

    if !diff.only_in_a.is_empty() {
        let _ = writeln!(out, "\nTables only in sqlite3:");
        for t in &diff.only_in_a {
            let _ = writeln!(out, "  - {t}");
        }
    }

    if !diff.only_in_b.is_empty() {
        let _ = writeln!(out, "\nTables only in fsqlite:");
        for t in &diff.only_in_b {
            let _ = writeln!(out, "  - {t}");
        }
    }

    if !diff.sql_differs.is_empty() {
        let _ = writeln!(out, "\nTables with different CREATE SQL:");
        for d in &diff.sql_differs {
            let _ = writeln!(out, "\n  Table: {}", d.table);
            let _ = writeln!(out, "  sqlite3: {}", d.sql_a);
            let _ = writeln!(out, "  fsqlite: {}", d.sql_b);
        }
    }

    if diff.only_in_a.is_empty() && diff.only_in_b.is_empty() && diff.sql_differs.is_empty() {
        out.push_str("\nSchemas are identical.\n");
    }

    out
}

/// Render dump diffs as human-readable text.
#[must_use]
fn render_dump_diffs(diffs: &[TableDumpDiff]) -> String {
    let mut out = String::new();
    out.push_str("=== Data Dump Diff ===\n\n");

    if diffs.is_empty() {
        out.push_str("No row-level differences found.\n");
        return out;
    }

    for diff in diffs {
        let _ = writeln!(
            out,
            "--- Table: \"{}\" (sqlite3: {} rows, fsqlite: {} rows) ---",
            diff.table, diff.rows_a, diff.rows_b
        );

        for rd in &diff.sample_diffs {
            let _ = writeln!(out, "  Row {}:", rd.row_index);
            if let Some(ref vals) = rd.values_a {
                let _ = writeln!(out, "    sqlite3: [{}]", vals.join(", "));
            } else {
                out.push_str("    sqlite3: <missing>\n");
            }
            if let Some(ref vals) = rd.values_b {
                let _ = writeln!(out, "    fsqlite: [{}]", vals.join(", "));
            } else {
                out.push_str("    fsqlite: <missing>\n");
            }
        }

        if diff.truncated {
            out.push_str("  ... (diff truncated)\n");
        }
        out.push('\n');
    }

    out
}

/// Render PRAGMA diffs as human-readable text.
#[must_use]
fn render_pragma_diffs(diffs: &[PragmaDiff]) -> String {
    let mut out = String::new();
    out.push_str("=== PRAGMA Diff ===\n\n");

    if diffs.is_empty() {
        out.push_str("No PRAGMA differences found.\n");
        return out;
    }

    out.push_str("| PRAGMA | sqlite3 | fsqlite |\n");
    out.push_str("|--------|---------|----------|\n");

    for d in diffs {
        let _ = writeln!(out, "| {} | {} | {} |", d.pragma, d.value_a, d.value_b);
    }

    out
}

/// Render the REPRO.md file with copy-paste reproduction commands.
#[must_use]
fn render_repro_md(meta: &BundleMetadata, bundle_name: &str) -> String {
    let mut md = String::new();

    md.push_str("# Mismatch Reproduction Guide\n\n");

    md.push_str("## Summary\n\n");
    let _ = writeln!(md, "- **Fixture:** `{}`", meta.fixture_id);
    if let Some(ref sha) = meta.golden_sha256 {
        let _ = writeln!(md, "- **Golden SHA-256:** `{sha}`");
    }
    let _ = writeln!(md, "- **Preset:** `{}`", meta.preset_name);
    let _ = writeln!(md, "- **Seed:** `{}`", meta.seed);
    let _ = writeln!(md, "- **Concurrency:** `{}`", meta.concurrency);
    let _ = writeln!(md, "- **Expected tier:** `{}`", meta.expected_tier);
    let _ = writeln!(
        md,
        "- **Achieved tier:** `{}`",
        meta.achieved_tier.as_deref().unwrap_or("none")
    );
    let _ = writeln!(md, "- **Detail:** {}", meta.mismatch_detail);

    md.push_str("\n## Engine Settings\n\n");
    let _ = writeln!(md, "- `journal_mode = {}`", meta.settings.journal_mode);
    let _ = writeln!(md, "- `synchronous = {}`", meta.settings.synchronous);
    let _ = writeln!(md, "- `cache_size = {}`", meta.settings.cache_size);
    let _ = writeln!(md, "- `page_size = {}`", meta.settings.page_size);
    let _ = writeln!(
        md,
        "- `busy_timeout = {} ms`",
        meta.settings.busy_timeout_ms
    );
    let _ = writeln!(
        md,
        "- `concurrent_mode = {}`",
        meta.settings.concurrent_mode
    );

    md.push_str("\n## Reproduce\n\n");
    md.push_str("```bash\n");
    md.push_str("# Run the exact same cell that produced this mismatch:\n");
    let _ = writeln!(
        md,
        "cargo run --release -p fsqlite-e2e --bin realdb-e2e -- compare \\",
    );
    let _ = writeln!(md, "  --fixture {} \\", meta.fixture_id);
    let _ = writeln!(md, "  --preset {} \\", meta.preset_name);
    let _ = writeln!(md, "  --seed {} \\", meta.seed);
    let _ = writeln!(md, "  --concurrency {}", meta.concurrency);
    md.push_str("```\n");

    md.push_str("\n## Inspect artifacts\n\n");
    md.push_str("```bash\n");
    let _ = writeln!(md, "# Bundle directory:");
    let _ = writeln!(md, "ls reports/mismatches/{bundle_name}/");
    md.push_str("\n");
    let _ = writeln!(md, "# View schema diff:");
    let _ = writeln!(md, "cat reports/mismatches/{bundle_name}/schema_diff.txt");
    md.push_str("\n");
    let _ = writeln!(md, "# View data diff:");
    let _ = writeln!(md, "cat reports/mismatches/{bundle_name}/dump_diff.txt");
    md.push_str("\n");
    let _ = writeln!(md, "# View PRAGMA diff:");
    let _ = writeln!(md, "cat reports/mismatches/{bundle_name}/pragma_diff.txt");
    md.push_str("```\n");

    if meta.sqlite3_canonical_sha256.is_some() || meta.fsqlite_canonical_sha256.is_some() {
        md.push_str("\n## Canonical Hashes\n\n");
        if let Some(ref sha) = meta.sqlite3_canonical_sha256 {
            let _ = writeln!(md, "- sqlite3: `{sha}`");
        }
        if let Some(ref sha) = meta.fsqlite_canonical_sha256 {
            let _ = writeln!(md, "- fsqlite: `{sha}`");
        }
    }

    md
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Open a database read-only via rusqlite.
fn open_readonly(path: &Path) -> E2eResult<rusqlite::Connection> {
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    Ok(rusqlite::Connection::open_with_flags(path, flags)?)
}

/// Get schema SQL for all user tables as a map.
fn schema_sql_map(
    conn: &rusqlite::Connection,
) -> E2eResult<std::collections::BTreeMap<String, String>> {
    let mut stmt = conn.prepare(
        "SELECT name, sql FROM sqlite_master \
         WHERE type='table' AND name NOT LIKE 'sqlite_%' \
         ORDER BY name",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
    Ok(rows)
}

/// List user table names, sorted.
fn list_user_tables(conn: &rusqlite::Connection) -> E2eResult<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master \
         WHERE type='table' AND name NOT LIKE 'sqlite_%' \
         ORDER BY name",
    )?;
    let names: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<String>, _>>()?;
    Ok(names)
}

/// Fetch all rows from a table, sorted by rowid (or first column fallback).
fn fetch_all_rows_sorted(conn: &rusqlite::Connection, table: &str) -> E2eResult<Vec<Vec<String>>> {
    let sql = format!("SELECT * FROM \"{table}\" ORDER BY rowid");
    let fallback_sql = format!("SELECT * FROM \"{table}\" ORDER BY 1");

    let sql_to_use = if conn.prepare(&sql).is_ok() {
        sql
    } else {
        fallback_sql
    };

    let mut stmt = conn.prepare(&sql_to_use)?;
    let col_count = stmt.column_count();
    let rows: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            let mut vals = Vec::with_capacity(col_count);
            for i in 0..col_count {
                let v: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                vals.push(format_value(&v));
            }
            Ok(vals)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Format a rusqlite value deterministically for diffing.
fn format_value(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(i) => i.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => s.clone(),
        rusqlite::types::Value::Blob(b) => {
            let mut hex = String::with_capacity(b.len() * 2 + 2);
            hex.push_str("X'");
            for byte in b {
                let _ = write!(hex, "{byte:02X}");
            }
            hex.push('\'');
            hex
        }
    }
}

/// Query a single PRAGMA value as a string.
fn query_pragma(conn: &rusqlite::Connection, pragma: &str) -> String {
    conn.query_row(&format!("PRAGMA {pragma}"), [], |row| {
        row.get::<_, String>(0)
    })
    .unwrap_or_else(|_| "<error>".to_owned())
}

/// Compute SHA-256 of a file.
fn sha256_file(path: &Path) -> E2eResult<String> {
    let data = std::fs::read(path)?;
    let digest = Sha256::digest(&data);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    Ok(hex)
}

/// Produce a deterministic SQL dump of a database.
///
/// Generates `CREATE TABLE` + `INSERT` statements for all user tables,
/// ordered by table name and rowid.
fn dump_database(db_path: &Path) -> E2eResult<String> {
    let conn = open_readonly(db_path)?;
    let tables = list_user_tables(&conn)?;

    let mut dump = String::new();
    dump.push_str("-- Deterministic SQL dump\n");
    let _ = writeln!(dump, "-- Source: {}", db_path.display());
    dump.push_str("BEGIN TRANSACTION;\n\n");

    for table in &tables {
        // Schema.
        let sql: String = conn.query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            [table],
            |row| row.get(0),
        )?;
        let _ = writeln!(dump, "{sql};\n");

        // Data.
        let rows = fetch_all_rows_sorted(&conn, table)?;
        if !rows.is_empty() {
            // Get column names for INSERT statements.
            let col_names = {
                let stmt = conn.prepare(&format!("SELECT * FROM \"{table}\" LIMIT 0"))?;
                (0..stmt.column_count())
                    .map(|i| format!("\"{}\"", stmt.column_name(i).unwrap_or("?")))
                    .collect::<Vec<_>>()
                    .join(", ")
            };

            for row in &rows {
                let values = row
                    .iter()
                    .map(|v| {
                        if v == "NULL" {
                            "NULL".to_owned()
                        } else {
                            // Escape single quotes.
                            format!("'{}'", v.replace('\'', "''"))
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let _ = writeln!(
                    dump,
                    "INSERT INTO \"{table}\" ({col_names}) VALUES ({values});"
                );
            }
            dump.push('\n');
        }
    }

    dump.push_str("COMMIT;\n");
    Ok(dump)
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch_runner::CellVerdict;
    use crate::canonicalize::TieredComparisonResult;

    #[test]
    fn test_bundle_dir_name() {
        let cell = CellResult {
            fixture_id: "chinook".to_owned(),
            preset_name: "hot_page_contention".to_owned(),
            concurrency: 4,
            seed: 42,
            wall_time_ms: 100,
            artifact_dir: None,
            sqlite_report: None,
            fsqlite_report: None,
            tiered_comparison: None,
            verdict: CellVerdict::Mismatch {
                expected_tier: "Tier1Raw".to_owned(),
                achieved_tier: Some("Tier 2".to_owned()),
                detail: "SHA-256 mismatch".to_owned(),
            },
        };
        assert_eq!(
            bundle_dir_name(&cell),
            "chinook__hot_page_contention__c4__s42"
        );
    }

    #[test]
    fn test_render_schema_diff_identical() {
        let diff = SchemaDiff {
            only_in_a: Vec::new(),
            only_in_b: Vec::new(),
            sql_differs: Vec::new(),
            matching_count: 5,
        };
        let rendered = render_schema_diff(&diff);
        assert!(rendered.contains("Schemas are identical"));
        assert!(rendered.contains("Matching tables: 5"));
    }

    #[test]
    fn test_render_schema_diff_with_differences() {
        let diff = SchemaDiff {
            only_in_a: vec!["extra_table".to_owned()],
            only_in_b: Vec::new(),
            sql_differs: vec![TableSqlDiff {
                table: "users".to_owned(),
                sql_a: "CREATE TABLE users (id INTEGER)".to_owned(),
                sql_b: "CREATE TABLE users (id INTEGER, name TEXT)".to_owned(),
            }],
            matching_count: 3,
        };
        let rendered = render_schema_diff(&diff);
        assert!(rendered.contains("extra_table"));
        assert!(rendered.contains("Tables with different CREATE SQL"));
        assert!(rendered.contains("users"));
    }

    #[test]
    fn test_render_dump_diffs_empty() {
        let rendered = render_dump_diffs(&[]);
        assert!(rendered.contains("No row-level differences"));
    }

    #[test]
    fn test_render_dump_diffs_with_data() {
        let diffs = vec![TableDumpDiff {
            table: "orders".to_owned(),
            rows_a: 10,
            rows_b: 8,
            sample_diffs: vec![RowDiff {
                row_index: 5,
                values_a: Some(vec!["5".to_owned(), "Alice".to_owned()]),
                values_b: Some(vec!["5".to_owned(), "Bob".to_owned()]),
            }],
            truncated: false,
        }];
        let rendered = render_dump_diffs(&diffs);
        assert!(rendered.contains("orders"));
        assert!(rendered.contains("sqlite3: [5, Alice]"));
        assert!(rendered.contains("fsqlite: [5, Bob]"));
    }

    #[test]
    fn test_render_pragma_diffs_empty() {
        let rendered = render_pragma_diffs(&[]);
        assert!(rendered.contains("No PRAGMA differences"));
    }

    #[test]
    fn test_render_pragma_diffs_with_data() {
        let diffs = vec![PragmaDiff {
            pragma: "page_count".to_owned(),
            value_a: "10".to_owned(),
            value_b: "12".to_owned(),
        }];
        let rendered = render_pragma_diffs(&diffs);
        assert!(rendered.contains("page_count"));
        assert!(rendered.contains("10"));
        assert!(rendered.contains("12"));
    }

    #[test]
    fn test_render_repro_md() {
        let meta = BundleMetadata {
            schema_version: "fsqlite-e2e.mismatch_bundle.v1".to_owned(),
            fixture_id: "chinook".to_owned(),
            golden_sha256: Some("abc123".to_owned()),
            preset_name: "hot_page_contention".to_owned(),
            seed: 42,
            concurrency: 4,
            settings: SettingsSnapshot {
                journal_mode: "wal".to_owned(),
                synchronous: "NORMAL".to_owned(),
                cache_size: -2000,
                page_size: 4096,
                busy_timeout_ms: 5000,
                concurrent_mode: true,
            },
            expected_tier: "Tier1Raw".to_owned(),
            achieved_tier: Some("Tier 2: Logical Match".to_owned()),
            mismatch_detail: "canonical SHA-256 differs".to_owned(),
            sqlite3_canonical_sha256: Some("sha_a".to_owned()),
            fsqlite_canonical_sha256: Some("sha_b".to_owned()),
            wall_time_ms: 250,
        };
        let md = render_repro_md(&meta, "chinook__hot_page_contention__c4__s42");
        assert!(md.contains("chinook"));
        assert!(md.contains("hot_page_contention"));
        assert!(md.contains("--seed 42"));
        assert!(md.contains("--concurrency 4"));
        assert!(md.contains("abc123"));
        assert!(md.contains("journal_mode = wal"));
        assert!(md.contains("sha_a"));
        assert!(md.contains("sha_b"));
    }

    #[test]
    fn test_settings_snapshot_from_harness() {
        let settings = HarnessSettings::default();
        let snap = SettingsSnapshot::from(&settings);
        assert_eq!(snap.journal_mode, "wal");
        assert_eq!(snap.synchronous, "NORMAL");
        assert_eq!(snap.page_size, 4096);
        assert!(snap.concurrent_mode);
    }

    #[test]
    fn test_bundle_config_defaults() {
        let config = BundleConfig::default();
        assert!(config.include_dumps);
        assert!(!config.include_canonical_dbs);
        assert!(!config.include_working_copies);
        assert_eq!(config.max_diff_rows, MAX_DIFF_ROWS);
    }

    #[test]
    fn test_write_mismatch_bundle_produces_files() {
        let tmp = tempfile::TempDir::new().unwrap();

        // Create two simple test databases.
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        let conn_a = rusqlite::Connection::open(&db_a).unwrap();
        conn_a
            .execute_batch("CREATE TABLE t(x INTEGER); INSERT INTO t VALUES(1);")
            .unwrap();
        drop(conn_a);

        let conn_b = rusqlite::Connection::open(&db_b).unwrap();
        conn_b
            .execute_batch("CREATE TABLE t(x INTEGER); INSERT INTO t VALUES(2);")
            .unwrap();
        drop(conn_b);

        let cell = CellResult {
            fixture_id: "test".to_owned(),
            preset_name: "basic".to_owned(),
            concurrency: 1,
            seed: 1,
            wall_time_ms: 50,
            artifact_dir: None,
            sqlite_report: None,
            fsqlite_report: None,
            tiered_comparison: Some(TieredComparisonResult {
                tier: crate::canonicalize::ComparisonTier::LogicalMatch,
                sha256_a: Some("aaa".to_owned()),
                sha256_b: Some("bbb".to_owned()),
                byte_match: false,
                logical_match: false,
                row_counts_match: true,
                detail: "row mismatch".to_owned(),
            }),
            verdict: CellVerdict::Mismatch {
                expected_tier: "Tier1Raw".to_owned(),
                achieved_tier: Some("Tier 2".to_owned()),
                detail: "SHA mismatch".to_owned(),
            },
        };

        let config = BundleConfig {
            output_base: tmp.path().join("bundles"),
            include_dumps: true,
            include_canonical_dbs: false,
            include_working_copies: false,
            max_diff_rows: 50,
        };

        let settings = HarnessSettings::default();
        let result = write_mismatch_bundle(&cell, &db_a, &db_b, None, &settings, &config);
        assert!(result.is_ok(), "write_mismatch_bundle failed: {result:?}");

        let bundle_dir = result.unwrap();
        assert!(bundle_dir.join("metadata.json").exists());
        assert!(bundle_dir.join("REPRO.md").exists());
        assert!(bundle_dir.join("schema_diff.txt").exists());
        assert!(bundle_dir.join("dump_diff.txt").exists());
        assert!(bundle_dir.join("pragma_diff.txt").exists());
        assert!(bundle_dir.join("sqlite3_dump.sql").exists());
        assert!(bundle_dir.join("fsqlite_dump.sql").exists());

        // Verify metadata is valid JSON.
        let meta_str = std::fs::read_to_string(bundle_dir.join("metadata.json")).unwrap();
        let meta: BundleMetadata = serde_json::from_str(&meta_str).unwrap();
        assert_eq!(meta.fixture_id, "test");
        assert_eq!(meta.preset_name, "basic");
        assert_eq!(meta.seed, 1);

        // Verify dump diff has actual content.
        let diff = std::fs::read_to_string(bundle_dir.join("dump_diff.txt")).unwrap();
        assert!(diff.contains("Table: \"t\""));

        // Verify REPRO.md has repro commands.
        let repro = std::fs::read_to_string(bundle_dir.join("REPRO.md")).unwrap();
        assert!(repro.contains("--preset basic"));
    }
}
