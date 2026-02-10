//! Per-run working directory management.
//!
//! Each E2E run gets its own isolated directory under `sample_sqlite_db_files/working/`
//! (or a custom base path).  Golden database files are **copied** into the run directory
//! so that workers can read and write freely without modifying the immutable golden copies.
//!
//! Directory naming uses a timestamp + random suffix to guarantee uniqueness even
//! under concurrent launches.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::{E2eError, E2eResult};

/// Default relative path from the project root to the golden corpus.
pub const GOLDEN_DIR: &str = "sample_sqlite_db_files/golden";

/// Default relative path from the project root to the working directory.
pub const WORKING_DIR: &str = "sample_sqlite_db_files/working";

/// Known sidecar extensions that should be copied alongside a `.db` file.
const SIDECAR_EXTENSIONS: &[&str] = &["-wal", "-shm", "-journal"];

// ── RunWorkspace ─────────────────────────────────────────────────────

/// An isolated run directory containing copies of golden databases.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunWorkspace {
    /// Absolute path to the run directory.
    pub run_dir: PathBuf,
    /// Database copies made into this workspace: `(db_id, db_path, sidecars)`.
    pub databases: Vec<RunDatabase>,
    /// Unix timestamp (seconds) when this workspace was created.
    pub created_at: u64,
}

/// A single database file prepared in the run workspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunDatabase {
    /// Identifier derived from the golden filename (stem).
    pub db_id: String,
    /// Absolute path to the copied database file.
    pub db_path: PathBuf,
    /// Paths to any copied sidecar files (WAL, SHM, journal).
    pub sidecars: Vec<PathBuf>,
    /// Absolute path to the original golden source.
    pub golden_source: PathBuf,
}

/// Configuration for workspace creation.
#[derive(Debug, Clone)]
pub struct WorkspaceConfig {
    /// Directory containing the golden (immutable) database files.
    pub golden_dir: PathBuf,
    /// Base directory for run working directories.
    pub working_base: PathBuf,
}

impl WorkspaceConfig {
    /// Create a config using the default paths relative to `project_root`.
    #[must_use]
    pub fn from_project_root(project_root: &Path) -> Self {
        Self {
            golden_dir: project_root.join(GOLDEN_DIR),
            working_base: project_root.join(WORKING_DIR),
        }
    }
}

// ── Public API ───────────────────────────────────────────────────────

/// Create a new run workspace and copy the specified golden databases into it.
///
/// If `db_ids` is empty, **all** golden databases are copied.
///
/// # Errors
///
/// Returns `E2eError::Io` if the golden directory is missing, a requested
/// `db_id` has no matching file, or any copy operation fails.
pub fn create_workspace(config: &WorkspaceConfig, db_ids: &[&str]) -> E2eResult<RunWorkspace> {
    create_workspace_inner(config, db_ids, None)
}

/// Create a new run workspace with a human-readable label embedded in the
/// run directory name.
///
/// The label is sanitized for filesystem safety and intended to embed stable
/// scenario ids into artifact paths (e.g. corruption demos).
///
/// # Errors
///
/// Same error conditions as [`create_workspace`].
pub fn create_workspace_with_label(
    config: &WorkspaceConfig,
    db_ids: &[&str],
    label: &str,
) -> E2eResult<RunWorkspace> {
    let sanitized = sanitize_run_label(label);
    let run_label = if sanitized.is_empty() {
        None
    } else {
        Some(sanitized)
    };
    create_workspace_inner(config, db_ids, run_label)
}

fn create_workspace_inner(
    config: &WorkspaceConfig,
    db_ids: &[&str],
    run_label: Option<String>,
) -> E2eResult<RunWorkspace> {
    // Validate golden dir exists.
    if !config.golden_dir.is_dir() {
        return Err(E2eError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "golden directory not found: {}",
                config.golden_dir.display()
            ),
        )));
    }

    // Generate a unique run directory name.
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let ts = now.as_secs();
    let nanos = now.subsec_nanos();
    let pid = std::process::id();
    let mut run_name = format!("run_{ts}_{nanos:09}_{pid}");
    if let Some(label) = run_label {
        // Stable, filesystem-safe label for reports and artifact directories.
        run_name.push('_');
        run_name.push_str(&label);
    }
    let run_dir = config.working_base.join(&run_name);

    // Create the run directory (fail if it somehow already exists).
    std::fs::create_dir_all(&run_dir)?;

    // Resolve which databases to copy.
    let golden_dbs = discover_golden_dbs(&config.golden_dir)?;

    let targets: Vec<&(String, PathBuf)> = if db_ids.is_empty() {
        golden_dbs.iter().collect()
    } else {
        let mut targets = Vec::with_capacity(db_ids.len());
        for id in db_ids {
            let found = golden_dbs.iter().find(|(name, _)| name == id);
            match found {
                Some(entry) => targets.push(entry),
                None => {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("no golden database with id `{id}`"),
                    )));
                }
            }
        }
        targets
    };

    // Copy each database (+ sidecars) into the run dir.
    let mut databases = Vec::with_capacity(targets.len());
    for (db_id, golden_path) in targets {
        let dest_name = golden_path
            .file_name()
            .expect("golden path should have a filename");
        let dest_path = run_dir.join(dest_name);
        std::fs::copy(golden_path, &dest_path)?;

        let mut sidecars = Vec::new();
        for ext in SIDECAR_EXTENSIONS {
            // Sidecars use the form "foo.db-wal", not "foo.-wal".
            let mut src_str = golden_path.as_os_str().to_os_string();
            src_str.push(ext);
            let sidecar_src = PathBuf::from(src_str);

            if sidecar_src.exists() {
                let sidecar_name = sidecar_src
                    .file_name()
                    .expect("sidecar should have a filename");
                let sidecar_dest = run_dir.join(sidecar_name);
                std::fs::copy(&sidecar_src, &sidecar_dest)?;
                sidecars.push(sidecar_dest);
            }
        }

        databases.push(RunDatabase {
            db_id: db_id.clone(),
            db_path: dest_path,
            sidecars,
            golden_source: golden_path.clone(),
        });
    }

    Ok(RunWorkspace {
        run_dir,
        databases,
        created_at: ts,
    })
}

/// Remove a run workspace directory and all its contents.
///
/// # Errors
///
/// Returns `E2eError::Io` on filesystem errors.
pub fn cleanup_workspace(workspace: &RunWorkspace) -> E2eResult<()> {
    if workspace.run_dir.exists() {
        std::fs::remove_dir_all(&workspace.run_dir)?;
    }
    Ok(())
}

/// Return a human-readable summary of the workspace.
#[must_use]
pub fn workspace_summary(workspace: &RunWorkspace) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "Run workspace: {}", workspace.run_dir.display());
    let _ = writeln!(out, "Created at: {} (unix)", workspace.created_at);
    let _ = writeln!(out, "Databases: {}", workspace.databases.len());
    for db in &workspace.databases {
        let sidecar_count = db.sidecars.len();
        let _ = writeln!(
            out,
            "  - {} → {} ({sidecar_count} sidecar(s))",
            db.db_id,
            db.db_path.display(),
        );
    }
    out
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Discover all `.db` files in the golden directory.  Returns `(stem, path)` pairs
/// sorted by stem.
fn discover_golden_dbs(golden_dir: &Path) -> E2eResult<Vec<(String, PathBuf)>> {
    let mut dbs: Vec<(String, PathBuf)> = Vec::new();

    for entry in std::fs::read_dir(golden_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("db") {
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_owned();
            if !stem.is_empty() {
                dbs.push((stem, path));
            }
        }
    }

    dbs.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(dbs)
}

fn sanitize_run_label(label: &str) -> String {
    // Keep run directory names portable and stable: ASCII-only and
    // limited to a small set of safe characters.
    let mut out = String::with_capacity(label.len().min(80));
    let mut prev_sep = false;
    for b in label.as_bytes().iter().copied().take(80) {
        let ch = b as char;
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch.to_ascii_lowercase());
            prev_sep = false;
        } else if !out.is_empty() && !prev_sep {
            out.push('_');
            prev_sep = true;
        }
    }
    out.trim_matches('_').to_owned()
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a tiny golden directory with fake DB files for testing.
    fn setup_fake_golden(dir: &Path) -> PathBuf {
        let golden = dir.join("golden");
        std::fs::create_dir_all(&golden).unwrap();

        // Create two fake DBs with sidecars.
        std::fs::write(golden.join("alpha.db"), b"alpha-data").unwrap();
        std::fs::write(golden.join("alpha.db-wal"), b"alpha-wal").unwrap();
        std::fs::write(golden.join("alpha.db-shm"), b"alpha-shm").unwrap();

        std::fs::write(golden.join("beta.db"), b"beta-data").unwrap();
        std::fs::write(golden.join("beta.db-wal"), b"beta-wal").unwrap();

        golden
    }

    #[test]
    fn test_create_workspace_copies_all_dbs() {
        let tmp = tempfile::tempdir().unwrap();
        let golden = setup_fake_golden(tmp.path());
        let working = tmp.path().join("working");
        std::fs::create_dir_all(&working).unwrap();

        let config = WorkspaceConfig {
            golden_dir: golden,
            working_base: working,
        };

        let ws = create_workspace(&config, &[]).unwrap();
        assert_eq!(ws.databases.len(), 2);
        assert!(ws.run_dir.exists());

        // Verify files were copied.
        for db in &ws.databases {
            assert!(
                db.db_path.exists(),
                "DB file should exist: {:?}",
                db.db_path
            );
            for sidecar in &db.sidecars {
                assert!(sidecar.exists(), "sidecar should exist: {sidecar:?}");
            }
        }

        // Verify golden source paths are populated.
        assert!(ws.databases.iter().all(|d| d.golden_source.exists()));
    }

    #[test]
    fn test_create_workspace_specific_db() {
        let tmp = tempfile::tempdir().unwrap();
        let golden = setup_fake_golden(tmp.path());
        let working = tmp.path().join("working");
        std::fs::create_dir_all(&working).unwrap();

        let config = WorkspaceConfig {
            golden_dir: golden,
            working_base: working,
        };

        let ws = create_workspace(&config, &["alpha"]).unwrap();
        assert_eq!(ws.databases.len(), 1);
        assert_eq!(ws.databases[0].db_id, "alpha");
        // alpha has 2 sidecars: -wal, -shm.
        assert_eq!(ws.databases[0].sidecars.len(), 2);
    }

    #[test]
    fn test_create_workspace_missing_db_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let golden = setup_fake_golden(tmp.path());
        let working = tmp.path().join("working");
        std::fs::create_dir_all(&working).unwrap();

        let config = WorkspaceConfig {
            golden_dir: golden,
            working_base: working,
        };

        let result = create_workspace(&config, &["nonexistent"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_concurrent_workspaces_are_isolated() {
        let tmp = tempfile::tempdir().unwrap();
        let golden = setup_fake_golden(tmp.path());
        let working = tmp.path().join("working");
        std::fs::create_dir_all(&working).unwrap();

        let config = WorkspaceConfig {
            golden_dir: golden,
            working_base: working,
        };

        let ws1 = create_workspace(&config, &["alpha"]).unwrap();
        let ws2 = create_workspace(&config, &["alpha"]).unwrap();

        // Different run directories.
        assert_ne!(ws1.run_dir, ws2.run_dir);

        // Both have the file.
        assert!(ws1.databases[0].db_path.exists());
        assert!(ws2.databases[0].db_path.exists());

        // Modifying one does not affect the other.
        std::fs::write(&ws1.databases[0].db_path, b"modified").unwrap();
        let ws2_content = std::fs::read(&ws2.databases[0].db_path).unwrap();
        assert_eq!(ws2_content, b"alpha-data");
    }

    #[test]
    fn test_cleanup_workspace_removes_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let golden = setup_fake_golden(tmp.path());
        let working = tmp.path().join("working");
        std::fs::create_dir_all(&working).unwrap();

        let config = WorkspaceConfig {
            golden_dir: golden,
            working_base: working,
        };

        let ws = create_workspace(&config, &[]).unwrap();
        assert!(ws.run_dir.exists());

        cleanup_workspace(&ws).unwrap();
        assert!(!ws.run_dir.exists());
    }

    #[test]
    fn test_golden_dir_is_never_written() {
        let tmp = tempfile::tempdir().unwrap();
        let golden = setup_fake_golden(tmp.path());
        let working = tmp.path().join("working");
        std::fs::create_dir_all(&working).unwrap();

        // Record golden file contents before creating workspace.
        let alpha_before = std::fs::read(golden.join("alpha.db")).unwrap();

        let config = WorkspaceConfig {
            golden_dir: golden.clone(),
            working_base: working,
        };

        let ws = create_workspace(&config, &["alpha"]).unwrap();

        // Write to the workspace copy.
        std::fs::write(&ws.databases[0].db_path, b"trashed").unwrap();

        // Golden file should be unchanged.
        let alpha_after = std::fs::read(golden.join("alpha.db")).unwrap();
        assert_eq!(alpha_before, alpha_after, "golden file must not change");
    }

    #[test]
    fn test_workspace_summary_includes_details() {
        let tmp = tempfile::tempdir().unwrap();
        let golden = setup_fake_golden(tmp.path());
        let working = tmp.path().join("working");
        std::fs::create_dir_all(&working).unwrap();

        let config = WorkspaceConfig {
            golden_dir: golden,
            working_base: working,
        };

        let ws = create_workspace(&config, &[]).unwrap();
        let summary = workspace_summary(&ws);

        assert!(summary.contains("Run workspace:"));
        assert!(summary.contains("Databases: 2"));
        assert!(summary.contains("alpha"));
        assert!(summary.contains("beta"));
    }

    #[test]
    fn test_sidecar_copy_preserves_content() {
        let tmp = tempfile::tempdir().unwrap();
        let golden = setup_fake_golden(tmp.path());
        let working = tmp.path().join("working");
        std::fs::create_dir_all(&working).unwrap();

        let config = WorkspaceConfig {
            golden_dir: golden,
            working_base: working,
        };

        let ws = create_workspace(&config, &["alpha"]).unwrap();

        let wal_content = std::fs::read(
            ws.databases[0]
                .sidecars
                .iter()
                .find(|p| p.to_string_lossy().contains("-wal"))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(wal_content, b"alpha-wal");

        let shm_content = std::fs::read(
            ws.databases[0]
                .sidecars
                .iter()
                .find(|p| p.to_string_lossy().contains("-shm"))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(shm_content, b"alpha-shm");
    }

    #[test]
    fn test_discover_golden_dbs_sorted() {
        let tmp = tempfile::tempdir().unwrap();
        let golden = setup_fake_golden(tmp.path());

        let dbs = discover_golden_dbs(&golden).unwrap();
        assert_eq!(dbs.len(), 2);
        assert_eq!(dbs[0].0, "alpha");
        assert_eq!(dbs[1].0, "beta");
    }

    #[test]
    fn test_missing_golden_dir_returns_error() {
        let config = WorkspaceConfig {
            golden_dir: PathBuf::from("/nonexistent/golden"),
            working_base: PathBuf::from("/tmp"),
        };

        let result = create_workspace(&config, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_workspace_with_label_sanitizes_and_embeds_label() {
        let tmp = tempfile::tempdir().unwrap();
        let golden = setup_fake_golden(tmp.path());
        let working = tmp.path().join("working");
        std::fs::create_dir_all(&working).unwrap();

        let config = WorkspaceConfig {
            golden_dir: golden,
            working_base: working,
        };

        let ws = create_workspace_with_label(&config, &["alpha"], "Wal Bitrot: frame#1!") //
            .unwrap();

        let run_name = ws.run_dir.file_name().unwrap().to_string_lossy();
        assert!(
            run_name.contains("_wal_bitrot_frame_1"),
            "run name should include sanitized label, got: {run_name}"
        );
    }
}
