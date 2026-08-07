//! Asupersync-only runtime enforcement (bd-ebua, §1.3).
//!
//! These tests gate CI to reject any introduction of forbidden async runtimes
//! (tokio, async-std, smol) and verify that I/O trait methods accept the `&Cx`
//! capability parameter.

use std::collections::HashSet;
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;

const BEAD_ID: &str = "bd-ebua";

/// Async runtimes that MUST NOT appear in the resolved dependency graph.
const FORBIDDEN_RUNTIMES: &[&str] = &["tokio", "async-std", "smol", "futures-executor"];

/// Macro prefixes from forbidden runtimes that MUST NOT appear in workspace source.
///
/// Built at runtime via `forbidden_macro_patterns()` so the literal strings do
/// not appear in *this* file and trigger a false-positive self-detection.
fn forbidden_macro_patterns() -> Vec<String> {
    // Each entry: (crate, attr).  We concatenate "#[{crate}::{attr}]" at runtime
    // so the scanner never sees the assembled pattern in source.
    let entries: &[(&str, &str)] = &[
        ("tokio", "main"),
        ("tokio", "test"),
        ("async_std", "main"),
        ("async_std", "test"),
        ("smol_potat", "main"),
        ("smol_potat", "test"),
    ];
    entries
        .iter()
        .map(|(krate, attr)| format!("#[{krate}::{attr}]"))
        .collect()
}

fn workspace_root() -> &'static Path {
    static ROOT: OnceLock<&Path> = OnceLock::new();
    ROOT.get_or_init(|| {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        manifest_dir
            .parent()
            .and_then(Path::parent)
            .expect("workspace root should be two levels up from fsqlite-harness")
    })
}

fn cargo_metadata_cached() -> &'static serde_json::Value {
    static METADATA: OnceLock<serde_json::Value> = OnceLock::new();
    METADATA.get_or_init(|| {
        let root = workspace_root();
        let output = Command::new("cargo")
            .args(["metadata", "--format-version=1", "--locked"])
            .current_dir(root)
            .output()
            .expect("failed to execute cargo metadata");
        assert!(
            output.status.success(),
            "bead_id={BEAD_ID} cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).expect("cargo metadata JSON parse failed")
    })
}

/// Extract workspace member package IDs from cargo metadata.
fn workspace_member_ids(metadata: &serde_json::Value) -> HashSet<String> {
    metadata["workspace_members"]
        .as_array()
        .expect("workspace_members should be an array")
        .iter()
        .filter_map(|m| m.as_str().map(String::from))
        .collect()
}

/// Extract crate name from a cargo metadata package ID.
fn name_from_pkg_id(id: &str) -> String {
    if id.starts_with("path+file://") {
        let without_fragment = id.split('#').next().unwrap_or(id);
        without_fragment
            .rsplit('/')
            .next()
            .unwrap_or(id)
            .to_string()
    } else {
        id.split_whitespace().next().unwrap_or(id).to_string()
    }
}

/// Return a list of forbidden direct dependency edges: `{crate} -> {dep}`.
fn forbidden_direct_deps(metadata: &serde_json::Value) -> Vec<String> {
    let member_ids = workspace_member_ids(metadata);

    let resolve = metadata["resolve"]["nodes"]
        .as_array()
        .expect("resolve.nodes should be an array");

    let mut violations: Vec<String> = Vec::new();

    for node in resolve {
        let id = node["id"].as_str().unwrap_or_default();
        let name = name_from_pkg_id(id);

        // Only check workspace crates' direct dependencies.
        if !member_ids.iter().any(|m| name_from_pkg_id(m) == name) {
            continue;
        }

        if let Some(deps) = node["deps"].as_array() {
            for dep in deps {
                let dep_name = dep["name"].as_str().unwrap_or_default().replace('_', "-");
                if FORBIDDEN_RUNTIMES.contains(&dep_name.as_str()) {
                    violations.push(format!("{name} -> {dep_name} (FORBIDDEN)"));
                }
            }
        }
    }

    violations
}

// ---------------------------------------------------------------------------
// Test: No forbidden runtimes in workspace dependency graph
// ---------------------------------------------------------------------------

#[test]
fn test_no_tokio_in_cargo_lock() {
    let metadata = cargo_metadata_cached();
    let violations = forbidden_direct_deps(metadata);

    assert!(
        violations.is_empty(),
        "bead_id={BEAD_ID} case=no_tokio_in_cargo_lock \
         Workspace crates must not directly depend on forbidden async runtimes.\n\
         Violations:\n{}",
        violations.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Test: No forbidden macros in workspace source
// ---------------------------------------------------------------------------

#[test]
fn test_no_tokio_macros_in_source() {
    let root = workspace_root();

    let mut violations: Vec<String> = Vec::new();

    let patterns = forbidden_macro_patterns();

    // Scan known workspace source roots (avoid target/ and other generated dirs).
    for rel_dir in ["crates", "src", "tests", "benches", "fuzz"] {
        let dir = root.join(rel_dir);
        if !dir.exists() {
            continue;
        }

        visit_rs_files(&dir, &mut |path, contents| {
            for macro_pat in &patterns {
                if contents.contains(macro_pat.as_str()) {
                    violations.push(format!(
                        "{}: contains `{macro_pat}`",
                        path.strip_prefix(root).unwrap_or(path).display()
                    ));
                }
            }
        });
    }

    assert!(
        violations.is_empty(),
        "bead_id={BEAD_ID} case=no_tokio_macros_in_source \
         Found forbidden async runtime macros in workspace source:\n{}",
        violations.join("\n")
    );
}

/// Recursively visit all `.rs` files under `dir`, calling `f(path, contents)`.
fn visit_rs_files(dir: &Path, f: &mut impl FnMut(&Path, &str)) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            visit_rs_files(&path, f);
        } else if path.extension().is_some_and(|ext| ext == "rs")
            && let Ok(contents) = std::fs::read_to_string(&path)
        {
            f(&path, &contents);
        }
    }
}

// ---------------------------------------------------------------------------
// Test: VFS trait methods accept &Cx
// ---------------------------------------------------------------------------

#[test]
fn test_cx_parameter_on_vfs_trait() {
    use fsqlite_error::Result;
    use fsqlite_types::LockLevel;
    use fsqlite_types::cx::Cx;
    use fsqlite_vfs::VfsFile;

    // Compile-time check: these generic wrappers only compile if the trait
    // methods accept `&Cx`. The async data path makes `VfsFile` intentionally
    // non-dyn-compatible, so this remains a static-dispatch contract check.
    fn _close<F: VfsFile>(f: &mut F, cx: &Cx) -> Result<()> {
        f.close(cx)
    }
    async fn _read<F: VfsFile>(f: &F, cx: &Cx, buf: &mut [u8], off: u64) -> Result<usize> {
        f.read(cx, buf, off).await
    }
    async fn _write<F: VfsFile>(f: &mut F, cx: &Cx, buf: &[u8], off: u64) -> Result<()> {
        f.write(cx, buf, off).await
    }

    fn _lock<F: VfsFile>(f: &mut F, cx: &Cx, level: LockLevel) -> Result<()> {
        f.lock(cx, level)
    }
    fn _unlock<F: VfsFile>(f: &mut F, cx: &Cx, level: LockLevel) -> Result<()> {
        f.unlock(cx, level)
    }
    let _ = _close::<fsqlite_vfs::MemoryFile>;
    let _ = _read::<fsqlite_vfs::MemoryFile>;
    let _ = _write::<fsqlite_vfs::MemoryFile>;
    let _ = _lock::<fsqlite_vfs::MemoryFile>;
    let _ = _unlock::<fsqlite_vfs::MemoryFile>;
}

// ---------------------------------------------------------------------------
// Test: No forbidden runtime crate names in Cargo.lock file
// ---------------------------------------------------------------------------

#[test]
fn test_no_forbidden_crates_in_lockfile() {
    // CI gate: forbidden runtimes must not appear anywhere in the resolved
    // dependency graph (direct OR transitive). This keeps the async story
    // unambiguous: asupersync only.
    let root = workspace_root();
    let lockfile_path = root.join("Cargo.lock");
    let contents = std::fs::read_to_string(&lockfile_path).expect("Cargo.lock should be readable");

    let mut violations: Vec<String> = Vec::new();
    for &runtime in FORBIDDEN_RUNTIMES {
        let needle = format!("name = \"{runtime}\"");
        if contents.contains(needle.as_str()) {
            violations.push(runtime.to_string());
        }
    }

    assert!(
        violations.is_empty(),
        "bead_id={BEAD_ID} case=no_forbidden_crates_in_lockfile \
         Found forbidden async runtime crates in Cargo.lock:\n{}",
        violations.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Test: Workspace Cargo.toml does not list forbidden runtimes
// ---------------------------------------------------------------------------

#[test]
fn test_workspace_deps_no_forbidden_runtimes() {
    let root = workspace_root();
    let manifest = std::fs::read_to_string(root.join("Cargo.toml"))
        .expect("workspace Cargo.toml should be readable");

    for &runtime in FORBIDDEN_RUNTIMES {
        // Check for `tokio = ` or `tokio = {` in [workspace.dependencies]
        let pattern = format!("{runtime} = ");
        assert!(
            !manifest.contains(&pattern),
            "bead_id={BEAD_ID} case=workspace_deps_no_forbidden \
             Workspace Cargo.toml must not list `{runtime}` in [workspace.dependencies]"
        );
    }
}

// ---------------------------------------------------------------------------
// E2E: Combined enforcement summary
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_bd_ebua() {
    let metadata = cargo_metadata_cached();
    let member_ids = workspace_member_ids(metadata);

    // 1. Count workspace members (sanity check)
    assert!(
        member_ids.len() >= 23,
        "bead_id={BEAD_ID} case=e2e expected >= 23 workspace members"
    );

    // 2. No forbidden direct deps
    let violations = forbidden_direct_deps(metadata);
    let forbidden_count = violations.len();

    // 3. Summary (grep-friendly)
    eprintln!("workspace_members={}", member_ids.len());
    eprintln!("forbidden_runtime_deps={forbidden_count}");

    assert!(
        violations.is_empty(),
        "bead_id={BEAD_ID} case=e2e_no_forbidden_deps\n{}",
        violations.join("\n")
    );
}
