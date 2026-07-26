use std::env;
use std::path::Path;
use std::process::Command;

fn command_stdout(program: &Path, args: &[&str]) -> Option<String> {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|stdout| !stdout.is_empty())
}

fn git_stdout(workspace_root: &Path, args: &[&str]) -> Option<String> {
    Command::new("git")
        .arg("-C")
        .arg(workspace_root)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|stdout| !stdout.is_empty())
}

fn git_output(workspace_root: &Path, args: &[&str]) -> Option<Vec<u8>> {
    Command::new("git")
        .arg("-C")
        .arg(workspace_root)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| output.stdout)
}

fn single_line(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn emit(name: &str, value: impl AsRef<str>) {
    println!("cargo:rustc-env={name}={}", single_line(value.as_ref()));
}

fn main() {
    let manifest_dir = env::var_os("CARGO_MANIFEST_DIR")
        .map(std::path::PathBuf::from)
        .expect("Cargo must set CARGO_MANIFEST_DIR for fsqlite-e2e");
    let workspace_root = manifest_dir
        .join("../..")
        .canonicalize()
        .unwrap_or_else(|_| manifest_dir.join("../.."));

    let git_sha = git_stdout(&workspace_root, &["rev-parse", "--verify", "HEAD"])
        .unwrap_or_else(|| "unknown".to_owned());
    let git_branch = git_stdout(&workspace_root, &["branch", "--show-current"])
        .unwrap_or_else(|| "detached-or-unknown".to_owned());
    let git_dirty = Command::new("git")
        .arg("-C")
        .arg(&workspace_root)
        .args(["status", "--porcelain=v1", "--untracked-files=normal"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map_or("unknown", |output| {
            if output.stdout.is_empty() {
                "false"
            } else {
                "true"
            }
        });

    let mut features = env::vars()
        .filter_map(|(name, value)| {
            (value == "1")
                .then(|| name.strip_prefix("CARGO_FEATURE_"))
                .flatten()
                .map(|feature| feature.to_ascii_lowercase().replace('_', "-"))
        })
        .collect::<Vec<_>>();
    features.sort_unstable();

    let rustc = env::var_os("RUSTC")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| "rustc".into());
    let cargo = env::var_os("CARGO")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| "cargo".into());

    emit(
        "FSQLITE_BENCH_BUILD_WORKSPACE_ROOT",
        workspace_root.to_string_lossy(),
    );
    emit("FSQLITE_BENCH_BUILD_GIT_SHA", git_sha);
    emit("FSQLITE_BENCH_BUILD_GIT_BRANCH", git_branch);
    emit("FSQLITE_BENCH_BUILD_GIT_DIRTY", git_dirty);
    emit(
        "FSQLITE_BENCH_BUILD_PROFILE",
        env::var("PROFILE").unwrap_or_else(|_| "unknown".to_owned()),
    );
    emit(
        "FSQLITE_BENCH_BUILD_PROFILE_LABEL",
        env::var("FSQLITE_BENCH_PROFILE_NAME").unwrap_or_else(|_| "unspecified".to_owned()),
    );
    emit(
        "FSQLITE_BENCH_BUILD_OPT_LEVEL",
        env::var("OPT_LEVEL").unwrap_or_else(|_| "unknown".to_owned()),
    );
    emit(
        "FSQLITE_BENCH_BUILD_DEBUG",
        env::var("DEBUG").unwrap_or_else(|_| "unknown".to_owned()),
    );
    emit(
        "FSQLITE_BENCH_BUILD_TARGET",
        env::var("TARGET").unwrap_or_else(|_| "unknown".to_owned()),
    );
    emit(
        "FSQLITE_BENCH_BUILD_HOST",
        env::var("HOST").unwrap_or_else(|_| "unknown".to_owned()),
    );
    emit(
        "FSQLITE_BENCH_BUILD_PANIC",
        env::var("CARGO_CFG_PANIC").unwrap_or_else(|_| "unknown".to_owned()),
    );
    emit("FSQLITE_BENCH_BUILD_FEATURES", features.join(","));
    emit(
        "FSQLITE_BENCH_BUILD_RUSTFLAGS_HEX",
        hex_bytes(
            env::var("CARGO_ENCODED_RUSTFLAGS")
                .unwrap_or_default()
                .as_bytes(),
        ),
    );
    emit(
        "FSQLITE_BENCH_BUILD_RUSTC_VERSION",
        command_stdout(&rustc, &["--version", "--verbose"]).unwrap_or_else(|| "unknown".to_owned()),
    );
    emit(
        "FSQLITE_BENCH_BUILD_CARGO_VERSION",
        command_stdout(&cargo, &["--version"]).unwrap_or_else(|| "unknown".to_owned()),
    );

    let tracked_files = git_output(&workspace_root, &["ls-files", "-z"]);
    emit(
        "FSQLITE_BENCH_BUILD_INPUT_TRACKING",
        if tracked_files.is_some() {
            "complete"
        } else {
            "unavailable"
        },
    );
    if let Some(tracked_files) = tracked_files {
        for tracked_file in tracked_files.split(|byte| *byte == 0) {
            if tracked_file.is_empty() {
                continue;
            }
            let tracked_file = String::from_utf8_lossy(tracked_file);
            println!(
                "cargo:rerun-if-changed={}",
                workspace_root.join(tracked_file.as_ref()).display()
            );
        }
    }
    for git_path in ["HEAD", "index", "packed-refs"] {
        if let Some(path) = git_stdout(
            &workspace_root,
            &[
                "rev-parse",
                "--path-format=absolute",
                "--git-path",
                git_path,
            ],
        ) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    if let Some(symbolic_ref) = git_stdout(&workspace_root, &["symbolic-ref", "-q", "HEAD"]) {
        if let Some(path) = git_stdout(
            &workspace_root,
            &[
                "rev-parse",
                "--path-format=absolute",
                "--git-path",
                symbolic_ref.as_str(),
            ],
        ) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    println!("cargo:rerun-if-env-changed=CARGO_ENCODED_RUSTFLAGS");
    println!("cargo:rerun-if-env-changed=FSQLITE_BENCH_PROFILE_NAME");
}
