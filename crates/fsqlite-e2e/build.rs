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

fn optional_lower_sha256_environment(name: &str) -> String {
    let Some(value) = env::var_os(name) else {
        return String::new();
    };
    let value = value.into_string().unwrap_or_else(|_| {
        panic!("{name} must be valid UTF-8 when supplied");
    });
    assert!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{name} must be exactly 64 lowercase hexadecimal characters when supplied"
    );
    value
}

fn selected_profile_from_out_dir() -> String {
    env::var_os("OUT_DIR")
        .map(std::path::PathBuf::from)
        .and_then(|out_dir| {
            out_dir
                .ancestors()
                .nth(3)
                .and_then(Path::file_name)
                .map(|profile| profile.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| "unknown".to_owned())
}

fn profile_override_environment() -> String {
    let mut overrides = env::vars()
        .filter(|(name, _)| {
            name.starts_with("CARGO_PROFILE_")
                || matches!(
                    name.as_str(),
                    "CARGO_INCREMENTAL"
                        | "CARGO_BUILD_INCREMENTAL"
                        | "CARGO_BUILD_RUSTFLAGS"
                        | "CARGO_BUILD_RUSTC_WRAPPER"
                        | "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER"
                        | "RUSTC_WRAPPER"
                        | "RUSTC_WORKSPACE_WRAPPER"
                )
                || (name.contains("RUSTFLAGS") && name != "CARGO_ENCODED_RUSTFLAGS")
                || name.ends_with("_RUSTC")
                || name.ends_with("_RUSTC_WRAPPER")
        })
        .collect::<Vec<_>>();
    overrides.sort_unstable();
    let mut encoded = String::new();
    for (index, (name, value)) in overrides.into_iter().enumerate() {
        if index != 0 {
            encoded.push('\0');
        }
        encoded.push_str(&name);
        encoded.push('=');
        encoded.push_str(&value);
    }
    encoded
}

fn native_build_override_environment() -> String {
    let exact_names = [
        "AR",
        "ARFLAGS",
        "CC",
        "CFLAGS",
        "CPPFLAGS",
        "CRATE_CC_NO_DEFAULTS",
        "CXX",
        "CXXFLAGS",
        "LIBSQLITE3_FLAGS",
        "LIBSQLITE3_SYS_USE_PKG_CONFIG",
        "SQLITE3_INCLUDE_DIR",
        "SQLITE3_LIB_DIR",
        "SQLITE3_NO_PKG_CONFIG",
        "SQLITE3_STATIC",
    ];
    let prefixes = [
        "AR_",
        "CC_",
        "CFLAGS_",
        "CMAKE_",
        "CPPFLAGS_",
        "CXX_",
        "CXXFLAGS_",
        "LIBSQLITE3_",
        "PKG_CONFIG",
        "SQLITE3_",
        "SQLITE_",
        "VCPKG_",
    ];
    let suffixes = ["_AR", "_CC", "_CFLAGS", "_CXX", "_CXXFLAGS", "_LINKER"];
    let mut overrides = env::vars()
        .filter(|(name, _)| {
            exact_names.contains(&name.as_str())
                || prefixes.iter().any(|prefix| name.starts_with(prefix))
                || suffixes.iter().any(|suffix| name.ends_with(suffix))
        })
        .collect::<Vec<_>>();
    overrides.sort_unstable();
    let mut encoded = String::new();
    for (index, (name, value)) in overrides.into_iter().enumerate() {
        if index != 0 {
            encoded.push('\0');
        }
        encoded.push_str(&name);
        encoded.push('=');
        encoded.push_str(&value);
    }
    encoded
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
        "FSQLITE_BENCH_BUILD_SELECTED_PROFILE",
        selected_profile_from_out_dir(),
    );
    emit(
        "FSQLITE_BENCH_BUILD_PROFILE_LABEL",
        env::var("FSQLITE_BENCH_PROFILE_NAME").unwrap_or_else(|_| "unspecified".to_owned()),
    );
    emit(
        "FSQLITE_BENCH_BUILD_NONCE",
        env::var("FSQLITE_BENCH_BUILD_NONCE").unwrap_or_else(|_| "unknown".to_owned()),
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
        "FSQLITE_BENCH_BUILD_ENCODED_RUSTFLAGS_PRESENT",
        if env::var_os("CARGO_ENCODED_RUSTFLAGS").is_some() {
            "true"
        } else {
            "false"
        },
    );
    emit(
        "FSQLITE_BENCH_BUILD_PROFILE_OVERRIDES_HEX",
        hex_bytes(profile_override_environment().as_bytes()),
    );
    emit(
        "FSQLITE_BENCH_BUILD_NATIVE_OVERRIDES_HEX",
        hex_bytes(native_build_override_environment().as_bytes()),
    );
    emit(
        "FSQLITE_BENCH_BUILD_RESOLVED_DEPENDENCY_FEATURE_GRAPH_SHA256",
        optional_lower_sha256_environment("FSQLITE_BENCH_RESOLVED_DEPENDENCY_FEATURE_GRAPH_SHA256"),
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
    println!("cargo:rerun-if-env-changed=FSQLITE_BENCH_BUILD_NONCE");
    println!("cargo:rerun-if-env-changed=FSQLITE_BENCH_PROFILE_NAME");
    println!("cargo:rerun-if-env-changed=FSQLITE_BENCH_RESOLVED_DEPENDENCY_FEATURE_GRAPH_SHA256");
    for profile in ["RELEASE", "RELEASE_PERF"] {
        for key in [
            "OPT_LEVEL",
            "LTO",
            "CODEGEN_UNITS",
            "PANIC",
            "DEBUG",
            "STRIP",
            "INCREMENTAL",
            "DEBUG_ASSERTIONS",
            "OVERFLOW_CHECKS",
            "RPATH",
            "SPLIT_DEBUGINFO",
        ] {
            println!("cargo:rerun-if-env-changed=CARGO_PROFILE_{profile}_{key}");
        }
    }
    for name in [
        "AR",
        "ARFLAGS",
        "CC",
        "CFLAGS",
        "CARGO_INCREMENTAL",
        "CARGO_BUILD_INCREMENTAL",
        "CARGO_BUILD_RUSTC",
        "CARGO_BUILD_RUSTC_WRAPPER",
        "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER",
        "CARGO_BUILD_RUSTFLAGS",
        "CPPFLAGS",
        "CRATE_CC_NO_DEFAULTS",
        "CXX",
        "CXXFLAGS",
        "LIBSQLITE3_FLAGS",
        "LIBSQLITE3_SYS_USE_PKG_CONFIG",
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
        "RUSTFLAGS",
        "SQLITE3_INCLUDE_DIR",
        "SQLITE3_LIB_DIR",
        "SQLITE3_NO_PKG_CONFIG",
        "SQLITE3_STATIC",
    ] {
        println!("cargo:rerun-if-env-changed={name}");
    }
}
