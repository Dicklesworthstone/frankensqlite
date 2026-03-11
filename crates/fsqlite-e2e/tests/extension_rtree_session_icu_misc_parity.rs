//! E2E parity checks for RTREE + Session + ICU + Misc closure wave.
//!
//! Bead: bd-1dp9.5.3

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_e2e::comparison::ComparisonRunner;
use fsqlite_ext_session::{
    ApplyOutcome, ChangeOp, Changeset, ChangesetValue, ConflictAction,
    ConflictType as SessionConflictType, Session, SimpleTarget,
};
use fsqlite_types::value::SqliteValue;
use serde_json::json;
use sha2::{Digest, Sha256};

const BEAD_ID: &str = "bd-1dp9.5.3";
const SESSION_EXTENSION_BEAD_ID: &str = "bd-37bk2";
const LOG_STANDARD_REF: &str = "bd-1fpm";
const SPATIAL_E2E_SEED: u64 = 1_095_301_001;
const SESSION_E2E_SEED: u64 = 1_095_301_002;
const SESSION_CONFLICT_SEED: u64 = 1_095_301_003;

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("bead_id={BEAD_ID} case=workspace_root_failed error={error}"))
}

fn runtime_dir(label: &str) -> Result<PathBuf, String> {
    let root = workspace_root()?
        .join("target")
        .join("bd_1dp9_5_3_e2e_runtime");
    fs::create_dir_all(&root).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=runtime_root_create_failed path={} error={error}",
            root.display()
        )
    })?;

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    let path = root.join(format!("{label}_{}_{}", std::process::id(), nanos));
    fs::create_dir_all(&path).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=runtime_subdir_create_failed path={} error={error}",
            path.display()
        )
    })?;
    Ok(path)
}

fn format_hex(blob: &[u8]) -> String {
    let mut hex = String::with_capacity(blob.len() * 2);
    for byte in blob {
        let _ = write!(hex, "{byte:02X}");
    }
    hex
}

fn apply_outcome_json(outcome: &ApplyOutcome) -> serde_json::Value {
    match outcome {
        ApplyOutcome::Success { applied, skipped } => json!({
            "kind": "success",
            "applied": applied,
            "skipped": skipped,
        }),
        ApplyOutcome::Aborted { applied } => json!({
            "kind": "aborted",
            "applied": applied,
        }),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn command_succeeded(program: &str, version_flag: &str) -> bool {
    Command::new(program)
        .arg(version_flag)
        .output()
        .is_ok_and(|output| output.status.success())
}

fn command_failure_details(output: &Output) -> String {
    format!(
        "status={} stdout={:?} stderr={:?}",
        output.status,
        String::from_utf8_lossy(&output.stdout).trim(),
        String::from_utf8_lossy(&output.stderr).trim()
    )
}

const SQLITE_SESSION_ORACLE_SCRIPT: &str = r#"
import subprocess
import sys


def fail(message: str) -> None:
    sys.stderr.write(message + "\n")
    raise SystemExit(1)


def run_sqlite3(args, input_text=None):
    return subprocess.run(
        ["sqlite3", *args],
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
    )


mode = sys.argv[1]

if mode == "probe":
    version = run_sqlite3(["--version"])
    if version.returncode != 0:
        fail(f"sqlite3_unavailable:{version.stderr.strip() or version.stdout.strip()}")

    compile_option = run_sqlite3(
        [":memory:", "SELECT sqlite_compileoption_used('ENABLE_SESSION');"]
    )
    if compile_option.returncode != 0:
        fail(
            "sqlite_session_probe_failed:"
            + (compile_option.stderr.strip() or compile_option.stdout.strip())
        )
    if compile_option.stdout.strip() != "1":
        fail("sqlite_session_compileoption_disabled")
    raise SystemExit(0)

if mode == "build":
    db_path = sys.argv[2]
    changeset_path = sys.argv[3]
    patchset_path = sys.argv[4]
    sql = f""".bail on
CREATE TABLE accounts (id INTEGER PRIMARY KEY, owner TEXT, balance INTEGER);
.session open main s1
.session s1 attach accounts
INSERT INTO accounts VALUES (1, 'alice', 100);
INSERT INTO accounts VALUES (2, 'bob', 50);
UPDATE accounts SET balance = 75 WHERE id = 2;
DELETE FROM accounts WHERE id = 1;
.session s1 changeset {changeset_path}
.session s1 patchset {patchset_path}
"""
    result = run_sqlite3([db_path], input_text=sql)
    if result.returncode != 0:
        fail(
            "sqlite_session_build_failed:"
            + (result.stderr.strip() or result.stdout.strip())
        )
    raise SystemExit(0)

fail(f"unsupported_mode:{mode}")
"#;

fn write_sqlite_session_oracle_script(dir: &Path) -> Result<PathBuf, String> {
    let script_path = dir.join("sqlite_session_oracle.py");
    fs::write(&script_path, SQLITE_SESSION_ORACLE_SCRIPT).map_err(|error| {
        format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_session_oracle_script_write_failed path={} error={error}",
            script_path.display()
        )
    })?;
    Ok(script_path)
}

fn ensure_sqlite_session_oracle_runtime() -> Result<(), String> {
    if !command_succeeded("python3", "--version") {
        return Err(format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=python3_unavailable"
        ));
    }

    let temp_dir = tempfile::tempdir().map_err(|error| {
        format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_probe_tempdir_failed error={error}"
        )
    })?;
    let script_path = write_sqlite_session_oracle_script(temp_dir.path())?;
    let output = Command::new("python3")
        .arg(&script_path)
        .arg("probe")
        .output()
        .map_err(|error| {
            format!(
                "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_session_probe_exec_failed error={error}"
            )
        })?;
    if !output.status.success() {
        return Err(format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_session_probe_status_failed {}",
            command_failure_details(&output)
        ));
    }

    Ok(())
}

fn should_skip_sqlite_session_oracle_tests() -> bool {
    match ensure_sqlite_session_oracle_runtime() {
        Ok(()) => false,
        Err(reason) => {
            eprintln!("SKIP: {reason}");
            true
        }
    }
}

fn changeset_value_json(value: &ChangesetValue) -> serde_json::Value {
    match value {
        ChangesetValue::Undefined => json!({ "type": "undefined" }),
        ChangesetValue::Null => json!({ "type": "null" }),
        ChangesetValue::Integer(value) => json!({ "type": "integer", "value": value }),
        ChangesetValue::Real(value) => json!({ "type": "real", "value": value }),
        ChangesetValue::Text(value) => json!({ "type": "text", "value": value }),
        ChangesetValue::Blob(value) => {
            json!({ "type": "blob", "hex": format_hex(value.as_slice()) })
        }
    }
}

fn sqlite_value_json(value: &SqliteValue) -> serde_json::Value {
    match value {
        SqliteValue::Null => json!({ "type": "null" }),
        SqliteValue::Integer(value) => json!({ "type": "integer", "value": value }),
        SqliteValue::Float(value) => json!({ "type": "real", "value": value }),
        SqliteValue::Text(value) => json!({ "type": "text", "value": value }),
        SqliteValue::Blob(value) => {
            json!({ "type": "blob", "hex": format_hex(value.as_slice()) })
        }
    }
}

fn normalize_manual_changeset(changeset: &Changeset) -> Vec<serde_json::Value> {
    let mut normalized = Vec::new();
    for table in &changeset.tables {
        for row in &table.rows {
            let op = match row.op {
                ChangeOp::Insert => "insert",
                ChangeOp::Delete => "delete",
                ChangeOp::Update => "update",
            };
            normalized.push(json!({
                "table": table.info.name,
                "pk_flags": table.info.pk_flags,
                "op": op,
                "old_values": row.old_values.iter().map(changeset_value_json).collect::<Vec<_>>(),
                "new_values": row.new_values.iter().map(changeset_value_json).collect::<Vec<_>>(),
                "indirect": false,
            }));
        }
    }
    normalized
}

fn record_accounts_session(session: &mut Session) {
    session.attach_table("accounts", 3, vec![true, false, false]);
    session.record_insert(
        "accounts",
        vec![
            ChangesetValue::Integer(1),
            ChangesetValue::Text("alice".to_owned()),
            ChangesetValue::Integer(100),
        ],
    );
    session.record_insert(
        "accounts",
        vec![
            ChangesetValue::Integer(2),
            ChangesetValue::Text("bob".to_owned()),
            ChangesetValue::Integer(50),
        ],
    );
    session.record_update(
        "accounts",
        vec![
            ChangesetValue::Integer(2),
            ChangesetValue::Text("bob".to_owned()),
            ChangesetValue::Integer(50),
        ],
        vec![
            ChangesetValue::Undefined,
            ChangesetValue::Undefined,
            ChangesetValue::Integer(75),
        ],
    );
    session.record_delete(
        "accounts",
        vec![
            ChangesetValue::Integer(1),
            ChangesetValue::Text("alice".to_owned()),
            ChangesetValue::Integer(100),
        ],
    );
}

fn build_sqlite_session_bytes() -> Result<(Vec<u8>, Vec<u8>), String> {
    ensure_sqlite_session_oracle_runtime()?;

    let temp_dir = tempfile::tempdir().map_err(|error| {
        format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_session_tempdir_failed error={error}"
        )
    })?;
    let db_path = temp_dir.path().join("session_oracle.db");
    let script_path = write_sqlite_session_oracle_script(temp_dir.path())?;
    let changeset_path = temp_dir.path().join("session_oracle.changeset");
    let patchset_path = temp_dir.path().join("session_oracle.patchset");
    let output = Command::new("python3")
        .arg(&script_path)
        .arg("build")
        .arg(&db_path)
        .arg(&changeset_path)
        .arg(&patchset_path)
        .output()
        .map_err(|error| {
            format!(
                "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_session_oracle_build_exec_failed error={error}"
            )
        })?;
    if !output.status.success() {
        return Err(format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_session_oracle_build_status_failed {}",
            command_failure_details(&output)
        ));
    }

    let changeset_blob = fs::read(&changeset_path).map_err(|error| {
        format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_changeset_read_failed path={} error={error}",
            changeset_path.display()
        )
    })?;
    let patchset_blob = fs::read(&patchset_path).map_err(|error| {
        format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_patchset_read_failed path={} error={error}",
            patchset_path.display()
        )
    })?;
    Ok((changeset_blob, patchset_blob))
}

fn manual_conflict_label(conflict: SessionConflictType) -> &'static str {
    match conflict {
        SessionConflictType::Data => "data",
        SessionConflictType::NotFound => "not_found",
        SessionConflictType::Conflict => "conflict",
        SessionConflictType::Constraint => "constraint",
        SessionConflictType::ForeignKey => "foreign_key",
    }
}

#[derive(Clone, Copy)]
enum ConflictDecision {
    Abort,
    Omit,
    Replace,
}

impl ConflictDecision {
    const fn label(self) -> &'static str {
        match self {
            Self::Abort => "abort",
            Self::Omit => "omit",
            Self::Replace => "replace",
        }
    }

    const fn manual_action(self) -> ConflictAction {
        match self {
            Self::Abort => ConflictAction::Abort,
            Self::Omit => ConflictAction::OmitChange,
            Self::Replace => ConflictAction::Replace,
        }
    }
}

fn build_manual_conflict_target() -> SimpleTarget {
    let mut target = SimpleTarget::default();
    target.tables.insert(
        "accounts".to_owned(),
        vec![
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("eve".to_owned()),
                SqliteValue::Integer(5),
            ],
            vec![
                SqliteValue::Integer(2),
                SqliteValue::Text("robert".to_owned()),
                SqliteValue::Integer(60),
            ],
        ],
    );
    target
}

fn manual_target_rows_json(target: &SimpleTarget, table: &str) -> Vec<serde_json::Value> {
    target
        .tables
        .get(table)
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    serde_json::Value::Array(row.iter().map(sqlite_value_json).collect::<Vec<_>>())
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

const SQLITE_SESSION_ORACLE_SCRIPT: &str = r#"
import ctypes
import ctypes.util
import json
import sys
from pathlib import Path

mode = sys.argv[1]

lib_path = ctypes.util.find_library("sqlite3")
if not lib_path:
    raise SystemExit("libsqlite3_not_found")

lib = ctypes.CDLL(lib_path)

required_symbols = [
    "sqlite3_open",
    "sqlite3_close",
    "sqlite3_errmsg",
    "sqlite3_exec",
    "sqlite3_free",
    "sqlite3_libversion",
    "sqlite3session_create",
    "sqlite3session_attach",
    "sqlite3session_changeset",
    "sqlite3session_patchset",
    "sqlite3session_delete",
    "sqlite3changeset_apply_v2",
]
missing_symbols = [symbol for symbol in required_symbols if not hasattr(lib, symbol)]
if missing_symbols:
    raise SystemExit(f"sqlite_session_symbols_missing:{','.join(missing_symbols)}")

SQLITE_OK = 0
SQLITE_CHANGESET_OMIT = 0
SQLITE_CHANGESET_REPLACE = 1
SQLITE_CHANGESET_ABORT = 2

ACTION_MAP = {
    "omit": SQLITE_CHANGESET_OMIT,
    "replace": SQLITE_CHANGESET_REPLACE,
    "abort": SQLITE_CHANGESET_ABORT,
}

CONFLICT_MAP = {
    1: "data",
    2: "not_found",
    3: "conflict",
    4: "constraint",
    5: "foreign_key",
}

EXEC_CALLBACK = ctypes.CFUNCTYPE(
    ctypes.c_int,
    ctypes.c_void_p,
    ctypes.c_int,
    ctypes.POINTER(ctypes.c_char_p),
    ctypes.POINTER(ctypes.c_char_p),
)
CONFLICT = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p)

lib.sqlite3_libversion.argtypes = []
lib.sqlite3_libversion.restype = ctypes.c_char_p
lib.sqlite3_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
lib.sqlite3_open.restype = ctypes.c_int
lib.sqlite3_close.argtypes = [ctypes.c_void_p]
lib.sqlite3_close.restype = ctypes.c_int
lib.sqlite3_errmsg.argtypes = [ctypes.c_void_p]
lib.sqlite3_errmsg.restype = ctypes.c_char_p
lib.sqlite3_exec.argtypes = [
    ctypes.c_void_p,
    ctypes.c_char_p,
    ctypes.c_void_p,
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_char_p),
]
lib.sqlite3_exec.restype = ctypes.c_int
lib.sqlite3_free.argtypes = [ctypes.c_void_p]
lib.sqlite3_free.restype = None
lib.sqlite3session_create.argtypes = [
    ctypes.c_void_p,
    ctypes.c_char_p,
    ctypes.POINTER(ctypes.c_void_p),
]
lib.sqlite3session_create.restype = ctypes.c_int
lib.sqlite3session_attach.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
lib.sqlite3session_attach.restype = ctypes.c_int
lib.sqlite3session_changeset.argtypes = [
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_int),
    ctypes.POINTER(ctypes.c_void_p),
]
lib.sqlite3session_changeset.restype = ctypes.c_int
lib.sqlite3session_patchset.argtypes = [
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_int),
    ctypes.POINTER(ctypes.c_void_p),
]
lib.sqlite3session_patchset.restype = ctypes.c_int
lib.sqlite3session_delete.argtypes = [ctypes.c_void_p]
lib.sqlite3session_delete.restype = None
lib.sqlite3changeset_apply_v2.argtypes = [
    ctypes.c_void_p,
    ctypes.c_int,
    ctypes.c_void_p,
    ctypes.c_void_p,
    CONFLICT,
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_void_p),
    ctypes.POINTER(ctypes.c_int),
    ctypes.c_int,
]
lib.sqlite3changeset_apply_v2.restype = ctypes.c_int

def decode_errmsg(db):
    message = lib.sqlite3_errmsg(db)
    return message.decode("utf-8") if message else "unknown"


def fail_with_db(label, rc, db):
    raise SystemExit(f"{label}:{rc}:{decode_errmsg(db)}")


def open_db(path):
    db = ctypes.c_void_p()
    rc = lib.sqlite3_open(path.encode("utf-8"), ctypes.byref(db))
    if rc != SQLITE_OK:
        fail_with_db("sqlite3_open_failed", rc, db)
    return db


def exec_sql(db, sql):
    err = ctypes.c_char_p()
    rc = lib.sqlite3_exec(db, sql.encode("utf-8"), None, None, ctypes.byref(err))
    if rc != SQLITE_OK:
        message = err.value.decode("utf-8") if err.value else decode_errmsg(db)
        raise SystemExit(f"sqlite3_exec_failed:{rc}:{message}")


def fetch_account_rows(db):
    raw_rows = []

    @EXEC_CALLBACK
    def on_row(_ctx, count, values, _columns):
        raw_rows.append(
            [values[index].decode("utf-8") if values[index] else None for index in range(count)]
        )
        return 0

    err = ctypes.c_char_p()
    rc = lib.sqlite3_exec(
        db,
        b"SELECT id, owner, balance FROM accounts ORDER BY id",
        on_row,
        None,
        ctypes.byref(err),
    )
    if rc != SQLITE_OK:
        message = err.value.decode("utf-8") if err.value else decode_errmsg(db)
        raise SystemExit(f"sqlite3_select_failed:{rc}:{message}")
    return [
        [
            {"type": "integer", "value": int(row[0])} if row[0] is not None else {"type": "null"},
            {"type": "text", "value": row[1]} if row[1] is not None else {"type": "null"},
            {"type": "integer", "value": int(row[2])} if row[2] is not None else {"type": "null"},
        ]
        for row in raw_rows
    ]


if mode == "probe":
    print(
        json.dumps(
            {
                "lib_path": lib_path,
                "libversion": lib.sqlite3_libversion().decode("utf-8"),
            }
        )
    )
elif mode == "build":
    db_path = sys.argv[2]
    changeset_path = sys.argv[3]
    patchset_path = sys.argv[4]

    db = open_db(db_path)
    session = ctypes.c_void_p()
    try:
        exec_sql(db, "CREATE TABLE accounts (id INTEGER PRIMARY KEY, owner TEXT, balance INTEGER)")
        rc = lib.sqlite3session_create(db, b"main", ctypes.byref(session))
        if rc != SQLITE_OK:
            fail_with_db("sqlite3session_create_failed", rc, db)
        rc = lib.sqlite3session_attach(session, b"accounts")
        if rc != SQLITE_OK:
            fail_with_db("sqlite3session_attach_failed", rc, db)
        exec_sql(
            db,
            """
            INSERT INTO accounts VALUES (1, 'alice', 100);
            INSERT INTO accounts VALUES (2, 'bob', 50);
            UPDATE accounts SET balance = 75 WHERE id = 2;
            DELETE FROM accounts WHERE id = 1;
            """,
        )

        changeset_size = ctypes.c_int()
        changeset_ptr = ctypes.c_void_p()
        rc = lib.sqlite3session_changeset(
            session,
            ctypes.byref(changeset_size),
            ctypes.byref(changeset_ptr),
        )
        if rc != SQLITE_OK:
            fail_with_db("sqlite3session_changeset_failed", rc, db)
        try:
            Path(changeset_path).write_bytes(
                ctypes.string_at(changeset_ptr, changeset_size.value)
            )
        finally:
            if changeset_ptr.value:
                lib.sqlite3_free(changeset_ptr)

        patchset_size = ctypes.c_int()
        patchset_ptr = ctypes.c_void_p()
        rc = lib.sqlite3session_patchset(
            session,
            ctypes.byref(patchset_size),
            ctypes.byref(patchset_ptr),
        )
        if rc != SQLITE_OK:
            fail_with_db("sqlite3session_patchset_failed", rc, db)
        try:
            Path(patchset_path).write_bytes(
                ctypes.string_at(patchset_ptr, patchset_size.value)
            )
        finally:
            if patchset_ptr.value:
                lib.sqlite3_free(patchset_ptr)
    finally:
        if session.value:
            lib.sqlite3session_delete(session)
        lib.sqlite3_close(db)
elif mode == "apply":
    decision = sys.argv[2]
    seed_mode = sys.argv[3]
    db_path = sys.argv[4]
    changeset_path = sys.argv[5]

    db = open_db(db_path)
    conflicts = []
    try:
        exec_sql(db, "CREATE TABLE accounts (id INTEGER PRIMARY KEY, owner TEXT, balance INTEGER)")
        if seed_mode == "conflict":
            exec_sql(
                db,
                """
                INSERT INTO accounts VALUES (1, 'eve', 5);
                INSERT INTO accounts VALUES (2, 'robert', 60);
                """,
            )

        changeset = Path(changeset_path).read_bytes()
        buffer = ctypes.create_string_buffer(changeset)

        @CONFLICT
        def on_conflict(_ctx, kind, _iter_ptr):
            conflicts.append(CONFLICT_MAP.get(kind, f"unknown_{kind}"))
            return ACTION_MAP[decision]

        rebase = ctypes.c_void_p()
        rebase_size = ctypes.c_int()
        rc = lib.sqlite3changeset_apply_v2(
            db,
            len(changeset),
            ctypes.cast(buffer, ctypes.c_void_p),
            None,
            on_conflict,
            None,
            ctypes.byref(rebase),
            ctypes.byref(rebase_size),
            0,
        )
        rows = fetch_account_rows(db)
        errmsg = decode_errmsg(db)
        if rebase.value:
            lib.sqlite3_free(rebase)

        print(
            json.dumps(
                {
                    "kind": "success" if rc == SQLITE_OK else "aborted",
                    "return_code": rc,
                    "error": None if rc == SQLITE_OK else errmsg,
                    "conflicts": conflicts,
                    "rows": rows,
                }
            )
        )
    finally:
        lib.sqlite3_close(db)
else:
    raise SystemExit(f"unknown_mode:{mode}")
"#;

fn write_sqlite_session_oracle_script(temp_dir: &Path) -> Result<PathBuf, String> {
    let script_path = temp_dir.join("sqlite_session_oracle.py");
    fs::write(&script_path, SQLITE_SESSION_ORACLE_SCRIPT).map_err(|error| {
        format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_session_script_write_failed path={} error={error}",
            script_path.display()
        )
    })?;
    Ok(script_path)
}

fn run_sqlite_apply_oracle(
    changeset_bytes: &[u8],
    decision: ConflictDecision,
    seed_mode: SqliteApplySeed,
) -> Result<serde_json::Value, String> {
    ensure_sqlite_session_oracle_runtime()?;

    let temp_dir = tempfile::tempdir().map_err(|error| {
        format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_apply_tempdir_failed error={error}"
        )
    })?;
    let db_path = temp_dir.path().join("apply_oracle.db");
    let changeset_path = temp_dir.path().join("apply_oracle.changeset");
    let script_path = write_sqlite_session_oracle_script(temp_dir.path())?;

    fs::write(&changeset_path, changeset_bytes).map_err(|error| {
        format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_apply_changeset_write_failed path={} error={error}",
            changeset_path.display()
        )
    })?;
    let output = Command::new("python3")
        .arg(&script_path)
        .arg("apply")
        .arg(decision.label())
        .arg(seed_mode.label())
        .arg(&db_path)
        .arg(&changeset_path)
        .output()
        .map_err(|error| {
            format!(
                "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_apply_oracle_exec_failed error={error}"
            )
        })?;
    if !output.status.success() {
        return Err(format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_apply_oracle_status_failed {}",
            command_failure_details(&output)
        ));
    }

    serde_json::from_slice(&output.stdout).map_err(|error| {
        format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_apply_oracle_json_failed error={error} stdout={:?}",
            String::from_utf8_lossy(&output.stdout).trim()
        )
    })
}

#[derive(Clone, Copy)]
enum SqliteApplySeed {
    Empty,
    Conflict,
}

impl SqliteApplySeed {
    const fn label(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Conflict => "conflict",
        }
    }
}

fn sqlite_apply_kind(
    apply_result: &serde_json::Value,
    decision: ConflictDecision,
) -> Result<&str, String> {
    apply_result
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            format!(
                "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_apply_kind_missing decision={}",
                decision.label()
            )
        })
}

fn sqlite_apply_rows(
    apply_result: &serde_json::Value,
    decision: ConflictDecision,
) -> Result<Vec<serde_json::Value>, String> {
    apply_result
        .get("rows")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .ok_or_else(|| {
            format!(
                "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_apply_rows_missing decision={}",
                decision.label()
            )
        })
}

fn sqlite_apply_conflicts(
    apply_result: &serde_json::Value,
    decision: ConflictDecision,
) -> Result<Vec<String>, String> {
    apply_result
        .get("conflicts")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            format!(
                "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_apply_conflicts_missing decision={}",
                decision.label()
            )
        })?
        .iter()
        .map(|value| {
            value.as_str().map(ToOwned::to_owned).ok_or_else(|| {
                format!(
                    "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_apply_conflict_non_string decision={}",
                    decision.label()
                )
            })
        })
        .collect()
}

#[test]
fn rtree_spatial_scenario_rows_match_csqlite() -> Result<(), String> {
    let run_id = format!("bd-1dp9.5.3-e2e-spatial-seed-{SPATIAL_E2E_SEED}");
    let statements = vec![
        "CREATE TABLE spatial (id INTEGER PRIMARY KEY, min_x REAL, min_y REAL, max_x REAL, max_y REAL, label TEXT)".to_owned(),
        "INSERT INTO spatial VALUES (1, 0.0, 0.0, 10.0, 10.0, 'region_a')".to_owned(),
        "INSERT INTO spatial VALUES (2, 5.0, 5.0, 15.0, 15.0, 'region_b')".to_owned(),
        "INSERT INTO spatial VALUES (3, 20.0, 20.0, 30.0, 30.0, 'region_c')".to_owned(),
        "SELECT id, label FROM spatial WHERE min_x <= 12.0 AND max_x >= 4.0 AND min_y <= 12.0 AND max_y >= 4.0 ORDER BY id".to_owned(),
        "SELECT COUNT(*) FROM spatial WHERE min_x <= 25.0 AND max_x >= 20.0".to_owned(),
    ];

    let runner = ComparisonRunner::new_in_memory()
        .map_err(|error| format!("bead_id={BEAD_ID} case=runner_create_failed error={error}"))?;
    let result = runner.run_and_compare(&statements);
    let hash = runner.compare_logical_state();
    let first_divergence_index = result.mismatches.first().map(|mismatch| mismatch.index);

    let runtime = runtime_dir("rtree_spatial")?;
    let artifact_path = runtime.join("rtree_spatial_e2e.json");
    let artifact = json!({
        "bead_id": BEAD_ID,
        "log_standard_ref": LOG_STANDARD_REF,
        "run_id": run_id,
        "seed": SPATIAL_E2E_SEED,
        "phase": "rtree_spatial_e2e",
        "operations_matched": result.operations_matched,
        "operations_mismatched": result.operations_mismatched,
        "first_divergence_index": first_divergence_index,
        "first_divergence_present": first_divergence_index.is_some(),
        "logical_hash": {
            "frank_sha256": hash.frank_sha256,
            "csqlite_sha256": hash.csqlite_sha256,
            "matched": hash.matched
        }
    });
    let pretty = serde_json::to_string_pretty(&artifact).map_err(|error| {
        format!("bead_id={BEAD_ID} case=artifact_serialize_failed error={error}")
    })?;
    fs::write(&artifact_path, pretty).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=artifact_write_failed path={} error={error}",
            artifact_path.display()
        )
    })?;

    eprintln!(
        "DEBUG bead_id={BEAD_ID} phase=rtree_spatial_e2e seed={SPATIAL_E2E_SEED} run_id={run_id} reference={LOG_STANDARD_REF} artifact_path={}",
        artifact_path.display()
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} phase=rtree_spatial_e2e seed={SPATIAL_E2E_SEED} run_id={run_id} matched={} mismatched={} logical_hash_match={}",
        result.operations_matched, result.operations_mismatched, hash.matched
    );
    if let Some(index) = first_divergence_index {
        eprintln!(
            "WARN bead_id={BEAD_ID} phase=rtree_spatial_e2e seed={SPATIAL_E2E_SEED} run_id={run_id} first_divergence_index={index}"
        );
        if let Some(mismatch) = result.mismatches.first() {
            eprintln!(
                "ERROR bead_id={BEAD_ID} phase=rtree_spatial_e2e seed={SPATIAL_E2E_SEED} run_id={run_id} first_divergence_sql={:?}",
                mismatch.sql
            );
        }
    } else {
        eprintln!(
            "DEBUG bead_id={BEAD_ID} phase=rtree_spatial_e2e seed={SPATIAL_E2E_SEED} run_id={run_id} first_divergence_index=none"
        );
        eprintln!(
            "INFO bead_id={BEAD_ID} phase=rtree_spatial_e2e seed={SPATIAL_E2E_SEED} run_id={run_id} first_divergence_count=0"
        );
    }

    if result.operations_mismatched != 0 {
        return Err(format!(
            "bead_id={BEAD_ID} case=rtree_spatial_mismatch mismatches={:?}",
            result.mismatches
        ));
    }
    if !hash.matched {
        return Err(format!(
            "bead_id={BEAD_ID} case=rtree_spatial_hash_mismatch frank_hash={} csqlite_hash={}",
            hash.frank_sha256, hash.csqlite_sha256
        ));
    }

    Ok(())
}

#[test]
fn session_changeset_blob_roundtrip_rows_match_csqlite() -> Result<(), String> {
    if should_skip_sqlite_session_oracle_tests() {
        return Ok(());
    }

    let mut manual_session = Session::new();
    record_accounts_session(&mut manual_session);
    let manual_changeset = manual_session.changeset();
    let manual_changeset_blob = manual_changeset.encode();
    let manual_patchset_blob = manual_session.patchset();
    let manual_patchset = Changeset::decode_patchset(&manual_patchset_blob).ok_or_else(|| {
        format!("bead_id={SESSION_EXTENSION_BEAD_ID} case=manual_patchset_decode_failed")
    })?;

    let mut changeset_target = SimpleTarget::default();
    let mut patchset_target = SimpleTarget::default();
    let changeset_outcome = changeset_target.apply(&manual_changeset, |_, _| ConflictAction::Abort);
    let patchset_outcome = patchset_target.apply(&manual_patchset, |_, _| ConflictAction::Abort);
    let states_match = changeset_target.tables == patchset_target.tables;
    if !states_match {
        return Err(format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=manual_changeset_patchset_state_mismatch changeset_state={:?} patchset_state={:?}",
            changeset_target.tables, patchset_target.tables
        ));
    }

    let (sqlite_changeset_blob, sqlite_patchset_blob) = build_sqlite_session_bytes()?;
    let manual_changeset_normalized = normalize_manual_changeset(&manual_changeset);
    let manual_patchset_normalized = normalize_manual_changeset(&manual_patchset);
    let changeset_blob_match = manual_changeset_blob == sqlite_changeset_blob;
    let patchset_blob_match = manual_patchset_blob == sqlite_patchset_blob;
    let sqlite_changeset_apply = run_sqlite_apply_oracle(
        &sqlite_changeset_blob,
        ConflictDecision::Abort,
        SqliteApplySeed::Empty,
    )?;
    let sqlite_patchset_apply = run_sqlite_apply_oracle(
        &sqlite_patchset_blob,
        ConflictDecision::Abort,
        SqliteApplySeed::Empty,
    )?;
    let sqlite_changeset_kind =
        sqlite_apply_kind(&sqlite_changeset_apply, ConflictDecision::Abort)?;
    let sqlite_patchset_kind = sqlite_apply_kind(&sqlite_patchset_apply, ConflictDecision::Abort)?;
    let sqlite_changeset_rows =
        sqlite_apply_rows(&sqlite_changeset_apply, ConflictDecision::Abort)?;
    let sqlite_patchset_rows = sqlite_apply_rows(&sqlite_patchset_apply, ConflictDecision::Abort)?;
    let manual_changeset_rows = manual_target_rows_json(&changeset_target, "accounts");
    let manual_patchset_rows = manual_target_rows_json(&patchset_target, "accounts");
    let changeset_rows_match = manual_changeset_rows == sqlite_changeset_rows;
    let patchset_rows_match = manual_patchset_rows == sqlite_patchset_rows;

    let run_id = format!("{SESSION_EXTENSION_BEAD_ID}-session-extension-seed-{SESSION_E2E_SEED}");
    let runtime = runtime_dir("session_sqlite_oracle")?;
    let artifact_path = runtime.join("session_sqlite_oracle_e2e.json");
    let artifact = json!({
        "bead_id": SESSION_EXTENSION_BEAD_ID,
        "legacy_wave_bead_id": BEAD_ID,
        "log_standard_ref": LOG_STANDARD_REF,
        "run_id": run_id,
        "seed": SESSION_E2E_SEED,
        "phase": "session_sqlite_oracle_e2e",
        "changeset_blob_len": manual_changeset_blob.len(),
        "patchset_blob_len": manual_patchset_blob.len(),
        "manual_changeset_sha256": sha256_hex(&manual_changeset_blob),
        "sqlite_changeset_sha256": sha256_hex(&sqlite_changeset_blob),
        "manual_patchset_sha256": sha256_hex(&manual_patchset_blob),
        "sqlite_patchset_sha256": sha256_hex(&sqlite_patchset_blob),
        "changeset_blob_match": changeset_blob_match,
        "patchset_blob_match": patchset_blob_match,
        "changeset_rows_match": changeset_rows_match,
        "patchset_rows_match": patchset_rows_match,
        "changeset_apply_outcome": apply_outcome_json(&changeset_outcome),
        "patchset_apply_outcome": apply_outcome_json(&patchset_outcome),
        "final_state": manual_changeset_rows,
        "sqlite_changeset_outcome": sqlite_changeset_apply,
        "sqlite_patchset_outcome": sqlite_patchset_apply,
        "manual_changeset": manual_changeset_normalized,
        "manual_patchset": manual_patchset_normalized,
    });
    let pretty = serde_json::to_string_pretty(&artifact).map_err(|error| {
        format!("bead_id={SESSION_EXTENSION_BEAD_ID} case=artifact_serialize_failed error={error}")
    })?;
    fs::write(&artifact_path, pretty).map_err(|error| {
        format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=artifact_write_failed path={} error={error}",
            artifact_path.display()
        )
    })?;

    eprintln!(
        "DEBUG bead_id={SESSION_EXTENSION_BEAD_ID} phase=session_sqlite_oracle_e2e seed={SESSION_E2E_SEED} run_id={run_id} reference={LOG_STANDARD_REF} artifact_path={}",
        artifact_path.display()
    );
    eprintln!(
        "INFO bead_id={SESSION_EXTENSION_BEAD_ID} phase=session_sqlite_oracle_e2e seed={SESSION_E2E_SEED} run_id={run_id} changeset_blob_match={} patchset_blob_match={} changeset_rows_match={} patchset_rows_match={}",
        changeset_blob_match, patchset_blob_match, changeset_rows_match, patchset_rows_match
    );

    if sqlite_changeset_kind != "success" {
        return Err(format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_changeset_apply_failed kind={sqlite_changeset_kind}"
        ));
    }
    if sqlite_patchset_kind != "success" {
        return Err(format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_patchset_apply_failed kind={sqlite_patchset_kind}"
        ));
    }
    if !changeset_blob_match {
        return Err(format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=changeset_blob_mismatch manual_sha256={} sqlite_sha256={}",
            sha256_hex(&manual_changeset_blob),
            sha256_hex(&sqlite_changeset_blob)
        ));
    }
    if !patchset_blob_match {
        return Err(format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=patchset_blob_mismatch manual_sha256={} sqlite_sha256={}",
            sha256_hex(&manual_patchset_blob),
            sha256_hex(&sqlite_patchset_blob)
        ));
    }
    if !changeset_rows_match {
        return Err(format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=changeset_rows_mismatch"
        ));
    }
    if !patchset_rows_match {
        return Err(format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=patchset_rows_mismatch"
        ));
    }

    Ok(())
}

#[test]
fn session_conflict_apply_semantics_match_sqlite_session_extension() -> Result<(), String> {
    if should_skip_sqlite_session_oracle_tests() {
        return Ok(());
    }

    let (sqlite_changeset_blob, _) = build_sqlite_session_bytes()?;
    let decoded_sqlite_changeset = Changeset::decode(&sqlite_changeset_blob).ok_or_else(|| {
        format!("bead_id={SESSION_EXTENSION_BEAD_ID} case=sqlite_conflict_changeset_decode_failed")
    })?;
    let run_id =
        format!("{SESSION_EXTENSION_BEAD_ID}-session-conflict-seed-{SESSION_CONFLICT_SEED}");
    let runtime = runtime_dir("session_conflict_oracle")?;
    let artifact_path = runtime.join("session_conflict_oracle_e2e.json");

    let mut cases = Vec::new();
    let mut failures = Vec::new();
    for decision in [
        ConflictDecision::Abort,
        ConflictDecision::Omit,
        ConflictDecision::Replace,
    ] {
        let mut manual_target = build_manual_conflict_target();
        let mut manual_conflicts = Vec::new();
        let manual_outcome = manual_target.apply(&decoded_sqlite_changeset, |conflict, _| {
            manual_conflicts.push(manual_conflict_label(conflict).to_owned());
            decision.manual_action()
        });
        let manual_rows = manual_target_rows_json(&manual_target, "accounts");

        let sqlite_apply =
            run_sqlite_apply_oracle(&sqlite_changeset_blob, decision, SqliteApplySeed::Conflict)?;
        let sqlite_kind = sqlite_apply_kind(&sqlite_apply, decision)?;
        let sqlite_rows = sqlite_apply_rows(&sqlite_apply, decision)?;
        let sqlite_conflicts = sqlite_apply_conflicts(&sqlite_apply, decision)?;

        let manual_kind = match &manual_outcome {
            ApplyOutcome::Success { .. } => "success",
            ApplyOutcome::Aborted { .. } => "aborted",
        };
        let outcome_match = manual_kind == sqlite_kind;
        let state_match = manual_rows == sqlite_rows;
        let conflict_match = manual_conflicts == sqlite_conflicts;

        cases.push(json!({
            "decision": decision.label(),
            "manual_outcome": apply_outcome_json(&manual_outcome),
            "sqlite_outcome": sqlite_apply,
            "manual_conflicts": manual_conflicts,
            "sqlite_conflicts": sqlite_conflicts,
            "manual_rows": manual_rows,
            "sqlite_rows": sqlite_rows,
            "outcome_match": outcome_match,
            "state_match": state_match,
            "conflict_match": conflict_match,
        }));

        if !outcome_match {
            failures.push(format!(
                "decision={} outcome manual={} sqlite={}",
                decision.label(),
                manual_kind,
                sqlite_kind
            ));
        }
        if !state_match {
            failures.push(format!(
                "decision={} state manual={:?} sqlite={:?}",
                decision.label(),
                manual_rows,
                sqlite_rows
            ));
        }
        if !conflict_match {
            failures.push(format!(
                "decision={} conflicts manual={:?} sqlite={:?}",
                decision.label(),
                manual_conflicts,
                sqlite_conflicts
            ));
        }
    }

    let artifact = json!({
        "bead_id": SESSION_EXTENSION_BEAD_ID,
        "legacy_wave_bead_id": BEAD_ID,
        "log_standard_ref": LOG_STANDARD_REF,
        "run_id": run_id,
        "seed": SESSION_CONFLICT_SEED,
        "phase": "session_conflict_oracle_e2e",
        "changeset_sha256": sha256_hex(&sqlite_changeset_blob),
        "cases": cases,
    });
    let pretty = serde_json::to_string_pretty(&artifact).map_err(|error| {
        format!("bead_id={SESSION_EXTENSION_BEAD_ID} case=artifact_serialize_failed error={error}")
    })?;
    fs::write(&artifact_path, pretty).map_err(|error| {
        format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=artifact_write_failed path={} error={error}",
            artifact_path.display()
        )
    })?;

    eprintln!(
        "DEBUG bead_id={SESSION_EXTENSION_BEAD_ID} phase=session_conflict_oracle_e2e seed={SESSION_CONFLICT_SEED} run_id={run_id} reference={LOG_STANDARD_REF} artifact_path={}",
        artifact_path.display()
    );
    eprintln!(
        "INFO bead_id={SESSION_EXTENSION_BEAD_ID} phase=session_conflict_oracle_e2e seed={SESSION_CONFLICT_SEED} run_id={run_id} cases={}",
        artifact["cases"].as_array().map_or(0, Vec::len)
    );

    if !failures.is_empty() {
        return Err(format!(
            "bead_id={SESSION_EXTENSION_BEAD_ID} case=conflict_oracle_mismatch {}",
            failures.join(" | ")
        ));
    }

    Ok(())
}
