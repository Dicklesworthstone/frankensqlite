# bd-e8jzh — `PRAGMA fsqlite.dqs` opt-out plumbing (DRAFT — apply with bd-jcjkf)

DRAFT-UNVERIFIED (disk 100%, no build). FREEZE-SAFE: `.md` is never compiled.
These are the exact anchored edits to `crates/fsqlite-core/src/connection.rs`,
modeled 1:1 on the existing `fsqlite.stmt_microbatch` boolean pragma (a
`fsqlite.`-namespaced `Cell<bool>` toggled by `parse_pragma_bool`). Apply all four
in the SAME commit as the DQS engine (bd_jcjkf_dqs_design_draft.md) and the keeper
(`git mv dqs_compat.rs.draft dqs_compat.rs`). Anchors are unique text (not line
numbers) so they survive god-file line drift.

The engine reads `self.dqs_enabled.get()` as its single gate; this plumbing just
lets a PRAGMA flip that field. Stock exposes DQS as SQLITE_DBCONFIG_DQS_DDL/DQS_DML
(a dbconfig, not a pragma); `PRAGMA fsqlite.dqs` is the frank-native equivalent. A
DDL/DML split (`fsqlite.dqs_ddl` / `fsqlite.dqs_dml`) can be added later if needed —
start with the single `dqs` gate since frank has no separate DDL-vs-DML DQS need yet.

---

## Edit 1 — result-column name mapping (near connection.rs ~4968)

ANCHOR (old_string):
```rust
    if full_name_is("fsqlite.concurrent_mode") || full_name_is("concurrent_mode") {
        return &["concurrent_mode"];
    }
```
REPLACE WITH (new_string):
```rust
    if full_name_is("fsqlite.concurrent_mode") || full_name_is("concurrent_mode") {
        return &["concurrent_mode"];
    }
    if full_name_is("fsqlite.dqs") || full_name_is("dqs") {
        return &["dqs"];
    }
```

## Edit 2 — Connection field declaration (near connection.rs ~11919)

ANCHOR (old_string):
```rust
    stmt_microbatch_enabled: Cell<bool>,
```
REPLACE WITH (new_string):
```rust
    stmt_microbatch_enabled: Cell<bool>,
    /// bd-jcjkf: DQS ("double-quoted string") compat gate. Stock SQLite defaults
    /// DQS ON (SQLITE_DQS=3): an unresolvable double-quoted identifier falls back
    /// to a string literal. Default `true` for byte-exact parity; `PRAGMA
    /// fsqlite.dqs = ON|OFF` (bd-e8jzh) flips it. The DQS rewrite-retry engine
    /// (execute/query/execute_with_params fallback) reads this as its only gate.
    dqs_enabled: Cell<bool>,
```

## Edit 3 — constructor inits (TWO sites; use replace_all)

ANCHOR (old_string), replace_all = true:
```rust
            stmt_microbatch_enabled: Cell::new(true),
```
REPLACE WITH (new_string):
```rust
            stmt_microbatch_enabled: Cell::new(true),
            dqs_enabled: Cell::new(true),
```
NOTE: this string occurs at BOTH Connection constructors (~13279 and ~13816). Use
`replace_all: true` so both get `dqs_enabled: Cell::new(true),`. VERIFY AT BUILD:
if a third constructor exists without this field, the compiler flags the missing
field (safe — Rust requires all fields), then add it there too.

## Edit 4 — pragma dispatch arm (near connection.rs ~69020, after concurrent_mode)

ANCHOR (old_string):
```rust
            "fsqlite.concurrent_mode" | "concurrent_mode" => {
                if let Some(ref val) = pragma.value {
                    let enabled = parse_pragma_bool(val)?;
                    *self.concurrent_mode_default.borrow_mut() = enabled;
                    Ok(vec![Row {
                        values: vec![SqliteValue::Integer(i64::from(enabled))],
                    }])
                } else {
                    let enabled = *self.concurrent_mode_default.borrow();
                    Ok(vec![Row {
                        values: vec![SqliteValue::Integer(i64::from(enabled))],
                    }])
                }
            }
```
REPLACE WITH (new_string): same block, then append:
```rust
            "fsqlite.concurrent_mode" | "concurrent_mode" => {
                if let Some(ref val) = pragma.value {
                    let enabled = parse_pragma_bool(val)?;
                    *self.concurrent_mode_default.borrow_mut() = enabled;
                    Ok(vec![Row {
                        values: vec![SqliteValue::Integer(i64::from(enabled))],
                    }])
                } else {
                    let enabled = *self.concurrent_mode_default.borrow();
                    Ok(vec![Row {
                        values: vec![SqliteValue::Integer(i64::from(enabled))],
                    }])
                }
            }
            // bd-jcjkf / bd-e8jzh: DQS ("double-quoted string") compat gate.
            // ON (default) makes an unresolvable double-quoted identifier fall back
            // to a string literal (stock SQLITE_DQS default); OFF restores strict,
            // typo-safe resolution. Mirrors the fsqlite.stmt_microbatch boolean form.
            "fsqlite.dqs" | "dqs" => {
                if let Some(ref val) = pragma.value {
                    let enabled = parse_pragma_bool(val)?;
                    self.dqs_enabled.set(enabled);
                    Ok(vec![Row {
                        values: vec![SqliteValue::Integer(i64::from(enabled))],
                    }])
                } else {
                    Ok(vec![Row {
                        values: vec![SqliteValue::Integer(i64::from(self.dqs_enabled.get()))],
                    }])
                }
            }
```

---

## Verify at build (with bd-jcjkf)
- `PRAGMA fsqlite.dqs;` returns 1 by default; `= OFF` sets 0; `= ON` sets 1 (readback).
- Keeper test (5) `dqs_off_bare_double_quoted_errors_bd_jcjkf` exercises OFF→error→ON→fallback.
- `parse_pragma_bool` (connection.rs ~130706) already accepts ON/OFF/1/0/true/false.
- No new imports needed (Cell, SqliteValue, Row, parse_pragma_bool all in scope here).
