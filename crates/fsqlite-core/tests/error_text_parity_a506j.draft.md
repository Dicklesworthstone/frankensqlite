# bd-a506j — error-text parity wave: oracle diffs + draft patches (FREEZE-SAFE)

DRAFT-UNVERIFIED (disk 100%, no build). FREEZE-SAFE: `.md`, never compiled. Oracle =
sqlite3 3.46.1 vs frank CLI `/data/tmp/p4-verify/debug/fsqlite` (STALE Aug-19 build) —
so each finding is ALSO verified in HEAD source (authoritative for message strings).
Apply patches in one commit when disk clears; several change error TYPES/text that
existing tests assert on, so expect rippling test updates (flagged per finding).

## Oracle diffs (frank vs stock), verified at HEAD in source

| # | Case | frank (stale CLI) | stock 3.46.1 | HEAD source verdict |
|---|------|-------------------|--------------|----------------------|
| F1a | INSERT dup rowid-IPK | `internal error: VDBE halted with code 19: PRIMARY KEY constraint failed` | `UNIQUE constraint failed: p.id (19)` | REAL: engine.rs:10927/11089 hardcode generic msg, no schema ctx; also F3-wrapped |
| F1b | INSERT dup composite PK | `UNIQUE constraint failed: c.a, b` | `UNIQUE constraint failed: c.a, c.b` | REAL: qualifier only on 1st col; label built in codegen (op.p4, consumed engine.rs:9030/11301) |
| F1c | INSERT dup WITHOUT ROWID PK | `UNIQUE constraint failed: w.a` | `UNIQUE constraint failed: w.a` | ALREADY CORRECT — no change |
| F2 | table_info WITHOUT ROWID PK cols | notnull=0 | notnull=1 | REAL but DISPLAY-ONLY (enforcement already rejects NULL — verified) |
| F3-nn | INSERT NULL into NOT NULL col | `internal error: VDBE halted with code 19: NOT NULL constraint failed: t.x` | `NOT NULL constraint failed: t.x (19)` | REAL: msg correct, WRAPPED by frankenerror_from_vdbe_halt |
| F3-chk | CHECK violation | `CHECK constraint failed: amt > 0` (clean) | `CHECK constraint failed: amt>0` | wrap already fixed; only spacing (=F4) |
| F3-col | SELECT unknown column | `internal error: no such column: nope in table t` | `no such column: nope` | REAL: wrapped Internal + extra " in table t" |
| F3-tbl | CREATE existing table | `internal error: table t already exists` | `table t already exists` | REAL: wrapped Internal |
| F4 | CHECK expr text | `amt > 0` (AST re-render) | `amt>0` (source text) | minor — source-text preservation |
| F5 | parser syntax error | `SQL error at offset N: unexpected token in expression: X` | `near "X": syntax error` | minor — format parity |

---

## PATCH F3 (HIGH VALUE, cleanest) — generalize `frankenerror_from_vdbe_halt`

Site: `crates/fsqlite-core/src/connection.rs:96948`. Today it routes ONLY CHECK to a
clean variant; every other constraint halt falls to `Internal("VDBE halted with code
19: ...")`. Mirror the CHECK pattern for the other SQLITE_CONSTRAINT messages (all the
clean variants exist with stock-correct Display: NotNullViolation, UniqueViolation,
ForeignKeyViolation — error/lib.rs:147-160).

ANCHOR (old_string):
```rust
    if message == "CHECK constraint failed" {
        return FrankenError::CheckViolation {
            name: String::new(),
        };
    }
    FrankenError::Internal(format!("VDBE halted with code {code}: {message}"))
```
REPLACE WITH (new_string):
```rust
    if message == "CHECK constraint failed" {
        return FrankenError::CheckViolation {
            name: String::new(),
        };
    }
    // bd-a506j F3: route the remaining SQLITE_CONSTRAINT (19) halts to their clean
    // variants so they surface with the exact stock text under SQLITE_CONSTRAINT,
    // instead of "internal error: VDBE halted with code 19: <msg>". Mirrors CHECK.
    if let Some(column) = message.strip_prefix("NOT NULL constraint failed: ") {
        return FrankenError::NotNullViolation {
            column: column.to_owned(),
        };
    }
    if let Some(columns) = message.strip_prefix("UNIQUE constraint failed: ") {
        return FrankenError::UniqueViolation {
            columns: columns.to_owned(),
        };
    }
    if message == "FOREIGN KEY constraint failed" {
        return FrankenError::ForeignKeyViolation;
    }
    FrankenError::Internal(format!("VDBE halted with code {code}: {message}"))
```
RIPPLE: audit tests asserting `Internal(".. VDBE halted .. NOT NULL/UNIQUE ..")` — they
must now assert the clean variant / stock text. Also the 3 sibling wrap sites
(connection.rs:120051, 120142, 135923) — confirm whether they funnel through THIS fn or
re-wrap independently; if independent, apply the same routing (or better, make them call
frankenerror_from_vdbe_halt).

## PATCH F3-col / F3-tbl — clean up `codegen_error_to_franken`

Site: `crates/fsqlite-core/src/connection.rs:118836`. Maps ColumnNotFound/TableNotFound
to `Internal(...)` strings (wrapped + non-stock text). Route to the structured clean
variants (SQLITE_ERROR, stock text).

ANCHOR (old_string):
```rust
        CodegenError::TableNotFound(name) => {
            FrankenError::Internal(format!("no such table: {name}"))
        }
        CodegenError::ColumnNotFound { table, column } => {
            FrankenError::Internal(format!("no such column: {column} in table {table}"))
        }
```
REPLACE WITH (new_string):
```rust
        // bd-a506j F3: stock text is "no such table: X" / "no such column: X" under
        // SQLITE_ERROR — not "internal error: ... in table Y". Route to the clean
        // structured variants (NoSuchTable/NoSuchColumn), matching the index-bind
        // mapper (create_index_bind_error_to_franken) already at ~118852.
        CodegenError::TableNotFound(name) => FrankenError::NoSuchTable { name },
        CodegenError::ColumnNotFound { column, .. } => {
            FrankenError::NoSuchColumn { name: column }
        }
```
INTERACTION with bd-jcjkf DQS: this makes `dqs_missing_column_name` simpler — the
`FrankenError::Internal("no such column: X in table Y")` arm becomes unnecessary; the
structured `NoSuchColumn { name }` arm covers it. Update the DQS engine draft's extractor
comment accordingly (still keep the Internal-parse arm as a belt-and-suspenders for any
path that has not been converted).
RIPPLE: tests asserting the old "no such column: X in table Y" / "internal error:" text.

## PATCH F2 (display-only) — WITHOUT ROWID PK columns report notnull=1

Site: `crates/fsqlite-core/src/connection.rs:70308` (table_info/table_xinfo row build).
Enforcement ALREADY treats WITHOUT ROWID PK columns as NOT NULL (verified: INSERT NULL ->
"NOT NULL constraint failed: wr.a"); only the DISPLAY is inconsistent. `pk` (PK position,
>0 for PK cols) is computed just below at 70313; reorder it above `notnull` and OR it in.

ANCHOR (old_string):
```rust
                                let notnull = i64::from(col.notnull);
                                let dflt =
                                    col.default_value.as_ref().map_or(SqliteValue::Null, |s| {
                                        SqliteValue::Text(s.clone().into())
                                    });
                                let pk = pk_positions.get(i).copied().unwrap_or(0);
```
REPLACE WITH (new_string):
```rust
                                let pk = pk_positions.get(i).copied().unwrap_or(0);
                                // bd-a506j F2: WITHOUT ROWID PK columns are implicitly
                                // NOT NULL (stock reports notnull=1, and frank already
                                // ENFORCES it). Reflect that in table_info/table_xinfo.
                                let notnull = i64::from(col.notnull || (t.without_rowid && pk > 0));
                                let dflt =
                                    col.default_value.as_ref().map_or(SqliteValue::Null, |s| {
                                        SqliteValue::Text(s.clone().into())
                                    });
```
CONFIRM AT BUILD: exact WITHOUT-ROWID flag on the table binding `t` here (TableSchema).
Candidates: `t.without_rowid` (bool). If the field name differs, adjust. If `t` is not a
TableSchema with that flag in scope, thread it from the schema lookup. Keeper: extend the
existing table_info tests; assert notnull=1 for a `PRIMARY KEY(a,b) WITHOUT ROWID`.

## F1a (rowid-IPK message) — needs schema context (harder; approach only)

engine.rs:10927 and 11089 emit `"PRIMARY KEY constraint failed"` from the raw Insert
opcode conflict path, which has NO table/IPK-column name (that is why the WITHOUT-ROWID
INDEX path — which DOES carry a columns label in op.p4 — is already correct, F1c). Stock
emits `UNIQUE constraint failed: <table>.<ipkcol>` for a rowid-IPK conflict.
FIX APPROACH: give the rowid-IPK Insert conflict the same label mechanism the index path
uses — codegen embeds `"<table>.<ipkcol>"` in the Insert op's P4 (or the cursor carries
the table schema), and the conflict site emits `UNIQUE constraint failed: {label}` via
`FrankenError::UniqueViolation`/`ExecOutcome::Error{code:Constraint}`. Then PATCH F3 keeps
it clean (unwrapped). MUST update the two test assertions at engine.rs:30213 and 30269
(they assert the generic string on a raw hand-built program with no schema — those may
keep the generic message if no label is available, i.e. the label is only added when
codegen provides one; decide at build). Verify vs stock for rowid-IPK, composite, and
WITHOUT ROWID.

## F1b (composite qualifier `c.a, b` -> `c.a, c.b`) — codegen label build

The columns label is built in codegen and embedded as op.p4 (consumed at engine.rs:9030
`let columns = match &op.p4` and 11301 `let columns_label = match &op.p4`). The bug: the
table qualifier is prefixed to the FIRST column only. FIX: where codegen builds the label,
qualify EVERY column: `cols.iter().map(|c| format!("{table}.{c}")).collect::<Vec<_>>()
.join(", ")` (stock qualifies each: `c.a, c.b`). LOCATE at build: grep codegen.rs for the
op.p4 constraint-label construction feeding the PK/unique-index Insert (search the P4
string built from the index's key columns + table name). Verify vs stock for 2- and
3-column PKs and multi-column UNIQUE indexes.

## F4 / F5 (minor)
- F4: CHECK expr text `amt > 0` (AST re-render) vs `amt>0` (source). Preserve the original
  CHECK source substring (span slice of the CREATE TABLE text) instead of re-rendering the
  AST. Low priority; cosmetic.
- F5: parser syntax error `SQL error at offset N: unexpected token in expression: X` vs
  stock `near "X": syntax error`. Reformat the parser error surface. Low priority.

## Recommended apply order (one commit)
F3 + F3-col/tbl + F2 first (clean, high-parity, low-risk aside from test ripples), then
F1a + F1b (need codegen label plumbing), F4/F5 last. Verify each vs sqlite3 3.46.1 /
rusqlite differential; re-run the error-text probe. Coordinate F3-col with the bd-jcjkf
DQS extractor.
