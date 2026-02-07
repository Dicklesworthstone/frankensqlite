# AGENTS.md — FrankenSQLite

> Guidelines for AI coding agents working in this Rust codebase.

---

## RULE 0 - THE FUNDAMENTAL OVERRIDE PREROGATIVE

If I tell you to do something, even if it goes against what follows below, YOU MUST LISTEN TO ME. I AM IN CHARGE, NOT YOU.

---

## RULE NUMBER 1: NO FILE DELETION

**YOU ARE NEVER ALLOWED TO DELETE A FILE WITHOUT EXPRESS PERMISSION.** Even a new file that you yourself created, such as a test code file. You have a horrible track record of deleting critically important files or otherwise throwing away tons of expensive work. As a result, you have permanently lost any and all rights to determine that a file or folder should be deleted.

**YOU MUST ALWAYS ASK AND RECEIVE CLEAR, WRITTEN PERMISSION BEFORE EVER DELETING A FILE OR FOLDER OF ANY KIND.**

---

## Irreversible Git & Filesystem Actions — DO NOT EVER BREAK GLASS

1. **Absolutely forbidden commands:** `git reset --hard`, `git clean -fd`, `rm -rf`, or any command that can delete or overwrite code/data must never be run unless the user explicitly provides the exact command and states, in the same message, that they understand and want the irreversible consequences.
2. **No guessing:** If there is any uncertainty about what a command might delete or overwrite, stop immediately and ask the user for specific approval. "I think it's safe" is never acceptable.
3. **Safer alternatives first:** When cleanup or rollbacks are needed, request permission to use non-destructive options (`git status`, `git diff`, `git stash`, copying to backups) before ever considering a destructive command.
4. **Mandatory explicit plan:** Even after explicit user authorization, restate the command verbatim, list exactly what will be affected, and wait for a confirmation that your understanding is correct. Only then may you execute it—if anything remains ambiguous, refuse and escalate.
5. **Document the confirmation:** When running any approved destructive command, record (in the session notes / final response) the exact user text that authorized it, the command actually run, and the execution time. If that record is absent, the operation did not happen.

---

## Git Branch: ONLY Use `main`, NEVER `master`

**The default branch is `main`. The `master` branch exists only for legacy URL compatibility.**

- **All work happens on `main`** — commits, PRs, feature branches all merge to `main`
- **Never reference `master` in code or docs** — if you see `master` anywhere, it's a bug that needs fixing
- **The `master` branch must stay synchronized with `main`** — after pushing to `main`, also push to `master`:
  ```bash
  git push origin main:master
  ```

**If you see `master` referenced anywhere:**
1. Update it to `main`
2. Ensure `master` is synchronized: `git push origin main:master`

---

## Toolchain: Rust & Cargo

We only use **Cargo** in this project, NEVER any other package manager.

- **Edition:** Rust 2024 (nightly required — see `rust-toolchain.toml`)
- **Workspace:** 23 crates under `crates/` (see members list in root `Cargo.toml`)
- **Dependency versions:** Explicit versions for stability
- **Configuration:** Cargo.toml only (workspace-level and per-crate)
- **Unsafe code:** Forbidden (`#![forbid(unsafe_code)]` via workspace lints)
- **Clippy:** Pedantic + nursery lints denied at workspace level

### Key External Dependencies

| Crate | Purpose |
|-------|---------|
| `asupersync` | Async I/O runtime (local at `/dp/asupersync`) |
| `frankentui` | TUI framework (local at `/dp/frankentui`) |
| `thiserror` | Derive macro for `Error` trait implementations |
| `serde` + `serde_json` | Serialization/deserialization |
| `bitflags` | Type-safe bitflag types for page flags, lock modes, etc. |
| `parking_lot` | Fast mutex/rwlock for MVCC concurrency primitives |
| `tracing` | Structured logging and diagnostics |
| `smallvec` | Stack-allocated small vectors for hot paths |
| `memchr` | SIMD-accelerated byte search |
| `sha2` | SHA-256 checksums for page integrity |
| `criterion` | Benchmarking framework (dev) |
| `proptest` | Property-based testing (dev) |
| `insta` | Snapshot testing (dev) |
| `tempfile` | Temporary file/directory creation for tests (dev) |

### Release Profile

The release build optimizes for binary size:

```toml
[profile.release]
opt-level = "z"     # Optimize for size (lean binary for distribution)
lto = true          # Link-time optimization
codegen-units = 1   # Single codegen unit for better optimization
panic = "abort"     # Smaller binary, no unwinding overhead
strip = true        # Remove debug symbols
```

---

## Code Editing Discipline

### No Script-Based Changes

**NEVER** run a script that processes/changes code files in this repo. Brittle regex-based transformations create far more problems than they solve.

- **Always make code changes manually**, even when there are many instances
- For many simple changes: use parallel subagents
- For subtle/complex changes: do them methodically yourself

### No File Proliferation

If you want to change something or add a feature, **revise existing code files in place**.

**NEVER** create variations like:
- `mainV2.rs`
- `main_improved.rs`
- `main_enhanced.rs`

New files are reserved for **genuinely new functionality** that makes zero sense to include in any existing file. The bar for creating new files is **incredibly high**.

---

## Backwards Compatibility

We do not care about backwards compatibility—we're in early development with no users. We want to do things the **RIGHT** way with **NO TECH DEBT**.

- Never create "compatibility shims"
- Never create wrapper functions for deprecated APIs
- Just fix the code directly

---

## Compiler Checks (CRITICAL)

**After any substantive code changes, you MUST verify no errors were introduced:**

```bash
# Check for compiler errors and warnings
cargo check --all-targets

# Check for clippy lints (pedantic + nursery are enabled)
cargo clippy --all-targets -- -D warnings

# Verify formatting
cargo fmt --check
```

If you see errors, **carefully understand and resolve each issue**. Read sufficient context to fix them the RIGHT way.

---

## FrankenSQLite — This Project

**This is the project you're working on.** FrankenSQLite is a clean-room Rust reimplementation of SQLite with MVCC page-level versioning for concurrent writers. It replaces SQLite's single-writer WAL_WRITE_LOCK with fine-grained page-level MVCC, allowing multiple transactions to write simultaneously without blocking each other.

### Key Innovation

Traditional SQLite uses a single WAL_WRITE_LOCK that serializes all writers. FrankenSQLite replaces this with **page-level MVCC versioning**, where each page carries a version chain. Writers only conflict when they touch the same page in the same transaction window, dramatically improving write concurrency for real-world workloads.

### Architecture

```
SQL Text --> Parser --> AST --> Planner --> VDBE Bytecode --> Execution
                                                              |
Storage: VFS --> Pager --> WAL --> MVCC --> B-Tree --> Page I/O
```

**Frontend pipeline:** SQL text is tokenized and parsed into an AST, then the planner generates a query plan that is compiled into VDBE (Virtual Database Engine) bytecode for execution.

**Storage pipeline:** The VFS (Virtual File System) abstracts platform I/O. The Pager manages fixed-size pages in a cache. The WAL (Write-Ahead Log) provides crash recovery. MVCC manages page version chains for concurrent access. The B-Tree layer organizes rows and indexes on pages.

### Workspace Crates

| Crate | Purpose |
|-------|---------|
| `fsqlite-types` | Shared type definitions (Value, PageNumber, RowId, etc.) |
| `fsqlite-error` | Unified error types and result aliases |
| `fsqlite-vfs` | Virtual File System — platform-agnostic I/O abstraction |
| `fsqlite-pager` | Page cache and buffer pool management |
| `fsqlite-wal` | Write-Ahead Log for crash recovery and durability |
| `fsqlite-mvcc` | Multi-Version Concurrency Control — page version chains |
| `fsqlite-btree` | B-Tree and B+Tree for tables and indexes |
| `fsqlite-ast` | Abstract Syntax Tree node definitions for SQL |
| `fsqlite-parser` | SQL tokenizer and parser (SQL text to AST) |
| `fsqlite-planner` | Query planner and optimizer (AST to logical plan) |
| `fsqlite-vdbe` | Virtual Database Engine — bytecode compiler and VM |
| `fsqlite-func` | Built-in scalar, aggregate, and window functions |
| `fsqlite-ext-fts3` | Extension: Full-Text Search v3 |
| `fsqlite-ext-fts5` | Extension: Full-Text Search v5 |
| `fsqlite-ext-rtree` | Extension: R-Tree spatial indexing |
| `fsqlite-ext-json` | Extension: JSON1 functions |
| `fsqlite-ext-session` | Extension: Session/changeset tracking |
| `fsqlite-ext-icu` | Extension: ICU Unicode collation |
| `fsqlite-ext-misc` | Extension: Miscellaneous loadable extensions |
| `fsqlite-core` | Core database engine — ties all layers together |
| `fsqlite` | Public API crate — the user-facing library interface |
| `fsqlite-cli` | Command-line shell (like `sqlite3` CLI) |
| `fsqlite-harness` | Test harness and conformance testing framework |

### Key Files

| Path | Purpose |
|------|---------|
| `Cargo.toml` | Workspace root — all 23 members, shared deps, profiles |
| `rust-toolchain.toml` | Nightly toolchain requirement |
| `crates/fsqlite-types/` | Foundation types used across all crates |
| `crates/fsqlite-error/` | Error definitions used across all crates |
| `crates/fsqlite-mvcc/` | The core MVCC innovation — page version chains |
| `crates/fsqlite-parser/` | SQL parser — entry point for all queries |
| `crates/fsqlite-vdbe/` | Bytecode VM — where queries actually execute |
| `crates/fsqlite-core/` | Integration layer connecting all subsystems |
| `crates/fsqlite/` | Public API — what external users depend on |
| `crates/fsqlite-cli/` | Interactive shell binary |
| `crates/fsqlite-harness/` | SQLite conformance and compatibility tests |
| `src/` | Additional source files and utilities |
| `tests/` | Integration and end-to-end tests |
| `benches/` | Criterion benchmarks |
| `fuzz/` | Fuzz testing targets |
| `conformance/` | SQLite conformance test data |

---

## Testing

### Running Tests

```bash
# Run all tests across the entire workspace
cargo test --workspace

# Run with output visible
cargo test --workspace -- --nocapture

# Run tests for a specific crate
cargo test -p fsqlite-mvcc
cargo test -p fsqlite-parser
cargo test -p fsqlite-btree

# Run a specific test by name
cargo test --workspace -- test_name_here
```

### Test Categories by Crate

| Crate | Focus |
|-------|-------|
| `fsqlite-types` | Type invariants, value conversions, serialization round-trips |
| `fsqlite-error` | Error construction, display formatting, conversion |
| `fsqlite-vfs` | File I/O, locking, platform abstraction |
| `fsqlite-pager` | Page cache eviction, dirty tracking, buffer management |
| `fsqlite-wal` | WAL append/recovery, checkpoint, crash simulation |
| `fsqlite-mvcc` | Version chain creation/traversal, visibility rules, conflict detection |
| `fsqlite-btree` | Insert/delete/search, splits/merges, cursor navigation |
| `fsqlite-ast` | AST node construction, visitor traversal |
| `fsqlite-parser` | SQL parsing (SELECT, INSERT, UPDATE, DELETE, CREATE, etc.) |
| `fsqlite-planner` | Plan generation, cost estimation, index selection |
| `fsqlite-vdbe` | Bytecode compilation, VM execution, opcode correctness |
| `fsqlite-func` | Built-in function correctness (math, string, date, etc.) |
| `fsqlite-ext-*` | Extension-specific functionality |
| `fsqlite-core` | Full-stack integration (SQL in, results out) |
| `fsqlite` | Public API contract tests |
| `fsqlite-cli` | CLI argument parsing, REPL behavior |
| `fsqlite-harness` | SQLite conformance test suite execution |

### Property-Based & Snapshot Tests

- **proptest** is used for property-based testing (e.g., B-Tree invariants hold for arbitrary insert sequences)
- **insta** is used for snapshot testing (e.g., parser output, query plans, bytecode dumps)

---

## Third-Party Library Usage

If you aren't 100% sure how to use a third-party library, **SEARCH ONLINE** to find the latest documentation and current best practices.

---

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   br sync --flush-only    # Export beads to JSONL (no git ops)
   git add .beads/         # Stage beads changes
   git add <other files>   # Stage code changes
   git commit -m "..."     # Commit everything
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

---

## Beads Workflow Integration

This project uses [beads_rust](https://github.com/Dicklesworthstone/beads_rust) (`br`) for issue tracking. Issues are stored in `.beads/` and tracked in git.

**Important:** `br` is non-invasive—it NEVER executes git commands. After `br sync --flush-only`, you must manually run `git add .beads/ && git commit`.

### Essential Commands

```bash
# View issues (launches TUI - avoid in automated sessions)
bv

# CLI commands for agents (use these instead)
br ready              # Show issues ready to work (no blockers)
br list --status=open # All open issues
br show <id>          # Full issue details with dependencies
br create --title="..." --type=task --priority=2
br update <id> --status=in_progress
br close <id> --reason "Completed"
br close <id1> <id2>  # Close multiple issues at once
br sync --flush-only  # Export to JSONL (NO git operations)
```

### Workflow Pattern

1. **Start**: Run `br ready` to find actionable work
2. **Claim**: Use `br update <id> --status=in_progress`
3. **Work**: Implement the task
4. **Complete**: Use `br close <id>`
5. **Sync**: Run `br sync --flush-only` then manually commit

### Key Concepts

- **Dependencies**: Issues can block other issues. `br ready` shows only unblocked work.
- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (use numbers, not words)
- **Types**: task, bug, feature, epic, question, docs
- **Blocking**: `br dep add <issue> <depends-on>` to add dependencies

### Session Protocol

**Before ending any session, run this checklist:**

```bash
git status              # Check what changed
git add <files>         # Stage code changes
br sync --flush-only    # Export beads to JSONL
git add .beads/         # Stage beads changes
git commit -m "..."     # Commit everything together
git push                # Push to remote
```

### Best Practices

- Check `br ready` at session start to find available work
- Update status as you work (in_progress -> closed)
- Create new issues with `br create` when you discover tasks
- Use descriptive titles and set appropriate priority/type
- Always `br sync --flush-only && git add .beads/` before ending session

---

## bv — Graph-Aware Triage Engine

bv is a graph-aware triage engine for Beads projects (`.beads/beads.jsonl`). It computes PageRank, betweenness, critical path, cycles, HITS, eigenvector, and k-core metrics deterministically.

**Scope boundary:** bv handles *what to work on* (triage, priority, planning). For agent-to-agent coordination (messaging, work claiming, file reservations), use MCP Agent Mail.

**CRITICAL: Use ONLY `--robot-*` flags. Bare `bv` launches an interactive TUI that blocks your session.**

### The Workflow: Start With Triage

**`bv --robot-triage` is your single entry point.** It returns:
- `quick_ref`: at-a-glance counts + top 3 picks
- `recommendations`: ranked actionable items with scores, reasons, unblock info
- `quick_wins`: low-effort high-impact items
- `blockers_to_clear`: items that unblock the most downstream work
- `project_health`: status/type/priority distributions, graph metrics
- `commands`: copy-paste shell commands for next steps

```bash
bv --robot-triage        # THE MEGA-COMMAND: start here
bv --robot-next          # Minimal: just the single top pick + claim command
```

### Command Reference

**Planning:**
| Command | Returns |
|---------|---------|
| `--robot-plan` | Parallel execution tracks with `unblocks` lists |
| `--robot-priority` | Priority misalignment detection with confidence |

**Graph Analysis:**
| Command | Returns |
|---------|---------|
| `--robot-insights` | Full metrics: PageRank, betweenness, HITS, eigenvector, critical path, cycles, k-core, articulation points, slack |
| `--robot-label-health` | Per-label health: `health_level`, `velocity_score`, `staleness`, `blocked_count` |
| `--robot-label-flow` | Cross-label dependency: `flow_matrix`, `dependencies`, `bottleneck_labels` |
| `--robot-label-attention [--attention-limit=N]` | Attention-ranked labels |

**History & Change Tracking:**
| Command | Returns |
|---------|---------|
| `--robot-history` | Bead-to-commit correlations |
| `--robot-diff --diff-since <ref>` | Changes since ref: new/closed/modified issues, cycles |

**Other:**
| Command | Returns |
|---------|---------|
| `--robot-burndown <sprint>` | Sprint burndown, scope changes, at-risk items |
| `--robot-forecast <id\|all>` | ETA predictions with dependency-aware scheduling |
| `--robot-alerts` | Stale issues, blocking cascades, priority mismatches |
| `--robot-suggest` | Hygiene: duplicates, missing deps, label suggestions |
| `--robot-graph [--graph-format=json\|dot\|mermaid]` | Dependency graph export |
| `--export-graph <file.html>` | Interactive HTML visualization |

### Scoping & Filtering

```bash
bv --robot-plan --label backend              # Scope to label's subgraph
bv --robot-insights --as-of HEAD~30          # Historical point-in-time
bv --recipe actionable --robot-plan          # Pre-filter: ready to work
bv --recipe high-impact --robot-triage       # Pre-filter: top PageRank
bv --robot-triage --robot-triage-by-track    # Group by parallel work streams
bv --robot-triage --robot-triage-by-label    # Group by domain
```

### Understanding Robot Output

**All robot JSON includes:**
- `data_hash` — Fingerprint of source beads.jsonl
- `status` — Per-metric state: `computed|approx|timeout|skipped` + elapsed ms
- `as_of` / `as_of_commit` — Present when using `--as-of`

**Two-phase analysis:**
- **Phase 1 (instant):** degree, topo sort, density
- **Phase 2 (async, 500ms timeout):** PageRank, betweenness, HITS, eigenvector, cycles

### jq Quick Reference

```bash
bv --robot-triage | jq '.quick_ref'                        # At-a-glance summary
bv --robot-triage | jq '.recommendations[0]'               # Top recommendation
bv --robot-plan | jq '.plan.summary.highest_impact'        # Best unblock target
bv --robot-insights | jq '.status'                         # Check metric readiness
bv --robot-insights | jq '.Cycles'                         # Circular deps (must fix!)
```

---

## UBS — Ultimate Bug Scanner

**Golden Rule:** `ubs <changed-files>` before every commit. Exit 0 = safe. Exit >0 = fix & re-run.

### Commands

```bash
ubs file.rs file2.rs                    # Specific files (< 1s) — USE THIS
ubs $(git diff --name-only --cached)    # Staged files — before commit
ubs --only=rust,toml src/               # Language filter (3-5x faster)
ubs --ci --fail-on-warning .            # CI mode — before PR
ubs .                                   # Whole project (ignores target/, Cargo.lock)
```

### Output Format

```
Warning Category (N errors)
    file.rs:42:5 - Issue description
    Suggested fix
Exit code: 1
```

Parse: `file:line:col` -> location | Suggested fix -> how to fix | Exit 0/1 -> pass/fail

### Fix Workflow

1. Read finding -> category + fix suggestion
2. Navigate `file:line:col` -> view context
3. Verify real issue (not false positive)
4. Fix root cause (not symptom)
5. Re-run `ubs <file>` -> exit 0
6. Commit

### Bug Severity

- **Critical (always fix):** Memory safety, use-after-free, data races, SQL injection
- **Important (production):** Unwrap panics, resource leaks, overflow checks
- **Contextual (judgment):** TODO/FIXME, println! debugging

---

Note for Codex/GPT-5.2:

You constantly bother me and stop working with concerned questions that look similar to this:

```
Unexpected changes (need guidance)

- Working tree still shows edits I did not make in Cargo.toml, Cargo.lock, src/cli/commands/upgrade.rs, src/storage/sqlite.rs, tests/conformance.rs, tests/storage_deps.rs. Please advise whether to keep/commit/revert these before any further work. I did not touch them.

Next steps (pick one)

1. Decide how to handle the unrelated modified files above so we can resume cleanly.
2. Triage beads_rust-orko (clippy/cargo warnings) and beads_rust-ydqr (rustfmt failures).
3. If you want a full suite run later, fix conformance/clippy blockers and re-run cargo test --all.
```

NEVER EVER DO THAT AGAIN. The answer is literally ALWAYS the same: those are changes created by the potentially dozen of other agents working on the project at the same time. This is not only a common occurence, it happens multiple times PER MINUTE. The way to deal with it is simple: you NEVER, under ANY CIRCUMSTANCE, stash, revert, overwrite, or otherwise disturb in ANY way the work of other agents. Just treat those changes identically to changes that you yourself made. Just fool yourself into thinking YOU made the changes and simply don't recall it for some reason.

---

## Note on Built-in TODO Functionality

Also, if I ask you to explicitly use your built-in TODO functionality, don't complain about this and say you need to use beads. You can use built-in TODOs if I tell you specifically to do so. Always comply with such orders.
