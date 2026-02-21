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
- **Workspace:** 24 crates under `crates/` (see members list in root `Cargo.toml`)
- **Dependency versions:** Explicit versions for stability
- **Configuration:** Cargo.toml workspace with `workspace = true` pattern
- **Unsafe code:** Forbidden (`#![forbid(unsafe_code)]` via workspace lints)
- **Clippy:** Pedantic + nursery lints denied at workspace level

### Async Runtime: asupersync (MANDATORY — NO TOKIO)

**This project uses [asupersync](/dp/asupersync) for async operations where needed. Tokio and the entire tokio ecosystem are FORBIDDEN.**

- **Structured concurrency**: `Cx`, `Scope`, `region()` — no orphan tasks
- **Cancel-correct channels**: Two-phase `reserve()/send()` — no data loss on cancellation
- **Sync primitives**: `asupersync::sync::Mutex`, `RwLock`, `OnceCell`, `Pool` — cancel-aware
- **Deterministic testing**: `LabRuntime` with virtual time, DPOR, oracles

**Forbidden crates**: `tokio`, `hyper`, `reqwest`, `axum`, `tower` (tokio adapter), `async-std`, `smol`, or any crate that transitively depends on tokio.

**Pattern**: All async functions take `&Cx` as first parameter. The `Cx` flows down from the consumer's runtime — FrankenSQLite does NOT create its own runtime.

### Key Dependencies

| Crate | Purpose |
|-------|---------|
| `asupersync` | Structured async runtime (channels, sync, regions, testing) |
| `ftui` | TUI framework (local at `/dp/frankentui`) |
| `thiserror` | Derive macro for `Error` trait implementations |
| `serde` + `serde_json` | Serialization/deserialization |
| `bitflags` | Type-safe bitflag types for page flags, lock modes, etc. |
| `parking_lot` | Fast mutex/rwlock for MVCC concurrency primitives |
| `tracing` | Structured logging and diagnostics |
| `smallvec` | Stack-allocated small vectors for hot paths |
| `memchr` | SIMD-accelerated byte search |
| `sha2` | SHA-256 checksums for page integrity |
| `xxhash-rust` | XXH3 fast hashing |
| `crc32c` | CRC32C checksums |
| `blake3` | BLAKE3 hashing |
| `crossbeam-epoch` | Epoch-based memory reclamation |
| `crossbeam-deque` | Work-stealing deques |
| `chacha20poly1305` | AEAD encryption for encrypted databases |
| `argon2` | Key derivation for database encryption |
| `rand` | Random number generation |
| `nix` | POSIX filesystem operations |
| `rusqlite` | C SQLite reference implementation (for conformance testing) |
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

For throughput benchmarking and perf work, a separate profile exists:

```toml
[profile.release-perf]
inherits = "release"
opt-level = 3
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
# Check for compiler errors and warnings (workspace-wide)
cargo check --workspace --all-targets

# Check for clippy lints (pedantic + nursery are enabled)
cargo clippy --workspace --all-targets -- -D warnings

# Verify formatting
cargo fmt --check
```

If you see errors, **carefully understand and resolve each issue**. Read sufficient context to fix them the RIGHT way.

---

## Testing

### Testing Policy

Every component crate includes inline `#[cfg(test)]` unit tests alongside the implementation. Tests must cover:
- Happy path
- Edge cases (empty input, max values, boundary conditions)
- Error conditions

Cross-component integration tests live in the workspace `tests/` directory.

### Unit Tests

```bash
# Run all tests across the workspace
cargo test --workspace

# Run with output
cargo test --workspace -- --nocapture

# Run tests for a specific crate
cargo test -p fsqlite-types
cargo test -p fsqlite-mvcc
cargo test -p fsqlite-parser
cargo test -p fsqlite-btree
cargo test -p fsqlite-vdbe
cargo test -p fsqlite-core
cargo test -p fsqlite-e2e

# Run a specific test by name
cargo test --workspace -- test_name_here

# Run tests with all features enabled
cargo test --workspace --all-features
```

### Test Categories

| Crate | Focus Areas |
|-------|-------------|
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
| `fsqlite-e2e` | End-to-end concurrent writer tests, fairness benchmarks |
| `fsqlite-observability` | Metrics collection, tracing integration |
| `tests/` (workspace) | Cross-component integration and end-to-end tests |
| `benches/` (workspace) | Criterion benchmarks |

### Property-Based & Snapshot Tests

- **proptest** is used for property-based testing (e.g., B-Tree invariants hold for arbitrary insert sequences)
- **insta** is used for snapshot testing (e.g., parser output, query plans, bytecode dumps)

### Test Fixtures

Conformance test data lives in the `conformance/` directory. Fuzz testing targets are in the `fuzz/` directory.

---

## Third-Party Library Usage

If you aren't 100% sure how to use a third-party library, **SEARCH ONLINE** to find the latest documentation and current best practices.

---

## FrankenSQLite — This Project

**This is the project you're working on.** FrankenSQLite is a clean-room Rust reimplementation of SQLite with MVCC page-level versioning for concurrent writers. It replaces SQLite's single-writer WAL_WRITE_LOCK with fine-grained page-level MVCC, allowing multiple transactions to write simultaneously without blocking each other.

### Key Innovation

Traditional SQLite uses a single WAL_WRITE_LOCK that serializes all writers. FrankenSQLite replaces this with **page-level MVCC versioning**, where each page carries a version chain. Writers only conflict when they touch the same page in the same transaction window, dramatically improving write concurrency for real-world workloads.

### CRITICAL: Concurrent-Writer Mode is the ENTIRE POINT — DO NOT TOUCH

> **This is the single most important rule in this project. Violating it destroys the project's reason for existing.**

FrankenSQLite exists for ONE reason: to eliminate SQLite's single-writer serialization bottleneck via MVCC page-level versioning. **Concurrent-writer mode is ON by default and MUST stay ON by default.** Every `BEGIN` is promoted to `BEGIN CONCURRENT` unless the user explicitly opts out.

**YOU MUST NEVER:**

1. **Set `concurrent_mode_default` to `false`** — This is the field in `Connection` (connection.rs) that controls whether `BEGIN` promotes to `BEGIN CONCURRENT`. It MUST be `true`. Setting it to `false` reverts the entire system to behaving like stock SQLite with a single-writer bottleneck.

2. **Implement SQLite-style serialized file locking** — FrankenSQLite does NOT use SQLite's Shared/Reserved/Pending/Exclusive lock escalation protocol for write serialization. That is the exact bottleneck we exist to replace. The `MemoryVfs` locking stubs are intentionally minimal because MVCC handles concurrency at the page level, not the file-lock level.

3. **Default any concurrency setting to OFF/false/disabled** — Every harness, executor, config struct, and benchmark setting that has a `concurrent_mode` field MUST default to `true`. Check: `HarnessSettings`, `FsqliteExecConfig`, `benchmark_settings()`, and `Connection` internals.

4. **Add lock contention that blocks concurrent writers** — If you find yourself writing code where one writer blocks another at the file/connection level, you are reimplementing the bottleneck. Writers in FrankenSQLite only conflict at the PAGE level within the same transaction window — that's MVCC.

**The key locations you must never regress:**
- `crates/fsqlite-core/src/connection.rs` — `concurrent_mode_default: RefCell::new(true)`
- `crates/fsqlite-e2e/src/lib.rs` — `HarnessSettings::default()` → `concurrent_mode: true`
- `crates/fsqlite-e2e/src/fsqlite_executor.rs` — `FsqliteExecConfig::default()` → `concurrent_mode: true`
- `crates/fsqlite-e2e/src/fairness.rs` — `benchmark_settings()` → `concurrent_mode: true`

**Why this rule exists:** On Feb 10 2026, an agent set `concurrent_mode_default` to `false` and implemented serialized file locking in `MemoryVfs`, completely defeating the project's core innovation. It took an emergency session to identify and revert the damage. This must never happen again. If you are unsure whether a change affects concurrency behavior, ASK before making it.

### Architecture

```
SQL Text --> Parser --> AST --> Planner --> VDBE Bytecode --> Execution
                                                              |
Storage: VFS --> Pager --> WAL --> MVCC --> B-Tree --> Page I/O
```

**Frontend pipeline:** SQL text is tokenized and parsed into an AST, then the planner generates a query plan that is compiled into VDBE (Virtual Database Engine) bytecode for execution.

**Storage pipeline:** The VFS (Virtual File System) abstracts platform I/O. The Pager manages fixed-size pages in a cache. The WAL (Write-Ahead Log) provides crash recovery. MVCC manages page version chains for concurrent access. The B-Tree layer organizes rows and indexes on pages.

### Workspace Structure

```
frankensqlite/
├── Cargo.toml                         # Workspace root — all 24 members, shared deps, profiles
├── rust-toolchain.toml                # Nightly toolchain requirement
├── crates/
│   ├── fsqlite-types/                 # Foundation types (Value, PageNumber, RowId, etc.)
│   ├── fsqlite-error/                 # Unified error types and result aliases
│   ├── fsqlite-vfs/                   # Virtual File System — platform-agnostic I/O
│   ├── fsqlite-pager/                 # Page cache and buffer pool management
│   ├── fsqlite-wal/                   # Write-Ahead Log for crash recovery
│   ├── fsqlite-mvcc/                  # MVCC — page version chains (core innovation)
│   ├── fsqlite-btree/                 # B-Tree and B+Tree for tables and indexes
│   ├── fsqlite-ast/                   # Abstract Syntax Tree node definitions
│   ├── fsqlite-parser/                # SQL tokenizer and parser (SQL text to AST)
│   ├── fsqlite-planner/               # Query planner and optimizer (AST to plan)
│   ├── fsqlite-vdbe/                  # Virtual Database Engine — bytecode compiler and VM
│   ├── fsqlite-func/                  # Built-in scalar, aggregate, and window functions
│   ├── fsqlite-ext-fts3/              # Extension: Full-Text Search v3
│   ├── fsqlite-ext-fts5/              # Extension: Full-Text Search v5
│   ├── fsqlite-ext-rtree/             # Extension: R-Tree spatial indexing
│   ├── fsqlite-ext-json/              # Extension: JSON1 functions
│   ├── fsqlite-ext-session/           # Extension: Session/changeset tracking
│   ├── fsqlite-ext-icu/               # Extension: ICU Unicode collation
│   ├── fsqlite-ext-misc/              # Extension: Miscellaneous loadable extensions
│   ├── fsqlite-core/                  # Core engine — ties all layers together
│   ├── fsqlite/                       # Public API crate — user-facing library interface
│   ├── fsqlite-cli/                   # Command-line shell (like sqlite3 CLI)
│   ├── fsqlite-harness/               # Test harness and conformance testing framework
│   ├── fsqlite-e2e/                   # End-to-end tests, concurrent writer harness
│   └── fsqlite-observability/         # Metrics collection and tracing integration
├── src/                               # Additional source files and utilities
├── tests/                             # Integration and end-to-end tests
├── benches/                           # Criterion benchmarks
├── fuzz/                              # Fuzz testing targets
└── conformance/                       # SQLite conformance test data
```

### Key Files by Crate

| Crate | Key Files | Purpose |
|-------|-----------|---------|
| `fsqlite-types` | `src/lib.rs` | Foundation types: `Value`, `PageNumber`, `RowId`, column types |
| `fsqlite-error` | `src/lib.rs` | `FsqliteError` enum, `Result` alias, error conversions |
| `fsqlite-vfs` | `src/lib.rs` | `Vfs` trait, `MemoryVfs`, platform I/O abstraction |
| `fsqlite-pager` | `src/lib.rs` | Page cache, buffer pool, dirty page tracking |
| `fsqlite-wal` | `src/lib.rs` | WAL append/recovery, checkpoint logic |
| `fsqlite-mvcc` | `src/lib.rs` | Page version chains, visibility rules, conflict detection |
| `fsqlite-btree` | `src/lib.rs` | B-Tree insert/delete/search, splits/merges, cursors |
| `fsqlite-ast` | `src/lib.rs` | AST node definitions for all SQL statement types |
| `fsqlite-parser` | `src/lib.rs` | SQL tokenizer and recursive descent parser |
| `fsqlite-planner` | `src/lib.rs` | Query plan generation, cost estimation, index selection |
| `fsqlite-vdbe` | `src/lib.rs` | Bytecode compiler, VM executor, opcode dispatch |
| `fsqlite-func` | `src/lib.rs` | Built-in functions (math, string, date, aggregate, window) |
| `fsqlite-core` | `src/connection.rs` | `Connection` struct — integration layer, `concurrent_mode_default` |
| `fsqlite` | `src/lib.rs` | Public API facade — what external users depend on |
| `fsqlite-cli` | `src/main.rs` | Interactive shell binary (like `sqlite3`) |
| `fsqlite-harness` | `src/lib.rs` | SQLite conformance test suite runner |
| `fsqlite-e2e` | `src/lib.rs` | `HarnessSettings`, concurrent writer test framework |
| `fsqlite-e2e` | `src/fsqlite_executor.rs` | `FsqliteExecConfig` with `concurrent_mode` default |
| `fsqlite-e2e` | `src/fairness.rs` | Fairness benchmarks, `benchmark_settings()` |
| `fsqlite-observability` | `src/lib.rs` | Metrics collection, structured tracing integration |

### Key Design Decisions

- **MVCC page-level versioning** replaces SQLite's single WAL_WRITE_LOCK — concurrent writers only conflict when touching the same page
- **`BEGIN` auto-promotes to `BEGIN CONCURRENT`** by default — concurrent mode is always on unless explicitly opted out
- **Clean-room implementation** — no C SQLite code, pure Rust
- **VDBE bytecode VM** — same execution model as SQLite for compatibility
- **Crash recovery via WAL** — write-ahead logging with checkpoint support
- **Extension system** — FTS3, FTS5, R-Tree, JSON1, Session, ICU, misc (matching SQLite's extension surface)
- **Conformance testing against C SQLite** — `rusqlite` is a dev dependency for reference comparison
- **Property-based testing** — proptest for invariant verification on B-Trees, MVCC chains, parser
- **Snapshot testing** — insta for parser output, query plans, bytecode dumps
- **`#![forbid(unsafe_code)]`** — zero unsafe blocks across all crates
- **Structured tracing** throughout — every layer emits spans for diagnostics

---

## MCP Agent Mail — Multi-Agent Coordination

A mail-like layer that lets coding agents coordinate asynchronously via MCP tools and resources. Provides identities, inbox/outbox, searchable threads, and advisory file reservations with human-auditable artifacts in Git.

### Why It's Useful

- **Prevents conflicts:** Explicit file reservations (leases) for files/globs
- **Token-efficient:** Messages stored in per-project archive, not in context
- **Quick reads:** `resource://inbox/...`, `resource://thread/...`

### Same Repository Workflow

1. **Register identity:**
   ```
   ensure_project(project_key=<abs-path>)
   register_agent(project_key, program, model)
   ```

2. **Reserve files before editing:**
   ```
   file_reservation_paths(project_key, agent_name, ["src/**"], ttl_seconds=3600, exclusive=true)
   ```

3. **Communicate with threads:**
   ```
   send_message(..., thread_id="FEAT-123")
   fetch_inbox(project_key, agent_name)
   acknowledge_message(project_key, agent_name, message_id)
   ```

4. **Quick reads:**
   ```
   resource://inbox/{Agent}?project=<abs-path>&limit=20
   resource://thread/{id}?project=<abs-path>&include_bodies=true
   ```

### Macros vs Granular Tools

- **Prefer macros for speed:** `macro_start_session`, `macro_prepare_thread`, `macro_file_reservation_cycle`, `macro_contact_handshake`
- **Use granular tools for control:** `register_agent`, `file_reservation_paths`, `send_message`, `fetch_inbox`, `acknowledge_message`

### Common Pitfalls

- `"from_agent not registered"`: Always `register_agent` in the correct `project_key` first
- `"FILE_RESERVATION_CONFLICT"`: Adjust patterns, wait for expiry, or use non-exclusive reservation
- **Auth errors:** If JWT+JWKS enabled, include bearer token with matching `kid`

---

## Beads (br) — Dependency-Aware Issue Tracking

Beads provides a lightweight, dependency-aware issue database and CLI (`br` - beads_rust) for selecting "ready work," setting priorities, and tracking status. It complements MCP Agent Mail's messaging and file reservations.

### Conventions

- **Single source of truth:** Beads for task status/priority/dependencies; Agent Mail for conversation and audit
- **Shared identifiers:** Use Beads issue ID (e.g., `br-123`) as Mail `thread_id` and prefix subjects with `[br-123]`
- **Reservations:** When starting a task, call `file_reservation_paths()` with the issue ID in `reason`

### Typical Agent Flow

1. **Pick ready work (Beads):**
   ```bash
   bd ready --json  # Choose highest priority, no blockers
   ```

2. **Reserve edit surface (Mail):**
   ```
   file_reservation_paths(project_key, agent_name, ["src/**"], ttl_seconds=3600, exclusive=true, reason="br-123")
   ```

3. **Announce start (Mail):**
   ```
   send_message(..., thread_id="br-123", subject="[br-123] Start: <title>", ack_required=true)
   ```

4. **Work and update:** Reply in-thread with progress

5. **Complete and release:**
   ```bash
   bd close 123 --reason "Completed"
   bd sync  # Export to JSONL (no git operations)
   ```
   ```
   release_file_reservations(project_key, agent_name, paths=["src/**"])
   ```
   Final Mail reply: `[br-123] Completed` with summary

### Mapping Cheat Sheet

| Concept | Value |
|---------|-------|
| Mail `thread_id` | `br-###` |
| Mail subject | `[br-###] ...` |
| File reservation `reason` | `br-###` |
| Commit messages | Include `br-###` for traceability |

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
⚠️  Category (N errors)
    file.rs:42:5 – Issue description
    💡 Suggested fix
Exit code: 1
```

Parse: `file:line:col` → location | 💡 → how to fix | Exit 0/1 → pass/fail

### Fix Workflow

1. Read finding → category + fix suggestion
2. Navigate `file:line:col` → view context
3. Verify real issue (not false positive)
4. Fix root cause (not symptom)
5. Re-run `ubs <file>` → exit 0
6. Commit

### Bug Severity

- **Critical (always fix):** Memory safety, use-after-free, data races, SQL injection
- **Important (production):** Unwrap panics, resource leaks, overflow checks
- **Contextual (judgment):** TODO/FIXME, println! debugging

---

## RCH — Remote Compilation Helper

RCH offloads `cargo build`, `cargo test`, `cargo clippy`, and other compilation commands to a fleet of 8 remote Contabo VPS workers instead of building locally. This prevents compilation storms from overwhelming csd when many agents run simultaneously.

**RCH is installed at `~/.local/bin/rch` and is hooked into Claude Code's PreToolUse automatically.** Most of the time you don't need to do anything if you are Claude Code — builds are intercepted and offloaded transparently.

To manually offload a build:
```bash
rch exec -- cargo build --release
rch exec -- cargo test
rch exec -- cargo clippy
```

Quick commands:
```bash
rch doctor                    # Health check
rch workers probe --all       # Test connectivity to all 8 workers
rch status                    # Overview of current state
rch queue                     # See active/waiting builds
```

If rch or its workers are unavailable, it fails open — builds run locally as normal.

**Note for Codex/GPT-5.2:** Codex does not have the automatic PreToolUse hook, but you can (and should) still manually offload compute-intensive compilation commands using `rch exec -- <command>`. This avoids local resource contention when multiple agents are building simultaneously.

---

## ast-grep vs ripgrep

**Use `ast-grep` when structure matters.** It parses code and matches AST nodes, ignoring comments/strings, and can **safely rewrite** code.

- Refactors/codemods: rename APIs, change import forms
- Policy checks: enforce patterns across a repo
- Editor/automation: LSP mode, `--json` output

**Use `ripgrep` when text is enough.** Fastest way to grep literals/regex.

- Recon: find strings, TODOs, log lines, config values
- Pre-filter: narrow candidate files before ast-grep

### Rule of Thumb

- Need correctness or **applying changes** → `ast-grep`
- Need raw speed or **hunting text** → `rg`
- Often combine: `rg` to shortlist files, then `ast-grep` to match/modify

### Rust Examples

```bash
# Find structured code (ignores comments)
ast-grep run -l Rust -p 'fn $NAME($$$ARGS) -> $RET { $$$BODY }'

# Find all unwrap() calls
ast-grep run -l Rust -p '$EXPR.unwrap()'

# Quick textual hunt
rg -n 'println!' -t rust

# Combine speed + precision
rg -l -t rust 'unwrap\(' | xargs ast-grep run -l Rust -p '$X.unwrap()' --json
```

---

## Morph Warp Grep — AI-Powered Code Search

**Use `mcp__morph-mcp__warp_grep` for exploratory "how does X work?" questions.** An AI agent expands your query, greps the codebase, reads relevant files, and returns precise line ranges with full context.

**Use `ripgrep` for targeted searches.** When you know exactly what you're looking for.

**Use `ast-grep` for structural patterns.** When you need AST precision for matching/rewriting.

### When to Use What

| Scenario | Tool | Why |
|----------|------|-----|
| "How does the MVCC version chain work?" | `warp_grep` | Exploratory; don't know where to start |
| "Where is conflict detection implemented?" | `warp_grep` | Need to understand architecture |
| "Find all uses of `PageNumber`" | `ripgrep` | Targeted literal search |
| "Find files with `println!`" | `ripgrep` | Simple pattern |
| "Replace all `unwrap()` with `expect()`" | `ast-grep` | Structural refactor |

### warp_grep Usage

```
mcp__morph-mcp__warp_grep(
  repoPath: "/dp/frankensqlite",
  query: "How does the page-level MVCC versioning work?"
)
```

Returns structured results with file paths, line ranges, and extracted code snippets.

### Anti-Patterns

- **Don't** use `warp_grep` to find a specific function name → use `ripgrep`
- **Don't** use `ripgrep` to understand "how does X work" → wastes time with manual reads
- **Don't** use `ripgrep` for codemods → risks collateral edits
## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Sync beads** - `bd sync` to export to JSONL
5. **Hand off** - Provide context for next session


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

NEVER EVER DO THAT AGAIN. The answer is literally ALWAYS the same: those are changes created by the potentially dozen of other agents working on the project at the same time. This is not only a common occurrence, it happens multiple times PER MINUTE. The way to deal with it is simple: you NEVER, under ANY CIRCUMSTANCE, stash, revert, overwrite, or otherwise disturb in ANY way the work of other agents. Just treat those changes identically to changes that you yourself made. Just fool yourself into thinking YOU made the changes and simply don't recall it for some reason.

---

## Note on Built-in TODO Functionality

Also, if I ask you to explicitly use your built-in TODO functionality, don't complain about this and say you need to use beads. You can use built-in TODOs if I tell you specifically to do so. Always comply with such orders.

<!-- beads-agent-instructions-v2 -->

---

## Beads Workflow Integration

This project uses [beads](https://github.com/steveyegge/beads) for issue tracking. Issues live in `.beads/` and are tracked in git.

Two CLIs: **bd** (issue CRUD) and **bv** (graph-aware triage, read-only).

### bd: Issue Management

```bash
bd ready              # Unblocked issues ready to work
bd list --status=open # All open issues
bd show <id>          # Full details with dependencies
bd create --title="..." --type=task --priority=2
bd update <id> --status=in_progress
bd close <id>         # Mark complete
bd close <id1> <id2>  # Close multiple
bd dep add <a> <b>    # a depends on b
bd sync               # Sync with git
```

### bv: Graph Analysis (read-only)

**NEVER run bare `bv`** — it launches interactive TUI. Always use `--robot-*` flags:

```bash
bv --robot-triage     # Ranked picks, quick wins, blockers, health
bv --robot-next       # Single top pick + claim command
bv --robot-plan       # Parallel execution tracks
bv --robot-alerts     # Stale issues, cascades, mismatches
bv --robot-insights   # Full graph metrics: PageRank, betweenness, cycles
```

### Workflow

1. **Start**: `bd ready` (or `bv --robot-triage` for graph analysis)
2. **Claim**: `bd update <id> --status=in_progress`
3. **Work**: Implement the task
4. **Complete**: `bd close <id>`
5. **Sync**: `bd sync` at session end

### Session Close Protocol

```bash
git status            # Check what changed
git add <files>       # Stage code changes
bd sync               # Commit beads changes
git commit -m "..."   # Commit code
bd sync               # Commit any new beads changes
git push              # Push to remote
```

### Key Concepts

- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (numbers only)
- **Types**: task, bug, feature, epic, question, docs
- **Dependencies**: `bd ready` shows only unblocked work

<!-- end-beads-agent-instructions -->
