# Coverage Toolchain for FrankenSQLite

## Decision Record: Coverage Tool Selection

**Status:** Accepted  
**Date:** 2026-02-13  
**Deciders:** SwiftOwl (agent), Project Maintainers

### Context

FrankenSQLite is a 24-crate Rust workspace that requires comprehensive coverage tracking for:
- Line coverage (executed lines / total lines)
- Branch coverage (decision branches taken)
- Function coverage (functions called / total functions)

We evaluated three workspace-safe coverage options:

| Tool | Mechanism | Branch Support | Workspace Support | Speed |
|------|-----------|----------------|-------------------|-------|
| **cargo-llvm-cov** | LLVM instrumentation | Yes (nightly) | Excellent | Fast |
| **grcov** | Requires gcov/llvm-profraw | Yes | Good | Medium |
| **cargo-tarpaulin** | Ptrace-based | Limited | Limited | Slow |

### Decision

**Primary tool: `cargo-llvm-cov`**

Rationale:
1. **LLVM-native:** Uses Rust built-in LLVM instrumentation (`-C instrument-coverage`)
2. **Workspace-safe:** Handles all 24 crates correctly with `--workspace` flag
3. **Branch coverage:** Supported via `--branch` flag on nightly
4. **Output formats:** JSON, LCOV, HTML, Cobertura for CI integration
5. **CI gates:** Built-in `--fail-under-lines` and `--fail-under-functions` flags
6. **No system deps:** No external coverage tools required (unlike grcov)

Why not grcov?
- Requires separate llvm-profraw handling
- More complex setup for branch coverage
- cargo-llvm-cov is a superset of its functionality

Why not tarpaulin?
- Ptrace-based approach is slower and has platform limitations
- Limited branch coverage support
- Known issues with large workspaces

### Consequences

- All coverage workflows use cargo-llvm-cov
- Branch coverage requires nightly toolchain (already our default)
- CI pipelines can use JSON output for thresholds
- Historical reports stored in `target/coverage/`

---

## Quick Start

### Prerequisites

```bash
# Install cargo-llvm-cov (one-time setup)
cargo install cargo-llvm-cov
```

### Basic Commands

```bash
# Full workspace coverage (HTML report)
./scripts/coverage.sh

# Quick summary (text output)
./scripts/coverage.sh --summary

# JSON output (for CI)
./scripts/coverage.sh --json

# Single crate coverage
./scripts/coverage.sh --crate fsqlite-mvcc

# With branch coverage (nightly)
./scripts/coverage.sh --branch

# CI with failure threshold
./scripts/coverage.sh --json --fail-under-lines 70 --fail-under-functions 60
```

### Output Locations

| Format | Location |
|--------|----------|
| HTML | `target/coverage/html_latest/index.html` |
| JSON | `target/coverage/coverage_latest.json` |
| LCOV | `target/coverage/coverage_latest.lcov` |

---

## Manual Commands Reference

For advanced usage or debugging, here are the raw cargo-llvm-cov commands:

### Workspace Coverage

```bash
# Text summary
cargo llvm-cov --workspace --summary-only

# JSON (for parsing)
cargo llvm-cov --workspace --json --output-path coverage.json

# HTML report
cargo llvm-cov --workspace --html --output-dir target/coverage/html

# LCOV format (for codecov.io, coveralls, etc.)
cargo llvm-cov --workspace --lcov --output-path coverage.lcov

# With branch coverage
cargo llvm-cov --workspace --branch --html
```

### Per-Crate Coverage

```bash
# Coverage for a specific crate
cargo llvm-cov -p fsqlite-mvcc --html

# Multiple crates
cargo llvm-cov -p fsqlite-mvcc -p fsqlite-vdbe --summary-only
```

### CI Integration

```bash
# Fail if line coverage < 70%%
cargo llvm-cov --workspace --fail-under-lines 70

# Fail if function coverage < 60%%
cargo llvm-cov --workspace --fail-under-functions 60

# Combined threshold check with JSON output
cargo llvm-cov --workspace --json \
    --fail-under-lines 70 \
    --fail-under-functions 60 \
    --output-path coverage.json
```

---

## Known Caveats

1. **Branch coverage is nightly-only:** The `--branch` flag requires a nightly compiler. Our workspace uses nightly by default.

2. **Cfg(coverage) interaction:** cargo-llvm-cov sets `cfg(coverage)` and `cfg(coverage_nightly)` during builds.

3. **Proc-macro coverage:** When using `--target`, proc-macros and build scripts do not show coverage.

4. **First run is slow:** Initial coverage run recompiles the entire workspace with instrumentation.

5. **Large workspace memory:** With 24 crates, coverage data can be substantial. Ensure adequate memory (4GB+).

---

## Coverage Targets (SLOs)

Initial baseline targets (to be refined after first measurement):

| Metric | Target | Rationale |
|--------|--------|-----------|
| Line coverage | >= 70%% | Industry standard for systems code |
| Function coverage | >= 60%% | Many helper functions may be unused |
| Branch coverage | >= 50%% | Error paths often untested initially |

### Critical Crate Targets

| Crate | Line Target | Priority |
|-------|-------------|----------|
| fsqlite-mvcc | 80%% | Core MVCC innovation |
| fsqlite-vdbe | 75%% | Bytecode VM execution |
| fsqlite-parser | 75%% | SQL parsing correctness |
| fsqlite-btree | 80%% | B-tree integrity |
| fsqlite-wal | 80%% | Crash recovery critical |
| fsqlite-pager | 75%% | Storage correctness |

---

## Troubleshooting

### "LLVM instrumentation not supported"
Run from the repository and confirm rustup selected the exact dated nightly in
`rust-toolchain.toml`:
```bash
rustup show active-toolchain
```

### "No coverage data generated"
Clean and rebuild:
```bash
cargo llvm-cov clean --workspace
cargo llvm-cov --workspace
```

### Slow coverage runs
Use `--no-clean` after the first run:
```bash
cargo llvm-cov --workspace --no-clean --summary-only
```
