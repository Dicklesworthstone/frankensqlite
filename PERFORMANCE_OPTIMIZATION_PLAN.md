# FrankenSQLite Performance Optimization Plan

> Profile-driven plan to close the single-threaded performance gap against C SQLite.
> Current state: 13-250x slower depending on workload. Target: <5x for common patterns.

## Executive Summary

The benchmark reveals FrankenSQLite is 13-250x slower than C SQLite for single-threaded
operations. Root cause analysis identifies **five dominant bottlenecks** that together
account for >95% of the gap:

1. **Statement re-parsing** (est. 30-40% of gap): SQL is tokenized + parsed even on cache hits due to full AST cloning
2. **Bytecode re-compilation** (est. 20-25%): VDBE programs cloned from cache; no plan cache exists
3. **B-tree seek inefficiency** (est. 15-20%): PK lookups 1000x+ slower at 10K rows suggests codegen routing issues
4. **Per-query allocations** (est. 10-15%): format!() SQL construction, Vec/String allocations on hot paths
5. **VDBE interpreter overhead** (est. 5-10%): Large match dispatch, no threaded interpretation

---

## Phase 0: Measurement Infrastructure (Week 1)

Before optimizing, instrument the hot paths to get per-component timing.

### 0.1 Add flamegraph-compatible profiling markers

```
File: crates/fsqlite-core/src/connection.rs
```

Add `#[inline(never)]` sentinel functions around each pipeline stage so that
`perf record` / `flamegraph` can attribute time:

- `_profile_parse(sql)` wrapping `cached_parse_single/multi`
- `_profile_compile(stmt)` wrapping `compile_with_cache`
- `_profile_plan(stmt)` wrapping planner invocation
- `_profile_execute(program)` wrapping VDBE execution
- `_profile_btree_seek(rowid)` wrapping cursor seeks

### 0.2 Add micro-benchmarks for each stage

Create `crates/fsqlite-e2e/benches/pipeline_stage_bench.rs` that benchmarks:
- Parse only (tokenize + parse, no execution)
- Compile only (AST to VDBE, no execution)
- Execute only (pre-compiled program, just run)
- B-tree seek only (cursor.seek to known rowid in pre-built tree)

This lets us track regressions per-stage as we optimize.

---

## Phase 1: Statement Cache Overhaul (Est. 3-5x speedup)

**The single highest-impact change.** Current caches are fundamentally broken:
- Parse cache: 256 entries, full-clear eviction (no LRU)
- Compiled cache: 128 entries, full-clear eviction
- Both clone their contents on every hit
- No query plan cache at all

### 1.1 Replace full-clear eviction with LRU

```
File: crates/fsqlite-core/src/connection.rs (~lines 1448-1456, 2267-2383)
```

Replace `HashMap<u64, ParseCacheEntry>` with a proper LRU cache:

- Use `hashlink::LinkedHashMap` (already a transitive dep via rusqlite) or a simple
  hand-rolled LRU with `Vec<(u64, Entry)>` + HashMap index
- Evict least-recently-used entry when at capacity, not the entire cache
- Increase parse cache to 512 entries, compiled cache to 256

**Expected impact:** Eliminates cache thrashing for workloads with >128 unique queries.
Currently, query 257 causes ALL 256 cached entries to be discarded.

### 1.2 Arc-wrap cached entries to eliminate cloning

```
File: crates/fsqlite-core/src/connection.rs
```

Current: `entry.statement.clone()` on every cache hit (full AST deep clone)
Current: `program.clone()` on every cache hit (full bytecode deep clone)

Change cache value types:
```rust
// Before
parse_cache: RefCell<HashMap<u64, ParseCacheEntry>>,
// where ParseCacheEntry { sql: String, statement: Statement }

// After
parse_cache: RefCell<LruCache<u64, Arc<ParseCacheEntry>>>,
// Cache hit returns Arc::clone() — O(1) refcount bump, not O(N) deep clone
```

Same for compiled cache:
```rust
compiled_cache: RefCell<LruCache<u64, Arc<VdbeProgram>>>,
```

**Expected impact:** Cache hits go from O(size_of_AST) to O(1). For a medium query
with 6-column INSERT, the AST is ~500 bytes; for complex SELECTs with JOINs, 2-5KB.
This alone may account for 2-3x of the gap.

### 1.3 Add query plan cache

```
File: crates/fsqlite-planner/src/lib.rs
New: crates/fsqlite-planner/src/plan_cache.rs (if needed)
```

The planner currently recomputes the full query plan on every execution.
For SELECT queries with multiple tables, this is O(2^N) cost model evaluation.

Add a plan cache keyed on (SQL hash, schema_cookie):
```rust
plan_cache: RefCell<LruCache<u64, Arc<QueryPlan>>>,
```

Invalidate on schema changes (DDL statements increment schema_cookie).

**Expected impact:** Complex SELECT queries with JOINs see 5-50x improvement in
planning time. Simple queries see 1.2-1.5x from avoiding plan recomputation.

### 1.4 Intern SQL strings in cache keys

Current: `sql.to_owned()` allocates a fresh String for every cache insertion.

Replace with a string interner or use the hash directly as the key with collision
checking via Arc<str> shared reference:
```rust
// Key: u64 hash
// Value: (Arc<str>, Arc<CachedItem>)
// Collision check: Arc::ptr_eq or Arc<str> comparison
```

**Expected impact:** Eliminates ~2 String allocations per cache miss.

---

## Phase 2: Eliminate Per-Query format!() Allocations (Est. 1.5-3x speedup)

The benchmark shows FrankenSQLite using `format!()` to construct SQL strings:
```rust
conn.execute(&format!("INSERT INTO bench VALUES ({i}, 'name_{i}', {})", i * 7))
```

While this is a benchmark artifact, it represents a real-world pattern. The fix is
two-fold: make prepared statements work well, AND optimize the raw SQL path.

### 2.1 Make prepared statements the fast path

```
File: crates/fsqlite-core/src/connection.rs (~lines 2218-2250)
```

Current `execute_prepared_with_params()` still calls `execute_statement()` which
goes through `compile_with_cache()`. The prepared statement should hold a pre-compiled
`Arc<VdbeProgram>` that is re-executed with new parameter bindings without any
re-compilation.

```rust
pub struct PreparedStatement<'conn> {
    program: Arc<VdbeProgram>,       // Pre-compiled, reusable
    param_slots: Vec<RegisterId>,    // Where to bind params
    // ... connection reference for execution context
}

// execute_prepared_with_params just:
// 1. Binds params into registers
// 2. Calls vdbe_execute(program, registers)
// No parsing, no planning, no compilation
```

**Expected impact:** Prepared statement execution should be <2x C SQLite (vs current 30-50x).

### 2.2 Add parameterized query fast-path

```
File: crates/fsqlite-core/src/connection.rs
```

Add a method that accepts SQL with `?` placeholders and params directly:
```rust
conn.execute_params("INSERT INTO bench VALUES (?, ?, ?)", &[
    SqliteValue::Integer(i),
    SqliteValue::Text(format!("name_{i}")),
    SqliteValue::Integer(i * 7),
])
```

This hits the cache on the template SQL (which is always the same string),
retrieves the `Arc<VdbeProgram>`, and binds params without any allocation
beyond the param values themselves.

### 2.3 SmallVec for register file

```
File: crates/fsqlite-vdbe/src/engine.rs
```

The VDBE register file is currently `Vec<SqliteValue>`. Most queries use <32
registers. Use `SmallVec<[SqliteValue; 32]>` to avoid heap allocation for
typical queries.

Similarly, use `SmallVec<[VdbeOp; 64]>` for the instruction buffer in
ProgramBuilder — most simple queries compile to <64 opcodes.

---

## Phase 3: B-Tree Seek Path Fix (Est. 2-10x for point lookups)

The benchmark shows PK lookups degrading catastrophically:
- 100 rows: 34x slower (11.9us vs 351ns)
- 1K rows: 220x slower (79.5us vs 361ns)
- 10K rows: 1840x slower (756.2us vs 411ns)

C SQLite stays ~350-400ns regardless of table size (log N B-tree). FrankenSQLite
grows linearly, suggesting a scan rather than seek.

### 3.1 Audit VDBE codegen for SELECT WHERE id = ?

```
File: crates/fsqlite-vdbe/src/codegen.rs (or wherever SELECT codegen lives)
```

Verify that `SELECT * FROM t WHERE id = <literal>` generates:
```
OpenRead cursor_0, root_page, num_cols
SeekEq cursor_0, label_not_found, register_with_id
Column cursor_0, 0..N, result_registers
ResultRow ...
label_not_found:
Close cursor_0
```

NOT:
```
OpenRead cursor_0, root_page, num_cols
Rewind cursor_0, label_end
label_loop:
Column cursor_0, 0, reg_id
Eq reg_id, reg_target, label_match
Next cursor_0, label_loop
```

If it's generating a scan, fix the codegen to emit SeekEq/SeekGe for equality
predicates on indexed columns (especially the implicit rowid PK index).

### 3.2 Verify cursor.seek uses B-tree binary search

```
File: crates/fsqlite-btree/src/cursor.rs (~lines 686-850)
```

The explore agent confirms binary search IS implemented correctly in the B-tree.
But the question is whether VDBE's SeekEq opcode actually calls the binary search
path, or whether it does something else (like sequential Next until match).

Check `engine.rs` SeekEq/SeekGe opcode handlers to verify they call
`cursor.table_seek()` (the binary search path) rather than scanning.

### 3.3 Page cache efficiency for B-tree traversal

```
File: crates/fsqlite-pager/src/lib.rs
```

Each B-tree node access requires a page fetch. Verify:
- Pages are cached in memory (not re-read from VFS on every access)
- The pager uses direct HashMap lookup, not linear scan
- Hot pages (root, first-level interior nodes) are pinned

---

## Phase 4: VDBE Interpreter Optimization (Est. 1.3-2x speedup)

### 4.1 Computed goto / indirect threading

```
File: crates/fsqlite-vdbe/src/engine.rs (~line 2822)
```

The current VDBE loop is:
```rust
loop {
    let op = &program.ops[pc];
    match op.opcode {
        Opcode::Add => { ... }
        Opcode::Subtract => { ... }
        // ... 200+ arms
    }
    pc += 1;
}
```

This is a classic interpreter bottleneck — the CPU branch predictor sees ONE
indirect branch (the match) that has 200+ targets, causing ~50% misprediction.

**Option A: Token threading (safe Rust)**
```rust
// Pre-compute dispatch table
type Handler = fn(&mut VdbeState, &VdbeOp) -> ControlFlow;
static DISPATCH: [Handler; 256] = [handle_add, handle_sub, ...];

loop {
    let op = &program.ops[pc];
    match DISPATCH[op.opcode as usize](&mut state, op) {
        ControlFlow::Continue => pc += 1,
        ControlFlow::Jump(target) => pc = target,
        ControlFlow::Halt => break,
    }
}
```

This replaces a match with a function pointer call — slightly better for branch
prediction since the call target varies per-instruction rather than being one
mega-switch.

**Option B: Opcode fusion**

Identify common opcode sequences and fuse them into super-instructions:
- `Column + ResultRow` → `ColumnResultRow`
- `Integer + Eq + If` → `IntegerEqIf` (constant comparison + conditional jump)
- `Column + Compare + Jump` → `FilterColumn` (single comparison scan step)

C SQLite does this extensively. Each fusion eliminates 1-2 dispatch overheads.

### 4.2 Register file: avoid SqliteValue enum overhead

```
File: crates/fsqlite-types/src/value.rs
```

`SqliteValue` is an enum with 5 variants. Each register access requires matching
on the discriminant. For arithmetic-heavy bytecode (SUM, COUNT), this adds overhead.

Consider a NaN-boxed representation for the register file where Integer and Float
values are stored inline (no enum discriminant check for the common case):
```rust
// 8 bytes: if top 13 bits are 0x7FF8, it's a tagged pointer (Text/Blob/Null)
// Otherwise it's a raw f64 or i64 (distinguished by tag bits)
struct NanBoxedValue(u64);
```

This is an advanced optimization — only pursue after Phases 1-3 are done.

---

## Phase 5: Allocation Reduction (Est. 1.2-1.5x speedup)

### 5.1 Arena allocator for AST nodes

```
File: crates/fsqlite-ast/src/lib.rs
File: crates/fsqlite-parser/src/parser.rs
```

The parser allocates each AST node (Expr, Select, Insert, etc.) individually on
the heap via Box. A per-parse arena allocator would batch these into a single
allocation:

```rust
struct ParseArena {
    chunks: Vec<Vec<u8>>,
    current_offset: usize,
}
```

All AST nodes allocated from the arena; the arena is freed in one shot when the
parse cache entry is evicted. This eliminates ~50-200 individual allocations per
parse.

### 5.2 Avoid Vec re-allocation in result collection

```
File: crates/fsqlite-core/src/connection.rs (query methods)
```

`conn.query()` returns `Vec<Row>`. If the query has a LIMIT clause, pre-allocate
the Vec to the limit size. For non-LIMIT queries, use a growth strategy that
avoids excessive re-allocation (start at 64, double).

### 5.3 String interning for column names and table names

```
File: crates/fsqlite-core/src/connection.rs
File: crates/fsqlite-btree/src/lib.rs
```

Column names like "id", "name", "score" are allocated as fresh Strings on every
query result. Intern these in a per-connection string table and return &str
references.

---

## Phase 6: Speculative / Advanced (Post-MVP)

### 6.1 JIT compilation for hot bytecode

The VDBE engine.rs already has a JIT stub (line 2788-2794). Implement a simple
copy-and-patch JIT using Cranelift or hand-rolled x86-64:
- Only JIT programs executed >100 times
- Fall back to interpreter on JIT failure
- Expected 3-10x speedup for tight loops (aggregate scans, bulk inserts)

### 6.2 Vectorized execution for scans

For full table scans and aggregates, process rows in batches of 1024 rather
than one at a time. This enables:
- SIMD-accelerated comparison and arithmetic
- Better cache locality (column-at-a-time processing)
- Reduced VDBE dispatch overhead (one dispatch per batch, not per row)

### 6.3 Compiled prepared statements

For prepared statements executed >10 times, generate specialized Rust closures
at runtime that encode the entire query plan as native code:
```rust
// Instead of interpreting VDBE bytecode:
let fast_insert = |params: &[SqliteValue]| -> Result<usize> {
    let page = pager.get_page(root_page)?;
    let cursor = btree.seek_for_insert(page, params[0].as_i64()?)?;
    let cell = encode_cell(&params)?;
    btree.insert_at(cursor, cell)?;
    Ok(1)
};
```

---

## Priority Order & Expected Cumulative Impact

| Phase | Change | Estimated Speedup | Cumulative |
|-------|--------|-------------------|------------|
| 1.2 | Arc-wrap cache entries (no clone) | 2-3x | 2-3x |
| 1.1 | LRU eviction (no full-clear) | 1.5-2x | 3-6x |
| 3.1 | Fix B-tree seek codegen | 2-10x (lookups) | 5-15x (lookups) |
| 2.1 | True prepared statements | 2-5x (repeated queries) | 5-15x |
| 1.3 | Query plan cache | 1.2-2x (complex queries) | 6-18x |
| 4.1 | Token threading VDBE | 1.3-1.5x | 8-25x |
| 5.1 | Arena allocator for AST | 1.1-1.3x | 9-30x |
| 2.3 | SmallVec registers | 1.1-1.2x | 10-35x |

**Conservative target after Phases 1-3:** 5-10x faster (from ~50x slower to ~5-10x slower)
**Optimistic target after Phases 1-5:** 15-30x faster (from ~50x to ~2-3x slower)
**With Phase 6 (JIT/vectorized):** Potential parity or faster for specific workloads

---

## Measurement Gates

Each phase must be validated with the comprehensive benchmark before proceeding:

```bash
# Build and run benchmark with HTML report
CARGO_TARGET_DIR=target-bench cargo run --profile release-perf \
  -p fsqlite-e2e --bin comprehensive-bench -- \
  --html benchmark_report_phase_N.html
```

**Gate criteria for each phase:**
- No correctness regressions (all conformance tests pass)
- Measurable improvement on at least one benchmark scenario
- No performance regression on any other scenario >10%
- Clippy + fmt clean

---

## Risk Analysis

| Risk | Mitigation |
|------|------------|
| Arc overhead for tiny queries | Profile: if Arc refcount > cache clone, revert |
| LRU eviction complexity | Use hashlink (already in dep tree) — battle-tested |
| B-tree codegen change breaks correctness | Run full conformance suite before/after |
| NaN-boxing complexity | Only pursue if Phases 1-3 don't close the gap |
| JIT security implications | Cranelift has no known sandbox escapes; memory-map W^X pages |
| Concurrent mode interaction | All caches are per-connection (RefCell), no cross-thread sharing |
