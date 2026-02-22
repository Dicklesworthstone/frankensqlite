# fsqlite-planner

Query planner providing name resolution, WHERE clause analysis, cost-based
index selection, and join ordering for FrankenSQLite.

## Overview

`fsqlite-planner` sits between the parser and the bytecode code generator. It
takes a parsed AST and schema metadata, then decides *how* to execute a query:
which indexes to use, what order to join tables, and whether a covering index
scan is possible.

Core capabilities:

- **Compound SELECT ORDER BY resolution** -- resolves ORDER BY terms across
  UNION/INTERSECT/EXCEPT queries following SQLite's "first SELECT wins" rule.
- **Single-table projection resolution** -- expands `*` and `table.*`, validates
  column references against the schema.
- **Cost model** -- estimates access path costs in page reads using table/index
  statistics (from ANALYZE or heuristic fallback).
- **Index usability analysis** -- determines which WHERE terms can exploit
  available indexes (equality, range, covering).
- **Join ordering** -- bounded beam search (NGQP-style) for multi-table joins,
  with optional DPccp exhaustive search for small join counts and Leapfrog
  Triejoin routing for compatible equi-joins.
- **Statistics** -- equi-depth histograms, NDV estimation, and column statistics
  for cardinality and selectivity estimation.
- **AST-to-VDBE codegen** -- translates SELECT, INSERT, UPDATE, DELETE into VDBE
  register-based bytecode instructions (in the `codegen` submodule).

**Position in the dependency graph:**

```
fsqlite-parser (AST)
  --> fsqlite-planner (this crate)
    --> fsqlite-vdbe (execution)
```

Dependencies: `fsqlite-types`, `fsqlite-error`, `fsqlite-ast`, `blake3`,
`serde`, `serde_json`.

## Key Types

- `QueryPlan` -- Final planner output: join order, access paths per table, join
  segments, and total estimated cost.
- `AccessPath` / `AccessPathKind` -- A concrete scan strategy (full table scan,
  index range scan, index equality scan, covering index scan, rowid lookup) with
  estimated cost and row count.
- `TableStats` / `IndexInfo` -- Schema metadata consumed by the cost model.
  `StatsSource` distinguishes ANALYZE data from heuristic fallbacks.
- `ResolvedCompoundOrderBy` -- A resolved ORDER BY term bound to a column index
  with direction, collation, and nulls ordering.
- `CompoundOrderByError` / `SingleTableProjectionError` -- Error types for
  resolution failures.
- `JoinPlanSegment` / `JoinOperator` -- Join operator decisions emitted by the
  join ordering algorithm.
- `PlannerFeatureFlags` -- Toggles for leapfrog join routing and DPccp
  exhaustive search.
- `Histogram` / `HistogramBucket` (in `stats`) -- Equi-depth histograms for
  selectivity estimation.
- `TableSchema` / `ColumnInfo` / `IndexSchema` (in `codegen`) -- Lightweight
  schema structs consumed by the bytecode code generator.
- `CodegenContext` (in `codegen`) -- Configuration for AST-to-VDBE compilation.

## Usage

```rust
use fsqlite_planner::{
    AccessPath, AccessPathKind, IndexInfo, QueryPlan, TableStats, StatsSource,
    resolve_compound_order_by, resolve_single_table_result_columns,
};

// Resolve ORDER BY for a compound SELECT
// (requires a parsed SelectBody and ORDER BY terms from fsqlite-ast)
// let resolved = resolve_compound_order_by(&body, &order_by_terms)?;

// Resolve column projections for a single-table SELECT
// let columns = resolve_single_table_result_columns(&select_core, &table_cols)?;

// Codegen: compile a parsed SELECT into VDBE bytecode
use fsqlite_planner::codegen::{codegen_select, CodegenContext, TableSchema};
// let program = codegen_select(&select_stmt, &schema, &context)?;
```

## License

MIT (with OpenAI/Anthropic Rider) -- see workspace root LICENSE file.
