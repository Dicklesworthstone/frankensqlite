# fsqlite-vdbe

Virtual Database Engine (VDBE) bytecode interpreter, program builder, register
allocator, coroutine mechanism, and vectorized execution engine for
FrankenSQLite.

## Overview

`fsqlite-vdbe` is the execution backend of FrankenSQLite. It takes a finalized
`VdbeProgram` (produced by the planner's code generator) and runs it
instruction-by-instruction through a register-based virtual machine. The engine
maintains a register file of `SqliteValue` cells, manages cursors over B-tree
storage, and emits result rows.

Key subsystems:

- **Program builder** (`ProgramBuilder`) -- Construct bytecode programs by
  emitting instructions, creating forward-reference labels, and allocating
  registers. Call `finish()` to validate and extract a `VdbeProgram`.
- **Label system** -- Opaque `Label` handles allow jump instructions to be
  emitted before the target address is known. All labels must be resolved before
  execution.
- **Register allocator** (`RegisterAllocator`) -- Sequential allocation starting
  at register 1, with a temporary register reuse pool.
- **Coroutines** (`CoroutineState`) -- Cooperative PC-swap state machines for
  subquery evaluation (NOT async). `InitCoroutine`/`Yield`/`EndCoroutine`
  opcodes drive the protocol.
- **Heap frame stack** (`VdbeFrame`) -- Supports nested trigger/subprogram
  execution without Rust call-stack recursion, enforcing
  `SQLITE_MAX_TRIGGER_DEPTH`.
- **Vectorized execution** -- Columnar batch processing for scans, joins
  (nested-loop and hash), aggregation, sorting, and dispatch. Benchmarks
  included.
- **VDBE engine** (`VdbeEngine`) -- The fetch-execute interpreter. Supports
  expression evaluation, control flow, arithmetic, comparison, row output,
  and cursor-based table operations through `MemDatabase` (in-memory) and
  B-tree backends.

**Position in the dependency graph:**

```
fsqlite-planner (codegen)
  --> fsqlite-vdbe (this crate)
    --> fsqlite-btree (cursor I/O)
    --> fsqlite-pager (page cache)
    --> fsqlite-wal (write-ahead log)
    --> fsqlite-mvcc (concurrency)
    --> fsqlite-func (SQL functions)
```

Dependencies: `fsqlite-types`, `fsqlite-error`, `fsqlite-pager`, `fsqlite-btree`,
`fsqlite-ast`, `fsqlite-mvcc`, `fsqlite-wal`, `fsqlite-func`,
`crossbeam-deque`, `tempfile`.

## Key Types

- `ProgramBuilder` -- Bytecode program under construction. Emits instructions,
  manages labels, allocates registers.
- `VdbeProgram` -- Finalized, immutable bytecode program ready for execution.
- `Label` -- Opaque forward-reference handle for jump targets.
- `RegisterAllocator` -- Sequential register allocator with temp reuse pool.
- `KeyInfo` -- Multi-column key structure for index comparisons (collation and
  sort order per field).
- `SortOrder` -- `Asc` or `Desc` for key comparison direction.
- `CoroutineState` -- Tracks cooperative coroutine execution (yield register,
  saved PC, exhaustion flag).
- `VdbeFrame` -- Heap-allocated execution frame for trigger/subprogram nesting.
- `RaiseResult` -- Result of RAISE() inside trigger bodies (Ignore, Rollback,
  Abort, Fail).
- `VdbeEngine` (in `engine`) -- The bytecode interpreter. Maintains the register
  file, bindings, cursors, and result rows.
- `MemDatabase` / `MemTable` (in `engine`) -- In-memory row store for
  cursor-based operations without the full B-tree stack.

## Usage

```rust
use fsqlite_vdbe::{ProgramBuilder, VdbeProgram, Label};
use fsqlite_types::opcode::{Opcode, P4, VdbeOp};

// Build a simple program
let mut builder = ProgramBuilder::new();
let result_reg = builder.alloc_reg();

// Emit: Integer 42 -> result_reg
builder.emit_op(Opcode::Integer, 42, result_reg, 0, P4::None, 0);
// Emit: ResultRow result_reg, 1
builder.emit_op(Opcode::ResultRow, result_reg, 1, 0, P4::None, 0);
// Emit: Halt
builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

let program: VdbeProgram = builder.finish().expect("all labels resolved");

// Execute via VdbeEngine (requires MemDatabase or B-tree backend)
// use fsqlite_vdbe::engine::{VdbeEngine, MemDatabase};
// let rows = engine.execute(&program)?;
```

## License

MIT (with OpenAI/Anthropic Rider) -- see workspace root LICENSE file.
