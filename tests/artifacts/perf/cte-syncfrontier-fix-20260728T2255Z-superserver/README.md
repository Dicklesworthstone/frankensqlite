# bd-gpi5i fix receipt: sync Direct frontier lane (superserver, 2026-07-28)

## Change
connection.rs: recursive-CTE Direct-plan arms whose expressions are all in a
conservative sync node set (Column/Literal/BinaryOp/UnaryOp/IsNull/numbered
Placeholder) evaluate through a new synchronous evaluator
(eval_recursive_cte_direct_expr_sync — arm bodies copied verbatim from the
async evaluator, same eval_join_binary_op terminal) and are dispatched as
plain function calls from the materialize_recursive_cte iteration loop: no
Pin<Box<dyn Future>> per AST node, no per-iteration future materialization.
Affinity/collation/func-registry-sensitive nodes (Between/Case/Like/In/Cast/
Collate/FunctionCall) stay on the async path by design.

## Receipt (comprehensive-bench --quick --filter cte, taskset, same host)
- BEFORE (cte-baseline.log, pre-fix build @951d9381 class):
  Recursive CTE general COUNT (1..1000): C=312.0us F=1.59ms  5.10x slower
- AFTER  (cte-after-fix.log, this change):
  Recursive CTE general COUNT (1..1000): C=265.7us F=1.01ms  3.82x slower (CV 1.8%/0.4%)
- Specialized SUM row unchanged-fast (10.9us, 26.7x FASTER) — no regression.
- Net: 0.58ms/query recovered (~36%); consistent with eliminating the
  ~135ns/node boxed-future term measured in
  cte-frontier-profile-20260728T1810Z-superserver.

## Residual (honest accounting; follow-up candidates, NOT this change)
~750ns/iter remains vs C's ~270ns: per-iteration frontier bookkeeping
(all_rows extend + working_set swap + Row/Vec allocs) + final temp-table
materialization + outer aggregate scan. NOTE: do NOT close this by
specializing COUNT into a closed form — that re-blinds the benchmark row to
the general path (the bd-czzlp bench-truth asymmetry is intentional).
Legitimate follow-up: allocation reduction in the general loop itself.

## Gates
cargo check/clippy fsqlite-core --lib clean; fmt applied; 4/4 integration
tests pass incl. 9-arm cross-engine rusqlite oracle parity (sync + CAST-forced
async lanes) and placeholder rebinds; inline counter tests
(test_recursive_cte_general_count_uses_sync_direct_eval) pass — see commit.
