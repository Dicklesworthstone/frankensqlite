# bd-gpi5i mechanism receipt: general recursive-CTE frontier is per-AST-node
# boxed-future evaluation (superserver, 2026-07-28)

## Environment
- Host: superserver (64-core, shared with other agents — minimums used)
- Binary: release-perf (opt-level 3, LTO), built at 951d9381 work tree clean;
  line-tables variant in /data/tmp/cargo-target-dbgline for perf attempts
- comprehensive-bench --filter cte; CLI differential via fsqlite-cli
  release-perf on :memory:, taskset quiet cores, 400 query reps/run × 5 runs

## Baselines (bench, C SQLite vs FrankenSQLite, medians)
- --quick:            C=312.0us  F=1.59ms   5.10x slower (CV 12.4%/0.9%)
- full run 1 (1000):  C=252.7us  F=1.62ms   6.40x slower (CV 1.9%/0.4%)
- full run 2 (1000):  C=257.1us  F=1.58ms   6.13x slower (CV 1.7%/0.6%)
- full run 3 (1000):  C=249.1us  F=1.59ms   6.37x slower (CV 3.2%/0.5%)
- full run 4 (1000):  C=251.7us  F=1.64ms   6.50x slower (CV 2.0%/2.2%)
- Specialized SUM row simultaneously 25.5-32.1x FASTER (closed form) —
  general frontier isolated as the gap.
- Logs: cte-baseline-quick.log, cte-full-run{,2,3}.log rows

## Differential experiment (the decisive receipt; timings.txt)
CLI, 400 reps/script, /usr/bin/time wall, startup baseline 0.00s. Minimums:
- n6_d500   (6 arm nodes,  500 iters): 0.35s -> 0.875ms/query (1.75us/iter)
- n6_d1000  (6 arm nodes, 1000 iters): 0.61s -> 1.53ms/query  (1.53us/iter)
  -> depth scaling LINEAR (1.75x for 2x depth incl. fixed costs); matches
     bench F median. No O(n^2) component.
- n16_d1000 (16 arm nodes, same Direct tier — parenthesized +0 padding
  passes recursive_cte_direct_eval_expr_supported): 1.15s -> 2.88ms/query
  -> two-point fit T_iter = F + k*N: k ~= 135ns/AST-node, F ~= 720ns.
  At the bench shape (N=6): node evaluation ~53%, fixed per-iteration
  async-loop overhead ~47%.
(High-variance outliers on n16 runs 3-5 (3.5-5.5s) = shared-host
contention; minimums used throughout.)

## Mechanism (source-verified at 82762d41)
- materialize_recursive_cte (connection.rs:61975) executes Direct-plan arms
  via execute_recursive_cte_direct_eval_plan (:62297), which evaluates WHERE
  + every projection through eval_expr_with_subqueries (:56302) — signature
  returns Pin<Box<dyn Future>>: one heap-boxed future PER AST NODE PER ROW
  PER ITERATION (~6 boxes/iter at the bench shape, k~135ns/node measured).
- The Direct matcher (recursive_cte_direct_eval_expr_supported, :66898)
  admits NO subquery expressions — the async machinery is unreachable waste
  on this tier.
- The async evaluator's fallthrough arm for simple nodes is literally
  `_ => eval_join_expr(expr, row, col_map)` (:56815) — the sync twin already
  exists; parity for simple nodes is by construction.
- C executes the same frontier at ~250ns/iter (queue-based VM coroutine).

## Perf-tooling negative results (recorded so nobody repeats them)
- Whole-run dwarf record at -F 999: kernel throttled to ~1K samples/2min
  (16KB dumps) — zero coverage of the 20ms --quick CTE window.
- Attach-on-trigger -p PID: ESRCH race at event open.
- Attach-on-trigger -C 8-15: system-wide synthesis reads /proc maps of every
  process (500ms timeout each) — recording started AFTER the 1.9s phase
  ended twice, even with --proc-map-timeout=50.
- --overwrite ring-buffer record: perf.data unparseable on this perf 6.17.13
  ("failed to process type ... [Invalid argument]").
- The CLI node-count differential replaced perf entirely and was decisive.

## Fix
Patch-ready hunk spec on bd-gpi5i (sync evaluation for sync-evaluable Direct
plans + fully-sync iteration loop when all arms qualify). Expected: per-iter
1.53us -> ~0.3-0.6us; query ~1.6ms -> 0.4-0.7ms class vs C 0.25ms.
