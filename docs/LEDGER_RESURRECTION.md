# Ledger Resurrection Audit — `docs/progress/perf-negative-results.md`

**Campaign:** FrankenSuite Performance Domination, 2026-07-25 — Meta-Lever #1.
**Lane:** frankensqlite `cc` / STRUCTURAL (owns this audit per campaign §5).
**Ledger audited:** `docs/progress/perf-negative-results.md` @ `ffda8070`, 20 499 lines, 583 `##` entries.
**Audit tooling:** three mechanical extraction passes, then hand-verification of every
entry that survived to a verdict. Scripts are scratch-only; every VOID verdict below was
read by a human-equivalent pass, not emitted by regex.

---

## Headline: the hypothesis does not hold for this repo, and the reason is on the record

The campaign's §1 hypothesis is that a large fraction of ledger REJECTs are **VOID** — the
measurement could not have detected the lever — with an expected yield around 5 % or better
(frankenlibc measured 39/93 = 42 %).

**frankensqlite's true void rate is 10 / 583 = 1.7 %** (1.9 % before excluding one
non-lever entry). That is roughly a 25× lower void rate than frankenlibc's.

This is not because the audit was run leniently. It is because **this repo already ran this
exact audit, four months before the campaign asked for it, and then strengthened it.** Two
entries dated 2026-07-10 are ledger-integrity self-audits explicitly triggered by the same
frankenmermaid crossing-minimization finding the campaign cites:

- `## 2026-07-10 - AUDIT: the five single-opcode hot-arm pruning rejects are NOT invalidated`
  (line 15930) — *"Requested by the crossing-min ledger-integrity finding: verify each REJECT
  was measured on an input that actually reaches the code under test."*
- `## 2026-07-10 - AUDIT v2 (exact dispatch counts): hot-arm stream matrix DOES reach the
  pruned arms` (line 15955) — supersedes v1 with a **stronger** reachability proof: exact
  per-opcode VDBE dispatch counters instead of sampled self-time, *"strictly stronger for the
  reachability question: they count dispatches rather than estimating them from samples."*

The v1 audit also records the distinction the campaign is trying to teach, in the repo's own
words: *"The crossing-min pathology is a NULL result on an input that misses the code. None of
these five/six are null results."*

**Recommendation to the orchestrator:** frankensqlite is a *source* of the §1 method, not a
target for it. The transferable artifact from this repo is not a resurrection yield — it is
**AUDIT v2's dispatch-counter reachability proof**, which is cheaper and stronger than
sampled self-time and which the other ten repos should copy. See "Method worth porting" below.

---

## What the passes found, and why the first two were wrong

| Pass | Criterion | Flagged | Verdict on the pass |
|---|---|---:|---|
| 1 | Literal §1 criteria (any of: no null control, no binary sha, ratio in null band, ~0 % self-time, cv gate) | 463 / 473 REJECT-ish | **Useless.** "No null control recorded" alone voids 98 % of the ledger and says nothing. |
| 2 | Pass 1 + require the reject to be *measurement*-decided (not correctness/architecture) + ratio-magnitude decidability | 100 | **Still wrong.** Hand-verification of the top-ranked hits found the strongest flags were false positives (below). |
| 3 | Decisive verdict is a **neutral** claim ("within noise" / "no improvement"), **no** null control, and the entry is a **primary lever measurement** (not a screen, audit, summary, or pre-edit kill) | **11** | **Holds up.** Every one hand-read; 10 are genuine, 1 is a non-lever entry. |

### The false positives are the interesting part

Pass 2's four top-ranked "void" entries were all sound, and each failed in an instructive way:

- **L15898 — `opcode_trace_enabled()` caching (`ZERO_SELFTIME`).** The 0.00 % self-time *was
  the entry's own finding*: the prepared DELETE takes the direct BtCursor bypass and never
  enters the VDBE, so the candidate was killed **before any source edit**. The mechanical flag
  fired on a correctly-executed reachability check. This entry is a model of the §1 method, not
  a victim of its absence. It also carries a load-bearing corollary: *"the `vdbe_pipeline_execute`
  stream matrix is the WRONG gate for a write_single DELETE lever"* — gating a DELETE lever there
  would measure a path the workload does not take.
- **L16244 — ephemeral packed same-leaf DELETE atlas (`RATIO_IN_NULL_BAND`).** The
  best-evidenced entry in the ledger. It records **two** SHA-256s (timing binary
  `4e0cc65d…`, symbolized profiling binary `736c1c88…`), the worker (`ovh-a`), six-way arm-order
  rotation, separate warmed fixtures, CV 0.47–4.45 %, and a ledger-integrity proof showing
  non-zero self-time for *both* arms. Its 1.095×/1.103×/1.106× ratios land inside my null band,
  but they are **consistent across three problem sizes with tight CV and rotated arm order** —
  that is a real ~10 % regression, not noise. It exceeds the campaign's §2 contract everywhere
  except the literal `paired(base, base)` call.
- **L4041 — INSERT/DELETE CPU screen.** Not a lever reject at all: *"Touched during this pass:
  no source files."*
- **L11127 — CASS/artifact follow-up.** A summary of older rejects, not a primary measurement.

**Lesson for the other ten repos running this audit: a mechanical void-screen has a high false-positive
rate and must not be reported as a yield.** Flags identify entries to *read*; they do not decide.
Reporting pass-1 or pass-2 numbers as a resurrection yield would have overstated this repo's void
rate by 46× and sent the fleet chasing entries that are already correctly closed.

---

## The VOID queue (10 genuine entries)

Ranked by resurrection value = (decisive claim's distance from a real decision) × (specificity of
the re-run recipe). Self-time ranking per §1 was not usable as the primary key: 7 of 10 record no
self-time for the target frame, which is itself part of why they are void.

| # | Line | Entry | Decisive claim | Why VOID | Re-runnable? |
|---|---:|---|---|---|---|
| **1** | 18529 | 2026-07-12 Table seek-cache MRU refresh short-circuit | median **25.457 → 24.646 µs, −3.19 %**; Criterion `−9.68 %..+0.34 %`, `p=0.07` | Rejected "as within noise" with **no A/A null control**. The point estimate is a 3.2 % *improvement* and the CI is almost entirely negative — it barely crosses zero at the top. "Within noise" was an assumption about the harness, never a measurement of it. | **Yes — best in queue.** Records exact bench, worker (`vmi1264463`), and full command. |
| 2 | 13625 | 2026-05-09 Same-leaf DELETE run search-hint narrowing | nearest ratio 1.65× | Neutral verdict, no null control; DELETE-tail surface, which is the repo's named remaining gap. | Partly — bench named, worker not pinned. |
| 3 | 11422 | 2026-05-05 SmallText direct-byte Eq/Ord/Hash traits | nearest ratio 3.08× | Neutral verdict, no null control. | Partly. |
| 4 | 7626 | 2026-05-07 Direct UPDATE fixed-width REAL leaf-payload patch | within noise | No null control; `write_single` surface. | Partly. |
| 5 | 10126 | 2026-05-06 Direct UPDATE lazy decoded-row scratch borrow | "improved only within noise", ratio ~1.00× | `hyperfine` A/B, 15 runs, no null control; sub-1.01× is undecidable per §2.3. | Yes (hyperfine recipe recorded). |
| 6 | 5730 | 2026-05-09 Same-leaf DELETE monotone duplicate-check skip | within noise | No null control. | Partly. |
| 7 | 8020 | 2026-05-07 Lazy conflict ring-buffer allocation | within noise on the focused probe | **Weakest of the ten** — the full-quick matrix independently moved the wrong way, which is a real signal the neutral focused probe does not undo. | Yes. |
| 8 | 1374 | 2026-05-24 VDBE `Opcode::Int64` hot-dispatch removal | within noise | No null control. Note the 2026-07-10 AUDIT v2 already validated the *sibling* hot-arm rejects by dispatch count; this one was not covered. | Yes — AUDIT v2's dispatch-counter harness applies directly. |
| 9 | 1769 | 2026-05-20 Planner reuse of `rowid_equality_term` for RowidLookup probe | within noise | No null control. | Partly. |
| 10 | 14960 | 2026-05-22 WASM private `init` start-function export removal | within noise | No null control; WASM lane, off the perf-campaign critical path. | Low priority. |

Excluded from the queue after inspection: 20 entries that are screens / audits / summaries /
pre-edit kills rather than primary lever measurements, and 1 entry that **did** record a null
control. One further neutral-verdict entry (L19895) is the `bd-x5gzk` benchmark-integrity
finding, not a lever — resolved separately in commit `ffda8070`.

---

## Re-run status

**#1 (L18529, seek-cache MRU short-circuit) is the only entry in this queue whose re-run is
justified on evidence rather than on completeness**, and it is queued behind the structural
lane's profiling build. Honest constraint, recorded rather than papered over: the original
measurement cost **24 m 18 s + 23 m 31 s of cold remote build** per arm because RCH invalidated
the target graph on each sync. Re-running five entries at that cost is not achievable in one
session, and re-running them *without* the §2 null control would reproduce the exact defect this
audit exists to catch. The remaining nine are ledgered as a standing queue with their recipes.

**No entry in this queue has been re-won yet.** Yield so far: 583 audited / 10 void / 1 queued
for re-run / 0 re-won. That number will be updated in place, not restated elsewhere.

---

## Method worth porting to the other ten repos

From `## 2026-07-10 - AUDIT v2`: when you need to prove a benchmark actually executes the code
under test, **exact dispatch counters beat sampled self-time**. Sampling can fail to obtain a
profile at all (AUDIT v2 was written precisely because sampled self-time was unobtainable), and
a 0-sample frame is ambiguous between "never ran" and "ran below the sampling floor". A counter
incremented in the dispatch path is unambiguous, survives a stripped binary, and costs one
`add` per dispatch. `crates/fsqlite-vdbe/tests/hot_arm_stream_reachability.rs` is the reference
implementation.

Second, from the same entry: **a non-null effect is itself an execution proof.** If removing a
code path makes the benchmark *slower*, that path demonstrably ran. Self-time need not be
re-derived; the effect direction settles it. This cheaply rescues rejects that would otherwise
be flagged void for missing profile data.
