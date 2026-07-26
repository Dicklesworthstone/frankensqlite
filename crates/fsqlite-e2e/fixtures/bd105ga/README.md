# bd-105ga corruption corpus

Two fixtures with very different standing. Read this before using either.

- **`pristine-input.db.gz` — the proven corrupting input.** This exact database
  is the input to a deterministic, currently-reproducing corruption. Keep it.
- **`corrupting-stream.sqllog.gz` — a documented dead end.** Captured from the
  wrong operation; it does not reproduce anything. Kept only as evidence.

## The reproducer (works today)

`br` (beads_rust) dogfoods fsqlite. Against a sandbox holding a copy of
`pristine-input.db` as `.beads/beads.db` plus a full `.beads/`
(`issues.jsonl`, `metadata.json`, `config.yaml`, `last-touched`):

```bash
br sync --merge --force
```

The engine detects the damage mid-operation:

```
Database error: database disk image is malformed:
table rowid seek on root 2 missed scan-visible rowid 986 on successor page 663
```

and stock `sqlite3` confirms it afterwards:

```
Tree 2 page 2 cell 0: Rowid 986 out of order      (run 1 and run 2)
Tree 8 page 8 cell 4: Rowid 3528 out of order     (run 2)
Tree 8 page 8 cell 3: Rowid 2671 out of order     (run 2)
```

Deterministic in fingerprint, **not** in extent — two runs from identical input
produced different amounts of damage. Reproduced 2026-07-26 with `br` 0.2.16
linking fsqlite **0.1.12**.

Two near-misses that do *not* corrupt, and are the reason this went unfound for
a while: `br list --limit 1` (it does a real 3,215-row export flush and leaves
the file `ok`) and plain `br sync --merge` without `--force` (it halts on
semantic conflicts before writing anything).

### Controlled A/B across engine versions

`br` compiles unmodified against both engines (both are the sync API), so the
only variable is the linked engine — same input, same `.beads/`, same command,
same host. To test any sync-API engine revision:

```bash
git worktree add --detach /tmp/fsq-<rev> <rev>          # e.g. v0.1.18, v0.1.19
# in a copy of the br source tree, replace [patch.crates-io] with one line per
# fsqlite crate pointing at /tmp/fsq-<rev>/crates/<crate>, then:
cargo +nightly build --release --bin br
```

Two gotchas worth knowing. First, `br` pins an older toolchain in its own
`rust-toolchain.toml`, which fails to build current `sysinfo`; the `+nightly`
override above is what makes it build. Second, patch every fsqlite crate, not
just `fsqlite-core` — patching one leaves the rest resolving to crates.io at a
different version. This does **not** work for `main`, which is async while `br`
is sync; testing `main` needs an engine-level reproducer built from the merge
workload shape (bd-nhc6g).

| Engine | Merge outcome | DB size after | `integrity_check` |
|---|---|---|---|
| fsqlite **0.1.12** | **aborts** mid-merge with the malformed-image error above | 19 MB → 25 MB (partial) | **corrupt** |
| fsqlite **v0.1.19** tag | **completes**: *"Base snapshot updated. JSONL exported."* | 19 MB → 34 MB | **ok** |

v0.1.19 is not merely avoiding the fault — it finishes strictly more work than
the run that corrupts, and 8 issue rows carry fresh `updated_at` timestamps
afterwards, so this is not a short-circuit.

**This does not prove the balance defect is repaired.** The v0.1.19 diff
contains no `fsqlite-btree` changes at all, so the likelier reading is that a
DML-lowering or execution change stopped *reaching* the faulty balance path —
masked rather than fixed, and able to resurface under a different write shape.
Supportable wording is "v0.1.19 passes the bd-105ga reproducer", not
"v0.1.19 fixes the corruption".

## Why the statement stream is a dead end

`corrupting-stream.sqllog.gz` is an **open-only** capture — br's open-time
export flush, 5,019 of 8,523 logged lines — taken while chasing the `br list`
hypothesis. Replaying it changes nothing anywhere:

| Replay target | Result |
|---|---|
| **v0.1.18 tag tree** (the alleged known-bad version) | 5,019 statements, 0 errors, engine `integrity_check` **ok**, sqlite3 **ok** |
| v0.1.19 tag | same — 0 errors, both **ok** |
| `main` | same — 0 errors, both **ok** |

Because it leaves the known-bad version intact, it is not an oracle, and a
clean replay of it proves nothing. The original bd-105ga conclusion
("corruption bounded to published 0.1.12–0.1.18, fixed on main") rested on
exactly that inference and was **retracted** on 2026-07-26: it compared *br on
0.1.18* (corrupted) against *this replay on main* (clean) and credited the
version for a difference at least as attributable to the harness.

The gap is now understood. The merge writes across `issues`, `dependencies`,
and `labels`, which is why damage appears in Tree 2 *and* Tree 8; an
`export_hashes`-only flush could never produce that. The earlier inference
that rollback-surviving damage implied a WAL/checkpoint page-application defect
was derived from the wrong operation and should be re-derived from the merge
path rather than carried forward.

## Status of the defect

Root cause **unattributed**. The bound to 0.1.12–0.1.18 is unproven, and
whether `main` is affected is untested — `br` is written against the sync API
and `main` is async, so testing `main` needs an engine-level reproducer built
from the merge workload shape. Tracked in bd-105ga (reopened), bd-nhc6g
(reproducer/bisect), bd-7m1ep (this corpus).

Two constraints on future hypotheses: the v0.1.19 squash (v0.1.18 plus one
commit) touches **no** `fsqlite-wal`, `-pager`, `-mvcc`, `-btree` or `-vfs`
source at all; and this database contains **no** WITHOUT ROWID tables, so that
squash's WITHOUT-ROWID REPLACE-victim codegen work cannot explain the damage.

## Files

Fixture bytes are pinned by SHA-256 in `tests/bd_105ga_replay_regression.rs`.
Do not regenerate or recompress them — physical page layout is part of the
stimulus, so a dump-restored database is not the same input.

| File | Contents | SHA-256 of decompressed bytes |
|------|----------|-------------------------------|
| `pristine-input.db.gz` | The exact pre-corruption 18 MB database that `br sync --merge --force` corrupts. | `9bc6c17c69d76db6fd0daa31727c980404d97d3c357800a1c26c4dbfcb58a87b` |
| `corrupting-stream.sqllog.gz` | Open-only capture: 5,019 statements — `BEGIN IMMEDIATE` (line 34), 1,024 `REPLACE INTO export_hashes`, ~3,981 SELECTs, `ROLLBACK` (line 5017). Line format: `SQL \x1f JSON-params`. | `9f41c9da8cd4594df971ca845452b27b9c9496af4b7ddf74822af4fea26895b4` |

Additional durable artifacts — corrupted outputs, the instrumented capture,
rebuild failure traces — are outside the repo at
`/data/tmp/beads-recovery-20260726T042107Z/`.
