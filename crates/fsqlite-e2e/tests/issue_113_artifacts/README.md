# GitHub #113 — deterministic reproduction (in-transaction `PRAGMA integrity_check`)

Minimal trigger: a `PRAGMA integrity_check` run **inside a live `BEGIN IMMEDIATE`
write transaction**, interleaved with delete+reinsert churn on a table that has
secondary indexes, deterministically reports `database disk image is malformed`
(a freelist page that is simultaneously on the freelist trunk *and* still linked
as a `table t` btree child). The reconciliation happens before COMMIT, so the
post-COMMIT file is clean per both frank and canonical sqlite3.

Root cause site: `connection.rs::with_integrity_txn` (~line 43878) reuses the
*active* transaction when one exists, so the integrity walk observes the
uncommitted, mid-rebalance page state of the live writer. Message emitted at
`connection.rs::record_integrity_page_owner` (~line 44458).

## Recipe (copy-pasteable)

```bash
# 1. Build the CLI (isolated target dir avoids shared-target toolchain races)
cd /data/projects/frankensqlite
CARGO_TARGET_DIR=/data/tmp/issue113-target cargo build -p fsqlite-cli
BIN=/data/tmp/issue113-target/debug/fsqlite

# 2. Generate the minimal repro .sql (deterministic, no RNG)
python3 crates/fsqlite-e2e/tests/issue_113_artifacts/gen_min_repro.sql.py   # -> /tmp/icheck.sql

# 3. Run through fsqlite. FIRES: 12 'database disk image is malformed' lines.
rm -f /tmp/ic.db*
$BIN -batch -init /tmp/icheck.sql /tmp/ic.db < /dev/null
#   -> 'database disk image is malformed: page N was freed earlier in this transaction'
#   -> 'database disk image is malformed: page N is referenced multiple times
#        (freelist trunk[1] leaf[..]; table `t` root -> child[..])'   x11

# 4. Control: move integrity_check OUTSIDE the txn -> all 'ok'
python3 crates/fsqlite-e2e/tests/issue_113_artifacts/gen_control_no_intxn_read.sql.py
$BIN -batch -init /tmp/control_noread.sql /tmp/ctrl.db < /dev/null   # 13x 'ok'
```

## Key findings

- The discriminator is the **in-transaction `PRAGMA integrity_check`**, NOT the
  table-scan `SELECT`. Variant with only forced table scans (`SELECT count(*)
  WHERE +id>=0`, `SELECT max(payload)`) interleaved → all `ok`.
- Index count is NOT the discriminator for this minimal trigger: it fires with a
  single secondary index too (even more violently — bogus billion-leaf freelist
  trunk counts from reading mid-write page bytes).
- Deterministic: byte-identical malformed output across reruns (12/12 cycles).
- Transient: post-COMMIT file is clean (frank + canonical sqlite3 both `ok`,
  index/scan counts agree). This is an in-transaction integrity-walk observing
  a mid-rebalance freelist/btree inconsistency, not a persisted on-disk corruption.
