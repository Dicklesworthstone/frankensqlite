# FrankenSQLite Contracts

This directory contains durable, machine-readable project contracts that are
consumed by verification scripts, harness tests, and release/parity reports.
They intentionally live outside the repository root so generated scratch files,
local SQLite databases, benchmark reports, and one-off agent artifacts do not
crowd the project entrypoint.

Keep new contract-style TOML artifacts here unless a toolchain convention
requires a root-level file.

## Test adaptation intake

`turso_test_adaptation_inventory.toml` is the canonical, schema-versioned
clean-room intake contract for `bd-turso-test-adaptation-zu081`. It pins the
reviewed upstream repository, commit, testing-tree object, entry counts, and
license metadata; maps every top-level `testing/` family to an adopt/defer/reject
decision and FrankenSQLite owners; and records the five root-vs-`docs/contracts/`
authority handoffs owned by bead `.18`.

The contract stores metadata and independent design decisions only. It does not
contain copied Turso source or fixtures. Validate it offline against tracked
`HEAD`, or include the pinned upstream Git-tree metadata audit, with:

```bash
./scripts/test_inventory.sh full
./scripts/test_inventory.sh audit
```

Both commands generate JSON, Markdown, and CSV views from one report model under
`target/test-inventory/`. Unknown upstream families, stale owners, unexplained
baseline drift, incomplete authority handoffs, and missing pinned provenance
fail closed.
