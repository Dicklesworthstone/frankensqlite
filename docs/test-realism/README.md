# Test Realism and Adaptation Inventory

The canonical inventory is generated from the repository's tracked `HEAD`
object set. This makes counts reproducible in a shared worktree: untracked files
and another agent's uncommitted edits cannot silently change the baseline, while
the report still records whether the worktree was dirty.

The scanner classifies tracked test and corpus files as unit, integration,
corpus, fuzz, end-to-end, or tracker-metadata coverage. File-backed execution is
reported as an orthogonal flag rather than a competing class. It also records
in-memory, mock, property-test, `rusqlite`, tracker-shaped, and literal
`.beads/issues.jsonl` usage, plus exact-content duplicate groups.

Do not maintain numeric totals in this README. The generated report is the
authority because test files and direct `#[test]` counts change frequently.

## Reports

One validated report model produces all three views under
`target/test-inventory/`:

| Artifact | Purpose |
|---|---|
| `test_inventory.json` | Complete machine-readable provenance, counts, decisions, ownership, diagnostics, and reproduction metadata |
| `summary.md` | Human-readable baseline reconciliation and Turso decision matrix |
| `test_inventory.csv` | Per-file classification and realism flags for analysis tools |

The JSON and Markdown views include the reviewed Turso testing portfolio pinned
by `docs/contracts/turso_test_adaptation_inventory.toml`. The `audit` command
validates a non-truncated GitHub Git-tree response for that exact commit without
loading or copying upstream source content.

## Commands

```bash
# Offline: validate the contract against tracked HEAD and generate all reports.
./scripts/test_inventory.sh full

# Online: additionally fetch and validate pinned Turso Git-tree metadata.
./scripts/test_inventory.sh audit

# Print the most recently generated Markdown report.
./scripts/test_inventory.sh summary

# Filter the generated CSV to one crate.
./scripts/test_inventory.sh crate fsqlite-core
```

`TURSO_TREE_JSON=/path/to/tree.json` makes `audit` use a previously captured
GitHub tree response. `FSQLITE_TEST_INVENTORY_BIN=/path/to/test_inventory` uses
a prebuilt runner, which is useful in remote-build and clean-checkout CI jobs.

## Drift Policy

Historical values in the contract are comparison baselines, not frozen targets.
Every changed value needs a reviewed, metric-specific explanation. Unknown test
layouts, unknown Turso families, missing provenance, stale owner paths or beads,
unknown feature IDs, and incomplete contract-authority handoffs fail the audit.

The baseline deliberately distinguishes files containing the broad
`issues.jsonl` marker from files containing the literal
`.beads/issues.jsonl` path. It also explains the tracked-`HEAD` differences from
the original exploratory counts that included an ignored E2E scratch file.
