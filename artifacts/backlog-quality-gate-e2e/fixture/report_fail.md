# Backlog Quality Gate Report

- overall_pass: `false`
- scanned_active_beads: `2`
- scanned_critical_beads: `2`
- total_failures: `2`
- critical_failures: `2`
- regression_failures: `1`
- missing_unit_property_count: `0`
- missing_deterministic_e2e_count: `2`
- missing_structured_logging_count: `2`

## Regression Failures

- `bd-fixture-new` (P1 critical) missing: deterministic_e2e, structured_logging

## All Failures

- `bd-fixture-known` (Known debt) missing: deterministic_e2e, structured_logging
  - remediation: Add deterministic end-to-end scenario requirement with replay instructions and artifact capture.
  - remediation: Add structured logging/metrics requirement including trace/run/scenario identifiers and actionable failure context.
- `bd-fixture-new` (New regression) missing: deterministic_e2e, structured_logging
  - remediation: Add deterministic end-to-end scenario requirement with replay instructions and artifact capture.
  - remediation: Add structured logging/metrics requirement including trace/run/scenario identifiers and actionable failure context.
