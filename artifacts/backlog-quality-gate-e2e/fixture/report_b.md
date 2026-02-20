# Backlog Quality Gate Report

- overall_pass: `true`
- scanned_active_beads: `2`
- scanned_critical_beads: `2`
- total_failures: `1`
- critical_failures: `1`
- regression_failures: `0`
- missing_unit_property_count: `0`
- missing_deterministic_e2e_count: `1`
- missing_structured_logging_count: `1`

## Regression Failures

None.

## All Failures

- `bd-fixture-known` (Known debt) missing: deterministic_e2e, structured_logging
  - remediation: Add deterministic end-to-end scenario requirement with replay instructions and artifact capture.
  - remediation: Add structured logging/metrics requirement including trace/run/scenario identifiers and actionable failure context.
