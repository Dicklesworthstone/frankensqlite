# Differential Manifest (bd-mblr.7.1.2)

run_id: `bd-mblr.7.1.3-smoke-seed-424242`
trace_id: `trace-75a51ff6414e95c5`
scenario_id: `DIFF-CI-713`
commit_sha: `af7fee7b8bf28a68c3cdd3d396b6fa5b981c3ede`
root_seed: `424242`
corpus_entries: `22`
fixture_entries_ingested: `0`
total_cases: `52`
passed: `52`
diverged: `0`
overall_pass: `true`
data_hash: `16d825c4a0bb212f9dd5fab1fb4565174d7da9e7670cde784610b2851f18dda9`

## Replay

`cargo run -p fsqlite-harness --bin differential_manifest_runner -- --workspace-root /data/projects/frankensqlite --run-id bd-mblr.7.1.3-smoke-seed-424242 --trace-id trace-75a51ff6414e95c5 --scenario-id DIFF-CI-713 --root-seed 424242 --max-cases-per-entry 4 --generated-unix-ms 1700000000000 --max-entries 32 --fixtures-dir /data/projects/frankensqlite/conformance --output-json /data/projects/frankensqlite/artifacts/differential-ci/smoke/run-a/differential_manifest.json --output-human /data/projects/frankensqlite/artifacts/differential-ci/smoke/run-a/differential_manifest.md`
