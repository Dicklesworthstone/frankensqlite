# Differential Manifest (bd-mblr.7.1.2)

run_id: `bd-mblr.7.1.3-expanded-seed-525252`
trace_id: `trace-f58db2700190c178`
scenario_id: `DIFF-CI-713`
commit_sha: `af7fee7b8bf28a68c3cdd3d396b6fa5b981c3ede`
root_seed: `525252`
corpus_entries: `22`
fixture_entries_ingested: `0`
total_cases: `61`
passed: `61`
diverged: `0`
overall_pass: `true`
data_hash: `16d825c4a0bb212f9dd5fab1fb4565174d7da9e7670cde784610b2851f18dda9`

## Replay

`cargo run -p fsqlite-harness --bin differential_manifest_runner -- --workspace-root /data/projects/frankensqlite --run-id bd-mblr.7.1.3-expanded-seed-525252 --trace-id trace-f58db2700190c178 --scenario-id DIFF-CI-713 --root-seed 525252 --max-cases-per-entry 12 --generated-unix-ms 1700000000000 --max-entries 160 --fixtures-dir /data/projects/frankensqlite/conformance --output-json /data/projects/frankensqlite/artifacts/differential-ci/expanded/run-a/differential_manifest.json --output-human /data/projects/frankensqlite/artifacts/differential-ci/expanded/run-a/differential_manifest.md`
