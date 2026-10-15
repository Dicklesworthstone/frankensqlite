#!/usr/bin/env python3
"""Measure the unchanged engine, apply the candidate, and certify actual gates.

This script only writes a validated receipt after native correctness, lint,
format, and paired performance gates pass. It never changes a Git ref. Logs,
including baseline failures and all individual samples, are retained on failure.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import math
import os
import pathlib
import re
import shutil
import statistics
import subprocess
import sys

from apply_candidate import CONNECTION, MODULE, apply, blob_hash

TEST = pathlib.Path('crates/fsqlite-core/tests/gh516_insert_batch.rs')
ARTIFACTS = pathlib.Path('artifacts/gh516')
CASES = TEST.with_name('gh516_cases.json')


def run(command: list[str], log: pathlib.Path, *, require: bool = True) -> int:
    print('+', ' '.join(command), flush=True)
    with log.open('w') as output:
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, check=False)
    print(log.read_text(errors='replace')[-12000:], flush=True)
    if require and result.returncode:
        raise RuntimeError(f'gate failed ({result.returncode}): {command}; see {log}')
    return result.returncode


def samples(path: pathlib.Path) -> dict[tuple[bool, int, str], list[float]]:
    data = {}
    for line in path.read_text().splitlines():
        match = re.search(r'GH516_BENCH (\{.*\})', line)
        if not match:
            continue
        record = json.loads(match[1])
        key = (record['wal'], record['batch'], record['kind'])
        values = record['ns_per_row']
        if key in data or len(values) != 7 or any(not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError(f'invalid/duplicate benchmark samples: {key}')
        data[key] = values
    expected = {(w,b,k) for w in (False,True) for b in (8,64,256) for k in ('none','before_false','after_false','after_true')}
    if set(data) != expected:
        raise ValueError(f'incomplete benchmark output: missing={expected-set(data)}, extra={set(data)-expected}')
    return data


def performance(before_path: pathlib.Path, after_path: pathlib.Path) -> dict:
    before, after = samples(before_path), samples(after_path)
    ratios = {key: statistics.median(before[key])/statistics.median(after[key]) for key in before}
    inactive = [key for key in ratios if key[2].endswith('false')]
    raw = math.exp(statistics.mean(math.log(ratios[k]) for k in inactive))
    normalized = math.exp(statistics.mean(math.log(ratios[k]/ratios[(k[0],k[1],'none')]) for k in inactive))
    # Hard performance assertions live in this explicit release experiment,
    # never in the ordinary timing-independent conformance test suite.
    passed = raw >= 1.15 and normalized >= 1.15 and min(ratios.values()) >= 0.90
    return {
        'passed': passed, 'inactive_geomean_speedup': raw,
        'control_normalized_inactive_speedup': normalized,
        'required_geomean_speedup': 1.15, 'minimum_allowed_case_speedup': 0.90,
        'cases': [dict(wal=k[0], batch=k[1], kind=k[2],
                       before_ns_per_row=before[k], after_ns_per_row=after[k],
                       before_median=statistics.median(before[k]),
                       after_median=statistics.median(after[k]), speedup=ratios[k]) for k in sorted(ratios)],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results', type=pathlib.Path, required=True)
    args = parser.parse_args()
    results = args.results.resolve()
    results.mkdir(parents=True, exist_ok=True)
    if (results/'validated.json').exists():
        raise ValueError('refusing to reuse a directory containing a validation receipt')
    root = pathlib.Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip())
    os.chdir(root)
    if subprocess.check_output(['git','status','--porcelain'],text=True).strip():
        raise ValueError('validation requires a clean checkout')
    base = subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
    if TEST.exists():
        raise ValueError(f'refusing to overwrite {TEST}')
    run(['python3',str(ARTIFACTS/'build_scenarios.py'),str(CASES)],results/'stock-corpus.log')
    shutil.copyfile(ARTIFACTS/'gh516_insert_batch.rs', TEST)
    cargo = ['cargo','test','--locked','--release','-p','fsqlite-core','--test','gh516_insert_batch']
    returning = ['cargo','test','--locked','--release','-p','fsqlite-core','--test','gh516_returning_conformance']
    trailing = ['cargo','test','--locked','--release','-p','fsqlite-core','--test','gh516_values_trailing_bindings']
    metadata = {'base_commit':base,'native_gates_passed':False,
                'environment':{k:os.environ.get(k) for k in (
                    'CARGO_BUILD_JOBS','CARGO_PROFILE_RELEASE_LTO',
                    'CARGO_PROFILE_RELEASE_CODEGEN_UNITS','CARGO_PROFILE_RELEASE_DEBUG')},
                'rustc':subprocess.check_output(['rustc','-Vv'],text=True)}
    try:
        # The baseline MUST include the same test/benchmark source. A known
        # conformance failure is retained, never silently converted to a pass.
        metadata['baseline_conformance_exit'] = run(cargo+['differential','--','--nocapture','--test-threads=1'], results/'baseline-conformance.log',require=False)
        metadata['baseline_returning_exit'] = run(returning+['--','--nocapture','--test-threads=1'],results/'baseline-returning.log',require=False)
        metadata['baseline_trailing_bindings_exit'] = run(trailing+['--','--nocapture','--test-threads=1'],results/'baseline-trailing-bindings.log',require=False)
        run(cargo+['release_throughput','--','--ignored','--nocapture','--test-threads=1'],results/'before.log')
        apply(root)
        run(['rustfmt','--edition','2024','--config','skip_children=true',str(CONNECTION),str(MODULE),str(TEST)],results/'format.log')
        # rustfmt is allowed to touch only the three explicitly staged files.
        run(cargo+['--','--nocapture','--test-threads=1'],results/'conformance.log')
        # Include the candidate-only RETURNING dispatch probe only after the
        # exact engine transform. Ordinary baseline conformance stays runnable.
        run(returning+['--','--include-ignored','--nocapture','--test-threads=1'],results/'returning.log')
        run(trailing+['--','--nocapture','--test-threads=1'],results/'trailing-bindings.log')
        run(['cargo','test','--locked','--release','-p','fsqlite-core','--lib','gh516_returning_admission','--','--test-threads=1'],results/'returning-admission.log')
        run(['cargo','test','--locked','--release','-p','fsqlite-core','--test','gh490_fallback_warn_once','--','--test-threads=1'],results/'gh490.log')
        run(['cargo','clippy','--locked','--release','-p','fsqlite-core','--lib','--test','gh516_insert_batch','--test','gh516_returning_conformance','--test','gh516_values_trailing_bindings','--','-D','warnings'],results/'clippy.log')
        run(['rustfmt','--edition','2024','--config','skip_children=true','--check',str(CONNECTION),str(MODULE),str(TEST)],results/'format-check.log')
        run(cargo+['release_throughput','--','--ignored','--nocapture','--test-threads=1'],results/'after.log')
        perf = performance(results/'before.log',results/'after.log')
        (results/'performance.json').write_text(json.dumps(perf,indent=2)+'\n')
        if not perf['passed']:
            raise RuntimeError('native semantics passed but measured performance gate failed; no promotion')
        allowed = {str(CONNECTION),str(MODULE),str(TEST),str(CASES)}
        changed = {line[3:] for line in subprocess.check_output(['git','status','--porcelain','--untracked-files=all'],text=True).splitlines()}
        if changed != allowed:
            raise ValueError(f'unexpected changed paths: {changed ^ allowed}')
        files = []
        for path in sorted(allowed):
            data = pathlib.Path(path).read_bytes()
            files.append({'path':path,'blob':blob_hash(data),'sha256':hashlib.sha256(data).hexdigest()})
        metadata.update(native_gates_passed=True,files=files,
                        performance=perf,
                        gates=['conformance_memory','conformance_wal','differential_parameters',
                               'single_statement_dispatch','returning_conformance','returning_parameter_namespace',
                               'returning_single_statement_dispatch','returning_admission','trailing_values_bindings','gh490','clippy','rustfmt','release_before_after'])
        (results/'validated.json').write_text(json.dumps(metadata,indent=2)+'\n')
    finally:
        (results/'attempt.json').write_text(json.dumps(metadata,indent=2)+'\n')
        with (results/'source.diff').open('w') as out:
            subprocess.run(['git','diff','--',str(CONNECTION)],stdout=out,check=False)
        for path in (MODULE,TEST,CASES):
            if path.exists():
                shutil.copyfile(path,results/path.name)


if __name__ == '__main__':
    main()
