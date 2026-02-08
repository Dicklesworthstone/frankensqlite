#!/usr/bin/env node
/**
 * E2E test for dataset tools (bd-24q.6.5).
 *
 * Exercises the full pipeline: generate (dry-run) -> generate -> validate -> snapshot check.
 * Safe: never modifies git state; only reads history and writes a temporary dataset file.
 *
 * Usage:
 *   node tools/test-dataset-e2e.mjs [options]
 *
 * Options:
 *   --dataset PATH   Existing dataset to validate (skips generate, runs validate + snapshot only)
 *   --verbose        Show detailed per-step output
 *   --help           Show this help
 *
 * Exit code 0 on success, 1 on any failure.
 */

import { execSync } from "node:child_process";
import { existsSync, unlinkSync, readFileSync, mkdtempSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { join } from "node:path";
import { tmpdir } from "node:os";

// --- CLI ---
const args = process.argv.slice(2);
const flags = { dataset: null, verbose: false, help: false };
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--dataset" && args[i + 1]) { flags.dataset = args[++i]; continue; }
  if (args[i] === "--verbose") { flags.verbose = true; continue; }
  if (args[i] === "--help" || args[i] === "-h") { flags.help = true; continue; }
  console.error(`Unknown argument: ${args[i]}`);
  process.exit(1);
}

if (flags.help) {
  console.log(`Usage: node tools/test-dataset-e2e.mjs [options]

Options:
  --dataset PATH   Existing dataset to validate (skips generate)
  --verbose        Show detailed per-step output
  --help           Show this help`);
  process.exit(0);
}

// --- Helpers ---
const report = { passed: 0, failed: 0, timings: [] };

function pass(msg) { report.passed++; console.log(`  PASS: ${msg}`); }
function fail(msg) { report.failed++; console.error(`  FAIL: ${msg}`); }

function timed(label, fn) {
  const t0 = performance.now();
  const result = fn();
  const ms = performance.now() - t0;
  report.timings.push({ label, ms });
  if (flags.verbose) console.log(`  [timing] ${label}: ${ms.toFixed(0)}ms`);
  return result;
}

function run(cmd, opts = {}) {
  const { allowFail = false, ...execOpts } = opts;
  try {
    return execSync(cmd, { encoding: "utf-8", maxBuffer: 50 * 1024 * 1024, ...execOpts }).trimEnd();
  } catch (e) {
    if (allowFail) return null;
    throw e;
  }
}

function git(cmd) {
  return run(`git ${cmd}`);
}

// --- Main ---
console.log("=== Dataset E2E Test ===\n");

const specPath = "COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md";
let datasetPath = flags.dataset;
let tempDir = null;
let generatedHere = false;

try {
  // Step 1: Verify spec file exists in git
  console.log("--- Step 1: Environment check ---");
  const specExists = existsSync(specPath);
  if (specExists) pass(`Spec file exists: ${specPath}`);
  else fail(`Spec file not found: ${specPath}`);

  // Check git is available
  const gitVersion = run("git --version", { allowFail: true });
  if (gitVersion) pass(`Git available: ${gitVersion}`);
  else { fail("Git not available"); process.exit(1); }

  // Check tools exist
  for (const tool of ["tools/generate-dataset.mjs", "tools/validate-dataset.mjs", "tools/test-dataset.mjs"]) {
    if (existsSync(tool)) pass(`Tool exists: ${tool}`);
    else fail(`Tool not found: ${tool}`);
  }

  // Step 2: Run unit tests first
  console.log("\n--- Step 2: Unit tests ---");
  const unitResult = timed("unit_tests", () => {
    try {
      const output = run("node tools/test-dataset.mjs");
      if (flags.verbose) console.log(output);
      return output;
    } catch (e) {
      return null;
    }
  });
  if (unitResult !== null && !unitResult.includes("FAILED")) pass("Unit tests passed");
  else fail("Unit tests failed");

  // Step 3: Generate dataset (dry-run first, then real)
  if (!datasetPath) {
    tempDir = mkdtempSync(join(tmpdir(), "fsqlite-dataset-e2e-"));
    datasetPath = join(tempDir, "test_dataset.json.gz");

    console.log("\n--- Step 3a: Generate dataset (dry-run) ---");
    const dryRunOutput = timed("generate_dry_run", () => {
      try {
        const output = run(`node tools/generate-dataset.mjs --spec-path "${specPath}" --output "${datasetPath}" --dry-run`);
        if (flags.verbose) console.log(output);
        return output;
      } catch (e) {
        return null;
      }
    });
    if (dryRunOutput !== null) {
      pass("Dry-run generate succeeded");
      // Verify no file was written
      if (!existsSync(datasetPath)) pass("Dry-run did not write output file");
      else fail("Dry-run wrote output file (should not)");

      // Extract commit count from output
      const countMatch = dryRunOutput.match(/Found (\d+) commits/);
      if (countMatch) {
        const commitCount = parseInt(countMatch[1], 10);
        if (commitCount > 0) pass(`Dry-run found ${commitCount} commits`);
        else fail("Dry-run found 0 commits");
      }
    } else {
      fail("Dry-run generate failed");
    }

    console.log("\n--- Step 3b: Generate dataset (real) ---");
    const genOutput = timed("generate_real", () => {
      try {
        const output = run(`node tools/generate-dataset.mjs --spec-path "${specPath}" --output "${datasetPath}"`);
        if (flags.verbose) console.log(output);
        return output;
      } catch (e) {
        console.error("  Generate error:", e.message);
        return null;
      }
    });
    if (genOutput !== null && existsSync(datasetPath)) {
      pass("Generate succeeded and wrote output file");
      generatedHere = true;

      // Check file size
      const stat = readFileSync(datasetPath);
      const sizeKB = (stat.length / 1024).toFixed(1);
      console.log(`  [info] Dataset size: ${sizeKB} KB (gzipped)`);
      if (stat.length > 100) pass(`Dataset file is non-trivial (${sizeKB} KB)`);
      else fail("Dataset file suspiciously small");

      // Extract hash from output
      const hashMatch = genOutput.match(/Dataset hash: ([a-f0-9]+)/);
      if (hashMatch) {
        pass(`Dataset hash: ${hashMatch[1].slice(0, 16)}...`);
      }

      // Verify determinism output
      if (genOutput.includes("Determinism verified")) pass("Generator reports deterministic output");
      else fail("Generator did not confirm determinism");
    } else {
      fail("Generate failed or did not write output file");
    }
  } else {
    console.log("\n--- Step 3: Using provided dataset ---");
    if (existsSync(datasetPath)) pass(`Dataset exists: ${datasetPath}`);
    else { fail(`Dataset not found: ${datasetPath}`); process.exit(1); }
  }

  // Step 4: Validate dataset
  console.log("\n--- Step 4: Validate dataset ---");
  const validateOutput = timed("validate", () => {
    try {
      const verboseFlag = flags.verbose ? " --verbose" : "";
      const output = run(`node tools/validate-dataset.mjs --input "${datasetPath}" --spec-path "${specPath}"${verboseFlag}`);
      if (flags.verbose) console.log(output);
      return output;
    } catch (e) {
      console.error("  Validate stderr:", e.stderr?.toString() || e.message);
      return null;
    }
  });
  if (validateOutput !== null && validateOutput.includes("ALL CHECKS PASSED")) {
    pass("Validation passed");
    // Extract report numbers
    const passedMatch = validateOutput.match(/Passed:\s+(\d+)/);
    const failedMatch = validateOutput.match(/Failed:\s+(\d+)/);
    if (passedMatch) console.log(`  [info] Validation checks passed: ${passedMatch[1]}`);
    if (failedMatch && failedMatch[1] !== "0") fail(`Validation had ${failedMatch[1]} failures`);
  } else {
    fail("Validation failed");
  }

  // Step 5: Snapshot verification against HEAD
  console.log("\n--- Step 5: Snapshot vs HEAD ---");
  timed("snapshot_check", () => {
    try {
      const raw = gunzipSync(readFileSync(datasetPath));
      const data = JSON.parse(raw.toString("utf-8"));
      const lastHash = data.commits?.[data.commits.length - 1]?.hash;
      if (!lastHash) { fail("No commits in dataset"); return; }

      const commitCount = data.commits.length;
      const patchCount = data.patches.length;
      console.log(`  [info] Commits: ${commitCount}, Patches: ${patchCount}`);
      console.log(`  [info] Last commit: ${lastHash.slice(0, 8)}`);
      console.log(`  [info] Base doc: ${(data.base_doc || "").length} chars`);

      // Patch size statistics
      const patchSizes = data.patches.map(p => (p || "").length);
      const totalPatchBytes = patchSizes.reduce((s, n) => s + n, 0);
      const avgPatch = patchSizes.length ? (totalPatchBytes / patchSizes.length).toFixed(0) : 0;
      const maxPatch = Math.max(...patchSizes);
      const minPatch = Math.min(...patchSizes);
      console.log(`  [info] Patch sizes: total=${(totalPatchBytes / 1024).toFixed(1)}KB, avg=${avgPatch}B, min=${minPatch}B, max=${maxPatch}B`);

      // Check that last commit is reachable
      const specAtLast = run(`git show ${lastHash}:"${data.spec_path || specPath}"`, { allowFail: true });
      if (specAtLast === null) {
        fail(`Cannot retrieve spec at ${lastHash.slice(0, 8)} (shallow clone?)`);
        return;
      }
      pass(`Spec at last commit (${lastHash.slice(0, 8)}) retrieved from git`);

      // Verify HEAD is at or after last dataset commit
      const headHash = git("rev-parse HEAD");
      const isAncestor = run(`git merge-base --is-ancestor ${lastHash} ${headHash}`, { allowFail: true });
      if (isAncestor !== null) {
        pass("Last dataset commit is ancestor of HEAD");
      } else {
        // Not necessarily a failure -- could be different branch
        console.log(`  [warn] Last dataset commit ${lastHash.slice(0, 8)} may not be ancestor of HEAD ${headHash.slice(0, 8)}`);
      }

      // Count how many new commits exist beyond dataset
      const newCommits = run(`git rev-list --count ${lastHash}..HEAD -- "${data.spec_path || specPath}"`, { allowFail: true });
      if (newCommits !== null) {
        const n = parseInt(newCommits, 10);
        if (n === 0) pass("Dataset is up to date (no new commits beyond last)");
        else console.log(`  [info] ${n} new commit(s) beyond dataset's last commit`);
      }

      pass("Snapshot check complete");
    } catch (e) {
      fail(`Snapshot check error: ${e.message}`);
    }
  });

  // Step 6: Append mode test (if we generated)
  if (generatedHere) {
    console.log("\n--- Step 6: Append mode (no-op) ---");
    const appendOutput = timed("append_noop", () => {
      try {
        const output = run(`node tools/generate-dataset.mjs --spec-path "${specPath}" --output "${datasetPath}" --append --dry-run`);
        if (flags.verbose) console.log(output);
        return output;
      } catch (e) {
        return null;
      }
    });
    if (appendOutput !== null) {
      pass("Append dry-run succeeded");
      if (appendOutput.includes("0 new commits")) pass("Append correctly found 0 new commits");
      else console.log("  [info] Append output:", appendOutput.split("\n").pop());
    } else {
      fail("Append dry-run failed");
    }
  }

} finally {
  // Cleanup temporary files
  if (tempDir && existsSync(datasetPath)) {
    try { unlinkSync(datasetPath); } catch { /* ignore */ }
    try {
      const { rmdirSync } = await import("node:fs");
      rmdirSync(tempDir);
    } catch { /* ignore */ }
  }
}

// --- Report ---
console.log("\n=== E2E Test Report ===");
console.log(`  Passed:  ${report.passed}`);
console.log(`  Failed:  ${report.failed}`);
console.log("\nTimings:");
for (const t of report.timings) {
  console.log(`  ${t.label}: ${t.ms.toFixed(0)}ms`);
}
const totalMs = report.timings.reduce((s, t) => s + t.ms, 0);
console.log(`  TOTAL: ${totalMs.toFixed(0)}ms`);

if (report.failed > 0) {
  console.error("\nSOME E2E TESTS FAILED");
  process.exit(1);
} else {
  console.log("\nALL E2E TESTS PASSED");
  process.exit(0);
}
