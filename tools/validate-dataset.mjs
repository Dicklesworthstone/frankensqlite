#!/usr/bin/env node
/**
 * Dataset validation tool for FrankenSQLite spec evolution visualization (bd-24q.6.2).
 *
 * Validates a spec_evolution_data_v1.json.gz dataset by:
 *   1. Checking commit_count == patch_count
 *   2. Applying patches sequentially from base_doc (matching viz's applyPatchLines)
 *   3. Verifying final snapshot matches the spec file at the dataset's last commit
 *   4. Verifying commit metadata (hash, short, author, date, add/del, subject) against git
 *   5. Producing a clear report and exiting non-zero on failures
 *
 * Usage:
 *   node tools/validate-dataset.mjs [options]
 *
 * Options:
 *   --input PATH       Dataset file (default: spec_evolution_data_v1.json.gz)
 *   --spec-path PATH   Override spec path (default: read from dataset)
 *   --skip-git         Skip git metadata verification (for CI without full history)
 *   --skip-snapshot    Skip final snapshot verification against git
 *   --verbose          Show per-commit progress details
 *   --help             Show this help
 */

import { execSync } from "node:child_process";
import { readFileSync, existsSync } from "node:fs";
import { gunzipSync } from "node:zlib";

// --- CLI arg parsing ---
const args = process.argv.slice(2);
const flags = {
  input: "spec_evolution_data_v1.json.gz",
  specPath: null,
  skipGit: false,
  skipSnapshot: false,
  verbose: false,
  help: false,
};
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--input" && args[i + 1]) { flags.input = args[++i]; continue; }
  if (args[i] === "--spec-path" && args[i + 1]) { flags.specPath = args[++i]; continue; }
  if (args[i] === "--skip-git") { flags.skipGit = true; continue; }
  if (args[i] === "--skip-snapshot") { flags.skipSnapshot = true; continue; }
  if (args[i] === "--verbose") { flags.verbose = true; continue; }
  if (args[i] === "--help" || args[i] === "-h") { flags.help = true; continue; }
  console.error(`Unknown argument: ${args[i]}`);
  process.exit(1);
}

if (flags.help) {
  console.log(`Usage: node tools/validate-dataset.mjs [options]

Options:
  --input PATH       Dataset file (default: ${flags.input})
  --spec-path PATH   Override spec path (default: read from dataset)
  --skip-git         Skip git metadata verification
  --skip-snapshot    Skip final snapshot verification against git
  --verbose          Show per-commit progress details
  --help             Show this help`);
  process.exit(0);
}

// --- Report accumulator ---
const report = { passed: 0, failed: 0, skipped: 0, errors: [] };
function pass(msg) { report.passed++; if (flags.verbose) console.log(`  PASS: ${msg}`); }
function fail(msg) { report.failed++; report.errors.push(msg); console.error(`  FAIL: ${msg}`); }
function skip(msg) { report.skipped++; if (flags.verbose) console.log(`  SKIP: ${msg}`); }

// --- Git helper ---
function git(cmd) {
  return execSync(`git ${cmd}`, { encoding: "utf-8", maxBuffer: 50 * 1024 * 1024 }).trimEnd();
}

// --- Patch application (matches viz's parseUnifiedHunks + applyPatchLines) ---
function parseUnifiedHunks(patch) {
  const lines = String(patch || "").split("\n");
  const hunks = [];
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!line.startsWith("@@")) continue;
    const m = /^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@/.exec(line);
    if (!m) continue;
    const oldStart = Number(m[1]);
    const oldCount = Number(m[2] || "1");
    const newStart = Number(m[3]);
    const newCount = Number(m[4] || "1");
    const hunkLines = [];
    i++;
    for (; i < lines.length; i++) {
      const l = lines[i];
      if (l.startsWith("@@")) { i--; break; }
      if (l.startsWith("diff --git")) break;
      if (l.startsWith("index ") || l.startsWith("---") || l.startsWith("+++")) continue;
      hunkLines.push(l);
    }
    hunks.push({ oldStart, oldCount, newStart, newCount, lines: hunkLines });
  }
  return hunks;
}

function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }

function applyPatchLines(prevLines, patch) {
  const hunks = parseUnifiedHunks(patch);
  const out = prevLines.slice();
  let offset = 0;
  for (const h of hunks) {
    let at = (h.oldStart - 1) + offset;
    at = clamp(at, 0, out.length);
    let cursor = at;
    const next = [];
    for (const hl of h.lines) {
      if (!hl) continue;
      const p = hl[0];
      const content = hl.slice(1);
      if (p === " ") { next.push(content); cursor += 1; }
      else if (p === "-") { cursor += 1; }
      else if (p === "+") { next.push(content); }
    }
    out.splice(at, cursor - at, ...next);
    offset += next.length - (cursor - at);
  }
  return out;
}

/** Count added/deleted lines from a unified diff patch (matches generate-dataset.mjs). */
function countDiffLines(patch) {
  let add = 0;
  let del = 0;
  for (const line of patch.split("\n")) {
    if (line.startsWith("+") && !line.startsWith("+++")) add++;
    if (line.startsWith("-") && !line.startsWith("---")) del++;
  }
  return { add, del, impact: add + del };
}

/** Get file content at a specific commit. */
function fileAt(hash, specPath) {
  try { return git(`show ${hash}:"${specPath}"`); } catch { return null; }
}

// --- Main ---
console.log(`[validate] Input: ${flags.input}`);

if (!existsSync(flags.input)) {
  console.error(`[validate] File not found: ${flags.input}`);
  process.exit(1);
}

const raw = gunzipSync(readFileSync(flags.input));
const data = JSON.parse(raw.toString("utf-8"));
const specPath = flags.specPath || data.spec_path;
console.log(`[validate] Spec path: ${specPath}`);
console.log(`[validate] Schema version: ${data.schema_version}`);
console.log(`[validate] Commits: ${data.commits?.length}, Patches: ${data.patches?.length}`);

// --- Check 1: Schema version ---
if (data.schema_version === 1) {
  pass("schema_version is 1");
} else {
  fail(`schema_version is ${data.schema_version}, expected 1`);
}

// --- Check 2: Required fields ---
for (const field of ["base_commit", "base_doc", "commits", "patches", "generated_from", "spec_path", "schema_version"]) {
  if (data[field] != null) pass(`required field '${field}' present`);
  else fail(`required field '${field}' missing`);
}

// --- Check 3: commit_count == patch_count ---
const commitCount = (data.commits || []).length;
const patchCount = (data.patches || []).length;
if (commitCount === patchCount) {
  pass(`commit_count (${commitCount}) == patch_count (${patchCount})`);
} else {
  fail(`commit_count (${commitCount}) != patch_count (${patchCount})`);
}

// --- Check 4: base_commit matches first commit hash ---
if (data.commits?.length > 0) {
  if (data.base_commit === data.commits[0].hash) {
    pass("base_commit matches first commit hash");
  } else {
    fail(`base_commit '${data.base_commit}' != first commit hash '${data.commits[0].hash}'`);
  }
}

// --- Check 5: Apply patches sequentially ---
// base_doc = content at commit[0]. patches[0] is the diff that created commit[0] from nothing.
// The viz's docTextAtLocal(0) returns base_doc directly and applies patches[1..N-1] for subsequent commits.
// So we skip patches[0] and start applying from patches[1].
console.log("[validate] Applying patches sequentially (patches[1..N-1] on base_doc)...");
let currentLines = String(data.base_doc || "").split("\n");
let patchErrors = 0;
if (flags.verbose) console.log(`  [0] ${data.commits[0]?.short || "?"}: base_doc = ${currentLines.length} lines`);
for (let i = 1; i < patchCount; i++) {
  const patch = data.patches[i];
  try {
    const before = currentLines.length;
    currentLines = applyPatchLines(currentLines, patch);
    const stats = countDiffLines(patch);
    if (flags.verbose) {
      console.log(`  [${i}] ${data.commits[i]?.short || "?"}: ${before} → ${currentLines.length} lines (patch: +${stats.add}/-${stats.del})`);
    }
  } catch (err) {
    patchErrors++;
    fail(`patch[${i}] (${data.commits[i]?.short || "?"}) failed to apply: ${err.message}`);
  }
  if (i % 20 === 0) {
    process.stdout.write(`\r[validate] Applied ${i}/${patchCount - 1} patches`);
  }
}
if (patchCount >= 20) console.log();
if (patchErrors === 0) {
  pass(`all ${patchCount} patches applied successfully`);
} else {
  fail(`${patchErrors}/${patchCount} patches failed to apply`);
}

// --- Check 6: Final snapshot matches spec at last commit ---
if (!flags.skipSnapshot && data.commits?.length > 0) {
  const lastHash = data.commits[data.commits.length - 1].hash;
  console.log(`[validate] Verifying final snapshot against git (${lastHash.slice(0, 8)}...)...`);
  const gitContent = fileAt(lastHash, specPath);
  if (gitContent === null) {
    skip(`cannot retrieve spec at ${lastHash.slice(0, 8)} from git (shallow clone?)`);
  } else {
    const reconstructed = currentLines.join("\n");
    if (reconstructed === gitContent) {
      pass("final snapshot matches spec file at last commit");
    } else {
      // Try with trailing newline normalization
      const normRecon = reconstructed.replace(/\n$/, "");
      const normGit = gitContent.replace(/\n$/, "");
      if (normRecon === normGit) {
        pass("final snapshot matches spec file at last commit (trailing newline normalized)");
      } else {
        const reconLines = reconstructed.split("\n");
        const gitLines = gitContent.split("\n");
        let firstDiff = -1;
        for (let i = 0; i < Math.max(reconLines.length, gitLines.length); i++) {
          if (reconLines[i] !== gitLines[i]) { firstDiff = i + 1; break; }
        }
        fail(`final snapshot mismatch: reconstructed=${reconLines.length} lines, git=${gitLines.length} lines, first diff at line ${firstDiff}`);
      }
    }
  }
} else if (flags.skipSnapshot) {
  skip("final snapshot verification (--skip-snapshot)");
}

// --- Check 7: Commit metadata matches git ---
if (!flags.skipGit && data.commits?.length > 0) {
  console.log("[validate] Verifying commit metadata against git...");
  let metaErrors = 0;
  for (let i = 0; i < data.commits.length; i++) {
    const c = data.commits[i];
    let gitMeta;
    try {
      const raw = git(`log -1 --format="%H|%h|%aI|%an|%s" ${c.hash}`);
      const [hash, short, dateIso, author, ...subjectParts] = raw.split("|");
      gitMeta = { hash, short, dateIso, author, subject: subjectParts.join("|") };
    } catch {
      skip(`commit ${c.short} not reachable in git (shallow clone?)`);
      continue;
    }

    const checks = [
      ["hash", c.hash, gitMeta.hash],
      ["short", c.short, gitMeta.short],
      ["author", c.author, gitMeta.author],
      ["dateIso", c.dateIso, gitMeta.dateIso],
      ["subject", c.subject, gitMeta.subject],
    ];

    for (const [field, got, expected] of checks) {
      if (got === expected) {
        if (flags.verbose) pass(`commit[${i}].${field} matches`);
      } else {
        metaErrors++;
        fail(`commit[${i}].${field}: got '${got}', expected '${expected}'`);
      }
    }

    // Verify add/del/impact from the patch
    const patch = data.patches[i] || "";
    const stats = countDiffLines(patch);
    if (c.add === stats.add && c.del === stats.del && c.impact === stats.impact) {
      if (flags.verbose) pass(`commit[${i}] add/del/impact matches patch`);
    } else {
      metaErrors++;
      fail(`commit[${i}] stats: got +${c.add}/-${c.del}/${c.impact}, patch says +${stats.add}/-${stats.del}/${stats.impact}`);
    }

    if ((i + 1) % 20 === 0) {
      process.stdout.write(`\r[validate] Verified ${i + 1}/${data.commits.length} commits`);
    }
  }
  if (data.commits.length >= 20) console.log();
  if (metaErrors === 0) {
    pass(`all ${data.commits.length} commit metadata entries verified`);
  }
} else if (flags.skipGit) {
  skip("git metadata verification (--skip-git)");
}

// --- Report ---
console.log("\n=== Validation Report ===");
console.log(`  Passed:  ${report.passed}`);
console.log(`  Failed:  ${report.failed}`);
console.log(`  Skipped: ${report.skipped}`);
if (report.errors.length > 0) {
  console.log("\nFailures:");
  for (const err of report.errors) console.log(`  - ${err}`);
}
console.log();

if (report.failed > 0) {
  console.error("[validate] FAILED");
  process.exit(1);
} else {
  console.log("[validate] ALL CHECKS PASSED");
  process.exit(0);
}
