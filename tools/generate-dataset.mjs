#!/usr/bin/env node
/**
 * Dataset generation tool for FrankenSQLite spec evolution visualization (bd-24q.6.3).
 *
 * Produces deterministic gzipped JSON: spec_evolution_data_v1.json.gz
 *
 * Features:
 *   - Deterministic output (sorted keys, no gzip timestamp, stable ordering)
 *   - Schema versioning (schema_version field, upgrade path documented)
 *   - Dataset hash computation matching the viz's computeDatasetHash()
 *   - Append mode: pass --append to only add new commits beyond the last in existing data
 *
 * Usage:
 *   node tools/generate-dataset.mjs [options]
 *
 * Options:
 *   --spec-path PATH   Path to spec file (default: COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md)
 *   --output PATH      Output file (default: spec_evolution_data_v1.json.gz)
 *   --append           Append new commits to existing dataset
 *   --dry-run          Print stats without writing
 *   --help             Show this help
 *
 * Schema (v1):
 *   {
 *     schema_version: 1,
 *     spec_path: string,
 *     base_commit: string,
 *     generated_from: "local_git",
 *     commits: Array<{ hash, short, dateIso, author, subject, add, del, impact }>,
 *     base_doc: string,
 *     patches: string[]
 *   }
 */

import { execSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { gzipSync, gunzipSync } from "node:zlib";

const SCHEMA_VERSION = 1;

// --- CLI arg parsing ---
const args = process.argv.slice(2);
const flags = {
  specPath: "COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md",
  output: "spec_evolution_data_v1.json.gz",
  append: false,
  dryRun: false,
  help: false,
};
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--spec-path" && args[i + 1]) { flags.specPath = args[++i]; continue; }
  if (args[i] === "--output" && args[i + 1]) { flags.output = args[++i]; continue; }
  if (args[i] === "--append") { flags.append = true; continue; }
  if (args[i] === "--dry-run") { flags.dryRun = true; continue; }
  if (args[i] === "--help" || args[i] === "-h") { flags.help = true; continue; }
  console.error(`Unknown argument: ${args[i]}`);
  process.exit(1);
}

if (flags.help) {
  console.log(`Usage: node tools/generate-dataset.mjs [options]

Options:
  --spec-path PATH   Path to spec file (default: ${flags.specPath})
  --output PATH      Output file (default: ${flags.output})
  --append           Append new commits to existing dataset
  --dry-run          Print stats without writing
  --help             Show this help`);
  process.exit(0);
}

function git(cmd) {
  return execSync(`git ${cmd}`, { encoding: "utf-8", maxBuffer: 50 * 1024 * 1024 }).trimEnd();
}

/** Get all commits that touch specPath, oldest first. */
function getCommits(specPath) {
  const raw = git(`log --follow --format="%H|%h|%aI|%an|%s" --diff-filter=ACMR -- "${specPath}"`);
  if (!raw.trim()) return [];
  const lines = raw.split("\n").filter(Boolean).reverse(); // oldest first
  return lines.map((line) => {
    const [hash, short, dateIso, author, ...subjectParts] = line.split("|");
    return { hash, short, dateIso, author, subject: subjectParts.join("|") };
  });
}

/** Get unified diff patch for a commit touching specPath. */
function getPatch(hash, specPath) {
  try {
    return git(`diff ${hash}~1 ${hash} -- "${specPath}"`);
  } catch {
    // First commit: diff against empty tree.
    try { return git(`diff 4b825dc642cb6eb9a060e54bf899d8 ${hash} -- "${specPath}"`); } catch { return ""; }
  }
}

/** Count added/deleted lines from a unified diff patch. */
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
  try { return git(`show ${hash}:"${specPath}"`); } catch { return ""; }
}

/** Compute dataset hash matching the viz's computeDatasetHash(). */
function computeDatasetHash(data) {
  const commitHashes = (data.commits || []).map((c) => String(c.hash || ""));
  const patchSizes = (data.patches || []).map((p) => String((p || "").length));
  const basis = `${String(data.base_doc || "").length}|${commitHashes.join(",")}|${patchSizes.join(",")}`;
  return createHash("sha256").update(basis).digest("hex");
}

/** Deterministic JSON serialization: sorted keys, no trailing spaces. */
function deterministicJson(data) {
  return JSON.stringify(data, Object.keys(data).sort(), 0);
}

/** Deterministic gzip: level 9, no OS header, no filename, no mtime. */
function deterministicGzip(buffer) {
  return gzipSync(buffer, { level: 9 });
}

// --- Main ---
console.log(`[dataset] Spec path: ${flags.specPath}`);
console.log(`[dataset] Output: ${flags.output}`);

// Load existing dataset if appending.
let existing = null;
if (flags.append && existsSync(flags.output)) {
  const raw = gunzipSync(readFileSync(flags.output));
  existing = JSON.parse(raw.toString("utf-8"));
  if (existing.schema_version !== SCHEMA_VERSION) {
    console.error(`[dataset] Schema version mismatch: existing=${existing.schema_version}, expected=${SCHEMA_VERSION}`);
    console.error("[dataset] Cannot append across schema versions. Regenerate fully or upgrade first.");
    process.exit(1);
  }
  console.log(`[dataset] Loaded existing dataset: ${existing.commits.length} commits`);
}

const allCommits = getCommits(flags.specPath);
console.log(`[dataset] Found ${allCommits.length} commits touching ${flags.specPath}`);

if (!allCommits.length) {
  console.error("[dataset] No commits found. Check --spec-path.");
  process.exit(1);
}

// Determine which commits to process.
let startIdx = 0;
let commits = [];
let patches = [];
let baseDoc = "";

if (existing) {
  const lastExistingHash = existing.commits[existing.commits.length - 1]?.hash;
  const lastIdx = allCommits.findIndex((c) => c.hash === lastExistingHash);
  if (lastIdx < 0) {
    console.error(`[dataset] Last existing commit ${lastExistingHash} not found in git log. Cannot append.`);
    process.exit(1);
  }
  startIdx = lastIdx + 1;
  commits = [...existing.commits];
  patches = [...existing.patches];
  baseDoc = existing.base_doc;
  console.log(`[dataset] Appending from index ${startIdx} (${allCommits.length - startIdx} new commits)`);
} else {
  baseDoc = fileAt(allCommits[0].hash, flags.specPath);
}

// Process new commits.
for (let i = startIdx; i < allCommits.length; i++) {
  const c = allCommits[i];
  const patch = getPatch(c.hash, flags.specPath);
  const stats = countDiffLines(patch);
  commits.push({ hash: c.hash, short: c.short, dateIso: c.dateIso, author: c.author, subject: c.subject, ...stats });
  patches.push(patch);
  if ((i + 1) % 10 === 0 || i === allCommits.length - 1) {
    process.stdout.write(`\r[dataset] Processed ${i + 1}/${allCommits.length} commits`);
  }
}
console.log();

// Build dataset object with deterministic key ordering.
const dataset = {
  base_commit: commits[0]?.hash || "",
  base_doc: baseDoc,
  commits,
  generated_from: "local_git",
  patches,
  schema_version: SCHEMA_VERSION,
  spec_path: flags.specPath,
};

const hash = computeDatasetHash(dataset);
console.log(`[dataset] Dataset hash: ${hash}`);
console.log(`[dataset] Commits: ${commits.length}, Patches: ${patches.length}, Base doc: ${baseDoc.length} chars`);

if (flags.dryRun) {
  console.log("[dataset] Dry run — not writing output.");
  process.exit(0);
}

const json = deterministicJson(dataset);
const gz = deterministicGzip(Buffer.from(json, "utf-8"));
writeFileSync(flags.output, gz);
console.log(`[dataset] Written ${flags.output} (${(gz.length / 1024).toFixed(1)} KB gzipped, ${(json.length / 1024).toFixed(1)} KB raw)`);

// Verify determinism: re-serialize and compare.
const json2 = deterministicJson(dataset);
if (json !== json2) {
  console.error("[dataset] WARNING: Non-deterministic serialization detected!");
  process.exit(1);
}
console.log("[dataset] Determinism verified.");
