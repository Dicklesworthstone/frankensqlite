#!/usr/bin/env node
/**
 * Unit tests for dataset tools (bd-24q.6.4).
 *
 * Tests:
 *   1. Patch apply: small fixture series with known outputs
 *   2. Metadata parsing: author/date/subject/add/del from git output format
 *   3. Determinism: same input yields same dataset hash
 *   4. countDiffLines: accurate add/del/impact from unified diff patches
 *   5. computeDatasetHash: stable hash from dataset structure
 *
 * Usage:
 *   node tools/test-dataset.mjs
 *
 * Exit code 0 on success, 1 on any failure.
 */

import { createHash } from "node:crypto";

// --- Functions under test (copied from generate-dataset.mjs / validate-dataset.mjs) ---

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

function countDiffLines(patch) {
  let add = 0;
  let del = 0;
  for (const line of patch.split("\n")) {
    if (line.startsWith("+") && !line.startsWith("+++")) add++;
    if (line.startsWith("-") && !line.startsWith("---")) del++;
  }
  return { add, del, impact: add + del };
}

function computeDatasetHash(data) {
  const commitHashes = (data.commits || []).map((c) => String(c.hash || ""));
  const patchSizes = (data.patches || []).map((p) => String((p || "").length));
  const basis = `${String(data.base_doc || "").length}|${commitHashes.join(",")}|${patchSizes.join(",")}`;
  return createHash("sha256").update(basis).digest("hex");
}

function deterministicJson(data) {
  return JSON.stringify(data, (key, value) => {
    if (value && typeof value === "object" && !Array.isArray(value)) {
      const sorted = {};
      for (const k of Object.keys(value).sort()) sorted[k] = value[k];
      return sorted;
    }
    return value;
  });
}

// --- Test harness ---
let passed = 0;
let failed = 0;

function assert(condition, msg, ctx) {
  if (condition) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg}`);
    if (ctx !== undefined) console.error(`    Context:`, typeof ctx === "string" ? ctx : JSON.stringify(ctx));
  }
}

function assertEq(got, expected, msg) {
  const g = JSON.stringify(got);
  const e = JSON.stringify(expected);
  if (g === e) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg}`);
    console.error(`    Expected: ${e}`);
    console.error(`    Got:      ${g}`);
  }
}

// ============================================================
// 1. Patch Apply Tests
// ============================================================
console.log("--- 1. Patch Apply ---");

// 1a. Simple insertion
{
  const prev = ["a", "b", "c"];
  const patch = "@@ -2,2 +2,3 @@\n b\n+ins\n c\n";
  const result = applyPatchLines(prev, patch);
  assertEq(result, ["a", "b", "ins", "c"], "1a: simple insertion after line 2");
}

// 1b. Simple deletion
{
  const prev = ["a", "b", "c"];
  const patch = "@@ -1,3 +1,2 @@\n a\n-b\n c\n";
  const result = applyPatchLines(prev, patch);
  assertEq(result, ["a", "c"], "1b: delete line 2");
}

// 1c. Replacement
{
  const prev = ["a", "b", "c"];
  const patch = "@@ -2,1 +2,1 @@\n-b\n+B\n";
  const result = applyPatchLines(prev, patch);
  assertEq(result, ["a", "B", "c"], "1c: replace b with B");
}

// 1d. Multi-line insertion from empty
{
  const prev = [];
  const patch = "@@ -0,0 +1,3 @@\n+alpha\n+beta\n+gamma\n";
  const result = applyPatchLines(prev, patch);
  assertEq(result, ["alpha", "beta", "gamma"], "1d: insert into empty array");
}

// 1e. Two hunks in one patch
{
  const prev = ["a", "b", "c", "d", "e"];
  const patch = "@@ -1,2 +1,2 @@\n-a\n+A\n b\n@@ -4,2 +4,2 @@\n-d\n+D\n e\n";
  const result = applyPatchLines(prev, patch);
  assertEq(result, ["A", "b", "c", "D", "e"], "1e: two hunks replacing a->A and d->D");
}

// 1f. Delete all lines
{
  const prev = ["x", "y"];
  const patch = "@@ -1,2 +0,0 @@\n-x\n-y\n";
  const result = applyPatchLines(prev, patch);
  assertEq(result, [], "1f: delete all lines");
}

// 1g. Context-only patch (no changes)
{
  const prev = ["a", "b", "c"];
  const patch = "@@ -1,3 +1,3 @@\n a\n b\n c\n";
  const result = applyPatchLines(prev, patch);
  assertEq(result, ["a", "b", "c"], "1g: context-only patch preserves content");
}

// 1h. Empty patch
{
  const prev = ["a", "b"];
  const result = applyPatchLines(prev, "");
  assertEq(result, ["a", "b"], "1h: empty patch is no-op");
}

// 1i. Patch with diff header lines (should be skipped)
{
  const prev = ["hello", "world"];
  const patch = "diff --git a/file.md b/file.md\nindex abc1234..def5678 100644\n--- a/file.md\n+++ b/file.md\n@@ -1,2 +1,2 @@\n-hello\n+Hello\n world\n";
  const result = applyPatchLines(prev, patch);
  assertEq(result, ["Hello", "world"], "1i: patch with full git diff headers");
}

// 1j. Sequential patches
{
  let lines = ["line1", "line2", "line3"];
  const p1 = "@@ -2,1 +2,1 @@\n-line2\n+LINE2\n";
  const p2 = "@@ -3,1 +3,1 @@\n-line3\n+LINE3\n";
  lines = applyPatchLines(lines, p1);
  lines = applyPatchLines(lines, p2);
  assertEq(lines, ["line1", "LINE2", "LINE3"], "1j: sequential patches");
}

// ============================================================
// 2. countDiffLines Tests
// ============================================================
console.log("--- 2. countDiffLines ---");

{
  const patch = "@@ -1,2 +1,3 @@\n a\n+new\n b\n";
  assertEq(countDiffLines(patch), { add: 1, del: 0, impact: 1 }, "2a: one addition");
}

{
  const patch = "@@ -1,3 +1,2 @@\n a\n-removed\n b\n";
  assertEq(countDiffLines(patch), { add: 0, del: 1, impact: 1 }, "2b: one deletion");
}

{
  const patch = "@@ -1,1 +1,1 @@\n-old\n+new\n";
  assertEq(countDiffLines(patch), { add: 1, del: 1, impact: 2 }, "2c: replace = 1 add + 1 del");
}

{
  assertEq(countDiffLines(""), { add: 0, del: 0, impact: 0 }, "2d: empty patch");
}

{
  const patch = "--- a/file\n+++ b/file\n@@ -1,1 +1,1 @@\n-old\n+new\n";
  assertEq(countDiffLines(patch), { add: 1, del: 1, impact: 2 }, "2e: --- and +++ lines not counted");
}

// ============================================================
// 3. computeDatasetHash Tests
// ============================================================
console.log("--- 3. computeDatasetHash ---");

{
  const data = { base_doc: "hello", commits: [{ hash: "abc" }], patches: ["p1"] };
  const h1 = computeDatasetHash(data);
  const h2 = computeDatasetHash(data);
  assertEq(h1, h2, "3a: same input -> same hash");
  assert(h1.length === 64, "3a: hash is 64 hex chars", h1.length);
}

{
  const d1 = { base_doc: "hello", commits: [{ hash: "abc" }], patches: ["p1"] };
  const d2 = { base_doc: "hello", commits: [{ hash: "def" }], patches: ["p1"] };
  const h1 = computeDatasetHash(d1);
  const h2 = computeDatasetHash(d2);
  assert(h1 !== h2, "3b: different commit hash -> different dataset hash");
}

{
  const d1 = { base_doc: "hello", commits: [{ hash: "abc" }], patches: ["p1"] };
  const d2 = { base_doc: "hello!", commits: [{ hash: "abc" }], patches: ["p1"] };
  const h1 = computeDatasetHash(d1);
  const h2 = computeDatasetHash(d2);
  assert(h1 !== h2, "3c: different base_doc length -> different hash");
}

// ============================================================
// 4. deterministicJson Tests
// ============================================================
console.log("--- 4. deterministicJson ---");

{
  const data = { z: 1, a: 2, m: 3 };
  const json = deterministicJson(data);
  assertEq(json, '{"a":2,"m":3,"z":1}', "4a: top-level keys sorted");
}

{
  const data = { outer: { z: 1, a: 2 } };
  const json = deterministicJson(data);
  assertEq(json, '{"outer":{"a":2,"z":1}}', "4b: nested keys sorted");
}

{
  const data = { arr: [{ b: 2, a: 1 }] };
  const json = deterministicJson(data);
  assertEq(json, '{"arr":[{"a":1,"b":2}]}', "4c: array element keys sorted");
}

{
  const data = { x: 1 };
  const j1 = deterministicJson(data);
  const j2 = deterministicJson(data);
  assertEq(j1, j2, "4d: deterministic across calls");
}

// ============================================================
// 5. parseUnifiedHunks Tests
// ============================================================
console.log("--- 5. parseUnifiedHunks ---");

{
  const hunks = parseUnifiedHunks("@@ -1,3 +1,4 @@\n a\n+new\n b\n c\n");
  assertEq(hunks.length, 1, "5a: single hunk parsed");
  assertEq(hunks[0].oldStart, 1, "5a: oldStart");
  assertEq(hunks[0].oldCount, 3, "5a: oldCount");
  assertEq(hunks[0].newStart, 1, "5a: newStart");
  assertEq(hunks[0].newCount, 4, "5a: newCount");
  assert(hunks[0].lines.length >= 4, "5a: at least 4 hunk lines (trailing empty ok)", hunks[0].lines.length);
}

{
  const hunks = parseUnifiedHunks("@@ -1,1 +1,1 @@\n-a\n+b\n@@ -5,1 +5,1 @@\n-x\n+y\n");
  assertEq(hunks.length, 2, "5b: two hunks parsed");
  assertEq(hunks[0].oldStart, 1, "5b: first hunk start");
  assertEq(hunks[1].oldStart, 5, "5b: second hunk start");
}

{
  const hunks = parseUnifiedHunks("");
  assertEq(hunks.length, 0, "5c: empty patch -> no hunks");
}

{
  const hunks = parseUnifiedHunks("no hunks here\njust text\n");
  assertEq(hunks.length, 0, "5d: no @@ lines -> no hunks");
}

{
  // Single line count (no comma)
  const hunks = parseUnifiedHunks("@@ -5 +5 @@\n-old\n+new\n");
  assertEq(hunks.length, 1, "5e: single-line hunk (no comma)");
  assertEq(hunks[0].oldCount, 1, "5e: oldCount defaults to 1");
  assertEq(hunks[0].newCount, 1, "5e: newCount defaults to 1");
}

// ============================================================
// 6. Metadata parsing (git format string) Tests
// ============================================================
console.log("--- 6. Metadata parsing ---");

{
  // Simulate the git format output: "%H|%h|%aI|%an|%s"
  const raw = "abc123def456|abc123d|2025-01-15T10:30:00+00:00|John Doe|fix: update spec";
  const [hash, short, dateIso, author, ...subjectParts] = raw.split("|");
  const subject = subjectParts.join("|");
  assertEq(hash, "abc123def456", "6a: hash parsed");
  assertEq(short, "abc123d", "6a: short hash parsed");
  assertEq(dateIso, "2025-01-15T10:30:00+00:00", "6a: date parsed");
  assertEq(author, "John Doe", "6a: author parsed");
  assertEq(subject, "fix: update spec", "6a: subject parsed");
}

{
  // Subject containing pipe character
  const raw = "abc|abc|2025-01-01T00:00:00Z|Author|feat: add A | B logic";
  const [hash, short, dateIso, author, ...subjectParts] = raw.split("|");
  const subject = subjectParts.join("|");
  assertEq(subject, "feat: add A | B logic", "6b: subject with pipe preserved");
}

{
  // Empty subject
  const raw = "abc|abc|2025-01-01T00:00:00Z|Author|";
  const [hash, short, dateIso, author, ...subjectParts] = raw.split("|");
  const subject = subjectParts.join("|");
  assertEq(subject, "", "6c: empty subject");
}

// ============================================================
// Report
// ============================================================
console.log(`\n=== Test Report ===`);
console.log(`  Passed: ${passed}`);
console.log(`  Failed: ${failed}`);

if (failed > 0) {
  console.error("\nSOME TESTS FAILED");
  process.exit(1);
} else {
  console.log("\nALL TESTS PASSED");
  process.exit(0);
}
