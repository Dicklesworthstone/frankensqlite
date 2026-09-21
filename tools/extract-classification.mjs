#!/usr/bin/env node
/**
 * Extract CLASS_EARLY / CLASS_MIDDLE / CLASS_LATE from the visualization HTML
 * into a standalone JSON file so the HTML can stop embedding giant data blobs.
 *
 * Usage:
 *   node tools/extract-classification.mjs \
 *     --html site/spec-evolution/visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html \
 *     --out site/spec-evolution/data/spec_evolution_classification_v1.json
 */

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import vm from "node:vm";

const args = process.argv.slice(2);
const flags = {
  html: "site/spec-evolution/visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html",
  out: "site/spec-evolution/data/spec_evolution_classification_v1.json",
};

for (let i = 0; i < args.length; i++) {
  if (args[i] === "--html" && args[i + 1]) {
    flags.html = args[++i];
    continue;
  }
  if (args[i] === "--out" && args[i + 1]) {
    flags.out = args[++i];
    continue;
  }
  if (args[i] === "--help" || args[i] === "-h") {
    console.log(`Usage: node tools/extract-classification.mjs [--html PATH] [--out PATH]

Defaults:
  --html ${flags.html}
  --out  ${flags.out}`);
    process.exit(0);
  }
  console.error(`Unknown argument: ${args[i]}`);
  process.exit(1);
}

function findArrayLiteral(src, marker, nextMarker) {
  const at = src.indexOf(marker);
  if (at < 0) throw new Error(`Marker not found: ${marker}`);
  const eq = src.indexOf("=", at);
  if (eq < 0) throw new Error(`'=' not found after marker: ${marker}`);
  const open = src.indexOf("[", eq);
  if (open < 0) throw new Error(`'[' not found after marker: ${marker}`);

  const limit = nextMarker ? src.indexOf(nextMarker, open) : -1;
  const endLimit = limit > 0 ? limit : src.length;

  // Scan for matching closing ']' while respecting strings.
  let depth = 0;
  let inStr = null; // "'" | '"' | "`"
  let escape = false;
  for (let i = open; i < endLimit; i++) {
    const ch = src[i];
    if (inStr) {
      if (escape) {
        escape = false;
        continue;
      }
      if (ch === "\\") {
        escape = true;
        continue;
      }
      if (ch === inStr) {
        inStr = null;
        continue;
      }
      continue;
    }
    if (ch === "'" || ch === '"' || ch === "`") {
      inStr = ch;
      continue;
    }
    if (ch === "[") depth++;
    if (ch === "]") {
      depth--;
      if (depth === 0) return src.slice(open, i + 1);
    }
  }

  throw new Error(
    `Unterminated array literal after marker: ${marker}` +
      (nextMarker ? ` (nextMarker=${nextMarker})` : ""),
  );
}

function evalArrayLiteral(lit) {
  const context = vm.createContext(Object.create(null));
  // Wrap in parens so it's an expression.
  return vm.runInContext(`(${lit})`, context, { timeout: 2000 });
}

const html = readFileSync(flags.html, "utf-8");

const earlyLit = findArrayLiteral(html, "const CLASS_EARLY", "const CLASS_MIDDLE");
const middleLit = findArrayLiteral(html, "const CLASS_MIDDLE", "const CLASS_LATE");
const lateLit = findArrayLiteral(html, "const CLASS_LATE", "const CSS_CACHE");

const early = evalArrayLiteral(earlyLit);
const middle = evalArrayLiteral(middleLit);
const late = evalArrayLiteral(lateLit);

if (!Array.isArray(early) || !Array.isArray(middle) || !Array.isArray(late)) {
  throw new Error("Expected all CLASS_* values to be arrays");
}

const out = {
  schema_version: 1,
  extracted_from_html: flags.html,
  extracted_at: new Date().toISOString(),
  early,
  middle,
  late,
};

mkdirSync(dirname(flags.out), { recursive: true });
writeFileSync(flags.out, JSON.stringify(out, null, 2) + "\n", "utf-8");
console.log(
  `[extract] Wrote ${flags.out} (early=${early.length}, middle=${middle.length}, late=${late.length})`,
);
