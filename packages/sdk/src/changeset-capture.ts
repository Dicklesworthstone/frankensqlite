import { encodeChangeset } from "./changeset-codec";
import type { ChangesetChange, ChangesetField, ChangesetLimits, ChangesetTable, ChangesetValue } from "./changeset-codec";
import type { ChangesetExecutor, ChangesetTarget } from "./changeset-apply";

export interface CaptureChangesetOptions {
  /** Existing ordinary main tables with declared primary keys; at most 64. */
  tables: readonly string[];
  /** Distinct first-touch keys retained in TEMP, including net-zero changes. */
  maxRows?: number;
  /** Accounted journal + collected row-image bytes, not heap/RSS. Default 8 MiB. */
  maxBytes?: number;
  /** Journal + collected row-image slots. Default 100,000. */
  maxCells?: number;
  /** Bounds the encoded changeset independently of the first-touch journal. */
  limits?: ChangesetLimits;
  /** Whole-scope flag. SQL triggers cannot infer native preupdate-hook depth. */
  indirect?: boolean;
  signal?: AbortSignal;
  timeoutMs?: number;
}
export interface CapturedChangeset<T> {
  readonly value: T;
  readonly changeset: Uint8Array;
  readonly changes: number;
  readonly touchedRows: number;
}
export class ChangesetCaptureError extends Error {
  constructor(readonly code: "ERR_FSQLITE_CAPTURE_INPUT" | "ERR_FSQLITE_CAPTURE_SCHEMA" |
    "ERR_FSQLITE_CAPTURE_RESULT" | "ERR_FSQLITE_CAPTURE_CANCELLED" | "ERR_FSQLITE_CAPTURE_TIMEOUT",
    message: string, options?: ErrorOptions) {
    super(message, options); this.name = "ChangesetCaptureError";
  }
}

const PREFIX = "__fsqlite_capture_";
const BUDGET = `${PREFIX}budget`;
const quote = (name: string): string => `"${name.replaceAll('"', '""')}"`;
const literal = (name: string): string => `'${name.replaceAll("'", "''")}'`;
const fold = (name: string): string => name.replace(/[A-Z]/g, c => c.toLowerCase());
function fail(kind: "INPUT" | "SCHEMA" | "RESULT", message: string): never {
  throw new ChangesetCaptureError(`ERR_FSQLITE_CAPTURE_${kind}`, message);
}
function name(value: unknown): string {
  if (typeof value !== "string" || !value.length || value.length > 1024 || value.includes("\0")) {
    return fail("INPUT", "Invalid capture table or column name");
  }
  const bytes = new TextEncoder().encode(value);
  if (bytes.length > 1024 || new TextDecoder("utf-8", { ignoreBOM: true }).decode(bytes) !== value) {
    return fail("INPUT", "Capture names require valid UTF-8 within 1024 bytes");
  }
  return value;
}
function count(value: unknown): number {
  if (typeof value === "bigint" && value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(value);
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return value;
  return fail("RESULT", "Expected a nonnegative safe SQL integer");
}
function bounded(value: unknown, fallback: number, ceiling: number): number {
  const n = value ?? fallback;
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 1 || n > ceiling) {
    return fail("INPUT", `Capture budget must be an integer in 1..${ceiling}`);
  }
  return n;
}
function settings(options: CaptureChangesetOptions) {
  const input = options?.tables;
  if (!Array.isArray(input) || !input.length || input.length > 64) fail("INPUT", "Capture 1..64 explicit tables");
  const tables: string[] = [], seen = new Set<string>(), n = input.length;
  for (let i = 0; i < n; i++) {
    const table = name(input[i]), key = fold(table);
    if (seen.has(key) || key.startsWith("sqlite_") || key.startsWith("__fsqlite_")) {
      fail("INPUT", "Capture distinct application tables, not SDK/system tables");
    }
    tables.push(table); seen.add(key);
  }
  const maxRows = bounded(options.maxRows, 10_000, 100_000);
  const maxBytes = bounded(options.maxBytes, 8 * 1024 * 1024, 64 * 1024 * 1024);
  const maxCells = bounded(options.maxCells, 100_000, 1_000_000);
  const indirect = options.indirect ?? false;
  if (typeof indirect !== "boolean") fail("INPUT", "indirect must be boolean");
  const inputLimits = options.limits;
  const limits: ChangesetLimits = {};
  for (const key of ["maxBytes", "maxTables", "maxColumns", "maxChanges", "maxCells"] as const) {
    const value = inputLimits?.[key];
    if (value !== undefined) limits[key] = value;
  }
  encodeChangeset([], limits);
  const signal = options.signal, timeoutMs = options.timeoutMs;
  if (signal !== undefined) {
    try { Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal); }
    catch { fail("INPUT", "signal must be an AbortSignal"); }
  }
  if (timeoutMs !== undefined) bounded(timeoutMs, 1, 2_147_483_647);
  const deadline = timeoutMs === undefined ? undefined : performance.now() + timeoutMs;
  const transactionOptions: { signal?: AbortSignal; timeoutMs?: number } = {};
  if (signal !== undefined) transactionOptions.signal = signal;
  if (timeoutMs !== undefined) transactionOptions.timeoutMs = timeoutMs;
  function checkpoint(): void {
    if (signal?.aborted) throw new ChangesetCaptureError("ERR_FSQLITE_CAPTURE_CANCELLED", "Changeset capture cancelled", { cause: signal.reason });
    if (deadline !== undefined && performance.now() >= deadline) {
      throw new ChangesetCaptureError("ERR_FSQLITE_CAPTURE_TIMEOUT", "Changeset capture deadline expired");
    }
  }
  return { tables, maxRows, maxBytes, maxCells, indirect, limits, transactionOptions, checkpoint };
}
type Settings = ReturnType<typeof settings>;
interface Plan { table: string; columns: string[]; pk: number[]; keys: number[]; journal: string }

async function read(tx: ChangesetExecutor, s: Settings, sql: string, params: readonly ChangesetValue[] = []) {
  s.checkpoint(); const result = await tx.query(sql, params); s.checkpoint();
  if (!Array.isArray(result.rowArrays) || result.rowArrays.some(row => !Array.isArray(row))) fail("RESULT", "Invalid capture query result");
  return result.rowArrays;
}
async function execute(tx: ChangesetExecutor, s: Settings, sql: string) {
  s.checkpoint(); await tx.execute(sql); s.checkpoint();
}
async function scalar(tx: ChangesetExecutor, s: Settings, sql: string): Promise<number> {
  const rows = await read(tx, s, sql);
  if (rows.length !== 1 || rows[0]!.length !== 1) fail("RESULT", "Invalid capture scalar result");
  return count(rows[0]![0]);
}
async function plan(tx: ChangesetExecutor, s: Settings, requested: string, i: number): Promise<Plan> {
  const listed = await read(tx, s, `PRAGMA main.table_list(${literal(requested)})`);
  const matches = listed.filter(row => row[0] === "main" && typeof row[1] === "string" && fold(row[1]) === fold(requested));
  if (matches.length !== 1 || matches[0]![2] !== "table") fail("SCHEMA", "Capture requires ordinary existing main tables");
  const table = name(matches[0]![1]);
  const info = await read(tx, s, `PRAGMA main.table_xinfo(${literal(table)})`);
  if (!info.length || info.length > 256 || info.length !== count(matches[0]![3])) fail("SCHEMA", "Capture supports 1..256 columns per table");
  const columns: string[] = [], pk: number[] = [], keys: number[] = [];
  for (let c = 0; c < info.length; c++) {
    const row = info[c]!;
    if (count(row[0]) !== c || count(row[6]) !== 0) fail("SCHEMA", "Hidden/generated capture columns are not supported");
    columns.push(name(row[1])); pk.push(count(row[5]));
    if (pk[c] !== 0) keys.push(c);
  }
  if (!keys.length || keys.length > 16 || keys.map(c => pk[c]!).sort((a, b) => a - b).some((v, j) => v !== j + 1)) {
    fail("SCHEMA", "Capture requires a declared primary key of 1..16 columns");
  }
  // BEFORE/AFTER user triggers on this table can reorder observation around an
  // INSERT's generated key. Reject instead of relying on undocumented trigger order.
  for (const ns of ["main", "temp"]) {
    if ((await read(tx, s, `SELECT name FROM ${ns}.sqlite_schema WHERE type = 'trigger' AND tbl_name = ? COLLATE NOCASE LIMIT 1`, [table])).length) {
      fail("SCHEMA", "Capture tables with application triggers are not supported");
    }
  }
  return { table, columns, pk, keys, journal: `${PREFIX}${i}` };
}

function valueCost(expression: string): string {
  // Double the encoded text length covers UTF-8 export from a UTF-16 database.
  return `(CASE typeof(${expression}) WHEN 'text' THEN 2 * length(CAST(${expression} AS BLOB)) WHEN 'blob' THEN length(${expression}) ELSE 8 END)`;
}
async function install(tx: ChangesetExecutor, s: Settings, p: Plan): Promise<void> {
  const fields = p.columns.map((_, i) => `v${i}`);
  await execute(tx, s, `CREATE TEMP TABLE ${quote(p.journal)} (seq INTEGER PRIMARY KEY, inserted INTEGER NOT NULL, ${fields.map(c => `${c} BLOB`).join(", ")})`);
  const index = p.keys.flatMap(i => [`typeof(v${i})`, `v${i} COLLATE BINARY`]).join(", ");
  await execute(tx, s, `CREATE UNIQUE INDEX temp.${quote(`${p.journal}_pk`)} ON ${quote(p.journal)} (${index})`);
  for (const [suffix, event, image, inserted] of [
    ["bu", "BEFORE UPDATE", "OLD", false], ["bd", "BEFORE DELETE", "OLD", false],
    ["ai", "AFTER INSERT", "NEW", true], ["au", "AFTER UPDATE", "NEW", true],
  ] as const) {
    const values = p.columns.map((c, i) => inserted && p.pk[i] === 0 ? "NULL" : `${image}.${quote(c)}`);
    const present = p.keys.map(i => `${image}.${quote(p.columns[i]!)} IS NOT NULL`).join(" AND ");
    const same = p.keys.map(i => `typeof(v${i}) = typeof(${values[i]}) AND v${i} COLLATE BINARY IS ${values[i]}`).join(" AND ");
    const bytes = `${64 + 16 * fields.length} + ${values.map(valueCost).join(" + ")}`;
    await execute(tx, s, `CREATE TEMP TRIGGER ${quote(`${p.journal}_${suffix}`)} ${event} ON main.${quote(p.table)} ` +
      `WHEN ${present} AND NOT EXISTS (SELECT 1 FROM ${quote(p.journal)} WHERE ${same}) BEGIN ` +
      `SELECT CASE WHEN n >= ${s.maxRows} OR cells + ${fields.length} > ${s.maxCells} OR bytes + (${bytes}) > ${s.maxBytes} ` +
      `THEN RAISE(ABORT, 'ERR_FSQLITE_CAPTURE_LIMIT') END FROM ${quote(BUDGET)}; ` +
      `UPDATE ${quote(BUDGET)} SET n = n + 1, cells = cells + ${fields.length}, bytes = bytes + (${bytes}); ` +
      `INSERT INTO ${quote(p.journal)} VALUES (NULL, ${Number(inserted)}, ${values.join(", ")}); END`);
  }
}

/** Typed projections avoid losing large integers or NUL-containing SQL text. */
function projection(expressions: readonly string[]): string {
  return expressions.flatMap((c, i) => [`typeof(${c}) AS t${i}`, `CASE typeof(${c}) WHEN 'integer' THEN CAST(${c} AS TEXT) WHEN 'text' THEN CAST(${c} AS BLOB) ELSE ${c} END AS x${i}`]).join(", ");
}
function decode(row: readonly unknown[], columns: number, text: TextDecoder): ChangesetValue[] {
  if (row.length !== columns * 2) fail("RESULT", "Invalid typed capture row width");
  return Array.from({ length: columns }, (_, i) => {
    const tag = row[i * 2], value = row[i * 2 + 1];
    if (tag === "null" && value === null) return null;
    if (tag === "text" && value instanceof Uint8Array) {
      try { return text.decode(value); } catch { return fail("RESULT", "Captured SQL text has invalid encoding"); }
    }
    if (tag === "blob" && value instanceof Uint8Array) return new Uint8Array(value);
    if (tag === "real" && typeof value === "number" && !Number.isNaN(value)) return value;
    if (tag === "integer" && typeof value === "string" && value.length <= 20 && /^-?(0|[1-9][0-9]*)$/.test(value)) {
      const n = BigInt(value);
      if (n >= -(1n << 63n) && n < 1n << 63n) return n;
    }
    return fail("RESULT", "SQL adapter did not preserve the captured storage class");
  });
}
function equal(a: ChangesetValue, b: ChangesetValue): boolean {
  if (a instanceof Uint8Array && b instanceof Uint8Array) return a.length === b.length && a.every((v, i) => v === b[i]);
  return typeof a === typeof b && a === b;
}
async function collect(tx: ChangesetExecutor, s: Settings, p: Plan, retained: { bytes: number; cells: number }, text: TextDecoder): Promise<ChangesetTable | null> {
  const changes: ChangesetChange[] = [];
  let after = 0;
  const columns = p.columns.map(c => quote(c));
  const typed = projection(p.columns.map((_, i) => `v${i}`));
  while (true) {
    const batch = await read(tx, s, `SELECT seq, inserted, ${typed} FROM temp.${quote(p.journal)} WHERE seq > ? ORDER BY seq LIMIT 32`, [BigInt(after)]);
    if (batch.length > 32) fail("RESULT", "Capture journal exceeded its page bound");
    if (!batch.length) break;
    for (const entry of batch) {
      const seq = count(entry[0]), inserted = count(entry[1]);
      if (seq <= after || inserted > 1) fail("RESULT", "Invalid capture journal ordering or operation");
      after = seq;
      const old = decode(entry.slice(2), columns.length, text), params = p.keys.map(i => old[i]!);
      if (params.some(v => v === null)) fail("RESULT", "Capture journal contained a NULL key");
      // Keep the declared index's affinity/collation for the seek, then compare
      // the returned key by storage class/BINARY value, as the journal does.
      const where = p.keys.map((i, k) => `${columns[i]} = ${typeof params[k] === "number" ? "CAST(? AS REAL)" : "?"}`).join(" AND ");
      const costs = await read(tx, s, `SELECT ${64 + columns.length * 16} + ${columns.map(valueCost).join(" + ")} FROM main.${quote(p.table)} WHERE ${where} LIMIT 2`, params);
      if (costs.length > 1 || costs.some(row => row.length !== 1)) fail("RESULT", "Invalid capture row-size query");
      if (costs.length) {
        retained.bytes += count(costs[0]![0]); retained.cells += columns.length;
        if (retained.bytes > s.maxBytes || retained.cells > s.maxCells) {
          // Check lengths before transferring/allocating potentially huge values.
          throw new Error("ERR_FSQLITE_CAPTURE_LIMIT: collected row images exceed the capture budget");
        }
      }
      const rows = await read(tx, s, `SELECT ${projection(columns)} FROM main.${quote(p.table)} WHERE ${where} LIMIT 2`, params);
      if (rows.length !== costs.length) fail("RESULT", "Capture primary-key lookup changed within one transaction");
      let current = rows.length ? decode(rows[0]!, columns.length, text) : null;
      if (current !== null && !p.keys.every(i => equal(old[i]!, current![i]!))) current = null;
      if (inserted) {
        if (current !== null) changes.push({ operation: "insert", indirect: s.indirect, new: current });
      } else if (current === null) {
        changes.push({ operation: "delete", indirect: s.indirect, old });
      } else {
        const before: ChangesetField[] = [], next: ChangesetField[] = [];
        let changed = false;
        for (let i = 0; i < columns.length; i++) {
          const modified = p.pk[i] === 0 && !equal(old[i]!, current[i]!);
          before.push(p.pk[i] !== 0 || modified ? old[i] : undefined);
          next.push(modified ? current[i] : undefined); changed ||= modified;
        }
        if (changed) changes.push({ operation: "update", indirect: s.indirect, old: before, new: next });
      }
    }
  }
  // Key changes (including case changes under NOCASE) must vacate old keys first.
  changes.sort((a, b) => Number(b.operation === "delete") - Number(a.operation === "delete"));
  return changes.length ? { name: p.table, primaryKey: p.pk, changes } : null;
}

/**
 * Capture callback DML through real SQL triggers inside one owned transaction.
 * Requires recursive_triggers=ON and no application triggers on captured tables.
 * No full-table snapshot, monkey-patched execute, persistent trigger or WASM shim.
 */
export async function captureChangeset<T>(target: ChangesetTarget,
  work: (tx: ChangesetExecutor) => T | Promise<T>, options: CaptureChangesetOptions): Promise<CapturedChangeset<T>> {
  if (typeof work !== "function") fail("INPUT", "Capture requires a callback");
  const s = settings(options); s.checkpoint();
  return target.transaction(async tx => {
    s.checkpoint();
    if (await scalar(tx, s, "PRAGMA recursive_triggers") !== 1) fail("SCHEMA", "Set PRAGMA recursive_triggers=ON before capture so REPLACE deletions cannot disappear");
    if ((await read(tx, s, "SELECT name FROM temp.sqlite_schema WHERE name GLOB ? LIMIT 1", [`${PREFIX}*`])).length) {
      fail("SCHEMA", "A capture scope or reserved TEMP object already exists on this connection");
    }
    const plans: Plan[] = [];
    for (const table of s.tables) plans.push(await plan(tx, s, table, plans.length));
    const encoding = await read(tx, s, "PRAGMA encoding");
    const label = encoding[0]?.[0];
    if (encoding.length !== 1 || encoding[0]!.length !== 1 ||
        (label !== "UTF-8" && label !== "UTF-16le" && label !== "UTF-16be")) fail("RESULT", "Unsupported capture text encoding");
    const text = new TextDecoder(label, { fatal: true, ignoreBOM: true });
    await execute(tx, s, `CREATE TEMP TABLE ${quote(BUDGET)} (n INTEGER NOT NULL, bytes INTEGER NOT NULL, cells INTEGER NOT NULL)`);
    await execute(tx, s, `INSERT INTO temp.${quote(BUDGET)} VALUES (0, 0, 0)`);
    for (const p of plans) await install(tx, s, p);
    const mainVersion = await scalar(tx, s, "PRAGMA main.schema_version");
    const tempVersion = await scalar(tx, s, "PRAGMA temp.schema_version");
    const value = await work(tx); s.checkpoint();
    if (await scalar(tx, s, "PRAGMA main.schema_version") !== mainVersion ||
        await scalar(tx, s, "PRAGMA temp.schema_version") !== tempVersion ||
        await scalar(tx, s, "PRAGMA recursive_triggers") !== 1) {
      fail("SCHEMA", "Capture callbacks must not change schemas or recursive_triggers");
    }
    const budget = await read(tx, s, `SELECT n, bytes, cells FROM temp.${quote(BUDGET)}`);
    if (budget.length !== 1 || budget[0]!.length !== 3) fail("RESULT", "Invalid capture budget row");
    const touchedRows = count(budget[0]![0]);
    const retained = { bytes: count(budget[0]![1]), cells: count(budget[0]![2]) };
    if (touchedRows > s.maxRows || retained.bytes > s.maxBytes || retained.cells > s.maxCells) fail("RESULT", "Invalid capture budget");
    const tables: ChangesetTable[] = [];
    for (const p of plans) { const table = await collect(tx, s, p, retained, text); if (table !== null) tables.push(table); }
    s.checkpoint(); const changeset = encodeChangeset(tables, s.limits); s.checkpoint();
    for (const p of plans) {
      for (const suffix of ["bu", "bd", "ai", "au"]) await execute(tx, s, `DROP TRIGGER temp.${quote(`${p.journal}_${suffix}`)}`);
      await execute(tx, s, `DROP TABLE temp.${quote(p.journal)}`);
    }
    await execute(tx, s, `DROP TABLE temp.${quote(BUDGET)}`);
    return { value, changeset, touchedRows, changes: tables.reduce((n, t) => n + t.changes.length, 0) };
  }, s.transactionOptions);
}
