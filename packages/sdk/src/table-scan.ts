import { FrankenSQLiteError } from "./errors";
import type { FrankenTransaction } from "./transaction";
import type { SqlScalar } from "./types";

const MAX_COLUMNS = 1024;
const MAX_KEYS = 16;
const MAX_KEY_BYTES = 1024 * 1024;
const quote = (name: string): string => `"${name.replaceAll('"', '""')}"`;
const literal = (name: string): string => `'${name.replaceAll("'", "''")}'`;
const fold = (name: string): string => name.replace(/[A-Z]/g, c => c.toLowerCase());

function invalid(message: string): FrankenSQLiteError {
  return new FrankenSQLiteError({ code: "ERR_FSQLITE_SCAN_INPUT", message, transient: false });
}
function unsupported(message: string): FrankenSQLiteError {
  return new FrankenSQLiteError({ code: "ERR_FSQLITE_SCAN_SCHEMA", message, transient: false });
}
function badResult(): FrankenSQLiteError {
  return new FrankenSQLiteError({ code: "ERR_FSQLITE_SCAN_RESULT", transient: false,
    message: "Table scan returned invalid metadata, keys or page bounds; the scan was stopped" });
}
function identifier(value: unknown): string {
  if (typeof value !== "string" || value.length === 0 || value.length > 1024 || value.includes("\0")) {
    throw invalid("Use nonempty table/column names of at most 1024 characters without NUL");
  }
  return value;
}
function integer(value: unknown): number {
  if (typeof value === "bigint" && value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(value);
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return value;
  throw badResult();
}

export interface TableScanPageOptions {
  /** Returned rows per page, 1..4096. Defaults to 256; not a byte/RSS limit. */
  batchSize?: number;
  /** Plain column names; omitted means every visible column, including generated columns. */
  columns?: readonly string[];
  /** Reverse the table's rowid / complete primary-key ordering. */
  reverse?: boolean;
}

/** Capture once before asynchronous admission; no application array iterators. */
export function captureTableScan(table: string, options: TableScanPageOptions = {}) {
  const name = identifier(table);
  if (fold(name).startsWith("sqlite_")) throw invalid("Scan an ordinary application table in main");
  const batchSize = options.batchSize ?? 256;
  const reverse = options.reverse ?? false;
  const input = options.columns;
  if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > 4096 || typeof reverse !== "boolean") {
    throw invalid("batchSize must be an integer in 1..4096 and reverse must be boolean");
  }
  let columns: readonly string[] | undefined;
  if (input !== undefined) {
    if (!Array.isArray(input) || input.length === 0 || input.length > MAX_COLUMNS) {
      throw invalid(`Select 1..${MAX_COLUMNS} columns`);
    }
    const names: string[] = [], seen = new Set<string>(), length = input.length;
    for (let i = 0; i < length; i++) {
      const column = identifier(input[i]);
      if (seen.has(fold(column))) throw invalid("Projection columns must be distinct");
      names.push(column); seen.add(fold(column));
    }
    columns = Object.freeze(names);
  }
  return Object.freeze({ name, batchSize, reverse, columns });
}

type Settings = ReturnType<typeof captureTableScan>;
interface Key { name: string; descending: boolean; collation: string | null; alias: string }

/**
 * Internal page reader. The caller must retain ONE managed transaction through
 * all pages. No OFFSET, SQL interpolation of values, query replay, or full-table
 * result is used. WITHOUT ROWID continuation is split into disjoint indexed
 * prefix ranges so mixed ASC/DESC keys do not need a broad OR filter or sort.
 */
export async function createTablePageReader(tx: FrankenTransaction, settings: Settings) {
  const listed = await tx.query(`PRAGMA main.table_list(${literal(settings.name)})`);
  const matches = listed.rowArrays.filter(row => row[0] === "main" &&
    typeof row[1] === "string" && fold(row[1]) === fold(settings.name));
  if (matches.length !== 1 || matches[0]![2] !== "table") {
    throw unsupported("Table scans require an ordinary main table, not a view, virtual or shadow table");
  }
  const metadata = matches[0]!, table = identifier(metadata[1]);
  const withoutRowid = integer(metadata[4]);
  if (withoutRowid > 1 || integer(metadata[3]) > MAX_COLUMNS) throw unsupported("Unsupported table layout");
  const info = await tx.query(`PRAGMA main.table_xinfo(${literal(table)})`);
  if (info.rowArrays.length === 0 || info.rowArrays.length !== integer(metadata[3])) throw badResult();
  const all = new Map<string, string>();
  for (const row of info.rowArrays) {
    const name = identifier(row[1]), hidden = integer(row[6]);
    if (hidden !== 0 && hidden !== 2 && hidden !== 3) throw unsupported("Hidden virtual columns are not supported");
    if (all.has(fold(name))) throw badResult();
    all.set(fold(name), name);
  }
  const columns = settings.columns === undefined ? [...all.values()] : settings.columns.map(name => {
    const canonical = all.get(fold(name));
    if (canonical === undefined) throw invalid(`Unknown scan column: ${name}`);
    return canonical;
  });
  const keys: Key[] = [];
  if (withoutRowid === 0) {
    const name = ["_rowid_", "rowid", "oid"].find(candidate => !all.has(candidate));
    if (name === undefined) throw unsupported("All hidden rowid aliases are shadowed by declared columns");
    keys.push({ name, descending: settings.reverse, collation: null, alias: "" });
  } else {
    const index = await tx.query(`PRAGMA main.index_xinfo(${literal(table)})`);
    for (const row of index.rowArrays) {
      if (integer(row[5]) === 0) continue;
      const name = identifier(row[2]), descending = integer(row[3]);
      if (integer(row[0]) !== keys.length || !all.has(fold(name)) || descending > 1 ||
          keys.some(key => fold(key.name) === fold(name))) throw badResult();
      keys.push({ name, descending: (descending === 1) !== settings.reverse,
        collation: identifier(row[4]), alias: "" });
    }
    if (keys.length === 0 || keys.length > MAX_KEYS) throw unsupported(`Use a primary key with 1..${MAX_KEYS} columns`);
  }
  // Unique labels preserve positional data through object-only core adapters;
  // a real table is allowed to contain these otherwise ordinary column names.
  const labels = new Set(all.keys());
  keys.forEach((key, i) => {
    let alias = `__fsqlite_scan_key_${i}`;
    while (labels.has(fold(alias))) alias += "_";
    key.alias = alias; labels.add(fold(alias));
  });
  const column = (name: string): string => `s.${quote(name)}`;
  const expression = (key: Key): string => column(key.name) +
    (key.collation === null ? "" : ` COLLATE ${quote(key.collation)}`);
  const order = keys.map(key => `${expression(key)} ${key.descending ? "DESC" : "ASC"}`).join(", ");
  const select = `SELECT ${keys.map(key => `${column(key.name)} AS ${quote(key.alias)}`).concat(columns.map(column)).join(", ")} ` +
    `FROM main.${quote(table)} AS s`;
  let last: SqlScalar[] | null = null, exhausted = false, queries = 0;

  function copyKey(row: readonly SqlScalar[]): SqlScalar[] {
    let size = 0;
    for (const value of row.slice(0, keys.length)) {
      size += typeof value === "string" ? value.length * 2 : value instanceof Uint8Array ? value.byteLength : 16;
      if (size > MAX_KEY_BYTES) throw unsupported("Continuation key exceeds the 1 MiB retained-key budget");
    }
    const key = row.slice(0, keys.length).map(value => {
      if (typeof value === "string") return value;
      if (typeof value === "number" && Number.isFinite(value)) return value;
      if (typeof value === "bigint") return value;
      if (value instanceof Uint8Array) return value.slice();
      throw badResult(); // PRIMARY KEY components and hidden rowids cannot be NULL.
    });
    if (withoutRowid === 0 && !(typeof key[0] === "bigint" ||
        (typeof key[0] === "number" && Number.isSafeInteger(key[0])))) throw badResult();
    return key;
  }
  function same(left: readonly SqlScalar[], right: readonly SqlScalar[]): boolean {
    return left.every((value, i) => {
      const other = right[i];
      if (value instanceof Uint8Array && other instanceof Uint8Array) {
        return value.length === other.length && value.every((byte, j) => byte === other[j]);
      }
      return value === other || (typeof value === "number" && typeof other === "bigint" &&
        Number.isSafeInteger(value) && BigInt(value) === other) ||
        (typeof value === "bigint" && typeof other === "number" && Number.isSafeInteger(other) && value === BigInt(other));
    });
  }

  return {
    table, columns: Object.freeze(columns),
    get exhausted() { return exhausted; },
    get queries() { return queries; },
    async read(): Promise<Record<string, unknown>[]> {
      if (exhausted) return [];
      const after = last;
      const rows: Record<string, unknown>[] = [];
      // (a,b,c) successors: a=A,b=B,c>C; then a=A,b>B; then a>A.
      // Each range is disjoint, in total key order, and independently seekable.
      const stages = after === null ? [-1] : keys.map((_, i) => i).reverse();
      for (const stage of stages) {
        const predicates: string[] = [], params: SqlScalar[] = [];
        if (after !== null) {
          for (let i = 0; i < stage; i++) { predicates.push(`${expression(keys[i]!)} = ?`); params.push(after[i]!); }
          const key = keys[stage]!;
          predicates.push(`${expression(key)} ${key.descending ? "<" : ">"} ?`); params.push(after[stage]!);
        }
        const remaining = settings.batchSize - rows.length;
        params.push(remaining);
        queries++;
        const result = await tx.query(select + (predicates.length ? ` WHERE ${predicates.join(" AND ")}` : "") +
          ` ORDER BY ${order} LIMIT ?`, params);
        if (result.rowArrays.length > remaining || result.columnCount !== keys.length + columns.length) throw badResult();
        for (const row of result.rowArrays) {
          if (row.length !== keys.length + columns.length) throw badResult();
          const next = copyKey(row);
          if (last !== null && (same(last, next) || (withoutRowid === 0 &&
              (settings.reverse ? BigInt(next[0] as number | bigint) >= BigInt(last[0] as number | bigint)
                : BigInt(next[0] as number | bigint) <= BigInt(last[0] as number | bigint))))) throw badResult();
          last = next;
          rows.push(Object.fromEntries(columns.map((name, i) => [name, row[keys.length + i]])));
        }
        if (rows.length === settings.batchSize) return rows;
      }
      exhausted = true;
      last = null;
      return rows;
    },
  };
}
