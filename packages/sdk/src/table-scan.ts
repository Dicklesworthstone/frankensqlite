import { FrankenSQLiteError } from "./errors";
import { observeQueueClose } from "./queue";
import type { FrankenDBQueue, QueuedJobOptions } from "./queue";
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

export interface TableScanOptions extends TableScanPageOptions, QueuedJobOptions {
  /** Consumer inactivity while retaining a snapshot; default 30s, 0 disables. */
  idleTimeoutMs?: number;
}
export interface TableScanStats {
  readonly pagesRead: number;
  readonly pageQueries: number;
  readonly rowsRead: number;
  readonly rowsYielded: number;
  readonly bufferedRows: number;
  readonly maxBufferedRows: number;
}
export interface TableScan<Row extends Record<string, unknown> = Record<string, unknown>> extends AsyncIterableIterator<Row> {
  readonly closed: boolean;
  readonly stats: Readonly<TableScanStats>;
  /** Joins the retained transaction and cleanup; drain iteration or call return(). */
  readonly done: Promise<void>;
  return(): Promise<IteratorResult<Row>>;
  throw(cause?: unknown): Promise<IteratorResult<Row>>;
}
function cancelledOnly(cause: unknown): boolean {
  if (cause instanceof AggregateError) return cause.errors.length > 0 && cause.errors.every(cancelledOnly);
  return cause instanceof FrankenSQLiteError &&
    (cause.code === "ERR_FSQLITE_TRANSACTION_CANCELLED" || cause.code === "ERR_FSQLITE_JOB_CANCELLED");
}

/**
 * Stream rows from one main table in stable storage-key order. Starts on next(),
 * retains one managed snapshot, and never fetches the next page ahead of demand.
 * Not a general SQL cursor; independent connections retain their own concurrency.
 */
export function scanTable<Row extends Record<string, unknown> = Record<string, unknown>>(
  queue: FrankenDBQueue, table: string, options: TableScanOptions = {},
): TableScan<Row> {
  const settings = captureTableScan(table, options);
  const callerSignal = options.signal, waitTimeoutMs = options.waitTimeoutMs;
  const idleTimeoutMs = options.idleTimeoutMs ?? 30_000;
  if (!Number.isInteger(idleTimeoutMs) || idleTimeoutMs < 0 || idleTimeoutMs > 2147483647 ||
      (waitTimeoutMs !== undefined && (!Number.isInteger(waitTimeoutMs) || waitTimeoutMs < 1 || waitTimeoutMs > 2147483647))) {
    throw invalid("Use idleTimeoutMs in 0..2147483647 and integer waitTimeoutMs in 1..2147483647");
  }
  if (callerSignal !== undefined) Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(callerSignal);
  const controller = new AbortController();
  const signal = AbortSignal.any(callerSignal === undefined ? [controller.signal] : [controller.signal, callerSignal]);
  const jobOptions = { signal, ...(waitTimeoutMs === undefined ? {} : { waitTimeoutMs }) };
  let started = false, running = false, stopped = false, finished = false, sourceComplete = false;
  let failure: { cause: unknown } | null = null;
  let buffered: (Record<string, unknown> | undefined)[] = [], position = 0;
  let pagesRead = 0, pageQueries = 0, rowsRead = 0, rowsYielded = 0, maxBufferedRows = 0;
  let pending: { resolve: (result: IteratorResult<Row>) => void; reject: (cause: unknown) => void } | null = null;
  let wake: (() => void) | null = null, unobserve: (() => void) | null = null;
  let idleTimer: ReturnType<typeof setTimeout> | undefined, idleDeadline: number | undefined;
  let resolveDone!: () => void, rejectDone!: (cause: unknown) => void;
  const done = new Promise<void>((resolve, reject) => { resolveDone = resolve; rejectDone = reject; });
  void done.catch(() => {});

  function fail(cause: unknown): void {
    if (failure === null) failure = { cause };
    else if (failure.cause !== cause) failure = { cause: new AggregateError([failure.cause, cause],
      "Table scan and cleanup both failed", { cause: failure.cause }) };
  }
  function clearIdle(): void {
    if (idleTimer !== undefined) clearTimeout(idleTimer);
    idleTimer = undefined; idleDeadline = undefined;
  }
  function expired(): void {
    stop({ cause: new FrankenSQLiteError({ code: "ERR_FSQLITE_SCAN_IDLE_TIMEOUT", transient: false,
      message: "Table scan consumer was inactive; its retained snapshot was released. This is not successful EOF." }) });
  }
  function touchIdle(): void {
    clearIdle();
    if (wake !== null && !stopped && idleTimeoutMs > 0) {
      idleDeadline = performance.now() + idleTimeoutMs;
      idleTimer = setTimeout(expired, idleTimeoutMs);
    }
  }
  function notify(): void {
    const resume = wake; wake = null; clearIdle(); resume?.();
  }
  function park(): Promise<void> {
    if (stopped) return Promise.resolve();
    return new Promise<void>(resolve => { wake = resolve; touchIdle(); });
  }
  function flush(): void {
    if (pending === null || stopped) return;
    if (position < buffered.length) {
      const row = buffered[position]!;
      buffered[position++] = undefined; // Release consumed rows even inside a large page.
      rowsYielded++;
      const demand = pending; pending = null;
      demand.resolve({ done: false, value: row as Row });
    }
    if (position === buffered.length) { buffered = []; position = 0; notify(); }
  }
  function settle(): void {
    if (running || (!stopped && !finished)) return;
    finished = true; clearIdle();
    signal.removeEventListener("abort", onAbort);
    unobserve?.(); unobserve = null;
    buffered = []; position = 0;
    const demand = pending; pending = null;
    if (failure !== null) { demand?.reject(failure.cause); rejectDone(failure.cause); }
    else { demand?.resolve({ done: true, value: undefined }); resolveDone(); }
  }
  function stop(error: { cause: unknown } | null = null): void {
    if (finished) return;
    if (error !== null) fail(error.cause);
    stopped = true; buffered = []; position = 0;
    controller.abort(); notify(); settle();
  }
  function cancellation(cause: unknown): FrankenSQLiteError {
    const error = new FrankenSQLiteError({ code: "ERR_FSQLITE_SCAN_CANCELLED", transient: false,
      message: "Table scan cancelled before completion; delivered rows are only a prefix, not successful EOF" });
    error.cause = cause;
    return error;
  }
  function onAbort(): void {
    // Explicit return() already set stopped before aborting our controller.
    // Once every row is delivered, the actual final transaction outcome wins.
    if (!stopped && !finished && !sourceComplete) stop({ cause: cancellation(signal.reason) });
  }

  async function produce(tx: FrankenTransaction): Promise<void> {
    if (stopped) return;
    const reader = await createTablePageReader(tx, settings);
    while (!stopped) {
      // A full consumed page alone does not authorize another query. Require
      // demand, including the final empty probe for an exact-sized last page.
      while (!stopped && pending === null) await park();
      if (stopped) break;
      const rows = await reader.read();
      pageQueries = reader.queries;
      if (stopped) break;
      pagesRead++; rowsRead += rows.length;
      maxBufferedRows = Math.max(maxBufferedRows, rows.length);
      buffered = rows; position = 0; flush();
      while (!stopped && position < buffered.length) await park();
      if (reader.exhausted) break;
    }
    if (!stopped) sourceComplete = true;
  }
  function start(): void {
    started = true; running = true;
    try {
      let observing = false;
      unobserve = observeQueueClose(queue, error => {
        if (sourceComplete && error === null) return;
        stop({ cause: error ?? (observing ? cancellation("Owning queue is closing") :
          new FrankenSQLiteError({ code: "ERR_FSQLITE_JOB_QUEUE_CLOSED", transient: false,
            message: "The scan did not start because its queue was already closing or closed" })) });
      });
      observing = true;
      if (stopped) { running = false; settle(); return; }
      void queue.transaction(produce, jobOptions).then(() => {
        running = false; finished = true; settle();
      }, (cause: unknown) => {
        if (!stopped || !cancelledOnly(cause)) fail(cause);
        running = false; stopped = true; notify(); settle();
      });
    } catch (cause: unknown) { fail(cause); running = false; stopped = true; settle(); }
  }

  signal.addEventListener("abort", onAbort, { once: true });
  if (signal.aborted) onAbort();
  const iterator: TableScan<Row> = Object.freeze({
    get closed() { return stopped || finished; },
    get stats() { return Object.freeze({ pagesRead, pageQueries, rowsRead, rowsYielded,
      bufferedRows: buffered.length - position, maxBufferedRows }); },
    done,
    next(): Promise<IteratorResult<Row>> {
      // Recheck elapsed idle time even when event-loop work delayed its timer.
      if (idleDeadline !== undefined && performance.now() >= idleDeadline) expired();
      if (stopped || finished) return done.then(() => ({ done: true, value: undefined }));
      if (pending !== null) return Promise.reject(new FrankenSQLiteError({ code: "ERR_FSQLITE_SCAN_NEXT_PENDING",
        message: "Await the outstanding scan next() before requesting another row", transient: false }));
      const result = new Promise<IteratorResult<Row>>((resolve, reject) => { pending = { resolve, reject }; });
      touchIdle(); flush();
      if (!started) start();
      else if (pending !== null) notify();
      return result;
    },
    async return(): Promise<IteratorResult<Row>> { stop(); await done; return { done: true, value: undefined }; },
    async throw(cause?: unknown): Promise<IteratorResult<Row>> { stop({ cause }); await done; throw cause; },
    [Symbol.asyncIterator]() { return iterator; },
  });
  return iterator;
}
