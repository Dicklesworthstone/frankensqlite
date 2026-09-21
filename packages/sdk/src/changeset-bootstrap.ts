import type { ChangesetExecutor, ChangesetTarget } from "./changeset-apply";
import { applyChangeset } from "./changeset-apply";
import type { ChangesetValue } from "./changeset-codec";
import { decodeChangeset } from "./changeset-codec";

/** Separate from ordinary per-message delivery: staging is NOT an application ACK. */
export const CHANGESET_BOOTSTRAP_PROTOCOL = "fsqlite-bootstrap-v1";
export const CHANGESET_BOOTSTRAP_STATE_TABLE = "__fsqlite_bootstrap_state";
export const CHANGESET_BOOTSTRAP_CHUNKS_TABLE = "__fsqlite_bootstrap_chunks";
export interface BootstrapManifestInput {
  readonly receiverId: string;
  readonly deliveryId: string;
  readonly tables: readonly string[];
  readonly chunks: number;
  readonly changes: number;
  readonly byteLength: number;
}
export interface BootstrapManifest extends BootstrapManifestInput {
  readonly protocol: typeof CHANGESET_BOOTSTRAP_PROTOCOL;
  /** Ordered SHA-256 chain binding the complete scope, chunk digests and totals. */
  readonly sha256: string;
}
export interface BootstrapOperationOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
}
export interface BootstrapProgress {
  readonly receivedChunks: number;
  readonly receivedBytes: number;
  readonly receivedChanges: number;
  /** SQL installation decision, NOT confirmation of persistent snapshot storage. */
  readonly installed: boolean;
}
export interface BootstrapInstallReceipt {
  readonly protocol: typeof CHANGESET_BOOTSTRAP_PROTOCOL;
  readonly receiverId: string;
  readonly deliveryId: string;
  readonly sha256: string;
  readonly chunks: number;
  readonly changes: number;
  readonly byteLength: number;
  readonly installed: true;
  readonly confirmed: true;
  readonly replayed: boolean;
}
export interface BootstrapReceiverOptions {
  receiverId: string;
  /** Fixed direct-target authority, checked on every operation, including replay. */
  tables: readonly string[];
  /** SAME top-level database; called after installation and every installed replay. */
  confirmCommit: () => Promise<unknown>;
  maxChunkBytes?: number;
  maxBytes?: number;
  maxChunks?: number;
  maxChanges?: number;
}
type ErrorKind =
  | "INPUT"
  | "LIMIT"
  | "STATE"
  | "CORRUPT"
  | "SCHEMA"
  | "BUSY"
  | "CANCELLED"
  | "TIMEOUT"
  | "FAILED"
  | "CONFIRM";
export class ChangesetBootstrapError extends Error {
  readonly code: `ERR_FSQLITE_BOOTSTRAP_${ErrorKind}`;
  constructor(kind: ErrorKind, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "ChangesetBootstrapError";
    this.code = `ERR_FSQLITE_BOOTSTRAP_${kind}`;
  }
}
const HARD_BYTES = 1024 * 1024 * 1024;
const HARD_CHUNK = 64 * 1024 * 1024;
const STATE = `main."${CHANGESET_BOOTSTRAP_STATE_TABLE}"`;
const CHUNKS = `main."${CHANGESET_BOOTSTRAP_CHUNKS_TABLE}"`;
const fold = (s: string): string => s.replace(/[A-Z]/g, (c) => c.toLowerCase());
const quote = (s: string): string => `"${s.replaceAll('"', '""')}"`;
const literal = (s: string): string => `'${s.replaceAll("'", "''")}'`;
function fail(kind: ErrorKind, message: string): never {
  throw new ChangesetBootstrapError(kind, message);
}
function data(value: unknown, key: string): unknown {
  if (typeof value !== "object" || value === null)
    fail("INPUT", "Expected an own-data bootstrap record");
  const field = Object.getOwnPropertyDescriptor(value, key);
  if (field === undefined || !Object.hasOwn(field, "value"))
    fail("INPUT", "Bootstrap fields must be own data properties");
  return field.value;
}
function text(value: unknown, maximum: number): string {
  if (typeof value !== "string" || !value.length || value.length > maximum || value.includes("\0"))
    fail("INPUT", "Invalid bootstrap name or identity");
  const bytes = new TextEncoder().encode(value);
  if (
    bytes.length > maximum ||
    new TextDecoder("utf-8", { ignoreBOM: true }).decode(bytes) !== value
  )
    fail("INPUT", "Bootstrap text must be bounded valid UTF-8");
  return value;
}
function number(value: unknown, maximum: number, minimum = 0): number {
  if (
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    value < minimum ||
    value > maximum
  )
    fail("INPUT", "Invalid bootstrap count or limit");
  return value;
}
function sqlNumber(value: unknown, maximum: number): number {
  const n =
    typeof value === "bigint" && value >= 0n && value <= BigInt(maximum) ? Number(value) : value;
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 0 || n > maximum)
    fail("CORRUPT", "Invalid stored bootstrap count");
  return n;
}
function digest(value: unknown): string {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value))
    fail("CORRUPT", "Invalid bootstrap digest");
  return value;
}
function tableNames(value: unknown): readonly string[] {
  if (!Array.isArray(value) || !value.length || value.length > 64)
    fail("INPUT", "Bootstrap requires 1..64 explicit application tables");
  const result: string[] = [],
    seen = new Set<string>();
  for (let i = 0, n = value.length; i < n; i++) {
    const name = fold(text(data(value, String(i)), 1024));
    if (seen.has(name) || name.startsWith("sqlite_") || name.startsWith("__fsqlite_"))
      fail("INPUT", "Use distinct application tables, not reserved tables");
    seen.add(name);
    result.push(name);
  }
  return Object.freeze(result);
}
function captureInput(value: unknown): BootstrapManifestInput {
  return Object.freeze({
    receiverId: text(data(value, "receiverId"), 256),
    deliveryId: text(data(value, "deliveryId"), 480),
    tables: tableNames(data(value, "tables")),
    chunks: number(data(value, "chunks"), 100_000, 1),
    changes: number(data(value, "changes"), 10_000_000),
    byteLength: number(data(value, "byteLength"), HARD_BYTES),
  });
}
function captureManifest(value: unknown): BootstrapManifest {
  if (data(value, "protocol") !== CHANGESET_BOOTSTRAP_PROTOCOL)
    fail("INPUT", "Wrong bootstrap protocol");
  return Object.freeze({
    protocol: CHANGESET_BOOTSTRAP_PROTOCOL,
    ...captureInput(value),
    sha256: digest(data(value, "sha256")),
  });
}
function owned(value: unknown, maximum: number): Uint8Array {
  if (!(value instanceof Uint8Array)) fail("INPUT", "Bootstrap chunks must be Uint8Array values");
  const proto = Object.getPrototypeOf(Uint8Array.prototype) as object;
  const get = (key: string): unknown =>
    Object.getOwnPropertyDescriptor(proto, key)!.get!.call(value);
  const buffer = get("buffer"),
    offset = get("byteOffset") as number,
    length = get("byteLength") as number;
  if (
    !(buffer instanceof ArrayBuffer) ||
    Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "resizable")?.get?.call(buffer)
  )
    fail("INPUT", "Use fixed, non-shared chunk buffers");
  if (length > maximum) fail("LIMIT", "Bootstrap chunk exceeds the byte budget");
  return new Uint8Array(new Uint8Array(buffer, offset, length));
}
async function hash(bytes: Uint8Array): Promise<string> {
  if (!globalThis.crypto?.subtle) fail("INPUT", "Bootstrap requires Web Crypto SHA-256");
  const result = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", bytes));
  return Array.from(result, (b) => b.toString(16).padStart(2, "0")).join("");
}
const hashJson = (value: unknown): Promise<string> =>
  hash(new TextEncoder().encode(JSON.stringify(value)));
function seed(m: BootstrapManifestInput): Promise<string> {
  return hashJson([
    CHANGESET_BOOTSTRAP_PROTOCOL,
    m.receiverId,
    m.deliveryId,
    m.tables,
    m.chunks,
    m.changes,
    m.byteLength,
  ]);
}
function link(
  previous: string,
  index: number,
  sha256: string,
  bytes: number,
  changes: number,
): Promise<string> {
  return hashJson([CHANGESET_BOOTSTRAP_PROTOCOL, previous, index, sha256, bytes, changes]);
}
function inspect(bytes: Uint8Array, m: BootstrapManifestInput): number {
  let changes = 0;
  for (const table of decodeChangeset(bytes)) {
    if (!m.tables.includes(fold(table.name)) || table.changes.some((c) => c.operation !== "insert"))
      fail("INPUT", "Bootstrap contains unauthorized tables or non-INSERT changes");
    changes += table.changes.length;
  }
  return changes;
}
class Budget {
  readonly signal: AbortSignal;
  readonly #timeout = new AbortController();
  readonly #reason = new Error("Bootstrap deadline expired");
  readonly #deadline: number | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  constructor(options: BootstrapOperationOptions = {}) {
    const external = options.signal,
      ms = options.timeoutMs;
    if (external !== undefined) {
      try {
        Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(external);
      } catch {
        fail("INPUT", "signal must be an AbortSignal");
      }
    }
    if (ms !== undefined) this.#deadline = performance.now() + number(ms, 2_147_483_647, 1);
    this.signal =
      external === undefined
        ? this.#timeout.signal
        : AbortSignal.any([external, this.#timeout.signal]);
    this.#arm();
  }
  #expire(): void {
    if (
      this.#deadline !== undefined &&
      performance.now() >= this.#deadline &&
      !this.#timeout.signal.aborted
    )
      this.#timeout.abort(this.#reason);
  }
  #arm(): void {
    if (this.#deadline === undefined || this.signal.aborted) return;
    this.#timer = setTimeout(
      () => {
        this.#expire();
        this.#arm();
      },
      Math.max(1, Math.ceil(this.#deadline - performance.now())),
    );
  }
  check(): void {
    this.#expire();
    if (this.signal.aborted)
      throw new ChangesetBootstrapError(
        this.signal.reason === this.#reason ? "TIMEOUT" : "CANCELLED",
        "Bootstrap stopped; reconcile the same manifest before retrying",
        { cause: this.signal.reason },
      );
  }
  options(): BootstrapOperationOptions {
    this.check();
    return this.#deadline === undefined
      ? { signal: this.signal }
      : {
          signal: this.signal,
          timeoutMs: Math.max(1, Math.ceil(this.#deadline - performance.now())),
        };
  }
  finish(): void {
    clearTimeout(this.#timer);
  }
}

/** Read immutable retained chunks, one at a time. This does NOT create a source snapshot. */
export async function createBootstrapManifest(
  input: BootstrapManifestInput,
  readChunk: (index: number) => Promise<Uint8Array>,
  options?: BootstrapOperationOptions,
): Promise<BootstrapManifest> {
  const m = captureInput(input);
  if (typeof readChunk !== "function") fail("INPUT", "A retained chunk reader is required");
  const budget = new Budget(options);
  try {
    budget.check();
    let chain = await seed(m),
      bytes = 0,
      changes = 0;
    for (let i = 0; i < m.chunks; i++) {
      budget.check();
      const chunk = owned(await readChunk(i), HARD_CHUNK);
      budget.check();
      const count = inspect(chunk, m);
      bytes += chunk.byteLength;
      changes += count;
      if (bytes > m.byteLength || changes > m.changes)
        fail("STATE", "Bootstrap exceeds its declared totals");
      chain = await link(chain, i, await hash(chunk), chunk.byteLength, count);
    }
    budget.check();
    if (bytes !== m.byteLength || changes !== m.changes)
      fail("STATE", "Bootstrap totals do not match its retained chunks");
    return Object.freeze({ protocol: CHANGESET_BOOTSTRAP_PROTOCOL, ...m, sha256: chain });
  } finally {
    budget.finish();
  }
}
async function query(
  tx: ChangesetExecutor,
  b: Budget,
  sql: string,
  params: readonly ChangesetValue[] = [],
) {
  b.check();
  const result = await tx.query(sql, params);
  b.check();
  if (!Array.isArray(result.rowArrays) || result.rowArrays.some((r) => !Array.isArray(r)))
    fail("CORRUPT", "Invalid bootstrap SQL result");
  return result.rowArrays;
}
async function write(
  tx: ChangesetExecutor,
  b: Budget,
  sql: string,
  params: readonly ChangesetValue[],
  expected: number,
) {
  b.check();
  const changed = await tx.execute(sql, params);
  b.check();
  if (changed !== expected) fail("CORRUPT", "Bootstrap write did not affect the expected rows");
}
const layouts = [
  {
    name: CHANGESET_BOOTSTRAP_STATE_TABLE,
    columns: ["id", "manifest", "received", "bytes", "changes", "chain", "installed"],
    types: ["INTEGER", "TEXT", "INTEGER", "INTEGER", "INTEGER", "TEXT", "INTEGER"],
  },
  {
    name: CHANGESET_BOOTSTRAP_CHUNKS_TABLE,
    columns: ["idx", "sha256", "byte_length", "change_count", "payload"],
    types: ["INTEGER", "TEXT", "INTEGER", "INTEGER", "BLOB"],
  },
] as const;
async function ensure(tx: ChangesetExecutor, b: Budget, create: boolean): Promise<boolean> {
  const present = await query(
    tx,
    b,
    "SELECT name FROM main.sqlite_schema WHERE name IN (?,?) COLLATE NOCASE",
    [CHANGESET_BOOTSTRAP_STATE_TABLE, CHANGESET_BOOTSTRAP_CHUNKS_TABLE],
  );
  if (!present.length && !create) return false;
  if (present.length !== 0 && present.length !== 2)
    fail("SCHEMA", "Partial bootstrap storage schema");
  for (const layout of layouts) {
    if (!present.length) {
      const columns = layout.columns.map(
        (c, i) => `${quote(c)} ${layout.types[i]} ${i === 0 ? "PRIMARY KEY" : "NOT NULL"}`,
      );
      b.check();
      await tx.execute(`CREATE TABLE main.${quote(layout.name)} (${columns.join(", ")})`);
      b.check();
    }
    const listed = (await query(tx, b, `PRAGMA main.table_list(${literal(layout.name)})`)).filter(
      (r) => r[0] === "main" && r[1] === layout.name,
    );
    if (
      listed.length !== 1 ||
      listed[0]![2] !== "table" ||
      sqlNumber(listed[0]![3], 7) !== layout.columns.length ||
      sqlNumber(listed[0]![4], 1) !== 0
    )
      fail("SCHEMA", "Invalid bootstrap storage table");
    const info = await query(tx, b, `PRAGMA main.table_xinfo(${literal(layout.name)})`);
    if (info.length !== layout.columns.length) fail("SCHEMA", "Invalid bootstrap storage columns");
    for (let i = 0; i < info.length; i++) {
      const r = info[i]!;
      if (
        sqlNumber(r[0], 7) !== i ||
        r[1] !== layout.columns[i] ||
        typeof r[2] !== "string" ||
        r[2].toUpperCase() !== layout.types[i] ||
        (i > 0 && sqlNumber(r[3], 1) !== 1) ||
        r[4] !== null ||
        sqlNumber(r[5], 1) !== (i === 0 ? 1 : 0) ||
        sqlNumber(r[6], 3) !== 0
      )
        fail("SCHEMA", "Invalid bootstrap storage column definition");
    }
    if (
      (await query(tx, b, `PRAGMA main.index_list(${literal(layout.name)})`)).length ||
      (await query(tx, b, `PRAGMA main.foreign_key_list(${literal(layout.name)})`)).length
    )
      fail("SCHEMA", "Unexpected bootstrap storage indexes or foreign keys");
    await noTriggers(tx, b, layout.name);
  }
  return true;
}
async function noTriggers(tx: ChangesetExecutor, b: Budget, table: string): Promise<void> {
  for (const ns of ["main", "temp"]) {
    if (
      (
        await query(
          tx,
          b,
          `SELECT name FROM ${ns}.sqlite_schema WHERE type='trigger' AND tbl_name=? COLLATE NOCASE LIMIT 1`,
          [table],
        )
      ).length
    )
      fail("SCHEMA", "Bootstrap target/storage triggers are not supported");
  }
}
interface Stored extends BootstrapProgress {
  chain: string;
}
function progress(s: Stored): BootstrapProgress {
  return Object.freeze({
    receivedChunks: s.receivedChunks,
    receivedBytes: s.receivedBytes,
    receivedChanges: s.receivedChanges,
    installed: s.installed,
  });
}
async function state(
  tx: ChangesetExecutor,
  b: Budget,
  m: BootstrapManifest,
): Promise<Stored | null> {
  const rows = await query(
    tx,
    b,
    `SELECT id, CASE WHEN length(CAST(manifest AS BLOB))<=131072 THEN manifest END, received, bytes, changes, ` +
      `CASE WHEN length(chain)=64 THEN chain END, installed FROM ${STATE} LIMIT 2`,
  );
  if (!rows.length) {
    if ((await query(tx, b, `SELECT 1 FROM ${CHUNKS} LIMIT 1`)).length)
      fail("CORRUPT", "Orphan bootstrap chunks");
    return null;
  }
  if (rows.length !== 1 || rows[0]!.length !== 7 || sqlNumber(rows[0]![0], 1) !== 1)
    fail("CORRUPT", "Invalid bootstrap state row");
  const r = rows[0]!;
  if (r[1] !== JSON.stringify(m))
    fail("STATE", "Receiver is bound to a different bootstrap manifest; do not reseed");
  const s = {
    receivedChunks: sqlNumber(r[2], m.chunks),
    receivedBytes: sqlNumber(r[3], m.byteLength),
    receivedChanges: sqlNumber(r[4], m.changes),
    chain: digest(r[5]),
    installed: sqlNumber(r[6], 1) === 1,
  };
  if (
    (s.installed && s.receivedChunks !== m.chunks) ||
    (s.receivedChunks === m.chunks &&
      (s.receivedBytes !== m.byteLength || s.receivedChanges !== m.changes || s.chain !== m.sha256))
  )
    fail("CORRUPT", "Incomplete bootstrap decision or invalid totals");
  return s;
}
async function chunkMeta(tx: ChangesetExecutor, b: Budget, index: number) {
  const rows = await query(
    tx,
    b,
    `SELECT CASE WHEN length(sha256)=64 THEN sha256 END, byte_length, change_count, typeof(payload), length(payload) FROM ${CHUNKS} WHERE idx=?`,
    [BigInt(index)],
  );
  if (rows.length !== 1 || rows[0]!.length !== 5 || rows[0]![3] !== "blob")
    fail("CORRUPT", "Missing or invalid staged chunk");
  const r = rows[0]!;
  return {
    sha256: digest(r[0]),
    byteLength: sqlNumber(r[1], HARD_CHUNK),
    changes: sqlNumber(r[2], 100_000),
    storedBytes: sqlNumber(r[4], HARD_CHUNK),
  };
}
async function emptyTargets(tx: ChangesetExecutor, b: Budget, m: BootstrapManifest): Promise<void> {
  for (const table of m.tables) {
    const rows = (await query(tx, b, `PRAGMA main.table_list(${literal(table)})`)).filter(
      (r) => r[0] === "main" && typeof r[1] === "string" && fold(r[1]) === table,
    );
    if (rows.length !== 1 || rows[0]![2] !== "table")
      fail("SCHEMA", "Bootstrap needs existing ordinary destination tables");
    const info = await query(tx, b, `PRAGMA main.table_xinfo(${literal(table)})`);
    if (
      !info.length ||
      info.length > 2000 ||
      info.length !== sqlNumber(rows[0]![3], 2000) ||
      info.some((r) => sqlNumber(r[6], 3) !== 0) ||
      !info.some((r) => sqlNumber(r[5], 2000) > 0)
    )
      fail("SCHEMA", "Bootstrap needs visible columns and declared primary keys");
    await noTriggers(tx, b, table);
    if ((await query(tx, b, `SELECT 1 FROM main.${quote(table)} LIMIT 1`)).length)
      fail("STATE", "Initial bootstrap refuses nonempty destination tables");
  }
}

/**
 * One persistent bootstrap per database. stage() stores bytes, never user rows.
 * install() rechecks all chunks and applies them plus the installed decision in
 * ONE top-level SQL transaction, then confirms storage. Use a trusted top-level
 * ChangesetTarget, not an enclosing savepoint that could roll back after ACK.
 */
export class ChangesetBootstrapReceiver {
  readonly #target: ChangesetTarget;
  readonly #id: string;
  readonly #tables: readonly string[];
  readonly #confirm: () => Promise<unknown>;
  readonly #chunkBytes: number;
  readonly #bytes: number;
  readonly #chunks: number;
  readonly #changes: number;
  #active = false;
  constructor(target: ChangesetTarget, options: BootstrapReceiverOptions) {
    this.#target = target;
    this.#id = text(options?.receiverId, 256);
    this.#tables = tableNames(options?.tables);
    const confirm = options?.confirmCommit;
    if (typeof confirm !== "function")
      fail("INPUT", "Bootstrap requires same-target storage confirmation");
    this.#confirm = confirm;
    this.#chunkBytes = number(options.maxChunkBytes ?? 8 * 1024 * 1024, HARD_CHUNK, 1);
    this.#bytes = number(options.maxBytes ?? 256 * 1024 * 1024, HARD_BYTES, 1);
    this.#chunks = number(options.maxChunks ?? 10_000, 100_000, 1);
    this.#changes = number(options.maxChanges ?? 1_000_000, 10_000_000, 1);
  }
  get receiverId(): string {
    return this.#id;
  }
  #admit(value: unknown): BootstrapManifest {
    const m = captureManifest(value);
    if (m.receiverId !== this.#id || m.tables.some((t) => !this.#tables.includes(t)))
      fail("INPUT", "Bootstrap recipient or table is not authorized");
    if (m.byteLength > this.#bytes || m.chunks > this.#chunks || m.changes > this.#changes)
      fail("LIMIT", "Bootstrap manifest exceeds receiver limits");
    return m;
  }
  async #run<T>(
    options: BootstrapOperationOptions | undefined,
    work: (b: Budget) => Promise<T>,
  ): Promise<T> {
    if (this.#active) fail("BUSY", "Bootstrap receiver is active; no request was queued");
    this.#active = true;
    let b: Budget | undefined;
    try {
      b = new Budget(options);
      b.check();
      return await work(b);
    } finally {
      b?.finish();
      this.#active = false;
    }
  }
  async status(
    manifest: BootstrapManifest,
    options?: BootstrapOperationOptions,
  ): Promise<BootstrapProgress | null> {
    const m = this.#admit(manifest);
    return this.#run(options, (b) =>
      this.#target.transaction(async (tx) => {
        if (!(await ensure(tx, b, false))) return null;
        const s = await state(tx, b, m);
        return s === null ? null : progress(s);
      }, b.options()),
    );
  }
  /** Contiguous, restartable upload; this result NEVER authorizes source payload reclamation. */
  async stage(
    manifest: BootstrapManifest,
    index: number,
    bytes: Uint8Array,
    options?: BootstrapOperationOptions,
  ): Promise<BootstrapProgress> {
    const m = this.#admit(manifest);
    number(index, m.chunks - 1);
    // Admission precedes copying: overlapping requests cannot allocate queued payloads.
    if (this.#active) fail("BUSY", "Bootstrap receiver is active; no request was queued");
    const chunk = owned(bytes, this.#chunkBytes);
    return this.#run(options, async (b) => {
      const changes = inspect(chunk, m),
        sha256 = await hash(chunk);
      b.check();
      return this.#target.transaction(async (tx) => {
        await ensure(tx, b, true);
        let s = await state(tx, b, m);
        if (s === null) {
          if (index !== 0) fail("STATE", "Start bootstrap at chunk zero");
          s = {
            receivedChunks: 0,
            receivedBytes: 0,
            receivedChanges: 0,
            chain: await seed(m),
            installed: false,
          };
          await write(
            tx,
            b,
            `INSERT OR ABORT INTO ${STATE} VALUES (1,?,0,0,0,?,0)`,
            [JSON.stringify(m), s.chain],
            1,
          );
        }
        if (index < s.receivedChunks) {
          const prior = await chunkMeta(tx, b, index);
          if (
            prior.sha256 !== sha256 ||
            prior.byteLength !== chunk.byteLength ||
            prior.changes !== changes ||
            prior.storedBytes !== (s.installed ? 0 : prior.byteLength)
          )
            fail("CORRUPT", "Staged chunk identity was reused or damaged");
          return progress(s);
        }
        if (s.installed || index !== s.receivedChunks)
          fail("STATE", "Stage the next contiguous chunk; installed baselines cannot change");
        const totalBytes = s.receivedBytes + chunk.byteLength,
          totalChanges = s.receivedChanges + changes;
        if (totalBytes > m.byteLength || totalChanges > m.changes)
          fail("STATE", "Chunk exceeds manifest totals");
        const chain = await link(s.chain, index, sha256, chunk.byteLength, changes);
        b.check();
        if (
          index + 1 === m.chunks &&
          (totalBytes !== m.byteLength || totalChanges !== m.changes || chain !== m.sha256)
        )
          fail("CORRUPT", "Complete upload does not match the manifest hash and totals");
        const params: ChangesetValue[] = [
          BigInt(index),
          sha256,
          BigInt(chunk.byteLength),
          BigInt(changes),
        ];
        if (chunk.byteLength) params.push(chunk);
        await write(
          tx,
          b,
          `INSERT OR ABORT INTO ${CHUNKS} VALUES (?,?,?,?,${chunk.byteLength ? "?" : "X''"})`,
          params,
          1,
        );
        await write(
          tx,
          b,
          `UPDATE OR ABORT ${STATE} SET received=?, bytes=?, changes=?, chain=? WHERE id=1 AND received=? AND installed=0`,
          [BigInt(index + 1), BigInt(totalBytes), BigInt(totalChanges), chain, BigInt(index)],
          1,
        );
        return progress({
          receivedChunks: index + 1,
          receivedBytes: totalBytes,
          receivedChanges: totalChanges,
          chain,
          installed: false,
        });
      }, b.options());
    });
  }
  async install(
    manifest: BootstrapManifest,
    options?: BootstrapOperationOptions,
  ): Promise<BootstrapInstallReceipt> {
    const m = this.#admit(manifest);
    return this.#run(options, async (b) => {
      const replayed = await this.#target.transaction(async (tx) => {
        if (!(await ensure(tx, b, false))) fail("STATE", "No staged bootstrap");
        const s = await state(tx, b, m);
        if (s === null || s.receivedChunks !== m.chunks)
          fail("STATE", "The complete bootstrap must be staged before installation");
        if (s.installed) return true;
        await emptyTargets(tx, b, m);
        const count = await query(tx, b, `SELECT count(*) FROM ${CHUNKS}`);
        if (
          count.length !== 1 ||
          count[0]!.length !== 1 ||
          sqlNumber(count[0]![0], m.chunks) !== m.chunks
        )
          fail("CORRUPT", "Unexpected staged chunk population");
        let chain = await seed(m),
          bytes = 0,
          changes = 0;
        // The outer target owns rollback/commit. Never commit chunks separately,
        // and never create per-chunk inbox receipts that could mask partial data.
        const inside: ChangesetTarget = { transaction: async (work) => work(tx) };
        for (let i = 0; i < m.chunks; i++) {
          const meta = await chunkMeta(tx, b, i);
          if (meta.storedBytes !== meta.byteLength || meta.byteLength > this.#chunkBytes)
            fail("CORRUPT", "Invalid staged payload length");
          const rows = await query(
            tx,
            b,
            `SELECT payload FROM ${CHUNKS} WHERE idx=? AND typeof(payload)='blob' AND length(payload)=?`,
            [BigInt(i), BigInt(meta.byteLength)],
          );
          if (rows.length !== 1 || rows[0]!.length !== 1)
            fail("CORRUPT", "Staged chunk disappeared");
          const chunk = owned(rows[0]![0], this.#chunkBytes),
            n = inspect(chunk, m);
          if (
            chunk.byteLength !== meta.byteLength ||
            n !== meta.changes ||
            (await hash(chunk)) !== meta.sha256
          )
            fail("CORRUPT", "Staged payload failed verification");
          chain = await link(chain, i, meta.sha256, meta.byteLength, n);
          bytes += meta.byteLength;
          changes += n;
          b.check();
          const result = await applyChangeset(inside, chunk, { tables: m.tables, ...b.options() });
          b.check();
          if (result.applied !== n || result.omitted !== 0 || result.replayed)
            fail("CORRUPT", "Bootstrap row application was incomplete");
        }
        if (chain !== m.sha256 || bytes !== m.byteLength || changes !== m.changes)
          fail("CORRUPT", "Staged bootstrap digest/totals mismatch");
        await write(tx, b, `UPDATE OR ABORT ${CHUNKS} SET payload=X''`, [], m.chunks);
        await write(
          tx,
          b,
          `UPDATE OR ABORT ${STATE} SET installed=1 WHERE id=1 AND installed=0`,
          [],
          1,
        );
        return false;
      }, b.options());
      // SQL may have committed even when cancellation arrived during COMMIT.
      // Always drain confirmation after a successful transaction, including replay.
      try {
        await this.#confirm();
      } catch (cause: unknown) {
        throw new ChangesetBootstrapError(
          "CONFIRM",
          "SQL installation exists but storage confirmation failed; retry this manifest, never reseed",
          { cause },
        );
      }
      b.check();
      return Object.freeze({
        protocol: CHANGESET_BOOTSTRAP_PROTOCOL,
        receiverId: m.receiverId,
        deliveryId: m.deliveryId,
        sha256: m.sha256,
        chunks: m.chunks,
        changes: m.changes,
        byteLength: m.byteLength,
        installed: true,
        confirmed: true,
        replayed,
      });
    });
  }
}
