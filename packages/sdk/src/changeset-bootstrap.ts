import type { ChangesetExecutor, ChangesetTarget } from "./changeset-apply";
import { applyChangeset } from "./changeset-apply";
import type { ChangesetValue } from "./changeset-codec";
import { decodeChangeset } from "./changeset-codec";
import { bootstrapOrderPrefix } from "./changeset-order";
import { acknowledgeFanoutBootstrapPrefix, assertSingleRecipient } from "./changeset-fanout";
import {
  TABLE as OUTBOX,
  chunkId,
  ensure as ensureOutbox,
  find as findOutboxEntry,
  inspectStream,
  load as loadOutboxPayload,
} from "./changeset-outbox-store";

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
/** Receiver route and table order chosen before the original seed transfer. */
export type BootstrapSourceManifestInput = Pick<
  BootstrapManifestInput, "receiverId" | "deliveryId" | "tables"
>;
export interface BootstrapOperationOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
}
export interface BootstrapAcknowledgeOptions extends BootstrapOperationOptions {
  /** Trusted source route, not a receiver identity copied from an incoming ACK. */
  receiverId: string;
  /** Require the installed order prefix for this exact source incarnation. */
  orderedSourceId?: string;
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
  /** Present only when the seed and its ordered prefix committed together. */
  readonly order?: Readonly<{
    protocol: "fsqlite-ordered-changeset-v1";
    streamId: string;
    /** Last seed sequence, not the current incremental tip. */
    sequence: string;
  }>;
}
export interface BootstrapReceiverOptions {
  receiverId: string;
  /** Fixed direct-target authority, checked on every operation, including replay. */
  tables: readonly string[];
  /** SAME top-level database; called after installation and every installed replay. */
  confirmCommit: () => Promise<unknown>;
  /**
   * Trusted source incarnation for an outbox seed occupying sequences 1..N.
   * Bind before the FIRST stage and preserve on reopen. Never inferred from
   * incoming data. Installation publishes the prefix in the SAME transaction.
   */
  orderedSourceId?: string;
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
/**
 * Build or recover the SAME receiver-specific manifest from a retained source
 * bootstrap, never by snapshotting current application rows. Pending bodies are
 * verified one at a time; acknowledged chunks use their retained identities.
 * Reads one source transaction, creates no storage, changes no cursors and
 * proves neither receiver installation nor source commit durability.
 */
export async function readBootstrapManifest(
  source: ChangesetTarget,
  input: BootstrapSourceManifestInput,
  options?: BootstrapOperationOptions,
): Promise<BootstrapManifest> {
  const route = Object.freeze({
    receiverId: text(data(input, "receiverId"), 256),
    deliveryId: text(data(input, "deliveryId"), 480),
    tables: tableNames(data(input, "tables")),
  });
  if (typeof source?.transaction !== "function") fail("INPUT", "A source transaction owner is required");
  const b = new Budget(options);
  try {
    b.check();
    const result = await source.transaction(async executor => {
      const retained = await inspectSourceBootstrap(sourceExecutor(executor, b), b, route);
      return retained.manifest;
    }, b.options());
    b.check();
    return result;
  } finally {
    b.finish();
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
function storedManifest(m: BootstrapManifest, orderedSourceId: string | undefined): string {
  // Local policy is separate from the portable bootstrap wire hash. Persist it
  // with the manifest so restart cannot silently downgrade an ordered install.
  return JSON.stringify(orderedSourceId === undefined ? m : { ...m, orderedSourceId });
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
  orderedSourceId: string | undefined,
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
  if (r[1] !== storedManifest(m, orderedSourceId))
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
  readonly #orderedSourceId: string | undefined;
  #active = false;
  constructor(target: ChangesetTarget, options: BootstrapReceiverOptions) {
    this.#target = target;
    this.#id = text(options?.receiverId, 256);
    this.#tables = tableNames(options?.tables);
    const confirm = options?.confirmCommit;
    if (typeof confirm !== "function")
      fail("INPUT", "Bootstrap requires same-target storage confirmation");
    this.#confirm = confirm;
    const orderedSourceId = options.orderedSourceId;
    this.#orderedSourceId = orderedSourceId === undefined ? undefined : text(orderedSourceId, 256);
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
        const s = await state(tx, b, m, this.#orderedSourceId);
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
        let s = await state(tx, b, m, this.#orderedSourceId);
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
            [storedManifest(m, this.#orderedSourceId), s.chain],
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
        const s = await state(tx, b, m, this.#orderedSourceId);
        if (s === null || s.receivedChunks !== m.chunks)
          fail("STATE", "The complete bootstrap must be staged before installation");
        if (s.installed) {
          await this.#orderPrefix(tx, b, m, true);
          return true;
        }
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
        await this.#orderPrefix(tx, b, m, false);
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
        ...(this.#orderedSourceId === undefined ? {} : {
          order: Object.freeze({
            protocol: "fsqlite-ordered-changeset-v1" as const,
            streamId: this.#orderedSourceId,
            sequence: String(m.chunks),
          }),
        }),
      });
    });
  }

  /** Verify retained chunk metadata even after bodies have been reclaimed. */
  async #orderPrefix(tx: ChangesetExecutor, b: Budget, m: BootstrapManifest, replayed: boolean): Promise<void> {
    if (this.#orderedSourceId === undefined) return;
    const population = await query(tx, b, `SELECT count(*) FROM ${CHUNKS}`);
    if (population.length !== 1 || population[0]!.length !== 1 ||
        sqlNumber(population[0]![0], m.chunks) !== m.chunks)
      fail("CORRUPT", "Bootstrap order prefix has missing or extra chunks");
    let chain = await seed(m), bytes = 0, changes = 0;
    await bootstrapOrderPrefix(tx, {
      receiverId: this.#id,
      sourceId: this.#orderedSourceId,
      deliveryId: m.deliveryId,
      chunks: m.chunks,
      replayed,
    }, async index => {
      const meta = await chunkMeta(tx, b, index);
      if (meta.storedBytes !== (replayed ? 0 : meta.byteLength) || meta.byteLength > this.#chunkBytes)
        fail("CORRUPT", "Bootstrap order prefix has invalid payload metadata");
      chain = await link(chain, index, meta.sha256, meta.byteLength, meta.changes);
      bytes += meta.byteLength;
      changes += meta.changes;
      if (bytes > m.byteLength || changes > m.changes)
        fail("CORRUPT", "Bootstrap order prefix exceeds manifest totals");
      return meta;
    }, () => b.check());
    b.check();
    if (chain !== m.sha256 || bytes !== m.byteLength || changes !== m.changes)
      fail("CORRUPT", "Bootstrap order prefix failed manifest verification");
  }
}

/** Check cancellation at every source-storage SQL boundary without racing it. */
function sourceExecutor(executor: ChangesetExecutor, b: Budget): ChangesetExecutor {
  return {
    execute: async (sql, params) => {
      b.check();
      const changed = await executor.execute(sql, params);
      b.check();
      return changed;
    },
    query: async (sql, params) => {
      b.check();
      const rows = await executor.query(sql, params);
      b.check();
      return rows;
    },
  };
}

/** Shared retained-source proof for manifest recovery and install ACK admission. */
async function inspectSourceBootstrap(
  tx: ChangesetExecutor,
  b: Budget,
  route: BootstrapSourceManifestInput,
  expected?: BootstrapManifest,
) {
  if (!(await ensureOutbox(tx, false))) fail("STATE", "No retained source bootstrap");
  const root = await findOutboxEntry(tx, route.deliveryId);
  if (root === null || root.stream?.index !== 0 || root.stream.summary === null)
    fail("STATE", "The original source bootstrap manifest is no longer retained");
  const scope = JSON.parse(root.scope) as { tables: string[] };
  if (JSON.stringify(scope.tables) !== JSON.stringify([...route.tables].sort()))
    fail("STATE", "Bootstrap manifest has a different source table scope");
  const before = await inspectStream(tx, root, () => b.check(), false);
  if (expected !== undefined && (before.chunks !== expected.chunks ||
      before.changes !== expected.changes || before.byteLength !== expected.byteLength))
    fail("STATE", "Install acknowledgement has different retained source totals");
  const input = captureInput({ ...route, chunks: before.chunks, changes: before.changes, byteLength: before.byteLength });
  let chain = await seed(input);
  for (let i = 0; i < input.chunks; i++) {
    b.check();
    const entry = await findOutboxEntry(tx, chunkId(input.deliveryId, i));
    if (entry === null || entry.stream?.id !== input.deliveryId || entry.stream.index !== i ||
        entry.stream.base !== root.stream.base)
      fail("CORRUPT", "Source bootstrap contains missing or foreign prefix entries");
    // Already acknowledged bodies are gone; their retained hashes still bind
    // replay. A pending body must pass the existing size/hash/codec checks.
    await loadOutboxPayload(tx, entry);
    const d = entry.delivery;
    chain = await link(chain, i, d.sha256, d.byteLength, d.changes);
  }
  b.check();
  if (expected !== undefined && chain !== expected.sha256)
    fail("CORRUPT", "Install manifest does not match the complete source prefix");
  const manifest: BootstrapManifest = Object.freeze({ protocol: CHANGESET_BOOTSTRAP_PROTOCOL, ...input, sha256: chain });
  return { root, before, manifest };
}

/**
 * Reclaim a single-recipient source seed after its complete, authenticated install
 * ACK. Pass the ORIGINAL outbound manifest and trusted route, not a manifest
 * reconstructed from the incoming receipt. No network or receiver SQL runs here.
 *
 * Verify source metadata and pending bodies in one snapshot, then acknowledge the
 * entire remaining prefix in that SAME transaction. Keep identity tombstones and
 * later incremental entries. Return newly acknowledged chunks; zero is an exact
 * retained replay, not permission to forget history. Snapshot sources must still
 * confirm their own commit. Fanout sources require per-replica acknowledgement.
 */
export async function acknowledgeBootstrapInstall(
  source: ChangesetTarget,
  manifest: BootstrapManifest,
  receipt: BootstrapInstallReceipt,
  options: BootstrapAcknowledgeOptions,
): Promise<number> {
  return acknowledgeInstall(source, manifest, receipt, options, false);
}

/**
 * Accept one required replica's complete install ACK on a fanout source. The
 * original receiver-bound manifest and trusted route are mandatory, just as for
 * acknowledgeBootstrapInstall(). Only that replica advances, without rewinding
 * newer incremental progress. Payload reclamation follows the minimum required
 * replica cursor, not this receiver's installation frontier.
 *
 * Return seed sequences newly acknowledged by THIS replica, not reclaimed rows
 * or bytes. Cursor advancement and any reclamation commit together. A retained
 * replay returns zero; missing history is an error. Confirm the same source's
 * storage before treating this SQL result as durable. No network runs here.
 */
export async function acknowledgeFanoutBootstrapInstall(
  source: ChangesetTarget,
  manifest: BootstrapManifest,
  receipt: BootstrapInstallReceipt,
  options: BootstrapAcknowledgeOptions,
): Promise<number> {
  return acknowledgeInstall(source, manifest, receipt, options, true);
}

async function acknowledgeInstall(
  source: ChangesetTarget,
  manifest: BootstrapManifest,
  receipt: BootstrapInstallReceipt,
  options: BootstrapAcknowledgeOptions,
  fanout: boolean,
): Promise<number> {
  const m = captureManifest(manifest);
  const receiverId = text(options?.receiverId, 256);
  const requestedSource = options.orderedSourceId;
  const orderedSourceId = requestedSource === undefined ? undefined : text(requestedSource, 256);
  if (typeof source?.transaction !== "function" || m.receiverId !== receiverId)
    fail("INPUT", "Use the source transaction owner and its trusted bootstrap receiver route");
  for (const key of ["protocol", "receiverId", "deliveryId", "sha256", "chunks", "changes", "byteLength"] as const) {
    if (data(receipt, key) !== m[key])
      fail("STATE", "Install acknowledgement does not match the original bootstrap manifest");
  }
  if (data(receipt, "installed") !== true || data(receipt, "confirmed") !== true ||
      typeof data(receipt, "replayed") !== "boolean")
    fail("STATE", "Only a confirmed complete installation can reclaim a source seed");
  if (orderedSourceId === undefined) {
    if ("order" in receipt)
      fail("INPUT", "Configure orderedSourceId before accepting an ordered install acknowledgement");
  } else {
    const order = data(receipt, "order");
    if (data(order, "protocol") !== "fsqlite-ordered-changeset-v1" ||
        data(order, "streamId") !== orderedSourceId || data(order, "sequence") !== String(m.chunks))
      fail("STATE", "Install acknowledgement does not confirm the expected source prefix");
  }
  // All receipt fields and route policy were captured before the first await.
  const b = new Budget(options);
  try {
    b.check();
    return await source.transaction(async executor => {
      const tx = sourceExecutor(executor, b);
      if (!fanout) await assertSingleRecipient(tx);
      const { root, before } = await inspectSourceBootstrap(tx, b, m, m);
      if (fanout) {
        const advanced = await acknowledgeFanoutBootstrapPrefix(tx, receiverId, root, () => b.check());
        b.check();
        return advanced;
      }
      const changed = m.chunks - before.acknowledgedChunks;
      if (changed === 0) return 0;
      await write(tx, b,
        `UPDATE OR ABORT ${OUTBOX} SET acknowledged=1,payload=X'' WHERE seq>=1 AND seq<=? AND acknowledged=0`,
        [BigInt(m.chunks)], changed);
      const after = await inspectStream(tx, root, () => b.check(), false);
      if (!after.complete || after.chunks !== before.chunks || after.changes !== before.changes ||
          after.byteLength !== before.byteLength || after.sha256 !== before.sha256)
        fail("CORRUPT", "Source bootstrap acknowledgement was not retained atomically");
      b.check();
      return changed;
    }, b.options());
  } finally {
    b.finish();
  }
}
