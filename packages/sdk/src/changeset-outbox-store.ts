import type { ChangesetExecutor } from "./changeset-apply";
import type {
  CaptureChangesetOptions,
  ChangesetSnapshot,
  ChangesetSnapshotStreamResult,
  SnapshotChangesetOptions,
  SnapshotChangesetStreamOptions,
} from "./changeset-capture";
import type { ChangesetValue } from "./changeset-codec";
import { decodeChangeset } from "./changeset-codec";

export const CHANGESET_OUTBOX_TABLE = "__fsqlite_changeset_outbox";
export interface ChangesetOutboxOptions {
  /** All retained identities, including acknowledged ones. Default 10,000. */
  maxEntries?: number;
  /** Retained pending payload bytes, not SQL-file or heap size. Default 64 MiB. */
  maxPayloadBytes?: number;
}
export interface OutboxDelivery {
  readonly sequence: bigint;
  readonly deliveryId: string;
  readonly sha256: string;
  readonly byteLength: number;
  readonly changes: number;
  readonly acknowledged: boolean;
}
export interface OutboxRecordOptions extends CaptureChangesetOptions {
  /** Stable, globally source-qualified operation ID. Never reuse for other work. */
  deliveryId: string;
}
export interface OutboxBootstrapOptions extends SnapshotChangesetOptions {
  /** Stable source-qualified seed identity, distinct from incremental operations. */
  deliveryId: string;
}
export interface OutboxBootstrapResult {
  readonly replayed: boolean;
  readonly delivery: OutboxDelivery;
}
export interface OutboxBootstrapChunksOptions extends SnapshotChangesetStreamOptions {
  /** Root operation identity, at most 480 UTF-8 bytes. Reserves its /chunk/ suffixes. */
  deliveryId: string;
}
export interface OutboxBootstrapChunksResult extends ChangesetSnapshotStreamResult {
  readonly replayed: boolean;
  readonly deliveryId: string;
  /** Digest of the first chunk, used with the exact root ID for explicit cleanup. */
  readonly sha256: string;
  readonly firstSequence: bigint;
  readonly lastSequence: bigint;
  readonly acknowledgedChunks: number;
  /** All source acknowledgement flags are set; NOT a checkpoint or visibility proof. */
  readonly complete: boolean;
}
export type OutboxRecordResult<T> =
  | { readonly replayed: false; readonly value: T; readonly delivery: OutboxDelivery }
  | { readonly replayed: true; readonly delivery: OutboxDelivery };
export interface OutboxReadResult {
  readonly delivery: OutboxDelivery;
  /** Acknowledged payloads are reclaimed, but their operation IDs remain. */
  readonly changeset: Uint8Array | null;
}
export interface OutboxPageOptions {
  after?: bigint;
  limit?: number;
}
export class ChangesetOutboxError extends Error {
  constructor(
    readonly code:
      | "ERR_FSQLITE_OUTBOX_INPUT"
      | "ERR_FSQLITE_OUTBOX_SCHEMA"
      | "ERR_FSQLITE_OUTBOX_CORRUPT"
      | "ERR_FSQLITE_OUTBOX_FULL"
      | "ERR_FSQLITE_OUTBOX_REUSE"
      | "ERR_FSQLITE_OUTBOX_ACK"
      | "ERR_FSQLITE_OUTBOX_STATE",
    message: string,
  ) {
    super(message);
    this.name = "ChangesetOutboxError";
  }
}
const TABLE = `main."${CHANGESET_OUTBOX_TABLE}"`;
const COLUMNS =
  "seq INTEGER PRIMARY KEY AUTOINCREMENT, delivery_id TEXT NOT NULL UNIQUE COLLATE BINARY, " +
  "sha256 TEXT NOT NULL, byte_length INTEGER NOT NULL, change_count INTEGER NOT NULL, " +
  "scope TEXT NOT NULL, acknowledged INTEGER NOT NULL, payload BLOB NOT NULL";
const COLUMN_NAMES = [
  "seq",
  "delivery_id",
  "sha256",
  "byte_length",
  "change_count",
  "scope",
  "acknowledged",
  "payload",
];
const COLUMN_TYPES = ["INTEGER", "TEXT", "TEXT", "INTEGER", "INTEGER", "TEXT", "INTEGER", "BLOB"];
const META =
  "CAST(seq AS TEXT) AS seq, " +
  "CASE WHEN length(CAST(delivery_id AS BLOB)) <= 1024 AND instr(delivery_id,char(0))=0 THEN delivery_id END AS delivery_id, " +
  "CASE WHEN length(sha256) = 64 AND instr(sha256,char(0))=0 THEN sha256 END AS sha256, byte_length, change_count, " +
  "CASE WHEN length(CAST(scope AS BLOB)) <= 262144 AND instr(scope,char(0))=0 THEN scope END AS scope, acknowledged, " +
  "typeof(payload) AS payload_type, length(payload) AS payload_length";
const fold = (s: string): string => s.replace(/[A-Z]/g, (c) => c.toLowerCase());
function fail(
  kind: "INPUT" | "SCHEMA" | "CORRUPT" | "FULL" | "REUSE" | "ACK" | "STATE",
  message: string,
): never {
  throw new ChangesetOutboxError(`ERR_FSQLITE_OUTBOX_${kind}`, message);
}
function identity(value: unknown): string {
  if (typeof value !== "string" || !value.length || value.length > 512 || value.includes("\0"))
    fail("INPUT", "deliveryId must be valid UTF-8 of 1..512 bytes without NUL");
  const bytes = new TextEncoder().encode(value);
  if (bytes.length > 512 || new TextDecoder("utf-8", { ignoreBOM: true }).decode(bytes) !== value)
    fail("INPUT", "Invalid delivery identity UTF-8");
  return value;
}
function digest(value: unknown): string {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value))
    fail("CORRUPT", "Invalid outbox SHA-256");
  return value;
}
function integer(value: unknown): number {
  if (typeof value === "bigint" && value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER))
    return Number(value);
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return value;
  return fail("CORRUPT", "Invalid outbox integer");
}
function bound(value: unknown, fallback: number, maximum: number): number {
  const n = value ?? fallback;
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 1 || n > maximum)
    fail("INPUT", `Outbox limit must be an integer in 1..${maximum}`);
  return n;
}
async function hash(bytes: Uint8Array): Promise<string> {
  if (globalThis.crypto?.subtle === undefined)
    fail("INPUT", "The outbox requires Web Crypto SHA-256");
  const sum = new Uint8Array(
    await globalThis.crypto.subtle.digest("SHA-256", new Uint8Array(bytes)),
  );
  return Array.from(sum, (b) => b.toString(16).padStart(2, "0")).join("");
}
async function query(tx: ChangesetExecutor, sql: string, params: readonly ChangesetValue[] = []) {
  const result = await tx.query(sql, params);
  if (!Array.isArray(result.rowArrays) || result.rowArrays.some((row) => !Array.isArray(row)))
    fail("CORRUPT", "Invalid outbox SQL result");
  return result.rowArrays;
}
/** Ignore quoted identifiers/strings and comments when checking the rowid rule. */
function hasAutoincrement(sql: string): boolean {
  for (let i = 0; i < sql.length; ) {
    const c = sql[i]!;
    if (c === "'" || c === '"' || c === "`" || c === "[") {
      const end = c === "[" ? "]" : c;
      i++;
      while (i < sql.length) {
        if (sql[i++] === end) {
          if (c !== "[" && sql[i] === end) {
            i++;
            continue;
          }
          break;
        }
      }
    } else if (sql.slice(i, i + 2) === "--") {
      const end = sql.indexOf("\n", i + 2);
      i = end < 0 ? sql.length : end + 1;
    } else if (sql.slice(i, i + 2) === "/*") {
      const end = sql.indexOf("*/", i + 2);
      i = end < 0 ? sql.length : end + 2;
    } else if (/[A-Za-z_]/.test(c)) {
      const start = i++;
      while (i < sql.length && /[A-Za-z0-9_]/.test(sql[i]!)) i++;
      if (sql.slice(start, i).toUpperCase() === "AUTOINCREMENT") return true;
    } else i++;
  }
  return false;
}
async function ensure(tx: ChangesetExecutor, create: boolean): Promise<boolean> {
  let rows = await query(
    tx,
    "SELECT type, sql FROM main.sqlite_schema WHERE name = ? COLLATE NOCASE",
    [CHANGESET_OUTBOX_TABLE],
  );
  if (!rows.length && create) {
    await tx.execute(`CREATE TABLE ${TABLE} (${COLUMNS})`);
    rows = await query(
      tx,
      "SELECT type, sql FROM main.sqlite_schema WHERE name = ? COLLATE NOCASE",
      [CHANGESET_OUTBOX_TABLE],
    );
  }
  if (!rows.length) return false;
  // Validate semantics, not SQL formatting: the engine may normalize CREATE.
  if (
    rows.length !== 1 ||
    rows[0]![0] !== "table" ||
    typeof rows[0]![1] !== "string" ||
    !hasAutoincrement(rows[0]![1])
  )
    fail("SCHEMA", "The outbox requires monotonic AUTOINCREMENT sequences");
  const listed = await query(tx, `PRAGMA main.table_list('${CHANGESET_OUTBOX_TABLE}')`);
  const main = listed.filter((row) => row[0] === "main" && row[1] === CHANGESET_OUTBOX_TABLE);
  if (
    main.length !== 1 ||
    main[0]![2] !== "table" ||
    integer(main[0]![3]) !== 8 ||
    integer(main[0]![4]) !== 0
  )
    fail("SCHEMA", "Invalid outbox table layout");
  const columns = await query(tx, `PRAGMA main.table_xinfo('${CHANGESET_OUTBOX_TABLE}')`);
  if (columns.length !== 8) fail("SCHEMA", "Invalid outbox columns");
  for (let i = 0; i < columns.length; i++) {
    const row = columns[i]!;
    if (
      integer(row[0]) !== i ||
      row[1] !== COLUMN_NAMES[i] ||
      typeof row[2] !== "string" ||
      row[2].toUpperCase() !== COLUMN_TYPES[i] ||
      (i > 0 && integer(row[3]) !== 1) ||
      row[4] !== null ||
      integer(row[5]) !== (i === 0 ? 1 : 0) ||
      integer(row[6]) !== 0
    )
      fail("SCHEMA", "Invalid outbox column definition");
  }
  const indexes = await query(tx, `PRAGMA main.index_list('${CHANGESET_OUTBOX_TABLE}')`);
  if (
    indexes.length !== 1 ||
    integer(indexes[0]![2]) !== 1 ||
    indexes[0]![3] !== "u" ||
    integer(indexes[0]![4]) !== 0
  )
    fail("SCHEMA", "Unexpected changeset outbox indexes");
  const indexName = indexes[0]![1];
  if (typeof indexName !== "string" || indexName.length > 1024 || indexName.includes("\0"))
    fail("SCHEMA", "Invalid outbox index name");
  const keys = (
    await query(tx, `PRAGMA main.index_xinfo('${indexName.replaceAll("'", "''")}')`)
  ).filter((row) => integer(row[5]) === 1);
  if (
    keys.length !== 1 ||
    integer(keys[0]![1]) !== 1 ||
    keys[0]![2] !== "delivery_id" ||
    keys[0]![4] !== "BINARY"
  )
    fail("SCHEMA", "Outbox delivery identities require a BINARY unique key");
  if ((await query(tx, `PRAGMA main.foreign_key_list('${CHANGESET_OUTBOX_TABLE}')`)).length)
    fail("SCHEMA", "Foreign keys on the outbox are not supported");
  for (const ns of ["main", "temp"]) {
    if (
      (
        await query(
          tx,
          `SELECT name FROM ${ns}.sqlite_schema WHERE type='trigger' AND tbl_name=? COLLATE NOCASE LIMIT 1`,
          [CHANGESET_OUTBOX_TABLE],
        )
      ).length
    )
      fail("SCHEMA", "Triggers on the changeset outbox are not supported");
  }
  return true;
}
interface StreamInfo {
  id: string;
  index: number;
  base: string;
  summary: ChangesetSnapshotStreamResult | null;
}
interface Stored {
  delivery: OutboxDelivery;
  scope: string;
  stream: StreamInfo | null;
}
function streamIdentity(value: unknown): string {
  const id = identity(value);
  if (new TextEncoder().encode(id).length > 480)
    fail("INPUT", "Chunked bootstrap identity exceeds 480 UTF-8 bytes");
  return id;
}
function chunkId(id: string, index: number): string {
  return index === 0 ? id : `${id}/chunk/${index}`;
}
function streamInfo(value: unknown, base: string, sequence: bigint, id: string): StreamInfo {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    fail("CORRUPT", "Invalid bootstrap chunk metadata");
  const v = value as { id?: unknown; index?: unknown; summary?: unknown };
  let root: string;
  try {
    root = streamIdentity(v.id);
  } catch {
    return fail("CORRUPT", "Invalid bootstrap root identity");
  }
  const index = integer(v.index);
  if (index >= 100_000 || sequence !== BigInt(index + 1) || id !== chunkId(root, index))
    fail("CORRUPT", "Bootstrap chunk identity/order mismatch");
  let summary: ChangesetSnapshotStreamResult | null = null;
  if (index === 0) {
    if (typeof v.summary !== "object" || v.summary === null || Array.isArray(v.summary))
      fail("CORRUPT", "Missing bootstrap completion manifest");
    const s = v.summary as { chunks?: unknown; changes?: unknown; byteLength?: unknown };
    const chunks = integer(s.chunks),
      changes = integer(s.changes),
      byteLength = integer(s.byteLength);
    if (chunks < 1 || chunks > 100_000 || changes > 10_000_000 || byteLength > 1024 * 1024 * 1024)
      fail("CORRUPT", "Invalid bootstrap totals");
    summary = Object.freeze({ chunks, changes, byteLength });
  } else if (v.summary !== undefined)
    fail("CORRUPT", "Only the first chunk may contain a bootstrap manifest");
  return { id: root, index, base, summary };
}
function metadata(row: readonly unknown[]): Stored {
  if (row.length !== 9 || typeof row[0] !== "string" || !/^[1-9][0-9]{0,18}$/.test(row[0]))
    fail("CORRUPT", "Invalid outbox sequence");
  const sequence = BigInt(row[0]);
  if (sequence > (1n << 63n) - 1n) fail("CORRUPT", "Outbox sequence exceeds int64");
  let deliveryId: string;
  try {
    deliveryId = identity(row[1]);
  } catch {
    return fail("CORRUPT", "Invalid stored delivery identity");
  }
  const sha256 = digest(row[2]),
    byteLength = integer(row[3]),
    changes = integer(row[4]),
    ack = integer(row[6]);
  if (
    byteLength > 64 * 1024 * 1024 ||
    changes > 100_000 ||
    ack > 1 ||
    row[7] !== "blob" ||
    integer(row[8]) !== (ack ? 0 : byteLength)
  )
    fail("CORRUPT", "Invalid outbox payload metadata");
  if (typeof row[5] !== "string" || row[5].length > 131072)
    fail("CORRUPT", "Invalid outbox capture scope");
  let scope: unknown;
  try {
    scope = JSON.parse(row[5]);
  } catch {
    return fail("CORRUPT", "Invalid outbox capture scope JSON");
  }
  if (typeof scope !== "object" || scope === null) fail("CORRUPT", "Invalid capture scope shape");
  const s = scope as { tables?: unknown; indirect?: unknown; snapshot?: unknown; stream?: unknown };
  if (s.snapshot !== undefined && s.snapshot !== true)
    fail("CORRUPT", "Invalid outbox snapshot scope");
  const tables = s.tables;
  if (
    !Array.isArray(tables) ||
    !tables.length ||
    tables.length > 64 ||
    typeof s.indirect !== "boolean" ||
    tables.some(
      (t) =>
        typeof t !== "string" ||
        !t.length ||
        t.length > 1024 ||
        t.includes("\0") ||
        t.startsWith("sqlite_") ||
        t.startsWith("__fsqlite_"),
    ) ||
    tables.some((t, i) => fold(t) !== t || (i > 0 && tables[i - 1] >= t))
  )
    fail("CORRUPT", "Invalid capture scope tables");
  if (s.stream !== undefined && s.snapshot !== true)
    fail("CORRUPT", "Bootstrap chunks must be snapshot operations");
  const stream =
    s.stream === undefined
      ? null
      : streamInfo(
          s.stream,
          JSON.stringify({ tables, indirect: s.indirect, snapshot: true }),
          sequence,
          deliveryId,
        );
  return {
    scope: row[5],
    stream,
    delivery: Object.freeze({
      sequence,
      deliveryId,
      sha256,
      byteLength,
      changes,
      acknowledged: ack === 1,
    }),
  };
}
async function find(tx: ChangesetExecutor, id: string): Promise<Stored | null> {
  const rows = await query(tx, `SELECT ${META} FROM ${TABLE} WHERE delivery_id=?`, [id]);
  if (rows.length > 1) fail("CORRUPT", "Multiple records for one delivery identity");
  return rows.length ? metadata(rows[0]!) : null;
}
async function load(tx: ChangesetExecutor, record: Stored): Promise<Uint8Array | null> {
  if (record.stream !== null) await validateStreamEntry(tx, record);
  if (record.delivery.acknowledged) return null;
  const rows = await query(
    tx,
    `SELECT payload FROM ${TABLE} WHERE delivery_id=? AND typeof(payload)='blob' AND length(payload)=?`,
    [record.delivery.deliveryId, BigInt(record.delivery.byteLength)],
  );
  if (rows.length !== 1 || rows[0]!.length !== 1 || !(rows[0]![0] instanceof Uint8Array))
    fail("CORRUPT", "Invalid outbox payload");
  const bytes = new Uint8Array(rows[0]![0]);
  if (
    bytes.byteLength !== record.delivery.byteLength ||
    (await hash(bytes)) !== record.delivery.sha256
  )
    fail("CORRUPT", "Outbox payload does not match its retained digest");
  const tables = decodeChangeset(bytes);
  if (tables.reduce((n, t) => n + t.changes.length, 0) !== record.delivery.changes)
    fail("CORRUPT", "Outbox change count disagrees with payload");
  const scope = JSON.parse(record.scope) as {
    tables: string[];
    indirect: boolean;
    snapshot?: true;
  };
  if (
    tables.some(
      (t) =>
        !scope.tables.includes(fold(t.name)) ||
        t.changes.some(
          (c) =>
            c.indirect !== scope.indirect || (scope.snapshot === true && c.operation !== "insert"),
        ),
    )
  ) {
    fail("CORRUPT", "Outbox payload disagrees with its capture scope");
  }
  return bytes;
}

async function validateStreamEntry(tx: ChangesetExecutor, entry: Stored): Promise<Stored> {
  const part = entry.stream;
  if (part === null) fail("STATE", "Not a chunked bootstrap delivery");
  const root = part.index === 0 ? entry : await find(tx, part.id);
  if (
    root === null ||
    root.stream?.summary === null ||
    root.stream?.summary === undefined ||
    root.stream.base !== part.base ||
    part.index >= root.stream.summary.chunks
  )
    fail("CORRUPT", "Bootstrap chunk has no matching complete manifest");
  return root;
}

/** One metadata page / one verified pending payload at a time, never a seed array. */
async function inspectStream(
  tx: ChangesetExecutor,
  root: Stored,
  checkpoint: () => void,
  verifyPayloads: boolean,
): Promise<Omit<OutboxBootstrapChunksResult, "replayed">> {
  const stream = root.stream,
    summary = stream?.summary;
  if (stream === null || summary === null || summary === undefined)
    fail("STATE", "Identity does not name a bootstrap manifest");
  let seen = 0,
    changes = 0,
    byteLength = 0,
    acknowledgedChunks = 0,
    pending = false;
  while (seen < summary.chunks) {
    checkpoint();
    const rows = await query(
      tx,
      `SELECT ${META} FROM ${TABLE} AS o WHERE o.seq>? AND o.seq<=? ORDER BY o.seq LIMIT 32`,
      [BigInt(seen), BigInt(summary.chunks)],
    );
    if (!rows.length || rows.length > 32)
      fail("CORRUPT", "Bootstrap manifest has missing chunks or an oversized metadata page");
    for (const row of rows) {
      const entry = metadata(row),
        part = entry.stream;
      if (
        part === null ||
        part.id !== stream.id ||
        part.index !== seen ||
        part.base !== stream.base
      )
        fail("CORRUPT", "Bootstrap manifest contains foreign or out-of-order chunks");
      if (entry.delivery.acknowledged) {
        if (pending) fail("CORRUPT", "Bootstrap acknowledgements are not a contiguous prefix");
        acknowledgedChunks++;
      } else pending = true;
      if (verifyPayloads) await load(tx, entry);
      checkpoint();
      seen++;
      changes += entry.delivery.changes;
      byteLength += entry.delivery.byteLength;
    }
  }
  const next = await query(tx, `SELECT ${META} FROM ${TABLE} WHERE seq=?`, [
    BigInt(summary.chunks + 1),
  ]);
  if (next.length > 1 || (next.length === 1 && metadata(next[0]!).stream?.id === stream.id))
    fail("CORRUPT", "Bootstrap manifest truncates its retained chunks");
  if (changes !== summary.changes || byteLength !== summary.byteLength)
    fail("CORRUPT", "Bootstrap totals disagree with retained chunks");
  checkpoint();
  return Object.freeze({
    ...summary,
    deliveryId: stream.id,
    sha256: root.delivery.sha256,
    firstSequence: 1n,
    lastSequence: BigInt(summary.chunks),
    acknowledgedChunks,
    complete: acknowledgedChunks === summary.chunks,
  });
}

/** Shared atomic publication for incremental captures and initial row snapshots. */
async function store(
  tx: ChangesetExecutor,
  id: string,
  scope: string,
  result: ChangesetSnapshot,
  checkpoint: () => void,
): Promise<OutboxDelivery> {
  const sha256 = await hash(result.changeset);
  checkpoint();
  // Some SQL adapters bind a zero-length typed array as NULL, not BLOB.
  const payload = result.changeset.byteLength === 0 ? "X''" : "?";
  const params: ChangesetValue[] = [
    id,
    sha256,
    BigInt(result.changeset.byteLength),
    BigInt(result.changes),
    scope,
  ];
  if (result.changeset.byteLength) params.push(result.changeset);
  const changed = await tx.execute(
    `INSERT OR ABORT INTO ${TABLE} (delivery_id,sha256,byte_length,change_count,scope,acknowledged,payload) VALUES (?,?,?,?,?,0,${payload})`,
    params,
  );
  checkpoint();
  if (changed !== 1) fail("CORRUPT", "Outbox insertion did not affect exactly one row");
  const saved = await find(tx, id);
  checkpoint();
  if (
    saved === null ||
    saved.delivery.sha256 !== sha256 ||
    saved.delivery.acknowledged ||
    saved.scope !== scope ||
    saved.delivery.changes !== result.changes ||
    saved.delivery.byteLength !== result.changeset.byteLength
  ) {
    fail("CORRUPT", "Outbox insertion was not confirmed");
  }
  return saved.delivery;
}


export async function pendingDeliveries(tx: ChangesetExecutor, after: bigint, limit: number): Promise<readonly OutboxDelivery[]> {
  if (!(await ensure(tx, false))) return [];
  // META exposes CAST(seq AS TEXT) AS seq to preserve int64. Unqualified
  // ORDER BY seq sorts that text alias (1,10,11,2), not the numeric key.
  const rows = await query(
    tx,
    `SELECT ${META} FROM ${TABLE} AS o WHERE acknowledged=0 AND o.seq>? ORDER BY o.seq LIMIT ?`,
    [after, BigInt(limit)],
  );
  if (rows.length > limit) fail("CORRUPT", "Outbox page exceeded its bound");
  let previous = after;
  return Object.freeze(
    rows.map((row) => {
      const item = metadata(row).delivery;
      if (item.acknowledged || item.sequence <= previous)
        fail("CORRUPT", "Invalid outbox page order");
      previous = item.sequence;
      return item;
    }),
  );
}

export async function acknowledgeDelivery(tx: ChangesetExecutor, id: string, expected: string): Promise<boolean> {
  if (!(await ensure(tx, false))) fail("ACK", "Unknown outbox delivery");
  const record = await find(tx, id);
  if (record === null || record.delivery.sha256 !== expected)
    fail("ACK", "Acknowledgement does not match a retained delivery");
  if (record.delivery.acknowledged) return false;
  if (record.stream !== null) {
    await validateStreamEntry(tx, record);
    if (record.stream.index > 0) {
      const previous = await find(tx, chunkId(record.stream.id, record.stream.index - 1));
      if (
        previous === null ||
        previous.stream?.id !== record.stream.id ||
        previous.stream.base !== record.stream.base ||
        !previous.delivery.acknowledged
      )
        fail("ACK", "Acknowledge bootstrap chunks in order, without skipping a predecessor");
    }
  }
  await load(tx, record);
  const changed = await tx.execute(
    `UPDATE OR ABORT ${TABLE} SET acknowledged=1,payload=X'' WHERE delivery_id=? AND sha256=? AND acknowledged=0`,
    [id, expected],
  );
  if (changed !== 1) fail("CORRUPT", "Outbox acknowledgement did not affect exactly one row");
  return true;
}

export async function forgetDelivery(tx: ChangesetExecutor, id: string, expected: string): Promise<boolean> {
  if (!(await ensure(tx, false))) return false;
  const record = await find(tx, id);
  if (record === null) return false;
  if (!record.delivery.acknowledged || record.delivery.sha256 !== expected)
    fail("STATE", "Only the exact acknowledged delivery may be forgotten");
  if (record.stream !== null)
    fail("STATE", "Use forgetBootstrapChunks after the entire bootstrap is acknowledged");
  const changed = await tx.execute(
    `DELETE FROM ${TABLE} WHERE delivery_id=? AND sha256=? AND acknowledged=1`,
    [id, expected],
  );
  if (changed !== 1) fail("CORRUPT", "Outbox forgetting did not affect exactly one row");
  return true;
}

export async function forgetBootstrap(tx: ChangesetExecutor, id: string, expected: string): Promise<boolean> {
  if (!(await ensure(tx, false))) return false;
  const root = await find(tx, id);
  if (root === null) return false;
  if (root.delivery.sha256 !== expected)
    fail("STATE", "Bootstrap cleanup digest does not match");
  const status = await inspectStream(tx, root, () => {}, false);
  if (!status.complete)
    fail("STATE", "All bootstrap chunks must be acknowledged before forgetting");
  const changed = await tx.execute(
    `DELETE FROM ${TABLE} WHERE seq>=1 AND seq<=? AND acknowledged=1`,
    [status.lastSequence],
  );
  if (changed !== status.chunks)
    fail("CORRUPT", "Bootstrap cleanup did not remove its complete acknowledged range");
  return true;
}

/** @internal Shared outbox storage; callers retain one owned SQL transaction. */
export { TABLE, META, fold, fail, identity, digest, integer, bound, query, ensure, find, load, inspectStream, store, streamIdentity, chunkId, metadata };
export type { Stored };
