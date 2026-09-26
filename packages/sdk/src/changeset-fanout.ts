import type { ChangesetExecutor, ChangesetTarget } from "./changeset-apply";
import type { OutboxDelivery, OutboxPageOptions, OutboxReadResult, Stored } from "./changeset-outbox-store";
import {
  TABLE, CHANGESET_OUTBOX_TABLE, acknowledgeDelivery, bound, chunkId, digest, ensure, find,
  forgetBootstrap, forgetDelivery, identity, inspectStream, integer, load,
  pendingDeliveries, query,
} from "./changeset-outbox-store";

export const CHANGESET_FANOUT_TABLE = "__fsqlite_changeset_fanout";
export const CHANGESET_FANOUT_PROGRESS_TABLE = "__fsqlite_changeset_fanout_progress";
const MANIFEST = `main."${CHANGESET_FANOUT_TABLE}"`;
const PROGRESS = `main."${CHANGESET_FANOUT_PROGRESS_TABLE}"`;
const MAX_SEQUENCE = (1n << 63n) - 1n;
// At most 256 identities of 256 UTF-8 bytes: JSON escaping can expand each
// byte to six characters, and UTF-16 storage can double that representation.
// Bound the SQL transfer, then require the exact validated canonical roster.
const MANIFEST_BYTES = 1024 * 1024;

export class ChangesetFanoutError extends Error {
  constructor(
    readonly code: "ERR_FSQLITE_FANOUT_INPUT" | "ERR_FSQLITE_FANOUT_SCHEMA" |
      "ERR_FSQLITE_FANOUT_CORRUPT" | "ERR_FSQLITE_FANOUT_STATE" | "ERR_FSQLITE_FANOUT_ACK",
    message: string,
  ) {
    super(message);
    this.name = "ChangesetFanoutError";
  }
}
function fail(kind: "INPUT" | "SCHEMA" | "CORRUPT" | "STATE" | "ACK", message: string): never {
  throw new ChangesetFanoutError(`ERR_FSQLITE_FANOUT_${kind}`, message);
}
function replicaIdentity(value: unknown): string {
  if (typeof value !== "string" || !value.length || value.length > 256 || value.includes("\0"))
    fail("INPUT", "Replica identities must contain 1..256 valid UTF-8 bytes without NUL");
  const bytes = new TextEncoder().encode(value);
  if (bytes.length > 256 || new TextDecoder("utf-8", { ignoreBOM: true }).decode(bytes) !== value)
    fail("INPUT", "Replica identities must contain 1..256 valid UTF-8 bytes without NUL");
  return value;
}
function captureRoster(input: readonly string[]): readonly string[] {
  if (!Array.isArray(input)) fail("INPUT", "An immutable roster of 1..256 replicas is required");
  const length = input.length;
  if (length < 1 || length > 256) fail("INPUT", "Use 1..256 replicas");
  const names: string[] = [];
  for (let i = 0; i < length; i++) names.push(replicaIdentity(input[i]));
  names.sort(); // Identity is case-sensitive; order supplied by the caller is not significant.
  if (names.some((name, i) => i > 0 && name === names[i - 1]))
    fail("INPUT", "Replica identities must be distinct");
  return Object.freeze(names);
}
function manifestFor(replicas: readonly string[]): string {
  return JSON.stringify({ version: 1, replicas });
}
function sequence(value: unknown): bigint {
  if (typeof value !== "string" || !/^(0|[1-9][0-9]{0,18})$/.test(value))
    fail("CORRUPT", "Invalid fanout sequence");
  const result = BigInt(value);
  if (result > MAX_SEQUENCE) fail("CORRUPT", "Fanout sequence exceeds int64");
  return result;
}

export interface ChangesetReplicaProgress {
  readonly receiverId: string;
  readonly sequence: bigint;
  readonly deliveryId: string | null;
  readonly sha256: string | null;
}
export interface ChangesetFanoutProgress {
  /** Every member has confirmed through this inclusive sequence. */
  readonly acknowledgedThrough: bigint;
  readonly sourceSequence: bigint;
  readonly replicas: readonly ChangesetReplicaProgress[];
}
/** A receiver-bound source for ChangesetDeliveryPump; confirmation policy stays caller-owned. */
export interface ChangesetReplicaOutbox {
  readonly receiverId: string;
  pending(options?: OutboxPageOptions): Promise<readonly OutboxDelivery[]>;
  read(deliveryId: string): Promise<OutboxReadResult | null>;
  acknowledge(deliveryId: string, sha256: string): Promise<boolean>;
}
interface State {
  manifest: string;
  replicas: readonly ChangesetReplicaProgress[];
  minimum: bigint;
  high: bigint;
}
function fingerprint(state: State): string {
  return JSON.stringify([state.manifest, state.replicas.map((r) =>
    [r.receiverId, r.sequence.toString(), r.deliveryId, r.sha256])]);
}

async function present(tx: ChangesetExecutor): Promise<boolean> {
  const rows = await query(tx,
    "SELECT name FROM main.sqlite_schema WHERE name COLLATE NOCASE IN (?,?)",
    [CHANGESET_FANOUT_TABLE, CHANGESET_FANOUT_PROGRESS_TABLE]);
  if (rows.length === 0) return false;
  if (rows.length !== 2) fail("CORRUPT", "Fanout membership or progress table is missing");
  return true;
}

/** Validate semantics, not the spelling of CREATE TABLE after engine normalization. */
async function validateTable(tx: ChangesetExecutor, name: string, columns: readonly string[], types: readonly string[], textKey: boolean): Promise<void> {
  const rows = (await query(tx, `PRAGMA main.table_list('${name}')`))
    .filter((row) => row[0] === "main" && row[1] === name);
  if (rows.length !== 1 || rows[0]![2] !== "table" ||
      integer(rows[0]![3]) !== columns.length || integer(rows[0]![4]) !== 0)
    fail("SCHEMA", "Fanout requires ordinary main metadata tables");
  const info = await query(tx, `PRAGMA main.table_xinfo('${name}')`);
  if (info.length !== columns.length) fail("SCHEMA", "Invalid fanout columns");
  for (let i = 0; i < info.length; i++) {
    const c = info[i]!;
    if (integer(c[0]) !== i || c[1] !== columns[i] || typeof c[2] !== "string" ||
        c[2].toUpperCase() !== types[i] || ((i > 0 || textKey) && integer(c[3]) !== 1) ||
        c[4] !== null || integer(c[5]) !== (i === 0 ? 1 : 0) || integer(c[6]) !== 0)
      fail("SCHEMA", "Invalid fanout column definition");
  }
  const indexes = await query(tx, `PRAGMA main.index_list('${name}')`);
  if (indexes.length !== (textKey ? 1 : 0)) fail("SCHEMA", "Unexpected fanout indexes");
  if (textKey) {
    const index = indexes[0]!, indexName = index[1];
    if (typeof indexName !== "string" || indexName.length > 1024 || indexName.includes("\0") ||
        integer(index[2]) !== 1 || index[3] !== "pk" || integer(index[4]) !== 0)
      fail("SCHEMA", "Fanout requires a BINARY replica primary key");
    const keys = (await query(tx, `PRAGMA main.index_xinfo('${indexName.replaceAll("'", "''")}')`))
      .filter((r) => integer(r[5]) === 1);
    if (keys.length !== 1 || integer(keys[0]![1]) !== 0 || keys[0]![2] !== columns[0] ||
        integer(keys[0]![3]) !== 0 || keys[0]![4] !== "BINARY")
      fail("SCHEMA", "Fanout requires a BINARY replica primary key");
  }
  if ((await query(tx, `PRAGMA main.foreign_key_list('${name}')`)).length)
    fail("SCHEMA", "Fanout metadata cannot have foreign keys");
  for (const ns of ["main", "temp"])
    if ((await query(tx,
      `SELECT name FROM ${ns}.sqlite_schema WHERE type='trigger' AND tbl_name=? COLLATE NOCASE LIMIT 1`,
      [name])).length) fail("SCHEMA", "Fanout metadata cannot have triggers");
}

async function sourceSequence(tx: ChangesetExecutor): Promise<bigint> {
  const rows = await query(tx,
    "SELECT CASE WHEN typeof(seq)='integer' THEN CAST(seq AS TEXT) END FROM main.sqlite_sequence WHERE name=? COLLATE BINARY",
    [CHANGESET_OUTBOX_TABLE]);
  if (rows.length > 1) fail("CORRUPT", "Ambiguous source sequence");
  return rows.length === 0 ? 0n : sequence(rows[0]![0]);
}

async function state(tx: ChangesetExecutor): Promise<State> {
  if (!(await present(tx))) fail("STATE", "Fanout has not been initialized");
  await validateTable(tx, CHANGESET_FANOUT_TABLE, ["id", "roster"], ["INTEGER", "TEXT"], false);
  await validateTable(tx, CHANGESET_FANOUT_PROGRESS_TABLE,
    ["replica_id", "sequence", "delivery_id", "sha256"], ["TEXT", "INTEGER", "TEXT", "TEXT"], true);
  if (!(await ensure(tx, false))) fail("CORRUPT", "Fanout source outbox is missing");
  const manifest = await query(tx,
    `SELECT id, CASE WHEN typeof(roster)='text' AND length(CAST(roster AS BLOB))<=${MANIFEST_BYTES} THEN roster END FROM ${MANIFEST} LIMIT 2`);
  if (manifest.length !== 1 || integer(manifest[0]![0]) !== 1 || typeof manifest[0]![1] !== "string")
    fail("CORRUPT", "Missing or malformed immutable fanout manifest");
  const text = manifest[0]![1];
  let replicas: readonly string[];
  try {
    const parsed = JSON.parse(text);
    replicas = captureRoster(parsed?.replicas);
    if (text !== manifestFor(replicas)) fail("CORRUPT", "Noncanonical fanout manifest");
  } catch {
    return fail("CORRUPT", "Invalid immutable fanout manifest");
  }
  // Bound database-encoded strings before the worker/SQL boundary. UTF-16
  // can double UTF-8 sizes; roster membership/identity() retain API limits.
  const rows = await query(tx,
    `SELECT CASE WHEN typeof(replica_id)='text' AND length(CAST(replica_id AS BLOB))<=512 AND instr(replica_id,char(0))=0 THEN replica_id END, ` +
    `CASE WHEN typeof(sequence)='integer' THEN CAST(sequence AS TEXT) END, ` +
    `CASE WHEN typeof(delivery_id)='text' AND length(CAST(delivery_id AS BLOB))<=1024 AND instr(delivery_id,char(0))=0 THEN delivery_id END, ` +
    `CASE WHEN typeof(sha256)='text' AND length(CAST(sha256 AS BLOB))<=128 AND length(sha256)<=64 AND instr(sha256,char(0))=0 THEN sha256 END FROM ${PROGRESS} LIMIT 257`);
  if (rows.length !== replicas.length) fail("CORRUPT", "A required replica cursor is missing or duplicated");
  const byId = new Map<string, ChangesetReplicaProgress>();
  const high = await sourceSequence(tx);
  for (const row of rows) {
    const receiverId = row[0], position = sequence(row[1]);
    if (typeof receiverId !== "string" || !replicas.includes(receiverId) || byId.has(receiverId) || position > high)
      fail("CORRUPT", "Foreign or out-of-range replica progress");
    let deliveryId: string | null = null, sha256: string | null = null;
    if (position === 0n) {
      if (row[2] !== "" || row[3] !== "") fail("CORRUPT", "Initial replica cursor has a receipt");
    } else {
      deliveryId = identity(row[2]);
      sha256 = digest(row[3]);
      const record = await find(tx, deliveryId);
      if (record === null || record.delivery.sequence !== position || record.delivery.sha256 !== sha256)
        fail("CORRUPT", "Replica progress has no matching retained source identity");
    }
    byId.set(receiverId, Object.freeze({ receiverId, sequence: position, deliveryId, sha256 }));
  }
  const progress = Object.freeze(replicas.map((id) => byId.get(id)!));
  const minimum = progress.reduce((n, r) => r.sequence < n ? r.sequence : n, high);
  // Every unreclaimed sequence must survive. Forgotten old ACKs may leave gaps
  // below the minimum, but a missing pending row must never skip delivery.
  const counts = await query(tx,
    `SELECT CAST(count(*) AS TEXT), CAST(coalesce(max(seq),0) AS TEXT) FROM ${TABLE} WHERE seq>?`, [minimum]);
  if (counts.length !== 1 || sequence(counts[0]![0]) !== high - minimum ||
      sequence(counts[0]![1]) !== (high === minimum ? 0n : high))
    fail("CORRUPT", "Fanout source has a missing or foreign pending sequence");
  if ((await query(tx,
    `SELECT 1 FROM ${TABLE} WHERE seq<1 OR typeof(acknowledged)<>'integer' OR acknowledged NOT IN (0,1) ` +
    `OR (seq<=? AND acknowledged<>1) OR (seq>? AND acknowledged<>0) LIMIT 1`, [minimum, minimum])).length)
    fail("CORRUPT", "Global reclamation disagrees with required replica progress");
  return { manifest: text, replicas: progress, minimum, high };
}

/** @internal Source DML may append records, but may not edit membership/cursors. */
export async function captureFanoutGuard(tx: ChangesetExecutor): Promise<string | null> {
  return (await present(tx)) ? fingerprint(await state(tx)) : null;
}
/** @internal Never let the legacy single-recipient API bypass a required replica. */
export async function assertSingleRecipient(tx: ChangesetExecutor): Promise<void> {
  if (await present(tx)) fail("STATE", "Use the receiver-bound fanout source for acknowledgements and fanout cleanup");
}

/**
 * @internal Called only AFTER the bootstrap module verifies the entire original
 * receiver-bound manifest, confirmed receipt and pending source payloads in this
 * SAME transaction. Never call this with an unverified incoming frontier.
 */
export async function acknowledgeFanoutBootstrapPrefix(
  tx: ChangesetExecutor,
  receiverId: string,
  root: Stored,
  checkpoint: () => void,
): Promise<number> {
  checkpoint();
  const current = await state(tx);
  const cursor = current.replicas.find((r) => r.receiverId === receiverId);
  if (cursor === undefined) fail("ACK", "Bootstrap receiver is not a required fanout member");
  const stream = root.stream, summary = stream?.summary;
  if (stream === null || summary === null || summary === undefined || stream.index !== 0)
    fail("CORRUPT", "Fanout bootstrap acknowledgement requires a complete retained seed");
  const end = BigInt(summary.chunks);
  const last = await find(tx, chunkId(stream.id, summary.chunks - 1));
  if (last === null || last.delivery.sequence !== end || last.stream?.id !== stream.id ||
      last.stream.index !== summary.chunks - 1 || last.stream.base !== stream.base || end > current.high)
    fail("CORRUPT", "Fanout bootstrap frontier has no matching retained source identity");
  checkpoint();
  // Manifest/payload verification has already run, even for historical ACKs.
  // Never move a newer cursor back to its seed or clear incremental payloads.
  if (cursor.sequence >= end) return 0;

  if (await tx.execute(
    `UPDATE OR ABORT ${PROGRESS} SET sequence=?,delivery_id=?,sha256=? WHERE replica_id=? AND sequence=? AND delivery_id=? AND sha256=?`,
    [end, last.delivery.deliveryId, last.delivery.sha256, receiverId,
      cursor.sequence, cursor.deliveryId ?? "", cursor.sha256 ?? ""],
  ) !== 1) fail("CORRUPT", "Bootstrap replica cursor did not advance exactly once");
  checkpoint();
  const replicas = current.replicas.map((r) => r.receiverId === receiverId
    ? { receiverId, sequence: end, deliveryId: last.delivery.deliveryId, sha256: last.delivery.sha256 } : r);
  const minimum = replicas.reduce((n, r) => r.sequence < n ? r.sequence : n, current.high);
  if (minimum < current.minimum || minimum > end)
    fail("CORRUPT", "Bootstrap acknowledgement has an invalid reclamation frontier");
  const reclaimed = Number(minimum - current.minimum);
  if (reclaimed > 0) {
    // The minimum may stop INSIDE the seed if another member has only a partial
    // prefix. It can never advance beyond this seed. Keep all later bytes and
    // every identity tombstone, including cursors needed for source-state checks.
    if (await tx.execute(
      `UPDATE OR ABORT ${TABLE} SET acknowledged=1,payload=X'' WHERE seq>? AND seq<=? AND acknowledged=0`,
      [current.minimum, minimum],
    ) !== reclaimed) fail("CORRUPT", "Bootstrap reclamation did not match the required-replica frontier");
    checkpoint();
  }
  const after = await inspectStream(tx, root, checkpoint, false);
  if (after.acknowledgedChunks !== Number(minimum) || after.chunks !== summary.chunks ||
      after.changes !== summary.changes || after.byteLength !== summary.byteLength ||
      after.sha256 !== root.delivery.sha256)
    fail("CORRUPT", "Bootstrap payload reclamation was not retained with replica progress");
  const next = await state(tx);
  if (fingerprint(next) !== fingerprint({ ...current, replicas, minimum }) ||
      next.minimum !== minimum || next.high !== current.high)
    fail("CORRUPT", "Bootstrap acknowledgement changed unrelated fanout state");
  checkpoint();
  return Number(end - cursor.sequence);
}

/**
 * Durable ALL-recipient retention over one existing source outbox. Initialize
 * before its first operation, then use ChangesetOutbox.record/bootstrap normally.
 * Membership is immutable: no removal, quorum, automatic expiry or silent reseed.
 * SQL commit and storage confirmation are distinct; use the existing delivery
 * pump's confirmSource/receiver confirmCommit barriers for snapshot persistence.
 */
export class ChangesetFanout {
  readonly #target: ChangesetTarget;
  readonly #manifest: string;
  readonly #replicas: readonly string[];
  get replicas(): readonly string[] { return this.#replicas; }
  private constructor(target: ChangesetTarget, replicas: readonly string[]) {
    this.#target = target;
    this.#replicas = replicas;
    this.#manifest = manifestFor(replicas);
  }

  static async open(target: ChangesetTarget, replicas: readonly string[]): Promise<ChangesetFanout> {
    const captured = captureRoster(replicas); // Before the first await/admission.
    if (typeof target?.transaction !== "function") fail("INPUT", "An owned SQL transaction target is required");
    const result = new ChangesetFanout(target, captured);
    await target.transaction(async (tx) => {
      if (!(await present(tx))) {
        await ensure(tx, true);
        if ((await query(tx, `SELECT 1 FROM ${TABLE} LIMIT 1`)).length ||
            (await query(tx, "SELECT 1 FROM main.sqlite_sequence WHERE name=? COLLATE BINARY LIMIT 1", [CHANGESET_OUTBOX_TABLE])).length)
          fail("STATE", "Initialize fanout before the source's first operation; used outboxes cannot be reseeded");
        await tx.execute(`CREATE TABLE ${MANIFEST} (id INTEGER PRIMARY KEY, roster TEXT NOT NULL)`);
        await tx.execute(`CREATE TABLE ${PROGRESS} (replica_id TEXT NOT NULL PRIMARY KEY COLLATE BINARY, sequence INTEGER NOT NULL, delivery_id TEXT NOT NULL, sha256 TEXT NOT NULL)`);
        if (await tx.execute(`INSERT INTO ${MANIFEST} VALUES (1,?)`, [result.#manifest]) !== 1)
          fail("CORRUPT", "Fanout manifest was not stored");
        for (const id of captured)
          if (await tx.execute(`INSERT INTO ${PROGRESS} VALUES (?,0,'','')`, [id]) !== 1)
            fail("CORRUPT", "Replica cursor was not initialized");
      }
      await result.#state(tx);
    });
    return result;
  }

  async #state(tx: ChangesetExecutor): Promise<State> {
    const current = await state(tx);
    if (current.manifest !== this.#manifest) fail("STATE", "Fanout membership does not match the immutable roster");
    return current;
  }

  async progress(): Promise<ChangesetFanoutProgress> {
    return this.#target.transaction(async (tx) => {
      const current = await this.#state(tx);
      return Object.freeze({ acknowledgedThrough: current.minimum, sourceSequence: current.high, replicas: current.replicas });
    });
  }

  forReplica(receiverId: string): ChangesetReplicaOutbox {
    const id = replicaIdentity(receiverId);
    if (!this.replicas.includes(id)) fail("INPUT", "Replica is not a member of this fanout");
    return Object.freeze({
      receiverId: id,
      pending: (options?: OutboxPageOptions) => this.#pending(id, options),
      read: (deliveryId: string) => this.#read(id, deliveryId),
      acknowledge: (deliveryId: string, sha256: string) => this.#acknowledge(id, deliveryId, sha256),
    });
  }

  async #pending(replica: string, options: OutboxPageOptions = {}): Promise<readonly OutboxDelivery[]> {
    const limit = bound(options.limit, 100, 256), after = options.after ?? 0n;
    if (typeof after !== "bigint" || after < 0n || after > MAX_SEQUENCE)
      fail("INPUT", "after must be a nonnegative int64 bigint");
    return this.#target.transaction(async (tx) => {
      const current = await this.#state(tx), cursor = current.replicas.find((r) => r.receiverId === replica)!;
      return pendingDeliveries(tx, after > cursor.sequence ? after : cursor.sequence, limit);
    });
  }

  async #read(replica: string, deliveryId: string): Promise<OutboxReadResult | null> {
    const id = identity(deliveryId);
    return this.#target.transaction(async (tx) => {
      const current = await this.#state(tx), cursor = current.replicas.find((r) => r.receiverId === replica)!;
      const record = await find(tx, id);
      if (record === null) return null;
      if (record.delivery.sequence <= cursor.sequence)
        return { delivery: Object.freeze({ ...record.delivery, acknowledged: true }), changeset: null };
      return { delivery: record.delivery, changeset: await load(tx, record) };
    });
  }

  async #acknowledge(replica: string, deliveryId: string, sha256: string): Promise<boolean> {
    const id = identity(deliveryId), expected = digest(sha256);
    return this.#target.transaction(async (tx) => {
      const current = await this.#state(tx), cursor = current.replicas.find((r) => r.receiverId === replica)!;
      const record = await find(tx, id);
      if (record === null || record.delivery.sha256 !== expected)
        fail("ACK", "Acknowledgement does not match a retained source identity and digest");
      if (record.delivery.sequence <= cursor.sequence) return false;
      if (record.delivery.sequence !== cursor.sequence + 1n)
        fail("ACK", "A replica must acknowledge its next source operation without skipping");
      await load(tx, record);
      if (await tx.execute(
        `UPDATE OR ABORT ${PROGRESS} SET sequence=?,delivery_id=?,sha256=? WHERE replica_id=? AND sequence=? AND delivery_id=? AND sha256=?`,
        [record.delivery.sequence, id, expected, replica, cursor.sequence, cursor.deliveryId ?? "", cursor.sha256 ?? ""],
      ) !== 1) fail("CORRUPT", "Replica acknowledgement was not stored exactly once");
      const replicas = current.replicas.map((r) => r.receiverId === replica
        ? { receiverId: replica, sequence: record.delivery.sequence, deliveryId: id, sha256: expected } : r);
      const minimum = replicas.reduce((n, r) => r.sequence < n ? r.sequence : n, current.high);
      if (minimum > current.minimum) {
        // Advancing one ordered cursor can cross at most this one operation.
        if (minimum !== record.delivery.sequence || minimum !== current.minimum + 1n)
          fail("CORRUPT", "Invalid fanout reclamation frontier");
        if (!(await acknowledgeDelivery(tx, id, expected))) fail("CORRUPT", "Payload was already reclaimed");
      }
      const next = await this.#state(tx);
      if (fingerprint(next) !== fingerprint({ ...current, replicas, minimum }) || next.high !== current.high)
        fail("CORRUPT", "Acknowledgement changed unrelated fanout state");
      return true;
    });
  }

  /** Explicitly ends ID deduplication. Current replica receipts stay retained. */
  async forgetAcknowledged(deliveryId: string, sha256: string): Promise<boolean> {
    const id = identity(deliveryId), expected = digest(sha256);
    return this.#target.transaction(async (tx) => {
      const current = await this.#state(tx), record = await find(tx, id);
      if (record === null) return false;
      if (record.delivery.sequence >= current.minimum)
        fail("STATE", "Advance every replica past a delivery before forgetting its identity");
      const result = await forgetDelivery(tx, id, expected);
      await this.#state(tx);
      return result;
    });
  }

  /** Forget a complete seed only after every replica has advanced beyond it. */
  async forgetBootstrapChunks(deliveryId: string, sha256: string): Promise<boolean> {
    const id = identity(deliveryId), expected = digest(sha256);
    return this.#target.transaction(async (tx) => {
      const current = await this.#state(tx), record = await find(tx, id);
      if (record === null) return false;
      const seed = await inspectStream(tx, record, () => {}, false);
      if (seed.lastSequence >= current.minimum)
        fail("STATE", "Advance every replica beyond the complete seed before forgetting it");
      const result = await forgetBootstrap(tx, id, expected);
      await this.#state(tx);
      return result;
    });
  }
}
