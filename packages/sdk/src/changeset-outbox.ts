import type { ChangesetExecutor, ChangesetTarget } from "./changeset-apply";
import {
  prepareChangesetCapture,
  prepareChangesetSnapshot,
  prepareSnapshotChangesetStream,
} from "./changeset-capture";
import { assertSingleRecipient, captureFanoutGuard } from "./changeset-fanout";
import type {
  ChangesetOutboxOptions,
  OutboxBootstrapChunksOptions,
  OutboxBootstrapChunksResult,
  OutboxBootstrapOptions,
  OutboxBootstrapResult,
  OutboxDelivery,
  OutboxPageOptions,
  OutboxReadResult,
  OutboxRecordOptions,
  OutboxRecordResult,
} from "./changeset-outbox-store";
import {
  TABLE,
  fold,
  fail,
  identity,
  digest,
  integer,
  bound,
  query,
  ensure,
  find,
  load,
  inspectStream,
  store,
  streamIdentity,
  chunkId,
  pendingDeliveries,
  acknowledgeDelivery,
  forgetDelivery,
  forgetBootstrap,
  CHANGESET_OUTBOX_TABLE,
} from "./changeset-outbox-store";
export type {
  ChangesetOutboxOptions,
  OutboxBootstrapChunksOptions,
  OutboxBootstrapChunksResult,
  OutboxBootstrapOptions,
  OutboxBootstrapResult,
  OutboxDelivery,
  OutboxPageOptions,
  OutboxReadResult,
  OutboxRecordOptions,
  OutboxRecordResult,
} from "./changeset-outbox-store";
export { CHANGESET_OUTBOX_TABLE, ChangesetOutboxError } from "./changeset-outbox-store";

/** Persistent source-side delivery state; transport and remote ACK policy are caller-owned. */
export class ChangesetOutbox {
  readonly #target: ChangesetTarget;
  readonly #maxEntries: number;
  readonly #maxPayloadBytes: number;
  constructor(target: ChangesetTarget, options: ChangesetOutboxOptions = {}) {
    this.#target = target;
    this.#maxEntries = bound(options.maxEntries, 10_000, 100_000);
    this.#maxPayloadBytes = bound(options.maxPayloadBytes, 64 * 1024 * 1024, 1024 * 1024 * 1024);
  }


  /** Metadata changes made by source callbacks must roll back with their DML. */
  async #sourceTransaction<T>(
    work: (tx: ChangesetExecutor) => Promise<T>,
    options?: { signal?: AbortSignal; timeoutMs?: number },
  ): Promise<T> {
    return this.#target.transaction(async (tx) => {
      const before = await captureFanoutGuard(tx);
      const result = await work(tx);
      if ((await captureFanoutGuard(tx)) !== before)
        fail("STATE", "Source work changed fanout membership or acknowledgement progress");
      return result;
    }, options);
  }

  /** Source DML, binary payload and delivery identity commit in one SQL transaction. */
  async record<T>(
    work: (tx: ChangesetExecutor) => T | Promise<T>,
    options: OutboxRecordOptions,
  ): Promise<OutboxRecordResult<T>> {
    const id = identity(options?.deliveryId);
    const capture = prepareChangesetCapture(work, options);
    const scope = JSON.stringify({
      tables: capture.tables.map(fold).sort(),
      indirect: capture.indirect,
    });
    if (globalThis.crypto?.subtle === undefined)
      fail("INPUT", "The outbox requires Web Crypto SHA-256");
    return this.#sourceTransaction(async (tx) => {
      capture.checkpoint();
      await ensure(tx, true);
      capture.checkpoint();
      const existing = await find(tx, id);
      capture.checkpoint();
      if (existing !== null) {
        if (existing.scope !== scope)
          fail("REUSE", "Delivery identity was already used with another capture scope");
        await load(tx, existing);
        capture.checkpoint();
        return { replayed: true, delivery: existing.delivery };
      }
      const size = await query(
        tx,
        `SELECT count(*), coalesce(sum(length(payload)),0) FROM ${TABLE}`,
      );
      if (size.length !== 1 || size[0]!.length !== 2)
        fail("CORRUPT", "Invalid outbox capacity result");
      const entries = integer(size[0]![0]),
        bytes = integer(size[0]![1]);
      if (entries >= this.#maxEntries || bytes > this.#maxPayloadBytes)
        fail(
          "FULL",
          "Outbox retention limit reached; acknowledge or explicitly forget old deliveries",
        );
      const result = await capture.run(tx);
      capture.checkpoint();
      if (result.changeset.byteLength > this.#maxPayloadBytes - bytes)
        fail("FULL", "Outbox pending payload budget exceeded");
      const delivery = await store(tx, id, scope, result, capture.checkpoint);
      return { replayed: false, value: result.value, delivery };
    }, capture.transactionOptions);
  }

  /**
   * Persist a consistent existing-row seed as this outbox's FIRST operation.
   * Later record() calls follow it in sequence. A retained seed ID is recovered,
   * never regenerated from newer rows. No source DML or receiver schema edits.
   */
  async bootstrap(options: OutboxBootstrapOptions): Promise<OutboxBootstrapResult> {
    const id = identity(options?.deliveryId),
      snapshot = prepareChangesetSnapshot(options);
    const scope = JSON.stringify({
      tables: snapshot.tables.map(fold).sort(),
      indirect: snapshot.indirect,
      snapshot: true,
    });
    if (globalThis.crypto?.subtle === undefined)
      fail("INPUT", "The outbox requires Web Crypto SHA-256");
    return this.#sourceTransaction(async (tx) => {
      snapshot.checkpoint();
      await ensure(tx, true);
      snapshot.checkpoint();
      const existing = await find(tx, id);
      snapshot.checkpoint();
      if (existing !== null) {
        if (existing.scope !== scope)
          fail(
            "REUSE",
            "Delivery identity was already used for another operation or snapshot scope",
          );
        await load(tx, existing);
        snapshot.checkpoint();
        return Object.freeze({ replayed: true, delivery: existing.delivery });
      }
      // An empty pending list is not a pristine history. Even explicitly
      // forgotten acknowledgements leave sqlite_sequence advanced. Never
      // append a fresh baseline after incremental data or silently reseed.
      if (
        (await query(tx, `SELECT 1 FROM ${TABLE} LIMIT 1`)).length ||
        (
          await query(
            tx,
            "SELECT 1 FROM main.sqlite_sequence WHERE name=? COLLATE BINARY LIMIT 1",
            [CHANGESET_OUTBOX_TABLE],
          )
        ).length
      ) {
        fail(
          "STATE",
          "Bootstrap requires an unused outbox; recover the original seed ID instead of reseeding",
        );
      }
      snapshot.checkpoint();
      const result = await snapshot.run(tx);
      snapshot.checkpoint();
      if (result.changeset.byteLength > this.#maxPayloadBytes)
        fail("FULL", "Outbox cannot retain the complete bootstrap payload");
      const delivery = await store(tx, id, scope, result, snapshot.checkpoint);
      if (delivery.sequence !== 1n)
        fail("STATE", "Bootstrap did not become the first outbox operation");
      return Object.freeze({ replayed: false, delivery });
    }, snapshot.transactionOptions);
  }

  /**
   * Atomically retain ALL seed chunks from one source snapshot before record().
   * The receiver applies separate transactions: keep it unpublished until the
   * whole baseline is acknowledged and storage-confirmed. No network inside SQL.
   */
  async bootstrapChunks(
    options: OutboxBootstrapChunksOptions,
  ): Promise<OutboxBootstrapChunksResult> {
    const id = streamIdentity(options?.deliveryId),
      snapshot = prepareSnapshotChangesetStream(options);
    const base = {
      tables: snapshot.tables.map(fold).sort(),
      indirect: snapshot.indirect,
      snapshot: true,
    };
    const baseScope = JSON.stringify(base);
    if (globalThis.crypto?.subtle === undefined)
      fail("INPUT", "The outbox requires Web Crypto SHA-256");
    return this.#sourceTransaction(async (tx) => {
      snapshot.checkpoint();
      await ensure(tx, true);
      snapshot.checkpoint();
      const existing = await find(tx, id);
      snapshot.checkpoint();
      if (existing !== null) {
        if (existing.stream?.index !== 0 || existing.stream.base !== baseScope)
          fail("REUSE", "Identity belongs to another operation or bootstrap scope");
        return Object.freeze({
          ...(await inspectStream(tx, existing, snapshot.checkpoint, true)),
          replayed: true,
        });
      }
      if (
        (await query(tx, `SELECT 1 FROM ${TABLE} LIMIT 1`)).length ||
        (
          await query(
            tx,
            "SELECT 1 FROM main.sqlite_sequence WHERE name=? COLLATE BINARY LIMIT 1",
            [CHANGESET_OUTBOX_TABLE],
          )
        ).length
      ) {
        fail("STATE", "Chunked bootstrap requires an unused outbox; recover the original identity");
      }
      let retainedBytes = 0,
        firstScope = "";
      const summary = await snapshot.run(tx, async (chunk) => {
        snapshot.checkpoint();
        if (
          chunk.index >= this.#maxEntries ||
          chunk.changeset.byteLength > this.#maxPayloadBytes - retainedBytes
        )
          fail("FULL", "Outbox cannot retain the complete chunked bootstrap");
        const stream =
          chunk.index === 0
            ? {
                id,
                index: 0,
                summary: {
                  chunks: 1,
                  changes: chunk.changes,
                  byteLength: chunk.changeset.byteLength,
                },
              }
            : { id, index: chunk.index };
        const scope = JSON.stringify({ ...base, stream });
        if (chunk.index === 0) firstScope = scope;
        const saved = await store(tx, chunkId(id, chunk.index), scope, chunk, snapshot.checkpoint);
        if (saved.sequence !== BigInt(chunk.index + 1))
          fail("STATE", "Bootstrap chunks did not occupy the initial sequence range");
        retainedBytes += chunk.changeset.byteLength;
      });
      snapshot.checkpoint();
      const scope = JSON.stringify({ ...base, stream: { id, index: 0, summary } });
      const changed = await tx.execute(
        `UPDATE OR ABORT ${TABLE} SET scope=? WHERE seq=1 AND delivery_id=? AND scope=? AND acknowledged=0`,
        [scope, id, firstScope],
      );
      snapshot.checkpoint();
      if (changed !== 1) fail("CORRUPT", "Bootstrap completion manifest was not stored");
      const root = await find(tx, id);
      if (root === null || root.scope !== scope)
        fail("CORRUPT", "Bootstrap completion manifest was not confirmed");
      return Object.freeze({
        ...(await inspectStream(tx, root, snapshot.checkpoint, false)),
        replayed: false,
      });
    }, snapshot.transactionOptions);
  }

  /** Explicitly forget the entire acknowledged bootstrap, never just its prefix. */
  async forgetBootstrapChunks(deliveryId: string, sha256: string): Promise<boolean> {
    const id = streamIdentity(deliveryId),
      expected = digest(sha256);
    return this.#target.transaction(async (tx) => {
      await assertSingleRecipient(tx);
      return forgetBootstrap(tx, id, expected);
    });
  }

  /** Bounded metadata page. Sequence cursors are monotonic, not SQL OFFSETs. */
  async pending(options: OutboxPageOptions = {}): Promise<readonly OutboxDelivery[]> {
    const limit = bound(options.limit, 100, 256),
      after = options.after ?? 0n;
    if (typeof after !== "bigint" || after < 0n || after > (1n << 63n) - 1n)
      fail("INPUT", "after must be a nonnegative int64 bigint sequence");
    return this.#target.transaction(async (tx) => {
      return pendingDeliveries(tx, after, limit);
    });
  }

  /** Hash and validate one pending payload before returning owned bytes. */
  async read(deliveryId: string): Promise<OutboxReadResult | null> {
    const id = identity(deliveryId);
    return this.#target.transaction(async (tx) => {
      if (!(await ensure(tx, false))) return null;
      const record = await find(tx, id);
      return record === null
        ? null
        : { delivery: record.delivery, changeset: await load(tx, record) };
    });
  }

  /** Call only after the receiver durably acknowledges this exact ID and digest. */
  async acknowledge(deliveryId: string, sha256: string): Promise<boolean> {
    const id = identity(deliveryId),
      expected = digest(sha256);
    return this.#target.transaction(async (tx) => {
      await assertSingleRecipient(tx);
      return acknowledgeDelivery(tx, id, expected);
    });
  }

  /** Explicitly ends deduplication for an acknowledged identity. Never auto-expires. */
  async forgetAcknowledged(deliveryId: string, sha256: string): Promise<boolean> {
    const id = identity(deliveryId),
      expected = digest(sha256);
    return this.#target.transaction(async (tx) => {
      await assertSingleRecipient(tx);
      return forgetDelivery(tx, id, expected);
    });
  }
}
