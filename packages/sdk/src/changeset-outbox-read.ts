import type { ChangesetExecutor } from "./changeset-apply";
import type { OutboxReadResult, Stored } from "./changeset-outbox-store";
import { bound, ensure, fail, find, identity, load } from "./changeset-outbox-store";

export interface OutboxBatchReadOptions {
  /** Total pending payload bytes returned from one snapshot. Default/max 64 MiB. */
  maxBytes?: number;
}
interface BatchRead {
  readonly ids: readonly string[];
  readonly maxBytes: number;
}

/** @internal Own identities and the aggregate budget before SQL or any await. */
export function captureBatchRead(
  ids: readonly string[], options: OutboxBatchReadOptions = {},
): BatchRead {
  if (!Array.isArray(ids)) fail("INPUT", "A batch read requires 1..256 delivery identities");
  const length: unknown = Object.getOwnPropertyDescriptor(ids, "length")?.value;
  if (typeof length !== "number" || !Number.isSafeInteger(length) || length < 1 || length > 256)
    fail("INPUT", "A batch read requires 1..256 delivery identities");
  const maxBytes = bound(options.maxBytes, 64 * 1024 * 1024, 64 * 1024 * 1024);
  const captured: string[] = [], seen = new Set<string>();
  for (let i = 0; i < length; i++) {
    const entry = Object.getOwnPropertyDescriptor(ids, String(i));
    if (entry === undefined || !Object.hasOwn(entry, "value"))
      fail("INPUT", "Batch read identities must be own data properties");
    const id = identity(entry.value);
    if (seen.has(id)) fail("INPUT", "Batch read identities must be distinct");
    seen.add(id); captured.push(id);
  }
  return Object.freeze({ ids: Object.freeze(captured), maxBytes });
}

/**
 * @internal Use the SAME owned SQL snapshot for metadata, recipient progress
 * and all payloads. Admit the complete aggregate before retrieving any payload.
 * The caller supplies only an already validated, snapshot-local replica cursor.
 */
export async function loadReadBatch(
  tx: ChangesetExecutor, request: BatchRead, acknowledgedThrough?: bigint,
): Promise<readonly OutboxReadResult[]> {
  if (!(await ensure(tx, false))) fail("STATE", "Batch read has no retained source outbox");
  const records: { stored: Stored; acknowledged: boolean; suppressed: boolean }[] = [];
  let bytes = 0;
  for (const id of request.ids) {
    const stored = await find(tx, id);
    if (stored === null) fail("STATE", "A batch read identity is no longer retained");
    const suppressed = acknowledgedThrough !== undefined && stored.delivery.sequence <= acknowledgedThrough;
    const acknowledged = stored.delivery.acknowledged || suppressed;
    if (!acknowledged) {
      if (stored.delivery.byteLength > request.maxBytes - bytes)
        fail("FULL", "Batch read payload exceeds its aggregate byte budget");
      bytes += stored.delivery.byteLength;
    }
    records.push({ stored, acknowledged, suppressed });
  }
  const results: OutboxReadResult[] = [];
  for (const { stored, acknowledged, suppressed } of records) {
    const delivery = acknowledged && !stored.delivery.acknowledged
      ? Object.freeze({ ...stored.delivery, acknowledged: true }) : stored.delivery;
    const changeset = suppressed ? null : await load(tx, stored);
    results.push(Object.freeze({ delivery, changeset }));
  }
  return Object.freeze(results);
}
