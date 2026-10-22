import type { ApplyChangesetResult, ChangesetTarget } from "./changeset-apply";
import type {
  ChangesetDeliveryOptions, ChangesetDeliveryReceipt, ChangesetEnvelope, ChangesetTransport,
} from "./changeset-delivery";
import type { OutboxReadResult } from "./changeset-outbox";
import { ChangesetOrder } from "./changeset-order";
import type { ChangesetOrderResult } from "./changeset-order";
import { inspectChangeset } from "./changeset-codec";

export const CHANGESET_ORDER_PROTOCOL = "fsqlite-ordered-changeset-v1";
const BASE_PROTOCOL: ChangesetEnvelope["protocol"] = "fsqlite-changeset-v1";
const HARD_BYTES = 64 * 1024 * 1024;
export interface ChangesetWireOrder {
  readonly protocol: typeof CHANGESET_ORDER_PROTOCOL;
  /** Must equal the receiver ledger's trusted sourceId, including incarnation. */
  readonly streamId: string;
  /** Canonical positive signed-int64 decimal string; never a JSON number. */
  readonly sequence: string;
}
export interface OrderedChangesetEnvelope extends ChangesetEnvelope {
  readonly order: Readonly<ChangesetWireOrder>;
}
export interface OrderedChangesetReceipt extends ChangesetDeliveryReceipt {
  readonly order: Readonly<ChangesetWireOrder>;
}
export interface OrderedChangesetSource {
  readonly receiverId?: string;
  read(deliveryId: string): Promise<OutboxReadResult | null>;
}
export interface OrderedChangesetTransportOptions {
  receiverId: string;
  streamId: string;
  /** Authenticate the peer and preserve the order envelope and receipt intact. */
  deliver(message: OrderedChangesetEnvelope, options: ChangesetDeliveryOptions): Promise<unknown>;
  maxMessageBytes?: number;
}
export interface OrderedChangesetBatchTransportOptions
  extends Omit<OrderedChangesetTransportOptions, "deliver"> {
  /** One authenticated exchange; return ALL receipts in the same source order. */
  deliver(messages: readonly OrderedChangesetEnvelope[], options: ChangesetDeliveryOptions): Promise<unknown>;
  /** Aggregate copied input per batch, default/maximum 64 MiB; not total RSS. */
  maxBatchBytes?: number;
}
/** Explicit batching, not a replacement for the single-message pump contract. */
export type OrderedChangesetBatchTransport = (
  messages: readonly ChangesetEnvelope[], options?: ChangesetDeliveryOptions,
) => Promise<readonly OrderedChangesetReceipt[]>;
/** Apply with the supplied target; never confirm durability inside this callback. */
export type OrderedDeliveryApply = (
  target: ChangesetTarget,
  message: ChangesetEnvelope,
  options: ChangesetDeliveryOptions,
) => Promise<ApplyChangesetResult>;
export interface OrderedChangesetReceiverOptions {
  apply: OrderedDeliveryApply;
  /** SAME top-level database as the ledger. Called AFTER SQL commit, also on replay. */
  confirmCommit: () => Promise<unknown>;
  /** Admission before copying; default 8 MiB, maximum 64 MiB. Ledger limits also apply. */
  maxMessageBytes?: number;
  /** Total owned input per receiveBatch, checked before copying; default/max 64 MiB. */
  maxBatchBytes?: number;
  /**
   * Trusted local policy, never read from incoming envelopes or call options.
   * Defer across the entire batch so parents may arrive in later messages.
   * The application callback must not independently defer each fragment.
   * Does not change the single-message receive policy.
   */
  batchForeignKeys?: "defer";
}
export interface OrderedChangesetReceiver {
  readonly receiverId: string;
  readonly streamId: string;
  receive(message: unknown, options?: ChangesetDeliveryOptions): Promise<OrderedChangesetReceipt>;
  /**
   * Atomically apply 1..256 contiguous existing envelopes, then confirm ONCE.
   * No receipt escapes until the entire barrier succeeds, including all-replay
   * batches. A rejection after commit is uncertain delivery, not proof of rollback:
   * retry the same identities/bytes. This is not a new HTTP or wire protocol.
   */
  receiveBatch(messages: readonly unknown[], options?: ChangesetDeliveryOptions): Promise<readonly OrderedChangesetReceipt[]>;
}
export class OrderedChangesetDeliveryError extends Error {
  constructor(
    readonly code: "ERR_FSQLITE_ORDERED_INPUT" | "ERR_FSQLITE_ORDERED_RECEIPT" |
      "ERR_FSQLITE_ORDERED_BUSY" | "ERR_FSQLITE_ORDERED_CANCELLED" | "ERR_FSQLITE_ORDERED_TIMEOUT",
    message: string,
  ) {
    super(message);
    this.name = "OrderedChangesetDeliveryError";
  }
}
function input(message: string): never { throw new OrderedChangesetDeliveryError("ERR_FSQLITE_ORDERED_INPUT", message); }
function mismatch(message: string): never { throw new OrderedChangesetDeliveryError("ERR_FSQLITE_ORDERED_RECEIPT", message); }
function own(value: unknown, key: string): unknown {
  if (typeof value !== "object" || value === null || Array.isArray(value)) input("Expected an ordered delivery record");
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  if (descriptor === undefined || !Object.hasOwn(descriptor, "value")) input(`Delivery ${key} must be an own data property`);
  return descriptor.value;
}
function identity(value: unknown, maximum: number): string {
  if (typeof value !== "string" || !value.length || value.length > maximum || value.includes("\0")) input("Invalid ordered delivery identity");
  const bytes = new TextEncoder().encode(value);
  if (bytes.length > maximum || new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes) !== value) input("Identity must be bounded valid UTF-8");
  return value;
}
function digest(value: unknown): string {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) input("Expected lowercase SHA-256");
  return value;
}
function bounded(value: unknown, maximum: number, minimum = 0): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum || value > maximum) input("Invalid ordered delivery bound");
  return value;
}
function captureOrder(value: unknown): Readonly<ChangesetWireOrder> {
  if (own(value, "protocol") !== CHANGESET_ORDER_PROTOCOL) input("Wrong ordered delivery protocol");
  const streamId = identity(own(value, "streamId"), 256), sequence = own(value, "sequence");
  if (typeof sequence !== "string" || !/^[1-9][0-9]{0,18}$/.test(sequence) || BigInt(sequence) >= 1n << 63n) input("Sequence must be canonical positive signed-int64 decimal");
  return Object.freeze({ protocol: CHANGESET_ORDER_PROTOCOL, streamId, sequence });
}
function ownBytes(value: unknown, maximum: number): Uint8Array {
  if (!(value instanceof Uint8Array)) input("A changeset Uint8Array is required");
  const proto = Object.getPrototypeOf(Uint8Array.prototype) as object;
  const slot = (key: string): unknown => Object.getOwnPropertyDescriptor(proto, key)!.get!.call(value);
  const buffer = slot("buffer"), offset = slot("byteOffset") as number, length = slot("byteLength") as number;
  if (!(buffer instanceof ArrayBuffer) || Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "resizable")?.get?.call(buffer)) input("Use fixed non-shared changeset buffers");
  if (length > maximum) input("Ordered message exceeds maxMessageBytes");
  return new Uint8Array(new Uint8Array(buffer, offset, length));
}
function captureMessage(value: unknown, receiverId: string, maximum: number): ChangesetEnvelope {
  if (own(value, "protocol") !== BASE_PROTOCOL || own(value, "receiverId") !== receiverId) input("Wrong delivery protocol or receiver");
  return Object.freeze({ protocol: BASE_PROTOCOL, receiverId,
    deliveryId: identity(own(value, "deliveryId"), 512), sha256: digest(own(value, "sha256")),
    changeset: ownBytes(own(value, "changeset"), maximum) });
}
function batchValues(value: unknown): readonly unknown[] {
  if (!Array.isArray(value)) input("Use 1..256 ordered messages or receipts");
  const length = bounded(Object.getOwnPropertyDescriptor(value, "length")?.value, 256, 1);
  const values: unknown[] = [];
  for (let index = 0; index < length; index++) {
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    if (descriptor === undefined || !Object.hasOwn(descriptor, "value"))
      input("Batch entries must be own data properties");
    values.push(descriptor.value);
  }
  return values;
}
function captureMessages(
  values: readonly unknown[], receiverId: string, maximum: number, maximumBatch: number,
): readonly ChangesetEnvelope[] {
  const messages: ChangesetEnvelope[] = [], ids = new Set<string>();
  let remaining = maximumBatch;
  for (const value of values) {
    // ownBytes checks the remaining aggregate limit BEFORE allocating a copy.
    const message = captureMessage(value, receiverId, Math.min(maximum, remaining));
    if (ids.has(message.deliveryId)) input("A batch repeats a delivery identity");
    ids.add(message.deliveryId);
    remaining -= message.changeset.byteLength;
    messages.push(message);
  }
  return Object.freeze(messages);
}
function requireNext(previous: ChangesetWireOrder | undefined, next: ChangesetWireOrder): void {
  if (previous !== undefined && BigInt(next.sequence) !== BigInt(previous.sequence) + 1n)
    input("Batch messages must be contiguous and in source order");
}
async function verifyHash(message: ChangesetEnvelope): Promise<void> {
  if (globalThis.crypto?.subtle === undefined) input("Ordered delivery requires Web Crypto SHA-256");
  const bytes = new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(message.changeset)));
  if (Array.from(bytes, (v) => v.toString(16).padStart(2, "0")).join("") !== message.sha256) input("Ordered payload does not match its digest");
}
function budget(options: ChangesetDeliveryOptions = {}) {
  const signal = options.signal, timeoutMs = options.timeoutMs;
  if (signal !== undefined) {
    try { Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal); }
    catch { input("signal must be an AbortSignal"); }
  }
  if (timeoutMs !== undefined) bounded(timeoutMs, 2_147_483_647, 1);
  const deadline = timeoutMs === undefined ? undefined : performance.now() + timeoutMs;
  const check = (): void => {
    if (signal?.aborted) throw new OrderedChangesetDeliveryError("ERR_FSQLITE_ORDERED_CANCELLED", "Ordered delivery cancelled");
    if (deadline !== undefined && performance.now() >= deadline) throw new OrderedChangesetDeliveryError("ERR_FSQLITE_ORDERED_TIMEOUT", "Ordered delivery deadline expired");
  };
  const remaining = (): ChangesetDeliveryOptions => {
    check();
    const result: ChangesetDeliveryOptions = {};
    if (signal !== undefined) result.signal = signal;
    if (deadline !== undefined) result.timeoutMs = Math.max(1, Math.ceil(deadline - performance.now()));
    return result;
  };
  check(); return { check, remaining };
}
function decisions(value: unknown): ApplyChangesetResult {
  const applied = bounded(own(value, "applied"), 100_000), omitted = bounded(own(value, "omitted"), 100_000), replayed = own(value, "replayed");
  if (applied + omitted > 100_000 || typeof replayed !== "boolean") mismatch("Invalid ordered receipt decisions");
  return Object.freeze({ applied, omitted, replayed });
}

/** Reject premature/forged application results INSIDE the ledger transaction. */
async function applyOnce(
  scoped: ChangesetTarget, apply: OrderedDeliveryApply, message: ChangesetEnvelope,
  options: ChangesetDeliveryOptions,
): Promise<ApplyChangesetResult> {
  const activity: { calls: number; pending: Promise<unknown> | null; completed: boolean; result: unknown; open: boolean } =
    { calls: 0, pending: null, completed: false, result: null, open: true };
  const target: ChangesetTarget = {
    transaction: (work, settings) => {
      if (!activity.open || ++activity.calls !== 1) mismatch("The application must use exactly one supplied transaction");
      const pending = scoped.transaction(work, settings).then((result) => {
        activity.completed = true; activity.result = result; return result;
      });
      activity.pending = pending;
      void pending.catch(() => {});
      return pending;
    },
  };
  try {
    const response = await apply(target, message, options);
    activity.open = false;
    const premature = !activity.completed;
    if (activity.pending !== null) await activity.pending;
    if (activity.calls !== 1 || premature) mismatch("Application returned before its transaction completed or ignored the supplied target");
    const result = decisions(response), actual = decisions(activity.result);
    for (const key of ["applied", "omitted", "replayed"] as const)
      if (result[key] !== actual[key]) mismatch("Application result differs from its SQL result");
    return result;
  } finally {
    activity.open = false;
    if (activity.pending !== null) await activity.pending.catch(() => {});
  }
}

interface ReceivedMessage {
  readonly message: ChangesetEnvelope;
  readonly position: Readonly<ChangesetWireOrder>;
  readonly changes: number;
}

/** Copy the entire bounded batch before hashing or invoking application code. */
function captureBatch(
  values: readonly unknown[], receiverId: string, streamId: string,
  maximum: number, maximumBatch: number,
): readonly ReceivedMessage[] {
  if (!Array.isArray(values)) input("An array of 1..256 ordered envelopes is required");
  const length = bounded(Object.getOwnPropertyDescriptor(values, "length")?.value, 256, 1);
  const admitted: ReceivedMessage[] = [];
  const ids = new Set<string>();
  let bytes = 0, previous: bigint | undefined;
  for (let i = 0; i < length; i++) {
    // Do not invoke an element accessor or a caller-supplied iterator.
    const element = Object.getOwnPropertyDescriptor(values, String(i));
    if (element === undefined || !Object.hasOwn(element, "value"))
      input("Batch envelopes must be own data properties");
    const position = captureOrder(own(element.value, "order"));
    const sequence = BigInt(position.sequence);
    if (position.streamId !== streamId) input("Wrong source stream incarnation");
    if (previous !== undefined && sequence !== previous + 1n)
      input("Batch sequences must be contiguous and in source order");
    const message = captureMessage(element.value, receiverId, Math.min(maximum, maximumBatch - bytes));
    if (ids.has(message.deliveryId)) input("A batch must contain distinct delivery identities");
    ids.add(message.deliveryId);
    bytes += message.changeset.byteLength;
    // Even a malformed last message must reject before the first application.
    const changes = inspectChangeset(message.changeset).changeCount;
    admitted.push(Object.freeze({ message, position, changes }));
    previous = sequence;
  }
  return Object.freeze(admitted);
}

/** Build private receipts only after every identity and decision is checked. */
function batchReceipts(
  results: readonly ChangesetOrderResult[], admitted: readonly ReceivedMessage[],
): readonly OrderedChangesetReceipt[] {
  if (!Array.isArray(results) || results.length !== admitted.length)
    mismatch("Ledger did not return the complete ordered batch");
  return Object.freeze(admitted.map(({ message, position, changes }, index) => {
    const result = results[index]!;
    if (result.receiverId !== message.receiverId || result.sourceId !== position.streamId ||
        result.sequence.toString() !== position.sequence || result.deliveryId !== message.deliveryId ||
        result.sha256 !== message.sha256 || result.byteLength !== message.changeset.byteLength)
      mismatch("Ledger did not confirm this exact batch member");
    const counts = decisions(result);
    if (counts.applied + counts.omitted !== changes)
      mismatch("Ordered receipt decisions disagree with batch message count");
    return Object.freeze({ protocol: BASE_PROTOCOL, receiverId: message.receiverId,
      deliveryId: message.deliveryId, sha256: message.sha256, byteLength: result.byteLength,
      ...counts, confirmed: true as const, order: position });
  }));
}

/**
 * Open an already provisioned ledger; never enroll an incoming stream implicitly.
 * Application and ordering commit together. Confirmation happens only afterwards,
 * including for historical replays, and is never raced against a timer.
 */
export async function createOrderedChangesetReceiver(
  order: ChangesetOrder, options: OrderedChangesetReceiverOptions,
): Promise<OrderedChangesetReceiver> {
  const apply = options?.apply, confirm = options?.confirmCommit;
  const maximum = bounded(options?.maxMessageBytes ?? 8 * 1024 * 1024, HARD_BYTES, 1);
  const maximumBatch = bounded(options?.maxBatchBytes ?? HARD_BYTES, HARD_BYTES, 1);
  const batchForeignKeys = options?.batchForeignKeys;
  if (batchForeignKeys !== undefined && batchForeignKeys !== "defer")
    input("batchForeignKeys must be defer when supplied");
  if (!(order instanceof ChangesetOrder) || typeof apply !== "function" || typeof confirm !== "function") input("An initialized ledger, application callback and commit confirmation are required");
  const head = await order.head();
  const receiverId = identity(head.receiverId, 256), streamId = identity(head.sourceId, 256);
  let active = false;
  return Object.freeze({ receiverId, streamId,
    receive: async (value: unknown, options: ChangesetDeliveryOptions = {}): Promise<OrderedChangesetReceipt> => {
      if (active) throw new OrderedChangesetDeliveryError("ERR_FSQLITE_ORDERED_BUSY", "This ordered receiver is already active");
      active = true;
      try {
        const time = budget(options), position = captureOrder(own(value, "order"));
        if (position.streamId !== streamId) input("Wrong source stream incarnation");
        const message = captureMessage(value, receiverId, maximum), byteLength = message.changeset.byteLength;
        const changes = inspectChangeset(message.changeset).changeCount;
        await verifyHash(message); time.check();
        const result = await order.apply({ sequence: BigInt(position.sequence), deliveryId: message.deliveryId,
          sha256: message.sha256, changeset: message.changeset },
          async (scoped, bytes) => {
            const applied = await applyOnce(scoped, apply,
              Object.freeze({ ...message, changeset: bytes }), time.remaining());
            // Reject inside the ledger transaction: once committed, a false
            // count becomes a retained replay that the sender can never accept.
            if (applied.applied + applied.omitted !== changes)
              mismatch("Application decisions disagree with message count");
            return applied;
          }, time.remaining());
        time.check();
        if (result.receiverId !== receiverId || result.sourceId !== streamId || result.sequence.toString() !== position.sequence ||
            result.deliveryId !== message.deliveryId || result.sha256 !== message.sha256 || result.byteLength !== byteLength)
          mismatch("Ledger did not confirm this ordered application");
        const counts = decisions(result);
        // Retained receipts may predate count admission. Do not confirm or
        // silently repair an inconsistent historical decision on replay.
        if (counts.applied + counts.omitted !== changes)
          mismatch("Ordered receipt decisions disagree with message count");
        // This barrier must never run from within order.apply's SQL transaction.
        await confirm();
        time.check();
        return Object.freeze({ protocol: BASE_PROTOCOL, receiverId, deliveryId: message.deliveryId,
          sha256: message.sha256, byteLength, ...counts, confirmed: true, order: position });
      } finally { active = false; }
    },
    receiveBatch: async (values: readonly unknown[], options: ChangesetDeliveryOptions = {}): Promise<readonly OrderedChangesetReceipt[]> => {
      if (active) throw new OrderedChangesetDeliveryError("ERR_FSQLITE_ORDERED_BUSY", "This ordered receiver is already active");
      active = true;
      try {
        const time = budget(options);
        const admitted = captureBatch(values, receiverId, streamId, maximum, maximumBatch);
        for (const { message } of admitted) { time.check(); await verifyHash(message); }
        time.check();
        const first = BigInt(admitted[0]!.position.sequence);
        const results = await order.applyBatch(admitted.map(({ message, position }) => ({
          sequence: BigInt(position.sequence), deliveryId: message.deliveryId,
          sha256: message.sha256, changeset: message.changeset,
        })), async (scoped, bytes, metadata) => {
          // Replayed members skip application. Never use callback count as identity.
          const entry = admitted[Number(metadata.sequence - first)];
          if (entry === undefined || metadata.receiverId !== receiverId || metadata.sourceId !== streamId ||
              metadata.deliveryId !== entry.message.deliveryId || metadata.sha256 !== entry.message.sha256 ||
              metadata.byteLength !== entry.message.changeset.byteLength)
            mismatch("Ledger selected a different batch member");
          const result = await applyOnce(scoped, apply,
            Object.freeze({ ...entry.message, changeset: bytes }), time.remaining());
          if (result.applied + result.omitted !== entry.changes)
            mismatch("Application decisions disagree with batch message count");
          return result;
        }, { ...time.remaining(), maxBatchBytes: maximumBatch,
          ...(batchForeignKeys === undefined ? {} : { foreignKeys: batchForeignKeys }) });
        time.check();
        const receipts = batchReceipts(results, admitted);
        // All fresh members share one committed transaction. Keep admission closed
        // while this single barrier settles; do not race it against cancellation.
        // Historical-only batches also require confirmation, exactly as receive().
        await confirm();
        time.check();
        return receipts;
      } finally { active = false; }
    },
  });
}

function baseReceipt(value: unknown, message: ChangesetEnvelope, byteLength: number): ChangesetDeliveryReceipt {
  if (own(value, "protocol") !== BASE_PROTOCOL || own(value, "receiverId") !== message.receiverId ||
      own(value, "deliveryId") !== message.deliveryId || own(value, "sha256") !== message.sha256 ||
      own(value, "byteLength") !== byteLength || own(value, "confirmed") !== true)
    mismatch("Receiver did not confirm this exact ordered payload");
  return Object.freeze({ protocol: BASE_PROTOCOL, receiverId: message.receiverId, deliveryId: message.deliveryId,
    sha256: message.sha256, byteLength, ...decisions(value), confirmed: true });
}

function sourceMetadata(record: unknown, message: ChangesetEnvelope, byteLength: number) {
  if (record === null) mismatch("Ordered delivery has no retained source identity");
  const entry = own(record, "delivery"), sequence = own(entry, "sequence");
  if (typeof sequence !== "bigint" || sequence < 1n || sequence >= 1n << 63n ||
      own(entry, "deliveryId") !== message.deliveryId || own(entry, "sha256") !== message.sha256 ||
      own(entry, "byteLength") !== byteLength || typeof own(entry, "acknowledged") !== "boolean")
    mismatch("Source metadata does not identify this ordered delivery");
  return { sequence, changes: bounded(own(entry, "changes"), 100_000) };
}
function orderedReceipt(
  response: unknown, message: ChangesetEnvelope, order: ChangesetWireOrder,
  byteLength: number, changes: number,
): OrderedChangesetReceipt {
  const proof = captureOrder(own(response, "order"));
  if (proof.streamId !== order.streamId || proof.sequence !== order.sequence)
    mismatch("Receiver did not confirm the exact source sequence");
  const receipt = baseReceipt(response, message, byteLength);
  if (receipt.applied + receipt.omitted !== changes)
    mismatch("Ordered receipt decisions disagree with source change count");
  return Object.freeze({ ...receipt, order });
}

/**
 * Pump-compatible transport whose successful return proves matching source order.
 * Read authoritative outbox metadata before transport; never renumber retries.
 * Legacy/stripped order receipts cannot release source payloads through this API.
 * No source SQL transaction is held across transport; the pump still owns ACK
 * persistence, its source-confirmation barrier and explicit omission policy.
 */
export function createOrderedChangesetTransport(
  source: OrderedChangesetSource, options: OrderedChangesetTransportOptions,
): ChangesetTransport {
  const receiverId = identity(options?.receiverId, 256), streamId = identity(options?.streamId, 256);
  const deliver = options?.deliver, read = source?.read, binding = source?.receiverId;
  const maximum = bounded(options?.maxMessageBytes ?? 8 * 1024 * 1024, HARD_BYTES, 1);
  if (typeof deliver !== "function" || typeof read !== "function" || (binding !== undefined && binding !== receiverId))
    input("Use a matching receiver-bound source and an ordered transport");
  let active = false;
  return async (value, options = {}) => {
    if (active) throw new OrderedChangesetDeliveryError("ERR_FSQLITE_ORDERED_BUSY", "This ordered transport is already active");
    active = true;
    try {
      const time = budget(options), message = captureMessage(value, receiverId, maximum);
      const byteLength = message.changeset.byteLength;
      const expectedChanges = inspectChangeset(message.changeset).changeCount;
      await verifyHash(message); time.check();
      const record = await read.call(source, message.deliveryId); time.check();
      const { sequence, changes } = sourceMetadata(record, message, byteLength);
      // Refuse stale/corrupt source counters before the receiver can commit.
      if (changes !== expectedChanges)
        mismatch("Source change count disagrees with the complete message payload");
      const order = Object.freeze({ protocol: CHANGESET_ORDER_PROTOCOL, streamId, sequence: sequence.toString() });
      const response = await deliver(Object.freeze({ ...message, order }), time.remaining());
      time.check();
      return orderedReceipt(response, message, order, byteLength, changes);
    } finally { active = false; }
  };
}

/**
 * Explicit atomic-window transport. Read each authoritative source sequence,
 * admit the whole contiguous window before sending, and validate EVERY receipt
 * before returning any of them. The peer must use atomic batch reception, not a
 * loop over single-message delivery. This is not an HTTP codec or auto-batching
 * pump; callers own authentication, omission consent, durable source ACKs and
 * recovery of lost responses. No source transaction spans network I/O.
 */
export function createOrderedChangesetBatchTransport(
  source: OrderedChangesetSource, options: OrderedChangesetBatchTransportOptions,
): OrderedChangesetBatchTransport {
  const receiverId = identity(options?.receiverId, 256), streamId = identity(options?.streamId, 256);
  const deliver = options?.deliver, read = source?.read, binding = source?.receiverId;
  const maximum = bounded(options?.maxMessageBytes ?? 8 * 1024 * 1024, HARD_BYTES, 1);
  const maximumBatch = bounded(options?.maxBatchBytes ?? HARD_BYTES, HARD_BYTES, 1);
  if (typeof deliver !== "function" || typeof read !== "function" || (binding !== undefined && binding !== receiverId))
    input("Use a matching receiver-bound source and an ordered batch transport");
  let active = false;
  return async (values, options = {}) => {
    if (active) throw new OrderedChangesetDeliveryError("ERR_FSQLITE_ORDERED_BUSY", "This ordered batch transport is already active");
    active = true;
    try {
      const time = budget(options);
      const messages = captureMessages(batchValues(values), receiverId, maximum, maximumBatch);
      const lengths = messages.map(message => message.changeset.byteLength);
      // Match the receiver's full-changeset contract before source reads or
      // transport. A corrupt retained count must not be discovered only after
      // the remote transaction commits; malformed tails cannot escape admission.
      const expectedCounts = messages.map(message => inspectChangeset(message.changeset).changeCount);
      for (const message of messages) { await verifyHash(message); time.check(); }
      const outgoing: OrderedChangesetEnvelope[] = [], counts: number[] = [];
      for (const message of messages) {
        const record = await read.call(source, message.deliveryId); time.check();
        const { sequence, changes } = sourceMetadata(record, message, lengths[outgoing.length]!);
        if (changes !== expectedCounts[outgoing.length])
          mismatch("Source change count disagrees with the complete batch payload");
        counts.push(changes);
        const order = Object.freeze({ protocol: CHANGESET_ORDER_PROTOCOL, streamId, sequence: sequence.toString() });
        requireNext(outgoing.at(-1)?.order, order);
        outgoing.push(Object.freeze({ ...message, order }));
      }
      // Authority is retained source metadata, never caller-supplied order.
      const response = await deliver(Object.freeze(outgoing), time.remaining());
      time.check();
      const receipts = batchValues(response);
      if (receipts.length !== outgoing.length) mismatch("Receiver did not confirm the complete ordered batch");
      const verified: OrderedChangesetReceipt[] = [];
      for (let index = 0; index < outgoing.length; index++) {
        const message = outgoing[index]!;
        verified.push(orderedReceipt(receipts[index], message, message.order, lengths[index]!, counts[index]!));
      }
      time.check();
      return Object.freeze(verified);
    } finally { active = false; }
  };
}
