import type { ApplyChangesetResult, ChangesetTarget } from "./changeset-apply";
import type {
  ChangesetDeliveryOptions, ChangesetDeliveryReceipt, ChangesetEnvelope, ChangesetTransport,
} from "./changeset-delivery";
import type { OutboxReadResult } from "./changeset-outbox";
import { ChangesetOrder } from "./changeset-order";

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
}
export interface OrderedChangesetReceiver {
  readonly receiverId: string;
  readonly streamId: string;
  receive(message: unknown, options?: ChangesetDeliveryOptions): Promise<OrderedChangesetReceipt>;
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
        await verifyHash(message); time.check();
        const result = await order.apply({ sequence: BigInt(position.sequence), deliveryId: message.deliveryId,
          sha256: message.sha256, changeset: message.changeset },
          (scoped, bytes) => applyOnce(scoped, apply, Object.freeze({ ...message, changeset: bytes }), time.remaining()),
          time.remaining());
        time.check();
        if (result.receiverId !== receiverId || result.sourceId !== streamId || result.sequence.toString() !== position.sequence ||
            result.deliveryId !== message.deliveryId || result.sha256 !== message.sha256 || result.byteLength !== byteLength)
          mismatch("Ledger did not confirm this ordered application");
        const counts = decisions(result);
        // This barrier must never run from within order.apply's SQL transaction.
        await confirm();
        time.check();
        return Object.freeze({ protocol: BASE_PROTOCOL, receiverId, deliveryId: message.deliveryId,
          sha256: message.sha256, byteLength, ...counts, confirmed: true, order: position });
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
      await verifyHash(message); time.check();
      const record = await read.call(source, message.deliveryId); time.check();
      if (record === null) mismatch("Ordered delivery has no retained source identity");
      const entry = own(record, "delivery"), sequence = own(entry, "sequence");
      if (typeof sequence !== "bigint" || sequence < 1n || sequence >= 1n << 63n ||
          own(entry, "deliveryId") !== message.deliveryId || own(entry, "sha256") !== message.sha256 ||
          own(entry, "byteLength") !== byteLength || typeof own(entry, "acknowledged") !== "boolean")
        mismatch("Source metadata does not identify this ordered delivery");
      const changes = bounded(own(entry, "changes"), 100_000);
      const order = Object.freeze({ protocol: CHANGESET_ORDER_PROTOCOL, streamId, sequence: sequence.toString() });
      const response = await deliver(Object.freeze({ ...message, order }), time.remaining());
      time.check();
      const proof = captureOrder(own(response, "order"));
      if (proof.streamId !== streamId || proof.sequence !== order.sequence) mismatch("Receiver did not confirm the exact source sequence");
      const receipt = baseReceipt(response, message, byteLength);
      if (receipt.applied + receipt.omitted !== changes) mismatch("Ordered receipt decisions disagree with source change count");
      return Object.freeze({ ...receipt, order });
    } finally { active = false; }
  };
}
