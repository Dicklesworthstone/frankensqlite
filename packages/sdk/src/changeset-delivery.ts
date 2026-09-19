import { applyChangeset } from "./changeset-apply";
import type { ApplyChangesetOptions, ApplyChangesetResult, ChangesetTarget } from "./changeset-apply";
import { decodeChangeset } from "./changeset-codec";
import type { ChangesetOutbox, OutboxDelivery } from "./changeset-outbox";

export const CHANGESET_DELIVERY_PROTOCOL = "fsqlite-changeset-v1";
export interface ChangesetEnvelope {
  readonly protocol: typeof CHANGESET_DELIVERY_PROTOCOL;
  readonly receiverId: string;
  /** Globally source-qualified identity retained by both outbox and inbox. */
  readonly deliveryId: string;
  readonly sha256: string;
  readonly changeset: Uint8Array;
}
export interface ChangesetDeliveryReceipt extends ApplyChangesetResult {
  readonly protocol: typeof CHANGESET_DELIVERY_PROTOCOL;
  readonly receiverId: string;
  readonly deliveryId: string;
  readonly sha256: string;
  readonly byteLength: number;
  /** The configured commit-confirmation barrier completed, including on replay. */
  readonly confirmed: true;
}
export interface ChangesetDeliveryOptions {
  /** Cooperative: in-flight SQL, transport and confirmation must settle first. */
  signal?: AbortSignal;
  /** Total monotonic budget, 1..2147483647 ms; never races a commit against a timer. */
  timeoutMs?: number;
}
export type ChangesetDeliveryPhase = "admission" | "validate" | "receiver-apply" | "receiver-confirm" |
  "source-read" | "source-confirm" | "transport" | "receipt" | "source-ack";
export class ChangesetDeliveryError extends Error {
  constructor(readonly code: "ERR_FSQLITE_DELIVERY_INPUT" | "ERR_FSQLITE_DELIVERY_BUSY" |
    "ERR_FSQLITE_DELIVERY_FAILED" | "ERR_FSQLITE_DELIVERY_RECEIPT" |
    "ERR_FSQLITE_DELIVERY_CANCELLED" | "ERR_FSQLITE_DELIVERY_TIMEOUT" |
    "ERR_FSQLITE_DELIVERY_LIMIT", readonly phase: ChangesetDeliveryPhase,
    readonly deliveryId: string | null, cause: unknown) {
    super(`Changeset delivery stopped during ${phase}; retain the same delivery identity and reconcile commit/confirmation state before retrying`, { cause });
    this.name = "ChangesetDeliveryError";
  }
}
export interface ChangesetReceiverOptions {
  /** Routing identity, NOT authentication. Authenticate callers in the transport. */
  receiverId: string;
  tables: readonly string[];
  /**
   * Required durability boundary on the SAME top-level target. Snapshot stores
   * must checkpoint/recover here. An already durable SQL target may explicitly
   * supply an async no-op. A memory no-op does NOT establish durability.
   */
  confirmCommit: () => Promise<unknown>;
  onConflict?: ApplyChangesetOptions["onConflict"];
  /** Before copying or decoding a received payload. Default 8 MiB, max 64 MiB. */
  maxMessageBytes?: number;
}
const HARD_BYTES = 64 * 1024 * 1024;
const fold = (s: string): string => s.replace(/[A-Z]/g, c => c.toLowerCase());
function reject(code: ChangesetDeliveryError["code"], message: string): never {
  throw new ChangesetDeliveryError(code, "validate", null, new Error(message));
}
function input(message: string): never { return reject("ERR_FSQLITE_DELIVERY_INPUT", message); }
function failure(cause: unknown, phase: ChangesetDeliveryPhase, id: string | null): ChangesetDeliveryError {
  return new ChangesetDeliveryError(cause instanceof ChangesetDeliveryError ? cause.code : "ERR_FSQLITE_DELIVERY_FAILED", phase, id, cause);
}
function field(value: unknown, name: string): unknown {
  if (typeof value !== "object" || value === null || Array.isArray(value)) input("Expected a delivery record");
  const d = Object.getOwnPropertyDescriptor(value, name);
  if (d === undefined || !Object.hasOwn(d, "value")) input(`Delivery ${name} must be an own data property`);
  return d.value;
}
function identity(value: unknown, maximum: number): string {
  if (typeof value !== "string" || !value.length || value.length > maximum || value.includes("\0")) input("Invalid delivery/receiver identity");
  const bytes = new TextEncoder().encode(value);
  if (bytes.length > maximum || new TextDecoder("utf-8", { ignoreBOM: true }).decode(bytes) !== value) input("Identity must be bounded valid UTF-8");
  return value;
}
function digest(value: unknown): string {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) input("Expected lowercase SHA-256");
  return value;
}
function bound(value: unknown, fallback: number, maximum: number): number {
  const n = value === undefined ? fallback : value;
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 1 || n > maximum) input(`Expected an integer in 1..${maximum}`);
  return n;
}
/** Intrinsic access avoids subclass getters/iterators bypassing the copy budget. */
function ownBytes(value: unknown, maximum: number): Uint8Array {
  if (!(value instanceof Uint8Array)) input("A changeset Uint8Array is required");
  const proto = Object.getPrototypeOf(Uint8Array.prototype) as object;
  const get = (key: string): unknown => Object.getOwnPropertyDescriptor(proto, key)!.get!.call(value);
  const buffer = get("buffer"), offset = get("byteOffset") as number, length = get("byteLength") as number;
  if (!(buffer instanceof ArrayBuffer) || Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "resizable")?.get?.call(buffer)) {
    input("Changesets require fixed, non-shared buffers");
  }
  if (length > maximum) reject("ERR_FSQLITE_DELIVERY_LIMIT", "Message exceeds the byte budget");
  // Constructing this view rejects detached buffers, even when length is zero.
  return new Uint8Array(new Uint8Array(buffer, offset, length));
}
async function hash(bytes: Uint8Array): Promise<string> {
  if (globalThis.crypto?.subtle === undefined) input("Changeset delivery requires Web Crypto SHA-256");
  const result = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", bytes));
  return Array.from(result, b => b.toString(16).padStart(2, "0")).join("");
}
function resultCounts(value: unknown, changes: number): ApplyChangesetResult {
  const applied = field(value, "applied"), omitted = field(value, "omitted"), replayed = field(value, "replayed");
  if (typeof applied !== "number" || !Number.isSafeInteger(applied) || applied < 0 ||
      typeof omitted !== "number" || !Number.isSafeInteger(omitted) || omitted < 0 ||
      applied + omitted !== changes || typeof replayed !== "boolean") {
    reject("ERR_FSQLITE_DELIVERY_RECEIPT", "Invalid delivery decision counts");
  }
  return { applied, omitted, replayed };
}
class DeliveryBudget {
  readonly signal: AbortSignal;
  readonly #deadline: number | undefined;
  readonly #timeout = new AbortController();
  readonly #reason = new Error("Changeset delivery deadline expired");
  #timer: ReturnType<typeof setTimeout> | undefined;
  constructor(options: ChangesetDeliveryOptions = {}) {
    const external = options.signal, timeoutMs = options.timeoutMs;
    if (external !== undefined) {
      try { Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(external); }
      catch { input("signal must be an AbortSignal"); }
    }
    if (timeoutMs !== undefined) {
      bound(timeoutMs, 1, 2_147_483_647);
      this.#deadline = performance.now() + timeoutMs;
    }
    this.signal = external === undefined ? this.#timeout.signal : AbortSignal.any([external, this.#timeout.signal]);
    this.#arm();
  }
  #expire(): void {
    if (this.#deadline !== undefined && performance.now() >= this.#deadline && !this.#timeout.signal.aborted) this.#timeout.abort(this.#reason);
  }
  #arm(): void {
    if (this.#deadline === undefined || this.signal.aborted) return;
    this.#timer = setTimeout(() => { this.#expire(); this.#arm(); }, Math.max(1, Math.ceil(this.#deadline - performance.now())));
  }
  checkpoint(): void {
    this.#expire();
    if (this.signal.aborted) throw new ChangesetDeliveryError(
      this.signal.reason === this.#reason ? "ERR_FSQLITE_DELIVERY_TIMEOUT" : "ERR_FSQLITE_DELIVERY_CANCELLED",
      "admission", null, this.signal.reason);
  }
  remainingMs(): number | undefined {
    this.checkpoint();
    return this.#deadline === undefined ? undefined : Math.max(1, Math.ceil(this.#deadline - performance.now()));
  }
  finish(): void { clearTimeout(this.#timer); }
}

/**
 * Verified, replay-safe receiver over the actual applyChangeset SQL path.
 * No ACK exists until confirmation finishes. SQL commit and remote success
 * remain distinct; a failed/lost response never authorizes a fresh delivery ID.
 */
export class ChangesetReceiver {
  readonly #target: ChangesetTarget;
  readonly #id: string;
  readonly #tables: readonly string[];
  readonly #confirm: () => Promise<unknown>;
  readonly #onConflict: ApplyChangesetOptions["onConflict"];
  readonly #maxBytes: number;
  #active = false;
  constructor(target: ChangesetTarget, options: ChangesetReceiverOptions) {
    this.#target = target;
    this.#id = identity(options?.receiverId, 256);
    const source = options?.tables, confirm = options?.confirmCommit, onConflict = options?.onConflict;
    this.#maxBytes = bound(options?.maxMessageBytes, 8 * 1024 * 1024, HARD_BYTES);
    if (!Array.isArray(source) || source.length > 256) input("An explicit table allowlist is required");
    const tables: string[] = [], seen = new Set<string>();
    for (let i = 0, n = source.length; i < n; i++) {
      const name = identity(source[i], 1024), key = fold(name);
      if (seen.has(key) || key.startsWith("sqlite_") || key.startsWith("__fsqlite_")) input("Use distinct application tables, not SDK/system tables");
      seen.add(key); tables.push(name);
    }
    if (typeof confirm !== "function" || (onConflict !== undefined && typeof onConflict !== "function")) input("A commit confirmation function and valid conflict policy are required");
    this.#tables = Object.freeze(tables); this.#confirm = confirm; this.#onConflict = onConflict;
  }
  get receiverId(): string { return this.#id; }

  async receive(message: unknown, options?: ChangesetDeliveryOptions): Promise<ChangesetDeliveryReceipt> {
    if (this.#active) throw new ChangesetDeliveryError("ERR_FSQLITE_DELIVERY_BUSY", "admission", null, new Error("This receiver is active; no additional request was queued"));
    this.#active = true;
    let budget: DeliveryBudget | undefined, phase: ChangesetDeliveryPhase = "validate", id: string | null = null;
    try {
      budget = new DeliveryBudget(options); budget.checkpoint();
      if (field(message, "protocol") !== CHANGESET_DELIVERY_PROTOCOL || field(message, "receiverId") !== this.#id) input("Wrong delivery protocol or receiver");
      id = identity(field(message, "deliveryId"), 512);
      const sha256 = digest(field(message, "sha256")), bytes = ownBytes(field(message, "changeset"), this.#maxBytes);
      const changes = decodeChangeset(bytes).reduce((n, t) => n + t.changes.length, 0);
      if (await hash(bytes) !== sha256) input("Payload does not match its advertised digest");
      budget.checkpoint(); phase = "receiver-apply";
      const applyOptions: ApplyChangesetOptions = { tables: this.#tables, deliveryId: id, signal: budget.signal };
      const remaining = budget.remainingMs();
      if (remaining !== undefined) applyOptions.timeoutMs = remaining;
      if (this.#onConflict !== undefined) applyOptions.onConflict = this.#onConflict;
      const result = resultCounts(await applyChangeset(this.#target, bytes, applyOptions), changes);
      budget.checkpoint(); phase = "receiver-confirm";
      // Replays MUST confirm too: the first call may have committed only in
      // memory, failed checkpointing, or lost its durable publication response.
      await this.#confirm(); budget.checkpoint();
      return Object.freeze({ protocol: CHANGESET_DELIVERY_PROTOCOL, receiverId: this.#id,
        deliveryId: id, sha256, byteLength: bytes.byteLength, ...result, confirmed: true });
    } catch (cause: unknown) { throw failure(cause, phase, id); }
    finally { budget?.finish(); this.#active = false; }
  }
}

/** Transport framing/authentication are caller-owned; success must return a receipt. */
export type ChangesetTransport = (message: ChangesetEnvelope, options: ChangesetDeliveryOptions) => Promise<unknown>;
export interface ChangesetPumpOptions {
  /** One fixed recipient for this outbox. Its acknowledgement state is not multicast. */
  receiverId: string;
  deliver: ChangesetTransport;
  /** Confirm the SAME source after reads and acknowledgements; recover snapshots here. */
  confirmSource: () => Promise<unknown>;
  maxMessageBytes?: number;
  /** Default false: retained receiver omissions require explicit sender acceptance. */
  allowOmissions?: boolean;
}
export interface ChangesetPumpRunOptions extends ChangesetDeliveryOptions {
  /** Maximum selected entries, including acknowledgement races. Default 100, max 10,000. */
  maxDeliveries?: number;
  /** Total payload bytes per run. Default 64 MiB, maximum 1 GiB. */
  maxBytes?: number;
}
export interface ChangesetPumpResult {
  readonly deliveries: number;
  readonly bytes: number;
  /** Original receipt decisions, including replayed deliveries; not new-write counts. */
  readonly applied: number;
  readonly omitted: number;
  readonly replays: number;
  readonly alreadyAcknowledged: number;
  /** empty means no pending row at the last read, not a permanent emptiness guarantee. */
  readonly stopped: "empty" | "limit";
}
function deliveryMetadata(value: unknown): OutboxDelivery {
  const sequence = field(value, "sequence"), byteLength = field(value, "byteLength"), changes = field(value, "changes");
  const deliveryId = identity(field(value, "deliveryId"), 512), sha256 = digest(field(value, "sha256"));
  const acknowledged = field(value, "acknowledged");
  if (typeof sequence !== "bigint" || sequence < 1n || sequence >= 1n << 63n ||
      typeof byteLength !== "number" || !Number.isSafeInteger(byteLength) || byteLength < 0 || byteLength > HARD_BYTES ||
      typeof changes !== "number" || !Number.isSafeInteger(changes) || changes < 0 || changes > 100_000 ||
      typeof acknowledged !== "boolean") input("Invalid outbox delivery metadata");
  return Object.freeze({ sequence, byteLength, changes, deliveryId, sha256, acknowledged });
}
function sameDelivery(a: OutboxDelivery, b: OutboxDelivery): boolean {
  return a.sequence === b.sequence && a.deliveryId === b.deliveryId && a.sha256 === b.sha256 &&
    a.byteLength === b.byteLength && a.changes === b.changes;
}
function verifyReceipt(value: unknown, delivery: OutboxDelivery, receiverId: string): ApplyChangesetResult {
  if (field(value, "protocol") !== CHANGESET_DELIVERY_PROTOCOL || field(value, "receiverId") !== receiverId ||
      field(value, "deliveryId") !== delivery.deliveryId || field(value, "sha256") !== delivery.sha256 ||
      field(value, "byteLength") !== delivery.byteLength || field(value, "confirmed") !== true) {
    reject("ERR_FSQLITE_DELIVERY_RECEIPT", "Receiver did not confirm this exact delivery");
  }
  return resultCounts(value, delivery.changes);
}

/**
 * One bounded, awaited outbox drain. No transaction is held across transport.
 * Every new run starts at the oldest pending entry; no cursor can skip a failure.
 * The source payload is reclaimed only after a matching confirmed receiver ACK.
 */
export class ChangesetDeliveryPump {
  readonly #outbox: Pick<ChangesetOutbox, "pending" | "read" | "acknowledge">;
  readonly #id: string;
  readonly #deliver: ChangesetTransport;
  readonly #confirm: () => Promise<unknown>;
  readonly #maxBytes: number;
  readonly #allowOmissions: boolean;
  #active = false;
  constructor(outbox: Pick<ChangesetOutbox, "pending" | "read" | "acknowledge">, options: ChangesetPumpOptions) {
    this.#outbox = outbox; this.#id = identity(options?.receiverId, 256);
    const deliver = options?.deliver, confirm = options?.confirmSource;
    const allowOmissions = options?.allowOmissions ?? false;
    if (typeof deliver !== "function" || typeof confirm !== "function" || typeof allowOmissions !== "boolean") input("Delivery requires a transport, source confirmation and explicit omission policy");
    this.#deliver = deliver; this.#confirm = confirm; this.#allowOmissions = allowOmissions;
    this.#maxBytes = bound(options?.maxMessageBytes, 8 * 1024 * 1024, HARD_BYTES);
  }

  async run(options: ChangesetPumpRunOptions = {}): Promise<ChangesetPumpResult> {
    if (this.#active) throw new ChangesetDeliveryError("ERR_FSQLITE_DELIVERY_BUSY", "admission", null, new Error("This pump is active; no run was queued"));
    this.#active = true;
    let budget: DeliveryBudget | undefined, phase: ChangesetDeliveryPhase = "admission", id: string | null = null;
    const counts = { deliveries: 0, bytes: 0, applied: 0, omitted: 0, replays: 0, alreadyAcknowledged: 0 };
    const result = (stopped: "empty" | "limit"): ChangesetPumpResult => Object.freeze({ ...counts, stopped });
    try {
      const maxDeliveries = bound(options.maxDeliveries, 100, 10_000), maxBytes = bound(options.maxBytes, HARD_BYTES, 1024 * 1024 * 1024);
      budget = new DeliveryBudget(options); budget.checkpoint();
      phase = "source-confirm";
      // A previous acknowledgement can be committed in memory but not yet
      // checkpointed. Even an empty pending list must not bypass its recovery.
      await this.#confirm(); budget.checkpoint();
      let lastSequence = 0n;
      for (let selected = 0; selected < maxDeliveries; selected++) {
        id = null; phase = "source-read"; budget.checkpoint();
        const pending = await this.#outbox.pending({ limit: 1 }); budget.checkpoint();
        if (!Array.isArray(pending) || pending.length > 1) input("Outbox exceeded its requested page bound");
        if (pending.length === 0) return result("empty");
        const next = deliveryMetadata(pending[0]); id = next.deliveryId;
        if (next.acknowledged || next.sequence <= lastSequence) input("Outbox did not advance past confirmed acknowledgements");
        if (next.byteLength > this.#maxBytes) reject("ERR_FSQLITE_DELIVERY_LIMIT", "Pending message exceeds maxMessageBytes; it was not skipped");
        if (next.byteLength > maxBytes - counts.bytes) {
          if (counts.deliveries === 0) reject("ERR_FSQLITE_DELIVERY_LIMIT", "The oldest message cannot fit this run's byte budget");
          return result("limit");
        }
        const loaded = await this.#outbox.read(id); budget.checkpoint();
        if (loaded === null) input("Selected outbox delivery disappeared; reconcile its retention state");
        const current = deliveryMetadata(field(loaded, "delivery")), data = field(loaded, "changeset");
        if (!sameDelivery(next, current)) input("Outbox delivery changed between selection and read");
        if (current.acknowledged) {
          if (data !== null) input("An acknowledged outbox entry retained unexpected payload bytes");
          phase = "source-confirm"; await this.#confirm(); budget.checkpoint();
          counts.alreadyAcknowledged++; lastSequence = current.sequence; continue;
        }
        const bytes = ownBytes(data, this.#maxBytes);
        if (bytes.byteLength !== current.byteLength || await hash(bytes) !== current.sha256 ||
            decodeChangeset(bytes).reduce((n, t) => n + t.changes.length, 0) !== current.changes) {
          input("Outbox payload disagrees with the selected identity/digest/count");
        }
        budget.checkpoint(); phase = "source-confirm";
        // Confirm AFTER reading: a concurrent record committed after the initial
        // confirmation must not be sent while its source image is still volatile.
        await this.#confirm(); budget.checkpoint(); phase = "transport";
        const transportOptions: ChangesetDeliveryOptions = { signal: budget.signal };
        const remaining = budget.remainingMs(); if (remaining !== undefined) transportOptions.timeoutMs = remaining;
        const response = await this.#deliver(Object.freeze({ protocol: CHANGESET_DELIVERY_PROTOCOL,
          receiverId: this.#id, deliveryId: id, sha256: current.sha256, changeset: bytes }), transportOptions);
        budget.checkpoint(); phase = "receipt";
        const receipt = verifyReceipt(response, current, this.#id);
        if (receipt.omitted !== 0 && !this.#allowOmissions) reject("ERR_FSQLITE_DELIVERY_RECEIPT", "Receiver omissions require explicit sender acceptance");
        budget.checkpoint(); phase = "source-ack";
        const acknowledged = await this.#outbox.acknowledge(id, current.sha256);
        if (typeof acknowledged !== "boolean") input("Invalid outbox acknowledgement result");
        // Once ack starts, finish its confirmation even if cancellation arrived.
        // A throwing/uncertain ack is instead reconciled at the next run's start.
        phase = "source-confirm"; await this.#confirm(); budget.checkpoint();
        counts.deliveries++; counts.bytes += current.byteLength;
        counts.applied += receipt.applied; counts.omitted += receipt.omitted;
        if (receipt.replayed) counts.replays++;
        lastSequence = current.sequence;
      }
      return result("limit");
    } catch (cause: unknown) { throw failure(cause, phase, id); }
    finally { budget?.finish(); this.#active = false; }
  }
}
