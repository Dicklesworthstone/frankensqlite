import { applyChangeset } from "./changeset-apply";
import type { ApplyChangesetOptions, ApplyChangesetResult, ChangesetTarget } from "./changeset-apply";
import { decodeChangeset } from "./changeset-codec";

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
export type ChangesetDeliveryPhase = "admission" | "validate" | "receiver-apply" | "receiver-confirm";
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
