import type { ChangesetTarget } from "./changeset-apply";
import type {
  BootstrapInstallReceipt, BootstrapManifest, BootstrapOperationOptions,
  BootstrapProgress, BootstrapSourceManifestInput, ChangesetBootstrapReceiver,
} from "./changeset-bootstrap";
import {
  acknowledgeBootstrapInstall, acknowledgeFanoutBootstrapInstall,
  readBootstrapManifest,
} from "./changeset-bootstrap";
import {
  TABLE as OUTBOX, chunkId, ensure, find, load,
} from "./changeset-outbox-store";

/** Authenticate the peer and preserve each operation's manifest and controls. */
export type BootstrapTransferTransport = Pick<ChangesetBootstrapReceiver, "status" | "stage" | "install">;
export interface BootstrapTransferOptions extends BootstrapSourceManifestInput {
  transport: BootstrapTransferTransport;
  /** The SAME top-level source: checkpoint/recover snapshot storage here. */
  confirmSource: () => Promise<unknown>;
  /** Explicitly select per-replica acknowledgement for a required-replica source. */
  acknowledgement?: "single-recipient" | "fanout";
  /** Trusted incarnation; never derived from the incoming installation receipt. */
  orderedSourceId?: string;
  /** Refuse oversized chunks before transferring bodies to JavaScript. Default 8 MiB. */
  maxChunkBytes?: number;
}
export interface BootstrapTransferRunOptions extends BootstrapOperationOptions {
  /** Upload calls in this run, not total baseline size. Default 100; max 10,000. */
  maxChunks?: number;
  /** Uploaded bytes in this run. Default 64 MiB; max 1 GiB. */
  maxBytes?: number;
}
interface TransferCounts {
  readonly manifest: BootstrapManifest;
  readonly uploadedChunks: number;
  readonly uploadedBytes: number;
  /** Latest receiver observation; a staging prefix is not a durability receipt. */
  readonly receivedChunks: number;
}
export type BootstrapTransferResult =
  | (TransferCounts & { readonly stopped: "limit"; readonly installed: false })
  | (TransferCounts & {
      readonly stopped: "installed";
      readonly installed: true;
      readonly receipt: BootstrapInstallReceipt;
      /** Newly advanced source seed sequences, not payload bytes reclaimed. */
      readonly newlyAcknowledged: number;
    });
export type BootstrapTransferPhase =
  | "admission" | "source-confirm" | "manifest" | "status"
  | "source-read" | "stage" | "install" | "source-ack";
export class BootstrapTransferError extends Error {
  readonly code: `ERR_FSQLITE_BOOTSTRAP_TRANSFER_${"INPUT" | "BUSY" | "LIMIT" | "STATE" | "CANCELLED" | "TIMEOUT" | "FAILED"}`;
  constructor(
    kind: "INPUT" | "BUSY" | "LIMIT" | "STATE" | "CANCELLED" | "TIMEOUT" | "FAILED",
    readonly phase: BootstrapTransferPhase,
    readonly deliveryId: string | null,
    cause: unknown,
  ) {
    super(`Bootstrap transfer stopped during ${phase}; retain the original seed and reconcile before retrying`, { cause });
    this.name = "BootstrapTransferError";
    this.code = `ERR_FSQLITE_BOOTSTRAP_TRANSFER_${kind}`;
  }
}
const MAX_BYTES = 1024 * 1024 * 1024;
function fail(kind: "INPUT" | "STATE" | "LIMIT", message: string): never {
  throw new BootstrapTransferError(kind, "admission", null, new Error(message));
}
function text(value: unknown, maximum: number): string {
  if (typeof value !== "string" || !value.length || value.length > maximum || value.includes("\0")) fail("INPUT", "Invalid bootstrap route");
  const bytes = new TextEncoder().encode(value);
  if (bytes.length > maximum || new TextDecoder("utf-8", { ignoreBOM: true }).decode(bytes) !== value) fail("INPUT", "Bootstrap route requires bounded UTF-8");
  return value;
}
function integer(value: unknown, maximum: number, minimum = 0): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum || value > maximum) fail("INPUT", "Invalid bootstrap count or budget");
  return value;
}
function data(value: unknown, key: string): unknown {
  if (typeof value !== "object" || value === null || Array.isArray(value)) fail("STATE", "Invalid bootstrap response");
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  if (descriptor === undefined || !Object.hasOwn(descriptor, "value")) fail("STATE", "Bootstrap response must use own data properties");
  return descriptor.value;
}
function progress(value: unknown, manifest: BootstrapManifest): BootstrapProgress {
  if (value === null) return Object.freeze({ receivedChunks: 0, receivedBytes: 0, receivedChanges: 0, installed: false });
  const receivedChunks = integer(data(value, "receivedChunks"), manifest.chunks);
  const receivedBytes = integer(data(value, "receivedBytes"), manifest.byteLength);
  const receivedChanges = integer(data(value, "receivedChanges"), manifest.changes);
  const installed = data(value, "installed");
  if (typeof installed !== "boolean" || (installed && receivedChunks !== manifest.chunks) ||
      (receivedChunks === 0 && (receivedBytes !== 0 || receivedChanges !== 0)) ||
      (receivedChunks === manifest.chunks && (receivedBytes !== manifest.byteLength || receivedChanges !== manifest.changes))) {
    fail("STATE", "Inconsistent bootstrap progress");
  }
  return Object.freeze({ receivedChunks, receivedBytes, receivedChanges, installed });
}

/** Capture before asynchronous source verification; transport objects stay caller-owned. */
function installReceipt(value: unknown, manifest: BootstrapManifest, orderedSourceId: string | undefined): BootstrapInstallReceipt {
  for (const key of ["protocol", "receiverId", "deliveryId", "sha256", "chunks", "changes", "byteLength"] as const) {
    if (data(value, key) !== manifest[key]) fail("STATE", "Installation receipt does not match the outbound manifest");
  }
  const replayed = data(value, "replayed");
  if (data(value, "installed") !== true || data(value, "confirmed") !== true || typeof replayed !== "boolean") fail("STATE", "Only confirmed installation may acknowledge a source baseline");
  let order: BootstrapInstallReceipt["order"];
  if (orderedSourceId === undefined) {
    if ("order" in (value as object)) fail("STATE", "An ordered receipt requires a trusted source-incarnation policy");
  } else {
    const input = data(value, "order");
    if (data(input, "protocol") !== "fsqlite-ordered-changeset-v1" ||
        data(input, "streamId") !== orderedSourceId || data(input, "sequence") !== String(manifest.chunks)) fail("STATE", "Installation did not confirm the expected ordered source prefix");
    order = Object.freeze({ protocol: "fsqlite-ordered-changeset-v1", streamId: orderedSourceId, sequence: String(manifest.chunks) });
  }
  return Object.freeze({ protocol: manifest.protocol, receiverId: manifest.receiverId,
    deliveryId: manifest.deliveryId, sha256: manifest.sha256, chunks: manifest.chunks,
    changes: manifest.changes, byteLength: manifest.byteLength, installed: true, confirmed: true,
    replayed, ...(order === undefined ? {} : { order }) });
}

/** One monotonic budget; never abandon started SQL, network work or confirmation. */
class Budget {
  readonly signal: AbortSignal;
  readonly #controller = new AbortController();
  readonly #expired = new Error("Bootstrap transfer deadline expired");
  readonly #deadline: number | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  constructor(options: BootstrapOperationOptions) {
    const signal = options.signal, ms = options.timeoutMs;
    if (signal !== undefined) {
      try { Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal); }
      catch { fail("INPUT", "signal must be an AbortSignal"); }
    }
    if (ms !== undefined) this.#deadline = performance.now() + integer(ms, 2_147_483_647, 1);
    this.signal = signal === undefined ? this.#controller.signal : AbortSignal.any([signal, this.#controller.signal]);
    this.#arm();
  }
  #expire(): void {
    if (this.#deadline !== undefined && performance.now() >= this.#deadline && !this.#controller.signal.aborted) this.#controller.abort(this.#expired);
  }
  #arm(): void {
    if (this.#deadline === undefined || this.signal.aborted) return;
    this.#timer = setTimeout(() => { this.#expire(); this.#arm(); }, Math.max(1, Math.ceil(this.#deadline - performance.now())));
  }
  check(): void {
    this.#expire();
    if (this.signal.aborted) throw new BootstrapTransferError(
      this.signal.reason === this.#expired ? "TIMEOUT" : "CANCELLED", "admission", null, this.signal.reason,
    );
  }
  options(): BootstrapOperationOptions {
    this.check();
    return Object.freeze(this.#deadline === undefined ? { signal: this.signal } : {
      signal: this.signal, timeoutMs: Math.max(1, Math.ceil(this.#deadline - performance.now())),
    });
  }
  finish(): void { clearTimeout(this.#timer); }
}

/**
 * Restartable atomic-bootstrap delivery, separate from the per-message pump.
 * Recover the immutable source manifest, resume the receiver's staged prefix,
 * install it atomically, then accept ONLY its confirmed whole-install receipt.
 * No SQL transaction spans a transport or confirmation callback. No background
 * task, automatic retry, source recapture, local resume cursor or implicit reset.
 */
export class ChangesetBootstrapTransfer {
  readonly #source: ChangesetTarget;
  readonly #route: BootstrapSourceManifestInput;
  readonly #transport: BootstrapTransferTransport;
  readonly #confirm: () => Promise<unknown>;
  readonly #fanout: boolean;
  readonly #orderedSourceId: string | undefined;
  readonly #maxChunkBytes: number;
  #active = false;

  constructor(source: ChangesetTarget, options: BootstrapTransferOptions) {
    if (typeof source?.transaction !== "function") fail("INPUT", "A top-level source transaction owner is required");
    const receiverId = text(options?.receiverId, 256), deliveryId = text(options?.deliveryId, 480);
    const input = options?.tables;
    if (!Array.isArray(input) || !input.length || input.length > 64) fail("INPUT", "Use 1..64 explicit source tables");
    const tables: string[] = [], seen = new Set<string>();
    for (let i = 0, n = input.length; i < n; i++) {
      const name = text(input[i], 1024).replace(/[A-Z]/g, c => c.toLowerCase());
      if (seen.has(name) || name.startsWith("sqlite_") || name.startsWith("__fsqlite_")) fail("INPUT", "Use distinct application tables");
      seen.add(name); tables.push(name);
    }
    const acknowledgement = options.acknowledgement ?? "single-recipient";
    if (acknowledgement !== "single-recipient" && acknowledgement !== "fanout") fail("INPUT", "Unknown source acknowledgement policy");
    const transport = options.transport, confirm = options.confirmSource;
    const status = transport?.status, stage = transport?.stage, install = transport?.install;
    if (typeof status !== "function" || typeof stage !== "function" || typeof install !== "function" || typeof confirm !== "function") fail("INPUT", "Status, stage, install and same-source confirmation are required");
    this.#source = source;
    this.#route = Object.freeze({ receiverId, deliveryId, tables: Object.freeze(tables) });
    // Capture methods once; preserve receivers that use private fields.
    this.#transport = Object.freeze({ status: status.bind(transport), stage: stage.bind(transport), install: install.bind(transport) });
    this.#confirm = confirm;
    this.#fanout = acknowledgement === "fanout";
    const ordered = options.orderedSourceId;
    this.#orderedSourceId = ordered === undefined ? undefined : text(ordered, 256);
    this.#maxChunkBytes = integer(options.maxChunkBytes ?? 8 * 1024 * 1024, 64 * 1024 * 1024, 1);
  }

  /** Limit manifest recovery too, in the SAME snapshot as payload verification. */
  async #manifest(b: Budget): Promise<BootstrapManifest> {
    return this.#source.transaction(async executor => {
      const tx = {
        execute: executor.execute.bind(executor),
        query: async (sql: string, params?: Parameters<typeof executor.query>[1]) => {
          b.check(); const rows = await executor.query(sql, params); b.check(); return rows;
        },
      };
      if (!await ensure(tx, false)) fail("STATE", "No retained source seed");
      const root = await find(tx, this.#route.deliveryId);
      if (root === null || root.stream?.index !== 0 || root.stream.summary === null) fail("STATE", "The original source seed manifest is missing");
      // readBootstrapManifest verifies every pending payload before returning.
      // Gate those reads, not just the subsequent per-upload body load. Actual
      // BLOB length also rejects a body larger than its advertised metadata.
      const rows = (await tx.query(`SELECT coalesce(max(length(CAST(payload AS BLOB))),0) FROM ${OUTBOX} WHERE seq>=1 AND seq<=?`,
        [BigInt(root.stream.summary.chunks)])).rowArrays;
      if (!Array.isArray(rows) || rows.length !== 1 || !Array.isArray(rows[0]) || rows[0].length !== 1) fail("STATE", "Invalid source payload-size result");
      const value = rows[0][0];
      const size = typeof value === "bigint" && value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : value;
      if (typeof size !== "number" || !Number.isSafeInteger(size) || size < 0) fail("STATE", "Invalid source payload size");
      if (size > this.#maxChunkBytes) fail("LIMIT", "A retained seed chunk exceeds maxChunkBytes");
      // The inner API borrows this read-only scope. Only the outer transaction
      // owns COMMIT; no second source snapshot or network operation is created.
      return readBootstrapManifest({ transaction: async work => work(tx) }, this.#route, b.options());
    }, b.options());
  }

  async run(options: BootstrapTransferRunOptions = {}): Promise<BootstrapTransferResult> {
    if (this.#active) throw new BootstrapTransferError("BUSY", "admission", this.#route.deliveryId, new Error("This transfer is active; no run was queued"));
    this.#active = true;
    let b: Budget | undefined, phase: BootstrapTransferPhase = "admission";
    try {
      const maxChunks = integer(options.maxChunks ?? 100, 10_000, 1);
      const maxBytes = integer(options.maxBytes ?? 64 * 1024 * 1024, MAX_BYTES, 1);
      b = new Budget(options); b.check();
      phase = "source-confirm";
      // An earlier source ACK may have committed but failed its checkpoint.
      // Never bypass reconciliation just because no payloads remain pending.
      await this.#confirm(); b.check();
      phase = "manifest";
      const manifest = await this.#manifest(b); b.check();
      phase = "source-confirm";
      await this.#confirm(); b.check();
      phase = "status";
      let current = progress(await this.#transport.status(manifest, b.options()), manifest); b.check();
      let uploadedChunks = 0, uploadedBytes = 0;
      const counts = () => ({ manifest, uploadedChunks, uploadedBytes, receivedChunks: current.receivedChunks });
      while (current.receivedChunks < manifest.chunks) {
        if (uploadedChunks >= maxChunks) return Object.freeze({ ...counts(), installed: false, stopped: "limit" });
        const index = current.receivedChunks;
        phase = "source-read";
        const budget = b;
        const selected = await this.#source.transaction(async executor => {
          const tx = {
            execute: executor.execute.bind(executor),
            query: async (sql: string, params?: Parameters<typeof executor.query>[1]) => {
              budget.check(); const result = await executor.query(sql, params); budget.check(); return result;
            },
          };
          if (!await ensure(tx, false)) fail("STATE", "Source seed storage disappeared");
          const entry = await find(tx, chunkId(manifest.deliveryId, index)); budget.check();
          if (entry === null || entry.stream?.id !== manifest.deliveryId || entry.stream.index !== index ||
              entry.delivery.sequence !== BigInt(index + 1)) fail("STATE", "Source seed chunk disappeared or changed identity");
          // Counts and byte ceilings are checked BEFORE loading an admitted body.
          const d = entry.delivery;
          if (d.acknowledged) return { kind: "chunk" as const, payload: null, changes: d.changes, byteLength: d.byteLength };
          if (d.byteLength > this.#maxChunkBytes) fail("LIMIT", "The next chunk exceeds maxChunkBytes; it was not skipped");
          if (d.byteLength > maxBytes - uploadedBytes) return { kind: "limit" as const };
          const payload = await load(tx, entry); budget.check();
          return { kind: "chunk" as const, payload, changes: d.changes, byteLength: d.byteLength };
        }, b.options());
        b.check();
        if (selected.kind === "limit") {
          if (uploadedChunks === 0) fail("LIMIT", "The next chunk cannot fit a fresh run's byte budget");
          return Object.freeze({ ...counts(), installed: false, stopped: "limit" });
        }
        if (selected.payload === null) {
          // A peer may have completed install+ACK after our status read. Cleared
          // bytes do not authorize assuming installation or regenerating a seed.
          phase = "status";
          current = progress(await this.#transport.status(manifest, b.options()), manifest); b.check();
          if (!current.installed) fail("STATE", "Source bytes were reclaimed but the destination is not installed");
          break;
        }
        phase = "source-confirm";
        await this.#confirm(); b.check();
        phase = "stage";
        const next = progress(await this.#transport.stage(manifest, index, selected.payload, b.options()), manifest); b.check();
        if (next.receivedChunks < index + 1 || next.receivedBytes < current.receivedBytes + selected.byteLength ||
            next.receivedChanges < current.receivedChanges + selected.changes ||
            (next.receivedChunks === index + 1 && (next.receivedBytes !== current.receivedBytes + selected.byteLength ||
              next.receivedChanges !== current.receivedChanges + selected.changes))) fail("STATE", "Receiver staging progress did not cover the submitted prefix");
        uploadedChunks++; uploadedBytes += selected.byteLength; current = next;
      }
      phase = "install";
      // status(installed) is not a confirmed ACK. install() must reconfirm on
      // replay, even after all source bytes have already been reclaimed.
      const response = await this.#transport.install(manifest, b.options()); b.check();
      const receipt = installReceipt(response, manifest, this.#orderedSourceId);
      phase = "source-ack";
      const ackOptions = { ...b.options(), receiverId: this.#route.receiverId,
        ...(this.#orderedSourceId === undefined ? {} : { orderedSourceId: this.#orderedSourceId }) };
      const acknowledge = this.#fanout ? acknowledgeFanoutBootstrapInstall : acknowledgeBootstrapInstall;
      // Existing source APIs verify the complete receipt and retained hash chain,
      // enforce the trusted order policy, and atomically preserve slow replicas.
      const newlyAcknowledged = await acknowledge(this.#source, manifest, receipt, ackOptions);
      phase = "source-confirm";
      // Once a successful ACK returns, drain its confirmation even on cancellation.
      await this.#confirm(); b.check();
      integer(newlyAcknowledged, manifest.chunks);
      return Object.freeze({ ...counts(), installed: true, stopped: "installed", receipt, newlyAcknowledged });
    } catch (cause: unknown) {
      const kind = cause instanceof BootstrapTransferError
        ? cause.code.slice("ERR_FSQLITE_BOOTSTRAP_TRANSFER_".length) as ConstructorParameters<typeof BootstrapTransferError>[0]
        : "FAILED";
      throw new BootstrapTransferError(kind, phase, this.#route.deliveryId, cause);
    } finally { b?.finish(); this.#active = false; }
  }
}
