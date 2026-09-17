import {
  createFrankenSqliteWorker, RequestBudget, resolveResultEncoding,
  validateManagedSql, validateSnapshotBytes,
} from "@frankensqlite/worker";
import type { QueryRequest } from "@frankensqlite/worker";
import { FrankenDB } from "./database";
import type { QueryResult, ResultEncoding, SqlBindings } from "./types";
import type { WorkerLike, WorkerErrorEventLike } from "./worker-client";

export interface SnapshotPoolOptions {
  /** Dedicated replicas, 1..8. Defaults to 2. No live shared-database writes. */
  workers?: number;
  /** Active plus waiting queries, 1..4096. Defaults to 64. */
  maxPendingQueries?: number;
  /** Accounted input payload across active and waiting queries; default 128 MiB. */
  maxPendingBytes?: number;
  wasmUrl?: string;
  resultEncoding?: ResultEncoding;
  /** Must return a new, dedicated worker on every invocation. */
  worker?: () => WorkerLike;
}

export interface SnapshotQueryOptions {
  /** Waiting work is removed; active work drains before its result is discarded. */
  signal?: AbortSignal;
  /** Start deadline, not a timeout or interrupt of a running SQL statement. */
  waitTimeoutMs?: number;
}

export interface SnapshotPoolIdentity {
  readonly sha256: string;
  readonly byteLength: number;
}

export interface SnapshotPoolStats {
  readonly state: "open" | "closing" | "closed";
  readonly workers: number;
  readonly activeQueries: number;
  readonly waitingQueries: number;
  readonly pendingQueries: number;
  readonly pendingBytes: number;
  readonly completedQueries: number;
  readonly failedQueries: number;
  readonly rejectedQueries: number;
}

export class FrankenPoolError extends Error {
  readonly transient = false;
  constructor(readonly code: string, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "FrankenPoolError";
  }
}

interface Replica {
  db: FrankenDB;
  worker: WorkerLike;
  busy: boolean;
  onError: (event: WorkerErrorEventLike) => void;
}
interface QueryJob {
  request: QueryRequest;
  signal: AbortSignal | undefined;
  deadline: number | undefined;
  timer: ReturnType<typeof setTimeout> | undefined;
  onAbort: () => void;
  aborted: FrankenPoolError | null;
  active: boolean;
  release: () => void;
  resolve: (result: QueryResult) => void;
  reject: (cause: unknown) => void;
}

/** Parallel, bounded reads of one copied SQLite image, never a live write pool. */
export class FrankenSnapshotPool {
  readonly #replicas: Replica[];
  readonly #budget: RequestBudget;
  readonly #identity: Readonly<SnapshotPoolIdentity>;
  readonly #waiting: QueryJob[] = [];
  #active = 0;
  #nextId = 1;
  #completed = 0;
  #failed = 0;
  #rejected = 0;
  #state: SnapshotPoolStats["state"] = "open";
  #terminal: FrankenPoolError | null = null;
  #closePromise: Promise<void> | null = null;
  #drained: (() => void) | null = null;

  private constructor(replicas: Replica[], budget: RequestBudget, identity: SnapshotPoolIdentity) {
    this.#replicas = replicas;
    this.#budget = budget;
    this.#identity = Object.freeze(identity);
    for (const replica of replicas) {
      replica.onError = event => this.#crashed(event.message);
      replica.worker.addEventListener("error", replica.onError);
    }
  }

  static async open(snapshot: Uint8Array, options: SnapshotPoolOptions = {}): Promise<FrankenSnapshotPool> {
    const workers = options.workers ?? 2;
    const maxPendingQueries = options.maxPendingQueries ?? 64;
    const maxPendingBytes = options.maxPendingBytes ?? 128 * 1024 * 1024;
    const wasmUrl = options.wasmUrl;
    const resultEncoding = resolveResultEncoding(options.resultEncoding);
    const factory = options.worker ?? (() => createFrankenSqliteWorker());
    if (!Number.isInteger(workers) || workers < 1 || workers > 8 || typeof factory !== "function" ||
        (wasmUrl !== undefined && typeof wasmUrl !== "string")) {
      throw new FrankenPoolError("ERR_FSQLITE_POOL_INPUT", "Use 1..8 workers and a dedicated worker factory");
    }
    const budget = new RequestBudget({ maxPendingRequests: maxPendingQueries, maxPendingBytes });
    validateSnapshotBytes(snapshot);
    if (!(snapshot.buffer instanceof ArrayBuffer) || snapshot.byteLength * workers > 128 * 1024 * 1024) {
      throw new FrankenPoolError("ERR_FSQLITE_POOL_INPUT", "Use an unshared image with at most 128 MiB across replicas");
    }
    // Capture before the first await; neither caller mutation nor transferred
    // worker copies can change the identity or content of another replica.
    const image = new Uint8Array(snapshot);
    validateSnapshotBytes(image);
    const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", image));
    const identity = { sha256: Array.from(hash, n => n.toString(16).padStart(2, "0")).join(""), byteLength: image.byteLength };
    const seen = new Set<WorkerLike>();
    const opened = await Promise.allSettled(Array.from({ length: workers }, async () => {
      const worker = factory();
      if (seen.has(worker)) throw new FrankenPoolError("ERR_FSQLITE_POOL_INPUT", "A worker cannot serve two replicas");
      seen.add(worker);
      const db = await FrankenDB.open({ worker, snapshot: image.slice(), persistence: "memory", resultEncoding,
        ...(wasmUrl === undefined ? {} : { wasmUrl }),
        requestLimits: { maxPendingRequests: 1, maxPendingBytes: Math.max(maxPendingBytes, image.byteLength + 4096) } });
      try {
        await db.execute("PRAGMA query_only = ON");
        const mode = await db.query("PRAGMA query_only");
        if (mode.rowArrays?.length !== 1 || mode.rowArrays[0]?.length !== 1 ||
            (mode.rowArrays[0][0] !== 1 && mode.rowArrays[0][0] !== 1n)) {
          throw new FrankenPoolError("ERR_FSQLITE_POOL_READ_ONLY", "Core did not acknowledge query_only; refusing a mutable replica");
        }
        return { db, worker, busy: false, onError: (_event: WorkerErrorEventLike) => {} };
      } catch (cause: unknown) {
        try { await db.close(); }
        catch (cleanup: unknown) { throw new AggregateError([cause, cleanup], "Replica initialization and cleanup failed", { cause }); }
        throw cause;
      }
    }));
    const replicas = opened.flatMap(result => result.status === "fulfilled" ? [result.value] : []);
    const failures = opened.flatMap(result => result.status === "rejected" ? [result.reason as unknown] : []);
    if (failures.length !== 0) {
      const closed = await Promise.allSettled(replicas.map(replica => replica.db.close()));
      for (const result of closed) if (result.status === "rejected") failures.push(result.reason);
      throw new AggregateError(failures, "Snapshot pool initialization failed; all opened replicas were closed", { cause: failures[0] });
    }
    return new FrankenSnapshotPool(replicas, budget, identity);
  }

  get snapshot(): Readonly<SnapshotPoolIdentity> { return this.#identity; }

  get stats(): SnapshotPoolStats {
    const budget = this.#budget.stats;
    return Object.freeze({ state: this.#state, workers: this.#replicas.length,
      activeQueries: this.#active, waitingQueries: this.#waiting.length,
      pendingQueries: budget.pendingRequests, pendingBytes: budget.pendingBytes,
      completedQueries: this.#completed, failedQueries: this.#failed, rejectedQueries: this.#rejected });
  }

  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string, params: SqlBindings = [], options: SnapshotQueryOptions = {},
  ): Promise<QueryResult<Row>> {
    let release: (() => void) | undefined;
    try {
      this.#assertOpen();
      const signal = options.signal, wait = options.waitTimeoutMs;
      if (signal !== undefined && !(signal instanceof AbortSignal)) throw new TypeError("signal must be an AbortSignal");
      if (wait !== undefined && (!Number.isFinite(wait) || wait < 0 || wait > 2147483647)) {
        throw new RangeError("waitTimeoutMs must be in 0..2147483647");
      }
      if (signal?.aborted) throw cancelled(signal);
      const deadline = wait === undefined ? undefined : performance.now() + wait;
      const admission = this.#budget.admit({ kind: "query", requestId: this.#nextId++, sql, params });
      release = admission.release;
      const captured = admission.request as QueryRequest;
      readOnlySql(captured.sql);
      // RequestBudget already bounded/captured own scalar fields. Refuse shared
      // buffers (structuredClone would retain shared storage), then snapshot all
      // values once, preserving binary aliases within this admitted request.
      const values = Array.isArray(captured.params) ? captured.params : Object.values(captured.params ?? {});
      for (const value of values) if (value instanceof Uint8Array && !(value.buffer instanceof ArrayBuffer)) {
        throw new FrankenPoolError("ERR_FSQLITE_POOL_INPUT", "Pool parameters cannot use shared backing buffers");
      }
      const request = structuredClone(captured);
      this.#assertOpen(); // Binding/option getters can re-enter close or admission.
      if (signal?.aborted) throw cancelled(signal);
      const result = new Promise<QueryResult>((resolve, reject) => {
        const job: QueryJob = { request, signal, deadline, timer: undefined, active: false,
          aborted: null, release: admission.release, resolve, reject, onAbort: () => {} };
        job.onAbort = () => {
          job.aborted ??= cancelled(signal!);
          if (!job.active) { this.#remove(job, job.aborted); this.#pump(); }
        };
        this.#waiting.push(job);
        signal?.addEventListener("abort", job.onAbort, { once: true });
        if (wait !== undefined) job.timer = setTimeout(() => {
          if (!job.active) { this.#remove(job, timedOut()); this.#pump(); }
        }, wait);
        this.#pump();
      });
      return result as Promise<QueryResult<Row>>;
    } catch (cause: unknown) {
      release?.();
      this.#rejected++;
      return Promise.reject(cause);
    }
  }

  close(): Promise<void> {
    if (this.#closePromise !== null) return this.#closePromise;
    this.#state = "closing";
    const drained = this.#active === 0 && this.#waiting.length === 0 ? Promise.resolve()
      : new Promise<void>(resolve => { this.#drained = resolve; });
    this.#closePromise = drained.then(async () => {
      const results = await Promise.allSettled(this.#replicas.map(async replica => {
        try { await replica.db.close(); }
        finally { replica.worker.removeEventListener("error", replica.onError); }
      }));
      this.#state = "closed";
      const errors = results.flatMap(result => result.status === "rejected" ? [result.reason as unknown] : []);
      if (errors.length) throw new AggregateError(errors, "Snapshot pool close failed", { cause: errors[0] });
    });
    return this.#closePromise;
  }

  #assertOpen(): void {
    if (this.#terminal !== null) throw this.#terminal;
    if (this.#state !== "open") throw new FrankenPoolError("ERR_FSQLITE_POOL_CLOSED", "Snapshot pool is closing or closed");
  }

  #crashed(message: string): void {
    this.#terminal ??= new FrankenPoolError("ERR_FSQLITE_POOL_UNUSABLE", `A snapshot worker crashed: ${message}`);
    for (const job of [...this.#waiting]) this.#remove(job, this.#terminal);
    // No transparent replay/replacement. Other active reads drain on their own
    // replicas, and every listener/worker is eventually released by close.
    void this.close().catch(() => {});
  }

  #remove(job: QueryJob, cause: unknown): void {
    const index = this.#waiting.indexOf(job);
    if (index < 0) return;
    this.#waiting.splice(index, 1);
    this.#finish(job);
    this.#failed++;
    job.reject(cause);
  }

  #finish(job: QueryJob): void {
    if (job.timer !== undefined) clearTimeout(job.timer);
    job.signal?.removeEventListener("abort", job.onAbort);
    job.release();
  }

  #pump(): void {
    for (const replica of this.#replicas) {
      if (replica.busy || this.#terminal !== null) continue;
      while (this.#waiting.length > 0) {
        const job = this.#waiting[0]!;
        if (job.deadline !== undefined && performance.now() >= job.deadline) {
          this.#remove(job, timedOut());
          continue;
        }
        this.#waiting.shift();
        if (job.timer !== undefined) clearTimeout(job.timer);
        job.active = true;
        replica.busy = true;
        this.#active++;
        void this.#execute(replica, job);
        break;
      }
    }
    if (this.#active === 0 && this.#waiting.length === 0) this.#drained?.();
  }

  async #execute(replica: Replica, job: QueryJob): Promise<void> {
    try {
      const result = await replica.db.query(job.request.sql, job.request.params);
      if (job.aborted !== null) throw job.aborted;
      this.#completed++;
      job.resolve(result);
    } catch (cause: unknown) {
      this.#failed++;
      job.reject(cause);
    } finally {
      this.#finish(job);
      replica.busy = false;
      this.#active--;
      this.#pump();
    }
  }
}

function cancelled(signal: AbortSignal): FrankenPoolError {
  return new FrankenPoolError("ERR_FSQLITE_POOL_CANCELLED", "Snapshot read cancelled; active SQL must drain", { cause: signal.reason });
}
function timedOut(): FrankenPoolError {
  return new FrankenPoolError("ERR_FSQLITE_POOL_TIMEOUT", "Snapshot read did not start before its deadline; no SQL ran");
}

function readOnlySql(sql: string): void {
  validateManagedSql(sql);
  let offset = 0;
  const word = (): string => {
    while (offset < sql.length) {
      if (/[\s;\uFEFF]/.test(sql[offset]!)) { offset++; continue; }
      if (sql.startsWith("--", offset)) { const end = sql.indexOf("\n", offset + 2); offset = end < 0 ? sql.length : end + 1; continue; }
      if (sql.startsWith("/*", offset)) { const end = sql.indexOf("*/", offset + 2); offset = end < 0 ? sql.length : end + 2; continue; }
      break;
    }
    const token = /^[A-Za-z]+/.exec(sql.slice(offset))?.[0] ?? "";
    offset += token.length;
    return token.toUpperCase();
  };
  let first = word();
  if (first === "EXPLAIN") {
    first = word();
    if (first === "QUERY") { first = word() === "PLAN" ? word() : ""; }
  }
  // PRAGMA may take effect during preparation even under EXPLAIN. Never admit
  // it, ATTACH, maintenance, scripts or transaction controls. WITH may contain
  // DML, so the verified engine query_only guard remains authoritative as well.
  if (first !== "SELECT" && first !== "WITH") {
    throw new FrankenPoolError("ERR_FSQLITE_POOL_READ_ONLY", "Snapshot pools accept SELECT, WITH, and their EXPLAIN forms only");
  }
}
