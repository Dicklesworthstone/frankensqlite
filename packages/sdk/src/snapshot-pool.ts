import type { QueryRequest } from "@frankensqlite/worker";
import {
  createFrankenSqliteWorker,
  RequestBudget,
  resolveResultEncoding,
  validateManagedSql,
  validateSnapshotBytes,
} from "@frankensqlite/worker";
import { FrankenDB, observeDatabaseFailure } from "./database";
import type { QueryResult, ResultEncoding, SqlBindings } from "./types";
import type { WorkerErrorEventLike, WorkerLike } from "./worker-client";

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
  readonly generation: number;
}

export interface SnapshotQueryResult<Row extends Record<string, unknown> = Record<string, unknown>>
  extends QueryResult<Row> {
  readonly snapshot: Readonly<SnapshotPoolIdentity>;
}

export interface SnapshotRefreshResult {
  /** The new generation has been published, even when old-replica cleanup failed. */
  readonly snapshot: Readonly<SnapshotPoolIdentity>;
  readonly cleanupErrors: readonly unknown[];
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
  readonly refreshing: boolean;
  readonly pendingSnapshotBytes: number;
}

export class FrankenPoolError extends Error {
  readonly transient = false;
  constructor(
    readonly code: string,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "FrankenPoolError";
  }
}

interface Replica {
  db: FrankenDB;
  worker: WorkerLike;
  busy: boolean;
  watch: {
    failure: FrankenPoolError | null;
    notify: ((error: FrankenPoolError) => void) | null;
    onError: (event: WorkerErrorEventLike) => void;
    stopFailure: () => void;
  };
}
interface QueryJob {
  kind: "query";
  request: QueryRequest;
  signal: AbortSignal | undefined;
  deadline: number | undefined;
  timer: ReturnType<typeof setTimeout> | undefined;
  onAbort: () => void;
  aborted: FrankenPoolError | null;
  active: boolean;
  release: () => void;
  resolve: (result: SnapshotQueryResult) => void;
  reject: (cause: unknown) => void;
}

interface RefreshJob {
  kind: "refresh";
  image: Uint8Array<ArrayBuffer>;
  resolve: (result: SnapshotRefreshResult) => void;
  reject: (cause: unknown) => void;
}

interface PoolConfiguration {
  workers: number;
  maxPendingBytes: number;
  wasmUrl: string | undefined;
  resultEncoding: ResultEncoding;
  factory: () => WorkerLike;
}

/** Parallel, bounded reads of one copied SQLite image, never a live write pool. */
export class FrankenSnapshotPool {
  #replicas: Replica[];
  readonly #budget: RequestBudget;
  readonly #configuration: PoolConfiguration;
  readonly #seen: WeakSet<WorkerLike>;
  #identity: Readonly<SnapshotPoolIdentity>;
  readonly #waiting: (QueryJob | RefreshJob)[] = [];
  #refreshPending = false;
  #refreshRunning = false;
  #pendingSnapshotBytes = 0;
  #active = 0;
  #nextId = 1;
  #completed = 0;
  #failed = 0;
  #rejected = 0;
  #state: SnapshotPoolStats["state"] = "open";
  #terminal: FrankenPoolError | null = null;
  #closePromise: Promise<void> | null = null;
  #drained: (() => void) | null = null;

  private constructor(
    replicas: Replica[],
    budget: RequestBudget,
    identity: SnapshotPoolIdentity,
    configuration: PoolConfiguration,
    seen: WeakSet<WorkerLike>,
  ) {
    this.#replicas = replicas;
    this.#budget = budget;
    this.#identity = Object.freeze(identity);
    this.#configuration = configuration;
    this.#seen = seen;
    for (const replica of replicas) {
      replica.watch.notify = (error) => this.#crashed(error);
      if (replica.watch.failure !== null) this.#crashed(replica.watch.failure);
    }
  }

  static async open(
    snapshot: Uint8Array,
    options: SnapshotPoolOptions = {},
  ): Promise<FrankenSnapshotPool> {
    const workers = options.workers ?? 2;
    const maxPendingQueries = options.maxPendingQueries ?? 64;
    const maxPendingBytes = options.maxPendingBytes ?? 128 * 1024 * 1024;
    const wasmUrl = options.wasmUrl;
    const resultEncoding = resolveResultEncoding(options.resultEncoding);
    const factory = options.worker ?? (() => createFrankenSqliteWorker());
    if (
      !Number.isInteger(workers) ||
      workers < 1 ||
      workers > 8 ||
      typeof factory !== "function" ||
      (wasmUrl !== undefined && typeof wasmUrl !== "string")
    ) {
      throw new FrankenPoolError(
        "ERR_FSQLITE_POOL_INPUT",
        "Use 1..8 workers and a dedicated worker factory",
      );
    }
    const budget = new RequestBudget({ maxPendingRequests: maxPendingQueries, maxPendingBytes });
    // Capture before the first await; neither caller mutation nor transferred
    // worker copies can change the identity or content of another replica.
    const image = captureImage(snapshot, workers);
    const configuration = { workers, maxPendingBytes, wasmUrl, resultEncoding, factory };
    const seen = new WeakSet<WorkerLike>();
    const opened = await openReplicas(image, configuration, seen, 1);
    const pool = new FrankenSnapshotPool(
      opened.replicas,
      budget,
      opened.identity,
      configuration,
      seen,
    );
    if (pool.#terminal !== null) {
      const cause = pool.#terminal;
      try {
        await pool.close();
      } catch (cleanup: unknown) {
        throw new AggregateError([cause, cleanup], "Snapshot workers failed during opening", {
          cause,
        });
      }
      throw cause;
    }
    return pool;
  }

  get snapshot(): Readonly<SnapshotPoolIdentity> {
    return this.#identity;
  }

  get stats(): SnapshotPoolStats {
    const budget = this.#budget.stats;
    return Object.freeze({
      state: this.#state,
      workers: this.#replicas.length,
      activeQueries: this.#active,
      waitingQueries: this.#waiting.filter((job) => job.kind === "query").length,
      pendingQueries: budget.pendingRequests,
      pendingBytes: budget.pendingBytes,
      completedQueries: this.#completed,
      failedQueries: this.#failed,
      rejectedQueries: this.#rejected,
      refreshing: this.#refreshPending,
      pendingSnapshotBytes: this.#pendingSnapshotBytes,
    });
  }

  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: SqlBindings = [],
    options: SnapshotQueryOptions = {},
  ): Promise<SnapshotQueryResult<Row>> {
    let release: (() => void) | undefined;
    try {
      this.#assertOpen();
      const signal = options.signal,
        wait = options.waitTimeoutMs;
      if (signal !== undefined && !(signal instanceof AbortSignal))
        throw new TypeError("signal must be an AbortSignal");
      if (wait !== undefined && (!Number.isFinite(wait) || wait < 0 || wait > 2147483647)) {
        throw new RangeError("waitTimeoutMs must be in 0..2147483647");
      }
      if (signal?.aborted) throw cancelled(signal);
      const deadline = wait === undefined ? undefined : performance.now() + wait;
      const admission = this.#budget.admit({
        kind: "query",
        requestId: this.#nextId++,
        sql,
        params,
      });
      release = admission.release;
      const captured = admission.request as QueryRequest;
      readOnlySql(captured.sql);
      // RequestBudget already bounded/captured own scalar fields. Refuse shared
      // buffers (structuredClone would retain shared storage), then snapshot all
      // values once, preserving binary aliases within this admitted request.
      const values = Array.isArray(captured.params)
        ? captured.params
        : Object.values(captured.params ?? {});
      for (const value of values)
        if (value instanceof Uint8Array && !(value.buffer instanceof ArrayBuffer)) {
          throw new FrankenPoolError(
            "ERR_FSQLITE_POOL_INPUT",
            "Pool parameters cannot use shared backing buffers",
          );
        }
      const request = structuredClone(captured);
      this.#assertOpen(); // Binding/option getters can re-enter close or admission.
      if (signal?.aborted) throw cancelled(signal);
      const result = new Promise<SnapshotQueryResult>((resolve, reject) => {
        const job: QueryJob = {
          kind: "query",
          request,
          signal,
          deadline,
          timer: undefined,
          active: false,
          aborted: null,
          release: admission.release,
          resolve,
          reject,
          onAbort: () => {},
        };
        job.onAbort = () => {
          job.aborted ??= cancelled(signal!);
          if (!job.active) {
            this.#remove(job, job.aborted);
            this.#pump();
          }
        };
        this.#waiting.push(job);
        signal?.addEventListener("abort", job.onAbort, { once: true });
        if (wait !== undefined)
          job.timer = setTimeout(() => {
            if (!job.active) {
              this.#remove(job, timedOut());
              this.#pump();
            }
          }, wait);
        this.#pump();
      });
      return result as Promise<SnapshotQueryResult<Row>>;
    } catch (cause: unknown) {
      release?.();
      this.#rejected++;
      return Promise.reject(cause);
    }
  }

  /** A pool-wide FIFO barrier, not independent per-replica reinitialization. */
  refresh(snapshot: Uint8Array): Promise<SnapshotRefreshResult> {
    try {
      const check = (): void => {
        this.#assertOpen();
        if (this.#refreshPending)
          throw new FrankenPoolError(
            "ERR_FSQLITE_POOL_REFRESH_BUSY",
            "Only one snapshot refresh may be pending",
          );
      };
      check();
      const image = captureImage(snapshot, this.#configuration.workers);
      check(); // Snapshot getters can re-enter close or another refresh.
      this.#refreshPending = true;
      this.#pendingSnapshotBytes = image.byteLength;
      return new Promise((resolve, reject) => {
        this.#waiting.push({ kind: "refresh", image, resolve, reject });
        this.#pump();
      });
    } catch (cause: unknown) {
      return Promise.reject(cause);
    }
  }

  close(): Promise<void> {
    if (this.#closePromise !== null) return this.#closePromise;
    this.#state = "closing";
    const drained =
      this.#active === 0 && this.#waiting.length === 0 && !this.#refreshRunning
        ? Promise.resolve()
        : new Promise<void>((resolve) => {
            this.#drained = resolve;
          });
    this.#closePromise = drained.then(async () => {
      const errors = await closeReplicas(this.#replicas);
      this.#state = "closed";
      if (errors.length)
        throw new AggregateError(errors, "Snapshot pool close failed", { cause: errors[0] });
    });
    return this.#closePromise;
  }

  #assertOpen(): void {
    if (this.#terminal !== null) throw this.#terminal;
    if (this.#state !== "open")
      throw new FrankenPoolError("ERR_FSQLITE_POOL_CLOSED", "Snapshot pool is closing or closed");
  }

  #crashed(error: FrankenPoolError): void {
    this.#terminal ??= error;
    for (const job of [...this.#waiting]) this.#remove(job, this.#terminal);
    // No transparent replay/replacement. Other active reads drain on their own
    // replicas, and every listener/worker is eventually released by close.
    void this.close().catch(() => {});
  }

  #remove(job: QueryJob | RefreshJob, cause: unknown): void {
    const index = this.#waiting.indexOf(job);
    if (index < 0) return;
    this.#waiting.splice(index, 1);
    if (job.kind === "query") {
      this.#finish(job);
      this.#failed++;
    } else {
      this.#refreshPending = false;
      this.#pendingSnapshotBytes = 0;
    }
    job.reject(cause);
  }

  #finish(job: QueryJob): void {
    if (job.timer !== undefined) clearTimeout(job.timer);
    job.signal?.removeEventListener("abort", job.onAbort);
    job.release();
  }

  #pump(): void {
    if (this.#refreshRunning) return;
    for (const replica of this.#replicas) {
      if (replica.busy || this.#terminal !== null) continue;
      while (this.#waiting.length > 0) {
        const job = this.#waiting[0]!;
        if (job.kind === "refresh") {
          if (this.#active === 0) {
            this.#waiting.shift();
            this.#refreshRunning = true;
            void this.#refresh(job);
          }
          return;
        }
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
    const snapshot = this.#identity;
    try {
      const result = await replica.db.query(job.request.sql, job.request.params);
      if (job.aborted !== null) throw job.aborted;
      this.#completed++;
      job.resolve({ ...result, snapshot });
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

  async #refresh(job: RefreshJob): Promise<void> {
    let staged: Replica[] | null = null;
    try {
      const opened = await openReplicas(
        job.image,
        this.#configuration,
        this.#seen,
        this.#identity.generation + 1,
      );
      staged = opened.replicas;
      if (this.#terminal !== null) throw this.#terminal;
      const fault = staged.find((replica) => replica.watch.failure !== null)?.watch.failure;
      if (fault) throw fault;
      const previous = this.#replicas;
      // No await between publishing the complete generation and installing its
      // crash observers. Retire old observers before their close can emit errors.
      for (const replica of previous) replica.watch.notify = null;
      this.#replicas = staged;
      this.#identity = Object.freeze(opened.identity);
      staged = null;
      for (const replica of this.#replicas) replica.watch.notify = (error) => this.#crashed(error);
      const cleanupErrors = await closeReplicas(previous);
      // Publication already happened. An old-worker close failure must not look
      // like a failed refresh or justify silently falling back to the old image.
      job.resolve(
        Object.freeze({ snapshot: this.#identity, cleanupErrors: Object.freeze(cleanupErrors) }),
      );
    } catch (cause: unknown) {
      const cleanup = staged === null ? [] : await closeReplicas(staged);
      const failure =
        cleanup.length === 0
          ? cause
          : new AggregateError([cause, ...cleanup], "Refresh and staged cleanup failed", { cause });
      const dependent = new FrankenPoolError(
        "ERR_FSQLITE_POOL_REFRESH_FAILED",
        "The preceding refresh failed; this query did not run on the old snapshot",
        { cause: failure },
      );
      // Everything remaining is behind this barrier. Never serve those reads
      // from stale replicas after failing to publish their requested generation.
      for (const queued of [...this.#waiting]) this.#remove(queued, dependent);
      job.reject(failure);
    } finally {
      this.#refreshPending = false;
      this.#refreshRunning = false;
      this.#pendingSnapshotBytes = 0;
      this.#pump();
    }
  }
}

function captureImage(snapshot: Uint8Array, workers: number): Uint8Array<ArrayBuffer> {
  validateSnapshotBytes(snapshot);
  if (
    !(snapshot.buffer instanceof ArrayBuffer) ||
    snapshot.byteLength * workers > 128 * 1024 * 1024
  ) {
    throw new FrankenPoolError(
      "ERR_FSQLITE_POOL_INPUT",
      "Use an unshared image with at most 128 MiB across replicas",
    );
  }
  const image = new Uint8Array(snapshot);
  validateSnapshotBytes(image);
  if (image.byteLength * workers > 128 * 1024 * 1024) {
    throw new FrankenPoolError("ERR_FSQLITE_POOL_INPUT", "Copied image exceeds the replica budget");
  }
  return image;
}

async function closeReplicas(replicas: Replica[]): Promise<unknown[]> {
  const results = await Promise.allSettled(
    replicas.map(async (replica) => {
      replica.watch.notify = null;
      try {
        await replica.db.close();
      } finally {
        replica.watch.stopFailure();
        replica.worker.removeEventListener("error", replica.watch.onError);
      }
    }),
  );
  return results.flatMap((result) =>
    result.status === "rejected" ? [result.reason as unknown] : [],
  );
}

async function openReplicas(
  image: Uint8Array<ArrayBuffer>,
  config: PoolConfiguration,
  seen: WeakSet<WorkerLike>,
  generation: number,
): Promise<{ replicas: Replica[]; identity: SnapshotPoolIdentity }> {
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", image));
  const identity = {
    sha256: Array.from(hash, (n) => n.toString(16).padStart(2, "0")).join(""),
    byteLength: image.byteLength,
    generation,
  };
  const opened = await Promise.allSettled(
    Array.from({ length: config.workers }, async (): Promise<Replica> => {
      const worker = config.factory();
      if (seen.has(worker))
        throw new FrankenPoolError(
          "ERR_FSQLITE_POOL_INPUT",
          "Each generation requires new, dedicated workers",
        );
      seen.add(worker);
      const watch: Replica["watch"] = {
        failure: null,
        notify: null,
        onError: () => {},
        stopFailure: () => {},
      };
      watch.onError = (event) => {
        watch.failure ??= new FrankenPoolError(
          "ERR_FSQLITE_POOL_UNUSABLE",
          `A snapshot worker crashed: ${event.message}`,
        );
        watch.notify?.(watch.failure);
      };
      worker.addEventListener("error", watch.onError);
      let db: FrankenDB | null = null;
      try {
        db = await FrankenDB.open({
          worker,
          snapshot: image.slice(),
          persistence: "memory",
          resultEncoding: config.resultEncoding,
          ...(config.wasmUrl === undefined ? {} : { wasmUrl: config.wasmUrl }),
          requestLimits: {
            maxPendingRequests: 1,
            maxPendingBytes: Math.max(config.maxPendingBytes, image.byteLength + 4096),
          },
        });
        watch.stopFailure = observeDatabaseFailure(db, (cause) => {
          watch.failure ??= new FrankenPoolError(
            "ERR_FSQLITE_POOL_UNUSABLE",
            `A snapshot connection failed: ${cause.message}`,
            { cause },
          );
          watch.notify?.(watch.failure);
        });
        await db.execute("PRAGMA query_only = ON");
        const mode = await db.query("PRAGMA query_only");
        if (
          mode.rowArrays?.length !== 1 ||
          mode.rowArrays[0]?.length !== 1 ||
          (mode.rowArrays[0][0] !== 1 && mode.rowArrays[0][0] !== 1n)
        ) {
          throw new FrankenPoolError(
            "ERR_FSQLITE_POOL_READ_ONLY",
            "Core did not acknowledge query_only; refusing a mutable replica",
          );
        }
        if (watch.failure !== null) throw watch.failure;
        return { db, worker, busy: false, watch };
      } catch (cause: unknown) {
        try {
          if (db !== null) await db.close();
        } catch (cleanup: unknown) {
          throw new AggregateError([cause, cleanup], "Replica initialization and cleanup failed", {
            cause,
          });
        } finally {
          watch.stopFailure();
          worker.removeEventListener("error", watch.onError);
        }
        throw cause;
      }
    }),
  );
  const replicas = opened.flatMap((result) =>
    result.status === "fulfilled" ? [result.value] : [],
  );
  const failures = opened.flatMap((result) =>
    result.status === "rejected" ? [result.reason as unknown] : [],
  );
  for (const replica of replicas)
    if (replica.watch.failure !== null) failures.push(replica.watch.failure);
  if (failures.length !== 0) {
    failures.push(...(await closeReplicas(replicas)));
    throw new AggregateError(
      failures,
      "Snapshot pool initialization failed; all opened replicas were closed",
      { cause: failures[0] },
    );
  }
  return { replicas, identity };
}

function cancelled(signal: AbortSignal): FrankenPoolError {
  return new FrankenPoolError(
    "ERR_FSQLITE_POOL_CANCELLED",
    "Snapshot read cancelled; active SQL must drain",
    { cause: signal.reason },
  );
}
function timedOut(): FrankenPoolError {
  return new FrankenPoolError(
    "ERR_FSQLITE_POOL_TIMEOUT",
    "Snapshot read did not start before its deadline; no SQL ran",
  );
}

function readOnlySql(sql: string): void {
  validateManagedSql(sql);
  let offset = 0;
  const word = (): string => {
    while (offset < sql.length) {
      if (/[\s;\uFEFF]/.test(sql[offset]!)) {
        offset++;
        continue;
      }
      if (sql.startsWith("--", offset)) {
        const end = sql.indexOf("\n", offset + 2);
        offset = end < 0 ? sql.length : end + 1;
        continue;
      }
      if (sql.startsWith("/*", offset)) {
        const end = sql.indexOf("*/", offset + 2);
        offset = end < 0 ? sql.length : end + 2;
        continue;
      }
      break;
    }
    const token = /^[A-Za-z]+/.exec(sql.slice(offset))?.[0] ?? "";
    offset += token.length;
    return token.toUpperCase();
  };
  let first = word();
  if (first === "EXPLAIN") {
    first = word();
    if (first === "QUERY") {
      first = word() === "PLAN" ? word() : "";
    }
  }
  // PRAGMA may take effect during preparation even under EXPLAIN. Never admit
  // it, ATTACH, maintenance, scripts or transaction controls. WITH may contain
  // DML, so the verified engine query_only guard remains authoritative as well.
  if (first !== "SELECT" && first !== "WITH") {
    throw new FrankenPoolError(
      "ERR_FSQLITE_POOL_READ_ONLY",
      "Snapshot pools accept SELECT, WITH, and their EXPLAIN forms only",
    );
  }
}
