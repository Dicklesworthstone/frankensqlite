import { FrankenDB, observeDatabaseFailure } from "./database";
import { FrankenSQLiteError } from "./errors";
import { TableChangeJournal, captureTables } from "./change-journal";
import { ChangeObserver, createChangeStream } from "./subscriptions";
import type { TableChangeListener, TableChangeStream, TableSubscription } from "./subscriptions";
import type { FrankenTransaction } from "./transaction";
import type { FrankenDbOpenOptions, SnapshotMetadata } from "./types";

type CloseListener = (failure: Error | null) => void;
const queueClosers = new WeakMap<FrankenDBQueue, (listener: CloseListener) => () => void>();

/** Internal close-intent observation for long-lived, privately owned reads. */
export function observeQueueClose(queue: FrankenDBQueue, listener: CloseListener): () => void {
  const observe = queueClosers.get(queue);
  if (observe === undefined) throw new TypeError("A FrankenDBQueue is required");
  return observe(listener);
}

export interface JobQueueOptions {
  /** Active plus waiting jobs, 1..4096. Defaults to 64. No overflow waiters. */
  maxPendingJobs?: number;
  /** Active or still-finishing subscriptions, 1..1024. Defaults to 64. */
  maxSubscriptions?: number;
}

export interface QueuedJobOptions {
  /** Cancel waiting work. Active export/checkpoint publication is not interruptible. */
  signal?: AbortSignal;
  /** Maximum time waiting to start, in milliseconds. Never times out live SQL. */
  waitTimeoutMs?: number;
}

export interface QueuedTransactionOptions extends QueuedJobOptions {
  /** Active transaction cancellation also drains callback, SQL and rollback. */
  signal?: AbortSignal;
}

export interface JobQueueStats {
  readonly state: "open" | "closing" | "closed";
  readonly maxPendingJobs: number;
  readonly pendingJobs: number;
  readonly waitingJobs: number;
  readonly activeJobs: number;
  readonly acceptedJobs: number;
  readonly completedJobs: number;
  /** Started jobs that failed, including cooperative active cancellation. */
  readonly failedJobs: number;
  /** Accepted jobs cancelled while waiting; their callbacks never ran. */
  readonly cancelledJobs: number;
  readonly timedOutJobs: number;
  /** Admission refusals, including invalid arguments and calls after close. */
  readonly rejectedJobs: number;
  readonly subscriptions: number;
  readonly reservedSubscriptions: number;
  readonly maxSubscriptions: number;
  readonly pendingNotifications: number;
  readonly activeListeners: number;
}

interface Job {
  state: "waiting" | "active" | "settled";
  readonly start: () => void;
  readonly reject: (error: unknown) => void;
  readonly signal: AbortSignal | undefined;
  readonly deadline: number | undefined;
  stopWaiting: () => void;
}

function cancelled(signal: AbortSignal): FrankenSQLiteError {
  const error = new FrankenSQLiteError({ code: "ERR_FSQLITE_JOB_CANCELLED",
    message: "Queued job cancelled before starting; no SQL was executed", transient: false });
  // The local reason need not be serializable across the worker boundary.
  error.cause = signal.reason;
  return error;
}

/**
 * A bounded FIFO of whole transactions on one privately owned connection.
 * Not a multi-worker pool: memory/snapshot connections do not share live data.
 */
export class FrankenDBQueue {
  readonly #db: FrankenDB;
  readonly #maxPendingJobs: number;
  readonly #maxSubscriptions: number;
  readonly #subscriptions = new Set<ChangeObserver>();
  readonly #observers = new Set<ChangeObserver>();
  readonly #closeListeners = new Set<CloseListener>();
  #journal: TableChangeJournal | null = null;
  #changeSequence = 0n;
  #terminalFailure: Error | null = null;
  #stopObservingFailure: (() => void) | null = null;
  readonly #waiting = new Set<Job>();
  #active: Job | null = null;
  #state: JobQueueStats["state"] = "open";
  #pumpScheduled = false;
  #closeStarted = false;
  #closePromise: Promise<void> | null = null;
  #resolveClose: (() => void) | null = null;
  #rejectClose: ((error: unknown) => void) | null = null;
  #accepted = 0;
  #completed = 0;
  #failed = 0;
  #cancelled = 0;
  #timedOut = 0;
  #rejected = 0;

  private constructor(db: FrankenDB, maxPendingJobs: number, maxSubscriptions: number) {
    this.#db = db;
    this.#maxPendingJobs = maxPendingJobs;
    this.#maxSubscriptions = maxSubscriptions;
    queueClosers.set(this, listener => {
      if (this.#state !== "open") { listener(this.#terminalFailure); return () => {}; }
      this.#closeListeners.add(listener);
      return () => { this.#closeListeners.delete(listener); };
    });
    this.#stopObservingFailure = observeDatabaseFailure(db, error => {
      this.#terminalFailure ??= error;
      for (const observer of this.#subscriptions) observer.fail(error);
      void this.close().catch(() => {});
    });
  }

  static async open(
    databaseOptions?: FrankenDbOpenOptions | string,
    queueOptions?: JobQueueOptions,
  ): Promise<FrankenDBQueue> {
    // Capture and validate before constructing a worker or importing bytes.
    const maxPendingJobs = queueOptions?.maxPendingJobs ?? 64;
    const maxSubscriptions = queueOptions?.maxSubscriptions ?? 64;
    if (!Number.isInteger(maxPendingJobs) || maxPendingJobs < 1 || maxPendingJobs > 4096) {
      throw new RangeError("maxPendingJobs must be an integer in 1..4096");
    }
    if (!Number.isInteger(maxSubscriptions) || maxSubscriptions < 1 || maxSubscriptions > 1024) {
      throw new RangeError("maxSubscriptions must be an integer in 1..1024");
    }
    const queue = new FrankenDBQueue(await FrankenDB.open(databaseOptions), maxPendingJobs, maxSubscriptions);
    if (queue.#terminalFailure !== null) {
      const cause = queue.#terminalFailure;
      try { await queue.close(); } catch (cleanup: unknown) {
        if (cleanup !== cause) throw new AggregateError([cause, cleanup], "Queue opening and cleanup failed", { cause });
      }
      throw cause;
    }
    return queue;
  }

  get path(): string { return this.#db.path; }
  get persistence() { return this.#db.persistence; }
  get snapshotRevision(): string | null { return this.#db.snapshotRevision; }
  /** Local watched-write sequence, not a native commit sequence or saved revision. */
  get changeSequence(): bigint { return this.#changeSequence; }

  /** Frozen scheduler accounting, not a database-health or memory measurement. */
  get stats(): JobQueueStats {
    const activeJobs = this.#active === null ? 0 : 1;
    return Object.freeze({
      state: this.#state, maxPendingJobs: this.#maxPendingJobs,
      pendingJobs: activeJobs + this.#waiting.size,
      waitingJobs: this.#waiting.size, activeJobs,
      acceptedJobs: this.#accepted, completedJobs: this.#completed,
      failedJobs: this.#failed, cancelledJobs: this.#cancelled,
      timedOutJobs: this.#timedOut, rejectedJobs: this.#rejected,
      subscriptions: this.#subscriptions.size, reservedSubscriptions: this.#observers.size,
      maxSubscriptions: this.#maxSubscriptions,
      pendingNotifications: [...this.#observers].filter(observer => observer.pending).length,
      activeListeners: [...this.#observers].filter(observer => observer.running).length,
    });
  }

  /**
   * Submit a complete transaction. Only the callback's tx may use the connection.
   * Await nested work through tx.transaction(), not through this queue: a queued
   * sibling cannot start while this callback owns the connection.
   */
  transaction<T>(
    work: (tx: FrankenTransaction) => T | Promise<T>,
    options?: QueuedTransactionOptions,
  ): Promise<T> {
    return this.#enqueue(async signal => {
      const transactionOptions = signal === undefined ? undefined : { signal };
      if (this.#journal === null) return this.#db.transaction(work, transactionOptions);
      const result = await this.#journal.run(this.#db, work, transactionOptions);
      if (result.tables.length !== 0) {
        const sequence = ++this.#changeSequence;
        for (const observer of this.#subscriptions) {
          try { observer.publish(result.tables, sequence); }
          catch (cause: unknown) { observer.fail(cause); }
        }
      }
      return result.value;
    }, options, work);
  }

  /** Register at a FIFO boundary; only later successful local commits notify. */
  subscribe(tables: readonly string[], listener: TableChangeListener,
    options?: QueuedJobOptions): Promise<TableSubscription> {
    let requested: readonly string[];
    try {
      this.#assertAdmission();
      if (typeof listener !== "function") throw new TypeError("A change listener is required");
      requested = captureTables(tables);
    } catch (cause: unknown) {
      this.#rejected++;
      return Promise.reject(cause);
    }
    return this.#enqueue(async signal => {
      if (this.#observers.size >= this.#maxSubscriptions) {
        throw new FrankenSQLiteError({ code: "ERR_FSQLITE_SUBSCRIPTION_LIMIT", transient: true,
          message: "Subscription capacity is reserved; unsubscribe and await done before retrying" });
      }
      const journal = this.#journal ??= new TableChangeJournal();
      const all = new Map(this.#watchedTables().map(name => [foldTable(name), name]));
      for (const name of requested) all.set(foldTable(name), name);
      const canonical = await journal.configure(this.#db, [...all.values()],
        signal === undefined ? undefined : { signal });
      if (this.#terminalFailure !== null) throw this.#terminalFailure;
      const keys = new Set(requested.map(foldTable));
      const observer = new ChangeObserver(canonical.filter(name => keys.has(foldTable(name))), listener,
        () => { this.#subscriptions.delete(observer); },
        () => { this.#observers.delete(observer); });
      this.#subscriptions.add(observer);
      this.#observers.add(observer);
      observer.activate(signal);
      if (this.#state !== "open") observer.stop();
      return observer.handle;
    }, options, listener);
  }

  /** One buffered invalidation range and one outstanding next(), never row results. */
  changes(tables: readonly string[], options?: QueuedJobOptions): Promise<TableChangeStream> {
    return createChangeStream(listener => this.subscribe(tables, listener, options));
  }

  /**
   * An ordered image barrier outside all managed transactions. Once started,
   * export's actual outcome wins over a late abort; no work is abandoned.
   */
  export(options?: QueuedJobOptions): Promise<Uint8Array> {
    const operation = () => this.#db.export();
    return this.#enqueue(operation, options, operation);
  }

  /**
   * Publish the current committed image in indexeddb-snapshot mode. Includes
   * earlier successful jobs, excludes later jobs, and waits for publication.
   * Preceding job failures do not implicitly cancel this independent barrier.
   */
  checkpoint(options?: QueuedJobOptions): Promise<SnapshotMetadata> {
    const operation = () => this.#db.checkpoint();
    return this.#enqueue(operation, options, operation);
  }

  /** Stop admission, drain accepted jobs, then close. Idempotent shared promise. */
  close(): Promise<void> {
    if (this.#closePromise !== null) return this.#closePromise;
    this.#state = "closing";
    this.#closePromise = new Promise<void>((resolve, reject) => {
      this.#resolveClose = resolve;
      this.#rejectClose = reject;
    });
    // A retained scan may be waiting for its consumer, not for SQL. Wake it
    // before draining jobs; ordinary transaction callbacks are not cancelled.
    const listeners = [...this.#closeListeners];
    this.#closeListeners.clear();
    for (const listener of listeners) {
      try { listener(this.#terminalFailure); } catch { /* Internal cleanup cannot suppress close. */ }
    }
    for (const observer of this.#subscriptions) observer.stop();
    this.#schedule();
    return this.#closePromise;
  }

  #assertAdmission(): void {
    if (this.#terminalFailure !== null) throw this.#terminalFailure;
    if (this.#state !== "open") {
      throw new FrankenSQLiteError({ code: "ERR_FSQLITE_JOB_QUEUE_CLOSED",
        message: "This job queue no longer accepts work" });
    }
    if (this.#waiting.size + (this.#active === null ? 0 : 1) >= this.#maxPendingJobs) {
      throw new FrankenSQLiteError({ code: "ERR_FSQLITE_JOB_QUEUE_FULL", transient: true,
        message: "Job queue is full; no SQL was executed",
        suggestion: "Await an accepted job before retrying the complete operation" });
    }
  }

  #enqueue<T>(
    operation: (signal: AbortSignal | undefined) => Promise<T>,
    options: QueuedJobOptions | undefined,
    callback: unknown,
  ): Promise<T> {
    let signal: AbortSignal | undefined;
    let waitTimeoutMs: number | undefined;
    try {
      this.#assertAdmission();
      if (typeof callback !== "function") throw new TypeError("A transaction callback is required");
      const requestedSignal = options?.signal;
      waitTimeoutMs = options?.waitTimeoutMs;
      if (waitTimeoutMs !== undefined && (!Number.isInteger(waitTimeoutMs)
        || waitTimeoutMs < 1 || waitTimeoutMs > 2_147_483_647)) {
        throw new RangeError("waitTimeoutMs must be an integer in 1..2147483647");
      }
      if (requestedSignal !== undefined) {
        Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(requestedSignal);
        // A dependent native signal is private. A caller's abort listener cannot
        // stopImmediatePropagation() and hide cancellation from this queue.
        signal = AbortSignal.any([requestedSignal]);
      }
      // Caller option getters can close/fill this queue; they grant no reservation.
      this.#assertAdmission();
      if (signal?.aborted) throw cancelled(signal);
    } catch (error: unknown) {
      this.#rejected++;
      return Promise.reject(error);
    }

    const deadline = waitTimeoutMs === undefined ? undefined : performance.now() + waitTimeoutMs;
    return new Promise<T>((resolve, reject) => {
      const job: Job = {
        state: "waiting", signal, deadline, reject, stopWaiting: () => {},
        start: () => {
          let result: Promise<T>;
          try { result = this.#reconcileSubscriptions().then(() => operation(signal)); }
          catch (error: unknown) { result = Promise.reject(error); }
          // Release capacity only after the real operation (including cleanup)
          // settles. Never race running SQL or a callback against a timer/abort.
          void result.then(value => {
            this.#finish(job, true);
            resolve(value);
          }, (error: unknown) => {
            this.#finish(job, false);
            reject(error);
          });
        },
      };
      const onAbort = () => this.#discard(job, "cancelled", cancelled(signal!));
      let timer: ReturnType<typeof setTimeout> | undefined;
      job.stopWaiting = () => {
        if (timer !== undefined) clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
      };
      this.#accepted++;
      this.#waiting.add(job);
      signal?.addEventListener("abort", onAbort, { once: true });
      if (waitTimeoutMs !== undefined) {
        timer = setTimeout(() => this.#expire(job), waitTimeoutMs);
      }
      this.#schedule();
    });
  }

  #expire(job: Job): void {
    this.#discard(job, "timeout", new FrankenSQLiteError({
      code: "ERR_FSQLITE_JOB_WAIT_TIMEOUT", transient: true,
      message: "Queued job exceeded its start deadline; no SQL was executed",
      suggestion: "Retry the complete job after queue pressure subsides",
    }));
  }

  #discard(job: Job, reason: "cancelled" | "timeout", error: unknown): void {
    if (job.state !== "waiting") return;
    job.state = "settled";
    job.stopWaiting();
    this.#waiting.delete(job);
    if (reason === "cancelled") this.#cancelled++;
    else this.#timedOut++;
    job.reject(error);
    this.#schedule();
  }

  #finish(job: Job, success: boolean): void {
    job.state = "settled";
    this.#active = null;
    if (success) this.#completed++;
    else this.#failed++;
    this.#schedule();
  }

  #watchedTables(): readonly string[] {
    return [...new Set([...this.#subscriptions].flatMap(observer => observer.handle.tables))];
  }

  async #reconcileSubscriptions(): Promise<void> {
    if (this.#journal === null) return;
    // Stop is immediate even under saturation. Remove abandoned TEMP triggers
    // at the next job boundary, with no hidden/unbounded cleanup job queue.
    let tables = this.#watchedTables();
    while (!this.#journal.matches(tables)) {
      await this.#journal.configure(this.#db, tables);
      tables = this.#watchedTables(); // More listeners can stop during the await.
    }
  }

  #schedule(): void {
    if (this.#pumpScheduled || this.#state === "closed") return;
    this.#pumpScheduled = true;
    queueMicrotask(() => {
      this.#pumpScheduled = false;
      this.#pump();
    });
  }

  #pump(): void {
    if (this.#active !== null) return;
    for (const job of this.#waiting) {
      // Timers can be delayed behind promise continuations or a blocked event
      // loop. Recheck the deadline here before permitting any BEGIN or callback.
      if (job.signal?.aborted) {
        this.#discard(job, "cancelled", cancelled(job.signal));
      } else if (job.deadline !== undefined && performance.now() >= job.deadline) {
        this.#expire(job);
      } else {
        this.#waiting.delete(job);
        job.stopWaiting();
        job.state = "active";
        this.#active = job;
        job.start();
        return;
      }
    }
    if (this.#state === "closing" && !this.#closeStarted) {
      this.#closeStarted = true;
      void this.#db.close().then(() => {
        this.#stopObservingFailure?.();
        this.#stopObservingFailure = null;
        this.#state = "closed";
        this.#resolveClose!();
        this.#resolveClose = null;
        this.#rejectClose = null;
      }, (error: unknown) => {
        this.#stopObservingFailure?.();
        this.#stopObservingFailure = null;
        this.#state = "closed";
        this.#rejectClose!(error);
        this.#resolveClose = null;
        this.#rejectClose = null;
      });
    }
  }
}

function foldTable(name: string): string {
  return name.replace(/[A-Z]/g, char => char.toLowerCase());
}
