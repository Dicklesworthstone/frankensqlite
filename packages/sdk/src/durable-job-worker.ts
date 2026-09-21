import type { DurableJobLease, DurableJobQueue, DurableJobTransaction } from "./durable-jobs";
import { DurableJobError } from "./durable-jobs";

/** The queue, not the runner, owns transactions, persistence and fencing. */
export type DurableWorkerQueue = Pick<
  DurableJobQueue,
  "claim" | "renew" | "complete" | "completeWith" | "fail" | "reapExpired"
>;

export interface DurableJobContext {
  /** Observe cancellation and await all child work before returning. */
  readonly signal: AbortSignal;
  /** Poll expiry even when CPU work or immediate promises starve timers. */
  checkpoint(): void;
}

/** Compute outside SQL, then publish effects and completion in one transaction. */
export interface DurableJobCompletion {
  readonly result?: string | null;
  /** Use only tx, await every operation, and perform no external side effects. */
  readonly apply: (tx: DurableJobTransaction, context: DurableJobContext) => void | Promise<void>;
}

/** External effects must be idempotent: delivery remains at least once. */
export type DurableJobHandler = (
  lease: DurableJobLease,
  context: DurableJobContext,
) =>
  | string
  | null
  | void
  | DurableJobCompletion
  | Promise<string | null | void | DurableJobCompletion>;

export interface DurableJobWorkerOptions {
  owner: string;
  /** Concurrent handlers, 1..64; defaults to 1. No prefetched waiting leases. */
  concurrency?: number;
  /** 3..86400000 milliseconds; defaults to 30000. */
  leaseMs?: number;
  /** 1..floor(leaseMs / 3); defaults to floor(leaseMs / 3). */
  heartbeatMs?: number;
  /** Delay after an empty claim, 1..2147483647; defaults to 1000. */
  pollIntervalMs?: number;
  /** Persisted delay after handler failure or cancellation; defaults to 1000. */
  retryDelayMs?: number;
  /** Periodic bounded recovery, including crashed final attempts. Default 30000. */
  reapIntervalMs?: number;
  /** Maximum expired claims recovered per sweep, 1..1000; defaults to 100. */
  reapLimit?: number;
  /** Finish when every slot observes no runnable job; defaults to false. */
  stopWhenIdle?: boolean;
  /** Wall clock shared with the queue; defaults to Date.now. Must agree across workers. */
  clock?: () => number;
  /** Aborting stops admission, signals handlers, and joins their cleanup. */
  signal?: AbortSignal;
}

export interface DurableJobWorkerStopOptions {
  /** Defaults to false: drain already-admitted claims and handlers normally. */
  abort?: boolean;
  reason?: unknown;
}

export interface DurableJobWorkerStats {
  readonly state: "running" | "draining" | "aborting" | "stopped" | "failed";
  readonly concurrency: number;
  readonly activeJobs: number;
  readonly pendingClaims: number;
  readonly claimed: number;
  readonly started: number;
  readonly completed: number;
  /** Acknowledged fail() calls for thrown/invalid handler results. */
  readonly failedJobs: number;
  /** Acknowledged fail() calls after cooperative cancellation. */
  readonly cancelledJobs: number;
  readonly lostLeases: number;
  readonly renewals: number;
  readonly reapedLeases: number;
}

export type DurableJobWorkerPhase =
  | "claim"
  | "renew"
  | "complete"
  | "fail"
  | "reap"
  | "lease"
  | "handler"
  | "run";

/** Storage failure or an uncertain outcome. Never authorizes automatic replay. */
export class DurableJobWorkerError extends Error {
  readonly code = "ERR_FSQLITE_JOB_WORKER_STOPPED";
  constructor(
    readonly phase: DurableJobWorkerPhase,
    cause: unknown,
  ) {
    super(`Durable job worker stopped during ${phase}; reconcile storage before restarting`, {
      cause,
    });
    this.name = "DurableJobWorkerError";
  }
}

type Policy = Required<Omit<DurableJobWorkerOptions, "signal">>;
interface ActiveJob {
  readonly cancel: AbortController;
  readonly stopHeartbeat: AbortController;
  lease: DurableJobLease;
  monotonicDeadline: number;
  expiryTimer: ReturnType<typeof setTimeout> | undefined;
  lost: boolean;
  closed: boolean;
}

/**
 * A supervised consumer, not a detached interval or a database owner. stop()
 * joins every handler, heartbeat and storage operation before settling. It
 * cannot forcibly terminate JavaScript that ignores its cancellation signal.
 */
export class DurableJobWorker {
  readonly #queue: DurableWorkerQueue;
  readonly #handler: DurableJobHandler;
  readonly #policy: Policy;
  readonly #waiters = new Set<() => void>();
  readonly #active = new Set<ActiveJob>();
  readonly #signal: AbortSignal | undefined;
  readonly #onAbort: () => void;
  #state: DurableJobWorkerStats["state"] = "running";
  #failure: DurableJobWorkerError | null = null;
  #abortReason: unknown;
  #pendingClaims = 0;
  #claimed = 0;
  #started = 0;
  #completed = 0;
  #failedJobs = 0;
  #cancelledJobs = 0;
  #lostLeases = 0;
  #renewals = 0;
  #reapedLeases = 0;
  readonly done: Promise<void>;

  private constructor(
    queue: DurableWorkerQueue,
    handler: DurableJobHandler,
    policy: Policy,
    signal: AbortSignal | undefined,
  ) {
    this.#queue = queue;
    this.#handler = handler;
    this.#policy = policy;
    this.#signal = signal;
    this.#onAbort = () => this.#requestStop(true, signal?.reason);
    // No SQL can run until start() has returned a fully initialized owner.
    this.done = Promise.resolve().then(() => this.#run());
    // The owner may inspect stats before awaiting done. Keep a rejected task
    // observed without changing the rejection exposed by done or stop().
    void this.done.catch(() => {});
    signal?.addEventListener("abort", this.#onAbort, { once: true });
    if (signal?.aborted) this.#onAbort();
  }

  static start(
    queue: DurableWorkerQueue,
    handler: DurableJobHandler,
    options: DurableJobWorkerOptions,
  ): DurableJobWorker {
    const { signal, ...policy } = capturePolicy(options);
    if (typeof handler !== "function") throw new TypeError("A job handler is required");
    if (
      queue === null ||
      typeof queue !== "object" ||
      ["claim", "renew", "complete", "completeWith", "fail", "reapExpired"].some(
        (key) => typeof Reflect.get(queue, key) !== "function",
      )
    ) {
      throw new TypeError("A durable job queue is required");
    }
    return new DurableJobWorker(queue, handler, policy, signal);
  }

  get stats(): DurableJobWorkerStats {
    return Object.freeze({
      state: this.#state,
      concurrency: this.#policy.concurrency,
      activeJobs: this.#active.size,
      pendingClaims: this.#pendingClaims,
      claimed: this.#claimed,
      started: this.#started,
      completed: this.#completed,
      failedJobs: this.#failedJobs,
      cancelledJobs: this.#cancelledJobs,
      lostLeases: this.#lostLeases,
      renewals: this.#renewals,
      reapedLeases: this.#reapedLeases,
    });
  }

  /** Calling from a handler is allowed; awaiting it there would await yourself. */
  stop(options?: DurableJobWorkerStopOptions): Promise<void> {
    const abort = options?.abort ?? false;
    if (typeof abort !== "boolean") throw new TypeError("abort must be a boolean");
    const reason = options?.reason;
    this.#requestStop(abort, reason);
    return this.done;
  }

  #requestStop(abort: boolean, reason: unknown): void {
    if (this.#state === "stopped" || this.#state === "failed") return;
    if (this.#state === "running") this.#state = "draining";
    for (const wake of this.#waiters) wake();
    if (abort && this.#state !== "aborting") {
      this.#state = "aborting";
      this.#abortReason = reason ?? new Error("Durable job worker cancelled");
      for (const job of this.#active) job.cancel.abort(this.#abortReason);
    }
  }

  #halt(phase: DurableJobWorkerPhase, cause: unknown): void {
    this.#failure ??= new DurableJobWorkerError(phase, cause);
    this.#requestStop(true, this.#failure);
    // Unknown storage state closes mutation admission, including renewals.
    for (const job of this.#active) {
      job.stopHeartbeat.abort();
      this.#disarmLease(job);
    }
  }

  async #run(): Promise<void> {
    try {
      if (this.#state === "running") {
        try {
          this.#reapedLeases += await this.#queue.reapExpired(this.#policy.reapLimit);
        } catch (cause: unknown) {
          this.#halt("reap", cause);
        }
      }
      if (this.#state === "running") {
        let consumers = this.#policy.concurrency;
        const tasks = Array.from({ length: consumers }, () =>
          this.#consume().finally(() => {
            if (--consumers === 0) this.#requestStop(false, undefined);
          }),
        );
        tasks.push(this.#reaper());
        // Catch inside each task so one failure cannot abandon its siblings.
        await Promise.all(tasks.map((task) => task.catch((cause) => this.#halt("run", cause))));
      }
    } finally {
      this.#signal?.removeEventListener("abort", this.#onAbort);
      for (const wake of this.#waiters) wake();
      this.#state = this.#failure === null ? "stopped" : "failed";
    }
    if (this.#failure !== null) throw this.#failure;
  }

  async #reaper(): Promise<void> {
    while (this.#state === "running") {
      await this.#wait(this.#policy.reapIntervalMs);
      if (this.#state !== "running") return;
      try {
        this.#reapedLeases += await this.#queue.reapExpired(this.#policy.reapLimit);
      } catch (cause: unknown) {
        this.#halt("reap", cause);
        return;
      }
    }
  }

  async #consume(): Promise<void> {
    while (this.#state === "running") {
      let lease: DurableJobLease | null;
      this.#pendingClaims++;
      const claimStarted = performance.now();
      try {
        lease = await this.#queue.claim(this.#policy.owner, this.#policy.leaseMs);
      } catch (cause: unknown) {
        this.#halt("claim", cause);
        return;
      } finally {
        this.#pendingClaims--;
      }
      if (lease === null && this.#policy.stopWhenIdle) return;
      if (lease !== null) {
        this.#claimed++;
        if (this.#failure !== null) return;
        // A claim admitted before graceful stop is drained, not discarded.
        await this.#handle(lease, claimStarted);
      }
      // Yield even after success: immediate promises must not starve heartbeat,
      // cancellation or application timers while a queue remains nonempty.
      await this.#wait(lease === null ? this.#policy.pollIntervalMs : 0);
    }
  }

  #wait(ms: number): Promise<void> {
    if (this.#state !== "running") return Promise.resolve();
    return new Promise((resolve) => {
      const finish = (): void => {
        clearTimeout(timer);
        this.#waiters.delete(finish);
        resolve();
      };
      const timer = setTimeout(finish, ms);
      this.#waiters.add(finish);
    });
  }

  #lose(job: ActiveJob, cause: unknown): void {
    if (job.lost) return;
    job.lost = true;
    this.#lostLeases++;
    this.#disarmLease(job);
    job.stopHeartbeat.abort();
    job.cancel.abort(cause);
  }

  #disarmLease(job: ActiveJob): void {
    clearTimeout(job.expiryTimer);
    job.expiryTimer = undefined;
  }

  #remaining(job: ActiveJob): number {
    const now = this.#policy.clock();
    if (!Number.isSafeInteger(now) || now < 0)
      throw new RangeError("clock must return nonnegative safe-integer milliseconds");
    return Math.min(job.lease.expiresAt - now, job.monotonicDeadline - performance.now());
  }

  #checkLease(job: ActiveJob): boolean {
    if (job.closed || job.lost || this.#failure !== null) return false;
    try {
      if (this.#remaining(job) <= 0)
        this.#lose(
          job,
          new DurableJobError(
            "ERR_FSQLITE_JOB_LEASE_LOST",
            "The acknowledged lease deadline expired; a pending renewal is not ownership authority",
          ),
        );
    } catch (cause: unknown) {
      this.#halt("lease", cause);
    }
    return !job.lost && this.#failure === null;
  }

  #armLease(job: ActiveJob): void {
    this.#disarmLease(job);
    if (!this.#checkLease(job)) return;
    try {
      job.expiryTimer = setTimeout(
        () => this.#armLease(job),
        Math.max(1, Math.ceil(Math.min(this.#remaining(job), this.#policy.heartbeatMs))),
      );
    } catch (cause: unknown) {
      this.#halt("lease", cause);
    }
  }

  #adoptLease(job: ActiveJob, lease: DurableJobLease, operationStarted: number): void {
    if (
      !Number.isSafeInteger(lease.expiresAt) ||
      lease.expiresAt < 0 ||
      lease.queue !== job.lease.queue ||
      lease.id !== job.lease.id ||
      lease.token !== job.lease.token ||
      lease.owner !== job.lease.owner ||
      lease.attempt !== job.lease.attempt
    ) {
      this.#halt("lease", new TypeError("Invalid or mismatched lease acknowledgement"));
      return;
    }
    job.lease = lease;
    // Start before admission, not when the receipt arrives: neither a slow
    // publication nor a backward wall-clock jump can mint a fresh full budget.
    job.monotonicDeadline = operationStarted + this.#policy.leaseMs;
    this.#armLease(job);
  }

  async #heartbeat(job: ActiveJob, lease: DurableJobLease): Promise<void> {
    while (!job.stopHeartbeat.signal.aborted) {
      await delay(this.#policy.heartbeatMs, job.stopHeartbeat.signal);
      if (job.stopHeartbeat.signal.aborted || !this.#checkLease(job)) return;
      const renewalStarted = performance.now();
      try {
        const renewed = await this.#queue.renew(lease, this.#policy.leaseMs);
        this.#renewals++;
        // Do not revive a handler already cancelled on its previous deadline,
        // even if this renewal actually committed and its receipt arrived late.
        if (!this.#checkLease(job)) return;
        this.#adoptLease(job, renewed, renewalStarted);
      } catch (cause: unknown) {
        if (leaseLost(cause)) this.#lose(job, cause);
        else this.#halt("renew", cause);
        return;
      }
    }
  }

  async #handle(lease: DurableJobLease, claimStarted: number): Promise<void> {
    const job: ActiveJob = {
      cancel: new AbortController(),
      stopHeartbeat: new AbortController(),
      lease,
      monotonicDeadline: 0,
      expiryTimer: undefined,
      lost: false,
      closed: false,
    };
    this.#active.add(job);
    if (this.#state === "aborting") job.cancel.abort(this.#abortReason);
    this.#adoptLease(job, lease, claimStarted);
    const context: DurableJobContext = Object.freeze({
      signal: job.cancel.signal,
      checkpoint: () => {
        if (job.closed)
          throw new DurableJobError(
            "ERR_FSQLITE_JOB_CONTEXT_CLOSED",
            "The job handler scope has finished",
          );
        this.#checkLease(job);
        job.cancel.signal.throwIfAborted();
      },
    });
    const heartbeat = this.#heartbeat(job, lease);
    let result: string | null = null;
    let apply: DurableJobCompletion["apply"] | undefined;
    let failed = false;
    let failure: unknown;
    try {
      try {
        if (!job.cancel.signal.aborted) {
          this.#started++;
          let value = await this.#handler(lease, context);
          if (typeof value === "object" && value !== null) {
            const capturedApply = value.apply;
            const capturedResult = value.result;
            if (typeof capturedApply !== "function")
              throw new TypeError("Job completion requires an apply callback");
            apply = capturedApply;
            value = capturedResult;
          }
          if (value !== undefined && value !== null && typeof value !== "string")
            throw new TypeError("Invalid job result");
          if (
            typeof value === "string" &&
            (value.length > 1024 * 1024 || new TextEncoder().encode(value).byteLength > 1024 * 1024)
          ) {
            throw new RangeError("Job result exceeds 1 MiB of UTF-8");
          }
          result = value ?? null;
        }
      } catch (cause: unknown) {
        if (uncertainOutcome(cause)) this.#halt("handler", cause);
        else {
          failed = true;
          failure = cause;
        }
      }
      // A heartbeat already in flight must finish before final mutation. Never
      // race complete/fail with renewal on a transaction-owning connection.
      job.stopHeartbeat.abort();
      await heartbeat;
      if (!this.#checkLease(job)) return;
      this.#disarmLease(job);
      const cancelled = job.cancel.signal.aborted;
      const phase = failed || cancelled ? "fail" : "complete";
      try {
        if (phase === "fail") {
          await this.#queue.fail(
            lease,
            describeFailure(cancelled ? job.cancel.signal.reason : failure),
            this.#policy.retryDelayMs,
          );
          if (cancelled) this.#cancelledJobs++;
          else this.#failedJobs++;
        } else {
          if (apply === undefined) await this.#queue.complete(lease, result);
          else {
            const application = apply;
            await this.#queue.completeWith(
              lease,
              async (tx) => {
                this.#armLease(job);
                try {
                  context.checkpoint();
                  await application(tx, context);
                  context.checkpoint();
                } finally {
                  this.#disarmLease(job);
                }
              },
              result,
            );
          }
          this.#completed++;
        }
      } catch (cause: unknown) {
        if (leaseLost(cause)) this.#lose(job, cause);
        else this.#halt(phase, cause);
      }
    } finally {
      this.#disarmLease(job);
      job.stopHeartbeat.abort();
      await heartbeat;
      job.closed = true;
      this.#active.delete(job);
    }
  }
}

function leaseLost(cause: unknown): boolean {
  return cause instanceof DurableJobError && cause.code === "ERR_FSQLITE_JOB_LEASE_LOST";
}

function capturePolicy(
  options: DurableJobWorkerOptions,
): Policy & { signal: AbortSignal | undefined } {
  const {
    owner,
    concurrency = 1,
    leaseMs = 30_000,
    heartbeatMs = Math.floor(leaseMs / 3),
    pollIntervalMs = 1000,
    retryDelayMs = 1000,
    reapIntervalMs = 30_000,
    reapLimit = 100,
    clock = Date.now,
    stopWhenIdle = false,
    signal,
  } = options;
  if (typeof stopWhenIdle !== "boolean") throw new TypeError("stopWhenIdle must be a boolean");
  if (typeof clock !== "function") throw new TypeError("clock must be a function");
  if (
    typeof owner !== "string" ||
    owner.length === 0 ||
    owner.length > 256 ||
    owner.includes("\0")
  ) {
    throw new TypeError("owner must be a nonempty string of at most 256 characters without NUL");
  }
  for (const [key, value, min, max] of [
    ["concurrency", concurrency, 1, 64],
    ["leaseMs", leaseMs, 3, 86_400_000],
    ["heartbeatMs", heartbeatMs, 1, Math.floor(leaseMs / 3)],
    ["pollIntervalMs", pollIntervalMs, 1, 2_147_483_647],
    ["retryDelayMs", retryDelayMs, 0, 2_147_483_647],
    ["reapIntervalMs", reapIntervalMs, 1, 2_147_483_647],
    ["reapLimit", reapLimit, 1, 1000],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < min || value > max)
      throw new RangeError(`${key} must be an integer in ${min}..${max}`);
  }
  if (signal !== undefined)
    Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal);
  return {
    owner,
    concurrency,
    leaseMs,
    heartbeatMs,
    pollIntervalMs,
    retryDelayMs,
    reapIntervalMs,
    reapLimit,
    clock,
    stopWhenIdle,
    signal,
  };
}

/** One timer and one removable listener; abort is a wake-up, not a rejection. */
function delay(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const finish = (): void => {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    };
    const timer = setTimeout(finish, ms);
    signal.addEventListener("abort", finish, { once: true });
    if (signal.aborted) finish();
  });
}

function describeFailure(cause: unknown): string {
  try {
    return (cause instanceof Error ? cause.message : String(cause)).slice(0, 16_384);
  } catch {
    return "Job handler failed with an unreadable error";
  }
}

/** Do not turn an explicit unknown commit in application code into a retry. */
function uncertainOutcome(cause: unknown): boolean {
  const pending: unknown[] = [cause];
  const seen = new Set<object>();
  let budget = 64;
  try {
    while (pending.length !== 0) {
      if (--budget < 0) return true;
      const value = pending.pop();
      if (typeof value !== "object" || value === null || seen.has(value)) continue;
      seen.add(value);
      const field = (key: string): unknown => {
        const descriptor = Object.getOwnPropertyDescriptor(value, key);
        if (descriptor !== undefined && !Object.hasOwn(descriptor, "value"))
          throw new TypeError("Unreadable error outcome");
        return descriptor?.value;
      };
      if (field("sqlCommitted") === true) return true;
      const code = field("code");
      if (
        code === "ERR_FSQLITE_COMMITTED_CHECKPOINT_FAILED" ||
        code === "ERR_FSQLITE_SNAPSHOT_RECEIPT" ||
        code === "ERR_FSQLITE_CHECKPOINT_RECOVERY_REQUIRED"
      )
        return true;
      pending.push(field("cause"));
      if (value instanceof AggregateError) {
        const errors = field("errors");
        if (!Array.isArray(errors) || errors.length > budget) return true;
        for (let i = 0; i < errors.length; i++) {
          const descriptor = Object.getOwnPropertyDescriptor(errors, String(i));
          if (descriptor === undefined || !Object.hasOwn(descriptor, "value")) return true;
          pending.push(descriptor.value);
        }
      }
    }
    return false;
  } catch {
    return true;
  }
}
