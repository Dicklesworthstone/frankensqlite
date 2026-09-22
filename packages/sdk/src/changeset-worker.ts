import type {
  ChangesetDeliveryPump,
  ChangesetPumpResult,
  ChangesetPumpRunOptions,
} from "./changeset-delivery";

/** The pump retains responsibility for SQL, confirmation, identities and ACKs. */
export type ChangesetDeliveryDriver = Pick<ChangesetDeliveryPump, "run">;

export interface ChangesetDeliveryWorkerOptions {
  /** Delay after an empty drain, 1..2147483647 ms; defaults to 1000. */
  pollIntervalMs?: number;
  /** Bounded selections per pump run, 1..10000; defaults to 100. */
  maxDeliveriesPerRun?: number;
  /** Payload budget per run, 1..1073741824 bytes; defaults to 64 MiB. */
  maxBytesPerRun?: number;
  /** Optional cooperative pump budget, 1..2147483647 ms. */
  runTimeoutMs?: number;
  /** Concurrent flush waiters, 1..65536; defaults to 1024. */
  maxPendingFlushes?: number;
  /** Stop after observing an empty outbox; defaults to false. */
  stopWhenIdle?: boolean;
  /** Stop admission and signal the active run, then await its actual outcome. */
  signal?: AbortSignal;
}

export interface ChangesetDeliveryWorkerStopOptions {
  /** Default false: finish the active bounded run without starting another. */
  abort?: boolean;
  reason?: unknown;
}

export interface ChangesetDeliveryFlushOptions {
  /** Cancels only this waiter, never a shared delivery or confirmation. */
  signal?: AbortSignal;
  /** Monotonic wait budget, 1..2147483647 ms; no default deadline. */
  timeoutMs?: number;
}

/** Failure to observe a fresh empty outbox is not evidence of rollback. */
export class ChangesetDeliveryFlushError extends Error {
  readonly code: `ERR_FSQLITE_DELIVERY_FLUSH_${"INPUT" | "LIMIT" | "CANCELLED" | "TIMEOUT" | "STOPPED"}`;
  constructor(
    kind: "INPUT" | "LIMIT" | "CANCELLED" | "TIMEOUT" | "STOPPED",
    cause?: unknown,
  ) {
    super(`Changeset delivery flush ${kind.toLowerCase()}; delivery may still be in progress`, { cause });
    this.name = "ChangesetDeliveryFlushError";
    this.code = `ERR_FSQLITE_DELIVERY_FLUSH_${kind}`;
  }
}

export interface ChangesetDeliveryWorkerStats {
  readonly state: "running" | "idle" | "draining" | "aborting" | "stopped" | "failed";
  readonly active: boolean;
  readonly attempts: number;
  readonly successfulRuns: number;
  readonly pendingFlushes: number;
  /** Counts from successful, confirmed runs only; a failed run may have committed work. */
  readonly deliveries: number;
  readonly bytes: number;
  readonly applied: number;
  readonly omitted: number;
  readonly replays: number;
  readonly alreadyAcknowledged: number;
}

/** A stopped runner is not evidence that an uncertain delivery was rolled back. */
export class ChangesetDeliveryWorkerError extends Error {
  readonly code = "ERR_FSQLITE_DELIVERY_WORKER_STOPPED";
  constructor(
    readonly phase: "run" | "result",
    cause: unknown,
  ) {
    super(`Changeset delivery worker stopped during ${phase}; reconcile before restarting`, {
      cause,
    });
    this.name = "ChangesetDeliveryWorkerError";
  }
}

interface Policy {
  readonly pollIntervalMs: number;
  readonly maxDeliveriesPerRun: number;
  readonly maxBytesPerRun: number;
  readonly runTimeoutMs: number | undefined;
  readonly maxPendingFlushes: number;
  readonly stopWhenIdle: boolean;
  readonly signal: AbortSignal | undefined;
}

interface FlushWaiter {
  readonly afterAttempt: number;
  readonly signal: AbortSignal | undefined;
  readonly deadline: number | undefined;
  readonly resolve: (stats: ChangesetDeliveryWorkerStats) => void;
  readonly reject: (error: ChangesetDeliveryFlushError) => void;
  readonly onAbort: () => void;
  timer: ReturnType<typeof setTimeout> | undefined;
}

function bound(value: unknown, fallback: number, maximum: number): number {
  const n = value === undefined ? fallback : value;
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 1 || n > maximum) {
    throw new RangeError(`Expected an integer in 1..${maximum}`);
  }
  return n;
}

function capturePolicy(options: ChangesetDeliveryWorkerOptions): Policy {
  const pollIntervalMs = bound(options.pollIntervalMs, 1000, 2_147_483_647);
  const maxDeliveriesPerRun = bound(options.maxDeliveriesPerRun, 100, 10_000);
  const maxBytesPerRun = bound(options.maxBytesPerRun, 64 * 1024 * 1024, 1024 * 1024 * 1024);
  const timeout = options.runTimeoutMs;
  const runTimeoutMs = timeout === undefined ? undefined : bound(timeout, 1, 2_147_483_647);
  const maxPendingFlushes = bound(options.maxPendingFlushes, 1024, 65_536);
  const stopWhenIdle = options.stopWhenIdle ?? false;
  const signal = options.signal;
  if (typeof stopWhenIdle !== "boolean") throw new TypeError("stopWhenIdle must be a boolean");
  if (signal !== undefined) {
    Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal);
  }
  return { pollIntervalMs, maxDeliveriesPerRun, maxBytesPerRun, runTimeoutMs,
    maxPendingFlushes, stopWhenIdle, signal };
}

function resultField(value: unknown, key: string): unknown {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("A pump result is required");
  }
  const field = Object.getOwnPropertyDescriptor(value, key);
  if (field === undefined || !Object.hasOwn(field, "value")) {
    throw new TypeError("Pump result fields must be own data properties");
  }
  return field.value;
}

function captureResult(value: unknown, policy: Policy): ChangesetPumpResult {
  const count = (key: string): number => {
    const n = resultField(value, key);
    if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 0) {
      throw new RangeError("Pump counters must be nonnegative safe integers");
    }
    return n;
  };
  const result = {
    deliveries: count("deliveries"),
    bytes: count("bytes"),
    applied: count("applied"),
    omitted: count("omitted"),
    replays: count("replays"),
    alreadyAcknowledged: count("alreadyAcknowledged"),
    stopped: resultField(value, "stopped"),
  };
  const selected = result.deliveries + result.alreadyAcknowledged;
  if (
    (result.stopped !== "empty" && result.stopped !== "limit") ||
    selected > policy.maxDeliveriesPerRun ||
    result.bytes > policy.maxBytesPerRun ||
    result.replays > result.deliveries ||
    (result.deliveries === 0 && (result.bytes !== 0 || result.applied !== 0 || result.omitted !== 0)) ||
    (result.stopped === "limit" && selected === 0)
  ) {
    throw new RangeError("Pump returned inconsistent counts or a non-progressing limit");
  }
  return { ...result, stopped: result.stopped };
}

/**
 * Supervised continuous delivery over one existing pump. No concurrent runs,
 * prefetched payloads, detached promises or automatic retries after failure.
 * Own this pump exclusively until done settles. notify() coalesces wakeups; it
 * does not replace awaiting the source commit before asking for delivery.
 */
export class ChangesetDeliveryWorker {
  readonly #run: ChangesetDeliveryDriver["run"];
  readonly #policy: Policy;
  readonly #cancel = new AbortController();
  readonly #onAbort: () => void;
  readonly #flushes = new Set<FlushWaiter>();
  #failure: ChangesetDeliveryWorkerError | undefined;
  #state: ChangesetDeliveryWorkerStats["state"] = "running";
  #active = false;
  #notified = false;
  #wake: (() => void) | undefined;
  #attempts = 0;
  #successfulRuns = 0;
  #totals = { deliveries: 0, bytes: 0, applied: 0, omitted: 0, replays: 0, alreadyAcknowledged: 0 };
  readonly done: Promise<void>;

  private constructor(run: ChangesetDeliveryDriver["run"], policy: Policy) {
    this.#run = run;
    this.#policy = policy;
    this.#onAbort = () => this.#requestStop(
      true,
      policy.signal === undefined ? undefined :
        Object.getOwnPropertyDescriptor(AbortSignal.prototype, "reason")!.get!.call(policy.signal),
    );
    if (policy.signal !== undefined) {
      // A subclass must not throw from an overridden listener method after a
      // startup microtask has already acquired unowned delivery authority.
      EventTarget.prototype.addEventListener.call(policy.signal, "abort", this.#onAbort, { once: true });
      if (Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(policy.signal)) {
        this.#onAbort();
      }
    }
    // Do not run user code until start() returns a fully initialized owner.
    this.done = Promise.resolve().then(() => this.#loop());
    void this.done.catch(() => {});
  }

  static start(
    pump: ChangesetDeliveryDriver,
    options: ChangesetDeliveryWorkerOptions = {},
  ): ChangesetDeliveryWorker {
    const policy = capturePolicy(options);
    const run = pump?.run;
    if (typeof run !== "function") throw new TypeError("A changeset delivery pump is required");
    return new ChangesetDeliveryWorker((settings) => Reflect.apply(run, pump, [settings]), policy);
  }

  get stats(): ChangesetDeliveryWorkerStats {
    return this.#snapshot();
  }

  #snapshot(): ChangesetDeliveryWorkerStats {
    return Object.freeze({
      state: this.#state,
      active: this.#active,
      attempts: this.#attempts,
      successfulRuns: this.#successfulRuns,
      pendingFlushes: this.#flushes.size,
      ...this.#totals,
    });
  }

  /** Wake an idle drain, or request one follow-up read after the active run. */
  notify(): boolean {
    if (this.#stopping()) return false;
    this.#notified = true;
    this.#wake?.();
    return true;
  }

  /**
   * Await an empty observation from a run STARTED after this call. Await the
   * source commit first. This is neither permanent emptiness nor a durable
   * cursor, and an older in-flight empty read cannot satisfy the barrier.
   */
  flush(options: ChangesetDeliveryFlushOptions = {}): Promise<ChangesetDeliveryWorkerStats> {
    let signal: AbortSignal | undefined;
    let timeoutMs: number | undefined;
    try {
      // Capture getters before reserving a waiter: they may re-enter stop().
      signal = options.signal;
      const timeout = options.timeoutMs;
      timeoutMs = timeout === undefined ? undefined : bound(timeout, 1, 2_147_483_647);
      if (signal !== undefined) {
        Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal);
      }
    } catch (cause: unknown) {
      return Promise.reject(new ChangesetDeliveryFlushError("INPUT", cause));
    }
    if (this.#stopping()) {
      return Promise.reject(new ChangesetDeliveryFlushError("STOPPED", this.#failure));
    }
    if (signal !== undefined &&
      Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal)) {
      return Promise.reject(new ChangesetDeliveryFlushError("CANCELLED",
        Object.getOwnPropertyDescriptor(AbortSignal.prototype, "reason")!.get!.call(signal)));
    }
    if (this.#flushes.size >= this.#policy.maxPendingFlushes) {
      return Promise.reject(new ChangesetDeliveryFlushError("LIMIT"));
    }
    const promise = new Promise<ChangesetDeliveryWorkerStats>((resolve, reject) => {
      const waiter: FlushWaiter = {
        afterAttempt: this.#attempts,
        signal,
        deadline: timeoutMs === undefined ? undefined : performance.now() + timeoutMs,
        resolve, reject,
        onAbort: () => { this.#checkFlush(waiter); },
        timer: undefined,
      };
      this.#flushes.add(waiter);
      if (signal !== undefined) {
        EventTarget.prototype.addEventListener.call(signal, "abort", waiter.onAbort, { once: true });
      }
      this.#armFlush(waiter);
      this.#notified = true;
      this.#wake?.();
    });
    // Retain the rejection for the caller without a detached unhandled task.
    void promise.catch(() => {});
    return promise;
  }

  /** Join the active run, including its confirmation/cleanup. Never closes the database. */
  stop(options: ChangesetDeliveryWorkerStopOptions = {}): Promise<void> {
    const abort = options.abort ?? false;
    const reason = options.reason;
    if (typeof abort !== "boolean") throw new TypeError("abort must be a boolean");
    this.#requestStop(abort, reason);
    return this.done;
  }

  #stopping(): boolean {
    return this.#state !== "running" && this.#state !== "idle";
  }

  #requestStop(abort: boolean, reason: unknown): void {
    if (this.#state === "stopped" || this.#state === "failed") return;
    if (!this.#stopping()) this.#state = "draining";
    if (abort && this.#state !== "aborting") {
      this.#state = "aborting";
      this.#cancel.abort(reason);
    }
    this.#wake?.();
  }

  #removeFlush(waiter: FlushWaiter): boolean {
    if (!this.#flushes.delete(waiter)) return false;
    clearTimeout(waiter.timer);
    waiter.timer = undefined;
    if (waiter.signal !== undefined) {
      EventTarget.prototype.removeEventListener.call(waiter.signal, "abort", waiter.onAbort);
    }
    return true;
  }

  #checkFlush(waiter: FlushWaiter): boolean {
    if (!this.#flushes.has(waiter)) return false;
    let error: ChangesetDeliveryFlushError | undefined;
    if (waiter.signal !== undefined &&
      Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(waiter.signal)) {
      error = new ChangesetDeliveryFlushError("CANCELLED",
        Object.getOwnPropertyDescriptor(AbortSignal.prototype, "reason")!.get!.call(waiter.signal));
    } else if (waiter.deadline !== undefined && performance.now() >= waiter.deadline) {
      error = new ChangesetDeliveryFlushError("TIMEOUT");
    }
    if (error === undefined) return true;
    this.#removeFlush(waiter);
    waiter.reject(error);
    return false;
  }

  #armFlush(waiter: FlushWaiter): void {
    if (!this.#checkFlush(waiter) || waiter.deadline === undefined) return;
    waiter.timer = setTimeout(() => {
      waiter.timer = undefined;
      // Early timers re-arm; starved timers are checked again before success.
      this.#armFlush(waiter);
    }, Math.max(1, Math.ceil(waiter.deadline - performance.now())));
  }

  #resolveFlushes(): void {
    const ready: FlushWaiter[] = [];
    for (const waiter of this.#flushes) {
      if (this.#checkFlush(waiter) && this.#attempts > waiter.afterAttempt) {
        this.#removeFlush(waiter);
        ready.push(waiter);
      }
    }
    const stats = this.#snapshot();
    for (const waiter of ready) waiter.resolve(stats);
  }

  #record(result: ChangesetPumpResult): void {
    const next = { ...this.#totals };
    for (const key of Object.keys(next) as (keyof typeof next)[]) {
      next[key] += result[key];
      if (!Number.isSafeInteger(next[key])) throw new RangeError("Delivery statistics overflowed");
    }
    if (!Number.isSafeInteger(this.#successfulRuns + 1)) throw new RangeError("Run count overflowed");
    this.#totals = next;
    this.#successfulRuns++;
  }

  async #loop(): Promise<void> {
    let phase: ChangesetDeliveryWorkerError["phase"] = "run";
    try {
      while (!this.#stopping()) {
        this.#notified = false;
        this.#state = "running";
        phase = "run";
        if (!Number.isSafeInteger(this.#attempts + 1)) throw new RangeError("Attempt count overflowed");
        this.#attempts++;
        const settings: ChangesetPumpRunOptions = {
          maxDeliveries: this.#policy.maxDeliveriesPerRun,
          maxBytes: this.#policy.maxBytesPerRun,
          signal: this.#cancel.signal,
        };
        if (this.#policy.runTimeoutMs !== undefined) settings.timeoutMs = this.#policy.runTimeoutMs;
        this.#active = true;
        let raw: ChangesetPumpResult;
        try {
          raw = await this.#run(settings);
        } finally {
          this.#active = false;
        }
        phase = "result";
        const result = captureResult(raw, this.#policy);
        this.#record(result);
        if (result.stopped === "empty") this.#resolveFlushes();
        if (this.#stopping()) break;
        if (result.stopped === "empty" && !this.#notified && this.#policy.stopWhenIdle) break;
        // Even endless immediately-resolved limit runs must yield to task-level
        // cancellation and other connections. notify() cannot bypass this turn.
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
        if (this.#stopping()) break;
        if (result.stopped === "empty" && !this.#notified) {
          this.#state = "idle";
          await this.#wait();
        }
      }
      this.#state = "stopped";
    } catch (cause: unknown) {
      this.#state = "failed";
      // Do not hide storage or transport failure just because stop/abort raced
      // it. The original phase and uncertain outcome remain available as cause.
      this.#failure = new ChangesetDeliveryWorkerError(phase, cause);
      throw this.#failure;
    } finally {
      this.#wake?.();
      for (const waiter of this.#flushes) {
        this.#removeFlush(waiter);
        waiter.reject(new ChangesetDeliveryFlushError("STOPPED", this.#failure));
      }
      if (this.#policy.signal !== undefined) {
        EventTarget.prototype.removeEventListener.call(this.#policy.signal, "abort", this.#onAbort);
      }
    }
  }

  #wait(): Promise<void> {
    if (this.#notified || this.#stopping()) return Promise.resolve();
    return new Promise((resolve) => {
      const timer = setTimeout(wake, this.#policy.pollIntervalMs);
      const self = this;
      function wake(): void {
        clearTimeout(timer);
        if (self.#wake === wake) self.#wake = undefined;
        resolve();
      }
      this.#wake = wake;
    });
  }
}
