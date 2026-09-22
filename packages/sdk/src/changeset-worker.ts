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

export interface ChangesetDeliveryWorkerStats {
  readonly state: "running" | "idle" | "draining" | "aborting" | "stopped" | "failed";
  readonly active: boolean;
  readonly attempts: number;
  readonly successfulRuns: number;
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
  readonly stopWhenIdle: boolean;
  readonly signal: AbortSignal | undefined;
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
  const stopWhenIdle = options.stopWhenIdle ?? false;
  const signal = options.signal;
  if (typeof stopWhenIdle !== "boolean") throw new TypeError("stopWhenIdle must be a boolean");
  if (signal !== undefined) {
    Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal);
  }
  return { pollIntervalMs, maxDeliveriesPerRun, maxBytesPerRun, runTimeoutMs, stopWhenIdle, signal };
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
    return Object.freeze({
      state: this.#state,
      active: this.#active,
      attempts: this.#attempts,
      successfulRuns: this.#successfulRuns,
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
      throw new ChangesetDeliveryWorkerError(phase, cause);
    } finally {
      this.#wake?.();
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
