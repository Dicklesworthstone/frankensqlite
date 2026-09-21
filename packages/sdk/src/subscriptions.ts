import { FrankenSQLiteError } from "./errors";

/** A local invalidation range, not a row-level change log or durable checkpoint. */
export interface CommittedTableChange {
  readonly tables: readonly string[];
  /** Queue-local sequences of matching committed invalidations, starting at 1. */
  readonly firstSequence: bigint;
  readonly lastSequence: bigint;
  /** Matching commits represented by this delivery; gaps can be unrelated tables. */
  readonly commits: bigint;
}

export type TableChangeListener = (change: CommittedTableChange) => void | Promise<void>;

export interface TableSubscription {
  readonly tables: readonly string[];
  readonly state: "active" | "stopped" | "failed";
  /** Original listener/connection failure. Use state to distinguish thrown undefined. */
  readonly failure: unknown;
  /** Settles after stop and any active listener; listener failures reject it. */
  readonly done: Promise<void>;
  /** Stop future deliveries immediately, without waiting for SQL queue capacity. */
  unsubscribe(): void;
}

export interface TableChangeStream extends AsyncIterableIterator<CommittedTableChange> {
  readonly tables: readonly string[];
  readonly closed: boolean;
}

/** Internal bounded coalescing; inputs are owned, immutable notification records. */
export function mergeChanges(
  previous: CommittedTableChange | null,
  next: CommittedTableChange,
): CommittedTableChange {
  if (previous === null) return next;
  return Object.freeze({
    tables: Object.freeze([...new Set([...previous.tables, ...next.tables])].sort()),
    firstSequence: previous.firstSequence,
    lastSequence: next.lastSequence,
    commits: previous.commits + next.commits,
  });
}

/** One running listener plus one coalesced pending record, never an event queue. */
export class ChangeObserver {
  readonly handle: TableSubscription;
  readonly #tables: ReadonlySet<string>;
  #listener: TableChangeListener | null;
  #detach: (() => void) | null;
  #release: (() => void) | null;
  #state: TableSubscription["state"] = "active";
  #failure: unknown;
  #pending: CommittedTableChange | null = null;
  #running = false;
  #settled = false;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #signal: AbortSignal | undefined;
  readonly #onAbort = () => this.stop();
  #resolve!: () => void;
  #reject!: (cause: unknown) => void;

  constructor(
    tables: readonly string[],
    listener: TableChangeListener,
    detach: () => void,
    release: () => void,
  ) {
    const names = Object.freeze([...tables]);
    this.#tables = new Set(names);
    this.#listener = listener;
    this.#detach = detach;
    this.#release = release;
    const done = new Promise<void>((resolve, reject) => {
      this.#resolve = resolve;
      this.#reject = reject;
    });
    // Observe internally without changing the public promise's rejection. Users
    // can attach error handling later without a process-level unhandled rejection.
    void done.catch(() => {});
    const observer = this;
    this.handle = Object.freeze({
      tables: names,
      done,
      get state() {
        return observer.#state;
      },
      get failure() {
        return observer.#failure;
      },
      unsubscribe() {
        observer.stop();
      },
    });
  }

  get pending(): boolean {
    return this.#pending !== null;
  }
  get running(): boolean {
    return this.#running;
  }

  /** Called only after insertion into the owner's active/reserved observer sets. */
  activate(signal: AbortSignal | undefined): void {
    this.#signal = signal;
    signal?.addEventListener("abort", this.#onAbort, { once: true });
    if (signal?.aborted) this.stop();
  }

  publish(tables: readonly string[], sequence: bigint): void {
    if (this.#state !== "active") return;
    const matching = tables.filter((table) => this.#tables.has(table));
    if (matching.length === 0) return;
    const change = Object.freeze({
      tables: Object.freeze(matching.sort()),
      firstSequence: sequence,
      lastSequence: sequence,
      commits: 1n,
    });
    this.#pending = mergeChanges(this.#pending, change);
    this.#schedule();
  }

  stop(): void {
    if (this.#state !== "active") return;
    this.#state = "stopped";
    this.#discard();
    this.#finish();
  }

  fail(cause: unknown): void {
    if (this.#settled) return;
    if (this.#state === "failed") {
      if (cause !== this.#failure)
        this.#failure = new AggregateError(
          [this.#failure, cause],
          "Subscription connection and listener both failed",
          { cause: this.#failure },
        );
      return;
    }
    this.#failure = cause;
    this.#state = "failed";
    this.#discard();
    this.#finish();
  }

  #discard(): void {
    this.#pending = null;
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#signal?.removeEventListener("abort", this.#onAbort);
    this.#signal = undefined;
    this.#detach?.();
    this.#detach = null;
  }

  #schedule(): void {
    if (
      this.#state !== "active" ||
      this.#running ||
      this.#timer !== undefined ||
      this.#pending === null
    )
      return;
    // A task (not a reentrant callback in COMMIT settlement) lets the SQL job
    // release ownership first. User code never decides a committed write's result.
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      void this.#deliver();
    }, 0);
  }

  async #deliver(): Promise<void> {
    if (this.#state !== "active" || this.#pending === null) return;
    const change = this.#pending;
    this.#pending = null;
    this.#running = true;
    try {
      await this.#listener!(change);
    } catch (cause: unknown) {
      this.fail(cause);
    } finally {
      this.#running = false;
      this.#finish();
      this.#schedule();
    }
  }

  #finish(): void {
    if (this.#state === "active" || this.#running || this.#settled) return;
    this.#settled = true;
    this.#release?.();
    this.#release = null;
    this.#listener = null;
    if (this.#state === "failed") this.#reject(this.#failure);
    else this.#resolve();
  }
}

/** Bounded pull delivery over the same committed subscription, not row streaming. */
export async function createChangeStream(
  register: (listener: TableChangeListener) => Promise<TableSubscription>,
): Promise<TableChangeStream> {
  let buffered: CommittedTableChange | null = null;
  let closed = false;
  let failure: { cause: unknown } | null = null;
  let waiting: {
    resolve: (value: IteratorResult<CommittedTableChange>) => void;
    reject: (cause: unknown) => void;
  } | null = null;
  const finish = (error: { cause: unknown } | null = null): void => {
    if (closed) return;
    closed = true;
    failure = error;
    buffered = null;
    const pending = waiting;
    waiting = null;
    if (error !== null) pending?.reject(error.cause);
    else pending?.resolve({ value: undefined, done: true });
  };
  const subscription = await register((change) => {
    if (closed) return;
    if (waiting !== null) {
      const pending = waiting;
      waiting = null;
      pending.resolve({ value: change, done: false });
    } else {
      buffered = mergeChanges(buffered, change);
    }
  });
  void subscription.done.then(
    () => finish(),
    (cause) => finish({ cause }),
  );
  const iterator: TableChangeStream = Object.freeze({
    tables: subscription.tables,
    get closed() {
      return closed;
    },
    next(): Promise<IteratorResult<CommittedTableChange>> {
      if (failure !== null) return Promise.reject(failure.cause);
      if (closed) return Promise.resolve({ value: undefined, done: true });
      if (buffered !== null) {
        const value = buffered;
        buffered = null;
        return Promise.resolve({ value, done: false });
      }
      if (waiting !== null)
        return Promise.reject(
          new FrankenSQLiteError({
            code: "ERR_FSQLITE_SUBSCRIPTION_NEXT_PENDING",
            transient: false,
            message: "Await the outstanding change-stream next() before requesting another value",
          }),
        );
      return new Promise((resolve, reject) => {
        waiting = { resolve, reject };
      });
    },
    return(): Promise<IteratorResult<CommittedTableChange>> {
      subscription.unsubscribe();
      finish();
      return Promise.resolve({ value: undefined, done: true });
    },
    throw(cause?: unknown): Promise<IteratorResult<CommittedTableChange>> {
      subscription.unsubscribe();
      finish({ cause });
      return Promise.reject(cause);
    },
    [Symbol.asyncIterator]() {
      return iterator;
    },
  });
  return iterator;
}
