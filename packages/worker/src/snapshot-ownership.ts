/** Session ownership is separate from the stores' short publication/CAS locks. */
export type SnapshotOwnership = "shared" | "exclusive";
type SnapshotBackend = "indexeddb-snapshot" | "opfs-snapshot";

export class SnapshotOwnershipError extends Error {
  readonly transient: boolean;
  readonly userRecoverable = true;
  constructor(readonly code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_INPUT" |
    "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE" | "ERR_FSQLITE_SNAPSHOT_OWNED", message: string) {
    super(message);
    this.name = "SnapshotOwnershipError";
    this.transient = code === "ERR_FSQLITE_SNAPSHOT_OWNED";
  }
}

/** Capture once at admission. An explicit request must never become a default. */
export function resolveSnapshotOwnership(value: unknown): SnapshotOwnership | undefined {
  if (value === undefined || value === "shared" || value === "exclusive") return value;
  throw new SnapshotOwnershipError("ERR_FSQLITE_SNAPSHOT_OWNERSHIP_INPUT",
    "snapshotOwnership must be shared or exclusive");
}

/**
 * A cooperative, origin-local session lease held in the database worker realm.
 * Shared sessions preserve independent-image/CAS semantics. An exclusive
 * session excludes both shared and exclusive peers using this protocol, so a
 * job consumer cannot compete with another live copy of the same snapshot.
 * This is NOT a native database lock, cross-origin authority or SQL MVCC.
 *
 * Acquire BEFORE loading stored bytes and release AFTER database cleanup.
 * Web Locks release abandoned ownership when the worker realm is terminated.
 */
export class SnapshotSessionLease {
  readonly #release: () => void;
  readonly #finished: Promise<void>;
  #closing = false;

  private constructor(
    readonly persistence: SnapshotBackend,
    readonly name: string,
    readonly mode: SnapshotOwnership,
    release: () => void,
    finished: Promise<void>,
  ) {
    this.#release = release;
    this.#finished = finished;
    Object.freeze(this);
  }

  /**
   * Non-waiting admission: refuse contention instead of retaining an unbounded
   * queue of opens. Defaults to a shared lease when Web Locks is available.
   * A legacy default session may run without Web Locks, but an explicit policy
   * always requires real ownership and must be acknowledged by the host.
   */
  static async acquire(
    persistence: SnapshotBackend, name: string, requested?: SnapshotOwnership,
  ): Promise<SnapshotSessionLease | null> {
    const mode = resolveSnapshotOwnership(requested) ?? "shared";
    if ((persistence !== "indexeddb-snapshot" && persistence !== "opfs-snapshot") ||
        typeof name !== "string" || name.trim().length === 0 || name === ":memory:" ||
        name.length > 256 || name.includes("\0")) {
      throw new SnapshotOwnershipError("ERR_FSQLITE_SNAPSHOT_OWNERSHIP_INPUT",
        "Snapshot ownership requires a snapshot backend and a valid named database");
    }
    const locks = globalThis.navigator?.locks;
    if (typeof locks?.request !== "function") {
      if (requested === undefined) return null;
      throw new SnapshotOwnershipError("ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE",
        "Explicit snapshot ownership requires secure-context Web Locks");
    }
    // JSON escaping preserves lone UTF-16 surrogates and makes the pair
    // unambiguous. Do not hash TextEncoder(name), which merges invalid strings.
    const key = `frankensqlite:snapshot-session:v1:${JSON.stringify([persistence, name])}`;
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    let finish!: () => void;
    let failFinish!: (cause: unknown) => void;
    const finished = new Promise<void>((resolve, reject) => { finish = resolve; failFinish = reject; });
    // release() still exposes a lock-manager failure to its owning close fence.
    void finished.catch(() => {});
    let grant!: (lease: SnapshotSessionLease) => void;
    let refuse!: (cause: unknown) => void;
    const admitted = new Promise<SnapshotSessionLease>((resolve, reject) => { grant = resolve; refuse = reject; });
    let entered = false;
    const lease = new SnapshotSessionLease(persistence, name, mode, release, finished);
    try {
      const request = locks.request(key, { mode, ifAvailable: true }, lock => {
        if (lock === null) {
          refuse(new SnapshotOwnershipError("ERR_FSQLITE_SNAPSHOT_OWNED",
            "This snapshot has an incompatible live owner; close that session before reopening"));
          return;
        }
        entered = true;
        grant(lease);
        return held;
      });
      void request.then(() => {
        if (!entered) refuse(new SnapshotOwnershipError("ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE",
          "The lock manager did not grant snapshot ownership"));
        finish();
      }, cause => { refuse(cause); failFinish(cause); });
    } catch (cause: unknown) {
      refuse(cause);
      failFinish(cause);
    }
    return admitted;
  }

  /** Reinitializing the same owned namespace may transfer, not reacquire, it. */
  matches(persistence: string, name: string, mode: SnapshotOwnership | undefined): boolean {
    return !this.#closing && this.persistence === persistence && this.name === name &&
      this.mode === (mode ?? "shared");
  }

  /** Idempotent; settlement acknowledges that the lock-manager callback ended. */
  close(): Promise<void> {
    if (!this.#closing) { this.#closing = true; this.#release(); }
    return this.#finished;
  }
}
