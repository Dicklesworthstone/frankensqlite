/** Persistent job state lives in ordinary SQL, never in the callback scheduler. */
export const DURABLE_JOBS_TABLE = "__fsqlite_durable_jobs_v1";
// Queue state must never resolve to a same-named TEMP or attached table.
// Qualify reads AND writes, including lease fences and continuation postludes.
const TABLE = `main."${DURABLE_JOBS_TABLE}"`;
const MAX_TEXT_BYTES = 1024 * 1024;
const MAX_LEASE_MS = 86_400_000;
const DEPENDENCIES_NAME = "__fsqlite_job_dependencies_v1";
const DEPENDENCIES = `main."${DEPENDENCIES_NAME}"`;
const MAX_DEPENDENCIES = 128;
type Parameter = string | number | null;
type SqlRow = Record<string, unknown>;

/** Minimal transaction surface; FrankenTransaction implements this directly. */
export interface DurableJobTransaction {
  execute(sql: string, params?: readonly Parameter[]): Promise<number>;
  query(sql: string, params?: readonly Parameter[]): Promise<{ readonly rows: readonly SqlRow[] }>;
}

/**
 * FrankenDB and FrankenDBQueue implement this contract. The callback MUST run
 * in one atomic transaction and settle only after commit/rollback completes.
 * Do not supply an adapter that retries callbacks or swallows commit failures.
 */
export interface DurableJobDatabase {
  transaction<T>(work: (tx: DurableJobTransaction) => Promise<T>): Promise<T>;
}

export interface DurableJobQueueOptions {
  /** Persisted leases use wall-clock milliseconds. Workers must agree on time. */
  clock?: () => number;
}

export interface DurableClaimOptions {
  /** Maximum receipts, 1..128; defaults to 16. */
  limit?: number;
  /** Lease duration for every claimed job; defaults to 30 seconds. */
  leaseMs?: number;
  /** Returned payload UTF-8 bytes, 1..64 MiB; defaults to 4 MiB. */
  maxPayloadBytes?: number;
}

export interface EnqueueJob {
  /** Stable idempotency key, scoped to this queue. Retain it across retries. */
  id: string;
  /** Application-encoded text (for example JSON), at most 1 MiB of UTF-8. */
  payload: string;
  priority?: number;
  availableAt?: number;
  /** Includes the first claim; defaults to 3, range 1..1,000,000. */
  maxAttempts?: number;
  /** Immutable existing prerequisites on this SAME database; every one must complete. */
  dependsOn?: readonly { readonly queue: string; readonly id: string }[];
}
type Dependency = NonNullable<EnqueueJob["dependsOn"]>[number];

export type DurableJobState = "ready" | "leased" | "completed" | "dead" | "cancelled";
export interface DurableJob {
  readonly id: string;
  readonly queue: string;
  readonly payload: string;
  readonly state: DurableJobState;
  readonly priority: number;
  readonly availableAt: number;
  readonly attempts: number;
  readonly maxAttempts: number;
  readonly owner: string | null;
  readonly leaseExpiresAt: number | null;
  readonly createdAt: number;
  readonly updatedAt: number;
  readonly result: string | null;
  readonly lastError: string | null;
}

/** An ownership receipt, not an authorization boundary against direct SQL. */
export interface DurableJobLease {
  readonly queue: string;
  readonly id: string;
  readonly payload: string;
  readonly owner: string;
  readonly token: string;
  readonly attempt: number;
  /** Informational; mutations always check the current persisted deadline. */
  readonly expiresAt: number;
}

export interface DurableEnqueueResult {
  readonly inserted: boolean;
  readonly job: DurableJob;
}

export interface DurableEnqueueWorkResult<T> extends DurableEnqueueResult {
  /** Undefined on deduplication: application work never ran again. */
  readonly value: T | undefined;
}

export interface DurableJobStats {
  readonly ready: number;
  readonly leased: number;
  readonly completed: number;
  readonly dead: number;
  readonly cancelled: number;
  /** Ready jobs whose schedule has arrived, with attempts remaining. */
  readonly available: number;
  /** Leased jobs whose deadline has passed, including exhausted attempts. */
  readonly expired: number;
  readonly total: number;
}

interface CapturedJob {
  readonly id: string;
  readonly payload: string;
  readonly priority: number;
  readonly availableAt: number | undefined;
  readonly maxAttempts: number;
  readonly dependsOn: readonly Dependency[];
}

export class DurableJobError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "DurableJobError";
  }
}

/**
 * At-least-once, fenced job delivery over a transaction-owning database.
 * Reopening the SAME durable database preserves jobs and claims. Independently
 * imported browser snapshots are NOT shared live queues; a snapshot checkpoint
 * conflict is not permission to replay jobs. Persistence is the host's policy.
 */
export class DurableJobQueue {
  readonly #db: DurableJobDatabase;
  readonly #clock: () => number;
  readonly name: string;

  private constructor(db: DurableJobDatabase, name: string, clock: () => number) {
    this.#db = db;
    this.name = name;
    this.#clock = clock;
  }

  static async open(
    db: DurableJobDatabase,
    name: string,
    options?: DurableJobQueueOptions,
  ): Promise<DurableJobQueue> {
    identifier(name, "queue name");
    if (db === null || typeof db !== "object" || typeof db.transaction !== "function") {
      throw new TypeError("A transaction-owning database is required");
    }
    const clock = options?.clock ?? Date.now;
    if (typeof clock !== "function") throw new TypeError("clock must be a function");
    const queue = new DurableJobQueue(db, name, clock);
    await db.transaction(async (tx) => {
      await tx.execute(`CREATE TABLE IF NOT EXISTS ${TABLE} (
        queue_name TEXT NOT NULL, job_id TEXT NOT NULL, payload TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('ready','leased','completed','dead','cancelled')),
        priority INTEGER NOT NULL, scheduled_at INTEGER NOT NULL, available_at INTEGER NOT NULL,
        attempts INTEGER NOT NULL CHECK (attempts >= 0),
        max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
        lease_owner TEXT, lease_token TEXT, lease_expires_at INTEGER,
        created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, result TEXT, last_error TEXT,
        PRIMARY KEY (queue_name, job_id),
        CHECK ((state = 'leased' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
          OR (state <> 'leased' AND lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL))
      )`);
      // SQLite qualifies the INDEX name; its table then belongs to that schema.
      await tx.execute(`CREATE INDEX IF NOT EXISTS main.__fsqlite_jobs_ready_v1 ON "${DURABLE_JOBS_TABLE}"
        (queue_name, state, available_at, priority)`);
      await tx.execute(`CREATE INDEX IF NOT EXISTS main.__fsqlite_jobs_expiry_v1 ON "${DURABLE_JOBS_TABLE}"
        (queue_name, state, lease_expires_at)`);
      await ensureDependencies(tx);
    });
    return queue;
  }

  /** A duplicate id returns its original job; conflicting input is rejected. */
  async enqueue(input: EnqueueJob): Promise<DurableEnqueueResult> {
    const captured = captureJob(input);
    return this.#db.transaction((tx) => this.#enqueueIn(tx, captured));
  }

  /**
   * Publish a bounded cross-queue workflow in one transaction. Input may name
   * parents later in the batch; cycles reject before SQL. Results retain input
   * order, not insertion order. External prerequisites must already exist.
   */
  async enqueueBatch(
    input: readonly (EnqueueJob & { readonly queue: string })[],
  ): Promise<readonly DurableEnqueueResult[]> {
    const jobs = captureJobBatch(input);
    const order = dependencyOrder(jobs);
    return this.#db.transaction(tx => this.#enqueueBatchIn(tx, jobs, order));
  }

  /**
   * Transactional outbox: application SQL and the new job commit together.
   * Duplicate ids never rerun work. Admitted callback SQL settles before commit
   * or rollback; a caught SQL failure still aborts. Saved handles expire when
   * work exits. Use only tx, await SQL, and never reenter this queue or perform
   * external side effects. This scope is not a SQL sandbox.
   */
  async enqueueWith<T>(
    input: EnqueueJob,
    work: (tx: DurableJobTransaction) => Promise<T>,
  ): Promise<DurableEnqueueWorkResult<T>> {
    const captured = captureJob(input);
    if (typeof work !== "function") throw new TypeError("An application SQL callback is required");
    return this.#db.transaction(async (tx) => {
      const result = await this.#enqueueIn(tx, captured);
      const value = result.inserted ? await runJobWork(tx, work) : undefined;
      return Object.freeze({ ...result, value });
    });
  }

  async get(id: string): Promise<DurableJob | null> {
    identifier(id, "job id");
    return this.#db.transaction(async (tx) => {
      const row = await this.#row(tx, id);
      return row === null ? null : decodeJob(row);
    });
  }

  /** A read-only snapshot of prerequisites. Missing/failed parents never satisfy a join. */
  async dependencies(id: string): Promise<readonly (Dependency & {
    readonly state: DurableJobState | null;
  })[] | null> {
    identifier(id, "job id");
    return this.#db.transaction(async tx => {
      if (await this.#row(tx, id) === null) return null;
      const refs = await readDependencies(tx, this.name, id);
      const result: (Dependency & { readonly state: DurableJobState | null })[] = [];
      for (const ref of refs) {
        const rows = (await tx.query(`SELECT state FROM ${TABLE} WHERE queue_name=? AND job_id=?`,
          [ref.queue, ref.id])).rows;
        if (rows.length > 1) throw corrupt("Ambiguous prerequisite job");
        result.push(Object.freeze({ ...ref, state: rows[0] === undefined ? null : jobState(rows[0]) }));
      }
      return Object.freeze(result);
    });
  }

  /**
   * Read ALL completed prerequisite results in one lease-checked SQL snapshot.
   * maxBytes counts database-encoded result bytes (not payloads, JS heap or RSS).
   * Admission checks every result size before loading any result body. Returned
   * byteLength uses that same encoding; null and empty text remain distinct.
   * This read does not renew a lease or authorize later external side effects.
   */
  async dependencyResults(
    lease: DurableJobLease,
    options: { readonly maxBytes?: number; readonly signal?: AbortSignal; readonly timeoutMs?: number } = {},
  ): Promise<readonly {
    readonly queue: string; readonly id: string; readonly result: string | null; readonly byteLength: number;
  }[]> {
    const keys = this.#keys(lease);
    const { maxBytes = 4 * MAX_TEXT_BYTES, signal, timeoutMs } = options;
    integer(maxBytes, "maxBytes", 0, 64 * MAX_TEXT_BYTES);
    if (timeoutMs !== undefined) integer(timeoutMs, "timeoutMs", 1, 2_147_483_647);
    if (signal !== undefined)
      Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal);
    const deadline = timeoutMs === undefined ? undefined : performance.now() + timeoutMs;
    const checkpoint = () => {
      signal?.throwIfAborted();
      if (deadline !== undefined && performance.now() >= deadline)
        throw new DurableJobError("ERR_FSQLITE_JOB_TIMEOUT", "Prerequisite result deadline expired");
    };
    checkpoint();
    return this.#db.transaction(async owner => {
      const tx: DurableJobTransaction = {
        execute: async () => { throw corrupt("Result reading cannot execute writes"); },
        query: async (sql, params) => {
          checkpoint();
          const rows = await owner.query(sql, params);
          checkpoint();
          if (!Array.isArray(rows.rows)) throw corrupt("Invalid prerequisite query result");
          return rows;
        },
      };
      const live = async () => {
        const rows = (await tx.query(`SELECT 1 AS live FROM ${TABLE} WHERE ${fence()} LIMIT 2`,
          [...keys, this.#now()])).rows;
        this.#changed(rows.length);
      };
      await live();
      await ensureDependencies(tx, false);
      const refs = await readDependencies(tx, this.name, keys[1]! as string);
      const metadata: { type: "text" | "null"; bytes: number }[] = [];
      let total = 0;
      // One bounded metadata row per parent. No parent/child payload, result,
      // error or other variable-size field is selected during admission.
      for (const ref of refs) {
        // Join the actual stored edge, not just its adapter-decoded identity.
        // Lossy decoding of corrupt key bytes must not select another parent.
        const rows = (await tx.query(`SELECT CASE WHEN parent.state='completed' THEN 1 ELSE 0 END AS completed,
          typeof(parent.result) AS result_type, CASE WHEN parent.result IS NULL THEN 0 ELSE length(CAST(parent.result AS BLOB)) END AS result_bytes
          FROM ${TABLE} AS parent JOIN ${DEPENDENCIES} AS edge
            ON edge.parent_queue=parent.queue_name COLLATE BINARY AND edge.parent_id=parent.job_id COLLATE BINARY
          WHERE edge.queue_name=? AND edge.job_id=? AND parent.queue_name=? AND parent.job_id=? LIMIT 2`,
          [this.name, keys[1]! as string, ref.queue, ref.id])).rows;
        if (rows.length !== 1 || number(rows[0]!, "completed") !== 1)
          throw new DurableJobError("ERR_FSQLITE_JOB_DEPENDENCY_INCOMPLETE", "Every prerequisite result requires a completed parent");
        const row = rows[0]!, type = row.result_type, bytes = number(row, "result_bytes");
        // A valid 1-MiB UTF-8 result may occupy 2 MiB in a UTF-16 database.
        if ((type !== "text" && type !== "null") || bytes < 0 || bytes > 2 * MAX_TEXT_BYTES ||
            (type === "null" && bytes !== 0)) throw corrupt("Invalid prerequisite result shape");
        if (bytes > maxBytes - total)
          throw new DurableJobError("ERR_FSQLITE_JOB_RESULT_LIMIT", "Complete prerequisite results exceed maxBytes; no bodies were loaded");
        total += bytes;
        metadata.push({ type, bytes });
      }
      const encodingRows = (await tx.query("PRAGMA main.encoding")).rows;
      const encoding = encodingRows[0]?.encoding;
      if (encodingRows.length !== 1 || (encoding !== "UTF-8" && encoding !== "UTF-16le" && encoding !== "UTF-16be"))
        throw corrupt("Unsupported prerequisite result encoding");
      const decoder = new TextDecoder(encoding, { fatal: true, ignoreBOM: true });
      const result: { readonly queue: string; readonly id: string; readonly result: string | null; readonly byteLength: number }[] = [];
      for (let i = 0; i < refs.length; i++) {
        const ref = refs[i]!, meta = metadata[i]!;
        // Guard the body projection too. A trusted same-owner callback that
        // changes a result between statements must not bypass its admitted size.
        const rows = (await tx.query(`SELECT CAST(result AS BLOB) AS result_data FROM ${TABLE}
          WHERE queue_name=? AND job_id=? AND state='completed' AND typeof(result)=?
            AND CASE WHEN result IS NULL THEN 0 ELSE length(CAST(result AS BLOB)) END=? LIMIT 2`,
          [ref.queue, ref.id, meta.type, meta.bytes])).rows;
        if (rows.length !== 1) throw corrupt("Prerequisite result changed during reading");
        const data = rows[0]!.result_data;
        let value: string | null = null;
        if (meta.type === "null") {
          if (data !== null) throw corrupt("NULL prerequisite result changed");
        } else {
          if (!(data instanceof Uint8Array) || data.byteLength !== meta.bytes)
            throw corrupt("Invalid prerequisite result bytes");
          try { value = decoder.decode(data); }
          catch { throw corrupt("Malformed prerequisite result encoding"); }
          if (new TextEncoder().encode(value).byteLength > MAX_TEXT_BYTES)
            throw corrupt("Stored prerequisite result exceeds its UTF-8 limit");
        }
        result.push(Object.freeze({ ...ref, result: value, byteLength: meta.bytes }));
      }
      await ensureDependencies(tx, false);
      await live();
      checkpoint();
      return Object.freeze(result);
    });
  }

  /**
   * Atomically claim ready work or reclaim an expired lease. Priority descends;
   * ties use availability, creation time and id. No user work runs in this txn.
   * Database contention propagates; there is no blind callback replay.
   */
  async claim(owner: string, leaseMs = 30_000): Promise<DurableJobLease | null> {
    const leases = await this.claimBatch(owner, {
      limit: 1,
      leaseMs,
      maxPayloadBytes: MAX_TEXT_BYTES,
    });
    return leases[0] ?? null;
  }

  /**
   * Claim a bounded prefix in one commit/checkpoint, without running handlers.
   * Stops at the byte budget rather than skipping higher-priority work. A SQL
   * or commit failure never returns a partially acknowledged batch of leases.
   */
  async claimBatch(
    owner: string,
    options?: DurableClaimOptions,
  ): Promise<readonly DurableJobLease[]> {
    identifier(owner, "worker owner");
    const { limit = 16, leaseMs = 30_000, maxPayloadBytes = 4 * MAX_TEXT_BYTES } = options ?? {};
    integer(limit, "limit", 1, 128);
    integer(leaseMs, "leaseMs", 1, MAX_LEASE_MS);
    integer(maxPayloadBytes, "maxPayloadBytes", MAX_TEXT_BYTES, 64 * MAX_TEXT_BYTES);
    return this.#db.transaction(async (tx) => {
      const now = this.#now();
      const expires = addTime(now, leaseMs);
      // Select only bounded identifiers: LIMIT must not materialize 128 MiB of
      // payload before the byte budget has a chance to stop admission.
      const candidates = (
        await tx.query(
          `SELECT job_id FROM ${TABLE} AS candidate WHERE queue_name = ?
        AND attempts < max_attempts AND ((state = 'ready' AND available_at <= ?)
          OR (state = 'leased' AND lease_expires_at <= ?))
        AND ${dependenciesReady("candidate")}
        ORDER BY priority DESC, available_at, created_at, job_id LIMIT ?`,
          [this.name, now, now, limit],
        )
      ).rows;
      const leases: DurableJobLease[] = [];
      let payloadBytes = 0;
      for (const candidate of candidates) {
        const id = string(candidate, "job_id");
        const row = (
          await tx.query(
            `SELECT payload, attempts FROM ${TABLE} WHERE queue_name = ? AND job_id = ?`,
            [this.name, id],
          )
        ).rows[0];
        if (row === undefined) throw corrupt("Claim candidate disappeared");
        const payload = string(row, "payload");
        const bytes = text(payload, "stored payload");
        if (payloadBytes + bytes > maxPayloadBytes) break;
        const token = crypto.randomUUID();
        const changed = await tx.execute(
          `UPDATE ${TABLE} SET state = 'leased', attempts = attempts + 1,
          lease_owner = ?, lease_token = ?, lease_expires_at = ?, updated_at = ?
          WHERE queue_name = ? AND job_id = ? AND attempts < max_attempts
            AND ((state = 'ready' AND available_at <= ?) OR (state = 'leased' AND lease_expires_at <= ?))`,
          [owner, token, expires, now, this.name, id, now, now],
        );
        if (changed !== 1)
          throw new DurableJobError(
            "ERR_FSQLITE_JOB_CONFLICT",
            "Job changed during claim; no lease was granted",
          );
        leases.push(
          Object.freeze({
            queue: this.name,
            id,
            payload,
            owner,
            token,
            attempt: number(row, "attempts") + 1,
            expiresAt: expires,
          }),
        );
        payloadBytes += bytes;
      }
      return Object.freeze(leases);
    });
  }

  /** A heartbeat cannot resurrect an expired lease or shorten its deadline. */
  async renew(lease: DurableJobLease, leaseMs = 30_000): Promise<DurableJobLease> {
    const keys = this.#keys(lease);
    integer(leaseMs, "leaseMs", 1, MAX_LEASE_MS);
    return this.#db.transaction(async (tx) => {
      const now = this.#now();
      const expires = addTime(now, leaseMs);
      this.#changed(
        await tx.execute(
          `UPDATE ${TABLE}
        SET lease_expires_at = CASE WHEN lease_expires_at > ? THEN lease_expires_at ELSE ? END, updated_at = ?
        WHERE ${fence()}`,
          [expires, expires, now, ...keys, now],
        ),
      );
      return this.#receipt(tx, keys[1]! as string);
    });
  }

  async complete(lease: DurableJobLease, result: string | null = null): Promise<void> {
    const keys = this.#keys(lease);
    if (result !== null) text(result, "result");
    await this.#db.transaction((tx) => this.#completeIn(tx, keys, result));
  }

  /**
   * Fence BEFORE application SQL, then check expiry again AFTER it. All SQL
   * effects and completion commit together, or all roll back. Compute outside
   * this callback; never do external I/O or call another queue method inside it.
   * The host's committed-but-unacknowledged errors propagate without replay.
   * Drain every admitted callback statement before the final lease fence;
   * SQL failures abort even if caught and escaped callback handles expire.
   */
  async completeWith<T>(
    lease: DurableJobLease,
    work: (tx: DurableJobTransaction) => Promise<T>,
    result: string | null = null,
  ): Promise<T> {
    const keys = this.#keys(lease);
    if (typeof work !== "function") throw new TypeError("An application SQL callback is required");
    if (result !== null) text(result, "result");
    return this.#completeWork(keys, (tx) => runJobWork(tx, work), result);
  }

  /**
   * Commit application effects, follow-up jobs and parent completion together.
   * Children may target other queues on this SAME database. Use stable child
   * ids: identical existing jobs deduplicate, conflicting inputs roll back all
   * effects. At most 128 children / 4 MiB combined UTF-8 payload are admitted.
   * No handler or external operation runs here, and nothing retries implicitly.
   */
  async completeAndEnqueue<T>(
    lease: DurableJobLease,
    next: readonly (EnqueueJob & { readonly queue: string })[],
    work: (tx: DurableJobTransaction) => Promise<T>,
    result: string | null = null,
  ): Promise<{
    readonly value: T;
    readonly jobs: readonly DurableEnqueueResult[];
  }> {
    const keys = this.#keys(lease);
    if (typeof work !== "function") throw new TypeError("An application SQL callback is required");
    if (result !== null) text(result, "result");
    const children = captureJobContinuations(this.name, keys[1]! as string, next);
    const order = dependencyOrder(children);
    return this.#completeWork(keys, async (tx) => {
      const value = await runJobWork(tx, work);
      const jobs = await this.#enqueueBatchIn(tx, children, order);
      return Object.freeze({ value, jobs });
    }, result);
  }

  async #enqueueBatchIn(
    tx: DurableJobTransaction,
    children: readonly (EnqueueJob & { readonly queue: string })[],
    order: readonly number[],
  ): Promise<readonly DurableEnqueueResult[]> {
    const jobs = new Array<DurableEnqueueResult>(children.length);
    for (const index of order) {
      // All queues share the existing owner. No open(), extra transaction,
      // checkpoint or worker execution is interleaved with graph publication.
      const child = children[index]!;
      const queue = new DurableJobQueue(this.#db, child.queue, this.#clock);
      jobs[index] = await queue.#enqueueIn(tx, captureJob(child));
    }
    return Object.freeze(jobs);
  }

  /** Internal: only private work may use the owner across the final lease check. */
  #completeWork<T>(
    keys: readonly Parameter[],
    work: (tx: DurableJobTransaction) => Promise<T>,
    result: string | null,
  ): Promise<T> {
    return this.#db.transaction(async (tx) => {
      const now = this.#now();
      // A conditional write, not an unlocked preflight read: competing claims
      // must conflict with this transaction on the ownership row.
      this.#changed(
        await tx.execute(`UPDATE ${TABLE} SET updated_at = ? WHERE ${fence()}`, [
          now,
          ...keys,
          now,
        ]),
      );
      const value = await work(tx);
      await this.#completeIn(tx, keys, result);
      return value;
    });
  }

  /** Live SQL counts in a single snapshot, not scheduler counters. */
  async stats(): Promise<DurableJobStats> {
    return this.#db.transaction(async (tx) => {
      const now = this.#now();
      const rows = (
        await tx.query(
          `SELECT state, COUNT(*) AS n,
        SUM(CASE WHEN state = 'ready' AND available_at <= ? AND attempts < max_attempts AND ${dependenciesReady("candidate")} THEN 1 ELSE 0 END) AS available,
        SUM(CASE WHEN state = 'leased' AND lease_expires_at <= ? THEN 1 ELSE 0 END) AS expired
        FROM ${TABLE} AS candidate WHERE queue_name = ? GROUP BY state`,
          [now, now, this.name],
        )
      ).rows;
      const counts = {
        ready: 0,
        leased: 0,
        completed: 0,
        dead: 0,
        cancelled: 0,
        available: 0,
        expired: 0,
        total: 0,
      };
      for (const row of rows) {
        const state = jobState(row);
        const count = number(row, "n");
        counts[state] = count;
        counts.available += number(row, "available");
        counts.expired += number(row, "expired");
        counts.total += count;
      }
      for (const value of Object.values(counts)) {
        if (!Number.isSafeInteger(value) || value < 0) throw corrupt("Invalid job count");
      }
      return Object.freeze(counts);
    });
  }

  async #completeIn(
    tx: DurableJobTransaction,
    keys: readonly Parameter[],
    result: string | null,
  ): Promise<void> {
    const now = this.#now();
    this.#changed(
      await tx.execute(
        `UPDATE ${TABLE} SET state = 'completed', result = ?, updated_at = ?,
      lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL WHERE ${fence()}`,
        [result, now, ...keys, now],
      ),
    );
  }

  async #enqueueIn(tx: DurableJobTransaction, input: CapturedJob): Promise<DurableEnqueueResult> {
    const { id, payload, priority, availableAt, maxAttempts } = input;
    // Edges only point to jobs that existed before this insertion. Immutable
    // requirements plus that order prevent cycles without graph-wide traversal.
    for (const ref of input.dependsOn) {
      if (ref.queue === this.name && ref.id === id)
        throw new TypeError("A job cannot depend on itself");
    }
    const now = this.#now();
    const scheduled = availableAt ?? now;
    const inserted = await tx.execute(
      `INSERT INTO ${TABLE}
      (queue_name,job_id,payload,state,priority,scheduled_at,available_at,attempts,max_attempts,created_at,updated_at)
      VALUES (?,?,?,'ready',?,?,?,0,?,?,?) ON CONFLICT(queue_name,job_id) DO NOTHING`,
      [this.name, id, payload, priority, scheduled, scheduled, maxAttempts, now, now],
    );
    const row = await this.#row(tx, id);
    if (row === null) throw corrupt("Enqueued job is missing");
    if (inserted !== 0 && inserted !== 1) throw corrupt("Invalid enqueue affected-row count");
    const retained = await readDependencies(tx, this.name, id);
    if (inserted === 1) {
      if (retained.length) throw corrupt("A new job has orphan prerequisite metadata");
      for (const ref of input.dependsOn) {
        const parent = (await tx.query(`SELECT state FROM ${TABLE} WHERE queue_name=? AND job_id=?`,
          [ref.queue, ref.id])).rows;
        if (parent.length !== 1) throw new DurableJobError("ERR_FSQLITE_JOB_DEPENDENCY_MISSING",
          "Every prerequisite must already exist on the same database");
        jobState(parent[0]!);
        if (await tx.execute(`INSERT INTO ${DEPENDENCIES} VALUES (?,?,?,?)`,
          [this.name, id, ref.queue, ref.id]) !== 1) throw corrupt("Prerequisite was not retained");
      }
    } else if (JSON.stringify(retained) !== JSON.stringify(input.dependsOn)) {
      throw new DurableJobError("ERR_FSQLITE_JOB_ID_CONFLICT", "Job id already identifies different prerequisites");
    }
    if (inserted === 1 && JSON.stringify(await readDependencies(tx, this.name, id)) !== JSON.stringify(input.dependsOn))
      throw corrupt("Job prerequisites were not retained exactly");
    const job = decodeJob(row);
    if (
      job.payload !== payload ||
      job.priority !== priority ||
      job.maxAttempts !== maxAttempts ||
      (availableAt !== undefined && number(row, "scheduled_at") !== availableAt)
    ) {
      throw new DurableJobError(
        "ERR_FSQLITE_JOB_ID_CONFLICT",
        "Job id already identifies different input",
      );
    }
    return Object.freeze({ inserted: inserted === 1, job });
  }

  /** Release to a delayed retry, or dead-letter the final attempt. */
  async fail(lease: DurableJobLease, error: string, retryDelayMs = 0): Promise<void> {
    const keys = this.#keys(lease);
    text(error, "error");
    integer(retryDelayMs, "retryDelayMs");
    await this.#db.transaction(async (tx) => {
      const now = this.#now();
      const available = addTime(now, retryDelayMs);
      this.#changed(
        await tx.execute(
          `UPDATE ${TABLE}
        SET state = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'ready' END,
          available_at = ?, updated_at = ?, last_error = ?,
          lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL WHERE ${fence()}`,
          [available, now, error, ...keys, now],
        ),
      );
    });
  }

  /** Cancel queued or leased work. A running handler must observe lease loss. */
  async cancel(id: string): Promise<boolean> {
    identifier(id, "job id");
    return this.#db.transaction(
      async (tx) =>
        (await tx.execute(
          `UPDATE ${TABLE}
      SET state = 'cancelled', updated_at = ?, lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
      WHERE queue_name = ? AND job_id = ? AND state IN ('ready','leased')`,
          [this.#now(), this.name, id],
        )) === 1,
    );
  }

  /**
   * Explicitly cancel up to limit ready jobs in THIS queue whose immutable
   * prerequisites include a dead/cancelled job. Repeat bounded pages to reach
   * descendants made impossible by this same transaction. Other queues require
   * their own sweep. Missing parents and retryable failures are not terminal
   * evidence. Never claim work, consume attempts, or run application callbacks.
   */
  async cancelBlocked(limit = 100): Promise<number> {
    integer(limit, "limit", 1, 1000);
    return this.#db.transaction(async tx => {
      // This is a live queue operation, not schema initialization or repair.
      await ensureDependencies(tx, false);
      const now = this.#now();
      const reason = "Cancelled because a prerequisite is dead or cancelled";
      let cancelled = 0;
      while (cancelled < limit) {
        const pageSize = Math.min(32, limit - cancelled);
        const rows = (await tx.query(`SELECT job_id FROM ${TABLE} AS candidate
          WHERE queue_name=? AND state='ready' AND ${dependenciesFailed("candidate")}
          ORDER BY job_id LIMIT ?`, [this.name, pageSize])).rows;
        if (!Array.isArray(rows) || rows.length > pageSize)
          throw corrupt("Blocked-job selection exceeded its requested page");
        if (!rows.length) break;
        const seen = new Set<string>();
        for (const row of rows) {
          const id = string(row, "job_id");
          identifier(id, "blocked job id");
          if (seen.has(id)) throw corrupt("Blocked-job selection repeated an identity");
          seen.add(id);
          // Recheck the terminal prerequisite in the mutation, not just a
          // preflight read. The owner provides isolation/conflict handling.
          const changed = await tx.execute(`UPDATE ${TABLE} AS candidate
            SET state='cancelled',updated_at=?,last_error=?,
              lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL
            WHERE queue_name=? AND job_id=? AND state='ready'
              AND ${dependenciesFailed("candidate")}`, [now, reason, this.name, id]);
          if (changed !== 1)
            throw new DurableJobError("ERR_FSQLITE_JOB_CONFLICT",
              "Blocked job changed during cancellation; the sweep must roll back");
          const saved = (await tx.query(`SELECT state,updated_at,last_error,
            lease_owner,lease_token,lease_expires_at FROM ${TABLE}
            WHERE queue_name=? AND job_id=?`, [this.name, id])).rows;
          if (saved.length !== 1 || saved[0]!.state !== "cancelled" ||
              number(saved[0]!, "updated_at") !== now || saved[0]!.last_error !== reason ||
              saved[0]!.lease_owner !== null || saved[0]!.lease_token !== null ||
              saved[0]!.lease_expires_at !== null)
            throw corrupt("Blocked-job cancellation was not retained");
          cancelled++;
        }
        // No cursor: a newly eligible descendant can sort BEFORE its parent.
        // Every nonempty page advances by at least one of the bounded mutations.
      }
      await ensureDependencies(tx, false);
      return cancelled;
    });
  }

  /** Bounded crash recovery, including workers that died on their final attempt. */
  async reapExpired(limit = 100): Promise<number> {
    integer(limit, "limit", 1, 1000);
    return this.#db.transaction(async (tx) => {
      const now = this.#now();
      return tx.execute(
        `UPDATE ${TABLE}
        SET state = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'ready' END,
          updated_at = ?, lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
        WHERE queue_name = ? AND state = 'leased' AND lease_expires_at <= ? AND job_id IN (
          SELECT job_id FROM ${TABLE} WHERE queue_name = ? AND state = 'leased' AND lease_expires_at <= ?
          ORDER BY lease_expires_at, job_id LIMIT ?)`,
        [now, this.name, now, this.name, now, limit],
      );
    });
  }

  #now(): number {
    const now = this.#clock();
    integer(now, "clock result");
    return now;
  }

  #keys(lease: DurableJobLease): readonly Parameter[] {
    const { queue, id, owner, token, attempt } = lease;
    if (queue !== this.name) throw new TypeError("Lease belongs to another queue");
    identifier(id, "job id");
    identifier(owner, "worker owner");
    identifier(token, "lease token");
    integer(attempt, "attempt", 1, 1_000_000);
    return [queue, id, owner, token, attempt];
  }

  #changed(changed: number): void {
    if (changed !== 1)
      throw new DurableJobError(
        "ERR_FSQLITE_JOB_LEASE_LOST",
        "Job lease expired, was cancelled, or belongs to another claim; no mutation was committed",
      );
  }

  async #row(tx: DurableJobTransaction, id: string): Promise<SqlRow | null> {
    const rows = (
      await tx.query(`SELECT * FROM ${TABLE} WHERE queue_name = ? AND job_id = ?`, [this.name, id])
    ).rows;
    return rows[0] ?? null;
  }

  async #receipt(tx: DurableJobTransaction, id: string): Promise<DurableJobLease> {
    const row = await this.#row(tx, id);
    if (row === null || row.state !== "leased") throw corrupt("Claimed job is missing");
    const job = decodeJob(row);
    return Object.freeze({
      queue: job.queue,
      id: job.id,
      payload: job.payload,
      owner: string(row, "lease_owner"),
      token: string(row, "lease_token"),
      attempt: job.attempts,
      expiresAt: number(row, "lease_expires_at"),
    });
  }
}

/** @internal Shared worker/queue callback drain, not a transaction owner. */
export async function runJobWork<T>(
  tx: DurableJobTransaction,
  work: (tx: DurableJobTransaction) => Promise<T>,
): Promise<T> {
  let accepting = true, failed = false;
  let firstFailure: unknown;
  const pending = new Set<Promise<unknown>>();
  const submit = <U>(operation: () => Promise<U>): Promise<U> => {
    if (!accepting) return Promise.reject(new DurableJobError(
      "ERR_FSQLITE_JOB_SCOPE_ENDED", "The job callback SQL scope has ended",
    ));
    // Convert synchronous adapter throws to observed statement failures too.
    const task = (async () => operation())();
    pending.add(task);
    void task.then(() => pending.delete(task), cause => {
      pending.delete(task);
      if (!failed) { failed = true; firstFailure = cause; }
    });
    return task;
  };
  const scoped = Object.freeze({
    execute: (sql, params) => submit(() => tx.execute(sql, params)),
    query: (sql, params) => submit(() => tx.query(sql, params)),
  } satisfies DurableJobTransaction);
  let value: T;
  try { value = await work(scoped); }
  finally {
    accepting = false;
    // A thrown callback (including cancellation) takes precedence, but cannot
    // leave started SQL racing the owner's rollback. Admission is now closed.
    await Promise.allSettled(pending);
  }
  if (failed) throw firstFailure;
  return value;
}

/** @internal Own and validate continuation input before worker heartbeat joins. */
export function captureJobContinuations(
  parentQueue: string,
  parentId: string,
  next: readonly (EnqueueJob & { readonly queue: string })[],
): readonly (EnqueueJob & { readonly queue: string })[] {
  const jobs = captureJobBatch(next);
  if (jobs.some(job => job.queue === parentQueue && job.id === parentId))
    throw new TypeError("A job cannot enqueue itself as a continuation");
  // Worker validation runs before joining heartbeats or starting apply effects.
  dependencyOrder(jobs);
  return jobs;
}

function captureJobBatch(
  next: readonly (EnqueueJob & { readonly queue: string })[],
): readonly (EnqueueJob & { readonly queue: string })[] {
  if (!Array.isArray(next) || next.length < 1 || next.length > 128)
    throw new RangeError("A continuation requires 1..128 follow-up jobs");
  const children: (EnqueueJob & { readonly queue: string })[] = [];
  const seen = new Set<string>();
  let bytes = 0;
  let dependencies = 0;
  // Do not use a caller's array iterator or keep mutable input across an await.
  for (let i = 0, n = next.length; i < n; i++) {
    const child = next[i]!;
    const queue = child?.queue;
    identifier(queue, "follow-up queue name");
    const input = captureJob(child);
    const key = JSON.stringify([queue, input.id]);
    if (seen.has(key)) throw new TypeError("Duplicate follow-up job identity");
    seen.add(key);
    bytes += new TextEncoder().encode(input.payload).byteLength;
    dependencies += input.dependsOn.length;
    if (dependencies > 1024) throw new RangeError("Follow-up jobs exceed 1024 prerequisites");
    if (bytes > 4 * MAX_TEXT_BYTES)
      throw new RangeError("Follow-up payloads exceed 4 MiB of UTF-8");
    children.push(Object.freeze({ queue, id: input.id, payload: input.payload,
      priority: input.priority, maxAttempts: input.maxAttempts,
      ...(input.availableAt === undefined ? {} : { availableAt: input.availableAt }),
      ...(input.dependsOn.length ? { dependsOn: input.dependsOn } : {}),
    }));
  }
  return Object.freeze(children);
}

/** Bounded iterative topological ordering, not a recursive whole-database walk. */
function dependencyOrder(jobs: readonly (EnqueueJob & { readonly queue: string })[]): readonly number[] {
  const key = (queue: string, id: string) => JSON.stringify([queue, id]);
  const index = new Map(jobs.map((job, i) => [key(job.queue, job.id), i]));
  const waiting = jobs.map(() => 0);
  const dependents: number[][] = jobs.map(() => []);
  for (let i = 0; i < jobs.length; i++) {
    for (const ref of jobs[i]!.dependsOn ?? []) {
      const parent = index.get(key(ref.queue, ref.id));
      if (parent === undefined) continue;
      waiting[i] = waiting[i]! + 1;
      dependents[parent]!.push(i);
    }
  }
  const order: number[] = [];
  for (let i = 0; i < jobs.length; i++) if (waiting[i] === 0) order.push(i);
  for (let cursor = 0; cursor < order.length; cursor++) {
    for (const child of dependents[order[cursor]!]!) {
      waiting[child] = waiting[child]! - 1;
      if (waiting[child] === 0) order.push(child);
    }
  }
  if (order.length !== jobs.length)
    throw new DurableJobError("ERR_FSQLITE_JOB_DEPENDENCY_CYCLE", "The batch contains cyclic job prerequisites");
  return Object.freeze(order);
}

function fence(): string {
  return "queue_name = ? AND job_id = ? AND lease_owner = ? AND lease_token = ? AND attempts = ? AND state = 'leased' AND lease_expires_at > ?";
}

function captureJob(input: EnqueueJob): CapturedJob {
  // Capture caller-owned getters once, before queue admission can yield.
  const { id, payload, priority = 0, availableAt, maxAttempts = 3, dependsOn } = input;
  identifier(id, "job id");
  text(payload, "payload");
  integer(priority, "priority", -2_147_483_648, 2_147_483_647);
  integer(maxAttempts, "maxAttempts", 1, 1_000_000);
  if (availableAt !== undefined) integer(availableAt, "availableAt");
  return { id, payload, priority, availableAt, maxAttempts, dependsOn: captureDependencies(dependsOn) };
}

function captureDependencies(input: EnqueueJob["dependsOn"]): readonly Dependency[] {
  if (input === undefined) return Object.freeze([]);
  if (!Array.isArray(input) || input.length > MAX_DEPENDENCIES)
    throw new RangeError(`A job supports at most ${MAX_DEPENDENCIES} prerequisites`);
  const refs: Dependency[] = [];
  const seen = new Set<string>();
  for (let i = 0, n = input.length; i < n; i++) {
    const ref = input[i], queue = ref?.queue, id = ref?.id;
    identifier(queue, "prerequisite queue"); identifier(id, "prerequisite id");
    const key = JSON.stringify([queue, id]);
    if (seen.has(key)) throw new TypeError("Duplicate prerequisite identity");
    seen.add(key); refs.push(Object.freeze({ queue, id }));
  }
  refs.sort((a, b) => a.queue < b.queue ? -1 : a.queue > b.queue ? 1 : a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
  return Object.freeze(refs);
}

/** Only internal aliases are accepted here; no caller SQL is interpolated. */
function dependenciesReady(alias: "candidate" | "NEW"): string {
  return `NOT EXISTS (SELECT 1 FROM ${DEPENDENCIES} AS dependency
    WHERE dependency.queue_name=${alias}.queue_name AND dependency.job_id=${alias}.job_id
      AND NOT EXISTS (SELECT 1 FROM ${TABLE} AS parent
        WHERE parent.queue_name=dependency.parent_queue AND parent.job_id=dependency.parent_id
          AND parent.state='completed'))`;
}

/** Only known terminal failure, not a missing or still-retryable parent. */
function dependenciesFailed(alias: "candidate"): string {
  return `EXISTS (SELECT 1 FROM ${DEPENDENCIES} AS dependency
    JOIN ${TABLE} AS parent ON parent.queue_name=dependency.parent_queue
      AND parent.job_id=dependency.parent_id
    WHERE dependency.queue_name=${alias}.queue_name AND dependency.job_id=${alias}.job_id
      AND parent.state IN ('dead','cancelled'))`;
}

async function readDependencies(tx: DurableJobTransaction, queue: string, id: string): Promise<readonly Dependency[]> {
  // Bound database-encoded identity bytes before the SQL adapter decodes text.
  // 256 UTF-16 code units fit in 1024 UTF-8 bytes and 512 UTF-16 bytes.
  const stored = (column: string) => `CASE WHEN typeof(${column})='text' AND length(CAST(${column} AS BLOB))<=1024 AND instr(${column},char(0))=0 THEN ${column} END AS ${column}`;
  const rows = (await tx.query(`SELECT ${stored("parent_queue")},${stored("parent_id")} FROM ${DEPENDENCIES}
    WHERE queue_name=? AND job_id=? LIMIT ${MAX_DEPENDENCIES + 1}`, [queue, id])).rows;
  // Canonical JS ordering is independent of the database's UTF-8/UTF-16 order.
  return captureDependencies(rows.map(row => ({ queue: string(row, "parent_queue"), id: string(row, "parent_id") })));
}

/** Install an immutable edge store and a storage-level claim guard as one unit. */
async function ensureDependencies(tx: DurableJobTransaction, create = true): Promise<void> {
  const objects = [
    { kind: "TABLE", name: DEPENDENCIES_NAME,
      body: "(queue_name TEXT NOT NULL COLLATE BINARY, job_id TEXT NOT NULL COLLATE BINARY, parent_queue TEXT NOT NULL COLLATE BINARY, parent_id TEXT NOT NULL COLLATE BINARY, PRIMARY KEY(queue_name,job_id,parent_queue,parent_id)) WITHOUT ROWID" },
    { kind: "TRIGGER", name: "__fsqlite_jobs_dependencies_claim_v1",
      body: `BEFORE UPDATE OF state ON "${DURABLE_JOBS_TABLE}" WHEN NEW.state='leased' AND NOT (${dependenciesReady("NEW")}) BEGIN SELECT RAISE(ABORT,'Job prerequisites are not completed'); END` },
    { kind: "TRIGGER", name: "__fsqlite_jobs_dependencies_update_v1",
      body: `BEFORE UPDATE ON "${DEPENDENCIES_NAME}" BEGIN SELECT RAISE(ABORT,'Job prerequisites are immutable'); END` },
    { kind: "TRIGGER", name: "__fsqlite_jobs_dependencies_delete_v1",
      body: `BEFORE DELETE ON "${DEPENDENCIES_NAME}" BEGIN SELECT RAISE(ABORT,'Job prerequisites are immutable'); END` },
  ];
  const read = async () => (await tx.query(`SELECT name,sql FROM main.sqlite_schema WHERE name COLLATE NOCASE IN (${objects.map(() => "?").join(",")})`, objects.map(o => o.name))).rows;
  let rows = await read();
  if (rows.length === 0 && create) {
    for (const object of objects)
      await tx.execute(`CREATE ${object.kind} main."${object.name}" ${object.body}`);
    rows = await read();
  }
  if (rows.length !== objects.length || objects.some(o => !rows.some(r =>
    r.name === o.name && r.sql === `CREATE ${o.kind} "${o.name}" ${o.body}`))) {
    throw new DurableJobError("ERR_FSQLITE_JOB_SCHEMA", "Job dependency storage is incomplete or incompatible; it was not repaired");
  }
}

function identifier(value: string, label: string): void {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > 256 ||
    value.includes("\0")
  ) {
    throw new TypeError(`${label} must be a nonempty string of at most 256 characters without NUL`);
  }
  // Graph keys must survive SQL UTF-8 binding unchanged. Distinct unpaired
  // surrogate strings otherwise collapse onto the same replacement character.
  if (new TextDecoder("utf-8", { ignoreBOM: true }).decode(new TextEncoder().encode(value)) !== value)
    throw new TypeError(`${label} must contain well-formed Unicode`);
}

function text(value: string, label: string): number {
  if (typeof value !== "string") throw new TypeError(`${label} must be a string`);
  // Check code-unit length first to avoid allocating for obviously oversized input.
  if (value.length > MAX_TEXT_BYTES)
    throw new RangeError(`${label} exceeds ${MAX_TEXT_BYTES} UTF-8 bytes`);
  const bytes = new TextEncoder().encode(value).byteLength;
  if (bytes > MAX_TEXT_BYTES) {
    throw new RangeError(`${label} exceeds ${MAX_TEXT_BYTES} UTF-8 bytes`);
  }
  return bytes;
}

function integer(value: number, label: string, min = 0, max = Number.MAX_SAFE_INTEGER): void {
  if (!Number.isSafeInteger(value) || value < min || value > max)
    throw new RangeError(`${label} must be an integer in ${min}..${max}`);
}

function addTime(now: number, duration: number): number {
  const value = now + duration;
  integer(value, "deadline");
  return value;
}

function corrupt(message: string): DurableJobError {
  return new DurableJobError("ERR_FSQLITE_JOB_CORRUPT", message);
}
function string(row: SqlRow, key: string): string {
  const value = row[key];
  if (typeof value !== "string") throw corrupt(`Invalid job field: ${key}`);
  return value;
}
function number(row: SqlRow, key: string): number {
  const value = typeof row[key] === "bigint" ? Number(row[key]) : row[key];
  if (typeof value !== "number" || !Number.isSafeInteger(value))
    throw corrupt(`Invalid job field: ${key}`);
  return value;
}
function nullableString(row: SqlRow, key: string): string | null {
  return row[key] === null ? null : string(row, key);
}
function jobState(row: SqlRow): DurableJobState {
  const state = string(row, "state");
  if (!["ready", "leased", "completed", "dead", "cancelled"].includes(state))
    throw corrupt("Invalid job state");
  return state as DurableJobState;
}
function decodeJob(row: SqlRow): DurableJob {
  return Object.freeze({
    id: string(row, "job_id"),
    queue: string(row, "queue_name"),
    payload: string(row, "payload"),
    state: jobState(row),
    priority: number(row, "priority"),
    availableAt: number(row, "available_at"),
    attempts: number(row, "attempts"),
    maxAttempts: number(row, "max_attempts"),
    owner: nullableString(row, "lease_owner"),
    leaseExpiresAt: row.lease_expires_at === null ? null : number(row, "lease_expires_at"),
    createdAt: number(row, "created_at"),
    updatedAt: number(row, "updated_at"),
    result: nullableString(row, "result"),
    lastError: nullableString(row, "last_error"),
  });
}
