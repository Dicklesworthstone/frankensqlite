import type { ChangesetExecutor } from "./changeset-apply";

/** @internal Keep each public surface's errors while sharing ownership rules. */
export interface ChangesetScopeErrors {
  closed(): Error;
  invalidChild(): Error;
  busy(): Error;
  uncertain(cause: unknown, cleanupErrors: readonly unknown[]): Error;
}

/**
 * One callback tree over an existing SQL owner, not a transaction implementation.
 * Reserve before invoking adapters, join all admitted work, and distinguish an
 * exact callback rejection after confirmed rollback from uncertain settlement.
 * No SQL boundary, pragma, retry, queue, or global writer lock is added here.
 */
export async function runChangesetScope<T>(
  tx: ChangesetExecutor,
  work: (tx: ChangesetExecutor) => T | Promise<T>,
  checkpoint: () => void,
  errors: ChangesetScopeErrors,
): Promise<T> {
  // Share the first unresolved cleanup obligation across ALL descendants.
  // Catching an error or rolling back an ancestor cannot erase this evidence.
  let uncertain: { error: unknown } | undefined;
  const stateError = (cause: unknown): Error =>
    errors.uncertain(cause, Object.freeze([uncertain!.error]));
  const cleanupComplete = (error: unknown): boolean => {
    if ((typeof error !== "object" && typeof error !== "function") || error === null) return true;
    try {
      const state = error as { cleanupErrors?: unknown; userRecoverable?: unknown };
      const cleanup = state.cleanupErrors;
      return state.userRecoverable !== false &&
        (cleanup === undefined || (Array.isArray(cleanup) && cleanup.length === 0));
    } catch { return false; }
  };
  async function run<U>(
    owner: ChangesetExecutor, callback: (scoped: ChangesetExecutor) => U | Promise<U>,
  ): Promise<U> {
    let accepting = true, childActive = false, failed = false, inFlight = 0;
    let firstFailure: unknown;
    const pending = new Set<Promise<unknown>>();
    const admission = (child: boolean): void => {
      if (!accepting) throw errors.closed();
      if (uncertain !== undefined) throw stateError(uncertain.error);
      if (childActive || (child && inFlight !== 0)) throw errors.busy();
    };
    const submit = <V>(operation: () => Promise<V>, recovered?: () => boolean): Promise<V> => {
      try { admission(recovered !== undefined); }
      catch (error: unknown) { return Promise.reject(error); }
      // pending.add() is necessarily AFTER invocation returns its promise.
      // Adapters can reenter synchronously before then: reserve now so a child
      // cannot absorb a parent write and later silently roll that write back.
      // Do not postpone invocation; adapters must still capture inputs now.
      inFlight++;
      const task = (async () => {
        checkpoint();
        const result = await operation();
        checkpoint();
        return result;
      })();
      pending.add(task);
      const settle = (): void => { pending.delete(task); inFlight--; };
      void task.then(settle, error => {
        settle();
        // Direct SQL failures remain sticky even if caught. A confirmed child
        // rollback restores its state, so its caller may handle the rejection.
        // Retain only the first failure, not an array of repeatedly caught SQL.
        if (recovered?.() !== true && !failed) { failed = true; firstFailure = error; }
      });
      return task;
    };
    const scoped: ChangesetExecutor = {
      execute: (sql, params) => submit(() => owner.execute(sql, params)),
      query: (sql, params) => submit(() => owner.query(sql, params)),
    };
    const nested = owner.transaction;
    if (typeof nested === "function") {
      scoped.transaction = <V>(childWork: (child: ChangesetExecutor) => Promise<V>): Promise<V> => {
        if (typeof childWork !== "function") return Promise.reject(errors.invalidChild());
        let recovered = false;
        return submit(async () => {
          childActive = true;
          let callbackFailed = false, callbackError: unknown;
          try {
            return await nested.call(owner, async (child: ChangesetExecutor) => {
              try { return await run(child, childWork); }
              catch (error: unknown) { callbackFailed = true; callbackError = error; throw error; }
            }) as V;
          } catch (error: unknown) {
            // The owned-child contract returns the EXACT callback rejection
            // only after rollback. Metadata may become stricter as it settles.
            recovered = uncertain === undefined && callbackFailed &&
              Object.is(error, callbackError) && cleanupComplete(error);
            if (!recovered && uncertain === undefined) uncertain = { error };
            throw error;
          } finally { childActive = false; }
        }, () => recovered);
      };
    }
    Object.freeze(scoped);
    let value!: U;
    let callbackFailed = false, callbackError: unknown;
    try { value = await callback(scoped); }
    catch (error: unknown) { callbackFailed = true; callbackError = error; }
    finally {
      accepting = false;
      // Join full child settlement, including unawaited descendants, BEFORE
      // callers collect rows, restore pragmas, commit, or roll back this owner.
      await Promise.allSettled(pending);
    }
    if (uncertain !== undefined) throw stateError(callbackFailed ? callbackError : uncertain.error);
    if (callbackFailed) throw callbackError;
    if (failed) throw firstFailure;
    checkpoint();
    return value;
  }
  return run(tx, work);
}
