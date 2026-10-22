// Actual OPFS store and shared snapshot validator; filesystem and Web Locks
// are deterministic models. Not browser, SQL rollback or power-loss proof.
import assert from "node:assert/strict";
import { register } from "node:module";
import { test } from "node:test";
import { setImmediate as turn } from "node:timers/promises";
import { databaseImage, deferred, installOpfsModel } from "./helpers/opfs-model.mjs";

register(`data:text/javascript,${encodeURIComponent(`
  export async function resolve(specifier, context, next) {
    try { return await next(specifier, context); }
    catch (error) {
      if (error.code === "ERR_MODULE_NOT_FOUND" && specifier.startsWith(".") && !/\\.[^/]+$/.test(specifier))
        return next(specifier + ".ts", context);
      throw error;
    }
  }
`)}`);
const { OpfsSnapshotStore } = await import("../src/opfs-snapshot-store.ts");
const lost = { code: "ERR_FSQLITE_SNAPSHOT_LOCK_LOST", transient: false, userRecoverable: false };

// A stolen lock releases its queue and rejects request(), but does not cancel
// the old callback. Track BOTH promises so tests drain even the unfixed code.
function fixture(t) {
  const m = installOpfsModel();
  t.after(() => m.restore());
  const tails = new Map(), calls = [];
  m.locks.request = (name, options, callback) => {
    assert.deepEqual(options, { mode: "exclusive" });
    const release = deferred(), entered = deferred(), done = deferred();
    let resolve, reject;
    const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
    const call = { name, entered, done, active: false,
      lose(error = new DOMException("Lock stolen", "AbortError")) {
        assert.equal(call.active, true);
        call.active = false;
        reject(error);
        release.resolve();
      },
      finishEarly() { call.active = false; resolve(undefined); release.resolve(); },
    };
    calls.push(call);
    const predecessor = tails.get(name) ?? Promise.resolve();
    tails.set(name, release.promise);
    void predecessor.then(async () => {
      call.active = true;
      entered.resolve();
      try { resolve(await callback({ name, mode: "exclusive" })); }
      catch (error) { reject(error); }
      finally { call.active = false; release.resolve(); done.resolve(); }
    });
    return promise;
  };
  return Object.assign(m, { calls, active: () => calls.findLast(call => call.active) });
}

function observe(promise) {
  const state = { settled: false, outcome: undefined };
  state.done = promise.then(
    value => { state.settled = true; state.outcome = { value }; },
    error => { state.settled = true; state.outcome = { error }; },
  );
  return state;
}

for (const phase of ["create", "write"]) {
  test(`loss during ${phase} joins the callback and prevents further publication`, async t => {
    const m = fixture(t), store = await OpfsSnapshotStore.open(`join-${phase}`);
    const previous = await store.save(databaseImage(1), null);
    const original = [...m.files.values()][0].slice();
    const entered = deferred(), release = deferred();
    m.hooks[phase] = async () => { delete m.hooks[phase]; entered.resolve(); await release.promise; };
    const saving = observe(store.save(databaseImage(2), previous.revision));
    await entered.promise;
    const owner = m.active();
    owner.lose();
    await turn();
    const settledBeforeDrain = saving.settled;
    release.resolve();
    await owner.done.promise;
    await saving.done;
    assert.equal(settledBeforeDrain, false, "request rejection must not detach active I/O");
    assert.equal(m.counts.publications, 1);
    assert.deepEqual([...m.files.values()][0], original);
    assert.equal(m.counts.aborts, 1);
    assert.ok(saving.outcome.error);
    for (const [key, value] of Object.entries(lost)) assert.equal(saving.outcome.error[key], value);
    const calls = m.calls.length;
    await assert.rejects(store.load(), lost);
    await assert.rejects(store.save(databaseImage(3), previous.revision), lost);
    assert.equal(m.calls.length, calls, "a lost store cannot request fresh authority");
  });
}

test("a stolen writer cannot overwrite the next owner's committed head", async t => {
  const m = fixture(t), a = await OpfsSnapshotStore.open("winner"), b = await OpfsSnapshotStore.open("winner");
  const first = await a.save(databaseImage(1), null);
  const entered = deferred(), release = deferred();
  m.hooks.write = async () => { delete m.hooks.write; entered.resolve(); await release.promise; };
  const stale = observe(a.save(databaseImage(2), first.revision));
  await entered.promise;
  const owner = m.active(); owner.lose();
  await turn();
  const winner = await b.save(databaseImage(3), first.revision);
  release.resolve();
  await owner.done.promise; await stale.done;
  assert.equal((await b.load()).revision, winner.revision);
  assert.equal((await b.load()).bytes[100], 3);
  assert.equal(m.counts.publications, 2);
  assert.equal(m.counts.removals, 0);
  assert.equal(stale.outcome.error.code, lost.code);
});

test("loss during close joins an uncertain publication without reporting rollback", async t => {
  const m = fixture(t), store = await OpfsSnapshotStore.open("committed");
  const previous = await store.save(databaseImage(1), null), revision = crypto.randomUUID();
  const entered = deferred(), release = deferred();
  m.hooks.beforeClose = async () => { delete m.hooks.beforeClose; entered.resolve(); await release.promise; };
  const saving = observe(store.save(databaseImage(2), previous.revision, revision));
  await entered.promise;
  const owner = m.active(); owner.lose(); await turn();
  const early = saving.settled;
  release.resolve(); await owner.done.promise; await saving.done;
  assert.equal(early, false);
  assert.equal(saving.outcome.error.code, lost.code);
  assert.equal(m.counts.publications, 2, "already-dispatched close is not undone");
  assert.equal(m.counts.removals, 0);
  const fresh = await OpfsSnapshotStore.open("committed");
  assert.equal((await fresh.confirmPublication(revision, previous.revision)).revision, revision);
  assert.equal(m.counts.publications, 2, "confirmation never republishes");
});

test("loss during failed-write cleanup joins abort and forbids empty-head deletion", async t => {
  const m = fixture(t), store = await OpfsSnapshotStore.open("cleanup");
  const failure = new Error("write failed"), entered = deferred(), release = deferred();
  m.hooks.write = () => { throw failure; };
  m.hooks.abort = async () => { entered.resolve(); await release.promise; };
  const saving = observe(store.save(databaseImage(1), null));
  await entered.promise;
  const owner = m.active(); owner.lose(); await turn();
  const early = saving.settled;
  release.resolve(); await owner.done.promise; await saving.done;
  assert.equal(early, false);
  assert.equal(m.counts.removals, 0, "deletion requires still-held publication authority");
  assert.equal(m.counts.aborts, 1);
  assert.equal(saving.outcome.error.code, lost.code);
  assert.ok(saving.outcome.error.cause instanceof AggregateError);
  assert.ok(saving.outcome.error.cause.errors.includes(failure), "preserve the operation failure too");
});

test("premature successful request settlement cannot acknowledge active publication", async t => {
  const m = fixture(t), store = await OpfsSnapshotStore.open("early-success");
  const entered = deferred(), release = deferred();
  m.hooks.write = async () => { delete m.hooks.write; entered.resolve(); await release.promise; };
  const saving = observe(store.save(databaseImage(1), null));
  await entered.promise;
  const owner = m.active(); owner.finishEarly(); await turn();
  const early = saving.settled;
  release.resolve(); await owner.done.promise; await saving.done;
  assert.equal(early, false);
  assert.equal(saving.outcome.error.code, lost.code);
  assert.equal(m.counts.publications, 0);
  assert.equal(m.counts.removals, 0);
});

test("queued same-store work is fenced after a lost grant", async t => {
  const m = fixture(t), store = await OpfsSnapshotStore.open("queued");
  const previous = await store.save(databaseImage(1), null);
  const entered = deferred(), release = deferred();
  m.hooks.write = async () => { delete m.hooks.write; entered.resolve(); await release.promise; };
  const saving = observe(store.save(databaseImage(2), previous.revision));
  await entered.promise;
  const owner = m.active(), reading = observe(store.load());
  owner.lose(); await turn();
  release.resolve(); await owner.done.promise; await saving.done; await reading.done;
  assert.equal(reading.outcome.error?.code, lost.code);
  assert.equal(m.counts.publications, 1);
});

test("pre-grant errors remain recoverable and late callbacks get no I/O authority", async t => {
  const m = fixture(t), store = await OpfsSnapshotStore.open("admission");
  const request = m.locks.request, denied = new Error("admission refused");
  let late;
  m.locks.request = (_name, _options, callback) => { late = callback; return Promise.reject(denied); };
  await assert.rejects(store.load(), error => error === denied);
  const calls = m.counts.publications;
  await assert.rejects(late({ name: "late", mode: "exclusive" }));
  assert.equal(m.counts.publications, calls);
  m.locks.request = request;
  // A genuinely refused request did not poison the store. The malicious late
  // callback is fenced locally, not permission to do work after settlement.
  const saved = await store.save(databaseImage(4), null);
  assert.equal((await store.load()).revision, saved.revision);
});

test("ordinary storage failures preserve identity and allow a real retry", async t => {
  const m = fixture(t), store = await OpfsSnapshotStore.open("retry");
  const previous = await store.save(databaseImage(1), null);
  const failure = new DOMException("quota", "QuotaExceededError");
  m.hooks.write = () => { throw failure; };
  await assert.rejects(store.save(databaseImage(2), previous.revision), error => error === failure);
  delete m.hooks.write;
  const saved = await store.save(databaseImage(3), previous.revision);
  assert.equal((await store.load()).revision, saved.revision);
});
