import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SqliteTarget } from './helpers/production-sqlite-target.mjs';
import { ChangesetReceiver, ChangesetDeliveryPump, CHANGESET_DELIVERY_PROTOCOL } from '../src/changeset-delivery.ts';
import { ChangesetRebaseJournal } from '../src/changeset-rebase-journal.ts';
import { decodeChangeset, encodeChangeset } from '../src/changeset-codec.ts';
import { ChangesetOutbox } from '../src/changeset-outbox.ts';
import { ChangesetFanout } from '../src/changeset-fanout.ts';

// The journal, inbox, policy scope and receiver are production modules.
// Native Session produces input; the adapter supplies only SQL ownership.
const generated = 'CREATE TABLE t(id INTEGER PRIMARY KEY, amount INTEGER, total INTEGER GENERATED ALWAYS AS(amount*2) STORED CHECK(total>=0));';
const cyclic = `CREATE TABLE parent(id INTEGER PRIMARY KEY, child_id INTEGER REFERENCES child(id), label TEXT,
 folded TEXT GENERATED ALWAYS AS(lower(label)) STORED UNIQUE);
 CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), amount INTEGER,
 total INTEGER GENERATED ALWAYS AS(amount*2) VIRTUAL CHECK(total>=0));`;
const policy = { generatedColumns: 'recompute', foreignKeys: 'defer' };
const encodings = ['UTF-8', 'UTF-16le', 'UTF-16be'];
function native(ddl, sql, order) {
  const db = new DatabaseSync(':memory:'); db.exec('PRAGMA foreign_keys=ON;' + ddl);
  const session = db.createSession();
  try {
    db.exec('BEGIN; PRAGMA defer_foreign_keys=ON;' + sql + ';COMMIT');
    const bytes = new Uint8Array(session.changeset());
    return order ? encodeChangeset([...decodeChangeset(bytes)].sort((a,b) => order.indexOf(a.name)-order.indexOf(b.name))) : bytes;
  } finally { session.close(); db.close(); }
}
const simple = native(generated, 'INSERT INTO t(id,amount) VALUES(1,3)');
const cycle = native(cyclic, "INSERT INTO child(id,parent_id,amount) VALUES(10,1,7); INSERT INTO parent(id,child_id,label) VALUES(1,10,'Parent')", ['child','parent']);
async function envelope(bytes, deliveryId = 'source:one', extras = {}) {
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
  return { protocol: CHANGESET_DELIVERY_PROTOCOL, receiverId: 'replica', deliveryId,
    sha256: Buffer.from(digest).toString('hex'), changeset: bytes, ...extras };
}
function options(journaled, extra = {}) {
  return { receiverId: 'replica', tables: ['t'], confirmCommit: async () => {},
    ...(journaled ? { rebaseJournal: { journalId: 'local-history' } } : {}), ...extra };
}
function storedRows(t, table) {
  if (!t.rows('SELECT 1 FROM sqlite_schema WHERE name=?', [table]).length) return 0;
  return Number(t.rows(`SELECT count(*) FROM "${table}"`)[0][0]);
}
function assertEmpty(t) {
  assert.equal(storedRows(t, '__fsqlite_changeset_receipts'), 0);
  assert.equal(storedRows(t, '__fsqlite_rebase_journal_entries'), 0);
  assert.equal(storedRows(t, '__fsqlite_rebase_journal_heads'), 0);
}

for (const enc of encodings) for (const journaled of [false, true]) {
  test(`${enc}/${journaled}: cyclic generated schemas through receiver and exact replay`, async () => {
    const t = new SqliteTarget(':memory:', `PRAGMA encoding='${enc}';` + cyclic);
    let confirmations = 0;
    const receiver = new ChangesetReceiver(t, options(journaled, { ...policy, tables: ['child','parent'], confirmCommit: async () => { confirmations++; assert.equal(t.active, false); } }));
    try {
      const message = await envelope(cycle);
      assert.deepEqual(decodeChangeset(cycle).map(x => x.name), ['child','parent']);
      const first = await receiver.receive(message);
      assert.equal(first.applied, 2); assert.equal(first.confirmed, true);
      assert.equal(first.replayed, false);
      assert.deepEqual(t.rows('SELECT id,parent_id,amount,total FROM child'), [[10n,1n,7n,14n]]);
      assert.deepEqual(t.rows('SELECT id,child_id,label,folded FROM parent'), [[1n,10n,'Parent','parent']]);
      assert.deepEqual(t.rows('PRAGMA foreign_key_check'), []);
      assert.deepEqual(t.rows('PRAGMA defer_foreign_keys'), [[0n]]);
      const second = await receiver.receive(message);
      assert.equal(second.replayed, true); assert.equal(confirmations, 2);
      assert.equal(storedRows(t, '__fsqlite_changeset_receipts'), 1);
      if (journaled) {
        assert.equal((await receiver.rebaseJournal.head()).position, 1);
        assert.equal((await receiver.rebaseJournal.read(message.deliveryId)).messageSha256, message.sha256);
      }
    } finally { t.close(); }
  });
}
for (const enc of encodings) {
  test(`${enc}: journal applies a generated-column conflict and rebases its retained original`, async () => {
    const t = new SqliteTarget(':memory:', `PRAGMA encoding='${enc}';${generated} INSERT INTO t(id,amount) VALUES(1,2);`);
    const j = new ChangesetRebaseJournal(t, { journalId: 'j' });
    try {
      const local = await j.captureLocal('local', tx => tx.execute('UPDATE t SET amount=9 WHERE id=1'), { tables: ['t'], generatedColumns: 'recompute' });
      const wire = native(generated, 'INSERT INTO t(id,amount) VALUES(1,3)');
      const result = await j.apply(wire, { tables: ['t'], deliveryId: 'remote', generatedColumns: 'recompute', onConflict: () => 'replace' });
      assert.equal(result.applied, 1);
      assert.deepEqual(t.rows('SELECT * FROM t'), [[1n,3n,6n]]);
      assert.ok(result.entry.rebaseInfo.length > 0);
      assert.deepEqual((await j.readLocal('local')).changeset, local.record.changeset);
      assert.equal((await j.rebaseLocal('local')).changeset.length, 0);
    } finally { t.close(); }
  });
}
for (const journaled of [false, true]) {
  test(`${journaled}: policy is captured at construction, never enrolled by wire fields`, async () => {
    const t = new SqliteTarget(':memory:', generated);
    const selected = options(journaled, { ...policy });
    const receiver = new ChangesetReceiver(t, selected);
    selected.generatedColumns = 'wrong'; selected.foreignKeys = 'wrong';
    try {
      assert.equal((await receiver.receive(await envelope(simple))).applied, 1);
      const plain = new SqliteTarget(':memory:', generated);
      try {
        const strict = new ChangesetReceiver(plain, options(journaled));
        await assert.rejects(strict.receive(await envelope(simple, 'unauthorized-policy', policy)));
        assert.deepEqual(plain.rows('SELECT * FROM t'), []); assertEmpty(plain);
      } finally { plain.close(); }
    } finally { t.close(); }
  });
  test(`${journaled}: replay checks current FK cleanliness before another confirmation`, async () => {
    const t = new SqliteTarget(':memory:', cyclic);
    let confirmed = 0;
    const receiver = new ChangesetReceiver(t, options(journaled, { ...policy, tables: ['parent','child'], confirmCommit: async () => { confirmed++; } }));
    try {
      const message = await envelope(cycle);
      await receiver.receive(message);
      t.db.exec('PRAGMA foreign_keys=OFF; UPDATE child SET parent_id=99; PRAGMA foreign_keys=ON');
      await assert.rejects(receiver.receive(message));
      assert.equal(confirmed, 1);
      assert.equal(storedRows(t, '__fsqlite_changeset_receipts'), 1);
      assert.deepEqual(t.rows('SELECT parent_id FROM child'), [[99n]]);
    } finally { t.close(); }
  });
  test(`${journaled}: failed storage confirmation reopens and retries the original decision`, async () => {
    const directory = mkdtempSync(join(tmpdir(), 'receiver-policy-'));
    const path = join(directory, 'db');
    let t = new SqliteTarget(path, generated), confirmations = 0;
    const settings = options(journaled, { ...policy, confirmCommit: async () => { confirmations++; if (confirmations === 1) throw Error('publication failed'); } });
    const message = await envelope(simple);
    try {
      await assert.rejects(new ChangesetReceiver(t, settings).receive(message));
      assert.deepEqual(t.rows('SELECT * FROM t'), [[1n,3n,6n]]);
    } finally { t.close(); }
    t = new SqliteTarget(path);
    try {
      assert.equal((await new ChangesetReceiver(t, settings).receive(message)).replayed, true);
      assert.equal(confirmations, 2); assert.equal(storedRows(t, '__fsqlite_changeset_receipts'), 1);
      if (journaled) assert.equal(storedRows(t, '__fsqlite_rebase_journal_entries'), 1);
    } finally { t.close(); }
  });
  test(`${journaled}: generated constraint failures roll back rows, receipt and journal`, async () => {
    const t = new SqliteTarget(':memory:', generated);
    let confirms = 0;
    // Native source lacks the destination constraint; wire fields are writable.
    const bad = native('CREATE TABLE t(id INTEGER PRIMARY KEY, amount INTEGER)', 'INSERT INTO t VALUES(1,3),(2,-1)');
    const receiver = new ChangesetReceiver(t, options(journaled, { ...policy, confirmCommit: async () => { confirms++; } }));
    try {
      await assert.rejects(receiver.receive(await envelope(bad)));
      assert.deepEqual(t.rows('SELECT * FROM t'), []); assertEmpty(t); assert.equal(confirms, 0);
    } finally { t.close(); }
  });
  test(`${journaled}: unresolved references abort at the whole transaction boundary`, async () => {
    const t = new SqliteTarget(':memory:', cyclic);
    const receiver = new ChangesetReceiver(t, options(journaled, { ...policy, tables: ['child','parent'] }));
    const invalid = encodeChangeset(decodeChangeset(cycle).filter(x => x.name === 'child'));
    try {
      await assert.rejects(receiver.receive(await envelope(invalid)));
      assert.equal(storedRows(t, 'child'), 0); assertEmpty(t);
      assert.deepEqual(t.rows('PRAGMA foreign_keys'), [[1n]]);
      assert.deepEqual(t.rows('PRAGMA defer_foreign_keys'), [[0n]]);
    } finally { t.close(); }
  });
  test(`${journaled}: foreign-key policy requires explicitly enabled enforcement`, async () => {
    const t = new SqliteTarget(':memory:', generated); t.db.exec('PRAGMA foreign_keys=OFF');
    try {
      const receiver = new ChangesetReceiver(t, options(journaled, policy));
      await assert.rejects(receiver.receive(await envelope(simple)));
      assert.equal(storedRows(t, 't'), 0); assertEmpty(t);
      assert.deepEqual(t.rows('PRAGMA foreign_keys'), [[0n]]);
    } finally { t.close(); }
  });
}
for (const key of ['generatedColumns','foreignKeys']) for (const value of [null, false, '', 'unknown']) {
  test(`${key}=${JSON.stringify(value)} rejects before SQL for journal and receiver`, async () => {
    let starts = 0;
    const owner = { transaction: async () => { starts++; throw Error('SQL should not start'); } };
    assert.throws(() => new ChangesetReceiver(owner, options(false, { [key]: value })), { code: 'ERR_FSQLITE_DELIVERY_INPUT' });
    const j = new ChangesetRebaseJournal(owner, { journalId: 'j' });
    await assert.rejects(j.apply(new Uint8Array(), { deliveryId: 'id', tables: [], [key]: value }), e => !e.message.includes('SQL should not start'));
    assert.equal(starts, 0);
  });
}

test('journal captures policies before asynchronous transaction admission', async () => {
  const t = new SqliteTarget(':memory:', cyclic);
  const selected = { ...policy, tables: ['child','parent'], deliveryId: 'id' };
  const j = new ChangesetRebaseJournal({ transaction: (work, controls) => {
    selected.foreignKeys = 'invalid'; selected.generatedColumns = 'invalid'; return t.transaction(work, controls);
  } }, { journalId: 'j' });
  try { assert.equal((await j.apply(cycle, selected)).applied, 2); }
  finally { t.close(); }
});

test('journal capacity rejection cannot publish part of a deferred transaction', async () => {
  const t = new SqliteTarget(':memory:', cyclic);
  const j = new ChangesetRebaseJournal(t, { journalId: 'j', maxEntries: 1 });
  try {
    await j.apply(new Uint8Array(), { deliveryId: 'empty', tables: [] });
    await assert.rejects(j.apply(cycle, { deliveryId: 'next', tables: ['child','parent'], ...policy }), { code: 'ERR_FSQLITE_REBASE_JOURNAL_LIMIT' });
    assert.equal(storedRows(t, 'child'), 0); assert.equal(storedRows(t, 'parent'), 0);
    assert.equal((await j.head()).position, 1); assert.equal(storedRows(t, '__fsqlite_changeset_receipts'), 1);
  } finally { t.close(); }
});


for (const enc of encodings) {
  test(`${enc}: captured source -> fanout pump -> journaled receivers survives lost ACK`, async () => {
    const sourceSchema = cyclic.replaceAll('REFERENCES child(id)', 'REFERENCES child(id) DEFERRABLE INITIALLY DEFERRED')
      .replaceAll('REFERENCES parent(id)', 'REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED');
    const source = new SqliteTarget(':memory:', `PRAGMA encoding='${enc}';` + sourceSchema);
    const east = new SqliteTarget(':memory:', `PRAGMA encoding='${enc}';` + cyclic);
    const west = new SqliteTarget(':memory:', `PRAGMA encoding='${enc}';` + cyclic);
    const receivers = [];
    let sourceCallbacks = 0, dropResponse = true, confirmations = 0;
    try {
      const outbox = new ChangesetOutbox(source);
      const fanout = await ChangesetFanout.open(source, ['east','west']);
      const capture = { tables: ['child','parent'], generatedColumns: 'recompute' };
      const first = await outbox.record(async tx => {
        sourceCallbacks++;
        await tx.execute('INSERT INTO child(id,parent_id,amount) VALUES(10,1,7)');
        await tx.execute("INSERT INTO parent(id,child_id,label) VALUES(1,10,'Parent')");
      }, { ...capture, deliveryId: 'source:cycle' });
      await outbox.record(tx => { sourceCallbacks++; return tx.execute('UPDATE child SET amount=11 WHERE id=10'); },
        { ...capture, deliveryId: 'source:update' });
      const confirmSource = async () => { assert.equal(source.active, false); };
      for (const [id, target] of [['east',east], ['west',west]]) {
        receivers.push(new ChangesetReceiver(target, options(true, {
          ...policy, receiverId: id, tables: ['child','parent'],
          confirmCommit: async () => { confirmations++; assert.equal(target.active, false); },
        })));
      }
      const eastPump = new ChangesetDeliveryPump(fanout.forReplica('east'), {
        receiverId: 'east', confirmSource,
        deliver: async (message, controls) => {
          const result = await receivers[0].receive(message, controls);
          if (dropResponse) { dropResponse = false; throw Error('lost installation response'); }
          return result;
        },
      });
      await assert.rejects(eastPump.run(), { code: 'ERR_FSQLITE_DELIVERY_FAILED' });
      assert.equal((await fanout.progress()).acknowledgedThrough, 0n);
      assert.equal((await fanout.forReplica('east').pending()).length, 2);
      assert.equal((await receivers[0].rebaseJournal.head()).position, 1);
      const retried = await eastPump.run();
      assert.equal(retried.deliveries, 2); assert.equal(retried.replays, 1);
      assert.ok((await outbox.read(first.delivery.deliveryId)).changeset.length > 0);
      // A second receiver with no local opt-in must not accept a source policy
      // or advance its cursor. The corrected provisioned receiver then succeeds.
      const strict = new ChangesetReceiver(west, options(true, { receiverId: 'west', tables: ['child','parent'] }));
      const strictPump = new ChangesetDeliveryPump(fanout.forReplica('west'), {
        receiverId: 'west', confirmSource, deliver: (message, controls) => strict.receive(message, controls),
      });
      await assert.rejects(strictPump.run()); assertEmpty(west);
      assert.equal((await fanout.progress()).acknowledgedThrough, 0n);
      const westPump = new ChangesetDeliveryPump(fanout.forReplica('west'), {
        receiverId: 'west', confirmSource, deliver: (message, controls) => receivers[1].receive(message, controls),
      });
      assert.equal((await westPump.run()).deliveries, 2);
      assert.equal((await fanout.progress()).acknowledgedThrough, 2n);
      assert.equal((await outbox.read(first.delivery.deliveryId)).changeset, null);
      assert.equal(sourceCallbacks, 2); assert.equal(confirmations, 5);
      for (const receiver of receivers) assert.equal((await receiver.rebaseJournal.head()).position, 2);
      for (const t of [east,west]) {
        assert.deepEqual(t.rows('SELECT * FROM parent'), source.rows('SELECT * FROM parent'));
        assert.deepEqual(t.rows('SELECT * FROM child'), source.rows('SELECT * FROM child'));
        assert.deepEqual(t.rows('PRAGMA foreign_key_check'), []);
        assert.deepEqual(t.rows('PRAGMA defer_foreign_keys'), [[0n]]);
      }
    } finally { west.close(); east.close(); source.close(); }
  });
}
