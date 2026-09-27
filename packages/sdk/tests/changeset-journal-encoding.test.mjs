import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { SqliteTarget } from './helpers/production-sqlite-target.mjs';
import { ChangesetRebaseJournal } from '../src/changeset-rebase-journal.ts';
import { decodeChangeset, decodeRebaseInfo } from '../src/changeset-codec.ts';

// Execute current capture/apply/journal/codec/rebase modules. This adapter only
// supplies reference SQLite SQL ownership, never journal or conflict decisions.
const encodings = ['UTF-8', 'UTF-16le', 'UTF-16be'];
const schema = 'CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);';
const initial = "INSERT INTO t VALUES(1,'old-a','old-b');";
const journalId = 'journal:' + '\u0001'.repeat(504);
const operationId = 'local:' + '😀'.repeat(126);
const remoteId = 'remote:' + 'x'.repeat(505);
function target(enc = 'UTF-8', path = ':memory:') {
  return new SqliteTarget(path, `PRAGMA encoding='${enc}';${schema}${initial}`);
}
function remoteBytes(sql = "UPDATE t SET a='remote-a' WHERE id=1") {
  const db = new DatabaseSync(':memory:'); db.exec(schema + initial);
  const session = db.createSession();
  try { db.exec(sql); return new Uint8Array(session.changeset()); }
  finally { session.close(); db.close(); }
}
const rows = t => t.rows('SELECT id,hex(CAST(a AS BLOB)),hex(CAST(b AS BLOB)) FROM t');
const journal = t => new ChangesetRebaseJournal(t, { journalId });

for (const enc of encodings) {
  test(`${enc}: retain, reopen and rebase ORIGINAL local work over remote conflicts`, async () => {
    const directory = mkdtempSync(join(tmpdir(), 'journal-encoding-'));
    const path = join(directory, 'local.db');
    let t = target(enc, path);
    let j = journal(t), calls = 0;
    const native = t.db.createSession({ table: 't' });
    let record;
    try {
      const basis = await j.bookmark();
      record = (await j.captureLocal(operationId, async tx => {
        calls++; await tx.execute('UPDATE t SET a=? WHERE id=1', ['\uFEFFlocal\0😀']); return 42;
      }, { tables: ['t'] })).record;
      assert.deepEqual(decodeChangeset(record.changeset), decodeChangeset(new Uint8Array(native.changeset())));
      assert.deepEqual(record.basis, basis);
    } finally { native.close(); t.close(); }
    t = new SqliteTarget(path); j = journal(t);
    const peer = target(enc);
    try {
      assert.deepEqual(await j.readLocal(operationId), record);
      const bytes = remoteBytes();
      const applied = await j.apply(bytes, { deliveryId: remoteId, tables: ['t'], onConflict: () => 'omit' });
      assert.equal(applied.omitted, 1); assert.equal(applied.entry.position, 1);
      assert.equal(decodeRebaseInfo(applied.entry.rebaseInfo)[0].changes.length, 1);
      const bookmark = await j.bookmark();
      const replay = await j.captureLocal(operationId, () => { calls++; throw Error('must not repeat'); }, { tables: ['t'] });
      assert.equal(replay.replayed, true); assert.equal(calls, 1);
      assert.deepEqual(replay.record, record);
      const rebased = await j.rebaseLocal(operationId, { through: bookmark });
      assert.deepEqual(rebased.afterBookmark, record.basis);
      assert.deepEqual(rebased.throughBookmark, bookmark);
      assert.deepEqual((await j.read(remoteId)), applied.entry);
      assert.equal((await j.apply(bytes, { deliveryId: remoteId, tables: ['t'], onConflict: () => { throw Error('replay'); } })).replayed, true);
      // Independent native application starts with the remote change, then
      // accepts our rebased local modification without conflict resolution.
      assert.equal(peer.db.applyChangeset(bytes), true);
      assert.equal(peer.db.applyChangeset(rebased.changeset), true);
      assert.equal(peer.rows('SELECT hex(CAST(a AS BLOB)) FROM t')[0][0], rows(t)[0][1]);
      assert.deepEqual(rows(peer), rows(t));
      assert.deepEqual((await j.readLocal(operationId)).changeset, record.changeset);
    } finally { peer.close(); t.close(); }
  });
  test(`${enc}: maximum delivery identities retain exact empty decisions and bookmarks`, async () => {
    const t = target(enc), j = journal(t);
    try {
      for (const id of [remoteId, '😀'.repeat(128), '\u0001'.repeat(512), '\uFEFF' + 'y'.repeat(509)]) {
        const result = await j.apply(new Uint8Array(), { deliveryId: id, tables: [] });
        assert.equal(result.entry.deliveryId, id);
        assert.equal(result.entry.rebaseInfo.length, 0);
        assert.deepEqual(await j.read(id), result.entry);
      }
      assert.equal((await j.bookmark()).position, 4);
      assert.equal((await j.head()).position, 4);
    } finally { t.close(); }
  });
  test(`${enc}: zero-change local operations remain exact replays after a lost commit response`, async () => {
    const t = target(enc), j = journal(t);
    let calls = 0;
    try {
      t.afterCommit = async () => { t.afterCommit = null; throw Error('lost commit response'); };
      await assert.rejects(j.captureLocal('empty', () => { calls++; }, { tables: ['t'] }), /lost commit/);
      const recovered = await j.captureLocal('empty', () => { calls++; }, { tables: ['t'] });
      assert.equal(recovered.replayed, true); assert.equal(calls, 1);
      assert.equal(recovered.record.changeset.length, 0);
      assert.equal((await j.rebaseLocal('empty')).changeset.length, 0);
    } finally { t.close(); }
  });
}

const localHashes = ['scope_sha256', 'basis_sha256', 'sha256', 'record_sha256'];
for (const enc of encodings) for (const column of localHashes) {
  test(`${enc}: corrupt ${column} is rejected before returning stored text`, async () => {
    const t = target(enc), j = journal(t);
    try {
      await j.captureLocal('local', tx => tx.execute("UPDATE t SET a='new'"), { tables: ['t'] });
      await t.execute(`UPDATE __fsqlite_rebase_journal_locals SET ${column}=${column}||char(0)||?`, ['x'.repeat(4096)]);
      let projected = 'not read';
      t.after = async (kind, sql, _params, result) => {
        if (kind === 'query' && sql.includes('FROM main."__fsqlite_rebase_journal_locals"') && sql.includes('scope_sha256'))
          projected = result.rowArrays[0][localHashes.indexOf(column)];
      };
      await assert.rejects(j.readLocal('local'), { code: 'ERR_FSQLITE_REBASE_JOURNAL_CORRUPT' });
      assert.equal(projected, null);
      await assert.rejects(j.captureLocal('local', () => { throw Error('callback must not run'); }, { tables: ['t'] }), e => !e.message.includes('callback'));
    } finally { t.close(); }
  });
}
for (const enc of encodings) for (const column of ['message_sha256', 'sha256']) {
  test(`${enc}: reject NUL-hidden remote ${column} without exporting its tail`, async () => {
    const t = target(enc), j = journal(t);
    try {
      await j.apply(remoteBytes(), { deliveryId: 'remote', tables: ['t'] });
      await t.execute(`UPDATE __fsqlite_rebase_journal_entries SET ${column}=${column}||char(0)||?`, ['tail'.repeat(2048)]);
      let projected = 'not read';
      t.after = async (kind, sql, _params, result) => {
        if (kind === 'query' && sql.includes('SELECT position,') && sql.includes('rebase_info'))
          projected = result.rowArrays[0][column === 'message_sha256' ? 2 : 4];
      };
      await assert.rejects(j.read('remote'), { code: 'ERR_FSQLITE_REBASE_JOURNAL_CORRUPT' });
      assert.equal(projected, null);
      await assert.rejects(j.bookmark());
    } finally { t.close(); }
  });
}
for (const enc of encodings) {
  test(`${enc}: cross-check receipt hashes without NUL-hidden text`, async () => {
    const t = target(enc), j = journal(t);
    try {
      await j.apply(remoteBytes(), { deliveryId: 'remote', tables: ['t'] });
      await t.execute("UPDATE __fsqlite_changeset_receipts SET sha256=sha256||char(0)||?", ['x'.repeat(4096)]);
      let projected = 'not read';
      t.after = async (kind, sql, _params, result) => {
        if (kind === 'query' && sql.includes('FROM main."__fsqlite_changeset_receipts"') && sql.includes('CASE')) projected = result.rowArrays[0][0];
      };
      await assert.rejects(j.bookmark(), { code: 'ERR_FSQLITE_REBASE_JOURNAL_CORRUPT' });
      assert.equal(projected, null);
    } finally { t.close(); }
  });
}

test('bookmarks and original-record digests are independent of database text encoding', async () => {
  const results = [];
  for (const enc of encodings) {
    const t = target(enc), j = journal(t);
    try {
      const local = await j.captureLocal(operationId, tx => tx.execute("UPDATE t SET a='local'"), { tables: ['t'] });
      await j.apply(remoteBytes(), { deliveryId: remoteId, tables: ['t'], onConflict: () => 'replace' });
      results.push([local.record.recordSha256, await j.bookmark(), [...(await j.rebaseLocal(operationId)).changeset]]);
    } finally { t.close(); }
  }
  assert.deepEqual(results[0], results[1]); assert.deepEqual(results[1], results[2]);
});
