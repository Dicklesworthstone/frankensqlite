// Explicit reference boundary doubles for the transfer orchestrator. SQL state
// and Session wire bytes are real Node SQLite; the functions exported under
// production source-API names are CONTRACT FIXTURES, not their implementations.
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
export const protocol = 'fsqlite-bootstrap-v1';
export const TABLE = 'seed';
const sum = bytes => createHash('sha256').update(bytes).digest('hex');
const jsonHash = value => sum(Buffer.from(JSON.stringify(value)));
export const chunkId = (id, index) => index === 0 ? id : `${id}/chunk/${index}`;
export const operation = (name, controls) => {
  controls?.signal?.throwIfAborted();
  if (controls?.timeoutMs !== undefined) assert.ok(controls.timeoutMs >= 1);
};
export class Source {
  constructor({ count = 3, filename = ':memory:', initialize = true, payloadSize = 0 } = {}) {
    this.db = new DatabaseSync(filename); this.depth = 0; this.events = []; this.hooks = {};
    this.loads = 0; this.manifestReads = 0; this.acks = []; this.confirmations = 0;
    if (initialize) {
      this.db.exec(`PRAGMA journal_mode=WAL; CREATE TABLE seed(seq INTEGER PRIMARY KEY,
        id TEXT UNIQUE, sha TEXT, bytes INTEGER, changes INTEGER, payload BLOB, ack INTEGER DEFAULT 0);
        CREATE TABLE replica(id TEXT PRIMARY KEY, head INTEGER); INSERT INTO replica VALUES('east',0),('west',0)`);
      const sessionSource = new DatabaseSync(':memory:');
      sessionSource.exec('CREATE TABLE t(id INTEGER PRIMARY KEY, value);');
      for (let i = 0; i < count; i++) {
        const session = sessionSource.createSession();
        sessionSource.prepare('INSERT INTO t VALUES(?,?)').run(i + 1, payloadSize ? Buffer.alloc(payloadSize, i % 256) : `row-${i}`);
        const bytes = new Uint8Array(session.changeset()); session.close();
        this.db.prepare('INSERT INTO seed(seq,id,sha,bytes,changes,payload) VALUES(?,?,?,?,?,?)')
          .run(i + 1, chunkId('source:seed', i), sum(bytes), bytes.length, 1, bytes);
      }
      sessionSource.close();
      if (count === 0) this.db.prepare('INSERT INTO seed(seq,id,sha,bytes,changes,payload) VALUES(1,?,?,?,?,?)')
        .run('source:seed', sum(new Uint8Array()), 0, 0, Buffer.alloc(0));
    }
  }
  async transaction(work, controls) {
    operation('transaction', controls);
    assert.equal(this.depth, 0, 'no source transaction overlap');
    this.depth++; this.events.push('begin'); this.db.exec('BEGIN');
    const tx = {
      query: async (sql, params = []) => {
        await this.hooks.beforeQuery?.(sql, params);
        const statement = this.db.prepare(sql); statement.setReturnArrays(true);
        const rowArrays = statement.all(...params);
        if (sql === 'SELECT seq,id,sha,bytes,changes FROM seed ORDER BY seq') this.manifestReads++;
        if (/SELECT payload/.test(sql)) this.loads++;
        await this.hooks.afterQuery?.(sql, params, rowArrays);
        return { rowArrays };
      },
      execute: async (sql, params = []) => {
        const changes = Number(this.db.prepare(sql).run(...params).changes);
        await this.hooks.afterWrite?.(sql, params);
        return changes;
      },
    };
    let committed = false;
    try {
      const result = await work(tx); await this.hooks.beforeCommit?.();
      this.db.exec('COMMIT'); committed = true; this.events.push('commit');
      await this.hooks.afterCommit?.(); return result;
    } catch (e) {
      if (!committed) { this.db.exec('ROLLBACK'); this.events.push('rollback'); }
      throw e;
    } finally { this.depth--; }
  }
  async confirm() {
    assert.equal(this.depth, 0, 'no transaction across source confirmation');
    this.events.push('confirm'); this.confirmations++;
    await this.hooks.confirm?.(this.confirmations);
  }
  pending() { return Number(this.db.prepare('SELECT count(*) AS n FROM seed WHERE ack=0').get().n); }
  close() { this.db.close(); }
}
export async function ensure(tx) {
  return (await tx.query("SELECT 1 FROM sqlite_schema WHERE name='seed'")).rowArrays.length === 1;
}
export async function find(tx, id) {
  const rows = (await tx.query('SELECT seq,id,sha,bytes,changes,ack FROM seed WHERE id=?', [id])).rowArrays;
  if (!rows.length) return null;
  const [seq, deliveryId, sha256, byteLength, changes, ack] = rows[0];
  const totals=(await tx.query('SELECT count(*),sum(bytes),sum(changes) FROM seed')).rowArrays[0];
  return { stream: { id: 'source:seed', index: Number(seq) - 1,
    summary: Number(seq) === 1 ? {chunks:Number(totals[0]),byteLength:Number(totals[1]),changes:Number(totals[2])}:null }, delivery: {
    sequence: BigInt(seq), deliveryId, sha256, byteLength: Number(byteLength), changes: Number(changes), acknowledged: !!ack,
  } };
}
export async function load(tx, entry) {
  if (entry.delivery.acknowledged) return null;
  const rows = (await tx.query('SELECT payload FROM seed WHERE id=?', [entry.delivery.deliveryId])).rowArrays;
  const bytes = new Uint8Array(rows[0][0]);
  assert.equal(sum(bytes), entry.delivery.sha256, 'retained body digest');
  assert.equal(bytes.length, entry.delivery.byteLength);
  return bytes;
}
export async function readBootstrapManifest(source, route, controls) {
  operation('manifest', controls);
  const result = await source.transaction(async tx => {
    const rows = (await tx.query('SELECT seq,id,sha,bytes,changes FROM seed ORDER BY seq')).rowArrays;
    assert.ok(rows.length, 'original seed must remain retained');
    assert.equal(rows[0][1], route.deliveryId);
    const manifest = { protocol, ...route, chunks: rows.length,
      byteLength: rows.reduce((n,r) => n+Number(r[3]),0), changes: rows.reduce((n,r) => n+Number(r[4]),0) };
    let chain = jsonHash([protocol, manifest.receiverId, manifest.deliveryId, manifest.tables, manifest.chunks, manifest.changes, manifest.byteLength]);
    for (let i=0;i<rows.length;i++) {
      assert.equal(Number(rows[i][0]), i+1); assert.equal(rows[i][1], chunkId(route.deliveryId,i));
      chain = jsonHash([protocol, chain, i, rows[i][2], Number(rows[i][3]), Number(rows[i][4])]);
    }
    return Object.freeze({ ...manifest, sha256: chain });
  }, controls);
  await source.hooks?.afterManifest?.(result); return result;
}
async function ack(source, manifest, receipt, controls, fanout) {
  operation('ack',controls); assert.equal(controls.receiverId, manifest.receiverId);
  assert.equal(receipt.sha256, manifest.sha256); assert.equal(receipt.confirmed,true);
  source.acks.push({ fanout, controls, receipt });
  await source.hooks.beforeAck?.(receipt);
  const changed = await source.transaction(async tx => {
    if (fanout) {
      const old = (await tx.query('SELECT head FROM replica WHERE id=?', [controls.receiverId])).rowArrays[0];
      assert.ok(old, 'required replica');
      await tx.execute('UPDATE replica SET head=? WHERE id=?', [manifest.chunks, controls.receiverId]);
      const floor = (await tx.query('SELECT min(head) FROM replica')).rowArrays[0][0];
      await tx.execute("UPDATE seed SET ack=1,payload=X'' WHERE seq<=? AND ack=0", [floor]);
      return manifest.chunks-Number(old[0]);
    }
    return tx.execute("UPDATE seed SET ack=1,payload=X'' WHERE seq<=? AND ack=0",[manifest.chunks]);
  }, controls);
  await source.hooks.afterAck?.(changed);
  return source.hooks.ackResult?.(changed) ?? changed;
}
export const acknowledgeBootstrapInstall = (s,m,r,o) => ack(s,m,r,o,false);
export const acknowledgeFanoutBootstrapInstall = (s,m,r,o) => ack(s,m,r,o,true);
export class Receiver {
  constructor(source, { filename = ':memory:', initialize = true, orderedSourceId } = {}) {
    this.db = new DatabaseSync(filename); this.source=source; this.calls=[]; this.hooks={}; this.orderedSourceId=orderedSourceId;
    if (initialize) this.db.exec(`PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY,value);
      CREATE TABLE chunks(idx INTEGER PRIMARY KEY,payload BLOB, bytes INTEGER, changes INTEGER);
      CREATE TABLE state(installed INTEGER,hash TEXT); INSERT INTO state VALUES(0,NULL);`);
  }
  async status(manifest, controls) {
    operation('status',controls); assert.equal(this.source.depth,0,'no transaction across transport'); this.calls.push('status');
    const s=this.db.prepare('SELECT * FROM state').get();
    if(s.hash!==null) assert.equal(s.hash,manifest.sha256,'manifest binding');
    const row=this.db.prepare('SELECT count(*) AS n,coalesce(sum(bytes),0) AS b,coalesce(sum(changes),0) AS c FROM chunks').get();
    const result=row.n===0?null:{receivedChunks:Number(row.n),receivedBytes:Number(row.b),receivedChanges:Number(row.c),installed:!!s.installed};
    return this.hooks.status ? this.hooks.status(manifest,result,controls) : result;
  }
  async stage(manifest,index,bytes,controls) {
    operation('stage',controls); assert.equal(this.source.depth,0); this.calls.push(`stage:${index}`);
    await this.hooks.beforeStage?.(manifest,index,bytes,controls);
    const previous=this.db.prepare('SELECT payload FROM chunks WHERE idx=?').get(index);
    if (!previous) {
      const n=this.db.prepare('SELECT count(*) AS n FROM chunks').get().n; assert.equal(index,Number(n));
      this.db.exec('BEGIN');
      try {
        this.db.prepare('INSERT INTO chunks VALUES(?,?,?,?)').run(index,bytes,bytes.length,bytes.length?1:0);
        this.db.prepare('UPDATE state SET hash=?').run(manifest.sha256); this.db.exec('COMMIT');
      } catch(e) {this.db.exec('ROLLBACK');throw e;}
    } else assert.deepEqual(new Uint8Array(previous.payload),bytes);
    const row=this.db.prepare('SELECT count(*) AS n,sum(bytes) AS b,sum(changes) AS c FROM chunks').get();
    const result={receivedChunks:Number(row.n),receivedBytes:Number(row.b),receivedChanges:Number(row.c),installed:false};
    return this.hooks.afterStage ? this.hooks.afterStage(manifest,index,result,controls) : result;
  }
  async install(manifest,controls) {
    operation('install',controls); assert.equal(this.source.depth,0); this.calls.push('install');
    await this.hooks.beforeInstall?.(manifest,controls);
    const replayed=!!this.db.prepare('SELECT installed FROM state').get().installed;
    if(!replayed) {
      const rows=this.db.prepare('SELECT * FROM chunks ORDER BY idx').all();assert.equal(rows.length,manifest.chunks);
      this.db.exec('BEGIN');
      try {
        let chain=jsonHash([protocol,manifest.receiverId,manifest.deliveryId,manifest.tables,manifest.chunks,manifest.changes,manifest.byteLength]);
        for(const r of rows) {
          chain=jsonHash([protocol,chain,Number(r.idx),sum(r.payload),Number(r.bytes),Number(r.changes)]);
          assert.equal(this.db.applyChangeset(r.payload),true);await this.hooks.afterRow?.();
        }
        assert.equal(chain,manifest.sha256);
        this.db.exec("UPDATE state SET installed=1; UPDATE chunks SET payload=X''; COMMIT");
      } catch(e){this.db.exec('ROLLBACK');throw e;}
    }
    await this.hooks.confirm?.();
    const result={protocol,receiverId:manifest.receiverId,deliveryId:manifest.deliveryId,sha256:manifest.sha256,
      chunks:manifest.chunks,changes:manifest.changes,byteLength:manifest.byteLength,installed:true,confirmed:true,replayed,
      ...(this.orderedSourceId?{order:{protocol:'fsqlite-ordered-changeset-v1',streamId:this.orderedSourceId,sequence:String(manifest.chunks)}}:{})};
    return this.hooks.afterInstall ? this.hooks.afterInstall(result,controls) : result;
  }
  rows(){return this.db.prepare('SELECT * FROM t ORDER BY id').all().map(r=>({...r}));}
  close(){this.db.close();}
}
