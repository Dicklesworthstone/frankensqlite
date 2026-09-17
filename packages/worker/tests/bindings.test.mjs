import { test } from 'node:test';
import assert from 'node:assert/strict';
import { parameterLayout, resolveBindings, MAX_BIND_PARAMETERS } from '../src/bindings.ts';
import { RequestBudget } from '../src/admission.ts';
import { sqliteBindingFixture } from './helpers/bindings-core.mjs';

async function fixture(t) {
  const f=sqliteBindingFixture(); t.after(()=>f.shutdown());
  let id=0;
  const send=message=>f.host.handle({requestId:++id,...message});
  const ok=async message=>{const response=await send(message);if(response.kind==='error')assert.fail(JSON.stringify(response.error));return response;};
  await ok({kind:'init',config:{}});
  return {...f,send,ok};
}

test('layout matches independent SQLite parameter metadata including holes and aliases', async t=>{
  const f=await fixture(t);
  for(const sql of [
    'SELECT ?, ?5, :x, ?2, :x, @x', 'SELECT :x,:x,?1', 'SELECT ?,?1',
    'SELECT ?01,?1,?', 'SELECT $::name(suffix), :a::b, @a(z), :a$b',
    'SELECT :世界, @UPPER, @upper, $a::b::c(x-y)',
    `SELECT '?' AS ":ignored", :x AS [@ignored], '@inside' AS \`$inside\` /*?9*/ --:no\n, :x`,
    'SELECT 1 AS foo$bar', 'SELECT ?32766',
  ]) {
    const oracle=await f.rpc({op:'prepare',sql}), layout=parameterLayout(sql);
    assert.equal(layout.count,oracle.parameterNames.length,sql);
    assert.deepEqual(layout.names,oracle.parameterNames,sql);
    assert.ok(Object.isFrozen(layout)); assert.ok(Object.isFrozen(layout.names));
  }
});

test('complete binding supports exact and unambiguous bare names without rewriting slots',()=>{
  const p=parameterLayout('SELECT ?2,:x,?1,@x,:x,$tail');
  assert.deepEqual(resolveBindings(p,{'?2':2,':x':3,'?1':1,'@x':4,tail:5}),[1,2,3,4,5]);
  assert.deepEqual(resolveBindings(parameterLayout('SELECT ?5,:v'),{'?5':5,v:6}),[null,null,null,null,5,6]);
  assert.deepEqual(resolveBindings(parameterLayout('SELECT :x,?1'),{x:8}),[8]);
});

test('ambiguous, missing, duplicate and unknown names fail explicitly',()=>{
  for(const [sql,params,code] of [
    ['SELECT :x,@x',{x:1},'NAME'], ['SELECT :x',{x:1,':x':2},'NAME'],
    ['SELECT ?01,?1',{'?01':1,'?1':1},'NAME'], ['SELECT :x',{y:1},'NAME'],
    ['SELECT :x',{},'ARITY'], ['SELECT ?',{},'ARITY'],
  ]) assert.throws(()=>resolveBindings(parameterLayout(sql),params),{code:`ERR_FSQLITE_BINDING_${code}`});
});

test('strict positional resolver checks actual slot count and scalar types',()=>{
  const p=parameterLayout('SELECT ?3');
  assert.deepEqual(resolveBindings(p,[null,null,3]),[null,null,3]);
  for(const values of [[],[3],[1,2,3,4]]) assert.throws(()=>resolveBindings(p,values),{code:'ERR_FSQLITE_BINDING_ARITY'});
  for(const value of [undefined,{},Symbol(),1n<<63n,-(1n<<63n)-1n]) {
    assert.throws(()=>resolveBindings(parameterLayout('SELECT ?'),[value]),{code:'ERR_FSQLITE_BINDING_INPUT'});
  }
  assert.throws(()=>resolveBindings(parameterLayout('SELECT ?'),new Array(1)),{code:'ERR_FSQLITE_BINDING_INPUT'});
});

test('parameter bombs and malformed tokens are bounded before core work',()=>{
  for(const sql of ['SELECT ?0',`SELECT ?${MAX_BIND_PARAMETERS+1}`,`SELECT ?${'9'.repeat(1000)}`,
    "SELECT 'unfinished",'SELECT $a(unclosed','SELECT $a(with space)','SELECT :','SELECT ?\0']) {
    assert.throws(()=>parameterLayout(sql),{code:'ERR_FSQLITE_BINDING_INPUT'},sql);
  }
});

test('native SQL receives exact named values, repeats, prefixes, holes and original column names',async t=>{
  const f=await fixture(t);
  const sql='SELECT :x, :x, @x, ?5, $tail';
  const r=await f.ok({kind:'query',sql,params:{':x':7,'@x':9,'?5':11,tail:13}});
  assert.deepEqual(r.data.rowArrays,[[7,7,9,11,13]]);
  assert.deepEqual(r.data.columns,[':x',':x','@x','?5','$tail']);
  assert.equal(f.requests.at(-1).sql,sql);
  assert.deepEqual(f.requests.at(-1).params,[7,9,null,null,11,13]);
});

test('prepared bindings reuse a real native statement and reset values between calls',async t=>{
  const f=await fixture(t);
  const p=await f.ok({kind:'prepare',sql:'SELECT :v AS v, :v AS repeated'});
  assert.equal(p.data.parameterCount,1); assert.deepEqual(p.data.parameterNames,[':v']);
  for(const v of [17,'text',null,9223372036854775807n,Uint8Array.of(0,255)]) {
    const r=await f.ok({kind:'statement-query',statementId:p.data.statementId,params:{v}});
    assert.deepEqual(r.data.rowArrays,[[v,v]]);
  }
  await f.ok({kind:'statement-finalize',statementId:p.data.statementId});
  assert.equal((await f.send({kind:'statement-query',statementId:p.data.statementId,params:{v:2}})).kind,'error');
});

test('missing or extra named values and script tails never execute a partial write',async t=>{
  const f=await fixture(t);
  await f.ok({kind:'execute',sql:'CREATE TABLE t(id PRIMARY KEY,v)'});
  for(const params of [{id:1},{id:1,v:'x',oops:0}]) {
    const before=f.requests.length;
    assert.equal((await f.send({kind:'query',sql:'INSERT INTO t VALUES(:id,:v) RETURNING id',params})).kind,'error');
    assert.equal(f.requests.length,before);
  }
  assert.equal((await f.send({kind:'execute',sql:'INSERT INTO t VALUES(:id,:v); COMMIT',params:{id:1,v:'x'}})).kind,'error');
  assert.deepEqual((await f.ok({kind:'query',sql:'SELECT * FROM t'})).data.rowArrays,[]);
  await f.ok({kind:'execute',sql:'INSERT INTO t VALUES(:id,:v)',params:{id:1,v:'x'}});
  assert.deepEqual((await f.ok({kind:'query',sql:'SELECT * FROM t'})).data.rowArrays,[[1,'x']]);
});

test('named failures fence a managed scope and preserve rollback',async t=>{
  const f=await fixture(t);
  await f.ok({kind:'execute',sql:'CREATE TABLE t(v)'});
  await f.ok({kind:'transaction',action:'begin',transactionId:'1'});
  await f.ok({kind:'execute',transactionId:'1',sql:'INSERT INTO t VALUES(:v)',params:{v:1}});
  const bad=await f.send({kind:'execute',transactionId:'1',sql:'INSERT INTO t VALUES(:v)',params:{wrong:2}});
  assert.equal(bad.error.code,'ERR_FSQLITE_BINDING_NAME');
  const after=await f.send({kind:'execute',transactionId:'1',sql:'INSERT INTO t VALUES(:v)',params:{v:3}});
  assert.equal(after.error.code,'ERR_FSQLITE_TRANSACTION_ABORTED');
  await f.ok({kind:'transaction',action:'rollback',transactionId:'1'});
  assert.deepEqual((await f.ok({kind:'query',sql:'SELECT * FROM t'})).data.rowArrays,[]);
});

test('named transport captures scalars, charges full backing buffers and excludes inherited keys',()=>{
  const budget=new RequestBudget({maxPendingBytes:512});
  const params=Object.create(null); params.__proto__='safe'; params.v=9;
  const admission=budget.admit({kind:'query',requestId:1,sql:'SELECT :__proto__,:v',params});
  params.v=10;
  assert.equal(admission.request.params.v,9); assert.equal(admission.request.params.__proto__,'safe');
  admission.release(); assert.equal(budget.stats.pendingBytes,0);
  assert.throws(()=>budget.admit({kind:'query',requestId:2,sql:'SELECT :v',params:{v:new Uint8Array(1024).subarray(0,1)}}),{code:'ERR_FSQLITE_REQUEST_TOO_LARGE'});
  assert.throws(()=>budget.admit({kind:'query',requestId:3,sql:'SELECT :v',params:Object.create({v:1})}),{code:'ERR_FSQLITE_BINDING_INPUT'});
});

test('quoted identifiers, comments and Tcl suffixes do not cause false bindings',async t=>{
  const f=await fixture(t);
  const sql=`SELECT :x AS "?", $::a(b-c) AS [@x], :a::b AS v, @a(z) AS w, '$ignored' AS literal /* :no */`;
  const data=(await f.ok({kind:'query',sql,params:{x:'hi','$::a(b-c)':2,':a::b':3,'@a(z)':4}})).data;
  assert.deepEqual(data.rowArrays,[['hi',2,3,4,'$ignored']]);
});
