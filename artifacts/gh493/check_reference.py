#!/usr/bin/env python3
"""Validate candidate SQL/fixtures against CPython sqlite3, NOT FrankenSQLite."""
from __future__ import annotations
import json
import re
import sqlite3
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RUST = (ROOT / 'gh493_pager_contract.rs').read_text()


def seed(path: Path) -> None:
    with sqlite3.connect(path) as db:
        db.executescript("""
            PRAGMA page_size=4096;
            CREATE TABLE bulk(id INTEGER PRIMARY KEY, body TEXT NOT NULL);
            CREATE TABLE a(id TEXT PRIMARY KEY, v INTEGER NOT NULL);
            CREATE TABLE b(id TEXT PRIMARY KEY, v INTEGER NOT NULL);
            CREATE TABLE nodes(id INTEGER PRIMARY KEY, parent INTEGER, label TEXT);
            INSERT INTO nodes VALUES (1,NULL,'root'),(2,1,'branch'),(3,2,'left'),(4,2,'right');
        """)
        db.executemany('INSERT INTO bulk VALUES (?,?)', ((i, 'x'*2048) for i in range(4096)))
        db.executemany('INSERT INTO a VALUES (?,?)', ((f'k{i:03}', i) for i in range(100)))
        db.executemany('INSERT INTO b VALUES (?,?)', ((f'k{i:03}', i*2) for i in range(100)))


def main() -> None:
    # Keep the source string literals themselves as the reference inputs.
    cases = [json.loads(s) for s in re.findall(r'^        ("WITH [^\n]*"),$', RUST, re.M)]
    if len(cases) != 14:
        raise AssertionError(f'expected 14 source-derived shape cases, got {len(cases)}')
    receipts = []
    with tempfile.TemporaryDirectory(prefix='gh493-reference-') as directory:
        path = Path(directory)/'main.db'
        aux = Path(directory)/'aux.db'
        seed(path)
        with sqlite3.connect(path) as db:
            for sql in cases:
                rows = db.execute(sql).fetchall()
                receipts.append({'sql':sql, 'rows':rows})
            db.execute('CREATE TEMP TABLE c(x INTEGER)')
            db.execute('INSERT INTO temp.c VALUES (41)')
            error_sql = "WITH c(x) AS (SELECT v FROM a WHERE id='k042'), d(y) AS (SELECT abs(-9223372036854775808) FROM c) SELECT y FROM d"
            try:
                db.execute(error_sql).fetchall()
            except sqlite3.OperationalError as error:
                assert 'overflow' in str(error), str(error)
            else:
                raise AssertionError('expected integer overflow')
            assert db.execute('SELECT x FROM temp.c').fetchall()==[(41,)]
            ipk = "WITH nodes(id,parent,label) AS (SELECT id+100,parent,label FROM main.nodes WHERE id<3) SELECT c.id,b.v FROM nodes c JOIN b ON b.id='k000' ORDER BY c.id"
            assert db.execute(ipk).fetchall()==[(101,0),(102,0)]
        cte = "WITH c AS (SELECT id,v FROM a WHERE id='k042') SELECT c.id,c.v+b.v FROM c JOIN b ON b.id=c.id"
        db = sqlite3.connect(path, isolation_level=None)
        db.execute('BEGIN IMMEDIATE')
        db.execute("UPDATE a SET v=900 WHERE id='k042'")
        assert db.execute(cte).fetchall()==[('k042',984)]
        db.execute('SAVEPOINT s')
        db.execute("UPDATE a SET v=901 WHERE id='k042'")
        assert db.execute(cte).fetchall()==[('k042',985)]
        db.execute('ROLLBACK TO s')
        assert db.execute(cte).fetchall()==[('k042',984)]
        db.execute('RELEASE s')
        db.execute('ROLLBACK')
        assert db.execute(cte).fetchall()==[('k042',126)]
        db.execute('PRAGMA journal_mode=WAL')
        reader = sqlite3.connect(path, isolation_level=None)
        reader.execute('BEGIN DEFERRED')
        assert reader.execute(cte).fetchall()==[('k042',126)]
        db.execute("UPDATE a SET v=77 WHERE id='k042'")
        assert reader.execute(cte).fetchall()==[('k042',126)]
        reader.execute('ROLLBACK')
        assert reader.execute(cte).fetchall()==[('k042',161)]
        reader.close()
        db.execute("UPDATE a SET v=42 WHERE id='k042'")
        db.execute('ATTACH ? AS aux',(str(aux),))
        db.execute('CREATE TABLE aux.sink(id TEXT PRIMARY KEY,v INTEGER NOT NULL)')
        inserts = [
            ("WITH c AS (SELECT id,v FROM a WHERE id='k042') INSERT INTO aux.sink(id,v) SELECT id,v FROM c RETURNING id,v", [('k042',42)]),
            ("WITH c AS (SELECT id,v+5 AS v FROM a WHERE id='k042') INSERT INTO aux.sink(id,v) SELECT id,v FROM c WHERE true ON CONFLICT(id) DO UPDATE SET v=excluded.v+1 RETURNING id,v", [('k042',48)]),
            ("WITH c AS (SELECT v FROM a WHERE id='k042') UPDATE aux.sink SET v=(SELECT v FROM c) WHERE id='k042' RETURNING id,v", [('k042',42)]),
        ]
        for sql, expected in inserts:
            assert db.execute(sql).fetchall()==expected
        db.execute('BEGIN')
        db.execute('SAVEPOINT s')
        delete = "WITH c AS (SELECT id FROM a WHERE id='k042') DELETE FROM aux.sink WHERE id IN (SELECT id FROM c) RETURNING id,v"
        assert db.execute(delete).fetchall()==[('k042',42)]
        assert db.execute('SELECT * FROM aux.sink').fetchall()==[]
        db.execute('ROLLBACK TO s')
        db.execute('RELEASE s')
        assert db.execute('SELECT * FROM aux.sink').fetchall()==[('k042',42)]
        provisional = "WITH c AS (SELECT id,v FROM a WHERE id='k043') INSERT INTO aux.sink(id,v) SELECT id,v FROM c RETURNING id,v"
        assert db.execute(provisional).fetchall()==[('k043',43)]
        db.execute('ROLLBACK')
        assert db.execute("WITH sink AS (SELECT id,v FROM a) SELECT id,v FROM aux.sink ORDER BY id").fetchall()==[('k042',42)]
        assert db.execute('PRAGMA main.integrity_check').fetchall()==[('ok',)]
        assert db.execute('PRAGMA aux.integrity_check').fetchall()==[('ok',)]
        assert db.execute('SELECT count(*) FROM bulk').fetchone()==(4096,)
        db.close()
        with sqlite3.connect(aux) as reopened:
            assert reopened.execute('SELECT * FROM sink').fetchall()==[('k042',42)]
        result = {
            'engine': 'CPython sqlite3 reference only',
            'sqlite_version': sqlite3.sqlite_version,
            'fixture_bytes_after_checkpoint': path.stat().st_size,
            'shape_cases': receipts,
            'local_savepoint_and_rollback': 'pass',
            'wal_reader_snapshot': 'pass',
            'attached_dml_upsert_returning_savepoints_and_reopen': 'pass',
            'overflow_cleanup_and_temp_shadow': 'pass',
            'ipk_cte_shadow_expected_rows': 'pass',
            'integrity_check_main_and_aux': 'pass',
            'rust_compilation': 'NOT RUN',
            'frankensqlite_tests': 'NOT RUN',
            'native_before_after_time_and_rss': 'NOT MEASURED',
            'candidate_integrated': False,
        }
        (ROOT/'reference-results.json').write_text(json.dumps(result, indent=2)+'\n')
        print(json.dumps({k:v for k,v in result.items() if k!='shape_cases'}, indent=2))

if __name__=='__main__':
    main()
