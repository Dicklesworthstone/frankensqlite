"""Native SQLite session/rebaser oracle, no emulated apply or rebase."""
import ctypes as C
import ctypes.util
import json
import struct
import sys

lib = C.CDLL(ctypes.util.find_library('sqlite3'))
P, I, S = C.c_void_p, C.c_int, C.c_char_p
for name, restype, args in [
    ('sqlite3_libversion', S, []),
    ('sqlite3_open', I, [S, C.POINTER(P)]),
    ('sqlite3_close', I, [P]),
    ('sqlite3_errmsg', S, [P]),
    ('sqlite3_exec', I, [P, S, P, P, C.POINTER(P)]),
    ('sqlite3_free', None, [P]),
    ('sqlite3session_create', I, [P, S, C.POINTER(P)]),
    ('sqlite3session_attach', I, [P, S]),
    ('sqlite3session_delete', None, [P]),
    ('sqlite3session_indirect', I, [P, I]),
    ('sqlite3session_changeset', I, [P, C.POINTER(I), C.POINTER(P)]),
    ('sqlite3changeset_apply_v2', I, [P, I, P, P, P, P, C.POINTER(P), C.POINTER(I), I]),
    ('sqlite3changeset_start', I, [C.POINTER(P), I, P]),
    ('sqlite3changeset_next', I, [P]),
    ('sqlite3changeset_finalize', I, [P]),
    ('sqlite3changeset_op', I, [P, C.POINTER(S), C.POINTER(I), C.POINTER(I), C.POINTER(I)]),
    ('sqlite3changeset_pk', I, [P, C.POINTER(P), C.POINTER(I)]),
    ('sqlite3changeset_old', I, [P, I, C.POINTER(P)]),
    ('sqlite3changeset_new', I, [P, I, C.POINTER(P)]),
    ('sqlite3_value_type', I, [P]),
    ('sqlite3_value_int64', C.c_int64, [P]),
    ('sqlite3_value_double', C.c_double, [P]),
    ('sqlite3_value_text', P, [P]),
    ('sqlite3_value_blob', P, [P]),
    ('sqlite3_value_bytes', I, [P]),
    ('sqlite3rebaser_create', I, [C.POINTER(P)]),
    ('sqlite3rebaser_configure', I, [P, I, P]),
    ('sqlite3rebaser_rebase', I, [P, I, P, C.POINTER(I), C.POINTER(P)]),
    ('sqlite3rebaser_delete', None, [P]),
]:
    f = getattr(lib, name)
    f.restype, f.argtypes = restype, args

def check(rc, db=None):
    if rc:
        msg = lib.sqlite3_errmsg(db).decode() if db else ''
        raise RuntimeError(f'SQLite returned {rc}: {msg}')

def output(ptr, size):
    try:
        return C.string_at(ptr, size.value) if size.value else b''
    finally:
        lib.sqlite3_free(ptr)

def value(p):
    if not p:
        return {'undefined': True}
    kind = lib.sqlite3_value_type(p)
    if kind == 1:
        return {'integer': str(lib.sqlite3_value_int64(p))}
    if kind == 2:
        return {'real': struct.pack('>d', lib.sqlite3_value_double(p)).hex()}
    if kind == 5:
        return {'null': True}
    data = lib.sqlite3_value_text(p) if kind == 3 else lib.sqlite3_value_blob(p)
    n = lib.sqlite3_value_bytes(p)
    b = C.string_at(data, n) if n else b''
    return {'text': b.decode('utf8')} if kind == 3 else {'blob': b.hex()}

def record(it):
    name, columns, op, indirect, pk = S(), I(), I(), I(), P()
    check(lib.sqlite3changeset_op(it, C.byref(name), C.byref(columns), C.byref(op), C.byref(indirect)))
    check(lib.sqlite3changeset_pk(it, C.byref(pk), C.byref(columns)))
    keys = list(C.string_at(pk, columns.value))
    result = {'name': name.value.decode(), 'primaryKey': keys,
              'operation': {18: 'insert', 9: 'delete', 23: 'update'}[op.value],
              'indirect': bool(indirect.value)}
    for label in ['old', 'new']:
        if (label == 'old' and op.value == 18) or (label == 'new' and op.value == 9):
            continue
        fields = []
        for i in range(columns.value):
            p = P()
            check(getattr(lib, 'sqlite3changeset_' + label)(it, i, C.byref(p)))
            # Native INSERT->UPDATE repeats PK in new. Normalize that redundant
            # key, and omit header-only tables via the native iterator itself.
            fields.append({'undefined': True} if label == 'new' and op.value == 23 and keys[i]
                          else value(p))
        result[label] = fields
    return result

def records(wire):
    it, buf = P(), C.create_string_buffer(wire)
    check(lib.sqlite3changeset_start(C.byref(it), len(wire), buf))
    result = []
    try:
        while True:
            rc = lib.sqlite3changeset_next(it)
            if rc == 101:
                return result
            if rc != 100:
                check(rc)
            result.append(record(it))
    finally:
        check(lib.sqlite3changeset_finalize(it))

def identity(change):
    fields = change['new'] if change['operation'] == 'insert' else change['old']
    return (change['name'], change['operation'],
            [v for v, pk in zip(fields, change['primaryKey']) if pk])

class DB:
    def __init__(self, sql):
        self.p = P()
        check(lib.sqlite3_open(b':memory:', C.byref(self.p)))
        self.exec(sql)
    def exec(self, sql):
        error = P()
        rc = lib.sqlite3_exec(self.p, sql.encode(), None, None, C.byref(error))
        try:
            if rc:
                detail = C.string_at(error).decode() if error else ''
                raise RuntimeError(f'SQLite {rc}: {detail}; SQL={sql!r}')
        finally:
            lib.sqlite3_free(error)
    def session(self, sql, indirect=False):
        p = P()
        check(lib.sqlite3session_create(self.p, b'main', C.byref(p)), self.p)
        try:
            check(lib.sqlite3session_attach(p, None), self.p)
            lib.sqlite3session_indirect(p, int(indirect))
            self.exec(sql)
            size, data = I(), P()
            check(lib.sqlite3session_changeset(p, C.byref(size), C.byref(data)), self.p)
            return output(data, size)
        finally:
            lib.sqlite3session_delete(p)
    def apply(self, wire, policy):
        decisions = []
        changes = records(wire)
        errors = []
        fn = C.CFUNCTYPE(I, P, I, P)
        def conflict(ctx, kind, it):
            try:
                index = len(decisions)
                action = policy[index] if isinstance(policy, list) else policy
                current = identity(record(it))
                matched = [i for i, c in enumerate(changes) if identity(c) == current]
                if len(matched) != 1:
                    raise ValueError('Ambiguous native conflict identity')
                decisions.append({'kind': kind, 'action': action, 'changeIndex': matched[0]})
                return {'omit': 0, 'replace': 1, 'abort': 2}[action]
            except Exception as error:
                errors.append(str(error))
                return 2
        callback = fn(conflict)
        data, size = P(), I()
        buf = C.create_string_buffer(wire)
        rc = lib.sqlite3changeset_apply_v2(self.p, len(wire), buf, None, callback,
                                          None, C.byref(data), C.byref(size), 0)
        rb = output(data, size)
        if errors:
            raise RuntimeError(errors)
        check(rc, self.p)
        return rb, decisions
    def close(self):
        check(lib.sqlite3_close(self.p))

def rebase(local, buffers):
    p = P()
    check(lib.sqlite3rebaser_create(C.byref(p)))
    try:
        for wire in buffers:
            check(lib.sqlite3rebaser_configure(p, len(wire), C.create_string_buffer(wire)))
        data, size = P(), I()
        check(lib.sqlite3rebaser_rebase(p, len(local), C.create_string_buffer(local),
                                       C.byref(size), C.byref(data)))
        return output(data, size)
    finally:
        lib.sqlite3rebaser_delete(p)

def scenario(case):
    source = DB(case['schema'] + case.get('seed', ''))
    remote = DB(case['schema'] + case.get('seed', ''))
    try:
        local = source.session(case['local'], case.get('indirect', False))
        rbs, remotes, decisions = [], [], []
        for step in case['remote']:
            wire = remote.session(step['sql'])
            rb, events = source.apply(wire, step.get('policy', 'omit'))
            remotes.append(wire)
            rbs.append(rb)
            decisions.append(events)
        result = rebase(local, rbs)
        # Also prove SQLite can apply the rebased local changes to the remote.
        remote.apply(result, 'abort')
        return {**case, 'localWire': local.hex(), 'remoteWires': [x.hex() for x in remotes],
                'rebaseBuffers': [x.hex() for x in rbs], 'expectedWire': result.hex(),
                'expectedRecords': records(result),
                'decisions': decisions}
    finally:
        source.close()
        remote.close()

if __name__ == '__main__':
    cases = json.load(sys.stdin)
    if isinstance(cases, dict):
        results = []
        for case in cases.get('rebases', [cases]):
            wire = rebase(bytes.fromhex(case['local']), [bytes.fromhex(x) for x in case['buffers']])
            results.append({'wire': wire.hex(), 'records': records(wire)})
        print(json.dumps(results if 'rebases' in cases else results[0]))
    else:
        print(json.dumps({'sqlite': lib.sqlite3_libversion().decode(),
                          'cases': [scenario(case) for case in cases]}))
