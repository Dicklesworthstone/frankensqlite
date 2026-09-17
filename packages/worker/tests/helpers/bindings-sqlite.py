"""Test-only SQLite C-API oracle. Original SQL and 1-based slots are unchanged.
Uses the system SQLite shared library; not FrankenSQLite WASM certification.
"""
import ctypes as C
import ctypes.util
import json
import sys

lib = C.CDLL(ctypes.util.find_library("sqlite3"))
P, I, S = C.c_void_p, C.c_int, C.c_char_p

def api(name, result, *args):
    fn = getattr(lib, "sqlite3_" + name)
    fn.restype, fn.argtypes = result, list(args)
    return fn

open_db = api("open", I, S, C.POINTER(P))
close_db = api("close", I, P)
prepare = api("prepare_v2", I, P, S, I, C.POINTER(P), C.POINTER(S))
finalize = api("finalize", I, P)
reset = api("reset", I, P)
clear = api("clear_bindings", I, P)
step = api("step", I, P)
errmsg = api("errmsg", S, P)
count_params = api("bind_parameter_count", I, P)
param_name = api("bind_parameter_name", S, P, I)
column_count = api("column_count", I, P)
column_name = api("column_name", S, P, I)
column_type = api("column_type", I, P, I)
column_int = api("column_int64", C.c_int64, P, I)
column_double = api("column_double", C.c_double, P, I)
column_text = api("column_text", P, P, I)
column_blob = api("column_blob", P, P, I)
column_bytes = api("column_bytes", I, P, I)
changes = api("changes", I, P)
bind_null = api("bind_null", I, P, I)
bind_int = api("bind_int64", I, P, I, C.c_int64)
bind_double = api("bind_double", I, P, I, C.c_double)
bind_text = api("bind_text", I, P, I, S, I, P)
bind_blob = api("bind_blob", I, P, I, S, I, P)
exec_sql = api("exec", I, P, S, P, P, P)
version = api("libversion", S)().decode()
db = P()
assert open_db(sys.argv[1].encode(), C.byref(db)) == 0
statements = {}
next_id = 0

class SqlError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(errmsg(db).decode())

def check(code):
    if code != 0:
        raise SqlError(code)

def prepared(sql):
    stmt = P()
    check(prepare(db, sql.encode(), -1, C.byref(stmt), None))
    if not stmt:
        raise ValueError("Empty statement")
    return stmt

def bind(stmt, values):
    reset(stmt)
    check(clear(stmt))
    for i, value in enumerate(values, 1):
        if value is None:
            check(bind_null(stmt, i))
        elif isinstance(value, dict) and "integer" in value:
            check(bind_int(stmt, i, int(value["integer"])))
        elif isinstance(value, dict) and "blob" in value:
            raw = bytes(value["blob"])
            check(bind_blob(stmt, i, raw, len(raw), P(-1)))
        elif isinstance(value, (bool, int)):
            check(bind_int(stmt, i, int(value)))
        elif isinstance(value, float):
            check(bind_double(stmt, i, value))
        else:
            raw = value.encode("utf-8")
            check(bind_text(stmt, i, raw, len(raw), P(-1)))

def cell(stmt, i):
    kind = column_type(stmt, i)
    if kind == 1:
        n = column_int(stmt, i)
        return {"integer": str(n)} if abs(n) > 9007199254740991 else n
    if kind == 2:
        return column_double(stmt, i)
    if kind in (3, 4):
        pointer = column_text(stmt, i) if kind == 3 else column_blob(stmt, i)
        raw = C.string_at(pointer, column_bytes(stmt, i))
        return raw.decode("utf-8") if kind == 3 else {"blob": list(raw)}
    return None

try:
    for line in sys.stdin:
        request = json.loads(line)
        try:
            op = request["op"]
            temporary = None
            if op == "prepare":
                stmt = prepared(request["sql"])
                next_id += 1
                statements[next_id] = stmt
                result = {"statementId": next_id, "columns": [column_name(stmt, i).decode() for i in range(column_count(stmt))],
                          "parameterNames": [param_name(stmt, i).decode() if param_name(stmt, i) else None for i in range(1, count_params(stmt) + 1)], "version": version}
            elif op == "batch":
                check(exec_sql(db, request["sql"].encode(), None, None, None))
                result = None
            elif op == "free":
                check(finalize(statements.pop(request["statementId"])))
                result = None
            else:
                stmt = statements[request["statementId"]] if "statementId" in request else prepared(request["sql"])
                if "statementId" not in request:
                    temporary = stmt
                try:
                    bind(stmt, request.get("params", []))
                    columns = [column_name(stmt, i).decode() for i in range(column_count(stmt))]
                    rows = []
                    while True:
                        code = step(stmt)
                        if code == 101:
                            break
                        if code != 100:
                            raise SqlError(code)
                        rows.append([cell(stmt, i) for i in range(len(columns))])
                    result = {"columns": columns, "rowArrays": rows, "changes": changes(db)}
                finally:
                    if temporary:
                        finalize(temporary)
                    else:
                        reset(stmt)
            print(json.dumps({"id": request["id"], "result": result}), flush=True)
        except Exception as error:
            print(json.dumps({"id": request["id"], "error": {"message": str(error), "sqliteCode": getattr(error, "code", 1)}}), flush=True)
finally:
    for stmt in statements.values():
        finalize(stmt)
    close_db(db)
