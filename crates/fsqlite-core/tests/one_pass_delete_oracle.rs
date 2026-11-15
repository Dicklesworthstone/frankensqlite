#![recursion_limit = "512"]

//! bd-w4pr1.13.20: a DELETE that scans its table deletes each matching row
//! while the scan stands on it (one pass) when nothing can observe the rows
//! deleted so far, and keeps the two passes (collect the rowids, then delete)
//! otherwise. Compared with stock SQLite (rusqlite, bundled): the changed-row
//! count or RETURNING rows, then every listed state query, on `:memory:` and
//! on a file, through `Connection::execute` and a prepared statement.
//!
//! The shapes a wrong one-pass lowering gets wrong, each covered below:
//! consecutive matching rows (a Next after the Delete skips the successor),
//! runs that empty leaves and rebalance the tree, the last row of a leaf and
//! of the table, index entries of the deleted rows, a WHERE that reads the
//! target table through a subquery (it must see the table as it was before
//! the statement), BEFORE / AFTER triggers and foreign-key actions (run around
//! the program in this engine), and RETURNING.
//!
//! An UPDATE that scans its table rewrites each matching row in the scan on
//! the same terms, and also keeps the two passes when it moves the rowid
//! (the scan could meet the row again) or resolves a conflict with IGNORE or
//! REPLACE. Its cases add rows that grow across page splits while the scan
//! continues, rows that shrink, an indexed and a UNIQUE indexed column, a
//! WHERE reading the column being set, and a SET reading the target table.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

struct Case {
    setup: &'static [&'static str],
    statement: &'static str,
    states: &'static [&'static str],
}

const SMALL: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT)",
    "INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c'), (4, 4, 'd'), (5, 5, 'e'), (6, 6, 'f'), (7, 7, 'g')",
];
const ROWS_STATE: &[&str] = &["SELECT rowid, * FROM t ORDER BY rowid"];

/// 3,000 rows of about 120 bytes: about 100 leaves, so long runs of deletes
/// empty whole leaves and rebalance interior pages.
const MANY: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 3000) \
     INSERT INTO t SELECT i, (i * 7919) % 1000, printf('%0100d', i) FROM n",
];
const MANY_STATE: &[&str] = &[
    "SELECT count(*), sum(id), sum(a), min(id), max(id) FROM t",
    "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id LIMIT 40)",
    "PRAGMA integrity_check",
];

const INDEXED: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
    "CREATE INDEX t_a ON t(a)",
    "CREATE UNIQUE INDEX t_b ON t(b)",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2000) \
     INSERT INTO t SELECT i, i % 13, 'k' || i FROM n",
];
const INDEXED_STATE: &[&str] = &[
    "SELECT count(*), sum(id) FROM t",
    "SELECT a, count(*) FROM t INDEXED BY t_a WHERE a >= 0 GROUP BY a ORDER BY a",
    "SELECT count(*) FROM t INDEXED BY t_b WHERE b > ''",
    "PRAGMA integrity_check",
];

const TRIGGERS: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT)",
    "CREATE TABLE log(seq INTEGER PRIMARY KEY, what TEXT, id INTEGER, live INTEGER)",
    "INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c'), (4, 4, 'd'), (5, 5, 'e'), (6, 6, 'f')",
    "CREATE TRIGGER t_bd BEFORE DELETE ON t BEGIN \
     INSERT INTO log(what, id, live) VALUES ('before', old.id, (SELECT count(*) FROM t)); END",
    "CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN \
     INSERT INTO log(what, id, live) VALUES ('after', old.id, (SELECT count(*) FROM t)); END",
];
const TRIGGER_STATE: &[&str] = &[
    "SELECT rowid, * FROM t ORDER BY rowid",
    "SELECT what, id, live FROM log ORDER BY seq",
];

/// An AFTER trigger that deletes the next row: the second delete of each
/// pair must see what the first one left.
const TRIGGER_DELETES_TARGET: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT)",
    "INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c'), (4, 4, 'd'), (5, 5, 'e'), (6, 6, 'f')",
    "CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN DELETE FROM t WHERE id = old.id + 1; END",
];

const FK_CASCADE: &[&str] = &[
    "PRAGMA foreign_keys = ON",
    "CREATE TABLE p(id INTEGER PRIMARY KEY, k INTEGER)",
    "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE, \
     nid INTEGER REFERENCES p(id) ON DELETE SET NULL)",
    "INSERT INTO p VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
    "INSERT INTO c VALUES (10, 1, 2), (11, 2, 3), (12, 3, 4), (13, 4, 5), (14, 5, 1)",
];
const FK_STATE: &[&str] = &["SELECT * FROM p ORDER BY id", "SELECT * FROM c ORDER BY id"];

/// A self-referencing foreign key: deleting a row cascades to rows of the
/// same table, some of which the scan has not reached yet.
const FK_SELF: &[&str] = &[
    "PRAGMA foreign_keys = ON",
    "CREATE TABLE t(id INTEGER PRIMARY KEY, parent INTEGER REFERENCES t(id) ON DELETE CASCADE, k INTEGER)",
    "INSERT INTO t VALUES (1, NULL, 0), (2, 1, 1), (3, 2, 0), (4, NULL, 1), (5, 4, 0), (6, 5, 1), (7, NULL, 0)",
];

const WITHOUT_ROWID: &[&str] = &[
    "CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID",
    "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f')",
];
const WITHOUT_ROWID_STATE: &[&str] = &["SELECT * FROM t ORDER BY k"];

const CASES: &[Case] = &[
    // Consecutive matches: a Next after each Delete would skip every other row.
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE k > 2",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE v <> 'zz'",
        states: ROWS_STATE,
    },
    // The first row, the last row, alternate rows, no row.
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE k % 7 = 1",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE k = 7",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE k % 2 = 0",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE k > 100",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE length(v) = 1 AND k BETWEEN 3 AND 5",
        states: ROWS_STATE,
    },
    // A WHERE that reads the target table: it must see the table as it was.
    // Deleting row 1 (no row 0) must not make row 2 deletable.
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM t AS u WHERE u.id = t.id - 1)",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE k <= (SELECT count(*) FROM t) - 4",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE id IN (SELECT id + 1 FROM t WHERE k % 2 = 1)",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE k > 4 RETURNING id, v",
        states: ROWS_STATE,
    },
    // Runs that empty leaves and rebalance; the table's last rows; all rows.
    Case {
        setup: MANY,
        statement: "DELETE FROM t WHERE id % 50 < 45",
        states: MANY_STATE,
    },
    Case {
        setup: MANY,
        statement: "DELETE FROM t WHERE id % 7 = 0",
        states: MANY_STATE,
    },
    Case {
        setup: MANY,
        statement: "DELETE FROM t WHERE id > 2900 OR id < 40",
        states: MANY_STATE,
    },
    Case {
        setup: MANY,
        statement: "DELETE FROM t WHERE a >= 0",
        states: MANY_STATE,
    },
    // Index entries of the deleted rows.
    Case {
        setup: INDEXED,
        statement: "DELETE FROM t WHERE id % 3 = 0",
        states: INDEXED_STATE,
    },
    Case {
        setup: INDEXED,
        statement: "DELETE FROM t WHERE substr(b, 2) + 0 > 1500",
        states: INDEXED_STATE,
    },
    // Triggers and foreign keys run around the program.
    Case {
        setup: TRIGGERS,
        statement: "DELETE FROM t WHERE k % 2 = 1",
        states: TRIGGER_STATE,
    },
    Case {
        setup: TRIGGER_DELETES_TARGET,
        statement: "DELETE FROM t WHERE k % 2 = 1",
        states: ROWS_STATE,
    },
    Case {
        setup: FK_CASCADE,
        statement: "DELETE FROM p WHERE k % 2 = 0",
        states: FK_STATE,
    },
    Case {
        setup: FK_SELF,
        statement: "DELETE FROM t WHERE k = 1",
        states: ROWS_STATE,
    },
    // The cascade reaches rows ahead of the scan that the statement does not
    // match (3 under 2, 5 and 6 under 4), so the count is comparable.
    Case {
        setup: FK_SELF,
        statement: "DELETE FROM t WHERE k = 1 AND id < 5",
        states: ROWS_STATE,
    },
    Case {
        setup: WITHOUT_ROWID,
        statement: "DELETE FROM t WHERE k % 2 = 0",
        states: WITHOUT_ROWID_STATE,
    },
];

/// Lever SD (bd-w4pr1.13.20): a plain DELETE's row on a leaf the transaction
/// already holds staged is deleted on the synchronous lane. MANY's rows with
/// a partial and an expression index, so each deleted row also loses its
/// index entries.
const SD_INDEXED: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 3000) \
     INSERT INTO t SELECT i, (i * 7919) % 1000, printf('%0100d', i) FROM n",
    "CREATE INDEX t_p ON t(a) WHERE a > 500",
    "CREATE INDEX t_e ON t(a + length(b))",
];
const SD_INDEXED_STATE: &[&str] = &[
    "SELECT changes(), total_changes()",
    "SELECT count(*), sum(id), sum(a) FROM t",
    "SELECT count(*), sum(id) FROM t INDEXED BY t_p WHERE a > 500",
    "SELECT count(*), sum(id) FROM t INDEXED BY t_e WHERE a + length(b) > 0",
    "PRAGMA integrity_check",
];
const SD_STATE: &[&str] = &[
    "SELECT changes(), total_changes()",
    "SELECT count(*), sum(id), sum(a), min(id), max(id) FROM t",
    "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id LIMIT 40)",
    "PRAGMA integrity_check",
];
/// The DELETE inside a transaction, under a savepoint the states roll back
/// to: the rows removed in place come back, and a second DELETE commits.
const SD_SAVEPOINT: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 3000) \
     INSERT INTO t SELECT i, (i * 7919) % 1000, printf('%0100d', i) FROM n",
    "BEGIN",
    "SAVEPOINT s1",
];
const SD_SAVEPOINT_STATE: &[&str] = &[
    "SELECT changes(), total_changes(), count(*), sum(id) FROM t",
    "ROLLBACK TO s1",
    "SELECT count(*), sum(id) FROM t",
    "DELETE FROM t WHERE id % 11 = 0",
    "COMMIT",
    "SELECT total_changes(), count(*), sum(id), sum(a) FROM t",
    "PRAGMA integrity_check",
];
/// A WHERE with a correlated subquery over another table.
const SD_CORRELATED: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 3000) \
     INSERT INTO t SELECT i, (i * 7919) % 1000, printf('%0100d', i) FROM n",
    "CREATE TABLE u(k INTEGER PRIMARY KEY, m INTEGER)",
    "INSERT INTO u VALUES (0, 900), (1, 100), (2, 500), (3, 0), (4, 999)",
];

const SD_CASES: &[Case] = &[
    // Every third row stays: two deletes in a row on one leaf, with the
    // cursor on the successor between them (a lane that forgot the pending
    // Next would skip the second).
    Case {
        setup: MANY,
        statement: "DELETE FROM t WHERE id % 3 <> 1",
        states: SD_STATE,
    },
    // The scorecard's shape, with index entries to remove.
    Case {
        setup: SD_INDEXED,
        statement: "DELETE FROM t WHERE id % 7 = 0",
        states: SD_INDEXED_STATE,
    },
    Case {
        setup: SD_SAVEPOINT,
        statement: "DELETE FROM t WHERE id % 5 = 0",
        states: SD_SAVEPOINT_STATE,
    },
    // Two passes: the second pass's SeekRowid + Delete take the lane.
    Case {
        setup: SD_CORRELATED,
        statement: "DELETE FROM t WHERE a > (SELECT m FROM u WHERE u.k = t.id % 5)",
        states: MANY_STATE,
    },
    Case {
        setup: MANY,
        statement: "DELETE FROM t WHERE id % 9 = 4 RETURNING id, a",
        states: MANY_STATE,
    },
];

/// Lever UR (bd-w4pr1.13.20): an UPDATE that keeps its rowid on a table
/// without a UNIQUE index cannot conflict, and its Delete keeps no copy of the
/// old row. The twin tables: `t` with a plain index only (the flag set), `u`
/// with a UNIQUE index on a column the UPDATEs below may or may not touch (the
/// flag absent; a conflict restores the old row).
const UR_TWINS: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
    "CREATE INDEX t_a ON t(a)",
    "CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, b TEXT UNIQUE)",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2000) \
     INSERT INTO t SELECT i, i % 13, 'k' || i FROM n",
    "INSERT INTO u SELECT * FROM t",
];
const UR_STATE: &[&str] = &[
    "SELECT count(*), sum(id), sum(a), sum(length(b)) FROM t",
    "SELECT a, count(*) FROM t INDEXED BY t_a WHERE a >= 0 GROUP BY a ORDER BY a",
    "SELECT count(*), sum(id), sum(a), sum(length(b)) FROM u",
    "SELECT group_concat(b) FROM (SELECT b FROM u WHERE id % 97 = 0 ORDER BY id)",
    "PRAGMA integrity_check",
];

const UR_CASES: &[Case] = &[
    // The flag set: same-size rewrites, rows that grow (the deferred delete
    // is materialized), and conflict clauses with nothing to conflict on.
    Case {
        setup: UR_TWINS,
        statement: "UPDATE t SET a = a + 1 WHERE id % 10 = 0",
        states: UR_STATE,
    },
    Case {
        setup: UR_TWINS,
        statement: "UPDATE t SET b = b || printf('%0200d', id) WHERE id % 7 = 0",
        states: UR_STATE,
    },
    Case {
        setup: UR_TWINS,
        statement: "UPDATE OR IGNORE t SET b = 'k1' WHERE id % 13 = 5",
        states: UR_STATE,
    },
    Case {
        setup: UR_TWINS,
        statement: "UPDATE OR REPLACE t SET a = NULL, b = 'k7' WHERE id % 13 = 5",
        states: UR_STATE,
    },
    // The flag absent: a UNIQUE conflict under IGNORE keeps each old row,
    // under REPLACE removes the victims, under ABORT fails mid-scan and rolls
    // the statement back; an UPDATE of another column on that table.
    Case {
        setup: UR_TWINS,
        statement: "UPDATE OR IGNORE u SET b = 'k1' WHERE id % 13 = 5",
        states: UR_STATE,
    },
    Case {
        setup: UR_TWINS,
        statement: "UPDATE OR REPLACE u SET b = 'k7' WHERE id % 13 = 5",
        states: UR_STATE,
    },
    Case {
        setup: UR_TWINS,
        statement: "UPDATE u SET b = 'k5' WHERE id > 3",
        states: UR_STATE,
    },
    Case {
        setup: UR_TWINS,
        statement: "UPDATE u SET a = a * 2 WHERE id % 3 = 0",
        states: UR_STATE,
    },
];

/// 600 short rows: an UPDATE that widens every third row to 300 bytes
/// splits leaves while the scan is still walking them.
const GROW: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 600) \
     INSERT INTO t SELECT i, i % 7, 'r' || i FROM n",
];
const GROW_STATE: &[&str] = &[
    "SELECT count(*), sum(id), sum(a), sum(length(b)), min(id), max(id) FROM t",
    "SELECT group_concat(id || ':' || length(b)) FROM (SELECT id, b FROM t ORDER BY id LIMIT 60)",
    "PRAGMA integrity_check",
];

const UPDATE_TRIGGERS: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT)",
    "CREATE TABLE log(seq INTEGER PRIMARY KEY, what TEXT, id INTEGER, old_k INTEGER, new_k INTEGER, total INTEGER)",
    "INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c'), (4, 4, 'd'), (5, 5, 'e'), (6, 6, 'f')",
    "CREATE TRIGGER t_bu BEFORE UPDATE ON t BEGIN \
     INSERT INTO log(what, id, old_k, new_k, total) VALUES ('before', old.id, old.k, new.k, (SELECT sum(k) FROM t)); END",
    "CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN \
     INSERT INTO log(what, id, old_k, new_k, total) VALUES ('after', old.id, old.k, new.k, (SELECT sum(k) FROM t)); END",
];

/// An UPDATE rewrites each matching row in the scan (one pass) when nothing
/// can observe the rewrite, and keeps the two passes for a changed rowid,
/// RETURNING, OR IGNORE / OR REPLACE and a subquery in SET or WHERE.
const UPDATE_CASES: &[Case] = &[
    // Consecutive matches; a row that grows a little; the WHERE reads the
    // column being set (each row is still met once).
    Case {
        setup: SMALL,
        statement: "UPDATE t SET v = v || 'xyz' WHERE k > 2",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "UPDATE t SET k = k + 10 WHERE k % 2 = 1",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "UPDATE t SET k = k * 2, v = NULL WHERE k < 5 OR k = 7",
        states: ROWS_STATE,
    },
    // Rows that grow across page splits mid-scan, rows that shrink, and the
    // scorecard's shape on 3,000 rows.
    Case {
        setup: GROW,
        statement: "UPDATE t SET b = printf('%0300d', id) WHERE id % 3 = 0",
        states: GROW_STATE,
    },
    Case {
        setup: MANY,
        statement: "UPDATE t SET b = b || b WHERE id % 5 = 0",
        states: MANY_STATE,
    },
    Case {
        setup: MANY,
        statement: "UPDATE t SET b = '' WHERE id % 2 = 0",
        states: MANY_STATE,
    },
    Case {
        setup: MANY,
        statement: "UPDATE t SET a = a + 1 WHERE id % 10 = 0",
        states: MANY_STATE,
    },
    // An indexed column, a UNIQUE indexed column.
    Case {
        setup: INDEXED,
        statement: "UPDATE t SET a = a + 1 WHERE id % 3 = 0",
        states: INDEXED_STATE,
    },
    Case {
        setup: INDEXED,
        statement: "UPDATE t SET b = b || '-u' WHERE id % 4 = 0",
        states: INDEXED_STATE,
    },
    // Two passes: OR IGNORE and OR REPLACE against the UNIQUE index.
    Case {
        setup: INDEXED,
        statement: "UPDATE OR IGNORE t SET b = 'k1' WHERE id % 13 = 5",
        states: INDEXED_STATE,
    },
    Case {
        setup: INDEXED,
        statement: "UPDATE OR REPLACE t SET b = 'k7' WHERE id % 13 = 5",
        states: INDEXED_STATE,
    },
    // Two passes: the rowid alias moves (each row must move once), a SET and
    // a WHERE that read the target table, RETURNING.
    Case {
        setup: SMALL,
        statement: "UPDATE t SET id = id + 1 WHERE k > 2",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "UPDATE t SET k = (SELECT max(k) FROM t) + 1 WHERE k % 2 = 0",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "UPDATE t SET k = k + 10 WHERE k < (SELECT avg(k) FROM t)",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "UPDATE t SET v = 'z' WHERE k > 4 RETURNING id, v",
        states: ROWS_STATE,
    },
    // Triggers and foreign keys run around the program.
    Case {
        setup: UPDATE_TRIGGERS,
        statement: "UPDATE t SET k = k + 100 WHERE k % 2 = 1",
        states: &[
            "SELECT rowid, * FROM t ORDER BY rowid",
            "SELECT what, id, old_k, new_k, total FROM log ORDER BY seq",
        ],
    },
    Case {
        setup: FK_CASCADE,
        statement: "UPDATE p SET k = k + 1 WHERE k % 2 = 0",
        states: FK_STATE,
    },
    Case {
        setup: FK_CASCADE,
        statement: "UPDATE c SET pid = 3 WHERE id % 2 = 0",
        states: FK_STATE,
    },
    Case {
        setup: WITHOUT_ROWID,
        statement: "UPDATE t SET v = v || '!' WHERE k % 2 = 0",
        states: WITHOUT_ROWID_STATE,
    },
];

/// Rowid reads beside column reads of the same rows (bd-w4pr1.13.20, lever R:
/// the scan's Rowid reads the leaf cell without the row-decode cache). The
/// INTEGER PRIMARY KEY column `id` fills the cache's rowid for a row, so a
/// Rowid that trusted the cache without checking the cursor's position would
/// return the previous row's rowid after Next; Column reads after a Rowid on a
/// moved, rewritten or regrown row must still see that row. `u` has an
/// implicit rowid and rows that grow past their cell when rewritten.
const ROWID_MIX: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1500) \
     INSERT INTO t SELECT i * 3, i % 11, printf('%040d', i) FROM n",
    "CREATE TABLE u(a INTEGER, b TEXT)",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1500) \
     INSERT INTO u SELECT i % 7, 'u' || i FROM n",
];
const ROWID_MIX_STATE: &[&str] = &[
    "SELECT rowid, a, rowid, id, b, rowid FROM t WHERE rowid % 9 = 0 ORDER BY rowid",
    "SELECT count(*), sum(rowid), sum(a * rowid), sum(id - rowid) FROM t WHERE a % 2 = rowid % 2",
    "SELECT id, rowid, a, rowid FROM t WHERE id % 7 = 1 ORDER BY id",
    "SELECT rowid, a, rowid, b FROM u WHERE rowid % 13 = 1 ORDER BY rowid",
    "SELECT count(*), sum(rowid), sum(a + rowid), max(length(b)) FROM u WHERE a = rowid % 5",
    "PRAGMA integrity_check",
];

const ROWID_CASES: &[Case] = &[
    Case {
        setup: ROWID_MIX,
        statement: "UPDATE t SET a = a + rowid % 5 WHERE id % 4 = 0",
        states: ROWID_MIX_STATE,
    },
    Case {
        setup: ROWID_MIX,
        statement: "UPDATE t SET b = b || rowid WHERE rowid % 6 = 0",
        states: ROWID_MIX_STATE,
    },
    Case {
        setup: ROWID_MIX,
        statement: "DELETE FROM t WHERE rowid % 6 = 3",
        states: ROWID_MIX_STATE,
    },
    Case {
        setup: ROWID_MIX,
        statement: "UPDATE u SET b = b || rowid || rowid WHERE rowid % 3 = 0",
        states: ROWID_MIX_STATE,
    },
    Case {
        setup: ROWID_MIX,
        statement: "DELETE FROM u WHERE a = rowid % 7",
        states: ROWID_MIX_STATE,
    },
];

/// INSERT ... SELECT row bodies whose second Affinity (after CHECK and NOT
/// NULL) still matters (bd-w4pr1.13.21, lever A): a NOT NULL ... ON CONFLICT
/// REPLACE column stores its DEFAULT over a NULL, and each DEFAULT here has
/// another storage class than its column (TEXT '7' into INTEGER, 3 into TEXT,
/// '2.5' into REAL), so only the second Affinity gives the stored value the
/// column's type; `d` has no constraint. `k` carries a CHECK over a row column,
/// and `p` none of them (its second Affinity is skipped).
const ROW_AFFINITY: &[&str] = &[
    "CREATE TABLE r(id INTEGER PRIMARY KEY, \
     a INTEGER NOT NULL ON CONFLICT REPLACE DEFAULT '7', \
     b TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 3, \
     c REAL NOT NULL ON CONFLICT REPLACE DEFAULT '2.5', d)",
    "CREATE TABLE k(id INTEGER PRIMARY KEY, a TEXT CHECK (length(a) >= 1 AND a <> 7), b NUMERIC)",
    "CREATE TABLE p(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d NUMERIC)",
    "CREATE TABLE src(x, y)",
    "INSERT INTO src VALUES (NULL, NULL), (1, 'q'), (NULL, 'z'), ('8', NULL), (9, 10), ('3.0', '4.5')",
];
const ROW_AFFINITY_STATE: &[&str] = &[
    "SELECT id, a, typeof(a), b, typeof(b), c, typeof(c), d, typeof(d) FROM r ORDER BY id",
    "SELECT id, a, typeof(a), b, typeof(b) FROM k ORDER BY id",
    "SELECT id, a, typeof(a), b, typeof(b), c, typeof(c), d, typeof(d) FROM p ORDER BY id",
];

/// Lever I (bd-w4pr1.13.21): an INSERT ... SELECT into a table with an
/// INTEGER PRIMARY KEY uses the alias register as the rowid register. The
/// sources give the alias NULL, an INTEGER, TEXT '7', REAL 8.0, a negative and
/// a value the table holds already; CHECKs read the alias and the rowid, and a
/// partial index covers the alias.
const IPK_SETUP: &[&str] = &[
    "CREATE TABLE s(k, v)",
    "INSERT INTO s VALUES (NULL, 'n1'), (5, 'i5'), ('7', 't7'), (8.0, 'r8'), (-3, 'neg'), \
     (NULL, 'n2'), (12, 'i12')",
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT CHECK (id <> 99), \
     w INTEGER DEFAULT 1 CHECK (rowid <> 98))",
    "CREATE INDEX t_part ON t(id) WHERE id > 6",
    "INSERT INTO t(id, v) VALUES (12, 'old12')",
    "CREATE TABLE ta(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
];
const IPK_STATE: &[&str] = &[
    "SELECT rowid, id, typeof(id), v, w FROM t ORDER BY rowid",
    "SELECT count(*), sum(id) FROM t INDEXED BY t_part WHERE id > 6",
    "SELECT rowid, id, v FROM ta ORDER BY rowid",
    "SELECT seq FROM sqlite_sequence WHERE name = 'ta'",
    "PRAGMA integrity_check",
];

const IPK_CASES: &[Case] = &[
    Case {
        setup: IPK_SETUP,
        statement: "INSERT INTO t(id, v) SELECT k, v FROM s WHERE k IS NOT 12",
        states: IPK_STATE,
    },
    Case {
        setup: IPK_SETUP,
        statement: "INSERT OR IGNORE INTO t(id, v) SELECT k, v FROM s",
        states: IPK_STATE,
    },
    Case {
        setup: IPK_SETUP,
        statement: "INSERT OR REPLACE INTO t(id, v) SELECT k, v FROM s",
        states: IPK_STATE,
    },
    // The duplicate 12 aborts the statement; 5.5 is not an integer key; the
    // CHECKs on the alias and on the rowid.
    Case {
        setup: IPK_SETUP,
        statement: "INSERT INTO t(id, v) SELECT k, v FROM s",
        states: IPK_STATE,
    },
    Case {
        setup: IPK_SETUP,
        statement: "INSERT INTO t(id, v) SELECT k + 0.5, v FROM s WHERE k = 5",
        states: IPK_STATE,
    },
    Case {
        setup: IPK_SETUP,
        statement: "INSERT INTO t(id, v) SELECT k + 94, v FROM s WHERE k = 5",
        states: IPK_STATE,
    },
    Case {
        setup: IPK_SETUP,
        statement: "INSERT INTO t(id, v) SELECT k + 93, v FROM s WHERE k = 5",
        states: IPK_STATE,
    },
    // The alias left out (every key generated), AUTOINCREMENT, RETURNING.
    Case {
        setup: IPK_SETUP,
        statement: "INSERT INTO t(v) SELECT v FROM s",
        states: IPK_STATE,
    },
    Case {
        setup: IPK_SETUP,
        statement: "INSERT INTO ta(id, v) SELECT k, v FROM s",
        states: IPK_STATE,
    },
    Case {
        setup: IPK_SETUP,
        statement: "INSERT INTO t(id, v) SELECT k, v FROM s WHERE k IS NOT 12 RETURNING rowid, id, v",
        states: IPK_STATE,
    },
    // Many rows from a recursive CTE, as the bulk row's generator.
    Case {
        setup: IPK_SETUP,
        statement: "INSERT INTO t(id, v, w) WITH RECURSIVE n(i) AS (SELECT 20 UNION ALL \
                    SELECT i + 1 FROM n WHERE i < 3000) SELECT i, 'g' || i, i % 7 FROM n",
        states: IPK_STATE,
    },
];

const ROW_AFFINITY_CASES: &[Case] = &[
    Case {
        setup: ROW_AFFINITY,
        statement: "INSERT INTO r(a, b, c, d) SELECT x, y, x, y FROM src",
        states: ROW_AFFINITY_STATE,
    },
    Case {
        setup: ROW_AFFINITY,
        statement: "INSERT INTO r(a, b, c, d) SELECT NULL, NULL, NULL, x FROM src",
        states: ROW_AFFINITY_STATE,
    },
    Case {
        setup: ROW_AFFINITY,
        statement: "INSERT OR REPLACE INTO r(a, b, c, d) SELECT y, x, y, x FROM src",
        states: ROW_AFFINITY_STATE,
    },
    Case {
        setup: ROW_AFFINITY,
        statement: "INSERT INTO k(a, b) SELECT y, x FROM src WHERE y IS NOT NULL AND x IS NOT NULL",
        states: ROW_AFFINITY_STATE,
    },
    Case {
        setup: ROW_AFFINITY,
        statement: "INSERT INTO p(a, b, c, d) SELECT x, x, y, y FROM src",
        states: ROW_AFFINITY_STATE,
    },
];

/// UPDATE row bodies (bd-w4pr1.13.20, lever U): only unassigned columns are
/// preloaded, a kept INTEGER PRIMARY KEY alias register is the re-insert's
/// rowid (and the one-pass scan reads no rowid of its own on such a table),
/// and the second Affinity runs only when a CHECK or a NOT NULL ... REPLACE
/// column can change a register. `t` has an IPK alias and mixed storage
/// classes, `u` an implicit rowid, `g` a STORED generated column over an
/// unassigned column, `r` NOT NULL ... REPLACE columns whose DEFAULT has
/// another storage class than the column, `k` a CHECK. A body that dropped
/// an unassigned column's preload stores NULL there (and computes `g.s`
/// from it); one that read the one-pass rowid from a skipped `Rowid` on `u`
/// re-inserts at the wrong key; one that skipped the second Affinity on `r`
/// stores TEXT '7' in the INTEGER column.
const UPDATE_BODY: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d)",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 400) \
     INSERT INTO t SELECT i, i % 9, 'b' || i, i * 0.5, \
     CASE i % 4 WHEN 0 THEN NULL WHEN 1 THEN i WHEN 2 THEN 'd' || i ELSE x'0102' END FROM n",
    "CREATE TABLE u(a INTEGER, b TEXT, c)",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 400) \
     INSERT INTO u SELECT i % 5, 'u' || i, i * 3 FROM n",
    "CREATE TABLE g(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, \
     s INTEGER GENERATED ALWAYS AS (a * 100 + b) STORED)",
    "INSERT INTO g(id, a, b) SELECT id, a, id FROM t WHERE id <= 60",
    "CREATE TABLE r(id INTEGER PRIMARY KEY, \
     a INTEGER NOT NULL ON CONFLICT REPLACE DEFAULT '7', \
     b TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 3, \
     c REAL NOT NULL ON CONFLICT REPLACE DEFAULT '2.5', d)",
    "INSERT INTO r(id, a, b, c, d) SELECT id, a, b, c, d FROM t WHERE id <= 40",
    "CREATE TABLE k(id INTEGER PRIMARY KEY, a TEXT CHECK (length(a) >= 1 AND a <> 7), b NUMERIC)",
    "INSERT INTO k(id, a, b) SELECT id, 'k' || id, id FROM t WHERE id <= 40",
];
const UPDATE_BODY_STATE: &[&str] = &[
    "SELECT rowid, id, a, typeof(a), b, typeof(b), c, typeof(c), d, typeof(d) FROM t ORDER BY id",
    "SELECT rowid, a, typeof(a), b, c, typeof(c) FROM u ORDER BY rowid",
    "SELECT id, a, b, s, typeof(s) FROM g ORDER BY id",
    "SELECT id, a, typeof(a), b, typeof(b), c, typeof(c), d, typeof(d) FROM r ORDER BY id",
    "SELECT id, a, typeof(a), b, typeof(b) FROM k ORDER BY id",
    "PRAGMA integrity_check",
];

const UPDATE_BODY_CASES: &[Case] = &[
    // Two columns swapped: each SET reads the old row, not a register.
    Case {
        setup: UPDATE_BODY,
        statement: "UPDATE t SET a = b, b = a WHERE id % 3 = 0",
        states: UPDATE_BODY_STATE,
    },
    Case {
        setup: UPDATE_BODY,
        statement: "UPDATE t SET (c, d) = (d, c) WHERE id % 5 = 1",
        states: UPDATE_BODY_STATE,
    },
    // A SET that reads the column it assigns, a column assigned twice (the
    // later one wins), and unassigned columns of every storage class kept.
    Case {
        setup: UPDATE_BODY,
        statement: "UPDATE t SET c = c + 0.25, a = a + 1, a = a * 10 WHERE id % 4 = 2",
        states: UPDATE_BODY_STATE,
    },
    // Affinity of the assigned value: TEXT into INTEGER / REAL, INTEGER into
    // TEXT, the rowid read through its alias.
    Case {
        setup: UPDATE_BODY,
        statement: "UPDATE t SET a = '0' || id, b = id, c = '1e1' WHERE id % 7 = 3",
        states: UPDATE_BODY_STATE,
    },
    // RETURNING the rowid and the alias (two passes, the alias register as the
    // re-insert rowid), on an alias table and on an implicit-rowid table.
    Case {
        setup: UPDATE_BODY,
        statement: "UPDATE t SET b = 'r' || rowid WHERE id % 50 = 7 RETURNING rowid, id, a, b, d",
        states: UPDATE_BODY_STATE,
    },
    Case {
        setup: UPDATE_BODY,
        statement: "UPDATE u SET b = b || '-' || rowid WHERE rowid % 60 = 1 RETURNING rowid, a, b, c",
        states: UPDATE_BODY_STATE,
    },
    // One pass on the implicit-rowid table: the scan's own Rowid is the key.
    Case {
        setup: UPDATE_BODY,
        statement: "UPDATE u SET c = c + rowid, a = a + 1 WHERE rowid % 3 = 0",
        states: UPDATE_BODY_STATE,
    },
    // The alias assigned: the row moves to the new key.
    Case {
        setup: UPDATE_BODY,
        statement: "UPDATE t SET id = id + 1000, a = -a WHERE id % 40 = 0",
        states: UPDATE_BODY_STATE,
    },
    // A STORED generated column over an unassigned column.
    Case {
        setup: UPDATE_BODY,
        statement: "UPDATE g SET a = a + 1 WHERE id % 2 = 0",
        states: UPDATE_BODY_STATE,
    },
    // NOT NULL ... REPLACE: the DEFAULT replaces the NULL and the second
    // Affinity gives it the column's type; and a plain assignment there.
    Case {
        setup: UPDATE_BODY,
        statement: "UPDATE r SET a = NULL, b = NULL, c = NULL WHERE id % 3 = 0",
        states: UPDATE_BODY_STATE,
    },
    Case {
        setup: UPDATE_BODY,
        statement: "UPDATE r SET a = '12', d = a WHERE id % 3 = 1",
        states: UPDATE_BODY_STATE,
    },
    // A CHECK over the assigned column.
    Case {
        setup: UPDATE_BODY,
        statement: "UPDATE k SET a = id * 2, b = '5' WHERE id % 4 = 0",
        states: UPDATE_BODY_STATE,
    },
];

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn stock_message(error: rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(m))
        | rusqlite::Error::SqlInputError { msg: m, .. } => m,
        other => other.to_string(),
    }
}

fn stock_rows(r: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut statement = r.prepare(sql).map_err(stock_message)?;
    let n = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .map_err(stock_message)
}

fn frank_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

fn stock_outcome(r: &rusqlite::Connection, sql: &str) -> String {
    if sql.contains("RETURNING") {
        return match stock_rows(r, sql) {
            Ok(rows) => format!("rows {rows:?}"),
            Err(m) => format!("error: {m}"),
        };
    }
    match r
        .prepare(sql)
        .and_then(|mut statement| statement.execute([]))
    {
        Ok(changed) => format!("changed {changed}"),
        Err(e) => format!("error: {}", stock_message(e)),
    }
}

async fn frank_outcome(f: &Connection, sql: &str, prepared: bool) -> String {
    if sql.contains("RETURNING") {
        return match f.query(sql).await {
            Ok(rows) => format!("rows {:?}", frank_rows(&rows)),
            Err(e) => format!("error: {e}"),
        };
    }
    let changed = if prepared {
        match f.prepare(sql).await {
            Ok(statement) => statement.execute().await,
            Err(e) => Err(e),
        }
    } else {
        f.execute(sql).await
    };
    match changed {
        Ok(changed) => format!("changed {changed}"),
        Err(e) => format!("error: {e}"),
    }
}

/// The one statement whose change count differs from stock for a reason
/// older than the one-pass delete (bd-n8g76, the same on the two-pass base):
/// a self-referencing cascade removes a row the statement also matches, stock
/// does not count that row and this engine, which runs FK actions after the
/// program, does. Its table state is still compared.
fn count_differs_by_bd_n8g76(case: &Case) -> bool {
    // By content: a `const` slice has no single address to compare.
    case.setup == FK_SELF && case.statement == "DELETE FROM t WHERE k = 1"
}

async fn check_case(case: &Case, file_backed: bool, prepared: bool) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = if file_backed {
        dir.path()
            .join("one_pass_delete.db")
            .to_str()
            .expect("utf-8 path")
            .to_owned()
    } else {
        ":memory:".to_owned()
    };
    let f = Connection::open(&path).await.expect("open");
    let r = rusqlite::Connection::open_in_memory().expect("stock open");
    for setup in case.setup {
        f.execute(setup).await.expect("frank setup");
        r.execute_batch(setup).expect("stock setup");
    }
    let mut failures = Vec::new();
    let label = format!(
        "[file_backed={file_backed} prepared={prepared}] `{}`",
        case.statement
    );
    let fo = frank_outcome(&f, case.statement, prepared).await;
    let so = stock_outcome(&r, case.statement);
    if fo != so && !count_differs_by_bd_n8g76(case) {
        failures.push(format!("{label}: frank {fo:?} vs stock {so:?}"));
    }
    for state in case.states {
        let fv = match f.query(state).await {
            Ok(rows) => Ok(frank_rows(&rows)),
            Err(e) => Err(e.to_string()),
        };
        let sv = stock_rows(&r, state);
        if fv != sv {
            failures.push(format!(
                "{label}, then `{state}`: frank {fv:?} vs stock {sv:?}"
            ));
        }
    }
    f.close().await.expect("close");
    failures
}

/// Every case on `:memory:` and on a file, direct and prepared.
async fn check_cases(cases: &[Case]) -> Vec<String> {
    let mut failures = Vec::new();
    for case in cases {
        // RETURNING rows of a prepared DML statement are not readable
        // through `PreparedStatement::query`; that case runs directly.
        let modes: &[bool] = if case.statement.contains("RETURNING") {
            &[false]
        } else {
            &[false, true]
        };
        for file_backed in [false, true] {
            for &prepared in modes {
                failures.extend(check_case(case, file_backed, prepared).await);
            }
        }
    }
    failures
}

#[test]
fn scan_delete_matches_stock_in_one_pass_and_two() {
    asupersync::test_utils::run_test(|| async {
        let failures = check_cases(CASES).await;
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

#[test]
fn staged_leaf_delete_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let failures = check_cases(SD_CASES).await;
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

/// Lever SD: a second connection's open snapshot keeps the rows another
/// connection's transaction deletes in place, before and after that commits,
/// and its next transaction sees the deletes; the deleting connection sees
/// its own. Compared step by step with stock on the same steps in WAL mode.
#[test]
fn staged_leaf_delete_is_invisible_to_an_open_snapshot_as_in_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let frank_path = dir
            .path()
            .join("frank.db")
            .to_str()
            .expect("utf-8 path")
            .to_owned();
        let stock_path = dir.path().join("stock.db");
        let f1 = Connection::open(&frank_path).await.expect("open");
        let s1 = rusqlite::Connection::open(&stock_path).expect("stock open");
        f1.query("PRAGMA journal_mode=WAL").await.expect("frank wal");
        stock_rows(&s1, "PRAGMA journal_mode=WAL").expect("stock wal");
        for setup in MANY {
            f1.execute(setup).await.expect("frank setup");
            s1.execute_batch(setup).expect("stock setup");
        }
        let f2 = Connection::open(&frank_path).await.expect("open second");
        let s2 = rusqlite::Connection::open(&stock_path).expect("stock open second");
        let steps: &[(u8, &str)] = &[
            (2, "BEGIN"),
            (2, "SELECT count(*), sum(id) FROM t"),
            (1, "BEGIN"),
            (1, "DELETE FROM t WHERE id % 7 = 0"),
            (1, "SELECT count(*), sum(id) FROM t"),
            (2, "SELECT count(*), sum(id) FROM t"),
            (1, "DELETE FROM t WHERE id % 3 = 0"),
            (1, "COMMIT"),
            (2, "SELECT count(*), sum(id) FROM t"),
            (2, "COMMIT"),
            (2, "SELECT count(*), sum(id) FROM t"),
            (1, "PRAGMA integrity_check"),
        ];
        let mut failures = Vec::new();
        for &(which, sql) in steps {
            let (f, s) = if which == 1 { (&f1, &s1) } else { (&f2, &s2) };
            let fv = match f.query(sql).await {
                Ok(rows) => Ok(frank_rows(&rows)),
                Err(e) => Err(e.to_string()),
            };
            let sv = stock_rows(s, sql);
            if fv != sv {
                failures.push(format!(
                    "connection {which}, `{sql}`: frank {fv:?} vs stock {sv:?}"
                ));
            }
        }
        f2.close().await.expect("close second");
        f1.close().await.expect("close");
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

#[test]
fn insert_select_alias_rowid_register_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let failures = check_cases(IPK_CASES).await;
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

#[test]
fn update_without_restore_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let failures = check_cases(UR_CASES).await;
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

#[test]
fn scan_update_matches_stock_in_one_pass_and_two() {
    asupersync::test_utils::run_test(|| async {
        let failures = check_cases(UPDATE_CASES).await;
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

#[test]
fn insert_select_affinity_after_constraints_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let failures = check_cases(ROW_AFFINITY_CASES).await;
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

#[test]
fn update_row_body_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let failures = check_cases(UPDATE_BODY_CASES).await;
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

#[test]
fn rowid_reads_beside_column_reads_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let failures = check_cases(ROWID_CASES).await;
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

/// Deletes and same-size rewrites on a leaf the transaction already staged
/// change the staged image in place (bd-w4pr1.13.20, lever F), so what a
/// savepoint and a failed statement undo was changed in place as well. The
/// first statements stage every leaf; the ones after the savepoint and the
/// two that fail partway (an integer overflow past a given rowid, after
/// rewriting or deleting the rows before it) run on staged leaves.
const IN_PLACE_SCRIPT: &[&str] = &[
    "BEGIN",
    "DELETE FROM t WHERE id % 7 = 0",
    "UPDATE t SET a = a + 1 WHERE id % 10 = 0",
    "SAVEPOINT s1",
    "DELETE FROM t WHERE id % 5 = 0",
    "UPDATE t SET a = a + 2 WHERE id % 3 = 0",
    "ROLLBACK TO s1",
    "UPDATE t SET a = a + 3 WHERE id % 4 = 0",
    "UPDATE t SET a = a + 1 WHERE id % 2 = 0 AND \
     (CASE WHEN id > 2500 THEN abs(id - id - 9223372036854775807 - 1) ELSE 1 END)",
    "DELETE FROM t WHERE id % 3 = 0 AND \
     (CASE WHEN id > 2600 THEN abs(id - id - 9223372036854775807 - 1) ELSE 1 END)",
    "RELEASE s1",
    "DELETE FROM t WHERE id % 11 = 0",
    "COMMIT",
];

/// An outcome with an error's text dropped (the two engines word errors
/// differently), and with no count for a transaction-control statement
/// (stock's `execute` reports the last DML statement's count for those).
fn outcome_kind<'a>(statement: &str, outcome: &'a str) -> &'a str {
    if outcome.starts_with("error") {
        "error"
    } else if statement.starts_with("DELETE") || statement.starts_with("UPDATE") {
        outcome
    } else {
        "ok"
    }
}

#[test]
fn scan_dml_on_staged_leaves_rolls_back_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().expect("temp dir");
            let path = if file_backed {
                dir.path()
                    .join("in_place.db")
                    .to_str()
                    .expect("utf-8 path")
                    .to_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&path).await.expect("open");
            let r = rusqlite::Connection::open_in_memory().expect("stock open");
            for setup in MANY {
                f.execute(setup).await.expect("frank setup");
                r.execute_batch(setup).expect("stock setup");
            }
            let mut errors = 0;
            for statement in IN_PLACE_SCRIPT {
                let fo = frank_outcome(&f, statement, false).await;
                let so = stock_outcome(&r, statement);
                if so.starts_with("error") {
                    errors += 1;
                }
                if outcome_kind(statement, &fo) != outcome_kind(statement, &so) {
                    failures.push(format!(
                        "[file_backed={file_backed}] `{statement}`: frank {fo:?} vs stock {so:?}"
                    ));
                }
                let state = "SELECT count(*), sum(id), sum(a), total(length(b)) FROM t";
                let fv = f.query(state).await.map(|rows| frank_rows(&rows));
                let sv = stock_rows(&r, state);
                if !matches!((&fv, &sv), (Ok(a), Ok(b)) if a == b) {
                    failures.push(format!(
                        "[file_backed={file_backed}] after `{statement}`: frank {fv:?} vs stock {sv:?}"
                    ));
                }
            }
            assert_eq!(errors, 2, "the two failing statements fail on stock");
            for state in MANY_STATE {
                let fv = f.query(state).await.map(|rows| frank_rows(&rows));
                let sv = stock_rows(&r, state);
                if !matches!((&fv, &sv), (Ok(a), Ok(b)) if a == b) {
                    failures.push(format!(
                        "[file_backed={file_backed}] final `{state}`: frank {fv:?} vs stock {sv:?}"
                    ));
                }
            }
            f.close().await.expect("close");
        }
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

/// Opcode names of `EXPLAIN <statement>` on `f`.
async fn explain_opcodes(f: &Connection, statement: &str) -> Vec<String> {
    f.query(&format!("EXPLAIN {statement}"))
        .await
        .expect("explain")
        .iter()
        .map(|row| match &row.values()[1] {
            SqliteValue::Text(name) => name.to_string(),
            other => format!("{other:?}"),
        })
        .collect()
}

/// The UPDATE shapes: the plain scan rewrites in place without a RowSet; a
/// changed rowid alias, RETURNING, OR IGNORE and a subquery reading the
/// target keep the two passes.
#[test]
fn scan_update_is_one_pass_only_where_nothing_can_observe_it() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("open");
        for setup in SMALL {
            f.execute(setup).await.expect("setup");
        }
        for (statement, one_pass) in [
            ("UPDATE t SET v = v || 'xyz' WHERE k > 2", true),
            ("UPDATE t SET k = k + 10 WHERE k % 2 = 1", true),
            ("UPDATE t SET id = id + 1 WHERE k > 2", false),
            ("UPDATE t SET v = 'z' WHERE k > 4 RETURNING id", false),
            ("UPDATE OR IGNORE t SET v = 'z' WHERE k > 4", false),
            ("UPDATE t SET k = (SELECT max(k) FROM t) WHERE k > 4", false),
        ] {
            let program = explain_opcodes(&f, statement).await;
            assert_eq!(
                !program.iter().any(|op| op == "RowSetAdd"),
                one_pass,
                "`{statement}` one pass expected {one_pass}: {program:?}"
            );
            let insert = program.iter().position(|op| op == "Insert").expect("Insert");
            let next = program.iter().rposition(|op| op == "Next").expect("Next");
            assert_eq!(
                insert < next,
                one_pass,
                "`{statement}` rewrites inside the scan exactly in one pass: {program:?}"
            );
        }
    });
}

/// The program shape this test file exists for: with no subquery and no
/// RETURNING the scan deletes in place (no RowSet); a subquery that reads the
/// target table, or RETURNING, keeps the two passes.
#[test]
fn scan_delete_is_one_pass_only_where_nothing_can_observe_it() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("open");
        for setup in SMALL {
            f.execute(setup).await.expect("setup");
        }
        let opcodes = |rows: Vec<fsqlite_core::connection::Row>| -> Vec<String> {
            rows.iter()
                .map(|row| match &row.values()[1] {
                    SqliteValue::Text(name) => name.to_string(),
                    other => format!("{other:?}"),
                })
                .collect()
        };
        for (statement, one_pass) in [
            ("DELETE FROM t WHERE k % 2 = 0", true),
            ("DELETE FROM t WHERE v <> 'x' AND k > 1", true),
            (
                "DELETE FROM t WHERE k <= (SELECT count(*) FROM t) - 4",
                false,
            ),
            ("DELETE FROM t WHERE k > 4 RETURNING id", false),
        ] {
            let program = opcodes(
                f.query(&format!("EXPLAIN {statement}"))
                    .await
                    .expect("explain"),
            );
            let uses_rowset = program.iter().any(|op| op == "RowSetAdd");
            assert_eq!(
                !uses_rowset, one_pass,
                "`{statement}` one pass expected {one_pass}: {program:?}"
            );
            if one_pass {
                let delete = program
                    .iter()
                    .position(|op| op == "Delete")
                    .expect("Delete");
                let next = program.iter().rposition(|op| op == "Next").expect("Next");
                assert!(
                    delete < next,
                    "`{statement}` deletes inside the scan: {program:?}"
                );
            }
        }
    });
}
