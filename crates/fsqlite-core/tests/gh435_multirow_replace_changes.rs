//! GH#435/GH#495: multi-row INSERT change counts match stock SQLite across
//! morsels, storage modes, conflict policies, and RETURNING. REPLACE counts
//! each inserted row once; IGNORE skips conflicts and FAIL retains its prefix.
#![recursion_limit = "512"]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "
    CREATE TABLE runs (run_id TEXT PRIMARY KEY NOT NULL);
    CREATE TABLE interactions (
      run_id TEXT NOT NULL, tick INTEGER NOT NULL CHECK (tick >= 0),
      seq INTEGER NOT NULL CHECK (seq >= 0), actor_agent_uid INTEGER, target_agent_uid INTEGER,
      kind TEXT NOT NULL CHECK (kind <> ''), value REAL, payload_json TEXT NOT NULL,
      island_id INTEGER NOT NULL,
      PRIMARY KEY (run_id, tick, seq), FOREIGN KEY (run_id) REFERENCES runs (run_id));
    CREATE INDEX i_actor ON interactions (run_id, actor_agent_uid, tick, seq);
    CREATE INDEX i_target ON interactions (run_id, target_agent_uid, tick, seq);
    INSERT INTO runs VALUES ('r');
";

fn insert_sql(rows: usize) -> String {
    let mut sql = String::from(
        "INSERT OR REPLACE INTO interactions (run_id, tick, seq, actor_agent_uid, \
         target_agent_uid, kind, value, payload_json, island_id) VALUES ",
    );
    for row in 0..rows {
        if row > 0 {
            sql.push_str(", ");
        }
        let base = row * 9;
        let params: Vec<String> = (1..=9).map(|i| format!("?{}", base + i)).collect();
        sql.push_str(&format!("({})", params.join(", ")));
    }
    sql
}

fn params(rows: usize, tick: i64) -> Vec<SqliteValue> {
    (0..rows)
        .flat_map(|i| {
            let i = i64::try_from(i).expect("row index");
            [
                SqliteValue::Text("r".into()),
                SqliteValue::Integer(tick),
                SqliteValue::Integer(i),
                SqliteValue::Integer(i % 60),
                SqliteValue::Integer((i * 7) % 60),
                SqliteValue::Text("contact".into()),
                SqliteValue::Float(0.5),
                SqliteValue::Text("{\"d\":1}".into()),
                SqliteValue::Integer(0),
            ]
        })
        .collect()
}

fn integer(value: &SqliteValue) -> usize {
    match value {
        SqliteValue::Integer(n) => usize::try_from(*n).expect("non-negative"),
        other => panic!("expected an integer, got {other:?}"),
    }
}

#[test]
fn multirow_insert_or_replace_counts_every_row() {
    asupersync::test_utils::run_test(|| async {
        for foreign_keys in ["OFF", "ON"] {
            for rows in [1_usize, 63, 64, 65, 113, 129] {
                let conn = Connection::open(":memory:").await.expect("open");
                conn.execute(&format!("PRAGMA foreign_keys = {foreign_keys}"))
                    .await
                    .expect("foreign_keys");
                conn.execute_batch(SCHEMA).await.expect("schema");
                let sql = insert_sql(rows);
                let fresh = conn
                    .execute_with_params(&sql, &params(rows, 0))
                    .await
                    .expect("insert");
                // Replacing every row writes every row again.
                let replaced = conn
                    .execute_with_params(&sql, &params(rows, 0))
                    .await
                    .expect("replace");
                let sql_changes = conn.query("SELECT changes()").await.expect("changes()");
                let sql_changes = integer(&sql_changes[0].values()[0]);
                let stored = conn
                    .query("SELECT count(*) FROM interactions")
                    .await
                    .expect("count");
                let stored = integer(&stored[0].values()[0]);
                assert_eq!(
                    (fresh, replaced, sql_changes, stored),
                    (rows, rows, rows, rows),
                    "foreign_keys={foreign_keys} rows={rows}"
                );
                conn.close().await.expect("close");
            }
        }
    });
}

// GH#495 reported the same 100 -> 36 symptom on v0.4.6, using a file-backed
// 13-column table, explicit transactions, and 1,300 numbered bindings. Keep
// this shape alongside GH#435: neither the returned count nor count(*) alone
// proves that REPLACE preserved the rows and SQLite's change accounting.
const EVENT_SCHEMA: &str = "
    PRAGMA foreign_keys = OFF;
    CREATE TABLE runs (run_id TEXT PRIMARY KEY);
    CREATE TABLE ev (
      run_id TEXT, tick INT, seq INT, agent_uid INT, scope TEXT,
      event_type TEXT, payload TEXT, px REAL, py REAL, cu INT,
      cx REAL, cy REAL, island INT,
      PRIMARY KEY (run_id, island, tick, seq),
      FOREIGN KEY (run_id) REFERENCES runs(run_id));
    CREATE INDEX ev_agent ON ev (run_id, agent_uid, tick, seq);
    INSERT INTO runs VALUES ('r');
";
const EVENT_COLUMNS: &str =
    "run_id,tick,seq,agent_uid,scope,event_type,payload,px,py,cu,cx,cy,island";

fn event_sql(rows: usize, verb: &str, returning: bool) -> String {
    let values = (0..rows)
        .map(|row| {
            let bindings = (1..=13)
                .map(|column| format!("?{}", row * 13 + column))
                .collect::<Vec<_>>()
                .join(",");
            format!("({bindings})")
        })
        .collect::<Vec<_>>()
        .join(",");
    let mut sql = format!("{verb} INTO ev ({EVENT_COLUMNS}) VALUES {values}");
    if returning {
        sql.push_str(" RETURNING ");
        sql.push_str(EVENT_COLUMNS);
    }
    sql
}

fn event_params(
    rows: usize,
    tick: i64,
    offset: usize,
    version: usize,
    distinct_keys: usize,
) -> Vec<SqliteValue> {
    let generation = i64::try_from(version).expect("small version");
    (0..rows)
        .flat_map(|row| {
            let seq = i64::try_from(offset + row % distinct_keys).expect("seq fits");
            let ordinal = i64::try_from(row).expect("small row index");
            let coordinate = f64::from(u32::try_from(row).expect("small coordinate"));
            [
                SqliteValue::Text("r".into()),
                SqliteValue::Integer(tick),
                SqliteValue::Integer(seq),
                SqliteValue::Integer(ordinal % 60 + generation * 1_000),
                SqliteValue::Text("world".into()),
                SqliteValue::Text("move".into()),
                SqliteValue::Text(
                    format!(
                        "version={version};seq={seq};ordinal={ordinal};body={}",
                        "x".repeat(row % 31 + 1)
                    )
                    .into(),
                ),
                SqliteValue::Float(coordinate + 0.25),
                SqliteValue::Float(-coordinate - 0.5),
                SqliteValue::Integer(ordinal % 7),
                SqliteValue::Float(0.5),
                SqliteValue::Float(-0.25),
                SqliteValue::Integer(0),
            ]
        })
        .collect()
}

fn stock_value(value: &SqliteValue) -> rusqlite::types::Value {
    match value {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(value) => rusqlite::types::Value::Integer(*value),
        SqliteValue::Float(value) => rusqlite::types::Value::Real(*value),
        SqliteValue::Text(value) => rusqlite::types::Value::Text(value.to_string()),
        SqliteValue::Blob(value) => rusqlite::types::Value::Blob(value.to_vec()),
    }
}

fn stock_rows(
    stock: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Vec<Vec<SqliteValue>> {
    let mut statement = stock.prepare(sql).expect("stock prepare");
    let columns = statement.column_count();
    statement
        .query_map(rusqlite::params_from_iter(params.iter().map(stock_value)), |row| {
            (0..columns)
                .map(|column| {
                    row.get::<_, rusqlite::types::Value>(column).map(|value| match value {
                        rusqlite::types::Value::Null => SqliteValue::Null,
                        rusqlite::types::Value::Integer(value) => SqliteValue::Integer(value),
                        rusqlite::types::Value::Real(value) => SqliteValue::Float(value),
                        rusqlite::types::Value::Text(value) => SqliteValue::Text(value.into()),
                        rusqlite::types::Value::Blob(value) => SqliteValue::Blob(value.into()),
                    })
                })
                .collect()
        })
        .expect("stock query")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("stock rows")
}

async fn assert_event_rows(
    conn: &Connection,
    stock: &rusqlite::Connection,
    count: usize,
    label: &str,
) {
    // Compare every stored column through both the table and its secondary
    // index. Changing agent_uid on replacement must remove old index entries.
    for source in ["NOT INDEXED", "INDEXED BY ev_agent"] {
        let sql = format!(
            "SELECT {EVENT_COLUMNS} FROM ev {source} ORDER BY run_id,island,tick,seq"
        );
        let expected = stock_rows(stock, &sql, &[]);
        assert_eq!(expected.len(), count, "{label}: stock stored rows");
        let actual = conn
            .query(&sql)
            .await
            .unwrap_or_else(|error| panic!("{label}: {source}: {error}"));
        let actual = actual.iter().map(|row| row.values().to_vec()).collect::<Vec<_>>();
        assert_eq!(actual, expected, "{label}: {source}: stored rows");
    }
}

struct EventDatabases {
    frank: Connection,
    stock: rusqlite::Connection,
    path: Option<std::path::PathBuf>,
    _directory: tempfile::TempDir,
}

impl EventDatabases {
    async fn open(file_backed: bool) -> Self {
        Self::open_with_schema(file_backed, EVENT_SCHEMA).await
    }

    async fn open_with_schema(file_backed: bool, schema: &str) -> Self {
        let directory = tempfile::tempdir().expect("temporary database directory");
        let path = file_backed.then(|| directory.path().join("events.db"));
        let frank = Connection::open(
            path.as_ref()
                .map_or(":memory:", |path| path.to_str().expect("UTF-8 test path")),
        )
        .await
        .expect("open FrankenSQLite");
        let stock = if file_backed {
            rusqlite::Connection::open(directory.path().join("stock.db"))
        } else {
            rusqlite::Connection::open_in_memory()
        }
        .expect("open stock SQLite");
        frank.execute_batch(schema).await.expect("create ev schema");
        stock.execute_batch(schema).expect("create stock ev schema");
        Self { frank, stock, path, _directory: directory }
    }

    async fn control(&self, sql: &str) {
        self.frank.execute(sql).await.expect("transaction control");
        self.stock.execute_batch(sql).expect("stock transaction control");
    }

    async fn assert_state(&self, counts: [usize; 3], label: &str) {
        let sql = "SELECT changes(), total_changes(), (SELECT count(*) FROM ev)";
        let expected = stock_rows(&self.stock, sql, &[]);
        assert_eq!(
            [integer(&expected[0][0]), integer(&expected[0][1]), integer(&expected[0][2])],
            counts,
            "{label}: stock changes/total_changes/stored rows"
        );
        let actual = self.frank.query(sql).await.expect("FrankenSQLite counters");
        assert_eq!(actual[0].values(), expected[0].as_slice(), "{label}: counters");
        assert_event_rows(&self.frank, &self.stock, counts[2], label).await;
    }

    async fn write(&self, verb: &str, params: &[SqliteValue], returning: bool, label: &str) {
        assert_eq!(params.len() % 13, 0, "complete ev rows");
        let rows = params.len() / 13;
        let sql = event_sql(rows, verb, returning);
        if returning {
            let mut expected = stock_rows(&self.stock, &sql, params);
            let mut actual = self.frank.query_with_params(&sql, params).await
                .unwrap_or_else(|error| panic!("{label}: RETURNING: {error}"))
                .iter().map(|row| row.values().to_vec()).collect::<Vec<_>>();
            // RETURNING order is unspecified. Compare all columns as a
            // multiset, retaining duplicates from repeated primary keys.
            expected.sort_by_cached_key(|row| format!("{row:?}"));
            actual.sort_by_cached_key(|row| format!("{row:?}"));
            assert_eq!(expected.len(), rows, "{label}: stock RETURNING count");
            assert_eq!(actual, expected, "{label}: RETURNING rows");
        } else {
            let expected = self.stock.execute(
                &sql, rusqlite::params_from_iter(params.iter().map(stock_value)),
            ).unwrap_or_else(|error| panic!("{label}: stock insert: {error}"));
            let actual = self.frank.execute_with_params(&sql, params).await
                .unwrap_or_else(|error| panic!("{label}: insert: {error}"));
            assert_eq!(expected, rows, "{label}: stock affected rows");
            assert_eq!(actual, expected, "{label}: affected rows");
        }
    }

    async fn batch(
        &self,
        verb: &str,
        params: &[SqliteValue],
        returning: bool,
        counts: [usize; 3],
        label: &str,
    ) {
        self.control("BEGIN").await;
        self.write(verb, params, returning, label).await;
        self.assert_state(counts, label).await;
        self.control("COMMIT").await;
        self.assert_state(counts, label).await;
    }

    async fn close(self, label: &str) {
        self.frank.close().await.expect("close FrankenSQLite");
        if let Some(path) = self.path {
            let sql = format!("SELECT {EVENT_COLUMNS} FROM ev ORDER BY run_id,island,tick,seq");
            let expected = stock_rows(&self.stock, &sql, &[]);
            {
                // Stock SQLite independently validates the file before any
                // FrankenSQLite reopen can repair or otherwise alter it.
                let persisted = rusqlite::Connection::open(&path).expect("open persisted ev database");
                assert_eq!(stock_rows(&persisted, &sql, &[]), expected, "{label}: persisted rows");
                assert_eq!(
                    stock_rows(&persisted, "PRAGMA integrity_check", &[]),
                    vec![vec![SqliteValue::Text("ok".into())]],
                    "{label}: persisted database integrity"
                );
            }
            let reopened = Connection::open(path.to_str().expect("UTF-8 test path"))
                .await.expect("reopen FrankenSQLite");
            assert_event_rows(&reopened, &self.stock, expected.len(), label).await;
            reopened.close().await.expect("close reopened FrankenSQLite");
        }
    }
}

#[test]
fn gh495_event_counts_match_sqlite_at_morsel_boundaries() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for rows in [1_usize, 63, 64, 65, 100, 127, 128, 129] {
                for returning in [false, true] {
                    let databases = EventDatabases::open(file_backed).await;
                    let repeated_keys = rows.min(3);
                    let mixed_count = 2 * rows + rows / 2;
                    let label = format!("file={file_backed} rows={rows} returning={returning}");
                    // Fresh plain INSERT, fresh REPLACE, full replacement with
                    // changed payloads/index keys, then mixed old and new keys.
                    // Both APIs also repeat keys within/across morsels: every
                    // insertion counts although only its last version stays.
                    for (phase, verb, tick, offset, keys, stored) in [
                        (0, "INSERT", 1, 0, rows, rows),
                        (1, "INSERT OR REPLACE", 0, 0, rows, 2 * rows),
                        (2, "INSERT OR REPLACE", 0, 0, rows, 2 * rows),
                        (3, "INSERT OR REPLACE", 0, rows / 2, rows, mixed_count),
                        (4, "INSERT OR REPLACE", 2, 0, repeated_keys, mixed_count + repeated_keys),
                        (5, "INSERT OR REPLACE", 2, 0, repeated_keys, mixed_count + repeated_keys),
                    ] {
                        databases.batch(
                            verb,
                            &event_params(rows, tick, offset, phase, keys),
                            returning,
                            [rows, 1 + (phase + 1) * rows, stored],
                            &format!("{label} phase={phase}"),
                        ).await;
                    }
                    databases.close(&label).await;
                }
            }
        }
    });
}

#[test]
fn gh495_thirty_file_backed_batches_report_one_hundred_changes_each() {
    asupersync::test_utils::run_test(|| async {
        let databases = EventDatabases::open(true).await;
        for batch in 0..30 {
            let stored = usize::try_from(batch + 1).expect("small batch") * 100;
            databases.batch(
                "INSERT OR REPLACE",
                &event_params(100, batch, 0, 0, 100),
                false,
                [100, 1 + stored, stored],
                &format!("GH#495 100-row batch={batch}"),
            ).await;
        }
        databases.assert_state([100, 3001, 3000], "GH#495 30 batches").await;
        databases.close("GH#495 30 batches").await;
    });
}

#[test]
fn gh495_execute_returning_one_counts_100_fresh_and_replaced_rows() {
    asupersync::test_utils::run_test(|| async {
        let sql = format!("{} RETURNING 1", event_sql(100, "INSERT OR REPLACE", false));
        for file_backed in [false, true] {
            let databases = EventDatabases::open(file_backed).await;
            let label = format!("GH#495 execute RETURNING 1 file={file_backed}");
            databases.control("BEGIN").await;
            for version in 0..2 {
                let params = event_params(100, 0, 0, version, 100);
                // rusqlite::execute rejects row-producing SQL. Drain the stock
                // RETURNING result so its change counters have been finalized.
                let expected = stock_rows(&databases.stock, &sql, &params);
                assert_eq!(expected, vec![vec![SqliteValue::Integer(1)]; 100],
                    "{label}: stock RETURNING rows");
                let actual = databases.frank.execute_with_params(&sql, &params).await
                    .expect("execute INSERT OR REPLACE RETURNING 1");
                assert_eq!(actual, expected.len(), "{label}: affected rows");
                databases.assert_state([100, 1 + (version + 1) * 100, 100], &label).await;
            }
            databases.control("COMMIT").await;
            databases.assert_state([100, 201, 100], &label).await;
            databases.close(&label).await;
        }
    });
}

#[test]
fn gh495_ignore_counts_only_rows_inserted_across_morsels() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for query_api in [false, true] {
                let label = format!("GH#495 IGNORE file={file_backed} query_api={query_api}");
                let databases = EventDatabases::open(file_backed).await;
                let mut seed = event_params(3, 0, 0, 9, 3);
                for (row, key) in [0_i64, 64, 128].into_iter().enumerate() {
                    seed[row * 13 + 2] = SqliteValue::Integer(key);
                }
                databases.write("INSERT", &seed, false, &label).await;
                let params = event_params(129, 0, 0, 1, 129);
                let sql = event_sql(129, "INSERT OR IGNORE", false);
                databases.control("BEGIN").await;
                // Conflicts straddle both 64-row boundaries. An entirely
                // skipped final morsel must not zero the statement count;
                // skipping every row on the next call must reset it to zero.
                for inserted in [126, 0] {
                    let expected = databases.stock.execute(
                        &sql, rusqlite::params_from_iter(params.iter().map(stock_value)),
                    ).expect("stock INSERT OR IGNORE");
                    assert_eq!(expected, inserted, "{label}: stock affected rows");
                    if query_api {
                        let actual = databases.frank.query_with_params(&sql, &params).await
                            .expect("query INSERT OR IGNORE");
                        assert!(actual.is_empty(), "{label}: no RETURNING rows");
                    } else {
                        let actual = databases.frank.execute_with_params(&sql, &params).await
                            .expect("execute INSERT OR IGNORE");
                        assert_eq!(actual, expected, "{label}: affected rows");
                    }
                    databases.assert_state([inserted, 130, 129], &label).await;
                }
                databases.control("COMMIT").await;
                databases.assert_state([0, 130, 129], &label).await;
                databases.close(&label).await;
            }
        }
    });
}

#[test]
fn gh495_fail_preserves_prior_morsels_in_change_counters() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prefix in [0_usize, 63, 64, 69, 128] {
                let label = format!("GH#495 FAIL file={file_backed} successful_prefix={prefix}");
                let databases = EventDatabases::open(file_backed).await;
                databases.write("INSERT", &event_params(1, 0, prefix, 9, 1), false, &label).await;
                let params = event_params(129, 0, 0, 1, 129);
                let sql = event_sql(129, "INSERT OR FAIL", false);
                databases.control("BEGIN").await;
                let expected = databases.stock.execute(
                    &sql, rusqlite::params_from_iter(params.iter().map(stock_value)),
                ).expect_err("stock primary-key conflict");
                assert_eq!(expected.sqlite_error_code(),
                    Some(rusqlite::ErrorCode::ConstraintViolation), "{label}: stock error");
                let actual = databases.frank.query_with_params(&sql, &params).await
                    .expect_err("FrankenSQLite primary-key conflict");
                assert_eq!(actual.error_code(), fsqlite_error::ErrorCode::Constraint,
                    "{label}: expected a constraint error: {actual}");
                // FAIL retains every successful preceding row, including all
                // earlier morsels, while preserving the conflicting seed row.
                let counts = [prefix, prefix + 2, prefix + 1];
                databases.assert_state(counts, &label).await;
                databases.control("COMMIT").await;
                databases.assert_state(counts, &label).await;
                databases.close(&label).await;
            }
        }
    });
}

#[test]
fn gh495_replace_check_failure_rolls_back_all_morsels_only() {
    asupersync::test_utils::run_test(|| async {
        let schema = EVENT_SCHEMA.replacen("cu INT,", "cu INT CHECK (cu >= 0),", 1);
        assert_ne!(schema, EVENT_SCHEMA, "fixture must install the CHECK constraint");
        for file_backed in [false, true] {
            let label = format!("GH#495 REPLACE CHECK file={file_backed}");
            let databases = EventDatabases::open_with_schema(file_backed, &schema).await;
            databases.write("INSERT", &event_params(129, 0, 0, 0, 129), false, &label).await;
            databases.control("BEGIN").await;
            // This prior statement must survive: a whole-transaction rollback
            // cannot satisfy the statement-atomicity keeper.
            databases.write("INSERT", &event_params(1, 1, 0, 0, 1), false, &label).await;
            databases.assert_state([1, 131, 130], &label).await;
            let mut params = event_params(129, 0, 0, 1, 129);
            params[69 * 13 + 9] = SqliteValue::Integer(-1);
            let sql = event_sql(129, "INSERT OR REPLACE", false);
            let expected = databases.stock.execute(
                &sql, rusqlite::params_from_iter(params.iter().map(stock_value)),
            ).expect_err("stock CHECK failure");
            assert_eq!(expected.sqlite_error_code(),
                Some(rusqlite::ErrorCode::ConstraintViolation), "{label}: stock error");
            let actual = databases.frank.execute_with_params(&sql, &params).await
                .expect_err("FrankenSQLite CHECK failure");
            assert!(matches!(actual, fsqlite_error::FrankenError::CheckViolation { .. }),
                "{label}: expected a CHECK violation: {actual}");
            // REPLACE resolves CHECK violations with ABORT. Restore every
            // earlier victim, including its old payload and index entry.
            databases.assert_state([0, 131, 130], &label).await;
            databases.write("INSERT", &event_params(1, 2, 0, 0, 1), false, &label).await;
            databases.assert_state([1, 132, 131], &label).await;
            databases.control("COMMIT").await;
            databases.assert_state([1, 132, 131], &label).await;
            databases.close(&label).await;
        }
    });
}
