#[allow(clippy::wildcard_imports)]
use super::*;

impl Connection {
    pub(super) async fn pragma_integrity_check_rows(
        &self,
        pragma: &fsqlite_ast::PragmaStatement,
    ) -> Vec<Row> {
        let quick = pragma.name.name.eq_ignore_ascii_case("quick_check");
        let max_errors = integrity_check_error_limit(pragma.value.as_ref());
        let mut failures = Vec::new();
        if let Err(error) = self.validate_database_integrity(quick).await {
            failures.push(error.to_string());
        } else {
            // Rows are only readable once the B-trees they live in are sound.
            let scope = integrity_check_table_scope(pragma.value.as_ref());
            match self
                .integrity_check_row_constraints(scope.as_deref(), max_errors)
                .await
            {
                Ok(reports) => failures.extend(reports),
                Err(error) => failures.push(error.to_string()),
            }
        }

        // Qualified attached PRAGMAs already delegate to their child Connection.
        // Unqualified whole-database checks must also visit every attachment;
        // a clean main database alone cannot establish an aggregate "ok" verdict.
        if pragma.name.schema.is_none() {
            let attached_schemas = self
                .attached_schemas
                .borrow()
                .all_schemas()
                .into_iter()
                .filter(|schema| !is_builtin_schema(schema))
                .map(str::to_owned)
                .collect::<Vec<_>>();
            for schema in attached_schemas {
                // The existing validator reports at most one failure per
                // database. N caps diagnostics across the entire traversal.
                if failures.len() >= max_errors {
                    break;
                }
                if let Err(error) = self
                    .with_attached_connection_async(&schema, async |child| {
                        child.validate_database_integrity(quick).await
                    })
                    .await
                {
                    failures.push(format!("*** in database {schema} ***\n{error}"));
                }
            }
        }

        if !fsqlite_observability::metrics::metrics_disabled() {
            let registry = fsqlite_observability::metrics::global();
            if failures.is_empty() {
                registry.integrity_check_ok_total.inc();
            } else {
                registry.integrity_check_fail_total.inc();
            }
        }
        if failures.is_empty() {
            failures.push("ok".to_owned());
        }
        let mut rows = failures
            .into_iter()
            .map(|outcome| Row {
                values: vec![SqliteValue::Text(outcome.into())],
            })
            .collect::<Vec<_>>();
        // bd-7o1vu (GH#370), complement option (1): surface a legacy orphaned
        // `%_content` shadow on a CONTENTLESS FTS5 table as an informational
        // NOTE. The shadow is a well-formed table, so it never fails the
        // ok/error verdict above (the database stays integrity-CLEAN); the note
        // only makes the condition discoverable so a user knows the one-time
        // first-open migration will reclaim it. Appended AFTER the verdict, so
        // an oracle that reads the first row still observes "ok".
        for shadow in self.orphaned_fts5_content_shadow_names() {
            rows.push(Row {
                values: vec![SqliteValue::Text(
                    format!(
                        "note: orphaned FTS5 contentless content shadow table {shadow} \
                         (reclaimable; the one-time first-open migration drops it)"
                    )
                    .into(),
                )],
            });
        }
        rows
    }

    /// bd-fjieg.4: stock `integrity_check` and `quick_check` both verify every
    /// stored row against its table's NOT NULL and CHECK constraints, reporting
    /// `NULL value in T.C` per violated column and at most one
    /// `CHECK constraint failed in T` per row. The scan asks each constrained
    /// table for its violating rows only, reading short records through their
    /// column defaults and evaluating CHECK exactly as a write does (a NULL
    /// result passes); a table with no such constraint is not read at all.
    async fn integrity_check_row_constraints(
        &self,
        only_table: Option<&str>,
        budget: usize,
    ) -> Result<Vec<String>> {
        let tables = {
            let temp_table_names = self.temp_table_names.borrow();
            let mut tables: Vec<TableSchema> = self
                .schema
                .borrow()
                .iter()
                .filter(|table| !temp_table_names.contains(&table.name.to_ascii_lowercase()))
                .cloned()
                .collect();
            tables.extend(self.shadowed_main_tables.borrow().values().cloned());
            tables.retain(|table| {
                table.root_page > 0
                    && only_table.is_none_or(|name| table.name.eq_ignore_ascii_case(name))
            });
            tables
        };
        let mut reports = Vec::new();
        for table in &tables {
            if reports.len() >= budget {
                break;
            }
            let primary_key = if table.without_rowid {
                table.primary_key_constraints.first().cloned().unwrap_or_default()
            } else {
                Vec::new()
            };
            let not_null: Vec<&str> = table
                .columns
                .iter()
                .filter(|column| {
                    !column.is_ipk
                        && (column.notnull
                            || primary_key
                                .iter()
                                .any(|key| key.eq_ignore_ascii_case(&column.name)))
                })
                .map(|column| column.name.as_str())
                .collect();
            if not_null.is_empty() && table.check_constraints.is_empty() {
                continue;
            }
            let predicates: Vec<String> = not_null
                .iter()
                .map(|column| format!("{} IS NULL", quote_identifier(column)))
                .chain(
                    table
                        .check_constraints
                        .iter()
                        .map(|check| format!("NOT ({})", check.expr)),
                )
                .collect();
            let sql = format!(
                "SELECT {} FROM main.{} WHERE {} LIMIT {}",
                predicates.join(", "),
                quote_identifier(&table.name),
                predicates
                    .iter()
                    .map(|predicate| format!("({predicate})"))
                    .collect::<Vec<_>>()
                    .join(" OR "),
                budget - reports.len()
            );
            let is_true = |value: &SqliteValue| matches!(value, SqliteValue::Integer(1));
            for row in self.query(&sql).await? {
                let (nulls, checks) = row.values().split_at(not_null.len());
                for (column, value) in not_null.iter().zip(nulls) {
                    if is_true(value) {
                        reports.push(format!("NULL value in {}.{column}", table.name));
                    }
                }
                if checks.iter().any(is_true) {
                    reports.push(format!("CHECK constraint failed in {}", table.name));
                }
            }
        }
        reports.truncate(budget);
        Ok(reports)
    }

    pub(super) async fn pragma_wal_checkpoint_rows(
        &self,
        pragma: &fsqlite_ast::PragmaStatement,
    ) -> Result<Vec<Row>> {
        let mode = if let Some(ref val) = pragma.value {
            parse_checkpoint_mode(val)?
        } else {
            self.checkpoint_schedule_override_mode()
                .unwrap_or(CheckpointMode::Passive)
        };

        // TEMP objects are connection-local and not pager/WAL-backed in
        // FrankenSQLite. A qualified TEMP checkpoint therefore has SQLite's
        // standard non-WAL sentinel and must not checkpoint `main` by accident.
        if pragma
            .name
            .schema
            .as_deref()
            .is_some_and(|schema| schema.eq_ignore_ascii_case("temp"))
        {
            return Ok(vec![Row {
                values: [0, -1, -1].into_iter().map(SqliteValue::Integer).collect(),
            }]);
        }

        let mut primary = self.pragma_wal_checkpoint_database(mode).await?;

        // SQLite interprets an unqualified wal_checkpoint as "all schemas".
        // Result counts come from the first database (main), while SQLITE_BUSY
        // is aggregated across every checkpointed database. Attached databases
        // are separate child Connections here, so fan out in attach order and
        // retain main's log/backfill values.
        if pragma.name.schema.is_none() {
            let attached_schemas = self
                .attached_schemas
                .borrow()
                .all_schemas()
                .into_iter()
                .filter(|schema| !is_builtin_schema(schema))
                .map(str::to_owned)
                .collect::<Vec<_>>();
            for schema in attached_schemas {
                let attached = self
                    .with_attached_connection_async(&schema, async |child| {
                        child.pragma_wal_checkpoint_database(mode).await
                    })
                    .await?;
                primary[0] = primary[0].max(attached[0]);
            }
        }

        Ok(vec![Row {
            values: primary
                .into_iter()
                .map(SqliteValue::Integer)
                .collect::<Vec<_>>(),
        }])
    }

    async fn pragma_wal_checkpoint_database(&self, mode: CheckpointMode) -> Result<[i64; 3]> {
        // SQLite returns the sentinel tuple instead of erroring when the
        // database is not in WAL mode.
        if self.pager.journal_mode() != JournalMode::Wal {
            return Ok([0, -1, -1]);
        }
        let cx = self.op_cx()?;
        if self.wal_checkpoint_blocked_by_active_concurrent_txns() {
            let log_frames =
                i64::try_from(self.pager.wal_frame_count(&cx).await).unwrap_or(i64::MAX);
            return Ok([1, log_frames, 0]);
        }

        self.invalidate_cached_write_txn(&cx).await;
        self.invalidate_cached_read_snapshot(&cx).await;
        let checkpoint_metrics_before = fsqlite_wal::GLOBAL_WAL_METRICS.snapshot();
        let result = match self.pager.checkpoint(&cx, mode).await {
            Ok(result) => result,
            // GH#399: another process owns the checkpoint fence right now.
            // `sqlite3_wal_checkpoint_v2` reports that as SQLITE_BUSY without
            // consulting the busy handler, and the PRAGMA surfaces it as
            // `busy = 1` with nothing checkpointed rather than as an error, so
            // peers closing at the same moment do not fail each other's
            // close-time checkpoints.
            Err(FrankenError::Busy) => {
                let log_frames =
                    i64::try_from(self.pager.wal_frame_count(&cx).await).unwrap_or(i64::MAX);
                return Ok([1, log_frames, 0]);
            }
            Err(error) => return Err(error),
        };
        // GH #384: the pager refreshed its durable WAL horizon while holding
        // the checkpoint fence. Carry that horizon into the process-shared
        // MVCC clock before a later BEGIN is compared with CommitIndex.
        self.align_commit_clock_floor(self.pager.published_snapshot().visible_commit_seq);
        let checkpoint_metrics_after = fsqlite_wal::GLOBAL_WAL_METRICS.snapshot();
        let checkpoint_duration_us = checkpoint_metrics_after
            .checkpoint_duration_us_total
            .saturating_sub(checkpoint_metrics_before.checkpoint_duration_us_total);
        self.checkpoint_advisor_note_checkpoint(mode, &result, checkpoint_duration_us);

        Ok(checkpoint_result_row(mode, &result))
    }
}

/// Translate successful pager execution into SQLite's checkpoint status row.
/// A PASSIVE checkpoint may stop at a reader's horizon without being busy;
/// failure to acquire the checkpoint lock is handled separately above.
fn checkpoint_result_row(
    mode: CheckpointMode,
    result: &fsqlite_pager::CheckpointResult,
) -> [i64; 3] {
    let reset_requested = matches!(mode, CheckpointMode::Restart | CheckpointMode::Truncate);
    let reset_completed = result.wal_was_reset
        && (mode != CheckpointMode::Truncate || result.effective_mode == CheckpointMode::Truncate);
    let blocked_by_readers = mode != CheckpointMode::Passive
        && (!result.completed || (reset_requested && result.total_frames > 0 && !reset_completed));

    // The pager records pre-checkpoint work for its metrics. SQLite instead
    // reports the *post-truncation* log and backfill counts. RESTART retains
    // its counts, and a safety downgrade must not pretend it truncated a WAL.
    if mode == CheckpointMode::Truncate && !blocked_by_readers && reset_completed {
        return [0, 0, 0];
    }
    [
        i64::from(blocked_by_readers),
        i64::from(result.total_frames),
        i64::from(result.frames_backfilled),
    ]
}

fn integrity_check_error_limit(value: Option<&fsqlite_ast::PragmaValue>) -> usize {
    let Some(value) = value else {
        return 100;
    };
    let expr = match value {
        fsqlite_ast::PragmaValue::Assign(expr) | fsqlite_ast::PragmaValue::Call(expr) => expr,
    };
    // SQLite reads the integer token's magnitude even when it has a sign.
    let expr = match expr {
        Expr::UnaryOp {
            op: UnaryOp::Plus | UnaryOp::Negate,
            expr,
            ..
        } => expr.as_ref(),
        _ => expr,
    };
    match expr {
        Expr::Literal(Literal::Integer(limit), _) if *limit != 0 => {
            usize::try_from(limit.unsigned_abs()).unwrap_or(usize::MAX)
        }
        _ => 100,
    }
}

/// `PRAGMA integrity_check(T)` / `('T')` limits the check to one table.
fn integrity_check_table_scope(value: Option<&fsqlite_ast::PragmaValue>) -> Option<String> {
    let (fsqlite_ast::PragmaValue::Assign(expr) | fsqlite_ast::PragmaValue::Call(expr)) = value?;
    match expr {
        Expr::Column(col_ref, _) if col_ref.table.is_none() => Some(col_ref.column.to_string()),
        Expr::Literal(Literal::String(name), _) => Some(name.clone()),
        _ => None,
    }
}

fn parse_checkpoint_mode(value: &fsqlite_ast::PragmaValue) -> Result<CheckpointMode> {
    let expr = match value {
        fsqlite_ast::PragmaValue::Assign(e) | fsqlite_ast::PragmaValue::Call(e) => e,
    };
    let text = match expr {
        Expr::Literal(Literal::String(s), _) => s.clone(),
        Expr::Column(col_ref, _) if col_ref.table.is_none() => col_ref.column.to_string(),
        _ => {
            return Err(FrankenError::Internal(
                "PRAGMA wal_checkpoint mode must be PASSIVE/FULL/RESTART/TRUNCATE".to_owned(),
            ));
        }
    };
    match text.to_uppercase().as_str() {
        "PASSIVE" => Ok(CheckpointMode::Passive),
        "FULL" => Ok(CheckpointMode::Full),
        "RESTART" => Ok(CheckpointMode::Restart),
        "TRUNCATE" => Ok(CheckpointMode::Truncate),
        _ => Err(FrankenError::Internal(format!(
            "PRAGMA wal_checkpoint mode must be PASSIVE/FULL/RESTART/TRUNCATE, got `{text}`"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn corrupt_integrity_test_root(conn: &Connection) -> Result<()> {
        let root_page = conn
            .schema
            .borrow()
            .iter()
            .find(|table| table.name.eq_ignore_ascii_case("t"))
            .map(|table| table.root_page)
            .expect("fixture table root");
        let cx = conn.op_cx()?;
        if conn.retained_autocommit_txn.borrow().is_some() {
            conn.flush_retained_autocommit_txn(&cx).await?;
        }
        conn.invalidate_cached_write_txn(&cx).await;
        conn.invalidate_cached_read_snapshot(&cx).await;
        let mut txn = conn.pager.begin(&cx, TransactionMode::Immediate).await?;
        let page_no = PageNumber::new(u32::try_from(root_page).unwrap()).unwrap();
        let mut page = txn.get_page(&cx, page_no).await?.into_vec();
        assert_eq!(page[0], 0x0D, "fixture must start as a table leaf");
        page[0] = 0xFF;
        txn.write_page(&cx, page_no, &page).await?;
        txn.commit(&cx).await
    }

    fn assert_integrity_ok(rows: &[Row]) {
        assert_eq!(rows.len(), 1, "clean databases return one verdict");
        assert_eq!(rows[0].values(), &[SqliteValue::Text("ok".into())]);
    }

    #[test]
    fn unqualified_integrity_checks_report_attached_corruption() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")
                .await
                .unwrap();
            for schema in ["good", "bad_first", "bad_second"] {
                conn.execute(&format!("ATTACH DATABASE ':memory:' AS {schema};"))
                    .await
                    .unwrap();
                conn.execute(&format!(
                    "CREATE TABLE {schema}.t(id INTEGER PRIMARY KEY, v TEXT); \
                     INSERT INTO {schema}.t VALUES (1, 'payload');"
                ))
                .await
                .unwrap();
            }
            for pragma in ["integrity_check", "quick_check"] {
                assert_integrity_ok(&conn.query(&format!("PRAGMA {pragma};")).await.unwrap());
                let prepared = conn.prepare(&format!("PRAGMA {pragma};")).await.unwrap();
                assert_integrity_ok(&prepared.query().await.unwrap());
            }
            for schema in ["bad_first", "bad_second"] {
                conn.with_attached_connection_async(schema, async |child| {
                    corrupt_integrity_test_root(child).await
                })
                .await
                .unwrap();
            }

            for pragma in ["integrity_check", "quick_check"] {
                for schema in ["main", "good"] {
                    assert_integrity_ok(
                        &conn
                            .query(&format!("PRAGMA {schema}.{pragma};"))
                            .await
                            .unwrap(),
                    );
                }
                // Establish actual corruption before testing aggregate dispatch.
                for schema in ["bad_first", "bad_second"] {
                    let rows = conn
                        .query(&format!("PRAGMA {schema}.{pragma};"))
                        .await
                        .unwrap();
                    assert_eq!(rows.len(), 1);
                    let SqliteValue::Text(message) = &rows[0].values()[0] else {
                        panic!("expected corruption diagnostic");
                    };
                    assert!(message.contains("invalid B-tree page type"), "{message}");
                }
                for prepared in [false, true] {
                    for (argument, expected_count) in [
                        ("", 2),
                        ("(1)", 1),
                        ("(2)", 2),
                        ("(0)", 2),
                        ("(-1)", 1),
                        ("(+1)", 1),
                    ] {
                        let sql = format!("PRAGMA {pragma}{argument};");
                        let rows = if prepared {
                            conn.prepare(&sql).await.unwrap().query().await.unwrap()
                        } else {
                            conn.query(&sql).await.unwrap()
                        };
                        assert_eq!(
                            rows.len(),
                            expected_count,
                            "{sql} prepared={prepared} must apply one shared error budget: {rows:?}"
                        );
                        // FrankenSQLite's existing validator returns one diagnostic
                        // per database, including for hard B-tree corruption. This
                        // guards traversal, not stock's hard-error sequencing.
                        for (row, schema) in rows.iter().zip(["bad_first", "bad_second"]) {
                            let SqliteValue::Text(message) = &row.values()[0] else {
                                panic!("expected corruption diagnostic");
                            };
                            assert!(
                                message.starts_with(&format!("*** in database {schema} ***\n")),
                                "{message}"
                            );
                            assert!(message.contains("invalid B-tree page type"), "{message}");
                        }
                    }
                }
            }
        });
    }

    #[test]
    fn qualified_integrity_checks_do_not_visit_corrupt_main() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE t(id INTEGER PRIMARY KEY); \
                 INSERT INTO t VALUES (1); \
                 ATTACH DATABASE ':memory:' AS aux; \
                 CREATE TABLE aux.t(id INTEGER PRIMARY KEY);",
            )
            .await
            .unwrap();
            corrupt_integrity_test_root(&conn).await.unwrap();
            for pragma in ["integrity_check", "quick_check"] {
                assert_integrity_ok(&conn.query(&format!("PRAGMA aux.{pragma};")).await.unwrap());
                for scope in ["", "main."] {
                    let rows = conn
                        .query(&format!("PRAGMA {scope}{pragma};"))
                        .await
                        .unwrap();
                    assert_eq!(rows.len(), 1);
                    let SqliteValue::Text(message) = &rows[0].values()[0] else {
                        panic!("expected corruption diagnostic");
                    };
                    assert!(message.contains("invalid B-tree page type"), "{message}");
                }
            }
        });
    }

    #[test]
    fn stock_integrity_checks_visit_attached_schemas() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "ATTACH DATABASE ':memory:' AS aux; \
             ATTACH DATABASE ':memory:' AS aux_second; \
             CREATE TABLE aux.t(v INTEGER CHECK(v > 0)); \
             CREATE TABLE aux_second.t(v INTEGER CHECK(v > 0)); \
             PRAGMA ignore_check_constraints=ON; \
             INSERT INTO aux.t VALUES(-1); \
             INSERT INTO aux_second.t VALUES(-1); \
             PRAGMA ignore_check_constraints=OFF;",
        )
        .unwrap();
        for pragma in ["integrity_check", "quick_check"] {
            let main: String = conn
                .query_row(&format!("PRAGMA main.{pragma};"), [], |row| row.get(0))
                .unwrap();
            assert_eq!(main, "ok");
            for scope in ["", "aux."] {
                let report: String = conn
                    .query_row(&format!("PRAGMA {scope}{pragma};"), [], |row| row.get(0))
                    .unwrap();
                assert!(report.contains("CHECK constraint failed"), "{report}");
            }
            for (argument, expected_count) in [
                ("", 2),
                ("(1)", 1),
                ("(2)", 2),
                ("(0)", 2),
                ("(-1)", 1),
                ("(+1)", 1),
            ] {
                let sql = format!("PRAGMA {pragma}{argument};");
                let mut statement = conn.prepare(&sql).unwrap();
                let reports = statement
                    .query_map([], |row| row.get::<_, String>(0))
                    .unwrap()
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(reports.len(), expected_count, "{sql}: {reports:?}");
                assert!(
                    reports
                        .iter()
                        .all(|report| report.contains("CHECK constraint failed"))
                );
            }
        }
    }

    /// bd-fjieg.4: stored rows that violate NOT NULL or CHECK are reported
    /// exactly as stock reports them, by both integrity_check and quick_check.
    #[test]
    fn integrity_checks_report_stored_not_null_and_check_violations_like_stock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("constraints.db");
        {
            let stock = rusqlite::Connection::open(&path).unwrap();
            stock
                .execute_batch(
                    "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INT, c INT DEFAULT 5);
                     INSERT INTO t(id, a, b) VALUES(1,'x',1),(2,NULL,-2),(3,NULL,-3),(4,'w',4);
                     CREATE TABLE u(p INT, q INT);
                     INSERT INTO u VALUES(NULL, 1),(7, 'abc');
                     CREATE TABLE w(k TEXT, v INT, PRIMARY KEY(k)) WITHOUT ROWID;
                     INSERT INTO w VALUES('a', 1),('b', 20);
                     CREATE TABLE clean(x INT NOT NULL CHECK(x > 0));
                     INSERT INTO clean VALUES(1);
                     PRAGMA writable_schema=ON;
                     UPDATE sqlite_schema SET sql='CREATE TABLE t(id INTEGER PRIMARY KEY, \
                         a TEXT NOT NULL, b INT CHECK(b>0), c INT DEFAULT 5 CHECK(c<5))'
                         WHERE name='t';
                     UPDATE sqlite_schema SET sql='CREATE TABLE u(p INT NOT NULL, q INT, \
                         CONSTRAINT q_not_one CHECK(q<>1), CHECK(q>0))' WHERE name='u';
                     UPDATE sqlite_schema SET sql='CREATE TABLE w(k TEXT, v INT CHECK(v<10), \
                         PRIMARY KEY(k)) WITHOUT ROWID' WHERE name='w';",
                )
                .unwrap();
        }
        let stock_reports = |sql: &str| -> Vec<String> {
            let stock = rusqlite::Connection::open(&path).unwrap();
            let mut statement = stock.prepare(sql).unwrap();
            let mut reports = statement
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            reports.sort();
            reports
        };
        let cases = [
            "PRAGMA integrity_check;",
            "PRAGMA quick_check;",
            "PRAGMA integrity_check(t);",
            "PRAGMA quick_check('w');",
            "PRAGMA integrity_check(clean);",
            "PRAGMA integrity_check(3);",
        ];
        let expected: Vec<Vec<String>> = cases.iter().map(|sql| stock_reports(sql)).collect();
        assert_eq!(expected[0].len(), 9, "fixture: {:?}", expected[0]);
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            for (sql, expected) in cases.iter().zip(&expected) {
                let mut reports: Vec<String> = conn
                    .query(sql)
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| match &row.values()[0] {
                        SqliteValue::Text(text) => text.to_string(),
                        other => panic!("{sql}: non-text report {other:?}"),
                    })
                    .collect();
                reports.sort();
                if sql.contains("(3)") {
                    // Stock visits tables in hash order; only the cap is shared.
                    assert_eq!(reports.len(), expected.len(), "{sql}: {reports:?}");
                } else {
                    assert_eq!(&reports, expected, "{sql}");
                }
            }
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn unqualified_wal_checkpoint_truncates_attached_wal() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let main_path = dir.path().join("main.db");
            let aux_path = dir.path().join("aux.db");
            let conn = Connection::open(main_path.to_str().unwrap()).await.unwrap();

            conn.execute("PRAGMA journal_mode=WAL;").await.unwrap();
            conn.execute(&format!(
                "ATTACH DATABASE '{}' AS aux;",
                aux_path.to_string_lossy().replace('\'', "''")
            ))
            .await
            .unwrap();
            conn.execute("PRAGMA aux.journal_mode=WAL;").await.unwrap();
            conn.execute("CREATE TABLE main_t(id INTEGER PRIMARY KEY, v TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE TABLE aux.aux_t(id INTEGER PRIMARY KEY, v TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO main_t VALUES (1, 'main');")
                .await
                .unwrap();
            conn.execute("INSERT INTO aux.aux_t VALUES (1, 'aux');")
                .await
                .unwrap();

            let aux_frames_before = conn
                .with_attached_connection_async("aux", async |child| {
                    let cx = child.op_cx()?;
                    Ok(child.pager.wal_frame_count(&cx).await)
                })
                .await
                .unwrap();
            assert!(
                aux_frames_before > 0,
                "test requires a non-empty auxiliary WAL"
            );

            conn.query("PRAGMA wal_checkpoint(TRUNCATE);")
                .await
                .unwrap();

            let aux_frames_after = conn
                .with_attached_connection_async("aux", async |child| {
                    let cx = child.op_cx()?;
                    Ok(child.pager.wal_frame_count(&cx).await)
                })
                .await
                .unwrap();
            assert_eq!(aux_frames_after, 0, "unqualified checkpoint must visit aux");
        });
    }

    #[test]
    fn temp_wal_checkpoint_does_not_checkpoint_main() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let main_path = dir.path().join("main.db");
            let conn = Connection::open(main_path.to_str().unwrap()).await.unwrap();

            conn.execute("PRAGMA journal_mode=WAL;").await.unwrap();
            conn.execute("CREATE TABLE main_t(id INTEGER PRIMARY KEY);")
                .await
                .unwrap();
            conn.execute("INSERT INTO main_t VALUES (1);")
                .await
                .unwrap();

            let cx = conn.op_cx().unwrap();
            let main_frames_before = conn.pager.wal_frame_count(&cx).await;
            assert!(main_frames_before > 0, "test requires a non-empty main WAL");

            let rows = conn
                .query("PRAGMA temp.wal_checkpoint(TRUNCATE);")
                .await
                .unwrap();
            let row = rows.first().expect("checkpoint returns one result row");
            assert_eq!(
                row.values,
                vec![
                    SqliteValue::Integer(0),
                    SqliteValue::Integer(-1),
                    SqliteValue::Integer(-1),
                ]
            );
            assert_eq!(
                conn.pager.wal_frame_count(&cx).await,
                main_frames_before,
                "TEMP checkpoint must not mutate main's WAL"
            );
        });
    }

    #[test]
    fn checkpoint_status_distinguishes_passive_progress_from_busy() {
        for mode in [
            CheckpointMode::Passive,
            CheckpointMode::Full,
            CheckpointMode::Restart,
            CheckpointMode::Truncate,
        ] {
            let result = fsqlite_pager::CheckpointResult {
                total_frames: 4,
                frames_backfilled: 3,
                completed: false,
                wal_was_reset: false,
                requested_mode: mode,
                effective_mode: mode,
            };
            let expected_busy = i64::from(mode != CheckpointMode::Passive);
            assert_eq!(checkpoint_result_row(mode, &result), [expected_busy, 4, 3]);
        }
    }

    #[test]
    fn checkpoint_status_preserves_unfinished_reset_and_downgrade() {
        for mode in [CheckpointMode::Restart, CheckpointMode::Truncate] {
            let mut result = fsqlite_pager::CheckpointResult {
                total_frames: 4,
                frames_backfilled: 4,
                completed: true,
                wal_was_reset: false,
                requested_mode: mode,
                effective_mode: CheckpointMode::Passive,
            };
            assert_eq!(checkpoint_result_row(mode, &result), [1, 4, 4]);
            // A completed RESTART still does not satisfy requested TRUNCATE.
            result.wal_was_reset = true;
            result.effective_mode = CheckpointMode::Restart;
            let expected = if mode == CheckpointMode::Truncate {
                [1, 4, 4]
            } else {
                [0, 4, 4]
            };
            assert_eq!(checkpoint_result_row(mode, &result), expected);
        }
    }

    #[test]
    fn checkpoint_status_zeros_only_successful_truncate_counts() {
        for frames in [0, 4, u32::MAX] {
            for mode in [
                CheckpointMode::Passive,
                CheckpointMode::Full,
                CheckpointMode::Restart,
                CheckpointMode::Truncate,
            ] {
                let result = fsqlite_pager::CheckpointResult {
                    total_frames: frames,
                    frames_backfilled: frames,
                    completed: true,
                    wal_was_reset: matches!(mode, CheckpointMode::Restart | CheckpointMode::Truncate),
                    requested_mode: mode,
                    effective_mode: mode,
                };
                let expected = if mode == CheckpointMode::Truncate {
                    [0, 0, 0]
                } else {
                    [0, i64::from(frames), i64::from(frames)]
                };
                assert_eq!(checkpoint_result_row(mode, &result), expected);
            }
        }
    }

    #[test]
    fn stock_checkpoint_reader_pin_and_truncate_status() {
        // Keep the stock fixture separate from native fixtures: two SQLite
        // implementations must not share a file in the same process.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stock-checkpoint.db");
        let writer = rusqlite::Connection::open(&path).unwrap();
        writer
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; \
                 PRAGMA busy_timeout=0; CREATE TABLE t(id INTEGER PRIMARY KEY); \
                 INSERT INTO t VALUES(1);",
            )
            .unwrap();
        let reader = rusqlite::Connection::open(&path).unwrap();
        reader.execute_batch("BEGIN;").unwrap();
        let pinned: i64 = reader
            .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
            .unwrap();
        assert_eq!(pinned, 1);
        writer.execute_batch("INSERT INTO t VALUES(2);").unwrap();
        for (mode, expected_busy) in [
            ("PASSIVE", 0),
            ("FULL", 1),
            ("RESTART", 1),
            ("TRUNCATE", 1),
        ] {
            let status: [i64; 3] = writer
                .query_row(&format!("PRAGMA main.wal_checkpoint({mode});"), [], |row| {
                    Ok([row.get(0)?, row.get(1)?, row.get(2)?])
                })
                .unwrap();
            assert_eq!(status[0], expected_busy, "{mode}: {status:?}");
            assert!(status[2] >= 0 && status[2] < status[1], "{mode}: {status:?}");
        }
        reader.execute_batch("ROLLBACK;").unwrap();
        let restarted: [i64; 3] = writer
            .query_row("PRAGMA main.wal_checkpoint(RESTART);", [], |row| {
                Ok([row.get(0)?, row.get(1)?, row.get(2)?])
            })
            .unwrap();
        assert_eq!(restarted[0], 0);
        assert!(restarted[1] > 0);
        assert_eq!(restarted[1], restarted[2]);
        for _ in 0..2 {
            let truncated: [i64; 3] = writer
                .query_row("PRAGMA main.wal_checkpoint(TRUNCATE);", [], |row| {
                    Ok([row.get(0)?, row.get(1)?, row.get(2)?])
                })
                .unwrap();
            assert_eq!(truncated, [0, 0, 0]);
        }
    }

    #[test]
    fn native_truncate_status_reports_empty_wal_for_direct_and_prepared_queries() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("native-checkpoint.db");
            let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            conn.execute("PRAGMA journal_mode=WAL;").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY);")
                .await
                .unwrap();
            for prepared in [false, true] {
                conn.execute("BEGIN;").await.unwrap();
                conn.execute("INSERT INTO t DEFAULT VALUES;").await.unwrap();
                conn.execute("COMMIT;").await.unwrap();
                let cx = conn.op_cx().unwrap();
                assert!(conn.pager.wal_frame_count(&cx).await > 0);
                let sql = "PRAGMA main.wal_checkpoint(TRUNCATE);";
                let rows = if prepared {
                    conn.prepare(sql).await.unwrap().query().await.unwrap()
                } else {
                    conn.query(sql).await.unwrap()
                };
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    rows[0].values(),
                    &[
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                    ]
                );
                assert_eq!(conn.pager.wal_frame_count(&cx).await, 0);
            }
            conn.close().await.unwrap();
        });
    }
}
