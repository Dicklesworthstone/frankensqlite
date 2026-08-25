#[allow(clippy::wildcard_imports)]
use super::*;

impl Connection {
    pub(super) async fn pragma_integrity_check_rows(&self, quick: bool) -> Vec<Row> {
        let outcome = match self.validate_database_integrity(quick).await {
            Ok(()) => "ok".to_owned(),
            Err(err) => err.to_string(),
        };
        let mut rows = vec![Row {
            values: vec![SqliteValue::Text(outcome.into())],
        }];
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
        let result = self.pager.checkpoint(&cx, mode).await?;
        let checkpoint_metrics_after = fsqlite_wal::GLOBAL_WAL_METRICS.snapshot();
        let checkpoint_duration_us = checkpoint_metrics_after
            .checkpoint_duration_us_total
            .saturating_sub(checkpoint_metrics_before.checkpoint_duration_us_total);
        self.checkpoint_advisor_note_checkpoint(mode, &result, checkpoint_duration_us);

        Ok([
            0,
            i64::from(result.total_frames),
            i64::from(result.frames_backfilled),
        ])
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
}
