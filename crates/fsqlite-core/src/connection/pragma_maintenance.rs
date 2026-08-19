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

        // SQLite returns the sentinel tuple instead of erroring when the
        // database is not in WAL mode.
        if self.pager.journal_mode() != JournalMode::Wal {
            return Ok(vec![Row {
                values: vec![
                    SqliteValue::Integer(0),
                    SqliteValue::Integer(-1),
                    SqliteValue::Integer(-1),
                ],
            }]);
        }
        let cx = self.op_cx()?;
        if self.wal_checkpoint_blocked_by_active_concurrent_txns() {
            let log_frames =
                i64::try_from(self.pager.wal_frame_count(&cx).await).unwrap_or(i64::MAX);
            return Ok(vec![Row {
                values: vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(log_frames),
                    SqliteValue::Integer(0),
                ],
            }]);
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

        Ok(vec![Row {
            values: vec![
                SqliteValue::Integer(0),
                SqliteValue::Integer(i64::from(result.total_frames)),
                SqliteValue::Integer(i64::from(result.frames_backfilled)),
            ],
        }])
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
