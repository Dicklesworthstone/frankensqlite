//! GH#516: one compiled row program, one enclosing SQL statement.
//!
//! The ordinary INSERT dispatcher below is extracted, not copied: both public
//! single-row dispatch and batch replay use exactly the same trigger/FK kernel.
//! A batch changes only entry/compilation ownership, never trigger evaluation.
#[allow(clippy::wildcard_imports)]
use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct InsertReplayIdentity {
    schema_cookie: u32,
    schema_generation: u64,
    function_generation: u64,
    concurrent: bool,
    text_encoding: TextEncoding,
}

pub(super) struct CompiledInsertReplay {
    statement: Statement,
    program: VdbeProgram,
    identity: InsertReplayIdentity,
    cx: Cx,
}

/// A row's projected result belongs to the outer INSERT, not a synthetic
/// nested statement. Keep it beside the affected count so a successful IGNORE
/// cannot be confused with a returned row, and errors cannot expose a partial
/// successful batch result.
pub(super) struct CompiledInsertReplayOutcome {
    pub(super) changes: usize,
    pub(super) returning_rows: Vec<Row>,
}

/// Admission only: expressions are still evaluated by the ordinary INSERT
/// program for each written row. In particular, do not evaluate RETURNING
/// against stored rows after AFTER triggers have changed them.
///
/// Function calls (including overridable LIKE/JSON operators), table reads,
/// subqueries, and unknown expression forms retain the existing replay path.
/// Some of those paths materialize at compilation time, so sharing their
/// bytecode could freeze the first row's result. Whitelist rather than guess
/// purity from a function name. Use a worklist rather than recursive Rust
/// calls: SQL expression depth must not become native-stack depth here.
fn reusable_insert_returning(columns: &[fsqlite_ast::ResultColumn]) -> bool {
    use fsqlite_ast::{Expr, Literal, ResultColumn};

    let mut pending = Vec::new();
    for column in columns {
        match column {
            ResultColumn::Star => {}
            ResultColumn::Expr { expr, .. } => pending.push(expr),
            ResultColumn::TableStar(_) => return false,
        }
    }
    while let Some(expr) = pending.pop() {
        match expr {
            Expr::Literal(
                Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp,
                _,
            ) => return false,
            Expr::Literal(..) | Expr::Column(..) | Expr::Placeholder(..) => {}
            Expr::BinaryOp { left, right, .. } => {
                pending.push(left.as_ref());
                pending.push(right.as_ref());
            }
            Expr::UnaryOp { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::Collate { expr, .. }
            | Expr::IsNull { expr, .. } => pending.push(expr.as_ref()),
            Expr::Between { expr, low, high, .. } => {
                pending.push(expr.as_ref());
                pending.push(low.as_ref());
                pending.push(high.as_ref());
            }
            Expr::Case {
                operand,
                whens,
                else_expr,
                ..
            } => {
                if let Some(operand) = operand {
                    pending.push(operand.as_ref());
                }
                for (condition, value) in whens {
                    pending.push(condition);
                    pending.push(value);
                }
                if let Some(value) = else_expr {
                    pending.push(value.as_ref());
                }
            }
            _ => return false,
        }
    }
    true
}

impl Connection {
    pub(super) fn insert_replay_identity(&self) -> InsertReplayIdentity {
        InsertReplayIdentity {
            schema_cookie: self.schema_cookie(),
            schema_generation: self.schema_generation(),
            function_generation: self.function_registry_generation(),
            concurrent: self.is_concurrent_transaction(),
            text_encoding: self.db_text_encoding.get(),
        }
    }

    pub(super) async fn prepare_compiled_insert_replay(
        &self,
        original: &fsqlite_ast::InsertStatement,
        row_sql: &str,
    ) -> Result<Option<CompiledInsertReplay>> {
        // Keep SELECT streaming, UPSERT OLD/NEW routing, non-row-local
        // RETURNING, virtual tables, and attached/TEMP ownership on their established
        // paths. FAIL/ROLLBACK also retain their historical replay entry until
        // their independently nested failure boundaries have a fast-path proof.
        // This is a positive admission rule, not a global fallback bypass.
        if !matches!(&original.source, InsertSource::Values(rows) if rows.len() > 1)
            || original.with.is_some()
            || !reusable_insert_returning(&original.returning)
            || !original.upsert.is_empty()
            || matches!(
                original.or_conflict,
                Some(fsqlite_ast::ConflictAction::Fail | fsqlite_ast::ConflictAction::Rollback)
            )
            || original
                .table
                .schema
                .as_deref()
                .is_some_and(|s| !s.eq_ignore_ascii_case("main"))
            || is_sqlite_schema_name(&original.table.name)
            || original.table.name.eq_ignore_ascii_case("sqlite_sequence")
            || self.has_live_vtab_instance(&original.table.name)
            || self.view_index_of(&original.table.name).is_some()
            || self.targets_shadowed_main(&original.table)
            || !self.temp_storage_roots().is_empty()
            || !self.active_txn_is_open_or_borrowed()
            || self.internal_statement_savepoint_depth.get() == 0
        {
            return Ok(None);
        }
        let event = fsqlite_ast::TriggerEvent::Insert;
        if !self.has_matching_triggers(
            &original.table.name,
            fsqlite_ast::TriggerTiming::Before,
            &event,
        ) && !self.has_matching_triggers(
            &original.table.name,
            fsqlite_ast::TriggerTiming::After,
            &event,
        ) {
            return Ok(None);
        }
        // VALUES have already been materialized by the enclosing dispatcher.
        // Only the parameter-only row template is compiled; source expressions
        // and WHEN expressions must never be folded against a batch snapshot.
        let statement = parse_single_statement(row_sql)?;
        let Statement::Insert(insert) = &statement else {
            return Err(FrankenError::internal(
                "INSERT row template is not an INSERT",
            ));
        };
        // The enclosing replay SQL builder owns original parameter numbering.
        // Reuse its exact row template and value slice, including RETURNING
        // bindings; do not allocate a second parameter namespace here.
        if !reusable_insert_returning(&insert.returning) {
            return Ok(None);
        }
        let identity = self.insert_replay_identity();
        let program = self.compile_table_insert(insert, None).await?;
        if self.insert_replay_identity() != identity {
            return Ok(None);
        }
        Ok(Some(CompiledInsertReplay {
            statement,
            program,
            identity,
            cx: self.op_cx_after_background_status(),
        }))
    }

    pub(super) fn execute_compiled_insert_replay_row<'a>(
        &'a self,
        plan: &'a CompiledInsertReplay,
        values: &'a [SqliteValue],
    ) -> FallbackStatementFuture<'a, Result<CompiledInsertReplayOutcome>> {
        // Keep the existing recursion/cancellation admission wrapper. Only
        // public SQL entry work is elided: no parse, rewrite, schema validation,
        // cache lookup, autocommit acquisition, or synthetic statement trace.
        self.with_fallback_statement(Box::pin(async move {
            if self.insert_replay_identity() != plan.identity {
                // Reentrant application functions may change schema/functions.
                // Never run bytecode whose binding proof has expired.
                let returning_rows = self
                    .execute_statement_impl_after_background_status(
                        &plan.statement,
                        Some(values),
                        None,
                        false,
                    )
                    .await?;
                return Ok(CompiledInsertReplayOutcome {
                    changes: self.last_changes.get(),
                    returning_rows,
                });
            }
            let Statement::Insert(insert) = &plan.statement else {
                return Err(FrankenError::internal(
                    "compiled INSERT row lost its statement",
                ));
            };
            self.clear_table_program_error_state();
            self.forget_constraint_failure_action_at_top_level();
            self.sync_change_tracking_context();
            let _depth = self.enter_statement_exec();
            let _conflict =
                DmlConflictClauseGuard::new(&self.dml_conflict_clause, insert.or_conflict);
            if self.pragma_state.borrow().query_only || self.pager.is_readonly() {
                return Err(FrankenError::ReadOnly);
            }
            self.guard_mutation_encoding_supported(&plan.statement)?;
            if Self::statement_may_invalidate_fk_parent_cache(&plan.statement) {
                self.clear_fk_parent_validation_cache();
            }
            // A previous row's trigger may leave direct writes buffered. The
            // parameter-only template is read-free, exactly the case admitted
            // by plan_statement_execution's flush (rather than reload) branch.
            if !self.skip_statement_memdb_refresh.get()
                && (self.memdb_requires_active_txn_reload.get()
                    || !self.pending_memdb_direct_upserts.borrow().is_empty())
            {
                self.flush_pending_direct_write_runs(&plan.cx).await?;
            }
            self.clear_prepared_direct_insert_append_hint();
            let previous_total = self.total_changes.get();
            let previous_trigger_steps = self.trigger_step_changes.get();
            let previous_rowid = self.current_last_insert_rowid();
            // The stream owns the statement savepoint and final-image FK scope.
            // The shared kernel keeps BEFORE -> write -> FK -> AFTER ordering,
            // IGNORE row skips, implicit REPLACE victims, and sequence updates.
            let result = Box::pin(self.execute_insert_dispatch(
                &plan.cx,
                &plan.statement,
                insert,
                Some(values),
                Some(&plan.program),
                Some(plan.identity),
            ))
            .await;
            match result {
                Ok(returning_rows) => Ok(CompiledInsertReplayOutcome {
                    changes: self.last_changes.get(),
                    returning_rows,
                }),
                Err(error) => {
                    self.restore_failed_statement_tracking(
                        statement_preserves_prior_changes_on_constraint(&plan.statement),
                        &error,
                        previous_total,
                        previous_trigger_steps,
                        previous_rowid,
                    );
                    // RAISE(ROLLBACK) inside an admitted trigger still rolls
                    // back the transaction even though OR ROLLBACK uses replay.
                    self.maybe_rollback_transaction_for_conflict_action(
                        statement_rolls_back_transaction_on_constraint(&plan.statement),
                        false,
                        &error,
                    )
                    .await?;
                    Err(error)
                }
            }
        }))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_insert_dispatch(
        &self,
        cx: &Cx,
        statement: &Statement,
        insert: &fsqlite_ast::InsertStatement,
        params: Option<&[SqliteValue]>,
        precompiled: Option<&VdbeProgram>,
        replay_identity: Option<InsertReplayIdentity>,
    ) -> Result<Vec<Row>> {
        if self.pragma_state.borrow().writable_schema && is_sqlite_schema_name(&insert.table.name) {
            let affected = Box::pin(self.execute_writable_schema_insert(insert, params)).await?;
            self.record_statement_changes(affected);
            return Ok(Vec::new());
        }
        // Reached only with writable_schema OFF: direct DML on the schema
        // table is rejected with SQLITE_ERROR, not the generic
        // "no such table" the interpreted path would otherwise raise.
        // (GH #284)
        if is_sqlite_schema_name(&insert.table.name) {
            return Err(FrankenError::FunctionError(format!(
                "table {} may not be modified",
                insert.table.name
            )));
        }
        if Box::pin(self.execute_fts5_maintenance_insert(insert)).await? {
            return Ok(Vec::new());
        }
        #[cfg(feature = "ext-fts5")]
        if let Some(affected) =
            Box::pin(self.execute_fts5_magic_delete_insert(insert, params)).await?
        {
            self.record_statement_changes(affected);
            return Ok(Vec::new());
        }
        if insert.with.is_some() {
            self.log_mem_execution_fallback("insert", "with_clause_materialization")?;
            return Box::pin(self.execute_insert_with_ctes(insert, params)).await;
        }

        let table_name = &insert.table.name;
        let insert_event = fsqlite_ast::TriggerEvent::Insert;
        let is_live_vtab = self.has_live_vtab_instance(table_name);
        let has_before_insert = self.has_matching_triggers(
            table_name,
            fsqlite_ast::TriggerTiming::Before,
            &insert_event,
        );
        let has_after_insert =
            self.has_matching_triggers(table_name, fsqlite_ast::TriggerTiming::After, &insert_event);
        // bd-y7old: a conflicting ON CONFLICT DO UPDATE resolves to the
        // UPDATE path, so it must fire BEFORE/AFTER UPDATE triggers (and
        // NOT AFTER INSERT). Compute their presence for the DO-UPDATE case.
        let is_do_update_upsert = insert
            .upsert
            .iter()
            .any(|clause| matches!(clause.action, fsqlite_ast::UpsertAction::Update { .. }));
        // `UPDATE OF <cols>` triggers match on the SET-assigned columns.
        // The exact event depends on WHICH ON CONFLICT clause is taken,
        // which is only known after conflict routing (bd-bwm0x): with
        // multiple DO-UPDATE clauses a LATER clause with different SET
        // columns can be the one applied. Here we only need a COARSE gate —
        // whether ANY DO-UPDATE clause could fire a BEFORE/AFTER UPDATE
        // trigger — to decide whether the (potentially expensive) conflict
        // routing runs at all. Use the union of every DO-UPDATE clause's
        // SET columns; the precise taken-clause event is derived below.
        let upsert_all_update_cols: Vec<String> = insert
            .upsert
            .iter()
            .filter_map(|clause| match &clause.action {
                fsqlite_ast::UpsertAction::Update { assignments, .. } => {
                    Some(Self::assignment_target_column_names(assignments))
                }
                fsqlite_ast::UpsertAction::Nothing => None,
            })
            .flatten()
            .collect();
        let coarse_update_event = fsqlite_ast::TriggerEvent::Update(upsert_all_update_cols);
        let table_has_upsert_update_trigger = is_do_update_upsert
            && (self.has_matching_triggers(
                table_name,
                fsqlite_ast::TriggerTiming::Before,
                &coarse_update_event,
            ) || self.has_matching_triggers(
                table_name,
                fsqlite_ast::TriggerTiming::After,
                &coarse_update_event,
            ));
        // The conflict/OLD-NEW plan is needed whenever a conflicting row
        // would change which triggers fire: BEFORE/AFTER UPDATE must fire
        // for the update, and AFTER INSERT must be SUPPRESSED because no
        // insert happened — the latter matters even with no UPDATE triggers
        // (stock fires only BEFORE INSERT on a conflict/update path).
        let upsert_needs_conflict_plan =
            is_do_update_upsert && (has_after_insert || table_has_upsert_update_trigger);
        if is_live_vtab {
            if !insert.returning.is_empty() {
                return Err(FrankenError::NotImplemented(
                    "RETURNING is not supported for live virtual-table INSERT".to_owned(),
                ));
            }
            // INSERT OR REPLACE is supported for live virtual tables
            // (delete any row conflicting on the rowid, then insert —
            // matching SQLite's REPLACE). UPSERT (`ON CONFLICT ... DO
            // ...`) and every other conflict action (ROLLBACK / ABORT /
            // FAIL / IGNORE) remain unimplemented. (GH #214)
            let is_replace = matches!(
                insert.or_conflict,
                Some(fsqlite_ast::ConflictAction::Replace)
            );
            if !insert.upsert.is_empty() || (insert.or_conflict.is_some() && !is_replace) {
                return Err(FrankenError::NotImplemented(
                    "UPSERT and conflict clauses are not supported for live virtual-table INSERT"
                        .to_owned(),
                ));
            }
        }
        if !is_live_vtab
            && let fsqlite_ast::InsertSource::Select(select_stmt) = &insert.source
        {
            // Detect simple VALUES clause (no compounds).
            // We must use the VDBE path for simple VALUES to avoid infinite recursion
            // in the fallback (which implements inserts by generating simple VALUES inserts).
            let is_simple_values = matches!(select_stmt.body.select, SelectCore::Values(_))
                && select_stmt.body.compounds.is_empty();

            // A real INSERT ... SELECT bypasses the VDBE VALUES-path
            // column-count check (codegen.rs), so a SELECT (with `*`
            // expanded) whose width does not match the INSERT target would
            // silently mis-insert. Validate it here, matching the VDBE
            // path's counts and messages exactly: no target list -> "table
            // T has N columns but M values were supplied"; an explicit list
            // -> "M values for N columns". (Simple VALUES keep flowing to the
            // VDBE path, which checks them.) bd-errmsg-parity-batch3-3brmm.
            if !is_simple_values {
                let source_cols =
                    self.select_result_column_count(select_stmt, &[], &mut Vec::new());
                let target_cols = {
                    let schemas = self.schema.borrow();
                    schemas
                        .iter()
                        .find(|t| t.name.eq_ignore_ascii_case(table_name))
                        .map(|t| {
                            if insert.columns.is_empty() {
                                t.columns
                                    .iter()
                                    .filter(|col| col.generated_expr.is_none())
                                    .count()
                            } else {
                                insert.columns.len()
                            }
                        })
                };
                if let Some(target_cols) = target_cols
                    && source_cols != target_cols
                {
                    return Err(FrankenError::FunctionError(
                        if insert.columns.is_empty() {
                            format!(
                                "table {table_name} has {target_cols} columns but {source_cols} values were supplied"
                            )
                        } else {
                            format!("{source_cols} values for {target_cols} columns")
                        },
                    ));
                }
            }

            // Complex INSERT ... SELECT statements are replayed as
            // per-row INSERT ... VALUES operations. Those inner
            // statements already handle trigger firing and change
            // tracking, so route around the outer trigger path here.
            let needs_row_by_row_replay = has_before_insert
                || has_after_insert
                || upsert_needs_conflict_plan
                || self.fk_enforcement_enabled();
            if !is_simple_values || needs_row_by_row_replay {
                self.log_mem_execution_fallback(
                    "insert_select",
                    "insert_select_row_by_row_fallback",
                )?;
                if insert.returning.is_empty() {
                    let _ = Box::pin(self.execute_insert_select_fallback(
                        insert,
                        select_stmt,
                        params,
                    ))
                    .await?;
                    return Ok(Vec::new());
                }
                return Box::pin(self.execute_insert_select_fallback_returning(
                    insert,
                    select_stmt,
                    params,
                ))
                .await;
            }
        }
        if !is_live_vtab
            && (has_before_insert
                || has_after_insert
                || upsert_needs_conflict_plan
                || self.fk_enforcement_enabled())
            && let fsqlite_ast::InsertSource::Values(rows) = &insert.source
            && rows.len() > 1
        {
            self.log_mem_execution_fallback(
                "insert_values",
                "insert_values_row_by_row_trigger_or_fk_fallback",
            )?;
            let source_rows = self
                .materialize_insert_values_source_rows(rows, params)
                .await?;
            // Source VALUES have already been materialized against the
            // caller's bindings. The replay's ?1..?N are now ROW
            // bindings, so trailing UPSERT and RETURNING placeholders
            // must first be resolved against the ORIGINAL statement's
            // globally numbered/named slots (as INSERT SELECT does).
            let resolved_insert;
            let replay_insert = match params {
                Some(params) if !insert.upsert.is_empty() || !insert.returning.is_empty() => {
                    let mut cloned = insert.clone();
                    bind_placeholders_in_insert_statement(
                        &mut cloned,
                        &mut BindParamState::default(),
                        params,
                    )?;
                    resolved_insert = cloned;
                    &resolved_insert
                }
                _ => insert,
            };
            let outcome = self
                .execute_insert_select_materialized_rows_outcome(replay_insert, &source_rows)
                .await?;
            return Ok(outcome.returning_rows);
        }
        // bd-xb07w: morsel-driven INSERT for large multi-row VALUES
        // without triggers, FK, RETURNING, or UPSERT. The morsel
        // scheduler splits the row stream into batches for future
        // parallel execution; this wiring executes them sequentially.
        if !is_live_vtab
            && insert.returning.is_empty()
            && !has_before_insert
            && !has_after_insert
            && !self.fk_enforcement_enabled()
            && insert.upsert.is_empty()
            && let fsqlite_ast::InsertSource::Values(rows) = &insert.source
            && rows.len() >= MORSEL_INSERT_THRESHOLD
        {
            self.log_mem_execution_fallback("insert_values", "morsel_insert_dispatch")?;
            let source_rows = self
                .materialize_insert_values_source_rows(rows, params)
                .await?;
            let _ = self.execute_morsel_insert(insert, source_rows).await?;
            return Ok(Vec::new());
        }
        let mut live_insert_rows = if is_live_vtab
            && !has_before_insert
            && !has_after_insert
            && matches!(insert.source, InsertSource::Select(_))
        {
            if let Some(affected) =
                Box::pin(self.try_execute_streaming_live_vtab_insert_select(insert, params)).await?
            {
                self.record_statement_changes(affected);
                return Ok(Vec::new());
            }
            Some(self.collect_live_vtab_insert_rows(insert, params).await?)
        } else if is_live_vtab {
            Some(self.collect_live_vtab_insert_rows(insert, params).await?)
        } else {
            None
        };
        // NEW row images plus the explicit rowid each row supplies (None
        // when the rowid is auto-assigned). The explicit rowid resolves
        // `NEW.rowid` in BEFORE INSERT triggers on tables without an
        // INTEGER PRIMARY KEY column (GH #205).
        let (mut trigger_new_rows, trigger_new_explicit_rowids): (
            Vec<Vec<SqliteValue>>,
            Vec<Option<i64>>,
        ) = if has_before_insert || has_after_insert || upsert_needs_conflict_plan {
            if let Some(rows) = live_insert_rows.as_ref() {
                // Live virtual-table rows are inserted through the module's
                // xUpdate; the rowid is chosen there, not known pre-insert.
                let values = rows.iter().map(|row| row.values.clone()).collect();
                let rowids = vec![None; rows.len()];
                (values, rowids)
            } else {
                self.collect_insert_trigger_rows(insert, params)
                    .await?
                    .into_iter()
                    .unzip()
            }
        } else {
            (Vec::new(), Vec::new())
        };

        // Phase 5G.2 (bd-iqam): Fire BEFORE INSERT triggers.
        let skip_dml = if has_before_insert {
            let mut skip = false;
            for (new_values, &explicit_rowid) in
                trigger_new_rows.iter().zip(&trigger_new_explicit_rowids)
            {
                // SQLite exposes `NEW.rowid` in BEFORE INSERT triggers as
                // the explicit rowid when the statement supplies one, and
                // as -1 when the rowid will be auto-assigned (it is not yet
                // known). Match that. (GH #205)
                if Box::pin(self.fire_before_triggers(
                    table_name,
                    &insert_event,
                    None,
                    Some(new_values),
                    None,
                    Some(explicit_rowid.unwrap_or(-1)),
                ))
                .await?
                {
                    skip = true;
                    break;
                }
            }
            skip
        } else {
            false
        };
        if skip_dml {
            // RAISE(IGNORE) was called - skip the INSERT.
            self.reset_statement_change_count();
            return Ok(Vec::new());
        }

        if is_live_vtab {
            let mut live_insert_rows = live_insert_rows.take().unwrap_or_else(Vec::new);
            let patch_first_column_with_rowid = {
                let key = table_name.to_ascii_uppercase();
                self.vtab_instances
                    .borrow()
                    .get(&key)
                    .is_some_and(|instance| is_rtree_instance(instance.as_ref()))
            };
            // INSERT OR REPLACE resolves a rowid conflict by deleting the
            // existing row before inserting the new one (GH #214).
            let replace = matches!(
                insert.or_conflict,
                Some(fsqlite_ast::ConflictAction::Replace)
            );
            let inserted_rowids = Box::pin(self.execute_live_vtab_insert_rows(
                table_name,
                &live_insert_rows,
                replace,
            ))
            .await?;

            if has_after_insert {
                if patch_first_column_with_rowid {
                    for (row, rowid) in live_insert_rows.iter_mut().zip(inserted_rowids.iter()) {
                        if row.values.first().is_some_and(|value| value.is_null()) {
                            row.values[0] = SqliteValue::Integer(*rowid);
                        }
                    }
                }
                for (row, rowid) in live_insert_rows.iter().zip(inserted_rowids.iter()) {
                    if let Err(error) = Box::pin(self.fire_after_triggers(
                        table_name,
                        &insert_event,
                        None,
                        Some(&row.values),
                        None,
                        Some(*rowid),
                    ))
                    .await
                    {
                        self.record_after_trigger_error_state(
                            live_insert_rows.len(),
                            inserted_rowids.last().copied(),
                        );
                        return Err(error);
                    }
                }
            }

            self.record_statement_changes(live_insert_rows.len());
            return Ok(Vec::new());
        }

        // bd-y7old / bd-bwm0x: a DO UPDATE upsert whose single attempted row
        // conflicts resolves to the UPDATE path. Determine the conflict +
        // OLD/NEW BEFORE running the VDBE program (which itself does the
        // insert-or-update), fire the BEFORE UPDATE trigger here, and
        // remember to fire AFTER UPDATE / suppress AFTER INSERT below. A
        // non-conflicting row keeps the ordinary insert path unchanged.
        //
        // Routing mirrors the VDBE (bd-aap9u/xa2qv): the FIRST ON CONFLICT
        // clause whose conflict target is violated is the one applied, so
        // `route_upsert_conflict` walks the clauses in order and returns the
        // TAKEN clause's SET columns (for `UPDATE OF <col>` matching) plus
        // its OLD/NEW plan — not always the first DO-UPDATE clause. Returns
        // None (fall back to VDBE-only triggers) when no clause conflicts, a
        // DO NOTHING conflict is taken (AFTER INSERT is then suppressed by
        // the affected==0 path), an indeterminate conflict target is reached
        // (expression/partial index, or an omitted target on a table without
        // an INTEGER PRIMARY KEY), or the taken clause's OLD/NEW cannot be
        // evaluated.
        let (update_event, has_before_update, has_after_update, upsert_update_plan) =
            if upsert_needs_conflict_plan && trigger_new_rows.len() == 1 {
                match self
                    .route_upsert_conflict(insert, &trigger_new_rows[0], params)
                    .await?
                {
                    Some((event_cols, plan)) => {
                        let event = fsqlite_ast::TriggerEvent::Update(event_cols);
                        let hbu = self.has_matching_triggers(
                            table_name,
                            fsqlite_ast::TriggerTiming::Before,
                            &event,
                        );
                        let hau = self.has_matching_triggers(
                            table_name,
                            fsqlite_ast::TriggerTiming::After,
                            &event,
                        );
                        (event, hbu, hau, Some(plan))
                    }
                    None => (
                        fsqlite_ast::TriggerEvent::Update(Vec::new()),
                        false,
                        false,
                        None,
                    ),
                }
            } else {
                (
                    fsqlite_ast::TriggerEvent::Update(Vec::new()),
                    false,
                    false,
                    None,
                )
            };
        // A conflict (plan present) always suppresses AFTER INSERT — whether
        // the DO UPDATE ... WHERE applies (an update) or not (a no-op that
        // still fired only BEFORE INSERT in stock).
        let upsert_conflicted = upsert_update_plan.is_some();
        let upsert_before_update_ignored =
            if let Some((rowid, old_row, new_row, where_true)) = &upsert_update_plan
                && *where_true
                && has_before_update
            {
                // bd-nn29x: stock runs the DO UPDATE, and so the triggers
                // it fires, under ABORT whatever this INSERT's clause is.
                let _do_update_clause = DmlConflictClauseGuard::new(
                    &self.dml_conflict_clause,
                    Some(fsqlite_ast::ConflictAction::Abort),
                );
                Box::pin(self.fire_before_triggers(
                    table_name,
                    &update_event,
                    Some(old_row),
                    Some(new_row),
                    *rowid,
                    *rowid,
                ))
                .await?
            } else {
                false
            };
        if upsert_before_update_ignored {
            // BEFORE UPDATE raised RAISE(IGNORE): skip this row's update
            // entirely (no mutation, no AFTER UPDATE, no AFTER INSERT).
            // BEFORE INSERT already fired above.
            self.reset_statement_change_count();
            return Ok(Vec::new());
        }

        // Phase 5B.2 (bd-1yi8): INSERT OR REPLACE / INSERT OR IGNORE
        // routes through VDBE which has UPSERT semantics in the
        // Insert opcode (table_move_to + delete + insert). The
        // previous execute_insert_or_conflict fast path read from
        // MemDatabase directly, which doesn't work now that
        // write-through stopped syncing inserts to MemDatabase.
        //
        // Performance note (bead bd-bldc5.4.3): re-enabling the fast
        // path requires updating execute_insert_or_conflict to read
        // from the pager via StorageCursor instead of MemDatabase.
        // The VDBE path is correct; this is an optimization only.

        let arc_prog;
        // A BEFORE trigger can invalidate code compiled at batch entry.
        // Check again here, after BEFORE and immediately before the write.
        let program: &VdbeProgram = if let Some(p) = precompiled.filter(|_| {
            replay_identity.is_none_or(|identity| self.insert_replay_identity() == identity)
        }) {
            p
        } else {
            let plan_span = tracing::span!(
                target: "fsqlite.plan",
                tracing::Level::TRACE,
                "plan",
                stage = "compile_table_insert"
            );
            record_trace_span_created();
            let _plan_guard = plan_span.enter();
            let sql_text = statement.to_string();
            let sql_key = Self::sql_hash(&sql_text);
            // A program built from resolved VALUES subqueries holds
            // their results for this execution: never reuse it.
            let _values_subquery_cache_bypass = insert_values_have_subquery(insert)
                .then(|| BoolCellRestoreGuard::new(&self.bypass_compiled_cache, true));
            arc_prog = self
                .compile_with_cache(sql_key, &sql_text, async |conn| {
                    conn.compile_table_insert(insert, params).await
                })
                .await?;
            &arc_prog
        };
        let runtime_requirements = self.insert_runtime_requirements(insert)?;
        let (rows, affected, last_insert_rowid) = self
            .execute_table_program_with_cx(
                program,
                params,
                true,
                runtime_requirements,
                cx,
                true,
            )
            .await?;

        // Implicit REPLACE deletes are parent-row deletions for inbound
        // FK purposes. Enforce their exact OLD rows before child-side
        // validation and AFTER INSERT processing.
        Box::pin(self.enforce_fk_on_replace_victims(table_name)).await?;

        // bd-thqgm: FK constraint checking on INSERT.
        // Skip FK enforcement when no row was written (e.g. an OR IGNORE
        // PK conflict, affected == 0): SQLite does not FK-check a row it
        // never inserted (#111).
        if affected > 0 && self.fk_enforcement_enabled() {
            Box::pin(self.enforce_fk_on_insert(insert, table_name, params)).await?;
        }

        // Patch trigger NEW rows: fill in auto-assigned INTEGER
        // PRIMARY KEY rowids that were NULL at pre-computation time.
        if has_after_insert && !trigger_new_rows.is_empty() {
            if affected == 0 {
                trigger_new_rows.clear();
            } else {
                let tbl_key = table_name.to_ascii_lowercase();
                if let Some(&ipk_idx) = self.rowid_alias_columns.borrow().get(&tbl_key) {
                    let last_rowid = self.current_last_insert_rowid();
                    let count = trigger_new_rows.len();
                    for (i, new_row) in trigger_new_rows.iter_mut().enumerate() {
                        if ipk_idx < new_row.len() && new_row[ipk_idx] == SqliteValue::Null {
                            #[allow(clippy::cast_possible_wrap)]
                            let rowid = last_rowid - (count as i64 - 1) + i as i64;
                            new_row[ipk_idx] = SqliteValue::Integer(rowid);
                        }
                    }
                }
            }
        }

        // Phase 5G.3: Fire AFTER INSERT triggers.
        // The direct VDBE INSERT path only reaches here single-row when
        // triggers are present (multi-row VALUES and INSERT...SELECT are
        // replayed row-by-row above), so the just-assigned
        // `last_insert_rowid` is the NEW.rowid for this row. Tables with an
        // INTEGER PRIMARY KEY carry the rowid in a snapshotted column and
        // resolve the alias there; passing it additionally is harmless and
        // makes `NEW.rowid` resolve on tables without an IPK column. (GH #205)
        // bd-y7old: a conflicting DO UPDATE upsert resolves to the UPDATE
        // path, so it fires AFTER UPDATE (below) rather than AFTER INSERT.
        if has_after_insert && !upsert_conflicted {
            let after_insert_rowid = self.current_last_insert_rowid();
            for new_values in &trigger_new_rows {
                if let Err(error) = Box::pin(self.fire_after_triggers(
                    table_name,
                    &insert_event,
                    None,
                    Some(new_values),
                    None,
                    Some(after_insert_rowid),
                ))
                .await
                {
                    self.record_after_trigger_error_state(affected, last_insert_rowid);
                    return Err(error);
                }
            }
        }
        // bd-y7old: fire AFTER UPDATE for a conflicting DO UPDATE whose WHERE
        // applied (the VDBE program performed the update above); a WHERE-false
        // conflict is a no-op that fires neither AFTER INSERT nor AFTER UPDATE.
        if let Some((rowid, old_row, new_row, where_true)) = &upsert_update_plan
            && *where_true
            && has_after_update
        {
            // bd-nn29x: the DO UPDATE's triggers run under ABORT.
            let _do_update_clause = DmlConflictClauseGuard::new(
                &self.dml_conflict_clause,
                Some(fsqlite_ast::ConflictAction::Abort),
            );
            if let Err(error) = Box::pin(self.fire_after_triggers(
                table_name,
                &update_event,
                Some(old_row),
                Some(new_row),
                *rowid,
                *rowid,
            ))
            .await
            {
                self.record_after_trigger_error_state(affected, None);
                return Err(error);
            }
        }

        if table_name.eq_ignore_ascii_case("sqlite_sequence") {
            Box::pin(self.refresh_sqlite_sequence_cache()).await?;
        } else {
            Box::pin(self.refresh_autoincrement_sequence_after_insert(
                table_name,
                affected,
                last_insert_rowid,
            ))
            .await?;
        }

        // 5D.4: Persistence now handled by pager WAL, not compat_persist.
        self.record_statement_changes(affected);
        if insert.returning.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(rows)
        }
    }
}

#[cfg(test)]
mod gh516_returning_admission {
    #[allow(clippy::wildcard_imports)]
use super::*;

    fn admits(projection: &str) -> bool {
        let sql = format!("INSERT INTO t VALUES(?1,?2) RETURNING {projection}");
        let Statement::Insert(insert) = parse_single_statement(&sql).unwrap() else {
            panic!("INSERT fixture");
        };
        reusable_insert_returning(&insert.returning)
    }

    #[test]
    fn row_local_projections_are_reusable_without_evaluation() {
        for projection in [
            "*",
            "id,v,?7",
            "id + 1, -id, CAST(id AS TEXT), v COLLATE NOCASE",
            "CASE id WHEN 1 THEN v ELSE NULL END",
            "CASE WHEN id BETWEEN 1 AND 3 THEN v ELSE 'other' END",
            "v IS NULL, v IS NOT NULL",
        ] {
            assert!(admits(projection), "{projection}");
        }
    }

    #[test]
    fn nested_reads_and_calls_never_enter_the_shared_program() {
        for projection in [
            "upper(v)",
            "(SELECT 1)",
            "id + (SELECT 1)",
            "CASE WHEN id=1 THEN upper(v) ELSE v END",
            "CASE WHEN EXISTS(SELECT 1) THEN id ELSE 0 END",
            "id BETWEEN 1 AND (SELECT 3)",
            "v LIKE 'a%'",
            "id IN (1,2)",
            "CURRENT_TIMESTAMP",
        ] {
            assert!(!admits(projection), "{projection}");
        }
    }
}
