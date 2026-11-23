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

/// bd-fpr5g lever 3b: the compiled program of one trigger body statement
/// (a `TriggerBodyTemplate`'s), run without the dispatcher's per-statement
/// validation and rewriting.
///
/// The program is the compiled cache's own entry for the text the ordinary
/// dispatcher compiles for this statement, taken after that dispatcher ran
/// the statement and it succeeded. It stays usable while the replay identity
/// is unchanged and the cache still holds that same entry, which is exactly
/// when the ordinary dispatcher would run it too.
pub(super) struct TriggerBodyKernel {
    program: Arc<VdbeProgram>,
    sql_key: u64,
    sql_text: Rc<str>,
    identity: InsertReplayIdentity,
}

#[cfg(test)]
thread_local! {
    /// Trigger body statements this thread ran through a kernel, for the lib
    /// tests that prove the kernel path is the one taken.
    static TRIGGER_BODY_KERNEL_RUNS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Rows this thread replayed through a compiled INSERT row program
    /// (`execute_compiled_insert_replay_row` with its identity unchanged), for
    /// the lib tests of which streams take it (bd-fpr5g lever 5).
    static COMPILED_INSERT_REPLAY_ROWS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Whether a trigger body template's statement runs through a kernel.
pub(super) enum TriggerBodyKernelSlot {
    /// None yet: the next ordinary run that succeeds captures one.
    Pending,
    Ready(Rc<TriggerBodyKernel>),
    /// The statement's shape is not admitted; it always runs the ordinary way.
    Refused,
}

/// The row image returned by the write program is the source of FK values.
/// Candidate INSERT values and interpreted SET expressions can differ from
/// the row actually written by the DO UPDATE arm.
struct UpsertForeignKeySnapshot {
    columns: Vec<ResultColumn>,
    updated_keys: Vec<usize>,
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
    /// Capture actual child FK values for a rowid UPSERT with one update arm.
    ///
    /// A rowid program reports whether this execution inserted a row through
    /// its last-insert receipt; UPDATE deliberately leaves that receipt empty.
    ///
    /// With one DO UPDATE arm, that also identifies exactly which child keys
    /// the update assigns without evaluating its SET expressions a second time.
    ///
    /// WITHOUT ROWID and multiple DO UPDATE arms need a richer program receipt
    /// to identify the action and taken arm. Keep their existing enforcement
    /// path until that information is available.
    fn upsert_foreign_key_snapshot(
        &self,
        insert: &fsqlite_ast::InsertStatement,
    ) -> Option<UpsertForeignKeySnapshot> {
        if !self.fk_enforcement_enabled()
            || !self.temp_storage_roots().is_empty()
            || self.targets_shadowed_main(&insert.table)
            || insert
                .table
                .schema
                .as_deref()
                .is_some_and(|schema| !schema.eq_ignore_ascii_case("main"))
        {
            return None;
        }
        let mut updates = insert.upsert.iter().filter_map(|clause| match &clause.action {
            fsqlite_ast::UpsertAction::Update { assignments, .. } => Some(assignments.as_slice()),
            fsqlite_ast::UpsertAction::Nothing => None,
        });
        let assignments = updates.next()?;
        if updates.next().is_some() {
            return None;
        }
        let columns = {
            let schema = self.schema.borrow();
            let table = schema
                .iter()
                .find(|table| table.name.eq_ignore_ascii_case(&insert.table.name))?;
            if table.without_rowid || table.foreign_keys.is_empty() {
                return None;
            }
            // Generated child keys need dependency-aware SET-column tracking;
            // the existing helper tracks directly assigned columns only.
            if table.foreign_keys.iter().any(|fk| {
                fk.child_columns.iter().any(|&column| {
                    table
                        .columns
                        .get(column)
                        .is_none_or(|column| column.generated_expr.is_some())
                })
            }) {
                return None;
            }
            table
                .columns
                .iter()
                .map(|column| ResultColumn::Expr {
                    expr: Expr::Column(ColumnRef::bare(column.name.clone()), Span::ZERO),
                    alias: None,
                })
                .collect()
        };
        Some(UpsertForeignKeySnapshot {
            columns,
            updated_keys: self.fk_keys_modified_by_update(&insert.table.name, assignments),
        })
    }

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
        source_rows_materialized: bool,
    ) -> Result<Option<CompiledInsertReplay>> {
        // Keep SELECT streaming, UPSERT OLD/NEW routing, non-row-local
        // RETURNING, virtual tables, and attached/TEMP ownership on their established
        // paths. FAIL/ROLLBACK also retain their historical replay entry until
        // their independently nested failure boundaries have a fast-path proof.
        // This is a positive admission rule, not a global fallback bypass.
        //
        // bd-fpr5g lever 5: a SELECT source is admitted when the stream's rows
        // were all evaluated before it started (a materialized chunk): such a
        // row is the same input as an evaluated VALUES row, and nothing is read
        // between rows. A stream whose producer reads its source while rows are
        // inserted keeps the prepared replay.
        let source_admitted = match &original.source {
            InsertSource::Values(rows) => rows.len() > 1,
            InsertSource::Select(_) => source_rows_materialized,
            InsertSource::DefaultValues => false,
        };
        if !source_admitted
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
            || self
                .temp_table_names
                .borrow()
                .contains(&original.table.name.to_ascii_lowercase())
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
        // bd-fpr5g lever 5: TEMP tables and materialized WITH / view bindings
        // are MemDatabase roots from the connection's TEMP namespace, which no
        // main root shares, and the routing and index suppression a compile
        // applies under them touch only those roots. A main target's template
        // (whose CHECK, DEFAULT and generated expressions read no table) opens
        // none of them, so their presence leaves its program as it is. Checked
        // here rather than assumed: a template that would open one stays on the
        // prepared replay.
        let temp_roots = self.temp_storage_roots();
        if !temp_roots.is_empty()
            && program
                .storage_root_usages()
                .any(|usage| temp_roots.contains(&usage.root_page))
        {
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
            #[cfg(test)]
            COMPILED_INSERT_REPLAY_ROWS.with(|rows| rows.set(rows.get() + 1));
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
            let previous_changes = self.last_changes.get();
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
                        previous_changes,
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

    /// bd-fpr5g lever 3b: whether a trigger body statement's shape runs
    /// through a [`TriggerBodyKernel`]. Checked once per template, after the
    /// ordinary dispatcher ran the statement and it succeeded.
    ///
    /// Admitted: a one-row INSERT ... VALUES, an UPDATE or a DELETE with no
    /// WITH, FROM, RETURNING, ORDER BY, LIMIT, upsert, subquery or row value,
    /// already in canonical placeholder form, that no outer-layer rewrite
    /// changes, on a MAIN table that is not a view, a virtual table, a sqlite_
    /// table or a TEMP name, and that has no triggers of its own. For such a
    /// statement every validation and rewrite the dispatcher runs before the
    /// program is a function of the statement and the schema, which the
    /// kernel's identity pins; what it does with the program is the arm's
    /// tail the kernel runs. Everything else (nested triggers among it) keeps
    /// the ordinary path.
    fn trigger_body_kernel_admits(&self, statement: &Statement) -> bool {
        let no_subquery = |expr: &Expr| !expr_contains_subquery_match(expr, &mut |_| true);
        let (target, canonical) = match statement {
            Statement::Insert(insert) => {
                let InsertSource::Values(rows) = &insert.source else {
                    return false;
                };
                if insert.with.is_some()
                    || !insert.upsert.is_empty()
                    || !insert.returning.is_empty()
                    || rows.len() != 1
                    || !rows.iter().flatten().all(no_subquery)
                    || insert_values_have_subquery(insert)
                {
                    return false;
                }
                let canonical = insert_is_plain(insert)
                    || canonicalize_insert_placeholders(insert).is_ok_and(|c| c == *insert);
                (&insert.table, canonical)
            }
            Statement::Update(update) => {
                if update.with.is_some()
                    || update.from.is_some()
                    || !update.returning.is_empty()
                    || !update.order_by.is_empty()
                    || update.limit.is_some()
                    || update.table.time_travel.is_some()
                    || !update
                        .assignments
                        .iter()
                        .all(|assignment| no_subquery(&assignment.value))
                    || !update.where_clause.as_ref().is_none_or(no_subquery)
                {
                    return false;
                }
                let canonical = update_is_plain(update)
                    || canonicalize_update_placeholders(update).is_ok_and(|c| c == *update);
                (&update.table.name, canonical)
            }
            Statement::Delete(delete) => {
                if delete.with.is_some()
                    || !delete.returning.is_empty()
                    || !delete.order_by.is_empty()
                    || delete.limit.is_some()
                    || delete.table.time_travel.is_some()
                    || !delete.where_clause.as_ref().is_none_or(no_subquery)
                {
                    return false;
                }
                let canonical = delete_is_plain(delete)
                    || canonicalize_delete_placeholders(delete).is_ok_and(|c| c == *delete);
                (&delete.table.name, canonical)
            }
            _ => return false,
        };
        let name = target.name.as_str();
        canonical
            && target
                .schema
                .as_deref()
                .is_none_or(|schema| schema.eq_ignore_ascii_case("main"))
            && !is_sqlite_schema_name(name)
            && !name.eq_ignore_ascii_case("sqlite_sequence")
            && !self
                .temp_table_names
                .borrow()
                .contains(&name.to_ascii_lowercase())
            && self.view_index_of(name).is_none()
            && !self.has_live_vtab_instance(name)
            && !self.table_name_is_virtual(name)
            && self
                .schema
                .borrow()
                .iter()
                .any(|table| table.name.eq_ignore_ascii_case(name))
            && !self
                .triggers
                .borrow()
                .iter()
                .any(|trigger| trigger.table_name.eq_ignore_ascii_case(name))
            // The dispatcher delegates to an attached connection only when
            // this names one (statement_may_target_attached_schema is a
            // coarser gate: it holds for every UPDATE and DELETE).
            && matches!(self.attached_target_schema(target), Ok(None))
            && matches!(
                fsqlite_ast::row_value::check_statement_row_values(statement),
                Ok(false)
            )
            && !statement_contains_rewritable_subquery(statement)
            && !statement_requires_in_table_name_rewrite(statement)
            && !statement_contains_deferred_values(statement)
            && expand_statement_hidden_table_function_stars(statement).is_none()
            && matches!(
                self.rewrite_statement_bare_pragma_table_functions(statement),
                Cow::Borrowed(_)
            )
            && matches!(self.bind_schema_qualified_columns(statement), Ok(None))
    }

    /// The kernel for a template's `statement`, after the ordinary dispatcher
    /// ran it with `params` and it succeeded: the compiled cache's entry for
    /// the text that dispatcher compiled. `Pending` when the cache no longer
    /// holds it (the next successful run tries again).
    pub(super) fn capture_trigger_body_kernel(
        &self,
        statement: &Statement,
        params: Option<&[SqliteValue]>,
    ) -> TriggerBodyKernelSlot {
        if !self.trigger_body_kernel_admits(statement) {
            return TriggerBodyKernelSlot::Refused;
        }
        // The same tree and text list each arm passes when it compiles.
        let (sql_text, sql_key) = match statement {
            Statement::Insert(_) => {
                self.dispatch_statement_text(params, statement, |seen| &mut seen.inserts)
            }
            Statement::Update(update) => {
                self.dispatch_statement_text(params, update, |seen| &mut seen.updates)
            }
            Statement::Delete(delete) => {
                self.dispatch_statement_text(params, delete, |seen| &mut seen.deletes)
            }
            _ => return TriggerBodyKernelSlot::Refused,
        };
        let Some(entry) = self.lookup_compiled_cache(sql_key, &sql_text) else {
            return TriggerBodyKernelSlot::Pending;
        };
        TriggerBodyKernelSlot::Ready(Rc::new(TriggerBodyKernel {
            program: Arc::clone(&entry.program),
            sql_key,
            sql_text: Rc::from(&*sql_text),
            identity: self.insert_replay_identity(),
        }))
    }

    /// Whether `kernel`'s program is still the one the ordinary dispatcher
    /// would run: the replay identity is unchanged and the compiled cache
    /// still holds that entry.
    pub(super) fn trigger_body_kernel_is_current(&self, kernel: &TriggerBodyKernel) -> bool {
        self.insert_replay_identity() == kernel.identity
            && self
                .lookup_compiled_cache(kernel.sql_key, &kernel.sql_text)
                .is_some_and(|entry| Arc::ptr_eq(&entry.program, &kernel.program))
    }

    /// Whether a kernel may run in the connection's current state. Each of
    /// these makes the ordinary dispatcher do something for a body statement
    /// that the kernel does not: compile without the cache, enforce foreign
    /// keys through its validation scope, retry at an autocommit boundary,
    /// refresh committed pager state, refuse an encryption-locked database,
    /// or emit per-statement trace events.
    pub(super) fn trigger_body_kernel_allowed(&self) -> bool {
        !self.pragma_state.borrow().foreign_keys
            && !self.bypass_compiled_cache.get()
            && !self.autocommit_conflict_retry_boundary()
            && !self.committed_pager_refresh_allowed()
            && !self.is_encryption_locked()
            && self.trace_registration.borrow().is_none()
            && !tracing::enabled!(target: "fsqlite.statement", tracing::Level::DEBUG)
            && !tracing::enabled!(target: "fsqlite.parse", tracing::Level::TRACE)
            && !tracing::enabled!(target: "fsqlite.statement_reuse", tracing::Level::INFO)
    }

    /// Run a trigger body statement through `kernel`. The caller checked
    /// [`Self::trigger_body_kernel_allowed`] and
    /// [`Self::trigger_body_kernel_is_current`].
    ///
    /// The steps with effects are the ordinary dispatcher's, in its order:
    /// `execute_statement`'s entry, `execute_statement_impl_body`'s
    /// bookkeeping and `plan_statement_execution`, its statement savepoint,
    /// failure restore, conflict rollback and autocommit resolve, and the
    /// dispatch gates (encoding, read-only). What is skipped is fixed for an
    /// admitted statement under an unchanged identity: the rewrites (none
    /// applies), the relation and semantic validation (it passed), and the
    /// arm's compile (this is its program). An INSERT then runs the shared
    /// INSERT kernel with the program; an UPDATE or DELETE runs its arm's
    /// tail for a target without triggers.
    pub(super) fn execute_trigger_body_kernel<'a>(
        &'a self,
        kernel: &'a TriggerBodyKernel,
        statement: &'a Statement,
        params: Option<&'a [SqliteValue]>,
    ) -> FallbackStatementFuture<'a, Result<()>> {
        self.with_fallback_statement(Box::pin(async move {
            #[cfg(test)]
            TRIGGER_BODY_KERNEL_RUNS.with(|runs| runs.set(runs.get() + 1));
            self.background_status()?;
            self.txn_replay_note_statement(statement);
            self.retained_flush_dropped_pending_writes.set(false);
            let _record_profile_scope =
                enter_record_profile_scope(RecordProfileScope::CoreConnection);
            self.clear_table_program_error_state();
            self.forget_constraint_failure_action_at_top_level();
            self.sync_change_tracking_context();
            let _statement_exec_depth_guard = self.enter_statement_exec();
            let _dml_conflict_clause_guard = DmlConflictClauseGuard::new(
                &self.dml_conflict_clause,
                statement_dml_conflict_clause(statement),
            );
            let StatementExecutionPlan {
                op_cx,
                capture_time_travel_snapshot,
                schema_change_boundary,
                was_auto,
                ..
            } = self.plan_statement_execution(statement).await?;
            let previous_changes = self.last_changes.get();
            let previous_total_changes = self.total_changes.get();
            let previous_trigger_step_changes = self.trigger_step_changes.get();
            let previous_last_insert_rowid = self.current_last_insert_rowid();
            self.txn_metrics_note_write();
            let preserve_prior_changes =
                statement_preserves_prior_changes_on_constraint(statement);
            let rollback_on_constraint_violation =
                statement_rolls_back_transaction_on_constraint(statement);
            let result = if self.should_use_statement_savepoint(was_auto) {
                let purpose = match statement {
                    Statement::Insert(_) => "insert",
                    Statement::Update(_) => "update",
                    _ => "delete",
                };
                self.with_internal_statement_savepoint_and_cx(
                    &op_cx,
                    purpose,
                    preserve_prior_changes,
                    false,
                    async || {
                        self.run_trigger_body_kernel_program(&op_cx, kernel, statement, params)
                            .await
                            .map_err(|error| Self::qualify_missing_target_table(error, statement))
                    },
                )
                .await
            } else {
                self.run_trigger_body_kernel_program(&op_cx, kernel, statement, params)
                    .await
                    .map_err(|error| Self::qualify_missing_target_table(error, statement))
            };
            let mut preserved_constraint_failure_rows = false;
            let result = match result {
                Ok(()) => Ok(()),
                Err(error) => {
                    preserved_constraint_failure_rows = self.restore_failed_statement_tracking(
                        preserve_prior_changes,
                        &error,
                        previous_changes,
                        previous_total_changes,
                        previous_trigger_step_changes,
                        previous_last_insert_rowid,
                    );
                    match self
                        .maybe_rollback_transaction_for_conflict_action(
                            rollback_on_constraint_violation,
                            was_auto,
                            &error,
                        )
                        .await
                    {
                        Ok(()) => Err(error),
                        Err(rollback_error) => Err(rollback_error),
                    }
                }
            };
            let commit_autocommit_on_error = was_auto
                && (preserved_constraint_failure_rows
                    || matches!(result.as_ref(), Err(FrankenError::RaiseFail(_))));
            let ok = result.is_ok() || commit_autocommit_on_error;
            self.resolve_autocommit_txn_with_dirty_table_and_capture_and_cx(
                was_auto,
                ok,
                Self::extract_written_table_name(statement),
                capture_time_travel_snapshot,
                schema_change_boundary,
                &op_cx,
            )
            .await?;
            result
        }))
    }

    /// The dispatch of a kernel's statement: the gates
    /// `execute_statement_dispatch_with_fk_scope` and `_impl` apply to it, then
    /// its program. The caller qualifies a missing-table error as that
    /// dispatcher does.
    async fn run_trigger_body_kernel_program(
        &self,
        cx: &Cx,
        kernel: &TriggerBodyKernel,
        statement: &Statement,
        params: Option<&[SqliteValue]>,
    ) -> Result<()> {
        self.guard_mutation_encoding_supported(statement)?;
        if self.pager.is_readonly() {
            return Err(FrankenError::ReadOnly);
        }
        self.clear_prepared_direct_insert_append_hint();
        match statement {
            Statement::Insert(insert) => {
                lean_future(|| {
                    self.execute_insert_dispatch(
                        cx,
                        statement,
                        insert,
                        params,
                        Some(&kernel.program),
                        Some(kernel.identity),
                    )
                })
                .await?;
            }
            Statement::Update(_) | Statement::Delete(_) => {
                let (_, affected, _) = lean_future(|| {
                    self.execute_table_program_with_cx(
                        &kernel.program,
                        params,
                        false,
                        TableExecutionRuntimeRequirements::read_path(),
                        cx,
                        true,
                        Vec::new(),
                        None,
                    )
                })
                .await?;
                if let Statement::Update(update) = statement {
                    // UPDATE OR REPLACE may delete another row: the arm's
                    // victim pass (a no-op without victims).
                    boxed_future(|| self.enforce_fk_on_replace_victims(&update.table.name)).await?;
                }
                self.record_statement_changes(affected);
            }
            _ => {
                return Err(FrankenError::internal(
                    "trigger body kernel holds a statement that is not INSERT, UPDATE or DELETE",
                ));
            }
        }
        Ok(())
    }

    /// bd-i3csr.1 (GH#491): whether `INSERT INTO t [(cols)] SELECT ... FROM s
    /// [WHERE ...]` runs as the one program `codegen_insert_select` emits
    /// (scan `s`, insert each row as it is read) instead of materializing the
    /// source and replaying it as INSERT ... VALUES. The caller has already
    /// declined a WITH clause, INSERT triggers, an UPSERT that needs conflict
    /// routing, foreign-key enforcement and a live virtual-table target.
    ///
    /// That program reads one base table in rowid order with an optional
    /// WHERE and does nothing else, so every shape it would get wrong
    /// declines here:
    /// - the source: `insert_select_keyset_rowid` admits one local rowid table
    ///   scanned in rowid order (no join, compound, DISTINCT, grouping,
    ///   aggregate, window, ORDER BY, LIMIT or WITH, and no WHERE on a column
    ///   an index leads with). Stock scans a covering index in index order,
    ///   which fsqlite does not do yet (bd-1bs3h), so a source with an index
    ///   holding every column the SELECT reads declines too, as does any
    ///   expression or partial index and any index hint.
    /// - reads during the inserts: the source is not the target (stock copies
    ///   a self-read to an ephemeral table first), the SELECT has no subquery
    ///   and no MATCH, and reads no change counter (each row would see the
    ///   rows inserted before it).
    /// - the target: an unqualified rowid table, no TEMP table in the
    ///   connection, no DELETE trigger (an OR REPLACE victim), and no UPSERT
    ///   or RETURNING.
    ///
    /// A generate_series source (bd-i3csr.2) is scanned through a
    /// virtual-table cursor instead; `insert_select_series_source_admitted`
    /// says which.
    fn insert_select_runs_as_one_program(
        &self,
        insert: &fsqlite_ast::InsertStatement,
        select_stmt: &SelectStatement,
    ) -> bool {
        if !insert.upsert.is_empty()
            || !insert.returning.is_empty()
            || insert.table.schema.is_some()
            || is_sqlite_schema_name(&insert.table.name)
            || insert.table.name.eq_ignore_ascii_case("sqlite_sequence")
            || !self.temp_storage_roots().is_empty()
            || select_may_observe_change_tracking(select_stmt)
            || select_contains_subquery_matching(select_stmt, self, &|_, _| true)
            || select_contains_match_operator(select_stmt)
        {
            return false;
        }
        let SelectCore::Select {
            columns,
            from: Some(from),
            where_clause,
            ..
        } = &select_stmt.body.select
        else {
            return false;
        };
        let target = insert.table.name.as_str();
        let target_is_rowid_table = self
            .schema
            .borrow()
            .iter()
            .find(|table| table.name.eq_ignore_ascii_case(target))
            .is_some_and(|table| !table.without_rowid);
        let delete_event = fsqlite_ast::TriggerEvent::Delete;
        if !target_is_rowid_table
            || self.has_matching_triggers(target, fsqlite_ast::TriggerTiming::Before, &delete_event)
            || self.has_matching_triggers(target, fsqlite_ast::TriggerTiming::After, &delete_event)
        {
            return false;
        }
        let name = match &from.source {
            TableOrSubquery::TableFunction { name, args, .. } => {
                return self.insert_select_series_source_admitted(
                    select_stmt,
                    name,
                    args,
                    columns,
                    where_clause.as_deref(),
                );
            }
            TableOrSubquery::Table {
                name,
                index_hint: None,
                time_travel: None,
                ..
            } => name,
            _ => return false,
        };
        if self.insert_select_keyset_rowid(select_stmt).is_none()
            || name.schema.is_some()
            || name.name.eq_ignore_ascii_case(target)
            || name.name.to_ascii_lowercase().starts_with("sqlite_")
        {
            return false;
        }
        let Some((source, _)) = self.local_join_binding_table(name) else {
            return false;
        };
        // The source columns the SELECT reads, besides the rowid that every
        // index entry carries.
        let mut reads_every_column = false;
        let mut read_columns: Vec<Arc<str>> = Vec::new();
        let mut note_column = |column_ref: &ColumnRef| {
            read_columns.push(Arc::clone(&column_ref.column));
        };
        for column in columns {
            match column {
                ResultColumn::Star | ResultColumn::TableStar(_) => reads_every_column = true,
                ResultColumn::Expr { expr, .. } => {
                    for_each_column_ref_in_expr(expr, &mut note_column);
                }
            }
        }
        if let Some(where_clause) = where_clause {
            for_each_column_ref_in_expr(where_clause, &mut note_column);
        }
        let needed: Vec<&str> = source
            .columns
            .iter()
            .filter(|column| !column.is_ipk)
            .map(|column| column.name.as_str())
            .filter(|column| {
                reads_every_column
                    || read_columns
                        .iter()
                        .any(|read| read.eq_ignore_ascii_case(column))
            })
            .collect();
        !source.indexes.iter().any(|index| {
            index.columns.is_empty()
                || index.where_clause.is_some()
                || needed.iter().all(|column| {
                    index
                        .columns
                        .iter()
                        .any(|indexed| indexed.eq_ignore_ascii_case(column))
                })
        })
    }

    /// bd-i3csr.2: whether `INSERT ... SELECT ... FROM generate_series(...)`
    /// runs as one program scanning the series through a virtual-table cursor.
    /// The program calls the module's xFilter once with the call's arguments,
    /// as the interpreted scan does, and reads each row as it is produced, so
    /// it admits only a scan nothing has to see whole: one call to the
    /// builtin module (an application that registered its own
    /// generate_series keeps the interpreted scan) with 1 to 3 arguments that
    /// read no column, no join, compound, DISTINCT, grouping, aggregate,
    /// window, ORDER BY, LIMIT or WITH, and only `value` read: `*`, the hidden
    /// start, stop and step columns and the rowid decline.
    fn insert_select_series_source_admitted(
        &self,
        select_stmt: &SelectStatement,
        name: &str,
        args: &[Expr],
        columns: &[ResultColumn],
        where_clause: Option<&Expr>,
    ) -> bool {
        if !name.eq_ignore_ascii_case("generate_series")
            || !(1..=3).contains(&args.len())
            || self.builtin_generate_series_module().is_none()
            || select_stmt.with.is_some()
            || !select_stmt.body.compounds.is_empty()
            || !select_stmt.order_by.is_empty()
            || select_stmt.limit.is_some()
            || has_window_functions(select_stmt)
            || self.has_implicit_aggregation_with_registry(select_stmt)
        {
            return false;
        }
        let SelectCore::Select {
            distinct,
            from: Some(from),
            group_by,
            having,
            windows,
            ..
        } = &select_stmt.body.select
        else {
            return false;
        };
        if *distinct != Distinctness::All
            || !from.joins.is_empty()
            || !group_by.is_empty()
            || having.is_some()
            || !windows.is_empty()
            || columns.is_empty()
        {
            return false;
        }
        // A name the function does not declare is left to the code generator,
        // which reports it as stock does ("no such column").
        let mut reads_no_hidden_column = true;
        let mut note_column = |column_ref: &ColumnRef| {
            reads_no_hidden_column &= !["start", "stop", "step", "rowid", "_rowid_", "oid"]
                .iter()
                .any(|hidden| column_ref.column.eq_ignore_ascii_case(hidden));
        };
        for column in columns {
            match column {
                ResultColumn::Expr { expr, .. } => {
                    for_each_column_ref_in_expr(expr, &mut note_column);
                }
                ResultColumn::Star | ResultColumn::TableStar(_) => return false,
            }
        }
        if let Some(where_clause) = where_clause {
            for_each_column_ref_in_expr(where_clause, &mut note_column);
        }
        let mut arguments_read_columns = false;
        for argument in args {
            for_each_column_ref_in_expr(argument, &mut |_| arguments_read_columns = true);
        }
        reads_no_hidden_column && !arguments_read_columns
    }

    /// bd-i3csr.2: the builtin generate_series module, unless the application
    /// registered its own under that name.
    fn builtin_generate_series_module(&self) -> Option<Arc<dyn VtabModuleFactory>> {
        let modules = self.vtab_modules.borrow();
        let registered = modules.get("GENERATE_SERIES")?;
        let builtin = shared_default_vtab_module_registry().get("GENERATE_SERIES")?;
        Arc::ptr_eq(registered, builtin).then(|| Arc::clone(registered))
    }

    /// bd-i3csr.2: the columns `codegen_insert_select` resolves a
    /// generate_series source against, when the INSERT ... SELECT reads the
    /// builtin module.
    pub(super) fn insert_select_function_columns(
        &self,
        insert: &fsqlite_ast::InsertStatement,
    ) -> Option<Vec<String>> {
        let InsertSource::Select(select_stmt) = &insert.source else {
            return None;
        };
        let SelectCore::Select {
            from: Some(from), ..
        } = &select_stmt.body.select
        else {
            return None;
        };
        let TableOrSubquery::TableFunction { name, .. } = &from.source else {
            return None;
        };
        (name.eq_ignore_ascii_case("generate_series")
            && self.builtin_generate_series_module().is_some())
        .then(|| {
            GENERATE_SERIES_TABLE_COLUMN_NAMES
                .iter()
                .map(|column| (*column).to_owned())
                .collect()
        })
    }

    /// bd-i3csr.2: the instances an INSERT ... SELECT program scans, one per
    /// `VOpen` cursor, connected from the module its P4 names: the builtin
    /// generate_series, the only one `insert_select_function_columns` hands
    /// to the code generator.
    fn insert_program_vtab_instances(
        &self,
        insert: &fsqlite_ast::InsertStatement,
        program: &VdbeProgram,
    ) -> Result<Vec<(i32, Box<dyn ErasedVtabInstance>)>> {
        let reads_function = matches!(
            &insert.source,
            InsertSource::Select(select_stmt) if matches!(
                &select_stmt.body.select,
                SelectCore::Select { from: Some(from), .. }
                    if matches!(from.source, TableOrSubquery::TableFunction { .. })
            )
        );
        if !reads_function {
            return Ok(Vec::new());
        }
        let mut instances = Vec::new();
        for op in program.ops() {
            if op.opcode != Opcode::VOpen {
                continue;
            }
            let P4::Str(name) = &op.p4 else {
                continue;
            };
            let factory = name
                .eq_ignore_ascii_case("generate_series")
                .then(|| self.builtin_generate_series_module())
                .flatten()
                .ok_or_else(|| {
                    FrankenError::internal(format!(
                        "INSERT ... SELECT program scans table-valued function {name} \
                         without its module"
                    ))
                })?;
            let cx = self.op_cx()?;
            let factory_args = [name.as_str(), "main", name.as_str()];
            instances.push((op.p1, factory.connect(&cx, &factory_args)?));
        }
        Ok(instances)
    }

    /// A RETURNING subquery left for execution must retain its own lexical
    /// scope even when it is not correlated. The scalar/EXISTS emitters do
    /// not handle WITH, derived tables or every LIMIT/aggregate shape; those
    /// expressions are projected through SELECT at the changed-row boundary.
    fn returning_has_uncompiled_subquery(&self, returning: &[ResultColumn]) -> bool {
        returning.iter().any(|column| {
            let ResultColumn::Expr { expr, .. } = column else {
                return false;
            };
            expr_contains_subquery_matching_deep(expr, self, &|subquery, conn| match subquery {
                SubqueryExprRef::Scalar(inner) => {
                    !scalar_subquery_answered_by_table_scan(inner, conn)
                }
                SubqueryExprRef::Exists(inner) => {
                    !exists_subquery_answered_by_table_scan(inner, conn)
                }
                SubqueryExprRef::In(inner) => {
                    !in_subquery_supported_by_vdbe(inner, conn)
                        || is_correlated_subquery_with_schema(inner, &conn.schema.borrow())
                }
            })
        })
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
        // Both FTS5 command forms name the table itself as the first column
        // (`INSERT INTO ft(ft, ...)`). Any other INSERT is neither, so it does
        // not allocate their futures (bd-w4pr1.13.35).
        if fts5_insert_uses_command_column(insert) {
            if Box::pin(self.execute_fts5_maintenance_insert(insert, params)).await? {
                return Ok(Vec::new());
            }
            #[cfg(feature = "ext-fts5")]
            if let Some(affected) =
                Box::pin(self.execute_fts5_magic_delete_insert(insert, params)).await?
            {
                self.record_statement_changes(affected);
                return Ok(Vec::new());
            }
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
        // bd-d8s37: a conflicting DO UPDATE on a foreign-key parent table must
        // enforce ON UPDATE RESTRICT / NO ACTION and execute ON UPDATE CASCADE /
        // SET NULL / SET DEFAULT before the parent's AFTER UPDATE triggers fire.
        let table_has_upsert_fk_parent_work = is_do_update_upsert
            && self.fk_cascade_propagation_enabled()
            && self.table_is_foreign_key_parent(table_name);
        // bd-jxurb: a DO UPDATE subquery the compiled program cannot answer
        // is bound for each row's conflicting row, one row at a time.
        let upsert_binds_subqueries = is_do_update_upsert
            && !is_live_vtab
            && self.upsert_has_uncompiled_correlated_subquery(insert);
        // The conflict/OLD-NEW plan is needed whenever a conflicting row
        // would change which triggers fire or enforce parent-side FK actions:
        // BEFORE/AFTER UPDATE must fire for the update, AFTER INSERT must be
        // SUPPRESSED because no insert happened, and parent-side ON UPDATE FK
        // checks/cascades must run on the updated parent key; or when the
        // conflicting row binds the DO UPDATE's subqueries.
        let upsert_needs_conflict_plan = is_do_update_upsert
            && (has_after_insert
                || table_has_upsert_update_trigger
                || table_has_upsert_fk_parent_work
                || upsert_binds_subqueries);
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
        // bd-d8fxs: INSERT needs the same changed-row projection boundary as
        // UPDATE/DELETE. In particular, a nested WITH or derived table must
        // not reach emit_scalar_subquery, which cannot preserve that scope.
        // bd-ow713, bd-j9tg3: nor may a subquery the fold left in place: one
        // that re-reads the target, which stock evaluates again after each
        // row is inserted, or one that may read what the first row changes,
        // which stock evaluates once at that row.
        let returning_projection = if !is_live_vtab
            && (self.returning_has_uncompiled_subquery(&insert.returning)
                || self.returning_defers_fold(&insert.returning, table_name))
        {
            self.log_mem_execution_fallback("insert", "insert_row_by_row_returning_fallback")?;
            Some(self.prepare_dml_returning_projection(
                &fsqlite_ast::QualifiedTableRef {
                    name: insert.table.clone(),
                    alias: None,
                    index_hint: None,
                    time_travel: None,
                },
                &insert.returning,
            )?)
        } else {
            None
        };
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
                || returning_projection.is_some()
                || self.fk_enforcement_enabled();
            // bd-i3csr.1: a source the INSERT program can scan itself runs
            // as that one program (the VDBE path below), not materialized
            // and replayed.
            let one_program = !is_simple_values
                && !needs_row_by_row_replay
                && self.insert_select_runs_as_one_program(insert, select_stmt);
            if (!is_simple_values || needs_row_by_row_replay) && !one_program {
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
                || returning_projection.is_some()
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
            let replay_insert = if !insert.upsert.is_empty() || !insert.returning.is_empty() {
                // These are caller bindings, before replay assigns ?1..?N to
                // source values. Omitted bindings mean NULL, whereas an
                // explicitly supplied short slice must still be rejected.
                let mut bind_state = BindParamState::default();
                let mut cloned =
                    canonicalize_insert_placeholders_with_state(insert, &mut bind_state)?;
                let unbound_params;
                let original_params = if let Some(params) = params {
                    params
                } else {
                    let parameter_slots =
                        usize::try_from(bind_state.next_index.saturating_sub(1)).unwrap_or(0);
                    unbound_params = vec![SqliteValue::Null; parameter_slots];
                    &unbound_params
                };
                bind_placeholders_in_insert_statement(
                    &mut cloned,
                    &mut BindParamState::default(),
                    original_params,
                )?;
                resolved_insert = cloned;
                &resolved_insert
            } else {
                insert
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
                    &insert.table,
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
                        &insert.table,
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

        // bd-jxurb: after BEFORE INSERT (whose effects a DO UPDATE sees), the
        // subqueries the compiled DO UPDATE cannot answer take their values
        // for this row, and the statement with those values is what the
        // program is compiled from and what the routing below reads.
        let bound_upsert;
        let (statement, insert, precompiled) = if upsert_binds_subqueries {
            let [attempted] = trigger_new_rows.as_slice() else {
                return Err(FrankenError::internal(
                    "an upsert binding its DO UPDATE subqueries needs exactly one attempted row",
                ));
            };
            let bound =
                Box::pin(self.bind_upsert_uncompiled_subqueries(insert, attempted, params)).await?;
            bound_upsert = (Statement::Insert(bound.clone()), bound);
            (&bound_upsert.0, &bound_upsert.1, None)
        } else {
            (statement, insert, precompiled)
        };

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
                    &insert.table,
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

        // bd-d8s37: Parent-side FK enforcement for a conflicting DO UPDATE arm.
        // Run AFTER BEFORE UPDATE triggers (so trigger-side child edits are
        // observed) and BEFORE writing the parent row (so ON UPDATE RESTRICT
        // rejects cleanly without mutating the table).
        let mut pending_upsert_fk_actions = Vec::new();
        if table_has_upsert_fk_parent_work
            && let Some((_, old_row, new_row, true)) = &upsert_update_plan
        {
            pending_upsert_fk_actions =
                Box::pin(self.check_fk_on_update(table_name, old_row, new_row)).await?;
            if !pending_upsert_fk_actions.is_empty() {
                self.ensure_trigger_program_admission_available()?;
            }
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

        // Capture the actual inserted/updated row in the VDBE. Projecting it
        // later must not re-read a key that an UPSERT changed, nor use the
        // candidate values when defaults, affinity or generated columns have
        // changed them. Multi-row inputs were replayed above so this snapshot
        // is evaluated before the next row and before AFTER triggers.
        let fk_snapshot = self.upsert_foreign_key_snapshot(insert);
        let snapshot_statement = if returning_projection.is_some() || fk_snapshot.is_some() {
            // A snapshot recompiles the statement instead of reusing its
            // prepared program. Number every original placeholder before
            // replacing or extending RETURNING, so explicit slots, anonymous
            // slots and repeated names retain their statement-wide identity.
            let mut snapshot_insert = canonicalize_insert_placeholders_with_state(
                insert,
                &mut BindParamState::default(),
            )?;
            if let Some(projection) = &returning_projection {
                snapshot_insert
                    .returning
                    .clone_from(&projection.snapshot_columns);
            } else if let Some(snapshot) = &fk_snapshot {
                // Keep the caller's RETURNING prefix and evaluation point.
                // The trailing FK image must not evaluate SET expressions again.
                snapshot_insert
                    .returning
                    .extend(snapshot.columns.iter().cloned());
            }
            Some(Statement::Insert(snapshot_insert))
        } else {
            None
        };
        let compiled_statement = snapshot_statement.as_ref().unwrap_or(statement);
        let compiled_insert = match compiled_statement {
            Statement::Insert(compiled_insert) => compiled_insert,
            _ => insert,
        };
        let arc_prog;
        // A BEFORE trigger can invalidate code compiled at batch entry.
        // Check again here, after BEFORE and immediately before the write.
        let program: &VdbeProgram = if let Some(p) = precompiled.filter(|_| {
            snapshot_statement.is_none()
                && replay_identity.is_none_or(|identity| self.insert_replay_identity() == identity)
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
            let (sql_text, sql_key) =
                self.dispatch_statement_text(params, compiled_statement, |seen| &mut seen.inserts);
            // Resolved VALUES subqueries hold this execution's values.
            // A RETURNING snapshot also changes the compiled shape independently
            // of any original SQL text retained by the dispatcher.
            // bd-jxurb: so do the values bound into an upsert's DO UPDATE.
            let _insert_cache_bypass = (insert_values_have_subquery(insert)
                || snapshot_statement.is_some()
                || upsert_binds_subqueries)
                .then(|| BoolCellRestoreGuard::new(&self.bypass_compiled_cache, true));
            arc_prog = self
                .compile_with_cache(sql_key, &sql_text, async |conn| {
                    conn.compile_table_insert(compiled_insert, params).await
                })
                .await?;
            &arc_prog
        };
        let runtime_requirements = self.insert_runtime_requirements(insert)?;
        let vtab_instances = self.insert_program_vtab_instances(insert, program)?;
        let (mut rows, affected, last_insert_rowid) = self
            .execute_table_program_with_cx(
                program,
                params,
                true,
                runtime_requirements,
                cx,
                true,
                vtab_instances,
                None,
            )
            .await?;

        // Implicit REPLACE deletes are parent-row deletions for inbound
        // FK purposes. Enforce their exact OLD rows before child-side
        // validation and AFTER INSERT processing. Only a program that
        // deleted rows left victims; without any the boxed pass is skipped.
        if !self.last_replace_victims.borrow().is_empty() {
            Box::pin(self.enforce_fk_on_replace_victims(&insert.table)).await?;
        }

        // bd-d8s37: an UPSERT checks the row the write program produced.
        // A real insert checks every child key; DO UPDATE checks only keys
        // assigned by its single update arm. No-op arms produce no snapshot.
        if let Some(snapshot) = &fk_snapshot {
            if rows.len() != affected {
                return Err(FrankenError::internal(
                    "UPSERT foreign-key snapshot count does not match changed rows",
                ));
            }
            for row in &mut rows {
                let width = snapshot.columns.len();
                let start = if returning_projection.is_some() {
                    0
                } else {
                    row.values.len().checked_sub(width).ok_or_else(|| {
                        FrankenError::internal("UPSERT foreign-key snapshot is incomplete")
                    })?
                };
                let written = row.values.get(start..start + width).ok_or_else(|| {
                    FrankenError::internal("UPSERT foreign-key snapshot is incomplete")
                })?;
                let keys = last_insert_rowid
                    .is_none()
                    .then_some(snapshot.updated_keys.as_slice());
                Box::pin(self.check_fk_parent_exists_for_keys(table_name, written, keys)).await?;
                if returning_projection.is_none() {
                    row.values.truncate(start);
                }
            }
        } else if affected > 0 && self.fk_enforcement_enabled() {
            // Ordinary INSERT and UPSERT shapes without a precise receipt keep
            // their established enforcement. Never check a skipped row.
            Box::pin(self.enforce_fk_on_insert(insert, table_name, params)).await?;
        }

        // bd-d8s37: SQLite's parent ON UPDATE FK programs (CASCADE / SET NULL /
        // SET DEFAULT) observe the updated parent row and run BEFORE the
        // parent's RETURNING projection and AFTER UPDATE triggers.
        if affected > 0 && !pending_upsert_fk_actions.is_empty() {
            let _do_update_clause = DmlConflictClauseGuard::new(
                &self.dml_conflict_clause,
                Some(fsqlite_ast::ConflictAction::Abort),
            );
            for action in &pending_upsert_fk_actions {
                Box::pin(self.execute_fk_update_action(action)).await?;
            }
        }

        let rows = if let Some(projection) = &returning_projection {
            Box::pin(self.project_dml_returning(projection, rows, params)).await?
        } else {
            rows
        };

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
                    &insert.table,
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
                &insert.table,
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
        } else if self.is_autoincrement_target(table_name, insert.table.schema.as_deref()) {
            // Only an AUTOINCREMENT table has a high-water to refresh; other
            // tables do not allocate the refresh future.
            Box::pin(self.refresh_autoincrement_sequence_after_insert(
                table_name,
                insert.table.schema.as_deref(),
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

    /// bd-jxurb: whether a DO UPDATE clause of `insert` holds, in its SET
    /// values or its WHERE, a correlated subquery its compiled program
    /// cannot answer: a scalar or EXISTS subquery over anything but one base
    /// table (a derived table, a compound, a join, a CTE, GROUP BY, a view),
    /// or a correlated IN subquery. The compiled DO UPDATE reads such a
    /// subquery's first table, or NULL, so the dispatcher binds it instead
    /// ([`Self::bind_upsert_uncompiled_subqueries`]).
    pub(super) fn upsert_has_uncompiled_correlated_subquery(
        &self,
        insert: &fsqlite_ast::InsertStatement,
    ) -> bool {
        insert
            .upsert
            .iter()
            .any(|clause| self.upsert_clause_has_uncompiled_correlated_subquery(clause))
    }

    fn upsert_clause_has_uncompiled_correlated_subquery(
        &self,
        clause: &fsqlite_ast::UpsertClause,
    ) -> bool {
        match &clause.action {
            fsqlite_ast::UpsertAction::Update {
                assignments,
                where_clause,
            } => {
                assignments.iter().any(|assignment| {
                    dml_expr_has_uncompiled_correlated_subquery(&assignment.value, self)
                }) || where_clause.as_deref().is_some_and(|predicate| {
                    dml_expr_has_uncompiled_correlated_subquery(predicate, self)
                })
            }
            fsqlite_ast::UpsertAction::Nothing => false,
        }
    }

    /// bd-jxurb: `insert` (one attempted row, `attempted`) with the
    /// subqueries of [`Self::upsert_has_uncompiled_correlated_subquery`]
    /// replaced by their values for that row, as stock evaluates them.
    ///
    /// The clause the row takes is the first whose conflict target an
    /// existing row violates, as the program routes it. In that clause each
    /// such subquery runs through the SELECT path with the existing row's
    /// columns bound (their declared affinity and collation, as a column of
    /// the target table carries) and `excluded`'s bound (no affinity and no
    /// collation, as stock's excluded registers carry), and is replaced by
    /// its value with the subquery's own result affinity; a row-value SET
    /// source becomes the row value of its first row. A clause the row does
    /// not take never runs its DO UPDATE, so its subqueries stay as they are,
    /// but a row-value source the program cannot compile reads NULLs there.
    ///
    /// The placeholders are numbered first, so a subquery run alone binds the
    /// statement's own parameter slots. A conflict target this probe cannot
    /// locate (an expression or partial index, or an omitted target without
    /// an INTEGER PRIMARY KEY) before the clause that needs binding is
    /// refused rather than guessed.
    pub(super) async fn bind_upsert_uncompiled_subqueries(
        &self,
        insert: &fsqlite_ast::InsertStatement,
        attempted: &[SqliteValue],
        params: Option<&[SqliteValue]>,
    ) -> Result<fsqlite_ast::InsertStatement> {
        let mut bound =
            canonicalize_insert_placeholders_with_state(insert, &mut BindParamState::default())?;
        let mut taken = None;
        for (index, clause) in insert.upsert.iter().enumerate() {
            match self
                .probe_upsert_conflict_row(insert, clause.target.as_ref(), attempted, params)
                .await?
            {
                UpsertConflictProbe::NoConflict => {}
                UpsertConflictProbe::Conflict { rowid, old_values } => {
                    taken = Some((index, rowid, old_values));
                    break;
                }
                UpsertConflictProbe::Indeterminate => {
                    if insert.upsert[index..]
                        .iter()
                        .any(|clause| self.upsert_clause_has_uncompiled_correlated_subquery(clause))
                    {
                        return Err(FrankenError::NotImplemented(
                            "an upsert DO UPDATE whose correlated subquery is not one SELECT of \
                             one table, after a conflict target that is an expression or partial \
                             index, or an omitted target on a table without an INTEGER PRIMARY KEY"
                                .to_owned(),
                        ));
                    }
                    break;
                }
            }
        }
        let binding = match &taken {
            Some((_, rowid, old_values)) => {
                Some(self.upsert_row_binding(insert, *rowid, old_values, attempted)?)
            }
            None => None,
        };
        for (index, clause) in bound.upsert.iter_mut().enumerate() {
            let fsqlite_ast::UpsertAction::Update {
                assignments,
                where_clause,
            } = &mut clause.action
            else {
                continue;
            };
            let Some(binding) = binding
                .as_ref()
                .filter(|_| taken.as_ref().is_some_and(|(taken, _, _)| *taken == index))
            else {
                // Not this row's clause: its DO UPDATE never runs.
                for assignment in assignments.iter_mut() {
                    if let fsqlite_ast::AssignmentTarget::ColumnList(targets) = &assignment.target
                        && targets.len() > 1
                        && matches!(assignment.value, Expr::Subquery(..))
                    {
                        let span = assignment.value.span();
                        assignment.value = Expr::RowValue(
                            vec![Expr::Literal(Literal::Null, span); targets.len()],
                            span,
                        );
                    }
                }
                continue;
            };
            // Stock tests the DO UPDATE's WHERE before it computes the SET.
            if let Some(predicate) = where_clause.as_deref_mut() {
                Box::pin(self.bind_upsert_subqueries_in_expr(predicate, binding, params)).await?;
            }
            for assignment in assignments.iter_mut() {
                let row_value = matches!(
                    &assignment.target,
                    fsqlite_ast::AssignmentTarget::ColumnList(targets) if targets.len() > 1
                );
                if row_value && self.upsert_subquery_needs_binding(&assignment.value) {
                    let source = std::mem::replace(
                        &mut assignment.value,
                        Expr::Literal(Literal::Null, Span::ZERO),
                    );
                    assignment.value =
                        Box::pin(self.evaluate_bound_upsert_subquery(source, binding, true, params))
                            .await?;
                } else {
                    Box::pin(self.bind_upsert_subqueries_in_expr(
                        &mut assignment.value,
                        binding,
                        params,
                    ))
                    .await?;
                }
            }
        }
        Ok(bound)
    }

    /// bd-jxurb: a subquery node the compiled DO UPDATE cannot answer: a
    /// scalar, EXISTS or IN subquery holding a correlated subquery its scan
    /// does not answer (the node itself or one nested in it).
    fn upsert_subquery_needs_binding(&self, expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::Subquery(..)
                | Expr::Exists { .. }
                | Expr::In {
                    set: InSet::Subquery(_),
                    ..
                }
        ) && dml_expr_has_uncompiled_correlated_subquery(expr, self)
    }

    /// bd-jxurb: the names a DO UPDATE reads for the row it updates. The
    /// target's columns (under the INSERT's alias, else its name) and its
    /// rowid bind the existing row; `excluded.*` binds the attempted row and
    /// is never reached unqualified (its entries are skipped as a USING
    /// join's are).
    fn upsert_row_binding(
        &self,
        insert: &fsqlite_ast::InsertStatement,
        rowid: Option<i64>,
        old_values: &[SqliteValue],
        attempted: &[SqliteValue],
    ) -> Result<UpsertRowBinding> {
        let schema = self.schema.borrow();
        let table = schema
            .iter()
            .find(|table| table.name.eq_ignore_ascii_case(&insert.table.name))
            .ok_or_else(|| {
                FrankenError::internal("an upsert binding its DO UPDATE lost its target table")
            })?;
        if old_values.len() != table.columns.len() || attempted.len() != table.columns.len() {
            return Err(FrankenError::internal(
                "an upsert binding its DO UPDATE read a row of the wrong width",
            ));
        }
        let label = insert.alias.as_deref().unwrap_or(&insert.table.name);
        let mut binding = UpsertRowBinding::default();
        for (column, value) in table.columns.iter().zip(old_values) {
            binding.push(
                (label.to_owned(), column.name.clone(), false),
                Expr::BoundOuterValue {
                    value: value.clone(),
                    collation: BoundCollation::from_declared_name(column.collation.clone()),
                    affinity: Some(affinity_char_to_type(column.affinity)),
                    span: Span::ZERO,
                },
                false,
            );
        }
        if let Some(rowid) = rowid {
            binding.push(
                (label.to_owned(), "rowid".to_owned(), true),
                Expr::BoundOuterValue {
                    value: SqliteValue::Integer(rowid),
                    collation: BoundCollation::Binary,
                    affinity: Some(TypeAffinity::Integer),
                    span: Span::ZERO,
                },
                false,
            );
        }
        for (column, value) in table.columns.iter().zip(attempted) {
            binding.push(
                ("excluded".to_owned(), column.name.clone(), false),
                Expr::BoundOuterValue {
                    value: value.clone(),
                    collation: BoundCollation::Unspecified,
                    affinity: None,
                    span: Span::ZERO,
                },
                true,
            );
        }
        Ok(binding)
    }

    /// bd-jxurb: replace each subquery node of `expr` the compiled DO UPDATE
    /// cannot answer by its value for the bound row.
    async fn bind_upsert_subqueries_in_expr(
        &self,
        expr: &mut Expr,
        binding: &UpsertRowBinding,
        params: Option<&[SqliteValue]>,
    ) -> Result<()> {
        let mut nodes = Vec::new();
        walk_expr_mut(expr, UNPLACED_AGGREGATE_WALK_SITE, false, &mut |node, _| {
            if self.upsert_subquery_needs_binding(node) {
                nodes.push(node.clone());
                return false;
            }
            true
        });
        if nodes.is_empty() {
            return Ok(());
        }
        let mut values = Vec::with_capacity(nodes.len());
        for node in nodes {
            values.push(
                Box::pin(self.evaluate_bound_upsert_subquery(node, binding, false, params)).await?,
            );
        }
        let mut values = values.into_iter();
        walk_expr_mut(expr, UNPLACED_AGGREGATE_WALK_SITE, false, &mut |node, _| {
            if self.upsert_subquery_needs_binding(node) {
                if let Some(value) = values.next() {
                    *node = value;
                }
                return false;
            }
            true
        });
        Ok(())
    }

    /// bd-jxurb: the value of one subquery node for the bound row: a scalar
    /// subquery's first row (NULL when it has none) with its result affinity,
    /// a row-value source's first row as a row value, or an EXISTS or IN
    /// node's truth value. Run through the SELECT path, which answers every
    /// source shape.
    async fn evaluate_bound_upsert_subquery(
        &self,
        mut node: Expr,
        binding: &UpsertRowBinding,
        row_value: bool,
        params: Option<&[SqliteValue]>,
    ) -> Result<Expr> {
        {
            let schema = self.schema.borrow();
            let lookup = OuterReferenceLookup {
                row: &binding.row,
                col_map: &binding.col_map,
                using_skip: Some(&binding.excluded),
                replacement_exprs: Some(&binding.replacements),
                column_collations: None,
                column_affinities: None,
                declared_blob_columns: None,
                schema: &schema,
                connection: Some(self),
                probe_unqualified_external_ref: false,
            };
            substitute_outer_refs_in_expr_mut(
                &mut node,
                &lookup,
                &[],
                Some(&HashSet::new()),
                &HashSet::new(),
                &CteScopes::new(),
            );
        }
        // Bound values identify no reusable program.
        let _cache_bypass = BoolCellRestoreGuard::new(&self.bypass_compiled_cache, true);
        match node {
            Expr::Subquery(subquery, span) => {
                let donor = (*subquery).clone();
                self.validate_select_column_references(&donor)?;
                if !row_value {
                    self.validate_scalar_subquery_column_count(&donor)?;
                }
                let mut probe = *subquery;
                self.limit_probe_to_its_first_row(&mut probe);
                let rows = Box::pin(self.execute_statement(&Statement::Select(probe), params)).await?;
                self.materialize_scalar_subquery_in_operand(&donor, rows, span)
            }
            node => {
                let span = node.span();
                let probe = select_of_result_exprs(vec![node], None);
                let rows = Box::pin(self.execute_statement(&Statement::Select(probe), params)).await?;
                let value = rows
                    .into_iter()
                    .next()
                    .and_then(|row| row.values.into_iter().next())
                    .unwrap_or(SqliteValue::Null);
                Ok(Expr::BoundOuterValue {
                    value,
                    collation: BoundCollation::Unspecified,
                    affinity: None,
                    span,
                })
            }
        }
    }
}

/// bd-jxurb: the existing row's and `excluded`'s names for an upsert's DO
/// UPDATE, as a replacement lookup: `col_map[i]` is replaced by
/// `replacements[i]` (`row[i]` is its value), and the `excluded` entries
/// resolve only qualified.
#[derive(Default)]
struct UpsertRowBinding {
    col_map: Vec<(String, String, bool)>,
    row: Vec<SqliteValue>,
    replacements: Vec<Expr>,
    excluded: HashSet<usize>,
}

impl UpsertRowBinding {
    fn push(&mut self, entry: (String, String, bool), replacement: Expr, excluded: bool) {
        if excluded {
            self.excluded.insert(self.col_map.len());
        }
        self.row.push(match &replacement {
            Expr::BoundOuterValue { value, .. } => value.clone(),
            _ => SqliteValue::Null,
        });
        self.col_map.push(entry);
        self.replacements.push(replacement);
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

/// bd-fpr5g lever 3b: the trigger body kernel is the path a body statement
/// takes after its first firing, and it is given up whenever the ordinary
/// dispatcher would do something it does not. The answers are pinned against
/// stock SQLite by `kernel_run_trigger_bodies_follow_targets_pragmas_failures_and_rollback`
/// in crates/fsqlite-core/tests/bd_fpr5g_trigger_body_cache_under_cte.rs;
/// these tests count kernel runs (a thread-local, so tests do not share it),
/// which an implementation that never takes the kernel, or takes it when it
/// must not, gets wrong.
#[cfg(test)]
mod trigger_body_kernel_runs {
    use super::*;

    fn kernel_runs() -> u64 {
        TRIGGER_BODY_KERNEL_RUNS.with(std::cell::Cell::get)
    }

    async fn ints(conn: &Connection, sql: &str) -> Vec<i64> {
        conn.query(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
            .iter()
            .flat_map(|row| row.values().to_vec())
            .map(|value| match value {
                SqliteValue::Integer(n) => n,
                other => panic!("{sql}: {other:?}"),
            })
            .collect()
    }

    const SETUP: &[&str] = &[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)",
        "CREATE TABLE lg(k, m)",
        "CREATE TABLE u(k TEXT PRIMARY KEY, c INTEGER)",
        "INSERT INTO u VALUES ('even', 0), ('odd', 0)",
        "CREATE TRIGGER ti AFTER INSERT ON t WHEN NEW.n % 7 <> 3 BEGIN \
           INSERT INTO lg VALUES (NEW.id, NEW.n > 10); \
           UPDATE u SET c = c + 1 WHERE k = CASE WHEN NEW.n % 2 = 0 THEN 'even' ELSE 'odd' END; \
         END",
    ];

    /// Insert `first..=last` into t from a recursive CTE; the firing rows.
    async fn insert_range(conn: &Connection, first: i64, last: i64) -> Vec<i64> {
        conn.execute(&format!(
            "WITH RECURSIVE s(i) AS (SELECT {first} UNION ALL SELECT i + 1 FROM s WHERE i < {last}) \
             INSERT INTO t SELECT i, i FROM s"
        ))
        .await
        .unwrap();
        (first..=last).filter(|n| n % 7 != 3).collect()
    }

    async fn assert_answers(conn: &Connection, fired: &[i64]) {
        let count = |keep: &dyn Fn(i64) -> bool| {
            i64::try_from(fired.iter().filter(|&&n| keep(n)).count()).unwrap()
        };
        assert_eq!(
            ints(conn, "SELECT count(*), sum(m) FROM lg").await,
            vec![count(&|_| true), count(&|n| n > 10)]
        );
        assert_eq!(
            ints(conn, "SELECT c FROM u ORDER BY k").await,
            vec![count(&|n| n % 2 == 0), count(&|n| n % 2 == 1)]
        );
    }

    #[test]
    fn body_statements_run_through_the_kernel_after_their_first_firing() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let file = dir.path().join("kernel.db");
            for path in [":memory:".to_owned(), file.to_string_lossy().into_owned()] {
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                let before = kernel_runs();
                let fired = insert_range(&conn, 1, 100).await;
                // Each body statement's first firing takes the ordinary
                // dispatcher and captures; every later firing takes the kernel.
                let fired_count = u64::try_from(fired.len()).unwrap();
                assert_eq!(kernel_runs() - before, 2 * fired_count - 2, "{path}");
                assert_answers(&conn, &fired).await;
            }
        });
    }

    #[test]
    fn the_kernel_is_given_up_when_the_dispatcher_would_differ() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            for sql in SETUP {
                conn.execute(sql).await.unwrap();
            }
            let mut fired = insert_range(&conn, 1, 100).await;

            // Foreign keys on: the dispatcher runs its FK validation scope, so
            // no body statement takes the kernel.
            conn.execute("PRAGMA foreign_keys = ON").await.unwrap();
            let before = kernel_runs();
            fired.extend(insert_range(&conn, 101, 120).await);
            assert_eq!(kernel_runs() - before, 0, "foreign_keys = ON");
            conn.execute("PRAGMA foreign_keys = OFF").await.unwrap();
            assert_answers(&conn, &fired).await;

            // A schema change (an index on the INSERT's target): the next
            // statement's templates take the ordinary path once, and the
            // index holds every row afterwards.
            conn.execute("CREATE INDEX lg_k ON lg(k)").await.unwrap();
            let before = kernel_runs();
            let range = insert_range(&conn, 121, 140).await;
            assert_eq!(
                kernel_runs() - before,
                2 * u64::try_from(range.len()).unwrap() - 2,
                "after CREATE INDEX"
            );
            fired.extend(range);
            assert_answers(&conn, &fired).await;
            assert_eq!(
                ints(&conn, "SELECT count(*) FROM lg WHERE k > 120").await,
                vec![i64::try_from(fired.iter().filter(|&&n| n > 120).count()).unwrap()]
            );
            let integrity = conn.query("PRAGMA integrity_check").await.unwrap();
            assert_eq!(
                integrity[0].values(),
                &[SqliteValue::Text("ok".into())],
                "integrity after kernel-run inserts into an indexed table"
            );

            // A trigger on the INSERT's target: that statement is refused (it
            // must fire the new trigger), the UPDATE and the new trigger's own
            // body statement take the kernel after their first firings.
            conn.execute(
                "CREATE TRIGGER tl AFTER INSERT ON lg BEGIN \
                   UPDATE u SET c = c + 1000 WHERE k = 'even'; END",
            )
            .await
            .unwrap();
            let before = kernel_runs();
            let range = insert_range(&conn, 141, 160).await;
            let range_count = u64::try_from(range.len()).unwrap();
            assert_eq!(
                kernel_runs() - before,
                2 * (range_count - 1),
                "a trigger on the body's target"
            );
            assert_eq!(
                ints(&conn, "SELECT c FROM u WHERE k = 'even'").await,
                vec![
                    i64::try_from(fired.iter().chain(&range).filter(|&&n| n % 2 == 0).count())
                        .unwrap()
                        + 1000 * i64::try_from(range.len()).unwrap()
                ]
            );
        });
    }
}

/// bd-fpr5g lever 5: which INSERT ... SELECT streams into a table with
/// triggers replay their rows through the compiled row program.
#[cfg(test)]
mod compiled_insert_replay_rows {
    use super::*;

    fn replayed_rows() -> u64 {
        COMPILED_INSERT_REPLAY_ROWS.with(std::cell::Cell::get)
    }

    async fn ints(conn: &Connection, sql: &str) -> Vec<i64> {
        conn.query(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
            .iter()
            .flat_map(|row| row.values().to_vec())
            .map(|value| match value {
                SqliteValue::Integer(n) => n,
                other => panic!("{sql}: {other:?}"),
            })
            .collect()
    }

    const SETUP: &[&str] = &[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)",
        "CREATE TABLE lg(k, m)",
        "CREATE TRIGGER ti AFTER INSERT ON t WHEN NEW.n % 7 <> 3 BEGIN \
           INSERT INTO lg VALUES (NEW.id, NEW.n * 2); END",
        "CREATE TABLE src(i INTEGER PRIMARY KEY, n INTEGER)",
        "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 300) \
         INSERT INTO src SELECT i, i FROM s",
    ];

    /// Rows and log entries `t` and `lg` hold, as (count, sum) pairs.
    async fn totals(conn: &Connection) -> Vec<i64> {
        let mut out = ints(conn, "SELECT count(*), sum(n) FROM t").await;
        out.extend(ints(conn, "SELECT count(*), sum(m) FROM lg").await);
        out
    }

    /// The totals after `range` went into t (and fired the trigger).
    fn expected(range: impl Iterator<Item = i64> + Clone) -> Vec<i64> {
        let fired = range.clone().filter(|n| n % 7 != 3);
        let count = |it: &mut dyn Iterator<Item = i64>| i64::try_from(it.count()).unwrap();
        vec![
            count(&mut range.clone()),
            range.sum(),
            count(&mut fired.clone()),
            fired.map(|n| n * 2).sum(),
        ]
    }

    #[test]
    fn a_materialized_select_source_replays_through_the_compiled_row() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let file = dir.path().join("replay.db");
            for path in [":memory:".to_owned(), file.to_string_lossy().into_owned()] {
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                // A projected expression keeps the streaming producer out: the
                // SELECT is materialized, then replayed.
                let before = replayed_rows();
                conn.execute("INSERT INTO t SELECT i, n + 0 FROM src WHERE i <= 200")
                    .await
                    .unwrap();
                assert_eq!(replayed_rows() - before, 200, "{path}: materialized SELECT");
                assert_eq!(totals(&conn).await, expected(1..=200), "{path}");

                // Bare columns of one table: the streaming producer emits the
                // rows while the stream runs, and they keep the prepared replay.
                conn.execute("CREATE TABLE src2(i INTEGER PRIMARY KEY, n INTEGER)")
                    .await
                    .unwrap();
                conn.execute("INSERT INTO src2 SELECT i + 200, n + 200 FROM src WHERE i <= 50")
                    .await
                    .unwrap();
                let before = replayed_rows();
                conn.execute("INSERT INTO t SELECT i, n FROM src2").await.unwrap();
                assert_eq!(replayed_rows() - before, 0, "{path}: streamed SELECT");
                assert_eq!(totals(&conn).await, expected(1..=250), "{path}");
            }
        });
    }

    #[test]
    fn temp_roots_keep_the_compiled_row_unless_the_target_is_temp() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            for sql in SETUP {
                conn.execute(sql).await.unwrap();
            }
            // A TEMP table elsewhere and a CTE source (a materialized binding
            // while the statement runs): both are temp roots, and the
            // template's program opens neither.
            conn.execute("CREATE TEMP TABLE scratch(x)").await.unwrap();
            let before = replayed_rows();
            conn.execute(
                "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 120) \
                 INSERT INTO t SELECT i, i FROM s",
            )
            .await
            .unwrap();
            assert_eq!(replayed_rows() - before, 120, "CTE source with a TEMP table present");
            assert_eq!(totals(&conn).await, expected(1..=120));

            // A TEMP target keeps the prepared replay.
            conn.execute("CREATE TEMP TABLE tt(id INTEGER PRIMARY KEY, n INTEGER)")
                .await
                .unwrap();
            conn.execute(
                "CREATE TEMP TRIGGER tti AFTER INSERT ON tt BEGIN \
                   INSERT INTO lg VALUES (NEW.id, 0); END",
            )
            .await
            .unwrap();
            let before = replayed_rows();
            conn.execute("INSERT INTO tt SELECT i, n + 0 FROM src WHERE i <= 30")
                .await
                .unwrap();
            assert_eq!(replayed_rows() - before, 0, "TEMP target");
            assert_eq!(ints(&conn, "SELECT count(*), sum(n) FROM tt").await, vec![30, 465]);
            let mut want = expected(1..=120);
            want[2] += 30;
            assert_eq!(totals(&conn).await, want);
        });
    }
}
