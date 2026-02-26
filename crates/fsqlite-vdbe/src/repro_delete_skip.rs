#[cfg(test)]
mod tests {
    use crate::ProgramBuilder;
    use crate::engine::{ExecOutcome, MemDatabase, VdbeEngine};
    use fsqlite_types::opcode::{Opcode, P4};
    use fsqlite_types::value::SqliteValue;

    #[test]
    fn test_delete_then_next_does_not_skip_successor_row() {
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        {
            let table = db.get_table_mut(root).expect("table exists");
            table.insert_row(1, vec![SqliteValue::Integer(1)]);
            table.insert_row(2, vec![SqliteValue::Integer(2)]);
            table.insert_row(3, vec![SqliteValue::Integer(3)]);
        }

        // Program:
        //   OpenWrite cursor on t
        //   Rewind loop
        //     rowid -> r1
        //     ResultRow r1
        //     if r1 == 2: Delete
        //   Next loop
        let mut b = ProgramBuilder::new();
        let done = b.emit_label();
        let skip_delete = b.emit_label();

        b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);
        b.emit_jump_to_label(Opcode::Rewind, 0, 0, done, P4::None, 0);
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let loop_start = b.current_addr() as i32;
        b.emit_op(Opcode::Rowid, 0, 1, 0, P4::None, 0);
        b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Integer, 2, 2, 0, P4::None, 0);
        b.emit_jump_to_label(Opcode::Ne, 2, 1, skip_delete, P4::None, 0);
        b.emit_op(Opcode::Delete, 0, 0, 0, P4::None, 0);
        b.resolve_label(skip_delete);
        b.emit_op(Opcode::Next, 0, loop_start, 0, P4::None, 0);
        b.resolve_label(done);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

        let program = b.finish().expect("program builds");
        let mut engine = VdbeEngine::new(program.register_count());
        engine.set_database(db);
        engine.set_reject_mem_fallback(false);

        let outcome = engine.execute(&program).expect("program executes");
        assert!(matches!(outcome, ExecOutcome::Done));

        let visited: Vec<i64> = engine
            .take_results()
            .into_iter()
            .filter_map(|row| row.first().and_then(SqliteValue::as_integer))
            .collect();
        assert_eq!(visited, vec![1, 2, 3], "row 3 must not be skipped");
    }
}
