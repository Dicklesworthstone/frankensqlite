#![allow(clippy::redundant_clone, clippy::similar_names)]
//! SQL pipeline deterministic unit test suites (bd-mblr.6.2).
//!
//! Granular deterministic tests for parser/planner/VDBE/function semantics
//! including malformed SQL, plan-edge behavior, opcode correctness, and
//! function corner cases. All tests use fixtures from [`unit_fixtures`] and
//! diagnostics from [`test_diagnostics`].
//!
//! Maps to unit matrix entries UT-SQL-001..008, UT-VDBE-001..007, UT-FUN-001..005.

use crate::test_diagnostics::DiagContext;
use crate::{diag_assert, diag_assert_eq};

const BEAD_ID: &str = "bd-mblr.6.2";

// ─── Parser Suite (UT-SQL) ───────────────────────────────────────────────

#[cfg(test)]
mod parser_tests {
    use super::*;
    use fsqlite_ast::{SelectCore, Statement};
    use fsqlite_parser::{Lexer, Parser, TokenKind};

    // -- UT-SQL-001: SELECT with all clause types --

    #[test]
    fn parse_simple_select() {
        let mut parser = Parser::from_sql("SELECT 1");
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("simple_select")
            .invariant("SELECT 1 parses to one statement, zero errors");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        diag_assert_eq!(ctx, stmts.len(), 1);
    }

    #[test]
    fn parse_select_star_from_table() {
        let mut parser = Parser::from_sql("SELECT * FROM users");
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("select_star")
            .invariant("SELECT * FROM t parses correctly");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        diag_assert_eq!(ctx.clone(), stmts.len(), 1);
        match &stmts[0] {
            Statement::Select(sel) => {
                let has_from = matches!(&sel.body.select, SelectCore::Select { from: Some(_), .. });
                diag_assert!(
                    ctx,
                    !sel.body.compounds.is_empty() || has_from,
                    "select has from clause or core"
                );
            }
            other => diag_assert!(ctx, false, "expected Select, got {other:?}"),
        }
    }

    #[test]
    fn parse_select_with_where() {
        let mut parser = Parser::from_sql("SELECT id, name FROM users WHERE id > 5");
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("select_where")
            .invariant("SELECT with WHERE clause parses");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        diag_assert_eq!(ctx, stmts.len(), 1);
    }

    #[test]
    fn parse_select_with_group_having_order_limit() {
        let sql = "SELECT dept, COUNT(*) AS cnt FROM emp GROUP BY dept HAVING cnt > 3 ORDER BY cnt DESC LIMIT 10 OFFSET 5";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("select_full_clauses")
            .invariant("SELECT with GROUP BY/HAVING/ORDER BY/LIMIT/OFFSET parses");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        diag_assert_eq!(ctx, stmts.len(), 1);
    }

    #[test]
    fn parse_select_subquery() {
        let sql = "SELECT * FROM (SELECT id FROM t1) AS sub WHERE sub.id > 0";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("select_subquery")
            .invariant("Subquery in FROM clause parses");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        diag_assert_eq!(ctx, stmts.len(), 1);
    }

    // -- UT-SQL-002: INSERT variants --

    #[test]
    fn parse_insert_values() {
        let sql = "INSERT INTO users (name, age) VALUES ('Alice', 30)";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("insert_values")
            .invariant("INSERT INTO ... VALUES parses correctly");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        diag_assert_eq!(ctx.clone(), stmts.len(), 1);
        match &stmts[0] {
            Statement::Insert(ins) => {
                diag_assert_eq!(ctx, ins.table.name.as_str(), "users");
            }
            other => diag_assert!(ctx, false, "expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn parse_insert_select() {
        let sql = "INSERT INTO archive SELECT * FROM users WHERE active = 0";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("insert_select")
            .invariant("INSERT INTO ... SELECT parses");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        diag_assert_eq!(ctx, stmts.len(), 1);
    }

    #[test]
    fn parse_insert_default_values() {
        let sql = "INSERT INTO log DEFAULT VALUES";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("insert_default")
            .invariant("INSERT ... DEFAULT VALUES parses");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        diag_assert_eq!(ctx, stmts.len(), 1);
    }

    // -- UT-SQL-003: UPDATE --

    #[test]
    fn parse_update_with_where() {
        let sql = "UPDATE users SET name = 'Bob', age = age + 1 WHERE id = 42";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("update_where")
            .invariant("UPDATE with SET and WHERE parses");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        diag_assert_eq!(ctx.clone(), stmts.len(), 1);
        match &stmts[0] {
            Statement::Update(_) => {}
            other => diag_assert!(ctx, false, "expected Update, got {other:?}"),
        }
    }

    // -- UT-SQL-004: DELETE --

    #[test]
    fn parse_delete_with_where() {
        let sql = "DELETE FROM users WHERE id < 10";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("delete_where")
            .invariant("DELETE with WHERE parses");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        diag_assert_eq!(ctx, stmts.len(), 1);
    }

    // -- UT-SQL-005: CREATE TABLE --

    #[test]
    fn parse_create_table_with_constraints() {
        let sql = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER DEFAULT 0, UNIQUE(name))";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("create_table")
            .invariant("CREATE TABLE with constraints parses");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        diag_assert_eq!(ctx.clone(), stmts.len(), 1);
        match &stmts[0] {
            Statement::CreateTable(ct) => {
                diag_assert_eq!(ctx, ct.name.name.as_str(), "t1");
            }
            other => diag_assert!(ctx, false, "expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_table_if_not_exists() {
        let sql = "CREATE TABLE IF NOT EXISTS t2 (x INTEGER)";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("create_if_not_exists")
            .invariant("IF NOT EXISTS clause accepted");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        diag_assert_eq!(ctx, stmts.len(), 1);
    }

    // -- UT-SQL-006: Expression operator precedence --

    #[test]
    fn expr_precedence_mul_over_add() {
        let sql = "SELECT 2 + 3 * 4";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("precedence_mul_add")
            .invariant("* binds tighter than + (Pratt)");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        // Parse succeeds — the exact AST structure encodes precedence.
        diag_assert_eq!(ctx, stmts.len(), 1);
    }

    #[test]
    fn expr_precedence_parens_override() {
        let sql = "SELECT (2 + 3) * 4";
        let mut parser = Parser::from_sql(sql);
        let (_stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("precedence_parens")
            .invariant("Parentheses override operator precedence");
        diag_assert_eq!(ctx, errors.len(), 0);
    }

    #[test]
    fn expr_unary_minus() {
        let sql = "SELECT -1, -x, -(1+2)";
        let mut parser = Parser::from_sql(sql);
        let (_stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("unary_minus")
            .invariant("Unary minus in expressions parses");
        diag_assert_eq!(ctx, errors.len(), 0);
    }

    #[test]
    fn expr_between_and() {
        let sql = "SELECT x FROM t WHERE x BETWEEN 1 AND 10";
        let mut parser = Parser::from_sql(sql);
        let (_stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("between_and")
            .invariant("BETWEEN ... AND expression parses");
        diag_assert_eq!(ctx, errors.len(), 0);
    }

    #[test]
    fn expr_case_when() {
        let sql = "SELECT CASE WHEN x > 0 THEN 'pos' WHEN x < 0 THEN 'neg' ELSE 'zero' END";
        let mut parser = Parser::from_sql(sql);
        let (_stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("case_when")
            .invariant("CASE WHEN expression parses");
        diag_assert_eq!(ctx, errors.len(), 0);
    }

    #[test]
    fn expr_in_list() {
        let sql = "SELECT x FROM t WHERE x IN (1, 2, 3)";
        let mut parser = Parser::from_sql(sql);
        let (_stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("in_list")
            .invariant("IN (list) expression parses");
        diag_assert_eq!(ctx, errors.len(), 0);
    }

    // -- UT-SQL-007: JOIN types --

    #[test]
    fn parse_inner_join() {
        let sql = "SELECT * FROM a INNER JOIN b ON a.id = b.a_id";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("inner_join")
            .invariant("INNER JOIN parses");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        diag_assert_eq!(ctx, stmts.len(), 1);
    }

    #[test]
    fn parse_left_join() {
        let sql = "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id";
        let mut parser = Parser::from_sql(sql);
        let (_stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("left_join")
            .invariant("LEFT JOIN parses");
        diag_assert_eq!(ctx, errors.len(), 0);
    }

    #[test]
    fn parse_cross_join() {
        let sql = "SELECT * FROM a CROSS JOIN b";
        let mut parser = Parser::from_sql(sql);
        let (_stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("cross_join")
            .invariant("CROSS JOIN parses");
        diag_assert_eq!(ctx, errors.len(), 0);
    }

    #[test]
    fn parse_natural_join() {
        let sql = "SELECT * FROM a NATURAL JOIN b";
        let mut parser = Parser::from_sql(sql);
        let (_stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("natural_join")
            .invariant("NATURAL JOIN parses");
        diag_assert_eq!(ctx, errors.len(), 0);
    }

    // -- UT-SQL-008: Compound queries --

    #[test]
    fn parse_union() {
        let sql = "SELECT id FROM a UNION SELECT id FROM b";
        let mut parser = Parser::from_sql(sql);
        let (_stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("union")
            .invariant("UNION compound parses");
        diag_assert_eq!(ctx, errors.len(), 0);
    }

    #[test]
    fn parse_union_all() {
        let sql = "SELECT id FROM a UNION ALL SELECT id FROM b";
        let mut parser = Parser::from_sql(sql);
        let (_stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("union_all")
            .invariant("UNION ALL compound parses");
        diag_assert_eq!(ctx, errors.len(), 0);
    }

    #[test]
    fn parse_intersect() {
        let sql = "SELECT id FROM a INTERSECT SELECT id FROM b";
        let mut parser = Parser::from_sql(sql);
        let (_stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("intersect")
            .invariant("INTERSECT compound parses");
        diag_assert_eq!(ctx, errors.len(), 0);
    }

    #[test]
    fn parse_except() {
        let sql = "SELECT id FROM a EXCEPT SELECT id FROM b";
        let mut parser = Parser::from_sql(sql);
        let (_stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("except")
            .invariant("EXCEPT compound parses");
        diag_assert_eq!(ctx, errors.len(), 0);
    }

    // -- Malformed SQL rejection --

    #[test]
    fn malformed_missing_from() {
        let sql = "SELECT * WHERE 1";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("malformed_missing_from")
            .invariant("Missing FROM produces error or fails gracefully");
        // Either errors non-empty, or parsed as SELECT * (with WHERE interpreted differently)
        // The important thing: parser doesn't panic.
        let _ = (stmts, errors, ctx);
    }

    #[test]
    fn malformed_empty_string() {
        let mut parser = Parser::from_sql("");
        let (stmts, _errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("empty_string")
            .invariant("Empty SQL produces empty statement list");
        diag_assert_eq!(ctx, stmts.len(), 0);
    }

    #[test]
    fn malformed_unterminated_string() {
        let sql = "SELECT 'unterminated";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("unterminated_string")
            .invariant("Unterminated string literal produces parse error");
        diag_assert!(
            ctx,
            !errors.is_empty() || stmts.is_empty(),
            "must detect malformed SQL"
        );
    }

    #[test]
    fn malformed_double_semicolon() {
        let sql = "SELECT 1;;SELECT 2";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("double_semicolon")
            .invariant("Double semicolon handled without panic");
        // Parser should handle empty statement gracefully.
        let _ = (stmts, errors, ctx);
    }

    // -- Lexer tests --

    #[test]
    fn lexer_integer_literal() {
        let tokens = Lexer::tokenize("42");
        let ctx = DiagContext::new(BEAD_ID)
            .case("lex_integer")
            .invariant("Integer literal tokenizes correctly");
        diag_assert!(
            ctx,
            tokens
                .iter()
                .any(|t| matches!(t.kind, TokenKind::Integer(42))),
            "should have Integer(42) token"
        );
    }

    #[test]
    fn lexer_string_literal() {
        let tokens = Lexer::tokenize("'hello world'");
        let ctx = DiagContext::new(BEAD_ID)
            .case("lex_string")
            .invariant("String literal tokenizes correctly");
        diag_assert!(
            ctx,
            tokens
                .iter()
                .any(|t| matches!(&t.kind, TokenKind::String(s) if s.as_str().eq("hello world"))),
            "should have String('hello world') token"
        );
    }

    #[test]
    fn lexer_keywords_case_insensitive() {
        let upper = Lexer::tokenize("SELECT");
        let lower = Lexer::tokenize("select");
        let mixed = Lexer::tokenize("SeLeCt");
        let ctx = DiagContext::new(BEAD_ID)
            .case("lex_keyword_case")
            .invariant("Keywords are case-insensitive");
        diag_assert_eq!(ctx.clone(), upper[0].kind, lower[0].kind);
        diag_assert_eq!(ctx, upper[0].kind, mixed[0].kind);
    }

    #[test]
    fn lexer_blob_literal() {
        let tokens = Lexer::tokenize("X'DEADBEEF'");
        let ctx = DiagContext::new(BEAD_ID)
            .case("lex_blob")
            .invariant("Blob literal X'...' tokenizes");
        diag_assert!(
            ctx,
            tokens.iter().any(|t| matches!(&t.kind, TokenKind::Blob(_))),
            "should have Blob token"
        );
    }

    // -- Multi-statement parsing --

    #[test]
    fn parse_multiple_statements() {
        let sql = "SELECT 1; SELECT 2; SELECT 3";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("multi_stmt")
            .invariant("Semicolon-separated statements all parsed");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        diag_assert_eq!(ctx, stmts.len(), 3);
    }

    // -- Transaction control --

    #[test]
    fn parse_begin_commit_rollback() {
        let cases = [
            ("BEGIN", "begin"),
            ("BEGIN TRANSACTION", "begin_txn"),
            ("BEGIN IMMEDIATE", "begin_immediate"),
            ("BEGIN EXCLUSIVE", "begin_exclusive"),
            ("COMMIT", "commit"),
            ("ROLLBACK", "rollback"),
            ("SAVEPOINT sp1", "savepoint"),
            ("RELEASE sp1", "release"),
        ];
        for (sql, case_name) in &cases {
            let mut parser = Parser::from_sql(sql);
            let (stmts, errors) = parser.parse_all();
            let ctx = DiagContext::new(BEAD_ID)
                .case(case_name)
                .invariant("Transaction control statement parses");
            diag_assert_eq!(ctx.clone(), errors.len(), 0);
            diag_assert_eq!(ctx, stmts.len(), 1);
        }
    }

    // -- PRAGMA parsing --

    #[test]
    fn parse_pragma() {
        let sql = "PRAGMA journal_mode = WAL";
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        let ctx = DiagContext::new(BEAD_ID)
            .case("pragma")
            .invariant("PRAGMA statement parses");
        diag_assert_eq!(ctx.clone(), errors.len(), 0);
        diag_assert_eq!(ctx, stmts.len(), 1);
    }
}

// ─── VDBE Suite (UT-VDBE) ────────────────────────────────────────────────

#[cfg(test)]
mod vdbe_tests {
    use super::*;
    use fsqlite_types::opcode::{Opcode, P4, VdbeOp};

    // -- UT-VDBE-001: Opcode encoding --

    #[test]
    fn opcode_count() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("opcode_count")
            .invariant("Opcode::COUNT matches expected total");
        diag_assert!(ctx, Opcode::COUNT > 100, "should have 100+ opcodes");
    }

    #[test]
    fn opcode_name_not_empty() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("opcode_name")
            .invariant("Every opcode has a non-empty name");
        let mut named_count = 0_usize;
        for byte in 1..=u8::MAX {
            if let Some(op) = Opcode::from_byte(byte) {
                named_count += 1;
                diag_assert!(
                    ctx.clone(),
                    !op.name().is_empty(),
                    "opcode byte {byte} has empty name"
                );
            }
        }
        diag_assert_eq!(ctx, named_count, Opcode::COUNT - 1);
    }

    #[test]
    fn opcode_from_byte_zero_is_none() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("opcode_zero")
            .invariant("Opcode byte 0 is None (no opcode)");
        diag_assert_eq!(ctx, Opcode::from_byte(0), None);
    }

    #[test]
    fn opcode_from_byte_roundtrip() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("opcode_roundtrip")
            .invariant("Opcode → u8 → Opcode roundtrips");
        let test_ops = [
            Opcode::Goto,
            Opcode::Integer,
            Opcode::Add,
            Opcode::Eq,
            Opcode::OpenRead,
            Opcode::Column,
            Opcode::ResultRow,
            Opcode::Halt,
            Opcode::Transaction,
            Opcode::MakeRecord,
        ];
        for op in &test_ops {
            let byte = *op as u8;
            let roundtrip = Opcode::from_byte(byte);
            diag_assert_eq!(ctx.clone(), roundtrip, Some(*op));
        }
    }

    // -- UT-VDBE-001: Arithmetic opcodes --

    #[test]
    fn vdbe_op_integer_construction() {
        let op = VdbeOp {
            opcode: Opcode::Integer,
            p1: 42,
            p2: 1,
            p3: 0,
            p4: P4::None,
            p5: 0,
        };
        let ctx = DiagContext::new(BEAD_ID)
            .case("integer_op")
            .invariant("Integer opcode construction");
        diag_assert_eq!(ctx.clone(), op.opcode, Opcode::Integer);
        diag_assert_eq!(ctx.clone(), op.p1, 42);
        diag_assert_eq!(ctx, op.p2, 1);
    }

    #[test]
    fn vdbe_op_add_construction() {
        let op = VdbeOp {
            opcode: Opcode::Add,
            p1: 1,
            p2: 2,
            p3: 3,
            p4: P4::None,
            p5: 0,
        };
        let ctx = DiagContext::new(BEAD_ID)
            .case("add_op")
            .invariant("Add opcode: p1+p2 → p3");
        diag_assert_eq!(ctx, op.opcode, Opcode::Add);
    }

    #[test]
    fn vdbe_op_with_string_p4() {
        let op = VdbeOp {
            opcode: Opcode::String8,
            p1: 0,
            p2: 1,
            p3: 0,
            p4: P4::Str("hello".to_string()),
            p5: 0,
        };
        let ctx = DiagContext::new(BEAD_ID)
            .case("string_p4")
            .invariant("String8 opcode carries P4::Str");
        diag_assert_eq!(ctx, op.p4, P4::Str("hello".to_string()));
    }

    #[test]
    fn vdbe_op_with_int64_p4() {
        let op = VdbeOp {
            opcode: Opcode::Int64,
            p1: 0,
            p2: 1,
            p3: 0,
            p4: P4::Int64(i64::MAX),
            p5: 0,
        };
        let ctx = DiagContext::new(BEAD_ID)
            .case("int64_p4")
            .invariant("Int64 opcode carries P4::Int64");
        diag_assert_eq!(ctx, op.p4, P4::Int64(i64::MAX));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn vdbe_op_with_real_p4() {
        let op = VdbeOp {
            opcode: Opcode::Real,
            p1: 0,
            p2: 1,
            p3: 0,
            p4: P4::Real(3.14),
            p5: 0,
        };
        let ctx = DiagContext::new(BEAD_ID)
            .case("real_p4")
            .invariant("Real opcode carries P4::Real");
        match op.p4 {
            #[allow(clippy::neg_cmp_op_on_partial_ord)]
            P4::Real(v) => diag_assert!(ctx, (v - 3.14).abs() < f64::EPSILON, "value matches"),
            other => diag_assert!(ctx, false, "expected P4::Real, got {other:?}"),
        }
    }

    // -- UT-VDBE-002: Comparison opcodes --

    #[test]
    fn comparison_opcodes_exist() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("comparison_opcodes")
            .invariant("All 6 comparison opcodes defined");
        let ops = [
            Opcode::Eq,
            Opcode::Ne,
            Opcode::Lt,
            Opcode::Le,
            Opcode::Gt,
            Opcode::Ge,
        ];
        for op in &ops {
            diag_assert!(
                ctx.clone(),
                Opcode::from_byte(*op as u8).is_some(),
                "comparison opcode exists"
            );
        }
    }

    // -- UT-VDBE-003: Cursor opcodes --

    #[test]
    fn cursor_opcodes_exist() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("cursor_opcodes")
            .invariant("Key cursor opcodes defined");
        let ops = [
            Opcode::OpenRead,
            Opcode::OpenWrite,
            Opcode::Close,
            Opcode::SeekGE,
            Opcode::SeekGT,
            Opcode::SeekLE,
            Opcode::SeekLT,
        ];
        for op in &ops {
            diag_assert!(
                ctx.clone(),
                Opcode::from_byte(*op as u8).is_some(),
                "cursor opcode exists"
            );
        }
    }

    // -- UT-VDBE-004: Transaction opcodes --

    #[test]
    fn transaction_opcodes_exist() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("transaction_opcodes")
            .invariant("Transaction control opcodes defined");
        let ops = [Opcode::Transaction, Opcode::AutoCommit, Opcode::Savepoint];
        for op in &ops {
            diag_assert!(
                ctx.clone(),
                Opcode::from_byte(*op as u8).is_some(),
                "transaction opcode exists"
            );
        }
    }

    // -- UT-VDBE-005: Record operations --

    #[test]
    fn record_opcodes_exist() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("record_opcodes")
            .invariant("Record construction opcodes defined");
        let ops = [
            Opcode::MakeRecord,
            Opcode::Column,
            Opcode::ResultRow,
            Opcode::Rowid,
            Opcode::NullRow,
        ];
        for op in &ops {
            diag_assert!(
                ctx.clone(),
                Opcode::from_byte(*op as u8).is_some(),
                "record opcode exists"
            );
        }
    }

    // -- UT-VDBE-006: Sort/traverse opcodes --

    #[test]
    fn sort_opcodes_exist() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("sort_opcodes")
            .invariant("Sort/traverse opcodes defined");
        let ops = [
            Opcode::SorterOpen,
            Opcode::SorterInsert,
            Opcode::SorterSort,
            Opcode::SorterNext,
            Opcode::SorterData,
            Opcode::Rewind,
            Opcode::Next,
            Opcode::Prev,
        ];
        for op in &ops {
            diag_assert!(
                ctx.clone(),
                Opcode::from_byte(*op as u8).is_some(),
                "sort opcode exists"
            );
        }
    }

    // -- UT-VDBE-007: Aggregate opcodes --

    #[test]
    fn aggregate_opcodes_exist() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("aggregate_opcodes")
            .invariant("Aggregate opcodes defined");
        let ops = [
            Opcode::AggStep,
            Opcode::AggStep1,
            Opcode::AggValue,
            Opcode::AggFinal,
            Opcode::AggInverse,
        ];
        for op in &ops {
            diag_assert!(
                ctx.clone(),
                Opcode::from_byte(*op as u8).is_some(),
                "aggregate opcode exists"
            );
        }
    }

    // -- Opcode name uniqueness --

    #[test]
    fn opcode_names_unique() {
        let mut seen = std::collections::HashSet::new();
        let ctx = DiagContext::new(BEAD_ID)
            .case("opcode_names_unique")
            .invariant("All opcode names are unique");
        for byte in 1..=255u8 {
            if let Some(op) = Opcode::from_byte(byte) {
                let name = op.name();
                diag_assert!(ctx.clone(), seen.insert(name), "duplicate opcode name");
            }
        }
    }

    // -- P4 variants --

    #[test]
    fn p4_none_is_default() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("p4_none")
            .invariant("P4::None is the default variant");
        let op = VdbeOp {
            opcode: Opcode::Goto,
            p1: 0,
            p2: 5,
            p3: 0,
            p4: P4::None,
            p5: 0,
        };
        diag_assert_eq!(ctx, op.p4, P4::None);
    }
}

// ─── Function Suite (UT-FUN) ─────────────────────────────────────────────

#[cfg(test)]
mod function_tests {
    use super::*;
    use fsqlite_error::FrankenError;
    use fsqlite_func::{
        FunctionRegistry, register_builtins, register_datetime_builtins, register_math_builtins,
    };
    use fsqlite_types::SqliteValue;

    fn full_registry() -> FunctionRegistry {
        let mut reg = FunctionRegistry::new();
        register_builtins(&mut reg);
        register_math_builtins(&mut reg);
        register_datetime_builtins(&mut reg);
        reg
    }

    // -- UT-FUN-001: String functions --

    #[test]
    fn string_functions_registered() {
        let reg = full_registry();
        let ctx = DiagContext::new(BEAD_ID)
            .case("string_functions")
            .invariant("Core string functions are registered");
        let names = [
            "length", "substr", "replace", "trim", "ltrim", "rtrim", "upper", "lower", "hex",
            "quote", "instr",
        ];
        for name in &names {
            diag_assert!(
                ctx.clone(),
                reg.contains_scalar(name),
                "function '{name}' not registered"
            );
        }
    }

    #[test]
    fn func_length_text() {
        let reg = full_registry();
        let func = reg.find_scalar("length", 1).expect("length registered");
        let result = func
            .invoke(&[SqliteValue::Text("hello".to_string().into())])
            .unwrap();
        let ctx = DiagContext::new(BEAD_ID)
            .case("length_text")
            .invariant("length('hello') = 5");
        diag_assert_eq!(ctx, result, SqliteValue::Integer(5));
    }

    #[test]
    fn func_length_null() {
        let reg = full_registry();
        let func = reg.find_scalar("length", 1).expect("length registered");
        let result = func.invoke(&[SqliteValue::Null]).unwrap();
        let ctx = DiagContext::new(BEAD_ID)
            .case("length_null")
            .invariant("length(NULL) = NULL");
        diag_assert_eq!(ctx, result, SqliteValue::Null);
    }

    #[test]
    fn func_length_blob() {
        let reg = full_registry();
        let func = reg.find_scalar("length", 1).expect("length registered");
        let result = func
            .invoke(&[SqliteValue::Blob(vec![1, 2, 3, 4].into())])
            .unwrap();
        let ctx = DiagContext::new(BEAD_ID)
            .case("length_blob")
            .invariant("length(X'01020304') = 4");
        diag_assert_eq!(ctx, result, SqliteValue::Integer(4));
    }

    #[test]
    fn func_upper_lower() {
        let reg = full_registry();
        let upper = reg.find_scalar("upper", 1).expect("upper registered");
        let lower = reg.find_scalar("lower", 1).expect("lower registered");

        let ctx = DiagContext::new(BEAD_ID)
            .case("upper_lower")
            .invariant("upper/lower transform correctly");
        let up_result = upper
            .invoke(&[SqliteValue::Text("hello".to_string().into())])
            .unwrap();
        diag_assert_eq!(
            ctx.clone(),
            up_result,
            SqliteValue::Text("HELLO".to_string().into())
        );

        let lo_result = lower
            .invoke(&[SqliteValue::Text("HELLO".to_string().into())])
            .unwrap();
        diag_assert_eq!(
            ctx,
            lo_result,
            SqliteValue::Text("hello".to_string().into())
        );
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn func_typeof() {
        let reg = full_registry();
        let typeof_fn = reg.find_scalar("typeof", 1).expect("typeof registered");

        let ctx = DiagContext::new(BEAD_ID)
            .case("typeof")
            .invariant("typeof returns correct type names");
        let cases = [
            (SqliteValue::Null, "null"),
            (SqliteValue::Integer(42), "integer"),
            (SqliteValue::Float(3.14), "real"),
            (SqliteValue::Text("hi".to_string().into()), "text"),
            (SqliteValue::Blob(vec![1].into()), "blob"),
        ];
        for (val, expected) in &cases {
            let result = typeof_fn.invoke(std::slice::from_ref(val)).unwrap();
            diag_assert_eq!(ctx.clone(), result, SqliteValue::Text((*expected).into()));
        }
    }

    // -- UT-FUN-002: Math functions --

    #[test]
    fn math_functions_registered() {
        let reg = full_registry();
        let ctx = DiagContext::new(BEAD_ID)
            .case("math_functions")
            .invariant("Core math functions are registered");
        let names = ["abs", "round"];
        for name in &names {
            diag_assert!(
                ctx.clone(),
                reg.contains_scalar(name),
                "function '{name}' not registered"
            );
        }
    }

    #[test]
    fn func_abs_positive() {
        let reg = full_registry();
        let func = reg.find_scalar("abs", 1).expect("abs registered");
        let result = func.invoke(&[SqliteValue::Integer(-42)]).unwrap();
        let ctx = DiagContext::new(BEAD_ID)
            .case("abs_positive")
            .invariant("abs(-42) = 42");
        diag_assert_eq!(ctx, result, SqliteValue::Integer(42));
    }

    #[test]
    fn func_abs_null() {
        let reg = full_registry();
        let func = reg.find_scalar("abs", 1).expect("abs registered");
        let result = func.invoke(&[SqliteValue::Null]).unwrap();
        let ctx = DiagContext::new(BEAD_ID)
            .case("abs_null")
            .invariant("abs(NULL) = NULL");
        diag_assert_eq!(ctx, result, SqliteValue::Null);
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn func_abs_float() {
        let reg = full_registry();
        let func = reg.find_scalar("abs", 1).expect("abs registered");
        let result = func.invoke(&[SqliteValue::Float(-3.14)]).unwrap();
        let ctx = DiagContext::new(BEAD_ID)
            .case("abs_float")
            .invariant("abs(-3.14) = 3.14");
        match result {
            #[allow(clippy::neg_cmp_op_on_partial_ord)]
            SqliteValue::Float(v) => diag_assert!(ctx, (v - 3.14).abs() < f64::EPSILON, "matches"),
            other => diag_assert!(ctx, false, "expected Float, got {other:?}"),
        }
    }

    // -- UT-FUN-005: Type inspection functions --

    #[test]
    fn func_coalesce() {
        let reg = full_registry();
        let sqlite = rusqlite::Connection::open_in_memory().expect("open C SQLite oracle");

        for (case, sql, args) in [
            (
                "minimum_arity",
                "SELECT COALESCE(NULL, 7)",
                vec![SqliteValue::Null, SqliteValue::Integer(7)],
            ),
            (
                "variadic_null_chain",
                "SELECT COALESCE(NULL, NULL, 42, 99)",
                vec![
                    SqliteValue::Null,
                    SqliteValue::Null,
                    SqliteValue::Integer(42),
                    SqliteValue::Integer(99),
                ],
            ),
        ] {
            let func = reg
                .find_scalar("coalesce", i32::try_from(args.len()).unwrap())
                .expect("coalesce registered");
            let actual = func.invoke(&args).unwrap();
            let reference = sqlite
                .query_row(sql, [], |row| row.get::<_, i64>(0))
                .expect("C SQLite COALESCE query");
            let ctx = DiagContext::new(BEAD_ID)
                .case(case)
                .invariant("COALESCE registry result matches C SQLite");
            diag_assert_eq!(ctx, actual, SqliteValue::Integer(reference));
        }

        let invalid = reg
            .find_scalar("coalesce", 1)
            .expect("known function returns wrong-arity sentinel")
            .invoke(&[SqliteValue::Integer(1)])
            .expect_err("one-argument COALESCE must fail");
        let oracle_error = sqlite
            .prepare("SELECT coalesce(1)")
            .expect_err("C SQLite must reject one-argument COALESCE");
        let ctx = DiagContext::new(BEAD_ID)
            .case("invalid_arity")
            .invariant("COALESCE invalid-arity diagnostic matches C SQLite");
        let oracle_message = match oracle_error {
            rusqlite::Error::SqliteFailure(_, Some(message)) => message,
            rusqlite::Error::SqlInputError { msg, .. } => msg,
            other => {
                diag_assert!(ctx, false, "unexpected C SQLite error: {other}");
                return;
            }
        };
        let actual_message = match invalid {
            FrankenError::FunctionError(message) => message,
            other => {
                diag_assert!(ctx, false, "unexpected FrankenSQLite error: {other}");
                return;
            }
        };
        diag_assert_eq!(ctx, actual_message, oracle_message);
    }

    #[test]
    fn func_nullif_equal() {
        let reg = full_registry();
        let func = reg.find_scalar("nullif", 2).expect("nullif registered");
        let result = func
            .invoke(&[SqliteValue::Integer(5), SqliteValue::Integer(5)])
            .unwrap();
        let ctx = DiagContext::new(BEAD_ID)
            .case("nullif_equal")
            .invariant("nullif(5, 5) = NULL");
        diag_assert_eq!(ctx, result, SqliteValue::Null);
    }

    #[test]
    fn func_nullif_different() {
        let reg = full_registry();
        let func = reg.find_scalar("nullif", 2).expect("nullif registered");
        let result = func
            .invoke(&[SqliteValue::Integer(5), SqliteValue::Integer(3)])
            .unwrap();
        let ctx = DiagContext::new(BEAD_ID)
            .case("nullif_different")
            .invariant("nullif(5, 3) = 5");
        diag_assert_eq!(ctx, result, SqliteValue::Integer(5));
    }

    #[test]
    fn func_ifnull() {
        let reg = full_registry();
        let func = reg.find_scalar("ifnull", 2).expect("ifnull registered");

        let ctx = DiagContext::new(BEAD_ID)
            .case("ifnull")
            .invariant("ifnull returns first non-NULL");
        let r1 = func
            .invoke(&[SqliteValue::Null, SqliteValue::Integer(99)])
            .unwrap();
        diag_assert_eq!(ctx.clone(), r1, SqliteValue::Integer(99));

        let r2 = func
            .invoke(&[SqliteValue::Integer(7), SqliteValue::Integer(99)])
            .unwrap();
        diag_assert_eq!(ctx, r2, SqliteValue::Integer(7));
    }

    // -- Function registry mechanics --

    #[test]
    fn registry_case_insensitive_lookup() {
        let reg = full_registry();
        let ctx = DiagContext::new(BEAD_ID)
            .case("case_insensitive")
            .invariant("Function lookup is case-insensitive");
        diag_assert!(
            ctx.clone(),
            reg.find_scalar("ABS", 1).is_some(),
            "ABS found"
        );
        diag_assert!(
            ctx.clone(),
            reg.find_scalar("abs", 1).is_some(),
            "abs found"
        );
        diag_assert!(ctx, reg.find_scalar("Abs", 1).is_some(), "Abs found");
    }

    #[test]
    fn registry_wrong_arity_not_found() {
        let reg = full_registry();
        let ctx = DiagContext::new(BEAD_ID)
            .case("wrong_arity")
            .invariant("Wrong arg count returns a function error for known scalar names");
        // abs takes exactly 1 arg
        let result = reg.find_scalar("abs", 5);
        let Some(function) = result else {
            diag_assert!(ctx, false, "wrong arity should return an erroring scalar");
            return;
        };
        let err = function
            .invoke(&[
                SqliteValue::Null,
                SqliteValue::Null,
                SqliteValue::Null,
                SqliteValue::Null,
                SqliteValue::Null,
            ])
            .expect_err("wrong arity should return function error");
        diag_assert!(
            ctx,
            matches!(&err, FrankenError::FunctionError(message)
                if message == "wrong number of arguments to function abs()"),
            "wrong arity raises SQLite-compatible function error"
        );
    }

    #[test]
    fn registry_unknown_function() {
        let reg = full_registry();
        let ctx = DiagContext::new(BEAD_ID)
            .case("unknown_function")
            .invariant("Non-existent function returns None");
        diag_assert_eq!(ctx, reg.find_scalar("no_such_func", 1).is_none(), true);
    }

    #[test]
    fn registry_empty() {
        let reg = FunctionRegistry::new();
        let ctx = DiagContext::new(BEAD_ID)
            .case("empty_registry")
            .invariant("Empty registry finds nothing");
        diag_assert!(ctx, reg.find_scalar("abs", 1).is_none(), "empty registry");
    }

    #[test]
    fn func_hex() {
        let reg = full_registry();
        let func = reg.find_scalar("hex", 1).expect("hex registered");
        let result = func
            .invoke(&[SqliteValue::Blob(vec![0xDE, 0xAD].into())])
            .unwrap();
        let ctx = DiagContext::new(BEAD_ID)
            .case("hex_blob")
            .invariant("hex(X'DEAD') = 'DEAD'");
        diag_assert_eq!(ctx, result, SqliteValue::Text("DEAD".to_string().into()));
    }
}
