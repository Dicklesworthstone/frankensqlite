//! Literal parameterization of ad-hoc SQL text (bd-5310l).
//!
//! The parse cache, the compiled-program cache and the prepared-statement
//! cache are all keyed on exact SQL text, so two statements that differ only
//! in their literals share nothing: `INSERT INTO t(a,b) VALUES (1,'x')` and
//! `INSERT INTO t(a,b) VALUES (2,'y')` each parse and compile from scratch.
//! This module recognizes a few statement shapes straight from the text,
//! replaces the literals whose value cannot change the plan with numbered
//! parameters, and returns the rewritten text with the values to bind. Every
//! statement of one shape then presents the same text to those caches.
//!
//! Recognized shapes (plain unquoted ASCII identifiers that are not keywords,
//! one statement, an optional trailing semicolon, no comments):
//!
//! * `INSERT INTO t [(c, ...)] VALUES (v, ...)` (one row)
//! * `SELECT * | c, ... FROM t WHERE k = int`
//! * `UPDATE t SET c = v | c2 | c2 +/- num [, ...] WHERE k = int`
//! * `DELETE FROM t WHERE k = int`
//!
//! where `v` is an optionally signed decimal integer or `digits.digits`
//! float, a string, or `NULL` (which stays in the text). Anything else is not
//! parameterized and takes the ordinary text path: hex, exponents and blobs,
//! an integer too large for `i64` (SQLite reads it as REAL, and
//! `-9223372036854775808` as an integer), and every byte the scan does not
//! expect where it stands.
//!
//! Recognition is a single pass over the bytes, about the price of copying
//! the statement, and reads each accepted token exactly as the SQL lexer
//! does: the same whitespace, the same identifier bytes checked against the
//! lexer's own keyword table, digits parsed by the same routine, and strings
//! closed by the same doubled-quote rule. The tests hold it to a matcher
//! built on the lexer itself.
//!
//! The caller decides whether the statement may run parameterized against the
//! current schema (the target must be an ordinary table and `k` its rowid
//! alias, so the plan is a rowid seek whatever the value). A byte offset an
//! error records in the rewritten text is moved back to the text the caller
//! wrote with [`ParameterizedLiteralStatement::original_offset`].

use fsqlite_parser::TokenKind;
use fsqlite_types::value::SqliteValue;

/// Most parameters one statement may be rewritten to. A wider row stays
/// literal text.
const MAX_LITERAL_PARAMETERS: usize = 256;

/// A literal statement rewritten to its parameterized form.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ParameterizedLiteralStatement {
    /// The statement text with each parameterized literal replaced by `?N`.
    /// Everything else is copied verbatim.
    pub(super) sql: String,
    /// The replaced values, in `?N` order.
    pub(super) params: Vec<SqliteValue>,
    /// The target table, as written.
    pub(super) table: String,
    /// The column compared with the key literal in `WHERE` (not for INSERT).
    pub(super) key_column: Option<String>,
    /// Where each parameter sits, in text order.
    slots: Vec<Slot>,
}

/// One parameter's place: the bytes `?N` takes in the rewritten text and the
/// bytes of the original text it replaced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Slot {
    rewritten_start: usize,
    rewritten_end: usize,
    original_start: usize,
    original_end: usize,
}

impl ParameterizedLiteralStatement {
    /// The byte offset, in the text the caller wrote, of the byte at `offset`
    /// in [`Self::sql`]. An offset inside a parameter is the start of the
    /// literal it replaced. Text outside the parameters is copied verbatim,
    /// so it moves by the difference in length of the parameters before it.
    pub(super) fn original_offset(&self, offset: u32) -> u32 {
        let offset = usize::try_from(offset).unwrap_or(usize::MAX);
        let mut original = offset;
        for slot in &self.slots {
            if offset < slot.rewritten_start {
                break;
            }
            original = if offset < slot.rewritten_end {
                slot.original_start
            } else {
                slot.original_end
                    .saturating_add(offset - slot.rewritten_end)
            };
        }
        u32::try_from(original).unwrap_or(u32::MAX)
    }
}

/// Rewrite `sql` to its parameterized form when it is one of the shapes this
/// module recognizes; `None` for everything else.
pub(super) fn parameterize_literal_statement(sql: &str) -> Option<ParameterizedLiteralStatement> {
    let mut scan = Scan {
        sql,
        bytes: sql.as_bytes(),
        pos: 0,
        out: String::with_capacity(sql.len()),
        copied_to: 0,
        params: Vec::new(),
        slots: Vec::new(),
    };
    let verb = scan.word()?;
    let (table, key_column) = if verb.eq_ignore_ascii_case("insert") {
        (scan.insert()?, None)
    } else if verb.eq_ignore_ascii_case("select") {
        let (table, key) = scan.select()?;
        (table, Some(key))
    } else if verb.eq_ignore_ascii_case("update") {
        let (table, key) = scan.update()?;
        (table, Some(key))
    } else if verb.eq_ignore_ascii_case("delete") {
        let (table, key) = scan.delete()?;
        (table, Some(key))
    } else {
        return None;
    };
    scan.finish()?;
    Some(ParameterizedLiteralStatement {
        sql: scan.out,
        params: scan.params,
        table: table.to_owned(),
        key_column: key_column.map(str::to_owned),
        slots: scan.slots,
    })
}

struct Scan<'a> {
    sql: &'a str,
    bytes: &'a [u8],
    pos: usize,
    /// The rewritten text so far: `sql[..copied_to]` with slots replaced.
    out: String,
    copied_to: usize,
    params: Vec<SqliteValue>,
    slots: Vec<Slot>,
}

impl<'a> Scan<'a> {
    /// Skip what the lexer skips between tokens, comments aside: a comment is
    /// simply not skipped here, and whatever token is expected next then
    /// fails to match.
    fn skip_whitespace(&mut self) {
        while self
            .bytes
            .get(self.pos)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.pos += 1;
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.skip_whitespace();
        self.bytes.get(self.pos).copied()
    }

    /// The next token when it is a bare word of ASCII letters, digits and
    /// underscores, as the lexer delimits one. A word that runs into a
    /// non-ASCII byte is longer for the lexer than for this scan, so it is
    /// refused.
    fn word(&mut self) -> Option<&'a str> {
        self.skip_whitespace();
        let start = self.pos;
        let first = *self.bytes.get(start)?;
        if !(first.is_ascii_alphabetic() || first == b'_') {
            return None;
        }
        let mut end = start + 1;
        while self
            .bytes
            .get(end)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            end += 1;
        }
        if self.bytes.get(end).is_some_and(|byte| *byte >= 0x80) {
            return None;
        }
        self.pos = end;
        self.sql.get(start..end)
    }

    fn keyword(&mut self, keyword: &str) -> Option<()> {
        self.word()?.eq_ignore_ascii_case(keyword).then_some(())
    }

    /// A word the lexer reads as a plain identifier, not as a keyword.
    fn identifier(&mut self) -> Option<&'a str> {
        let word = self.word()?;
        TokenKind::lookup_keyword(word).is_none().then_some(word)
    }

    fn punctuation(&mut self, expected: u8) -> Option<()> {
        (self.peek()? == expected).then(|| self.pos += 1)
    }

    /// `=` as its own token (`==` is a different one).
    fn equals(&mut self) -> Option<()> {
        self.punctuation(b'=')?;
        (self.bytes.get(self.pos) != Some(&b'=')).then_some(())
    }

    /// Replace `sql[start..end]` with the next numbered parameter.
    fn slot(&mut self, start: usize, end: usize, value: SqliteValue) -> Option<()> {
        if self.params.len() >= MAX_LITERAL_PARAMETERS {
            return None;
        }
        self.out.push_str(self.sql.get(self.copied_to..start)?);
        let rewritten_start = self.out.len();
        self.params.push(value);
        self.out.push('?');
        self.out.push_str(&self.params.len().to_string());
        self.slots.push(Slot {
            rewritten_start,
            rewritten_end: self.out.len(),
            original_start: start,
            original_end: end,
        });
        self.copied_to = end;
        Some(())
    }

    /// A decimal integer or `digits.digits` float at the current position,
    /// negated when `negative`. The lexer reads the same bytes as one number
    /// token and parses them with the same routines; every other spelling of
    /// a number (hex, digit separators, exponents, a bare leading or trailing
    /// dot) is refused, as is a number the lexer would reject for running
    /// into a letter.
    fn unsigned_number(&mut self, negative: bool) -> Option<SqliteValue> {
        let start = self.pos;
        let mut end = start;
        while self.bytes.get(end).is_some_and(u8::is_ascii_digit) {
            end += 1;
        }
        if end == start {
            return None;
        }
        let mut is_float = false;
        if self.bytes.get(end) == Some(&b'.') {
            let fraction = end + 1;
            let mut fraction_end = fraction;
            while self.bytes.get(fraction_end).is_some_and(u8::is_ascii_digit) {
                fraction_end += 1;
            }
            if fraction_end == fraction {
                return None;
            }
            is_float = true;
            end = fraction_end;
        }
        if self
            .bytes
            .get(end)
            .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_' || *byte == b'.' || *byte >= 0x80)
        {
            return None;
        }
        let text = self.sql.get(start..end)?;
        let value = if is_float {
            let value = text.parse::<f64>().ok()?;
            if !value.is_finite() {
                return None;
            }
            SqliteValue::Float(if negative { -value } else { value })
        } else {
            // Too large for i64 is a REAL to SQLite, except right after a
            // minus sign: neither is what a bound integer would be.
            let value = text.parse::<i64>().ok()?;
            SqliteValue::Integer(if negative { -value } else { value })
        };
        self.pos = end;
        Some(value)
    }

    /// An optionally signed number, with the byte range it spans (sign
    /// included). Whitespace may separate the sign from the digits.
    fn signed_number(&mut self) -> Option<(usize, usize, SqliteValue)> {
        let start = self.pos;
        let negative = match self.bytes.get(self.pos) {
            Some(b'-') => {
                self.pos += 1;
                self.skip_whitespace();
                true
            }
            Some(b'+') => {
                self.pos += 1;
                self.skip_whitespace();
                false
            }
            _ => false,
        };
        let value = self.unsigned_number(negative)?;
        Some((start, self.pos, value))
    }

    /// A single-quoted string at the current position: its value with
    /// doubled quotes undone, and the scan left just past the closing quote.
    fn string(&mut self) -> Option<SqliteValue> {
        let mut cursor = self.pos + 1;
        let mut value = String::new();
        loop {
            let quote = cursor + self.bytes.get(cursor..)?.iter().position(|byte| *byte == b'\'')?;
            value.push_str(self.sql.get(cursor..quote)?);
            if self.bytes.get(quote + 1) == Some(&b'\'') {
                value.push('\'');
                cursor = quote + 2;
            } else {
                self.pos = quote + 1;
                return Some(SqliteValue::from(value));
            }
        }
    }

    /// One VALUES term or SET value: a signed number or a string (each
    /// becomes a parameter) or `NULL` (kept as text).
    fn value(&mut self) -> Option<()> {
        let first = self.peek()?;
        let start = self.pos;
        if first == b'\'' {
            let value = self.string()?;
            self.slot(start, self.pos, value)
        } else if first.is_ascii_alphabetic() {
            self.keyword("null")
        } else {
            let (start, end, value) = self.signed_number()?;
            self.slot(start, end, value)
        }
    }

    /// `WHERE k = int`: returns `k`.
    fn key_filter(&mut self) -> Option<&'a str> {
        self.keyword("where")?;
        let key = self.identifier()?;
        self.equals()?;
        self.skip_whitespace();
        let (start, end, value) = self.signed_number()?;
        if !matches!(value, SqliteValue::Integer(_)) {
            return None;
        }
        self.slot(start, end, value)?;
        Some(key)
    }

    fn insert(&mut self) -> Option<&'a str> {
        self.keyword("into")?;
        let table = self.identifier()?;
        if self.peek()? == b'(' {
            self.pos += 1;
            loop {
                self.identifier()?;
                if self.peek()? == b',' {
                    self.pos += 1;
                } else {
                    break;
                }
            }
            self.punctuation(b')')?;
        }
        self.keyword("values")?;
        // One row only: a multi-row VALUES list has its own batch lane on the
        // text path, and its text would differ with every row count anyway.
        self.punctuation(b'(')?;
        loop {
            self.value()?;
            if self.peek()? == b',' {
                self.pos += 1;
            } else {
                break;
            }
        }
        self.punctuation(b')')?;
        Some(table)
    }

    fn select(&mut self) -> Option<(&'a str, &'a str)> {
        if self.peek()? == b'*' {
            self.pos += 1;
        } else {
            loop {
                self.identifier()?;
                if self.peek()? == b',' {
                    self.pos += 1;
                } else {
                    break;
                }
            }
        }
        self.keyword("from")?;
        let table = self.identifier()?;
        let key = self.key_filter()?;
        Some((table, key))
    }

    fn update(&mut self) -> Option<(&'a str, &'a str)> {
        let table = self.identifier()?;
        self.keyword("set")?;
        loop {
            self.identifier()?;
            self.equals()?;
            let first = self.peek()?;
            if first.is_ascii_alphabetic() || first == b'_' {
                let word = self.word()?;
                if word.eq_ignore_ascii_case("null") {
                    // `c = NULL` stays as text.
                } else if TokenKind::lookup_keyword(word).is_some() {
                    return None;
                } else if matches!(self.peek(), Some(b'+' | b'-')) {
                    // `c = c2 +/- number`: the operator stays.
                    self.pos += 1;
                    self.skip_whitespace();
                    let start = self.pos;
                    let value = self.unsigned_number(false)?;
                    self.slot(start, self.pos, value)?;
                }
            } else {
                self.value()?;
            }
            if self.peek()? == b',' {
                self.pos += 1;
            } else {
                break;
            }
        }
        let key = self.key_filter()?;
        Some((table, key))
    }

    fn delete(&mut self) -> Option<(&'a str, &'a str)> {
        self.keyword("from")?;
        let table = self.identifier()?;
        let key = self.key_filter()?;
        Some((table, key))
    }

    /// Accept the end of the statement and complete the rewritten text. A
    /// statement with no parameter to replace is left to the text path.
    fn finish(&mut self) -> Option<()> {
        if self.peek() == Some(b';') {
            self.pos += 1;
        }
        if self.peek().is_some() || self.params.is_empty() {
            return None;
        }
        self.out.push_str(self.sql.get(self.copied_to..)?);
        Some(())
    }
}

/// The same recognition built on the SQL lexer's own tokens. It is what the
/// byte scan above must agree with wherever the scan accepts a statement, and
/// it accepts more (comments, hex, exponents, blobs); it costs several times
/// as much, which is why it is only the yardstick.
#[cfg(test)]
mod lexer_reference {
    use fsqlite_parser::lexer::Lexer;
    use fsqlite_parser::{Token, TokenKind};
    use fsqlite_types::value::SqliteValue;

    use super::MAX_LITERAL_PARAMETERS;

    /// What the reference makes of a statement it recognizes.
    #[derive(Debug, PartialEq)]
    pub(super) struct Recognized {
        pub(super) sql: String,
        pub(super) params: Vec<SqliteValue>,
        pub(super) table: String,
        pub(super) key_column: Option<String>,
    }

    pub(super) fn parameterize(sql: &str) -> Option<Recognized> {
        if u32::try_from(sql.len()).is_err() {
            return None;
        }
        let mut matcher = Matcher::new(sql);
        let (table, key_column) = match matcher.current.kind {
            TokenKind::KwInsert => (matcher.insert()?, None),
            TokenKind::KwSelect => {
                let (table, key) = matcher.select()?;
                (table, Some(key))
            }
            TokenKind::KwUpdate => {
                let (table, key) = matcher.update()?;
                (table, Some(key))
            }
            TokenKind::KwDelete => {
                let (table, key) = matcher.delete()?;
                (table, Some(key))
            }
            _ => return None,
        };
        let (sql, params) = matcher.finish()?;
        Some(Recognized {
            sql,
            params,
            table,
            key_column,
        })
    }

    struct Matcher<'a> {
        sql: &'a str,
        lexer: Lexer<'a>,
        current: Token,
        out: String,
        copied_to: usize,
        params: Vec<SqliteValue>,
    }

    impl<'a> Matcher<'a> {
        fn new(sql: &'a str) -> Self {
            let mut lexer = Lexer::new(sql);
            let current = lexer.next_token();
            Self {
                sql,
                lexer,
                current,
                out: String::with_capacity(sql.len()),
                copied_to: 0,
                params: Vec::new(),
            }
        }

        fn advance(&mut self) {
            self.current = self.lexer.next_token();
        }

        fn eat(&mut self, matches: impl FnOnce(&TokenKind) -> bool) -> Option<()> {
            if matches(&self.current.kind) {
                self.advance();
                Some(())
            } else {
                None
            }
        }

        fn identifier(&mut self) -> Option<String> {
            let TokenKind::Id(name) = &self.current.kind else {
                return None;
            };
            let name = name.to_string();
            self.advance();
            Some(name)
        }

        fn slot(&mut self, start: u32, end: u32, value: SqliteValue) -> Option<()> {
            if self.params.len() >= MAX_LITERAL_PARAMETERS {
                return None;
            }
            let (start, end) = (start as usize, end as usize);
            self.out.push_str(self.sql.get(self.copied_to..start)?);
            self.params.push(value);
            self.out.push('?');
            self.out.push_str(&self.params.len().to_string());
            self.copied_to = end;
            Some(())
        }

        fn signed_number(&mut self) -> Option<(u32, u32, SqliteValue)> {
            let start = self.current.span.start;
            let negative = match self.current.kind {
                TokenKind::Minus => {
                    self.advance();
                    true
                }
                TokenKind::Plus => {
                    self.advance();
                    false
                }
                _ => false,
            };
            let value = match self.current.kind {
                TokenKind::Integer(value) => SqliteValue::Integer(if negative {
                    value.checked_neg()?
                } else {
                    value
                }),
                TokenKind::Float(value) if value.is_finite() => {
                    SqliteValue::Float(if negative { -value } else { value })
                }
                _ => return None,
            };
            let end = self.current.span.end;
            self.advance();
            Some((start, end, value))
        }

        fn value(&mut self) -> Option<()> {
            match &self.current.kind {
                TokenKind::KwNull => {
                    self.advance();
                    Some(())
                }
                TokenKind::String(text) => {
                    let value = SqliteValue::from(text.as_str());
                    let (start, end) = (self.current.span.start, self.current.span.end);
                    self.advance();
                    self.slot(start, end, value)
                }
                TokenKind::Blob(bytes) => {
                    let value = SqliteValue::from(bytes.clone());
                    let (start, end) = (self.current.span.start, self.current.span.end);
                    self.advance();
                    self.slot(start, end, value)
                }
                _ => {
                    let (start, end, value) = self.signed_number()?;
                    self.slot(start, end, value)
                }
            }
        }

        fn key_filter(&mut self) -> Option<String> {
            self.eat(|kind| matches!(kind, TokenKind::KwWhere))?;
            let key = self.identifier()?;
            self.eat(|kind| matches!(kind, TokenKind::Eq))?;
            let (start, end, value) = self.signed_number()?;
            if !matches!(value, SqliteValue::Integer(_)) {
                return None;
            }
            self.slot(start, end, value)?;
            Some(key)
        }

        fn insert(&mut self) -> Option<String> {
            self.eat(|kind| matches!(kind, TokenKind::KwInsert))?;
            self.eat(|kind| matches!(kind, TokenKind::KwInto))?;
            let table = self.identifier()?;
            if matches!(self.current.kind, TokenKind::LeftParen) {
                self.advance();
                loop {
                    self.identifier()?;
                    if matches!(self.current.kind, TokenKind::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }
                self.eat(|kind| matches!(kind, TokenKind::RightParen))?;
            }
            self.eat(|kind| matches!(kind, TokenKind::KwValues))?;
            self.eat(|kind| matches!(kind, TokenKind::LeftParen))?;
            loop {
                self.value()?;
                if matches!(self.current.kind, TokenKind::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            self.eat(|kind| matches!(kind, TokenKind::RightParen))?;
            Some(table)
        }

        fn select(&mut self) -> Option<(String, String)> {
            self.eat(|kind| matches!(kind, TokenKind::KwSelect))?;
            if matches!(self.current.kind, TokenKind::Star) {
                self.advance();
            } else {
                loop {
                    self.identifier()?;
                    if matches!(self.current.kind, TokenKind::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }
            }
            self.eat(|kind| matches!(kind, TokenKind::KwFrom))?;
            let table = self.identifier()?;
            let key = self.key_filter()?;
            Some((table, key))
        }

        fn update(&mut self) -> Option<(String, String)> {
            self.eat(|kind| matches!(kind, TokenKind::KwUpdate))?;
            let table = self.identifier()?;
            self.eat(|kind| matches!(kind, TokenKind::KwSet))?;
            loop {
                self.identifier()?;
                self.eat(|kind| matches!(kind, TokenKind::Eq))?;
                if matches!(self.current.kind, TokenKind::Id(_)) {
                    self.advance();
                    if matches!(self.current.kind, TokenKind::Plus | TokenKind::Minus) {
                        self.advance();
                        let (start, end) = (self.current.span.start, self.current.span.end);
                        let value = match self.current.kind {
                            TokenKind::Integer(value) => SqliteValue::Integer(value),
                            TokenKind::Float(value) if value.is_finite() => {
                                SqliteValue::Float(value)
                            }
                            _ => return None,
                        };
                        self.advance();
                        self.slot(start, end, value)?;
                    }
                } else {
                    self.value()?;
                }
                if matches!(self.current.kind, TokenKind::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            let key = self.key_filter()?;
            Some((table, key))
        }

        fn delete(&mut self) -> Option<(String, String)> {
            self.eat(|kind| matches!(kind, TokenKind::KwDelete))?;
            self.eat(|kind| matches!(kind, TokenKind::KwFrom))?;
            let table = self.identifier()?;
            let key = self.key_filter()?;
            Some((table, key))
        }

        fn finish(mut self) -> Option<(String, Vec<SqliteValue>)> {
            if matches!(self.current.kind, TokenKind::Semicolon) {
                self.advance();
            }
            if !matches!(self.current.kind, TokenKind::Eof) || self.params.is_empty() {
                return None;
            }
            self.out.push_str(self.sql.get(self.copied_to..)?);
            Some((self.out, self.params))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewritten(sql: &str) -> Option<(String, Vec<SqliteValue>)> {
        parameterize_literal_statement(sql).map(|statement| (statement.sql, statement.params))
    }

    #[test]
    fn insert_literals_become_numbered_parameters_and_null_stays_text() {
        let statement = parameterize_literal_statement(
            "INSERT INTO t(a, b, c, d) VALUES (7, -2.5, 'it''s', NULL);",
        )
        .expect("recognized");
        assert_eq!(statement.table, "t");
        assert_eq!(statement.key_column, None);
        assert_eq!(
            statement.sql,
            "INSERT INTO t(a, b, c, d) VALUES (?1, ?2, ?3, NULL);"
        );
        assert_eq!(
            statement.params,
            vec![
                SqliteValue::Integer(7),
                SqliteValue::Float(-2.5),
                SqliteValue::from("it's"),
            ]
        );
    }

    #[test]
    fn statements_differing_only_in_literals_share_one_text() {
        let first = rewritten("INSERT INTO e(w,s) VALUES(1,'p-1-1')").expect("recognized");
        let second = rewritten("INSERT INTO e(w,s) VALUES(987654,'a much longer payload')")
            .expect("recognized");
        assert_eq!(first.0, second.0);
        assert_eq!(first.0, "INSERT INTO e(w,s) VALUES(?1,?2)");
        assert_ne!(first.1, second.1);
    }

    #[test]
    fn whitespace_is_copied_verbatim() {
        let (sql, params) =
            rewritten("  insert\tINTO t\nVALUES ( +3 , - 4 , 'a' ) ;\n").expect("recognized");
        assert_eq!(sql, "  insert\tINTO t\nVALUES ( ?1 , ?2 , ?3 ) ;\n");
        assert_eq!(
            params,
            vec![
                SqliteValue::Integer(3),
                SqliteValue::Integer(-4),
                SqliteValue::from("a"),
            ]
        );
    }

    #[test]
    fn key_lookups_parameterize_the_integer_key_only() {
        let select =
            parameterize_literal_statement("SELECT a, b FROM t WHERE id = 42").expect("recognized");
        assert_eq!(select.table, "t");
        assert_eq!(select.sql, "SELECT a, b FROM t WHERE id = ?1");
        assert_eq!(select.params, vec![SqliteValue::Integer(42)]);
        assert_eq!(select.key_column.as_deref(), Some("id"));

        let star = rewritten("SELECT * FROM t WHERE id = -1;").expect("recognized");
        assert_eq!(star.0, "SELECT * FROM t WHERE id = ?1;");
        assert_eq!(star.1, vec![SqliteValue::Integer(-1)]);

        let delete =
            parameterize_literal_statement("DELETE FROM t WHERE id = 9").expect("recognized");
        assert_eq!(delete.key_column.as_deref(), Some("id"));
        assert_eq!(delete.sql, "DELETE FROM t WHERE id = ?1");

        let update = parameterize_literal_statement(
            "UPDATE t SET a = a + 1, b = 'x', c = d, e = NULL, f = g - 0.5 WHERE id = 5",
        )
        .expect("recognized");
        assert_eq!(update.key_column.as_deref(), Some("id"));
        assert_eq!(
            update.sql,
            "UPDATE t SET a = a + ?1, b = ?2, c = d, e = NULL, f = g - ?3 WHERE id = ?4"
        );
        assert_eq!(
            update.params,
            vec![
                SqliteValue::Integer(1),
                SqliteValue::from("x"),
                SqliteValue::Float(0.5),
                SqliteValue::Integer(5),
            ]
        );
    }

    /// Statements the scan must leave to the text path.
    const LEFT_AS_TEXT: &[&str] = &[
        // Values SQLite reads differently from a bound parameter: too large
        // for i64 is a REAL, and the negated form is the integer i64::MIN.
        "INSERT INTO t VALUES (9223372036854775808)",
        "INSERT INTO t VALUES (-9223372036854775808)",
        // A sign applies to numbers only.
        "INSERT INTO t VALUES (-'5')",
        "INSERT INTO t VALUES (-NULL)",
        // Number spellings the scan does not read.
        "INSERT INTO t VALUES (0x10)",
        "INSERT INTO t VALUES (1e3)",
        "INSERT INTO t VALUES (1.5e3)",
        "INSERT INTO t VALUES (.5)",
        "INSERT INTO t VALUES (5.)",
        "INSERT INTO t VALUES (1_000)",
        "INSERT INTO t VALUES (12abc)",
        "INSERT INTO t VALUES (1.2.3)",
        "INSERT INTO t VALUES (x'00ff')",
        // Comments.
        "INSERT /* why */ INTO t VALUES (1)",
        "INSERT INTO t VALUES (1) -- tail",
        "INSERT INTO t VALUES (1 /* one */)",
        "INSERT INTO t VALUES (- -1)",
        "INSERT INTO t VALUES (--1\n)",
        // Nothing to parameterize.
        "INSERT INTO t VALUES (NULL)",
        "SELECT a FROM t",
        // Expressions, functions, subqueries, parameters already present.
        "INSERT INTO t VALUES (1 + 2)",
        "INSERT INTO t VALUES (abs(1))",
        "INSERT INTO t VALUES ((SELECT 1))",
        "INSERT INTO t VALUES (?1, 2)",
        "INSERT INTO t VALUES (:a, 2)",
        "INSERT INTO t VALUES (true)",
        "INSERT INTO t SELECT 1",
        "INSERT INTO t DEFAULT VALUES",
        // A multi-row VALUES list keeps its batch lane.
        "INSERT INTO t VALUES (1,'a'),(2,'b')",
        // Clauses whose handling depends on more than the values.
        "INSERT OR REPLACE INTO t VALUES (1)",
        "REPLACE INTO t VALUES (1)",
        "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING",
        "INSERT INTO t VALUES (1) RETURNING a",
        "INSERT INTO main.t VALUES (1)",
        "INSERT INTO t AS x VALUES (1)",
        "WITH c AS (SELECT 1) INSERT INTO t VALUES (1)",
        // Quoted names keep double-quoted-string handling on the text path.
        "INSERT INTO \"t\" VALUES (1)",
        "INSERT INTO t(\"a\") VALUES (1)",
        "INSERT INTO t VALUES (\"x\")",
        "INSERT INTO [t] VALUES (1)",
        "INSERT INTO `t` VALUES (1)",
        // Names the lexer reads as keywords, or as longer than they look.
        "INSERT INTO t(key) VALUES (1)",
        "INSERT INTO order VALUES (1)",
        "INSERT INTO t\u{e9} VALUES (1)",
        "INSERT INTO t(a\u{e9}) VALUES (1)",
        "INSERT INTO t$ VALUES (1)",
        "SELECT a FROM t WHERE index = 1",
        // Result expressions and anything that orders or limits.
        "SELECT 1 FROM t WHERE id = 1",
        "SELECT a AS b FROM t WHERE id = 1",
        "SELECT a + 1 FROM t WHERE id = 1",
        "SELECT count(*) FROM t WHERE id = 1",
        "SELECT DISTINCT a FROM t WHERE id = 1",
        "SELECT a FROM t WHERE id = 1 ORDER BY 1",
        "SELECT a FROM t WHERE id = 1 LIMIT 1",
        "SELECT a FROM t, u WHERE id = 1",
        "SELECT a FROM t WHERE id = 1 AND a = 2",
        "SELECT a FROM t WHERE id > 1",
        "SELECT a FROM t WHERE id == 1",
        "SELECT a FROM t WHERE 1 = id",
        // A key that is not a plain integer.
        "SELECT a FROM t WHERE id = '1'",
        "SELECT a FROM t WHERE id = 1.0",
        "SELECT a FROM t WHERE id = NULL",
        "DELETE FROM t WHERE id = 1 RETURNING a",
        "DELETE FROM t",
        "UPDATE t SET a = 1",
        "UPDATE OR IGNORE t SET a = 1 WHERE id = 1",
        "UPDATE t SET a = a * 2 WHERE id = 1",
        "UPDATE t SET a = a + b WHERE id = 1",
        "UPDATE t SET a = a - -1 WHERE id = 1",
        "UPDATE t SET a = select WHERE id = 1",
        "UPDATE t SET (a, b) = (1, 2) WHERE id = 1",
        // More than one statement, or text after the statement.
        "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)",
        "INSERT INTO t VALUES (1);;",
        "INSERT INTO t VALUES (1) garbage",
        // Not a statement this module looks at.
        "BEGIN",
        "",
        "   ",
        "INSERT INTO t VALUES ('unterminated",
        "INSERT INTO t VALUES ('a' 'b')",
    ];

    #[test]
    fn every_other_shape_stays_text() {
        for sql in LEFT_AS_TEXT {
            assert_eq!(rewritten(sql), None, "{sql}");
        }
    }

    #[test]
    fn a_row_past_the_parameter_cap_stays_text() {
        let at_cap = format!(
            "INSERT INTO t VALUES ({})",
            vec!["1"; MAX_LITERAL_PARAMETERS].join(",")
        );
        assert_eq!(
            rewritten(&at_cap).map(|(_, params)| params.len()),
            Some(MAX_LITERAL_PARAMETERS)
        );
        let past_cap = format!(
            "INSERT INTO t VALUES ({})",
            vec!["1"; MAX_LITERAL_PARAMETERS + 1].join(",")
        );
        assert_eq!(rewritten(&past_cap), None);
    }

    #[test]
    fn offsets_in_the_rewritten_text_map_back_to_the_text_as_written() {
        let original = "INSERT INTO t(a, b) VALUES (12345, 'x''y') ;";
        let statement = parameterize_literal_statement(original).expect("recognized");
        assert_eq!(statement.sql, "INSERT INTO t(a, b) VALUES (?1, ?2) ;");
        let at = |needle: &str, text: &str| {
            u32::try_from(text.find(needle).expect("present")).expect("fits")
        };
        let rewritten = statement.sql.as_str();
        // Before the first parameter nothing moves.
        assert_eq!(
            statement.original_offset(at("VALUES", rewritten)),
            at("VALUES", original)
        );
        // A parameter, and any byte inside it, is the start of its literal.
        assert_eq!(
            statement.original_offset(at("?1", rewritten)),
            at("12345", original)
        );
        assert_eq!(
            statement.original_offset(at("?1", rewritten) + 1),
            at("12345", original)
        );
        assert_eq!(
            statement.original_offset(at("?2", rewritten)),
            at("'x''y'", original)
        );
        // Text after a parameter moves by the lengths replaced before it; an
        // offset handed back unchanged would point into the middle of a literal.
        assert_eq!(
            statement.original_offset(at(", ?2", rewritten)),
            at(", 'x", original)
        );
        assert_eq!(
            statement.original_offset(at(") ;", rewritten)),
            at(") ;", original)
        );
        assert_eq!(
            statement.original_offset(u32::try_from(rewritten.len()).expect("fits")),
            u32::try_from(original.len()).expect("fits")
        );
    }

    /// The scan's answer for `sql` is the lexer-built matcher's, and every
    /// offset in the rewritten text maps back to the byte it was copied from
    /// (or, inside a parameter, to the start of the literal it replaced).
    fn assert_agrees_with_the_lexer(sql: &str, scanned: &ParameterizedLiteralStatement) {
        let reference = lexer_reference::parameterize(sql)
            .unwrap_or_else(|| panic!("the lexer refuses what the scan accepts: {sql}"));
        assert_eq!(
            (
                &scanned.sql,
                &scanned.params,
                &scanned.table,
                &scanned.key_column
            ),
            (
                &reference.sql,
                &reference.params,
                &reference.table,
                &reference.key_column
            ),
            "the scan and the lexer disagree on: {sql}"
        );
        let rewritten = scanned.sql.as_bytes();
        let original = sql.as_bytes();
        for offset in 0..=rewritten.len() {
            let mapped = usize::try_from(
                scanned.original_offset(u32::try_from(offset).expect("fits")),
            )
            .expect("fits");
            let parameter = scanned
                .slots
                .iter()
                .find(|slot| (slot.rewritten_start..slot.rewritten_end).contains(&offset));
            match parameter {
                Some(slot) => assert_eq!(mapped, slot.original_start, "{sql} at {offset}"),
                None => assert_eq!(
                    original.get(mapped),
                    rewritten.get(offset),
                    "{sql} at {offset}"
                ),
            }
        }
    }

    #[test]
    fn the_scan_agrees_with_the_lexer_wherever_it_accepts() {
        // Pieces combined into a few thousand statements: every pairing of a
        // layout with a value spelling, in each statement shape. Whatever the
        // scan accepts, the matcher built on the lexer's own tokens must
        // accept with the same text, the same values and the same names. A
        // scan that read a token differently from the lexer (an identifier
        // boundary, a doubled quote, a sign, a float) would differ here.
        let values = [
            "0",
            "7",
            "-7",
            "+7",
            "- 7",
            "9223372036854775807",
            "-9223372036854775807",
            "9223372036854775808",
            "007",
            "1.5",
            "-0.25",
            "+10.0",
            "0.0",
            "-0.0",
            "123456789.123456789",
            "''",
            "'a'",
            "'it''s'",
            "''''",
            "'semi;colon -- not a comment /* nor this */'",
            "'caf\u{e9} \u{1f600}'",
            "' ? ?1 '",
            "NULL",
            "null",
            "1e3",
            "0x1F",
            ".5",
            "5.",
            "x'00'",
            "1 + 1",
            "a",
            "'a' 'b'",
            "'open",
            "--1",
            "1_000",
            "12ab",
            "1.2.3",
            "TRUE",
        ];
        let layouts: [(&str, &str, &str); 5] = [
            ("INSERT INTO t(a, b) VALUES (", ", ", ")"),
            ("insert into T (a,b) values(", ",", ");"),
            ("  INSERT\tINTO t\nVALUES\n(\n", "\n,\n", "\n)\n;\n"),
            ("UPDATE t SET a = ", ", b = ", " WHERE id = 3"),
            ("UPDATE t SET a = a + 1, b = ", ", c = d - 2.5, e = ", " WHERE id = -3;"),
        ];
        let mut accepted = 0;
        let mut compared = 0;
        for (head, between, tail) in layouts {
            for first in values {
                for second in values {
                    let sql = format!("{head}{first}{between}{second}{tail}");
                    compared += 1;
                    if let Some(scanned) = parameterize_literal_statement(&sql) {
                        accepted += 1;
                        assert_agrees_with_the_lexer(&sql, &scanned);
                    }
                }
            }
        }
        let keys = ["0", "5", "-5", "+5", "- 5", "9223372036854775807", "9223372036854775808", "1.0", "'5'", "5 ", "5;", "5 ;", "5 x", "05", "5e0", "NULL"];
        for head in [
            "SELECT a, b FROM t WHERE id = ",
            "SELECT * FROM t WHERE id=",
            "select a from t where rowid =",
            "DELETE FROM t WHERE id = ",
            "UPDATE t SET a = 'x' WHERE id = ",
        ] {
            for key in keys {
                let sql = format!("{head}{key}");
                compared += 1;
                if let Some(scanned) = parameterize_literal_statement(&sql) {
                    accepted += 1;
                    assert_agrees_with_the_lexer(&sql, &scanned);
                }
            }
        }
        for sql in LEFT_AS_TEXT {
            compared += 1;
            assert!(parameterize_literal_statement(sql).is_none(), "{sql}");
        }
        assert!(
            accepted > 1_000 && compared > 2 * accepted,
            "the comparison covers accepted and refused statements alike: {accepted} of {compared}"
        );
    }
}
