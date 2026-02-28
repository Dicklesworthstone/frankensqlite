// bd-2tu6: §10.1 SQL Lexer
//
// Converts SQL text into a stream of tokens. Uses memchr for accelerated
// string scanning. Tracks line/column for error reporting.

use fsqlite_ast::Span;
use memchr::memchr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::Level;

use crate::token::{Token, TokenKind};

/// Histogram buckets for `fsqlite_tokenize_duration_seconds`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TokenizeDurationSecondsHistogram {
    /// Duration <= 100 µs.
    pub le_100us: u64,
    /// Duration <= 250 µs.
    pub le_250us: u64,
    /// Duration <= 500 µs.
    pub le_500us: u64,
    /// Duration <= 1 ms.
    pub le_1ms: u64,
    /// Duration <= 5 ms.
    pub le_5ms: u64,
    /// Duration > 5 ms.
    pub gt_5ms: u64,
}

/// Point-in-time tokenize metric snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TokenizeMetricsSnapshot {
    /// Monotonic token counter across all tokenize calls.
    pub fsqlite_tokenize_tokens_total: u64,
    /// Histogram buckets for tokenize runtime.
    pub fsqlite_tokenize_duration_seconds: TokenizeDurationSecondsHistogram,
    /// Total tokenize observations recorded in histogram.
    pub fsqlite_tokenize_duration_seconds_count: u64,
    /// Sum of tokenize durations in microseconds.
    pub fsqlite_tokenize_duration_seconds_sum_micros: u64,
}

static FSQLITE_TOKENIZE_TOKENS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FSQLITE_TOKENIZE_DURATION_SECONDS_LE_100US: AtomicU64 = AtomicU64::new(0);
static FSQLITE_TOKENIZE_DURATION_SECONDS_LE_250US: AtomicU64 = AtomicU64::new(0);
static FSQLITE_TOKENIZE_DURATION_SECONDS_LE_500US: AtomicU64 = AtomicU64::new(0);
static FSQLITE_TOKENIZE_DURATION_SECONDS_LE_1MS: AtomicU64 = AtomicU64::new(0);
static FSQLITE_TOKENIZE_DURATION_SECONDS_LE_5MS: AtomicU64 = AtomicU64::new(0);
static FSQLITE_TOKENIZE_DURATION_SECONDS_GT_5MS: AtomicU64 = AtomicU64::new(0);
static FSQLITE_TOKENIZE_DURATION_SECONDS_COUNT: AtomicU64 = AtomicU64::new(0);
static FSQLITE_TOKENIZE_DURATION_SECONDS_SUM_MICROS: AtomicU64 = AtomicU64::new(0);

fn saturating_u64_from_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn saturating_u64_from_u128(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn record_tokenize_metrics(token_count: usize, elapsed_micros: u64) {
    FSQLITE_TOKENIZE_TOKENS_TOTAL
        .fetch_add(saturating_u64_from_usize(token_count), Ordering::Relaxed);
    FSQLITE_TOKENIZE_DURATION_SECONDS_COUNT.fetch_add(1, Ordering::Relaxed);
    FSQLITE_TOKENIZE_DURATION_SECONDS_SUM_MICROS.fetch_add(elapsed_micros, Ordering::Relaxed);

    let bucket = match elapsed_micros {
        0..=100 => &FSQLITE_TOKENIZE_DURATION_SECONDS_LE_100US,
        101..=250 => &FSQLITE_TOKENIZE_DURATION_SECONDS_LE_250US,
        251..=500 => &FSQLITE_TOKENIZE_DURATION_SECONDS_LE_500US,
        501..=1_000 => &FSQLITE_TOKENIZE_DURATION_SECONDS_LE_1MS,
        1_001..=5_000 => &FSQLITE_TOKENIZE_DURATION_SECONDS_LE_5MS,
        _ => &FSQLITE_TOKENIZE_DURATION_SECONDS_GT_5MS,
    };
    bucket.fetch_add(1, Ordering::Relaxed);
}

/// Point-in-time snapshot of tokenize metrics.
#[must_use]
pub fn tokenize_metrics_snapshot() -> TokenizeMetricsSnapshot {
    TokenizeMetricsSnapshot {
        fsqlite_tokenize_tokens_total: FSQLITE_TOKENIZE_TOKENS_TOTAL.load(Ordering::Relaxed),
        fsqlite_tokenize_duration_seconds: TokenizeDurationSecondsHistogram {
            le_100us: FSQLITE_TOKENIZE_DURATION_SECONDS_LE_100US.load(Ordering::Relaxed),
            le_250us: FSQLITE_TOKENIZE_DURATION_SECONDS_LE_250US.load(Ordering::Relaxed),
            le_500us: FSQLITE_TOKENIZE_DURATION_SECONDS_LE_500US.load(Ordering::Relaxed),
            le_1ms: FSQLITE_TOKENIZE_DURATION_SECONDS_LE_1MS.load(Ordering::Relaxed),
            le_5ms: FSQLITE_TOKENIZE_DURATION_SECONDS_LE_5MS.load(Ordering::Relaxed),
            gt_5ms: FSQLITE_TOKENIZE_DURATION_SECONDS_GT_5MS.load(Ordering::Relaxed),
        },
        fsqlite_tokenize_duration_seconds_count: FSQLITE_TOKENIZE_DURATION_SECONDS_COUNT
            .load(Ordering::Relaxed),
        fsqlite_tokenize_duration_seconds_sum_micros: FSQLITE_TOKENIZE_DURATION_SECONDS_SUM_MICROS
            .load(Ordering::Relaxed),
    }
}

/// Reset tokenize metrics (used by tests/diagnostics).
pub fn reset_tokenize_metrics() {
    FSQLITE_TOKENIZE_TOKENS_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_TOKENIZE_DURATION_SECONDS_LE_100US.store(0, Ordering::Relaxed);
    FSQLITE_TOKENIZE_DURATION_SECONDS_LE_250US.store(0, Ordering::Relaxed);
    FSQLITE_TOKENIZE_DURATION_SECONDS_LE_500US.store(0, Ordering::Relaxed);
    FSQLITE_TOKENIZE_DURATION_SECONDS_LE_1MS.store(0, Ordering::Relaxed);
    FSQLITE_TOKENIZE_DURATION_SECONDS_LE_5MS.store(0, Ordering::Relaxed);
    FSQLITE_TOKENIZE_DURATION_SECONDS_GT_5MS.store(0, Ordering::Relaxed);
    FSQLITE_TOKENIZE_DURATION_SECONDS_COUNT.store(0, Ordering::Relaxed);
    FSQLITE_TOKENIZE_DURATION_SECONDS_SUM_MICROS.store(0, Ordering::Relaxed);
}

/// SQL lexer that produces a stream of tokens from source text.
pub struct Lexer<'a> {
    /// The source bytes (UTF-8).
    src: &'a [u8],
    /// Current byte offset into src.
    pos: usize,
    /// Current line number (1-based).
    line: u32,
    /// Current column number (1-based).
    col: u32,
    /// Whether TRACE character-level logging is enabled.
    trace_chars: bool,
}

impl<'a> Lexer<'a> {
    fn log_token(token: &Token) {
        tracing::debug!(
            target: "fsqlite.parse",
            token = ?token.kind,
            start = token.span.start,
            end = token.span.end,
            line = token.line,
            col = token.col,
            "tokenized token"
        );
    }

    /// Create a new lexer for the given SQL source text.
    #[must_use]
    pub fn new(source: &'a str) -> Self {
        Self {
            src: source.as_bytes(),
            pos: 0,
            line: 1,
            col: 1,
            trace_chars: tracing::enabled!(target: "fsqlite.parse", Level::TRACE),
        }
    }

    /// Tokenize the entire input into a Vec of tokens.
    #[must_use]
    pub fn tokenize(source: &'a str) -> Vec<Token> {
        let input_bytes = source.len();
        let span = tracing::span!(
            target: "fsqlite.parse",
            Level::TRACE,
            "tokenize",
            token_count = tracing::field::Empty,
            input_bytes,
            elapsed_us = tracing::field::Empty,
        );
        let _guard = span.enter();
        let started = Instant::now();

        let mut lexer = Self::new(source);
        let mut tokens = Vec::new();
        loop {
            let tok = lexer.next_token();
            let is_eof = tok.kind == TokenKind::Eof;
            tokens.push(tok);
            if is_eof {
                break;
            }
        }

        let elapsed = started.elapsed();
        let elapsed_us = saturating_u64_from_u128(elapsed.as_micros());
        span.record("token_count", saturating_u64_from_usize(tokens.len()));
        span.record("elapsed_us", elapsed_us);
        record_tokenize_metrics(tokens.len(), elapsed_us);
        tokens
    }

    /// Expose tokenize metrics as a snapshot.
    #[must_use]
    pub fn metrics_snapshot() -> TokenizeMetricsSnapshot {
        tokenize_metrics_snapshot()
    }

    /// Reset tokenize metrics.
    pub fn reset_metrics() {
        reset_tokenize_metrics();
    }

    /// Produce the next token.
    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace_and_comments();

        if self.pos >= self.src.len() {
            let token = self.make_token(TokenKind::Eof, self.pos, self.pos);
            Self::log_token(&token);
            return token;
        }

        let start = self.pos;
        let start_line = self.line;
        let start_col = self.col;
        let ch = self.src[self.pos];

        let kind = match ch {
            // String literal (single-quoted)
            b'\'' => self.lex_string(),

            // Double-quoted identifier
            b'"' => self.lex_double_quoted_id(),

            // Backtick-quoted identifier
            b'`' => self.lex_backtick_id(),

            // Bracket-quoted identifier
            b'[' => self.lex_bracket_id(),

            // Blob literal or hex
            b'X' | b'x' if self.peek_at(1) == Some(b'\'') => self.lex_blob(),

            // Numbers
            b'0'..=b'9' => self.lex_number(),
            b'.' if self.peek_at(1).is_some_and(|c| c.is_ascii_digit()) => self.lex_number(),

            // Identifiers and keywords
            b'a'..=b'z' | b'A'..=b'Z' | b'_' | 0x80..=0xFF => self.lex_identifier(),

            // Bind parameters
            b'?' => self.lex_question(),
            b':' => self.lex_colon_param(),
            b'@' => self.lex_at_param(),
            b'$' => self.lex_dollar_param(),

            // Operators and punctuation
            b'+' => {
                self.advance();
                TokenKind::Plus
            }
            b'*' => {
                self.advance();
                TokenKind::Star
            }
            b'/' => {
                self.advance();
                TokenKind::Slash
            }
            b'%' => {
                self.advance();
                TokenKind::Percent
            }
            b'&' => {
                self.advance();
                TokenKind::Ampersand
            }
            b'~' => {
                self.advance();
                TokenKind::Tilde
            }
            b',' => {
                self.advance();
                TokenKind::Comma
            }
            b';' => {
                self.advance();
                TokenKind::Semicolon
            }
            b'(' => {
                self.advance();
                TokenKind::LeftParen
            }
            b')' => {
                self.advance();
                TokenKind::RightParen
            }
            b'.' => {
                self.advance();
                TokenKind::Dot
            }

            // Multi-character operators
            b'-' => self.lex_minus_or_arrow(),
            b'<' => self.lex_lt(),
            b'>' => self.lex_gt(),
            b'=' => self.lex_eq(),
            b'!' => self.lex_bang(),
            b'|' => self.lex_pipe(),

            _ => {
                self.advance();
                let s = String::from_utf8_lossy(&self.src[start..self.pos]).into_owned();
                TokenKind::Error(format!("unexpected character: {s}"))
            }
        };

        let token = Token {
            kind,
            #[allow(clippy::cast_possible_truncation)]
            span: Span::new(start as u32, self.pos as u32),
            line: start_line,
            col: start_col,
        };

        Self::log_token(&token);
        token
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn advance(&mut self) -> u8 {
        let pos = self.pos;
        let line = self.line;
        let col = self.col;
        let ch = self.src[self.pos];
        self.pos += 1;
        if ch == b'\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        if self.trace_chars {
            tracing::trace!(
                target: "fsqlite.parse",
                byte = ch,
                pos,
                line,
                col,
                "tokenize char"
            );
        }
        ch
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<u8> {
        self.src.get(self.pos + offset).copied()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn make_token(&self, kind: TokenKind, start: usize, end: usize) -> Token {
        Token {
            kind,
            span: Span::new(start as u32, end as u32),
            line: self.line,
            col: self.col,
        }
    }

    /// Skip whitespace, line comments (`--`), and block comments (`/* */`).
    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // Skip whitespace
            while self.pos < self.src.len() && self.src[self.pos].is_ascii_whitespace() {
                self.advance();
            }

            if self.pos >= self.src.len() {
                break;
            }

            // Line comment: `-- ...`
            if self.src[self.pos] == b'-' && self.peek_at(1) == Some(b'-') {
                self.advance(); // skip -
                self.advance(); // skip -
                while self.pos < self.src.len() && self.src[self.pos] != b'\n' {
                    self.advance();
                }
                continue;
            }

            // Block comment: `/* ... */` (SQLite does NOT support nesting)
            if self.src[self.pos] == b'/' && self.peek_at(1) == Some(b'*') {
                self.advance(); // skip /
                self.advance(); // skip *
                while self.pos < self.src.len() {
                    if self.src[self.pos] == b'*' && self.peek_at(1) == Some(b'/') {
                        self.advance();
                        self.advance();
                        break;
                    }
                    self.advance();
                }
                continue;
            }

            break;
        }
    }

    // -----------------------------------------------------------------------
    // Literal tokenizers
    // -----------------------------------------------------------------------

    /// Lex a single-quoted string literal. Uses memchr for fast quote search.
    fn lex_string(&mut self) -> TokenKind {
        let start = self.pos;
        self.advance(); // skip opening quote

        let mut value = String::new();
        loop {
            // Use memchr to find the next single quote quickly
            let remaining = &self.src[self.pos..];
            if let Some(offset) = memchr(b'\'', remaining) {
                // Append bytes up to the quote
                value.push_str(&String::from_utf8_lossy(
                    &self.src[self.pos..self.pos + offset],
                ));
                // Advance past the accumulated bytes and the quote
                for _ in 0..offset {
                    self.advance();
                }
                self.advance(); // the quote itself

                // Check for escaped quote ('')
                if self.peek() == Some(b'\'') {
                    value.push('\'');
                    self.advance();
                } else {
                    return TokenKind::String(value);
                }
            } else {
                // Unterminated string
                let rest = String::from_utf8_lossy(&self.src[self.pos..]).into_owned();
                value.push_str(&rest);
                while self.pos < self.src.len() {
                    self.advance();
                }
                return TokenKind::Error(format!(
                    "unterminated string literal starting at byte {}",
                    start
                ));
            }
        }
    }

    /// Lex a double-quoted identifier. Sets the EP_DblQuoted flag.
    fn lex_double_quoted_id(&mut self) -> TokenKind {
        let start = self.pos;
        self.advance(); // skip opening "

        let mut value = String::new();
        loop {
            let remaining = &self.src[self.pos..];
            if let Some(offset) = memchr(b'"', remaining) {
                value.push_str(&String::from_utf8_lossy(
                    &self.src[self.pos..self.pos + offset],
                ));
                for _ in 0..offset {
                    self.advance();
                }
                self.advance(); // the quote

                // Doubled-quote escape: "" -> "
                if self.peek() == Some(b'"') {
                    value.push('"');
                    self.advance();
                } else {
                    return TokenKind::QuotedId(value, true);
                }
            } else {
                while self.pos < self.src.len() {
                    self.advance();
                }
                return TokenKind::Error(format!(
                    "unterminated double-quoted identifier at byte {}",
                    start
                ));
            }
        }
    }

    /// Lex a backtick-quoted identifier.
    fn lex_backtick_id(&mut self) -> TokenKind {
        let start = self.pos;
        self.advance(); // skip `

        let mut value = String::new();
        loop {
            let remaining = &self.src[self.pos..];
            if let Some(offset) = memchr(b'`', remaining) {
                value.push_str(&String::from_utf8_lossy(
                    &self.src[self.pos..self.pos + offset],
                ));
                for _ in 0..offset {
                    self.advance();
                }
                self.advance(); // the backtick

                if self.peek() == Some(b'`') {
                    value.push('`');
                    self.advance();
                } else {
                    return TokenKind::QuotedId(value, false);
                }
            } else {
                while self.pos < self.src.len() {
                    self.advance();
                }
                return TokenKind::Error(format!(
                    "unterminated backtick identifier at byte {}",
                    start
                ));
            }
        }
    }

    /// Lex a bracket-quoted identifier `[name]`.
    fn lex_bracket_id(&mut self) -> TokenKind {
        let start = self.pos;
        self.advance(); // skip [

        let mut value = String::new();
        let remaining = &self.src[self.pos..];
        if let Some(offset) = memchr(b']', remaining) {
            value.push_str(&String::from_utf8_lossy(
                &self.src[self.pos..self.pos + offset],
            ));
            for _ in 0..offset {
                self.advance();
            }
            self.advance(); // skip ]
            TokenKind::QuotedId(value, false)
        } else {
            while self.pos < self.src.len() {
                self.advance();
            }
            TokenKind::Error(format!("unterminated bracket identifier at byte {}", start))
        }
    }

    /// Lex a blob literal `X'...'` / `x'...'`.
    fn lex_blob(&mut self) -> TokenKind {
        let start = self.pos;
        self.advance(); // skip X/x
        self.advance(); // skip '

        let hex_start = self.pos;
        let remaining = &self.src[self.pos..];
        if let Some(offset) = memchr(b'\'', remaining) {
            let hex_bytes = &self.src[hex_start..hex_start + offset];
            for _ in 0..offset {
                self.advance();
            }
            self.advance(); // skip closing '

            // Validate hex content
            if hex_bytes.len() % 2 != 0 {
                return TokenKind::Error(format!(
                    "blob literal has odd number of hex digits at byte {}",
                    start
                ));
            }

            // Work directly on raw bytes to avoid panics from
            // string-slicing multi-byte UTF-8 sequences.
            let mut bytes = Vec::with_capacity(hex_bytes.len() / 2);
            for pair in hex_bytes.chunks_exact(2) {
                let hi = hex_digit(pair[0]);
                let lo = hex_digit(pair[1]);
                match (hi, lo) {
                    (Some(h), Some(l)) => bytes.push((h << 4) | l),
                    _ => {
                        return TokenKind::Error(format!(
                            "invalid hex in blob literal at byte {}",
                            start
                        ));
                    }
                }
            }
            TokenKind::Blob(bytes)
        } else {
            while self.pos < self.src.len() {
                self.advance();
            }
            TokenKind::Error(format!("unterminated blob literal at byte {}", start))
        }
    }

    /// Lex a number: integer, hex integer, or float.
    fn lex_number(&mut self) -> TokenKind {
        let start = self.pos;

        // Check for hex prefix
        if self.src[self.pos] == b'0' && self.peek_at(1).is_some_and(|c| c == b'x' || c == b'X') {
            self.advance(); // 0
            self.advance(); // x
            let hex_start = self.pos;
            while self.pos < self.src.len() && self.src[self.pos].is_ascii_hexdigit() {
                self.advance();
            }
            if self.pos == hex_start {
                return TokenKind::Error("empty hex literal".to_owned());
            }
            let hex_str = String::from_utf8_lossy(&self.src[hex_start..self.pos]);
            // Strip leading zeros then check significant digit count,
            // matching C SQLite's sqlite3DecOrHexToI64 which rejects
            // hex literals with >16 significant digits.
            let significant = hex_str.trim_start_matches('0');
            if significant.len() > 16 {
                return TokenKind::Error(format!("hex literal out of range at byte {start}"));
            }
            let parse_str = if significant.is_empty() {
                "0"
            } else {
                significant
            };
            // Parse as u64 and bitwise-cast to i64 — matching C SQLite's
            // sqlite3DecOrHexToI64 which uses memcpy(pOut, &u, 8).
            return match u64::from_str_radix(parse_str, 16) {
                Ok(v) => {
                    #[allow(clippy::cast_possible_wrap)]
                    let i = v as i64;
                    TokenKind::Integer(i)
                }
                Err(_) => TokenKind::Error(format!("hex literal out of range at byte {start}")),
            };
        }

        // Decimal integer or float
        let mut is_float = false;

        // Integer part (may be empty for `.5` style)
        while self.pos < self.src.len() && self.src[self.pos].is_ascii_digit() {
            self.advance();
        }

        // Helper to check if the current position (+ offset) starts a valid exponent.
        let is_valid_exponent = |lexer: &Self, mut offset: usize| -> bool {
            if let Some(c) = lexer.peek_at(offset) {
                if c == b'e' || c == b'E' {
                    offset += 1;
                    if let Some(s) = lexer.peek_at(offset) {
                        if s == b'+' || s == b'-' {
                            offset += 1;
                        }
                    }
                    if let Some(d) = lexer.peek_at(offset) {
                        return d.is_ascii_digit();
                    }
                }
            }
            false
        };

        // Fractional part
        if self.pos < self.src.len()
            && self.src[self.pos] == b'.'
            && (self.peek_at(1).is_some_and(|c| c.is_ascii_digit()) || is_valid_exponent(self, 1))
        {
            is_float = true;
            self.advance(); // skip dot
            while self.pos < self.src.len() && self.src[self.pos].is_ascii_digit() {
                self.advance();
            }
        } else if self.pos < self.src.len()
            && self.src[self.pos] == b'.'
            && start < self.pos // we had digits before the dot
            && !self.peek_at(1).is_some_and(|c| c.is_ascii_alphanumeric() || c == b'_')
        {
            // e.g. `123.` with nothing meaningful after -- still a float
            is_float = true;
            self.advance(); // skip dot
        }

        // Handle case where input starts with '.'
        if self.src[start] == b'.' {
            is_float = true;
        }

        // Exponent
        if is_valid_exponent(self, 0) {
            is_float = true;
            self.advance(); // skip e/E
            if self.pos < self.src.len()
                && (self.src[self.pos] == b'+' || self.src[self.pos] == b'-')
            {
                self.advance();
            }
            while self.pos < self.src.len() && self.src[self.pos].is_ascii_digit() {
                self.advance();
            }
        }

        let text = String::from_utf8_lossy(&self.src[start..self.pos]);
        if is_float {
            match text.parse::<f64>() {
                Ok(v) => TokenKind::Float(v),
                Err(_) => TokenKind::Error(format!("invalid float: {text}")),
            }
        } else {
            match text.parse::<i64>() {
                Ok(v) => TokenKind::Integer(v),
                Err(_) => {
                    // SQLite promotes oversized integers to REAL.
                    match text.parse::<f64>() {
                        Ok(v) => TokenKind::Float(v),
                        Err(_) => TokenKind::Error(format!("integer out of range: {text}")),
                    }
                }
            }
        }
    }

    /// Lex an identifier or keyword.
    fn lex_identifier(&mut self) -> TokenKind {
        let start = self.pos;
        self.advance(); // first character already validated

        while self.pos < self.src.len() {
            let ch = self.src[self.pos];
            if ch.is_ascii_alphanumeric() || ch == b'_' || ch >= 0x80 {
                self.advance();
            } else {
                break;
            }
        }

        let text = String::from_utf8_lossy(&self.src[start..self.pos]).into_owned();

        // Check for keyword
        if let Some(kw) = TokenKind::lookup_keyword(&text) {
            kw
        } else {
            TokenKind::Id(text)
        }
    }

    /// Lex `?` or `?NNN`.
    fn lex_question(&mut self) -> TokenKind {
        self.advance(); // skip ?
        if self.pos < self.src.len() && self.src[self.pos].is_ascii_digit() {
            let num_start = self.pos;
            while self.pos < self.src.len() && self.src[self.pos].is_ascii_digit() {
                self.advance();
            }
            let text = String::from_utf8_lossy(&self.src[num_start..self.pos]);
            match text.parse::<u32>() {
                Ok(n) => TokenKind::QuestionNum(n),
                Err(_) => TokenKind::Error("invalid parameter number".to_owned()),
            }
        } else {
            TokenKind::Question
        }
    }

    /// Lex `:name`.
    fn lex_colon_param(&mut self) -> TokenKind {
        self.advance(); // skip :
        let name_start = self.pos;
        while self.pos < self.src.len() {
            let ch = self.src[self.pos];
            if ch.is_ascii_alphanumeric() || ch == b'_' || ch >= 0x80 {
                self.advance();
            } else {
                break;
            }
        }
        if self.pos == name_start {
            return TokenKind::Error("empty parameter name after ':'".to_owned());
        }
        let name = String::from_utf8_lossy(&self.src[name_start..self.pos]).into_owned();
        TokenKind::ColonParam(name)
    }

    /// Lex `@name`.
    fn lex_at_param(&mut self) -> TokenKind {
        self.advance(); // skip @
        let name_start = self.pos;
        while self.pos < self.src.len() {
            let ch = self.src[self.pos];
            if ch.is_ascii_alphanumeric() || ch == b'_' || ch >= 0x80 {
                self.advance();
            } else {
                break;
            }
        }
        if self.pos == name_start {
            return TokenKind::Error("empty parameter name after '@'".to_owned());
        }
        let name = String::from_utf8_lossy(&self.src[name_start..self.pos]).into_owned();
        TokenKind::AtParam(name)
    }

    /// Lex `$name`.
    fn lex_dollar_param(&mut self) -> TokenKind {
        self.advance(); // skip $
        let name_start = self.pos;
        while self.pos < self.src.len() {
            let ch = self.src[self.pos];
            if ch.is_ascii_alphanumeric() || ch == b'_' || ch >= 0x80 {
                self.advance();
            } else {
                break;
            }
        }
        if self.pos == name_start {
            return TokenKind::Error("empty parameter name after '$'".to_owned());
        }
        let name = String::from_utf8_lossy(&self.src[name_start..self.pos]).into_owned();
        TokenKind::DollarParam(name)
    }

    // -----------------------------------------------------------------------
    // Multi-character operator tokenizers
    // -----------------------------------------------------------------------

    /// Lex `-`, `->`, or `->>`.
    fn lex_minus_or_arrow(&mut self) -> TokenKind {
        self.advance(); // skip -
        if self.peek() == Some(b'>') {
            self.advance(); // skip >
            if self.peek() == Some(b'>') {
                self.advance(); // skip >
                TokenKind::DoubleArrow
            } else {
                TokenKind::Arrow
            }
        } else {
            TokenKind::Minus
        }
    }

    /// Lex `<`, `<=`, `<>`, or `<<`.
    fn lex_lt(&mut self) -> TokenKind {
        self.advance(); // skip <
        match self.peek() {
            Some(b'=') => {
                self.advance();
                TokenKind::Le
            }
            Some(b'>') => {
                self.advance();
                TokenKind::LtGt
            }
            Some(b'<') => {
                self.advance();
                TokenKind::ShiftLeft
            }
            _ => TokenKind::Lt,
        }
    }

    /// Lex `>`, `>=`, or `>>`.
    fn lex_gt(&mut self) -> TokenKind {
        self.advance(); // skip >
        match self.peek() {
            Some(b'=') => {
                self.advance();
                TokenKind::Ge
            }
            Some(b'>') => {
                self.advance();
                TokenKind::ShiftRight
            }
            _ => TokenKind::Gt,
        }
    }

    /// Lex `=` or `==`.
    fn lex_eq(&mut self) -> TokenKind {
        self.advance(); // skip =
        if self.peek() == Some(b'=') {
            self.advance();
            TokenKind::EqEq
        } else {
            TokenKind::Eq
        }
    }

    /// Lex `!=`.
    fn lex_bang(&mut self) -> TokenKind {
        self.advance(); // skip !
        if self.peek() == Some(b'=') {
            self.advance();
            TokenKind::Ne
        } else {
            TokenKind::Error("unexpected '!', did you mean '!='?".to_owned())
        }
    }

    /// Lex `|` or `||`.
    fn lex_pipe(&mut self) -> TokenKind {
        self.advance(); // skip |
        if self.peek() == Some(b'|') {
            self.advance();
            TokenKind::Concat
        } else {
            TokenKind::Pipe
        }
    }
}

/// Convert an ASCII hex digit byte to its numeric value (0-15).
/// Returns `None` for non-hex bytes.
const fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(src: &str) -> Vec<Token> {
        Lexer::tokenize(src)
    }

    fn kinds(src: &str) -> Vec<TokenKind> {
        lex(src).into_iter().map(|t| t.kind).collect()
    }

    #[test]
    fn test_lex_integer_literals() {
        let tokens = kinds("42 0 0xFF");
        assert_eq!(
            tokens,
            vec![
                TokenKind::Integer(42),
                TokenKind::Integer(0),
                TokenKind::Integer(255),
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn test_lex_float_literals() {
        let tokens = kinds("3.14 1e10 .5 1.0e-3 0.0");
        // Avoid clippy::approx_constant (3.14 is interpreted as an approximation of PI),
        // but keep the test input string stable.
        let expected = 3.0 + 0.14;
        assert!(matches!(
            tokens[0],
            TokenKind::Float(v) if (v - expected).abs() < 1e-10
        ));
        assert!(matches!(tokens[1], TokenKind::Float(v) if (v - 1e10).abs() < 1.0));
        assert!(matches!(tokens[2], TokenKind::Float(v) if (v - 0.5).abs() < 1e-10));
        assert!(matches!(tokens[3], TokenKind::Float(v) if (v - 0.001).abs() < 1e-10));
        assert!(matches!(tokens[4], TokenKind::Float(v) if v.abs() < 1e-10));
        assert_eq!(tokens[5], TokenKind::Eof);
    }

    #[test]
    fn test_lex_string_literals() {
        let tokens = kinds("'hello' 'it''s' ''");
        assert_eq!(tokens[0], TokenKind::String("hello".to_owned()));
        assert_eq!(tokens[1], TokenKind::String("it's".to_owned()));
        assert_eq!(tokens[2], TokenKind::String(String::new()));
        assert_eq!(tokens[3], TokenKind::Eof);
    }

    #[test]
    fn test_lex_blob_literals() {
        let tokens = kinds("X'CAFE' x'00ff' X''");
        assert_eq!(tokens[0], TokenKind::Blob(vec![0xCA, 0xFE]));
        assert_eq!(tokens[1], TokenKind::Blob(vec![0x00, 0xFF]));
        assert_eq!(tokens[2], TokenKind::Blob(vec![]));
        assert_eq!(tokens[3], TokenKind::Eof);
    }

    #[test]
    fn test_lex_blob_odd_hex_error() {
        let tokens = kinds("X'CAF'");
        assert!(matches!(tokens[0], TokenKind::Error(_)));
    }

    #[test]
    fn test_lex_blob_non_ascii_no_panic() {
        // bd-20gf regression: multi-byte UTF-8 inside a blob literal must
        // produce an error, not panic on string-slice boundary.
        let tokens = kinds("X'U\u{05fc} '");
        assert!(matches!(tokens[0], TokenKind::Error(_)));

        // Also test with raw non-hex ASCII chars.
        let tokens2 = kinds("X'GG'");
        assert!(matches!(tokens2[0], TokenKind::Error(_)));
    }

    #[test]
    fn test_lex_variables() {
        let tokens = kinds("?1 :name @param $var ?");
        assert_eq!(tokens[0], TokenKind::QuestionNum(1));
        assert_eq!(tokens[1], TokenKind::ColonParam("name".to_owned()));
        assert_eq!(tokens[2], TokenKind::AtParam("param".to_owned()));
        assert_eq!(tokens[3], TokenKind::DollarParam("var".to_owned()));
        assert_eq!(tokens[4], TokenKind::Question);
        assert_eq!(tokens[5], TokenKind::Eof);
    }

    #[test]
    fn test_lex_quoted_identifiers() {
        let tokens = kinds("\"table_name\" [column] `backtick`");
        assert_eq!(
            tokens[0],
            TokenKind::QuotedId("table_name".to_owned(), true)
        );
        assert_eq!(tokens[1], TokenKind::QuotedId("column".to_owned(), false));
        assert_eq!(tokens[2], TokenKind::QuotedId("backtick".to_owned(), false));
    }

    #[test]
    fn test_lex_dqs_flag() {
        let tokens = kinds("\"hello\"");
        // Double-quoted strings produce QuotedId with EP_DblQuoted=true
        assert_eq!(tokens[0], TokenKind::QuotedId("hello".to_owned(), true));
    }

    #[test]
    fn test_lex_keywords() {
        let tokens = kinds("SELECT FROM WHERE INSERT CREATE TABLE CONCURRENT");
        assert_eq!(tokens[0], TokenKind::KwSelect);
        assert_eq!(tokens[1], TokenKind::KwFrom);
        assert_eq!(tokens[2], TokenKind::KwWhere);
        assert_eq!(tokens[3], TokenKind::KwInsert);
        assert_eq!(tokens[4], TokenKind::KwCreate);
        assert_eq!(tokens[5], TokenKind::KwTable);
        assert_eq!(tokens[6], TokenKind::KwConcurrent);

        // Case insensitivity
        let tokens2 = kinds("select from where");
        assert_eq!(tokens2[0], TokenKind::KwSelect);
        assert_eq!(tokens2[1], TokenKind::KwFrom);
        assert_eq!(tokens2[2], TokenKind::KwWhere);
    }

    #[test]
    fn test_lex_operators() {
        let tokens = kinds("+ - * / % & | ~ << >> = < <= > >= == != <> || -> ->>");
        let expected = vec![
            TokenKind::Plus,
            TokenKind::Minus,
            TokenKind::Star,
            TokenKind::Slash,
            TokenKind::Percent,
            TokenKind::Ampersand,
            TokenKind::Pipe,
            TokenKind::Tilde,
            TokenKind::ShiftLeft,
            TokenKind::ShiftRight,
            TokenKind::Eq,
            TokenKind::Lt,
            TokenKind::Le,
            TokenKind::Gt,
            TokenKind::Ge,
            TokenKind::EqEq,
            TokenKind::Ne,
            TokenKind::LtGt,
            TokenKind::Concat,
            TokenKind::Arrow,
            TokenKind::DoubleArrow,
            TokenKind::Eof,
        ];
        assert_eq!(tokens, expected);
    }

    #[test]
    fn test_lex_eq_vs_eqeq() {
        let tokens = kinds("= ==");
        assert_eq!(tokens[0], TokenKind::Eq);
        assert_eq!(tokens[1], TokenKind::EqEq);
    }

    #[test]
    fn test_lex_ne_vs_ltgt() {
        let tokens = kinds("!= <>");
        assert_eq!(tokens[0], TokenKind::Ne);
        assert_eq!(tokens[1], TokenKind::LtGt);
    }

    #[test]
    fn test_lex_error_unterminated_string() {
        let tokens = kinds("'hello");
        assert!(matches!(tokens[0], TokenKind::Error(_)));
    }

    #[test]
    fn test_lex_line_column_tracking() {
        let tokens = lex("SELECT\n  a,\n  b");
        assert_eq!(tokens[0].line, 1);
        assert_eq!(tokens[0].col, 1);
        // 'a' is on line 2, col 3
        assert_eq!(tokens[1].line, 2);
        assert_eq!(tokens[1].col, 3);
        // ',' is on line 2, col 4
        assert_eq!(tokens[2].line, 2);
        assert_eq!(tokens[2].col, 4);
        // 'b' is on line 3, col 3
        assert_eq!(tokens[3].line, 3);
        assert_eq!(tokens[3].col, 3);
    }

    #[test]
    fn test_lex_whitespace_and_comments_skipped() {
        let tokens = kinds("SELECT -- this is a comment\n  a /* block */ FROM b");
        assert_eq!(tokens[0], TokenKind::KwSelect);
        assert_eq!(tokens[1], TokenKind::Id("a".to_owned()));
        assert_eq!(tokens[2], TokenKind::KwFrom);
        assert_eq!(tokens[3], TokenKind::Id("b".to_owned()));
        assert_eq!(tokens[4], TokenKind::Eof);
    }

    #[test]
    fn test_lex_hex_large_values() {
        // C SQLite parses hex as u64 and memcpy to i64.
        // 0xFFFFFFFFFFFFFFFF = u64::MAX → i64 -1.
        let tokens = kinds("0xFFFFFFFFFFFFFFFF");
        assert_eq!(tokens[0], TokenKind::Integer(-1));

        // 0x8000000000000000 = i64::MIN.
        let tokens = kinds("0x8000000000000000");
        assert_eq!(tokens[0], TokenKind::Integer(i64::MIN));

        // 0x7FFFFFFFFFFFFFFF = i64::MAX.
        let tokens = kinds("0x7FFFFFFFFFFFFFFF");
        assert_eq!(tokens[0], TokenKind::Integer(i64::MAX));
    }

    #[test]
    fn test_lex_hex_overflow_17_digits_rejects() {
        // 0x10000000000000000 has 17 significant hex digits → must error,
        // not silently truncate to 0.
        let tokens = kinds("0x10000000000000000");
        assert!(
            matches!(&tokens[0], TokenKind::Error(msg) if msg.contains("out of range")),
            "expected error for 17-digit hex, got {:?}",
            tokens[0]
        );
    }

    #[test]
    fn test_lex_hex_leading_zeros_accepted() {
        // Leading zeros are stripped before the length check, so
        // 0x00000000000000001 (17 chars, 1 significant) is valid.
        let tokens = kinds("0x00000000000000001");
        assert_eq!(tokens[0], TokenKind::Integer(1));
    }

    fn histogram_total(hist: &TokenizeDurationSecondsHistogram) -> u64 {
        hist.le_100us + hist.le_250us + hist.le_500us + hist.le_1ms + hist.le_5ms + hist.gt_5ms
    }

    #[test]
    fn test_tokenize_metrics_accumulate_tokens_and_histogram_samples() {
        reset_tokenize_metrics();

        let first = lex("SELECT 1;");
        let second = lex("SELECT 2;");

        let expected_total_tokens =
            u64::try_from(first.len() + second.len()).expect("small token vectors should fit");
        let snap = tokenize_metrics_snapshot();
        assert_eq!(snap.fsqlite_tokenize_tokens_total, expected_total_tokens);
        assert_eq!(snap.fsqlite_tokenize_duration_seconds_count, 2);
        assert_eq!(
            histogram_total(&snap.fsqlite_tokenize_duration_seconds),
            snap.fsqlite_tokenize_duration_seconds_count
        );
    }

    #[test]
    fn test_tokenize_metrics_reset_clears_all_fields() {
        reset_tokenize_metrics();
        let _ = lex("SELECT 42;");

        let before = tokenize_metrics_snapshot();
        assert!(before.fsqlite_tokenize_tokens_total > 0);
        assert!(before.fsqlite_tokenize_duration_seconds_count > 0);

        reset_tokenize_metrics();
        let after = tokenize_metrics_snapshot();
        assert_eq!(after.fsqlite_tokenize_tokens_total, 0);
        assert_eq!(after.fsqlite_tokenize_duration_seconds_count, 0);
        assert_eq!(after.fsqlite_tokenize_duration_seconds_sum_micros, 0);
        assert_eq!(histogram_total(&after.fsqlite_tokenize_duration_seconds), 0);
    }
}
