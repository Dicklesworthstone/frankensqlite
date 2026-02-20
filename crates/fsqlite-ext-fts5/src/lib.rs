//! FTS5 full-text search extension (§14.2).
//!
//! Provides: tokenizer API (unicode61, ascii, porter, trigram), inverted index,
//! boolean query parsing (implicit AND, OR, NOT binary-only, phrase, prefix,
//! NEAR, column filter, caret), BM25 ranking, FTS5 virtual table with content
//! modes, and secure-delete / contentless-delete configuration.

use std::collections::HashMap;

use fsqlite_error::{FrankenError, Result};
use fsqlite_func::ScalarFunction;
use fsqlite_func::vtab::{ColumnContext, IndexInfo, VirtualTable, VirtualTableCursor};
use fsqlite_types::SqliteValue;
use fsqlite_types::cx::Cx;
use tracing::debug;

// ---------------------------------------------------------------------------
// Extension name
// ---------------------------------------------------------------------------

#[must_use]
pub const fn extension_name() -> &'static str {
    "fts5"
}

// ---------------------------------------------------------------------------
// Configuration (existing + expanded)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentMode {
    Stored,
    Contentless,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteAction {
    Reject,
    Tombstone,
    PhysicalPurge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
pub struct Fts5Config {
    secure_delete: bool,
    content_mode: ContentMode,
    contentless_delete: bool,
}

impl Fts5Config {
    #[must_use]
    pub const fn new(content_mode: ContentMode) -> Self {
        Self {
            secure_delete: false,
            content_mode,
            contentless_delete: false,
        }
    }

    #[must_use]
    pub const fn secure_delete_enabled(self) -> bool {
        self.secure_delete
    }

    #[must_use]
    pub const fn contentless_delete_enabled(self) -> bool {
        self.contentless_delete
    }

    #[must_use]
    pub const fn content_mode(self) -> ContentMode {
        self.content_mode
    }

    #[must_use]
    pub const fn delete_action(self) -> DeleteAction {
        match self.content_mode {
            ContentMode::Stored => {
                if self.secure_delete {
                    DeleteAction::PhysicalPurge
                } else {
                    DeleteAction::Tombstone
                }
            }
            ContentMode::Contentless => {
                if !self.contentless_delete {
                    DeleteAction::Reject
                } else if self.secure_delete {
                    DeleteAction::PhysicalPurge
                } else {
                    DeleteAction::Tombstone
                }
            }
        }
    }

    pub fn apply_control_command(&mut self, command: &str) -> bool {
        let trimmed = command.trim();
        let Some((raw_key, raw_value)) = trimmed.split_once('=') else {
            return false;
        };

        let key = raw_key.trim().to_ascii_lowercase();
        let Some(value) = parse_bool_like(raw_value) else {
            return false;
        };

        match key.as_str() {
            "secure-delete" | "secure_delete" => {
                self.secure_delete = value;
                true
            }
            "contentless_delete" => {
                self.contentless_delete = value;
                true
            }
            _ => false,
        }
    }
}

impl Default for Fts5Config {
    fn default() -> Self {
        Self::new(ContentMode::Stored)
    }
}

fn parse_bool_like(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "on" | "true" => Some(true),
        "0" | "off" | "false" => Some(false),
        _ => None,
    }
}

fn parse_option_assignment(input: &str) -> Option<(&str, &str)> {
    let (key, value) = input.split_once('=')?;
    Some((key.trim(), value.trim()))
}

fn unquote_fts_arg(value: &str) -> &str {
    let trimmed = value.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        let first = bytes[0];
        let last = bytes[trimmed.len() - 1];
        if (first == b'\'' && last == b'\'')
            || (first == b'"' && last == b'"')
            || (first == b'`' && last == b'`')
        {
            return &trimmed[1..trimmed.len() - 1];
        }
    }
    trimmed
}

// ---------------------------------------------------------------------------
// Tokenizer API
// ---------------------------------------------------------------------------

/// A single token produced by an FTS5 tokenizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5Token {
    /// The normalized term (lowercased, stemmed, etc.).
    pub term: String,
    /// Byte offset of the start of this token in the original text.
    pub start: usize,
    /// Byte offset of the end of this token in the original text.
    pub end: usize,
    /// Whether this token is colocated with the previous one (synonym).
    pub colocated: bool,
}

/// Trait for FTS5 tokenizers.
pub trait Fts5Tokenizer: Send + Sync {
    /// Return the tokenizer name.
    fn name(&self) -> &'static str;

    /// Tokenize the input text, producing a list of tokens.
    fn tokenize(&self, text: &str) -> Vec<Fts5Token>;
}

/// Unicode61 tokenizer: splits on non-alphanumeric characters, lowercases.
#[derive(Debug, Default)]
pub struct Unicode61Tokenizer {
    /// Characters to treat as separators (empty = default Unicode categories).
    pub separators: String,
    /// Characters to treat as token characters (override default).
    pub token_chars: String,
    /// Whether to remove diacritics (0=no, 1=non-ASCII, 2=all).
    pub remove_diacritics: u8,
}

impl Unicode61Tokenizer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn is_token_char(&self, ch: char) -> bool {
        if !self.token_chars.is_empty() && self.token_chars.contains(ch) {
            return true;
        }
        if !self.separators.is_empty() && self.separators.contains(ch) {
            return false;
        }
        ch.is_alphanumeric()
    }
}

impl Fts5Tokenizer for Unicode61Tokenizer {
    fn name(&self) -> &'static str {
        "unicode61"
    }

    fn tokenize(&self, text: &str) -> Vec<Fts5Token> {
        let mut tokens = Vec::new();
        let mut token_start = None;
        let mut current_term = String::new();

        for (byte_idx, ch) in text.char_indices() {
            if self.is_token_char(ch) {
                if token_start.is_none() {
                    token_start = Some(byte_idx);
                    current_term.clear();
                }
                for lc in ch.to_lowercase() {
                    current_term.push(lc);
                }
            } else if let Some(start) = token_start.take() {
                if !current_term.is_empty() {
                    tokens.push(Fts5Token {
                        term: current_term.clone(),
                        start,
                        end: byte_idx,
                        colocated: false,
                    });
                }
            }
        }

        // Flush trailing token.
        if let Some(start) = token_start {
            if !current_term.is_empty() {
                tokens.push(Fts5Token {
                    term: current_term,
                    start,
                    end: text.len(),
                    colocated: false,
                });
            }
        }

        tokens
    }
}

/// ASCII tokenizer: like unicode61 but only ASCII alphanumeric characters.
#[derive(Debug, Default)]
pub struct AsciiTokenizer;

impl Fts5Tokenizer for AsciiTokenizer {
    fn name(&self) -> &'static str {
        "ascii"
    }

    fn tokenize(&self, text: &str) -> Vec<Fts5Token> {
        let mut tokens = Vec::new();
        let mut token_start = None;
        let mut current_term = String::new();

        for (byte_idx, ch) in text.char_indices() {
            if ch.is_ascii_alphanumeric() {
                if token_start.is_none() {
                    token_start = Some(byte_idx);
                    current_term.clear();
                }
                current_term.push(ch.to_ascii_lowercase());
            } else if let Some(start) = token_start.take() {
                if !current_term.is_empty() {
                    tokens.push(Fts5Token {
                        term: current_term.clone(),
                        start,
                        end: byte_idx,
                        colocated: false,
                    });
                }
            }
        }

        if let Some(start) = token_start {
            if !current_term.is_empty() {
                tokens.push(Fts5Token {
                    term: current_term,
                    start,
                    end: text.len(),
                    colocated: false,
                });
            }
        }

        tokens
    }
}

/// Porter stemmer tokenizer: wraps another tokenizer and applies Porter
/// stemming to each term.
pub struct PorterTokenizer {
    inner: Box<dyn Fts5Tokenizer>,
}

impl std::fmt::Debug for PorterTokenizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PorterTokenizer")
            .field("inner", &self.inner.name())
            .finish()
    }
}

impl PorterTokenizer {
    pub fn new(inner: Box<dyn Fts5Tokenizer>) -> Self {
        Self { inner }
    }
}

impl Fts5Tokenizer for PorterTokenizer {
    fn name(&self) -> &'static str {
        "porter"
    }

    fn tokenize(&self, text: &str) -> Vec<Fts5Token> {
        let mut tokens = self.inner.tokenize(text);
        for token in &mut tokens {
            token.term = porter_stem(&token.term);
        }
        tokens
    }
}

/// Simplified Porter stemmer (covers common English suffixes).
fn porter_stem(word: &str) -> String {
    let mut s = word.to_owned();

    // Step 1a: plurals
    if let Some(base) = s.strip_suffix("sses") {
        s = format!("{base}ss");
    } else if let Some(base) = s.strip_suffix("ies") {
        s = format!("{base}i");
    } else if s.ends_with('s') && !s.ends_with("ss") && s.len() > 3 {
        s.pop();
    }

    // Step 1b: -ed, -ing
    if let Some(base) = s.strip_suffix("eed") {
        if base.len() > 1 {
            s = format!("{base}ee");
        }
    } else if let Some(base) = s.strip_suffix("ed") {
        if contains_vowel(base) {
            s = base.to_owned();
            step1b_fixup(&mut s);
        }
    } else if let Some(base) = s.strip_suffix("ing") {
        if contains_vowel(base) {
            s = base.to_owned();
            step1b_fixup(&mut s);
        }
    }

    // Step 1c: terminal y -> i if stem contains vowel
    if s.ends_with('y') && s.len() > 2 && contains_vowel(&s[..s.len() - 1]) {
        s.pop();
        s.push('i');
    }

    // Step 2: double-suffix removal (common cases)
    apply_step2(&mut s);

    // Step 3: more suffixes
    apply_step3(&mut s);

    s
}

fn contains_vowel(s: &str) -> bool {
    s.chars().any(|c| matches!(c, 'a' | 'e' | 'i' | 'o' | 'u'))
}

fn step1b_fixup(s: &mut String) {
    if s.ends_with("at") || s.ends_with("bl") || s.ends_with("iz") {
        s.push('e');
    } else if s.len() >= 2 {
        let bytes = s.as_bytes();
        let last = bytes[bytes.len() - 1];
        let prev = bytes[bytes.len() - 2];
        if last == prev && !matches!(last, b'l' | b's' | b'z') {
            s.pop();
        }
    }
}

fn apply_step2(s: &mut String) {
    let replacements: &[(&str, &str)] = &[
        ("ational", "ate"),
        ("tional", "tion"),
        ("enci", "ence"),
        ("anci", "ance"),
        ("izer", "ize"),
        ("alism", "al"),
        ("ation", "ate"),
        ("ator", "ate"),
        ("aliti", "al"),
        ("iviti", "ive"),
        ("ousli", "ous"),
        ("biliti", "ble"),
        ("logi", "log"),
    ];

    for (suffix, replacement) in replacements {
        if let Some(base) = s.strip_suffix(suffix) {
            if measure(base) > 0 {
                *s = format!("{base}{replacement}");
                return;
            }
        }
    }
}

fn apply_step3(s: &mut String) {
    let replacements: &[(&str, &str)] = &[
        ("icate", "ic"),
        ("ative", ""),
        ("alize", "al"),
        ("iciti", "ic"),
        ("ical", "ic"),
        ("ful", ""),
        ("ness", ""),
    ];

    for (suffix, replacement) in replacements {
        if let Some(base) = s.strip_suffix(suffix) {
            if measure(base) > 0 {
                *s = format!("{base}{replacement}");
                return;
            }
        }
    }
}

/// Compute the "measure" m of a stem (number of VC sequences).
fn measure(s: &str) -> u32 {
    let mut m = 0u32;
    let mut in_vowel_seq = false;

    for ch in s.chars() {
        let is_vowel = matches!(ch, 'a' | 'e' | 'i' | 'o' | 'u');
        if is_vowel {
            in_vowel_seq = true;
        } else if in_vowel_seq {
            m += 1;
            in_vowel_seq = false;
        }
    }

    m
}

/// Trigram tokenizer: generates all 3-character substrings of the input.
#[derive(Debug, Default)]
pub struct TrigramTokenizer {
    /// Whether to also remove diacritics before generating trigrams.
    pub case_sensitive: bool,
}

impl Fts5Tokenizer for TrigramTokenizer {
    fn name(&self) -> &'static str {
        "trigram"
    }

    fn tokenize(&self, text: &str) -> Vec<Fts5Token> {
        let chars: Vec<(usize, char)> = text.char_indices().collect();
        if chars.len() < 3 {
            return Vec::new();
        }

        let mut tokens = Vec::new();
        for window in chars.windows(3) {
            let start = window[0].0;
            let end_char = window[2];
            let end = end_char.0 + end_char.1.len_utf8();
            let term: String = if self.case_sensitive {
                window.iter().map(|(_, c)| *c).collect()
            } else {
                window.iter().flat_map(|(_, c)| c.to_lowercase()).collect()
            };
            tokens.push(Fts5Token {
                term,
                start,
                end,
                colocated: false,
            });
        }
        tokens
    }
}

/// Create a tokenizer by name with optional arguments.
#[must_use]
pub fn create_tokenizer(name: &str) -> Option<Box<dyn Fts5Tokenizer>> {
    match name.to_ascii_lowercase().as_str() {
        "unicode61" => Some(Box::new(Unicode61Tokenizer::new())),
        "ascii" => Some(Box::new(AsciiTokenizer)),
        "porter" => Some(Box::new(PorterTokenizer::new(Box::new(
            Unicode61Tokenizer::new(),
        )))),
        "trigram" => Some(Box::new(TrigramTokenizer::default())),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Query parsing
// ---------------------------------------------------------------------------

/// Token kinds in an FTS5 query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fts5QueryTokenKind {
    Term,
    Phrase,
    And,
    Or,
    Not,
    Near,
    LParen,
    RParen,
    ColumnFilter,
    Prefix,
    Caret,
}

/// A token in an FTS5 query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5QueryToken {
    pub kind: Fts5QueryTokenKind,
    pub lexeme: String,
}

/// FTS5 query validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fts5QueryError {
    EmptyQuery,
    UnclosedPhrase,
    UnbalancedParentheses,
    UnaryNotForbidden,
    InvalidColumnFilter(String),
    InvalidNearSyntax,
}

impl std::fmt::Display for Fts5QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyQuery => write!(f, "empty FTS5 query"),
            Self::UnclosedPhrase => write!(f, "unclosed phrase literal"),
            Self::UnbalancedParentheses => write!(f, "unbalanced parentheses"),
            Self::UnaryNotForbidden => {
                write!(f, "FTS5 NOT is binary-only; unary NOT is not allowed")
            }
            Self::InvalidColumnFilter(col) => write!(f, "invalid column filter: {col}"),
            Self::InvalidNearSyntax => write!(f, "invalid NEAR syntax"),
        }
    }
}

/// Parse an FTS5 query string into tokens.
///
/// FTS5 uses implicit AND (two adjacent terms are ANDed together).
/// NOT is binary-only (requires a left operand).
pub fn parse_fts5_query(query: &str) -> std::result::Result<Vec<Fts5QueryToken>, Fts5QueryError> {
    let tokens = tokenize_fts5_query(query)?;
    validate_fts5_parentheses(&tokens)?;
    validate_fts5_not_binary(&tokens)?;
    Ok(insert_implicit_and(&tokens))
}

fn tokenize_fts5_query(query: &str) -> std::result::Result<Vec<Fts5QueryToken>, Fts5QueryError> {
    let mut chars = query.chars().peekable();
    let mut tokens = Vec::new();

    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_whitespace() {
            let _ = chars.next();
            continue;
        }

        if ch == '(' {
            let _ = chars.next();
            tokens.push(Fts5QueryToken {
                kind: Fts5QueryTokenKind::LParen,
                lexeme: "(".to_owned(),
            });
            continue;
        }

        if ch == ')' {
            let _ = chars.next();
            tokens.push(Fts5QueryToken {
                kind: Fts5QueryTokenKind::RParen,
                lexeme: ")".to_owned(),
            });
            continue;
        }

        if ch == '^' {
            let _ = chars.next();
            tokens.push(Fts5QueryToken {
                kind: Fts5QueryTokenKind::Caret,
                lexeme: "^".to_owned(),
            });
            continue;
        }

        if ch == '"' {
            let _ = chars.next();
            let mut phrase = String::new();
            let mut closed = false;

            for phrase_ch in chars.by_ref() {
                if phrase_ch == '"' {
                    closed = true;
                    break;
                }
                phrase.push(phrase_ch);
            }

            if !closed {
                return Err(Fts5QueryError::UnclosedPhrase);
            }
            if !phrase.is_empty() {
                tokens.push(Fts5QueryToken {
                    kind: Fts5QueryTokenKind::Phrase,
                    lexeme: phrase,
                });
            }
            continue;
        }

        // Read a word.
        let mut word = String::new();
        while let Some(word_ch) = chars.peek().copied() {
            if word_ch.is_ascii_whitespace() || matches!(word_ch, '(' | ')' | '"' | '^') {
                break;
            }
            let _ = chars.next();
            word.push(word_ch);
        }

        if word.is_empty() {
            continue;
        }

        // Check for prefix operator (trailing *).
        if word.ends_with('*') {
            let base = word.trim_end_matches('*');
            if !base.is_empty() {
                tokens.push(Fts5QueryToken {
                    kind: Fts5QueryTokenKind::Prefix,
                    lexeme: base.to_owned(),
                });
                continue;
            }
        }

        // Check for column filter (word:).
        if word.ends_with(':') {
            let col_name = word.trim_end_matches(':');
            if !col_name.is_empty() {
                tokens.push(Fts5QueryToken {
                    kind: Fts5QueryTokenKind::ColumnFilter,
                    lexeme: col_name.to_owned(),
                });
                continue;
            }
        }

        let upper = word.to_ascii_uppercase();
        let kind = match upper.as_str() {
            "OR" => Fts5QueryTokenKind::Or,
            "NOT" => Fts5QueryTokenKind::Not,
            "NEAR" => Fts5QueryTokenKind::Near,
            _ => Fts5QueryTokenKind::Term,
        };

        tokens.push(Fts5QueryToken { kind, lexeme: word });
    }

    if tokens.is_empty() {
        return Err(Fts5QueryError::EmptyQuery);
    }
    Ok(tokens)
}

fn validate_fts5_parentheses(tokens: &[Fts5QueryToken]) -> std::result::Result<(), Fts5QueryError> {
    let mut depth = 0u32;
    for token in tokens {
        match token.kind {
            Fts5QueryTokenKind::LParen => depth = depth.saturating_add(1),
            Fts5QueryTokenKind::RParen => {
                if depth == 0 {
                    return Err(Fts5QueryError::UnbalancedParentheses);
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(Fts5QueryError::UnbalancedParentheses);
    }
    Ok(())
}

fn validate_fts5_not_binary(tokens: &[Fts5QueryToken]) -> std::result::Result<(), Fts5QueryError> {
    for (i, token) in tokens.iter().enumerate() {
        if token.kind == Fts5QueryTokenKind::Not {
            // In FTS5, NOT is binary-only. It must have a left operand.
            if i == 0 {
                return Err(Fts5QueryError::UnaryNotForbidden);
            }
            // Check the token to the left is an expression-ending token.
            let left = &tokens[i - 1];
            if !matches!(
                left.kind,
                Fts5QueryTokenKind::Term
                    | Fts5QueryTokenKind::Phrase
                    | Fts5QueryTokenKind::Prefix
                    | Fts5QueryTokenKind::RParen
            ) {
                return Err(Fts5QueryError::UnaryNotForbidden);
            }
        }
    }
    Ok(())
}

/// Insert implicit AND tokens between adjacent expressions in FTS5.
fn insert_implicit_and(tokens: &[Fts5QueryToken]) -> Vec<Fts5QueryToken> {
    let mut result = Vec::with_capacity(tokens.len() * 2);

    for (i, token) in tokens.iter().enumerate() {
        if i > 0 {
            let prev = &tokens[i - 1];
            let prev_ends = matches!(
                prev.kind,
                Fts5QueryTokenKind::Term
                    | Fts5QueryTokenKind::Phrase
                    | Fts5QueryTokenKind::Prefix
                    | Fts5QueryTokenKind::RParen
            );
            let cur_starts = matches!(
                token.kind,
                Fts5QueryTokenKind::Term
                    | Fts5QueryTokenKind::Phrase
                    | Fts5QueryTokenKind::Prefix
                    | Fts5QueryTokenKind::LParen
                    | Fts5QueryTokenKind::Caret
                    | Fts5QueryTokenKind::ColumnFilter
                    | Fts5QueryTokenKind::Near
            );

            if prev_ends && cur_starts {
                result.push(Fts5QueryToken {
                    kind: Fts5QueryTokenKind::And,
                    lexeme: "AND".to_owned(),
                });
            }
        }
        result.push(token.clone());
    }

    result
}

// ---------------------------------------------------------------------------
// Query evaluation
// ---------------------------------------------------------------------------

/// A parsed FTS5 query expression tree.
#[derive(Debug, Clone)]
pub enum Fts5Expr {
    Term(String),
    Prefix(String),
    Phrase(Vec<String>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>, Box<Self>),
    Near(Vec<String>, u32),
    ColumnFilter(String, Box<Self>),
    InitialToken(Box<Self>),
}

/// Build an expression tree from parsed FTS5 query tokens.
pub fn build_expr(tokens: &[Fts5QueryToken]) -> std::result::Result<Fts5Expr, Fts5QueryError> {
    let (expr, _rest) = parse_or(tokens)?;
    Ok(expr)
}

fn parse_or(
    tokens: &[Fts5QueryToken],
) -> std::result::Result<(Fts5Expr, &[Fts5QueryToken]), Fts5QueryError> {
    let (mut left, mut rest) = parse_and(tokens)?;

    while let Some(token) = rest.first() {
        if token.kind == Fts5QueryTokenKind::Or {
            let (right, r) = parse_and(&rest[1..])?;
            left = Fts5Expr::Or(Box::new(left), Box::new(right));
            rest = r;
        } else {
            break;
        }
    }

    Ok((left, rest))
}

fn parse_and(
    tokens: &[Fts5QueryToken],
) -> std::result::Result<(Fts5Expr, &[Fts5QueryToken]), Fts5QueryError> {
    let (mut left, mut rest) = parse_not(tokens)?;

    while let Some(token) = rest.first() {
        if token.kind == Fts5QueryTokenKind::And {
            let (right, r) = parse_not(&rest[1..])?;
            left = Fts5Expr::And(Box::new(left), Box::new(right));
            rest = r;
        } else {
            break;
        }
    }

    Ok((left, rest))
}

fn parse_not(
    tokens: &[Fts5QueryToken],
) -> std::result::Result<(Fts5Expr, &[Fts5QueryToken]), Fts5QueryError> {
    let (mut left, mut rest) = parse_primary(tokens)?;

    while let Some(token) = rest.first() {
        if token.kind == Fts5QueryTokenKind::Not {
            let (right, r) = parse_primary(&rest[1..])?;
            left = Fts5Expr::Not(Box::new(left), Box::new(right));
            rest = r;
        } else {
            break;
        }
    }

    Ok((left, rest))
}

#[allow(clippy::too_many_lines)]
fn parse_primary(
    tokens: &[Fts5QueryToken],
) -> std::result::Result<(Fts5Expr, &[Fts5QueryToken]), Fts5QueryError> {
    let Some(token) = tokens.first() else {
        return Err(Fts5QueryError::EmptyQuery);
    };

    match token.kind {
        Fts5QueryTokenKind::Term => Ok((Fts5Expr::Term(token.lexeme.clone()), &tokens[1..])),
        Fts5QueryTokenKind::Prefix => Ok((Fts5Expr::Prefix(token.lexeme.clone()), &tokens[1..])),
        Fts5QueryTokenKind::Phrase => {
            let words: Vec<String> = token
                .lexeme
                .split_whitespace()
                .map(str::to_lowercase)
                .collect();
            Ok((Fts5Expr::Phrase(words), &tokens[1..]))
        }
        Fts5QueryTokenKind::LParen => {
            let (expr, rest) = parse_or(&tokens[1..])?;
            if let Some(close) = rest.first() {
                if close.kind == Fts5QueryTokenKind::RParen {
                    return Ok((expr, &rest[1..]));
                }
            }
            Err(Fts5QueryError::UnbalancedParentheses)
        }
        Fts5QueryTokenKind::Caret => {
            let (inner, rest) = parse_primary(&tokens[1..])?;
            Ok((Fts5Expr::InitialToken(Box::new(inner)), rest))
        }
        Fts5QueryTokenKind::ColumnFilter => {
            let col = token.lexeme.clone();
            let (inner, rest) = parse_primary(&tokens[1..])?;
            Ok((Fts5Expr::ColumnFilter(col, Box::new(inner)), rest))
        }
        Fts5QueryTokenKind::Near => {
            // NEAR(term1 term2, N)
            // Simplified: just parse as AND for now; collect nearby terms
            let rest = &tokens[1..];
            if !rest
                .first()
                .is_some_and(|t| t.kind == Fts5QueryTokenKind::LParen)
            {
                return Err(Fts5QueryError::InvalidNearSyntax);
            }
            let rest = &rest[1..]; // skip (
            let mut terms = Vec::new();
            let mut distance = 10u32; // default NEAR distance
            let mut rest = rest;

            while let Some(t) = rest.first() {
                if t.kind == Fts5QueryTokenKind::RParen {
                    rest = &rest[1..];
                    break;
                }
                if t.kind == Fts5QueryTokenKind::Term {
                    // Check if it's a distance specifier like ",5"
                    if let Some(stripped) = t.lexeme.strip_prefix(',') {
                        if let Ok(d) = stripped.parse::<u32>() {
                            distance = d;
                        }
                    } else {
                        terms.push(t.lexeme.clone());
                    }
                }
                rest = &rest[1..];
            }

            if terms.len() < 2 {
                return Err(Fts5QueryError::InvalidNearSyntax);
            }

            Ok((Fts5Expr::Near(terms, distance), rest))
        }
        _ => Err(Fts5QueryError::EmptyQuery),
    }
}

// ---------------------------------------------------------------------------
// Inverted index
// ---------------------------------------------------------------------------

/// A posting in the inverted index.
#[derive(Debug, Clone)]
pub struct Posting {
    pub docid: i64,
    pub column: u32,
    pub positions: Vec<u32>,
}

/// In-memory inverted index for FTS5.
#[derive(Debug, Default)]
pub struct InvertedIndex {
    /// term -> list of postings
    index: HashMap<String, Vec<Posting>>,
    /// Total number of documents
    doc_count: u64,
    /// Total token count per document (for BM25 avgdl)
    doc_lengths: HashMap<i64, u32>,
}

impl InvertedIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Index a document's tokens for a given column.
    pub fn add_document(&mut self, docid: i64, column: u32, tokens: &[Fts5Token]) {
        // Build term -> positions map for this document+column.
        let mut term_positions: HashMap<&str, Vec<u32>> = HashMap::new();
        #[allow(clippy::cast_possible_truncation)]
        for (pos, token) in tokens.iter().enumerate() {
            term_positions
                .entry(&token.term)
                .or_default()
                .push(pos as u32);
        }

        for (term, positions) in term_positions {
            self.index
                .entry(term.to_owned())
                .or_default()
                .push(Posting {
                    docid,
                    column,
                    positions,
                });
        }

        #[allow(clippy::cast_possible_truncation)]
        let new_len = tokens.len() as u32;
        *self.doc_lengths.entry(docid).or_insert(0) += new_len;
        self.doc_count = u64::try_from(self.doc_lengths.len()).unwrap_or(u64::MAX);
    }

    /// Remove a document from the index.
    pub fn remove_document(&mut self, docid: i64) {
        for postings in self.index.values_mut() {
            postings.retain(|p| p.docid != docid);
        }
        self.doc_lengths.remove(&docid);
        self.doc_count = u64::try_from(self.doc_lengths.len()).unwrap_or(u64::MAX);
    }

    /// Look up postings for a term.
    #[must_use]
    pub fn get_postings(&self, term: &str) -> &[Posting] {
        self.index.get(term).map_or(&[], Vec::as_slice)
    }

    /// Look up postings for terms matching a prefix.
    #[must_use]
    pub fn get_prefix_postings(&self, prefix: &str) -> Vec<&Posting> {
        let mut result = Vec::new();
        for (term, postings) in &self.index {
            if term.starts_with(prefix) {
                result.extend(postings);
            }
        }
        result
    }

    /// Get the number of documents containing a term.
    #[must_use]
    pub fn doc_frequency(&self, term: &str) -> u64 {
        let postings = self.get_postings(term);
        let mut unique_docs: Vec<i64> = postings.iter().map(|p| p.docid).collect();
        unique_docs.sort_unstable();
        unique_docs.dedup();
        u64::try_from(unique_docs.len()).unwrap_or(u64::MAX)
    }

    /// Get term frequency for a term in a specific document.
    #[must_use]
    pub fn term_frequency(&self, term: &str, docid: i64) -> u32 {
        self.get_postings(term)
            .iter()
            .filter(|p| p.docid == docid)
            .map(|p| u32::try_from(p.positions.len()).unwrap_or(u32::MAX))
            .sum()
    }

    /// Get total document count.
    #[must_use]
    pub fn total_docs(&self) -> u64 {
        self.doc_count
    }

    /// Get average document length.
    #[must_use]
    pub fn avg_doc_length(&self) -> f64 {
        if self.doc_lengths.is_empty() {
            return 0.0;
        }
        let total: u64 = self.doc_lengths.values().map(|v| u64::from(*v)).sum();
        total as f64 / self.doc_lengths.len() as f64
    }

    /// Get a specific document's length.
    #[must_use]
    pub fn doc_length(&self, docid: i64) -> u32 {
        self.doc_lengths.get(&docid).copied().unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// BM25 ranking
// ---------------------------------------------------------------------------

/// Standard BM25 parameters.
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;

/// Compute BM25 score for a document against a set of query terms.
///
/// Lower values mean better matches (following SQLite FTS5 convention where
/// `rank` returns negative BM25 scores).
#[must_use]
#[allow(clippy::similar_names)]
pub fn bm25_score(
    index: &InvertedIndex,
    docid: i64,
    query_terms: &[String],
    weights: &[f64],
) -> f64 {
    let n = index.total_docs() as f64;
    let avgdl = index.avg_doc_length();
    let dl = f64::from(index.doc_length(docid));

    let mut score = 0.0;

    for term in query_terms {
        let df = index.doc_frequency(term) as f64;
        if df == 0.0 {
            continue;
        }

        // IDF component
        let idf = ((n - df + 0.5) / (df + 0.5)).ln_1p();

        // Get per-column frequencies for weighting
        let postings = index.get_postings(term);
        for posting in postings {
            if posting.docid != docid {
                continue;
            }
            let tf = posting.positions.len() as f64;
            let col_weight = weights.get(posting.column as usize).copied().unwrap_or(1.0);

            let denom = if avgdl > 0.0 {
                BM25_K1.mul_add(1.0 - BM25_B + BM25_B * dl / avgdl, tf)
            } else {
                tf + BM25_K1
            };

            score += col_weight * idf * (tf * (BM25_K1 + 1.0)) / denom;
        }
    }

    // Return negative score (lower = better, SQLite FTS5 convention).
    -score
}

// ---------------------------------------------------------------------------
// Query evaluation against index
// ---------------------------------------------------------------------------

/// Evaluate an FTS5 expression against the inverted index, returning
/// matching document IDs.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn evaluate_expr(index: &InvertedIndex, expr: &Fts5Expr) -> Vec<i64> {
    match expr {
        Fts5Expr::Term(term) => {
            let lower = term.to_lowercase();
            let mut docs: Vec<i64> = index.get_postings(&lower).iter().map(|p| p.docid).collect();
            docs.sort_unstable();
            docs.dedup();
            docs
        }
        Fts5Expr::Prefix(prefix) => {
            let lower = prefix.to_lowercase();
            let mut docs: Vec<i64> = index
                .get_prefix_postings(&lower)
                .iter()
                .map(|p| p.docid)
                .collect();
            docs.sort_unstable();
            docs.dedup();
            docs
        }
        Fts5Expr::Phrase(words) => evaluate_phrase(index, words),
        Fts5Expr::And(left, right) => {
            let left_docs = evaluate_expr(index, left);
            let right_docs = evaluate_expr(index, right);
            intersect_sorted(&left_docs, &right_docs)
        }
        Fts5Expr::Or(left, right) => {
            let left_docs = evaluate_expr(index, left);
            let right_docs = evaluate_expr(index, right);
            union_sorted(&left_docs, &right_docs)
        }
        Fts5Expr::Not(left, right) => {
            let left_docs = evaluate_expr(index, left);
            let right_docs = evaluate_expr(index, right);
            difference_sorted(&left_docs, &right_docs)
        }
        Fts5Expr::Near(terms, distance) => evaluate_near(index, terms, *distance),
        Fts5Expr::ColumnFilter(_col, inner) => {
            // Simplified: evaluate inner without column restriction for now
            evaluate_expr(index, inner)
        }
        Fts5Expr::InitialToken(inner) => {
            // For initial token (^), filter results to those where the term/phrase
            // appears at position 0.
            match inner.as_ref() {
                Fts5Expr::Term(term) => {
                    let lower = term.to_lowercase();
                    let mut docs: Vec<i64> = index
                        .get_postings(&lower)
                        .iter()
                        .filter(|p| p.positions.contains(&0))
                        .map(|p| p.docid)
                        .collect();
                    docs.sort_unstable();
                    docs.dedup();
                    docs
                }
                Fts5Expr::Prefix(prefix) => {
                    let lower = prefix.to_lowercase();
                    let mut docs: Vec<i64> = index
                        .get_prefix_postings(&lower)
                        .iter()
                        .filter(|p| p.positions.contains(&0))
                        .map(|p| p.docid)
                        .collect();
                    docs.sort_unstable();
                    docs.dedup();
                    docs
                }
                Fts5Expr::Phrase(words) => {
                    // Evaluate phrase but require the first word to be at position 0.
                    if words.is_empty() {
                        return Vec::new();
                    }
                    let first_lower = words[0].to_lowercase();
                    let first_postings = index.get_postings(&first_lower);
                    let mut result = Vec::new();

                    for first_p in first_postings {
                        // Optimization: if first word isn't at 0, this doc can't match ^phrase.
                        if !first_p.positions.contains(&0) {
                            continue;
                        }

                        // Check subsequent words
                        let mut match_found = true;
                        for (offset, word) in words.iter().enumerate().skip(1) {
                            #[allow(clippy::cast_possible_truncation)]
                            let target_pos = offset as u32; // implied start_pos = 0
                            let found = index.get_postings(&word.to_lowercase()).iter().any(|p| {
                                p.docid == first_p.docid
                                    && p.column == first_p.column
                                    && p.positions.contains(&target_pos)
                            });
                            if !found {
                                match_found = false;
                                break;
                            }
                        }
                        if match_found && !result.contains(&first_p.docid) {
                            result.push(first_p.docid);
                        }
                    }
                    result.sort_unstable();
                    result
                }
                _ => evaluate_expr(index, inner),
            }
        }
    }
}

fn evaluate_phrase(index: &InvertedIndex, words: &[String]) -> Vec<i64> {
    if words.is_empty() {
        return Vec::new();
    }

    // Get postings for first word.
    let first_postings = index.get_postings(&words[0]);
    if first_postings.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::new();

    // For each document that has the first word, check if subsequent words
    // appear in consecutive positions.
    for first_p in first_postings {
        'positions: for &start_pos in &first_p.positions {
            for (offset, word) in words.iter().enumerate().skip(1) {
                let target_pos = start_pos + u32::try_from(offset).unwrap_or(u32::MAX);
                let found = index.get_postings(word).iter().any(|p| {
                    p.docid == first_p.docid
                        && p.column == first_p.column
                        && p.positions.contains(&target_pos)
                });
                if !found {
                    continue 'positions;
                }
            }
            // All words found in consecutive positions.
            if !result.contains(&first_p.docid) {
                result.push(first_p.docid);
            }
        }
    }

    result
}

fn evaluate_near(index: &InvertedIndex, terms: &[String], distance: u32) -> Vec<i64> {
    if terms.len() < 2 {
        return Vec::new();
    }

    let first_lower = terms[0].to_lowercase();
    let first_postings = index.get_postings(&first_lower);
    let mut result = Vec::new();

    for first_p in first_postings {
        let mut all_near = true;

        for term in &terms[1..] {
            let lower = term.to_lowercase();
            let found = index.get_postings(&lower).iter().any(|p| {
                if p.docid != first_p.docid || p.column != first_p.column {
                    return false;
                }
                // Check if any position pair is within distance.
                first_p.positions.iter().any(|&pos1| {
                    p.positions
                        .iter()
                        .any(|&pos2| pos1.abs_diff(pos2) <= distance)
                })
            });
            if !found {
                all_near = false;
                break;
            }
        }

        if all_near && !result.contains(&first_p.docid) {
            result.push(first_p.docid);
        }
    }

    result
}

fn intersect_sorted(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result
}

fn union_sorted(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                result.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                result.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result.extend_from_slice(&a[i..]);
    result.extend_from_slice(&b[j..]);
    result
}

fn difference_sorted(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() {
        if j < b.len() {
            match a[i].cmp(&b[j]) {
                std::cmp::Ordering::Less => {
                    result.push(a[i]);
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    i += 1;
                    j += 1;
                }
            }
        } else {
            result.push(a[i]);
            i += 1;
        }
    }
    result
}

// ---------------------------------------------------------------------------
// FTS5 Virtual Table
// ---------------------------------------------------------------------------

/// FTS5 virtual table: full-text search index.
#[derive(Debug)]
pub struct Fts5Table {
    /// Column names.
    columns: Vec<String>,
    /// Configuration.
    config: Fts5Config,
    /// Tokenizer.
    tokenizer_name: String,
    /// Inverted index.
    index: InvertedIndex,
    /// Stored document content: docid -> (col0, col1, ...).
    documents: HashMap<i64, Vec<String>>,
    /// Next auto-generated rowid.
    next_rowid: i64,
}

impl Fts5Table {
    /// Create a new FTS5 table with the given column names.
    #[must_use]
    pub fn with_columns(columns: Vec<String>) -> Self {
        Self {
            columns,
            config: Fts5Config::default(),
            tokenizer_name: "unicode61".to_owned(),
            index: InvertedIndex::new(),
            documents: HashMap::new(),
            next_rowid: 1,
        }
    }

    /// Insert a document into the FTS5 table.
    pub fn insert_document(&mut self, rowid: i64, column_values: &[String]) {
        let tokenizer = create_tokenizer(&self.tokenizer_name)
            .unwrap_or_else(|| Box::new(Unicode61Tokenizer::new()));

        #[allow(clippy::cast_possible_truncation)]
        for (col_idx, text) in column_values.iter().enumerate() {
            let tokens = tokenizer.tokenize(text);
            self.index.add_document(rowid, col_idx as u32, &tokens);
        }

        self.documents.insert(rowid, column_values.to_vec());
        debug!(rowid, cols = column_values.len(), "fts5: indexed document");
    }

    /// Delete a document from the FTS5 table.
    pub fn delete_document(&mut self, rowid: i64) {
        self.index.remove_document(rowid);
        self.documents.remove(&rowid);
        debug!(rowid, "fts5: removed document");
    }

    /// Search the FTS5 table with a query, returning matching rowids ranked
    /// by BM25.
    pub fn search(&self, query: &str) -> std::result::Result<Vec<(i64, f64)>, Fts5QueryError> {
        let tokens = parse_fts5_query(query)?;
        let expr = build_expr(&tokens)?;
        let matching_docs = evaluate_expr(&self.index, &expr);

        // Extract query terms for BM25 scoring.
        let query_terms = extract_query_terms(&expr);
        let weights: Vec<f64> = self.columns.iter().map(|_| 1.0).collect();

        let mut results: Vec<(i64, f64)> = matching_docs
            .into_iter()
            .map(|docid| {
                let score = bm25_score(&self.index, docid, &query_terms, &weights);
                (docid, score)
            })
            .collect();

        // Sort by score (lower = better in FTS5 convention).
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        Ok(results)
    }

    /// Get document content for a rowid.
    #[must_use]
    pub fn get_document(&self, rowid: i64) -> Option<&[String]> {
        self.documents.get(&rowid).map(Vec::as_slice)
    }

    /// Get the FTS5 config.
    #[must_use]
    pub fn config(&self) -> &Fts5Config {
        &self.config
    }

    /// Get a mutable reference to the FTS5 config.
    pub fn config_mut(&mut self) -> &mut Fts5Config {
        &mut self.config
    }

    /// Get column names.
    #[must_use]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

/// Extract all leaf-level terms from an expression tree for BM25 scoring.
fn extract_query_terms(expr: &Fts5Expr) -> Vec<String> {
    match expr {
        Fts5Expr::Term(t) => vec![t.to_lowercase()],
        Fts5Expr::Prefix(p) => vec![p.to_lowercase()],
        Fts5Expr::Phrase(words) => words.clone(),
        Fts5Expr::And(l, r) | Fts5Expr::Or(l, r) | Fts5Expr::Not(l, r) => {
            let mut terms = extract_query_terms(l);
            terms.extend(extract_query_terms(r));
            terms
        }
        Fts5Expr::Near(terms, _) => terms.iter().map(|t| t.to_lowercase()).collect(),
        Fts5Expr::ColumnFilter(_, inner) | Fts5Expr::InitialToken(inner) => {
            extract_query_terms(inner)
        }
    }
}

// ---------------------------------------------------------------------------
// VirtualTable implementation
// ---------------------------------------------------------------------------

impl VirtualTable for Fts5Table {
    type Cursor = Fts5Cursor;

    fn connect(_cx: &Cx, args: &[&str]) -> Result<Self>
    where
        Self: Sized,
    {
        let mut columns: Vec<String> = Vec::new();
        let mut config = Fts5Config::default();
        let mut tokenizer_name = "unicode61".to_owned();

        if args.len() > 3 {
            for raw in &args[3..] {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    continue;
                }

                if let Some((key, value)) = parse_option_assignment(trimmed) {
                    let key_lower = key.to_ascii_lowercase();
                    let value_unquoted = unquote_fts_arg(value).to_ascii_lowercase();
                    match key_lower.as_str() {
                        "tokenize" => {
                            let tok = value_unquoted
                                .split_whitespace()
                                .next()
                                .unwrap_or("unicode61");
                            tok.clone_into(&mut tokenizer_name);
                        }
                        "content" => {
                            if value_unquoted.is_empty() {
                                config.content_mode = ContentMode::Contentless;
                            } else {
                                config.content_mode = ContentMode::Stored;
                            }
                        }
                        "contentless_delete" | "secure_delete" | "secure-delete" => {
                            let _ = config.apply_control_command(&format!("{key}={value}"));
                        }
                        // Parsed for compatibility but not used in this in-memory path yet.
                        "prefix" | "detail" | "columnsize" | "insttoken" => {}
                        _ => {
                            return Err(FrankenError::function_error(format!(
                                "fts5: unsupported option '{key}'"
                            )));
                        }
                    }
                    continue;
                }

                // Column declarations may include `UNINDEXED` or collation hints;
                // keep the leading identifier as the column name.
                let column = trimmed
                    .split_whitespace()
                    .next()
                    .unwrap_or_default()
                    .trim_matches(|ch| matches!(ch, '"' | '\'' | '`' | '[' | ']'));
                if !column.is_empty() {
                    columns.push(column.to_owned());
                }
            }
        }

        if columns.is_empty() {
            columns.push("content".to_owned());
        }

        debug!(
            columns = ?columns,
            tokenizer = %tokenizer_name,
            content_mode = ?config.content_mode,
            secure_delete = config.secure_delete,
            contentless_delete = config.contentless_delete,
            "fts5: connecting virtual table"
        );

        let mut table = Self::with_columns(columns);
        table.config = config;
        table.tokenizer_name = tokenizer_name;
        Ok(table)
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // Check if there's a MATCH constraint.
        let mut has_match = false;
        for (i, constraint) in info.constraints.iter().enumerate() {
            if constraint.op == fsqlite_func::vtab::ConstraintOp::Match && constraint.usable {
                info.constraint_usage[i].argv_index = 1;
                info.constraint_usage[i].omit = true;
                has_match = true;
            }
        }

        if has_match {
            info.estimated_cost = 10.0;
            info.estimated_rows = 10;
            info.idx_num = 1; // MATCH query
        } else {
            info.estimated_cost = 1_000_000.0;
            #[allow(clippy::cast_possible_wrap)]
            {
                info.estimated_rows = self.documents.len() as i64;
            }
            info.idx_num = 0; // full scan
        }

        Ok(())
    }

    fn open(&self) -> Result<Fts5Cursor> {
        Ok(Fts5Cursor {
            results: Vec::new(),
            position: 0,
            columns: self.columns.clone(),
        })
    }

    fn update(&mut self, _cx: &Cx, args: &[SqliteValue]) -> Result<Option<i64>> {
        if args.is_empty() {
            return Err(FrankenError::function_error("fts5: empty update args"));
        }

        // DELETE: args[0] = old rowid, args len == 1
        if args.len() == 1 && !args[0].is_null() {
            let rowid = args[0].to_integer();
            if self.config.content_mode == ContentMode::Contentless
                && !self.config.contentless_delete
            {
                return Err(FrankenError::function_error(
                    "fts5: cannot delete from contentless table without contentless_delete=1",
                ));
            }
            self.delete_document(rowid);
            return Ok(None);
        }

        // INSERT: args[0] = Null (no old rowid)
        if args[0].is_null() {
            let rowid = if args.len() > 1 && !args[1].is_null() {
                args[1].to_integer()
            } else {
                let r = self.next_rowid;
                self.next_rowid += 1;
                r
            };

            let col_values: Vec<String> = args.iter().skip(2).map(SqliteValue::to_text).collect();

            self.insert_document(rowid, &col_values);
            return Ok(Some(rowid));
        }

        // UPDATE: delete old, insert new
        let old_rowid = args[0].to_integer();
        self.delete_document(old_rowid);

        let new_rowid = if args.len() > 1 && !args[1].is_null() {
            args[1].to_integer()
        } else {
            old_rowid
        };

        let col_values: Vec<String> = args.iter().skip(2).map(SqliteValue::to_text).collect();

        self.insert_document(new_rowid, &col_values);
        Ok(Some(new_rowid))
    }
}

/// FTS5 cursor for scanning query results.
#[derive(Debug)]
pub struct Fts5Cursor {
    /// Matching (rowid, score, column_values) tuples.
    results: Vec<(i64, f64, Vec<String>)>,
    /// Current position in results.
    position: usize,
    /// Column names.
    columns: Vec<String>,
}

impl VirtualTableCursor for Fts5Cursor {
    fn filter(
        &mut self,
        _cx: &Cx,
        idx_num: i32,
        _idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Result<()> {
        self.results.clear();
        self.position = 0;

        if idx_num == 1 {
            // MATCH query: args[0] is the query string.
            if let Some(query_val) = args.first() {
                let _query = query_val.to_text();
                // Results populated by the table during filter setup.
                // In a real implementation, the cursor would reference the
                // table's index. For our implementation, results are set
                // externally via `set_results`.
            }
        }

        Ok(())
    }

    fn next(&mut self, _cx: &Cx) -> Result<()> {
        self.position += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.position >= self.results.len()
    }

    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()> {
        if let Some((_, score, cols)) = self.results.get(self.position) {
            #[allow(clippy::cast_sign_loss)]
            let col_idx = col as usize;

            // Column -1 or column == num_columns is the rank.
            if col < 0 || col_idx >= self.columns.len() {
                ctx.set_value(SqliteValue::Float(*score));
            } else if let Some(val) = cols.get(col_idx) {
                ctx.set_value(SqliteValue::Text(val.clone()));
            } else {
                ctx.set_value(SqliteValue::Null);
            }
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        self.results
            .get(self.position)
            .map_or(Ok(0), |(rowid, _, _)| Ok(*rowid))
    }
}

impl Fts5Cursor {
    /// Set the query results for this cursor (used in integrated search).
    pub fn set_results(&mut self, results: Vec<(i64, f64, Vec<String>)>) {
        self.results = results;
        self.position = 0;
    }
}

// ---------------------------------------------------------------------------
// Highlight / Snippet helpers
// ---------------------------------------------------------------------------

/// Generate a highlighted version of text with matching terms wrapped in
/// markers.
#[must_use]
pub fn highlight(text: &str, terms: &[String], open_tag: &str, close_tag: &str) -> String {
    let tokenizer = Unicode61Tokenizer::new();
    let tokens = tokenizer.tokenize(text);
    let lower_terms: Vec<String> = terms.iter().map(|t| t.to_lowercase()).collect();

    let mut result = String::new();
    let mut last_end = 0;

    for token in &tokens {
        if lower_terms.contains(&token.term) {
            result.push_str(&text[last_end..token.start]);
            result.push_str(open_tag);
            result.push_str(&text[token.start..token.end]);
            result.push_str(close_tag);
            last_end = token.end;
        }
    }

    result.push_str(&text[last_end..]);
    result
}

/// Generate a snippet of text around matching terms.
#[must_use]
#[allow(clippy::similar_names)]
pub fn snippet(
    text: &str,
    terms: &[String],
    open_tag: &str,
    close_tag: &str,
    ellipsis: &str,
    max_tokens: usize,
) -> String {
    let tokenizer = Unicode61Tokenizer::new();
    let tokens = tokenizer.tokenize(text);
    let lower_terms: Vec<String> = terms.iter().map(|t| t.to_lowercase()).collect();

    // Find first matching token position.
    let match_pos = tokens
        .iter()
        .position(|t| lower_terms.contains(&t.term))
        .unwrap_or(0);

    // Calculate window around match.
    let half = max_tokens / 2;
    let start = match_pos.saturating_sub(half);
    let end = (start + max_tokens).min(tokens.len());

    let mut result = String::new();
    if start > 0 {
        result.push_str(ellipsis);
    }

    let window_tokens = &tokens[start..end];
    if let Some(first) = window_tokens.first() {
        let last = &window_tokens[window_tokens.len() - 1];
        let slice = &text[first.start..last.end];

        // Highlight matching terms within the snippet.
        result.push_str(&highlight(slice, terms, open_tag, close_tag));
    }

    if end < tokens.len() {
        result.push_str(ellipsis);
    }

    result
}

// ---------------------------------------------------------------------------
// Scalar functions for FTS5
// ---------------------------------------------------------------------------

/// fts5_source_id() — returns the FTS5 extension version string.
pub struct Fts5SourceIdFunc;

impl ScalarFunction for Fts5SourceIdFunc {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(SqliteValue::Text(
            "fts5: FrankenSQLite FTS5 extension".to_owned(),
        ))
    }

    fn num_args(&self) -> i32 {
        0
    }

    fn name(&self) -> &'static str {
        "fts5_source_id"
    }
}

/// Register FTS5 scalar functions into a `FunctionRegistry`.
pub fn register_fts5_scalars(registry: &mut fsqlite_func::FunctionRegistry) {
    registry.register_scalar(Fts5SourceIdFunc);
    debug!("fts5: registered scalar functions");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extension_name_matches_crate_suffix() {
        let expected = env!("CARGO_PKG_NAME")
            .strip_prefix("fsqlite-ext-")
            .expect("extension crates should use fsqlite-ext-* naming");
        assert_eq!(extension_name(), expected);
    }

    // -- Config tests (preserved from original) --

    #[test]
    fn test_secure_delete_enable_command() {
        let mut config = Fts5Config::default();
        assert!(config.apply_control_command("secure-delete=1"));
        assert!(config.secure_delete_enabled());
        assert_eq!(config.delete_action(), DeleteAction::PhysicalPurge);
    }

    #[test]
    fn test_secure_delete_disable_command() {
        let mut config = Fts5Config::default();
        assert!(config.apply_control_command("secure_delete=true"));
        assert!(config.secure_delete_enabled());
        assert!(config.apply_control_command("secure-delete=0"));
        assert!(!config.secure_delete_enabled());
        assert_eq!(config.delete_action(), DeleteAction::Tombstone);
    }

    #[test]
    fn test_invalid_control_command_is_ignored() {
        let mut config = Fts5Config::default();
        assert!(!config.apply_control_command("secure-delete=maybe"));
        assert!(!config.apply_control_command("integrity-check=1"));
        assert_eq!(config.delete_action(), DeleteAction::Tombstone);
    }

    #[test]
    fn test_contentless_delete_rejects_without_toggle() {
        let config = Fts5Config::new(ContentMode::Contentless);
        assert_eq!(config.delete_action(), DeleteAction::Reject);
    }

    #[test]
    fn test_contentless_delete_tombstone_mode() {
        let mut config = Fts5Config::new(ContentMode::Contentless);
        assert!(config.apply_control_command("contentless_delete=1"));
        assert_eq!(config.delete_action(), DeleteAction::Tombstone);
    }

    #[test]
    fn test_contentless_delete_secure_delete_combo() {
        let mut config = Fts5Config::new(ContentMode::Contentless);
        assert!(config.apply_control_command("contentless_delete=1"));
        assert!(config.apply_control_command("secure-delete=on"));
        assert_eq!(config.delete_action(), DeleteAction::PhysicalPurge);
    }

    // -- Tokenizer tests --

    #[test]
    fn test_unicode61_tokenizer_basic() {
        let tok = Unicode61Tokenizer::new();
        let tokens = tok.tokenize("Hello, World! This is a Test.");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["hello", "world", "this", "is", "a", "test"]);
    }

    #[test]
    fn test_unicode61_tokenizer_unicode() {
        let tok = Unicode61Tokenizer::new();
        let tokens = tok.tokenize("café résumé naïve");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["café", "résumé", "naïve"]);
    }

    #[test]
    fn test_unicode61_tokenizer_offsets() {
        let tok = Unicode61Tokenizer::new();
        let tokens = tok.tokenize("abc def");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].start, 0);
        assert_eq!(tokens[0].end, 3);
        assert_eq!(tokens[1].start, 4);
        assert_eq!(tokens[1].end, 7);
    }

    #[test]
    fn test_ascii_tokenizer() {
        let tok = AsciiTokenizer;
        let tokens = tok.tokenize("Hello World 123");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["hello", "world", "123"]);
    }

    #[test]
    fn test_porter_tokenizer_stemming() {
        let tok = PorterTokenizer::new(Box::new(Unicode61Tokenizer::new()));
        let tokens = tok.tokenize("running jumps connected");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms[0], "run");
        assert_eq!(terms[1], "jump");
        assert_eq!(terms[2], "connect");
    }

    #[test]
    fn test_porter_stem_plurals() {
        assert_eq!(porter_stem("caresses"), "caress");
        assert_eq!(porter_stem("ponies"), "poni");
        assert_eq!(porter_stem("cats"), "cat");
    }

    #[test]
    fn test_trigram_tokenizer() {
        let tok = TrigramTokenizer::default();
        let tokens = tok.tokenize("abcde");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["abc", "bcd", "cde"]);
    }

    #[test]
    fn test_trigram_tokenizer_short_input() {
        let tok = TrigramTokenizer::default();
        let tokens = tok.tokenize("ab");
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_create_tokenizer_by_name() {
        assert!(create_tokenizer("unicode61").is_some());
        assert!(create_tokenizer("ascii").is_some());
        assert!(create_tokenizer("porter").is_some());
        assert!(create_tokenizer("trigram").is_some());
        assert!(create_tokenizer("nonexistent").is_none());
    }

    // -- Query parsing tests --

    #[test]
    fn test_fts5_query_implicit_and() {
        let tokens = parse_fts5_query("hello world").unwrap();
        let kinds: Vec<Fts5QueryTokenKind> = tokens.iter().map(|t| t.kind).collect();
        assert_eq!(
            kinds,
            vec![
                Fts5QueryTokenKind::Term,
                Fts5QueryTokenKind::And,
                Fts5QueryTokenKind::Term,
            ]
        );
    }

    #[test]
    fn test_fts5_query_or() {
        let tokens = parse_fts5_query("hello OR world").unwrap();
        let kinds: Vec<Fts5QueryTokenKind> = tokens.iter().map(|t| t.kind).collect();
        assert_eq!(
            kinds,
            vec![
                Fts5QueryTokenKind::Term,
                Fts5QueryTokenKind::Or,
                Fts5QueryTokenKind::Term,
            ]
        );
    }

    #[test]
    fn test_fts5_query_not_binary_only() {
        // Unary NOT is forbidden in FTS5.
        let err = parse_fts5_query("NOT hello").unwrap_err();
        assert_eq!(err, Fts5QueryError::UnaryNotForbidden);
    }

    #[test]
    fn test_fts5_query_binary_not() {
        let tokens = parse_fts5_query("hello NOT world").unwrap();
        let kinds: Vec<Fts5QueryTokenKind> = tokens.iter().map(|t| t.kind).collect();
        assert_eq!(
            kinds,
            vec![
                Fts5QueryTokenKind::Term,
                Fts5QueryTokenKind::Not,
                Fts5QueryTokenKind::Term,
            ]
        );
    }

    #[test]
    fn test_fts5_query_phrase() {
        let tokens = parse_fts5_query(r#""exact phrase""#).unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].kind, Fts5QueryTokenKind::Phrase);
        assert_eq!(tokens[0].lexeme, "exact phrase");
    }

    #[test]
    fn test_fts5_query_prefix() {
        let tokens = parse_fts5_query("hel*").unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].kind, Fts5QueryTokenKind::Prefix);
        assert_eq!(tokens[0].lexeme, "hel");
    }

    #[test]
    fn test_fts5_query_column_filter() {
        let tokens = parse_fts5_query("title: hello").unwrap();
        assert_eq!(tokens[0].kind, Fts5QueryTokenKind::ColumnFilter);
        assert_eq!(tokens[0].lexeme, "title");
    }

    #[test]
    fn test_fts5_query_unbalanced_parens() {
        let err = parse_fts5_query("(hello").unwrap_err();
        assert_eq!(err, Fts5QueryError::UnbalancedParentheses);
    }

    #[test]
    fn test_fts5_query_unclosed_phrase() {
        let err = parse_fts5_query(r#""unclosed"#).unwrap_err();
        assert_eq!(err, Fts5QueryError::UnclosedPhrase);
    }

    #[test]
    fn test_fts5_query_complex() {
        let tokens = parse_fts5_query("(hello OR world) NOT goodbye").unwrap();
        let kinds: Vec<Fts5QueryTokenKind> = tokens.iter().map(|t| t.kind).collect();
        assert_eq!(
            kinds,
            vec![
                Fts5QueryTokenKind::LParen,
                Fts5QueryTokenKind::Term,
                Fts5QueryTokenKind::Or,
                Fts5QueryTokenKind::Term,
                Fts5QueryTokenKind::RParen,
                Fts5QueryTokenKind::Not,
                Fts5QueryTokenKind::Term,
            ]
        );
    }

    // -- Inverted index tests --

    #[test]
    fn test_inverted_index_add_and_query() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        let tokens = tok.tokenize("hello world");
        index.add_document(1, 0, &tokens);

        let tokens = tok.tokenize("hello rust");
        index.add_document(2, 0, &tokens);

        assert_eq!(index.total_docs(), 2);
        assert_eq!(index.doc_frequency("hello"), 2);
        assert_eq!(index.doc_frequency("world"), 1);
        assert_eq!(index.doc_frequency("rust"), 1);
        assert_eq!(index.term_frequency("hello", 1), 1);
    }

    #[test]
    fn test_inverted_index_remove_document() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello world"));
        index.add_document(2, 0, &tok.tokenize("hello rust"));

        index.remove_document(1);
        assert_eq!(index.total_docs(), 1);
        assert_eq!(index.doc_frequency("hello"), 1);
        assert_eq!(index.doc_frequency("world"), 0);
    }

    #[test]
    fn test_inverted_index_prefix_search() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello help heap"));
        index.add_document(2, 0, &tok.tokenize("world wide web"));

        let results = index.get_prefix_postings("hel");
        let docs: Vec<i64> = results.iter().map(|p| p.docid).collect();
        assert!(docs.contains(&1));
        assert!(!docs.contains(&2));
    }

    // -- BM25 tests --

    #[test]
    fn test_bm25_ranking_orders_by_relevance() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        // Doc 1: "rust" appears 3 times
        index.add_document(1, 0, &tok.tokenize("rust rust rust programming"));
        // Doc 2: "rust" appears 1 time
        index.add_document(2, 0, &tok.tokenize("rust programming language features"));
        // Doc 3: no "rust"
        index.add_document(3, 0, &tok.tokenize("python programming language"));

        let query_terms = vec!["rust".to_owned()];
        let weights = vec![1.0];

        let score1 = bm25_score(&index, 1, &query_terms, &weights);
        let score2 = bm25_score(&index, 2, &query_terms, &weights);
        let score3 = bm25_score(&index, 3, &query_terms, &weights);

        // Lower score = better match (negative BM25).
        assert!(score1 < score2, "doc1 should rank higher (more rust)");
        assert!(
            score2 < score3,
            "doc2 should rank higher than doc3 (has rust)"
        );
        assert!(score3.abs() < f64::EPSILON, "doc3 should have score ~0");
    }

    // -- Expression evaluation tests --

    #[test]
    fn test_evaluate_term() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello world"));
        index.add_document(2, 0, &tok.tokenize("hello rust"));
        index.add_document(3, 0, &tok.tokenize("goodbye world"));

        let expr = Fts5Expr::Term("hello".to_owned());
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1, 2]);
    }

    #[test]
    fn test_evaluate_and() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello world"));
        index.add_document(2, 0, &tok.tokenize("hello rust"));
        index.add_document(3, 0, &tok.tokenize("goodbye world"));

        let expr = Fts5Expr::And(
            Box::new(Fts5Expr::Term("hello".to_owned())),
            Box::new(Fts5Expr::Term("world".to_owned())),
        );
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1]);
    }

    #[test]
    fn test_evaluate_or() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello world"));
        index.add_document(2, 0, &tok.tokenize("rust lang"));
        index.add_document(3, 0, &tok.tokenize("goodbye world"));

        let expr = Fts5Expr::Or(
            Box::new(Fts5Expr::Term("hello".to_owned())),
            Box::new(Fts5Expr::Term("rust".to_owned())),
        );
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1, 2]);
    }

    #[test]
    fn test_evaluate_not() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello world"));
        index.add_document(2, 0, &tok.tokenize("hello rust"));
        index.add_document(3, 0, &tok.tokenize("goodbye world"));

        let expr = Fts5Expr::Not(
            Box::new(Fts5Expr::Term("hello".to_owned())),
            Box::new(Fts5Expr::Term("world".to_owned())),
        );
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![2]);
    }

    #[test]
    fn test_evaluate_phrase() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("the quick brown fox"));
        index.add_document(2, 0, &tok.tokenize("brown the quick fox"));

        let expr = Fts5Expr::Phrase(vec!["quick".to_owned(), "brown".to_owned()]);
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1]);
    }

    #[test]
    fn test_evaluate_prefix() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello help heap"));
        index.add_document(2, 0, &tok.tokenize("world wide web"));

        let expr = Fts5Expr::Prefix("hel".to_owned());
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1]);
    }

    #[test]
    fn test_evaluate_initial_token() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello world"));
        index.add_document(2, 0, &tok.tokenize("world hello"));

        let expr = Fts5Expr::InitialToken(Box::new(Fts5Expr::Term("hello".to_owned())));
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1]);
    }

    // -- FTS5 Table integration tests --

    #[test]
    fn test_fts5_table_insert_and_search() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.insert_document(
            1,
            &["the quick brown fox jumps over the lazy dog".to_owned()],
        );
        table.insert_document(2, &["the quick red car drives over the bridge".to_owned()]);
        table.insert_document(3, &["a lazy cat sleeps on the mat".to_owned()]);

        let results = table.search("quick").unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(id, _)| *id == 1));
        assert!(results.iter().any(|(id, _)| *id == 2));
    }

    #[test]
    fn test_fts5_table_search_implicit_and() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.insert_document(1, &["hello world".to_owned()]);
        table.insert_document(2, &["hello rust".to_owned()]);

        let results = table.search("hello world").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
    }

    #[test]
    fn test_fts5_table_search_or() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.insert_document(1, &["hello world".to_owned()]);
        table.insert_document(2, &["goodbye rust".to_owned()]);
        table.insert_document(3, &["test data".to_owned()]);

        let results = table.search("hello OR rust").unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_fts5_table_delete_document() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.insert_document(1, &["hello world".to_owned()]);
        table.insert_document(2, &["hello rust".to_owned()]);

        table.delete_document(1);

        let results = table.search("hello").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 2);
    }

    #[test]
    fn test_fts5_table_multicolumn() {
        let mut table = Fts5Table::with_columns(vec!["title".to_owned(), "body".to_owned()]);

        table.insert_document(
            1,
            &[
                "Rust Programming".to_owned(),
                "Rust is a systems language".to_owned(),
            ],
        );
        table.insert_document(
            2,
            &[
                "Python Guide".to_owned(),
                "Python is interpreted".to_owned(),
            ],
        );

        let results = table.search("rust").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
    }

    #[test]
    fn test_fts5_table_bm25_ranking_order() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.insert_document(1, &["rust rust rust is great".to_owned()]);
        table.insert_document(2, &["rust is a programming language".to_owned()]);

        let results = table.search("rust").unwrap();
        assert_eq!(results.len(), 2);
        // Doc 1 has more occurrences of "rust", so it should rank higher
        // (lower negative score).
        assert_eq!(results[0].0, 1);
    }

    // -- Virtual table trait tests --

    #[test]
    fn test_fts5_vtab_connect() {
        let cx = Cx::new();
        let vtab = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();
        assert_eq!(vtab.columns(), &["title", "body"]);
    }

    #[test]
    fn test_fts5_vtab_default_column() {
        let cx = Cx::new();
        let vtab = Fts5Table::connect(&cx, &["fts5"]).unwrap();
        assert_eq!(vtab.columns(), &["content"]);
    }

    #[test]
    fn test_fts5_vtab_connect_applies_options() {
        let cx = Cx::new();
        let vtab = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "title",
                "body UNINDEXED",
                "tokenize='porter'",
                "content=''",
                "contentless_delete=1",
            ],
        )
        .unwrap();
        assert_eq!(vtab.columns(), &["title", "body"]);
        assert_eq!(vtab.tokenizer_name, "porter");
        assert_eq!(vtab.config.content_mode(), ContentMode::Contentless);
        assert!(vtab.config.contentless_delete_enabled());
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_unknown_option() {
        let cx = Cx::new();
        let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "mystery=1"])
            .expect_err("unsupported option should fail");
        assert!(err.to_string().contains("unsupported option"));
    }

    #[test]
    fn test_fts5_vtab_update_insert() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();

        let result = vtab
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(1),
                    SqliteValue::Text("hello world".to_owned()),
                ],
            )
            .unwrap();
        assert_eq!(result, Some(1));
        assert!(vtab.get_document(1).is_some());
    }

    #[test]
    fn test_fts5_vtab_update_delete() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();

        vtab.update(
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Integer(1),
                SqliteValue::Text("hello".to_owned()),
            ],
        )
        .unwrap();

        vtab.update(&cx, &[SqliteValue::Integer(1)]).unwrap();
        assert!(vtab.get_document(1).is_none());
    }

    // -- Highlight/Snippet tests --

    #[test]
    fn test_highlight_basic() {
        let result = highlight(
            "the quick brown fox",
            &["quick".to_owned(), "fox".to_owned()],
            "<b>",
            "</b>",
        );
        assert_eq!(result, "the <b>quick</b> brown <b>fox</b>");
    }

    #[test]
    fn test_snippet_with_ellipsis() {
        let text = "alpha beta gamma delta epsilon zeta eta theta iota kappa";
        let result = snippet(text, &["delta".to_owned()], "<b>", "</b>", "...", 5);
        assert!(result.contains("<b>delta</b>"));
        assert!(result.contains("..."));
    }

    // -- Scalar function tests --

    #[test]
    fn test_fts5_source_id_func() {
        let func = Fts5SourceIdFunc;
        let result = func.invoke(&[]).unwrap();
        if let SqliteValue::Text(s) = result {
            assert!(s.contains("FTS5"));
        } else {
            panic!("expected text result");
        }
    }

    #[test]
    fn test_register_fts5_scalars() {
        let mut registry = fsqlite_func::FunctionRegistry::new();
        register_fts5_scalars(&mut registry);
        assert!(registry.find_scalar("fts5_source_id", 0).is_some());
    }

    // -- Full query pipeline test --

    #[test]
    fn test_fts5_full_query_pipeline() {
        let mut table = Fts5Table::with_columns(vec!["title".to_owned(), "body".to_owned()]);

        table.insert_document(
            1,
            &[
                "Introduction to Rust".to_owned(),
                "Rust is a systems programming language focused on safety".to_owned(),
            ],
        );
        table.insert_document(
            2,
            &[
                "Python for Data Science".to_owned(),
                "Python is widely used in data science and machine learning".to_owned(),
            ],
        );
        table.insert_document(
            3,
            &[
                "Rust Web Development".to_owned(),
                "Building web applications with Rust and Actix".to_owned(),
            ],
        );

        // Test implicit AND
        let results = table.search("rust safety").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);

        // Test OR
        let results = table.search("safety OR data").unwrap();
        assert_eq!(results.len(), 2);

        // Test NOT
        let results = table.search("rust NOT web").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);

        // Test phrase
        let results = table.search(r#""data science""#).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 2);

        // Test prefix
        let results = table.search("prog*").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
    }

    // -- Near query test --

    #[test]
    fn test_fts5_near_query() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        // "hello" at pos 0, "world" at pos 1 -> within distance 5
        index.add_document(1, 0, &tok.tokenize("hello world foo bar"));
        // "hello" at pos 0, "world" at pos 4 -> within distance 5
        index.add_document(2, 0, &tok.tokenize("hello a b c world"));
        // "hello" at pos 0, "world" at pos 6 -> NOT within distance 5
        index.add_document(3, 0, &tok.tokenize("hello a b c d e world"));

        let expr = Fts5Expr::Near(vec!["hello".to_owned(), "world".to_owned()], 5);
        let docs = evaluate_expr(&index, &expr);
        assert!(docs.contains(&1));
        assert!(docs.contains(&2));
        assert!(!docs.contains(&3));
    }

    // -- Edge case tests --

    #[test]
    fn test_empty_query_error() {
        let err = parse_fts5_query("").unwrap_err();
        assert_eq!(err, Fts5QueryError::EmptyQuery);
    }

    #[test]
    fn test_whitespace_only_query_error() {
        let err = parse_fts5_query("   ").unwrap_err();
        assert_eq!(err, Fts5QueryError::EmptyQuery);
    }

    #[test]
    fn test_doc_length_tracking() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("one two three"));
        index.add_document(2, 0, &tok.tokenize("a"));

        assert_eq!(index.doc_length(1), 3);
        assert_eq!(index.doc_length(2), 1);
        assert!((index.avg_doc_length() - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_fts5_config_default() {
        let config = Fts5Config::default();
        assert_eq!(config.content_mode(), ContentMode::Stored);
        assert!(!config.secure_delete_enabled());
        assert!(!config.contentless_delete_enabled());
    }

    #[test]
    fn test_query_error_display() {
        assert_eq!(
            format!("{}", Fts5QueryError::EmptyQuery),
            "empty FTS5 query"
        );
        assert_eq!(
            format!("{}", Fts5QueryError::UnaryNotForbidden),
            "FTS5 NOT is binary-only; unary NOT is not allowed"
        );
    }

    // -- Additional edge case tests --

    #[test]
    fn test_query_error_display_all_variants() {
        assert_eq!(
            format!("{}", Fts5QueryError::UnclosedPhrase),
            "unclosed phrase literal"
        );
        assert_eq!(
            format!("{}", Fts5QueryError::UnbalancedParentheses),
            "unbalanced parentheses"
        );
        assert_eq!(
            format!("{}", Fts5QueryError::InvalidColumnFilter("foo".to_owned())),
            "invalid column filter: foo"
        );
        assert_eq!(
            format!("{}", Fts5QueryError::InvalidNearSyntax),
            "invalid NEAR syntax"
        );
    }

    #[test]
    fn test_parse_bool_like_all_values() {
        assert_eq!(parse_bool_like("1"), Some(true));
        assert_eq!(parse_bool_like("on"), Some(true));
        assert_eq!(parse_bool_like("true"), Some(true));
        assert_eq!(parse_bool_like("TRUE"), Some(true));
        assert_eq!(parse_bool_like("  On  "), Some(true));
        assert_eq!(parse_bool_like("0"), Some(false));
        assert_eq!(parse_bool_like("off"), Some(false));
        assert_eq!(parse_bool_like("false"), Some(false));
        assert_eq!(parse_bool_like("FALSE"), Some(false));
        assert_eq!(parse_bool_like("maybe"), None);
        assert_eq!(parse_bool_like(""), None);
    }

    #[test]
    fn test_config_apply_no_equals() {
        let mut config = Fts5Config::default();
        assert!(!config.apply_control_command("noequals"));
    }

    #[test]
    fn test_unicode61_custom_separators() {
        let tok = Unicode61Tokenizer {
            separators: ".".to_owned(),
            token_chars: String::new(),
            remove_diacritics: 0,
        };
        let tokens = tok.tokenize("hello.world");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["hello", "world"]);
    }

    #[test]
    fn test_unicode61_custom_token_chars() {
        let tok = Unicode61Tokenizer {
            separators: String::new(),
            token_chars: "-".to_owned(),
            remove_diacritics: 0,
        };
        let tokens = tok.tokenize("well-known");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        // '-' is treated as a token character, so the whole thing is one token.
        assert_eq!(terms, vec!["well-known"]);
    }

    #[test]
    fn test_unicode61_empty_input() {
        let tok = Unicode61Tokenizer::new();
        assert!(tok.tokenize("").is_empty());
    }

    #[test]
    fn test_unicode61_only_separators() {
        let tok = Unicode61Tokenizer::new();
        assert!(tok.tokenize("   ...   ").is_empty());
    }

    #[test]
    fn test_ascii_tokenizer_non_ascii_dropped() {
        let tok = AsciiTokenizer;
        let tokens = tok.tokenize("café hello");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        // 'é' is not ASCII alphanumeric, so "caf" and "hello" are separate tokens.
        assert_eq!(terms, vec!["caf", "hello"]);
    }

    #[test]
    fn test_ascii_tokenizer_empty() {
        let tok = AsciiTokenizer;
        assert!(tok.tokenize("").is_empty());
    }

    #[test]
    fn test_ascii_tokenizer_name() {
        let tok = AsciiTokenizer;
        assert_eq!(Fts5Tokenizer::name(&tok), "ascii");
    }

    #[test]
    fn test_porter_tokenizer_debug() {
        let tok = PorterTokenizer::new(Box::new(Unicode61Tokenizer::new()));
        let debug = format!("{tok:?}");
        assert!(debug.contains("PorterTokenizer"));
        assert!(debug.contains("unicode61"));
    }

    #[test]
    fn test_porter_tokenizer_name() {
        let tok = PorterTokenizer::new(Box::new(Unicode61Tokenizer::new()));
        assert_eq!(Fts5Tokenizer::name(&tok), "porter");
    }

    #[test]
    fn test_porter_stem_step1b_at_suffix() {
        // "conflated" -> strip "ed" -> "conflat" -> fixup: ends with "at" -> "conflate"
        assert_eq!(porter_stem("conflated"), "conflate");
    }

    #[test]
    fn test_porter_stem_step1b_bl_suffix() {
        assert_eq!(porter_stem("troubled"), "trouble");
    }

    #[test]
    fn test_porter_stem_step1b_iz_suffix() {
        assert_eq!(porter_stem("sized"), "size");
    }

    #[test]
    fn test_porter_stem_step1b_double_consonant() {
        // "hopping" -> strip "ing" -> "hopp" -> double consonant, not l/s/z -> "hop"
        assert_eq!(porter_stem("hopping"), "hop");
    }

    #[test]
    fn test_porter_stem_eed() {
        // "agreed" -> "eed" suffix -> base "agr" (len > 1) -> "agree"
        assert_eq!(porter_stem("agreed"), "agree");
    }

    #[test]
    fn test_porter_stem_terminal_y() {
        // "happy" -> step1c: terminal y with vowel in stem -> "happi"
        assert_eq!(porter_stem("happy"), "happi");
    }

    #[test]
    fn test_porter_stem_step2_ational() {
        assert_eq!(porter_stem("relational"), "relate");
    }

    #[test]
    fn test_porter_stem_step3_ful() {
        assert_eq!(porter_stem("hopeful"), "hope");
    }

    #[test]
    fn test_porter_stem_short_word() {
        assert_eq!(porter_stem("a"), "a");
        assert_eq!(porter_stem("an"), "an");
    }

    #[test]
    fn test_measure_function() {
        assert_eq!(measure(""), 0);
        assert_eq!(measure("a"), 0);
        assert_eq!(measure("ab"), 1);
        assert_eq!(measure("abc"), 1);
        assert_eq!(measure("abab"), 2);
    }

    #[test]
    fn test_contains_vowel_function() {
        assert!(contains_vowel("hello"));
        assert!(contains_vowel("a"));
        assert!(!contains_vowel("xyz"));
        assert!(!contains_vowel(""));
    }

    #[test]
    fn test_trigram_case_sensitive() {
        let tok = TrigramTokenizer {
            case_sensitive: true,
        };
        let tokens = tok.tokenize("ABC");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].term, "ABC");
    }

    #[test]
    fn test_trigram_case_insensitive() {
        let tok = TrigramTokenizer {
            case_sensitive: false,
        };
        let tokens = tok.tokenize("ABC");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].term, "abc");
    }

    #[test]
    fn test_trigram_unicode() {
        let tok = TrigramTokenizer::default();
        let tokens = tok.tokenize("café");
        assert!(!tokens.is_empty());
        // "café" has 4 chars, so we get 2 trigrams.
        assert_eq!(tokens.len(), 2);
    }

    #[test]
    fn test_trigram_exact_3_chars() {
        let tok = TrigramTokenizer::default();
        let tokens = tok.tokenize("abc");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].term, "abc");
        assert_eq!(tokens[0].start, 0);
        assert_eq!(tokens[0].end, 3);
    }

    #[test]
    fn test_trigram_tokenizer_name() {
        let tok = TrigramTokenizer::default();
        assert_eq!(Fts5Tokenizer::name(&tok), "trigram");
    }

    #[test]
    fn test_create_tokenizer_case_insensitive() {
        assert!(create_tokenizer("UNICODE61").is_some());
        assert!(create_tokenizer("ASCII").is_some());
        assert!(create_tokenizer("Porter").is_some());
        assert!(create_tokenizer("Trigram").is_some());
    }

    #[test]
    fn test_query_caret_token() {
        let tokens = parse_fts5_query("^hello").unwrap();
        assert_eq!(tokens[0].kind, Fts5QueryTokenKind::Caret);
        assert_eq!(tokens[1].kind, Fts5QueryTokenKind::Term);
    }

    #[test]
    fn test_query_nested_parens() {
        let tokens = parse_fts5_query("((hello))").unwrap();
        let kinds: Vec<Fts5QueryTokenKind> = tokens.iter().map(|t| t.kind).collect();
        assert_eq!(kinds[0], Fts5QueryTokenKind::LParen);
        assert_eq!(kinds[1], Fts5QueryTokenKind::LParen);
    }

    #[test]
    fn test_query_extra_close_paren() {
        let err = parse_fts5_query(")hello").unwrap_err();
        assert_eq!(err, Fts5QueryError::UnbalancedParentheses);
    }

    #[test]
    fn test_query_empty_phrase_ignored() {
        // Empty phrase (just "") should be ignored and result in EmptyQuery.
        let err = parse_fts5_query(r#""""#).unwrap_err();
        assert_eq!(err, Fts5QueryError::EmptyQuery);
    }

    #[test]
    fn test_query_not_after_or_is_unary() {
        let err = parse_fts5_query("hello OR NOT world").unwrap_err();
        assert_eq!(err, Fts5QueryError::UnaryNotForbidden);
    }

    #[test]
    fn test_build_expr_term() {
        let tokens = parse_fts5_query("hello").unwrap();
        let expr = build_expr(&tokens).unwrap();
        assert!(matches!(expr, Fts5Expr::Term(_)));
    }

    #[test]
    fn test_build_expr_and_or_precedence() {
        // "a OR b c" should parse as "a OR (b AND c)" due to AND having higher precedence.
        let tokens = parse_fts5_query("a OR b c").unwrap();
        let expr = build_expr(&tokens).unwrap();
        assert!(matches!(expr, Fts5Expr::Or(_, _)));
    }

    #[test]
    fn test_build_expr_near_invalid() {
        // NEAR without parens is invalid.
        let tokens = parse_fts5_query("NEAR hello").unwrap();
        let err = build_expr(&tokens);
        assert!(err.is_err());
    }

    #[test]
    fn test_inverted_index_empty() {
        let index = InvertedIndex::new();
        assert_eq!(index.total_docs(), 0);
        assert_eq!(index.doc_frequency("anything"), 0);
        assert_eq!(index.term_frequency("anything", 1), 0);
        assert!(index.avg_doc_length().abs() < f64::EPSILON);
        assert_eq!(index.doc_length(1), 0);
        assert!(index.get_postings("nothing").is_empty());
        assert!(index.get_prefix_postings("n").is_empty());
    }

    #[test]
    fn test_inverted_index_multi_column() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("title words"));
        index.add_document(1, 1, &tok.tokenize("body words here"));

        assert_eq!(index.total_docs(), 1);
        assert_eq!(index.doc_length(1), 5); // 2 + 3
        assert_eq!(index.doc_frequency("words"), 1); // same docid
    }

    #[test]
    fn test_inverted_index_remove_nonexistent() {
        let mut index = InvertedIndex::new();
        index.remove_document(999); // should not panic
        assert_eq!(index.total_docs(), 0);
    }

    #[test]
    fn test_evaluate_phrase_empty() {
        let index = InvertedIndex::new();
        let expr = Fts5Expr::Phrase(vec![]);
        let docs = evaluate_expr(&index, &expr);
        assert!(docs.is_empty());
    }

    #[test]
    fn test_initial_token_prefix() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        // Doc 1: "hello world" (starts with 'hel')
        index.add_document(1, 0, &tok.tokenize("hello world"));
        // Doc 2: "world hello" (contains 'hel' but not at start)
        index.add_document(2, 0, &tok.tokenize("world hello"));

        let expr = build_expr(&parse_fts5_query("^ hel*").unwrap()).unwrap();
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1]);
    }

    #[test]
    fn test_initial_token_phrase() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        // Doc 1: "hello world" (matches ^ "hello world")
        index.add_document(1, 0, &tok.tokenize("hello world"));
        // Doc 2: "say hello world" (contains phrase but not at start)
        index.add_document(2, 0, &tok.tokenize("say hello world"));

        let expr = build_expr(&parse_fts5_query("^ \"hello world\"").unwrap()).unwrap();
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1]);
    }

    #[test]
    fn test_evaluate_near_single_term() {
        let index = InvertedIndex::new();
        let expr = Fts5Expr::Near(vec!["only".to_owned()], 5);
        let docs = evaluate_expr(&index, &expr);
        assert!(docs.is_empty());
    }

    #[test]
    fn test_evaluate_column_filter() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();
        index.add_document(1, 0, &tok.tokenize("hello world"));

        let expr = Fts5Expr::ColumnFilter(
            "title".to_owned(),
            Box::new(Fts5Expr::Term("hello".to_owned())),
        );
        let docs = evaluate_expr(&index, &expr);
        // Simplified implementation doesn't filter by column.
        assert_eq!(docs, vec![1]);
    }

    #[test]
    fn test_intersect_sorted_disjoint() {
        let result = intersect_sorted(&[1, 3, 5], &[2, 4, 6]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_intersect_sorted_empty() {
        assert!(intersect_sorted(&[], &[1, 2, 3]).is_empty());
        assert!(intersect_sorted(&[1, 2, 3], &[]).is_empty());
    }

    #[test]
    fn test_union_sorted_no_overlap() {
        let result = union_sorted(&[1, 3], &[2, 4]);
        assert_eq!(result, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_union_sorted_empty() {
        assert_eq!(union_sorted(&[], &[1, 2]), vec![1, 2]);
        assert_eq!(union_sorted(&[1, 2], &[]), vec![1, 2]);
    }

    #[test]
    fn test_difference_sorted_all_excluded() {
        let result = difference_sorted(&[1, 2, 3], &[1, 2, 3]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_difference_sorted_none_excluded() {
        let result = difference_sorted(&[1, 2, 3], &[4, 5]);
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_bm25_no_matching_terms() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();
        index.add_document(1, 0, &tok.tokenize("hello world"));

        let score = bm25_score(&index, 1, &["nonexistent".to_owned()], &[1.0]);
        assert!(score.abs() < f64::EPSILON);
    }

    #[test]
    fn test_bm25_weighted_columns() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();
        index.add_document(1, 0, &tok.tokenize("rust"));
        index.add_document(1, 1, &tok.tokenize("other stuff"));

        let low_weight = bm25_score(&index, 1, &["rust".to_owned()], &[0.1, 1.0]);
        let high_weight = bm25_score(&index, 1, &["rust".to_owned()], &[10.0, 1.0]);

        // Higher column weight should produce a more negative (better) score.
        assert!(high_weight < low_weight);
    }

    #[test]
    fn test_fts5_table_get_document() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        table.insert_document(1, &["hello world".to_owned()]);

        let doc = table.get_document(1).unwrap();
        assert_eq!(doc, &["hello world"]);

        assert!(table.get_document(99).is_none());
    }

    #[test]
    fn test_fts5_table_search_no_results() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        table.insert_document(1, &["hello world".to_owned()]);

        let results = table.search("nonexistent").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_fts5_table_auto_rowid() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();

        // Insert without explicit rowid.
        let r1 = vtab
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Null,
                    SqliteValue::Text("first".to_owned()),
                ],
            )
            .unwrap();
        let r2 = vtab
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Null,
                    SqliteValue::Text("second".to_owned()),
                ],
            )
            .unwrap();

        assert_ne!(r1, r2);
    }

    #[test]
    fn test_fts5_vtab_update_modify() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();

        vtab.update(
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Integer(1),
                SqliteValue::Text("original".to_owned()),
            ],
        )
        .unwrap();

        // Update: old_rowid=1, new_rowid=1, new_content="modified"
        vtab.update(
            &cx,
            &[
                SqliteValue::Integer(1),
                SqliteValue::Integer(1),
                SqliteValue::Text("modified".to_owned()),
            ],
        )
        .unwrap();

        let doc = vtab.get_document(1).unwrap();
        assert_eq!(doc, &["modified"]);
    }

    #[test]
    fn test_fts5_vtab_update_empty_args() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();
        assert!(vtab.update(&cx, &[]).is_err());
    }

    #[test]
    fn test_fts5_vtab_contentless_delete_rejected() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();
        *vtab.config_mut() = Fts5Config::new(ContentMode::Contentless);

        vtab.insert_document(1, &["data".to_owned()]);

        let result = vtab.update(&cx, &[SqliteValue::Integer(1)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_fts5_cursor_set_results() {
        let mut cursor = Fts5Cursor {
            results: Vec::new(),
            position: 0,
            columns: vec!["content".to_owned()],
        };

        assert!(cursor.eof());

        cursor.set_results(vec![
            (1, -1.5, vec!["hello".to_owned()]),
            (2, -0.5, vec!["world".to_owned()]),
        ]);

        assert!(!cursor.eof());
        assert_eq!(cursor.rowid().unwrap(), 1);

        let cx = Cx::new();
        cursor.next(&cx).unwrap();
        assert!(!cursor.eof());
        assert_eq!(cursor.rowid().unwrap(), 2);

        cursor.next(&cx).unwrap();
        assert!(cursor.eof());
        assert_eq!(cursor.rowid().unwrap(), 0); // past end returns 0
    }

    #[test]
    fn test_fts5_cursor_column() {
        let mut cursor = Fts5Cursor {
            results: Vec::new(),
            position: 0,
            columns: vec!["content".to_owned()],
        };

        cursor.set_results(vec![(1, -1.0, vec!["hello world".to_owned()])]);

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 0).unwrap();
        assert_eq!(
            ctx.take_value(),
            Some(SqliteValue::Text("hello world".to_owned()))
        );

        // Rank column (beyond column count).
        let mut ctx2 = ColumnContext::new();
        cursor.column(&mut ctx2, 1).unwrap();
        assert_eq!(ctx2.take_value(), Some(SqliteValue::Float(-1.0)));
    }

    #[test]
    fn test_highlight_no_matches() {
        let result = highlight("hello world", &["nonexistent".to_owned()], "<b>", "</b>");
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_highlight_empty_text() {
        let result = highlight("", &["hello".to_owned()], "<b>", "</b>");
        assert_eq!(result, "");
    }

    #[test]
    fn test_snippet_no_matches() {
        let result = snippet(
            "hello world",
            &["nonexistent".to_owned()],
            "<b>",
            "</b>",
            "...",
            3,
        );
        // Should still return something from the start.
        assert!(!result.is_empty());
    }

    #[test]
    fn test_snippet_empty_text() {
        let result = snippet("", &["hello".to_owned()], "<b>", "</b>", "...", 5);
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_query_terms() {
        let expr = Fts5Expr::And(
            Box::new(Fts5Expr::Term("Hello".to_owned())),
            Box::new(Fts5Expr::Or(
                Box::new(Fts5Expr::Prefix("Wor".to_owned())),
                Box::new(Fts5Expr::Phrase(vec![
                    "exact".to_owned(),
                    "match".to_owned(),
                ])),
            )),
        );
        let terms = extract_query_terms(&expr);
        assert_eq!(terms, vec!["hello", "wor", "exact", "match"]);
    }

    #[test]
    fn test_fts5_source_id_func_num_args() {
        let func = Fts5SourceIdFunc;
        assert_eq!(func.num_args(), 0);
        assert_eq!(func.name(), "fts5_source_id");
    }

    #[test]
    fn test_fts5_config_content_mode_accessor() {
        let config = Fts5Config::new(ContentMode::Contentless);
        assert_eq!(config.content_mode(), ContentMode::Contentless);
    }

    #[test]
    fn test_fts5_table_config_accessors() {
        let mut table = Fts5Table::with_columns(vec!["c".to_owned()]);
        assert_eq!(table.config().content_mode(), ContentMode::Stored);
        table.config_mut().apply_control_command("secure-delete=1");
        assert!(table.config().secure_delete_enabled());
    }

    #[test]
    fn test_fts5_token_colocated_field() {
        let token = Fts5Token {
            term: "test".to_owned(),
            start: 0,
            end: 4,
            colocated: true,
        };
        assert!(token.colocated);
    }

    // -----------------------------------------------------------------------
    // bd-6i2s required: FTS5 secure-delete table-level tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_fts5_secure_delete_removes() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        table.config_mut().apply_control_command("secure-delete=1");

        table.insert_document(1, &["sensitive data here".to_owned()]);
        table.insert_document(2, &["public information".to_owned()]);

        // Verify document found before delete.
        let before = table.search("sensitive").unwrap();
        assert_eq!(before.len(), 1);

        // Delete with secure-delete enabled.
        table.delete_document(1);

        // After delete, "sensitive" should return no results.
        let after = table.search("sensitive").unwrap();
        assert!(
            after.is_empty(),
            "secure-deleted term should not be searchable"
        );
    }

    #[test]
    fn test_fts5_secure_delete_integrity() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        table.config_mut().apply_control_command("secure-delete=1");

        for i in 0..10 {
            table.insert_document(i, &[format!("document number {i}")]);
        }

        // Delete half the documents.
        for i in (0..10).step_by(2) {
            table.delete_document(i);
        }

        // Remaining documents should still be searchable.
        let results = table.search("document").unwrap();
        assert_eq!(results.len(), 5, "5 remaining docs should be found");

        // Each remaining doc should have an odd ID.
        for (rowid, _) in &results {
            assert!(rowid % 2 == 1, "only odd-numbered docs should remain");
        }
    }

    #[test]
    fn test_fts5_contentless_delete_tombstone() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        *table.config_mut() = Fts5Config::new(ContentMode::Contentless);
        table
            .config_mut()
            .apply_control_command("contentless_delete=1");

        table.insert_document(1, &["hello world".to_owned()]);
        table.insert_document(2, &["hello rust".to_owned()]);

        table.delete_document(1);

        // Deleted entry should no longer match.
        let results = table.search("world").unwrap();
        assert!(
            results.is_empty(),
            "tombstoned entry should not match queries"
        );

        // Non-deleted entry should still match.
        let results = table.search("rust").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_fts5_vtab_best_index_full_scan() {
        let cx = Cx::new();
        let vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();
        let mut info = IndexInfo::new(vec![], vec![]);
        vtab.best_index(&mut info).unwrap();
        assert_eq!(info.idx_num, 0); // full scan
        assert!(info.estimated_cost > 100_000.0);
    }
}
