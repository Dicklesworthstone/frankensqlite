//! FTS5 full-text search extension (§14.2).
//!
//! Provides: tokenizer API (unicode61, ascii, porter, trigram), inverted index,
//! boolean query parsing (implicit AND, OR, NOT binary-only, phrase, prefix,
//! NEAR, column filter, caret), BM25 ranking, FTS5 virtual table with content
//! modes, and secure-delete / contentless-delete configuration.

use std::collections::{HashMap, HashSet};

use fsqlite_error::{FrankenError, Result};
use fsqlite_func::ScalarFunction;
use fsqlite_func::vtab::{
    ColumnContext, IndexInfo, TransactionalVtabState, VirtualTable, VirtualTableCursor,
};
use fsqlite_types::cx::Cx;
use fsqlite_types::value::{SmallText, SqliteValue};
use smallvec::SmallVec;
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
pub enum DetailMode {
    Full,
    Column,
    None,
}

impl std::fmt::Display for DetailMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "full"),
            Self::Column => write!(f, "column"),
            Self::None => write!(f, "none"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
pub struct Fts5Config {
    secure_delete: bool,
    content_mode: ContentMode,
    contentless_delete: bool,
    columnsize: bool,
    detail: DetailMode,
}

impl Fts5Config {
    #[must_use]
    pub const fn new(content_mode: ContentMode) -> Self {
        Self {
            secure_delete: false,
            content_mode,
            contentless_delete: false,
            columnsize: true,
            detail: DetailMode::Full,
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
    pub const fn columnsize_enabled(self) -> bool {
        self.columnsize
    }

    #[must_use]
    pub const fn detail_mode(self) -> DetailMode {
        self.detail
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

fn parse_columnsize_option(value: &str) -> Option<bool> {
    match value.trim() {
        "0" => Some(false),
        "1" => Some(true),
        _ => None,
    }
}

fn parse_detail_option(value: &str) -> Option<DetailMode> {
    match value.trim() {
        "full" => Some(DetailMode::Full),
        "column" => Some(DetailMode::Column),
        "none" => Some(DetailMode::None),
        _ => None,
    }
}

fn parse_prefix_option(value: &str) -> Option<Vec<usize>> {
    let mut prefix_lengths = Vec::new();
    for segment in value.split_ascii_whitespace() {
        let prefix_length = segment.parse::<usize>().ok()?;
        if prefix_length == 0 {
            return None;
        }
        prefix_lengths.push(prefix_length);
    }
    (!prefix_lengths.is_empty()).then_some(prefix_lengths)
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

    /// Visit each token in the input text without forcing callers to materialize
    /// an intermediate token vector.
    fn visit_tokens(&self, text: &str, sink: &mut dyn FnMut(&str, usize, usize, bool));

    /// Tokenize the input text, producing a list of tokens.
    fn tokenize(&self, text: &str) -> Vec<Fts5Token> {
        let mut tokens = Vec::new();
        self.visit_tokens(text, &mut |term, start, end, colocated| {
            tokens.push(Fts5Token {
                term: term.to_owned(),
                start,
                end,
                colocated,
            });
        });
        tokens
    }
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

    fn visit_tokens(&self, text: &str, sink: &mut dyn FnMut(&str, usize, usize, bool)) {
        let mut token_start = None;
        let mut current_term = String::new();
        let mut borrowed_ascii_token = false;

        for (byte_idx, ch) in text.char_indices() {
            if self.is_token_char(ch) {
                if token_start.is_none() {
                    token_start = Some(byte_idx);
                    current_term.clear();
                    borrowed_ascii_token = true;
                }
                if borrowed_ascii_token {
                    if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
                        continue;
                    }
                    borrowed_ascii_token = false;
                    if let Some(start) = token_start {
                        current_term.push_str(&text[start..byte_idx]);
                    }
                }
                for lc in ch.to_lowercase() {
                    current_term.push(lc);
                }
            } else if let Some(start) = token_start.take() {
                if borrowed_ascii_token {
                    sink(&text[start..byte_idx], start, byte_idx, false);
                } else if !current_term.is_empty() {
                    sink(current_term.as_str(), start, byte_idx, false);
                    current_term.clear();
                }
            }
        }

        // Flush trailing token.
        if let Some(start) = token_start {
            if borrowed_ascii_token {
                sink(&text[start..], start, text.len(), false);
            } else if !current_term.is_empty() {
                sink(current_term.as_str(), start, text.len(), false);
            }
        }
    }
}

/// ASCII tokenizer: like unicode61 but only ASCII alphanumeric characters.
#[derive(Debug, Default)]
pub struct AsciiTokenizer;

impl Fts5Tokenizer for AsciiTokenizer {
    fn name(&self) -> &'static str {
        "ascii"
    }

    fn visit_tokens(&self, text: &str, sink: &mut dyn FnMut(&str, usize, usize, bool)) {
        let mut token_start = None;
        let mut current_term = String::new();
        let mut borrowed_ascii_token = false;

        for (byte_idx, ch) in text.char_indices() {
            if ch.is_ascii_alphanumeric() {
                if token_start.is_none() {
                    token_start = Some(byte_idx);
                    current_term.clear();
                    borrowed_ascii_token = true;
                }
                if borrowed_ascii_token {
                    if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
                        continue;
                    }
                    borrowed_ascii_token = false;
                    if let Some(start) = token_start {
                        current_term.push_str(&text[start..byte_idx]);
                    }
                }
                current_term.push(ch.to_ascii_lowercase());
            } else if let Some(start) = token_start.take() {
                if borrowed_ascii_token {
                    sink(&text[start..byte_idx], start, byte_idx, false);
                } else if !current_term.is_empty() {
                    sink(current_term.as_str(), start, byte_idx, false);
                    current_term.clear();
                }
            }
        }

        if let Some(start) = token_start {
            if borrowed_ascii_token {
                sink(&text[start..], start, text.len(), false);
            } else if !current_term.is_empty() {
                sink(current_term.as_str(), start, text.len(), false);
            }
        }
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

    fn visit_tokens(&self, text: &str, sink: &mut dyn FnMut(&str, usize, usize, bool)) {
        self.inner
            .visit_tokens(text, &mut |term, start, end, colocated| {
                let stemmed = porter_stem(term);
                sink(stemmed.as_str(), start, end, colocated);
            });
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
        if last == prev && last.is_ascii_lowercase() && !matches!(last, b'l' | b's' | b'z') {
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

    fn visit_tokens(&self, text: &str, sink: &mut dyn FnMut(&str, usize, usize, bool)) {
        let chars: Vec<(usize, char)> = text.char_indices().collect();
        if chars.len() < 3 {
            return;
        }

        for window in chars.windows(3) {
            let start = window[0].0;
            let end_char = window[2];
            let end = end_char.0 + end_char.1.len_utf8();
            let term: String = if self.case_sensitive {
                window.iter().map(|(_, c)| *c).collect()
            } else {
                window.iter().flat_map(|(_, c)| c.to_lowercase()).collect()
            };
            sink(term.as_str(), start, end, false);
        }
    }
}

fn split_fts5_tokenizer_spec(spec: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut chars = spec.chars().peekable();

    while let Some(ch) = chars.next() {
        if let Some(quote_ch) = quote {
            if ch == quote_ch {
                if chars.peek() == Some(&quote_ch) {
                    let _ = chars.next();
                    current.push(quote_ch);
                } else {
                    quote = None;
                }
            } else {
                current.push(ch);
            }
        } else if ch == '\'' || ch == '"' {
            quote = Some(ch);
        } else if ch.is_ascii_whitespace() {
            if !current.is_empty() {
                parts.push(std::mem::take(&mut current));
            }
        } else {
            current.push(ch);
        }
    }

    if !current.is_empty() {
        parts.push(current);
    }

    parts
}

fn take_tokenizer_option<'a>(parts: &'a [String], index: &mut usize) -> Option<(&'a str, &'a str)> {
    let raw = parts.get(*index)?;
    *index += 1;
    if let Some((key, value)) = raw.split_once('=') {
        return Some((key, value));
    }

    let value = parts.get(*index)?;
    *index += 1;
    Some((raw.as_str(), value.as_str()))
}

fn unicode61_tokenizer_from_args(args: &[String]) -> Unicode61Tokenizer {
    let mut tokenizer = Unicode61Tokenizer::new();
    let mut index = 0;

    while let Some((key, value)) = take_tokenizer_option(args, &mut index) {
        match key.to_ascii_lowercase().as_str() {
            "tokenchars" | "token_chars" => value.clone_into(&mut tokenizer.token_chars),
            "separators" => value.clone_into(&mut tokenizer.separators),
            "remove_diacritics" => {
                tokenizer.remove_diacritics = value.parse().unwrap_or(tokenizer.remove_diacritics);
            }
            _ => {}
        }
    }

    tokenizer
}

fn create_tokenizer_from_parts(parts: &[String]) -> Option<Box<dyn Fts5Tokenizer>> {
    let name = parts.first()?.to_ascii_lowercase();
    let args = &parts[1..];

    match name.as_str() {
        "unicode61" => Some(Box::new(unicode61_tokenizer_from_args(args))),
        "ascii" => Some(Box::new(AsciiTokenizer)),
        "porter" => {
            let inner = if args.is_empty() {
                Box::new(Unicode61Tokenizer::new()) as Box<dyn Fts5Tokenizer>
            } else {
                create_tokenizer_from_parts(args)?
            };
            Some(Box::new(PorterTokenizer::new(inner)))
        }
        "trigram" => Some(Box::new(TrigramTokenizer::default())),
        _ => None,
    }
}

/// Create a tokenizer by name with optional arguments.
#[must_use]
pub fn create_tokenizer(name: &str) -> Option<Box<dyn Fts5Tokenizer>> {
    let parts = split_fts5_tokenizer_spec(name);
    create_tokenizer_from_parts(&parts)
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
    UnsupportedByDetailMode {
        detail: DetailMode,
        feature: &'static str,
    },
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
            Self::UnsupportedByDetailMode { detail, feature } => {
                write!(f, "detail={detail} does not support {feature}")
            }
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

            while let Some(phrase_ch) = chars.next() {
                if phrase_ch == '"' {
                    if chars.peek() == Some(&'"') {
                        // Escaped double-quote.
                        let _ = chars.next();
                        phrase.push('"');
                    } else {
                        closed = true;
                        break;
                    }
                } else {
                    phrase.push(phrase_ch);
                }
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

        push_query_word_tokens(&word, &mut tokens);
    }

    if tokens.is_empty() {
        return Err(Fts5QueryError::EmptyQuery);
    }
    Ok(tokens)
}

fn push_query_word_tokens(word: &str, tokens: &mut Vec<Fts5QueryToken>) {
    if word.ends_with(':') {
        let col_name = word.trim_end_matches(':');
        if !col_name.is_empty() {
            tokens.push(Fts5QueryToken {
                kind: Fts5QueryTokenKind::ColumnFilter,
                lexeme: col_name.to_owned(),
            });
        }
        return;
    }

    if let Some((column_name, remainder)) = word.split_once(':')
        && !column_name.is_empty()
        && !remainder.is_empty()
    {
        tokens.push(Fts5QueryToken {
            kind: Fts5QueryTokenKind::ColumnFilter,
            lexeme: column_name.to_owned(),
        });
        push_query_word_tokens(remainder, tokens);
        return;
    }

    if word.ends_with('*') {
        let base = word.trim_end_matches('*');
        if !base.is_empty() {
            tokens.push(Fts5QueryToken {
                kind: Fts5QueryTokenKind::Prefix,
                lexeme: base.to_owned(),
            });
            return;
        }
    }

    let upper = word.to_ascii_uppercase();
    let kind = match upper.as_str() {
        "AND" => Fts5QueryTokenKind::And,
        "OR" => Fts5QueryTokenKind::Or,
        "NOT" => Fts5QueryTokenKind::Not,
        "NEAR" => Fts5QueryTokenKind::Near,
        _ => Fts5QueryTokenKind::Term,
    };

    tokens.push(Fts5QueryToken {
        kind,
        lexeme: word.to_owned(),
    });
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
    let (mut left, mut rest) = parse_primary(tokens)?;

    while let Some(token) = rest.first() {
        if token.kind == Fts5QueryTokenKind::And {
            let (right, r) = parse_primary(&rest[1..])?;
            left = Fts5Expr::And(Box::new(left), Box::new(right));
            rest = r;
        } else if token.kind == Fts5QueryTokenKind::Not {
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
            // Supported subset: NEAR(term1 term2 ..., N) with bare term operands.
            let mut rest = &tokens[1..];
            if !rest
                .first()
                .is_some_and(|t| t.kind == Fts5QueryTokenKind::LParen)
            {
                return Err(Fts5QueryError::InvalidNearSyntax);
            }
            rest = &rest[1..]; // skip (
            let mut terms = Vec::new();
            let mut distance = 10u32; // default NEAR distance
            let mut expect_distance = false;

            while let Some(t) = rest.first() {
                if t.kind == Fts5QueryTokenKind::And {
                    rest = &rest[1..];
                    continue;
                }

                if t.kind == Fts5QueryTokenKind::RParen {
                    if expect_distance || terms.len() < 2 {
                        return Err(Fts5QueryError::InvalidNearSyntax);
                    }
                    rest = &rest[1..];
                    break;
                }

                if t.kind != Fts5QueryTokenKind::Term {
                    return Err(Fts5QueryError::InvalidNearSyntax);
                }

                let lexeme = t.lexeme.trim();
                if lexeme.is_empty() {
                    return Err(Fts5QueryError::InvalidNearSyntax);
                }

                if expect_distance {
                    let raw_distance = lexeme.strip_prefix(',').unwrap_or(lexeme);
                    distance = raw_distance
                        .parse::<u32>()
                        .map_err(|_| Fts5QueryError::InvalidNearSyntax)?;
                    expect_distance = false;
                    rest = &rest[1..];
                    continue;
                }

                if lexeme == "," {
                    if terms.len() < 2 {
                        return Err(Fts5QueryError::InvalidNearSyntax);
                    }
                    expect_distance = true;
                    rest = &rest[1..];
                    continue;
                }

                if let Some((raw_term, raw_distance)) = lexeme.split_once(',') {
                    let term = raw_term.trim();
                    let trailing = raw_distance.trim();

                    if term.is_empty() {
                        if terms.len() < 2 || trailing.is_empty() {
                            return Err(Fts5QueryError::InvalidNearSyntax);
                        }
                        distance = trailing
                            .parse::<u32>()
                            .map_err(|_| Fts5QueryError::InvalidNearSyntax)?;
                    } else {
                        terms.push(term.to_owned());
                        if trailing.is_empty() {
                            expect_distance = true;
                        } else {
                            distance = trailing
                                .parse::<u32>()
                                .map_err(|_| Fts5QueryError::InvalidNearSyntax)?;
                        }
                    }
                    rest = &rest[1..];
                    continue;
                }

                terms.push(lexeme.to_owned());
                rest = &rest[1..];
            }

            if expect_distance || terms.len() < 2 {
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
type Positions = SmallVec<[u32; 4]>;
type PostingList = SmallVec<[Posting; 1]>;

#[derive(Debug, Clone)]
pub struct Posting {
    pub docid: i64,
    pub column: u32,
    pub positions: Positions,
}

/// In-memory inverted index for FTS5.
#[derive(Debug, Clone)]
pub struct InvertedIndex {
    /// term -> list of postings
    index: HashMap<SmallText, PostingList>,
    /// prefix length -> (prefix term -> list of postings)
    prefix_indexes: HashMap<usize, HashMap<SmallText, PostingList>>,
    /// How much positional detail is retained for each posting.
    detail: DetailMode,
    /// Set of rowids currently present in the index.
    doc_ids: HashSet<i64>,
    /// Total token count per document (for BM25 avgdl)
    doc_lengths: Option<HashMap<i64, u32>>,
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl InvertedIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::with_options(true, &[], DetailMode::Full)
    }

    #[must_use]
    pub fn with_column_sizes(track_column_sizes: bool) -> Self {
        Self::with_options(track_column_sizes, &[], DetailMode::Full)
    }

    #[must_use]
    pub fn with_options(
        track_column_sizes: bool,
        prefix_lengths: &[usize],
        detail: DetailMode,
    ) -> Self {
        Self {
            index: HashMap::new(),
            prefix_indexes: prefix_lengths
                .iter()
                .copied()
                .map(|prefix_length| (prefix_length, HashMap::new()))
                .collect(),
            detail,
            doc_ids: HashSet::new(),
            doc_lengths: track_column_sizes.then(HashMap::new),
        }
    }

    #[must_use]
    pub const fn tracks_column_sizes(&self) -> bool {
        self.doc_lengths.is_some()
    }

    #[must_use]
    pub fn tracks_prefix_length(&self, prefix_length: usize) -> bool {
        self.prefix_indexes.contains_key(&prefix_length)
    }

    #[must_use]
    pub const fn detail_mode(&self) -> DetailMode {
        self.detail
    }

    fn append_position(&mut self, term: &str, docid: i64, column: u32, position: u32) {
        let stored_column = if self.detail == DetailMode::None {
            0
        } else {
            column
        };
        let stored_position = if self.detail == DetailMode::Full {
            position
        } else {
            0
        };
        append_position_to_postings(
            self.index.entry(SmallText::from(term)).or_default(),
            docid,
            stored_column,
            stored_position,
        );

        for (prefix_length, prefix_index) in &mut self.prefix_indexes {
            if let Some(prefix) = prefix_slice(term, *prefix_length) {
                append_position_to_postings(
                    prefix_index.entry(SmallText::from(prefix)).or_default(),
                    docid,
                    stored_column,
                    stored_position,
                );
            }
        }
    }

    /// Index a document's tokens for a given column.
    pub fn add_document(&mut self, docid: i64, column: u32, tokens: &[Fts5Token]) {
        self.doc_ids.insert(docid);
        // Build term -> positions map for this document+column.
        let mut term_positions: HashMap<&str, Positions> = HashMap::new();
        #[allow(clippy::cast_possible_truncation)]
        for (pos, token) in tokens.iter().enumerate() {
            term_positions
                .entry(&token.term)
                .or_default()
                .push(pos as u32);
        }

        for (term, positions) in term_positions {
            for position in positions {
                self.append_position(term, docid, column, position);
            }
        }

        #[allow(clippy::cast_possible_truncation)]
        let new_len = tokens.len() as u32;
        if let Some(doc_lengths) = self.doc_lengths.as_mut() {
            *doc_lengths.entry(docid).or_insert(0) += new_len;
        }
    }

    /// Index a raw text value directly from a tokenizer, avoiding a temporary
    /// `Vec<Fts5Token>` on hot ingest paths.
    pub fn add_text(&mut self, docid: i64, column: u32, tokenizer: &dyn Fts5Tokenizer, text: &str) {
        self.doc_ids.insert(docid);
        let mut token_count = 0_u32;
        tokenizer.visit_tokens(text, &mut |term, _start, _end, _| {
            let position = token_count;
            token_count = token_count.saturating_add(1);
            self.append_position(term, docid, column, position);
        });

        if let Some(doc_lengths) = self.doc_lengths.as_mut() {
            *doc_lengths.entry(docid).or_insert(0) += token_count;
        }
    }

    /// Remove a document from the index.
    pub fn remove_document(&mut self, docid: i64) {
        for postings in self.index.values_mut() {
            postings.retain(|p| p.docid != docid);
        }
        for prefix_index in self.prefix_indexes.values_mut() {
            for postings in prefix_index.values_mut() {
                postings.retain(|p| p.docid != docid);
            }
        }
        self.doc_ids.remove(&docid);
        if let Some(doc_lengths) = self.doc_lengths.as_mut() {
            doc_lengths.remove(&docid);
        }
    }

    /// Look up postings for a term.
    #[must_use]
    pub fn get_postings(&self, term: &str) -> &[Posting] {
        self.index.get(term).map_or(&[], SmallVec::as_slice)
    }

    /// Look up postings for terms matching a prefix.
    #[must_use]
    pub fn get_prefix_postings(&self, prefix: &str) -> Vec<&Posting> {
        if let Some(prefix_index) = self.prefix_indexes.get(&prefix.chars().count()) {
            return prefix_index
                .get(prefix)
                .map_or_else(Vec::new, |postings| postings.iter().collect());
        }

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
        u64::try_from(self.doc_ids.len()).unwrap_or(u64::MAX)
    }

    /// Get average document length.
    #[must_use]
    pub fn avg_doc_length(&self) -> f64 {
        if let Some(doc_lengths) = self.doc_lengths.as_ref() {
            if doc_lengths.is_empty() {
                return 0.0;
            }
            let total: u64 = doc_lengths.values().map(|v| u64::from(*v)).sum();
            return total as f64 / doc_lengths.len() as f64;
        }

        if self.doc_ids.is_empty() {
            return 0.0;
        }
        let total: u64 = self
            .doc_ids
            .iter()
            .map(|docid| u64::from(self.doc_length(*docid)))
            .sum();
        total as f64 / self.doc_ids.len() as f64
    }

    /// Get a specific document's length.
    #[must_use]
    pub fn doc_length(&self, docid: i64) -> u32 {
        if let Some(doc_lengths) = self.doc_lengths.as_ref() {
            return doc_lengths.get(&docid).copied().unwrap_or(0);
        }

        self.index
            .values()
            .flat_map(|postings| postings.iter())
            .filter(|posting| posting.docid == docid)
            .map(|posting| u32::try_from(posting.positions.len()).unwrap_or(u32::MAX))
            .sum()
    }
}

fn append_position_to_postings(postings: &mut PostingList, docid: i64, column: u32, position: u32) {
    if let Some(last_posting) = postings.last_mut()
        && last_posting.docid == docid
        && last_posting.column == column
    {
        last_posting.positions.push(position);
    } else {
        let mut positions = Positions::new();
        positions.push(position);
        postings.push(Posting {
            docid,
            column,
            positions,
        });
    }
}

fn prefix_slice(term: &str, prefix_length: usize) -> Option<&str> {
    if prefix_length == 0 {
        return None;
    }

    let mut chars = term.char_indices();
    for idx in 0..prefix_length {
        let (offset, ch) = chars.next()?;
        if idx + 1 == prefix_length {
            return Some(&term[..offset + ch.len_utf8()]);
        }
    }
    None
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
        let df_int = index.doc_frequency(term);
        if df_int == 0 {
            continue;
        }
        let df = df_int as f64;

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
    evaluate_expr_impl(index, expr, &[], None)
}

fn evaluate_expr_for_columns(
    index: &InvertedIndex,
    expr: &Fts5Expr,
    columns: &[String],
) -> Vec<i64> {
    evaluate_expr_impl(index, expr, columns, None)
}

fn tokenize_query_leaf(tokenizer: &dyn Fts5Tokenizer, text: &str) -> Vec<String> {
    tokenizer
        .tokenize(text)
        .into_iter()
        .map(|token| token.term)
        .collect()
}

fn normalize_query_term_expr(term: String, tokenizer: &dyn Fts5Tokenizer) -> Fts5Expr {
    let mut terms = tokenize_query_leaf(tokenizer, &term).into_iter();
    let Some(first) = terms.next() else {
        return Fts5Expr::Term(term.to_lowercase());
    };

    terms.fold(Fts5Expr::Term(first), |left, right| {
        Fts5Expr::And(Box::new(left), Box::new(Fts5Expr::Term(right)))
    })
}

fn normalize_query_phrase_terms(phrase: &[String], tokenizer: &dyn Fts5Tokenizer) -> Vec<String> {
    let joined = phrase.join(" ");
    let terms = tokenize_query_leaf(tokenizer, &joined);
    if terms.is_empty() {
        phrase.iter().map(|term| term.to_lowercase()).collect()
    } else {
        terms
    }
}

fn normalize_query_expr_with_tokenizer(expr: Fts5Expr, tokenizer: &dyn Fts5Tokenizer) -> Fts5Expr {
    match expr {
        Fts5Expr::Term(term) => normalize_query_term_expr(term, tokenizer),
        Fts5Expr::Prefix(prefix) => {
            let terms = tokenize_query_leaf(tokenizer, &prefix);
            let normalized = terms
                .first()
                .cloned()
                .unwrap_or_else(|| prefix.to_lowercase());
            Fts5Expr::Prefix(normalized)
        }
        Fts5Expr::Phrase(words) => {
            Fts5Expr::Phrase(normalize_query_phrase_terms(&words, tokenizer))
        }
        Fts5Expr::And(left, right) => Fts5Expr::And(
            Box::new(normalize_query_expr_with_tokenizer(*left, tokenizer)),
            Box::new(normalize_query_expr_with_tokenizer(*right, tokenizer)),
        ),
        Fts5Expr::Or(left, right) => Fts5Expr::Or(
            Box::new(normalize_query_expr_with_tokenizer(*left, tokenizer)),
            Box::new(normalize_query_expr_with_tokenizer(*right, tokenizer)),
        ),
        Fts5Expr::Not(left, right) => Fts5Expr::Not(
            Box::new(normalize_query_expr_with_tokenizer(*left, tokenizer)),
            Box::new(normalize_query_expr_with_tokenizer(*right, tokenizer)),
        ),
        Fts5Expr::Near(terms, distance) => {
            let normalized = terms
                .iter()
                .flat_map(|term| tokenize_query_leaf(tokenizer, term))
                .collect();
            Fts5Expr::Near(normalized, distance)
        }
        Fts5Expr::ColumnFilter(column_name, inner) => Fts5Expr::ColumnFilter(
            column_name,
            Box::new(normalize_query_expr_with_tokenizer(*inner, tokenizer)),
        ),
        Fts5Expr::InitialToken(inner) => Fts5Expr::InitialToken(Box::new(
            normalize_query_expr_with_tokenizer(*inner, tokenizer),
        )),
    }
}

fn evaluate_query_string(
    index: &InvertedIndex,
    columns: &[String],
    query: &str,
    tokenizer: Option<&dyn Fts5Tokenizer>,
) -> std::result::Result<(Vec<i64>, Vec<String>), Fts5QueryError> {
    let tokens = parse_fts5_query(query)?;
    let mut expr = build_expr(&tokens)?;
    if let Some(tokenizer) = tokenizer {
        expr = normalize_query_expr_with_tokenizer(expr, tokenizer);
    }
    validate_detail_mode(&expr, index.detail_mode())?;
    validate_column_filters(&expr, columns)?;
    let matching_docs = evaluate_expr_for_columns(index, &expr, columns);
    let query_terms = extract_query_terms(&expr);
    Ok((matching_docs, query_terms))
}

fn evaluate_query_strings(
    index: &InvertedIndex,
    columns: &[String],
    queries: &[&str],
    tokenizer: Option<&dyn Fts5Tokenizer>,
) -> std::result::Result<(Vec<i64>, Vec<String>), Fts5QueryError> {
    let mut combined_docs: Option<Vec<i64>> = None;
    let mut query_terms = Vec::new();

    for query in queries {
        let (matching_docs, match_terms) = evaluate_query_string(index, columns, query, tokenizer)?;
        query_terms.extend(match_terms);
        combined_docs = Some(match combined_docs {
            Some(existing) => intersect_sorted(&existing, &matching_docs),
            None => matching_docs,
        });
    }

    Ok((combined_docs.unwrap_or_default(), query_terms))
}

fn search_rows_with_weights_from_parts(
    index: &InvertedIndex,
    columns: &[String],
    documents: &HashMap<i64, Vec<String>>,
    queries: &[&str],
    weights: &[f64],
    tokenizer: Option<&dyn Fts5Tokenizer>,
) -> std::result::Result<Vec<(i64, f64, Vec<String>)>, Fts5QueryError> {
    let (matching_docs, query_terms) = evaluate_query_strings(index, columns, queries, tokenizer)?;

    let mut results: Vec<(i64, f64, Vec<String>)> = matching_docs
        .into_iter()
        .map(|docid| {
            let score = bm25_score(index, docid, &query_terms, weights);
            let row = documents.get(&docid).cloned().unwrap_or_default();
            (docid, score, row)
        })
        .collect();

    results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    Ok(results)
}

fn search_docids_with_weights_from_parts(
    index: &InvertedIndex,
    columns: &[String],
    queries: &[&str],
    weights: &[f64],
    tokenizer: Option<&dyn Fts5Tokenizer>,
) -> std::result::Result<Vec<(i64, f64)>, Fts5QueryError> {
    let (matching_docs, query_terms) = evaluate_query_strings(index, columns, queries, tokenizer)?;

    let mut results: Vec<(i64, f64)> = matching_docs
        .into_iter()
        .map(|docid| (docid, bm25_score(index, docid, &query_terms, weights)))
        .collect();

    results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    Ok(results)
}

fn evaluate_expr_impl(
    index: &InvertedIndex,
    expr: &Fts5Expr,
    columns: &[String],
    allowed_columns: Option<&[u32]>,
) -> Vec<i64> {
    match expr {
        Fts5Expr::Term(term) => {
            let lower = term.to_lowercase();
            let mut docs: Vec<i64> = index
                .get_postings(&lower)
                .iter()
                .filter(|posting| posting_matches_allowed_columns(posting.column, allowed_columns))
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
                .filter(|posting| posting_matches_allowed_columns(posting.column, allowed_columns))
                .map(|p| p.docid)
                .collect();
            docs.sort_unstable();
            docs.dedup();
            docs
        }
        Fts5Expr::Phrase(words) => evaluate_phrase(index, words, allowed_columns),
        Fts5Expr::And(left, right) => {
            let left_docs = evaluate_expr_impl(index, left, columns, allowed_columns);
            let right_docs = evaluate_expr_impl(index, right, columns, allowed_columns);
            intersect_sorted(&left_docs, &right_docs)
        }
        Fts5Expr::Or(left, right) => {
            let left_docs = evaluate_expr_impl(index, left, columns, allowed_columns);
            let right_docs = evaluate_expr_impl(index, right, columns, allowed_columns);
            union_sorted(&left_docs, &right_docs)
        }
        Fts5Expr::Not(left, right) => {
            let left_docs = evaluate_expr_impl(index, left, columns, allowed_columns);
            let right_docs = evaluate_expr_impl(index, right, columns, allowed_columns);
            difference_sorted(&left_docs, &right_docs)
        }
        Fts5Expr::Near(terms, distance) => evaluate_near(index, terms, *distance, allowed_columns),
        Fts5Expr::ColumnFilter(column_name, inner) => {
            let resolved_columns = resolve_allowed_columns(columns, column_name);
            let combined_columns = combine_allowed_columns(allowed_columns, &resolved_columns);
            evaluate_expr_impl(index, inner, columns, Some(combined_columns.as_slice()))
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
                        .filter(|posting| {
                            posting.positions.contains(&0)
                                && posting_matches_allowed_columns(posting.column, allowed_columns)
                        })
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
                        .filter(|posting| {
                            posting.positions.contains(&0)
                                && posting_matches_allowed_columns(posting.column, allowed_columns)
                        })
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
                        if !first_p.positions.contains(&0)
                            || !posting_matches_allowed_columns(first_p.column, allowed_columns)
                        {
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
                _ => evaluate_expr_impl(index, inner, columns, allowed_columns),
            }
        }
    }
}

fn posting_matches_allowed_columns(column: u32, allowed_columns: Option<&[u32]>) -> bool {
    match allowed_columns {
        Some(allowed) => allowed.contains(&column),
        None => true,
    }
}

fn resolve_allowed_columns(columns: &[String], column_name: &str) -> Vec<u32> {
    columns
        .iter()
        .enumerate()
        .filter_map(|(idx, candidate)| {
            if candidate.eq_ignore_ascii_case(column_name) {
                u32::try_from(idx).ok()
            } else {
                None
            }
        })
        .collect()
}

fn combine_allowed_columns(existing: Option<&[u32]>, resolved: &[u32]) -> Vec<u32> {
    match existing {
        Some(existing) => existing
            .iter()
            .copied()
            .filter(|column| resolved.contains(column))
            .collect(),
        None => resolved.to_vec(),
    }
}

fn validate_column_filters(
    expr: &Fts5Expr,
    columns: &[String],
) -> std::result::Result<(), Fts5QueryError> {
    match expr {
        Fts5Expr::And(left, right) | Fts5Expr::Or(left, right) | Fts5Expr::Not(left, right) => {
            validate_column_filters(left, columns)?;
            validate_column_filters(right, columns)
        }
        Fts5Expr::ColumnFilter(column_name, inner) => {
            if resolve_allowed_columns(columns, column_name).is_empty() {
                return Err(Fts5QueryError::InvalidColumnFilter(column_name.clone()));
            }
            validate_column_filters(inner, columns)
        }
        Fts5Expr::InitialToken(inner) => validate_column_filters(inner, columns),
        Fts5Expr::Term(_) | Fts5Expr::Prefix(_) | Fts5Expr::Phrase(_) | Fts5Expr::Near(_, _) => {
            Ok(())
        }
    }
}

fn validate_detail_mode(
    expr: &Fts5Expr,
    detail: DetailMode,
) -> std::result::Result<(), Fts5QueryError> {
    match expr {
        Fts5Expr::And(left, right) | Fts5Expr::Or(left, right) | Fts5Expr::Not(left, right) => {
            validate_detail_mode(left, detail)?;
            validate_detail_mode(right, detail)
        }
        Fts5Expr::Phrase(_) => {
            if detail == DetailMode::Full {
                Ok(())
            } else {
                Err(Fts5QueryError::UnsupportedByDetailMode {
                    detail,
                    feature: "phrase queries",
                })
            }
        }
        Fts5Expr::Near(_, _) => {
            if detail == DetailMode::Full {
                Ok(())
            } else {
                Err(Fts5QueryError::UnsupportedByDetailMode {
                    detail,
                    feature: "NEAR queries",
                })
            }
        }
        Fts5Expr::InitialToken(inner) => {
            if detail == DetailMode::Full {
                validate_detail_mode(inner, detail)
            } else {
                Err(Fts5QueryError::UnsupportedByDetailMode {
                    detail,
                    feature: "initial-token queries",
                })
            }
        }
        Fts5Expr::ColumnFilter(_, inner) => {
            if detail == DetailMode::None {
                Err(Fts5QueryError::UnsupportedByDetailMode {
                    detail,
                    feature: "column filters",
                })
            } else {
                validate_detail_mode(inner, detail)
            }
        }
        Fts5Expr::Term(_) | Fts5Expr::Prefix(_) => Ok(()),
    }
}

fn evaluate_phrase(
    index: &InvertedIndex,
    words: &[String],
    allowed_columns: Option<&[u32]>,
) -> Vec<i64> {
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
        if !posting_matches_allowed_columns(first_p.column, allowed_columns) {
            continue;
        }
        'positions: for &start_pos in &first_p.positions {
            for (offset, word) in words.iter().enumerate().skip(1) {
                #[allow(clippy::cast_possible_truncation)]
                let target_pos = start_pos + offset as u32; // implied start_pos = 0
                let found = index.get_postings(&word.to_lowercase()).iter().any(|p| {
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

fn evaluate_near(
    index: &InvertedIndex,
    terms: &[String],
    distance: u32,
    allowed_columns: Option<&[u32]>,
) -> Vec<i64> {
    if terms.len() < 2 {
        return Vec::new();
    }

    let first_lower = terms[0].to_lowercase();
    let first_postings = index.get_postings(&first_lower);
    let mut result = Vec::new();

    for first_p in first_postings {
        if !posting_matches_allowed_columns(first_p.column, allowed_columns) {
            continue;
        }
        let mut all_near = true;

        for term in &terms[1..] {
            let lower = term.to_lowercase();
            let found = index.get_postings(&lower).iter().any(|p| {
                if p.docid != first_p.docid || p.column != first_p.column {
                    return false;
                }
                if !posting_matches_allowed_columns(p.column, allowed_columns) {
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
#[derive(Debug, Clone)]
pub struct Fts5Table {
    /// Column names.
    columns: Vec<String>,
    /// Configuration.
    config: Fts5Config,
    /// Tokenizer.
    tokenizer_name: String,
    /// Configured prefix index lengths.
    prefix_lengths: Vec<usize>,
    /// Inverted index.
    index: InvertedIndex,
    /// Stored document content: docid -> (col0, col1, ...).
    documents: HashMap<i64, Vec<String>>,
    /// Next auto-generated rowid.
    next_rowid: i64,
    /// Savepoint markers for live VTAB writes. Stored snapshots are O(1)
    /// markers into the pending-delta log; rollback walks the log backwards.
    txn_state: TransactionalVtabState<Fts5TableSavepoint>,
    /// Whether a virtual-table transaction is currently open. Mutations record
    /// reverse-deltas while this is true; outside a transaction (e.g. during
    /// initial table construction or `rebuild_documents`) they do not.
    in_transaction: bool,
    /// Reverse-delta log accumulated since the current transaction's `xBegin`.
    /// Each entry describes how to undo a single mutation; rollback pops and
    /// replays entries in reverse order back to the requested savepoint.
    pending_deltas: Vec<RowDelta>,
    /// Suppress delta recording while restore_state is replaying inverse
    /// deltas. Without this, every reverse mutation would re-record itself
    /// into the same log we are walking and we would never converge.
    silent_mutations: bool,
}

/// Reverse-delta capturing a single mutation that can be undone to roll back
/// to a savepoint without holding a full copy of the inverted index.
#[derive(Debug, Clone)]
enum RowDelta {
    /// `rowid` did not exist before this mutation. Reverse by deleting it.
    Inserted { rowid: i64 },
    /// `rowid` existed before this mutation with `columns`, then was deleted.
    /// Reverse by re-inserting with `columns`.
    Deleted { rowid: i64, columns: Vec<String> },
    /// `rowid` existed before this mutation with `old_columns`, then was
    /// updated. Reverse by deleting and re-inserting with `old_columns`.
    Updated { rowid: i64, old_columns: Vec<String> },
}

/// Lightweight savepoint marker. Records the schema scalars (cheap to clone)
/// plus the position in the pending-delta log at which this savepoint opened.
/// Rolling back to this savepoint pops deltas down to that position.
#[derive(Debug, Clone)]
struct Fts5TableSavepoint {
    config: Fts5Config,
    tokenizer_name: String,
    prefix_lengths: Vec<usize>,
    next_rowid: i64,
    deltas_at_savepoint: usize,
}

impl Fts5Table {
    /// Create a new FTS5 table with the given column names.
    #[must_use]
    pub fn with_columns(columns: Vec<String>) -> Self {
        Self {
            columns,
            config: Fts5Config::default(),
            tokenizer_name: "unicode61".to_owned(),
            prefix_lengths: Vec::new(),
            index: InvertedIndex::with_options(true, &[], DetailMode::Full),
            documents: HashMap::new(),
            next_rowid: 1,
            txn_state: TransactionalVtabState::default(),
            in_transaction: false,
            pending_deltas: Vec::new(),
            silent_mutations: false,
        }
    }

    /// Capture the current savepoint marker. O(1): records schema scalars and
    /// the current depth of the pending-delta log, NOT a copy of the inverted
    /// index or documents map.
    fn snapshot_state(&self) -> Fts5TableSavepoint {
        Fts5TableSavepoint {
            config: self.config,
            tokenizer_name: self.tokenizer_name.clone(),
            prefix_lengths: self.prefix_lengths.clone(),
            next_rowid: self.next_rowid,
            deltas_at_savepoint: self.pending_deltas.len(),
        }
    }

    /// Restore state back to a previous savepoint by replaying inverse deltas.
    /// Walks the pending-delta log backwards from its current tail down to the
    /// position recorded at `snapshot`, undoing each mutation. Schema scalars
    /// are restored by direct assignment.
    fn restore_state(&mut self, snapshot: Fts5TableSavepoint) {
        self.config = snapshot.config;
        self.tokenizer_name = snapshot.tokenizer_name;
        self.prefix_lengths = snapshot.prefix_lengths;
        self.next_rowid = snapshot.next_rowid;

        let tokenizer = self.create_tokenizer_instance();
        self.silent_mutations = true;
        while self.pending_deltas.len() > snapshot.deltas_at_savepoint {
            let delta = self
                .pending_deltas
                .pop()
                .expect("len > target implies pop succeeds");
            self.reverse_delta(delta, tokenizer.as_ref());
        }
        self.silent_mutations = false;
    }

    /// Apply the inverse of a previously-recorded mutation. Called with
    /// `silent_mutations = true` so the reverse mutation does not append a new
    /// delta to the log.
    fn reverse_delta(&mut self, delta: RowDelta, tokenizer: &dyn Fts5Tokenizer) {
        match delta {
            RowDelta::Inserted { rowid } => {
                self.index.remove_document(rowid);
                self.documents.remove(&rowid);
            }
            RowDelta::Deleted { rowid, columns } => {
                self.index_document_with_tokenizer(rowid, &columns, tokenizer);
                self.documents.insert(rowid, columns);
            }
            RowDelta::Updated { rowid, old_columns } => {
                if self.documents.contains_key(&rowid) {
                    self.index.remove_document(rowid);
                }
                self.index_document_with_tokenizer(rowid, &old_columns, tokenizer);
                self.documents.insert(rowid, old_columns);
            }
        }
    }

    fn index_document_with_tokenizer(
        &mut self,
        rowid: i64,
        column_values: &[String],
        tokenizer: &dyn Fts5Tokenizer,
    ) {
        #[allow(clippy::cast_possible_truncation)]
        for (col_idx, text) in column_values.iter().enumerate() {
            self.index.add_text(rowid, col_idx as u32, tokenizer, text);
        }
    }

    fn store_document_with_tokenizer(
        &mut self,
        rowid: i64,
        column_values: Vec<String>,
        tokenizer: &dyn Fts5Tokenizer,
    ) {
        let recording = self.in_transaction && !self.silent_mutations;
        let old_columns = if recording {
            self.documents.get(&rowid).cloned()
        } else {
            None
        };

        let existed = self.documents.contains_key(&rowid);
        if existed {
            self.index.remove_document(rowid);
        }
        self.index_document_with_tokenizer(rowid, &column_values, tokenizer);
        self.next_rowid = self.next_rowid.max(rowid.saturating_add(1));
        self.documents.insert(rowid, column_values);

        if recording {
            if let Some(old_columns) = old_columns {
                self.pending_deltas
                    .push(RowDelta::Updated { rowid, old_columns });
            } else {
                self.pending_deltas.push(RowDelta::Inserted { rowid });
            }
        }
        debug!(rowid, "fts5: indexed document");
    }

    pub fn create_tokenizer_instance(&self) -> Box<dyn Fts5Tokenizer> {
        create_tokenizer(&self.tokenizer_name)
            .unwrap_or_else(|| Box::new(Unicode61Tokenizer::new()))
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    pub fn allocate_rowid(&mut self) -> i64 {
        let rowid = self.next_rowid;
        self.next_rowid = self.next_rowid.saturating_add(1);
        rowid
    }

    pub fn insert_document_owned_with_tokenizer(
        &mut self,
        rowid: i64,
        column_values: Vec<String>,
        tokenizer: &dyn Fts5Tokenizer,
    ) {
        self.store_document_with_tokenizer(rowid, column_values, tokenizer);
    }

    fn insert_document_owned(&mut self, rowid: i64, column_values: Vec<String>) {
        let tokenizer = self.create_tokenizer_instance();
        self.store_document_with_tokenizer(rowid, column_values, tokenizer.as_ref());
    }

    /// Insert a document into the FTS5 table.
    pub fn insert_document(&mut self, rowid: i64, column_values: &[String]) {
        self.insert_document_owned(rowid, column_values.to_vec());
    }

    /// Delete a document from the FTS5 table.
    pub fn delete_document(&mut self, rowid: i64) {
        let recording = self.in_transaction && !self.silent_mutations;
        let captured = if recording {
            self.documents.get(&rowid).cloned()
        } else {
            None
        };

        self.index.remove_document(rowid);
        let removed = self.documents.remove(&rowid);

        if recording {
            if let Some(columns) = captured.or(removed) {
                self.pending_deltas
                    .push(RowDelta::Deleted { rowid, columns });
            }
        }
        debug!(rowid, "fts5: removed document");
    }

    /// Rebuild the in-memory index and rowid allocator from persisted rows.
    pub fn rebuild_documents(&mut self, rows: Vec<(i64, Vec<String>)>) {
        self.index = InvertedIndex::with_options(
            self.config.columnsize_enabled(),
            &self.prefix_lengths,
            self.config.detail_mode(),
        );
        self.documents = HashMap::with_capacity(rows.len());
        self.next_rowid = 1;
        let tokenizer = create_tokenizer(&self.tokenizer_name)
            .unwrap_or_else(|| Box::new(Unicode61Tokenizer::new()));
        for (rowid, columns) in rows {
            self.index_document_with_tokenizer(rowid, &columns, tokenizer.as_ref());
            self.next_rowid = self.next_rowid.max(rowid.saturating_add(1));
            self.documents.insert(rowid, columns);
        }
    }

    /// Search the FTS5 table with a query, returning matching rowids ranked
    /// by BM25.
    pub fn search(&self, query: &str) -> std::result::Result<Vec<(i64, f64)>, Fts5QueryError> {
        let weights: Vec<f64> = self.columns.iter().map(|_| 1.0).collect();
        self.search_queries_with_weights(&[query], &weights)
    }

    pub fn search_queries_with_weights(
        &self,
        queries: &[&str],
        weights: &[f64],
    ) -> std::result::Result<Vec<(i64, f64)>, Fts5QueryError> {
        let tokenizer = self.create_tokenizer_instance();
        search_docids_with_weights_from_parts(
            &self.index,
            &self.columns,
            queries,
            weights,
            Some(tokenizer.as_ref()),
        )
    }

    pub fn search_rows(
        &self,
        query: &str,
    ) -> std::result::Result<Vec<(i64, f64, Vec<String>)>, Fts5QueryError> {
        let weights: Vec<f64> = self.columns.iter().map(|_| 1.0).collect();
        self.search_rows_for_queries_with_weights(&[query], &weights)
    }

    pub fn search_rows_for_queries_with_weights(
        &self,
        queries: &[&str],
        weights: &[f64],
    ) -> std::result::Result<Vec<(i64, f64, Vec<String>)>, Fts5QueryError> {
        let tokenizer = self.create_tokenizer_instance();
        search_rows_with_weights_from_parts(
            &self.index,
            &self.columns,
            &self.documents,
            queries,
            weights,
            Some(tokenizer.as_ref()),
        )
    }

    pub fn query_terms_for_queries(
        &self,
        queries: &[&str],
    ) -> std::result::Result<Vec<String>, Fts5QueryError> {
        let tokenizer = self.create_tokenizer_instance();
        evaluate_query_strings(
            &self.index,
            &self.columns,
            queries,
            Some(tokenizer.as_ref()),
        )
        .map(|(_docs, terms)| terms)
    }

    #[must_use]
    pub fn all_rows(&self) -> Vec<(i64, Vec<String>)> {
        let mut rows: Vec<(i64, Vec<String>)> = self
            .documents
            .iter()
            .map(|(rowid, columns)| (*rowid, columns.clone()))
            .collect();
        rows.sort_by_key(|(rowid, _)| *rowid);
        rows
    }

    #[must_use]
    pub fn row_count(&self) -> usize {
        self.documents.len()
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

    #[must_use]
    pub fn index(&self) -> &InvertedIndex {
        &self.index
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
        let mut prefix_lengths = Vec::new();

        if args.len() > 3 {
            for raw in &args[3..] {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    continue;
                }

                if let Some((key, value)) = parse_option_assignment(trimmed) {
                    let key_lower = key.to_ascii_lowercase();
                    let value_unquoted_raw = unquote_fts_arg(value);
                    let value_unquoted = value_unquoted_raw.to_ascii_lowercase();
                    match key_lower.as_str() {
                        "tokenize" => {
                            value_unquoted_raw.clone_into(&mut tokenizer_name);
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
                        "columnsize" => {
                            config.columnsize = parse_columnsize_option(value_unquoted.as_str())
                                .ok_or_else(|| {
                                    FrankenError::function_error("fts5: columnsize must be 0 or 1")
                                })?;
                        }
                        "detail" => {
                            config.detail = parse_detail_option(value_unquoted.as_str())
                                .ok_or_else(|| {
                                    FrankenError::function_error(
                                        "fts5: detail must be full, column, or none",
                                    )
                                })?;
                        }
                        "prefix" => {
                            prefix_lengths.extend(
                                parse_prefix_option(value_unquoted.as_str()).ok_or_else(|| {
                                    FrankenError::function_error(
                                        "fts5: prefix must be a whitespace separated list of positive integers",
                                    )
                                })?,
                            );
                        }
                        // Parsed for compatibility but not used in this in-memory path yet.
                        "insttoken" => {}
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
        prefix_lengths.sort_unstable();
        prefix_lengths.dedup();

        debug!(
            columns = ?columns,
            tokenizer = %tokenizer_name,
            content_mode = ?config.content_mode,
            secure_delete = config.secure_delete,
            contentless_delete = config.contentless_delete,
            detail = ?config.detail_mode(),
            prefix_lengths = ?prefix_lengths,
            "fts5: connecting virtual table"
        );

        let mut table = Self::with_columns(columns);
        table.config = config;
        table.tokenizer_name = tokenizer_name;
        table.prefix_lengths = prefix_lengths;
        table.index = InvertedIndex::with_options(
            table.config.columnsize_enabled(),
            &table.prefix_lengths,
            table.config.detail_mode(),
        );
        Ok(table)
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // Check if there's a MATCH constraint.
        let mut has_match = false;
        let mut next_argv_index = 1;
        for (i, constraint) in info.constraints.iter().enumerate() {
            if constraint.op == fsqlite_func::vtab::ConstraintOp::Match && constraint.usable {
                info.constraint_usage[i].argv_index = next_argv_index;
                info.constraint_usage[i].omit = true;
                has_match = true;
                next_argv_index += 1;
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
            tokenizer_name: self.tokenizer_name.clone(),
            index: self.index.clone(),
            documents: self.documents.clone(),
        })
    }

    fn begin(&mut self, _cx: &Cx) -> Result<()> {
        // Capture the savepoint marker BEFORE flipping in_transaction so the
        // snapshot reflects the pre-transaction state. The marker is O(1):
        // schema scalars + pending-delta log position (which is currently 0
        // for the outermost begin).
        let snapshot = self.snapshot_state();
        self.txn_state.begin(snapshot);
        self.in_transaction = true;
        Ok(())
    }

    fn sync_txn(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
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
                if self.config.content_mode == ContentMode::Contentless
                    && !self.config.columnsize_enabled()
                {
                    return Err(FrankenError::function_error(
                        "fts5: contentless tables with columnsize=0 require an explicit rowid",
                    ));
                }
                let r = self.next_rowid;
                self.next_rowid += 1;
                r
            };
            if self.documents.contains_key(&rowid) {
                return Err(FrankenError::PrimaryKeyViolation);
            }

            let col_values: Vec<String> = args.iter().skip(2).map(SqliteValue::to_text).collect();
            self.insert_document_owned(rowid, col_values);
            return Ok(Some(rowid));
        }

        // UPDATE: validate rowid movement before mutating so conflict failures
        // preserve the old row and its index postings.
        let old_rowid = args[0].to_integer();
        let new_rowid = if args.len() > 1 && !args[1].is_null() {
            args[1].to_integer()
        } else {
            old_rowid
        };
        if old_rowid != new_rowid && self.documents.contains_key(&new_rowid) {
            return Err(FrankenError::PrimaryKeyViolation);
        }
        if !self.documents.contains_key(&old_rowid) {
            return Err(FrankenError::Internal(
                "fts5 update referenced a missing rowid".to_owned(),
            ));
        }
        self.delete_document(old_rowid);

        let col_values: Vec<String> = args.iter().skip(2).map(SqliteValue::to_text).collect();
        self.insert_document_owned(new_rowid, col_values);
        Ok(None)
    }

    fn commit(&mut self, _cx: &Cx) -> Result<()> {
        // All mutations committed; the reverse-delta log is no longer needed.
        self.txn_state.commit();
        self.pending_deltas.clear();
        self.in_transaction = false;
        Ok(())
    }

    fn rollback(&mut self, _cx: &Cx) -> Result<()> {
        if let Some(snapshot) = self.txn_state.rollback() {
            self.restore_state(snapshot);
        }
        // restore_state walked the delta log back to the begin marker (pos 0);
        // any residual entries (impossible if state was consistent) are dropped.
        self.pending_deltas.clear();
        self.in_transaction = false;
        Ok(())
    }

    fn savepoint(&mut self, _cx: &Cx, n: i32) -> Result<()> {
        self.txn_state.savepoint(n, self.snapshot_state());
        Ok(())
    }

    fn release(&mut self, _cx: &Cx, n: i32) -> Result<()> {
        self.txn_state.release(n);
        Ok(())
    }

    fn rollback_to(&mut self, _cx: &Cx, n: i32) -> Result<()> {
        if let Some(snapshot) = self.txn_state.rollback_to(n) {
            self.restore_state(snapshot);
        }
        Ok(())
    }
}

/// FTS5 cursor for scanning query results.
#[derive(Debug)]
pub struct Fts5Cursor {
    /// Matching (rowid, score) tuples.
    results: Vec<(i64, f64)>,
    /// Current position in results.
    position: usize,
    /// Column names.
    columns: Vec<String>,
    /// Tokenizer spec copied from the table at cursor-open time.
    tokenizer_name: String,
    /// Snapshot of the inverted index at cursor-open time.
    index: InvertedIndex,
    /// Snapshot of stored documents at cursor-open time.
    documents: HashMap<i64, Vec<String>>,
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
            // MATCH query: every filter arg is an additional MATCH expression
            // on the same table, combined with AND like SQLite does.
            if !args.is_empty() {
                let weights: Vec<f64> = self.columns.iter().map(|_| 1.0).collect();
                let queries: Vec<String> = args.iter().map(SqliteValue::to_text).collect();
                let query_refs: Vec<&str> = queries.iter().map(String::as_str).collect();
                let tokenizer = create_tokenizer(&self.tokenizer_name)
                    .unwrap_or_else(|| Box::new(Unicode61Tokenizer::new()));
                self.results = search_docids_with_weights_from_parts(
                    &self.index,
                    &self.columns,
                    &query_refs,
                    &weights,
                    Some(tokenizer.as_ref()),
                )
                .map_err(|e| FrankenError::function_error(format!("fts5 query error: {e}")))?;
            }
        } else {
            // Full table scan (idx_num == 0): return all documents.
            let mut rows: Vec<(i64, f64)> = self
                .documents
                .keys()
                .copied()
                .map(|rowid| (rowid, 0.0))
                .collect();
            rows.sort_by_key(|(rowid, _)| *rowid);
            self.results = rows;
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
        let Some((rowid, score)) = self.results.get(self.position) else {
            ctx.set_value(SqliteValue::Null);
            return Ok(());
        };

        #[allow(clippy::cast_sign_loss)]
        let col_idx = col as usize;

        let is_rank_column = col == -1 || (col >= 0 && col_idx == self.columns.len());

        // Column -1 or column == num_columns is the rank.
        if is_rank_column {
            ctx.set_value(SqliteValue::Float(*score));
        } else if let Some(cols) = self.documents.get(rowid)
            && let Some(val) = cols.get(col_idx)
        {
            ctx.set_value(SqliteValue::Text(SmallText::new(val.as_str())));
        } else {
            ctx.set_value(SqliteValue::Null);
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        self.results
            .get(self.position)
            .map_or(Ok(0), |(rowid, _)| Ok(*rowid))
    }
}

impl Fts5Cursor {
    /// Set the query results for this cursor (used in integrated search).
    pub fn set_results(&mut self, results: Vec<(i64, f64, Vec<String>)>) {
        self.documents = results
            .iter()
            .map(|(rowid, _, columns)| (*rowid, columns.clone()))
            .collect();
        self.results = results
            .into_iter()
            .map(|(rowid, score, _columns)| (rowid, score))
            .collect();
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

fn fallback_query_terms(query: &str) -> Vec<String> {
    query
        .split_whitespace()
        .filter_map(|raw| {
            let trimmed = raw
                .trim_matches(|ch: char| matches!(ch, '"' | '\'' | '(' | ')' | ','))
                .trim_start_matches('^')
                .trim_end_matches('*')
                .to_ascii_lowercase();
            if trimmed.is_empty() || matches!(trimmed.as_str(), "and" | "or" | "not" | "near") {
                None
            } else {
                Some(trimmed)
            }
        })
        .collect()
}

fn query_terms_from_query_text(query: &str) -> Vec<String> {
    parse_fts5_query(query)
        .and_then(|tokens| build_expr(&tokens))
        .map_or_else(
            |_| fallback_query_terms(query),
            |expr| extract_query_terms(&expr),
        )
}

/// highlight(text, query, open, close) — highlight FTS5 query terms in text.
pub struct Fts5HighlightFunc;

impl ScalarFunction for Fts5HighlightFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.iter().any(|arg| matches!(arg, SqliteValue::Null)) {
            return Ok(SqliteValue::Null);
        }

        let text = args[0].to_text();
        let query = args[1].to_text();
        let open_tag = args[2].to_text();
        let close_tag = args[3].to_text();
        let terms = query_terms_from_query_text(&query);

        Ok(SqliteValue::Text(SmallText::from_string(highlight(
            &text, &terms, &open_tag, &close_tag,
        ))))
    }

    fn num_args(&self) -> i32 {
        4
    }

    fn name(&self) -> &'static str {
        "highlight"
    }
}

/// snippet(text, query, open, close, ellipsis, max_tokens) — derive a
/// highlighted snippet from text using FTS5 query terms.
pub struct Fts5SnippetFunc;

impl ScalarFunction for Fts5SnippetFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.iter().any(|arg| matches!(arg, SqliteValue::Null)) {
            return Ok(SqliteValue::Null);
        }

        let text = args[0].to_text();
        let query = args[1].to_text();
        let open_tag = args[2].to_text();
        let close_tag = args[3].to_text();
        let ellipsis = args[4].to_text();
        let max_tokens = usize::try_from(args[5].to_integer()).unwrap_or(0);
        let terms = query_terms_from_query_text(&query);

        Ok(SqliteValue::Text(SmallText::from_string(snippet(
            &text, &terms, &open_tag, &close_tag, &ellipsis, max_tokens,
        ))))
    }

    fn num_args(&self) -> i32 {
        6
    }

    fn name(&self) -> &'static str {
        "snippet"
    }
}

/// fts5_source_id() — returns the FTS5 extension version string.
pub struct Fts5SourceIdFunc;

impl ScalarFunction for Fts5SourceIdFunc {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(SqliteValue::Text(SmallText::new(
            "fts5: FrankenSQLite FTS5 extension",
        )))
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
    registry.register_scalar(Fts5HighlightFunc);
    registry.register_scalar(Fts5SnippetFunc);
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
    fn test_fts5_query_inline_column_filter() {
        let tokens = parse_fts5_query("title:hello").unwrap();
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].kind, Fts5QueryTokenKind::ColumnFilter);
        assert_eq!(tokens[0].lexeme, "title");
        assert_eq!(tokens[1].kind, Fts5QueryTokenKind::Term);
        assert_eq!(tokens[1].lexeme, "hello");
    }

    #[test]
    fn test_fts5_query_inline_column_filter_prefix() {
        let tokens = parse_fts5_query("title:hel*").unwrap();
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].kind, Fts5QueryTokenKind::ColumnFilter);
        assert_eq!(tokens[0].lexeme, "title");
        assert_eq!(tokens[1].kind, Fts5QueryTokenKind::Prefix);
        assert_eq!(tokens[1].lexeme, "hel");
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
    fn test_inverted_index_add_text_matches_tokenized_path() {
        let tok = Unicode61Tokenizer::new();
        let text = "hello world hello";

        let mut via_tokens = InvertedIndex::new();
        via_tokens.add_document(1, 0, &tok.tokenize(text));

        let mut via_stream = InvertedIndex::new();
        via_stream.add_text(1, 0, &tok, text);

        assert_eq!(via_stream.total_docs(), via_tokens.total_docs());
        assert_eq!(
            via_stream.doc_frequency("hello"),
            via_tokens.doc_frequency("hello")
        );
        assert_eq!(
            via_stream.term_frequency("hello", 1),
            via_tokens.term_frequency("hello", 1)
        );
        assert_eq!(via_stream.doc_length(1), via_tokens.doc_length(1));

        let expected_positions: Vec<u32> = via_tokens.get_postings("hello")[0]
            .positions
            .iter()
            .copied()
            .collect();
        let actual_positions: Vec<u32> = via_stream.get_postings("hello")[0]
            .positions
            .iter()
            .copied()
            .collect();
        assert_eq!(actual_positions, expected_positions);
    }

    #[test]
    fn test_inverted_index_add_text_preserves_case_folded_terms() {
        let tok = Unicode61Tokenizer::new();
        let text = "HELLO hello";

        let mut via_tokens = InvertedIndex::new();
        via_tokens.add_document(1, 0, &tok.tokenize(text));

        let mut via_stream = InvertedIndex::new();
        via_stream.add_text(1, 0, &tok, text);

        assert_eq!(
            via_stream.doc_frequency("hello"),
            via_tokens.doc_frequency("hello")
        );
        assert_eq!(
            via_stream.term_frequency("hello", 1),
            via_tokens.term_frequency("hello", 1)
        );
    }

    #[test]
    fn test_inverted_index_add_text_preserves_porter_terms() {
        let tok = PorterTokenizer::new(Box::new(Unicode61Tokenizer::new()));
        let text = "CATS cats";

        let mut via_tokens = InvertedIndex::new();
        via_tokens.add_document(1, 0, &tok.tokenize(text));

        let mut via_stream = InvertedIndex::new();
        via_stream.add_text(1, 0, &tok, text);

        assert_eq!(
            via_stream.doc_frequency("cat"),
            via_tokens.doc_frequency("cat")
        );
        assert_eq!(
            via_stream.term_frequency("cat", 1),
            via_tokens.term_frequency("cat", 1)
        );
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

    #[test]
    fn test_prefix_index_population_and_cleanup() {
        let mut index = InvertedIndex::with_options(true, &[3], DetailMode::Full);
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello help"));
        index.add_document(2, 0, &tok.tokenize("world"));

        let results = index.get_prefix_postings("hel");
        let docs: Vec<i64> = results.iter().map(|posting| posting.docid).collect();
        assert!(index.tracks_prefix_length(3));
        assert!(docs.contains(&1));
        assert!(!docs.contains(&2));

        index.remove_document(1);
        assert!(index.get_prefix_postings("hel").is_empty());
    }

    #[test]
    fn test_inverted_index_detail_none_collapses_columns() {
        let mut index = InvertedIndex::with_options(true, &[], DetailMode::None);
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello"));
        index.add_document(1, 1, &tok.tokenize("hello"));

        let postings = index.get_postings("hello");
        assert_eq!(index.detail_mode(), DetailMode::None);
        assert_eq!(postings.len(), 1);
        assert_eq!(postings[0].column, 0);
        assert_eq!(postings[0].positions.len(), 2);
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
        table.insert_document(2, &["rust lang".to_owned()]);
        table.insert_document(3, &["goodbye world".to_owned()]);

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
    fn test_fts5_table_search_column_filter() {
        let mut table = Fts5Table::with_columns(vec!["title".to_owned(), "body".to_owned()]);

        table.insert_document(1, &["Rust title".to_owned(), "plain body".to_owned()]);
        table.insert_document(2, &["Plain title".to_owned(), "rust body".to_owned()]);

        let title_results = table.search("title:rust").unwrap();
        assert_eq!(title_results.len(), 1);
        assert_eq!(title_results[0].0, 1);

        let body_results = table.search("body:rust").unwrap();
        assert_eq!(body_results.len(), 1);
        assert_eq!(body_results[0].0, 2);
    }

    #[test]
    fn test_fts5_table_search_invalid_column_filter() {
        let mut table = Fts5Table::with_columns(vec!["title".to_owned(), "body".to_owned()]);
        table.insert_document(1, &["Rust title".to_owned(), "plain body".to_owned()]);

        let error = table
            .search("summary:rust")
            .expect_err("unknown column filters should fail");
        assert_eq!(
            error,
            Fts5QueryError::InvalidColumnFilter("summary".to_owned())
        );
    }

    #[test]
    fn test_fts5_table_detail_column_rejects_offset_queries() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &["fts5", "main", "docs", "title", "body", "detail=column"],
        )
        .unwrap();
        table.insert_document(1, &["rust title".to_owned(), "rust body".to_owned()]);

        let term_results = table.search("title:rust").unwrap();
        assert_eq!(term_results.len(), 1);
        assert_eq!(term_results[0].0, 1);

        assert_eq!(
            table.search(r#""rust title""#).unwrap_err(),
            Fts5QueryError::UnsupportedByDetailMode {
                detail: DetailMode::Column,
                feature: "phrase queries",
            }
        );
        assert_eq!(
            table.search("NEAR(rust title, 5)").unwrap_err(),
            Fts5QueryError::UnsupportedByDetailMode {
                detail: DetailMode::Column,
                feature: "NEAR queries",
            }
        );
        assert_eq!(
            table.search("^rust").unwrap_err(),
            Fts5QueryError::UnsupportedByDetailMode {
                detail: DetailMode::Column,
                feature: "initial-token queries",
            }
        );
    }

    #[test]
    fn test_fts5_table_detail_none_rejects_column_filters() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &["fts5", "main", "docs", "title", "body", "detail=none"],
        )
        .unwrap();
        table.insert_document(1, &["rust title".to_owned(), "plain body".to_owned()]);
        table.insert_document(2, &["plain title".to_owned(), "rust body".to_owned()]);

        let results = table.search("rust").unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            table.search("title:rust").unwrap_err(),
            Fts5QueryError::UnsupportedByDetailMode {
                detail: DetailMode::None,
                feature: "column filters",
            }
        );
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
                "columnsize=0",
                "detail='column'",
                "prefix='2 3'",
            ],
        )
        .unwrap();
        assert_eq!(vtab.columns(), &["title", "body"]);
        assert_eq!(vtab.tokenizer_name, "porter");
        assert_eq!(vtab.config.content_mode(), ContentMode::Contentless);
        assert!(vtab.config.contentless_delete_enabled());
        assert!(!vtab.config.columnsize_enabled());
        assert_eq!(vtab.config.detail_mode(), DetailMode::Column);
        assert_eq!(vtab.prefix_lengths, vec![2, 3]);
        assert!(!vtab.index().tracks_column_sizes());
        assert_eq!(vtab.index().detail_mode(), DetailMode::Column);
        assert!(vtab.index().tracks_prefix_length(2));
        assert!(vtab.index().tracks_prefix_length(3));
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_unknown_option() {
        let cx = Cx::new();
        let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "mystery=1"])
            .expect_err("unsupported option should fail");
        assert!(err.to_string().contains("unsupported option"));
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_invalid_columnsize() {
        let cx = Cx::new();
        let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "columnsize=2"])
            .expect_err("invalid columnsize should fail");
        assert!(err.to_string().contains("columnsize must be 0 or 1"));
    }

    #[test]
    fn test_fts5_vtab_connect_accumulates_prefix_options() {
        let cx = Cx::new();
        let vtab = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "title",
                "prefix='3 2'",
                "prefix='4 3'",
            ],
        )
        .unwrap();

        assert_eq!(vtab.prefix_lengths, vec![2, 3, 4]);
        assert!(vtab.index().tracks_prefix_length(2));
        assert!(vtab.index().tracks_prefix_length(3));
        assert!(vtab.index().tracks_prefix_length(4));
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_invalid_prefix_option() {
        let cx = Cx::new();
        let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "prefix='2 0'"])
            .expect_err("invalid prefix should fail");
        assert!(
            err.to_string()
                .contains("prefix must be a whitespace separated list of positive integers")
        );
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_invalid_detail() {
        let cx = Cx::new();
        let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "detail='offsets'"])
            .expect_err("invalid detail should fail");
        assert!(
            err.to_string()
                .contains("detail must be full, column, or none")
        );
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
                    SqliteValue::Text(SmallText::from_string("hello world")),
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
                SqliteValue::Text(SmallText::from_string("hello")),
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
        assert!(registry.find_scalar("highlight", 4).is_some());
        assert!(registry.find_scalar("snippet", 6).is_some());
        assert!(registry.find_scalar("fts5_source_id", 0).is_some());
    }

    #[test]
    fn test_highlight_scalar_func_uses_query_text() {
        let func = Fts5HighlightFunc;
        let result = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("the quick brown fox")),
                SqliteValue::Text(SmallText::from_string("quick OR fox")),
                SqliteValue::Text(SmallText::from_string("<b>")),
                SqliteValue::Text(SmallText::from_string("</b>")),
            ])
            .unwrap();

        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string("the <b>quick</b> brown <b>fox</b>"))
        );
    }

    #[test]
    fn test_highlight_scalar_func_falls_back_for_invalid_query() {
        let func = Fts5HighlightFunc;
        let result = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("hello world")),
                SqliteValue::Text(SmallText::from_string("(hello")),
                SqliteValue::Text(SmallText::from_string("<b>")),
                SqliteValue::Text(SmallText::from_string("</b>")),
            ])
            .unwrap();

        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string("<b>hello</b> world"))
        );
    }

    #[test]
    fn test_snippet_scalar_func_uses_query_text() {
        let func = Fts5SnippetFunc;
        let result = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("alpha beta gamma delta epsilon")),
                SqliteValue::Text(SmallText::from_string("delta")),
                SqliteValue::Text(SmallText::from_string("[")),
                SqliteValue::Text(SmallText::from_string("]")),
                SqliteValue::Text(SmallText::from_string("...")),
                SqliteValue::Integer(3),
            ])
            .unwrap();

        let SqliteValue::Text(snippet_text) = result else {
            panic!("snippet() should return text");
        };
        assert!(snippet_text.contains("[delta]"));
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
    fn test_doc_length_tracking_falls_back_when_columnsize_disabled() {
        let mut index = InvertedIndex::with_column_sizes(false);
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("one two"));
        index.add_document(1, 1, &tok.tokenize("three"));
        index.add_document(2, 0, &tok.tokenize("solo"));

        assert!(!index.tracks_column_sizes());
        assert_eq!(index.doc_length(1), 3);
        assert_eq!(index.doc_length(2), 1);
        assert!((index.avg_doc_length() - 2.0).abs() < f64::EPSILON);

        index.remove_document(1);
        assert_eq!(index.total_docs(), 1);
        assert_eq!(index.doc_length(1), 0);
        assert_eq!(index.doc_length(2), 1);
    }

    #[test]
    fn test_fts5_config_default() {
        let config = Fts5Config::default();
        assert_eq!(config.content_mode(), ContentMode::Stored);
        assert!(!config.secure_delete_enabled());
        assert!(!config.contentless_delete_enabled());
        assert!(config.columnsize_enabled());
        assert_eq!(config.detail_mode(), DetailMode::Full);
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
        assert_eq!(
            format!(
                "{}",
                Fts5QueryError::UnsupportedByDetailMode {
                    detail: DetailMode::Column,
                    feature: "phrase queries",
                }
            ),
            "detail=column does not support phrase queries"
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
    fn test_tokenizer_spec_preserves_unicode61_tokenchars() {
        let tok = create_tokenizer("unicode61 tokenchars '-_./:@#$%'").unwrap();
        let tokens = tok.tokenize("AuthController.ts my_function");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();

        assert_eq!(terms, vec!["authcontroller.ts", "my_function"]);
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
    fn test_build_expr_near_default_distance() {
        let tokens = parse_fts5_query("NEAR(hello world)").unwrap();
        let expr = build_expr(&tokens).unwrap();
        match expr {
            Fts5Expr::Near(terms, distance) => {
                assert_eq!(terms, vec!["hello".to_owned(), "world".to_owned()]);
                assert_eq!(distance, 10);
            }
            other => panic!("expected NEAR expression, got {other:?}"),
        }
    }

    #[test]
    fn test_build_expr_near_explicit_distance() {
        let tokens = parse_fts5_query("NEAR(hello world, 5)").unwrap();
        let expr = build_expr(&tokens).unwrap();
        match expr {
            Fts5Expr::Near(terms, distance) => {
                assert_eq!(terms, vec!["hello".to_owned(), "world".to_owned()]);
                assert_eq!(distance, 5);
            }
            other => panic!("expected NEAR expression, got {other:?}"),
        }
    }

    #[test]
    fn test_build_expr_near_inline_distance_token() {
        let tokens = parse_fts5_query("NEAR(hello world,5)").unwrap();
        let expr = build_expr(&tokens).unwrap();
        match expr {
            Fts5Expr::Near(terms, distance) => {
                assert_eq!(terms, vec!["hello".to_owned(), "world".to_owned()]);
                assert_eq!(distance, 5);
            }
            other => panic!("expected NEAR expression, got {other:?}"),
        }
    }

    #[test]
    fn test_build_expr_near_rejects_boolean_operator() {
        let tokens = parse_fts5_query("NEAR(hello OR world)").unwrap();
        let err = build_expr(&tokens).unwrap_err();
        assert_eq!(err, Fts5QueryError::InvalidNearSyntax);
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
        let columns = vec!["title".to_owned(), "body".to_owned()];
        index.add_document(1, 0, &tok.tokenize("hello title"));
        index.add_document(1, 1, &tok.tokenize("plain body"));
        index.add_document(2, 0, &tok.tokenize("plain title"));
        index.add_document(2, 1, &tok.tokenize("hello body"));

        let expr = Fts5Expr::ColumnFilter(
            "title".to_owned(),
            Box::new(Fts5Expr::Term("hello".to_owned())),
        );
        let docs = evaluate_expr_for_columns(&index, &expr, &columns);
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
    fn test_fts5_table_search_normalizes_query_with_porter_tokenizer() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "content",
                "tokenize='porter unicode61 remove_diacritics 2'",
            ],
        )
        .unwrap();
        table.insert_document(1, &["I am running the tests".to_owned()]);

        let results = table.search("\"running\"").unwrap();

        assert_eq!(
            results.iter().map(|(rowid, _)| *rowid).collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn test_fts5_table_search_normalizes_query_with_code_tokenchars() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "content",
                "tokenize=\"unicode61 tokenchars '-_./:@#$%'\"",
            ],
        )
        .unwrap();
        table.insert_document(1, &["Call my_function in AuthController.ts".to_owned()]);

        let identifier_results = table.search("\"my_function\"").unwrap();
        let filename_results = table.search("\"AuthController.ts\"").unwrap();

        assert_eq!(
            identifier_results
                .iter()
                .map(|(rowid, _)| *rowid)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(
            filename_results
                .iter()
                .map(|(rowid, _)| *rowid)
                .collect::<Vec<_>>(),
            vec![1]
        );
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
                    SqliteValue::Text(SmallText::from_string("first")),
                ],
            )
            .unwrap();
        let r2 = vtab
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Null,
                    SqliteValue::Text(SmallText::from_string("second")),
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
                SqliteValue::Text(SmallText::from_string("original")),
            ],
        )
        .unwrap();

        // Update: old_rowid=1, new_rowid=1, new_content="modified"
        vtab.update(
            &cx,
            &[
                SqliteValue::Integer(1),
                SqliteValue::Integer(1),
                SqliteValue::Text(SmallText::from_string("modified")),
            ],
        )
        .unwrap();

        let doc = vtab.get_document(1).unwrap();
        assert_eq!(doc, &["modified"]);
        assert!(vtab.search("original").unwrap().is_empty());
        assert_eq!(vtab.search("modified").unwrap()[0].0, 1);
    }

    #[test]
    fn test_fts5_vtab_insert_duplicate_rowid_preserves_original() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();

        vtab.update(
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Integer(1),
                SqliteValue::Text(SmallText::from_string("alpha original")),
            ],
        )
        .unwrap();

        let err = vtab
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(1),
                    SqliteValue::Text(SmallText::from_string("beta duplicate")),
                ],
            )
            .unwrap_err();
        assert!(matches!(err, FrankenError::PrimaryKeyViolation));

        assert_eq!(vtab.get_document(1).unwrap(), &["alpha original"]);
        assert_eq!(vtab.search("alpha").unwrap()[0].0, 1);
        assert!(vtab.search("beta").unwrap().is_empty());
    }

    #[test]
    fn test_fts5_vtab_update_rowid_conflict_preserves_original_entries() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();

        for (rowid, text) in [(1, "alpha one"), (2, "beta two")] {
            vtab.update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(rowid),
                    SqliteValue::Text(SmallText::from_string(text)),
                ],
            )
            .unwrap();
        }

        let err = vtab
            .update(
                &cx,
                &[
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(2),
                    SqliteValue::Text(SmallText::from_string("gamma conflict")),
                ],
            )
            .unwrap_err();
        assert!(matches!(err, FrankenError::PrimaryKeyViolation));

        assert_eq!(vtab.get_document(1).unwrap(), &["alpha one"]);
        assert_eq!(vtab.get_document(2).unwrap(), &["beta two"]);
        assert_eq!(vtab.search("alpha").unwrap()[0].0, 1);
        assert_eq!(vtab.search("beta").unwrap()[0].0, 2);
        assert!(vtab.search("gamma").unwrap().is_empty());
    }

    #[test]
    fn test_fts5_insert_document_replaces_existing_rowid_index_entries() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.insert_document(1, &["old token".to_owned()]);
        table.insert_document(1, &["new token".to_owned()]);

        assert_eq!(table.get_document(1).unwrap(), &["new token"]);
        assert!(table.search("old").unwrap().is_empty());
        assert_eq!(table.search("new").unwrap()[0].0, 1);
    }

    #[test]
    fn test_fts5_vtab_update_empty_args() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();
        assert!(vtab.update(&cx, &[]).is_err());
    }

    #[test]
    fn test_fts5_rebuild_documents_preserves_documents_and_next_rowid() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        table.insert_document(1, &["stale".to_owned()]);

        table.rebuild_documents(vec![
            (3, vec!["hello world".to_owned()]),
            (7, vec!["second doc".to_owned()]),
        ]);

        let expected_doc_3 = vec!["hello world".to_owned()];
        let expected_doc_7 = vec!["second doc".to_owned()];
        assert_eq!(table.get_document(3), Some(expected_doc_3.as_slice()));
        assert_eq!(table.get_document(7), Some(expected_doc_7.as_slice()));
        assert!(table.get_document(1).is_none());

        let cx = Cx::new();
        let rowid = table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Null,
                    SqliteValue::Text(SmallText::from_string("third doc")),
                ],
            )
            .expect("insert after rebuild")
            .expect("rowid");
        assert_eq!(rowid, 8);
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
    fn test_fts5_vtab_contentless_columnsize_zero_requires_explicit_rowid() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(
            &cx,
            &["fts5", "main", "t", "content", "content=''", "columnsize=0"],
        )
        .unwrap();

        let result = vtab.update(
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Null,
                SqliteValue::Text(SmallText::from_string("contentless row")),
            ],
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("require an explicit rowid")
        );
    }

    #[test]
    fn test_fts5_cursor_set_results() {
        let mut cursor = Fts5Cursor {
            results: Vec::new(),
            position: 0,
            columns: vec!["content".to_owned()],
            tokenizer_name: "unicode61".to_owned(),
            index: InvertedIndex::new(),
            documents: HashMap::new(),
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
            tokenizer_name: "unicode61".to_owned(),
            index: InvertedIndex::new(),
            documents: HashMap::new(),
        };

        cursor.set_results(vec![(1, -1.0, vec!["hello world".to_owned()])]);

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 0).unwrap();
        assert_eq!(
            ctx.take_value(),
            Some(SqliteValue::Text(SmallText::from_string("hello world")))
        );

        // Rank column (beyond column count).
        let mut ctx2 = ColumnContext::new();
        cursor.column(&mut ctx2, 1).unwrap();
        assert_eq!(ctx2.take_value(), Some(SqliteValue::Float(-1.0)));
    }

    #[test]
    fn test_fts5_cursor_column_past_end_returns_null() {
        let mut cursor = Fts5Cursor {
            results: Vec::new(),
            position: 0,
            columns: vec!["content".to_owned()],
            tokenizer_name: "unicode61".to_owned(),
            index: InvertedIndex::new(),
            documents: HashMap::new(),
        };

        cursor.set_results(vec![(1, -1.0, vec!["hello world".to_owned()])]);

        let cx = Cx::new();
        cursor.next(&cx).unwrap();
        assert!(cursor.eof());

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 0).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
    }

    #[test]
    fn test_fts5_cursor_column_out_of_range_returns_null() {
        let mut cursor = Fts5Cursor {
            results: Vec::new(),
            position: 0,
            columns: vec!["content".to_owned()],
            tokenizer_name: "unicode61".to_owned(),
            index: InvertedIndex::new(),
            documents: HashMap::new(),
        };

        cursor.set_results(vec![(1, -1.0, vec!["hello world".to_owned()])]);

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 2).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
    }

    #[test]
    fn test_fts5_cursor_column_negative_out_of_range_returns_null() {
        let mut cursor = Fts5Cursor {
            results: Vec::new(),
            position: 0,
            columns: vec!["content".to_owned()],
            tokenizer_name: "unicode61".to_owned(),
            index: InvertedIndex::new(),
            documents: HashMap::new(),
        };

        cursor.set_results(vec![(1, -1.0, vec!["hello world".to_owned()])]);

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, -2).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
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
        assert_eq!(table.config().detail_mode(), DetailMode::Full);
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

    #[test]
    fn test_fts5_vtab_best_index_assigns_distinct_match_argv_slots() {
        let cx = Cx::new();
        let vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();
        let mut info = IndexInfo::new(
            vec![
                fsqlite_func::vtab::IndexConstraint {
                    column: 0,
                    op: fsqlite_func::vtab::ConstraintOp::Match,
                    usable: true,
                },
                fsqlite_func::vtab::IndexConstraint {
                    column: 0,
                    op: fsqlite_func::vtab::ConstraintOp::Match,
                    usable: true,
                },
            ],
            vec![],
        );
        vtab.best_index(&mut info).unwrap();
        assert_eq!(info.idx_num, 1);
        assert_eq!(info.constraint_usage[0].argv_index, 1);
        assert_eq!(info.constraint_usage[1].argv_index, 2);
        assert!(info.constraint_usage.iter().all(|usage| usage.omit));
    }

    #[test]
    fn test_fts5_cursor_filter_intersects_multiple_match_args() {
        let cx = Cx::new();
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        table.insert_document(1, &["hello world".to_owned()]);
        table.insert_document(2, &["hello rust".to_owned()]);
        table.insert_document(3, &["world only".to_owned()]);

        let mut cursor = table.open().unwrap();
        cursor
            .filter(
                &cx,
                1,
                None,
                &[
                    SqliteValue::Text(SmallText::from_string("hello")),
                    SqliteValue::Text(SmallText::from_string("world")),
                ],
            )
            .unwrap();

        assert_eq!(cursor.rowid().unwrap(), 1);
        cursor.next(&cx).unwrap();
        assert!(cursor.eof());
    }

    // -- Savepoint / rollback / delta-journal tests --
    //
    // These tests exercise the reverse-delta path that replaces the previous
    // eager-clone snapshot. They are the regression coverage for the spec-014
    // memset stall fix: with the eager-clone version, every INSERT into the
    // FTS5 vtab cloned the entire in-memory inverted index inside the SQLite
    // statement-level savepoint. With the delta journal, snapshot_state is
    // O(1) and rollback walks an O(deltas-since-savepoint) reverse log.

    #[test]
    fn test_snapshot_state_does_not_clone_index_or_documents() {
        // Snapshot must capture only the schema scalars + delta-log position,
        // not a copy of the index or documents. We can't directly assert "no
        // memmove happened", but we can assert the structural property:
        // taking a snapshot does not change anything observable about the
        // table state, AND the snapshot type does not own its own copy of the
        // index / documents (the type only has small scalars + a usize).
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        table.insert_document(1, &["alpha bravo charlie".to_owned()]);

        let initial_doc_count = table.documents.len();
        let initial_index_total = table.index.total_docs();

        // snapshot_state must not mutate the table.
        let snap = table.snapshot_state();
        assert_eq!(table.documents.len(), initial_doc_count);
        assert_eq!(table.index.total_docs(), initial_index_total);
        assert_eq!(snap.deltas_at_savepoint, 0);

        // The snapshot retains the schema scalars but does NOT carry an
        // InvertedIndex / HashMap field. This is enforced by the type
        // definition; we re-verify by exercising the move below.
        let _moved = snap.clone();
        let _moved2 = _moved.clone();
        // Clones of the marker must be O(1). If snapshot_state grew an index
        // field again, this test would still pass at runtime — the type-level
        // protection is the absence of that field. The runtime check we can
        // make: cloning the marker many times is fast and harmless.
        for _ in 0..10_000 {
            let _ = table.snapshot_state();
        }
        // Table still observably unchanged after 10K snapshots.
        assert_eq!(table.documents.len(), initial_doc_count);
        assert_eq!(table.index.total_docs(), initial_index_total);
    }

    #[test]
    fn test_begin_commit_inserts_persist_and_clear_delta_log() {
        let cx = Cx::new();
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.begin(&cx).unwrap();
        assert!(table.in_transaction);
        table.insert_document(1, &["alpha bravo".to_owned()]);
        table.insert_document(2, &["charlie delta".to_owned()]);
        assert_eq!(table.pending_deltas.len(), 2);
        table.commit(&cx).unwrap();

        // After commit, deltas are gone and the rows persist.
        assert!(!table.in_transaction);
        assert_eq!(table.pending_deltas.len(), 0);
        assert_eq!(table.documents.len(), 2);
        assert!(table.documents.contains_key(&1));
        assert!(table.documents.contains_key(&2));
        // Index still has the inserted content.
        let results = table.search("alpha").unwrap();
        assert!(results.iter().any(|(rowid, _)| *rowid == 1));
    }

    #[test]
    fn test_full_rollback_undoes_all_inserts() {
        let cx = Cx::new();
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.begin(&cx).unwrap();
        table.insert_document(1, &["alpha bravo".to_owned()]);
        table.insert_document(2, &["charlie delta".to_owned()]);
        assert_eq!(table.documents.len(), 2);
        assert_eq!(table.pending_deltas.len(), 2);

        table.rollback(&cx).unwrap();

        // After rollback, the table is back to pre-begin state.
        assert!(!table.in_transaction);
        assert_eq!(table.pending_deltas.len(), 0);
        assert_eq!(table.documents.len(), 0);
        assert_eq!(table.index.total_docs(), 0);
        let results = table.search("alpha").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_savepoint_release_keeps_inserts() {
        let cx = Cx::new();
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.begin(&cx).unwrap();
        table.savepoint(&cx, 1).unwrap();
        table.insert_document(1, &["alpha bravo".to_owned()]);
        table.savepoint(&cx, 2).unwrap();
        table.insert_document(2, &["charlie delta".to_owned()]);
        table.release(&cx, 2).unwrap();
        table.release(&cx, 1).unwrap();

        // Both rows present; deltas still pending until commit.
        assert_eq!(table.documents.len(), 2);
        assert_eq!(table.pending_deltas.len(), 2);

        table.commit(&cx).unwrap();
        assert_eq!(table.documents.len(), 2);
        assert_eq!(table.pending_deltas.len(), 0);
    }

    #[test]
    fn test_rollback_to_inner_savepoint_keeps_outer_inserts() {
        let cx = Cx::new();
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.begin(&cx).unwrap();
        table.savepoint(&cx, 1).unwrap();
        table.insert_document(1, &["alpha".to_owned()]); // kept
        table.savepoint(&cx, 2).unwrap();
        table.insert_document(2, &["bravo".to_owned()]); // rolled back
        table.insert_document(3, &["charlie".to_owned()]); // rolled back

        assert_eq!(table.documents.len(), 3);
        assert_eq!(table.pending_deltas.len(), 3);

        table.rollback_to(&cx, 2).unwrap();

        // Inserts after savepoint 2 are undone; insert before it is kept.
        assert_eq!(table.documents.len(), 1);
        assert!(table.documents.contains_key(&1));
        assert_eq!(table.pending_deltas.len(), 1);

        // Search confirms index is consistent with documents.
        assert_eq!(table.search("alpha").unwrap().len(), 1);
        assert_eq!(table.search("bravo").unwrap().len(), 0);
        assert_eq!(table.search("charlie").unwrap().len(), 0);

        // We're still inside savepoint 1; commit completes the txn cleanly.
        table.release(&cx, 1).unwrap();
        table.commit(&cx).unwrap();
        assert_eq!(table.documents.len(), 1);
    }

    #[test]
    fn test_rollback_undoes_delete_of_pre_existing_row() {
        let cx = Cx::new();
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        // Pre-existing row, inserted OUTSIDE any transaction.
        table.insert_document(1, &["alpha bravo".to_owned()]);
        assert_eq!(table.documents.len(), 1);
        assert_eq!(table.pending_deltas.len(), 0); // no transaction, no delta

        // Now open a txn and delete the row.
        table.begin(&cx).unwrap();
        table.delete_document(1);
        assert_eq!(table.documents.len(), 0);
        assert_eq!(table.pending_deltas.len(), 1);

        // Rollback should re-create the row with its original columns.
        table.rollback(&cx).unwrap();
        assert_eq!(table.documents.len(), 1);
        assert_eq!(table.pending_deltas.len(), 0);
        let results = table.search("alpha").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
    }

    #[test]
    fn test_rollback_undoes_update_of_pre_existing_row() {
        let cx = Cx::new();
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        table.insert_document(1, &["original content".to_owned()]);
        assert_eq!(table.search("original").unwrap().len(), 1);

        table.begin(&cx).unwrap();
        // Update row 1's content via the public store path.
        table.insert_document(1, &["replaced content".to_owned()]);
        assert_eq!(table.pending_deltas.len(), 1);
        // Old term gone, new term present.
        assert_eq!(table.search("original").unwrap().len(), 0);
        assert_eq!(table.search("replaced").unwrap().len(), 1);

        table.rollback(&cx).unwrap();
        // Old content restored, new content gone.
        assert_eq!(table.search("original").unwrap().len(), 1);
        assert_eq!(table.search("replaced").unwrap().len(), 0);
    }

    #[test]
    fn test_mutations_outside_transaction_do_not_record_deltas() {
        // Mutations before xBegin (e.g. during table construction, or via
        // `rebuild_documents`) must not accumulate spurious deltas. If they
        // did, the first xBegin's snapshot would be at offset > 0 and a
        // subsequent rollback could not roll them back (and shouldn't).
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        for i in 1..=50 {
            table.insert_document(i, &[format!("row {}", i)]);
        }
        assert_eq!(table.pending_deltas.len(), 0);
        assert_eq!(table.documents.len(), 50);
    }

    #[test]
    fn test_many_inserts_inside_transaction_grow_log_linearly() {
        // Sanity that pending_deltas.len() == number of mutations recorded.
        // This is the structural property that lets us argue snapshot cost
        // is O(deltas-since-savepoint), not O(state).
        let cx = Cx::new();
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        table.begin(&cx).unwrap();
        for i in 1..=200 {
            table.insert_document(i, &[format!("row {}", i)]);
        }
        assert_eq!(table.pending_deltas.len(), 200);
        // Taking a savepoint at this point is O(1):
        let marker = table.snapshot_state();
        assert_eq!(marker.deltas_at_savepoint, 200);

        // Insert 50 more, rollback to the marker, all 50 are undone but the
        // first 200 stay.
        for i in 201..=250 {
            table.insert_document(i, &[format!("row {}", i)]);
        }
        assert_eq!(table.pending_deltas.len(), 250);
        table.restore_state(marker);
        assert_eq!(table.pending_deltas.len(), 200);
        assert_eq!(table.documents.len(), 200);
    }
}
