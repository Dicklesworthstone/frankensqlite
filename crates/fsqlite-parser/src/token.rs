// bd-2tu6: §10.1 SQL Token Types
//
// Every SQL token carries a discriminant and a byte-offset Span.
// Keywords are their own variants for O(1) matching in the parser.

use fsqlite_ast::Span;

/// A single token produced by the lexer.
#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    /// The token discriminant.
    pub kind: TokenKind,
    /// Byte-offset span into the original source.
    pub span: Span,
    /// Line number (1-based) at the start of the token.
    pub line: u32,
    /// Column number (1-based) at the start of the token.
    pub col: u32,
}

/// Token discriminant.
///
/// Organized by category: literals, identifiers, keywords (~120), operators,
/// punctuation, and special tokens.
#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    // === Literals ===
    /// Integer literal: `42`, `-7`, `0xFF`.
    Integer(i64),
    /// Float literal: `3.14`, `1e10`, `.5`.
    Float(f64),
    /// String literal (single-quoted): `'hello'`.
    String(String),
    /// Blob literal: `X'CAFE'`.
    Blob(Vec<u8>),

    // === Identifiers ===
    /// Unquoted identifier.
    Id(String),
    /// Quoted identifier (`"name"`, `[name]`, `` `name` ``).
    /// The bool is the EP_DblQuoted flag (true if double-quoted).
    QuotedId(String, bool),

    // === Variables / bind parameters ===
    /// `?` anonymous positional.
    Question,
    /// `?NNN` numbered positional.
    QuestionNum(u32),
    /// `:name` colon-prefixed named.
    ColonParam(String),
    /// `@name` at-prefixed named.
    AtParam(String),
    /// `$name` dollar-prefixed named.
    DollarParam(String),

    // === Operators ===
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Ampersand,
    Pipe,
    Tilde,
    ShiftLeft,
    ShiftRight,
    Eq,   // `=`
    EqEq, // `==`
    Ne,   // `!=`
    LtGt, // `<>`
    Lt,
    Le,
    Gt,
    Ge,
    Concat,      // `||`
    Arrow,       // `->`
    DoubleArrow, // `->>`

    // === Punctuation ===
    Dot,
    Comma,
    Semicolon,
    LeftParen,
    RightParen,

    // === Keywords ===
    KwAbort,
    KwAction,
    KwAdd,
    KwAfter,
    KwAll,
    KwAlter,
    KwAlways,
    KwAnalyze,
    KwAnd,
    KwAs,
    KwAsc,
    KwAttach,
    KwAutoincrement,
    KwBefore,
    KwBegin,
    KwBetween,
    KwBy,
    KwCascade,
    KwCase,
    KwCast,
    KwCheck,
    KwCollate,
    KwColumn,
    KwCommit,
    KwConcurrent,
    KwConflict,
    KwConstraint,
    KwCreate,
    KwCross,
    KwCurrentDate,
    KwCurrentTime,
    KwCurrentTimestamp,
    KwDatabase,
    KwDefault,
    KwDeferrable,
    KwDeferred,
    KwDelete,
    KwDesc,
    KwDetach,
    KwDistinct,
    KwDo,
    KwDrop,
    KwEach,
    KwElse,
    KwEnd,
    KwEscape,
    KwExcept,
    KwExclude,
    KwExclusive,
    KwExists,
    KwExplain,
    KwFail,
    KwFilter,
    KwFirst,
    KwFollowing,
    KwFor,
    KwForeign,
    KwFrom,
    KwFull,
    KwGenerated,
    KwGlob,
    KwGroup,
    KwGroups,
    KwHaving,
    KwIf,
    KwIgnore,
    KwImmediate,
    KwIn,
    KwIndex,
    KwIndexed,
    KwInitially,
    KwInner,
    KwInsert,
    KwInstead,
    KwIntersect,
    KwInto,
    KwIs,
    KwIsnull,
    KwJoin,
    KwKey,
    KwLast,
    KwLeft,
    KwLike,
    KwLimit,
    KwMatch,
    KwMaterialized,
    KwNatural,
    KwNo,
    KwNot,
    KwNothing,
    KwNotnull,
    KwNull,
    KwNulls,
    KwOf,
    KwOffset,
    KwOn,
    KwOr,
    KwOrder,
    KwOthers,
    KwOuter,
    KwOver,
    KwPartition,
    KwPlan,
    KwPragma,
    KwPreceding,
    KwPrimary,
    KwQuery,
    KwRaise,
    KwRange,
    KwRecursive,
    KwReferences,
    KwRegexp,
    KwReindex,
    KwRelease,
    KwRename,
    KwReplace,
    KwRestrict,
    KwReturning,
    KwRight,
    KwRollback,
    KwRow,
    KwRows,
    KwSavepoint,
    KwSelect,
    KwSet,
    KwStored,
    KwStrict,
    KwTable,
    KwTemp,
    KwTemporary,
    KwThen,
    KwTies,
    KwTo,
    KwTransaction,
    KwTrigger,
    KwTrue,
    KwFalse,
    KwUnbounded,
    KwUnion,
    KwUnique,
    KwUpdate,
    KwUsing,
    KwVacuum,
    KwValues,
    KwView,
    KwVirtual,
    KwWhen,
    KwWhere,
    KwWindow,
    KwWith,
    KwWithout,

    // === Special ===
    /// End of input.
    Eof,
    /// Lexer error (invalid input).
    Error(String),
}

impl TokenKind {
    /// Look up an identifier string to see if it's a keyword.
    /// Returns the keyword variant if so, else `None`.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn lookup_keyword(s: &str) -> Option<Self> {
        // Case-insensitive keyword matching.
        // We uppercase for comparison since SQL keywords are case-insensitive.
        match s.to_ascii_uppercase().as_str() {
            "ABORT" => Some(Self::KwAbort),
            "ACTION" => Some(Self::KwAction),
            "ADD" => Some(Self::KwAdd),
            "AFTER" => Some(Self::KwAfter),
            "ALL" => Some(Self::KwAll),
            "ALTER" => Some(Self::KwAlter),
            "ALWAYS" => Some(Self::KwAlways),
            "ANALYZE" => Some(Self::KwAnalyze),
            "AND" => Some(Self::KwAnd),
            "AS" => Some(Self::KwAs),
            "ASC" => Some(Self::KwAsc),
            "ATTACH" => Some(Self::KwAttach),
            "AUTOINCREMENT" => Some(Self::KwAutoincrement),
            "BEFORE" => Some(Self::KwBefore),
            "BEGIN" => Some(Self::KwBegin),
            "BETWEEN" => Some(Self::KwBetween),
            "BY" => Some(Self::KwBy),
            "CASCADE" => Some(Self::KwCascade),
            "CASE" => Some(Self::KwCase),
            "CAST" => Some(Self::KwCast),
            "CHECK" => Some(Self::KwCheck),
            "COLLATE" => Some(Self::KwCollate),
            "COLUMN" => Some(Self::KwColumn),
            "COMMIT" => Some(Self::KwCommit),
            "CONCURRENT" => Some(Self::KwConcurrent),
            "CONFLICT" => Some(Self::KwConflict),
            "CONSTRAINT" => Some(Self::KwConstraint),
            "CREATE" => Some(Self::KwCreate),
            "CROSS" => Some(Self::KwCross),
            "CURRENT_DATE" => Some(Self::KwCurrentDate),
            "CURRENT_TIME" => Some(Self::KwCurrentTime),
            "CURRENT_TIMESTAMP" => Some(Self::KwCurrentTimestamp),
            "DATABASE" => Some(Self::KwDatabase),
            "DEFAULT" => Some(Self::KwDefault),
            "DEFERRABLE" => Some(Self::KwDeferrable),
            "DEFERRED" => Some(Self::KwDeferred),
            "DELETE" => Some(Self::KwDelete),
            "DESC" => Some(Self::KwDesc),
            "DETACH" => Some(Self::KwDetach),
            "DISTINCT" => Some(Self::KwDistinct),
            "DO" => Some(Self::KwDo),
            "DROP" => Some(Self::KwDrop),
            "EACH" => Some(Self::KwEach),
            "ELSE" => Some(Self::KwElse),
            "END" => Some(Self::KwEnd),
            "ESCAPE" => Some(Self::KwEscape),
            "EXCEPT" => Some(Self::KwExcept),
            "EXCLUDE" => Some(Self::KwExclude),
            "EXCLUSIVE" => Some(Self::KwExclusive),
            "EXISTS" => Some(Self::KwExists),
            "EXPLAIN" => Some(Self::KwExplain),
            "FAIL" => Some(Self::KwFail),
            "FILTER" => Some(Self::KwFilter),
            "FIRST" => Some(Self::KwFirst),
            "FOLLOWING" => Some(Self::KwFollowing),
            "FOR" => Some(Self::KwFor),
            "FOREIGN" => Some(Self::KwForeign),
            "FROM" => Some(Self::KwFrom),
            "FULL" => Some(Self::KwFull),
            "GENERATED" => Some(Self::KwGenerated),
            "GLOB" => Some(Self::KwGlob),
            "GROUP" => Some(Self::KwGroup),
            "GROUPS" => Some(Self::KwGroups),
            "HAVING" => Some(Self::KwHaving),
            "IF" => Some(Self::KwIf),
            "IGNORE" => Some(Self::KwIgnore),
            "IMMEDIATE" => Some(Self::KwImmediate),
            "IN" => Some(Self::KwIn),
            "INDEX" => Some(Self::KwIndex),
            "INDEXED" => Some(Self::KwIndexed),
            "INITIALLY" => Some(Self::KwInitially),
            "INNER" => Some(Self::KwInner),
            "INSERT" => Some(Self::KwInsert),
            "INSTEAD" => Some(Self::KwInstead),
            "INTERSECT" => Some(Self::KwIntersect),
            "INTO" => Some(Self::KwInto),
            "IS" => Some(Self::KwIs),
            "ISNULL" => Some(Self::KwIsnull),
            "JOIN" => Some(Self::KwJoin),
            "KEY" => Some(Self::KwKey),
            "LAST" => Some(Self::KwLast),
            "LEFT" => Some(Self::KwLeft),
            "LIKE" => Some(Self::KwLike),
            "LIMIT" => Some(Self::KwLimit),
            "MATCH" => Some(Self::KwMatch),
            "MATERIALIZED" => Some(Self::KwMaterialized),
            "NATURAL" => Some(Self::KwNatural),
            "NO" => Some(Self::KwNo),
            "NOT" => Some(Self::KwNot),
            "NOTHING" => Some(Self::KwNothing),
            "NOTNULL" => Some(Self::KwNotnull),
            "NULL" => Some(Self::KwNull),
            "NULLS" => Some(Self::KwNulls),
            "OF" => Some(Self::KwOf),
            "OFFSET" => Some(Self::KwOffset),
            "ON" => Some(Self::KwOn),
            "OR" => Some(Self::KwOr),
            "ORDER" => Some(Self::KwOrder),
            "OTHERS" => Some(Self::KwOthers),
            "OUTER" => Some(Self::KwOuter),
            "OVER" => Some(Self::KwOver),
            "PARTITION" => Some(Self::KwPartition),
            "PLAN" => Some(Self::KwPlan),
            "PRAGMA" => Some(Self::KwPragma),
            "PRECEDING" => Some(Self::KwPreceding),
            "PRIMARY" => Some(Self::KwPrimary),
            "QUERY" => Some(Self::KwQuery),
            "RAISE" => Some(Self::KwRaise),
            "RANGE" => Some(Self::KwRange),
            "RECURSIVE" => Some(Self::KwRecursive),
            "REFERENCES" => Some(Self::KwReferences),
            "REGEXP" => Some(Self::KwRegexp),
            "REINDEX" => Some(Self::KwReindex),
            "RELEASE" => Some(Self::KwRelease),
            "RENAME" => Some(Self::KwRename),
            "REPLACE" => Some(Self::KwReplace),
            "RESTRICT" => Some(Self::KwRestrict),
            "RETURNING" => Some(Self::KwReturning),
            "RIGHT" => Some(Self::KwRight),
            "ROLLBACK" => Some(Self::KwRollback),
            "ROW" => Some(Self::KwRow),
            "ROWS" => Some(Self::KwRows),
            "SAVEPOINT" => Some(Self::KwSavepoint),
            "SELECT" => Some(Self::KwSelect),
            "SET" => Some(Self::KwSet),
            "STORED" => Some(Self::KwStored),
            "STRICT" => Some(Self::KwStrict),
            "TABLE" => Some(Self::KwTable),
            "TEMP" => Some(Self::KwTemp),
            "TEMPORARY" => Some(Self::KwTemporary),
            "THEN" => Some(Self::KwThen),
            "TIES" => Some(Self::KwTies),
            "TO" => Some(Self::KwTo),
            "TRANSACTION" => Some(Self::KwTransaction),
            "TRIGGER" => Some(Self::KwTrigger),
            "TRUE" => Some(Self::KwTrue),
            "FALSE" => Some(Self::KwFalse),
            "UNBOUNDED" => Some(Self::KwUnbounded),
            "UNION" => Some(Self::KwUnion),
            "UNIQUE" => Some(Self::KwUnique),
            "UPDATE" => Some(Self::KwUpdate),
            "USING" => Some(Self::KwUsing),
            "VACUUM" => Some(Self::KwVacuum),
            "VALUES" => Some(Self::KwValues),
            "VIEW" => Some(Self::KwView),
            "VIRTUAL" => Some(Self::KwVirtual),
            "WHEN" => Some(Self::KwWhen),
            "WHERE" => Some(Self::KwWhere),
            "WINDOW" => Some(Self::KwWindow),
            "WITH" => Some(Self::KwWith),
            "WITHOUT" => Some(Self::KwWithout),
            _ => None,
        }
    }

    /// Returns true if this is a keyword that can start a statement.
    /// Used by the parser for error recovery sync points.
    #[must_use]
    pub fn is_statement_start(&self) -> bool {
        matches!(
            self,
            Self::KwSelect
                | Self::KwInsert
                | Self::KwUpdate
                | Self::KwDelete
                | Self::KwCreate
                | Self::KwDrop
                | Self::KwAlter
                | Self::KwBegin
                | Self::KwCommit
                | Self::KwRollback
                | Self::KwSavepoint
                | Self::KwRelease
                | Self::KwAttach
                | Self::KwDetach
                | Self::KwPragma
                | Self::KwVacuum
                | Self::KwReindex
                | Self::KwAnalyze
                | Self::KwExplain
                | Self::KwWith
                | Self::KwReplace
        )
    }
}
