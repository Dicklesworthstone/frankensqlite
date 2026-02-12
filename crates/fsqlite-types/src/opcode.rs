/// VDBE (Virtual Database Engine) opcodes.
///
/// These correspond 1:1 to the upstream SQLite VDBE opcode set. Each opcode
/// represents a single operation in the bytecode program that the VDBE
/// executes. Opcodes are numbered sequentially; the specific numeric values
/// match C SQLite for debugging/comparison purposes.
///
/// Reference: canonical upstream SQLite opcode definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[allow(clippy::enum_variant_names)]
pub enum Opcode {
    // === Control Flow ===
    /// Jump to address P2.
    Goto = 1,
    /// Push return address, jump to P2.
    Gosub = 2,
    /// Pop return address, jump to it.
    Return = 3,
    /// Initialize coroutine. P1=coroutine reg, P2=jump on first entry.
    InitCoroutine = 4,
    /// End coroutine, jump to return address.
    EndCoroutine = 5,
    /// Yield control to/from coroutine.
    Yield = 6,
    /// Halt if register P3 is NULL.
    HaltIfNull = 7,
    /// Halt execution (with optional error).
    Halt = 8,

    // === Constants & Values ===
    /// Set register P2 to integer value P1.
    Integer = 9,
    /// Set register P2 to 64-bit integer from P4.
    Int64 = 10,
    /// Set register P2 to real value from P4.
    Real = 11,
    /// Set register P2 to string P4 (zero-terminated).
    String8 = 12,
    /// Set register P2 to string of length P1 from P4.
    String = 13,
    /// Begin subroutine / set register P2 to NULL.
    BeginSubrtn = 14,
    /// Set registers P2..P2+P3-1 to NULL.
    Null = 15,
    /// Set register to soft NULL (for optimization).
    SoftNull = 16,
    /// Set register P2 to blob of length P1 from P4.
    Blob = 17,
    /// Set register P2 to the value of variable/parameter P1.
    Variable = 18,

    // === Register Operations ===
    /// Move P3 registers from P1 to P2.
    Move = 19,
    /// Copy register P1 to P2 (and optionally more).
    Copy = 20,
    /// Shallow copy register P1 to P2.
    SCopy = 21,
    /// Copy integer value from P1 to P2.
    IntCopy = 22,

    // === Foreign Key ===
    /// Check foreign key constraints.
    FkCheck = 23,

    // === Result ===
    /// Output a row of P2 registers starting at P1.
    ResultRow = 24,

    // === Arithmetic & String ===
    /// Concatenate P1 and P2, store in P3.
    Concat = 25,
    /// P3 = P2 + P1.
    Add = 26,
    /// P3 = P2 - P1.
    Subtract = 27,
    /// P3 = P2 * P1.
    Multiply = 28,
    /// P3 = P2 / P1.
    Divide = 29,
    /// P3 = P2 % P1.
    Remainder = 30,

    // === Collation ===
    /// Set collation sequence for comparison.
    CollSeq = 31,

    // === Bitwise ===
    /// P3 = P1 & P2.
    BitAnd = 32,
    /// P3 = P1 | P2.
    BitOr = 33,
    /// P3 = P2 << P1.
    ShiftLeft = 34,
    /// P3 = P2 >> P1.
    ShiftRight = 35,

    // === Type Conversion ===
    /// Add integer P2 to register P1.
    AddImm = 36,
    /// Fail if P1 is not an integer; optionally jump to P2.
    MustBeInt = 37,
    /// If P1 is integer, convert to real in-place.
    RealAffinity = 38,
    /// Cast register P1 to type P2.
    Cast = 39,

    // === Comparison ===
    /// Jump to P2 if P1 == P3.
    Eq = 40,
    /// Jump to P2 if P1 != P3.
    Ne = 41,
    /// Jump to P2 if P3 < P1.
    Lt = 42,
    /// Jump to P2 if P3 <= P1.
    Le = 43,
    /// Jump to P2 if P3 > P1.
    Gt = 44,
    /// Jump to P2 if P3 >= P1.
    Ge = 45,
    /// Jump if the previous comparison was Eq (for multi-column indexes).
    ElseEq = 46,

    // === Permutation & Compare ===
    /// Set up permutation for subsequent Compare.
    Permutation = 47,
    /// Compare P1..P1+P3-1 with P2..P2+P3-1.
    Compare = 48,

    // === Branching ===
    /// Jump to one of P1, P2, or P3 based on comparison result.
    Jump = 49,
    /// P3 = P1 AND P2 (three-valued logic).
    And = 50,
    /// P3 = P1 OR P2 (three-valued logic).
    Or = 51,
    /// Apply IS TRUE test.
    IsTrue = 52,
    /// P2 = NOT P1.
    Not = 53,
    /// P2 = ~P1 (bitwise not).
    BitNot = 54,
    /// Jump to P2 on first execution only.
    Once = 55,
    /// Jump to P2 if P1 is true (non-zero and non-NULL).
    If = 56,
    /// Jump to P2 if P1 is false (zero or NULL).
    IfNot = 57,
    /// Jump to P2 if P1 is NULL.
    IsNull = 58,
    /// Type check against P5 type mask; jump to P2 on mismatch.
    IsType = 59,
    /// P2 = 0 if any of P1, P2, P3 is NULL.
    ZeroOrNull = 60,
    /// Jump to P2 if P1 is not NULL.
    NotNull = 61,
    /// Jump to P2 if the current row of cursor P1 is NULL.
    IfNullRow = 62,

    // === Column Access ===
    /// Extract byte offset of cursor.
    Offset = 63,
    /// Extract column P2 from cursor P1 into register P3.
    Column = 64,
    /// Type-check columns against declared types.
    TypeCheck = 65,
    /// Apply type affinity to P2 registers starting at P1.
    Affinity = 66,

    // === Record Building ===
    /// Build a record from P1..P1+P2-1 registers into P3.
    MakeRecord = 67,

    // === Counting ===
    /// Store the number of rows in cursor P1 into register P2.
    Count = 68,

    // === Transaction Control ===
    /// Begin, release, or rollback a savepoint.
    Savepoint = 69,
    /// Set or clear auto-commit mode.
    AutoCommit = 70,
    /// Begin a transaction on database P1.
    Transaction = 71,

    // === Cookie Access ===
    /// Read database cookie P3 from database P1 into register P2.
    ReadCookie = 72,
    /// Write P3 to database cookie P2 of database P1.
    SetCookie = 73,

    // === Cursor Operations ===
    /// Reopen an index cursor (P1) if it's on a different root page.
    ReopenIdx = 74,
    /// Open a read cursor on table/index P2 in database P3.
    OpenRead = 75,
    /// Open a write cursor on table/index P2 in database P3.
    OpenWrite = 76,
    /// Open cursor P1 as a duplicate of cursor P2.
    OpenDup = 77,
    /// Open an ephemeral (temporary) table cursor.
    OpenEphemeral = 78,
    /// Open an auto-index ephemeral cursor.
    OpenAutoindex = 79,
    /// Open a sorter cursor.
    SorterOpen = 80,
    /// Test if sequence number has been used.
    SequenceTest = 81,
    /// Open a pseudo-table cursor (reads from a register).
    OpenPseudo = 82,
    /// Close cursor P1.
    Close = 83,
    /// Set the columns-used mask for cursor P1.
    ColumnsUsed = 84,

    // === Seek Operations ===
    /// Seek cursor P1 to the largest entry less than P3.
    SeekLT = 85,
    /// Seek cursor P1 to the largest entry <= P3.
    SeekLE = 86,
    /// Seek cursor P1 to the smallest entry >= P3.
    SeekGE = 87,
    /// Seek cursor P1 to the smallest entry greater than P3.
    SeekGT = 88,
    /// Optimized seek-scan for small result sets.
    SeekScan = 89,
    /// Mark seek hit range for covering index optimization.
    SeekHit = 90,
    /// Jump to P2 if cursor P1 is not open.
    IfNotOpen = 91,

    // === Index Lookup ===
    /// Like NotFound but with Bloom filter check.
    IfNoHope = 92,
    /// Jump to P2 if key P3 is NOT found (no conflict).
    NoConflict = 93,
    /// Jump to P2 if key P3 is NOT found in cursor P1.
    NotFound = 94,
    /// Jump to P2 if key P3 IS found in cursor P1.
    Found = 95,

    // === Rowid Seek ===
    /// Seek cursor P1 to rowid P3; jump to P2 if not found.
    SeekRowid = 96,
    /// Jump to P2 if rowid P3 does NOT exist in cursor P1.
    NotExists = 97,

    // === Sequence & Rowid ===
    /// Store next sequence value for cursor P1 into register P2.
    Sequence = 98,
    /// Generate a new unique rowid for cursor P1.
    NewRowid = 99,

    // === Insert & Delete ===
    /// Insert record from P2 with rowid P3 into cursor P1.
    Insert = 100,
    /// Copy a cell directly from one cursor to another.
    RowCell = 101,
    /// Delete the current row of cursor P1.
    Delete = 102,
    /// Reset the change counter.
    ResetCount = 103,

    // === Sorter Operations ===
    /// Compare sorter key.
    SorterCompare = 104,
    /// Read data from the sorter.
    SorterData = 105,

    // === Row Data ===
    /// Copy the complete row data of cursor P1 into register P2.
    RowData = 106,
    /// Store the rowid of cursor P1 into register P2.
    Rowid = 107,
    /// Set cursor P1 to a NULL row.
    NullRow = 108,

    // === Cursor Navigation ===
    /// Seek to end of table (no-op for reading, positions for append).
    SeekEnd = 109,
    /// Move cursor P1 to the last entry; jump to P2 if empty.
    Last = 110,
    /// Jump to P2 if table size is between P3 and P4.
    IfSizeBetween = 111,
    /// Sort (alias for SorterSort in some contexts).
    SorterSort = 112,
    /// Sort cursor P1.
    Sort = 113,
    /// Rewind cursor P1 to the first entry; jump to P2 if empty.
    Rewind = 114,
    /// Jump to P2 if cursor P1's table is empty.
    IfEmpty = 115,

    // === Iteration ===
    /// Advance sorter to next entry.
    SorterNext = 116,
    /// Move cursor P1 to the previous entry; jump to P2 if done.
    Prev = 117,
    /// Move cursor P1 to the next entry; jump to P2 if done.
    Next = 118,

    // === Index Insert/Delete ===
    /// Insert record P2 into index cursor P1.
    IdxInsert = 119,
    /// Insert into sorter.
    SorterInsert = 120,
    /// Delete from index cursor P1.
    IdxDelete = 121,

    // === Deferred Seek ===
    /// Defer a seek on cursor P1 using the rowid from index cursor P2.
    DeferredSeek = 122,
    /// Extract rowid from index entry of cursor P1.
    IdxRowid = 123,
    /// Complete a previously deferred seek.
    FinishSeek = 124,

    // === Index Comparison ===
    /// Jump to P2 if index key of P1 <= key.
    IdxLE = 125,
    /// Jump to P2 if index key of P1 > key.
    IdxGT = 126,
    /// Jump to P2 if index key of P1 < key.
    IdxLT = 127,
    /// Jump to P2 if index key of P1 >= key.
    IdxGE = 128,

    // === DDL Operations ===
    /// Destroy (drop) a B-tree rooted at page P1.
    Destroy = 129,
    /// Clear (delete all rows from) a table or index.
    Clear = 130,
    /// Reset a sorter cursor.
    ResetSorter = 131,
    /// Allocate a new B-tree, store root page number in P2.
    CreateBtree = 132,

    // === Schema Operations ===
    /// Execute an SQL statement stored in P4.
    SqlExec = 133,
    /// Parse the schema for database P1.
    ParseSchema = 134,
    /// Load analysis data for database P1.
    LoadAnalysis = 135,
    /// Drop a table.
    DropTable = 136,
    /// Drop an index.
    DropIndex = 137,
    /// Drop a trigger.
    DropTrigger = 138,

    // === Integrity Check ===
    /// Run integrity check on database P1.
    IntegrityCk = 139,

    // === RowSet Operations ===
    /// Add integer P2 to rowset P1.
    RowSetAdd = 140,
    /// Read next value from rowset P1 into P3; jump to P2 when empty.
    RowSetRead = 141,
    /// Test if P3 exists in rowset P1; jump to P2 if found.
    RowSetTest = 142,

    // === Trigger/Program ===
    /// Call a trigger sub-program.
    Program = 143,
    /// Copy trigger parameter into register P2.
    Param = 144,

    // === FK Counters ===
    /// Increment or decrement FK counter.
    FkCounter = 145,
    /// Jump to P2 if FK counter is zero.
    FkIfZero = 146,

    // === Memory/Counter ===
    /// Set register P2 to max of P2 and register P1.
    MemMax = 147,

    // === Conditional Jumps ===
    /// Jump to P2 if register P1 > 0; decrement by P3.
    IfPos = 148,
    /// Compute offset limit.
    OffsetLimit = 149,
    /// Jump to P2 if register P1 is not zero.
    IfNotZero = 150,
    /// Decrement P1, jump to P2 if result is zero.
    DecrJumpZero = 151,

    // === Aggregate Functions ===
    /// Invoke aggregate inverse function.
    AggInverse = 152,
    /// Invoke aggregate step function.
    AggStep = 153,
    /// Step variant with different init semantics.
    AggStep1 = 154,
    /// Extract aggregate intermediate value.
    AggValue = 155,
    /// Finalize aggregate function.
    AggFinal = 156,

    // === WAL & Journal ===
    /// Checkpoint the WAL for database P1.
    Checkpoint = 157,
    /// Set journal mode for database P1.
    JournalMode = 158,

    // === Vacuum ===
    /// Vacuum the database.
    Vacuum = 159,
    /// Incremental vacuum step; jump to P2 if done.
    IncrVacuum = 160,

    // === Expiry & Locking ===
    /// Mark prepared statement as expired.
    Expire = 161,
    /// Lock cursor P1.
    CursorLock = 162,
    /// Unlock cursor P1.
    CursorUnlock = 163,
    /// Lock table P2 in database P1.
    TableLock = 164,

    // === Virtual Table ===
    /// Begin a virtual table transaction.
    VBegin = 165,
    /// Create a virtual table.
    VCreate = 166,
    /// Destroy a virtual table.
    VDestroy = 167,
    /// Open a virtual table cursor.
    VOpen = 168,
    /// Check virtual table integrity.
    VCheck = 169,
    /// Initialize IN constraint for virtual table.
    VInitIn = 170,
    /// Apply filter to virtual table cursor.
    VFilter = 171,
    /// Read column from virtual table cursor.
    VColumn = 172,
    /// Advance virtual table cursor.
    VNext = 173,
    /// Rename a virtual table.
    VRename = 174,
    /// Update/insert/delete on virtual table.
    VUpdate = 175,

    // === Page Count ===
    /// Store database page count in register P2.
    Pagecount = 176,
    /// Set or read max page count.
    MaxPgcnt = 177,

    // === Functions ===
    /// Call a pure (deterministic) function.
    PureFunc = 178,
    /// Call a function (possibly with side effects).
    Function = 179,

    // === Subtype Operations ===
    /// Clear the subtype from register P1.
    ClrSubtype = 180,
    /// Get subtype of P1 into P2.
    GetSubtype = 181,
    /// Set subtype of P2 from P1.
    SetSubtype = 182,

    // === Bloom Filter ===
    /// Add entry to Bloom filter.
    FilterAdd = 183,
    /// Test Bloom filter; jump to P2 if definitely not present.
    Filter = 184,

    // === Trace & Init ===
    /// Trace/profile callback.
    Trace = 185,
    /// Initialize VDBE program; jump to P2.
    Init = 186,

    // === Hints & Debug ===
    /// Provide cursor hint to storage engine.
    CursorHint = 187,
    /// Mark that this program can be aborted.
    Abortable = 188,
    /// Release register range.
    ReleaseReg = 189,

    // === Noop (always last) ===
    /// No operation.
    Noop = 190,
}

impl Opcode {
    /// Total number of opcodes defined.
    pub const COUNT: usize = 191;

    /// Get the opcode name as a static string slice.
    #[allow(clippy::too_many_lines)]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Goto => "Goto",
            Self::Gosub => "Gosub",
            Self::Return => "Return",
            Self::InitCoroutine => "InitCoroutine",
            Self::EndCoroutine => "EndCoroutine",
            Self::Yield => "Yield",
            Self::HaltIfNull => "HaltIfNull",
            Self::Halt => "Halt",
            Self::Integer => "Integer",
            Self::Int64 => "Int64",
            Self::Real => "Real",
            Self::String8 => "String8",
            Self::String => "String",
            Self::BeginSubrtn => "BeginSubrtn",
            Self::Null => "Null",
            Self::SoftNull => "SoftNull",
            Self::Blob => "Blob",
            Self::Variable => "Variable",
            Self::Move => "Move",
            Self::Copy => "Copy",
            Self::SCopy => "SCopy",
            Self::IntCopy => "IntCopy",
            Self::FkCheck => "FkCheck",
            Self::ResultRow => "ResultRow",
            Self::Concat => "Concat",
            Self::Add => "Add",
            Self::Subtract => "Subtract",
            Self::Multiply => "Multiply",
            Self::Divide => "Divide",
            Self::Remainder => "Remainder",
            Self::CollSeq => "CollSeq",
            Self::BitAnd => "BitAnd",
            Self::BitOr => "BitOr",
            Self::ShiftLeft => "ShiftLeft",
            Self::ShiftRight => "ShiftRight",
            Self::AddImm => "AddImm",
            Self::MustBeInt => "MustBeInt",
            Self::RealAffinity => "RealAffinity",
            Self::Cast => "Cast",
            Self::Eq => "Eq",
            Self::Ne => "Ne",
            Self::Lt => "Lt",
            Self::Le => "Le",
            Self::Gt => "Gt",
            Self::Ge => "Ge",
            Self::ElseEq => "ElseEq",
            Self::Permutation => "Permutation",
            Self::Compare => "Compare",
            Self::Jump => "Jump",
            Self::And => "And",
            Self::Or => "Or",
            Self::IsTrue => "IsTrue",
            Self::Not => "Not",
            Self::BitNot => "BitNot",
            Self::Once => "Once",
            Self::If => "If",
            Self::IfNot => "IfNot",
            Self::IsNull => "IsNull",
            Self::IsType => "IsType",
            Self::ZeroOrNull => "ZeroOrNull",
            Self::NotNull => "NotNull",
            Self::IfNullRow => "IfNullRow",
            Self::Offset => "Offset",
            Self::Column => "Column",
            Self::TypeCheck => "TypeCheck",
            Self::Affinity => "Affinity",
            Self::MakeRecord => "MakeRecord",
            Self::Count => "Count",
            Self::Savepoint => "Savepoint",
            Self::AutoCommit => "AutoCommit",
            Self::Transaction => "Transaction",
            Self::ReadCookie => "ReadCookie",
            Self::SetCookie => "SetCookie",
            Self::ReopenIdx => "ReopenIdx",
            Self::OpenRead => "OpenRead",
            Self::OpenWrite => "OpenWrite",
            Self::OpenDup => "OpenDup",
            Self::OpenEphemeral => "OpenEphemeral",
            Self::OpenAutoindex => "OpenAutoindex",
            Self::SorterOpen => "SorterOpen",
            Self::SequenceTest => "SequenceTest",
            Self::OpenPseudo => "OpenPseudo",
            Self::Close => "Close",
            Self::ColumnsUsed => "ColumnsUsed",
            Self::SeekLT => "SeekLT",
            Self::SeekLE => "SeekLE",
            Self::SeekGE => "SeekGE",
            Self::SeekGT => "SeekGT",
            Self::SeekScan => "SeekScan",
            Self::SeekHit => "SeekHit",
            Self::IfNotOpen => "IfNotOpen",
            Self::IfNoHope => "IfNoHope",
            Self::NoConflict => "NoConflict",
            Self::NotFound => "NotFound",
            Self::Found => "Found",
            Self::SeekRowid => "SeekRowid",
            Self::NotExists => "NotExists",
            Self::Sequence => "Sequence",
            Self::NewRowid => "NewRowid",
            Self::Insert => "Insert",
            Self::RowCell => "RowCell",
            Self::Delete => "Delete",
            Self::ResetCount => "ResetCount",
            Self::SorterCompare => "SorterCompare",
            Self::SorterData => "SorterData",
            Self::RowData => "RowData",
            Self::Rowid => "Rowid",
            Self::NullRow => "NullRow",
            Self::SeekEnd => "SeekEnd",
            Self::Last => "Last",
            Self::IfSizeBetween => "IfSizeBetween",
            Self::SorterSort => "SorterSort",
            Self::Sort => "Sort",
            Self::Rewind => "Rewind",
            Self::IfEmpty => "IfEmpty",
            Self::SorterNext => "SorterNext",
            Self::Prev => "Prev",
            Self::Next => "Next",
            Self::IdxInsert => "IdxInsert",
            Self::SorterInsert => "SorterInsert",
            Self::IdxDelete => "IdxDelete",
            Self::DeferredSeek => "DeferredSeek",
            Self::IdxRowid => "IdxRowid",
            Self::FinishSeek => "FinishSeek",
            Self::IdxLE => "IdxLE",
            Self::IdxGT => "IdxGT",
            Self::IdxLT => "IdxLT",
            Self::IdxGE => "IdxGE",
            Self::Destroy => "Destroy",
            Self::Clear => "Clear",
            Self::ResetSorter => "ResetSorter",
            Self::CreateBtree => "CreateBtree",
            Self::SqlExec => "SqlExec",
            Self::ParseSchema => "ParseSchema",
            Self::LoadAnalysis => "LoadAnalysis",
            Self::DropTable => "DropTable",
            Self::DropIndex => "DropIndex",
            Self::DropTrigger => "DropTrigger",
            Self::IntegrityCk => "IntegrityCk",
            Self::RowSetAdd => "RowSetAdd",
            Self::RowSetRead => "RowSetRead",
            Self::RowSetTest => "RowSetTest",
            Self::Program => "Program",
            Self::Param => "Param",
            Self::FkCounter => "FkCounter",
            Self::FkIfZero => "FkIfZero",
            Self::MemMax => "MemMax",
            Self::IfPos => "IfPos",
            Self::OffsetLimit => "OffsetLimit",
            Self::IfNotZero => "IfNotZero",
            Self::DecrJumpZero => "DecrJumpZero",
            Self::AggInverse => "AggInverse",
            Self::AggStep => "AggStep",
            Self::AggStep1 => "AggStep1",
            Self::AggValue => "AggValue",
            Self::AggFinal => "AggFinal",
            Self::Checkpoint => "Checkpoint",
            Self::JournalMode => "JournalMode",
            Self::Vacuum => "Vacuum",
            Self::IncrVacuum => "IncrVacuum",
            Self::Expire => "Expire",
            Self::CursorLock => "CursorLock",
            Self::CursorUnlock => "CursorUnlock",
            Self::TableLock => "TableLock",
            Self::VBegin => "VBegin",
            Self::VCreate => "VCreate",
            Self::VDestroy => "VDestroy",
            Self::VOpen => "VOpen",
            Self::VCheck => "VCheck",
            Self::VInitIn => "VInitIn",
            Self::VFilter => "VFilter",
            Self::VColumn => "VColumn",
            Self::VNext => "VNext",
            Self::VRename => "VRename",
            Self::VUpdate => "VUpdate",
            Self::Pagecount => "Pagecount",
            Self::MaxPgcnt => "MaxPgcnt",
            Self::PureFunc => "PureFunc",
            Self::Function => "Function",
            Self::ClrSubtype => "ClrSubtype",
            Self::GetSubtype => "GetSubtype",
            Self::SetSubtype => "SetSubtype",
            Self::FilterAdd => "FilterAdd",
            Self::Filter => "Filter",
            Self::Trace => "Trace",
            Self::Init => "Init",
            Self::CursorHint => "CursorHint",
            Self::Abortable => "Abortable",
            Self::ReleaseReg => "ReleaseReg",
            Self::Noop => "Noop",
        }
    }

    /// Try to convert a u8 to an Opcode.
    #[allow(clippy::too_many_lines)]
    pub const fn from_byte(byte: u8) -> Option<Self> {
        if byte == 0 || byte > 190 {
            return None;
        }
        // SAFETY: All values 1..=190 are valid discriminants.
        // We verified byte is in range above.
        // Since the enum is repr(u8) with consecutive values, this is safe.
        // However, since unsafe is forbidden, we use a match instead.
        // For now, we accept the compile-time cost of a big match.
        match byte {
            1 => Some(Self::Goto),
            2 => Some(Self::Gosub),
            3 => Some(Self::Return),
            4 => Some(Self::InitCoroutine),
            5 => Some(Self::EndCoroutine),
            6 => Some(Self::Yield),
            7 => Some(Self::HaltIfNull),
            8 => Some(Self::Halt),
            9 => Some(Self::Integer),
            10 => Some(Self::Int64),
            11 => Some(Self::Real),
            12 => Some(Self::String8),
            13 => Some(Self::String),
            14 => Some(Self::BeginSubrtn),
            15 => Some(Self::Null),
            16 => Some(Self::SoftNull),
            17 => Some(Self::Blob),
            18 => Some(Self::Variable),
            19 => Some(Self::Move),
            20 => Some(Self::Copy),
            21 => Some(Self::SCopy),
            22 => Some(Self::IntCopy),
            23 => Some(Self::FkCheck),
            24 => Some(Self::ResultRow),
            25 => Some(Self::Concat),
            26 => Some(Self::Add),
            27 => Some(Self::Subtract),
            28 => Some(Self::Multiply),
            29 => Some(Self::Divide),
            30 => Some(Self::Remainder),
            31 => Some(Self::CollSeq),
            32 => Some(Self::BitAnd),
            33 => Some(Self::BitOr),
            34 => Some(Self::ShiftLeft),
            35 => Some(Self::ShiftRight),
            36 => Some(Self::AddImm),
            37 => Some(Self::MustBeInt),
            38 => Some(Self::RealAffinity),
            39 => Some(Self::Cast),
            40 => Some(Self::Eq),
            41 => Some(Self::Ne),
            42 => Some(Self::Lt),
            43 => Some(Self::Le),
            44 => Some(Self::Gt),
            45 => Some(Self::Ge),
            46 => Some(Self::ElseEq),
            47 => Some(Self::Permutation),
            48 => Some(Self::Compare),
            49 => Some(Self::Jump),
            50 => Some(Self::And),
            51 => Some(Self::Or),
            52 => Some(Self::IsTrue),
            53 => Some(Self::Not),
            54 => Some(Self::BitNot),
            55 => Some(Self::Once),
            56 => Some(Self::If),
            57 => Some(Self::IfNot),
            58 => Some(Self::IsNull),
            59 => Some(Self::IsType),
            60 => Some(Self::ZeroOrNull),
            61 => Some(Self::NotNull),
            62 => Some(Self::IfNullRow),
            63 => Some(Self::Offset),
            64 => Some(Self::Column),
            65 => Some(Self::TypeCheck),
            66 => Some(Self::Affinity),
            67 => Some(Self::MakeRecord),
            68 => Some(Self::Count),
            69 => Some(Self::Savepoint),
            70 => Some(Self::AutoCommit),
            71 => Some(Self::Transaction),
            72 => Some(Self::ReadCookie),
            73 => Some(Self::SetCookie),
            74 => Some(Self::ReopenIdx),
            75 => Some(Self::OpenRead),
            76 => Some(Self::OpenWrite),
            77 => Some(Self::OpenDup),
            78 => Some(Self::OpenEphemeral),
            79 => Some(Self::OpenAutoindex),
            80 => Some(Self::SorterOpen),
            81 => Some(Self::SequenceTest),
            82 => Some(Self::OpenPseudo),
            83 => Some(Self::Close),
            84 => Some(Self::ColumnsUsed),
            85 => Some(Self::SeekLT),
            86 => Some(Self::SeekLE),
            87 => Some(Self::SeekGE),
            88 => Some(Self::SeekGT),
            89 => Some(Self::SeekScan),
            90 => Some(Self::SeekHit),
            91 => Some(Self::IfNotOpen),
            92 => Some(Self::IfNoHope),
            93 => Some(Self::NoConflict),
            94 => Some(Self::NotFound),
            95 => Some(Self::Found),
            96 => Some(Self::SeekRowid),
            97 => Some(Self::NotExists),
            98 => Some(Self::Sequence),
            99 => Some(Self::NewRowid),
            100 => Some(Self::Insert),
            101 => Some(Self::RowCell),
            102 => Some(Self::Delete),
            103 => Some(Self::ResetCount),
            104 => Some(Self::SorterCompare),
            105 => Some(Self::SorterData),
            106 => Some(Self::RowData),
            107 => Some(Self::Rowid),
            108 => Some(Self::NullRow),
            109 => Some(Self::SeekEnd),
            110 => Some(Self::Last),
            111 => Some(Self::IfSizeBetween),
            112 => Some(Self::SorterSort),
            113 => Some(Self::Sort),
            114 => Some(Self::Rewind),
            115 => Some(Self::IfEmpty),
            116 => Some(Self::SorterNext),
            117 => Some(Self::Prev),
            118 => Some(Self::Next),
            119 => Some(Self::IdxInsert),
            120 => Some(Self::SorterInsert),
            121 => Some(Self::IdxDelete),
            122 => Some(Self::DeferredSeek),
            123 => Some(Self::IdxRowid),
            124 => Some(Self::FinishSeek),
            125 => Some(Self::IdxLE),
            126 => Some(Self::IdxGT),
            127 => Some(Self::IdxLT),
            128 => Some(Self::IdxGE),
            129 => Some(Self::Destroy),
            130 => Some(Self::Clear),
            131 => Some(Self::ResetSorter),
            132 => Some(Self::CreateBtree),
            133 => Some(Self::SqlExec),
            134 => Some(Self::ParseSchema),
            135 => Some(Self::LoadAnalysis),
            136 => Some(Self::DropTable),
            137 => Some(Self::DropIndex),
            138 => Some(Self::DropTrigger),
            139 => Some(Self::IntegrityCk),
            140 => Some(Self::RowSetAdd),
            141 => Some(Self::RowSetRead),
            142 => Some(Self::RowSetTest),
            143 => Some(Self::Program),
            144 => Some(Self::Param),
            145 => Some(Self::FkCounter),
            146 => Some(Self::FkIfZero),
            147 => Some(Self::MemMax),
            148 => Some(Self::IfPos),
            149 => Some(Self::OffsetLimit),
            150 => Some(Self::IfNotZero),
            151 => Some(Self::DecrJumpZero),
            152 => Some(Self::AggInverse),
            153 => Some(Self::AggStep),
            154 => Some(Self::AggStep1),
            155 => Some(Self::AggValue),
            156 => Some(Self::AggFinal),
            157 => Some(Self::Checkpoint),
            158 => Some(Self::JournalMode),
            159 => Some(Self::Vacuum),
            160 => Some(Self::IncrVacuum),
            161 => Some(Self::Expire),
            162 => Some(Self::CursorLock),
            163 => Some(Self::CursorUnlock),
            164 => Some(Self::TableLock),
            165 => Some(Self::VBegin),
            166 => Some(Self::VCreate),
            167 => Some(Self::VDestroy),
            168 => Some(Self::VOpen),
            169 => Some(Self::VCheck),
            170 => Some(Self::VInitIn),
            171 => Some(Self::VFilter),
            172 => Some(Self::VColumn),
            173 => Some(Self::VNext),
            174 => Some(Self::VRename),
            175 => Some(Self::VUpdate),
            176 => Some(Self::Pagecount),
            177 => Some(Self::MaxPgcnt),
            178 => Some(Self::PureFunc),
            179 => Some(Self::Function),
            180 => Some(Self::ClrSubtype),
            181 => Some(Self::GetSubtype),
            182 => Some(Self::SetSubtype),
            183 => Some(Self::FilterAdd),
            184 => Some(Self::Filter),
            185 => Some(Self::Trace),
            186 => Some(Self::Init),
            187 => Some(Self::CursorHint),
            188 => Some(Self::Abortable),
            189 => Some(Self::ReleaseReg),
            190 => Some(Self::Noop),
            _ => None,
        }
    }

    /// Whether this opcode is a jump instruction (has a P2 jump target).
    pub const fn is_jump(self) -> bool {
        matches!(
            self,
            Self::Goto
                | Self::Gosub
                | Self::InitCoroutine
                | Self::Yield
                | Self::HaltIfNull
                | Self::Once
                | Self::If
                | Self::IfNot
                | Self::IsNull
                | Self::IsType
                | Self::NotNull
                | Self::IfNullRow
                | Self::Jump
                | Self::Eq
                | Self::Ne
                | Self::Lt
                | Self::Le
                | Self::Gt
                | Self::Ge
                | Self::ElseEq
                | Self::SeekLT
                | Self::SeekLE
                | Self::SeekGE
                | Self::SeekGT
                | Self::SeekRowid
                | Self::NotExists
                | Self::IfNotOpen
                | Self::IfNoHope
                | Self::NoConflict
                | Self::NotFound
                | Self::Found
                | Self::Last
                | Self::Rewind
                | Self::IfEmpty
                | Self::IfSizeBetween
                | Self::Next
                | Self::Prev
                | Self::SorterNext
                | Self::SorterSort
                | Self::Sort
                | Self::IdxLE
                | Self::IdxGT
                | Self::IdxLT
                | Self::IdxGE
                | Self::RowSetRead
                | Self::RowSetTest
                | Self::Program
                | Self::FkIfZero
                | Self::IfPos
                | Self::IfNotZero
                | Self::DecrJumpZero
                | Self::IncrVacuum
                | Self::VFilter
                | Self::VNext
                | Self::Filter
                | Self::Init
        )
    }
}

impl std::fmt::Display for Opcode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A single VDBE instruction.
#[derive(Debug, Clone, PartialEq)]
pub struct VdbeOp {
    /// The opcode.
    pub opcode: Opcode,
    /// First operand (typically a register number or cursor index).
    pub p1: i32,
    /// Second operand (often a jump target address).
    pub p2: i32,
    /// Third operand.
    pub p3: i32,
    /// Fourth operand (polymorphic: string, function pointer, collation, etc.).
    pub p4: P4,
    /// Fifth operand (small flags, typically bit flags or type mask).
    pub p5: u16,
}

/// The P4 operand of a VDBE instruction.
///
/// P4 is a polymorphic operand that can hold different types depending on
/// the opcode.
#[derive(Debug, Clone, PartialEq)]
pub enum P4 {
    /// No P4 value.
    None,
    /// A 32-bit integer value.
    Int(i32),
    /// A 64-bit integer value.
    Int64(i64),
    /// A 64-bit float value.
    Real(f64),
    /// A string value.
    Str(String),
    /// A blob value.
    Blob(Vec<u8>),
    /// A collation sequence name.
    Collation(String),
    /// A function name (for Function/PureFunc opcodes).
    FuncName(String),
    /// A table name.
    Table(String),
    /// An index name (for IdxInsert/IdxDelete opcodes).
    Index(String),
    /// An affinity string (one char per column).
    Affinity(String),
}

// ── VDBE Program Builder ────────────────────────────────────────────────────
//
// NOTE: These types intentionally live in `fsqlite-types` so that the planner
// (Layer 3) can generate VDBE bytecode without depending on `fsqlite-vdbe`
// (Layer 5). This is enforced by the workspace layering tests (bd-1wwc).

use fsqlite_error::{FrankenError, Result};

/// An opaque handle representing a forward-reference label.
///
/// Labels allow codegen to emit jump instructions before the target address is
/// known. All labels MUST be resolved before execution begins; unresolved
/// labels are a codegen bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Label(u32);

/// Internal tracking for label resolution.
#[derive(Debug)]
enum LabelState {
    /// Not yet resolved. Contains the indices of instructions whose `p2` field
    /// should be patched when the label is resolved.
    Unresolved(Vec<usize>),
    /// Resolved to a concrete instruction address.
    Resolved(i32),
}

/// Sequential register allocator for the VDBE register file.
///
/// Registers are numbered starting at 1 (register 0 is reserved/unused),
/// matching C SQLite convention.
#[derive(Debug)]
pub struct RegisterAllocator {
    /// The next register number to allocate (starts at 1).
    next_reg: i32,
    /// Pool of returned temporary registers available for reuse.
    temp_pool: Vec<i32>,
}

impl RegisterAllocator {
    /// Create a new allocator. First allocation returns register 1.
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_reg: 1,
            temp_pool: Vec::new(),
        }
    }

    /// Allocate a single persistent register.
    pub fn alloc_reg(&mut self) -> i32 {
        let reg = self.next_reg;
        self.next_reg += 1;
        reg
    }

    /// Allocate a contiguous block of `n` persistent registers.
    ///
    /// Returns the first register number. The block spans `[result, result+n)`.
    pub fn alloc_regs(&mut self, n: i32) -> i32 {
        let first = self.next_reg;
        self.next_reg += n;
        first
    }

    /// Allocate a temporary register (reuses from pool if available).
    pub fn alloc_temp(&mut self) -> i32 {
        self.temp_pool.pop().unwrap_or_else(|| {
            let reg = self.next_reg;
            self.next_reg += 1;
            reg
        })
    }

    /// Return a temporary register to the reuse pool.
    pub fn free_temp(&mut self, reg: i32) {
        self.temp_pool.push(reg);
    }

    /// The total number of registers allocated (high water mark).
    #[must_use]
    pub fn count(&self) -> i32 {
        self.next_reg - 1
    }
}

impl Default for RegisterAllocator {
    fn default() -> Self {
        Self::new()
    }
}

/// A VDBE bytecode program under construction.
///
/// Provides methods to emit instructions, create/resolve labels for forward
/// jumps, and allocate registers. Once construction is complete, call
/// [`finish`](Self::finish) to validate and extract the final instruction
/// sequence.
#[derive(Debug)]
pub struct ProgramBuilder {
    /// The instruction sequence.
    ops: Vec<VdbeOp>,
    /// Label states (indexed by `Label.0`).
    labels: Vec<LabelState>,
    /// Register allocator.
    regs: RegisterAllocator,
}

impl ProgramBuilder {
    /// Create a new empty program builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            ops: Vec::new(),
            labels: Vec::new(),
            regs: RegisterAllocator::new(),
        }
    }

    // ── Instruction emission ────────────────────────────────────────────

    /// Emit a single instruction and return its address (index in `ops`).
    pub fn emit(&mut self, op: VdbeOp) -> usize {
        let addr = self.ops.len();
        self.ops.push(op);
        addr
    }

    /// Emit a simple instruction from parts.
    pub fn emit_op(&mut self, opcode: Opcode, p1: i32, p2: i32, p3: i32, p4: P4, p5: u16) -> usize {
        self.emit(VdbeOp {
            opcode,
            p1,
            p2,
            p3,
            p4,
            p5,
        })
    }

    /// The current address (index of the next instruction to be emitted).
    #[must_use]
    pub fn current_addr(&self) -> usize {
        self.ops.len()
    }

    /// Get a reference to the instruction at `addr`.
    #[must_use]
    pub fn op_at(&self, addr: usize) -> Option<&VdbeOp> {
        self.ops.get(addr)
    }

    /// Get a mutable reference to the instruction at `addr`.
    #[must_use]
    pub fn op_at_mut(&mut self, addr: usize) -> Option<&mut VdbeOp> {
        self.ops.get_mut(addr)
    }

    // ── Label system ────────────────────────────────────────────────────

    /// Create a new label for forward-reference jumps.
    #[must_use]
    pub fn emit_label(&mut self) -> Label {
        let id = u32::try_from(self.labels.len()).expect("too many labels");
        self.labels.push(LabelState::Unresolved(Vec::new()));
        Label(id)
    }

    /// Emit a jump instruction whose p2 target is a label (forward reference).
    ///
    /// The label's address will be patched into p2 when `resolve_label` is called.
    pub fn emit_jump_to_label(
        &mut self,
        opcode: Opcode,
        p1: i32,
        p3: i32,
        label: Label,
        p4: P4,
        p5: u16,
    ) -> usize {
        let addr = self.emit(VdbeOp {
            opcode,
            p1,
            p2: -1, // placeholder; will be patched
            p3,
            p4,
            p5,
        });

        let state = self
            .labels
            .get_mut(usize::try_from(label.0).expect("label fits usize"))
            .expect("label must exist");

        match state {
            LabelState::Unresolved(refs) => refs.push(addr),
            LabelState::Resolved(target) => {
                // Label already resolved; patch immediately.
                self.ops[addr].p2 = *target;
            }
        }

        addr
    }

    /// Resolve a label to the current address and patch all forward refs.
    pub fn resolve_label(&mut self, label: Label) {
        let addr = i32::try_from(self.current_addr()).expect("program too large");
        self.resolve_label_to(label, addr);
    }

    /// Resolve a label to an explicit address (used for some control patterns).
    pub fn resolve_label_to(&mut self, label: Label, address: i32) {
        let idx = usize::try_from(label.0).expect("label fits usize");
        let state = self.labels.get_mut(idx).expect("label must exist");

        match state {
            LabelState::Unresolved(refs) => {
                // Patch all references.
                for &ref_addr in refs.iter() {
                    self.ops[ref_addr].p2 = address;
                }
                *state = LabelState::Resolved(address);
            }
            LabelState::Resolved(_) => {
                // Idempotent: resolving twice is allowed as long as it's consistent.
                *state = LabelState::Resolved(address);
            }
        }
    }

    // ── Register allocation ─────────────────────────────────────────────

    /// Allocate a single persistent register.
    pub fn alloc_reg(&mut self) -> i32 {
        self.regs.alloc_reg()
    }

    /// Allocate a contiguous block of persistent registers.
    pub fn alloc_regs(&mut self, n: i32) -> i32 {
        self.regs.alloc_regs(n)
    }

    /// Allocate a temporary register (reusable).
    pub fn alloc_temp(&mut self) -> i32 {
        self.regs.alloc_temp()
    }

    /// Return a temporary register to the pool.
    pub fn free_temp(&mut self, reg: i32) {
        self.regs.free_temp(reg);
    }

    /// Total registers allocated (high water mark).
    #[must_use]
    pub fn register_count(&self) -> i32 {
        self.regs.count()
    }

    // ── Finalization ────────────────────────────────────────────────────

    /// Validate all labels are resolved and return the finished program.
    pub fn finish(self) -> Result<VdbeProgram> {
        // Check for unresolved labels.
        for (i, state) in self.labels.iter().enumerate() {
            if let LabelState::Unresolved(refs) = state {
                if !refs.is_empty() {
                    return Err(FrankenError::Internal(format!(
                        "unresolved label {i} referenced by {} instruction(s)",
                        refs.len()
                    )));
                }
            }
        }

        Ok(VdbeProgram {
            ops: self.ops,
            register_count: self.regs.count(),
        })
    }
}

impl Default for ProgramBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A finalized VDBE bytecode program ready for execution.
#[derive(Debug, Clone, PartialEq)]
pub struct VdbeProgram {
    /// The instruction sequence.
    ops: Vec<VdbeOp>,
    /// Number of registers needed (high water mark from allocation).
    register_count: i32,
}

impl VdbeProgram {
    /// The instruction sequence.
    #[must_use]
    pub fn ops(&self) -> &[VdbeOp] {
        &self.ops
    }

    /// Number of instructions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Whether the program is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Number of registers required.
    #[must_use]
    pub fn register_count(&self) -> i32 {
        self.register_count
    }

    /// Get the instruction at the given program counter.
    #[must_use]
    pub fn get(&self, pc: usize) -> Option<&VdbeOp> {
        self.ops.get(pc)
    }

    /// Disassemble the program to a human-readable string.
    ///
    /// Output format matches SQLite's `EXPLAIN` output.
    #[must_use]
    pub fn disassemble(&self) -> String {
        use std::fmt::Write;

        let mut out = std::string::String::with_capacity(self.ops.len() * 60);
        out.push_str("addr  opcode           p1    p2    p3    p4                 p5\n");
        out.push_str("----  ---------------  ----  ----  ----  -----------------  --\n");

        for (addr, op) in self.ops.iter().enumerate() {
            let p4_str = match &op.p4 {
                P4::None => String::new(),
                P4::Int(v) => format!("(int){v}"),
                P4::Int64(v) => format!("(i64){v}"),
                P4::Real(v) => format!("(real){v}"),
                P4::Str(s) => format!("(str){s}"),
                P4::Blob(b) => format!("(blob)[{}B]", b.len()),
                P4::Collation(c) => format!("(coll){c}"),
                P4::FuncName(f) => format!("(func){f}"),
                P4::Table(t) => format!("(tbl){t}"),
                P4::Index(i) => format!("(idx){i}"),
                P4::Affinity(a) => format!("(aff){a}"),
            };

            writeln!(
                &mut out,
                "{addr:<4}  {:<15}  {:<4}  {:<4}  {:<4}  {:<17}  {:<2}",
                op.opcode.name(),
                op.p1,
                op.p2,
                op.p3,
                p4_str,
                op.p5,
            )
            .expect("write to string");
        }

        out
    }
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn opcode_count() {
        assert_eq!(Opcode::COUNT, 191);
    }

    #[test]
    fn opcode_name_roundtrip() {
        // Spot check a few opcodes
        assert_eq!(Opcode::Goto.name(), "Goto");
        assert_eq!(Opcode::Halt.name(), "Halt");
        assert_eq!(Opcode::Insert.name(), "Insert");
        assert_eq!(Opcode::Delete.name(), "Delete");
        assert_eq!(Opcode::ResultRow.name(), "ResultRow");
        assert_eq!(Opcode::Noop.name(), "Noop");
    }

    #[test]
    fn opcode_from_byte() {
        assert_eq!(Opcode::from_byte(0), None);
        assert_eq!(Opcode::from_byte(1), Some(Opcode::Goto));
        assert_eq!(Opcode::from_byte(8), Some(Opcode::Halt));
        assert_eq!(Opcode::from_byte(190), Some(Opcode::Noop));
        assert_eq!(Opcode::from_byte(191), None);
        assert_eq!(Opcode::from_byte(255), None);
    }

    #[test]
    fn opcode_from_byte_exhaustive() {
        // Every value 1..=190 should produce Some
        for i in 1..=190u8 {
            assert!(
                Opcode::from_byte(i).is_some(),
                "from_byte({i}) returned None"
            );
        }
    }

    #[test]
    fn test_opcode_distinct_u8_values() {
        let mut encoded = HashSet::new();
        for byte in 1..=190_u8 {
            let opcode = Opcode::from_byte(byte).expect("opcode byte must decode");
            let inserted = encoded.insert(opcode as u8);
            assert!(inserted, "duplicate opcode byte value for {:?}", opcode);
        }

        assert_eq!(encoded.len(), 190, "every opcode must map to a unique byte");
    }

    #[test]
    fn opcode_display() {
        assert_eq!(Opcode::Goto.to_string(), "Goto");
        assert_eq!(Opcode::Init.to_string(), "Init");
    }

    #[test]
    fn opcode_is_jump() {
        assert!(Opcode::Goto.is_jump());
        assert!(Opcode::If.is_jump());
        assert!(Opcode::IfNot.is_jump());
        assert!(Opcode::Eq.is_jump());
        assert!(Opcode::Next.is_jump());
        assert!(Opcode::Rewind.is_jump());
        assert!(Opcode::Init.is_jump());

        assert!(!Opcode::Integer.is_jump());
        assert!(!Opcode::Add.is_jump());
        assert!(!Opcode::Insert.is_jump());
        assert!(!Opcode::Noop.is_jump());
        assert!(!Opcode::ResultRow.is_jump());
    }

    #[test]
    fn vdbe_op_basic() {
        let op = VdbeOp {
            opcode: Opcode::Integer,
            p1: 42,
            p2: 1,
            p3: 0,
            p4: P4::None,
            p5: 0,
        };
        assert_eq!(op.opcode, Opcode::Integer);
        assert_eq!(op.p1, 42);
    }

    #[test]
    fn p4_variants() {
        let p4 = P4::Int(42);
        assert_eq!(p4, P4::Int(42));

        let p4 = P4::Str("hello".to_owned());
        assert_eq!(p4, P4::Str("hello".to_owned()));

        let p4 = P4::Real(3.14);
        assert_eq!(p4, P4::Real(3.14));
    }
}
