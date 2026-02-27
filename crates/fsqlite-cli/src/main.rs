use std::ffi::OsString;
use std::io::{self, BufRead, ErrorKind, Write};

use fsqlite::{Connection, Row};
use fsqlite_core::decode_proofs::{
    DECODE_PROOF_SCHEMA_VERSION_V1, DEFAULT_DECODE_PROOF_POLICY_ID, DEFAULT_DECODE_PROOF_SLACK,
    DecodeProofVerificationConfig, EcsDecodeProof, RejectedSymbol, SymbolDigest,
};
use serde::Deserialize;

const DEFAULT_DB_PATH: &str = ":memory:";
const PROMPT_PRIMARY: &str = "fsqlite> ";
const PROMPT_CONTINUATION: &str = "   ...> ";
const DEFAULT_VERIFY_POLICY_ID: u32 = DEFAULT_DECODE_PROOF_POLICY_ID;
const DEFAULT_VERIFY_SLACK: u32 = DEFAULT_DECODE_PROOF_SLACK;

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliOptions {
    db_path: String,
    command: Option<String>,
    verify_proof_path: Option<String>,
    verify_policy_id: u32,
    verify_slack: u32,
    show_help: bool,
}

#[derive(Debug, Deserialize)]
struct VerifyProofInput {
    proof: EcsDecodeProof,
    #[serde(default)]
    symbol_digests: Vec<SymbolDigest>,
    #[serde(default)]
    rejected_symbols: Vec<RejectedSymbol>,
    #[serde(default)]
    expected_policy_id: Option<u32>,
    #[serde(default)]
    decode_success_slack: Option<u32>,
}

fn main() {
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut stdout = io::stdout();
    let mut stderr = io::stderr();

    let exit_code = run(std::env::args_os(), &mut input, &mut stdout, &mut stderr);
    drop(input);
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

fn run<I, R, W, E>(args: I, input: &mut R, out: &mut W, err: &mut E) -> i32
where
    I: IntoIterator<Item = OsString>,
    R: BufRead,
    W: Write,
    E: Write,
{
    let options = match parse_args(args) {
        Ok(options) => options,
        Err(message) => {
            let _ = writeln!(err, "error: {message}");
            let _ = write_usage(err);
            return 2;
        }
    };

    if options.show_help {
        if write_usage(out).is_err() {
            return 1;
        }
        return 0;
    }

    if let Some(path) = options.verify_proof_path.as_deref() {
        return run_verify_proof(
            path,
            options.verify_policy_id,
            options.verify_slack,
            out,
            err,
        );
    }

    let connection = match Connection::open(&options.db_path) {
        Ok(connection) => connection,
        Err(error) => {
            let _ = writeln!(err, "error: {error}");
            return 1;
        }
    };

    if let Some(command) = options.command {
        return run_command(&connection, &command, out, err);
    }

    run_repl(&connection, input, out, err)
}

#[allow(clippy::too_many_lines)]
fn parse_args<I>(args: I) -> Result<CliOptions, String>
where
    I: IntoIterator<Item = OsString>,
{
    let mut iter = args.into_iter();
    let _argv0 = iter.next();

    let mut db_path = String::from(DEFAULT_DB_PATH);
    let mut has_path = false;
    let mut command: Option<String> = None;
    let mut verify_proof_path: Option<String> = None;
    let mut verify_policy_id = DEFAULT_VERIFY_POLICY_ID;
    let mut verify_slack = DEFAULT_VERIFY_SLACK;
    let mut show_help = false;

    while let Some(argument) = iter.next() {
        let arg = argument.to_string_lossy();
        let arg_str = arg.as_ref();

        match arg_str {
            "-h" | "--help" => {
                show_help = true;
            }
            "-c" | "--command" => {
                if verify_proof_path.is_some() {
                    return Err(String::from(
                        "`-c/--command` cannot be combined with `--verify-proof`",
                    ));
                }
                if command.is_some() {
                    return Err(String::from("`-c/--command` may only be provided once"));
                }
                let next = iter
                    .next()
                    .ok_or_else(|| String::from("missing SQL argument for `-c/--command`"))?;
                command = Some(next.to_string_lossy().into_owned());
            }
            "--verify-proof" => {
                if verify_proof_path.is_some() {
                    return Err(String::from("`--verify-proof` may only be provided once"));
                }
                if command.is_some() {
                    return Err(String::from(
                        "`--verify-proof` cannot be combined with `-c/--command`",
                    ));
                }
                if has_path {
                    return Err(String::from(
                        "`--verify-proof` cannot be combined with a DB path",
                    ));
                }
                let next = iter
                    .next()
                    .ok_or_else(|| String::from("missing JSON file path for `--verify-proof`"))?;
                verify_proof_path = Some(next.to_string_lossy().into_owned());
            }
            "--verify-policy-id" => {
                let next = iter.next().ok_or_else(|| {
                    String::from("missing integer argument for `--verify-policy-id`")
                })?;
                verify_policy_id =
                    parse_u32_option(next.to_string_lossy().as_ref(), "--verify-policy-id")?;
            }
            "--verify-slack" => {
                let next = iter
                    .next()
                    .ok_or_else(|| String::from("missing integer argument for `--verify-slack`"))?;
                verify_slack = parse_u32_option(next.to_string_lossy().as_ref(), "--verify-slack")?;
            }
            _ => {
                if let Some(value) = arg_str.strip_prefix("-c=") {
                    if verify_proof_path.is_some() {
                        return Err(String::from(
                            "`-c/--command` cannot be combined with `--verify-proof`",
                        ));
                    }
                    if command.is_some() {
                        return Err(String::from("`-c/--command` may only be provided once"));
                    }
                    command = Some(value.to_owned());
                    continue;
                }

                if let Some(value) = arg_str.strip_prefix("--command=") {
                    if verify_proof_path.is_some() {
                        return Err(String::from(
                            "`-c/--command` cannot be combined with `--verify-proof`",
                        ));
                    }
                    if command.is_some() {
                        return Err(String::from("`-c/--command` may only be provided once"));
                    }
                    command = Some(value.to_owned());
                    continue;
                }

                if let Some(value) = arg_str.strip_prefix("--verify-proof=") {
                    if verify_proof_path.is_some() {
                        return Err(String::from("`--verify-proof` may only be provided once"));
                    }
                    if command.is_some() {
                        return Err(String::from(
                            "`--verify-proof` cannot be combined with `-c/--command`",
                        ));
                    }
                    if has_path {
                        return Err(String::from(
                            "`--verify-proof` cannot be combined with a DB path",
                        ));
                    }
                    verify_proof_path = Some(value.to_owned());
                    continue;
                }

                if let Some(value) = arg_str.strip_prefix("--verify-policy-id=") {
                    verify_policy_id = parse_u32_option(value, "--verify-policy-id")?;
                    continue;
                }

                if let Some(value) = arg_str.strip_prefix("--verify-slack=") {
                    verify_slack = parse_u32_option(value, "--verify-slack")?;
                    continue;
                }

                if arg_str.starts_with('-') {
                    return Err(format!("unknown option `{arg_str}`"));
                }

                if verify_proof_path.is_some() {
                    return Err(String::from(
                        "DB path cannot be combined with `--verify-proof`",
                    ));
                }
                if has_path {
                    return Err(String::from(
                        "too many positional arguments; expected at most one DB path",
                    ));
                }

                arg_str.clone_into(&mut db_path);
                has_path = true;
            }
        }
    }

    Ok(CliOptions {
        db_path,
        command,
        verify_proof_path,
        verify_policy_id,
        verify_slack,
        show_help,
    })
}

fn parse_u32_option(value: &str, flag: &str) -> Result<u32, String> {
    value
        .parse::<u32>()
        .map_err(|_| format!("invalid integer for `{flag}`: `{value}`"))
}

fn run_verify_proof<W, E>(
    path: &str,
    verify_policy_id: u32,
    verify_slack: u32,
    out: &mut W,
    err: &mut E,
) -> i32
where
    W: Write,
    E: Write,
{
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) => {
            let _ = writeln!(err, "error: failed reading proof input `{path}`: {error}");
            return 1;
        }
    };
    let parsed: VerifyProofInput = match serde_json::from_str(&contents) {
        Ok(parsed) => parsed,
        Err(error) => {
            let _ = writeln!(err, "error: invalid proof input JSON `{path}`: {error}");
            return 1;
        }
    };

    let config = DecodeProofVerificationConfig {
        expected_schema_version: DECODE_PROOF_SCHEMA_VERSION_V1,
        expected_policy_id: parsed.expected_policy_id.unwrap_or(verify_policy_id),
        decode_success_slack: parsed.decode_success_slack.unwrap_or(verify_slack),
    };
    let report =
        parsed
            .proof
            .verification_report(config, &parsed.symbol_digests, &parsed.rejected_symbols);

    let rendered = match serde_json::to_string_pretty(&report) {
        Ok(json) => json,
        Err(error) => {
            let _ = writeln!(
                err,
                "error: failed serializing verification report: {error}"
            );
            return 1;
        }
    };
    if writeln!(out, "{rendered}").is_err() {
        let _ = writeln!(err, "error: failed writing verification report");
        return 1;
    }

    if report.ok {
        0
    } else {
        let _ = writeln!(
            err,
            "error: proof verification failed with {} issue(s)",
            report.issues.len()
        );
        1
    }
}

fn run_command<W, E>(connection: &Connection, command: &str, out: &mut W, err: &mut E) -> i32
where
    W: Write,
    E: Write,
{
    i32::from(!execute_sql(connection, command, out, err))
}

fn run_repl<R, W, E>(connection: &Connection, input: &mut R, out: &mut W, err: &mut E) -> i32
where
    R: BufRead,
    W: Write,
    E: Write,
{
    let mut pending_sql = String::new();
    let mut line_buffer = String::new();

    loop {
        let prompt = if pending_sql.trim().is_empty() {
            PROMPT_PRIMARY
        } else {
            PROMPT_CONTINUATION
        };

        if write!(out, "{prompt}").and_then(|()| out.flush()).is_err() {
            return 1;
        }

        line_buffer.clear();
        let bytes_read = match input.read_line(&mut line_buffer) {
            Ok(bytes_read) => bytes_read,
            Err(error) if error.kind() == ErrorKind::Interrupted => {
                // Keep the shell alive on Ctrl-C style interrupts.
                pending_sql.clear();
                let _ = writeln!(out);
                continue;
            }
            Err(error) => {
                let _ = writeln!(err, "error: {error}");
                return 1;
            }
        };

        if bytes_read == 0 {
            if !pending_sql.trim().is_empty() {
                let _ = execute_sql(connection, pending_sql.trim(), out, err);
            }
            return 0;
        }

        let line = line_buffer.trim_end_matches(['\n', '\r']);
        let trimmed = line.trim();

        if pending_sql.trim().is_empty() {
            if matches!(trimmed, ".exit" | ".quit") {
                return 0;
            }

            if trimmed == ".help" {
                if write_repl_help(out).is_err() {
                    return 1;
                }
                continue;
            }

            if try_execute_read_command(trimmed, connection, out, err) {
                continue;
            }

            if trimmed.is_empty() {
                continue;
            }
        }

        if !pending_sql.is_empty() {
            pending_sql.push('\n');
        }
        pending_sql.push_str(line);

        if statement_complete(&pending_sql) {
            let _ = execute_sql(connection, pending_sql.trim(), out, err);
            pending_sql.clear();
        }
    }
}

fn execute_sql<W, E>(connection: &Connection, sql: &str, out: &mut W, err: &mut E) -> bool
where
    W: Write,
    E: Write,
{
    match connection.query(sql) {
        Ok(rows) => {
            if write_rows(&rows, out).is_err() {
                let _ = writeln!(err, "error: failed writing query results");
                return false;
            }
            true
        }
        Err(error) => {
            let _ = writeln!(err, "error: {error}");
            false
        }
    }
}

fn write_rows<W>(rows: &[Row], out: &mut W) -> io::Result<()>
where
    W: Write,
{
    for row in rows {
        writeln!(out, "{}", format_row(row))?;
    }
    Ok(())
}

fn format_row(row: &Row) -> String {
    row.values()
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join(" | ")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementScanState {
    Normal,
    SingleQuote,
    DoubleQuote,
    Backtick,
    BracketIdent,
    LineComment,
    BlockComment,
}

impl StatementScanState {
    const fn is_unterminated(self) -> bool {
        matches!(
            self,
            Self::SingleQuote
                | Self::DoubleQuote
                | Self::Backtick
                | Self::BracketIdent
                | Self::BlockComment
        )
    }
}

fn is_line_comment_start(bytes: &[u8], i: usize) -> bool {
    if bytes.get(i) != Some(&b'-') {
        return false;
    }
    if bytes.get(i + 1) != Some(&b'-') {
        return false;
    }
    true
}

fn is_block_comment_start(bytes: &[u8], i: usize) -> bool {
    bytes.get(i) == Some(&b'/') && bytes.get(i + 1) == Some(&b'*')
}

fn is_block_comment_end(bytes: &[u8], i: usize) -> bool {
    bytes.get(i) == Some(&b'*') && bytes.get(i + 1) == Some(&b'/')
}

fn statement_complete(buffer: &str) -> bool {
    let bytes = buffer.as_bytes();
    let mut state = StatementScanState::Normal;
    let mut last_significant: Option<u8> = None;

    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        match state {
            StatementScanState::Normal => {
                if b.is_ascii_whitespace() {
                    i += 1;
                    continue;
                }

                if is_line_comment_start(bytes, i) {
                    state = StatementScanState::LineComment;
                    i += 2;
                    continue;
                }

                if is_block_comment_start(bytes, i) {
                    state = StatementScanState::BlockComment;
                    i += 2;
                    continue;
                }

                last_significant = Some(b);

                match b {
                    b'\'' => state = StatementScanState::SingleQuote,
                    b'"' => state = StatementScanState::DoubleQuote,
                    b'`' => state = StatementScanState::Backtick,
                    b'[' => state = StatementScanState::BracketIdent,
                    _ => {}
                }

                i += 1;
            }
            StatementScanState::SingleQuote => {
                if b == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                    } else {
                        state = StatementScanState::Normal;
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
            StatementScanState::DoubleQuote => {
                if b == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                    } else {
                        state = StatementScanState::Normal;
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
            StatementScanState::Backtick => {
                if b == b'`' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'`' {
                        i += 2;
                    } else {
                        state = StatementScanState::Normal;
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
            StatementScanState::BracketIdent => {
                if b == b']' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b']' {
                        i += 2;
                    } else {
                        state = StatementScanState::Normal;
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
            StatementScanState::LineComment => {
                if b == b'\n' || b == b'\r' {
                    state = StatementScanState::Normal;
                }
                i += 1;
            }
            StatementScanState::BlockComment => {
                if is_block_comment_end(bytes, i) {
                    state = StatementScanState::Normal;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        }
    }

    if state.is_unterminated() {
        return false;
    }

    last_significant == Some(b';')
}

fn try_execute_read_command<W, E>(
    trimmed: &str,
    connection: &Connection,
    out: &mut W,
    err: &mut E,
) -> bool
where
    W: Write,
    E: Write,
{
    let Some(rest) = trimmed.strip_prefix(".read") else {
        return false;
    };

    if let Some(first_char) = rest.chars().next() {
        if !first_char.is_whitespace() {
            return false;
        }
    }

    let path = rest.trim();
    if path.is_empty() {
        let _ = writeln!(err, "error: .read requires a file path");
        return true;
    }

    match std::fs::read_to_string(path) {
        Ok(contents) => {
            let _ = execute_sql(connection, &contents, out, err);
        }
        Err(error) => {
            let _ = writeln!(err, "error: {error}");
        }
    }

    true
}

fn write_usage<W>(out: &mut W) -> io::Result<()>
where
    W: Write,
{
    writeln!(
        out,
        "Usage: fsqlite [DB_PATH] [-c|--command SQL]\n\
         \n\
         Verify decode proof JSON:\n\
         fsqlite --verify-proof proof.json [--verify-policy-id N] [--verify-slack N]\n\
         \n\
         Examples:\n\
         \n\
         fsqlite\n\
         fsqlite app.db\n\
         fsqlite -c \"SELECT 1 + 2;\"\n\
         fsqlite app.db --command \"SELECT * FROM users;\"\n\
         fsqlite --verify-proof decode_proof.json\n",
    )
}

fn write_repl_help<W>(out: &mut W) -> io::Result<()>
where
    W: Write,
{
    writeln!(
        out,
        "Dot commands:\n\
         \n\
         .help      Show this help\n\
         .quit      Exit the shell\n\
         .exit      Exit the shell\n\
         .read FILE Execute SQL from file\n\
         \n\
         Enter SQL statements terminated by `;`.\n",
    )
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::fs;
    use std::io::{self, BufRead, Cursor, Read};
    use std::time::{SystemTime, UNIX_EPOCH};

    use fsqlite_core::decode_proofs::{
        EcsDecodeProof, RejectedSymbol, SymbolDigest, SymbolRejectionReason,
    };
    use fsqlite_types::ObjectId;
    use serde_json::json;

    use super::{format_row, parse_args, run, statement_complete};

    fn parse_from(args: &[&str]) -> Result<super::CliOptions, String> {
        let os_args: Vec<OsString> = args.iter().map(OsString::from).collect();
        parse_args(os_args)
    }

    #[derive(Debug)]
    struct InterruptOnceBufRead {
        interrupted_once: bool,
        inner: Cursor<Vec<u8>>,
    }

    impl InterruptOnceBufRead {
        fn new(bytes: Vec<u8>) -> Self {
            Self {
                interrupted_once: false,
                inner: Cursor::new(bytes),
            }
        }
    }

    impl Read for InterruptOnceBufRead {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.inner.read(buf)
        }
    }

    impl BufRead for InterruptOnceBufRead {
        fn fill_buf(&mut self) -> io::Result<&[u8]> {
            self.inner.fill_buf()
        }

        fn consume(&mut self, amt: usize) {
            self.inner.consume(amt);
        }

        fn read_line(&mut self, buf: &mut String) -> io::Result<usize> {
            if !self.interrupted_once {
                self.interrupted_once = true;
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "simulated interrupt",
                ));
            }
            self.inner.read_line(buf)
        }
    }

    #[test]
    fn test_parse_defaults() {
        let options = parse_from(&["fsqlite"]).expect("default args should parse");
        assert_eq!(options.db_path, ":memory:");
        assert_eq!(options.command, None);
        assert!(!options.show_help);
    }

    #[test]
    fn test_parse_db_path_and_command() {
        let options =
            parse_from(&["fsqlite", "demo.db", "-c", "SELECT 1;"]).expect("args should parse");
        assert_eq!(options.db_path, "demo.db");
        assert_eq!(options.command.as_deref(), Some("SELECT 1;"));
    }

    #[test]
    fn test_parse_command_equals_form() {
        let options = parse_from(&["fsqlite", "--command=SELECT 2;"]).expect("args should parse");
        assert_eq!(options.command.as_deref(), Some("SELECT 2;"));
    }

    #[test]
    fn test_parse_verify_proof_mode() {
        let options = parse_from(&[
            "fsqlite",
            "--verify-proof",
            "proof.json",
            "--verify-policy-id",
            "7",
            "--verify-slack=3",
        ])
        .expect("verify-proof args should parse");
        assert_eq!(options.verify_proof_path.as_deref(), Some("proof.json"));
        assert_eq!(options.verify_policy_id, 7);
        assert_eq!(options.verify_slack, 3);
        assert!(options.command.is_none());
    }

    #[test]
    fn test_parse_verify_proof_conflicts_with_command() {
        let error = parse_from(&["fsqlite", "--verify-proof", "proof.json", "-c", "SELECT 1;"])
            .expect_err("verify-proof and command should conflict");
        assert!(error.contains("cannot be combined"));
    }

    #[test]
    fn test_parse_unknown_option_fails() {
        let error = parse_from(&["fsqlite", "--wat"]).expect_err("unknown option should fail");
        assert!(error.contains("unknown option"));
    }

    #[test]
    fn test_parse_multiple_paths_fails() {
        let error = parse_from(&["fsqlite", "a.db", "b.db"])
            .expect_err("multiple positional args should fail");
        assert!(error.contains("too many positional arguments"));
    }

    #[test]
    fn test_statement_complete_requires_trailing_semicolon() {
        assert!(statement_complete("SELECT 1;"));
        assert!(statement_complete("SELECT 1;\n"));
        assert!(!statement_complete("SELECT 1"));
    }

    #[test]
    fn test_statement_complete_allows_trailing_line_comment() {
        assert!(statement_complete("SELECT 1; -- comment"));
        assert!(statement_complete("SELECT 1;-- comment"));
        assert!(statement_complete("SELECT 1;\n-- comment"));
        assert!(statement_complete("SELECT 1; -- comment\n"));
    }

    #[test]
    fn test_statement_complete_allows_trailing_block_comment() {
        assert!(statement_complete("SELECT 1; /* comment */"));
        assert!(statement_complete("SELECT 1; /* multi\nline\ncomment */"));
        assert!(!statement_complete("SELECT 1; /* unterminated"));
    }

    #[test]
    fn test_statement_complete_ignores_semicolon_in_string_literal() {
        assert!(!statement_complete("SELECT ';'"));
        assert!(statement_complete("SELECT ';';"));
        assert!(statement_complete("SELECT 'it''s; fine';"));
    }

    #[test]
    fn test_statement_complete_treats_double_minus_as_comment() {
        // SQLite treats `--` as a comment regardless of whitespace.
        assert!(!statement_complete("SELECT 1--2;")); // semicolon is part of the comment
        assert!(statement_complete("SELECT 1--2;\n;")); // semicolon on next line completes it
    }

    #[test]
    fn test_format_row_joins_with_pipes() {
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = vec![
            OsString::from("fsqlite"),
            OsString::from("-c"),
            OsString::from("SELECT 1, 'x';"),
        ];
        let exit_code = run(args, &mut input, &mut out, &mut err);
        assert_eq!(exit_code, 0);

        let stdout = String::from_utf8(out).expect("output should be utf-8");
        assert!(
            stdout.contains("1 | 'x'"),
            "expected rendered row in output, got: {stdout}",
        );
    }

    #[test]
    fn test_repl_quit_command_exits_cleanly() {
        let mut input = Cursor::new(b".quit\n".to_vec());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = vec![OsString::from("fsqlite")];

        let exit_code = run(args, &mut input, &mut out, &mut err);
        assert_eq!(exit_code, 0);
        assert!(err.is_empty(), "unexpected stderr: {:?}", err);
    }

    #[test]
    fn test_repl_executes_statement_then_quits() {
        let mut input = Cursor::new(b"SELECT 7;\n.quit\n".to_vec());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = vec![OsString::from("fsqlite")];

        let exit_code = run(args, &mut input, &mut out, &mut err);
        assert_eq!(exit_code, 0);
        assert!(err.is_empty(), "unexpected stderr: {:?}", err);

        let stdout = String::from_utf8(out).expect("output should be utf-8");
        assert!(stdout.contains('7'), "expected query result in output");
    }

    #[test]
    fn test_repl_read_line_interrupted_keeps_shell_running() {
        let mut input = InterruptOnceBufRead::new(b".quit\n".to_vec());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = vec![OsString::from("fsqlite")];

        let exit_code = run(args, &mut input, &mut out, &mut err);
        assert_eq!(exit_code, 0);
        assert!(err.is_empty(), "unexpected stderr: {:?}", err);
    }

    #[test]
    fn test_repl_read_command_executes_sql_from_file() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after UNIX_EPOCH");
        let file_name = format!(
            "fsqlite_cli_read_{}_{}.sql",
            std::process::id(),
            now.as_nanos()
        );
        let path = std::env::temp_dir().join(file_name);

        fs::write(&path, "SELECT 42;\n").expect("temp SQL file should be writable");

        let input_script = format!(".read {}\n.quit\n", path.display());
        let mut input = Cursor::new(input_script.into_bytes());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = vec![OsString::from("fsqlite")];

        let exit_code = run(args, &mut input, &mut out, &mut err);
        let _ = fs::remove_file(&path);

        assert_eq!(exit_code, 0);
        assert!(err.is_empty(), "unexpected stderr: {:?}", err);

        let stdout = String::from_utf8(out).expect("output should be utf-8");
        assert!(
            stdout.contains("42"),
            "expected .read query output in stdout"
        );
    }

    #[test]
    fn test_repl_read_command_requires_path() {
        let mut input = Cursor::new(b".read\n.quit\n".to_vec());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = vec![OsString::from("fsqlite")];

        let exit_code = run(args, &mut input, &mut out, &mut err);
        assert_eq!(exit_code, 0);

        let stderr = String::from_utf8(err).expect("stderr should be utf-8");
        assert!(
            stderr.contains(".read requires a file path"),
            "expected .read path error in stderr",
        );
    }

    #[test]
    fn test_format_row_helper_with_connection_row() {
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = vec![
            OsString::from("fsqlite"),
            OsString::from("-c"),
            OsString::from("SELECT NULL;"),
        ];
        let exit_code = run(args, &mut input, &mut out, &mut err);
        assert_eq!(exit_code, 0);

        // Also directly exercise `format_row` on a real row.
        let conn = fsqlite::Connection::open(":memory:").expect("connection should open");
        let row = conn
            .query_row("SELECT 10, 'abc', NULL;")
            .expect("query_row should succeed");
        let rendered = format_row(&row);
        assert_eq!(rendered, "10 | 'abc' | NULL");
    }

    #[test]
    fn test_verify_proof_cli_success() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after UNIX_EPOCH");
        let file_name = format!(
            "fsqlite_cli_verify_proof_ok_{}_{}.json",
            std::process::id(),
            now.as_nanos()
        );
        let path = std::env::temp_dir().join(file_name);

        let oid = ObjectId::derive_from_canonical_bytes(b"cli-proof-ok");
        let symbol_digests = vec![
            SymbolDigest {
                esi: 0,
                digest_xxh3: 101,
            },
            SymbolDigest {
                esi: 1,
                digest_xxh3: 202,
            },
        ];
        let rejected = vec![RejectedSymbol {
            esi: 9,
            reason: SymbolRejectionReason::HashMismatch,
        }];
        let proof = EcsDecodeProof::from_esis(oid, 4, &[0, 1, 2, 3, 4, 5], true, Some(4), 1, 42)
            .with_symbol_digests(symbol_digests.clone())
            .with_rejected_symbols(rejected.clone());
        let payload = json!({
            "proof": proof,
            "symbol_digests": symbol_digests,
            "rejected_symbols": rejected
        });
        fs::write(
            &path,
            serde_json::to_string_pretty(&payload).expect("serialize proof payload"),
        )
        .expect("write verify-proof payload");

        let mut input = Cursor::new(Vec::<u8>::new());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = vec![
            OsString::from("fsqlite"),
            OsString::from("--verify-proof"),
            path.as_os_str().to_os_string(),
        ];
        let exit_code = run(args, &mut input, &mut out, &mut err);
        let _ = fs::remove_file(&path);

        assert_eq!(exit_code, 0);
        assert!(err.is_empty(), "unexpected stderr: {:?}", err);
        let stdout = String::from_utf8(out).expect("stdout should be utf-8");
        assert!(
            stdout.contains("\"ok\": true"),
            "expected successful verification report, got: {stdout}",
        );
    }

    #[test]
    fn test_verify_proof_cli_failure_on_policy_mismatch() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after UNIX_EPOCH");
        let file_name = format!(
            "fsqlite_cli_verify_proof_fail_{}_{}.json",
            std::process::id(),
            now.as_nanos()
        );
        let path = std::env::temp_dir().join(file_name);

        let oid = ObjectId::derive_from_canonical_bytes(b"cli-proof-fail");
        let proof = EcsDecodeProof::from_esis(oid, 4, &[0, 1, 2, 3, 4, 5], true, Some(4), 1, 42);
        let payload = json!({
            "proof": proof,
            "symbol_digests": [],
            "rejected_symbols": []
        });
        fs::write(
            &path,
            serde_json::to_string_pretty(&payload).expect("serialize proof payload"),
        )
        .expect("write verify-proof payload");

        let mut input = Cursor::new(Vec::<u8>::new());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = vec![
            OsString::from("fsqlite"),
            OsString::from("--verify-proof"),
            path.as_os_str().to_os_string(),
            OsString::from("--verify-policy-id"),
            OsString::from("999"),
        ];
        let exit_code = run(args, &mut input, &mut out, &mut err);
        let _ = fs::remove_file(&path);

        assert_eq!(exit_code, 1);
        let stdout = String::from_utf8(out).expect("stdout should be utf-8");
        assert!(
            stdout.contains("policy_id_mismatch"),
            "expected policy mismatch in report, got: {stdout}",
        );
        let stderr = String::from_utf8(err).expect("stderr should be utf-8");
        assert!(
            stderr.contains("proof verification failed"),
            "expected failure summary in stderr, got: {stderr}",
        );
    }
}
