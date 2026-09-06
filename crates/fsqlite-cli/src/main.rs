// The `#[cfg(test)]` module drives deeply nested engine futures through
// `run_test`; the default 128 is not enough for the trait-solver queries those
// generate. Harmless for the binary itself (the limit is a compiler budget).
#![recursion_limit = "512"]
// bd-h9o9r: the CLI drives fsqlite-core's deliberately non-`Send`, deeply
// nested engine futures (the same nesting behind the recursion_limit
// above); `future_not_send` and `large_futures` contradict that design —
// see fsqlite-core/src/lib.rs for the full rationale, including why boxing
// was rejected by the perf ledger. Pre-existing cache-masked redness (68
// findings on untouched main), same family as the rest of the chain.
#![allow(clippy::future_not_send)]
#![allow(clippy::large_futures)]

use std::borrow::Cow;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::future::Future;
use std::io::{self, BufRead, ErrorKind, IsTerminal, Write};
use std::path::Path;
use std::pin::Pin;

use fsqlite::{Connection, Row, SqliteValue};
use fsqlite_core::decode_proofs::{
    DECODE_PROOF_SCHEMA_VERSION_V1, DEFAULT_DECODE_PROOF_POLICY_ID, DEFAULT_DECODE_PROOF_SLACK,
    DecodeProofVerificationConfig, EcsDecodeProof, RejectedSymbol, SymbolDigest,
};
use fsqlite_parser::Parser;
use fsqlite_types::value::format_sqlite_float_g;
use serde::Deserialize;

const DEFAULT_DB_PATH: &str = ":memory:";
const PROMPT_PRIMARY: &str = "fsqlite> ";
const PROMPT_CONTINUATION: &str = "   ...> ";
const DEFAULT_VERIFY_POLICY_ID: u32 = DEFAULT_DECODE_PROOF_POLICY_ID;
const DEFAULT_VERIFY_SLACK: u32 = DEFAULT_DECODE_PROOF_SLACK;
const ANSI_RESET: &str = "\x1b[0m";
const ANSI_BOLD_CYAN: &str = "\x1b[1;36m";
const ANSI_BOLD_BLUE: &str = "\x1b[1;34m";
const ANSI_GREEN: &str = "\x1b[32m";
const ANSI_MAGENTA: &str = "\x1b[35m";
const ANSI_YELLOW: &str = "\x1b[33m";
const ANSI_DIM: &str = "\x1b[2m";

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliOptions {
    db_path: String,
    command: Option<String>,
    init_path: Option<String>,
    verify_proof_path: Option<String>,
    verify_policy_id: u32,
    verify_slack: u32,
    force_batch: bool,
    show_help: bool,
    show_version: bool,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ShellOptions {
    show_prompts: bool,
    colorize_prompts: bool,
    fail_on_error: bool,
}

impl ShellOptions {
    #[cfg(test)]
    const fn interactive() -> Self {
        Self {
            show_prompts: true,
            colorize_prompts: false,
            fail_on_error: false,
        }
    }

    const fn batch() -> Self {
        Self {
            show_prompts: false,
            colorize_prompts: false,
            fail_on_error: true,
        }
    }

    #[allow(clippy::unused_self)] // signature parallels nested_script for symmetry
    const fn forced_batch(self) -> Self {
        Self {
            show_prompts: false,
            colorize_prompts: false,
            fail_on_error: true,
        }
    }

    const fn nested_script(self) -> Self {
        Self {
            show_prompts: false,
            colorize_prompts: self.colorize_prompts,
            fail_on_error: self.fail_on_error,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    List,
    Column,
    Csv,
    Tabs,
    Line,
    /// `.mode quote`: SQL-literal rendering (text `'..'`, blob `X'..'`, NULL
    /// bare, numbers bare) with a comma separator — matches sqlite3's quote
    /// mode, the format oracle-comparison tooling reads back.
    Quote,
}

impl OutputMode {
    fn parse(raw: &str) -> Option<Self> {
        match raw.to_ascii_lowercase().as_str() {
            "list" => Some(Self::List),
            "column" | "columns" => Some(Self::Column),
            "csv" => Some(Self::Csv),
            "tabs" | "tab" => Some(Self::Tabs),
            "line" => Some(Self::Line),
            "quote" => Some(Self::Quote),
            _ => None,
        }
    }

    const fn separator(self) -> &'static str {
        match self {
            // sqlite3's default list mode joins columns with a bare `|`
            // (`SEP_Column`), which its oracle-diff tooling reads back.
            Self::List => "|",
            Self::Column => "  ",
            Self::Csv | Self::Quote => ",",
            Self::Tabs => "\t",
            Self::Line => "",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OutputOptions {
    mode: OutputMode,
    headers: bool,
    headers_explicit: bool,
}

impl Default for OutputOptions {
    fn default() -> Self {
        Self {
            mode: OutputMode::List,
            headers: false,
            headers_explicit: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShellFlow {
    Continue,
    Exit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ShellOutcome {
    flow: ShellFlow,
    had_error: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DotCommandResult {
    NotHandled,
    Continue,
    Exit,
}

fn main() {
    let stdin = io::stdin();
    let interactive_input = stdin.is_terminal();
    let mut input = stdin.lock();
    let mut stdout = io::stdout();
    let interactive_output = stdout.is_terminal();
    let mut stderr = io::stderr();
    let shell_options = ShellOptions {
        show_prompts: interactive_input && interactive_output,
        colorize_prompts: interactive_output,
        fail_on_error: !interactive_input,
    };

    // The CLI is the top-level consumer, so it owns the async runtime and hands
    // the resulting `Cx` down into the engine. FrankenSQLite itself never builds
    // a runtime (AGENTS.md).
    let runtime = match asupersync::runtime::RuntimeBuilder::current_thread().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = writeln!(stderr, "error: failed to start async runtime: {error}");
            std::process::exit(1);
        }
    };
    let exit_code = runtime.block_on(run_with_shell_options(
        std::env::args_os(),
        &mut input,
        &mut stdout,
        &mut stderr,
        shell_options,
    ));
    drop(input);
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

#[cfg(test)]
async fn run<I, R, W, E>(args: I, input: &mut R, out: &mut W, err: &mut E) -> i32
where
    I: IntoIterator<Item = OsString>,
    R: BufRead,
    W: Write,
    E: Write,
{
    run_with_shell_options(args, input, out, err, ShellOptions::interactive()).await
}

async fn run_with_shell_options<I, R, W, E>(
    args: I,
    input: &mut R,
    out: &mut W,
    err: &mut E,
    shell_options: ShellOptions,
) -> i32
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

    if options.show_version {
        if writeln!(out, "fsqlite {}", env!("CARGO_PKG_VERSION")).is_err() {
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

    let shell_options = if options.force_batch {
        shell_options.forced_batch()
    } else {
        shell_options
    };
    let mut current_db_path = options.db_path.clone();
    // bd-fo6xw: the CLI owns the process runtime (built in main and alive
    // for the whole session), so a RuntimeContext constructed HERE — inside
    // that runtime — captures its strong handle, giving every operation an
    // io_uring driver spawner that outlives the connection. The default
    // process-global env deliberately has no runtime handle, so plain
    // Connection::open keeps the Unix fallback; this is the opt-in surface.
    let cli_runtime_env = fsqlite::ConnectionEnv::new(std::sync::Arc::new(
        fsqlite::RuntimeContext::new(fsqlite::RuntimeConfig::default()),
    ));
    let mut connection =
        match Connection::open_with_env(&options.db_path, cli_runtime_env.clone()).await {
            Ok(connection) => connection,
            Err(error) => {
                let _ = writeln!(err, "error: {error}");
                return 1;
            }
        };
    let mut output_options = OutputOptions::default();

    // Keep every post-open exit inside this block so shutdown is awaited for
    // EOF, dot-command exit, startup failure and command-mode completion.
    let exit_code = async {
        if let Some(path) = options.init_path.as_deref() {
            let Some(outcome) = execute_script_file(
                path,
                &mut connection,
                &mut current_db_path,
                &mut output_options,
                out,
                err,
                shell_options.nested_script(),
            )
            .await
            else {
                return 1;
            };
            if shell_options.fail_on_error && outcome.had_error {
                return 1;
            }
            if outcome.flow == ShellFlow::Exit {
                return 0;
            }
        }

        if let Some(command) = options.command {
            return run_command(
                &mut connection,
                &mut current_db_path,
                &mut output_options,
                &command,
                out,
                err,
            )
            .await;
        }

        run_repl(
            &mut connection,
            &mut current_db_path,
            &mut output_options,
            input,
            out,
            err,
            shell_options,
        )
        .await
    }
    .await;

    if let Err(error) = connection.close_in_place().await {
        let _ = writeln!(err, "error: closing database: {error}");
        return 1;
    }
    exit_code
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
    let mut init_path: Option<String> = None;
    let mut verify_proof_path: Option<String> = None;
    let mut verify_policy_id = DEFAULT_VERIFY_POLICY_ID;
    let mut verify_slack = DEFAULT_VERIFY_SLACK;
    let mut verify_policy_id_set = false;
    let mut verify_slack_set = false;
    let mut force_batch = false;
    let mut show_help = false;
    let mut show_version = false;

    while let Some(argument) = iter.next() {
        let arg = argument.to_string_lossy();
        let arg_str = arg.as_ref();

        match arg_str {
            "-h" | "--help" => {
                show_help = true;
            }
            "-V" | "--version" => {
                show_version = true;
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
            "-batch" | "--batch" => {
                force_batch = true;
            }
            "-init" | "--init" => {
                if verify_proof_path.is_some() {
                    return Err(String::from(
                        "`-init/--init` cannot be combined with `--verify-proof`",
                    ));
                }
                if init_path.is_some() {
                    return Err(String::from("`-init/--init` may only be provided once"));
                }
                let next = iter
                    .next()
                    .ok_or_else(|| String::from("missing file path for `-init/--init`"))?;
                init_path = Some(next.to_string_lossy().into_owned());
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
                if verify_policy_id_set {
                    return Err(String::from(
                        "`--verify-policy-id` may only be provided once",
                    ));
                }
                let next = iter.next().ok_or_else(|| {
                    String::from("missing integer argument for `--verify-policy-id`")
                })?;
                verify_policy_id =
                    parse_u32_option(next.to_string_lossy().as_ref(), "--verify-policy-id")?;
                verify_policy_id_set = true;
            }
            "--verify-slack" => {
                if verify_slack_set {
                    return Err(String::from("`--verify-slack` may only be provided once"));
                }
                let next = iter
                    .next()
                    .ok_or_else(|| String::from("missing integer argument for `--verify-slack`"))?;
                verify_slack = parse_u32_option(next.to_string_lossy().as_ref(), "--verify-slack")?;
                verify_slack_set = true;
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

                if let Some(value) = arg_str.strip_prefix("--init=") {
                    if verify_proof_path.is_some() {
                        return Err(String::from(
                            "`-init/--init` cannot be combined with `--verify-proof`",
                        ));
                    }
                    if init_path.is_some() {
                        return Err(String::from("`-init/--init` may only be provided once"));
                    }
                    init_path = Some(value.to_owned());
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
                    if verify_policy_id_set {
                        return Err(String::from(
                            "`--verify-policy-id` may only be provided once",
                        ));
                    }
                    verify_policy_id = parse_u32_option(value, "--verify-policy-id")?;
                    verify_policy_id_set = true;
                    continue;
                }

                if let Some(value) = arg_str.strip_prefix("--verify-slack=") {
                    if verify_slack_set {
                        return Err(String::from("`--verify-slack` may only be provided once"));
                    }
                    verify_slack = parse_u32_option(value, "--verify-slack")?;
                    verify_slack_set = true;
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

    if !show_help && verify_proof_path.is_none() && (verify_policy_id_set || verify_slack_set) {
        return Err(String::from(
            "`--verify-policy-id` and `--verify-slack` require `--verify-proof`",
        ));
    }

    Ok(CliOptions {
        db_path,
        command,
        init_path,
        verify_proof_path,
        verify_policy_id,
        verify_slack,
        force_batch,
        show_help,
        show_version,
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

async fn run_command<W, E>(
    connection: &mut Connection,
    current_db_path: &mut String,
    output_options: &mut OutputOptions,
    command: &str,
    out: &mut W,
    err: &mut E,
) -> i32
where
    W: Write,
    E: Write,
{
    let mut input = io::Cursor::new({
        let mut buffer = command.as_bytes().to_vec();
        if !buffer.ends_with(b"\n") {
            buffer.push(b'\n');
        }
        buffer
    });
    match run_shell(
        connection,
        current_db_path,
        output_options,
        &mut input,
        out,
        err,
        ShellOptions::batch(),
    )
    .await
    {
        Some(outcome) if !outcome.had_error => 0,
        Some(_) | None => 1,
    }
}

async fn run_repl<R, W, E>(
    connection: &mut Connection,
    current_db_path: &mut String,
    output_options: &mut OutputOptions,
    input: &mut R,
    out: &mut W,
    err: &mut E,
    shell_options: ShellOptions,
) -> i32
where
    R: BufRead,
    W: Write,
    E: Write,
{
    match run_shell(
        connection,
        current_db_path,
        output_options,
        input,
        out,
        err,
        shell_options,
    )
    .await
    {
        Some(outcome) if !(shell_options.fail_on_error && outcome.had_error) => 0,
        Some(_) | None => 1,
    }
}

// `.read FILE` can execute a script that itself contains `.read`, so this sits
// in a cycle with `run_shell`/`try_execute_dot_command`. Boxing this one future
// breaks the otherwise-infinite future type.
fn execute_script_file<'a, W, E>(
    path: &'a str,
    connection: &'a mut Connection,
    current_db_path: &'a mut String,
    output_options: &'a mut OutputOptions,
    out: &'a mut W,
    err: &'a mut E,
    shell_options: ShellOptions,
) -> Pin<Box<dyn Future<Output = Option<ShellOutcome>> + 'a>>
where
    W: Write,
    E: Write,
{
    Box::pin(async move {
        let contents = match std::fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(error) => {
                let _ = writeln!(err, "error: {error}");
                return None;
            }
        };
        let mut nested = io::Cursor::new(contents.into_bytes());
        run_shell(
            connection,
            current_db_path,
            output_options,
            &mut nested,
            out,
            err,
            shell_options,
        )
        .await
    })
}

async fn run_shell<R, W, E>(
    connection: &mut Connection,
    current_db_path: &mut String,
    output_options: &mut OutputOptions,
    input: &mut R,
    out: &mut W,
    err: &mut E,
    shell_options: ShellOptions,
) -> Option<ShellOutcome>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    let mut pending_sql = String::new();
    let mut line_buffer = String::new();
    let mut had_error = false;

    loop {
        if shell_options.show_prompts {
            let prompt = render_prompt(current_db_path, &pending_sql, shell_options);
            if write!(out, "{prompt}").and_then(|()| out.flush()).is_err() {
                return None;
            }
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
                return None;
            }
        };

        if bytes_read == 0 {
            if !pending_sql.trim().is_empty() {
                had_error |=
                    !execute_sql(connection, pending_sql.trim(), *output_options, out, err).await;
            }
            return Some(ShellOutcome {
                flow: ShellFlow::Continue,
                had_error,
            });
        }

        let line = line_buffer.trim_end_matches(['\n', '\r']);
        let trimmed = line.trim();

        if pending_sql.trim().is_empty() {
            if matches!(trimmed, ".exit" | ".quit") {
                return Some(ShellOutcome {
                    flow: ShellFlow::Exit,
                    had_error,
                });
            }

            if trimmed == ".help" {
                if write_repl_help(out).is_err() {
                    return None;
                }
                continue;
            }

            match try_execute_dot_command(
                trimmed,
                connection,
                current_db_path,
                output_options,
                out,
                err,
                shell_options,
                &mut had_error,
            )
            .await
            {
                DotCommandResult::NotHandled => {}
                DotCommandResult::Continue => continue,
                DotCommandResult::Exit => {
                    return Some(ShellOutcome {
                        flow: ShellFlow::Exit,
                        had_error,
                    });
                }
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
            had_error |=
                !execute_sql(connection, pending_sql.trim(), *output_options, out, err).await;
            pending_sql.clear();
        }
    }
}

fn render_prompt(current_db_path: &str, pending_sql: &str, shell_options: ShellOptions) -> String {
    let primary_prompt = pending_sql.trim().is_empty();
    let is_default_db = current_db_path == DEFAULT_DB_PATH;
    let label = (!is_default_db).then(|| prompt_db_label(current_db_path));
    if primary_prompt {
        return if shell_options.colorize_prompts {
            match label {
                Some(label) => {
                    format!(
                        "{ANSI_BOLD_CYAN}fsqlite{ANSI_RESET}[{ANSI_YELLOW}{label}{ANSI_RESET}]> "
                    )
                }
                None => format!("{ANSI_BOLD_CYAN}fsqlite{ANSI_RESET}> "),
            }
        } else {
            match label {
                Some(label) => format!("fsqlite[{label}]> "),
                None => String::from(PROMPT_PRIMARY),
            }
        };
    }

    let preview = render_pending_sql_preview(pending_sql, shell_options.colorize_prompts);
    if shell_options.colorize_prompts {
        match label {
            Some(label) => {
                format!("{ANSI_DIM}...{ANSI_RESET}[{ANSI_YELLOW}{label}{ANSI_RESET}] {preview}> ")
            }
            None => format!("{ANSI_DIM}...{ANSI_RESET} {preview}> "),
        }
    } else {
        match label {
            Some(label) => format!("...[{label}] {preview}> "),
            None => format!("{PROMPT_CONTINUATION}{preview} "),
        }
    }
}

fn prompt_db_label(current_db_path: &str) -> String {
    if current_db_path == DEFAULT_DB_PATH {
        return current_db_path.to_owned();
    }

    Path::new(current_db_path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(current_db_path)
        .to_owned()
}

async fn execute_sql<W, E>(
    connection: &Connection,
    sql: &str,
    output_options: OutputOptions,
    out: &mut W,
    err: &mut E,
) -> bool
where
    W: Write,
    E: Write,
{
    let mut parser = Parser::from_sql(sql);
    let mut statement_start = 0;
    loop {
        // Connection::query returns only the final statement's rows. Execute
        // one parser-delimited statement at a time so every result is printed
        // and a later error cannot discard earlier output or side effects.
        let tail_offset = match parser.parse_next_statement_with_tail() {
            Ok(Some((_, tail_offset))) => tail_offset,
            Ok(None) => return true,
            Err(error) => {
                let error = Connection::parse_error_to_franken_error(sql, error);
                let _ = writeln!(err, "error: {error}");
                return false;
            }
        };
        let statement_sql = sql[statement_start..tail_offset].trim_start();
        let column_names = infer_result_column_names(connection, statement_sql).await;
        match connection.query(statement_sql).await {
            Ok(rows) => {
                if write_rows(&rows, column_names.as_deref(), output_options, out).is_err() {
                    let _ = writeln!(err, "error: failed writing query results");
                    return false;
                }
            }
            Err(error) => {
                let _ = writeln!(err, "error: {error}");
                return false;
            }
        }
        statement_start = tail_offset;
    }
}

fn write_rows<W>(
    rows: &[Row],
    column_names: Option<&[String]>,
    output_options: OutputOptions,
    out: &mut W,
) -> io::Result<()>
where
    W: Write,
{
    // The stock shell emits headers only when the query produces a row.
    if rows.is_empty() {
        return Ok(());
    }
    let column_count = rows
        .first()
        .map(|row| row.values().len())
        .or_else(|| column_names.map(<[String]>::len))
        .unwrap_or(0);
    let resolved_column_names = resolved_column_names(column_names, column_count);

    match output_options.mode {
        OutputMode::List => write_delimited_rows(
            rows,
            &resolved_column_names,
            output_options,
            OutputMode::List.separator(),
            out,
        ),
        OutputMode::Csv => write_delimited_rows(
            rows,
            &resolved_column_names,
            output_options,
            OutputMode::Csv.separator(),
            out,
        ),
        OutputMode::Tabs => write_delimited_rows(
            rows,
            &resolved_column_names,
            output_options,
            OutputMode::Tabs.separator(),
            out,
        ),
        OutputMode::Column => {
            write_column_rows(rows, &resolved_column_names, output_options.headers, out)
        }
        OutputMode::Line => write_line_rows(rows, &resolved_column_names, out),
        OutputMode::Quote => write_delimited_rows(
            rows,
            &resolved_column_names,
            output_options,
            OutputMode::Quote.separator(),
            out,
        ),
    }
}

#[cfg(test)]
fn format_row(row: &Row) -> String {
    let bytes = row
        .values()
        .iter()
        .map(render_display_value)
        .collect::<Vec<_>>()
        .join(OutputMode::List.separator().as_bytes());
    String::from_utf8(bytes).expect("format_row fixtures contain UTF-8")
}

fn resolved_column_names(column_names: Option<&[String]>, column_count: usize) -> Vec<String> {
    let mut resolved: Vec<String> = column_names
        .map(|names| names.iter().take(column_count).cloned().collect())
        .unwrap_or_default();
    while resolved.len() < column_count {
        resolved.push(format!("column{}", resolved.len() + 1));
    }
    resolved
}

fn write_delimited_rows<W>(
    rows: &[Row],
    column_names: &[String],
    output_options: OutputOptions,
    separator: &str,
    out: &mut W,
) -> io::Result<()>
where
    W: Write,
{
    // bd-dx29q: `.mode csv` terminates every record with CRLF (RFC 4180 /
    // sqlite3), so scripts diffing against sqlite3 CSV output parse correctly.
    // `list` and `tabs` keep the bare LF terminator sqlite3 uses for them.
    let line_ending = if output_options.mode == OutputMode::Csv {
        "\r\n"
    } else {
        "\n"
    };

    if output_options.headers && !column_names.is_empty() {
        for (index, name) in column_names.iter().enumerate() {
            if index > 0 {
                out.write_all(separator.as_bytes())?;
            }
            out.write_all(&render_output_header(name, output_options.mode))?;
        }
        out.write_all(line_ending.as_bytes())?;
    }

    for row in rows {
        for (index, value) in row.values().iter().enumerate() {
            if index > 0 {
                out.write_all(separator.as_bytes())?;
            }
            out.write_all(&render_output_value(value, output_options.mode))?;
        }
        out.write_all(line_ending.as_bytes())?;
    }
    Ok(())
}

fn write_column_rows<W>(
    rows: &[Row],
    column_names: &[String],
    show_headers: bool,
    out: &mut W,
) -> io::Result<()>
where
    W: Write,
{
    let column_count = rows
        .first()
        .map(|row| row.values().len())
        .unwrap_or(column_names.len());
    let mut widths = vec![0usize; column_count];

    for (index, name) in column_names.iter().take(column_count).enumerate() {
        widths[index] = widths[index].max(name.chars().count());
    }

    let rendered_rows = rows
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(render_display_value)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    for row in &rendered_rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(display_character_count(value));
        }
    }

    if show_headers && !column_names.is_empty() {
        let names: Vec<_> = column_names
            .iter()
            .map(|name| Cow::Borrowed(name.as_bytes()))
            .collect();
        write_column_line(&names, &widths, out)?;
        let underline = widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join(OutputMode::Column.separator());
        writeln!(out, "{underline}")?;
    }

    for row in rendered_rows {
        write_column_line(&row, &widths, out)?;
    }

    Ok(())
}

/// sqlite3 `.mode line` right-aligns column names in a field at least this
/// wide (shell.c `MODE_Line`: `int w = 5;`).
const LINE_MODE_MIN_NAME_WIDTH: usize = 5;

fn write_line_rows<W>(rows: &[Row], column_names: &[String], out: &mut W) -> io::Result<()>
where
    W: Write,
{
    // sqlite3 pads every name to the longest one (minimum 5) so the `=` signs
    // line up: `   id = 1` / `name = x`.
    let name_width = column_names
        .iter()
        .map(|name| name.chars().count())
        .max()
        .unwrap_or(0)
        .max(LINE_MODE_MIN_NAME_WIDTH);
    for (row_index, row) in rows.iter().enumerate() {
        for (column_index, value) in row.values().iter().enumerate() {
            let name = column_names
                .get(column_index)
                .map(String::as_str)
                .unwrap_or("column");
            write!(out, "{name:>name_width$} = ")?;
            out.write_all(&render_display_value(value))?;
            writeln!(out)?;
        }
        if row_index + 1 < rows.len() {
            writeln!(out)?;
        }
    }
    Ok(())
}

fn display_character_count(value: &[u8]) -> usize {
    // Lossy decoding is used only for alignment; output retains the original
    // bytes, including invalid UTF-8.
    String::from_utf8_lossy(value).chars().count()
}

fn write_column_line<W: Write>(
    values: &[Cow<'_, [u8]>],
    widths: &[usize],
    out: &mut W,
) -> io::Result<()> {
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.write_all(OutputMode::Column.separator().as_bytes())?;
        }
        out.write_all(value)?;
        let padding = widths[index].saturating_sub(display_character_count(value));
        write!(out, "{:padding$}", "")?;
    }
    writeln!(out)
}

fn render_output_header(name: &str, mode: OutputMode) -> Cow<'_, [u8]> {
    match mode {
        OutputMode::Csv => Cow::Owned(render_csv_field(name.as_bytes())),
        // sqlite3 `.mode quote` SQL-quotes header names too: `'x','y'`.
        OutputMode::Quote => Cow::Owned(format!("'{}'", name.replace('\'', "''")).into_bytes()),
        OutputMode::Tabs | OutputMode::List | OutputMode::Column | OutputMode::Line => {
            Cow::Borrowed(name.as_bytes())
        }
    }
}

fn render_output_value(value: &SqliteValue, mode: OutputMode) -> Cow<'_, [u8]> {
    match mode {
        OutputMode::List | OutputMode::Column | OutputMode::Line | OutputMode::Tabs => {
            render_display_value(value)
        }
        // sqlite3 `.mode quote` lowercases blob hex (`X'0aff'`) unlike the other
        // SQL-literal display modes; oracle-diff tooling reads it byte-exact.
        OutputMode::Quote => Cow::Owned(render_quote_value(value).into_bytes()),
        OutputMode::Csv if matches!(value, SqliteValue::Null) => Cow::Borrowed(b""),
        OutputMode::Csv => Cow::Owned(render_csv_field(&render_display_value(value))),
    }
}

/// `list` / `column` / `line` value rendering, matching sqlite3's display
/// modes (bd-zy4es): they print `sqlite3_column_text()` verbatim — bare text
/// (no SQL quoting), an empty string for NULL (the default `.nullvalue`), a
/// blob's raw bytes, and the engine's REAL-to-TEXT form for numbers.
fn render_display_value(value: &SqliteValue) -> Cow<'_, [u8]> {
    let bytes = match value {
        SqliteValue::Null => return Cow::Borrowed(b""),
        SqliteValue::Text(text) => text.as_bytes(),
        SqliteValue::Blob(bytes) => bytes.as_ref(),
        _ => return Cow::Owned(value.to_string().into_bytes()),
    };
    // The stock shell writes these values as C strings. Preserve raw bytes
    // up to the first NUL, rather than replacing invalid UTF-8 sequences.
    let len = bytes
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(bytes.len());
    Cow::Borrowed(&bytes[..len])
}

/// `sqlite3 .mode quote` renders REAL columns with `sqlite3_snprintf("%!.20g")`
/// (shell.c, `MODE_Quote`): the full 18-19 significant digits of the exact
/// double, decimal point kept, so an INSERT built from the output reproduces
/// the stored value bit for bit.
const QUOTE_MODE_REAL_PRECISION: usize = 20;

/// `.mode quote` value rendering: sqlite3's SQL-literal output (shell.c
/// `MODE_Quote`). NULL is bare, text is `'..'` with `''` escaping, integers are
/// bare, blob hex is lowercase (`X'0aff'`), and reals carry sqlite3's `%!.20g`
/// expansion (`0.1` -> `0.1000000000000000056`, `1e300` ->
/// `1.000000000000000052e+300`) instead of the 17-digit REAL-to-TEXT form, so
/// oracle byte-diff tooling compares cleanly (bd-7p5z3).
fn render_quote_value(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => String::from("NULL"),
        SqliteValue::Text(text) => format!("'{}'", text.replace('\'', "''")),
        SqliteValue::Blob(bytes) => {
            let mut rendered = String::from("X'");
            for byte in bytes.iter() {
                let _ = write!(rendered, "{byte:02x}");
            }
            rendered.push('\'');
            rendered
        }
        SqliteValue::Float(real) => format_sqlite_float_g(*real, QUOTE_MODE_REAL_PRECISION),
        SqliteValue::Integer(_) => value.to_string(),
    }
}

fn render_csv_field(value: &[u8]) -> Vec<u8> {
    if value.is_empty()
        || value
            .iter()
            .any(|&byte| byte <= b' ' || byte >= 0x7f || matches!(byte, b',' | b'"' | b'\''))
    {
        let mut quoted = Vec::new();
        quoted.push(b'"');
        for &byte in value {
            quoted.push(byte);
            if byte == b'"' {
                quoted.push(b'"');
            }
        }
        quoted.push(b'"');
        quoted
    } else {
        value.to_vec()
    }
}

async fn infer_result_column_names(connection: &Connection, sql: &str) -> Option<Vec<String>> {
    let statement = last_sql_statement(sql)?;
    let prepared = connection.prepare(statement).await.ok()?;
    let column_names = prepared.column_names();
    (!column_names.is_empty()).then(|| column_names.to_vec())
}

fn render_pending_sql_preview(pending_sql: &str, colorize: bool) -> String {
    let preview = last_sql_statement(pending_sql).unwrap_or(pending_sql);
    let collapsed = preview.split_whitespace().collect::<Vec<_>>().join(" ");
    let preview = truncate_preview(&collapsed, 28);
    if preview.is_empty() {
        String::from("...")
    } else if colorize {
        highlight_sql(&preview)
    } else {
        preview
    }
}

fn truncate_preview(text: &str, max_chars: usize) -> String {
    let mut truncated = text.chars().take(max_chars).collect::<String>();
    if text.chars().count() > max_chars {
        truncated.push_str("...");
    }
    truncated
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

fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_identifier_continue(byte: u8) -> bool {
    is_identifier_start(byte) || byte.is_ascii_digit()
}

fn last_sql_statement(buffer: &str) -> Option<&str> {
    let bytes = buffer.as_bytes();
    let mut state = StatementScanState::Normal;
    let mut statement_start = 0usize;
    let mut last_statement = None;
    let mut i = 0usize;

    while i < bytes.len() {
        let byte = bytes[i];
        match state {
            StatementScanState::Normal => {
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
                if byte == b'\'' {
                    state = StatementScanState::SingleQuote;
                    i += 1;
                    continue;
                }
                if byte == b'"' {
                    state = StatementScanState::DoubleQuote;
                    i += 1;
                    continue;
                }
                if byte == b'`' {
                    state = StatementScanState::Backtick;
                    i += 1;
                    continue;
                }
                if byte == b'[' {
                    state = StatementScanState::BracketIdent;
                    i += 1;
                    continue;
                }
                if byte == b';' {
                    let statement = buffer[statement_start..=i].trim();
                    if sql_segment_has_tokens(statement) {
                        last_statement = Some(statement);
                    }
                    statement_start = i + 1;
                }
                i += 1;
            }
            StatementScanState::SingleQuote => {
                if byte == b'\'' {
                    if bytes.get(i + 1) == Some(&b'\'') {
                        i += 2;
                    } else {
                        state = StatementScanState::Normal;
                        i += 1;
                    }
                } else {
                    i += buffer[i..].chars().next().map_or(1, char::len_utf8);
                }
            }
            StatementScanState::DoubleQuote => {
                if byte == b'"' {
                    if bytes.get(i + 1) == Some(&b'"') {
                        i += 2;
                    } else {
                        state = StatementScanState::Normal;
                        i += 1;
                    }
                } else {
                    i += buffer[i..].chars().next().map_or(1, char::len_utf8);
                }
            }
            StatementScanState::Backtick => {
                if byte == b'`' {
                    if bytes.get(i + 1) == Some(&b'`') {
                        i += 2;
                    } else {
                        state = StatementScanState::Normal;
                        i += 1;
                    }
                } else {
                    i += buffer[i..].chars().next().map_or(1, char::len_utf8);
                }
            }
            StatementScanState::BracketIdent => {
                if byte == b']' {
                    if bytes.get(i + 1) == Some(&b']') {
                        i += 2;
                    } else {
                        state = StatementScanState::Normal;
                        i += 1;
                    }
                } else {
                    i += buffer[i..].chars().next().map_or(1, char::len_utf8);
                }
            }
            StatementScanState::LineComment => {
                if byte == b'\n' || byte == b'\r' {
                    state = StatementScanState::Normal;
                }
                i += 1;
            }
            StatementScanState::BlockComment => {
                if is_block_comment_end(bytes, i) {
                    state = StatementScanState::Normal;
                    i += 2;
                } else {
                    i += buffer[i..].chars().next().map_or(1, char::len_utf8);
                }
            }
        }
    }

    let trailing = buffer[statement_start..].trim();
    if sql_segment_has_tokens(trailing) {
        last_statement = Some(trailing);
    }

    last_statement
}

fn sql_segment_has_tokens(segment: &str) -> bool {
    let bytes = segment.as_bytes();
    let mut state = StatementScanState::Normal;
    let mut i = 0usize;

    while i < bytes.len() {
        let byte = bytes[i];
        match state {
            StatementScanState::Normal => {
                if byte.is_ascii_whitespace() {
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
                return true;
            }
            StatementScanState::SingleQuote
            | StatementScanState::DoubleQuote
            | StatementScanState::Backtick
            | StatementScanState::BracketIdent => unreachable!(),
            StatementScanState::LineComment => {
                if byte == b'\n' || byte == b'\r' {
                    state = StatementScanState::Normal;
                }
                i += 1;
            }
            StatementScanState::BlockComment => {
                if is_block_comment_end(bytes, i) {
                    state = StatementScanState::Normal;
                    i += 2;
                } else {
                    i += segment[i..].chars().next().map_or(1, char::len_utf8);
                }
            }
        }
    }

    false
}

fn highlight_sql(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut highlighted = String::with_capacity(sql.len() + 32);
    let mut i = 0usize;

    while i < bytes.len() {
        if is_line_comment_start(bytes, i) {
            let start = i;
            i += 2;
            while i < bytes.len() && !matches!(bytes[i], b'\n' | b'\r') {
                i += sql[i..].chars().next().map_or(1, char::len_utf8);
            }
            push_colored_segment(&mut highlighted, &sql[start..i], ANSI_DIM);
            continue;
        }

        if is_block_comment_start(bytes, i) {
            let start = i;
            i += 2;
            while i < bytes.len() && !is_block_comment_end(bytes, i) {
                i += sql[i..].chars().next().map_or(1, char::len_utf8);
            }
            if is_block_comment_end(bytes, i) {
                i += 2;
            }
            push_colored_segment(&mut highlighted, &sql[start..i], ANSI_DIM);
            continue;
        }

        match bytes[i] {
            b'\'' => {
                let start = i;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if bytes.get(i + 1) == Some(&b'\'') {
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += sql[i..].chars().next().map_or(1, char::len_utf8);
                    }
                }
                push_colored_segment(&mut highlighted, &sql[start..i], ANSI_GREEN);
            }
            byte if byte.is_ascii_digit() => {
                let start = i;
                i += 1;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric()
                        || matches!(bytes[i], b'.' | b'+' | b'-' | b'_'))
                {
                    i += 1;
                }
                push_colored_segment(&mut highlighted, &sql[start..i], ANSI_MAGENTA);
            }
            byte if is_identifier_start(byte) => {
                let start = i;
                i += 1;
                while i < bytes.len() && is_identifier_continue(bytes[i]) {
                    i += 1;
                }
                let word = &sql[start..i];
                if is_sql_keyword(word) {
                    push_colored_segment(&mut highlighted, word, ANSI_BOLD_BLUE);
                } else {
                    highlighted.push_str(word);
                }
            }
            _ => {
                let ch = sql[i..]
                    .chars()
                    .next()
                    .expect("slice should contain a char");
                highlighted.push(ch);
                i += ch.len_utf8();
            }
        }
    }

    highlighted
}

fn push_colored_segment(buffer: &mut String, segment: &str, color: &str) {
    buffer.push_str(color);
    buffer.push_str(segment);
    buffer.push_str(ANSI_RESET);
}

fn is_sql_keyword(token: &str) -> bool {
    matches!(
        token.to_ascii_uppercase().as_str(),
        "ALTER"
            | "ANALYZE"
            | "AND"
            | "AS"
            | "ASC"
            | "ATTACH"
            | "BEGIN"
            | "BY"
            | "CASE"
            | "CHECK"
            | "COMMIT"
            | "CREATE"
            | "DELETE"
            | "DESC"
            | "DETACH"
            | "DISTINCT"
            | "DROP"
            | "ELSE"
            | "END"
            | "EXISTS"
            | "EXPLAIN"
            | "FROM"
            | "GROUP"
            | "HAVING"
            | "IN"
            | "INDEX"
            | "INSERT"
            | "INTO"
            | "IS"
            | "JOIN"
            | "LEFT"
            | "LIMIT"
            | "NOT"
            | "NULL"
            | "ON"
            | "OR"
            | "ORDER"
            | "PRIMARY"
            | "REPLACE"
            | "RIGHT"
            | "ROLLBACK"
            | "SELECT"
            | "SET"
            | "TABLE"
            | "THEN"
            | "TRANSACTION"
            | "UNION"
            | "UNIQUE"
            | "UPDATE"
            | "VALUES"
            | "VIEW"
            | "WHEN"
            | "WHERE"
    )
}

#[allow(clippy::too_many_arguments)]
async fn try_execute_dot_command<W, E>(
    trimmed: &str,
    connection: &mut Connection,
    current_db_path: &mut String,
    output_options: &mut OutputOptions,
    out: &mut W,
    err: &mut E,
    shell_options: ShellOptions,
    had_error: &mut bool,
) -> DotCommandResult
where
    W: Write,
    E: Write,
{
    if let Some(arg) = dot_command_arg(trimmed, ".read") {
        let Some(path) = parse_optional_quoted_arg(arg) else {
            let _ = writeln!(err, "error: .read requires a file path");
            *had_error = true;
            return DotCommandResult::Continue;
        };

        match execute_script_file(
            &path,
            connection,
            current_db_path,
            output_options,
            out,
            err,
            shell_options.nested_script(),
        )
        .await
        {
            Some(outcome) => {
                *had_error |= outcome.had_error;
                if outcome.flow == ShellFlow::Exit {
                    return DotCommandResult::Exit;
                }
            }
            None => {
                *had_error = true;
            }
        }
        return DotCommandResult::Continue;
    }

    if let Some(arg) = dot_command_arg(trimmed, ".open") {
        let Some(path) = parse_optional_quoted_arg(arg) else {
            let _ = writeln!(err, "error: .open requires a database path");
            *had_error = true;
            return DotCommandResult::Continue;
        };

        match Connection::open(&path).await {
            Ok(new_connection) => {
                if let Err(error) = connection.close_in_place().await {
                    let _ = writeln!(err, "error: closing current database: {error}");
                    *had_error = true;
                    if let Err(cleanup_error) = new_connection.close().await {
                        let _ =
                            writeln!(err, "error: closing replacement database: {cleanup_error}");
                    }
                } else {
                    *connection = new_connection;
                    *current_db_path = path;
                }
            }
            Err(error) => {
                let _ = writeln!(err, "error: {error}");
                *had_error = true;
            }
        }
        return DotCommandResult::Continue;
    }

    if let Some(arg) = dot_command_arg(trimmed, ".schema") {
        let filter = parse_optional_quoted_arg(arg);
        if let Err(error) = write_schema(
            connection,
            filter.as_deref(),
            shell_options.colorize_prompts,
            out,
        )
        .await
        {
            let _ = writeln!(err, "error: {error}");
            *had_error = true;
        }
        return DotCommandResult::Continue;
    }

    if let Some(arg) = dot_command_arg(trimmed, ".dump") {
        let filter = parse_optional_quoted_arg(arg);
        if let Err(error) = write_dump(
            connection,
            filter.as_deref(),
            shell_options.colorize_prompts,
            out,
        )
        .await
        {
            let _ = writeln!(err, "error: {error}");
            *had_error = true;
        }
        return DotCommandResult::Continue;
    }

    if let Some(arg) = dot_command_arg(trimmed, ".tables") {
        let filter = parse_optional_quoted_arg(arg);
        if let Err(error) = write_tables(connection, filter.as_deref(), out).await {
            let _ = writeln!(err, "error: {error}");
            *had_error = true;
        }
        return DotCommandResult::Continue;
    }

    if let Some(arg) = dot_command_arg(trimmed, ".mode") {
        let Some(value) = parse_optional_quoted_arg(arg) else {
            let _ = writeln!(
                err,
                "error: .mode requires one of: list, column, csv, tabs, line, quote"
            );
            *had_error = true;
            return DotCommandResult::Continue;
        };
        let Some(mode) = OutputMode::parse(&value) else {
            let _ = writeln!(
                err,
                "error: unknown output mode `{value}`; expected one of: list, column, csv, tabs, line, quote"
            );
            *had_error = true;
            return DotCommandResult::Continue;
        };
        output_options.mode = mode;
        if mode == OutputMode::Column && !output_options.headers_explicit {
            output_options.headers = true;
        }
        return DotCommandResult::Continue;
    }

    if let Some(arg) =
        dot_command_arg(trimmed, ".headers").or_else(|| dot_command_arg(trimmed, ".header"))
    {
        let Some(value) = parse_optional_quoted_arg(arg) else {
            let _ = writeln!(err, "error: .header/.headers requires `on` or `off`");
            *had_error = true;
            return DotCommandResult::Continue;
        };
        let Some(headers) = parse_on_off(&value) else {
            let _ = writeln!(
                err,
                "error: .header/.headers expects `on` or `off`, got `{value}`"
            );
            *had_error = true;
            return DotCommandResult::Continue;
        };
        output_options.headers = headers;
        output_options.headers_explicit = true;
        return DotCommandResult::Continue;
    }

    if trimmed.starts_with('.') {
        let _ = writeln!(err, "error: unknown dot command `{trimmed}`");
        *had_error = true;
        DotCommandResult::Continue
    } else {
        DotCommandResult::NotHandled
    }
}

fn dot_command_arg<'a>(trimmed: &'a str, command: &str) -> Option<&'a str> {
    let rest = trimmed.strip_prefix(command)?;
    if let Some(first_char) = rest.chars().next()
        && !first_char.is_whitespace()
    {
        return None;
    }
    Some(rest.trim())
}

fn parse_optional_quoted_arg(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.len() >= 2
        && ((trimmed.starts_with('"') && trimmed.ends_with('"'))
            || (trimmed.starts_with('\'') && trimmed.ends_with('\'')))
    {
        return Some(trimmed[1..trimmed.len() - 1].to_owned());
    }

    Some(trimmed.to_owned())
}

fn parse_on_off(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "on" | "true" | "1" | "yes" => Some(true),
        "off" | "false" | "0" | "no" => Some(false),
        _ => None,
    }
}

async fn write_tables<W>(
    connection: &Connection,
    filter: Option<&str>,
    out: &mut W,
) -> Result<(), String>
where
    W: Write,
{
    let sql = "\
        SELECT name \
        FROM sqlite_schema \
        WHERE type IN ('table', 'view') \
          AND name NOT LIKE 'sqlite_%' \
        ORDER BY name";
    let filtered_sql = "\
        SELECT name \
        FROM sqlite_schema \
        WHERE type IN ('table', 'view') \
          AND name NOT LIKE 'sqlite_%' \
          AND name LIKE ?1 \
        ORDER BY name";

    let rows = match filter {
        Some(filter) => {
            connection
                .query_with_params(filtered_sql, &[SqliteValue::from(filter.to_owned())])
                .await
        }
        None => connection.query(sql).await,
    }
    .map_err(|error| error.to_string())?;

    let table_names = rows
        .iter()
        .filter_map(|row| row.get(0))
        .filter_map(SqliteValue::as_text)
        .collect::<Vec<_>>();
    if !table_names.is_empty() {
        writeln!(out, "{}", table_names.join(" ")).map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn write_schema<W>(
    connection: &Connection,
    filter: Option<&str>,
    colorize_sql: bool,
    out: &mut W,
) -> Result<(), String>
where
    W: Write,
{
    let sql = "\
        SELECT sql \
        FROM sqlite_schema \
        WHERE sql IS NOT NULL \
          AND type IN ('table', 'index', 'trigger', 'view') \
          AND name NOT LIKE 'sqlite_%' \
        ORDER BY CASE type \
            WHEN 'table' THEN 0 \
            WHEN 'index' THEN 1 \
            WHEN 'trigger' THEN 2 \
            WHEN 'view' THEN 3 \
            ELSE 4 \
        END, name";
    let filtered_sql = "\
        SELECT sql \
        FROM sqlite_schema \
        WHERE sql IS NOT NULL \
          AND type IN ('table', 'index', 'trigger', 'view') \
          AND name NOT LIKE 'sqlite_%' \
          AND (name LIKE ?1 OR tbl_name LIKE ?1) \
        ORDER BY CASE type \
            WHEN 'table' THEN 0 \
            WHEN 'index' THEN 1 \
            WHEN 'trigger' THEN 2 \
            WHEN 'view' THEN 3 \
            ELSE 4 \
        END, name";

    let rows = match filter {
        Some(filter) => {
            connection
                .query_with_params(filtered_sql, &[SqliteValue::from(filter.to_owned())])
                .await
        }
        None => connection.query(sql).await,
    }
    .map_err(|error| error.to_string())?;

    for row in rows {
        let Some(SqliteValue::Text(statement)) = row.get(0) else {
            continue;
        };
        write_sql_statement(out, statement, colorize_sql).map_err(|error| error.to_string())?;
    }

    Ok(())
}

async fn write_dump<W>(
    connection: &Connection,
    filter: Option<&str>,
    colorize_sql: bool,
    out: &mut W,
) -> Result<(), String>
where
    W: Write,
{
    let table_sql = "\
        SELECT name, sql \
        FROM sqlite_schema \
        WHERE type = 'table' \
          AND sql IS NOT NULL \
          AND name NOT LIKE 'sqlite_%' \
        ORDER BY name";
    let filtered_table_sql = "\
        SELECT name, sql \
        FROM sqlite_schema \
        WHERE type = 'table' \
          AND sql IS NOT NULL \
          AND name NOT LIKE 'sqlite_%' \
          AND name LIKE ?1 \
        ORDER BY name";
    let object_sql = "\
        SELECT sql \
        FROM sqlite_schema \
        WHERE sql IS NOT NULL \
          AND type IN ('index', 'trigger', 'view') \
          AND name NOT LIKE 'sqlite_%' \
        ORDER BY CASE type \
            WHEN 'index' THEN 0 \
            WHEN 'trigger' THEN 1 \
            WHEN 'view' THEN 2 \
            ELSE 3 \
        END, name";
    let filtered_object_sql = "\
        SELECT sql \
        FROM sqlite_schema \
        WHERE sql IS NOT NULL \
          AND type IN ('index', 'trigger', 'view') \
          AND name NOT LIKE 'sqlite_%' \
          AND (name LIKE ?1 OR tbl_name LIKE ?1) \
        ORDER BY CASE type \
            WHEN 'index' THEN 0 \
            WHEN 'trigger' THEN 1 \
            WHEN 'view' THEN 2 \
            ELSE 3 \
        END, name";

    let table_rows = match filter {
        Some(filter) => {
            connection
                .query_with_params(filtered_table_sql, &[SqliteValue::from(filter.to_owned())])
                .await
        }
        None => connection.query(table_sql).await,
    }
    .map_err(|error| error.to_string())?;

    // Match sqlite3's .dump preamble: tables are emitted in name order, not
    // FK-dependency order, so the reload must run with FK enforcement off.
    write_sql_statement(out, "PRAGMA foreign_keys=OFF;", colorize_sql)
        .map_err(|error| error.to_string())?;
    write_sql_statement(out, "BEGIN TRANSACTION;", colorize_sql)
        .map_err(|error| error.to_string())?;

    for row in &table_rows {
        let Some(SqliteValue::Text(statement)) = row.get(1) else {
            continue;
        };
        write_sql_statement(out, statement, colorize_sql).map_err(|error| error.to_string())?;
    }

    for row in &table_rows {
        let Some(SqliteValue::Text(table_name)) = row.get(0) else {
            continue;
        };
        let quoted_table = quote_identifier(table_name);
        let rows = connection
            .query(&format!("SELECT * FROM {quoted_table};"))
            .await
            .map_err(|error| error.to_string())?;
        for row in rows {
            writeln!(
                out,
                "INSERT INTO {quoted_table} VALUES({});",
                row.values()
                    .iter()
                    .map(sql_literal)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .map_err(|error| error.to_string())?;
        }
    }

    write_sqlite_sequence_dump(connection, filter, colorize_sql, out).await?;

    let object_rows = match filter {
        Some(filter) => {
            connection
                .query_with_params(filtered_object_sql, &[SqliteValue::from(filter.to_owned())])
                .await
        }
        None => connection.query(object_sql).await,
    }
    .map_err(|error| error.to_string())?;

    for row in object_rows {
        let Some(SqliteValue::Text(statement)) = row.get(0) else {
            continue;
        };
        write_sql_statement(out, statement, colorize_sql).map_err(|error| error.to_string())?;
    }

    write_sql_statement(out, "COMMIT;", colorize_sql).map_err(|error| error.to_string())?;
    Ok(())
}

async fn write_sqlite_sequence_dump<W>(
    connection: &Connection,
    filter: Option<&str>,
    colorize_sql: bool,
    out: &mut W,
) -> Result<(), String>
where
    W: Write,
{
    let exists = connection
        .query(
            "SELECT name FROM sqlite_schema \
             WHERE type = 'table' AND name = 'sqlite_sequence'",
        )
        .await
        .map_err(|error| error.to_string())?;
    if exists.is_empty() {
        return Ok(());
    }

    let sql = "SELECT name, seq FROM sqlite_sequence ORDER BY name";
    let filtered_sql = "\
        SELECT name, seq \
        FROM sqlite_sequence \
        WHERE name LIKE ?1 \
        ORDER BY name";
    let rows = match filter {
        Some(filter) => {
            connection
                .query_with_params(filtered_sql, &[SqliteValue::from(filter.to_owned())])
                .await
        }
        None => connection.query(sql).await,
    }
    .map_err(|error| error.to_string())?;
    if rows.is_empty() {
        return Ok(());
    }

    let scoped_reset = filter.is_some();
    if !scoped_reset {
        write_sql_statement(out, "DELETE FROM sqlite_sequence;", colorize_sql)
            .map_err(|error| error.to_string())?;
    }
    let quoted_table = quote_identifier("sqlite_sequence");
    for row in rows {
        if scoped_reset {
            let Some(sequence_name) = row.get(0) else {
                continue;
            };
            let delete_statement = format!(
                "DELETE FROM sqlite_sequence WHERE name = {};",
                sql_literal(sequence_name)
            );
            write_sql_statement(out, &delete_statement, colorize_sql)
                .map_err(|error| error.to_string())?;
        }
        writeln!(
            out,
            "INSERT INTO {quoted_table} VALUES({});",
            row.values()
                .iter()
                .map(sql_literal)
                .collect::<Vec<_>>()
                .join(", ")
        )
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn write_sql_statement<W>(out: &mut W, statement: &str, colorize_sql: bool) -> io::Result<()>
where
    W: Write,
{
    let trimmed = statement.trim();
    if trimmed.is_empty() {
        return Ok(());
    }
    let rendered = if colorize_sql {
        highlight_sql(trimmed)
    } else {
        trimmed.to_owned()
    };
    if trimmed.ends_with(';') {
        writeln!(out, "{rendered}")
    } else {
        writeln!(out, "{rendered};")
    }
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn sql_literal(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Text(text) => format!("'{}'", text.replace('\'', "''")),
        SqliteValue::Blob(bytes) => {
            let mut rendered = String::from("X'");
            for byte in bytes.iter() {
                let _ = write!(rendered, "{byte:02X}");
            }
            rendered.push('\'');
            rendered
        }
        // Non-finite REALs have no SQL literal form; sqlite3's .dump emits
        // 9.0e+999 / -9.0e+999 (which parse back as infinities) and NULL
        // for NaN. `Display` would render `Inf`, a syntax error on reload.
        SqliteValue::Float(f) if f.is_nan() => "NULL".to_owned(),
        SqliteValue::Float(f) if f.is_infinite() => {
            if *f < 0.0 { "-9.0e+999" } else { "9.0e+999" }.to_owned()
        }
        _ => value.to_string(),
    }
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
    // Trigger awareness (like sqlite3_complete()): a statement starting with
    // CREATE [TEMP|TEMPORARY] TRIGGER contains `;`-terminated body statements
    // and is only complete when its final `;` follows the closing END keyword.
    // `head_words` tracks the first tokens of the CURRENT statement (reset at
    // each top-level `;`), mirroring sqlite3_complete()'s per-statement state.
    let mut head_words: Vec<String> = Vec::with_capacity(3);
    let mut last_word = String::new();
    let mut in_trigger = false;

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

                if b.is_ascii_alphanumeric() || b == b'_' {
                    let start = i;
                    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
                    {
                        i += 1;
                    }
                    last_significant = Some(bytes[i - 1]);
                    let word = buffer[start..i].to_ascii_uppercase();
                    if !in_trigger && head_words.len() < 3 {
                        head_words.push(word.clone());
                        in_trigger = head_words.first().is_some_and(|w| w == "CREATE")
                            && (head_words.get(1).is_some_and(|w| w == "TRIGGER")
                                || (head_words
                                    .get(1)
                                    .is_some_and(|w| w == "TEMP" || w == "TEMPORARY")
                                    && head_words.get(2).is_some_and(|w| w == "TRIGGER")));
                    }
                    last_word = word;
                    continue;
                }

                last_significant = Some(b);

                if b == b';' {
                    if in_trigger {
                        // Only `END ;` closes the trigger body; inner
                        // `;`-terminated body statements do not.
                        if last_word == "END" {
                            in_trigger = false;
                            head_words.clear();
                        }
                    } else {
                        // Statement boundary: restart head tracking so a
                        // trailing CREATE TRIGGER in a multi-statement buffer
                        // is detected (sqlite3_complete() resets its state
                        // machine at each semicolon the same way).
                        head_words.clear();
                    }
                    last_word.clear();
                }

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

    last_significant == Some(b';') && !in_trigger
}

fn write_usage<W>(out: &mut W) -> io::Result<()>
where
    W: Write,
{
    writeln!(
        out,
        "Usage: fsqlite [DB_PATH] [-c|--command SQL] [-batch|--batch] [-init FILE]\n\
         \n\
         Piped input runs in batch mode automatically (no prompts).\n\
         `-batch` forces batch mode even on a TTY.\n\
         `-init FILE` executes a startup script before command mode or the REPL.\n\
         `-V` / `--version` prints the binary version and exits.\n\
         Dot commands in command mode are also supported: `fsqlite -c \".schema\"`.\n\
         \n\
         Verify decode proof JSON:\n\
         fsqlite --verify-proof proof.json [--verify-policy-id N] [--verify-slack N]\n\
         \n\
         Examples:\n\
         \n\
         fsqlite\n\
         fsqlite app.db\n\
         fsqlite --batch --init boot.sql app.db\n\
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
         .help         Show this help\n\
         .open FILE    Re-open the shell against another database\n\
         .tables ?PAT  List tables and views, optionally filtered by LIKE pattern\n\
         .schema ?PAT  Show schema SQL, optionally filtered by pattern\n\
         .dump ?PAT    Emit SQL text for schema + table contents\n\
         .mode MODE    Set output mode: list, column, csv, tabs, line\n\
         .headers on|off Toggle column headers for row output (`.header` alias also works)\n\
         .quit         Exit the shell\n\
         .exit         Exit the shell\n\
         .read FILE    Execute SQL from file\n\
         \n\
         Enter SQL statements terminated by `;`.\n\
         Piped stdin runs in batch mode with prompts disabled.\n",
    )
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::fs;
    use std::io::{self, BufRead, Cursor, Read};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use fsqlite_core::decode_proofs::{
        EcsDecodeProof, RejectedSymbol, SymbolDigest, SymbolRejectionReason,
    };
    use fsqlite_types::ObjectId;
    use serde_json::json;

    use super::{
        ANSI_BOLD_BLUE, ANSI_DIM, ANSI_GREEN, ANSI_MAGENTA, ANSI_RESET, OutputMode, OutputOptions,
        ShellOptions, format_row, highlight_sql, parse_args, render_display_value, render_prompt,
        run, run_with_shell_options, statement_complete, write_delimited_rows,
    };

    fn parse_from(args: &[&str]) -> Result<super::CliOptions, String> {
        let os_args: Vec<OsString> = args.iter().map(OsString::from).collect();
        parse_args(os_args)
    }

    fn unique_temp_path(prefix: &str, extension: &str) -> PathBuf {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after UNIX_EPOCH");
        let file_name = format!(
            "{prefix}_{}_{}.{}",
            std::process::id(),
            now.as_nanos(),
            extension
        );
        std::env::temp_dir().join(file_name)
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
        assert!(!options.show_version);
    }

    #[test]
    fn test_parse_version_flag() {
        let options = parse_from(&["fsqlite", "--version"]).expect("version args should parse");
        assert!(options.show_version);
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
    fn test_parse_batch_and_init_flags() {
        let options = parse_from(&["fsqlite", "--batch", "--init", "boot.sql", "demo.db"])
            .expect("batch and init flags should parse");
        assert_eq!(options.db_path, "demo.db");
        assert_eq!(options.init_path.as_deref(), Some("boot.sql"));
        assert!(options.force_batch);
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
    fn test_parse_verify_policy_id_requires_verify_proof() {
        let error = parse_from(&["fsqlite", "--verify-policy-id", "7"])
            .expect_err("verify-policy-id should require verify-proof mode");
        assert!(error.contains("require `--verify-proof`"));
    }

    #[test]
    fn test_parse_verify_slack_requires_verify_proof() {
        let error = parse_from(&["fsqlite", "--verify-slack=3"])
            .expect_err("verify-slack should require verify-proof mode");
        assert!(error.contains("require `--verify-proof`"));
    }

    #[test]
    fn test_parse_verify_policy_id_rejects_duplicates() {
        let error = parse_from(&[
            "fsqlite",
            "--verify-proof",
            "proof.json",
            "--verify-policy-id=7",
            "--verify-policy-id",
            "8",
        ])
        .expect_err("duplicate verify-policy-id flags should fail");
        assert_eq!(error, "`--verify-policy-id` may only be provided once");
    }

    #[test]
    fn test_parse_verify_slack_rejects_duplicates() {
        let error = parse_from(&[
            "fsqlite",
            "--verify-proof",
            "proof.json",
            "--verify-slack",
            "3",
            "--verify-slack=4",
        ])
        .expect_err("duplicate verify-slack flags should fail");
        assert_eq!(error, "`--verify-slack` may only be provided once");
    }

    #[test]
    fn test_parse_help_still_allows_verify_flags_without_verify_proof() {
        let options = parse_from(&["fsqlite", "--help", "--verify-policy-id", "7"])
            .expect("help should short-circuit option-specific validation");
        assert!(options.show_help);
        assert_eq!(options.verify_policy_id, 7);
        assert!(options.verify_proof_path.is_none());
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
    fn test_statement_complete_waits_for_trigger_end() {
        // A trigger body's inner `;` must not complete the statement.
        assert!(!statement_complete(
            "CREATE TRIGGER t AFTER INSERT ON x BEGIN\n  UPDATE y SET a = 1;\n"
        ));
        assert!(statement_complete(
            "CREATE TRIGGER t AFTER INSERT ON x BEGIN\n  UPDATE y SET a = 1;\nEND;"
        ));
        assert!(statement_complete(
            "create temp trigger t before delete on x begin select 1; end ;"
        ));
        assert!(!statement_complete(
            "CREATE TEMPORARY TRIGGER t AFTER INSERT ON x BEGIN\n  SELECT 1;\n  SELECT 2;\n"
        ));
        // `END` inside a string literal does not close the trigger.
        assert!(!statement_complete(
            "CREATE TRIGGER t AFTER INSERT ON x BEGIN INSERT INTO y VALUES('END;');\n"
        ));
        // Non-trigger statements are unaffected.
        assert!(statement_complete("BEGIN;"));
        assert!(statement_complete("CREATE TABLE endings(x);"));
        // A trailing CREATE TRIGGER in a multi-statement buffer is still
        // detected (head tracking resets at each top-level `;`).
        assert!(!statement_complete(
            "INSERT INTO t VALUES(1); CREATE TRIGGER tr AFTER INSERT ON x BEGIN SELECT 1;"
        ));
        assert!(statement_complete(
            "INSERT INTO t VALUES(1); CREATE TRIGGER tr AFTER INSERT ON x BEGIN SELECT 1; END;"
        ));
    }

    #[test]
    fn test_repl_accepts_multi_line_trigger() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(
                b"CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT);\n\
CREATE TABLE audit(item_id INTEGER, v TEXT);\n\
CREATE TRIGGER items_ai AFTER INSERT ON items BEGIN\n\
  INSERT INTO audit VALUES(NEW.id, NEW.v);\n\
END;\n\
INSERT INTO items(v) VALUES('hello');\n\
SELECT item_id, v FROM audit;\n\
.quit\n"
                    .to_vec(),
            );
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;

            let stderr = String::from_utf8_lossy(&err);
            assert_eq!(exit_code, 0, "stderr: {stderr}");
            assert!(err.is_empty(), "unexpected stderr: {stderr}");
            let stdout = String::from_utf8(out).expect("output should be utf-8");
            assert!(
                stdout.contains("hello"),
                "trigger should have fired and audit row should be selected, got: {stdout}"
            );
        });
    }

    #[test]
    fn test_dump_emits_foreign_keys_off_and_nonfinite_reals() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(
                b"CREATE TABLE r(x REAL);\n\
INSERT INTO r VALUES(9e999), (-9e999), (1.5);\n\
.dump\n\
.quit\n"
                    .to_vec(),
            );
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;

            assert_eq!(exit_code, 0);
            let stdout = String::from_utf8(out).expect("output should be utf-8");
            assert!(
                stdout.contains("PRAGMA foreign_keys=OFF;"),
                "dump must disable FK enforcement for reload, got: {stdout}"
            );
            assert!(
                stdout.contains("9.0e+999"),
                "infinite REAL must dump as a parseable literal, got: {stdout}"
            );
            assert!(
                !stdout.contains("Inf"),
                "raw Inf is not a valid SQL literal, got: {stdout}"
            );
        });
    }

    #[test]
    fn test_statement_complete_treats_double_minus_as_comment() {
        // SQLite treats `--` as a comment regardless of whitespace.
        assert!(!statement_complete("SELECT 1--2;")); // semicolon is part of the comment
        assert!(statement_complete("SELECT 1--2;\n;")); // semicolon on next line completes it
    }

    #[test]
    fn test_format_row_joins_with_pipes() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(Vec::<u8>::new());
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![
                OsString::from("fsqlite"),
                OsString::from("-c"),
                OsString::from("SELECT 1, 'x';"),
            ];
            let exit_code = run(args, &mut input, &mut out, &mut err).await;
            assert_eq!(exit_code, 0);

            let stdout = String::from_utf8(out).expect("output should be utf-8");
            // sqlite3 list mode: bare `|` separator, unquoted text (bd-zy4es).
            assert!(
                stdout.contains("1|x"),
                "expected rendered row in output, got: {stdout}",
            );
        });
    }

    #[test]
    fn test_version_flag_prints_binary_version_without_opening_database() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(Vec::<u8>::new());
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite"), OsString::from("--version")];

            let exit_code =
                run_with_shell_options(args, &mut input, &mut out, &mut err, ShellOptions::batch())
                    .await;
            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);

            let stdout = String::from_utf8(out).expect("stdout should be utf-8");
            assert_eq!(
                stdout.trim(),
                format!("fsqlite {}", env!("CARGO_PKG_VERSION"))
            );
        });
    }

    #[test]
    fn test_file_backed_database_roundtrip() {
        // bd-slgya regression guard: the workspace `fsqlite` dep is
        // `default-features = false`, so a dropped facade feature silently
        // compiles out the file-backed pager and every file-DB open fails at
        // runtime — :memory: coverage cannot catch it, and the v0.3.8 release
        // binaries shipped exactly that defect. Solo `-p fsqlite-cli` builds
        // (= the dsr release build) exercise the file path only through this
        // test.
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().expect("tempdir");
            let db_path = dir.path().join("roundtrip.db");
            let db_arg = db_path.to_string_lossy().into_owned();

            let mut input = Cursor::new(Vec::<u8>::new());
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![
                OsString::from("fsqlite"),
                OsString::from(&db_arg),
                OsString::from("-c"),
                OsString::from(
                    "CREATE TABLE t(x INTEGER); INSERT INTO t VALUES(42); SELECT x FROM t;",
                ),
            ];
            let exit_code =
                run_with_shell_options(args, &mut input, &mut out, &mut err, ShellOptions::batch())
                    .await;
            let stderr = String::from_utf8_lossy(&err).into_owned();
            assert_eq!(exit_code, 0, "file-backed open failed: {stderr}");
            assert!(err.is_empty(), "unexpected stderr: {stderr}");

            let stdout = String::from_utf8(out).expect("output should be utf-8");
            assert!(
                stdout.contains("42"),
                "expected query result, got: {stdout}"
            );
            assert!(db_path.exists(), "database file was not created on disk");
        });
    }

    #[test]
    fn test_repl_quit_command_exits_cleanly() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(b".quit\n".to_vec());
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;
            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);
        });
    }

    #[test]
    fn test_repl_executes_statement_then_quits() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(b"SELECT 7;\n.quit\n".to_vec());
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;
            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);

            let stdout = String::from_utf8(out).expect("output should be utf-8");
            assert!(stdout.contains('7'), "expected query result in output");
        });
    }

    #[test]
    fn test_batch_mode_suppresses_prompts() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(b"SELECT 7; SELECT 8;\n".to_vec());
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code =
                run_with_shell_options(args, &mut input, &mut out, &mut err, ShellOptions::batch())
                    .await;
            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);

            let stdout = String::from_utf8(out).expect("output should be utf-8");
            assert_eq!(stdout, "7\n8\n", "both results must be printed in order");
            assert!(
                !stdout.contains("fsqlite> ") && !stdout.contains("   ...> "),
                "batch mode should not render prompts, got: {stdout}",
            );
        });
    }

    #[test]
    fn test_command_mode_statement_batches_preserve_each_result() {
        asupersync::test_utils::run_test(|| async {
            let cases = [
                ("SELECT 1; SELECT 2;", "1\n2\n"),
                ("; /* empty; */ ; -- trailing;", ""),
                (
                    "SELECT 'it''s; fine'; /* ; */ SELECT '雪;'; -- trailing;",
                    "it's; fine\n雪;\n",
                ),
                (
                    "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT x FROM t; \
                     UPDATE t SET x = 2; SELECT x FROM t; PRAGMA user_version = 7; \
                     PRAGMA user_version;",
                    "1\n2\n7\n",
                ),
                (
                    "CREATE TABLE t(x); CREATE TABLE audit(x); \
                     CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
                     INSERT INTO audit VALUES(NEW.x); \
                     INSERT INTO audit VALUES('trigger; value'); END; \
                     INSERT INTO t VALUES('first'); SELECT x FROM audit ORDER BY rowid; \
                     SELECT 'last';",
                    "first\ntrigger; value\nlast\n",
                ),
                (
                    ".headers on\nSELECT 1 AS first; SELECT 2 AS second;",
                    "first\n1\nsecond\n2\n",
                ),
            ];
            for (sql, expected) in cases {
                let mut input = Cursor::new(Vec::<u8>::new());
                let mut out = Vec::new();
                let mut err = Vec::new();
                let args = ["fsqlite", "-c", sql].map(OsString::from);
                let exit_code = run(args, &mut input, &mut out, &mut err).await;
                assert_eq!(
                    exit_code,
                    0,
                    "SQL: {sql}; stderr: {}",
                    String::from_utf8_lossy(&err)
                );
                assert!(err.is_empty(), "SQL: {sql}; stderr: {err:?}");
                assert_eq!(out, expected.as_bytes(), "SQL: {sql}");
            }
        });
    }

    #[test]
    fn test_command_mode_batch_error_keeps_prior_output_and_commits() {
        asupersync::test_utils::run_test(|| async {
            for failure in ["SELEC 2", "SELECT * FROM missing_table"] {
                let dir = tempfile::tempdir().expect("tempdir");
                let db_path = dir.path().join("batch_error.db");
                let sql = format!(
                    "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT x FROM t; \
                     {failure}; INSERT INTO t VALUES(3); SELECT 99;"
                );
                let mut input = Cursor::new(Vec::<u8>::new());
                let mut out = Vec::new();
                let mut err = Vec::new();
                let args = [
                    OsString::from("fsqlite"),
                    db_path.as_os_str().to_owned(),
                    OsString::from("-c"),
                    OsString::from(&sql),
                ];
                let exit_code = run(args, &mut input, &mut out, &mut err).await;
                assert_eq!(exit_code, 1, "SQL: {sql}");
                assert_eq!(out, b"1\n", "earlier result must survive: {sql}");
                assert!(
                    String::from_utf8_lossy(&err).contains("error:"),
                    "failure must be reported: {err:?}"
                );
                let stock = rusqlite::Connection::open_with_flags(
                    &db_path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )
                .expect("stock opens the closed CLI database");
                let rows: Vec<i64> = stock
                    .prepare("SELECT x FROM t ORDER BY rowid")
                    .expect("earlier CREATE TABLE must survive")
                    .query_map([], |row| row.get(0))
                    .expect("read committed rows")
                    .collect::<rusqlite::Result<_>>()
                    .expect("collect committed rows");
                assert_eq!(rows, [1], "stop before the later INSERT: {sql}");
            }
        });
    }

    #[test]
    fn test_command_mode_batch_preserves_engine_parse_diagnostics() {
        asupersync::test_utils::run_test(|| async {
            for (failure, expected_error) in [
                ("SELEC 2;", "error: near \"SELEC\": syntax error\n"),
                (
                    "SELECT sum(x) OVER (ROWS 1 FOLLOWING) FROM (SELECT 1 AS x);",
                    "error: unsupported frame specification\n",
                ),
                (
                    "SELECT sum(x) OVER (ROWS UNBOUNDED FOLLOWING) FROM (SELECT 1 AS x);",
                    "error: near \"FOLLOWING\": syntax error\n",
                ),
            ] {
                let sql = format!("SELECT '雪'; {failure}");
                let args = ["fsqlite", "-c", &sql].map(OsString::from);
                let mut input = Cursor::new(Vec::<u8>::new());
                let mut out = Vec::new();
                let mut err = Vec::new();
                assert_eq!(run(args, &mut input, &mut out, &mut err).await, 1);
                assert_eq!(out, "雪\n".as_bytes());
                assert_eq!(err, expected_error.as_bytes(), "SQL: {sql}");
            }
        });
    }

    #[test]
    fn test_command_mode_sql_error_returns_failure_exit_code() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(Vec::<u8>::new());
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![
                OsString::from("fsqlite"),
                OsString::from("-c"),
                OsString::from("SELECT * FROM missing_table;"),
            ];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;
            assert_eq!(exit_code, 1);
            let stderr = String::from_utf8(err).expect("stderr should be utf-8");
            assert!(
                stderr.contains("missing_table") || stderr.contains("no such table"),
                "expected SQL failure in stderr, got: {stderr}",
            );
        });
    }

    #[test]
    fn test_batch_mode_read_error_returns_failure_exit_code() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(b".read /definitely/missing/path.sql\n".to_vec());
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code =
                run_with_shell_options(args, &mut input, &mut out, &mut err, ShellOptions::batch())
                    .await;
            assert_eq!(exit_code, 1);
            let stderr = String::from_utf8(err).expect("stderr should be utf-8");
            assert!(
                stderr.contains("error:"),
                "expected .read failure in stderr, got: {stderr}",
            );
        });
    }

    #[test]
    fn test_repl_read_line_interrupted_keeps_shell_running() {
        asupersync::test_utils::run_test(|| async {
            let mut input = InterruptOnceBufRead::new(b".quit\n".to_vec());
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;
            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);
        });
    }

    #[test]
    fn test_repl_read_command_executes_sql_from_file() {
        asupersync::test_utils::run_test(|| async {
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

            let exit_code = run(args, &mut input, &mut out, &mut err).await;
            let _ = fs::remove_file(&path);

            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);

            let stdout = String::from_utf8(out).expect("output should be utf-8");
            assert!(
                stdout.contains("42"),
                "expected .read query output in stdout"
            );
        });
    }

    #[test]
    fn test_repl_read_command_requires_path() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(b".read\n.quit\n".to_vec());
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;
            assert_eq!(exit_code, 0);

            let stderr = String::from_utf8(err).expect("stderr should be utf-8");
            assert!(
                stderr.contains(".read requires a file path"),
                "expected .read path error in stderr",
            );
        });
    }

    #[test]
    fn test_init_file_executes_before_command_mode() {
        asupersync::test_utils::run_test(|| async {
            let path = unique_temp_path("fsqlite_cli_init", "sql");
            fs::write(
                &path,
                "CREATE TABLE seeded(id INTEGER PRIMARY KEY);\nINSERT INTO seeded VALUES(1);\n",
            )
            .expect("startup SQL file should be writable");

            let mut input = Cursor::new(Vec::<u8>::new());
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![
                OsString::from("fsqlite"),
                OsString::from("--init"),
                path.as_os_str().to_os_string(),
                OsString::from("-c"),
                OsString::from("SELECT COUNT(*) AS n FROM seeded;"),
            ];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;
            let _ = fs::remove_file(&path);

            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);
            let stdout = String::from_utf8(out).expect("stdout should be utf-8");
            assert!(
                stdout.contains('1'),
                "expected startup script side effects in command mode, got: {stdout}",
            );
        });
    }

    #[test]
    fn test_repl_open_command_switches_database() {
        asupersync::test_utils::run_test(|| async {
            let path = unique_temp_path("fsqlite_cli_open", "db");
            let input_script = format!(
                "CREATE TABLE before_open(id INTEGER);\n.open {}\nSELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'before_open';\n.quit\n",
                path.display()
            );
            let mut input = Cursor::new(input_script.into_bytes());
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;
            let _ = fs::remove_file(&path);

            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);

            let stdout = String::from_utf8(out).expect("output should be utf-8");
            assert!(
                stdout.contains('0'),
                "expected .open to switch to a fresh database, got: {stdout}",
            );
        });
    }

    #[test]
    fn test_tables_command_lists_tables_and_views() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(
                b"CREATE TABLE widgets(id INTEGER PRIMARY KEY);\n\
CREATE VIEW widget_names AS SELECT id FROM widgets;\n\
.tables\n\
.quit\n"
                    .to_vec(),
            );
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;

            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);
            let stdout = String::from_utf8(out).expect("stdout should be utf-8");
            assert!(
                stdout.contains("widget_names") && stdout.contains("widgets"),
                "expected .tables output to include tables and views, got: {stdout}",
            );
        });
    }

    #[test]
    fn test_mode_and_header_commands_affect_query_rendering() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(
                b".mode column\n\
.header on\n\
SELECT 1 AS one, 'x' AS two;\n\
.quit\n"
                    .to_vec(),
            );
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;

            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);
            let stdout = String::from_utf8(out).expect("stdout should be utf-8");
            assert!(
                stdout.contains("one") && stdout.contains("two"),
                "expected headers in column mode output, got: {stdout}",
            );
            // sqlite3 column mode prints text bare (`x`, not `'x'`), each
            // cell padded to its header width: `1    x  ` (bd-zy4es).
            assert!(
                stdout.contains("1    x"),
                "expected row data in column mode output, got: {stdout}",
            );
            assert!(
                !stdout.contains("'x'"),
                "column mode must not SQL-quote text, got: {stdout}",
            );
        });
    }

    #[test]
    fn test_mode_csv_uses_raw_text_and_header_row() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(
                b".mode csv\n\
.header on\n\
SELECT 1 AS one, 'two,three' AS two;\n\
.quit\n"
                    .to_vec(),
            );
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;

            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);
            let stdout = String::from_utf8(out).expect("stdout should be utf-8");
            assert!(
                stdout.contains("one,two"),
                "expected CSV header row, got: {stdout}",
            );
            assert!(
                stdout.contains("1,\"two,three\""),
                "expected CSV value escaping without SQL quotes, got: {stdout}",
            );
        });
    }

    async fn assert_display_script_matches_stock(script: &str, expected: &[u8]) {
        let mut input = Cursor::new(script.as_bytes());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let exit = run_with_shell_options(
            vec![OsString::from("fsqlite")],
            &mut input,
            &mut out,
            &mut err,
            ShellOptions::batch(),
        )
        .await;
        assert_eq!(exit, 0, "stderr: {}", String::from_utf8_lossy(&err));
        assert!(err.is_empty(), "unexpected stderr: {err:?}");
        assert_eq!(out, expected, "script: {script}");
        if let Some((_, _, version)) = system_sqlite3_version() {
            assert_eq!(system_sqlite3_stdout(script.as_bytes()), expected);
            eprintln!("event=display_bytes_stock_verified sqlite={version}");
        }
    }

    #[test]
    fn test_csv_empty_control_and_unicode_fields_lxtng() {
        asupersync::test_utils::run_test(|| async {
            assert_display_script_matches_stock(
                ".mode csv\n.headers on\nSELECT NULL AS n, '' AS e, 'a b' AS space, \
                 'it''s' AS apostrophe, char(9) AS tab, char(127) AS del, \
                 'é' AS utf8, x'' AS empty_blob, x'ff410042' AS raw, \
                 char(65,0,66) AS nul, 'a\"b' AS quote, 'a,b' AS comma;\n",
                b"n,e,space,apostrophe,tab,del,utf8,empty_blob,raw,nul,quote,comma\r\n\
                  ,\"\",\"a b\",\"it's\",\"\t\",\"\x7f\",\"\xc3\xa9\",\"\",\"\xffA\",A,\"a\"\"b\",\"a,b\"\r\n",
            )
            .await;
            if system_sqlite3_version().is_some() {
                let mut script = String::from(".mode csv\n");
                for byte in 0_u16..=255 {
                    use std::fmt::Write as _;
                    writeln!(script, "SELECT char({byte}), x'{byte:02x}';").expect("write script");
                }
                let expected = system_sqlite3_stdout(script.as_bytes());
                assert_display_script_matches_stock(&script, &expected).await;
                eprintln!("event=csv_byte_corpus_verified values=256 columns=2");
            }
        });
    }

    #[test]
    fn test_column_implicit_headers_and_explicit_override_lxtng() {
        asupersync::test_utils::run_test(|| async {
            assert_display_script_matches_stock(
                ".mode column\nSELECT 1 AS a;\n.mode list\nSELECT 2 AS b;\n\
                 .mode column\nSELECT 3 AS c;\n.headers off\n.mode list\n\
                 .mode column\nSELECT 4 AS d;\n.header on\nSELECT 5 AS e;\n",
                b"a\n-\n1\nb\n2\nc\n-\n3\n4\ne\n-\n5\n",
            )
            .await;
            for mode in ["list", "column", "line", "csv", "tabs", "quote"] {
                assert_display_script_matches_stock(
                    &format!(".headers on\n.mode {mode}\nSELECT 1 AS a WHERE 0;\n"),
                    b"",
                )
                .await;
            }
            assert_display_script_matches_stock(
                ".headers off\n.mode column\nSELECT 1 AS a;\n",
                b"1\n",
            )
            .await;
        });
    }

    #[test]
    fn test_display_modes_preserve_blob_bytes_lxtng() {
        asupersync::test_utils::run_test(|| async {
            for (mode, expected) in [
                ("list", b"\xffA||\n".as_slice()),
                ("tabs", b"\xffA\t\t\n".as_slice()),
                ("line", b"    a = \xffA\n    b = \n    n = \n".as_slice()),
                ("column", b"\xffA      \n".as_slice()),
                ("csv", b"\"\xffA\",\"\",\r\n".as_slice()),
            ] {
                assert_display_script_matches_stock(
                    &format!(
                        ".headers off\n.mode {mode}\n\
                         SELECT x'ff410042' AS a, x'00ff' AS b, NULL AS n;\n"
                    ),
                    expected,
                )
                .await;
            }
        });
    }

    #[test]
    fn test_display_modes_propagate_full_output_buffer_lxtng() {
        asupersync::test_utils::run_test(|| async {
            let conn = fsqlite::Connection::open(":memory:").await.expect("open");
            let rows = conn.query("SELECT x'ff', NULL, ''; ").await.expect("rows");
            for mode in [
                OutputMode::List,
                OutputMode::Tabs,
                OutputMode::Csv,
                OutputMode::Column,
                OutputMode::Line,
                OutputMode::Quote,
            ] {
                let mut full_buffer = &mut [][..];
                let error = super::write_rows(
                    &rows,
                    None,
                    OutputOptions {
                        mode,
                        ..OutputOptions::default()
                    },
                    &mut full_buffer,
                )
                .expect_err("a full output buffer must report write failure");
                assert_eq!(error.kind(), io::ErrorKind::WriteZero);
            }
            conn.close().await.expect("close");
        });
    }

    #[test]
    fn test_shell_shutdown_checkpoints_commits_and_rolls_back_open_writes_uo4uk() {
        asupersync::test_utils::run_test(|| async {
            for (case, ending, expected_exit) in [
                ("eof", "", 0),
                ("quit", ".quit\n", 0),
                ("command", "", 0),
                ("init_exit", ".quit\n", 0),
                ("init_error", "SELECT missing_shutdown_column;\n", 1),
                ("script_error", "SELECT missing_shutdown_column;\n", 1),
                ("switch", ".open :memory:\nSELECT 77;\n", 0),
                ("failed_switch", "", 0),
            ] {
                let dir = tempfile::tempdir().expect("tempdir");
                let path = dir.path().join("shell.db");
                let ending = if case == "failed_switch" {
                    format!(
                        ".open '{}'\nSELECT id FROM shutdown_rows ORDER BY id;\n",
                        dir.path().join("missing-parent/other.db").display()
                    )
                } else {
                    ending.to_owned()
                };
                let script = format!(
                    "PRAGMA journal_mode=WAL;\nPRAGMA wal_autocheckpoint=0;\n\
                     CREATE TABLE shutdown_rows(id INTEGER PRIMARY KEY, value TEXT);\n\
                     INSERT INTO shutdown_rows VALUES(1, 'committed');\n\
                     BEGIN;\nINSERT INTO shutdown_rows VALUES(2, 'must roll back');\n{ending}"
                );
                let mut args = vec![OsString::from("fsqlite"), path.clone().into_os_string()];
                let input_bytes = if case.starts_with("init_") {
                    let init = dir.path().join("init.sql");
                    fs::write(&init, &script).expect("write init script");
                    args.extend([OsString::from("--init"), init.into_os_string()]);
                    // An init exit or error must bypass this failing command.
                    args.extend([
                        OsString::from("-c"),
                        OsString::from("SELECT missing_after_init;"),
                    ]);
                    Vec::new()
                } else if case == "command" {
                    args.extend([OsString::from("-c"), OsString::from(script)]);
                    Vec::new()
                } else {
                    script.into_bytes()
                };
                let mut input = Cursor::new(input_bytes);
                let mut out = Vec::new();
                let mut err = Vec::new();
                let mut shell_options = ShellOptions::batch();
                shell_options.fail_on_error = case != "failed_switch";
                let exit =
                    run_with_shell_options(args, &mut input, &mut out, &mut err, shell_options)
                        .await;
                assert_eq!(exit, expected_exit, "case={case}, stderr={err:?}");
                if case == "failed_switch" {
                    assert!(!err.is_empty(), "failed open must report an error");
                    assert!(out.ends_with(b"1\n2\n"), "old transaction lost: {out:?}");
                } else if expected_exit == 0 {
                    assert!(err.is_empty(), "case={case}, stderr={err:?}");
                } else {
                    assert!(String::from_utf8_lossy(&err).contains("missing_shutdown_column"));
                    assert!(!String::from_utf8_lossy(&err).contains("missing_after_init"));
                }
                // Copy only the main file: a successful oracle read proves
                // the close checkpoint, not recovery by consulting the WAL.
                let checkpoint_image = dir.path().join("checkpoint-image.db");
                fs::copy(&path, &checkpoint_image).expect("copy main database only");
                for oracle_path in [&checkpoint_image, &path] {
                    let oracle = rusqlite::Connection::open(oracle_path).expect("stock reopen");
                    let rows: Vec<(i64, String)> = oracle
                        .prepare("SELECT id, value FROM shutdown_rows ORDER BY id")
                        .unwrap_or_else(|error| {
                            panic!("case={case}, path={oracle_path:?}: {error}")
                        })
                        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                        .expect("stock query")
                        .collect::<Result<_, _>>()
                        .expect("stock rows");
                    assert_eq!(rows, [(1, "committed".to_owned())], "case={case}");
                    let integrity: String = oracle
                        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
                        .expect("stock integrity");
                    assert_eq!(integrity, "ok", "case={case}");
                }
                eprintln!(
                    "event=shell_shutdown_verified case={case} rows=1 checkpoint=true rollback=true"
                );
            }
        });
    }

    #[test]
    fn test_mode_quote_renders_sql_literals_with_comma_separator() {
        // `.mode quote` matches sqlite3: SQL-quoted header names, text `'..'`
        // (with `''` escaping), NULL bare, blob `X'..'`, numbers bare, comma sep.
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(
                b".mode quote\n\
.headers on\n\
SELECT 1 AS one, 'a''b' AS two, NULL AS three, x'01' AS four;\n\
.quit\n"
                    .to_vec(),
            );
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;

            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);
            let stdout = String::from_utf8(out).expect("stdout should be utf-8");
            assert!(
                stdout.contains("'one','two','three','four'"),
                "expected SQL-quoted header row, got: {stdout}",
            );
            assert!(
                stdout.contains("1,'a''b',NULL,X'01'"),
                "expected quote-mode SQL-literal value row, got: {stdout}",
            );
        });
    }

    #[test]
    fn test_mode_quote_blob_hex_is_lowercase() {
        // bd-7p5z3(b): sqlite3 `.mode quote` emits blob hex in lowercase
        // (`X'0aff'`) so oracle byte-diff tooling compares cleanly; the other
        // SQL-literal display modes stay uppercase.
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(
                b".mode quote\n\
SELECT x'0aff' AS b;\n\
.quit\n"
                    .to_vec(),
            );
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;

            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);
            let stdout = String::from_utf8(out).expect("stdout should be utf-8");
            assert!(
                stdout.contains("X'0aff'"),
                "expected lowercase blob hex in quote mode, got: {stdout}",
            );
            assert!(
                !stdout.contains("X'0AFF'"),
                "quote mode must not emit uppercase blob hex, got: {stdout}",
            );
        });
    }

    /// bd-7p5z3(b): SQL expressions the `.mode quote` REAL keepers render, one
    /// `SELECT` (one output line) each. The flag marks values whose `%!.20g`
    /// text is identical across SQLite's float-to-text rewrites (3.45 Dekker
    /// double-double, 3.51 widened landing zone, 3.53 exact `Fp2Convert10`):
    /// short exact binary fractions, integer-valued reals, exponent-form
    /// powers of ten, infinities. Everything else is byte-exact only against
    /// a 3.53+ shell.
    const QUOTE_MODE_REAL_CORPUS: &[(&str, bool)] = &[
        ("0.1", false),
        ("-0.1", false),
        ("3.14159", false),
        ("0.1 + 0.2", false),
        ("2.0 / 3.0", false),
        ("0.9999999999999999", false),
        ("1.0", true),
        ("100.0", true),
        ("-2.5", true),
        ("0.5", true),
        ("1.25", true),
        ("0.0", true),
        ("-0.0", true),
        ("1.5e10", true),
        ("123456789012345678.0", true),
        ("1e15", true),
        ("1e19", true),
        ("1e20", true),
        ("1e21", true),
        ("0.0001", false),
        ("0.00001", false),
        ("1e-7", false),
        ("1e300", false),
        ("-1e-300", false),
        ("5e-324", false),
        ("2.2250738585072014e-308", false),
        ("1.7976931348623157e308", false),
        ("49.47", false),
        ("1e999", true),
        ("-1e999", true),
        ("1.5, 'x', NULL, x'0aff', 7", true),
    ];

    fn quote_mode_script() -> Vec<u8> {
        let mut script = String::from(".mode quote\n");
        for (expr, _) in QUOTE_MODE_REAL_CORPUS {
            script.push_str("SELECT ");
            script.push_str(expr);
            script.push_str(";\n");
        }
        script.into_bytes()
    }

    /// Runs the quote-mode corpus through the fsqlite shell (batch mode, so
    /// stdout carries only result rows) and returns one line per `SELECT`.
    async fn fsqlite_quote_mode_lines() -> Vec<String> {
        let mut input = Cursor::new(quote_mode_script());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = vec![OsString::from("fsqlite")];
        let exit_code =
            run_with_shell_options(args, &mut input, &mut out, &mut err, ShellOptions::batch())
                .await;
        assert_eq!(exit_code, 0, "stderr: {}", String::from_utf8_lossy(&err));
        assert!(err.is_empty(), "unexpected stderr: {:?}", err);
        let stdout = String::from_utf8(out).expect("stdout should be utf-8");
        let lines: Vec<String> = stdout.lines().map(str::to_owned).collect();
        assert_eq!(
            lines.len(),
            QUOTE_MODE_REAL_CORPUS.len(),
            "one quote-mode line per SELECT, got: {stdout}"
        );
        lines
    }

    /// bd-7p5z3(b): `.mode quote` REAL rendering is sqlite3's `%!.20g`
    /// (shell.c `MODE_Quote`), differential against the bundled stock library
    /// (rusqlite, the workspace conformance oracle) evaluating the same
    /// `printf('%!.20g', <expr>)` the shell calls — byte for byte, every entry.
    #[test]
    fn test_mode_quote_reals_match_stock_printf_20g_bd_7p5z3() {
        asupersync::test_utils::run_test(|| async {
            let lines = fsqlite_quote_mode_lines().await;

            let oracle = rusqlite::Connection::open_in_memory().expect("stock in-memory oracle");
            for ((expr, _), line) in QUOTE_MODE_REAL_CORPUS.iter().zip(&lines) {
                // The mixed row exercises the non-REAL branches; stock renders
                // it as one quote-mode line too, so compare the shell shape.
                let expected: String = if expr.contains(',') {
                    "1.5,'x',NULL,X'0aff',7".to_owned()
                } else {
                    oracle
                        .query_row(&format!("SELECT printf('%!.20g', {expr})"), [], |row| {
                            row.get(0)
                        })
                        .expect("stock printf('%!.20g')")
                };
                assert_eq!(line, &expected, "`.mode quote` rendering of SELECT {expr}");
            }

            // Discriminator: the pre-fix renderer reused the 17-digit
            // REAL-to-TEXT form (`0.1`); stock's quote mode spells out the
            // stored double (`0.1000000000000000056`) so the text round-trips.
            assert_ne!(
                lines[0], "0.1",
                "quote mode must not reuse the 17-digit display form"
            );
            assert!(
                lines[0].starts_with("0.100000000000000005"),
                "expected stock's full-precision 0.1, got {}",
                lines[0]
            );
        });
    }

    /// bd-7p5z3(b): the same corpus through the system `sqlite3` shell itself.
    /// Byte-exact for every entry when that shell is 3.53+ (the exact
    /// `Fp2Convert10` decode fsqlite ports); older shells differ in the last
    /// digits of long expansions, so only the version-stable entries are
    /// compared there and the skipped ones are named on stderr. Skips with a
    /// message when no `sqlite3` binary is on PATH.
    /// The system `sqlite3` shell's `(major, minor, full --version line)`, or
    /// `None` (with a SKIP note on stderr) when there is no usable binary on
    /// PATH — the stock-shell differentials below then do not run.
    fn system_sqlite3_version() -> Option<(u32, u32, String)> {
        let Ok(version_output) = std::process::Command::new("sqlite3")
            .arg("--version")
            .output()
        else {
            eprintln!("SKIP: no `sqlite3` binary on PATH; stock shell differential not run");
            return None;
        };
        if !version_output.status.success() {
            eprintln!("SKIP: `sqlite3 --version` failed; stock shell differential not run");
            return None;
        }
        let version_text = String::from_utf8_lossy(&version_output.stdout)
            .trim()
            .to_owned();
        let mut version_parts = version_text
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .split('.')
            .map(|part| part.parse::<u32>().unwrap_or(0));
        let major = version_parts.next().unwrap_or(0);
        let minor = version_parts.next().unwrap_or(0);
        Some((major, minor, version_text))
    }

    /// Pipes `script` through the system `sqlite3` shell on a fresh in-memory
    /// database and returns its stdout.
    fn system_sqlite3_stdout(script: &[u8]) -> Vec<u8> {
        let mut child = std::process::Command::new("sqlite3")
            .arg(":memory:")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn sqlite3");
        {
            use std::io::Write as _;
            let mut stdin = child.stdin.take().expect("sqlite3 stdin");
            stdin.write_all(script).expect("write script to sqlite3");
        }
        let stock = child.wait_with_output().expect("sqlite3 output");
        assert!(
            stock.status.success(),
            "sqlite3 failed: {}",
            String::from_utf8_lossy(&stock.stderr)
        );
        stock.stdout
    }

    #[test]
    fn test_mode_quote_matches_system_sqlite3_shell_bd_7p5z3() {
        let Some((major, minor, version_text)) = system_sqlite3_version() else {
            return;
        };
        let exact_float_decode = (major, minor) >= (3, 53);

        let stock_stdout = system_sqlite3_stdout(&quote_mode_script());
        let stock_lines: Vec<String> = String::from_utf8_lossy(&stock_stdout)
            .lines()
            .map(str::to_owned)
            .collect();
        assert_eq!(
            stock_lines.len(),
            QUOTE_MODE_REAL_CORPUS.len(),
            "sqlite3 {version_text} printed an unexpected number of quote-mode lines"
        );

        asupersync::test_utils::run_test(|| async {
            let fsqlite_lines = fsqlite_quote_mode_lines().await;
            for (((expr, stable), stock_line), fsqlite_line) in QUOTE_MODE_REAL_CORPUS
                .iter()
                .zip(&stock_lines)
                .zip(&fsqlite_lines)
            {
                if exact_float_decode || *stable {
                    assert_eq!(
                        fsqlite_line, stock_line,
                        "`.mode quote` SELECT {expr}: fsqlite vs sqlite3 {version_text}"
                    );
                } else {
                    eprintln!(
                        "NOTE: sqlite3 {version_text} predates the 3.53 float decode; \
                         SELECT {expr} is compared against the bundled oracle only \
                         (shell printed {stock_line}, fsqlite {fsqlite_line})"
                    );
                }
            }
        });
    }

    /// bd-zy4es: one script exercising every display mode the shell shares with
    /// sqlite3 — `list` (bare `|`, unquoted text, empty NULL, raw blob bytes),
    /// `column`, `line`, `tabs`, `csv`, with and without headers. The reals are
    /// short exact binary fractions so their text is identical on every SQLite
    /// version. The CSV quoting and raw-byte boundary corpus is covered by
    /// the separate bd-lxtng guards. `tabs` runs before `csv` because the stock
    /// shell keeps CSV's CRLF row separator for later modes.
    const DISPLAY_MODE_SCRIPT: &str = concat!(
        "CREATE TABLE t(id INTEGER, name TEXT, note TEXT, b BLOB, r REAL, n);\n",
        "INSERT INTO t VALUES(1, 'plain', 'it''s \"quoted\"', x'4142', 1.5, NULL);\n",
        "INSERT INTO t VALUES(2, 'has|pipe', 'tab\there', x'0a41', -2.5, NULL);\n",
        "INSERT INTO t VALUES(3, 'z', 'a,b', x'43', 100.0, 7);\n",
        ".mode list\n",
        "SELECT id, name, note, b, r, n FROM t ORDER BY id;\n",
        "SELECT NULL, '', 'NULL', x'';\n",
        ".headers on\n",
        "SELECT id, name FROM t WHERE id = 1;\n",
        ".mode column\n",
        "SELECT id, name, r, n FROM t ORDER BY id;\n",
        ".mode line\n",
        "SELECT id, name AS a_longer_name, r, n FROM t WHERE id <= 2 ORDER BY id;\n",
        ".mode tabs\n",
        "SELECT id, name, note, r, n FROM t ORDER BY id;\n",
        ".mode csv\n",
        "SELECT id, name, b, r, n FROM t ORDER BY id;\n",
    );

    /// Byte-exact stdout of `sqlite3 :memory: < DISPLAY_MODE_SCRIPT` — identical
    /// on sqlite3 3.46.1 (Linux) and 3.51.0 (macOS). Note the blob `x'0a41'`
    /// printed as its raw bytes (a line break then `A`), the empty NULL cells,
    /// the right-aligned `line` names and the padded `column` cells.
    const DISPLAY_MODE_STOCK_OUTPUT: &str = concat!(
        "1|plain|it's \"quoted\"|AB|1.5|\n",
        "2|has|pipe|tab\there|\n",
        "A|-2.5|\n",
        "3|z|a,b|C|100.0|7\n",
        "||NULL|\n",
        "id|name\n",
        "1|plain\n",
        "id  name      r      n\n",
        "--  --------  -----  -\n",
        "1   plain     1.5     \n",
        "2   has|pipe  -2.5    \n",
        "3   z         100.0  7\n",
        "           id = 1\n",
        "a_longer_name = plain\n",
        "            r = 1.5\n",
        "            n = \n",
        "\n",
        "           id = 2\n",
        "a_longer_name = has|pipe\n",
        "            r = -2.5\n",
        "            n = \n",
        "id\tname\tnote\tr\tn\n",
        "1\tplain\tit's \"quoted\"\t1.5\t\n",
        "2\thas|pipe\ttab\there\t-2.5\t\n",
        "3\tz\ta,b\t100.0\t7\n",
        "id,name,b,r,n\r\n",
        "1,plain,AB,1.5,\r\n",
        "2,has|pipe,\"\nA\",-2.5,\r\n",
        "3,z,C,100.0,7\r\n",
    );

    /// bd-zy4es: `list`/`column`/`line`/`tabs`/`csv` output is byte-identical
    /// to the sqlite3 shell's. The fsqlite shell must reproduce the pinned stock
    /// output, and when a `sqlite3` binary is on PATH (3.33+, the modern
    /// `column` layout) that binary must reproduce it too, closing the
    /// fsqlite == stock differential live.
    #[test]
    fn test_display_modes_match_stock_sqlite3_shell_bd_zy4es() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(DISPLAY_MODE_SCRIPT.as_bytes().to_vec());
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];
            let exit_code =
                run_with_shell_options(args, &mut input, &mut out, &mut err, ShellOptions::batch())
                    .await;
            assert_eq!(exit_code, 0, "stderr: {}", String::from_utf8_lossy(&err));
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);
            let stdout = String::from_utf8(out).expect("stdout should be utf-8");
            assert_eq!(
                stdout, DISPLAY_MODE_STOCK_OUTPUT,
                "fsqlite display modes must print exactly what the sqlite3 shell prints"
            );
        });

        let Some((major, minor, version_text)) = system_sqlite3_version() else {
            return;
        };
        if (major, minor) < (3, 33) {
            eprintln!(
                "SKIP: sqlite3 {version_text} predates the 3.33 column-mode layout; \
                 live stock differential not run"
            );
            return;
        }
        let stock_stdout = system_sqlite3_stdout(DISPLAY_MODE_SCRIPT.as_bytes());
        assert_eq!(
            String::from_utf8_lossy(&stock_stdout),
            DISPLAY_MODE_STOCK_OUTPUT,
            "system sqlite3 {version_text} disagrees with the pinned stock output"
        );
    }

    /// Display output preserves blob bytes and stock's NUL termination.
    #[test]
    fn test_render_display_value_is_bare_text_bd_zy4es() {
        use fsqlite::SqliteValue;

        assert_eq!(render_display_value(&SqliteValue::Null).as_ref(), b"");
        assert_eq!(
            render_display_value(&SqliteValue::from("it's")).as_ref(),
            b"it's"
        );
        assert_eq!(render_display_value(&SqliteValue::from("")).as_ref(), b"");
        assert_eq!(
            render_display_value(&SqliteValue::Integer(-7)).as_ref(),
            b"-7"
        );
        assert_eq!(
            render_display_value(&SqliteValue::Float(0.1 + 0.2)).as_ref(),
            b"0.30000000000000004"
        );
        assert_eq!(
            render_display_value(&SqliteValue::from(b"AB".to_vec())).as_ref(),
            b"AB"
        );
        assert_eq!(
            render_display_value(&SqliteValue::from(vec![0xff, b'A', 0, b'B'])).as_ref(),
            b"\xffA"
        );
    }

    #[test]
    fn test_headers_alias_toggles_header_output() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(
                b".mode column\n\
.headers on\n\
SELECT 1 AS one, 'x' AS two;\n\
.quit\n"
                    .to_vec(),
            );
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;

            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);
            let stdout = String::from_utf8(out).expect("stdout should be utf-8");
            assert!(
                stdout.contains("one") && stdout.contains("two"),
                "expected .headers alias to enable column headers, got: {stdout}",
            );
        });
    }

    #[test]
    fn test_command_mode_dot_schema_supports_filtering() {
        asupersync::test_utils::run_test(|| async {
            let path = unique_temp_path("fsqlite_cli_schema", "db");
            let path_text = path.to_string_lossy().into_owned();
            let conn = fsqlite::Connection::open(path_text.clone())
                .await
                .expect("connection should open");
            conn.query("CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .expect("create widgets table");
            conn.query("CREATE TABLE gadgets(id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .expect("create gadgets table");
            drop(conn);

            let mut input = Cursor::new(Vec::<u8>::new());
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![
                OsString::from("fsqlite"),
                OsString::from(path_text),
                OsString::from("-c"),
                OsString::from(".schema widgets"),
            ];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;
            let _ = fs::remove_file(&path);

            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);

            let stdout = String::from_utf8(out).expect("output should be utf-8");
            assert!(
                stdout.contains("CREATE TABLE widgets"),
                "expected widgets schema in output, got: {stdout}",
            );
            assert!(
                !stdout.contains("CREATE TABLE gadgets"),
                "unexpected gadgets schema in filtered output: {stdout}",
            );
        });
    }

    #[test]
    fn test_repl_dump_command_emits_schema_and_escaped_values() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(
            b"CREATE TABLE notes(id INTEGER PRIMARY KEY, name TEXT, payload BLOB, note TEXT);\n\
INSERT INTO notes VALUES(1, 'O''Malley', x'0102', NULL);\n\
.dump\n\
.quit\n"
                .to_vec(),
        );
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let exit_code = run(args, &mut input, &mut out, &mut err).await;

            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);

            let stdout = String::from_utf8(out).expect("output should be utf-8");
            assert!(
                stdout.contains("BEGIN TRANSACTION;"),
                "expected transaction header in dump, got: {stdout}",
            );
            assert!(
                stdout.contains("CREATE TABLE notes"),
                "expected table DDL in dump, got: {stdout}",
            );
            assert!(
                stdout.contains("INSERT INTO \"notes\" VALUES(1, 'O''Malley', X'0102', NULL);"),
                "expected escaped INSERT in dump, got: {stdout}",
            );
            assert!(
                stdout.contains("COMMIT;"),
                "expected transaction trailer in dump, got: {stdout}",
            );
        });
    }

    #[test]
    fn test_dump_preserves_autoincrement_sequence_for_restore() {
        asupersync::test_utils::run_test(|| async {
            let mut dump_input = Cursor::new(
                b"CREATE TABLE ai(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);\n\
INSERT INTO ai(name) VALUES('a');\n\
INSERT INTO ai(name) VALUES('b');\n\
DELETE FROM ai WHERE id = 2;\n\
.dump\n"
                    .to_vec(),
            );
            let mut dump_out = Vec::new();
            let mut dump_err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let dump_exit = run_with_shell_options(
                args,
                &mut dump_input,
                &mut dump_out,
                &mut dump_err,
                ShellOptions::batch(),
            )
            .await;
            assert_eq!(dump_exit, 0);
            assert!(
                dump_err.is_empty(),
                "unexpected dump stderr: {:?}",
                dump_err
            );

            let dump = String::from_utf8(dump_out).expect("dump output should be utf-8");
            assert!(
                dump.contains("DELETE FROM sqlite_sequence;"),
                "expected dump to reset sqlite_sequence before restoring AUTOINCREMENT state: {dump}",
            );
            assert!(
                dump.contains("INSERT INTO \"sqlite_sequence\" VALUES('ai', 2);"),
                "expected dump to preserve AUTOINCREMENT high-water mark: {dump}",
            );

            let restore_script = format!(
                "{dump}\n\
INSERT INTO ai(name) VALUES('c');\n\
SELECT id FROM ai WHERE name = 'c';\n"
            );
            let mut restore_input = Cursor::new(restore_script.into_bytes());
            let mut restore_out = Vec::new();
            let mut restore_err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let restore_exit = run_with_shell_options(
                args,
                &mut restore_input,
                &mut restore_out,
                &mut restore_err,
                ShellOptions::batch(),
            )
            .await;
            assert_eq!(restore_exit, 0);
            assert!(
                restore_err.is_empty(),
                "unexpected restore stderr: {:?}",
                restore_err
            );

            let restored = String::from_utf8(restore_out).expect("restore output should be utf-8");
            assert!(
                restored.lines().any(|line| line.trim() == "3"),
                "restored AUTOINCREMENT table should continue at id=3, got: {restored}",
            );
        });
    }

    #[test]
    fn test_filtered_dump_does_not_reset_unrelated_autoincrement_sequence() {
        asupersync::test_utils::run_test(|| async {
            let mut dump_input = Cursor::new(
                b"CREATE TABLE ai(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);\n\
INSERT INTO ai(name) VALUES('a');\n\
INSERT INTO ai(name) VALUES('b');\n\
DELETE FROM ai WHERE id = 2;\n\
.dump ai\n"
                    .to_vec(),
            );
            let mut dump_out = Vec::new();
            let mut dump_err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let dump_exit = run_with_shell_options(
                args,
                &mut dump_input,
                &mut dump_out,
                &mut dump_err,
                ShellOptions::batch(),
            )
            .await;
            assert_eq!(dump_exit, 0);
            assert!(
                dump_err.is_empty(),
                "unexpected dump stderr: {:?}",
                dump_err
            );

            let dump = String::from_utf8(dump_out).expect("dump output should be utf-8");
            assert!(
                !dump.contains("DELETE FROM sqlite_sequence;"),
                "filtered dump must not clear unrelated sqlite_sequence rows: {dump}",
            );
            assert!(
                dump.contains("DELETE FROM sqlite_sequence WHERE name = 'ai';"),
                "filtered dump should reset only the dumped table sequence row: {dump}",
            );

            let restore_script = format!(
                "CREATE TABLE keep(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);\n\
INSERT INTO keep(name) VALUES('k1');\n\
INSERT INTO keep(name) VALUES('k2');\n\
DELETE FROM keep WHERE id = 2;\n\
{dump}\n\
INSERT INTO keep(name) VALUES('k3');\n\
SELECT id FROM keep WHERE name = 'k3';\n"
            );
            let mut restore_input = Cursor::new(restore_script.into_bytes());
            let mut restore_out = Vec::new();
            let mut restore_err = Vec::new();
            let args = vec![OsString::from("fsqlite")];

            let restore_exit = run_with_shell_options(
                args,
                &mut restore_input,
                &mut restore_out,
                &mut restore_err,
                ShellOptions::batch(),
            )
            .await;
            assert_eq!(restore_exit, 0);
            assert!(
                restore_err.is_empty(),
                "unexpected restore stderr: {:?}",
                restore_err
            );

            let restored = String::from_utf8(restore_out).expect("restore output should be utf-8");
            assert!(
                restored.lines().any(|line| line.trim() == "3"),
                "filtered restore should preserve unrelated AUTOINCREMENT sequence state, got: {restored}",
            );
        });
    }

    #[test]
    fn test_format_row_helper_with_connection_row() {
        asupersync::test_utils::run_test(|| async {
            let mut input = Cursor::new(Vec::<u8>::new());
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args = vec![
                OsString::from("fsqlite"),
                OsString::from("-c"),
                OsString::from("SELECT NULL;"),
            ];
            let exit_code = run(args, &mut input, &mut out, &mut err).await;
            assert_eq!(exit_code, 0);

            // Also directly exercise `format_row` on a real row.
            let conn = fsqlite::Connection::open(":memory:")
                .await
                .expect("connection should open");
            let row = conn
                .query_row("SELECT 10, 'abc', NULL;")
                .await
                .expect("query_row should succeed");
            let rendered = format_row(&row);
            // sqlite3 list mode: `|` separator, bare text, NULL empty (bd-zy4es).
            assert_eq!(rendered, "10|abc|");
        });
    }

    #[test]
    fn test_csv_mode_terminates_records_with_crlf_dx29q() {
        asupersync::test_utils::run_test(|| async {
            let conn = fsqlite::Connection::open(":memory:")
                .await
                .expect("connection should open");
            conn.execute("CREATE TABLE t(a, b)")
                .await
                .expect("create table");
            conn.execute("INSERT INTO t VALUES (1, 'x'), (2, 'has,comma')")
                .await
                .expect("insert rows");
            let rows = conn.query("SELECT a, b FROM t").await.expect("query rows");
            let column_names = vec!["a".to_owned(), "b".to_owned()];

            // bd-dx29q: `.mode csv` must terminate every record with CRLF
            // (RFC 4180 / sqlite3), quoting the comma-bearing field.
            let mut csv_out = Vec::new();
            write_delimited_rows(
                &rows,
                &column_names,
                OutputOptions {
                    mode: OutputMode::Csv,
                    headers: false,
                    ..OutputOptions::default()
                },
                OutputMode::Csv.separator(),
                &mut csv_out,
            )
            .expect("write csv rows");
            assert_eq!(
                String::from_utf8(csv_out).expect("utf-8"),
                "1,x\r\n2,\"has,comma\"\r\n",
                "CSV mode must emit RFC 4180 CRLF record terminators"
            );

            // `list` mode keeps the bare LF terminator sqlite3 uses.
            let mut list_out = Vec::new();
            write_delimited_rows(
                &rows,
                &column_names,
                OutputOptions {
                    mode: OutputMode::List,
                    headers: false,
                    ..OutputOptions::default()
                },
                OutputMode::List.separator(),
                &mut list_out,
            )
            .expect("write list rows");
            let list = String::from_utf8(list_out).expect("utf-8");
            assert!(
                !list.contains('\r'),
                "list mode must not use CRLF: {list:?}"
            );
            assert!(list.ends_with('\n'), "list mode uses a bare LF terminator");
        });
    }

    #[test]
    fn test_highlight_sql_colors_keywords_literals_and_comments() {
        let highlighted = highlight_sql("SELECT 7, 'x' -- note");
        assert!(highlighted.contains(&format!("{ANSI_BOLD_BLUE}SELECT{ANSI_RESET}")));
        assert!(highlighted.contains(&format!("{ANSI_MAGENTA}7{ANSI_RESET}")));
        assert!(highlighted.contains(&format!("{ANSI_GREEN}'x'{ANSI_RESET}")));
        assert!(highlighted.contains(&format!("{ANSI_DIM}-- note{ANSI_RESET}")));
    }

    #[test]
    fn test_render_prompt_includes_pending_sql_preview() {
        let prompt = render_prompt(
            "demo.db",
            "SELECT 1 FROM widgets",
            ShellOptions {
                show_prompts: true,
                colorize_prompts: false,
                fail_on_error: false,
            },
        );
        assert!(
            prompt.contains("SELECT 1 FROM widgets"),
            "expected continuation prompt preview, got: {prompt}",
        );
    }

    #[test]
    fn test_render_prompt_colorizes_pending_sql_preview() {
        let prompt = render_prompt(
            "demo.db",
            "SELECT 7, 'x'",
            ShellOptions {
                show_prompts: true,
                colorize_prompts: true,
                fail_on_error: false,
            },
        );
        assert!(
            prompt.contains(&format!("{ANSI_BOLD_BLUE}SELECT{ANSI_RESET}")),
            "expected SQL keyword highlighting in prompt preview, got: {prompt}",
        );
        assert!(
            prompt.contains(&format!("{ANSI_MAGENTA}7{ANSI_RESET}")),
            "expected numeric literal highlighting in prompt preview, got: {prompt}",
        );
        assert!(
            prompt.contains(&format!("{ANSI_GREEN}'x'{ANSI_RESET}")),
            "expected string literal highlighting in prompt preview, got: {prompt}",
        );
    }

    #[test]
    fn test_verify_proof_cli_success() {
        asupersync::test_utils::run_test(|| async {
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
            let proof =
                EcsDecodeProof::from_esis(oid, 4, &[0, 1, 2, 3, 4, 5], true, Some(4), 1, 42)
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
            let exit_code = run(args, &mut input, &mut out, &mut err).await;
            let _ = fs::remove_file(&path);

            assert_eq!(exit_code, 0);
            assert!(err.is_empty(), "unexpected stderr: {:?}", err);
            let stdout = String::from_utf8(out).expect("stdout should be utf-8");
            assert!(
                stdout.contains("\"ok\": true"),
                "expected successful verification report, got: {stdout}",
            );
        });
    }

    #[test]
    fn test_verify_proof_cli_failure_on_policy_mismatch() {
        asupersync::test_utils::run_test(|| async {
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
            let proof =
                EcsDecodeProof::from_esis(oid, 4, &[0, 1, 2, 3, 4, 5], true, Some(4), 1, 42);
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
            let exit_code = run(args, &mut input, &mut out, &mut err).await;
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
        });
    }
}
