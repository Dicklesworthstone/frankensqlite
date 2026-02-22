# fsqlite-cli

Interactive SQL shell for FrankenSQLite, providing a REPL and single-command
execution mode.

## Overview

`fsqlite-cli` is the command-line binary for FrankenSQLite. It compiles to the
`fsqlite` executable and provides:

- **Interactive REPL** -- Multi-line SQL input with primary (`fsqlite> `) and
  continuation (`   ...> `) prompts. Statements are delimited by semicolons.
- **Single-command mode** (`-c` / `--command`) -- Execute one SQL string and
  exit. Useful for scripting and pipelines.
- **Decode proof verification** (`--verify-proof`) -- Verify ECS decode proofs
  from a JSON file, with configurable policy ID and slack parameters.
- **In-memory or file-backed** -- Defaults to `:memory:` if no database path is
  provided; pass a file path as a positional argument for persistent storage.

**Position in the dependency graph:**

```
fsqlite-cli (this crate) -- the user-facing binary
  --> fsqlite (public API facade)
    --> fsqlite-core (engine)
      --> fsqlite-parser, fsqlite-planner, fsqlite-vdbe, ...
```

Dependencies: `fsqlite`, `fsqlite-core`, `fsqlite-error`, `fsqlite-types`,
`serde`, `serde_json`.

## Key Types

- `CliOptions` -- Parsed command-line arguments (db path, command, verify-proof
  path, policy ID, slack, help flag).
- `main()` / `run()` -- Entry point. Dispatches to REPL, single-command
  execution, or proof verification based on CLI arguments.

## Usage

```bash
# Interactive REPL with in-memory database
fsqlite

# Interactive REPL with a file-backed database
fsqlite mydb.sqlite

# Execute a single SQL command
fsqlite mydb.sqlite -c "SELECT * FROM users;"

# Execute against in-memory database
fsqlite -c "SELECT 1 + 2;"

# Verify an ECS decode proof
fsqlite --verify-proof proof.json

# Show help
fsqlite --help
```

### CLI Options

| Flag | Description |
|------|-------------|
| `<db_path>` | Database file path (default: `:memory:`) |
| `-c`, `--command <SQL>` | Execute SQL and exit |
| `--verify-proof <path>` | Verify ECS decode proof from JSON file |
| `--verify-policy-id <N>` | Policy ID for proof verification |
| `--verify-slack <N>` | Slack parameter for proof verification |
| `-h`, `--help` | Show usage information |

## License

MIT (with OpenAI/Anthropic Rider) -- see workspace root LICENSE file.
