#!/usr/bin/env python3
"""Apply GH516 by exact extraction; refuse unknown or already modified input.

The 12 MB connection.rs cannot be safely replaced through an inline connector
write. This deterministic transform moves its existing INSERT arm byte-for-byte
into a shared executor, adding only a compilation-identity check at its VDBE
boundary. It does not mutate Git refs. Native tests must precede promotion.
"""
from __future__ import annotations
import argparse
import hashlib
import pathlib
import textwrap

CONNECTION = pathlib.Path("crates/fsqlite-core/src/connection.rs")
MODULE = pathlib.Path("crates/fsqlite-core/src/connection/insert_batch.rs")
BASE_BLOB = "b3903e679debd22edaa305b77d8f82200ca16d1c"


def blob_hash(data: bytes) -> str:
    return hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()


def replace_once(text: str, old: str, new: str) -> str:
    count = text.count(old)
    if count != 1:
        raise ValueError(f"expected exactly one anchor, found {count}: {old[:100]!r}")
    return text.replace(old, new, 1)


def transform(source: str, template: str) -> tuple[str, str]:
    if "mod insert_batch;" in source:
        raise ValueError("GH516 has already been applied")
    dispatch = source.index("    async fn execute_statement_dispatch_impl(")
    head = "            Statement::Insert(insert) => {\n"
    start = source.index(head + "                if self.pragma_state.borrow().writable_schema", dispatch)
    end = source.index("            Statement::Update(update) => {\n", start)
    arm = source[start:end]
    if not arm.endswith("            }\n"):
        raise ValueError("unexpected INSERT arm closing delimiter")
    body = textwrap.dedent(arm[len(head):-len("            }\n")])
    original_body = body
    body = replace_once(
        body,
        "let program: &VdbeProgram = if let Some(p) = precompiled {",
        "// A BEFORE trigger can invalidate code compiled at batch entry.\n"
        "// Check again here, after BEFORE and immediately before the write.\n"
        "let program: &VdbeProgram = if let Some(p) = precompiled.filter(|_| {\n"
        "    replay_identity.is_none_or(|identity| self.insert_replay_identity() == identity)\n"
        "}) {",
    )
    module = replace_once(template, "/* INSERT_DISPATCH_BODY */", textwrap.indent(body, "        ").rstrip())
    new_arm = head + (
        "                Box::pin(self.execute_insert_dispatch(\n"
        "                    cx, statement, insert, params, precompiled, None,\n"
        "                )).await\n"
        "            }\n"
    )
    source = source[:start] + new_arm + source[end:]
    # A module declaration is an item and may precede use declarations, but
    # never inner documentation/attributes. Place it next to an existing module.
    source = replace_once(source, "mod conformal_retry;", "mod conformal_retry;\nmod insert_batch;")
    source = replace_once(source,
        "    prepared: Option<PreparedStatement<'conn>>,\n    returning_statement: Option<Statement>,",
        "    prepared: Option<PreparedStatement<'conn>>,\n"
        "    compiled_insert: Option<insert_batch::CompiledInsertReplay>,\n"
        "    returning_statement: Option<Statement>,")
    source = replace_once(source,
        "        let row_result = if let Some(returning_statement) = self.returning_statement.as_ref() {",
        "        let row_result = if let Some(plan) = self.compiled_insert.as_ref() {\n"
        "            match self.connection.execute_compiled_insert_replay_row(plan, row_values).await {\n"
        "                Ok(outcome) => {\n"
        "                    self.returning_rows.extend(outcome.returning_rows);\n"
        "                    Ok(outcome.changes)\n"
        "                }\n"
        "                Err(error) => Err(error),\n"
        "            }\n"
        "        } else if let Some(returning_statement) = self.returning_statement.as_ref() {")
    source = replace_once(source,
        "                let prepared = if collect_returning {\n                    None\n                } else {\n"
        "                    Some(self.prepare_after_background_status(&insert_sql).await?)\n                };",
        "                let compiled_insert = self.prepare_compiled_insert_replay(insert, &insert_sql).await?;\n"
        "                let prepared = if collect_returning || compiled_insert.is_some() {\n"
        "                    None\n                } else {\n"
        "                    Some(self.prepare_after_background_status(&insert_sql).await?)\n                };")
    source = replace_once(source,
        "                    table_column_count,\n                    prepared,\n                    returning_statement,",
        "                    table_column_count,\n                    prepared,\n                    compiled_insert,\n                    returning_statement,")
    source = replace_once(source,
        "                let returning_statement = if collect_returning {",
        "                let returning_statement = if collect_returning && compiled_insert.is_none() {")
    # These assertions prove the extraction didn't silently prune semantics.
    for marker in ["fire_before_triggers(", "fire_after_triggers(",
                   "enforce_fk_on_replace_victims(", "enforce_fk_on_insert(",
                   "route_upsert_conflict(", "refresh_autoincrement_sequence_after_insert("]:
        if module.count(marker) != original_body.count(marker):
            raise ValueError(f"INSERT semantic operation changed during extraction: {marker}")
    return source, module


def apply(root: pathlib.Path) -> list[pathlib.Path]:
    data = (root / CONNECTION).read_bytes()
    if blob_hash(data) != BASE_BLOB:
        raise ValueError(f"connection.rs changed: expected {BASE_BLOB}, found {blob_hash(data)}; review/rebase required")
    if (root / MODULE).exists():
        raise ValueError(f"refusing to overwrite {MODULE}")
    template = (root / "artifacts/gh516/insert_batch.rs.in").read_text()
    connection, module = transform(data.decode(), template)
    # Compute everything and check all anchors before modifying either file.
    (root / MODULE).write_text(module)
    (root / CONNECTION).write_text(connection)
    return [CONNECTION, MODULE]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=pathlib.Path, default=pathlib.Path.cwd())
    args = parser.parse_args()
    for changed in apply(args.root.resolve()):
        print(changed)
