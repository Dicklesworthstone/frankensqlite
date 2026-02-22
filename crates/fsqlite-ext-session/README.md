# fsqlite-ext-session

Session, changeset, and patchset extension for FrankenSQLite.

## Overview

This crate implements the SQLite session extension, which records changes made to a database and produces binary changesets or patchsets. These changesets can be serialized, transmitted, and applied to other databases to replicate modifications. The crate handles the full lifecycle: tracking changes via `Session`, encoding/decoding the binary changeset format (including varints, table headers, and per-row operation records), inverting changesets, concatenating multiple changesets, and applying changesets with conflict resolution.

This is a leaf crate in the fsqlite workspace dependency graph. It depends only on `fsqlite-types` (for `SqliteValue` and varint utilities) and `fsqlite-error`.

## Key Types

- `Session` - Tracks changes to attached tables, producing `Changeset` objects on demand
- `Changeset` - A complete set of changes across one or more tables, with methods for binary encoding, decoding, inversion, and concatenation
- `TableChangeset` - Changes scoped to a single table within a changeset
- `ChangesetRow` - A single DML operation (insert, delete, or update) with old/new column values
- `ChangeOp` - Enum of operation types: `Insert`, `Delete`, `Update`
- `ChangesetValue` - Column value in the binary format: `Undefined`, `Null`, `Integer`, `Real`, `Text`, `Blob`
- `TableInfo` - Schema metadata for a tracked table (name, column names, primary key columns)
- `ConflictType` - Category of conflict during apply: `Data`, `NotFound`, `Conflict`, `Constraint`, `ForeignKey`
- `ConflictAction` - Caller-chosen resolution: `OmitChange`, `Replace`, `Abort`
- `SimpleTarget` - A basic apply target implementing in-memory conflict resolution
- `ApplyOutcome` - Result of applying a changeset: `Applied`, `Aborted`, or `PartiallyApplied`

## Key Functions

- `extension_name()` - Returns `"session"`
- `changeset_varint_len()` - Compute the encoded length of a varint in the changeset binary format

## Dependencies

- `fsqlite-types`
- `fsqlite-error`

## License

MIT
