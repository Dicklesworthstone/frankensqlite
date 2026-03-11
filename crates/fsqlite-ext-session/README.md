# fsqlite-ext-session

Session, changeset, and patchset extension for FrankenSQLite.

## Overview

This crate implements the SQLite session extension, which records changes made to a database and produces binary changesets or patchsets. These changesets can be serialized, transmitted, and applied to other databases to replicate modifications. The crate handles the full lifecycle: tracking changes via `Session`, encoding/decoding the binary changeset and patchset formats (including varints, table headers, and per-row operation records), inverting changesets, concatenating multiple changesets, and applying decoded changes with conflict resolution.

The current `Session` API is a manual recorder rather than an engine-side preupdate hook. To stay aligned with SQLite session semantics, only explicitly attached tables with an explicit primary key participate in emitted changesets or patchsets; unattached tables and attached tables with no primary-key columns are ignored.

This is a leaf crate in the fsqlite workspace dependency graph. It depends only on `fsqlite-types` (for `SqliteValue` and varint utilities) and `fsqlite-error`.

## Key Types

- `Session` - Tracks changes to attached tables with explicit primary keys, producing `Changeset` objects on demand
- `Changeset` - A complete set of changes across one or more tables, with methods for binary encoding, changeset/patchset decoding, inversion, and concatenation
- `TableChangeset` - Changes scoped to a single table within a changeset
- `ChangesetRow` - A single DML operation (insert, delete, or update) with old/new column values
- `ChangeOp` - Enum of operation types: `Insert`, `Delete`, `Update`
- `ChangesetValue` - Column value in the binary format: `Undefined`, `Null`, `Integer`, `Real`, `Text`, `Blob`
- `TableInfo` - Schema metadata for a tracked table (name, column names, primary key columns)
- `ConflictType` - Category of conflict during apply: `Data`, `NotFound`, `Conflict`, `Constraint`, `ForeignKey`
- `ConflictAction` - Caller-chosen resolution: `OmitChange`, `Replace`, `Abort`
- `SimpleTarget` - A basic apply target implementing in-memory conflict resolution; synthetic no-PK changesets fall back to full-row identity matching
- `ApplyOutcome` - Result of applying a changeset: `Applied`, `Aborted`, or `PartiallyApplied`

## Key Functions

- `extension_name()` - Returns `"session"`
- `changeset_varint_len()` - Compute the encoded length of a varint in the changeset binary format
- `Changeset::decode()` / `Changeset::decode_patchset()` - Decode a full changeset or compact patchset into the reusable in-memory representation

## Dependencies

- `fsqlite-types`
- `fsqlite-error`

## License

MIT
