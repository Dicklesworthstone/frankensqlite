//! ATTACH/DETACH database schema registry (§12.11, bd-7pxb).
//!
//! Each attached database gets a schema namespace. Tables are accessible as
//! `schema-name.table-name`. The main database is always `main`, the temp
//! database is always `temp`. Maximum 10 attached databases (`SQLITE_MAX_ATTACHED`).

use fsqlite_error::{FrankenError, Result};
use tracing::{debug, info};

/// Maximum number of attached databases (not counting `main` and `temp`).
pub const SQLITE_MAX_ATTACHED: usize = 10;

// ---------------------------------------------------------------------------
// Attached database entry
// ---------------------------------------------------------------------------

/// Metadata for a single attached database.
#[derive(Debug, Clone)]
pub struct AttachedDb {
    /// Schema name (used in `schema.table` references).
    pub schema: String,
    /// File path or URI for the database file.
    pub path: String,
}

// ---------------------------------------------------------------------------
// Schema registry
// ---------------------------------------------------------------------------

/// Registry of attached databases for a connection.
///
/// The `main` and `temp` schemas are always present and cannot be detached.
/// Up to `SQLITE_MAX_ATTACHED` additional databases can be attached.
#[derive(Debug)]
pub struct SchemaRegistry {
    /// Additional attached databases (not including `main`/`temp`).
    attached: Vec<AttachedDb>,
}

impl SchemaRegistry {
    /// Create a new registry with only `main` and `temp`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            attached: Vec::new(),
        }
    }

    /// Attach a database file with the given schema name.
    ///
    /// # Errors
    /// Returns error if the name is already in use, or if the maximum number
    /// of attached databases would be exceeded (invariant #8).
    pub fn attach(&mut self, schema: String, path: String) -> Result<()> {
        let lower = schema.to_ascii_lowercase();

        // Cannot re-use reserved names.
        if lower == "main" || lower == "temp" {
            return Err(FrankenError::internal(format!(
                "cannot attach with reserved schema name: {schema}"
            )));
        }

        // Check for duplicate.
        if self
            .attached
            .iter()
            .any(|db| db.schema.eq_ignore_ascii_case(&schema))
        {
            return Err(FrankenError::internal(format!(
                "database already attached with schema name: {schema}"
            )));
        }

        // Enforce SQLITE_MAX_ATTACHED (invariant #8).
        if self.attached.len() >= SQLITE_MAX_ATTACHED {
            return Err(FrankenError::internal(format!(
                "too many attached databases (max {SQLITE_MAX_ATTACHED})"
            )));
        }

        info!(
            schema = %schema,
            path = %path,
            "database attached"
        );

        self.attached.push(AttachedDb { schema, path });
        Ok(())
    }

    /// Detach a database by schema name.
    ///
    /// # Errors
    /// Returns error if the schema name is not found or is reserved.
    pub fn detach(&mut self, schema: &str) -> Result<()> {
        let lower = schema.to_ascii_lowercase();

        if lower == "main" || lower == "temp" {
            return Err(FrankenError::internal(format!(
                "cannot detach reserved schema: {schema}"
            )));
        }

        let pos = self
            .attached
            .iter()
            .position(|db| db.schema.eq_ignore_ascii_case(schema))
            .ok_or_else(|| FrankenError::internal(format!("no such database: {schema}")))?;

        let removed = self.attached.remove(pos);
        debug!(
            schema = %removed.schema,
            path = %removed.path,
            "database detached"
        );

        Ok(())
    }

    /// Look up an attached database by schema name.
    ///
    /// Returns `None` for `main`/`temp` (they are implicit) and for unknown names.
    #[must_use]
    pub fn find(&self, schema: &str) -> Option<&AttachedDb> {
        self.attached
            .iter()
            .find(|db| db.schema.eq_ignore_ascii_case(schema))
    }

    /// Number of attached databases (not counting `main`/`temp`).
    #[must_use]
    pub fn count(&self) -> usize {
        self.attached.len()
    }

    /// Resolve a schema-qualified name. Returns `true` if the schema is
    /// `main`, `temp`, or a currently attached database.
    #[must_use]
    pub fn is_valid_schema(&self, schema: &str) -> bool {
        let lower = schema.to_ascii_lowercase();
        lower == "main" || lower == "temp" || self.find(schema).is_some()
    }

    /// List all schema names (including `main` and `temp`).
    #[must_use]
    pub fn all_schemas(&self) -> Vec<&str> {
        let mut names: Vec<&str> = vec!["main", "temp"];
        for db in &self.attached {
            names.push(&db.schema);
        }
        names
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // === Test 15: ATTACH creates accessible schema ===
    #[test]
    fn test_attach_database() {
        let mut reg = SchemaRegistry::new();
        reg.attach("aux".to_owned(), "/tmp/aux.db".to_owned())
            .unwrap();
        assert_eq!(reg.count(), 1);
        assert!(reg.is_valid_schema("aux"));
    }

    // === Test 16: Schema-qualified access ===
    #[test]
    fn test_attach_schema_qualified_access() {
        let mut reg = SchemaRegistry::new();
        reg.attach("mydb".to_owned(), "/tmp/mydb.db".to_owned())
            .unwrap();

        // Schema is accessible.
        assert!(reg.is_valid_schema("mydb"));
        let db = reg.find("mydb").unwrap();
        assert_eq!(db.schema, "mydb");
        assert_eq!(db.path, "/tmp/mydb.db");

        // Main and temp are always valid (invariant #9).
        assert!(reg.is_valid_schema("main"));
        assert!(reg.is_valid_schema("temp"));
    }

    // === Test 17: DETACH removes attached database ===
    #[test]
    fn test_detach_database() {
        let mut reg = SchemaRegistry::new();
        reg.attach("aux".to_owned(), "/tmp/aux.db".to_owned())
            .unwrap();
        assert_eq!(reg.count(), 1);
        reg.detach("aux").unwrap();
        assert_eq!(reg.count(), 0);
        assert!(!reg.is_valid_schema("aux"));
    }

    // === Test 18: Cannot attach more than SQLITE_MAX_ATTACHED (invariant #8) ===
    #[test]
    fn test_attach_max_limit() {
        let mut reg = SchemaRegistry::new();
        for i in 0..SQLITE_MAX_ATTACHED {
            reg.attach(format!("db{i}"), format!("/tmp/db{i}.db"))
                .unwrap();
        }
        assert_eq!(reg.count(), SQLITE_MAX_ATTACHED);

        // The 11th attach should fail.
        let result = reg.attach("overflow".to_owned(), "/tmp/overflow.db".to_owned());
        assert!(result.is_err());
    }

    // === Test 19: Cross-database transaction tracking ===
    // Note: Full cross-database atomic WAL transactions via 2PC are
    // covered in bd-d2m7. This test verifies multi-schema awareness.
    #[test]
    fn test_cross_database_transaction() {
        let mut reg = SchemaRegistry::new();
        reg.attach("aux1".to_owned(), "/tmp/aux1.db".to_owned())
            .unwrap();
        reg.attach("aux2".to_owned(), "/tmp/aux2.db".to_owned())
            .unwrap();

        // All schemas visible.
        let schemas = reg.all_schemas();
        assert!(schemas.contains(&"main"));
        assert!(schemas.contains(&"temp"));
        assert!(schemas.contains(&"aux1"));
        assert!(schemas.contains(&"aux2"));
    }

    // === Test: Cannot detach main/temp ===
    #[test]
    fn test_cannot_detach_reserved() {
        let mut reg = SchemaRegistry::new();
        assert!(reg.detach("main").is_err());
        assert!(reg.detach("temp").is_err());
    }

    // === Test: Cannot attach duplicate schema name ===
    #[test]
    fn test_attach_duplicate() {
        let mut reg = SchemaRegistry::new();
        reg.attach("aux".to_owned(), "/tmp/aux.db".to_owned())
            .unwrap();
        assert!(
            reg.attach("aux".to_owned(), "/tmp/other.db".to_owned())
                .is_err()
        );
    }

    // === Test: Case-insensitive schema lookup ===
    #[test]
    fn test_schema_case_insensitive() {
        let mut reg = SchemaRegistry::new();
        reg.attach("MyDb".to_owned(), "/tmp/mydb.db".to_owned())
            .unwrap();
        assert!(reg.is_valid_schema("mydb"));
        assert!(reg.is_valid_schema("MYDB"));
        assert!(reg.is_valid_schema("MyDb"));
    }
}
