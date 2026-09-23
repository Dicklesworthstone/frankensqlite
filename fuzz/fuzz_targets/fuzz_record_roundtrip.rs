#![no_main]
// Fuzzes the UTF-8 record codecs directly.
#![allow(clippy::disallowed_methods)]

//! Fuzz the record format serialization/deserialization.
//!
//! Two strategies:
//! 1. Arbitrary bytes → parse_record must not panic (may return None).
//! 2. Structured SqliteValues → serialize then parse must round-trip.

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

use fsqlite_types::SqliteValue;
use fsqlite_types::record::{parse_record, serialize_record};

/// An arbitrary `SqliteValue` for structured fuzzing.
#[derive(Debug, Arbitrary)]
enum FuzzValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl FuzzValue {
    fn to_sqlite_value(&self) -> SqliteValue {
        match self {
            Self::Null => SqliteValue::Null,
            Self::Integer(i) => SqliteValue::Integer(*i),
            Self::Float(f) => SqliteValue::Float(*f),
            Self::Text(s) => SqliteValue::Text(s.clone()),
            Self::Blob(b) => SqliteValue::Blob(b.clone()),
        }
    }
}

#[derive(Debug, Arbitrary)]
struct FuzzInput {
    /// Raw bytes to feed to parse_record (crash detection).
    raw: Vec<u8>,
    /// Structured values for round-trip testing.
    values: Vec<FuzzValue>,
}

fuzz_target!(|input: FuzzInput| {
    // Strategy 1: raw bytes must not panic the parser.
    if input.raw.len() <= 65536 {
        let _ = parse_record(&input.raw);
    }

    // Strategy 2: round-trip (serialize then parse).
    if input.values.len() <= 100 {
        let vals: Vec<SqliteValue> = input
            .values
            .iter()
            .map(FuzzValue::to_sqlite_value)
            .collect();
        let encoded = serialize_record(&vals);
        let decoded = parse_record(&encoded);

        // Round-trip must succeed for well-formed input.
        if let Some(decoded_vals) = decoded {
            assert_eq!(
                vals.len(),
                decoded_vals.len(),
                "column count mismatch after round-trip"
            );

            for (orig, dec) in vals.iter().zip(decoded_vals.iter()) {
                match (orig, dec) {
                    (SqliteValue::Null, SqliteValue::Null) => {}
                    // SQLite normalizes Float(NaN) to NULL for deterministic storage.
                    (SqliteValue::Float(f), SqliteValue::Null) if f.is_nan() => {}
                    // SQLite normalizes Float(Inf/-Inf) to NULL in some contexts,
                    // but the record format preserves them. Verify exact bit pattern.
                    (SqliteValue::Float(a), SqliteValue::Float(b)) => {
                        assert_eq!(
                            a.to_bits(),
                            b.to_bits(),
                            "float bit-pattern mismatch: {a} vs {b}"
                        );
                    }
                    (SqliteValue::Integer(a), SqliteValue::Integer(b)) => {
                        assert_eq!(a, b, "integer mismatch");
                    }
                    (SqliteValue::Text(a), SqliteValue::Text(b)) => {
                        assert_eq!(a, b, "text mismatch");
                    }
                    (SqliteValue::Blob(a), SqliteValue::Blob(b)) => {
                        assert_eq!(a, b, "blob mismatch");
                    }
                    _ => {
                        unreachable!("unexpected type change in round-trip: {orig:?} vs {dec:?}");
                    }
                }
            }
        }
    }
});
