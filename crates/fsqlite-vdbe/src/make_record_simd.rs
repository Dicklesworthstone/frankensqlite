use fsqlite_types::SqliteValue;

pub fn try_serialize_integer_record_iter_into<'a, I>(values: I, buf: &mut Vec<u8>) -> bool
where
    I: Iterator<Item = &'a SqliteValue> + Clone,
{
    fsqlite_types::record::simd_serialize_integer_record(values, buf)
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::try_serialize_integer_record_iter_into;
    use fsqlite_types::record::serialize_record;
    use fsqlite_types::value::SqliteValue;

    #[test]
    fn integer_record_fast_path_matches_scalar_record_bytes() {
        let row = vec![
            SqliteValue::Integer(0),
            SqliteValue::Integer(1),
            SqliteValue::Integer(-128),
            SqliteValue::Integer(32_767),
            SqliteValue::Integer(32_768),
            SqliteValue::Integer(8_388_607),
            SqliteValue::Integer(8_388_608),
            SqliteValue::Integer(i64::MIN),
        ];

        let mut fast = Vec::new();
        assert!(try_serialize_integer_record_iter_into(
            row.iter(),
            &mut fast
        ));
        assert_eq!(fast, serialize_record(&row));
    }

    #[test]
    fn integer_record_fast_path_rejects_non_integer_rows() {
        let row = vec![
            SqliteValue::Integer(7),
            SqliteValue::Text("not-an-integer".into()),
            SqliteValue::Integer(9),
        ];

        let mut fast = Vec::from([0xAA, 0xBB]);
        assert!(!try_serialize_integer_record_iter_into(
            row.iter(),
            &mut fast
        ));
        assert_eq!(fast, vec![0xAA, 0xBB]);
    }

    #[test]
    fn integer_record_fast_path_handles_large_headers() {
        let row = (0_i64..140).map(SqliteValue::Integer).collect::<Vec<_>>();

        let mut fast = Vec::new();
        assert!(try_serialize_integer_record_iter_into(
            row.iter(),
            &mut fast
        ));
        assert_eq!(fast, serialize_record(&row));
    }
}
