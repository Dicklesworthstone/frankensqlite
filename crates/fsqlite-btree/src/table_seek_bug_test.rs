#[cfg(test)]
mod tests {
    use asupersync::runtime::RuntimeBuilder;

    use crate::cursor::{BtCursor, MemPageStore};
    use crate::traits::BtreeCursorOps;
    use fsqlite_types::PageNumber;
    use fsqlite_types::cx::Cx;

    #[test]
    fn test_table_seek_bug_on_deleted_max_key() {
        RuntimeBuilder::current_thread()
            .build()
            .expect("build table seek regression test runtime")
            .block_on(async {
                let cx = Cx::new();
                let root = PageNumber::new(2).unwrap();
                let store = MemPageStore::with_empty_table(root, 4096);
                let mut cursor = BtCursor::new(store, root, 4096, true);

                // Insert a bunch of rows to create an interior node.
                for i in 0..100 {
                    cursor.table_insert(&cx, i, b"value").await.unwrap();
                }

                // Let's print the tree or just delete some elements.
                for i in 40..50 {
                    assert!(cursor.table_move_to(&cx, i).await.unwrap().is_found());
                    cursor.delete(&cx).await.unwrap();
                }

                // Now let's try to seek to 45.
                // 45 was deleted. So table_move_to should return NotFound.
                // And cursor.rowid() should be 50.
                let res = cursor.table_move_to(&cx, 45).await.unwrap();
                assert!(!res.is_found());
                let rowid = cursor.rowid(&cx).await.unwrap();
                assert_eq!(rowid, 50);
            });
    }
}
