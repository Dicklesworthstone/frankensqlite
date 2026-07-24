#[cfg(test)]
mod tests2 {
    use asupersync::runtime::RuntimeBuilder;

    use crate::cursor::{BtCursor, MemPageStore};
    use crate::traits::BtreeCursorOps;
    use fsqlite_types::PageNumber;
    use fsqlite_types::cx::Cx;

    #[test]
    fn test_my_seek_bug() {
        RuntimeBuilder::current_thread()
            .build()
            .expect("build index seek regression test runtime")
            .block_on(async {
                let cx = Cx::new();
                let root = PageNumber::new(2).unwrap();
                let store = MemPageStore::with_empty_index(root, 4096);
                let mut cursor = BtCursor::new(store, root, 4096, false);

                // Let's print out what happens
                if let Err(error) = cursor.index_insert(&cx, b"M").await {
                    println!("ERR: {error:?}");
                }
            });
    }
}
