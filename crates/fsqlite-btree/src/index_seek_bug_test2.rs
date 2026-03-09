#[cfg(test)]
mod tests2 {
    use crate::cursor::{BtCursor, MemPageStore};
    use crate::traits::BtreeCursorOps;
    use fsqlite_types::PageNumber;
    use fsqlite_types::cx::Cx;

    #[test]
    fn test_my_seek_bug() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_index(root, 4096);
        let mut cursor = BtCursor::new(store, root, 4096, false);

        // Let's print out what happens
        if let Err(e) = cursor.index_insert(&cx, b"M") {
            println!("ERR: {:?}", e);
        }
    }
}
