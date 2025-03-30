mod memory;

use crate::{KvError, Kvpair, Value};
pub use memory::MemTable;

// we don't care where the data is stored, but we need to define how to deal with storage
pub trait Storage: Send + Sync + 'static {
    // get a key value from HashTable
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    // set a key value to HashTable, return old value
    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError>;
    // check if a key exists in HashTable
    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError>;
    // delete a key from HashTable
    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    // list all key-value pairs in HashTable, return iterator of Kvpair
    fn get_all(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError>;
}

/// Provide Storage Iterator, so that the implementation of trait only needs
/// provide the iterator to StorageIter, we need implement 'Into<Kvpair>' for
/// the type from the 'iterator.next()'
pub struct StorageIter<I> {
    iter: I,
}

impl<I> StorageIter<I> {
    pub fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I> Iterator for StorageIter<I>
where
    I: Iterator,
    I::Item: Into<Kvpair>,
{
    type Item = Kvpair;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| v.into())
    }
}

#[cfg(test)]
mod tests {
    use super::{memory::MemTable, *};

    #[test]
    fn memtable_basic_interface_should_work() {
        let store = MemTable::new();
        test_basi_interface(store);
    }

    #[test]
    fn memtable_get_all_should_work() {
        let store = MemTable::new();
        test_get_all(store);
    }

    fn test_basi_interface(store: impl Storage) {
        // first set should create the table, insert key and return None Value
        let v = store.set("t1", "hello".into(), "world".into());
        assert!(v.unwrap().is_none());

        // second set should update the key, and return old value
        let v1 = store.set("t1", "hello".into(), "world1".into());
        assert_eq!(v1.unwrap(), Some("world".into()));

        // get exited key should return the latest value
        let v = store.get("t1", "hello");
        assert_eq!(v.unwrap(), Some("world1".into()));

        // get non-exited key should return None
        assert_eq!(None, store.get("t1", "hello1").unwrap());
        assert!(store.get("t2", "hello1").unwrap().is_none());

        // contains should return true if the key exists, false otherwise
        assert!(store.contains("t1", "hello").unwrap());
        assert!(!store.contains("t1", "hello1").unwrap());
        assert!(!store.contains("t2", "hello").unwrap());

        // delete exited key should return the last updated value
        let v = store.del("t1", "hello");
        assert_eq!(v.unwrap(), Some("world1".into()));

        // delete non-exited key should return None
        assert_eq!(None, store.del("t1", "hello1").unwrap());
        assert_eq!(None, store.del("t2", "hello").unwrap());
    }

    fn test_get_all(store: impl Storage) {
        store.set("t2", "k1".into(), "v1".into()).unwrap();
        store.set("t2", "k2".into(), "v2".into()).unwrap();

        let mut res = store.get_all("t2").unwrap().collect::<Vec<Kvpair>>();
        res.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            res,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into())
            ]
        )
    }
}
