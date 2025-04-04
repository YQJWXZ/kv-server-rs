use std::{path::Path, str};

use sled::{Db, IVec};

use crate::{KvError, Kvpair, Value};

use super::{Storage, StorageIter};

#[derive(Debug)]
pub struct SledDb(Db);

impl SledDb {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(sled::open(path).unwrap())
    }

    fn get_full_key(table: &str, key: &str) -> String {
        format!("{}:{}", table, key)
    }

    fn get_table_prefix(table: &str) -> String {
        format!("{}:", table)
    }
}

// Option<Result<T, E>> -> Result<Option<T>, E>
fn flip<T, E>(x: Option<Result<T, E>>) -> Result<Option<T>, E> {
    x.map_or(Ok(None), |v| v.map(Some))
}

impl Storage for SledDb {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let name = SledDb::get_full_key(table, key);
        let res = self.0.get(name.as_bytes())?.map(|v| v.as_ref().try_into());
        flip(res)
    }

    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError> {
        let name = SledDb::get_full_key(table, &key);
        let data: Vec<u8> = value.try_into()?;
        let res = self.0.insert(name, data)?.map(|v| v.as_ref().try_into());
        flip(res)
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let name = SledDb::get_full_key(table, key);
        Ok(self.0.contains_key(name)?)
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let name = SledDb::get_full_key(table, key);
        let res = self.0.remove(name)?.map(|v| v.as_ref().try_into());
        flip(res)
    }

    fn get_all(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError> {
        let prefix = SledDb::get_table_prefix(table);
        let iter = StorageIter::new(self.0.scan_prefix(prefix));

        Ok(Box::new(iter))
    }
}

impl From<Result<(IVec, IVec), sled::Error>> for Kvpair {
    fn from(v: Result<(IVec, IVec), sled::Error>) -> Self {
        match v {
            Ok((k, v)) => match v.as_ref().try_into() {
                Ok(v) => Kvpair::new(ivec_to_key(&k), v),
                Err(_) => Kvpair::default(),
            },

            _ => Kvpair::default(),
        }
    }
}

fn ivec_to_key(ivec: &IVec) -> &str {
    let s = str::from_utf8(ivec.as_ref()).unwrap();
    let mut iter = s.split(':');
    iter.next();
    iter.next().unwrap()
}
