use crate::*;

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(format!("table {}, key {}", self.table, self.key)).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.keys
            .iter()
            .map(|key| match store.get(&self.table, key) {
                Ok(Some(v)) => v,
                _ => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(v) => v.collect::<Vec<Kvpair>>().into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self.pair {
            Some(v) => match store.set(&self.table, v.key, v.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
            None => KvError::InvalidCommand(format!("{:?}", self)).into(),
        }
    }
}

impl CommandService for Hmset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let pairs = self.pairs;
        let table = self.table;
        pairs
            .into_iter()
            .map(|pair| {
                let res = store.set(&table, pair.key, pair.value.unwrap_or_default());
                match res {
                    Ok(Some(v)) => v,
                    _ => Value::default(),
                }
            })
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.del(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => Value::default().into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let table = self.table;
        let keys = self.keys;
        keys.into_iter()
            .map(|key| match store.del(&table, &key) {
                Ok(Some(v)) => v,
                _ => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hexists {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.contains(&self.table, &self.key) {
            Ok(v) => Value::from(v).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmexists {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let table = self.table;
        let keys = self.keys;
        keys.into_iter()
            .map(|key| match store.contains(&table, &key) {
                Ok(v) => v.into(),
                _ => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::hset("t1", "hello", "world".into());
        dispatch(cmd, &store);
        let cmd = CommandRequest::hget("t1", "hello");
        let res = dispatch(cmd, &store);
        assert_res_ok(&res, &["world".into()], &[]);
    }

    #[test]
    fn hget_with_non_exist_key_should_return_404() {
        let store = MemTable::new();
        let cmd = CommandRequest::hget("t1", "hello");
        let res = dispatch(cmd, &store);
        assert_res_err(&res, 404, "Not found");
    }

    #[test]
    fn hmget_should_work() {
        let store = MemTable::new();
        set_key_pairs(
            "t1",
            vec![("hello", "world"), ("foo", "bar"), ("baz", "qux")],
            &store,
        );

        let cmd = CommandRequest::hgetall("t1");
        let res = dispatch(cmd, &store);
        let pairs = &[
            // sorted
            Kvpair::new("baz", "qux".into()),
            Kvpair::new("foo", "bar".into()),
            Kvpair::new("hello", "world".into()),
        ];

        assert_res_ok(&res, &[], pairs);
    }

    #[test]
    fn hgetall_should_work() {
        let store = MemTable::new();
        set_key_pairs(
            "t1",
            vec![("hello", "world"), ("foo", "bar"), ("baz", "qux")],
            &store,
        );

        let cmd = CommandRequest::hgetall("t1");
        let res = dispatch(cmd, &store);

        let pairs = &[
            // sorted
            Kvpair::new("baz", "qux".into()),
            Kvpair::new("foo", "bar".into()),
            Kvpair::new("hello", "world".into()),
        ];

        assert_res_ok(&res, &[], pairs);
    }

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::hset("t1", "hello", "world".into());
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(&res, &[Value::default()], &[]);

        let res = dispatch(cmd, &store);
        assert_res_ok(&res, &["world".into()], &[]);
    }

    #[test]
    fn hmset_should_work() {
        let store = MemTable::new();
        set_key_pairs("t1", vec![("hello", "world")], &store);

        let pairs = vec![
            Kvpair::new("hello", "rust".into()),
            Kvpair::new("foo", "bar".into()),
        ];
        let cmd = CommandRequest::hmset("t1", pairs);
        let res = dispatch(cmd, &store);
        assert_res_ok(&res, &["world".into(), Value::default()], &[]);
    }

    #[test]
    fn hdel_should_work() {
        let store = MemTable::new();
        set_key_pairs("t1", vec![("hello", "world")], &store);

        let cmd = CommandRequest::hdel("t1", "hello");
        let res = dispatch(cmd, &store);
        assert_res_ok(&res, &["world".into()], &[]);

        let cmd = CommandRequest::hdel("t1", "hello");
        let res = dispatch(cmd, &store);
        assert_res_ok(&res, &[Value::default()], &[]);
    }

    #[test]
    fn hmdel_should_work() {
        let store = MemTable::new();
        // hmdel with exist keys
        set_key_pairs("t1", vec![("hello", "world"), ("foo", "bar")], &store);
        let cmd = CommandRequest::hmdel("t1", vec!["hello".into(), "foo".into()]);
        let res = dispatch(cmd, &store);
        assert_res_ok(&res, &["world".into(), "bar".into()], &[]);

        // hmdel with non-exist keys
        set_key_pairs("t2", vec![("hello", "world"), ("foo", "bar")], &store);
        let cmd = CommandRequest::hmdel("t2", vec!["hello".into(), "zxc".into()]);
        let res = dispatch(cmd, &store);
        assert_res_ok(&res, &["world".into(), Value::default()], &[]);
    }

    #[test]
    fn hexist_should_work() {
        let store = MemTable::new();
        set_key_pairs("t1", vec![("hello", "world")], &store);

        let cmd = CommandRequest::hexist("t1", "hello");
        let res = dispatch(cmd, &store);
        assert_res_ok(&res, &[true.into()], &[]);

        let cmd = CommandRequest::hexist("t1", "foo");
        let res = dispatch(cmd, &store);
        assert_res_ok(&res, &[false.into()], &[])
    }

    #[test]
    fn hmexist_should_work() {
        let store = MemTable::new();
        set_key_pairs("t1", vec![("hello", "world"), ("foo", "bar")], &store);

        let cmd = CommandRequest::hmexist("t1", vec!["hello".into(), "zxc".into()]);
        let res = dispatch(cmd, &store);
        assert_res_ok(&res, &[true.into(), false.into()], &[]);
    }

    // store key-value pairs into a table
    fn set_key_pairs<T: Into<Value>>(table: &str, pairs: Vec<(&str, T)>, store: &impl Storage) {
        pairs
            .into_iter()
            .map(|(k, v)| CommandRequest::hset(table, k, v.into()))
            .for_each(|cmd| {
                dispatch(cmd, store);
            });
    }
}
