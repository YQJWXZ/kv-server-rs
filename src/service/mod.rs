use tracing::debug;

use crate::{
    command_request::RequestData, CommandRequest, CommandResponse, KvError, MemTable, Storage,
};
use std::sync::Arc;

mod command_service;
pub trait CommandService {
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

// the service data structure can cross threads,
// and call `execute` to execute the CommandRequest command
// and return CommandResponse
pub struct Service<T = MemTable> {
    inner: Arc<ServiceInner<T>>,
}

impl<T> Clone for Service<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: Storage> Service<T> {
    pub fn new(store: T) -> Self {
        Self {
            inner: Arc::new(ServiceInner { store }),
        }
    }
    pub fn execute(&self, cmd: CommandRequest) -> CommandResponse {
        debug!("Got request: {:?}", cmd);
        // TODO: send on_received event
        let res = dispatch(cmd, &self.inner.store);
        debug!("Executed response: {:?}", res);

        // TODO: send on_executed event

        res
    }
}

pub struct ServiceInner<T> {
    store: T,
}

impl<T: Storage> ServiceInner<T> {
    pub fn new(store: T) -> Self {
        Self { store }
    }
}

impl<T: Storage> From<ServiceInner<T>> for Service<T> {
    fn from(inner: ServiceInner<T>) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}
// This function is used to dispatch the command to the corresponding service
pub fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
    match cmd.request_data {
        Some(RequestData::Hget(param)) => param.execute(store),
        Some(RequestData::Hmget(param)) => param.execute(store),
        Some(RequestData::Hgetall(param)) => param.execute(store),
        Some(RequestData::Hset(param)) => param.execute(store),
        Some(RequestData::Hmset(param)) => param.execute(store),
        Some(RequestData::Hdel(param)) => param.execute(store),
        Some(RequestData::Hmdel(param)) => param.execute(store),
        Some(RequestData::Hexists(param)) => param.execute(store),
        Some(RequestData::Hmexists(param)) => param.execute(store),
        None => KvError::InvalidCommand("Request has no data".into()).into(),
    }
}

#[cfg(test)]
use crate::{Kvpair, Value};

/// this function is used to assert the response is ok
/// the function has implemented the sorting for pairs
#[cfg(test)]
pub fn assert_res_ok(res: &CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    let mut sorted_pairs = res.pairs.clone();
    sorted_pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(sorted_pairs, pairs);
}

// this function is used to assert the response is error
#[cfg(test)]
pub fn assert_res_err(res: &CommandResponse, code: u32, msg: &str) {
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, &[]);
    assert_eq!(res.pairs, &[]);
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn service_should_work() {
        let service: Service = ServiceInner::new(MemTable::default()).into();
        let cloned = service.clone();
        tokio::spawn(async move {
            let res = cloned.execute(CommandRequest::hset("t1", "hello", "world".into()));
            assert_res_ok(&res, &[Value::default()], &[]);
        })
        .await
        .unwrap();
    }
}
