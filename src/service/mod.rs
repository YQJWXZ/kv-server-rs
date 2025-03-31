use tracing::debug;

use crate::{
    command_request::RequestData, CommandRequest, CommandResponse, KvError, MemTable, Storage,
};
use std::sync::Arc;

mod command_service;

pub trait CommandService {
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

// event notify(no-mutable)
pub trait Notify<Arg> {
    fn notify(&self, arg: &Arg);
}

// event notify(mutable)
pub trait NotifyMut<Arg> {
    fn notify(&self, arg: &mut Arg);
}

impl<Arg> Notify<Arg> for Vec<fn(&Arg)> {
    #[inline]
    fn notify(&self, arg: &Arg) {
        for f in self {
            f(arg);
        }
    }
}

impl<Arg> NotifyMut<Arg> for Vec<fn(&mut Arg)> {
    #[inline]
    fn notify(&self, arg: &mut Arg) {
        for f in self {
            f(arg);
        }
    }
}

// the service data structure can cross threads,
// and call `execute` to execute the CommandRequest command
// and return CommandResponse
pub struct Service<T = MemTable> {
    inner: Arc<ServiceInner<T>>,
}

impl<T: Storage> Service<T> {
    pub fn execute(&self, cmd: CommandRequest) -> CommandResponse {
        debug!("Got request: {:?}", cmd);
        self.inner.on_received.notify(&cmd);
        let mut res = dispatch(cmd, &self.inner.store);
        debug!("Executed response: {:?}", res);
        self.inner.on_executed.notify(&res);
        self.inner.on_before_send.notify(&mut res);
        if !self.inner.on_before_send.is_empty() {
            debug!("Modified response: {:?}", res);
        };

        res
    }
}
impl<T> Clone for Service<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

// the inner service data structure
pub struct ServiceInner<T> {
    store: T,
    /// event triggered when the server receives CommandRequest
    on_received: Vec<fn(&CommandRequest)>,
    /// event triggered when the server finishes processing CommandRequest
    /// and gets CommandResponse
    on_executed: Vec<fn(&CommandResponse)>,
    /// event triggered before the server sends CommandResponse
    /// Note that this interface provides a &mut CommandResponse
    /// so that the event can modify the CommandResponse as needed before sending
    on_before_send: Vec<fn(&mut CommandResponse)>,
    /// event triggered after the server sends CommandResponse
    on_after_send: Vec<fn()>,
}

impl<T: Storage> ServiceInner<T> {
    pub fn new(store: T) -> Self {
        Self {
            store,
            on_received: Vec::new(),
            on_executed: Vec::new(),
            on_before_send: Vec::new(),
            on_after_send: Vec::new(),
        }
    }

    pub fn on_received(mut self, f: fn(&CommandRequest)) -> Self {
        self.on_received.push(f);
        self
    }

    pub fn on_executed(mut self, f: fn(&CommandResponse)) -> Self {
        self.on_executed.push(f);
        self
    }

    pub fn on_before_send(mut self, f: fn(&mut CommandResponse)) -> Self {
        self.on_before_send.push(f);
        self
    }

    pub fn on_after_send(mut self, f: fn()) -> Self {
        self.on_after_send.push(f);
        self
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
    use http::StatusCode;
    use tracing::info;

    use super::*;
    use std::thread;

    #[test]
    fn service_should_work() {
        let service: Service = ServiceInner::new(MemTable::default()).into();
        let cloned = service.clone();
        let handle = thread::spawn(move || {
            let res = cloned.execute(CommandRequest::hset("t1", "hello", "world".into()));
            assert_res_ok(&res, &[Value::default()], &[]);
        });

        handle.join().unwrap();

        let res = service.execute(CommandRequest::hget("t1", "hello"));
        assert_res_ok(&res, &["world".into()], &[]);
    }

    #[test]
    fn event_registration_should_work() {
        fn rece(cmd: &CommandRequest) {
            info!("Got: {:?}", cmd);
        }

        fn resp(res: &CommandResponse) {
            info!("{:?}", res);
        }

        fn b_send(res: &mut CommandResponse) {
            res.status = StatusCode::CREATED.as_u16() as _;
        }

        fn a_send() {
            info!("Data is sent");
        }

        let service: Service = ServiceInner::new(MemTable::default())
            .on_received(rece)
            .on_executed(resp)
            .on_before_send(b_send)
            .on_after_send(a_send)
            .into();

        let res = service.execute(CommandRequest::hset("t1", "hello", "world".into()));

        assert_eq!(res.status, StatusCode::CREATED.as_u16() as _);
        assert_eq!(res.message, "");
        assert_eq!(res.values, &[Value::default()]);
    }
}
