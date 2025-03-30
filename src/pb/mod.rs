use abi::{
    command_request::RequestData, value, CommandRequest, CommandResponse, Hdel, Hexists, Hget,
    Hgetall, Hmdel, Hmexists, Hmget, Hmset, Hset, Kvpair, Value,
};
use bytes::Bytes;
use http::StatusCode;
pub mod abi;

impl CommandRequest {
    pub fn new_hget(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hget(Hget {
                table: table.into(),
                key: key.into(),
            })),
        }
    }

    pub fn new_hgetall(table: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hgetall(Hgetall {
                table: table.into(),
            })),
        }
    }

    pub fn new_hmget(table: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hmget(Hmget {
                table: table.into(),
                keys,
            })),
        }
    }

    pub fn new_hset(table: impl Into<String>, key: impl Into<String>, value: Value) -> Self {
        Self {
            request_data: Some(RequestData::Hset(Hset {
                table: table.into(),
                pair: Some(Kvpair::new(key, value)),
            })),
        }
    }

    pub fn new_hmset(table: impl Into<String>, pairs: Vec<Kvpair>) -> Self {
        Self {
            request_data: Some(RequestData::Hmset(Hmset {
                table: table.into(),
                pairs,
            })),
        }
    }

    pub fn new_hdel(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hdel(Hdel {
                table: table.into(),
                key: key.into(),
            })),
        }
    }

    pub fn new_hmdel(table: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hmdel(Hmdel {
                table: table.into(),
                keys,
            })),
        }
    }

    pub fn new_hexist(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hexists(Hexists {
                table: table.into(),
                key: key.into(),
            })),
        }
    }

    pub fn new_hnexists(table: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hmexists(Hmexists {
                table: table.into(),
                keys,
            })),
        }
    }

    // transform to string for error handling
    pub fn format(&self) -> String {
        format!("{:?}", self)
    }
}

impl CommandResponse {
    pub fn ok() -> Self {
        CommandResponse {
            status: StatusCode::OK.as_u16() as _,
            ..Default::default()
        }
    }

    pub fn internal_error() -> Self {
        CommandResponse {
            status: StatusCode::INTERNAL_SERVER_ERROR.as_u16() as _,
            ..Default::default()
        }
    }

    // transform to string for error handling
    pub fn format(&self) -> String {
        format!("{:?}", self)
    }
}

impl Value {
    // transform to string for error handling
    pub fn format(&self) -> String {
        format!("{:?}", self)
    }
}

impl Kvpair {
    pub fn new(key: impl Into<String>, value: Value) -> Self {
        Self {
            key: key.into(),
            value: Some(value),
        }
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self {
            value: Some(value::Value::String(s)),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self {
            value: Some(value::Value::String(s.into())),
        }
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Self {
            value: Some(value::Value::Integer(i)),
        }
    }
}

impl<const N: usize> From<[u8; N]> for Value {
    fn from(buf: [u8; N]) -> Self {
        Bytes::copy_from_slice(&buf[..]).into()
    }
}

impl From<Bytes> for Value {
    fn from(buf: Bytes) -> Self {
        Self {
            value: Some(value::Value::Binary(buf)),
        }
    }
}

impl From<Value> for CommandResponse {
    fn from(v: Value) -> Self {
        CommandResponse {
            status: StatusCode::OK.as_u16() as _,
            values: vec![v],
            ..Default::default()
        }
    }
}
