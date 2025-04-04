use std::pin::Pin;

use futures::{Stream, StreamExt};

use crate::{CommandResponse, KvError};

pub struct StreamResult {
    pub id: u32,
    #[allow(dead_code)]
    inner: Pin<Box<dyn Stream<Item = Result<CommandResponse, KvError>> + Send>>,
}

impl StreamResult {
    pub async fn new<T>(mut stream: T) -> Result<Self, KvError>
    where
        T: Stream<Item = Result<CommandResponse, KvError>> + Send + Unpin + 'static,
    {
        let id = match stream.next().await {
            Some(Ok(CommandResponse {
                status: 200,
                values: v,
                ..
            })) => {
                if v.is_empty() {
                    return Err(KvError::Internal("Invalid stream".into()));
                }
                let id: i64 = (&v[0]).try_into().unwrap();
                Ok(id as u32)
            }

            _ => Err(KvError::Internal("Invalid stream".into())),
        };

        Ok(StreamResult {
            id: id?,
            inner: Box::pin(stream),
        })
    }
}
