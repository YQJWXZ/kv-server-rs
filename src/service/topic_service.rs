use std::{pin::Pin, sync::Arc};

use futures::{stream, Stream};
use tokio_stream::wrappers::ReceiverStream;

use crate::{CommandResponse, Publish, Subscribe, Unsubscribe};

use super::topic::Topic;

pub type StreamingResponse = Pin<Box<dyn Stream<Item = Arc<CommandResponse>> + Send>>;

pub trait TopicService {
    fn execute(self, topic: impl Topic) -> StreamingResponse;
}

impl TopicService for Subscribe {
    fn execute(self, topic: impl Topic) -> StreamingResponse {
        let rx = topic.subscribe(self.topic);
        Box::pin(ReceiverStream::new(rx))
    }
}

impl TopicService for Unsubscribe {
    fn execute(self, topic: impl Topic) -> StreamingResponse {
        let res = match topic.unsubscribe(self.topic, self.id) {
            Ok(_) => CommandResponse::ok(),
            Err(e) => e.into(),
        };
        Box::pin(stream::once(async { Arc::new(res) }))
    }
}

impl TopicService for Publish {
    fn execute(self, topic: impl Topic) -> StreamingResponse {
        topic.publish(self.topic, Arc::new(self.data.into()));
        Box::pin(stream::once(async { Arc::new(CommandResponse::ok()) }))
    }
}

#[cfg(test)]
mod tests {

    use futures::StreamExt;

    use crate::{
        assert_res_err, assert_res_ok, dispatch_stream, service::topic::Broadcaster, CommandRequest,
    };

    use super::*;

    #[tokio::test]
    async fn dispatch_subscribe_should_work() {
        let topic = Arc::new(Broadcaster::default());
        let cmd = CommandRequest::subscribe("lobby");
        let mut res = dispatch_stream(cmd, topic);
        let id = get_id(&mut res).await;
        assert!(id > 0);
    }

    #[tokio::test]
    async fn dispatch_unsubscribe_random_id_should_error() {
        let topic = Arc::new(Broadcaster::default());
        let cmd = CommandRequest::unsubscribe("lobby", 9527);
        let mut res = dispatch_stream(cmd, topic);
        let data = res.next().await.unwrap();

        assert_res_err(&data, 404, "subscription 9527 not found");
    }

    #[tokio::test]
    async fn dispatch_publish_should_work() {
        let topic = Arc::new(Broadcaster::default());
        let cmd = CommandRequest::publish("lobby", vec!["hello".into()]);
        let mut res = dispatch_stream(cmd, topic);
        let data = res.next().await.unwrap();
        assert_res_ok(&data, &[], &[]);
    }

    pub async fn get_id(res: &mut StreamingResponse) -> u32 {
        let id: i64 = res.next().await.unwrap().as_ref().try_into().unwrap();
        id as u32
    }
}
