use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use dashmap::{DashMap, DashSet};
use tokio::sync::mpsc;
use tracing::{debug, info, instrument, warn};

use crate::{CommandResponse, KvError, Value};

use super::subscribe_gc::Subscription;

/// the max data size of a topic
const BROADCAST_CAPACITY: usize = 128;

/// Next subscription id
static NEXT_ID: AtomicU32 = AtomicU32::new(1);

// get next subscription id
fn get_next_subscription_id() -> u32 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

pub trait Topic: Send + Sync + 'static {
    /// Subscribe to the topic
    fn subscribe(self, name: String) -> mpsc::Receiver<Arc<CommandResponse>>;
    /// Unsubscribe from the topic
    fn unsubscribe(self, name: String, id: u32) -> Result<u32, KvError>;
    /// Publish to the topic
    fn publish(self, name: String, value: Arc<CommandResponse>);
}

#[derive(Default)]
pub struct Broadcaster {
    // all topics
    pub(crate) topics: DashMap<String, DashSet<u32>>,
    // all subscriptions
    pub(crate) subscriptions: DashMap<u32, Subscription>,
}

impl Topic for Arc<Broadcaster> {
    #[instrument(name = "topic_subscribe", skip_all)]
    fn subscribe(self, name: String) -> mpsc::Receiver<Arc<CommandResponse>> {
        let id = {
            let entry = self.topics.entry(name).or_default();
            let id = get_next_subscription_id();
            entry.value().insert(id);
            id
        };

        let (tx, rx) = mpsc::channel(BROADCAST_CAPACITY);
        let v: Value = (id as i64).into();

        let tx1 = tx.clone();
        let self_clone = self.clone();
        tokio::spawn(async move {
            if let Err(_e) = tx1.send(Arc::new(v.into())).await {
                debug!("Subscription {} is disconnected", id);
                self_clone.subscriptions.remove(&id);
                if let Some(topic_name) = self_clone.topics.iter().find_map(|entry| {
                    if entry.value().contains(&id) {
                        Some(entry.key().clone())
                    } else {
                        None
                    }
                }) {
                    if let Some(topic) = self_clone.topics.get_mut(&topic_name) {
                        topic.remove(&id);
                    }
                }
            }
        });

        // add tx to subscription table
        self.subscriptions.insert(
            id,
            Subscription {
                sender: tx,
                last_active: std::time::Instant::now(),
            },
        );
        debug!("Subscription {} is added", id);

        rx
    }

    #[instrument(name = "topic_unsubscribe", skip_all)]
    fn unsubscribe(self, name: String, id: u32) -> Result<u32, KvError> {
        match self.remove_subscription(name, id) {
            Some(id) => Ok(id),
            None => {
                let err = KvError::NotFound(format!("subscription {} not found", id));
                tracing::error!("Unsubscribe error: {:?}", err);
                Err(err)
            }
        }
    }

    #[instrument(name = "topic_publish", skip_all)]
    fn publish(self, name: String, value: Arc<CommandResponse>) {
        tokio::spawn(async move {
            let mut ids = vec![];
            if let Some(topic) = self.topics.get(&name) {
                // copy all subscription id in topic
                let subscriptions = topic.value().clone();
                drop(topic);

                for id in subscriptions.into_iter() {
                    if let Some(mut sub) = self.subscriptions.get_mut(&id) {
                        if let Err(e) = sub.sender.send(value.clone()).await {
                            warn!("Publish to {} failed error: {:?}", id, e);
                            ids.push(id);
                        } else {
                            sub.last_active = std::time::Instant::now();
                        }
                    }
                }
            }

            for id in ids {
                self.subscriptions.remove(&id);
            }
        });
    }
}

impl Broadcaster {
    pub fn remove_subscription(&self, name: String, id: u32) -> Option<u32> {
        if let Some(v) = self.topics.get_mut(&name) {
            v.remove(&id);

            if v.is_empty() {
                info!("Topic: {:?} is deleted", &name);
                drop(v);
                self.topics.remove(&name);
            }
        }

        debug!("SubScription {} is removed!", id);
        self.subscriptions.remove(&id).map(|(id, _)| id)
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::Receiver;

    use crate::assert_res_ok;

    use super::*;

    #[tokio::test]
    async fn pub_sub_should_work() {
        let b = Arc::new(Broadcaster::default());
        let lobby = "lobby".to_string();

        // subscribe
        let mut stream1 = b.clone().subscribe(lobby.clone());
        let mut stream2 = b.clone().subscribe(lobby.clone());

        // publish
        let v: Value = "hello".into();
        b.clone().publish(lobby.clone(), Arc::new(v.clone().into()));

        let id1 = get_id(&mut stream1).await;
        let id2 = get_id(&mut stream2).await;

        assert!(id1 != id2);

        let res1 = stream1.recv().await.unwrap();
        let res2 = stream2.recv().await.unwrap();

        assert_eq!(res1, res2);
        assert_res_ok(&res1, &[v.clone()], &[]);

        // if cancel subscribe, the stream will not receive any more message
        let result = b.clone().unsubscribe(lobby.clone(), id1 as _).unwrap();
        assert_eq!(result, id1);

        // publish
        let v: Value = "world".into();
        b.clone().publish(lobby.clone(), Arc::new(v.clone().into()));
        assert!(stream1.recv().await.is_none());

        let res2 = stream2.recv().await.unwrap();
        assert_res_ok(&res2, &[v.clone()], &[]);
    }
    pub async fn get_id(res: &mut Receiver<Arc<CommandResponse>>) -> u32 {
        let id: i64 = res.recv().await.unwrap().as_ref().try_into().unwrap();
        id as u32
    }
}
