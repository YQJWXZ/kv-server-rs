use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::CommandResponse;

use super::topic::Broadcaster;

/// Subscription timeout
const SUBSCRIPTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60 * 60); // 1 hour

pub struct Subscription {
    pub sender: mpsc::Sender<Arc<CommandResponse>>,
    pub last_active: std::time::Instant,
}

/// This function is used to clean up the subscriptions that are not used anymore.
pub async fn gc_subscriptions(broadcaster: Arc<Broadcaster>) {
    let mut stale_ids = vec![];
    for entry in broadcaster.subscriptions.iter() {
        let id = *entry.key();
        let sub = entry.value();
        let elapsed = sub.last_active.elapsed();

        if elapsed > SUBSCRIPTION_TIMEOUT {
            // check if the client is still connected
            if sub.sender.is_closed() {
                warn!(
                    "Subscription {} is stale (inactive for {:?}) and client is disconnected",
                    id, elapsed
                );
                stale_ids.push(id);
            } else {
                debug!("Subscription {} is still active", id);
            }
        }
    }

    for id in stale_ids {
        // find topic name
        if let Some(topic_name) = broadcaster.topics.iter().find_map(|entry| {
            if entry.value().contains(&id) {
                Some(entry.key().clone())
            } else {
                None
            }
        }) {
            broadcaster.remove_subscription(topic_name, id);
        }
    }
}
