use crate::errors::*;
use crate::kafka::ctopic::*;
use crate::stores::store::Store;
use async_trait::async_trait;
use rdkafka::message::OwnedMessage;

#[async_trait]
pub trait Collection<State>: Store<State>
where
    State: Clone + Send + Sync + 'static,
{
    /// Get changelog topic
    fn changelog_topic(&self) -> CTopic;

    fn set_changelog_topic(&self, changelog_topic: CTopic);

    fn changelog_topic_name(&self) -> String;

    async fn send_changelog(
        &self,
        partition: usize,
        serialized_key: Vec<u8>,
        serialized_value: Vec<u8>,
    ) -> Result<()>;

    fn partition_for_key(&self, key: Vec<u8>, msg: OwnedMessage) -> Result<Option<usize>>;
}
