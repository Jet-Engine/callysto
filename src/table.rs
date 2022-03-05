use crate::errors::*;
use crate::kafka::ctopic::{CTopic, CTP};
use crate::stores::store::Store;
use async_trait::async_trait;

pub struct CTable {}

#[async_trait]
pub trait Collection<State>: Store<State>
where
    State: Clone + Send + Sync + 'static,
{
    /// Get changelog topic
    fn changelog_topic(&self) -> CTopic;

    fn set_changelog_topic(&self, changelog_topic: CTopic);

    fn changelog_topic_name(&self) -> String;

    fn send_changelog(&self, partition: usize, key: &[u8], value: &[u8]) -> Result<()>;
}
