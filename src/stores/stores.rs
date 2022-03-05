use crate::errors::*;
use crate::kafka::ctopic::CTP;
use crate::service::*;
use async_trait::*;
use rdkafka::message::OwnedMessage;

#[async_trait]
pub trait Store<State>: Service<State>
where
    State: Clone + Send + Sync + 'static,
{
    fn persisted_offset(&self, tp: CTP) -> Result<Option<usize>>;

    fn set_persisted_offset(&self, tp: CTP, offset: usize) -> Result<()>;

    fn apply_changelog_batch(&self, events: Vec<OwnedMessage>) -> Result<()>;

    fn reset_state(&self) -> Result<()>;

    async fn on_rebalance(
        &self,
        assigned: Vec<CTP>,
        revoked: Vec<CTP>,
        newly_assigned: Vec<CTP>,
        generation_id: usize,
    ) -> Result<()>;

    async fn on_recovery_completed(
        &self,
        active_tps: Vec<CTP>,
        standby_tps: Vec<CTP>,
    ) -> Result<()>;
}
