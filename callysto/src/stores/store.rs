use crate::errors::*;
use crate::kafka::ctopic::CTP;
use crate::types::service::Service;
use crate::types::table::CTable;
use async_trait::*;
use rdkafka::message::OwnedMessage;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

#[async_trait]
pub trait Store<State>: Service<State>
where
    State: Clone + Send + Sync + 'static,
{
    fn get(&self, serialized_key: Vec<u8>, msg: OwnedMessage) -> Result<Option<Vec<u8>>>;

    fn set(
        &self,
        serialized_key: Vec<u8>,
        serialized_val: Vec<u8>,
        msg: OwnedMessage,
    ) -> Result<()>;

    fn del(&self, serialized_key: Vec<u8>, msg: OwnedMessage) -> Result<()>;

    fn table(&self) -> CTable<State>;

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

    #[allow(clippy::wrong_self_convention)]
    fn into_service(&self) -> &dyn Service<State>;
}
