use crate::definitions::{TaskDef, ServiceDef};
use crate::table::CTable;
use crate::kafka::CTopic;
use lever::sync::atomics::AtomicBox;
use std::sync::Arc;
use lever::prelude::{LOTable, HOPTable};

pub struct Callysto<Store>
where
    Store: 'static
{
    app_name: String,
    storage: Store,
    broker: String,
    tasks: LOTable<String, Arc<dyn TaskDef<Store>>>,
    services: LOTable<String, Arc<dyn ServiceDef<Store>>>,
}

impl Callysto<()> {
    #[must_use]
    pub fn new() -> Self {
        Self::with_storage(())
    }
}

impl Default for Callysto<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Store> Callysto<Store>
where
    Store: Clone + Send + Sync + 'static
{
    pub fn with_storage(storage: Store) -> Self {
        todo!()
    }

    pub fn task(&mut self, t: impl TaskDef<Store>) -> &mut Self {
        todo!()
    }

    pub fn timer(&mut self, t: impl TaskDef<Store>) -> &mut Self {
        todo!()
    }

    pub fn service(&mut self, s: impl ServiceDef<Store>) -> &mut Self {
        todo!()
    }

    pub fn topic(&self) -> CTopic {
        todo!()
    }

    pub fn table(&self) -> CTable {
        todo!()
    }
}