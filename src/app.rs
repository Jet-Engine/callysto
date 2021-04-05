use crate::definitions::{TaskDef, ServiceDef};
use crate::table::CTable;
use crate::kafka::CTopic;
use lever::sync::atomics::AtomicBox;
use std::sync::Arc;
use lever::prelude::{LOTable, HOPTable};
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Callysto<Store>
where
    Store: 'static
{
    app_name: String,
    storage: Store,
    broker: String,
    stubs: Arc<AtomicUsize>,
    tasks: LOTable<usize, Arc<dyn TaskDef<Store>>>,
    services: LOTable<usize, Arc<dyn ServiceDef<Store>>>,
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
        Self {
            app_name: "callysto-app".to_owned(),
            storage,
            stubs: Arc::new(AtomicUsize::default()),
            broker: "localhost:9092".to_owned(),
            tasks: LOTable::default(),
            services: LOTable::default()
        }
    }

    pub fn name<T: AsRef<str>>(&mut self, name: T) -> &mut Self {
        self.app_name = name.as_ref().to_string();
        self
    }

    pub fn task(&mut self, t: impl TaskDef<Store>) -> &mut Self {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        self.tasks.insert(stub, Arc::new(t));
        self
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