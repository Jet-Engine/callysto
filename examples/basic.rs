use async_trait::async_trait;
use callysto::prelude::*;
use callysto::prelude::message::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[derive(Clone)]
struct SharedState {
    value: Arc<AtomicU32>,
}

impl SharedState {
    fn new() -> Self {
        Self {
            value: Arc::new(AtomicU32::new(0)),
        }
    }
}

// pub struct CounterAgent;

// #[async_trait]
// impl Agent<State> for CounterAgent {
//     async fn process(&self, m: st: Context<State>) -> Result<State> {
//         todo!()
//     }
// }


async fn counter_agent(msg: Option<OwnedMessage>, ctx: Context<SharedState>) -> Result<()> {
    todo!()
}

fn main() {
    let mut app = Callysto::with_storage(SharedState::new());

    app
        .with_name("basic-app");

    // with topic specified
    // let topic = app.topic("example");
    app
        .agent(app.topic("example"), counter_agent);

    app.run();
}