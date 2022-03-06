use callysto::prelude::message::*;
use callysto::prelude::*;
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

async fn counter_agent(msg: Option<OwnedMessage>, ctx: Context<SharedState>) -> Result<()> {
    // Read the incoming bytes as string
    msg.map(|m| {
        let strm = m.payload_view::<str>().unwrap().unwrap().to_owned();
        println!("Received payload: `{}`", strm);
    });

    // Increment message counter and print it.
    // Show how you can store a application state.
    let state = ctx.state();
    let msgcount = state.value.fetch_add(1, Ordering::AcqRel);
    println!("Message count: `{}`", msgcount);

    Ok(())
}

fn main() {
    let mut app = Callysto::with_state(SharedState::new());

    app.with_name("basic-app");
    app.agent("counter_agent", app.topic("example"), counter_agent);

    app.run();
}
