use callysto::futures::StreamExt;
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
            value: Arc::new(AtomicU32::default()),
        }
    }
}

async fn manual_commit_counter_agent(mut stream: CStream, ctx: Context<SharedState>) -> Result<()> {
    while let Some(Some(m)) = stream.next().await {
        // Read the incoming bytes as string
        let strm = m.payload_view::<str>().unwrap().unwrap().to_owned();
        println!("Received payload: `{}`", strm);

        // Increment message counter and print it.
        // Show how you can store an application state.
        let state = ctx.state();
        let msgcount = state.value.fetch_add(1, Ordering::AcqRel);
        println!("Message count: `{}`", msgcount);

        // We are done here.
        ctx.commit_async(m).await?;
    }

    Ok(())
}

fn main() {
    let mut app = Callysto::with_state(SharedState::new());

    let mut config = Config::default();
    config.kafka_config.enable_auto_commit = false;

    app.with_name("manual-commit").with_config(config);
    app.agent(
        "manual_commit_counter_agent",
        app.topic("example"),
        manual_commit_counter_agent,
    );

    app.run();
}
