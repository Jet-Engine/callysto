use callysto::futures::StreamExt;
use callysto::kafka::cconsumer::CStream;
use callysto::kafka::enums::OffsetReset;
use callysto::prelude::message::*;
use callysto::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

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

async fn counter_agent_1(mut stream: CStream, ctx: Context<SharedState>) -> Result<()> {
    while let Some(Some(m)) = stream.next().await {
        // Read the incoming bytes as string
        let _strm = m
            .payload_view::<str>()
            .ok_or(CallystoError::GeneralError("Payload view failure".into()))??;
        info!("Received payload on counter_agent_1");

        // Increment message counter and print it.
        // Show how you can store a application state.
        let state = ctx.state();
        let msgcount = state.value.fetch_add(1, Ordering::AcqRel);
        if msgcount == 0 {
            let now = SystemTime::now();
            let du = now.duration_since(UNIX_EPOCH).unwrap();
            println!("Start time in millis: {}", du.as_millis());
        }
        if msgcount == 199_999 {
            let now = SystemTime::now();
            let du = now.duration_since(UNIX_EPOCH).unwrap();
            println!("End time in millis: {}", du.as_millis());
        }
    }

    Ok(())
}

async fn counter_agent_2(mut stream: CStream, ctx: Context<SharedState>) -> Result<()> {
    while let Some(Some(m)) = stream.next().await {
        // Read the incoming bytes as string
        let _strm = m
            .payload_view::<str>()
            .ok_or(CallystoError::GeneralError("Payload view failure".into()))??;
        info!("Received payload on counter_agent_2");

        // Increment message counter and print it.
        // Show how you can store a application state.
        let state = ctx.state();
        let msgcount = state.value.fetch_add(1, Ordering::AcqRel);
        if msgcount == 0 {
            let now = SystemTime::now();
            let du = now.duration_since(UNIX_EPOCH).unwrap();
            println!("Start time in millis: {}", du.as_millis());
        }
        if msgcount == 199_999 {
            let now = SystemTime::now();
            let du = now.duration_since(UNIX_EPOCH).unwrap();
            println!("End time in millis: {}", du.as_millis());
        }
    }

    Ok(())
}

fn main() {
    // Throughput: 278.472900390625 MB/sec
    //
    let mut config = Config::default();
    config.kafka_config.auto_offset_reset = OffsetReset::Earliest;

    let mut app = Callysto::with_state(SharedState::new());

    app.with_config(config);
    app.with_name("double-agent");
    app.agent(
        "counter_agent_1",
        app.topic("double-agent-1"),
        counter_agent_1,
    )
    .agent(
        "counter_agent_2",
        app.topic("double-agent-2"),
        counter_agent_2,
    );

    app.run();
}
