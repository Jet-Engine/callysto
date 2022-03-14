use callysto::prelude::message::*;
use callysto::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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

async fn counter_agent_1(msg: Option<OwnedMessage>, ctx: Context<SharedState>) -> Result<()> {
    // Read the incoming bytes as string
    let m = msg.unwrap();
    let strm = m
        .payload_view::<str>()
        .ok_or(CallystoError::GeneralError("Payload view failure".into()))??;
    // let s = strm?;
    println!("Received payload from Agent 1: `{}`", strm);

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
    // println!("Message count: `{}`", msgcount);

    Ok(())
}

async fn counter_agent_2(msg: Option<OwnedMessage>, ctx: Context<SharedState>) -> Result<()> {
    // Read the incoming bytes as string
    msg.map(|m| {
        let strm = m.payload_view::<str>().unwrap().unwrap().to_owned();
        // println!("Received payload from Agent 2: `{}`", strm);
    });

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
    // println!("Message count: `{}`", msgcount);

    Ok(())
}

fn main() {
    let mut app = Callysto::with_state(SharedState::new());

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
