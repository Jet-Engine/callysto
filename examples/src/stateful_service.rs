use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use callysto::prelude::*;
use futures_timer::Delay;

#[derive(Clone)]
struct SharedState {
    pub value: Arc<AtomicU32>,
}

impl SharedState {
    fn new() -> Self {
        Self {
            value: Arc::new(AtomicU32::default()),
        }
    }
}

async fn say_hi_every5_seconds(ctx: Context<SharedState>) -> Result<SharedState> {
    let state = ctx.state().to_owned();
    println!("{} - Hi from Callysto!", state.value.fetch_add(1, Ordering::SeqCst));
    Delay::new(std::time::Duration::from_secs(5)).await;
    Ok(state)
}

fn main() {
    let mut app = Callysto::with_state(SharedState::new());
    app.with_name("sayhi");
    app.stateful_service("SayHiEvery5Seconds", say_hi_every5_seconds, vec![]);

    app.run();
}