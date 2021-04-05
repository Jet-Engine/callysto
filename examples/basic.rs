use callysto::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[derive(Clone)]
struct State {
    value: Arc<AtomicU32>,
}

impl State {
    fn new() -> Self {
        Self {
            value: Arc::new(AtomicU32::new(0)),
        }
    }
}

fn main() {
    let app = Callysto::with_storage(State::new())
        .with_name("basic-app");
}