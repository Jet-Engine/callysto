use callysto::prelude::message::*;
use callysto::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

fn main() {
    let mut app = Callysto::new();
    app.with_name("web-app");

    app.run();
}
