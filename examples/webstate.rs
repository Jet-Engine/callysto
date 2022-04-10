use callysto::http_types::StatusCode;
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

async fn web_counter(_req: CWebRequest, ctx: Context<SharedState>) -> CWebResult<CWebResponse> {
    let mut res = CWebResponse::new(StatusCode::Ok);
    let state = ctx.state();
    let current = state.value.fetch_add(1, Ordering::AcqRel);
    res.insert_header("Content-Type", "text/plain");
    res.set_body(current.to_string());
    Ok(res)
}

fn main() {
    let mut app = Callysto::with_state(SharedState::new());
    app.with_name("web-state-app");

    app.page("/counter", web_counter);

    app.run();
}
