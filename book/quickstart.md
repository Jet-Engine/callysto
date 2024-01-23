# Quickstart Guide

To include callysto to your project, add this to your `Cargo.toml`.

```toml
callysto = "0.1"
```

## First steps

We initialize the app like below in Callysto:

```rust
use callysto::prelude::*;

fn main() {
    let mut app = Callysto::new();
    app.with_name("first-steps");
    app.run();
}
```

In Callysto, everything is a `Service`, apart from one-off tasks, they belong to `Task` class.
`Service` trait needs to be implemented to implement code that work as a service definition.
Every runnable in Callysto has it's own lifetime. Lifetime of these runnables (tasks, services, agents, etc.) 
embedded to their implementation. So you manage stop, start, restart, crash procedures by yourself.
Callysto is not opinionated on how to write your lifecycle procedures. It rather leaves it to user.


## Customizing Configuration

Configuration can be passed differently, even we use some sane defaults you can still change the configuration as in:
```rust
fn main() {
    let mut config = Config::default();
    config.kafka_config.auto_offset_reset = OffsetReset::Earliest;

    let mut app = Callysto::with_state(SharedState::new());

    app.with_config(config);
    app.with_name("always-read-the-earliest-message");

    app.run();
}

```


## Defining a Service

Services and agents are twofold in Callysto: Stateful and Stateless.
Stateful services are the ones that uses global state whereas stateless ones doesn't care and won't use the global application state.

### Stateful Services
Here we are going to show you how to define stateful services.

Here is a boilerplate for a simple service definition which says hi every 5 seconds and also counts how many times it says "hi":
```rust
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
```

### Stateless/Custom Services

Here we will give an example of stateless services.
Stateless services doesn't share a state at application level, or they don't care about the global application state, rather they communicate with each other via messages.
Same example above with no state baked in:

```rust
use callysto::prelude::*;
use futures_timer::Delay;

async fn service_core(ctx: Context<()>) -> Result<()> {
    println!("Hi from Callysto!");
    Delay::new(std::time::Duration::from_secs(5)).await;
    Ok(())
}

fn main() {
    let mut app = Callysto::new();

    app.with_name("sayhi");

    let service = CService::new(
        "SayHiEvery5Seconds",   // Service name
        service_core,           // Service function
        (),                     // State, as of now, no state.
        vec![]                  // Dependency services which should start before this service starts.
    );
    app.service(service);

    app.run();
}
```

## Running