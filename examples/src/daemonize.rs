use callysto::futures::StreamExt;
use callysto::prelude::message::*;
use callysto::prelude::*;
use std::fs::File;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use daemonize::Daemonize;

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

async fn counter_agent(mut stream: CStream, ctx: Context<SharedState>) -> Result<()> {
    while let Some(msg) = stream.next().await {
        // Read the incoming bytes as string
        msg.map(|m| {
            let strm = m.payload_view::<str>().unwrap().unwrap().to_owned();
            println!("Received payload: `{}`", strm);
        });

        // Increment message counter and print it.
        // Show how you can store an application state.
        let state = ctx.state();
        let msgcount = state.value.fetch_add(1, Ordering::AcqRel);
        println!("Message count: `{}`", msgcount);
    }

    Ok(())
}

fn main() {
    let stdout = File::create("/tmp/daemon.out").unwrap();
    let stderr = File::create("/tmp/daemon.err").unwrap();

    let daemonize = Daemonize::new()
        .pid_file("/tmp/test.pid") // Every method except `new` and `start`
        // .chown_pid_file(true) // is optional, see `Daemonize` documentation
        .working_directory("/tmp") // for default behaviour.
        .user("nobody")
        .group("daemon") // Group name
        .group(2) // or group id.
        .umask(0o777) // Set umask, `0o027` by default.
        .stdout(stdout) // Redirect stdout to `/tmp/daemon.out`.
        .stderr(stderr) // Redirect stderr to `/tmp/daemon.err`.
        .exit_action(|| println!("Executed before master process exits"))
        .privileged_action(|| {
            let mut app = Callysto::with_state(SharedState::new());

            app.with_name("basic-app");
            app.agent("counter_agent", app.topic("example"), counter_agent);

            app.run();
        });

    match daemonize.start() {
        Ok(_) => println!("Success, daemonized"),
        Err(e) => eprintln!("Error, {}", e),
    }
}
