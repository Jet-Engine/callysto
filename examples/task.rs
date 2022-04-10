use callysto::prelude::*;

async fn on_start(_ctx: Context<()>) -> Result<()> {
    println!("Started Task Example.");

    Ok(())
}

fn main() {
    let mut app = Callysto::new();

    app.with_name("task-example");
    app.task(on_start);

    app.run();
}
