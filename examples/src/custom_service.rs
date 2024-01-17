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

    let service = CService::new("SayHiEvery5Seconds", service_core, (), vec![]);
    app.service(service);

    app.run();
}
