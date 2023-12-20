use callysto::prelude::*;

async fn check_liveness(_ctx: Context<()>) -> Result<()> {
    println!("Deckard Cain is identifying you.");

    Ok(())
}

fn main() {
    let mut app = Callysto::new();

    app.with_name("timer-example");
    app.timer(1.5_f64, check_liveness);

    app.run();
}
