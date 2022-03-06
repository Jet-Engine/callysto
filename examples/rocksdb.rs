use callysto::prelude::message::*;
use callysto::prelude::*;

async fn counter_agent(
    msg: Option<OwnedMessage>,
    table: CTable<()>,
    ctx: Context<()>,
) -> Result<()> {
    // Read the incoming bytes as string
    msg.map(|m| {
        let strm = m.payload_view::<str>().unwrap().unwrap().to_owned();
        println!("Received payload: `{}`", strm);
    });

    Ok(())
}

fn main() {
    let mut app = Callysto::default();

    app.with_name("durable-app")
        .with_storage("rocksdb:///home/theo/projects/calstorage");

    app.table_agent(
        app.topic("example"),
        app.table("odd_numbers"),
        counter_agent,
    );

    app.run();
}
