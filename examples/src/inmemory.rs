use callysto::prelude::message::*;
use callysto::prelude::*;

async fn inmemory_agent(
    msg: Option<OwnedMessage>,
    tables: Tables<()>,
    _ctx: Context<()>,
) -> Result<()> {
    // Read the incoming bytes as string
    msg.map(|m| {
        let strm = m.payload_view::<str>().unwrap().unwrap().to_owned();
        println!("Received payload: `{}`", strm);

        // Update tables based on incoming message.
        let num = strm.parse::<usize>().unwrap();
        if num % 2 == 0 {
            let even_numbers = tables.get("even_numbers").unwrap();
            even_numbers.set(num, num, m).unwrap();
        } else {
            let odd_numbers = tables.get("odd_numbers").unwrap();
            odd_numbers.set(num, num, m).unwrap();
        }
    });

    Ok(())
}

fn main() {
    let mut app = Callysto::default();

    // Configure application.
    app.with_name("inmemory-app")
        .with_storage("inmemory://ramdisk");

    // Create all the tables that we need.
    let mut tables = Tables::new();
    tables.insert("odd_numbers".into(), app.table("odd_numbers"));
    tables.insert("even_numbers".into(), app.table("even_numbers"));

    app.table_agent(
        "inmemory_agent",
        app.topic("example"),
        tables,
        inmemory_agent,
    );

    app.run();
}
