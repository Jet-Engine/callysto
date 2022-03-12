use callysto::prelude::message::*;
use callysto::prelude::*;
use std;
use std::fs::File;
use std::io::BufRead;
use std::path::Path;

async fn durable_agent(
    msg: Option<OwnedMessage>,
    tables: Tables<()>,
    _ctx: Context<()>,
) -> Result<()> {
    // Read the incoming bytes as string
    msg.map(|m| {
        let strm = m.payload_view::<str>().unwrap().unwrap().to_owned();
        println!("Received payload: `{}`", strm);
        let num = strm.parse::<usize>().unwrap();
        if num % 2 == 0 {
            let even_numbers = tables.get("even_numbers").unwrap();
            even_numbers.set(num, num, m).unwrap();

            // Database is created, and started to be filled in.
            let even_numbers_db_log = read_lines(
                "/home/vertexclique/projects/calstorage/durable-app-even_numbers-0.db/LOG",
            )
            .unwrap();
            for l in even_numbers_db_log.take(10) {
                println!("{}", l.unwrap());
            }
        } else {
            let even_numbers = tables.get("odd_numbers").unwrap();
            even_numbers.set(num, num, m).unwrap();

            // Database is created, and started to be filled in.
            let odd_numbers_db_log = read_lines(
                "/home/vertexclique/projects/calstorage/durable-app-odd_numbers-0.db/LOG",
            )
            .unwrap();
            for l in odd_numbers_db_log.take(10) {
                println!("{}", l.unwrap());
            }
        }
    });

    Ok(())
}

fn read_lines<P>(filename: P) -> std::io::Result<std::io::Lines<std::io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(std::io::BufReader::new(file).lines())
}

fn main() {
    let mut app = Callysto::default();
    // Configure application.
    app.with_name("durable-app")
        .with_storage("rocksdb:///home/vertexclique/projects/calstorage");

    // Create all the tables that we need.
    let mut tables = Tables::new();
    tables.insert("odd_numbers".into(), app.table("odd_numbers"));
    tables.insert("even_numbers".into(), app.table("even_numbers"));

    app.table_agent("durable_agent", app.topic("example"), tables, durable_agent);

    app.run();
}
