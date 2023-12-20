use callysto::errors::CallystoError;
use callysto::futures::StreamExt;
use callysto::prelude::message::*;
use callysto::prelude::postgres::{CPostgresRow, CPostgresSink};
use callysto::prelude::*;
use serde::{Deserialize, Serialize};

/// create table spotify ( id bigserial primary key, song_url text );
/// rpk topic create --brokers localhost:9092 example

#[derive(Serialize, Deserialize)]
pub struct SpotifyDocument {
    song_url: String,
}

impl SpotifyDocument {
    pub fn new(song_url: String) -> Self {
        Self { song_url }
    }
}

async fn postgres_agent(stream: CStream, _ctx: Context<()>) -> Result<()> {
    stream
        .enumerate()
        .map(|(idx, m)| {
            m.map(|e| {
                let doc = SpotifyDocument::new(e.payload_view::<str>().unwrap().unwrap().into());
                println!("Processing document ID: {}", idx);

                let query = "INSERT INTO 
					spotify (song_url)
				    VALUES ($1)";

                CPostgresRow::new(query.into(), vec![doc.song_url])
            })
            .ok_or(CallystoError::GeneralError("No payload".into()))
        })
        .forward(
            CPostgresSink::new("postgres://testuser:testpassword@localhost/testdb", 4, 0).unwrap(),
        )
        .await?;

    Ok(())
}

fn main() {
    let mut app = Callysto::new();

    app.with_name("postgres-sink-app");
    app.agent("postgres-agent", app.topic("example"), postgres_agent);

    app.run();
}
