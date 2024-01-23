use callysto::errors::CallystoError;
use callysto::futures::StreamExt;
use callysto::prelude::message::*;
use callysto::prelude::*;
use callysto_avro::prelude::*;
use serde::{Deserialize, Serialize};

/// rpk topic create --brokers localhost:9092 example

#[derive(Clone)]
struct State {
    pub cc: Option<ClientConfig>,
}

#[derive(Serialize, Deserialize)]
pub struct SpotifyDocument {
    song_url: String,
}

impl SpotifyDocument {
    pub fn new(song_url: String) -> Self {
        Self { song_url }
    }
}

async fn avro_sink(stream: CStream, ctx: Context<State>) -> Result<()> {
    let raw_schema = r#"
    {
        "type": "record",
        "name": "spotify_document",
        "fields": [
            {"name": "song_url", "type": "string"}
        ]
    }
"#;

    let state = ctx.state();
    let Some(ref cc) = state.cc else { todo!() };

    stream
        .enumerate()
        .map(|(idx, m)| {
            m.map(|e| {
                println!("Producing spotify song...");
                SpotifyDocument::new(
                    "https://open.spotify.com/track/5qdhrNibheeUS7HVSJ1m3T".to_string(),
                )
            })
            .ok_or(CallystoError::GeneralError("No payload".into()))
        })
        .forward(CAvroSink::new("avrosink".into(), raw_schema.into(), cc.clone(), 0).unwrap())
        .await?;

    Ok(())
}

fn main() {
    let mut app = Callysto::with_state(State { cc: None });

    app.set_state(State {
        cc: Some(app.build_client_config()),
    });
    app.with_name("avro-sink-app");
    app.agent("avro-sink", app.topic("avro"), avro_sink);

    app.run();
}
