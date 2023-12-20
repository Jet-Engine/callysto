use callysto::futures::StreamExt;
use callysto::prelude::*;
use callysto::sinks::elasticsearch::{CElasticSearchDocument, CElasticSearchSink};
use futures::SinkExt;
use serde::*;

// NOTE: This example requires `tokio` feature to be enabled.

#[derive(Serialize, Deserialize)]
pub struct SpotifyDocument {
    song_url: String,
}

impl SpotifyDocument {
    pub fn new(song_url: String) -> Self {
        Self { song_url }
    }
}

async fn elastic_agent(mut stream: CStream, ctx: Context<()>) -> Result<()> {
    stream
        .enumerate()
        .map(|(idx, m)| {
            m.map(|e| {
                let doc = SpotifyDocument::new(e.payload_view::<str>().unwrap().unwrap().into());
                println!("Processing document ID: {}", idx);
                CElasticSearchDocument::new(format!("{}", idx), doc)
            })
            .ok_or(CallystoError::GeneralError("No payload".into()))
        })
        .forward(
            CElasticSearchSink::new("callysto".into(), "http://0.0.0.0:9200".into(), 0).unwrap(),
        )
        .await;

    Ok(())
}

fn main() {
    let mut app = Callysto::new();

    app.with_name("elasticsearch-app");
    app.agent("elastic-agent", app.topic("elastic-input"), elastic_agent);

    app.run();
}
