use callysto::futures::StreamExt;
use callysto::kafka::cconsumer::CStream;
use callysto::kafka::enums::OffsetReset;
use callysto::prelude::message::*;
use callysto::prelude::*;
use callysto_avro::prelude::types::*;
use callysto_avro::prelude::*;
use serde::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

#[derive(Clone)]
struct SharedState {
    value: Arc<AtomicU32>,
}

impl SharedState {
    fn new() -> Self {
        Self {
            value: Arc::new(AtomicU32::new(0)),
        }
    }
}

async fn avro_raw_stream(mut stream: CStream, ctx: Context<SharedState>) -> Result<()> {
    let raw_schema = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"}
        ]
    }
"#;
    let mut s = AvroDeserializer::create(stream, raw_schema)?
        .deser_raw()
        .await;
    while let Some(Ok(m)) = s.next().await {
        // Read the incoming bytes as JSON value
        let record = m
            .iter()
            .map(|e| e.as_ref().unwrap())
            .collect::<Vec<&Value>>();
        println!("avro_raw_stream: Received record: {:?}", record);
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct Test {
    a: u64,
    b: String,
}
async fn avro_typed_stream(mut stream: CStream, ctx: Context<SharedState>) -> Result<()> {
    let raw_schema = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"}
        ]
    }
"#;
    let mut s = AvroDeserializer::create(stream, raw_schema)?
        .deser::<Test>()
        .await;
    while let Some(Ok(record)) = s.next().await {
        // Read the incoming bytes as JSON value
        println!("avro_typed_stream: Received record: {:?}", record);
    }

    Ok(())
}

fn main() {
    // Throughput: 278.472900390625 MB/sec
    //
    let mut config = Config::default();
    config.kafka_config.auto_offset_reset = OffsetReset::Earliest;

    let mut app = Callysto::with_state(SharedState::new());

    app.with_config(config);
    app.with_name("avro-stream");
    app.agent("avro_raw_stream", app.topic("avro"), avro_raw_stream);
    app.agent(
        "avro_typed_stream",
        app.topic("avrotyped"),
        avro_typed_stream,
    );

    app.run();
}
