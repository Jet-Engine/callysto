use std::convert::identity;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use apache_avro::{AvroResult, from_value, Reader, Schema};
use apache_avro::types::Value;
use callysto::errors::*;
use callysto::futures::{Stream, TryStreamExt};
use callysto::prelude::CStream;
use callysto::prelude::message::OwnedMessage;
use callysto::rdkafka::Message;
use futures_lite::stream::StreamExt;
use serde::Deserialize;

///
/// Raw value Avro deserializing stream
pub struct AvroValueDeserStream {
    stream: CStream,
    deser: Pin<Box<dyn Stream<Item = Option<Vec<AvroResult<Value>>>>>>
}

impl AvroValueDeserStream {
    pub fn new(stream: CStream, deser: Box<dyn Stream<Item = Option<Vec<AvroResult<Value>>>>>) -> Self {
        Self {
            stream,
            deser: Box::into_pin(deser)
        }
    }

    ///
    /// Give raw [CStream] that this value based deserializer stream is using.
    pub fn raw_stream(mut self) -> CStream {
        self.stream
    }
}

impl Stream for AvroValueDeserStream {
    type Item = Option<Vec<AvroResult<Value>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.deser.poll_next(cx)
    }
}

///
/// User defined value Avro deserializing stream
pub struct AvroDeserStream<T>
where
    T: for<'ud> Deserialize<'ud>
{
    stream: CStream,
    deser: Pin<Box<dyn Stream<Item = Option<Vec<T>>>>>
}

impl<T> AvroDeserStream<T>
where
    T: for<'ud> Deserialize<'ud>
{
    pub fn new(stream: CStream, deser: Box<dyn Stream<Item = Option<Vec<T>>>>) -> Self {
        Self {
            stream,
            deser: Box::into_pin(deser)
        }
    }

    ///
    /// Give raw [CStream] that this value based deserializer stream is using.
    pub fn raw_stream(mut self) -> CStream {
        self.stream
    }
}

impl<T> Stream for AvroDeserStream<T>
where
    T: for<'ud> Deserialize<'ud>
{
    type Item = Option<Vec<T>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.deser.poll_next(cx)
    }
}


///
/// Avro deserializer that takes [Schema] and [CStream].
pub struct AvroDeserializer {
    stream: CStream,
    schema: Schema
}

impl AvroDeserializer {
    pub fn create(mut stream: CStream, schema: &str) -> Result<Self> {
        let schema =
            Schema::parse_str(schema).map_err(|e| CallystoError::GeneralError(e.to_string()))?;
        Ok(AvroDeserializer { stream, schema })
    }

    ///
    /// Deserialize the stream as raw value and restream back as a new stream
    pub async fn deser_raw(mut self) -> AvroValueDeserStream {
        let s = self.stream.clone().map(move |e| {
            match e {
                Some(msg) => {
                    match msg.payload() {
                        Some(p) => {
                            let reader = Reader::with_schema(&self.schema, p).ok()?;
                            Some(reader.into_iter().collect::<Vec<_>>())
                        },
                        _ => None
                    }
                }
                _ => None
            }
        });

        AvroValueDeserStream::new(self.stream, Box::new(s))
    }

    ///
    /// Deserialize the stream into a user-defined struct value and restream back as a new stream
    pub async fn deser<T>(mut self) -> AvroDeserStream<T>
    where
        T: for<'ud> Deserialize<'ud>
    {
        let s = self.stream.clone().map(move |e| {
            match e {
                Some(msg) => {
                    match msg.payload() {
                        Some(p) => {
                            let reader = Reader::with_schema(&self.schema, p).ok()?;
                            let mut elems: Vec<T> = vec![];
                            for elem in reader {
                                let d: T = match elem {
                                    Ok(e) => from_value::<T>(&e),
                                    Err(e) => Err(e)
                                }.ok()?;
                                elems.push(d);
                            }
                            Some(elems)
                        },
                        _ => None
                    }
                }
                _ => None
            }
        });

        AvroDeserStream::new(self.stream, Box::new(s))
    }
}
