use apache_avro::types::Value;
use apache_avro::{from_value, AvroResult, Reader, Schema};
use callysto::errors::*;
use callysto::futures::{Stream, TryStreamExt};
use callysto::prelude::message::OwnedMessage;
use callysto::prelude::CStream;
use callysto::rdkafka::Message;
use futures_lite::stream::{Map, StreamExt};
use pin_project_lite::pin_project;
use serde::Deserialize;
use std::convert::identity;
use std::future::Future;
use std::marker::PhantomData as marker;
use std::pin::Pin;
use std::task::{Context, Poll};

///
/// Raw value Avro deserializing stream
pin_project! {
    pub struct AvroValueDeserStream {
        #[pin]
        stream: CStream,
        schema: Schema
    }
}

impl AvroValueDeserStream {
    pub fn new(stream: CStream, schema: Schema) -> Self {
        Self { stream, schema }
    }

    ///
    /// Give raw [CStream] that this value based deserializer stream is using.
    pub fn raw_stream(mut self) -> CStream {
        self.stream
    }
}

impl Stream for AvroValueDeserStream {
    type Item = Result<Vec<AvroResult<Value>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(message) => match message {
                Some(Some(msg)) => match msg.payload() {
                    Some(p) => match Reader::with_schema(&this.schema, p) {
                        Ok(reader) => Poll::Ready(Some(Ok(reader
                            .into_iter()
                            .collect::<Vec<AvroResult<Value>>>()))),
                        Err(e) => {
                            Poll::Ready(Some(Err(CallystoError::GeneralError(e.to_string()))))
                        }
                    },
                    _ => Poll::Ready(Some(Ok(vec![]))),
                },
                _ => Poll::Ready(Some(Ok(vec![]))),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

///
/// User defined value Avro deserializing stream
pin_project! {
    pub struct AvroDeserStream<T>
    {
        #[pin]
        stream: CStream,
        schema: Schema,
        _marker: marker<T>
    }
}

impl<T> AvroDeserStream<T>
where
    T: for<'ud> Deserialize<'ud>,
{
    pub fn new(stream: CStream, schema: Schema) -> Self {
        Self {
            stream,
            schema,
            _marker: marker,
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
    T: for<'ud> Deserialize<'ud>,
{
    type Item = Result<Vec<T>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(message) => match message {
                Some(Some(msg)) => match msg.payload() {
                    Some(p) => match Reader::with_schema(&this.schema, p) {
                        Ok(reader) => {
                            let mut elems: Vec<T> = vec![];
                            for elem in reader {
                                let el = match elem {
                                    Ok(e) => from_value::<T>(&e),
                                    Err(e) => Err(e),
                                };
                                match el {
                                    Ok(d) => elems.push(d),
                                    Err(e) => {
                                        return Poll::Ready(Some(Err(CallystoError::GeneralError(
                                            e.to_string(),
                                        ))))
                                    }
                                }
                            }
                            Poll::Ready(Some(Ok(elems)))
                        }
                        Err(e) => {
                            Poll::Ready(Some(Err(CallystoError::GeneralError(e.to_string()))))
                        }
                    },
                    _ => Poll::Ready(Some(Ok(vec![]))),
                },
                _ => Poll::Ready(Some(Ok(vec![]))),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

///
/// Avro deserializer that takes [Schema] and [CStream].
pub struct AvroDeserializer {
    stream: CStream,
    schema: Schema,
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
        AvroValueDeserStream::new(self.stream, self.schema)
    }

    ///
    /// Deserialize the stream into a user-defined struct value and restream back as a new stream
    pub async fn deser<T>(mut self) -> AvroDeserStream<T>
    where
        T: for<'ud> Deserialize<'ud>,
    {
        AvroDeserStream::new(self.stream, self.schema)
    }
}
