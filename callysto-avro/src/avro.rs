use apache_avro::types::Value;
use apache_avro::{from_value, AvroResult, Reader, Schema, Writer};
use callysto::app::Callysto;
use callysto::errors::*;
use callysto::futures::{Sink, Stream, TryStreamExt};
use callysto::nuclei;
use callysto::nuclei::Task;
use callysto::prelude::message::OwnedMessage;
use callysto::prelude::producer::FutureRecord;
use callysto::prelude::{CProducer, CStream, ClientConfig};
use callysto::rdkafka::Message;
use crossbeam_channel::Sender;
use cuneiform_fields::prelude::ArchPadding;
use futures_lite::stream::{Map, StreamExt};
use pin_project_lite::pin_project;
use serde::{Deserialize, Serialize};
use std::convert::identity;
use std::future::Future;
use std::io::Cursor;
use std::marker::PhantomData as marker;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use polars::frame::DataFrame;
use polars::io::avro::AvroReader;
use polars::io::SerReader;
use tracing::trace;

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


pin_project! {
    /// Dataframe stream for Avro
    pub struct AvroDFStream
    {
        #[pin]
        stream: CStream,
        schema: Schema
    }
}

impl AvroDFStream
{
    pub fn new(stream: CStream, schema: Schema) -> Self {
        Self {
            stream,
            schema
        }
    }

    ///
    /// Give raw [CStream] that this value based deserializer stream is using.
    pub fn raw_stream(mut self) -> CStream {
        self.stream
    }
}

impl Stream for AvroDFStream {
    type Item = Result<DataFrame>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(message) => match message {
                Some(Some(msg)) => match msg.payload() {
                    Some(data) => {
                        let r = Cursor::new(data);
                        let df = AvroReader::new(r).finish()
                            .map_err(|e| CallystoError::GeneralError(e.to_string()));
                        Poll::Ready(Some(df))
                    },
                    _ => Poll::Ready(None),
                },
                _ => Poll::Ready(None),
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

    /// Deserialize the stream into a Polar's dataframe.
    pub async fn deser_df(mut self) -> AvroDFStream
    {
        AvroDFStream::new(self.stream, self.schema)
    }
}

pin_project! {
    pub struct CAvroSink<T>
    where
        T: Serialize,
        T: Send,
        T: 'static
    {
        tx: ArchPadding<Sender<T>>,
        buffer_size: usize,
        schema: Schema,
        #[pin]
        data_sink: Task<()>
    }
}

impl<T> CAvroSink<T>
where
    T: Serialize + Send + 'static,
{
    pub fn new(
        topic: String,
        schema: String,
        cc: ClientConfig,
        buffer_size: usize,
    ) -> Result<Self> {
        let (tx, rx) = crossbeam_channel::unbounded::<T>();
        let (tx, rx) = (ArchPadding::new(tx), ArchPadding::new(rx));

        let sch =
            Schema::parse_str(&*schema).map_err(|e| CallystoError::GeneralError(e.to_string()))?;
        let schema = sch.clone();
        let data_sink = nuclei::spawn(async move {
            let producer = CProducer::new(cc);
            while let Ok(item) = rx.recv() {
                let mut writer = Writer::new(&sch, Vec::new());
                writer.append_ser(item).unwrap();
                let encoded = writer.into_inner().unwrap();
                producer.send_value(&topic, encoded).await;
                trace!("CAvroSink - Ingestion - Sink received an element.");
            }
        });

        Ok(CAvroSink {
            tx,
            buffer_size,
            schema,
            data_sink,
        })
    }
}

impl<T> Sink<T> for CAvroSink<T>
where
    T: Serialize + Send + 'static,
{
    type Error = CallystoError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.buffer_size == 0 {
            // Bypass buffering
            return Poll::Ready(Ok(()));
        }

        if self.tx.len() >= self.buffer_size {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<()> {
        let mut this = self.project();
        this.tx
            .send(item)
            .map_err(|e| CallystoError::GeneralError(format!("Failed to send to Kafka: `{}`", e)))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.tx.len() > 0 {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.tx.len() > 0 {
            Poll::Pending
        } else {
            // TODO: Drop the task `data_sink`.
            Poll::Ready(Ok(()))
        }
    }
}
