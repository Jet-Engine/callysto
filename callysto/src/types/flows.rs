use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::stream::FusedStream;
use futures_lite::{Stream, StreamExt};
use futures_timer::Delay;
use pin_project_lite::pin_project;
use tracing::{error, info};
use crate::prelude::{Agent, CAgent, Context as CContext, CStream, CTopic, Service, ServiceState};
use crate::errors::Result as CResult;

pin_project! {
    ///
    /// Flow that wraps the Source stream.
    #[derive(Clone, Debug)]
    #[must_use = "you need to poll streams otherwise it won't work"]
    pub struct CSource<S>
    where
        S: Clone
    {
        #[pin]
        pub stream: S
    }
}

impl<S> CSource<S>
    where
        S: Clone + Stream,
{
    /// Create source stream from the given stream.
    pub fn new(stream: S) -> Self {
        Self { stream }
    }

    /// Get underlying stream from the source stream.
    pub fn underlying(&self) -> S {
        self.stream.clone()
    }
}

impl<S> Stream for CSource<S>
where
    S: Clone + Stream,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        this.stream.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // stream is either empty or infinite
        match self.stream.size_hint() {
            size @ (0, Some(0)) => size,
            (0, _) => (0, None),
            _ => (usize::max_value(), None),
        }
    }
}

impl<S> FusedStream for CSource<S>
where
    S: Clone + Stream,
{
    fn is_terminated(&self) -> bool {
        // stream is either empty or infinite
        if let (0, Some(0)) = self.size_hint() {
            true
        } else {
            false
        }
    }
}


pub struct CFlow<State, S, R, F, Fut>
where
    S: Stream + Clone + Send,
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(CSource<S>, CContext<State>) -> Fut,
    Fut: Future<Output = CResult<R>> + Send + 'static,
{
    stream: CSource<S>,
    clo: F,
    app_name: String,
    flow_name: String,
    state: State,
    dependencies: Vec<Arc<dyn Service<State>>>,
}


impl<State, S, R, F, Fut> CFlow<State, S, R, F, Fut>
where
    R: 'static,
    S: Stream + Clone + Send,
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(CSource<S>, CContext<State>) -> Fut,
    Fut: Future<Output = CResult<R>> + Send + 'static,
{
    pub fn new(
        stream: CSource<S>,
        clo: F,
        app_name: String,
        flow_name: String,
        state: State,
        dependencies: Vec<Arc<dyn Service<State>>>,
    ) -> Self {
        Self {
            stream,
            clo,
            app_name,
            flow_name,
            state,
            dependencies,
        }
    }
}

#[async_trait]
pub trait Flow<S, R, State>: Service<State> + Send + Sync + 'static
where
    R: 'static,
    S: Stream + Clone + Send,
    State: Clone + Send + Sync + 'static,
{
    /// Do work on given stream with state passed in
    async fn call(&self, stream: CSource<S>, st: CContext<State>) -> CResult<R>;
}

#[async_trait]
impl<State, S, R, F, Fut> Flow<S, R, State> for CFlow<State, S, R, F, Fut>
where
    R: Send + 'static,
    S: Stream + Clone + Send + Sync + 'static,
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(CSource<S>, CContext<State>) -> Fut,
    Fut: Future<Output = CResult<R>> + Send + 'static,
{
    async fn call(&self, stream: CSource<S>, req: CContext<State>) -> CResult<R> {
        let fut = (self.clo)(stream, req);
        let res = fut.await?;
        Ok(res)
    }
}

#[async_trait]
impl<State, S, R, F, Fut> Service<State> for CFlow<State, S, R, F, Fut>
where
    R: Send + 'static,
    S: Stream + Clone + Send + Sync + 'static,
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(CSource<S>, CContext<State>) -> Fut,
    Fut: Future<Output = CResult<R>> + Send + 'static,
{
    async fn call(&self, st: CContext<State>) -> CResult<State> {
        Ok(self.state.clone())
    }

    async fn start(&self) -> CResult<BoxFuture<'_, ()>> {
        let state = self.state.clone();
        let closure = async move {
            for x in &self.dependencies {
                info!("CFlow - {} - Dependencies are starting", self.flow_name);
                x.start().await;
            }

            info!("Started CFlow {}", self.flow_name);

            'fallback: loop {
                info!("Launched CFlow worker.");
                self.service_state()
                    .await
                    .replace_with(|e| ServiceState::Running);
                'main: loop {
                    if self.stopped().await {
                        break 'main;
                    }
                    let state = state.clone();
                    let mut context = CContext::new(state);
                    if let Err(e) = Flow::<S, R, State>::call(self, self.stream.clone(), context).await {
                        error!("CFlow failed: {}", e);
                        self.crash().await;
                        break 'main;
                    }
                }

                if self.stopped().await {
                    break 'fallback;
                }
            }
        };

        Ok(closure.boxed())
    }

    async fn wait_until_stopped(&self) {
        while !self.stopped().await {
            Delay::new(Duration::from_millis(10)).await;
        }
    }

    async fn state(&self) -> String {
        todo!()
    }

    async fn label(&self) -> String {
        format!("{}@{}", self.app_name, self.shortlabel().await)
    }

    async fn shortlabel(&self) -> String {
        format!("flow:{}", self.flow_name)
    }
}