use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures_lite::{Stream, StreamExt};
use futures_timer::Delay;
use pin_project_lite::pin_project;
use tracing::{error, info};
use crate::prelude::{Agent, CAgent, Context, CStream, CTopic, Service, ServiceState};
use crate::errors::Result as CResult;

// pin_project! {
//     pub struct CFlow<State, S, R, F, Fut>
//     where
//         S: Stream,
//         State: Clone,
//         State: Send,
//         State: Sync,
//         State: 'static,
//         F: Send,
//         F: Sync,
//         F: 'static,
//         F: Fn(S, Context<State>) -> Fut,
//         Fut: Future<Output = CResult<R>>,
//         Fut: Send,
//         Fut: 'static,
//     {
//         #[pin]
//         stream: S,
//         clo: F,
//         app_name: String,
//         flow_name: String,
//         state: State,
//         dependencies: Vec<Arc<dyn Service<State>>>,
//     }
// }

pub struct CFlow<State, S, R, F, Fut>
where
    S: Stream + Send,
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(&S, Context<State>) -> Fut,
    Fut: Future<Output = CResult<R>> + Send + 'static,
{
    stream: S,
    clo: F,
    app_name: String,
    flow_name: String,
    state: State,
    dependencies: Vec<Arc<dyn Service<State>>>,
}


impl<State, S, R, F, Fut> CFlow<State, S, R, F, Fut>
where
    R: 'static,
    S: Stream + Send,
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(&S, Context<State>) -> Fut,
    Fut: Future<Output = CResult<R>> + Send + 'static,
{
    pub fn new(
        stream: S,
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
    S: Stream + Send,
    State: Clone + Send + Sync + 'static,
{
    /// Do work on given stream with state passed in
    async fn call(&self, stream: &S, st: Context<State>) -> CResult<R>;
}

#[async_trait]
impl<State, S, R, F, Fut> Flow<S, R, State> for CFlow<State, S, R, F, Fut>
where
    R: Send + 'static,
    S: Stream + Send + Sync + 'static,
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(&S, Context<State>) -> Fut,
    Fut: Future<Output = CResult<R>> + Send + 'static,
{
    async fn call(&self, stream: &S, req: Context<State>) -> CResult<R> {
        let fut = (self.clo)(stream, req);
        let res = fut.await?;
        Ok(res)
    }
}

#[async_trait]
impl<State, S, R, F, Fut> Service<State> for CFlow<State, S, R, F, Fut>
where
    R: Send + 'static,
    S: Stream + Send + Sync + 'static,
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(&S, Context<State>) -> Fut,
    Fut: Future<Output = CResult<R>> + Send + 'static,
{
    async fn call(&self, st: Context<State>) -> CResult<State> {
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
                    let mut context = Context::new(state);
                    if let Err(e) = Flow::<S, R, State>::call(self, &self.stream, context).await {
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