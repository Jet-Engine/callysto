use super::async_con::Arc as AArc;
use crate::errors::*;
use crate::prelude::{Context, ServiceState};
use crate::types::route::Router;
use crate::types::service::Service;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures_timer::Delay;
use http_types::{Request, Response, StatusCode};
use nuclei::Handle;
use std::future;
use std::future::Future;
use std::net::TcpListener;
use std::process::Output;
use std::sync::Arc;
use tracing::{debug, error, info};

pub struct Web<State>
where
    State: Clone + Send + Sync + 'static,
{
    app_name: String,
    state: State,
    dispatcher: RouteDispatcher<State>,
    dependencies: Vec<Arc<dyn Service<State>>>,
}

impl<State> Web<State>
where
    State: Clone + Send + Sync + 'static,
{
    pub fn new(
        app_name: String,
        state: State,
        routes: Vec<Arc<dyn Router<State>>>,
        dependencies: Vec<Arc<dyn Service<State>>>,
    ) -> Self {
        Self {
            app_name,
            state: state.clone(),
            dispatcher: RouteDispatcher::new(state, routes),
            dependencies,
        }
    }

    /// Listens for incoming connections and serves them.
    async fn listen(&self, listener: Handle<TcpListener>) -> Result<()> {
        // Format the full host address.
        let host = format!("http://{}", listener.get_ref().local_addr()?);
        info!("Listening on {}", host);

        let dispatcher = Arc::new(self.dispatcher.clone());
        loop {
            // Accept the next connection.
            let (stream, _) = listener.accept().await?;

            // Spawn a background task serving this connection.
            let stream = AArc::new(stream);

            // Share the dispatcher between threads.
            let dispatcher = dispatcher.clone();
            nuclei::spawn(async move {
                if let Err(err) = async_h1::accept(stream, |req| dispatcher.serve(req)).await {
                    error!("Connection error: {:#?}", err);
                }
            })
            .detach();
        }
    }
}

#[async_trait]
impl<State> Service<State> for Web<State>
where
    State: Clone + Send + Sync + 'static,
{
    async fn call(&self, st: Context<State>) -> crate::errors::Result<State> {
        todo!()
    }

    async fn start(&self) -> crate::errors::Result<BoxFuture<'_, ()>> {
        let closure = async move {
            nuclei::spawn_blocking(|| {
                nuclei::drive(async {
                    Delay::new(std::time::Duration::from_millis(5)).await;
                })
            })
            .detach();

            for x in &self.dependencies {
                info!("WebServer - {} - Dependencies are starting", self.app_name);
                x.start().await;
            }

            info!(
                "Started WebServer Service - Consumer Group `{}`",
                self.app_name
            );

            'fallback: loop {
                info!("Launched Web Service worker.");
                self.service_state()
                    .await
                    .replace_with(|e| ServiceState::Running);
                'main: loop {
                    if self.stopped().await {
                        break 'main;
                    }

                    match Handle::<TcpListener>::bind("0.0.0.0:3000") {
                        Ok(handle) => {
                            let http = self.listen(handle);
                            http.await;
                        }
                        Err(e) => {
                            error!("WebServer bind failed: {:?}", e);
                            self.crash().await;
                            break 'main;
                        }
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
        todo!()
    }

    async fn state(&self) -> String {
        todo!()
    }

    async fn label(&self) -> String {
        format!("{}@{}", self.app_name, self.shortlabel().await)
    }

    async fn shortlabel(&self) -> String {
        String::from("web")
    }
}

#[derive(Clone)]
struct RouteDispatcher<State>
where
    State: Clone + Send + Sync + 'static,
{
    state: State,
    routes: Vec<Arc<dyn Router<State>>>,
}

impl<State> RouteDispatcher<State>
where
    State: Clone + Send + Sync + 'static,
{
    fn new(state: State, routes: Vec<Arc<dyn Router<State>>>) -> Self {
        Self { state, routes }
    }

    /// Serves a request and returns a response.
    async fn serve(&self, req: Request) -> http_types::Result<Response> {
        debug!("Serving {}", req.url());

        let ctx = Context::new(self.state.clone());
        // TODO (vcq): Ankur we might need to cache this thing or make a fast lookup here.
        // This one is poor man's solution.
        let res = match self
            .routes
            .iter()
            .find(|e| e.get_slug() == req.url().path())
        {
            Some(route) => route.call(req, ctx).await?,
            _ => {
                let mut res = Response::new(StatusCode::NotFound);
                res.insert_header("Content-Type", "text/plain");
                res.set_body("Callysto: Route not found.");
                res
            }
        };

        Ok(res)
    }
}
