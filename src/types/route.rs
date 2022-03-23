use async_trait::async_trait;
use super::context::Context;
use std::future::Future;
use http_types::{Request, Response};

pub struct Route<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Request, Context<State>) -> Fut,
    Fut: Future<Output = http_types::Result<Response>> + Send + 'static,
{
    clo: F,
    slug: String,
    state: State,
    app_name: String,
}

impl<State, F, Fut> Route<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Request, Context<State>) -> Fut,
    Fut: Future<Output = http_types::Result<Response>> + Send + 'static,
{
    pub fn new(clo: F, state: State, slug: String, app_name: String) -> Self {
        Self { clo, slug, state, app_name }
    }
}

#[async_trait]
impl<State, F, Fut> Router<State> for Route<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Request, Context<State>) -> Fut,
    Fut: Future<Output = http_types::Result<Response>> + Send + 'static,
{
    async fn call(&self, request: Request, ctx: Context<State>) -> http_types::Result<Response> {
        let fut = (self.clo)(request, ctx);
        let res = fut.await?;
        Ok(res.into())
    }

    fn get_slug(&self) -> String {
        self.slug.clone()
    }
}

#[async_trait]
pub trait Router<State>: Send + Sync + 'static
where
    State: Clone + Send + Sync + 'static,
{
    /// Execute the given route
    async fn call(&self, request: Request, st: Context<State>) -> http_types::Result<Response>;

    fn get_slug(&self) -> String;
}