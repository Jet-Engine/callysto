use crate::service::*;

pub trait Storage<State>: Service<State>
where
    State: Clone + Send + Sync + 'static
{

}