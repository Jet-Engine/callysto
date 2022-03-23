#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_must_use)]
#![allow(unused)]
// XXX: Reverse trait coercion is needed for layered composition.
#![feature(trait_upcasting)]
#![allow(incomplete_features)]

pub mod app;
pub mod config;
pub mod errors;
pub mod kafka;
pub mod sensors;
pub mod table;
pub mod types;

mod runtime;
mod stores;

pub use rdkafka;
pub use http_types;

pub mod prelude {
    pub use super::app::*;
    pub use super::config::*;
    pub use super::errors::*;
    pub use super::kafka::*;
    pub use super::rdkafka::*;
    pub use super::table::*;
    pub use crate::types::prelude::*;
    pub use super::http_types::{
        Request as CWebRequest,
        Result as CWebResult,
        Response as CWebResponse,
        *
    };
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
