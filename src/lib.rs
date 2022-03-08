#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_must_use)]
#![allow(unused)]

pub mod app;
pub mod types;
pub mod config;
pub mod errors;
pub mod kafka;
pub mod table;

mod runtime;
mod stores;

pub use rdkafka;

pub mod prelude {
    pub use super::app::*;
    pub use super::config::*;
    pub use crate::types::prelude::*;
    pub use super::errors::*;
    pub use super::kafka::*;
    pub use super::rdkafka::*;
    pub use super::table::*;
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
