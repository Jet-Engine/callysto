#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_must_use)]
#![allow(unused)]

pub mod app;
pub mod config;
pub mod errors;
pub mod kafka;
pub mod table;
pub mod types;

mod runtime;
mod stores;

pub use rdkafka;

pub mod prelude {
    pub use super::app::*;
    pub use super::config::*;
    pub use super::errors::*;
    pub use super::kafka::*;
    pub use super::rdkafka::*;
    pub use super::table::*;
    pub use crate::types::prelude::*;
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
