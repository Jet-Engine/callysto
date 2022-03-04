#![allow(unused_imports)]
#![allow(dead_code)]

pub mod app;
pub mod config;
pub mod definitions;
pub mod errors;
pub mod kafka;
pub mod service;
pub mod table;

mod stores;

pub use rdkafka;

pub mod prelude {
    pub use super::app::*;
    pub use super::config::*;
    pub use super::definitions::*;
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
