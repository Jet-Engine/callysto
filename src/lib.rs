pub mod kafka;
pub mod table;
pub mod enums;
pub mod app;
pub mod definitions;
pub mod errors;

pub mod prelude {
    pub use super::kafka::*;
    pub use super::table::*;
    pub use super::enums::*;
    pub use super::app::*;
    pub use super::definitions::*;
    pub use super::errors::*;
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
