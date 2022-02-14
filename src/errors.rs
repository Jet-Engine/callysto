use std::result;
use thiserror::Error;

pub type Result<T> = result::Result<T, CallystoError>;


#[derive(Error, Debug)]
pub enum CallystoError {
    #[error("Failed to parse the configuration from env: `{0}`")]
    InvalidConfig(String),
}