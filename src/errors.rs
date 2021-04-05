use std::result;
use thiserror::Error;

pub type Result<T> = result::Result<T, CallystoError>;


#[derive(Error, Debug)]
pub enum CallystoError {

}