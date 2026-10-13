//! Protocol buffer serialization utilities for history requests and responses.

mod history;

pub use history::{decode_response, encode_request};
