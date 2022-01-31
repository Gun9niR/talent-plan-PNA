#![deny(missing_docs)]
//! A simple key/value store.

// THe modules are private.
mod kv;
mod error;

// Use `pub use` to re-export the modules
pub use kv::KvStore;
pub use error::{KvsError, Result};
