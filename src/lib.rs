//! tantalum: a small, fast, Redis-protocol key/value store.

pub mod cmd;
pub mod context;
mod entry;
mod pages;
pub mod resp;
mod segment;
pub mod server;
pub mod snapshot;
pub mod store;
mod table;
