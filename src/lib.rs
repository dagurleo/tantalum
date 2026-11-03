//! tantalum: a small, fast, Redis-protocol key/value store.

pub mod cmd;
mod collection;
pub mod context;
mod entry;
mod glob;
mod hash;
mod list;
mod pages;
pub mod resp;
mod segment;
pub mod server;
mod set;
pub mod snapshot;
pub mod store;
mod table;
