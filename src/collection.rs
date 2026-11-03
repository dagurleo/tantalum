//! Big collections: lists, sets and sorted sets too big to stay packed in
//! their entry, and streams. Their shard keeps them as objects and commands
//! change them in place; the store sees them only through this module.

use crate::list::{self, BigList};
use crate::segment::Kind;
use crate::set::{self, BigSet};
use crate::stream::{self, BigStream};
use crate::zset::{self, BigZSet};

#[derive(Clone)]
pub enum Collection {
    List(BigList),
    Set(BigSet),
    ZSet(BigZSet),
    Stream(BigStream),
}

impl std::fmt::Debug for Collection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "a big {} ({} bytes)",
            self.kind().name(),
            self.allocation_size()
        )
    }
}

impl Collection {
    pub fn kind(&self) -> Kind {
        match self {
            Collection::List(_) => Kind::List,
            Collection::Set(_) => Kind::Set,
            Collection::ZSet(_) => Kind::ZSet,
            Collection::Stream(_) => Kind::Stream,
        }
    }

    /// Roughly what it takes in memory, for `used_memory` and eviction.
    pub fn allocation_size(&self) -> usize {
        match self {
            Collection::List(list) => list.allocation_size(),
            Collection::Set(set) => set.allocation_size(),
            Collection::ZSet(set) => set.allocation_size(),
            Collection::Stream(stream) => stream.allocation_size(),
        }
    }

    /// Its contents in the packed format of its kind (however long), for
    /// snapshots.
    pub fn to_compact(&self) -> Vec<u8> {
        match self {
            Collection::List(list) => list.to_packed(),
            Collection::Set(set) => set.to_compact(),
            Collection::ZSet(set) => set.to_packed(),
            Collection::Stream(stream) => stream.to_compact(),
        }
    }

    /// The big form of packed bytes (`is_valid` ones).
    pub fn from_compact(kind: Kind, bytes: &[u8]) -> Self {
        match kind {
            Kind::List => Collection::List(BigList::from_packed(bytes)),
            Kind::Set => Collection::Set(BigSet::from_compact(bytes)),
            Kind::ZSet => Collection::ZSet(BigZSet::from_packed(bytes)),
            Kind::Stream => {
                Collection::Stream(BigStream::from_compact(bytes).expect("a valid stream"))
            }
            other => unreachable!("{} isn't a collection", other.name()),
        }
    }
}

/// Whether packed bytes of a collection `kind` are within its limits for
/// staying packed; past them it becomes a [`Collection`].
pub fn fits_compact(kind: Kind, bytes: &[u8]) -> bool {
    match kind {
        Kind::List => list::fits_packed(bytes),
        Kind::Set => set::fits_compact(bytes),
        Kind::ZSet => zset::fits_packed(bytes),
        Kind::Stream => false,
        _ => true,
    }
}

/// Whether `bytes` are packed contents of `kind`, for values read from files.
pub fn is_valid(kind: Kind, bytes: &[u8]) -> bool {
    match kind {
        Kind::List => list::is_valid(bytes),
        Kind::Set => set::is_valid(bytes),
        Kind::ZSet => zset::is_valid(bytes),
        Kind::Stream => stream::is_valid(bytes),
        _ => false,
    }
}
