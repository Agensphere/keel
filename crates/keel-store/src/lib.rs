//! Event store implementations behind [`keel_core::EventStore`].

pub mod memory;
#[cfg(feature = "postgres")]
pub mod postgres;

pub use memory::MemoryStore;
#[cfg(feature = "postgres")]
pub use postgres::PgStore;
