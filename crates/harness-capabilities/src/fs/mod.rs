//! Filesystem capabilities (Phase 3.10a). Owner-cardinality `fs.list`
//! and `fs.read` over operator-configured scopes. Path confinement via
//! `cap-std` `Dir` handles — TOCTOU-free.
//!
//! 3.10-fts will layer `fs.search` + `fs.grep` on top using sqlite-FTS5.

pub mod list;
pub mod read;
pub mod scope;

pub use list::FsListCapability;
pub use read::FsReadCapability;
pub use scope::{ScopeConfig, ScopeError, ScopeRegistry};
