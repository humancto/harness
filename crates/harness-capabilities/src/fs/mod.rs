//! Filesystem capabilities (Phase 3.10). Owner-cardinality `fs.list`,
//! `fs.read` (3.10a — ADR-0015), plus `fs.grep` (streaming regex scan)
//! and `fs.search` (sqlite-FTS5 sidecar index) from 3.10-fts
//! (ADR-0016). Path confinement everywhere via `cap-std` `Dir`
//! handles — TOCTOU-free.

pub mod grep;
pub mod index;
pub mod list;
pub mod read;
pub mod scope;
pub mod search;
mod walk;

pub use grep::FsGrepCapability;
pub use list::FsListCapability;
pub use read::FsReadCapability;
pub use scope::{ScopeConfig, ScopeError, ScopeRegistry};
pub use search::FsSearchCapability;
