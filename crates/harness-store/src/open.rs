//! `Store` — the `SQLite` handle. One connection behind a `parking_lot::Mutex`
//! since `SQLite` serializes writers and our workload is single-daemon-per-DB.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::Connection;

use crate::error::StoreError;
use crate::migrations;

const PRAGMAS: &str = "
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    PRAGMA foreign_keys = ON;
    PRAGMA temp_store = MEMORY;
    PRAGMA mmap_size = 268435456;
    PRAGMA busy_timeout = 5000;
";

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub path: PathBuf,
}

impl StoreConfig {
    /// `~/.harness/harness.db`, creating the directory if missing.
    pub fn default_path() -> Result<Self, StoreError> {
        let mut root = dirs::home_dir().ok_or(StoreError::HomeNotFound)?;
        root.push(".harness");
        std::fs::create_dir_all(&root)?;
        root.push("harness.db");
        Ok(Self { path: root })
    }

    #[must_use]
    pub fn at(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_owned(),
        }
    }
}

/// SQLite-backed persistence handle. Cheaply cloneable (`Arc` internal).
#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store").finish_non_exhaustive()
    }
}

impl Store {
    /// Open (creating if needed) the `SQLite` db at `config.path`. Applies
    /// pragmas + pending migrations idempotently.
    ///
    /// # Errors
    /// Surfaces `rusqlite` open errors, IO errors creating the parent
    /// directory, and migration errors with a typed `Migration { ... }`
    /// variant naming the failing migration.
    pub fn open(config: &StoreConfig) -> Result<Self, StoreError> {
        if let Some(parent) = config.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(&config.path)?;
        // PRAGMAs must run before the first WAL write so the journal mode
        // sticks; rusqlite's `execute_batch` handles a multi-statement
        // string fine.
        conn.execute_batch(PRAGMAS)?;
        migrations::run(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Open at the default path (`~/.harness/harness.db`).
    pub fn open_default() -> Result<Self, StoreError> {
        Self::open(&StoreConfig::default_path()?)
    }

    /// In-memory database — for tests.
    pub fn open_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(PRAGMAS)?;
        migrations::run(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Run a closure with the connection. Holds the mutex for the closure's
    /// lifetime. Sync — callers needing async should `tokio::task::spawn_blocking`.
    pub fn with_conn<R>(
        &self,
        f: impl FnOnce(&Connection) -> Result<R, StoreError>,
    ) -> Result<R, StoreError> {
        let g = self.conn.lock();
        f(&g)
    }

    /// Read the schema version metadata key.
    pub fn schema_version(&self) -> Result<String, StoreError> {
        self.with_conn(|c| {
            let v: String = c.query_row(
                "SELECT value FROM harness_meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )?;
            Ok(v)
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn open_memory_runs_migrations() {
        let s = Store::open_memory().expect("open");
        assert_eq!(s.schema_version().unwrap(), "1");
    }

    #[test]
    fn open_file_creates_db_and_pragmas_persist() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("harness.db");
        let cfg = StoreConfig::at(&path);
        let s = Store::open(&cfg).expect("open");
        assert_eq!(s.schema_version().unwrap(), "1");
        assert!(path.exists());

        // Re-open is a no-op for migrations.
        drop(s);
        let s2 = Store::open(&cfg).expect("re-open");
        assert_eq!(s2.schema_version().unwrap(), "1");
    }

    #[test]
    fn migrations_table_records_applied_version() {
        let s = Store::open_memory().expect("open");
        let count: i64 = s
            .with_conn(|c| Ok(c.query_row("SELECT count(*) FROM _migrations", [], |r| r.get(0))?))
            .expect("count");
        assert_eq!(count, 1);
    }
}
