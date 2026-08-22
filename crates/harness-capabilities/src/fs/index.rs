//! Per-scope sqlite-FTS5 sidecar index for `fs.search` (3.10-fts).
//!
//! One `SQLite` database per scope at
//! `<index_dir>/<sanitized-scope-id>-<blake3/8>.fts.db` (ADR-0016 §3).
//! The bundled `SQLite` shipped via `rusqlite`'s `bundled` feature is
//! compiled with `SQLITE_ENABLE_FTS5` — no new native dependency.
//!
//! ## Schema (`user_version` = 1)
//!
//! - `files(path PRIMARY KEY, mtime_ns, size_bytes, fts_rowid)` —
//!   incremental-reindex bookkeeping.
//! - `fts` — `fts5(path UNINDEXED, content)`; `rowid` is joined from
//!   `files.fts_rowid`.
//!
//! ## Incremental reindex
//!
//! One walk over the scope (bounded — see `walk.rs`), `mtime_ns` compared
//! against `files`; unchanged files are untouched, changed files get
//! delete+insert, files gone from disk are purged. All inside a single
//! transaction, so a crashed reindex leaves the previous index intact.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use rusqlite::{Connection, OptionalExtension};
use thiserror::Error;

use crate::fs::scope::ScopeConfig;
use crate::fs::walk::{looks_binary, walk_files, BINARY_SNIFF_BYTES};

/// Files larger than this are not indexed (counted in stats).
pub const MAX_INDEX_FILE_BYTES: u64 = 4 * 1024 * 1024;

const PRAGMAS: &str = "
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    PRAGMA temp_store = MEMORY;
    PRAGMA busy_timeout = 5000;
";

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS files (
        path       TEXT PRIMARY KEY,
        mtime_ns   INTEGER NOT NULL,
        size_bytes INTEGER NOT NULL,
        fts_rowid  INTEGER NOT NULL
    ) WITHOUT ROWID;
    CREATE VIRTUAL TABLE IF NOT EXISTS fts USING fts5(
        path UNINDEXED,
        content,
        tokenize = 'unicode61'
    );
    PRAGMA user_version = 1;
";

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum IndexError {
    #[error("index db at {path:?}: {source}")]
    Sqlite {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },

    #[error("create index dir {path:?}: {source}")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// FTS5 MATCH syntax error — surfaced to the caller as
    /// `InvalidInput` with `SQLite`'s diagnostic.
    #[error("fts5 query error: {0}")]
    Query(rusqlite::Error),
}

/// Counters from one (re)index pass.
#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub struct IndexStats {
    /// Files newly added to the index.
    pub added: u32,
    /// Files re-indexed because mtime changed.
    pub updated: u32,
    /// Files removed (deleted from disk since the last pass).
    pub removed: u32,
    /// Files unchanged (mtime match) — content untouched.
    pub unchanged: u32,
    pub skipped_too_large: u32,
    pub skipped_binary: u32,
    /// Walk hit a depth / file-count bound — index may be partial.
    pub walk_truncated: bool,
}

/// One `fs.search` hit.
#[derive(Debug, serde::Serialize)]
pub struct SearchHit {
    /// Relative to the scope root. Never absolute.
    pub path: String,
    /// `-bm25(fts)` — higher is better, best hit first.
    pub score: f64,
    /// `snippet()` context with `[`/`]` around matched terms.
    pub snippet: String,
}

/// Sidecar DB path for a scope: sanitized id + 8 hex chars of
/// blake3(id) to stay collision-free and filesystem-safe for any
/// operator-chosen scope id.
#[must_use]
pub fn index_db_path(index_dir: &Path, scope_id: &str) -> PathBuf {
    let sanitized: String = scope_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let hash = blake3::hash(scope_id.as_bytes());
    let hex = hash.to_hex();
    let short = &hex.as_str()[..8];
    index_dir.join(format!("{sanitized}-{short}.fts.db"))
}

fn open_db(db_path: &Path) -> Result<Connection, IndexError> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| IndexError::CreateDir {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let conn = Connection::open(db_path).map_err(|source| IndexError::Sqlite {
        path: db_path.to_path_buf(),
        source,
    })?;
    conn.execute_batch(PRAGMAS)
        .and_then(|()| conn.execute_batch(SCHEMA))
        .map_err(|source| IndexError::Sqlite {
            path: db_path.to_path_buf(),
            source,
        })?;
    Ok(conn)
}

/// Build or incrementally refresh the index for `scope`. Runs one
/// bounded walk; unchanged files (same `mtime_ns`) are not re-read.
/// The whole pass is one transaction — crash-safe.
pub fn reindex(scope: &ScopeConfig, db_path: &Path) -> Result<IndexStats, IndexError> {
    let mut conn = open_db(db_path)?;
    let wrap = |source: rusqlite::Error| IndexError::Sqlite {
        path: db_path.to_path_buf(),
        source,
    };

    let tx = conn.transaction().map_err(wrap)?;
    let mut stats = IndexStats::default();
    let known = load_known(&tx).map_err(wrap)?;

    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut sql_err: Option<rusqlite::Error> = None;

    let walk_stats = walk_files(&scope.root_dir, |file| {
        match index_one_file(&tx, &known, &file) {
            Ok(outcome) => {
                match outcome {
                    FileOutcome::Added => stats.added = stats.added.saturating_add(1),
                    FileOutcome::Updated => stats.updated = stats.updated.saturating_add(1),
                    FileOutcome::Unchanged => stats.unchanged = stats.unchanged.saturating_add(1),
                    FileOutcome::TooLarge => {
                        stats.skipped_too_large = stats.skipped_too_large.saturating_add(1);
                    }
                    FileOutcome::Binary => {
                        stats.skipped_binary = stats.skipped_binary.saturating_add(1);
                    }
                    FileOutcome::IoSkip => {}
                }
                if matches!(
                    outcome,
                    FileOutcome::Added | FileOutcome::Updated | FileOutcome::Unchanged
                ) {
                    seen.insert(file.relative.to_string());
                }
                true
            }
            Err(e) => {
                sql_err = Some(e);
                false // abort the walk — the transaction will roll back
            }
        }
    });
    if let Some(e) = sql_err {
        return Err(wrap(e));
    }
    stats.walk_truncated = walk_stats.truncated;

    // Purge files that vanished from disk. If the walk was truncated we
    // skip the purge — an elided subtree must not be mistaken for
    // deleted files.
    if !walk_stats.truncated {
        for (path, (_, rowid)) in &known {
            if seen.contains(path) {
                continue;
            }
            tx.execute("DELETE FROM fts WHERE rowid = ?1", [*rowid])
                .map_err(wrap)?;
            tx.execute("DELETE FROM files WHERE path = ?1", [path.as_str()])
                .map_err(wrap)?;
            stats.removed = stats.removed.saturating_add(1);
        }
    }

    tx.commit().map_err(wrap)?;

    let now_secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    scope.mark_indexed(now_secs);
    Ok(stats)
}

/// Bookkeeping snapshot: path → (`mtime_ns`, `fts_rowid`).
fn load_known(
    tx: &rusqlite::Transaction<'_>,
) -> Result<HashMap<String, (i64, i64)>, rusqlite::Error> {
    let mut known = HashMap::new();
    let mut stmt = tx.prepare("SELECT path, mtime_ns, fts_rowid FROM files")?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            (r.get::<_, i64>(1)?, r.get::<_, i64>(2)?),
        ))
    })?;
    for row in rows {
        let (path, v) = row?;
        known.insert(path, v);
    }
    Ok(known)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileOutcome {
    Added,
    Updated,
    Unchanged,
    TooLarge,
    Binary,
    /// Open / read raced away — skipped with a debug breadcrumb; not
    /// marked seen so a previously-indexed version survives the purge.
    IoSkip,
}

/// Index one walked file: mtime-compare against `known`, then (for
/// new/changed files) bounded read + binary sniff + FTS upsert.
fn index_one_file(
    tx: &rusqlite::Transaction<'_>,
    known: &HashMap<String, (i64, i64)>,
    file: &crate::fs::walk::WalkedFile<'_>,
) -> Result<FileOutcome, rusqlite::Error> {
    let size = file.metadata.len();
    if size > MAX_INDEX_FILE_BYTES {
        return Ok(FileOutcome::TooLarge);
    }
    let mtime_ns = mtime_ns_of(file.metadata);
    let existing = known.get(file.relative).copied();
    if let Some((known_mtime, _)) = existing {
        if known_mtime == mtime_ns {
            return Ok(FileOutcome::Unchanged);
        }
    }
    // Changed or new — read (bounded; size checked above, +1 sentinel
    // guards against growth since stat) and sniff.
    let fh = match file.parent.open(file.name) {
        Ok(f) => f,
        Err(err) => {
            tracing::debug!(?err, path = %file.relative, "fs index: open failed; skipping");
            return Ok(FileOutcome::IoSkip);
        }
    };
    let mut buf: Vec<u8> = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
    if let Err(err) = fh.take(MAX_INDEX_FILE_BYTES + 1).read_to_end(&mut buf) {
        tracing::debug!(?err, path = %file.relative, "fs index: read failed; skipping");
        return Ok(FileOutcome::IoSkip);
    }
    if buf.len() as u64 > MAX_INDEX_FILE_BYTES {
        return Ok(FileOutcome::TooLarge);
    }
    if looks_binary(&buf[..buf.len().min(BINARY_SNIFF_BYTES)]) {
        return Ok(FileOutcome::Binary);
    }
    let content = String::from_utf8_lossy(&buf);

    if let Some((_, old_rowid)) = existing {
        tx.execute("DELETE FROM fts WHERE rowid = ?1", [old_rowid])?;
    }
    tx.execute(
        "INSERT INTO fts (path, content) VALUES (?1, ?2)",
        rusqlite::params![file.relative, content.as_ref()],
    )?;
    let rowid = tx.last_insert_rowid();
    tx.execute(
        "INSERT INTO files (path, mtime_ns, size_bytes, fts_rowid)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(path) DO UPDATE
         SET mtime_ns = ?2, size_bytes = ?3, fts_rowid = ?4",
        rusqlite::params![
            file.relative,
            mtime_ns,
            i64::try_from(buf.len()).unwrap_or(i64::MAX),
            rowid
        ],
    )?;
    Ok(if existing.is_some() {
        FileOutcome::Updated
    } else {
        FileOutcome::Added
    })
}

/// Query the index. `query` uses FTS5 MATCH syntax; results are ranked
/// by bm25 (best first). A MATCH syntax error comes back as
/// [`IndexError::Query`].
pub fn query(db_path: &Path, query: &str, limit: u32) -> Result<Vec<SearchHit>, IndexError> {
    let conn = open_db(db_path)?;
    let wrap = |source: rusqlite::Error| IndexError::Sqlite {
        path: db_path.to_path_buf(),
        source,
    };

    let mut stmt = conn
        .prepare(
            "SELECT path, bm25(fts) AS rank,
                    snippet(fts, 1, '[', ']', ' … ', 12)
             FROM fts
             WHERE fts MATCH ?1
             ORDER BY rank
             LIMIT ?2",
        )
        .map_err(wrap)?;

    let rows = stmt.query_map(rusqlite::params![query, limit], |r| {
        Ok(SearchHit {
            path: r.get(0)?,
            score: -r.get::<_, f64>(1)?,
            snippet: r.get(2)?,
        })
    });
    let rows = match rows {
        Ok(rows) => rows,
        // MATCH syntax errors surface at query time.
        Err(e) => return Err(IndexError::Query(e)),
    };
    let mut hits = Vec::new();
    for row in rows {
        hits.push(row.map_err(IndexError::Query)?);
    }
    Ok(hits)
}

/// Number of indexed files, or `None` when the DB doesn't exist yet.
/// Used for lazy first-build detection without creating the file.
pub fn indexed_file_count(db_path: &Path) -> Result<Option<u64>, IndexError> {
    if !db_path.exists() {
        return Ok(None);
    }
    let conn = open_db(db_path)?;
    let wrap = |source: rusqlite::Error| IndexError::Sqlite {
        path: db_path.to_path_buf(),
        source,
    };
    let n: Option<i64> = conn
        .query_row("SELECT COUNT(*) FROM files", [], |r| r.get(0))
        .optional()
        .map_err(wrap)?;
    Ok(n.map(|v| u64::try_from(v).unwrap_or(0)))
}

fn mtime_ns_of(meta: &cap_std::fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|t| {
            t.into_std()
                .duration_since(SystemTime::UNIX_EPOCH)
                .ok()
                .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        })
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod unit_tests {
    use super::*;

    #[test]
    fn fts5_module_is_compiled_into_bundled_sqlite() {
        // The whole 3.10-fts design rests on rusqlite's bundled SQLite
        // shipping FTS5. Prove it at test time, loudly.
        let conn = Connection::open_in_memory().expect("open");
        conn.execute_batch("CREATE VIRTUAL TABLE t USING fts5(body);")
            .expect("FTS5 must be available in bundled sqlite");
    }

    #[test]
    fn index_db_path_sanitizes_and_disambiguates() {
        let dir = Path::new("/idx");
        let a = index_db_path(dir, "my/scope");
        let b = index_db_path(dir, "my_scope");
        // Same sanitized stem, different hash → different files.
        assert_ne!(a, b);
        let name = a.file_name().unwrap().to_str().unwrap();
        assert!(!name.contains('/'));
        assert!(name.ends_with(".fts.db"));
    }
}
