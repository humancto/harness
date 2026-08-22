//! Bounded recursive file walk over a cap-std `Dir` — shared by
//! `fs.grep` (3.10-fts, streaming scan) and the FTS5 index builder
//! (`fs.search`). Confinement invariants match ADR-0015:
//!
//! - resolution happens through the scope's `Dir` handle only;
//! - symlinks are never followed during enumeration (loop risk +
//!   scope-leak) — they are silently skipped;
//! - non-UTF-8 entry names are skipped and counted;
//! - depth and visited-file counts are hard-bounded.

use cap_std::fs::Dir;

/// Hard depth bound for recursive walks. Deeper trees are silently cut
/// off (reported via [`WalkStats::truncated`]).
pub(crate) const WALK_MAX_DEPTH: u32 = 32;

/// Hard bound on regular files visited in one walk.
pub(crate) const WALK_MAX_FILES: u32 = 100_000;

/// NUL-sniff window for binary detection.
pub(crate) const BINARY_SNIFF_BYTES: usize = 8 * 1024;

/// Counters accumulated during one walk.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct WalkStats {
    /// Regular files handed to the callback.
    pub files_visited: u32,
    /// Entries skipped because the name is not valid UTF-8.
    pub skipped_non_utf8: u32,
    /// `true` when the walk stopped early: depth cut-off, file-count
    /// bound, or the callback returned `false`.
    pub truncated: bool,
}

/// One regular file yielded by [`walk_files`].
pub(crate) struct WalkedFile<'a> {
    /// Directory handle containing the file — open via
    /// `parent.open(name)`; cap-std re-confines on that open.
    pub parent: &'a Dir,
    /// File name within `parent`.
    pub name: &'a str,
    /// Path relative to the walk root, `/`-separated.
    pub relative: &'a str,
    /// `fstatat(2)` metadata — symlinks NOT followed.
    pub metadata: &'a cap_std::fs::Metadata,
}

/// Depth-first walk over every regular file under `root`, bounded by
/// [`WALK_MAX_DEPTH`] / [`WALK_MAX_FILES`]. The callback returns
/// `true` to continue, `false` to stop the walk (marks `truncated`).
///
/// Unreadable subtrees / entries are skipped with a `tracing::debug`
/// breadcrumb — a permissions pothole must not kill the whole scan.
pub(crate) fn walk_files<F>(root: &Dir, mut on_file: F) -> WalkStats
where
    F: FnMut(WalkedFile<'_>) -> bool,
{
    let mut stats = WalkStats::default();
    walk_dir(root, "", 1, &mut stats, &mut on_file);
    stats
}

fn walk_dir<F>(dir: &Dir, prefix: &str, depth: u32, stats: &mut WalkStats, on_file: &mut F)
where
    F: FnMut(WalkedFile<'_>) -> bool,
{
    if stats.truncated {
        return;
    }
    let read_dir = match dir.entries() {
        Ok(rd) => rd,
        Err(err) => {
            tracing::debug!(?err, "fs walk: read_dir failed; skipping subtree");
            return;
        }
    };

    // Collect + sort for deterministic visit order (ReadDir order is
    // platform-dependent; determinism keeps grep truncation stable
    // across runs on the same tree).
    let mut entries: Vec<cap_std::fs::DirEntry> = Vec::new();
    for de in read_dir {
        match de {
            Ok(d) => entries.push(d),
            Err(err) => {
                tracing::debug!(?err, "fs walk: dir entry read failed; skipping");
            }
        }
    }
    entries.sort_by_key(cap_std::fs::DirEntry::file_name);

    for de in entries {
        if stats.truncated {
            return;
        }
        let raw_name = de.file_name();
        let Some(name) = raw_name.to_str() else {
            stats.skipped_non_utf8 = stats.skipped_non_utf8.saturating_add(1);
            continue;
        };
        // `DirEntry::metadata` does not follow symlinks (fstatat with
        // AT_SYMLINK_NOFOLLOW semantics) — matches fs.list (ADR-0015 §5).
        let meta = match de.metadata() {
            Ok(m) => m,
            Err(err) => {
                tracing::debug!(?err, name = %name, "fs walk: metadata failed; skipping");
                continue;
            }
        };
        let relative = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}/{name}")
        };

        if meta.is_file() {
            if stats.files_visited >= WALK_MAX_FILES {
                stats.truncated = true;
                return;
            }
            stats.files_visited += 1;
            let keep_going = on_file(WalkedFile {
                parent: dir,
                name,
                relative: &relative,
                metadata: &meta,
            });
            if !keep_going {
                stats.truncated = true;
                return;
            }
        } else if meta.is_dir() {
            if depth >= WALK_MAX_DEPTH {
                // Depth cut-off elides files — surface it.
                stats.truncated = true;
                continue;
            }
            let child = match de.open_dir() {
                Ok(d) => d,
                Err(err) => {
                    tracing::debug!(?err, "fs walk: open child dir failed; skipping");
                    continue;
                }
            };
            walk_dir(&child, &relative, depth + 1, stats, on_file);
        }
        // Symlinks / FIFOs / sockets / devices: skipped entirely.
    }
}

/// Binary sniff: a NUL byte in the first [`BINARY_SNIFF_BYTES`] of the
/// buffer marks the file as binary (git's heuristic).
pub(crate) fn looks_binary(head: &[u8]) -> bool {
    let window = &head[..head.len().min(BINARY_SNIFF_BYTES)];
    window.contains(&0)
}

/// Translate a shell-style glob into an anchored regex over the
/// `/`-separated relative path. Supported: `*` (any run excluding
/// `/`), `**` (any run including `/`), `?` (one char excluding `/`).
/// Everything else is matched literally.
///
/// A glob with no `/` matches against the file *name* (so `*.rs`
/// matches `src/main.rs`); a glob containing `/` matches against the
/// full relative path.
pub(crate) fn glob_to_regex(glob: &str, ignore_case: bool) -> Result<regex::Regex, regex::Error> {
    let mut re = String::with_capacity(glob.len() + 16);
    if ignore_case {
        re.push_str("(?i)");
    }
    re.push('^');
    let mut chars = glob.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '*' => {
                if chars.peek() == Some(&'*') {
                    chars.next();
                    re.push_str(".*");
                } else {
                    re.push_str("[^/]*");
                }
            }
            '?' => re.push_str("[^/]"),
            other => {
                let mut buf = [0u8; 4];
                re.push_str(&regex::escape(other.encode_utf8(&mut buf)));
            }
        }
    }
    re.push('$');
    regex::Regex::new(&re)
}

/// Whether `relative` matches the compiled glob, honoring the
/// path-vs-basename rule documented on [`glob_to_regex`].
pub(crate) fn glob_matches(re: &regex::Regex, glob_has_slash: bool, relative: &str) -> bool {
    if glob_has_slash {
        re.is_match(relative)
    } else {
        let basename = relative.rsplit('/').next().unwrap_or(relative);
        re.is_match(basename)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod unit_tests {
    use super::*;

    #[test]
    fn glob_star_does_not_cross_slash() {
        let re = glob_to_regex("*.rs", false).expect("compile");
        assert!(glob_matches(&re, false, "src/main.rs"));
        assert!(!glob_matches(&re, false, "src/main.rss"));
        let re = glob_to_regex("src/*.rs", false).expect("compile");
        assert!(glob_matches(&re, true, "src/main.rs"));
        assert!(!glob_matches(&re, true, "src/deep/main.rs"));
    }

    #[test]
    fn glob_double_star_crosses_slash() {
        let re = glob_to_regex("src/**/*.rs", false).expect("compile");
        assert!(glob_matches(&re, true, "src/a/b/c.rs"));
        assert!(!glob_matches(&re, true, "other/a/b/c.rs"));
    }

    #[test]
    fn glob_escapes_regex_metachars() {
        let re = glob_to_regex("a+b(1).txt", false).expect("compile");
        assert!(glob_matches(&re, false, "a+b(1).txt"));
        assert!(!glob_matches(&re, false, "aab1.txt"));
    }

    #[test]
    fn binary_sniff_finds_nul_in_window_only() {
        assert!(looks_binary(&[b'a', 0, b'b']));
        assert!(!looks_binary(b"plain text"));
        let mut late_nul = vec![b'x'; BINARY_SNIFF_BYTES];
        late_nul.push(0);
        assert!(!looks_binary(&late_nul), "NUL after window is not sniffed");
    }
}
