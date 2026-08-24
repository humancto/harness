//! Schema migrations. Hand-rolled (no `refinery`) to keep deps minimal —
//! a `_migrations` table tracks applied versions and we apply pending
//! ones in order on every `Store::open`. Idempotent.

use rusqlite::Connection;

use crate::error::StoreError;

/// Each migration: `(version, name, sql)`. Append-only; never edit a
/// shipped migration's bytes — write a new one.
const MIGRATIONS: &[(u32, &str, &str)] = &[
    (
        1,
        "initial_schema",
        include_str!("../migrations/V0001__initial_schema.sql"),
    ),
    (2, "leases", include_str!("../migrations/V0002__leases.sql")),
    (
        3,
        "replica",
        include_str!("../migrations/V0003__replica.sql"),
    ),
    (
        4,
        "task_results",
        include_str!("../migrations/V0004__task_results.sql"),
    ),
    (
        5,
        "assigned_node",
        include_str!("../migrations/V0005__assigned_node.sql"),
    ),
    (
        6,
        "result_provenance",
        include_str!("../migrations/V0006__result_provenance.sql"),
    ),
    (
        7,
        "result_cost",
        include_str!("../migrations/V0007__result_cost.sql"),
    ),
    (
        8,
        "checkpoints",
        include_str!("../migrations/V0008__checkpoints.sql"),
    ),
    (
        9,
        "audit_log",
        include_str!("../migrations/V0009__audit_log.sql"),
    ),
];

pub(crate) fn run(conn: &Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _migrations (
            version    INTEGER PRIMARY KEY NOT NULL,
            name       TEXT    NOT NULL,
            applied_at INTEGER NOT NULL
        ) WITHOUT ROWID;",
    )?;

    let applied: std::collections::HashSet<u32> = conn
        .prepare("SELECT version FROM _migrations")?
        .query_map([], |row| row.get::<_, u32>(0))?
        .collect::<Result<_, _>>()?;

    for (version, name, sql) in MIGRATIONS {
        if applied.contains(version) {
            continue;
        }
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(sql).map_err(|e| StoreError::Migration {
            version: *version,
            reason: e.to_string(),
        })?;
        tx.execute(
            "INSERT INTO _migrations(version, name, applied_at) VALUES (?, ?, strftime('%s','now'))",
            rusqlite::params![version, name],
        )
        .map_err(|e| StoreError::Migration {
            version: *version,
            reason: e.to_string(),
        })?;
        tx.commit()?;
        tracing::info!(target: "harness.store", version, name, "migration applied");
    }

    Ok(())
}
