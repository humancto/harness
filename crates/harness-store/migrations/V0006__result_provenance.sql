-- Phase 4.5 — per-node provenance for Federated results.
-- JSON-encoded Vec<NodeContribution> (see harness-core result.rs); NULL
-- for Anyone/Owner results, which have no fan-out to attribute. The
-- coordinator persists it when writing the parent terminal so a restart
-- (or the API/UI) can reconstruct which nodes contributed what.
-- See `.planning/4.5-federated-lifecycle.plan.md` and ADR-0027.

INSERT OR REPLACE INTO harness_meta(key, value) VALUES ('schema_version', '6');

ALTER TABLE task_results ADD COLUMN provenance TEXT;
