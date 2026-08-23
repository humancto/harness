-- 5.9 (ADR-0037) — actual dollars on the result row. Written by the
-- gated sites (local executor + issuer-side remote-result ingest)
-- only for capabilities whose LOCAL manifest is CostHint::CloudPaid;
-- NULL everywhere else. Local derived data — never gossiped (the
-- replica stream carries ReplicatedTaskState only). NOTE: the dead
-- tasks.cost_usd/cost_tokens_in/cost_tokens_out columns from V0001
-- remain unwritten (ADR-0037 records them as legacy).

INSERT OR REPLACE INTO harness_meta(key, value) VALUES ('schema_version', '7');

ALTER TABLE task_results ADD COLUMN cost_usd REAL;
