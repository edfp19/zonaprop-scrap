CREATE TABLE IF NOT EXISTS ops.pipeline_budget_claims (
    project_id TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('seed', 'ingest', 'maintain', 'gc')),
    compute_cu_hours DOUBLE PRECISION NOT NULL CHECK (compute_cu_hours > 0),
    transfer_bytes BIGINT NOT NULL CHECK (transfer_bytes > 0),
    PRIMARY KEY (project_id, observed_at)
);
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pipeline_writer') THEN
        GRANT SELECT, INSERT, DELETE ON ops.pipeline_budget_claims TO pipeline_writer;
    END IF;
END $$;
