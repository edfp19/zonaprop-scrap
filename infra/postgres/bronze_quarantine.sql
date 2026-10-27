-- Apply as owner after init.sql. Runtime uses DML under storage_lock only.
CREATE TABLE IF NOT EXISTS ops.bronze_quarantine (
    run_id TEXT PRIMARY KEY,
    reserved_bytes BIGINT NOT NULL CHECK (reserved_bytes > 0),
    state TEXT NOT NULL DEFAULT 'reserved'
        CHECK (state IN ('reserved', 'preserved', 'failed', 'promoted', 'deleted')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    manifest JSONB CHECK (jsonb_typeof(manifest) = 'object'),
    prefix TEXT UNIQUE,
    CHECK ((manifest IS NULL) = (prefix IS NULL)),
    CHECK (state NOT IN ('preserved', 'promoted', 'deleted') OR manifest IS NOT NULL)
);
-- Local Compose has no pipeline_writer role; do not create one implicitly.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pipeline_writer') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE ON ops.bronze_quarantine TO pipeline_writer;
    END IF;
END $$;
