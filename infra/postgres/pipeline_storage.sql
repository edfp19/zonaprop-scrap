-- Apply as the owner after init.sql and pipeline_writer.sql, on a direct host.
-- Runtime code only performs DML. No new roles or ownership grants.
CREATE TABLE IF NOT EXISTS ops.silver_generations (
    generation_id TEXT PRIMARY KEY,
    prefix TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL DEFAULT 'preparing'
        CHECK (state IN ('preparing', 'active', 'superseded', 'deleted')),
    created_at TIMESTAMPTZ NOT NULL,
    retired_at TIMESTAMPTZ,
    manifest JSONB NOT NULL CHECK (jsonb_typeof(manifest) = 'object'),
    CHECK (state <> 'active' OR retired_at IS NULL),
    CHECK (state <> 'superseded' OR retired_at IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS silver_generations_retirement_idx
    ON ops.silver_generations (state, retired_at);

-- Independent receipts survive object deletion; the maintenance coordinator expires
-- these only after 90 days, with replay already bounded to the retained window.
CREATE TABLE IF NOT EXISTS ops.silver_source_receipts (
    run_id TEXT NOT NULL,
    generation_id TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    manifest JSONB NOT NULL,
    PRIMARY KEY (run_id, generation_id)
);
CREATE INDEX IF NOT EXISTS silver_source_receipts_published_idx
    ON ops.silver_source_receipts (published_at);
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pipeline_writer') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE ON ops.silver_generations TO pipeline_writer;
        GRANT SELECT, INSERT, DELETE ON ops.silver_source_receipts TO pipeline_writer;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION ops.guard_silver_generation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.generation_id IS DISTINCT FROM OLD.generation_id
       OR NEW.prefix IS DISTINCT FROM OLD.prefix
       OR NEW.manifest IS DISTINCT FROM OLD.manifest
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'generation metadata is immutable';
    END IF;
    IF NOT (NEW.state = OLD.state
        OR (OLD.state = 'preparing' AND NEW.state IN ('active', 'deleted'))
        OR (OLD.state = 'active' AND NEW.state = 'superseded')
        OR (OLD.state = 'superseded' AND NEW.state = 'deleted')) THEN
        RAISE EXCEPTION 'invalid generation state transition';
    END IF;
    IF OLD.retired_at IS NOT NULL AND NEW.retired_at IS DISTINCT FROM OLD.retired_at THEN
        RAISE EXCEPTION 'retirement timestamp is immutable';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS guard_silver_generation ON ops.silver_generations;
CREATE TRIGGER guard_silver_generation BEFORE UPDATE ON ops.silver_generations
    FOR EACH ROW EXECUTE FUNCTION ops.guard_silver_generation();
