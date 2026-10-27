-- Run as the database owner after init.sql and migrate_gold.sql, using a direct
-- connection. Set the login password privately in the provider console.
-- This role owns no schemas/tables and receives no CREATE, ALTER or DROP rights.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pipeline_writer') THEN
        CREATE ROLE pipeline_writer LOGIN;
    END IF;
END $$;

-- PostgreSQL databases/public schemas may grant CREATE or TEMP to PUBLIC.
-- Removing these ambient grants also applies to the reader, preserving SELECT.
DO $$ BEGIN
    EXECUTE format('REVOKE CREATE, TEMPORARY ON DATABASE %I FROM PUBLIC', current_database());
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO pipeline_writer', current_database());
END $$;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA gold, ops TO pipeline_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON gold.opportunities TO pipeline_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ops.pipeline_runs TO pipeline_writer;
