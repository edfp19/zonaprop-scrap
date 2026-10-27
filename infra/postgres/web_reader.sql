-- Run as the database owner, after init.sql, in the intended database.
-- Set this role's password privately in Neon; never commit it to this file.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'web_reader') THEN
        CREATE ROLE web_reader LOGIN;
    END IF;
END $$;

GRANT USAGE ON SCHEMA gold, ops TO web_reader;
GRANT SELECT ON gold.opportunities, ops.pipeline_runs TO web_reader;
