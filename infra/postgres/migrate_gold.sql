-- Run once as the database owner; safe to repeat on current or phase-1 volumes.
BEGIN;
ALTER TABLE gold.opportunities
    ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS photo_url TEXT,
    ADD COLUMN IF NOT EXISTS geolocation_visibility TEXT;

DO $$
DECLARE old_key TEXT;
BEGIN
    SELECT conname INTO old_key FROM pg_constraint
    WHERE conrelid = 'gold.opportunities'::regclass
      AND contype = 'p' AND cardinality(conkey) = 1;
    IF old_key IS NOT NULL THEN
        EXECUTE format('ALTER TABLE gold.opportunities DROP CONSTRAINT %I', old_key);
        ALTER TABLE gold.opportunities ADD PRIMARY KEY (listing_id, operation);
    END IF;
END $$;
COMMIT;
