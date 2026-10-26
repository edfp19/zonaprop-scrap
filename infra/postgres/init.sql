CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('EN_PROGRESO', 'EXITOSO', 'FALLIDO')),
    bronze_records INTEGER NOT NULL DEFAULT 0,
    silver_records INTEGER NOT NULL DEFAULT 0,
    gold_records INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS gold.opportunities (
    listing_id TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    property_type TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    area_name TEXT NOT NULL,
    parent_area_name TEXT,
    rooms INTEGER NOT NULL,
    covered_area_m2 DOUBLE PRECISION NOT NULL,
    total_area_m2 DOUBLE PRECISION,
    price_usd DOUBLE PRECISION NOT NULL,
    price_per_m2_usd DOUBLE PRECISION,
    comparable_average_usd DOUBLE PRECISION,
    comparable_median_usd DOUBLE PRECISION,
    comparable_count BIGINT NOT NULL,
    discount_percent DOUBLE PRECISION,
    has_sufficient_data BOOLEAN NOT NULL,
    is_opportunity BOOLEAN NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    run_id TEXT NOT NULL,
    quality_flags TEXT
);

CREATE INDEX IF NOT EXISTS opportunities_filters_idx
    ON gold.opportunities (operation, area_name, rooms, is_opportunity);
CREATE INDEX IF NOT EXISTS opportunities_discount_idx
    ON gold.opportunities (discount_percent DESC NULLS LAST);
