-- ============================================================
-- 008: Add latitude/longitude to location lookup tables
--      for tiered geocoding (specific → city → region → country)
-- ============================================================

-- Add coords to cities
ALTER TABLE frl.frl_location_cities
    ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;

-- Add coords to regions
ALTER TABLE frl.frl_location_regions
    ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;

-- Add coords to countries
ALTER TABLE frl.frl_location_countries
    ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;

-- Add coords to continents (for fallback)
ALTER TABLE frl.frl_location_continents
    ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;

-- Geocode cache for specific_location values
-- (avoids re-geocoding the same location name repeatedly)
CREATE TABLE IF NOT EXISTS frl.frl_geocode_cache (
    id              SERIAL          PRIMARY KEY,
    location_key    TEXT            NOT NULL UNIQUE,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    source          VARCHAR(50)     NOT NULL DEFAULT 'nominatim',
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_frl_geocode_cache_key
    ON frl.frl_geocode_cache(location_key);
