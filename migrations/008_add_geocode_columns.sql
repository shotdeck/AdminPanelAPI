-- ============================================================
-- 008: Add coordinates (POINT) to location lookup tables
--      for tiered geocoding (specific → city → region → country)
--      Uses POINT type consistent with frl_images_location.coordinates
-- ============================================================

-- Add coordinates to cities
ALTER TABLE frl.frl_location_cities
    ADD COLUMN IF NOT EXISTS coordinates POINT;

-- Add coordinates to regions
ALTER TABLE frl.frl_location_regions
    ADD COLUMN IF NOT EXISTS coordinates POINT;

-- Add coordinates to countries
ALTER TABLE frl.frl_location_countries
    ADD COLUMN IF NOT EXISTS coordinates POINT;

-- Add coordinates to continents (for fallback)
ALTER TABLE frl.frl_location_continents
    ADD COLUMN IF NOT EXISTS coordinates POINT;

-- Geocode cache for specific_location values
-- (avoids re-geocoding the same location name repeatedly)
CREATE TABLE IF NOT EXISTS frl.frl_geocode_cache (
    id              SERIAL          PRIMARY KEY,
    location_key    TEXT            NOT NULL UNIQUE,
    coordinates     POINT,
    source          VARCHAR(50)     NOT NULL DEFAULT 'nominatim',
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_frl_geocode_cache_key
    ON frl.frl_geocode_cache(location_key);
