-- ============================================================
-- 005: Add planets lookup table
-- ============================================================

-- 1. Planet lookup table -------------------------------------------

CREATE TABLE IF NOT EXISTS frl.frl_location_planets (
    id          SERIAL      PRIMARY KEY,
    name        VARCHAR(100) NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Add planet_id column to frl_images_location -------------------

ALTER TABLE frl.frl_images_location
    ADD COLUMN IF NOT EXISTS planet_id INTEGER REFERENCES frl.frl_location_planets(id);

CREATE INDEX IF NOT EXISTS idx_frl_images_location_planet_id
    ON frl.frl_images_location(planet_id);

-- 3. Seed canonical planets ----------------------------------------

INSERT INTO frl.frl_location_planets (name) VALUES
    ('Earth'),
    ('Mars')
ON CONFLICT (name) DO NOTHING;
