-- ============================================================
-- 004: Normalized filming-location schema
-- ============================================================

-- 1. Lookup tables ------------------------------------------------

CREATE TABLE IF NOT EXISTS frl.frl_location_continents (
    id          SERIAL      PRIMARY KEY,
    name        VARCHAR(100) NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS frl.frl_location_countries (
    id              SERIAL      PRIMARY KEY,
    continent_id    INTEGER     REFERENCES frl.frl_location_continents(id),
    name            VARCHAR(200) NOT NULL UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS frl.frl_location_regions (
    id          SERIAL      PRIMARY KEY,
    country_id  INTEGER     REFERENCES frl.frl_location_countries(id),
    name        VARCHAR(200) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (country_id, name)
);

CREATE TABLE IF NOT EXISTS frl.frl_location_cities (
    id          SERIAL      PRIMARY KEY,
    region_id   INTEGER     REFERENCES frl.frl_location_regions(id),
    country_id  INTEGER     REFERENCES frl.frl_location_countries(id),
    name        VARCHAR(200) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (country_id, name)
);

-- 2. Main image-location join table --------------------------------

CREATE TABLE IF NOT EXISTS frl.frl_images_location (
    id                  BIGSERIAL       PRIMARY KEY,
    image_id            INTEGER         NOT NULL,
    raw_location        TEXT,
    continent_id        INTEGER         REFERENCES frl.frl_location_continents(id),
    country_id          INTEGER         REFERENCES frl.frl_location_countries(id),
    region_id           INTEGER         REFERENCES frl.frl_location_regions(id),
    city_id             INTEGER         REFERENCES frl.frl_location_cities(id),
    specific_location   TEXT,
    coordinates         POINT,
    confidence          REAL            DEFAULT 1.0,
    needs_review        BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_frl_images_location_image
        FOREIGN KEY (image_id) REFERENCES frl.frl_images(idnum)
        ON DELETE CASCADE
);

-- 3. Indexes -------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_frl_images_location_image_id
    ON frl.frl_images_location(image_id);

CREATE INDEX IF NOT EXISTS idx_frl_images_location_country_id
    ON frl.frl_images_location(country_id);

CREATE INDEX IF NOT EXISTS idx_frl_images_location_city_id
    ON frl.frl_images_location(city_id);

CREATE INDEX IF NOT EXISTS idx_frl_images_location_needs_review
    ON frl.frl_images_location(needs_review) WHERE needs_review = TRUE;

CREATE INDEX IF NOT EXISTS idx_frl_images_location_coordinates
    ON frl.frl_images_location USING GIST (coordinates);

-- 4. Seed canonical continents ------------------------------------

INSERT INTO frl.frl_location_continents (name) VALUES
    ('Africa'),
    ('Antarctica'),
    ('Asia'),
    ('Europe'),
    ('North America'),
    ('Oceania'),
    ('South America')
ON CONFLICT (name) DO NOTHING;
