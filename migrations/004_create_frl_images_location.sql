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

-- 2. Alias tables (map variants / typos to canonical entries) -----

CREATE TABLE IF NOT EXISTS frl.frl_location_continent_aliases (
    id              SERIAL      PRIMARY KEY,
    alias           VARCHAR(200) NOT NULL UNIQUE,
    continent_id    INTEGER     NOT NULL REFERENCES frl.frl_location_continents(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS frl.frl_location_country_aliases (
    id          SERIAL      PRIMARY KEY,
    alias       VARCHAR(200) NOT NULL UNIQUE,
    country_id  INTEGER     NOT NULL REFERENCES frl.frl_location_countries(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS frl.frl_location_region_aliases (
    id          SERIAL      PRIMARY KEY,
    alias       VARCHAR(200) NOT NULL UNIQUE,
    region_id   INTEGER     NOT NULL REFERENCES frl.frl_location_regions(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS frl.frl_location_city_aliases (
    id          SERIAL      PRIMARY KEY,
    alias       VARCHAR(200) NOT NULL UNIQUE,
    city_id     INTEGER     NOT NULL REFERENCES frl.frl_location_cities(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. Main image-location join table --------------------------------

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

-- 4. Indexes -------------------------------------------------------

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

-- 5. Seed canonical continents ------------------------------------

INSERT INTO frl.frl_location_continents (name) VALUES
    ('Africa'),
    ('Antarctica'),
    ('Asia'),
    ('Europe'),
    ('North America'),
    ('Oceania'),
    ('South America')
ON CONFLICT (name) DO NOTHING;

-- 6. Seed continent aliases ---------------------------------------

INSERT INTO frl.frl_location_continent_aliases (alias, continent_id) VALUES
    ('Aisa',             (SELECT id FROM frl.frl_location_continents WHERE name = 'Asia')),
    ('Asias',            (SELECT id FROM frl.frl_location_continents WHERE name = 'Asia')),
    ('asia',             (SELECT id FROM frl.frl_location_continents WHERE name = 'Asia')),
    ('South Asia',       (SELECT id FROM frl.frl_location_continents WHERE name = 'Asia')),
    ('West Asia',        (SELECT id FROM frl.frl_location_continents WHERE name = 'Asia')),
    ('Western Asia',     (SELECT id FROM frl.frl_location_continents WHERE name = 'Asia')),
    ('Eruope',           (SELECT id FROM frl.frl_location_continents WHERE name = 'Europe')),
    ('Europ',            (SELECT id FROM frl.frl_location_continents WHERE name = 'Europe')),
    ('North AMerica',    (SELECT id FROM frl.frl_location_continents WHERE name = 'North America')),
    ('North Amaerica',   (SELECT id FROM frl.frl_location_continents WHERE name = 'North America')),
    ('North Ameerica',   (SELECT id FROM frl.frl_location_continents WHERE name = 'North America')),
    ('North Amerca',     (SELECT id FROM frl.frl_location_continents WHERE name = 'North America')),
    ('North Amercia',    (SELECT id FROM frl.frl_location_continents WHERE name = 'North America')),
    ('North American',   (SELECT id FROM frl.frl_location_continents WHERE name = 'North America')),
    ('North Amerifca',   (SELECT id FROM frl.frl_location_continents WHERE name = 'North America')),
    ('North Ameriica',   (SELECT id FROM frl.frl_location_continents WHERE name = 'North America')),
    ('north america',    (SELECT id FROM frl.frl_location_continents WHERE name = 'North America')),
    ('North America of America', (SELECT id FROM frl.frl_location_continents WHERE name = 'North America')),
    ('Central America',  (SELECT id FROM frl.frl_location_continents WHERE name = 'North America')),
    ('Latin America',    (SELECT id FROM frl.frl_location_continents WHERE name = 'South America')),
    ('Caribbean',        (SELECT id FROM frl.frl_location_continents WHERE name = 'North America')),
    ('Carribean',        (SELECT id FROM frl.frl_location_continents WHERE name = 'North America')),
    ('Middle East',      (SELECT id FROM frl.frl_location_continents WHERE name = 'Asia')),
    ('Middle-East',      (SELECT id FROM frl.frl_location_continents WHERE name = 'Asia')),
    ('Oceana',           (SELECT id FROM frl.frl_location_continents WHERE name = 'Oceania')),
    ('Australia',        (SELECT id FROM frl.frl_location_continents WHERE name = 'Oceania')),
    ('Arctic Circle',    (SELECT id FROM frl.frl_location_continents WHERE name = 'Antarctica')),
    ('The Arctic',       (SELECT id FROM frl.frl_location_continents WHERE name = 'Antarctica')),
    ('Scandinavia',      (SELECT id FROM frl.frl_location_continents WHERE name = 'Europe')),
    ('America',          (SELECT id FROM frl.frl_location_continents WHERE name = 'North America'))
ON CONFLICT (alias) DO NOTHING;
