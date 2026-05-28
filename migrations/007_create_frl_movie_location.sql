-- ============================================================
-- 007: Movie-level filming locations (from Wikidata / Wikipedia)
-- ============================================================

CREATE TABLE IF NOT EXISTS frl.frl_movie_location (
    id              SERIAL          PRIMARY KEY,
    movie_id        INTEGER         NOT NULL REFERENCES frl.frl_movies(idnum) ON DELETE CASCADE,
    location_name   TEXT            NOT NULL,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    source          VARCHAR(50)     NOT NULL DEFAULT 'wikidata',
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_frl_movie_location_movie_id
    ON frl.frl_movie_location(movie_id);

CREATE INDEX IF NOT EXISTS idx_frl_movie_location_coords
    ON frl.frl_movie_location USING GIST (
        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
    )
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
