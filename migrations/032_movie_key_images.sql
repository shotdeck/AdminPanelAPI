-- Frames a tagger picked out of a movie while watching it. Only the timestamp
-- matters for the eventual full-quality extract from the master; the thumbnail
-- is the preview grabbed in the browser at capture time.
CREATE TABLE IF NOT EXISTS frl.frl_movie_key_images (
    id               BIGSERIAL     PRIMARY KEY,
    movie_id         INTEGER       NOT NULL,
    position_seconds NUMERIC(10,3) NOT NULL,
    thumbnail        TEXT,
    captured_by      VARCHAR(120),
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_fmki_movie_position
    ON frl.frl_movie_key_images (movie_id, position_seconds);
