-- Which film an uploaded master is. A master arrives named after the film but
-- bound to nothing, so a tagger searches TMDB and confirms one result; this is
-- where that confirmation lives until (and after) the file is moved onto its
-- movie folder. Keyed on the R2 object, because the frl_movies record may not
-- exist yet at the moment the file is identified.
--
-- info holds the whole TMDB answer, so the popup and any later step can read
-- runtime, credits and the rest without asking TMDB again.
CREATE TABLE IF NOT EXISTS frl.frl_movie_file_identification (
    source_key      TEXT         PRIMARY KEY,
    tmdb_id         INTEGER      NOT NULL,
    movie_id        INTEGER,
    title           TEXT         NOT NULL,
    year            INTEGER,
    imdb_id         VARCHAR(16),
    runtime_minutes INTEGER,
    poster_path     TEXT,
    info            JSONB        NOT NULL,
    identified_by   TEXT         NOT NULL,
    identified_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fmfi_movie ON frl.frl_movie_file_identification (movie_id);
CREATE INDEX IF NOT EXISTS idx_fmfi_tmdb  ON frl.frl_movie_file_identification (tmdb_id);
