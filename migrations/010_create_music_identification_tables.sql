-- Create music identification tables for movie music scanning
-- (music detection + ACRCloud pipeline, mirrors the dialogue transcription tables).
--
-- Normalized: artists and songs are deduplicated across all movies; each
-- per-movie occurrence (matched song or unmatched music window) is a row in
-- frl_join_movies_music_segments. Only tables that carry a movieid use the
-- frl_join_movies_ prefix.

-- One row per scan job for a movie.
CREATE TABLE IF NOT EXISTS frl.frl_join_movies_music_identification_jobs (
    id              BIGSERIAL PRIMARY KEY,
    movieid         INT NOT NULL,
    status          VARCHAR(50) NOT NULL DEFAULT 'Queued',
    current_step    TEXT,
    progress_pct    INT DEFAULT 0,
    matched_count   INT,
    unmatched_count INT,
    r2_key          VARCHAR(1000),
    r2_url          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    error           TEXT
);

CREATE INDEX IF NOT EXISTS idx_mij_movieid ON frl.frl_join_movies_music_identification_jobs (movieid);
CREATE INDEX IF NOT EXISTS idx_mij_status ON frl.frl_join_movies_music_identification_jobs (status);

-- Unique artists, deduplicated by name across all movies.
CREATE TABLE IF NOT EXISTS frl.frl_music_artists (
    id                 BIGSERIAL PRIMARY KEY,
    name               VARCHAR(500) NOT NULL,
    acrcloud_artist_id VARCHAR(100),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_music_artists_name UNIQUE (name)
);

-- Unique songs/recordings, deduplicated by ACRCloud recording id (acrid).
CREATE TABLE IF NOT EXISTS frl.frl_music_songs (
    id         BIGSERIAL PRIMARY KEY,
    title      VARCHAR(500),
    isrc       VARCHAR(50),
    acrid      VARCHAR(100) NOT NULL,
    artist_id  BIGINT REFERENCES frl.frl_music_artists (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_music_songs_acrid UNIQUE (acrid)
);

CREATE INDEX IF NOT EXISTS idx_music_songs_artist ON frl.frl_music_songs (artist_id);

-- Per-movie occurrences: a matched song (song_id set) or an unmatched music
-- window (song_id NULL) at a time range within the movie.
CREATE TABLE IF NOT EXISTS frl.frl_join_movies_music_segments (
    id         BIGSERIAL PRIMARY KEY,
    movieid    INT NOT NULL,
    song_id    BIGINT REFERENCES frl.frl_music_songs (id),
    start_time DOUBLE PRECISION NOT NULL,
    end_time   DOUBLE PRECISION NOT NULL,
    matched    BOOLEAN NOT NULL DEFAULT FALSE,
    score      DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_ms_movieid ON frl.frl_join_movies_music_segments (movieid);
CREATE INDEX IF NOT EXISTS idx_ms_matched ON frl.frl_join_movies_music_segments (matched);
CREATE INDEX IF NOT EXISTS idx_ms_song ON frl.frl_join_movies_music_segments (song_id);
