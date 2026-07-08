-- Create music identification tables for movie music scanning
-- (music detection + ACRCloud pipeline, mirrors the dialogue transcription tables).

CREATE TABLE IF NOT EXISTS frl.frl_music_identification_jobs (
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

CREATE INDEX IF NOT EXISTS idx_mij_movieid ON frl.frl_music_identification_jobs (movieid);
CREATE INDEX IF NOT EXISTS idx_mij_status ON frl.frl_music_identification_jobs (status);

CREATE TABLE IF NOT EXISTS frl.frl_music_segments (
    id              BIGSERIAL PRIMARY KEY,
    movieid         INT NOT NULL,
    start_time      DOUBLE PRECISION NOT NULL,
    end_time        DOUBLE PRECISION NOT NULL,
    matched         BOOLEAN NOT NULL DEFAULT FALSE,
    title           VARCHAR(500),
    artist          VARCHAR(500),
    recording_id    VARCHAR(100),
    score           DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_ms_movieid ON frl.frl_music_segments (movieid);
CREATE INDEX IF NOT EXISTS idx_ms_matched ON frl.frl_music_segments (matched);
