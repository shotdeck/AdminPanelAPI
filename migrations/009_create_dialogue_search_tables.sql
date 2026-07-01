-- Create dialogue search tables for movie transcription and phrase search

CREATE TABLE IF NOT EXISTS frl.frl_dialogue_transcription_jobs (
    id              BIGSERIAL PRIMARY KEY,
    movieid         INT NOT NULL,
    status          VARCHAR(50) NOT NULL DEFAULT 'Queued',
    current_step    TEXT,
    progress_pct    INT DEFAULT 0,
    word_count      INT,
    r2_key          VARCHAR(1000),
    r2_url          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    error           TEXT
);

CREATE INDEX IF NOT EXISTS idx_dtj_movieid ON frl.frl_dialogue_transcription_jobs (movieid);
CREATE INDEX IF NOT EXISTS idx_dtj_status ON frl.frl_dialogue_transcription_jobs (status);

CREATE TABLE IF NOT EXISTS frl.frl_transcript_words (
    id              BIGSERIAL PRIMARY KEY,
    movieid         INT NOT NULL,
    word_index      INT NOT NULL,
    word            VARCHAR(200) NOT NULL,
    start_time      DOUBLE PRECISION NOT NULL,
    end_time        DOUBLE PRECISION NOT NULL,
    confidence      DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_tw_word ON frl.frl_transcript_words (word);
CREATE INDEX IF NOT EXISTS idx_tw_movieid_word_index ON frl.frl_transcript_words (movieid, word_index);
