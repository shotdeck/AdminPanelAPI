-- What has been done to a movie without anyone asking. Once a movie's SF proxy
-- appears in R2, a worker describes the film shot by shot and analyses it for
-- key images, so both are waiting by the time a tagger finishes watching. One
-- row per movie, keyed on the SF it was prepared from: a re-encoded proxy is a
-- new source and is prepared again.
--
-- status columns: idle | running | completed | error
CREATE TABLE IF NOT EXISTS frl.frl_movie_preparation (
    movie_id            INTEGER      PRIMARY KEY,
    source_key          TEXT         NOT NULL,

    walkthrough_job_id  VARCHAR(64),
    walkthrough_status  VARCHAR(16)  NOT NULL DEFAULT 'idle',
    walkthrough_stage   VARCHAR(32),
    walkthrough_progress NUMERIC(5,3) NOT NULL DEFAULT 0,
    walkthrough_shots   INTEGER,
    walkthrough_error   TEXT,

    analysis_job_id     VARCHAR(64),
    analysis_status     VARCHAR(16)  NOT NULL DEFAULT 'idle',
    analysis_stage      VARCHAR(32),
    analysis_progress   NUMERIC(5,3) NOT NULL DEFAULT 0,
    analysis_proposals  INTEGER,
    analysis_error      TEXT,

    started_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fmp_running
    ON frl.frl_movie_preparation (walkthrough_status, analysis_status);
