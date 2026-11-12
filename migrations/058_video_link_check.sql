-- Weekly music video / commercial link check jobs and per-movie results (AdminPanelAPI VideoLinkCheck).
-- The API also creates these on startup; safe to re-run.
CREATE TABLE IF NOT EXISTS frl.frl_video_link_check_jobs (
    id                BIGSERIAL    PRIMARY KEY,
    status            VARCHAR(20)  NOT NULL DEFAULT 'queued',
    triggered_by      VARCHAR(40)  NOT NULL DEFAULT 'manual',
    email             BOOLEAN      NOT NULL DEFAULT TRUE,
    email_to          TEXT,
    last_id           BIGINT       NOT NULL DEFAULT 0,
    scanned           BIGINT       NOT NULL DEFAULT 0,
    active            BIGINT       NOT NULL DEFAULT 0,
    unavailable       BIGINT       NOT NULL DEFAULT 0,
    unknown_platform  BIGINT       NOT NULL DEFAULT 0,
    errors            BIGINT       NOT NULL DEFAULT 0,
    newly_unavailable BIGINT       NOT NULL DEFAULT 0,
    previous_job_id   BIGINT,
    claimed_by        TEXT,
    email_status      TEXT,
    error             TEXT,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    started_at        TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_vlcj_status ON frl.frl_video_link_check_jobs (status);
CREATE TABLE IF NOT EXISTS frl.frl_video_link_check_results (
    job_id      BIGINT       NOT NULL REFERENCES frl.frl_video_link_check_jobs (id) ON DELETE CASCADE,
    movie_id    BIGINT       NOT NULL,
    title       TEXT         NOT NULL DEFAULT '',
    media_type  TEXT         NOT NULL DEFAULT '',
    url         TEXT         NOT NULL DEFAULT '',
    platform    VARCHAR(20)  NOT NULL DEFAULT '',
    video_id    TEXT         NOT NULL DEFAULT '',
    status      VARCHAR(30)  NOT NULL,
    note        TEXT,
    checked_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, movie_id)
);
