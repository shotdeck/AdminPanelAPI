-- Concurrent "Fetch Next Batch" support for Camera Movement QC.
--
-- Two tables:
--  1. frl_camera_movement_jobs  - one row per fetch run so every session can
--     see that "someone" is running a fetch, with a display name and live
--     progress. No real auth; started_by is a self-entered label.
--  2. frl_camera_movement_claims - short-lived per-image claims so two
--     simultaneous fetches don't grab and double-analyze the same images.
--     Claims older than the analyze window are treated as abandoned.

CREATE TABLE IF NOT EXISTS frl.frl_camera_movement_jobs (
    job_id      UUID PRIMARY KEY,
    started_by  VARCHAR(120),
    status      VARCHAR(20)  NOT NULL DEFAULT 'running',  -- running | done | error | stale
    requested   INTEGER      NOT NULL DEFAULT 0,
    processed   INTEGER      NOT NULL DEFAULT 0,
    failed      INTEGER      NOT NULL DEFAULT 0,
    started_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cmj_status ON frl.frl_camera_movement_jobs (status);

CREATE TABLE IF NOT EXISTS frl.frl_camera_movement_claims (
    imageid     INTEGER      PRIMARY KEY,
    job_id      UUID         NOT NULL,
    claimed_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cmc_job_id     ON frl.frl_camera_movement_claims (job_id);
CREATE INDEX IF NOT EXISTS idx_cmc_claimed_at ON frl.frl_camera_movement_claims (claimed_at);
