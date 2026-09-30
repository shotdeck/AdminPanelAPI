-- "Fetch Movie" jobs: analyse every remaining image of one title and assign
-- it to a reviewer. Processed server-side by CameraMovementMovieFetchWorker,
-- one job at a time, so a reviewer can log out while their title is prepared.
-- claim_id is the job_id stamped on frl_camera_movement_claims for its batches.
-- Also created on demand by the API; this file documents the schema.

CREATE TABLE IF NOT EXISTS frl.frl_camera_movement_movie_jobs (
    id            SERIAL       PRIMARY KEY,
    movie_id      INTEGER      NOT NULL,
    owner         VARCHAR(120) NOT NULL,
    requested_by  VARCHAR(120) NOT NULL,
    claim_id      UUID         NOT NULL,
    status        VARCHAR(20)  NOT NULL DEFAULT 'queued',  -- queued | running | done | cancelled
    assigned      INTEGER      NOT NULL DEFAULT 0,         -- already-analysed images assigned on fetch
    to_analyze    INTEGER      NOT NULL DEFAULT 0,         -- images needing analysis when queued
    processed     INTEGER      NOT NULL DEFAULT 0,
    failed        INTEGER      NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    started_at    TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_cmmj_status ON frl.frl_camera_movement_movie_jobs (status);
