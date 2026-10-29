-- "Relocate clips" jobs: check every clip of one movie against its still,
-- find the real frame of any still that isn't at the clip's centre by searching
-- the whole film, and (with apply) recut those clips with new scene boundaries
-- and previews. Processed server-side by ClipRelocateWorker, one job at a time;
-- the film search runs on Modal (clip-relocate app).
-- Also created on demand by the API; this file documents the schema.

CREATE TABLE IF NOT EXISTS frl.frl_clip_relocate_jobs (
    id             BIGSERIAL    PRIMARY KEY,
    movie_id       INTEGER      NOT NULL,
    apply          BOOLEAN      NOT NULL DEFAULT FALSE,  -- false: report only
    write_history  BOOLEAN      NOT NULL DEFAULT FALSE,  -- add a frl_imagehistory row per relocated still
    status         VARCHAR(20)  NOT NULL DEFAULT 'queued',  -- queued | running | finishing | done | failed
    modal_call_id  TEXT,
    phase          TEXT,
    progress       JSONB,
    images         INTEGER      NOT NULL DEFAULT 0,
    flagged        INTEGER      NOT NULL DEFAULT 0,      -- still not at the clip's centre
    relocated      INTEGER      NOT NULL DEFAULT 0,      -- recut (or would be, in a dry run)
    not_found      INTEGER      NOT NULL DEFAULT 0,      -- still not found anywhere in the film
    failed         INTEGER      NOT NULL DEFAULT 0,
    report         JSONB,
    error          TEXT,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    started_at     TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_crj_status ON frl.frl_clip_relocate_jobs (status);

-- Scene-boundary rows replaced by an applied job, kept for rollback.
CREATE TABLE IF NOT EXISTS frl.frl_clip_relocate_boundary_backup AS
SELECT 0::BIGINT AS job_id, now() AS backed_up_at, b.*
FROM frl.frl_image_scene_boundaries b
WITH NO DATA;
