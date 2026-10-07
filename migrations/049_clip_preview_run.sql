-- Where the clip preview backfill has got to, so a recycled app resumes it.
CREATE TABLE IF NOT EXISTS frl.frl_clip_preview_run (
    id                     INTEGER      PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    running                BOOLEAN      NOT NULL DEFAULT FALSE,
    started_after_image_id INTEGER      NOT NULL DEFAULT 0,
    batch_size             INTEGER      NOT NULL DEFAULT 2000,
    overwrite              BOOLEAN      NOT NULL DEFAULT FALSE,
    motion_only            BOOLEAN      NOT NULL DEFAULT TRUE,
    cursor_image_id        INTEGER      NOT NULL DEFAULT 0,
    batches                INTEGER      NOT NULL DEFAULT 0,
    requested              BIGINT       NOT NULL DEFAULT 0,
    created                BIGINT       NOT NULL DEFAULT 0,
    exists_count           BIGINT       NOT NULL DEFAULT 0,
    skipped                BIGINT       NOT NULL DEFAULT 0,
    errors                 BIGINT       NOT NULL DEFAULT 0,
    completed_all          BOOLEAN      NOT NULL DEFAULT FALSE,
    last_error             TEXT,
    last_error_at          TIMESTAMPTZ,
    started_at             TIMESTAMPTZ,
    finished_at            TIMESTAMPTZ,
    updated_at             TIMESTAMPTZ  NOT NULL DEFAULT now()
);
