-- Qdrant sync / insert-missing background jobs (AdminPanelAPI QdrantSync).
-- The API also creates this on startup; safe to re-run.
CREATE TABLE IF NOT EXISTS frl.frl_qdrant_sync_jobs (
    id              BIGSERIAL    PRIMARY KEY,
    kind            VARCHAR(20)  NOT NULL DEFAULT 'sync',
    apply           BOOLEAN      NOT NULL DEFAULT FALSE,
    triggered_by    VARCHAR(20)  NOT NULL DEFAULT 'manual',
    nightly_date    DATE,
    status          VARCHAR(20)  NOT NULL DEFAULT 'queued',
    start_after_id  INTEGER      NOT NULL DEFAULT 0,
    max_rows        BIGINT       NOT NULL DEFAULT 0,
    email           BOOLEAN      NOT NULL DEFAULT FALSE,
    last_id         INTEGER      NOT NULL DEFAULT 0,
    scanned         BIGINT       NOT NULL DEFAULT 0,
    found           BIGINT       NOT NULL DEFAULT 0,
    missing         BIGINT       NOT NULL DEFAULT 0,
    changed         BIGINT       NOT NULL DEFAULT 0,
    unchanged       BIGINT       NOT NULL DEFAULT 0,
    written         BIGINT       NOT NULL DEFAULT 0,
    field_counts    JSONB        NOT NULL DEFAULT '{}'::jsonb,
    examples        JSONB        NOT NULL DEFAULT '[]'::jsonb,
    retries         INTEGER      NOT NULL DEFAULT 0,
    claimed_by      TEXT,
    email_status    TEXT,
    error           TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_qsj_status ON frl.frl_qdrant_sync_jobs (status);
CREATE UNIQUE INDEX IF NOT EXISTS ux_qsj_nightly ON frl.frl_qdrant_sync_jobs (kind, nightly_date) WHERE nightly_date IS NOT NULL;
