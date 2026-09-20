-- What each week's retraining of the image tagger did, so the dashboard can
-- show it without anyone reading Modal's logs.
CREATE TABLE IF NOT EXISTS frl.frl_tagger_training_runs (
    id            BIGSERIAL    PRIMARY KEY,
    status        VARCHAR(16)  NOT NULL DEFAULT 'running',
    model_version VARCHAR(32),
    frames        INTEGER      NOT NULL DEFAULT 0,
    decisions     INTEGER      NOT NULL DEFAULT 0,
    corrections   INTEGER      NOT NULL DEFAULT 0,
    promoted      BOOLEAN      NOT NULL DEFAULT FALSE,
    note          TEXT,
    report        JSONB,
    started_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fttr_started
    ON frl.frl_tagger_training_runs (started_at DESC);
