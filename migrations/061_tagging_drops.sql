-- Drops: the releases titles are tagged for. Each drop has a drop date and a
-- due date (by default a few days before it); a title belongs to one drop,
-- takes the drop's due date unless an admin gives it its own, and may be
-- marked priority. The API also creates these itself on first use; safe to
-- run twice.

CREATE TABLE IF NOT EXISTS frl.frl_tagging_drops (
    id          BIGSERIAL    PRIMARY KEY,
    drop_date   DATE         NOT NULL UNIQUE,
    due_date    DATE         NOT NULL,
    note        TEXT,
    created_by  VARCHAR(120),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS frl.frl_tagging_drop_titles (
    movie_id           INTEGER      PRIMARY KEY,
    drop_id            BIGINT       NOT NULL
        REFERENCES frl.frl_tagging_drops (id) ON DELETE CASCADE,
    priority           BOOLEAN      NOT NULL DEFAULT false,
    due_date_override  DATE,
    updated_by         VARCHAR(120),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ftdt_drop
    ON frl.frl_tagging_drop_titles (drop_id);

CREATE TABLE IF NOT EXISTS frl.frl_tagging_settings (
    name        VARCHAR(64)  PRIMARY KEY,
    value       INTEGER      NOT NULL,
    updated_by  VARCHAR(120),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

INSERT INTO frl.frl_tagging_settings (name, value) VALUES
    ('max_titles_per_tagger', 3),
    ('open_drop_weeks', 2),
    ('due_days_before_drop', 5)
ON CONFLICT (name) DO NOTHING;
