-- Allocation of uploaded movies to taggers, plus the progress the admin
-- tagging page reports. One row per movie; re-allocating a movie updates it.

CREATE TABLE IF NOT EXISTS frl.frl_movie_tagger_assignments (
    movie_id     INTEGER      PRIMARY KEY,
    tagger       VARCHAR(120) NOT NULL,
    status       VARCHAR(24)  NOT NULL DEFAULT 'not_started',
    note         TEXT,
    assigned_by  VARCHAR(120),
    assigned_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fmta_tagger
    ON frl.frl_movie_tagger_assignments (lower(tagger));
