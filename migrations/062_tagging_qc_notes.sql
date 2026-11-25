-- QC notes: a reviewer's notes on a tagged title, optionally on one of its
-- frames and one tag category. A title with open notes goes back to its tagger
-- as "Address QC Notes" until each note is marked addressed. Also adds the
-- setting that holds back the next drop until enough of the earlier one is
-- assigned. The API also creates these itself on first use; safe to run twice.

CREATE TABLE IF NOT EXISTS frl.frl_tagging_qc_notes (
    id            BIGSERIAL     PRIMARY KEY,
    movie_id      INTEGER       NOT NULL,
    key_image_id  BIGINT,
    category      VARCHAR(80),
    note          TEXT          NOT NULL,
    created_by    VARCHAR(120),
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    reply         TEXT,
    addressed_by  VARCHAR(120),
    addressed_at  TIMESTAMPTZ
);
ALTER TABLE frl.frl_tagging_qc_notes
    ADD COLUMN IF NOT EXISTS position_seconds NUMERIC(10,3);
CREATE INDEX IF NOT EXISTS idx_ftqn_movie
    ON frl.frl_tagging_qc_notes (movie_id);

INSERT INTO frl.frl_tagging_settings (name, value) VALUES
    ('release_next_drop_percent', 75)
ON CONFLICT (name) DO NOTHING;
