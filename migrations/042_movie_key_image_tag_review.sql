-- Reviewing a key image's technical terms: the tagger ticks off each image
-- once its terms are right, and every term they changed is kept as its own
-- row, so the model can later be retrained on where it was wrong.
ALTER TABLE frl.frl_movie_key_images
    ADD COLUMN IF NOT EXISTS tags_confirmed_by VARCHAR(120),
    ADD COLUMN IF NOT EXISTS tags_confirmed_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS frl.frl_movie_key_image_tag_changes (
    id             BIGSERIAL    PRIMARY KEY,
    key_image_id   BIGINT       NOT NULL
        REFERENCES frl.frl_movie_key_images (id) ON DELETE CASCADE,
    movie_id       INTEGER      NOT NULL,
    category       VARCHAR(40)  NOT NULL,
    ai_value       TEXT,
    ai_confidence  NUMERIC(6,4),
    model_version  VARCHAR(32),
    previous_value TEXT,
    value          TEXT,
    changed_by     VARCHAR(120),
    changed_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    exported_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fmkitc_movie
    ON frl.frl_movie_key_image_tag_changes (movie_id, changed_at);
CREATE INDEX IF NOT EXISTS idx_fmkitc_unexported
    ON frl.frl_movie_key_image_tag_changes (changed_at)
    WHERE exported_at IS NULL;
