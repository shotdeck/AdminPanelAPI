-- The technical terms an image carries — frame size, lens, lighting, and the
-- rest of the fifteen categories — read off a kept key image by the image
-- tagger and then put to a human. Both readings are kept per category:
--
--   ai_value    what the model called it, with its confidence and the legal
--               alternatives it ranked below the winner
--   value       what the tagger settled on, null while nobody has looked
--
-- so a term the tagger changed can always be told from the model's own guess,
-- and the changes can be fed back into training later.
CREATE TABLE IF NOT EXISTS frl.frl_movie_key_image_tags (
    id            BIGSERIAL    PRIMARY KEY,
    key_image_id  BIGINT       NOT NULL
        REFERENCES frl.frl_movie_key_images (id) ON DELETE CASCADE,
    category      VARCHAR(40)  NOT NULL,
    ai_value      TEXT,
    ai_confidence NUMERIC(6,4),
    options       JSONB,
    value         TEXT,
    decided_by    VARCHAR(120),
    decided_at    TIMESTAMPTZ,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_fmkit_image_category
    ON frl.frl_movie_key_image_tags (key_image_id, category);

-- Where a kept frame has got to in being tagged: pending (waiting to be read),
-- running (with the tagger API), tagged, or error with what went wrong. Frames
-- kept before this stage existed are picked up by the same sweep.
ALTER TABLE frl.frl_movie_key_images
    ADD COLUMN IF NOT EXISTS tag_status        VARCHAR(16),
    ADD COLUMN IF NOT EXISTS tag_model_version VARCHAR(32),
    ADD COLUMN IF NOT EXISTS tag_error         TEXT,
    ADD COLUMN IF NOT EXISTS tagged_at         TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_fmki_tag_status
    ON frl.frl_movie_key_images (tag_status)
    WHERE tag_status IN ('pending', 'running');
