-- A confirmed frame is one training example, so a retraining batch has to
-- remember which frames it has already taken or every week relearns the same
-- ones.
ALTER TABLE frl.frl_movie_key_images
    ADD COLUMN IF NOT EXISTS tags_exported_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_fmki_training_pending
    ON frl.frl_movie_key_images (tags_confirmed_at)
    WHERE tags_confirmed_at IS NOT NULL AND tags_exported_at IS NULL;
