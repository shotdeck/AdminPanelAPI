-- Where each key image's picture sits inside its black bars, as fractions of
-- the frame. image_key stays the uncropped still; cropped_key is the cut copy.
ALTER TABLE frl.frl_movie_key_images
    ADD COLUMN IF NOT EXISTS crop_x      NUMERIC(6,5),
    ADD COLUMN IF NOT EXISTS crop_y      NUMERIC(6,5),
    ADD COLUMN IF NOT EXISTS crop_w      NUMERIC(6,5),
    ADD COLUMN IF NOT EXISTS crop_h      NUMERIC(6,5),
    ADD COLUMN IF NOT EXISTS crop_source VARCHAR(16),
    ADD COLUMN IF NOT EXISTS cropped_key TEXT,
    ADD COLUMN IF NOT EXISTS cropped_by  VARCHAR(120),
    ADD COLUMN IF NOT EXISTS cropped_at  TIMESTAMPTZ;
