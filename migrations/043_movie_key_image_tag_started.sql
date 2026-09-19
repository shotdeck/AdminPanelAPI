-- When a frame was claimed by the image tagger, so a pass that was cut short
-- can have its frames put back rather than leaving them being read for ever.
ALTER TABLE frl.frl_movie_key_images
    ADD COLUMN IF NOT EXISTS tag_started_at TIMESTAMPTZ;
