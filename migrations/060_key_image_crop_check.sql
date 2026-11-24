-- Auto crops the bar detector had to guess, so a tagger can check them.
-- The API also adds this column itself on first use; safe to run twice.
ALTER TABLE frl.frl_movie_key_images
    ADD COLUMN IF NOT EXISTS crop_check BOOLEAN NOT NULL DEFAULT false;
