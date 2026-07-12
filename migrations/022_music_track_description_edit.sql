-- Human-editable, lockable track descriptions.
--
-- A description can be manually edited by an admin. Once edited it is "locked":
-- the AI generation/backfill path must never overwrite it (even on refresh),
-- and the UI labels it as human-authored rather than AI-generated.
--
-- We reuse frl_music_track_ai_description (which is already keyed per
-- (song, movie) and holds the description shown in the panel) and just add an
-- `edited` flag. When edited = true the row's `description` is the human text,
-- `sources` is left as-is, and `model` is cleared.
--
-- Additive and IF NOT EXISTS, so it is safe to run anytime.
ALTER TABLE frl.frl_music_track_ai_description
    ADD COLUMN IF NOT EXISTS edited BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE frl.frl_music_track_ai_description
    ADD COLUMN IF NOT EXISTS edited_at TIMESTAMPTZ;
