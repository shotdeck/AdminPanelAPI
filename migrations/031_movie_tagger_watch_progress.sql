-- The movie tagging lifecycle: HD Uploaded -> SF Created -> Tagger Allocated
-- -> Movie Watched -> Key Images Extracted. A tagger must watch the HD movie
-- through, so how far they have legitimately reached is kept here rather than
-- trusted from the browser.

ALTER TABLE frl.frl_movie_tagger_assignments
    ADD COLUMN IF NOT EXISTS watch_position_seconds INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS watch_duration_seconds INTEGER,
    ADD COLUMN IF NOT EXISTS watched_at             TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS key_images_at          TIMESTAMPTZ;

UPDATE frl.frl_movie_tagger_assignments
SET status = 'movie_watched',
    watched_at = COALESCE(watched_at, updated_at)
WHERE status = 'done';

UPDATE frl.frl_movie_tagger_assignments
SET status = 'tagger_allocated'
WHERE status IN ('not_started', 'in_progress');
