-- The last stage of a movie: every frame the image tagger read has been stood
-- by, so there is nothing left on it for the tagger to do.
ALTER TABLE frl.frl_movie_tagger_assignments
    ADD COLUMN IF NOT EXISTS tags_confirmed_at TIMESTAMPTZ;

-- Movies already finished before the stage existed.
UPDATE frl.frl_movie_tagger_assignments a
SET status = 'tags_confirmed',
    tags_confirmed_at = COALESCE(a.tags_confirmed_at, now()),
    updated_at = now()
WHERE a.status = 'ai_tags_read'
  AND EXISTS (
      SELECT 1 FROM frl.frl_movie_key_images k
      WHERE k.movie_id = a.movie_id AND k.decision = 'kept'
        AND k.image_key IS NOT NULL AND k.tag_status = 'tagged')
  AND NOT EXISTS (
      SELECT 1 FROM frl.frl_movie_key_images k
      WHERE k.movie_id = a.movie_id AND k.decision = 'kept'
        AND k.image_key IS NOT NULL
        AND (k.tag_status IS NULL
             OR k.tag_status IN ('pending', 'running')
             OR (k.tag_status = 'tagged' AND k.tags_confirmed_at IS NULL)));
