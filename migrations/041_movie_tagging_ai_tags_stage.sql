-- The AI tags stage: a movie moves on from key_images_extracted once the
-- image tagger has read every kept frame of it.
ALTER TABLE frl.frl_movie_tagger_assignments
    ADD COLUMN IF NOT EXISTS ai_tags_at TIMESTAMPTZ;
