-- score is the combined ranking value (look + 0.35 * story), which cannot say
-- why a frame was proposed. These are the two halves it is made of, each
-- normalised to 0..1 within its own analysis run:
--
--   look_score  how the frame rates as a photograph (aesthetic head)
--   story_score how much it matches the film's description (CLIP relevance)
--
-- Null on proposals made before the analysis reported them.
ALTER TABLE frl.frl_movie_key_images
    ADD COLUMN IF NOT EXISTS look_score  NUMERIC(6,3),
    ADD COLUMN IF NOT EXISTS story_score NUMERIC(6,3);
