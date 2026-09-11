-- story_score says how much the moment matters, but not how that was judged.
-- Since the walkthrough drives selection where a movie has one, the same
-- number means two different things:
--
--   'walkthrough'  a language model read the shot's own description
--   'description'  CLIP matched the frame against the film's plot summary
--
-- Null on proposals stored before the run recorded which it used.
ALTER TABLE frl.frl_movie_key_images
    ADD COLUMN IF NOT EXISTS story_from VARCHAR(16);
