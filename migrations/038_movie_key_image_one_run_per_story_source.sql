-- A movie can now hold two analyses at once, one judging the story half from
-- its walkthrough and one from its plot, so the same second of film may be
-- proposed by both and the frames can be compared. The key therefore takes in
-- how the frame was judged; frames picked by hand have no source and keep one
-- row per second as before.
DROP INDEX IF EXISTS frl.ux_fmki_movie_position;

CREATE UNIQUE INDEX IF NOT EXISTS ux_fmki_movie_position_story
    ON frl.frl_movie_key_images (movie_id, position_seconds, COALESCE(story_from, ''));
