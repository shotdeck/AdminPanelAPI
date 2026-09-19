-- AI-generated, web-grounded track descriptions, cached per (song, movie).
--
-- Kept separate from frl_music_track_details (which is song-level and reused
-- across every movie that shares the song) because these descriptions are
-- movie-specific: they describe how the track is used in a particular film
-- (the scene, its significance), so the same song can have a different write-up
-- per movie. Sources are the web citations returned by the model.
--
-- Fetched lazily the first time a track's info panel is opened for a given
-- movie, then cached here. Additive and IF NOT EXISTS, safe to run anytime.
CREATE TABLE IF NOT EXISTS frl.frl_music_track_ai_description (
    song_id     BIGINT      NOT NULL
        REFERENCES frl.frl_music_songs (id) ON DELETE CASCADE,
    movieid     INTEGER     NOT NULL,
    description TEXT,
    sources     JSONB       NOT NULL DEFAULT '[]'::jsonb,
    model       VARCHAR(60),
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (song_id, movieid)
);
