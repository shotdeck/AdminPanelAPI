-- Cache of "Check with AI" (audio-listening identification) results.
--
-- The audio-identify endpoint listens to a clip and returns an advisory
-- suggestion (composer/kind of music + a ranked shortlist of candidate cues
-- with listen links). That call is slow (clip extraction + an audio LLM +
-- web-search) and its shortlist can vary run-to-run, so we persist the last
-- successful result per (movie, song). A second press returns it instantly;
-- passing refresh=true forces a fresh listen and overwrites the cache.
--
-- Additive and IF NOT EXISTS, so it is safe to run anytime.
CREATE TABLE IF NOT EXISTS frl.frl_music_audio_identify_cache (
    movieid     INTEGER     NOT NULL,
    song_id     BIGINT      NOT NULL,
    suggestion  JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (movieid, song_id)
);
