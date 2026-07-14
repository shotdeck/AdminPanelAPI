-- Add publishers to the song-level track details cache.
--
-- Publishers are the music-publishing companies credited on the underlying
-- work in MusicBrainz (work -> label "publishing" relationships), distinct
-- from the record label on the release. Stored as JSONB arrays of
-- { "name": ..., "mbid": ... } (MusicBrainz label id) to match the existing
-- writers/composers/producers shape and keep a future "browse by publisher"
-- lookup cheap.
--
-- Additive with IF NOT EXISTS, safe to run anytime. Existing cached rows keep
-- an empty array until their song-level details are re-fetched.
ALTER TABLE frl.frl_music_track_details
    ADD COLUMN IF NOT EXISTS publishers JSONB NOT NULL DEFAULT '[]'::jsonb;
