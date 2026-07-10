-- Track which recognition provider matched each occurrence, and store the
-- Spotify URL a provider (currently AudD) resolves for a song.
--
-- ACRCloud and AudD are complementary: ACRCloud returns a fingerprint score
-- (0..100), AudD returns a positive identification with no numeric score but
-- often a Spotify link. `source` records which one matched a given occurrence;
-- `spotify_url` links straight to the track in the UI.

ALTER TABLE frl.frl_join_movies_music_segments
    ADD COLUMN IF NOT EXISTS source VARCHAR(20);

ALTER TABLE frl.frl_music_songs
    ADD COLUMN IF NOT EXISTS spotify_url TEXT;
