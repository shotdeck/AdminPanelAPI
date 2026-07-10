-- Universal all-services listen link (Odesli / song.link page) for an
-- identified song, alongside the existing direct spotify_url. Additive and
-- IF NOT EXISTS, so it is safe to run anytime.
ALTER TABLE frl.frl_music_songs
    ADD COLUMN IF NOT EXISTS streaming_url VARCHAR(500);
