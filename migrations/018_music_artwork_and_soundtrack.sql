-- Album cover art for an identified song (captured from Spotify during the
-- streaming-link backfill), plus a per-movie official soundtrack record
-- (album name + Spotify album link + cover art + Wikipedia article URL).
-- Additive and IF NOT EXISTS, so it is safe to run anytime.
ALTER TABLE frl.frl_music_songs
    ADD COLUMN IF NOT EXISTS artwork_url VARCHAR(500);

CREATE TABLE IF NOT EXISTS frl.frl_music_movie_soundtrack (
    movieid       INTEGER PRIMARY KEY,
    album_name    VARCHAR(300),
    spotify_url   VARCHAR(500),
    artwork_url   VARCHAR(500),
    wikipedia_url VARCHAR(500),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
