-- Per-track enrichment (description, credits, release metadata), fetched
-- lazily the first time a track's detail panel is opened and cached here so
-- later views are instant and re-usable across movies that share the song.
--
-- Credits are stored as JSONB arrays of { "name": ..., "mbid": ... } objects
-- (MusicBrainz artist id) rather than flat text, so a future "click a writer
-- / producer -> more tracks by them" feature can look people up by mbid
-- without re-parsing.
--
-- Additive and IF NOT EXISTS, so it is safe to run anytime.
CREATE TABLE IF NOT EXISTS frl.frl_music_track_details (
    song_id            BIGINT PRIMARY KEY
        REFERENCES frl.frl_music_songs (id) ON DELETE CASCADE,
    description        TEXT,
    description_source VARCHAR(50),
    wikipedia_url      VARCHAR(500),
    writers            JSONB NOT NULL DEFAULT '[]'::jsonb,
    composers          JSONB NOT NULL DEFAULT '[]'::jsonb,
    producers          JSONB NOT NULL DEFAULT '[]'::jsonb,
    album              VARCHAR(300),
    release_date       VARCHAR(20),
    label              VARCHAR(200),
    preview_url        VARCHAR(500),
    musicbrainz_url    VARCHAR(500),
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
