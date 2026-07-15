-- Composition-level metadata fallback marker.
--
-- Some tracks are film-specific arrangements or covers (e.g. a Hollywood
-- Symphony Orchestra rendition made for a movie) that the streaming/metadata
-- providers don't list under that exact recording. For those we fall back to
-- the underlying composition (its MusicBrainz work) and the composition's
-- best-known recording to fill writers/composers, artwork and a Spotify link.
--
-- This flag records that the credits/links shown were resolved from the
-- composition rather than the exact recording heard in the film, so the UI can
-- label them as being about the composition, not the film cue.
--
-- Additive and IF NOT EXISTS, so it is safe to run anytime.
ALTER TABLE frl.frl_music_track_details
    ADD COLUMN IF NOT EXISTS composition_fallback BOOLEAN NOT NULL DEFAULT false;
