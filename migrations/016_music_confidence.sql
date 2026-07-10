-- Migration 016: soundtrack-reconciliation confidence per identified occurrence.
-- Additive/nullable (IF NOT EXISTS) — safe to run anytime, no rescan needed.
-- Values: 'confirmed' (artist matches the film's known soundtrack),
--         'review'    (song title matches but artist does not — likely a cover
--                      or a track sharing the movie's name),
--         'unverified' (no match against the known soundtrack).
-- NULL means the movie has not been reconciled yet.

ALTER TABLE frl.frl_join_movies_music_segments
    ADD COLUMN IF NOT EXISTS confidence VARCHAR(20);
