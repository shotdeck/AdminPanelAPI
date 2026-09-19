-- Allow a movie processing job to run in "missing clips only" mode, where the
-- pipeline skips clips that already have a scene boundary row.

ALTER TABLE frl.frl_movie_processing_jobs
ADD COLUMN IF NOT EXISTS missing_only BOOLEAN NOT NULL DEFAULT FALSE;
