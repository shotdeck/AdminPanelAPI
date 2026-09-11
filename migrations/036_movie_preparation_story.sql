-- Reading the film. Between describing a movie shot by shot and analysing it
-- for key images sits a third job: a language model reads the walkthrough and
-- rates every shot for how much the moment is worth a still, which is what the
-- analysis then ranks frames on. A movie whose rating did not run is analysed
-- against its plot instead, as before.
--
-- story_rated is how many shots came back worth a still.
ALTER TABLE frl.frl_movie_preparation
    ADD COLUMN IF NOT EXISTS story_job_id  VARCHAR(64),
    ADD COLUMN IF NOT EXISTS story_status  VARCHAR(16)  NOT NULL DEFAULT 'idle',
    ADD COLUMN IF NOT EXISTS story_stage   VARCHAR(32),
    ADD COLUMN IF NOT EXISTS story_progress NUMERIC(5,3) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS story_rated   INTEGER,
    ADD COLUMN IF NOT EXISTS story_error   TEXT;

CREATE INDEX IF NOT EXISTS idx_fmp_running_story
    ON frl.frl_movie_preparation (story_status);
