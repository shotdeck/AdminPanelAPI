-- Records why an image failed camera-movement analysis. Previously a failure
-- only bumped a counter on the job, so the reason returned by the analysis API
-- was discarded and the same broken clips were retried at the head of the
-- popularity-ordered queue on every subsequent fetch.
CREATE TABLE IF NOT EXISTS frl.frl_camera_movement_failures (
    imageid       INTEGER      PRIMARY KEY,
    reason        TEXT         NOT NULL,
    attempts      INTEGER      NOT NULL DEFAULT 1,
    first_failed  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_failed   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cmf_attempts
    ON frl.frl_camera_movement_failures (attempts);
CREATE INDEX IF NOT EXISTS idx_cmf_last_failed
    ON frl.frl_camera_movement_failures (last_failed DESC);
