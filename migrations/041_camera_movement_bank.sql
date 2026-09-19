-- Bank of pre-analysed camera-movement images. The CameraMovementBankWorker
-- keeps this topped up per media type (analysed, tags stored, no owner); a
-- reviewer's Fetch Next Batch moves rows out of here into
-- frl_camera_movement_image_owner instantly instead of waiting on the GPU.
-- Also created on demand by the API; this file documents the schema.

CREATE TABLE IF NOT EXISTS frl.frl_camera_movement_bank (
    imageid      INTEGER      PRIMARY KEY,
    media_type   VARCHAR(60)  NOT NULL,
    analyzed_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cmb_media_type ON frl.frl_camera_movement_bank (media_type);
