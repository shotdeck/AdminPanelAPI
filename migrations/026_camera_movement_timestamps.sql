-- Add created_at / updated_at to the camera-movement join table so the QC
-- clip listing can sort by "recently edited" and "order AI tagged".
--
-- Columns are added nullable (metadata-only, instant even on a large table)
-- with a DEFAULT for future inserts. Existing rows stay NULL and sort last.
-- The API also ensures these columns at runtime (EnsureTimestampColumnsAsync),
-- so this migration is only for the record / fresh environments. Idempotent.

ALTER TABLE frl.frl_join_image_camera_movements ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
ALTER TABLE frl.frl_join_image_camera_movements ALTER COLUMN created_at SET DEFAULT now();

ALTER TABLE frl.frl_join_image_camera_movements ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE frl.frl_join_image_camera_movements ALTER COLUMN updated_at SET DEFAULT now();
