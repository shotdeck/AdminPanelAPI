-- Every other image join table is named frl_join_images_* (plural); the
-- camera-movement table was the odd one out. Idempotent: no-op once renamed.
ALTER TABLE IF EXISTS frl.frl_join_image_camera_movements
    RENAME TO frl_join_images_camera_movements;
