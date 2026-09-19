-- Join-table convention is frl_join_images_$metatype.$metatype, so the tag
-- column is camera_movements rather than movement. Idempotent.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'frl'
                 AND table_name = 'frl_join_images_camera_movements'
                 AND column_name = 'movement') THEN
        ALTER TABLE frl.frl_join_images_camera_movements
            RENAME COLUMN movement TO camera_movements;
    END IF;
END $$;
