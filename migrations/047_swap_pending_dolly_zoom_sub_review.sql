-- Swap not-yet-reviewed dolly_zoom_in <-> dolly_zoom_out sub-review rows.
-- Confirmed zoom_in now promotes to dolly_zoom_out (and vice versa), so
-- pending rows queued under the old mapping are flipped to match.
-- Reviewed rows (ok / bad / flagged) are left untouched.
-- Run manually, in one transaction, after the API deploy.

BEGIN;

UPDATE frl.frl_join_images_camera_movements
SET camera_movements = CASE camera_movements
        WHEN 'dolly_zoom_in'  THEN 'dolly_zoom_out'
        WHEN 'dolly_zoom_out' THEN 'dolly_zoom_in'
    END,
    updated_at = now()
WHERE camera_movements IN ('dolly_zoom_in', 'dolly_zoom_out')
  AND status = 'not_checked'
  AND confidence = 0
  -- skip images that already have the counterpart row (unique on imageid, camera_movements)
  AND NOT EXISTS (
        SELECT 1 FROM frl.frl_join_images_camera_movements o
        WHERE o.imageid = frl.frl_join_images_camera_movements.imageid
          AND o.camera_movements = CASE frl.frl_join_images_camera_movements.camera_movements
                WHEN 'dolly_zoom_in'  THEN 'dolly_zoom_out'
                ELSE 'dolly_zoom_in' END
  );

COMMIT;
