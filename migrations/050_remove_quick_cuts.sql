-- Retire the quick_cuts camera-movement tag (redundant with the clip-length
-- detector). DESTRUCTIVE: run by hand, once.
--
-- Images whose only tag is quick_cuts become 'hold' (what the API stores when
-- the AI returns nothing usable), so they are not treated as unanalysed and
-- re-fetched. Every other quick_cuts row is deleted.

BEGIN;

UPDATE frl.frl_join_images_camera_movements cm
SET camera_movements = 'hold', confidence = 0, status = 'not_checked', updated_at = now()
WHERE cm.camera_movements = 'quick_cuts'
  AND NOT EXISTS (
      SELECT 1 FROM frl.frl_join_images_camera_movements o
      WHERE o.imageid = cm.imageid AND o.camera_movements <> 'quick_cuts');

DELETE FROM frl.frl_join_images_camera_movements
WHERE camera_movements = 'quick_cuts';

-- Hold-only images also get no_movement (same rule as migration 013).
INSERT INTO frl.frl_join_images_camera_movements (imageid, camera_movements, confidence, status)
SELECT cm.imageid, 'no_movement', 0, 'not_checked'
FROM frl.frl_join_images_camera_movements cm
WHERE cm.camera_movements = 'hold'
  AND NOT EXISTS (
      SELECT 1 FROM frl.frl_join_images_camera_movements o
      WHERE o.imageid = cm.imageid AND o.camera_movements NOT IN ('hold', 'no_movement'))
ON CONFLICT (imageid, camera_movements) DO NOTHING;

COMMIT;
