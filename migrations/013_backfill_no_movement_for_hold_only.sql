-- Auto-tag "no_movement" on images whose ONLY camera-movement tag is "hold".
-- "No Movement" is a new, reviewable QC category: images that got nothing but
-- the no-movement (hold) class get a no_movement row added (in addition to
-- hold) with status 'not_checked' so QC can review the auto-tagging.
--
-- An image qualifies when it has a "hold" row and no other movement besides
-- "no_movement" itself (so re-running is idempotent). Images with 2+ movement
-- categories (even if one is hold) are left unchanged.

INSERT INTO frl.frl_join_images_camera_movements (imageid, camera_movements, confidence, status)
SELECT imageid, 'no_movement', 0, 'not_checked'
FROM frl.frl_join_images_camera_movements
GROUP BY imageid
HAVING bool_or(camera_movements = 'hold')
   AND count(*) FILTER (WHERE camera_movements NOT IN ('hold', 'no_movement')) = 0
ON CONFLICT (imageid, camera_movements) DO NOTHING;
