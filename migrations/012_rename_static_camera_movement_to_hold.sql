-- Rename the "static" camera-movement label to "hold" to match the renamed
-- class in the CameraMotionAPI (Modal VideoMAE) service. Going forward the
-- model returns "hold" for the no-movement class, and the AdminPanelAPI
-- fallback insert now also uses "hold"; this backfills existing data so the
-- old "static" rows are consistent.

-- 1. Movement tag rows. The table has a unique (imageid, movement) constraint,
--    so drop any "static" row that would collide with an existing "hold" row
--    for the same image before renaming the rest.
DELETE FROM frl.frl_join_image_camera_movements s
WHERE s.movement = 'static'
  AND EXISTS (
      SELECT 1 FROM frl.frl_join_image_camera_movements h
      WHERE h.imageid = s.imageid AND h.movement = 'hold'
  );

UPDATE frl.frl_join_image_camera_movements
SET movement = 'hold'
WHERE movement = 'static';

-- 2. Per-segment analysis JSON stores the raw model labels. Rename the label
--    value inside the stored JSON. Only the label string values can equal
--    "static", so a targeted value replace is safe.
UPDATE frl.frl_image_analysis_segments
SET segments_json = regexp_replace(
        segments_json::text,
        '("label"\s*:\s*")static(")',
        '\1hold\2',
        'g'
    )::jsonb
WHERE segments_json::text LIKE '%static%';
