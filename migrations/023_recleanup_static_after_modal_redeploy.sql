-- Re-run the static -> hold cleanup. Migration 012 renamed existing "static"
-- rows to "hold", but the Modal camera-motion service was still running the
-- pre-rename code afterward, so a later processing batch wrote new "static"
-- rows. The Modal service has since been redeployed (returns "hold") and
-- AdminPanelAPI now points at it, so this backfills the rows written in the
-- interim. Same logic as 012; safe to run repeatedly.

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
