-- Parent -> sub-variant review promotion (backfill).
-- For every image whose PARENT camera-movement tag is already QC-confirmed
-- (status = 'ok'), queue each of that parent's more-specific "sub" variants as
-- a 'not_checked' row so reviewers can decide whether the clip is actually the
-- niche variant. Going forward this is done automatically in PUT /review.
--
-- Parent -> subs:
--   zoom_in   -> crash_zoom_in, dolly_zoom_in
--   zoom_out  -> crash_zoom_out, dolly_zoom_out
--   dolly_in  -> push_in, following        (Forward)
--   dolly_out -> pull_out, leading          (Backward)
--   pan_left  -> whip_pan_left
--   pan_right -> whip_pan_right
--
-- Existing rows (subs already reviewed either way) are left untouched.

INSERT INTO frl.frl_join_image_camera_movements (imageid, movement, confidence, status)
SELECT p.imageid, sub.movement, 0, 'not_checked'
FROM frl.frl_join_image_camera_movements p
JOIN (VALUES
    ('zoom_in',   'crash_zoom_in'),
    ('zoom_in',   'dolly_zoom_in'),
    ('zoom_out',  'crash_zoom_out'),
    ('zoom_out',  'dolly_zoom_out'),
    ('dolly_in',  'push_in'),
    ('dolly_in',  'following'),
    ('dolly_out', 'pull_out'),
    ('dolly_out', 'leading'),
    ('pan_left',  'whip_pan_left'),
    ('pan_right', 'whip_pan_right')
) AS sub(parent, movement) ON sub.parent = p.movement
WHERE p.status = 'ok'
ON CONFLICT (imageid, movement) DO NOTHING;
