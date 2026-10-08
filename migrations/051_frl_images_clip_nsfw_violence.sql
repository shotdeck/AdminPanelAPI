-- Clip-level NSFW/Violence flag on frl_images, kept in sync by the QC
-- "NSFW/Violence" tag (added/confirmed -> true; incorrect/removed/sent back -> false).
-- Run the steps one at a time in pgAdmin.

-- Step 1 (read-only): find the existing image-level NSFW column.
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'frl' AND table_name = 'frl_images'
  AND (column_name ILIKE '%nsfw%' OR column_name ILIKE '%mature%'
       OR column_name ILIKE '%explicit%' OR column_name ILIKE '%adult%');

-- Step 2: add the column. A constant default is metadata-only, so this is
-- instant even on the full table.
ALTER TABLE frl.frl_images
    ADD COLUMN IF NOT EXISTS clip_nsfw_violence BOOLEAN NOT NULL DEFAULT false;

-- Step 3: default clips from the existing image NSFW value. Replace
-- EXISTING_NSFW_COLUMN with the name from step 1. Only rows that are NSFW
-- are touched. (If that column isn't boolean, use e.g. "= 1" instead of IS TRUE.)
UPDATE frl.frl_images
SET clip_nsfw_violence = true
WHERE EXISTING_NSFW_COLUMN IS TRUE
  AND clip_nsfw_violence = false;

-- Step 4: carry over any NSFW/Violence tags QC has already confirmed.
UPDATE frl.frl_images i
SET clip_nsfw_violence = true
FROM frl.frl_join_images_camera_movements cm
WHERE cm.imageid = i.idnum
  AND cm.camera_movements = 'nsfw_violence'
  AND cm.status = 'ok'
  AND i.clip_nsfw_violence = false;
