-- The claim query in CameraMovementController.ClaimImagesAsync orders live
-- images by weighted_score and takes the top N not yet analysed. Without
-- these two indexes Postgres has to build every candidate (seq scan of all
-- scene boundaries, one index probe per row into frl_images) and sort them
-- on disk before it can apply the LIMIT — ~7 minutes for 10 rows.
--
-- With them the planner walks frl_images in score order, probes the scene
-- boundary for each image, and stops after LIMIT rows.
--
-- CONCURRENTLY cannot run inside a transaction: run each statement on its
-- own (psql does this by default). CameraMovementIndexService applies the
-- same statements at API startup, so running this by hand is optional.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sb_movieid_filename
    ON frl.frl_image_scene_boundaries (movieid, filename);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_images_live_weighted_score
    ON frl.frl_images (weighted_score DESC)
    WHERE status = 'live';
