-- Add segment mapping to transcript words for smooth clip playback.
-- Each movie is split into ~10s stream-copied segments stored in R2 under
-- segments/{movieid}/{segment_index:06d}.mp4. Because stream-copy cuts land on
-- keyframes, segments are ~10s but not exactly 10s, so we store the segment's
-- real start offset (segment_start, seconds) alongside its index. Playback
-- seeks (word.start_time - segment_start) inside the small segment file.

ALTER TABLE frl.frl_transcript_words
    ADD COLUMN IF NOT EXISTS segment_index INT,
    ADD COLUMN IF NOT EXISTS segment_start DOUBLE PRECISION;
