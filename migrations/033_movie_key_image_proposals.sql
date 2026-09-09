-- Frames the analysis proposes live in the same table as the ones a tagger
-- picks by hand: a proposal the tagger keeps becomes a key image without
-- moving rows, and the grid is one query either way.
--
-- decision: proposed (waiting on the tagger) | kept | discarded
-- source:   ai (proposed by the analysis) | tagger (picked while watching)
ALTER TABLE frl.frl_movie_key_images
    ADD COLUMN IF NOT EXISTS frame_number INTEGER,
    ADD COLUMN IF NOT EXISTS source       VARCHAR(16)  NOT NULL DEFAULT 'tagger',
    ADD COLUMN IF NOT EXISTS decision     VARCHAR(16)  NOT NULL DEFAULT 'kept',
    ADD COLUMN IF NOT EXISTS score        NUMERIC(6,3),
    ADD COLUMN IF NOT EXISTS image_key    TEXT,
    ADD COLUMN IF NOT EXISTS decided_by   VARCHAR(120),
    ADD COLUMN IF NOT EXISTS decided_at   TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_fmki_movie_decision
    ON frl.frl_movie_key_images (movie_id, decision);
