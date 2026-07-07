-- Add a punctuation/case-insensitive normalized form of each word so searches
-- match regardless of apostrophes or casing (e.g. "lets go" matches "let's go").
--
-- The column is GENERATED STORED, so it is populated for all existing rows on
-- ALTER and stays in sync automatically on insert/update — no backfill needed.

ALTER TABLE frl.frl_transcript_words
    ADD COLUMN IF NOT EXISTS word_normalized VARCHAR(200)
    GENERATED ALWAYS AS (regexp_replace(lower(word), '[^a-z0-9]', '', 'g')) STORED;

CREATE INDEX IF NOT EXISTS idx_tw_word_normalized
    ON frl.frl_transcript_words (word_normalized);

CREATE INDEX IF NOT EXISTS idx_tw_movieid_word_normalized
    ON frl.frl_transcript_words (movieid, word_normalized);
