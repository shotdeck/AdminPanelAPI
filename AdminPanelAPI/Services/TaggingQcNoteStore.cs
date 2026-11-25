namespace AdminPanelAPI.Services
{
    /// <summary>
    /// QC notes on tagged titles: a reviewer leaves a note on a title, or on one
    /// of its frames and tag categories, and the title's tagger marks each one
    /// addressed, with an optional reply.
    /// </summary>
    public static class TaggingQcNoteStore
    {
        /// <summary>Mirrors migrations/062.</summary>
        public const string Schema = @"
CREATE TABLE IF NOT EXISTS frl.frl_tagging_qc_notes (
    id            BIGSERIAL     PRIMARY KEY,
    movie_id      INTEGER       NOT NULL,
    key_image_id  BIGINT,
    category      VARCHAR(80),
    note          TEXT          NOT NULL,
    created_by    VARCHAR(120),
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    reply         TEXT,
    addressed_by  VARCHAR(120),
    addressed_at  TIMESTAMPTZ
);
ALTER TABLE frl.frl_tagging_qc_notes
    ADD COLUMN IF NOT EXISTS position_seconds NUMERIC(10,3);
CREATE INDEX IF NOT EXISTS idx_ftqn_movie
    ON frl.frl_tagging_qc_notes (movie_id);";

        public const int MaxNoteLength = 4000;
    }
}
