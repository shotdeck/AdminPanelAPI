using Npgsql;
using NpgsqlTypes;

namespace AdminPanelAPI.Services
{
    /// <summary>One week's attempt at retraining the image tagger.</summary>
    public sealed record TrainingRun(
        long Id,
        string Status,
        string? ModelVersion,
        int Frames,
        int Decisions,
        int Corrections,
        bool Promoted,
        string? Note,
        string? Report,
        DateTime StartedAt,
        DateTime? FinishedAt);

    /// <summary>
    /// What the weekly retraining did, kept so the dashboard can show it
    /// without anyone reading Modal's logs: how much human work went in, how
    /// the new model scored against the one in service, and whether it was
    /// good enough to take its place.
    /// </summary>
    public static class TrainingRunStore
    {
        public const string Running = "running";
        public const string Skipped = "skipped";
        public const string Trained = "trained";
        public const string Failed = "failed";

        /// <summary>Mirrors migrations/046.</summary>
        public const string Schema = @"
CREATE TABLE IF NOT EXISTS frl.frl_tagger_training_runs (
    id            BIGSERIAL    PRIMARY KEY,
    status        VARCHAR(16)  NOT NULL DEFAULT 'running',
    model_version VARCHAR(32),
    frames        INTEGER      NOT NULL DEFAULT 0,
    decisions     INTEGER      NOT NULL DEFAULT 0,
    corrections   INTEGER      NOT NULL DEFAULT 0,
    promoted      BOOLEAN      NOT NULL DEFAULT FALSE,
    note          TEXT,
    report        JSONB,
    started_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_fttr_started
    ON frl.frl_tagger_training_runs (started_at DESC);";

        public static async Task<long> StartAsync(
            NpgsqlConnection connection, int frames, int decisions, int corrections,
            CancellationToken ct)
        {
            const string sql = @"
INSERT INTO frl.frl_tagger_training_runs (status, frames, decisions, corrections)
VALUES ('running', @frames, @decisions, @corrections)
RETURNING id;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@frames", frames);
            cmd.Parameters.AddWithValue("@decisions", decisions);
            cmd.Parameters.AddWithValue("@corrections", corrections);
            return (long)(await cmd.ExecuteScalarAsync(ct))!;
        }

        public static async Task<bool> FinishAsync(
            NpgsqlConnection connection,
            long id,
            string status,
            string? modelVersion,
            bool promoted,
            string? note,
            string? report,
            CancellationToken ct)
        {
            const string sql = @"
UPDATE frl.frl_tagger_training_runs
SET status        = @status,
    model_version = @modelVersion,
    promoted      = @promoted,
    note          = @note,
    report        = @report,
    finished_at   = now()
WHERE id = @id;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.AddWithValue("@status", status);
            cmd.Parameters.AddWithValue("@modelVersion", (object?)modelVersion ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@promoted", promoted);
            cmd.Parameters.AddWithValue("@note", (object?)note ?? DBNull.Value);
            cmd.Parameters.Add(new NpgsqlParameter("@report", NpgsqlDbType.Jsonb)
            {
                Value = (object?)report ?? DBNull.Value
            });
            return await cmd.ExecuteNonQueryAsync(ct) > 0;
        }

        public static async Task<List<TrainingRun>> ListAsync(
            NpgsqlConnection connection, int limit, CancellationToken ct)
        {
            const string sql = @"
SELECT id, status, model_version, frames, decisions, corrections, promoted,
       note, report::text, started_at, finished_at
FROM frl.frl_tagger_training_runs
ORDER BY started_at DESC
LIMIT @limit;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@limit", limit);

            var rows = new List<TrainingRun>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                rows.Add(new TrainingRun(
                    reader.GetInt64(0),
                    reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2),
                    reader.GetInt32(3),
                    reader.GetInt32(4),
                    reader.GetInt32(5),
                    reader.GetBoolean(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7),
                    reader.IsDBNull(8) ? null : reader.GetString(8),
                    reader.GetDateTime(9),
                    reader.IsDBNull(10) ? null : reader.GetDateTime(10)));
            return rows;
        }
    }
}
