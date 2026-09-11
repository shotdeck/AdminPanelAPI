using Npgsql;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// One movie's automatic preparation: the walkthrough and the key image
    /// analysis a worker runs off the back of the SF proxy appearing, so that a
    /// tagger who finishes watching finds both already done.
    /// </summary>
    public sealed class MoviePreparationRow
    {
        public int MovieId { get; init; }
        public string SourceKey { get; init; } = "";

        public string? WalkthroughJobId { get; init; }
        public string WalkthroughStatus { get; init; } = "idle";
        public string? WalkthroughStage { get; init; }
        public double WalkthroughProgress { get; init; }
        public int? WalkthroughShots { get; init; }
        public string? WalkthroughError { get; init; }

        public string? StoryJobId { get; init; }
        public string StoryStatus { get; init; } = "idle";
        public string? StoryStage { get; init; }
        public double StoryProgress { get; init; }
        public int? StoryRated { get; init; }
        public string? StoryError { get; init; }

        public string? AnalysisJobId { get; init; }
        public string AnalysisStatus { get; init; } = "idle";
        public string? AnalysisStage { get; init; }
        public double AnalysisProgress { get; init; }
        public int? AnalysisProposals { get; init; }
        public string? AnalysisError { get; init; }

        public DateTimeOffset StartedAt { get; init; }
        public DateTimeOffset UpdatedAt { get; init; }

        public bool Running =>
            WalkthroughStatus == "running" ||
            StoryStatus == "running" ||
            AnalysisStatus == "running";
    }

    /// <summary>Reads and writes the preparation row for a movie.</summary>
    public static class MoviePreparationStore
    {
        public const string Running = "running";
        public const string Completed = "completed";
        public const string Error = "error";
        public const string Idle = "idle";

        /// <summary>
        /// Mirrors migrations/035 and 036, so a slot that has not had migrations run
        /// still works — the same approach the tagging tables take.
        /// </summary>
        public const string Schema = @"
CREATE TABLE IF NOT EXISTS frl.frl_movie_preparation (
    movie_id             INTEGER      PRIMARY KEY,
    source_key           TEXT         NOT NULL,
    walkthrough_job_id   VARCHAR(64),
    walkthrough_status   VARCHAR(16)  NOT NULL DEFAULT 'idle',
    walkthrough_stage    VARCHAR(32),
    walkthrough_progress NUMERIC(5,3) NOT NULL DEFAULT 0,
    walkthrough_shots    INTEGER,
    walkthrough_error    TEXT,
    analysis_job_id      VARCHAR(64),
    analysis_status      VARCHAR(16)  NOT NULL DEFAULT 'idle',
    analysis_stage       VARCHAR(32),
    analysis_progress    NUMERIC(5,3) NOT NULL DEFAULT 0,
    analysis_proposals   INTEGER,
    analysis_error       TEXT,
    started_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fmp_running
    ON frl.frl_movie_preparation (walkthrough_status, analysis_status);
ALTER TABLE frl.frl_movie_preparation
    ADD COLUMN IF NOT EXISTS story_job_id   VARCHAR(64),
    ADD COLUMN IF NOT EXISTS story_status   VARCHAR(16)  NOT NULL DEFAULT 'idle',
    ADD COLUMN IF NOT EXISTS story_stage    VARCHAR(32),
    ADD COLUMN IF NOT EXISTS story_progress NUMERIC(5,3) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS story_rated    INTEGER,
    ADD COLUMN IF NOT EXISTS story_error    TEXT;";

        private const string Columns = @"movie_id, source_key,
       walkthrough_job_id, walkthrough_status, walkthrough_stage,
       walkthrough_progress, walkthrough_shots, walkthrough_error,
       story_job_id, story_status, story_stage,
       story_progress, story_rated, story_error,
       analysis_job_id, analysis_status, analysis_stage,
       analysis_progress, analysis_proposals, analysis_error,
       started_at, updated_at";

        public static async Task EnsureTableAsync(NpgsqlConnection connection, CancellationToken ct)
        {
            await using var cmd = new NpgsqlCommand(Schema, connection);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public static async Task<MoviePreparationRow?> GetAsync(
            NpgsqlConnection connection, int movieId, CancellationToken ct)
        {
            var sql = $"SELECT {Columns} FROM frl.frl_movie_preparation WHERE movie_id = @movieId;";
            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            return await reader.ReadAsync(ct) ? Read(reader) : null;
        }

        /// <summary>Every movie with a job still in flight, for the worker's poll.</summary>
        public static async Task<List<MoviePreparationRow>> ListRunningAsync(
            NpgsqlConnection connection, CancellationToken ct)
        {
            var sql = $@"
SELECT {Columns} FROM frl.frl_movie_preparation
WHERE walkthrough_status = 'running'
   OR story_status = 'running'
   OR analysis_status = 'running'
ORDER BY started_at;";
            await using var cmd = new NpgsqlCommand(sql, connection);

            var rows = new List<MoviePreparationRow>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                rows.Add(Read(reader));
            return rows;
        }

        /// <summary>
        /// The preparation rows for a set of movies, for the tagging list's
        /// status column: one query rather than one request per row.
        /// </summary>
        public static async Task<List<MoviePreparationRow>> ListAsync(
            NpgsqlConnection connection, IEnumerable<int> movieIds, CancellationToken ct)
        {
            var ids = movieIds.Distinct().ToArray();
            var rows = new List<MoviePreparationRow>();
            if (ids.Length == 0) return rows;

            var sql = $@"
SELECT {Columns} FROM frl.frl_movie_preparation
WHERE movie_id = ANY(@ids) ORDER BY movie_id;";
            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@ids", ids);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                rows.Add(Read(reader));
            return rows;
        }

        /// <summary>
        /// Claim a movie for preparation, returning false when another worker (or
        /// an earlier pass) already has this SF in hand. Claiming and starting are
        /// separate steps, so the insert is what stops two runs of the same film:
        /// a row is only taken when it has no source key of this name in flight.
        /// </summary>
        public static async Task<bool> TryClaimAsync(
            NpgsqlConnection connection, int movieId, string sourceKey, CancellationToken ct)
        {
            const string sql = @"
INSERT INTO frl.frl_movie_preparation (movie_id, source_key, started_at, updated_at)
VALUES (@movieId, @sourceKey, now(), now())
ON CONFLICT (movie_id) DO UPDATE
    SET source_key           = EXCLUDED.source_key,
        walkthrough_job_id   = NULL,
        walkthrough_status   = 'idle',
        walkthrough_stage    = NULL,
        walkthrough_progress = 0,
        walkthrough_shots    = NULL,
        walkthrough_error    = NULL,
        story_job_id         = NULL,
        story_status         = 'idle',
        story_stage          = NULL,
        story_progress       = 0,
        story_rated          = NULL,
        story_error          = NULL,
        analysis_job_id      = NULL,
        analysis_status      = 'idle',
        analysis_stage       = NULL,
        analysis_progress    = 0,
        analysis_proposals   = NULL,
        analysis_error       = NULL,
        started_at           = now(),
        updated_at           = now()
    WHERE frl.frl_movie_preparation.source_key <> EXCLUDED.source_key
      AND frl.frl_movie_preparation.walkthrough_status <> 'running'
      AND frl.frl_movie_preparation.story_status <> 'running'
      AND frl.frl_movie_preparation.analysis_status <> 'running'
RETURNING movie_id;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            cmd.Parameters.AddWithValue("@sourceKey", sourceKey);
            return await cmd.ExecuteScalarAsync(ct) != null;
        }

        public static async Task SetJobAsync(
            NpgsqlConnection connection,
            int movieId,
            string job,
            string? jobId,
            string status,
            string? error,
            CancellationToken ct)
        {
            var sql = $@"
UPDATE frl.frl_movie_preparation
SET {job}_job_id = @jobId, {job}_status = @status, {job}_error = @error,
    {job}_progress = 0, updated_at = now()
WHERE movie_id = @movieId;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            cmd.Parameters.AddWithValue("@jobId", (object?)jobId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@status", status);
            cmd.Parameters.AddWithValue("@error", (object?)error ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public static async Task SetProgressAsync(
            NpgsqlConnection connection,
            int movieId,
            string job,
            string status,
            string? stage,
            double progress,
            int? count,
            string? error,
            CancellationToken ct)
        {
            var countColumn = job switch
            {
                "walkthrough" => "walkthrough_shots",
                "story" => "story_rated",
                _ => "analysis_proposals"
            };
            var sql = $@"
UPDATE frl.frl_movie_preparation
SET {job}_status = @status, {job}_stage = @stage, {job}_progress = @progress,
    {countColumn} = COALESCE(@count, {countColumn}),
    {job}_error = @error, updated_at = now()
WHERE movie_id = @movieId;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            cmd.Parameters.AddWithValue("@status", status);
            cmd.Parameters.AddWithValue("@stage", (object?)stage ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@progress", Math.Clamp(progress, 0, 1));
            cmd.Parameters.AddWithValue("@count", (object?)count ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@error", (object?)error ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        /// <summary>Movies already prepared from these SF keys, so a pass skips them.</summary>
        public static async Task<Dictionary<int, string>> KnownSourcesAsync(
            NpgsqlConnection connection, CancellationToken ct)
        {
            const string sql = "SELECT movie_id, source_key FROM frl.frl_movie_preparation;";
            await using var cmd = new NpgsqlCommand(sql, connection);

            var known = new Dictionary<int, string>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                known[reader.GetInt32(0)] = reader.GetString(1);
            return known;
        }

        private static MoviePreparationRow Read(NpgsqlDataReader reader) => new()
        {
            MovieId = reader.GetInt32(0),
            SourceKey = reader.GetString(1),
            WalkthroughJobId = reader.IsDBNull(2) ? null : reader.GetString(2),
            WalkthroughStatus = reader.GetString(3),
            WalkthroughStage = reader.IsDBNull(4) ? null : reader.GetString(4),
            WalkthroughProgress = (double)reader.GetDecimal(5),
            WalkthroughShots = reader.IsDBNull(6) ? null : reader.GetInt32(6),
            WalkthroughError = reader.IsDBNull(7) ? null : reader.GetString(7),
            StoryJobId = reader.IsDBNull(8) ? null : reader.GetString(8),
            StoryStatus = reader.GetString(9),
            StoryStage = reader.IsDBNull(10) ? null : reader.GetString(10),
            StoryProgress = (double)reader.GetDecimal(11),
            StoryRated = reader.IsDBNull(12) ? null : reader.GetInt32(12),
            StoryError = reader.IsDBNull(13) ? null : reader.GetString(13),
            AnalysisJobId = reader.IsDBNull(14) ? null : reader.GetString(14),
            AnalysisStatus = reader.GetString(15),
            AnalysisStage = reader.IsDBNull(16) ? null : reader.GetString(16),
            AnalysisProgress = (double)reader.GetDecimal(17),
            AnalysisProposals = reader.IsDBNull(18) ? null : reader.GetInt32(18),
            AnalysisError = reader.IsDBNull(19) ? null : reader.GetString(19),
            StartedAt = reader.GetFieldValue<DateTimeOffset>(20),
            UpdatedAt = reader.GetFieldValue<DateTimeOffset>(21)
        };
    }
}
