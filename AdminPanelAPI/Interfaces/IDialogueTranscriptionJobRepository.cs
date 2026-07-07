using AdminPanelAPI.Models;
using Npgsql;

public interface IDialogueTranscriptionJobRepository
{
    Task<long> CreateJobAsync(int movieId, string? r2Key, string? r2Url, CancellationToken cancellationToken);
    Task<DialogueTranscriptionJobStatusResponse?> GetJobAsync(long jobId, CancellationToken cancellationToken);
    Task MarkRunningAsync(long jobId, CancellationToken cancellationToken);
    Task MarkCompletedAsync(long jobId, int wordCount, CancellationToken cancellationToken);
    Task MarkFailedAsync(long jobId, string error, CancellationToken cancellationToken);
    Task UpdateProgressAsync(long jobId, string step, int progressPct, CancellationToken cancellationToken);
    Task<List<int>> GetUntranscribedMovieIdsAsync(int limit, CancellationToken cancellationToken);
    Task<List<int>> GetMovieIdsNeedingSegmentsAsync(int limit, CancellationToken cancellationToken);
    Task<List<int>> GetSegmentedMovieIdsAsync(int limit, CancellationToken cancellationToken);
    Task ClearSegmentMappingAsync(int movieId, CancellationToken cancellationToken);
}

public class DialogueTranscriptionJobRepository : IDialogueTranscriptionJobRepository
{
    private readonly string _connectionString;

    public DialogueTranscriptionJobRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("Default")
            ?? throw new InvalidOperationException("Missing connection string: Default");
    }

    public async Task<long> CreateJobAsync(int movieId, string? r2Key, string? r2Url, CancellationToken cancellationToken)
    {
        const string sql = @"
INSERT INTO frl.frl_dialogue_transcription_jobs (
    movieid,
    r2_key,
    r2_url,
    status
)
VALUES (
    @movieid,
    @r2_key,
    @r2_url,
    'Queued'
)
RETURNING id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("movieid", movieId);
        cmd.Parameters.AddWithValue("r2_key", (object?)r2Key ?? DBNull.Value);
        cmd.Parameters.AddWithValue("r2_url", (object?)r2Url ?? DBNull.Value);

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(result);
    }

    public async Task UpdateProgressAsync(long jobId, string step, int progressPct, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_dialogue_transcription_jobs
SET current_step = @step,
    progress_pct = @progress_pct
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);
        cmd.Parameters.AddWithValue("step", (object?)step ?? DBNull.Value);
        cmd.Parameters.AddWithValue("progress_pct", progressPct);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<List<int>> GetUntranscribedMovieIdsAsync(int limit, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT DISTINCT i.movieid
FROM frl.frl_images i
WHERE i.status = 'live'
  AND i.movieid IS NOT NULL
  AND i.movieid > 0
  AND NOT EXISTS (
      SELECT 1
      FROM frl.frl_dialogue_transcription_jobs j
      WHERE j.movieid = i.movieid
  )
ORDER BY i.movieid
LIMIT @limit;";

        var movieIds = new List<int>();

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.CommandTimeout = 180;
        cmd.Parameters.AddWithValue("limit", limit);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            movieIds.Add(reader.GetInt32(0));
        }

        return movieIds;
    }

    public async Task<List<int>> GetMovieIdsNeedingSegmentsAsync(int limit, CancellationToken cancellationToken)
    {
        // Movies that have transcript words but none mapped to a segment yet.
        const string sql = @"
SELECT movieid
FROM frl.frl_transcript_words
GROUP BY movieid
HAVING COUNT(*) FILTER (WHERE segment_index IS NOT NULL) = 0
ORDER BY movieid
LIMIT @limit;";

        var movieIds = new List<int>();

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.CommandTimeout = 180;
        cmd.Parameters.AddWithValue("limit", limit);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            movieIds.Add(reader.GetInt32(0));
        }

        return movieIds;
    }

    public async Task<List<int>> GetSegmentedMovieIdsAsync(int limit, CancellationToken cancellationToken)
    {
        // Movies whose words are already mapped to segments (for force re-segment).
        const string sql = @"
SELECT DISTINCT movieid
FROM frl.frl_transcript_words
WHERE segment_index IS NOT NULL
ORDER BY movieid
LIMIT @limit;";

        var movieIds = new List<int>();

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.CommandTimeout = 180;
        cmd.Parameters.AddWithValue("limit", limit);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            movieIds.Add(reader.GetInt32(0));
        }

        return movieIds;
    }

    public async Task ClearSegmentMappingAsync(int movieId, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_transcript_words
SET segment_index = NULL, segment_start = NULL
WHERE movieid = @movieid;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.CommandTimeout = 120;
        cmd.Parameters.AddWithValue("movieid", movieId);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<DialogueTranscriptionJobStatusResponse?> GetJobAsync(long jobId, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT
    id,
    movieid,
    status,
    current_step,
    progress_pct,
    word_count,
    r2_key,
    r2_url,
    created_at,
    started_at,
    completed_at,
    error
FROM frl.frl_dialogue_transcription_jobs
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
            return null;

        return new DialogueTranscriptionJobStatusResponse
        {
            JobId = reader.GetInt64(0),
            MovieId = reader.GetInt32(1),
            Status = reader.GetString(2),
            CurrentStep = reader.IsDBNull(3) ? null : reader.GetString(3),
            ProgressPct = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
            WordCount = reader.IsDBNull(5) ? null : reader.GetInt32(5),
            R2Key = reader.IsDBNull(6) ? null : reader.GetString(6),
            R2Url = reader.IsDBNull(7) ? null : reader.GetString(7),
            CreatedAt = reader.GetDateTime(8),
            StartedAt = reader.IsDBNull(9) ? null : reader.GetDateTime(9),
            CompletedAt = reader.IsDBNull(10) ? null : reader.GetDateTime(10),
            Error = reader.IsDBNull(11) ? null : reader.GetString(11)
        };
    }

    public async Task MarkRunningAsync(long jobId, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_dialogue_transcription_jobs
SET status = 'Running',
    started_at = now(),
    error = null
WHERE id = @id;";

        await ExecuteNonQueryAsync(sql, jobId, null, cancellationToken);
    }

    public async Task MarkCompletedAsync(long jobId, int wordCount, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_dialogue_transcription_jobs
SET status = 'Completed',
    completed_at = now(),
    word_count = @word_count,
    progress_pct = 100,
    error = null
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);
        cmd.Parameters.AddWithValue("word_count", wordCount);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkFailedAsync(long jobId, string error, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_dialogue_transcription_jobs
SET status = 'Failed',
    completed_at = now(),
    error = @error
WHERE id = @id;";

        await ExecuteNonQueryAsync(sql, jobId, error, cancellationToken);
    }

    private async Task ExecuteNonQueryAsync(string sql, long jobId, string? error, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);

        if (sql.Contains("@error"))
            cmd.Parameters.AddWithValue("error", (object?)error ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }
}
