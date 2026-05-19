using AdminPanelAPI.Models;
using Npgsql;

public interface ICaptionEmbeddingJobRepository
{
    Task<long> CreateJobAsync(int batchSize, CancellationToken cancellationToken);
    Task<CaptionEmbeddingJobStatusResponse?> GetJobAsync(long jobId, CancellationToken cancellationToken);
    Task MarkRunningAsync(long jobId, CancellationToken cancellationToken);
    Task MarkCompletedAsync(long jobId, CancellationToken cancellationToken);
    Task MarkFailedAsync(long jobId, string error, CancellationToken cancellationToken);

    Task UpdateProgressAsync(
        long jobId,
        string step,
        int? current,
        int? total,
        CancellationToken cancellationToken);

    Task<List<(int Idnum, string Filename, string? Randid)>> GetUnprocessedImageIdsAsync(
        int limit,
        CancellationToken cancellationToken);
}

public class CaptionEmbeddingJobRepository : ICaptionEmbeddingJobRepository
{
    private readonly string _connectionString;

    public CaptionEmbeddingJobRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("Default")
            ?? throw new InvalidOperationException("Missing connection string: Default");
    }

    public async Task<long> CreateJobAsync(int batchSize, CancellationToken cancellationToken)
    {
        const string sql = @"
CREATE TABLE IF NOT EXISTS frl.frl_caption_embedding_jobs (
    id BIGSERIAL PRIMARY KEY,
    batch_size INT NOT NULL,
    status TEXT NOT NULL DEFAULT 'Queued',
    current_step TEXT,
    progress_current INT,
    progress_total INT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT
);

INSERT INTO frl.frl_caption_embedding_jobs (batch_size, status)
VALUES (@batchSize, 'Queued')
RETURNING id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("batchSize", batchSize);

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(result);
    }

    public async Task<CaptionEmbeddingJobStatusResponse?> GetJobAsync(long jobId, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT
    id,
    batch_size,
    status,
    current_step,
    progress_current,
    progress_total,
    created_at,
    started_at,
    completed_at,
    error
FROM frl.frl_caption_embedding_jobs
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        if (await reader.ReadAsync(cancellationToken))
        {
            return new CaptionEmbeddingJobStatusResponse
            {
                JobId = reader.GetInt64(reader.GetOrdinal("id")),
                BatchSize = reader.GetInt32(reader.GetOrdinal("batch_size")),
                Status = reader.GetString(reader.GetOrdinal("status")),
                CurrentStep = reader.IsDBNull(reader.GetOrdinal("current_step"))
                    ? null : reader.GetString(reader.GetOrdinal("current_step")),
                ProgressCurrent = reader.IsDBNull(reader.GetOrdinal("progress_current"))
                    ? null : reader.GetInt32(reader.GetOrdinal("progress_current")),
                ProgressTotal = reader.IsDBNull(reader.GetOrdinal("progress_total"))
                    ? null : reader.GetInt32(reader.GetOrdinal("progress_total")),
                CreatedAt = reader.GetDateTime(reader.GetOrdinal("created_at")),
                StartedAt = reader.IsDBNull(reader.GetOrdinal("started_at"))
                    ? null : reader.GetDateTime(reader.GetOrdinal("started_at")),
                CompletedAt = reader.IsDBNull(reader.GetOrdinal("completed_at"))
                    ? null : reader.GetDateTime(reader.GetOrdinal("completed_at")),
                Error = reader.IsDBNull(reader.GetOrdinal("error"))
                    ? null : reader.GetString(reader.GetOrdinal("error")),
            };
        }

        return null;
    }

    public async Task MarkRunningAsync(long jobId, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_caption_embedding_jobs
SET status = 'Running', started_at = NOW()
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkCompletedAsync(long jobId, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_caption_embedding_jobs
SET status = 'Completed', completed_at = NOW()
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkFailedAsync(long jobId, string error, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_caption_embedding_jobs
SET status = 'Failed', error = @error, completed_at = NOW()
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);
        cmd.Parameters.AddWithValue("error", error);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpdateProgressAsync(
        long jobId,
        string step,
        int? current,
        int? total,
        CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_caption_embedding_jobs
SET current_step = @step,
    progress_current = @current,
    progress_total = @total
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);
        cmd.Parameters.AddWithValue("step", (object?)step ?? DBNull.Value);
        cmd.Parameters.AddWithValue("current", (object?)current ?? DBNull.Value);
        cmd.Parameters.AddWithValue("total", (object?)total ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<List<(int Idnum, string Filename, string? Randid)>> GetUnprocessedImageIdsAsync(
        int limit,
        CancellationToken cancellationToken)
    {
        if (limit <= 0)
            return new List<(int, string, string?)>();

        const string sql = @"
SELECT i.idnum, i.filename, i.randid
FROM frl.frl_images i
WHERE i.status = 'live'
  AND i.filename IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM frl.frl_caption_embeddings ce
      WHERE ce.idnum = i.idnum
  )
ORDER BY i.idnum
LIMIT @limit;";

        var results = new List<(int Idnum, string Filename, string? Randid)>();

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.CommandTimeout = 180;
        cmd.Parameters.AddWithValue("limit", limit);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add((
                reader.GetInt32(reader.GetOrdinal("idnum")),
                reader.GetString(reader.GetOrdinal("filename")),
                reader.IsDBNull(reader.GetOrdinal("randid"))
                    ? null : reader.GetString(reader.GetOrdinal("randid"))
            ));
        }

        return results;
    }
}
