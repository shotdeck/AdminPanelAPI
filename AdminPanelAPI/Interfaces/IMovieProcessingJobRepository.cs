using AdminPanelAPI.Models;
using Npgsql;

public interface IMovieProcessingJobRepository
{
    Task<long> CreateJobAsync(int movieId, double threshold, bool overwrite, CancellationToken cancellationToken);
    Task<MovieProcessingJobStatusResponse?> GetJobAsync(long jobId, CancellationToken cancellationToken);
    Task MarkRunningAsync(long jobId, CancellationToken cancellationToken);
    Task MarkCompletedAsync(long jobId, CancellationToken cancellationToken);
    Task MarkFailedAsync(long jobId, string error, CancellationToken cancellationToken);

    Task UpdateProgressAsync(
        long jobId,
        string step,
        int? current,
        int? total,
        CancellationToken cancellationToken);

    Task<List<int>> GetUnprocessedMovieIdsAsync(int limit, CancellationToken cancellationToken);
}

public class MovieProcessingJobRepository : IMovieProcessingJobRepository
{
    private readonly string _connectionString;

    public MovieProcessingJobRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("Default")
    ?? throw new InvalidOperationException("Missing connection string: Default");
    }

    public async Task<long> CreateJobAsync(int movieId, double threshold, bool overwrite, CancellationToken cancellationToken)
    {
        const string sql = @"
INSERT INTO frl.frl_movie_processing_jobs (
    movieid,
    threshold,
    overwrite,
    status
)
VALUES (
    @movieid,
    @threshold,
    @overwrite,
    'Queued'
)
RETURNING id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("movieid", movieId);
        cmd.Parameters.AddWithValue("threshold", threshold);
        cmd.Parameters.AddWithValue("overwrite", overwrite);

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(result);
    }

    public async Task UpdateProgressAsync(
    long jobId,
    string step,
    int? current,
    int? total,
    CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_movie_processing_jobs
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

    public async Task<List<int>> GetUnprocessedMovieIdsAsync(
      int limit,
      CancellationToken cancellationToken)
    {
        var results = new List<int>();
        var offset = 0;
        var batchSize = 5000;
        var maxScanned = 150000;

        const string sql = @"
WITH candidate_movies AS (
    SELECT m.idnum
    FROM frl.frl_movies m
    WHERE m.idnum > 0
      AND NOT EXISTS (
          SELECT 1
          FROM frl.frl_movie_processing_jobs j
          WHERE j.movieid = m.idnum
      )
    ORDER BY m.idnum
    LIMIT @batchSize OFFSET @offset
),
matching_movies AS (
    SELECT DISTINCT i.movieid
    FROM frl.frl_images i
    JOIN candidate_movies cm
      ON cm.idnum = i.movieid
    JOIN frl.frl_imagehistory h
      ON h.imageid = i.idnum
     AND h.action = 'Shot Time Autodetected'
    LEFT JOIN frl.frl_image_scene_boundaries s
      ON s.movieid = i.movieid
     AND s.filename = i.randid
    WHERE i.status = 'live'
      AND i.randid IS NOT NULL
      AND s.filename IS NULL
)
SELECT movieid
FROM matching_movies
ORDER BY movieid;";

        const string countSql = @"
SELECT COUNT(*)
FROM (
    SELECT m.idnum
    FROM frl.frl_movies m
    WHERE m.idnum > 0
      AND NOT EXISTS (
          SELECT 1
          FROM frl.frl_movie_processing_jobs j
          WHERE j.movieid = m.idnum
      )
    ORDER BY m.idnum
    LIMIT @batchSize OFFSET @offset
) x;";

        while (results.Count < limit && offset < maxScanned)
        {
            var foundThisBatch = 0;

            await using (var conn = new NpgsqlConnection(_connectionString))
            {
                await conn.OpenAsync(cancellationToken);

                await using var cmd = new NpgsqlCommand(sql, conn);
                cmd.CommandTimeout = 180;
                cmd.Parameters.AddWithValue("batchSize", batchSize);
                cmd.Parameters.AddWithValue("offset", offset);

                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

                while (await reader.ReadAsync(cancellationToken))
                {
                    var movieId = reader.GetInt32(0);

                    if (!results.Contains(movieId))
                    {
                        results.Add(movieId);
                        foundThisBatch++;
                    }

                    if (results.Count >= limit)
                        break;
                }
            }

            if (foundThisBatch == 0)
            {
                int candidateCount;

                await using (var countConn = new NpgsqlConnection(_connectionString))
                {
                    await countConn.OpenAsync(cancellationToken);

                    await using var countCmd = new NpgsqlCommand(countSql, countConn);
                    countCmd.CommandTimeout = 180;
                    countCmd.Parameters.AddWithValue("batchSize", batchSize);
                    countCmd.Parameters.AddWithValue("offset", offset);

                    candidateCount = Convert.ToInt32(
                        await countCmd.ExecuteScalarAsync(cancellationToken));
                }

                if (candidateCount == 0)
                    break;
            }

            offset += batchSize;
        }

        return results
            .Distinct()
            .OrderBy(x => x)
            .Take(limit)
            .ToList();
    }
    public async Task<MovieProcessingJobStatusResponse?> GetJobAsync(long jobId, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT
    id,
    movieid,
    threshold,
    overwrite,
    status,
    current_step,
    progress_current,
    progress_total,
    created_at,
    started_at,
    completed_at,
    error
FROM frl.frl_movie_processing_jobs
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
            return null;

        return new MovieProcessingJobStatusResponse
        {
            JobId = reader.GetInt64(0),
            MovieId = reader.GetInt32(1),
            Threshold = reader.GetDouble(2),
            Overwrite = reader.GetBoolean(3),
            Status = reader.GetString(4),
            CurrentStep = reader.IsDBNull(5) ? null : reader.GetString(5),
            ProgressCurrent = reader.IsDBNull(6) ? null : reader.GetInt32(6),
            ProgressTotal = reader.IsDBNull(7) ? null : reader.GetInt32(7),
            CreatedAt = reader.GetDateTime(8),
            StartedAt = reader.IsDBNull(9) ? null : reader.GetDateTime(9),
            CompletedAt = reader.IsDBNull(10) ? null : reader.GetDateTime(10),
            Error = reader.IsDBNull(11) ? null : reader.GetString(11)
        };
    }

    public async Task MarkRunningAsync(long jobId, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_movie_processing_jobs
SET status = 'Running',
    started_at = now(),
    error = null
WHERE id = @id;";

        await ExecuteNonQueryAsync(sql, jobId, null, cancellationToken);
    }

    public async Task MarkCompletedAsync(long jobId, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_movie_processing_jobs
SET status = 'Completed',
    completed_at = now(),
    error = null
WHERE id = @id;";

        await ExecuteNonQueryAsync(sql, jobId, null, cancellationToken);
    }

    public async Task MarkFailedAsync(long jobId, string error, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_movie_processing_jobs
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