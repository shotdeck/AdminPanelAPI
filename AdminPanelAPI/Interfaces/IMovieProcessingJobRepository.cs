using AdminPanelAPI.Models;
using Npgsql;

public interface IMovieProcessingJobRepository
{
    Task<long> CreateJobAsync(int movieId, double threshold, bool overwrite, bool missingOnly, CancellationToken cancellationToken);
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

    Task<List<int>> GetMovieIdsWithMissingClipsAsync(int limit, CancellationToken cancellationToken);

    Task<List<string>> GetBoundaryFilenamesAsync(int movieId, CancellationToken cancellationToken);

    Task<List<string>> GetLiveImageFilenamesAsync(int movieId, CancellationToken cancellationToken);

    Task<MovieMissingClipSummaryResponse> GetMissingClipSummaryAsync(int limit, CancellationToken cancellationToken);
}

public class MovieProcessingJobRepository : IMovieProcessingJobRepository
{
    private readonly string _connectionString;

    public MovieProcessingJobRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("Default")
    ?? throw new InvalidOperationException("Missing connection string: Default");
    }

    public async Task<long> CreateJobAsync(int movieId, double threshold, bool overwrite, bool missingOnly, CancellationToken cancellationToken)
    {
        const string sql = @"
INSERT INTO frl.frl_movie_processing_jobs (
    movieid,
    threshold,
    overwrite,
    missing_only,
    status
)
VALUES (
    @movieid,
    @threshold,
    @overwrite,
    @missing_only,
    'Queued'
)
RETURNING id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("movieid", movieId);
        cmd.Parameters.AddWithValue("threshold", threshold);
        cmd.Parameters.AddWithValue("overwrite", overwrite);
        cmd.Parameters.AddWithValue("missing_only", missingOnly);

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
        if (limit <= 0)
            return new List<int>();

        const string sql = @"
SELECT DISTINCT i.movieid
FROM frl.frl_images i
JOIN frl.frl_imagehistory h
  ON h.imageid = i.idnum
 AND h.action = 'Shot Time Autodetected'
WHERE i.status = 'live'
  AND i.movieid IS NOT NULL
  AND i.movieid > 0
  AND i.randid IS NOT NULL

  -- image does not already have a scene boundary
  AND NOT EXISTS (
      SELECT 1
      FROM frl.frl_image_scene_boundaries s
      WHERE s.movieid = i.movieid
        AND s.filename = i.randid
  )

  -- movie has never been queued / processed before
  AND NOT EXISTS (
      SELECT 1
      FROM frl.frl_movie_processing_jobs j
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
            if (reader.GetInt32(0) == 3277)
            {
                continue;
            }
            movieIds.Add(reader.GetInt32(0));
        }

        return movieIds;
    }

    public async Task<List<int>> GetMovieIdsWithMissingClipsAsync(
    int limit,
    CancellationToken cancellationToken)
    {
        if (limit <= 0)
            return new List<int>();

        const string sql = @"
SELECT DISTINCT i.movieid
FROM frl.frl_images i
JOIN frl.frl_imagehistory h
  ON h.imageid = i.idnum
 AND h.action = 'Shot Time Autodetected'
WHERE i.status = 'live'
  AND i.movieid IS NOT NULL
  AND i.movieid > 0
  AND i.randid IS NOT NULL

  -- image is still missing its scene boundary
  AND NOT EXISTS (
      SELECT 1
      FROM frl.frl_image_scene_boundaries s
      WHERE s.movieid = i.movieid
        AND s.filename = i.randid
  )

  -- but the movie has already been through the pipeline
  AND EXISTS (
      SELECT 1
      FROM frl.frl_movie_processing_jobs j
      WHERE j.movieid = i.movieid
        AND j.status = 'Completed'
  )

  -- and nothing is currently queued or running for it
  AND NOT EXISTS (
      SELECT 1
      FROM frl.frl_movie_processing_jobs j
      WHERE j.movieid = i.movieid
        AND j.status IN ('Queued', 'Running')
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

    public async Task<MovieMissingClipSummaryResponse> GetMissingClipSummaryAsync(
    int limit,
    CancellationToken cancellationToken)
    {
        const string sql = @"
WITH clips AS (
    SELECT DISTINCT i.movieid, i.randid
    FROM frl.frl_images i
    JOIN frl.frl_imagehistory h
      ON h.imageid = i.idnum
     AND h.action = 'Shot Time Autodetected'
    WHERE i.status = 'live'
      AND i.movieid IS NOT NULL
      AND i.movieid > 0
      AND i.randid IS NOT NULL
),
agg AS (
    SELECT
        c.movieid,
        COUNT(*) AS live_images,
        COUNT(*) FILTER (WHERE s.filename IS NULL) AS missing
    FROM clips c
    LEFT JOIN frl.frl_image_scene_boundaries s
      ON s.movieid = c.movieid
     AND s.filename = c.randid
    GROUP BY c.movieid
    HAVING COUNT(*) FILTER (WHERE s.filename IS NULL) > 0
)
SELECT
    a.movieid,
    a.live_images,
    a.missing,
    EXISTS (
        SELECT 1
        FROM frl.frl_movie_processing_jobs j
        WHERE j.movieid = a.movieid
          AND j.status = 'Completed'
    ) AS has_completed_job,
    (COUNT(*) OVER ())::int AS total_movies,
    (SUM(a.missing) OVER ())::bigint AS total_missing
FROM agg a
ORDER BY a.missing DESC, a.movieid
LIMIT @limit;";

        var response = new MovieMissingClipSummaryResponse();

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.CommandTimeout = 600;
        cmd.Parameters.AddWithValue("limit", limit <= 0 ? int.MaxValue : limit);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            response.TotalMovies = reader.GetInt32(4);
            response.TotalMissingBoundaries = reader.GetInt64(5);

            response.Movies.Add(new MovieMissingClipSummary
            {
                MovieId = reader.GetInt32(0),
                LiveImages = (int)reader.GetInt64(1),
                MissingBoundaries = (int)reader.GetInt64(2),
                HasCompletedJob = reader.GetBoolean(3)
            });
        }

        return response;
    }

    public async Task<List<string>> GetBoundaryFilenamesAsync(
    int movieId,
    CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT DISTINCT filename
FROM frl.frl_image_scene_boundaries
WHERE movieid = @movieid
  AND filename IS NOT NULL;";

        return await ReadFilenamesAsync(sql, movieId, cancellationToken);
    }

    public async Task<List<string>> GetLiveImageFilenamesAsync(
    int movieId,
    CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT DISTINCT i.randid
FROM frl.frl_images i
WHERE i.movieid = @movieid
  AND i.status = 'live'
  AND i.randid IS NOT NULL;";

        return await ReadFilenamesAsync(sql, movieId, cancellationToken);
    }

    private async Task<List<string>> ReadFilenamesAsync(
        string sql,
        int movieId,
        CancellationToken cancellationToken)
    {
        var filenames = new List<string>();

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.CommandTimeout = 180;
        cmd.Parameters.AddWithValue("movieid", movieId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            if (!reader.IsDBNull(0))
                filenames.Add(reader.GetString(0));
        }

        return filenames;
    }

    public async Task<MovieProcessingJobStatusResponse?> GetJobAsync(long jobId, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT
    id,
    movieid,
    threshold,
    overwrite,
    missing_only,
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
            MissingOnly = reader.GetBoolean(4),
            Status = reader.GetString(5),
            CurrentStep = reader.IsDBNull(6) ? null : reader.GetString(6),
            ProgressCurrent = reader.IsDBNull(7) ? null : reader.GetInt32(7),
            ProgressTotal = reader.IsDBNull(8) ? null : reader.GetInt32(8),
            CreatedAt = reader.GetDateTime(9),
            StartedAt = reader.IsDBNull(10) ? null : reader.GetDateTime(10),
            CompletedAt = reader.IsDBNull(11) ? null : reader.GetDateTime(11),
            Error = reader.IsDBNull(12) ? null : reader.GetString(12)
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