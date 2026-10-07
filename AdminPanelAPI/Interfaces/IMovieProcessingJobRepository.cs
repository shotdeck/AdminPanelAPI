using AdminPanelAPI.Models;
using Npgsql;

public interface IMovieProcessingJobRepository
{
    Task<long> CreateJobAsync(int movieId, double threshold, bool overwrite, bool missingOnly, CancellationToken cancellationToken);
    Task<MovieProcessingJobStatusResponse?> GetJobAsync(long jobId, CancellationToken cancellationToken);
    Task MarkRunningAsync(long jobId, CancellationToken cancellationToken);
    Task MarkCompletedAsync(long jobId, CancellationToken cancellationToken);
    Task MarkFailedAsync(long jobId, string error, CancellationToken cancellationToken);

    /// <summary>
    /// Moves the oldest queued job to Running and returns it, so a worker owns a
    /// job that outlives the process that created it. Returns null when nothing
    /// is queued.
    /// </summary>
    Task<MovieProcessingJobStatusResponse?> ClaimNextQueuedJobAsync(CancellationToken cancellationToken);

    /// <summary>Puts a job back in the queue, whatever state it is in.</summary>
    Task<bool> RequeueJobAsync(long jobId, CancellationToken cancellationToken);

    /// <summary>
    /// Requeues failed jobs and/or jobs left Running by a crashed worker, which
    /// no worker would otherwise touch again.
    /// </summary>
    Task<MovieJobRequeueResult> RequeueJobsAsync(
        bool includeFailed,
        int? staleRunningMinutes,
        int limit,
        bool dryRun,
        CancellationToken cancellationToken);

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

    Task<List<ClipPreviewBoundary>> GetSceneBoundariesAsync(int movieId, CancellationToken cancellationToken);

    Task<List<int>> GetMovieIdsWithSceneBoundariesAsync(int limit, int afterMovieId, CancellationToken cancellationToken);

    Task<List<ClipPreviewBoundary>> GetPreviewCandidateBoundariesAsync(int limit, int afterImageId, bool motionOnly, CancellationToken cancellationToken);

    Task<ClipPreviewMotionCounts> GetPreviewCandidateCountsAsync(int afterImageId, int? movieId, bool motionOnly, CancellationToken cancellationToken);

    Task<MovieMissingClipSummaryResponse> GetMissingClipSummaryAsync(int limit, CancellationToken cancellationToken);

    /// <summary>
    /// Records where the clip preview run has got to, so a recycled app picks the
    /// run up again instead of leaving the backfill half done.
    /// </summary>
    Task SaveClipPreviewRunAsync(ClipPreviewRunStatus status, CancellationToken cancellationToken);

    /// <summary>The stored run, or null when no run has ever been recorded.</summary>
    Task<ClipPreviewRunStatus?> GetClipPreviewRunAsync(CancellationToken cancellationToken);
}

public class MovieProcessingJobRepository : IMovieProcessingJobRepository
{
    /// <summary>Limits preview candidates to images a Motion filter can select.</summary>
    private const string MotionTagPredicate = @"
  AND EXISTS (
        SELECT 1
        FROM frl.frl_join_images_camera_movements cm
        WHERE cm.imageid = i.idnum)";

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

    public async Task<List<ClipPreviewBoundary>> GetSceneBoundariesAsync(
    int movieId,
    CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT filename, start_time, end_time
FROM frl.frl_image_scene_boundaries
WHERE movieid = @movieid
  AND filename IS NOT NULL
  AND start_time IS NOT NULL
  AND end_time IS NOT NULL
  AND end_time > start_time;";

        var boundaries = new List<ClipPreviewBoundary>();

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.CommandTimeout = 180;
        cmd.Parameters.AddWithValue("movieid", movieId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            boundaries.Add(new ClipPreviewBoundary
            {
                MovieId = movieId,
                Filename = reader.GetString(0),
                StartTime = reader.GetDouble(1),
                EndTime = reader.GetDouble(2)
            });
        }

        return boundaries;
    }

    public async Task<List<ClipPreviewBoundary>> GetPreviewCandidateBoundariesAsync(
    int limit,
    int afterImageId,
    bool motionOnly,
    CancellationToken cancellationToken)
    {
        if (limit <= 0)
            return new List<ClipPreviewBoundary>();

        var sql = $@"
SELECT i.idnum, i.movieid, i.randid, sb.start_time, sb.end_time
FROM frl.frl_images i
JOIN frl.frl_image_scene_boundaries sb
  ON sb.movieid = i.movieid
 AND sb.filename = i.randid
WHERE i.idnum > @after_idnum
  AND i.status = 'live'
  AND sb.start_time IS NOT NULL
  AND sb.end_time IS NOT NULL
  AND sb.end_time > sb.start_time
  {(motionOnly ? MotionTagPredicate : "")}
ORDER BY i.idnum
LIMIT @limit;";

        var boundaries = new List<ClipPreviewBoundary>();

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.CommandTimeout = 300;
        cmd.Parameters.AddWithValue("after_idnum", afterImageId);
        cmd.Parameters.AddWithValue("limit", limit);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            boundaries.Add(new ClipPreviewBoundary
            {
                ImageId = reader.GetInt32(0),
                MovieId = reader.GetInt32(1),
                Filename = reader.GetString(2),
                StartTime = reader.GetDouble(3),
                EndTime = reader.GetDouble(4)
            });
        }

        return boundaries;
    }

    public async Task<ClipPreviewMotionCounts> GetPreviewCandidateCountsAsync(
    int afterImageId,
    int? movieId,
    bool motionOnly,
    CancellationToken cancellationToken)
    {
        var sql = $@"
SELECT count(*) AS total,
       count(*) FILTER (WHERE i.idnum <= @after_idnum) AS at_or_before_cursor,
       min(i.idnum) AS first_idnum,
       max(i.idnum) AS last_idnum
FROM frl.frl_images i
JOIN frl.frl_image_scene_boundaries sb
  ON sb.movieid = i.movieid
 AND sb.filename = i.randid
WHERE i.status = 'live'
  AND sb.start_time IS NOT NULL
  AND sb.end_time IS NOT NULL
  AND sb.end_time > sb.start_time
  AND (@movieid::int IS NULL OR i.movieid = @movieid)
  {(motionOnly ? MotionTagPredicate : "")};";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.CommandTimeout = 300;
        cmd.Parameters.AddWithValue("after_idnum", afterImageId);
        cmd.Parameters.AddWithValue("movieid", movieId.HasValue ? movieId.Value : DBNull.Value);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
            return new ClipPreviewMotionCounts();

        return new ClipPreviewMotionCounts
        {
            Total = reader.GetInt64(0),
            AtOrBeforeCursor = reader.GetInt64(1),
            FirstImageId = reader.IsDBNull(2) ? null : reader.GetInt32(2),
            LastImageId = reader.IsDBNull(3) ? null : reader.GetInt32(3)
        };
    }

    public async Task<List<int>> GetMovieIdsWithSceneBoundariesAsync(
    int limit,
    int afterMovieId,
    CancellationToken cancellationToken)
    {
        if (limit <= 0)
            return new List<int>();

        const string sql = @"
SELECT DISTINCT movieid
FROM frl.frl_image_scene_boundaries
WHERE movieid > @after_movieid
  AND start_time IS NOT NULL
  AND end_time IS NOT NULL
ORDER BY movieid
LIMIT @limit;";

        var movieIds = new List<int>();

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.CommandTimeout = 180;
        cmd.Parameters.AddWithValue("after_movieid", afterMovieId);
        cmd.Parameters.AddWithValue("limit", limit);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            movieIds.Add(reader.GetInt32(0));
        }

        return movieIds;
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

        return ReadJob(reader);
    }

    private static MovieProcessingJobStatusResponse ReadJob(NpgsqlDataReader reader)
    {
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

    public async Task<MovieProcessingJobStatusResponse?> ClaimNextQueuedJobAsync(CancellationToken cancellationToken)
    {
        // SKIP LOCKED keeps concurrent workers, and concurrent app instances, off
        // each other's job.
        const string sql = @"
UPDATE frl.frl_movie_processing_jobs
SET status = 'Running',
    started_at = now(),
    error = null
WHERE id = (
    SELECT id
    FROM frl.frl_movie_processing_jobs
    WHERE status = 'Queued'
    ORDER BY id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING
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
    error;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
            return null;

        return ReadJob(reader);
    }

    public async Task<bool> RequeueJobAsync(long jobId, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_movie_processing_jobs
SET status = 'Queued',
    started_at = null,
    completed_at = null,
    current_step = null,
    progress_current = null,
    progress_total = null,
    error = null
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);

        return await cmd.ExecuteNonQueryAsync(cancellationToken) > 0;
    }

    public async Task<MovieJobRequeueResult> RequeueJobsAsync(
        bool includeFailed,
        int? staleRunningMinutes,
        int limit,
        bool dryRun,
        CancellationToken cancellationToken)
    {
        const string selectSql = @"
SELECT id, status, error
FROM frl.frl_movie_processing_jobs
WHERE (@include_failed AND status = 'Failed')
   OR (@stale_minutes::int IS NOT NULL
       AND status = 'Running'
       AND started_at IS NOT NULL
       AND started_at < now() - make_interval(mins => @stale_minutes))
ORDER BY id
LIMIT @limit;";

        const string updateSql = @"
UPDATE frl.frl_movie_processing_jobs
SET status = 'Queued',
    started_at = null,
    completed_at = null,
    current_step = null,
    progress_current = null,
    progress_total = null,
    error = null
WHERE id = ANY(@ids);";

        var result = new MovieJobRequeueResult { DryRun = dryRun };
        var errorCounts = new Dictionary<string, int>();

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using (var cmd = new NpgsqlCommand(selectSql, conn))
        {
            cmd.CommandTimeout = 180;
            cmd.Parameters.AddWithValue("include_failed", includeFailed);
            cmd.Parameters.AddWithValue(
                "stale_minutes",
                staleRunningMinutes.HasValue ? staleRunningMinutes.Value : DBNull.Value);
            cmd.Parameters.AddWithValue("limit", limit);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                result.JobIds.Add(reader.GetInt64(0));

                if (reader.GetString(1) == "Failed")
                {
                    result.Failed++;

                    var error = reader.IsDBNull(2) ? "(none)" : reader.GetString(2);
                    errorCounts[error] = errorCounts.GetValueOrDefault(error) + 1;
                }
                else
                {
                    result.StaleRunning++;
                }
            }
        }

        result.Errors = errorCounts
            .OrderByDescending(entry => entry.Value)
            .Select(entry => new MovieJobRequeueErrorGroup
            {
                Error = entry.Key,
                Count = entry.Value
            })
            .ToList();

        if (dryRun || result.JobIds.Count == 0)
            return result;

        await using (var cmd = new NpgsqlCommand(updateSql, conn))
        {
            cmd.CommandTimeout = 180;
            cmd.Parameters.AddWithValue("ids", result.JobIds.ToArray());

            result.Requeued = await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        return result;
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

    /// <summary>Mirrors migrations/049.</summary>
    private const string ClipPreviewRunSchema = @"
CREATE TABLE IF NOT EXISTS frl.frl_clip_preview_run (
    id                     INTEGER      PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    running                BOOLEAN      NOT NULL DEFAULT FALSE,
    started_after_image_id INTEGER      NOT NULL DEFAULT 0,
    batch_size             INTEGER      NOT NULL DEFAULT 2000,
    overwrite              BOOLEAN      NOT NULL DEFAULT FALSE,
    motion_only            BOOLEAN      NOT NULL DEFAULT TRUE,
    cursor_image_id        INTEGER      NOT NULL DEFAULT 0,
    batches                INTEGER      NOT NULL DEFAULT 0,
    requested              BIGINT       NOT NULL DEFAULT 0,
    created                BIGINT       NOT NULL DEFAULT 0,
    exists_count           BIGINT       NOT NULL DEFAULT 0,
    skipped                BIGINT       NOT NULL DEFAULT 0,
    errors                 BIGINT       NOT NULL DEFAULT 0,
    completed_all          BOOLEAN      NOT NULL DEFAULT FALSE,
    last_error             TEXT,
    last_error_at          TIMESTAMPTZ,
    started_at             TIMESTAMPTZ,
    finished_at            TIMESTAMPTZ,
    updated_at             TIMESTAMPTZ  NOT NULL DEFAULT now()
);";

    private static int _clipPreviewRunTableReady;

    public async Task SaveClipPreviewRunAsync(ClipPreviewRunStatus status, CancellationToken cancellationToken)
    {
        const string sql = @"
INSERT INTO frl.frl_clip_preview_run (
    id, running, started_after_image_id, batch_size, overwrite, motion_only,
    cursor_image_id, batches, requested, created, exists_count, skipped, errors,
    completed_all, last_error, last_error_at, started_at, finished_at, updated_at)
VALUES (
    1, @running, @startedAfter, @batchSize, @overwrite, @motionOnly,
    @cursor, @batches, @requested, @created, @exists, @skipped, @errors,
    @completedAll, @lastError, @lastErrorAt, @startedAt, @finishedAt, now())
ON CONFLICT (id) DO UPDATE SET
    running                = excluded.running,
    started_after_image_id = excluded.started_after_image_id,
    batch_size             = excluded.batch_size,
    overwrite              = excluded.overwrite,
    motion_only            = excluded.motion_only,
    cursor_image_id        = excluded.cursor_image_id,
    batches                = excluded.batches,
    requested              = excluded.requested,
    created                = excluded.created,
    exists_count           = excluded.exists_count,
    skipped                = excluded.skipped,
    errors                 = excluded.errors,
    completed_all          = excluded.completed_all,
    last_error             = excluded.last_error,
    last_error_at          = excluded.last_error_at,
    started_at             = excluded.started_at,
    finished_at            = excluded.finished_at,
    updated_at             = now();";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await EnsureClipPreviewRunTableAsync(conn, cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("running", status.Running);
        cmd.Parameters.AddWithValue("startedAfter", status.StartedAfterImageId);
        cmd.Parameters.AddWithValue("batchSize", status.BatchSize);
        cmd.Parameters.AddWithValue("overwrite", status.Overwrite);
        cmd.Parameters.AddWithValue("motionOnly", status.MotionOnly);
        cmd.Parameters.AddWithValue("cursor", status.Cursor);
        cmd.Parameters.AddWithValue("batches", status.Batches);
        cmd.Parameters.AddWithValue("requested", (long)status.Requested);
        cmd.Parameters.AddWithValue("created", (long)status.Created);
        cmd.Parameters.AddWithValue("exists", (long)status.Exists);
        cmd.Parameters.AddWithValue("skipped", (long)status.Skipped);
        cmd.Parameters.AddWithValue("errors", (long)status.Errors);
        cmd.Parameters.AddWithValue("completedAll", status.CompletedAll);
        cmd.Parameters.AddWithValue("lastError", (object?)status.LastError ?? DBNull.Value);
        cmd.Parameters.AddWithValue("lastErrorAt", (object?)ToUtc(status.LastErrorAtUtc) ?? DBNull.Value);
        cmd.Parameters.AddWithValue("startedAt", (object?)ToUtc(status.StartedAtUtc) ?? DBNull.Value);
        cmd.Parameters.AddWithValue("finishedAt", (object?)ToUtc(status.FinishedAtUtc) ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<ClipPreviewRunStatus?> GetClipPreviewRunAsync(CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT running, started_after_image_id, batch_size, overwrite, motion_only,
       cursor_image_id, batches, requested, created, exists_count, skipped, errors,
       completed_all, last_error, last_error_at, started_at, finished_at
FROM frl.frl_clip_preview_run
WHERE id = 1;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await EnsureClipPreviewRunTableAsync(conn, cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
            return null;

        return new ClipPreviewRunStatus
        {
            Running = reader.GetBoolean(0),
            StartedAfterImageId = reader.GetInt32(1),
            BatchSize = reader.GetInt32(2),
            Overwrite = reader.GetBoolean(3),
            MotionOnly = reader.GetBoolean(4),
            Cursor = reader.GetInt32(5),
            Batches = reader.GetInt32(6),
            Requested = (int)reader.GetInt64(7),
            Created = (int)reader.GetInt64(8),
            Exists = (int)reader.GetInt64(9),
            Skipped = (int)reader.GetInt64(10),
            Errors = (int)reader.GetInt64(11),
            CompletedAll = reader.GetBoolean(12),
            LastError = reader.IsDBNull(13) ? null : reader.GetString(13),
            LastErrorAtUtc = reader.IsDBNull(14) ? null : reader.GetDateTime(14),
            StartedAtUtc = reader.IsDBNull(15) ? null : reader.GetDateTime(15),
            FinishedAtUtc = reader.IsDBNull(16) ? null : reader.GetDateTime(16)
        };
    }

    private static DateTime? ToUtc(DateTime? value) =>
        value.HasValue ? DateTime.SpecifyKind(value.Value, DateTimeKind.Utc) : null;

    private static async Task EnsureClipPreviewRunTableAsync(
        NpgsqlConnection conn, CancellationToken cancellationToken)
    {
        if (Interlocked.CompareExchange(ref _clipPreviewRunTableReady, 1, 1) == 1)
            return;

        await using (var cmd = new NpgsqlCommand(ClipPreviewRunSchema, conn))
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        Interlocked.Exchange(ref _clipPreviewRunTableReady, 1);
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