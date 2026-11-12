using AdminPanelAPI.VideoLinkCheck.Models;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace AdminPanelAPI.VideoLinkCheck.Services;

/// <summary>
/// Link-check jobs (<c>frl.frl_video_link_check_jobs</c>) and their per-movie results
/// (<c>frl.frl_video_link_check_results</c>). Both App Service slots run the worker, so a job
/// belongs to the instance in its <c>claimed_by</c>; one whose heartbeat (updated_at) is older
/// than <see cref="StaleMinutes"/> is taken over and resumed after its last id.
/// frl_movies / frl_images are only read.
/// </summary>
public sealed class VideoLinkCheckStore
{
    public const int StaleMinutes = 15;
    private const string Jobs = "frl.frl_video_link_check_jobs";
    private const string Results = "frl.frl_video_link_check_results";
    private const string LockKey = "frl_video_link_check_jobs";

    private readonly string _connectionString;
    private readonly IOptionsMonitor<VideoLinkCheckOptions> _options;

    public VideoLinkCheckStore(IConfiguration configuration, IOptionsMonitor<VideoLinkCheckOptions> options)
    {
        _connectionString = configuration.GetConnectionString("Default")
            ?? throw new InvalidOperationException("Missing connection string: Default");
        _options = options;
    }

    private async Task<NpgsqlConnection> OpenAsync(CancellationToken ct)
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        return conn;
    }

    public const string Schema = $@"
CREATE TABLE IF NOT EXISTS {Jobs} (
    id                BIGSERIAL    PRIMARY KEY,
    status            VARCHAR(20)  NOT NULL DEFAULT 'queued',
    triggered_by      VARCHAR(40)  NOT NULL DEFAULT 'manual',
    email             BOOLEAN      NOT NULL DEFAULT TRUE,
    email_to          TEXT,
    last_id           BIGINT       NOT NULL DEFAULT 0,
    scanned           BIGINT       NOT NULL DEFAULT 0,
    active            BIGINT       NOT NULL DEFAULT 0,
    unavailable       BIGINT       NOT NULL DEFAULT 0,
    unknown_platform  BIGINT       NOT NULL DEFAULT 0,
    errors            BIGINT       NOT NULL DEFAULT 0,
    newly_unavailable BIGINT       NOT NULL DEFAULT 0,
    previous_job_id   BIGINT,
    claimed_by        TEXT,
    email_status      TEXT,
    error             TEXT,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    started_at        TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_vlcj_status ON {Jobs} (status);
CREATE TABLE IF NOT EXISTS {Results} (
    job_id      BIGINT       NOT NULL REFERENCES {Jobs} (id) ON DELETE CASCADE,
    movie_id    BIGINT       NOT NULL,
    title       TEXT         NOT NULL DEFAULT '',
    media_type  TEXT         NOT NULL DEFAULT '',
    url         TEXT         NOT NULL DEFAULT '',
    platform    VARCHAR(20)  NOT NULL DEFAULT '',
    video_id    TEXT         NOT NULL DEFAULT '',
    status      VARCHAR(30)  NOT NULL,
    note        TEXT,
    checked_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, movie_id)
);";

    public async Task EnsureTablesAsync(CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(Schema, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private const string Columns = @"id, status, triggered_by, email, email_to, last_id, scanned, active, unavailable,
unknown_platform, errors, newly_unavailable, previous_job_id, email_status, error, created_at, started_at, updated_at, finished_at";

    private static VideoLinkCheckJob Read(NpgsqlDataReader r) => new()
    {
        Id = r.GetInt64(0),
        Status = r.GetString(1),
        TriggeredBy = r.GetString(2),
        Email = r.GetBoolean(3),
        EmailTo = r.IsDBNull(4) ? null : r.GetString(4),
        LastId = r.GetInt64(5),
        Scanned = r.GetInt64(6),
        Active = r.GetInt64(7),
        Unavailable = r.GetInt64(8),
        UnknownPlatform = r.GetInt64(9),
        Errors = r.GetInt64(10),
        NewlyUnavailable = r.GetInt64(11),
        PreviousJobId = r.IsDBNull(12) ? null : r.GetInt64(12),
        EmailStatus = r.IsDBNull(13) ? null : r.GetString(13),
        Error = r.IsDBNull(14) ? null : r.GetString(14),
        CreatedAt = r.GetDateTime(15),
        StartedAt = r.IsDBNull(16) ? null : r.GetDateTime(16),
        UpdatedAt = r.GetDateTime(17),
        FinishedAt = r.IsDBNull(18) ? null : r.GetDateTime(18),
    };

    private static async Task LockAsync(NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT pg_advisory_xact_lock(hashtext(@k))", conn, tx);
        cmd.Parameters.AddWithValue("k", LockKey);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Queues a check unless one is already queued or running; returns that one's id instead.</summary>
    public async Task<(long JobId, bool Created)> CreateJobAsync(string triggeredBy, bool email, string? emailTo, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await LockAsync(conn, tx, ct);
        await using (var active = new NpgsqlCommand(
            $"SELECT id FROM {Jobs} WHERE status IN ('queued', 'running') ORDER BY id LIMIT 1", conn, tx))
        {
            if (await active.ExecuteScalarAsync(ct) is long existing)
                return (existing, false);
        }
        await using var cmd = new NpgsqlCommand(
            $"INSERT INTO {Jobs} (triggered_by, email, email_to) VALUES (@t, @e, @to) RETURNING id", conn, tx);
        cmd.Parameters.AddWithValue("t", triggeredBy.Length > 40 ? triggeredBy[..40] : triggeredBy);
        cmd.Parameters.AddWithValue("e", email);
        cmd.Parameters.Add(new NpgsqlParameter("to", NpgsqlDbType.Text) { Value = (object?)emailTo ?? DBNull.Value });
        var id = Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
        await tx.CommitAsync(ct);
        return (id, true);
    }

    public async Task<VideoLinkCheckJob?> GetJobAsync(long id, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($"SELECT {Columns} FROM {Jobs} WHERE id = @id", conn);
        cmd.Parameters.AddWithValue("id", id);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        return await r.ReadAsync(ct) ? Read(r) : null;
    }

    public async Task<List<VideoLinkCheckJob>> ListJobsAsync(int limit, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($"SELECT {Columns} FROM {Jobs} ORDER BY id DESC LIMIT @l", conn);
        cmd.Parameters.AddWithValue("l", Math.Clamp(limit, 1, 500));
        var jobs = new List<VideoLinkCheckJob>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct)) jobs.Add(Read(r));
        return jobs;
    }

    public async Task<bool> CancelAsync(long id, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($@"
UPDATE {Jobs} SET status = 'cancelled', finished_at = now(), updated_at = now()
WHERE id = @id AND status IN ('queued', 'running') RETURNING id", conn);
        cmd.Parameters.AddWithValue("id", id);
        return await cmd.ExecuteScalarAsync(ct) != null;
    }

    /// <summary>Re-queues a failed or cancelled job to carry on after its last id, unless another is queued or running.</summary>
    public async Task<bool> ResumeAsync(long id, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await LockAsync(conn, tx, ct);
        await using var cmd = new NpgsqlCommand($@"
UPDATE {Jobs} j SET status = 'queued', claimed_by = NULL, error = NULL, email_status = NULL, finished_at = NULL, updated_at = now()
WHERE j.id = @id AND j.status IN ('failed', 'cancelled')
  AND NOT EXISTS (SELECT 1 FROM {Jobs} o WHERE o.status IN ('queued', 'running'))
RETURNING j.id", conn, tx);
        cmd.Parameters.AddWithValue("id", id);
        var resumed = await cmd.ExecuteScalarAsync(ct) != null;
        await tx.CommitAsync(ct);
        return resumed;
    }

    /// <summary>Takes a stale running job (resumed after its last id), else the oldest queued one, if none is running.</summary>
    public async Task<VideoLinkCheckJob?> ClaimNextAsync(string instance, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await LockAsync(conn, tx, ct);
        VideoLinkCheckJob? job = null;
        await using (var cmd = new NpgsqlCommand($@"
UPDATE {Jobs} SET status = 'running', claimed_by = @me, started_at = COALESCE(started_at, now()), updated_at = now()
WHERE id = (
    SELECT j.id FROM {Jobs} j
    WHERE (j.status = 'queued' OR (j.status = 'running' AND j.updated_at < now() - make_interval(mins => @stale)))
      AND NOT EXISTS (
          SELECT 1 FROM {Jobs} r WHERE r.status = 'running' AND r.updated_at >= now() - make_interval(mins => @stale))
    ORDER BY CASE WHEN j.status = 'running' THEN 0 ELSE 1 END, j.id LIMIT 1)
RETURNING {Columns}", conn, tx))
        {
            cmd.Parameters.AddWithValue("me", instance);
            cmd.Parameters.AddWithValue("stale", StaleMinutes);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            if (await r.ReadAsync(ct)) job = Read(r);
        }
        await tx.CommitAsync(ct);
        return job;
    }

    /// <summary>Next page of movies to check: idnum after <paramref name="afterId"/> with a link and enough live images.</summary>
    public async Task<List<MovieLink>> FetchMoviesAsync(long afterId, int limit, CancellationToken ct)
    {
        var o = _options.CurrentValue;
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(@"
SELECT m.idnum::bigint, m.video_link::text, COALESCE(m.media_type::text, ''), COALESCE(m.title::text, '')
FROM frl.frl_movies m
WHERE m.video_link IS NOT NULL
  AND m.video_link <> ''
  AND m.idnum > @after
  AND lower(m.media_type::text) = ANY(@types)
  AND (SELECT COUNT(*) FROM frl.frl_images i WHERE i.movieid = m.idnum AND i.status = 'live') >= @minLive
ORDER BY m.idnum
LIMIT @lim", conn) { CommandTimeout = Math.Max(30, o.DbTimeoutSeconds) };
        cmd.Parameters.AddWithValue("after", afterId);
        cmd.Parameters.AddWithValue("types", o.MediaTypeList);
        cmd.Parameters.AddWithValue("minLive", (long)Math.Max(0, o.MinLiveImages));
        cmd.Parameters.AddWithValue("lim", limit);
        var list = new List<MovieLink>(limit);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
            list.Add(new MovieLink(r.GetInt64(0), r.GetString(1), r.GetString(2), r.GetString(3)));
        return list;
    }

    /// <summary>
    /// Saves a checked batch and moves the cursor to <paramref name="lastId"/> in one transaction (also the heartbeat).
    /// False when the job is no longer this instance's (cancelled or taken over), so the run should stop.
    /// </summary>
    public async Task<bool> SaveBatchAsync(string instance, long jobId, long lastId, IReadOnlyList<LinkCheckResult> results, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await using (var job = new NpgsqlCommand($@"
UPDATE {Jobs} SET last_id = @last, updated_at = now(),
    scanned = scanned + @n,
    active = active + @a,
    unavailable = unavailable + @u,
    unknown_platform = unknown_platform + @k,
    errors = errors + @e
WHERE id = @id AND claimed_by = @me AND status = 'running' RETURNING id", conn, tx))
        {
            job.Parameters.AddWithValue("id", jobId);
            job.Parameters.AddWithValue("me", instance);
            job.Parameters.AddWithValue("last", lastId);
            job.Parameters.AddWithValue("n", (long)results.Count);
            job.Parameters.AddWithValue("a", (long)results.Count(r => r.Status == "active"));
            job.Parameters.AddWithValue("u", (long)results.Count(r => r.Status == "unavailable"));
            job.Parameters.AddWithValue("k", (long)results.Count(r => r.Status == "unknown_platform"));
            job.Parameters.AddWithValue("e", (long)results.Count(r => r.Status == "error"));
            if (await job.ExecuteScalarAsync(ct) == null)
            {
                await tx.RollbackAsync(ct);
                return false;
            }
        }
        if (results.Count > 0)
        {
            await using var insert = new NpgsqlCommand($@"
INSERT INTO {Results} (job_id, movie_id, title, media_type, url, platform, video_id, status, note, checked_at)
SELECT @job, * FROM unnest(@m, @t, @mt, @u, @p, @v, @s, @n, @c)
ON CONFLICT (job_id, movie_id) DO UPDATE SET
    title = EXCLUDED.title, media_type = EXCLUDED.media_type, url = EXCLUDED.url, platform = EXCLUDED.platform,
    video_id = EXCLUDED.video_id, status = EXCLUDED.status, note = EXCLUDED.note, checked_at = EXCLUDED.checked_at", conn, tx);
            insert.Parameters.AddWithValue("job", jobId);
            insert.Parameters.AddWithValue("m", results.Select(r => r.MovieId).ToArray());
            insert.Parameters.AddWithValue("t", results.Select(r => r.Title).ToArray());
            insert.Parameters.AddWithValue("mt", results.Select(r => r.MediaType).ToArray());
            insert.Parameters.AddWithValue("u", results.Select(r => r.Url).ToArray());
            insert.Parameters.AddWithValue("p", results.Select(r => r.Platform).ToArray());
            insert.Parameters.AddWithValue("v", results.Select(r => r.VideoId).ToArray());
            insert.Parameters.AddWithValue("s", results.Select(r => r.Status).ToArray());
            insert.Parameters.Add(new NpgsqlParameter("n", NpgsqlDbType.Array | NpgsqlDbType.Text)
            {
                Value = results.Select(r => (object?)r.Note ?? DBNull.Value).ToArray()
            });
            insert.Parameters.Add(new NpgsqlParameter("c", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz)
            {
                Value = results.Select(r => DateTime.SpecifyKind(r.CheckedAt, DateTimeKind.Utc)).ToArray()
            });
            await insert.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);
        return true;
    }

    /// <summary>Hands a running job back to the queue (shutdown), keeping its cursor.</summary>
    public async Task ReleaseAsync(string instance, long id, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($@"
UPDATE {Jobs} SET status = 'queued', claimed_by = NULL, updated_at = now()
WHERE id = @id AND claimed_by = @me AND status = 'running'", conn);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("me", instance);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Finishes the job: recounts from its results and compares with the last finished check
    /// (<c>newly_unavailable</c> = unavailable now, active then).
    /// </summary>
    public async Task<bool> FinishAsync(string instance, long id, string status, string? error, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($@"
UPDATE {Jobs} j SET status = @s, error = @e, finished_at = now(), updated_at = now(),
    scanned = c.n, active = c.a, unavailable = c.u, unknown_platform = c.k, errors = c.x,
    previous_job_id = p.id,
    newly_unavailable = (
        SELECT COUNT(*) FROM {Results} r
        JOIN {Results} pr ON pr.job_id = p.id AND pr.movie_id = r.movie_id
        WHERE r.job_id = @id AND r.status = 'unavailable' AND pr.status = 'active')
FROM (
    SELECT COUNT(*) AS n,
           COUNT(*) FILTER (WHERE status = 'active') AS a,
           COUNT(*) FILTER (WHERE status = 'unavailable') AS u,
           COUNT(*) FILTER (WHERE status = 'unknown_platform') AS k,
           COUNT(*) FILTER (WHERE status = 'error') AS x
    FROM {Results} WHERE job_id = @id) c
LEFT JOIN LATERAL (SELECT id FROM {Jobs} WHERE status = 'done' AND id < @id ORDER BY id DESC LIMIT 1) p ON TRUE
WHERE j.id = @id AND j.claimed_by = @me AND j.status = 'running' RETURNING j.id", conn);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("me", instance);
        cmd.Parameters.AddWithValue("s", status);
        cmd.Parameters.Add(new NpgsqlParameter("e", NpgsqlDbType.Text)
        {
            Value = error == null ? DBNull.Value : error.Length > 4000 ? error[..4000] : error
        });
        return await cmd.ExecuteScalarAsync(ct) != null;
    }

    /// <summary>A job's results by movie id, each with its status in the previous check (if any).</summary>
    public async Task<List<LinkCheckResult>> GetResultsAsync(VideoLinkCheckJob job, bool issuesOnly, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($@"
SELECT r.movie_id, r.title, r.media_type, r.url, r.platform, r.video_id, r.status, r.checked_at, r.note, pr.status
FROM {Results} r
LEFT JOIN {Results} pr ON pr.job_id = @prev AND pr.movie_id = r.movie_id
WHERE r.job_id = @id{(issuesOnly ? " AND r.status <> 'active'" : "")}
ORDER BY r.movie_id", conn) { CommandTimeout = Math.Max(30, _options.CurrentValue.DbTimeoutSeconds) };
        cmd.Parameters.AddWithValue("id", job.Id);
        cmd.Parameters.Add(new NpgsqlParameter("prev", NpgsqlDbType.Bigint) { Value = (object?)job.PreviousJobId ?? DBNull.Value });
        var list = new List<LinkCheckResult>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
            list.Add(new LinkCheckResult(r.GetInt64(0), r.GetString(1), r.GetString(2), r.GetString(3), r.GetString(4),
                r.GetString(5), r.GetString(6), r.GetDateTime(7), r.IsDBNull(8) ? null : r.GetString(8),
                r.IsDBNull(9) ? null : r.GetString(9)));
        return list;
    }

    public async Task SetEmailStatusAsync(long id, string status, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($"UPDATE {Jobs} SET email_status = @s WHERE id = @id", conn);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("s", status.Length > 1000 ? status[..1000] : status);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
