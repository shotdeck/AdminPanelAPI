using System.Text.Json;
using AdminPanelAPI.QdrantSync.Models;
using Npgsql;
using NpgsqlTypes;

namespace AdminPanelAPI.QdrantSync.Services;

/// <summary>
/// Sync jobs in <c>frl.frl_qdrant_sync_jobs</c>. Both App Service slots run the worker against
/// this table, so starting, checkpointing and finishing all go through a claim: a job belongs to
/// the instance whose <c>claimed_by</c> it carries, and a claim whose heartbeat (updated_at) is
/// older than <see cref="StaleMinutes"/> can be taken over and resumed from its last id.
/// </summary>
public sealed class QdrantSyncStore
{
    public const int StaleMinutes = 15;
    private const string Table = "frl.frl_qdrant_sync_jobs";
    private const string LockKey = "frl_qdrant_sync_jobs";

    private readonly string _connectionString;

    public QdrantSyncStore(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("Default")
            ?? throw new InvalidOperationException("Missing connection string: Default");
    }

    private async Task<NpgsqlConnection> OpenAsync(CancellationToken ct)
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        return conn;
    }

    public async Task EnsureTablesAsync(CancellationToken ct)
    {
        const string sql = $@"
CREATE TABLE IF NOT EXISTS {Table} (
    id              BIGSERIAL    PRIMARY KEY,
    kind            VARCHAR(20)  NOT NULL DEFAULT 'sync',
    apply           BOOLEAN      NOT NULL DEFAULT FALSE,
    triggered_by    VARCHAR(20)  NOT NULL DEFAULT 'manual',
    nightly_date    DATE,
    status          VARCHAR(20)  NOT NULL DEFAULT 'queued',
    start_after_id  INTEGER      NOT NULL DEFAULT 0,
    max_rows        BIGINT       NOT NULL DEFAULT 0,
    email           BOOLEAN      NOT NULL DEFAULT FALSE,
    last_id         INTEGER      NOT NULL DEFAULT 0,
    scanned         BIGINT       NOT NULL DEFAULT 0,
    found           BIGINT       NOT NULL DEFAULT 0,
    missing         BIGINT       NOT NULL DEFAULT 0,
    changed         BIGINT       NOT NULL DEFAULT 0,
    unchanged       BIGINT       NOT NULL DEFAULT 0,
    written         BIGINT       NOT NULL DEFAULT 0,
    field_counts    JSONB        NOT NULL DEFAULT '{{}}'::jsonb,
    examples        JSONB        NOT NULL DEFAULT '[]'::jsonb,
    retries         INTEGER      NOT NULL DEFAULT 0,
    claimed_by      TEXT,
    email_status    TEXT,
    error           TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_qsj_status ON {Table} (status);
CREATE UNIQUE INDEX IF NOT EXISTS ux_qsj_nightly ON {Table} (kind, nightly_date) WHERE nightly_date IS NOT NULL;";
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private const string Columns = @"id, kind, apply, triggered_by, nightly_date, status, start_after_id, max_rows, email,
last_id, scanned, found, missing, changed, unchanged, written, field_counts::text, examples::text, retries,
email_status, error, created_at, started_at, updated_at, finished_at";

    private static QdrantSyncJob Read(NpgsqlDataReader r) => new()
    {
        Id = r.GetInt64(0),
        Kind = r.GetString(1),
        Apply = r.GetBoolean(2),
        TriggeredBy = r.GetString(3),
        NightlyDate = r.IsDBNull(4) ? null : DateOnly.FromDateTime(r.GetDateTime(4)),
        Status = r.GetString(5),
        StartAfterId = r.GetInt32(6),
        MaxRows = r.GetInt64(7),
        Email = r.GetBoolean(8),
        LastId = r.GetInt32(9),
        Scanned = r.GetInt64(10),
        Found = r.GetInt64(11),
        Missing = r.GetInt64(12),
        Changed = r.GetInt64(13),
        Unchanged = r.GetInt64(14),
        Written = r.GetInt64(15),
        FieldCounts = JsonSerializer.Deserialize<Dictionary<string, long>>(r.GetString(16)) ?? new(),
        Examples = r.IsDBNull(17) ? null : JsonSerializer.Deserialize<List<ChangeExample>>(r.GetString(17)),
        Retries = r.GetInt32(18),
        EmailStatus = r.IsDBNull(19) ? null : r.GetString(19),
        Error = r.IsDBNull(20) ? null : r.GetString(20),
        CreatedAt = r.GetDateTime(21),
        StartedAt = r.IsDBNull(22) ? null : r.GetDateTime(22),
        UpdatedAt = r.GetDateTime(23),
        FinishedAt = r.IsDBNull(24) ? null : r.GetDateTime(24),
    };

    private static async Task LockAsync(NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT pg_advisory_xact_lock(hashtext(@k))", conn, tx);
        cmd.Parameters.AddWithValue("k", LockKey);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long?> ActiveJobIdAsync(NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            $"SELECT id FROM {Table} WHERE status IN ('queued', 'running') ORDER BY id LIMIT 1", conn, tx);
        var id = await cmd.ExecuteScalarAsync(ct);
        return id is long l ? l : null;
    }

    /// <summary>Queues a job unless one is already queued or running; returns that one's id instead.</summary>
    public async Task<(long JobId, bool Created)> CreateJobAsync(
        string kind, bool apply, int startAfterId, long maxRows, bool email, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await LockAsync(conn, tx, ct);
        if (await ActiveJobIdAsync(conn, tx, ct) is long active)
            return (active, false);
        await using var cmd = new NpgsqlCommand($@"
INSERT INTO {Table} (kind, apply, start_after_id, last_id, max_rows, email)
VALUES (@k, @a, @s, @s, @m, @e) RETURNING id", conn, tx);
        cmd.Parameters.AddWithValue("k", kind);
        cmd.Parameters.AddWithValue("a", apply);
        cmd.Parameters.AddWithValue("s", startAfterId);
        cmd.Parameters.AddWithValue("m", maxRows);
        cmd.Parameters.AddWithValue("e", email);
        var id = Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
        await tx.CommitAsync(ct);
        return (id, true);
    }

    /// <summary>
    /// Queues the nightly jobs for <paramref name="date"/> once each (insert first, so the sync
    /// runs after it); they wait behind any job that is already running.
    /// </summary>
    public async Task<List<long>> CreateNightlyJobsAsync(DateOnly date, IEnumerable<string> kinds, bool apply, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await LockAsync(conn, tx, ct);
        var created = new List<long>();
        foreach (var kind in kinds)
        {
            await using var cmd = new NpgsqlCommand($@"
INSERT INTO {Table} (kind, apply, triggered_by, nightly_date, email)
VALUES (@k, @a, 'nightly', @d, TRUE)
ON CONFLICT (kind, nightly_date) WHERE nightly_date IS NOT NULL DO NOTHING
RETURNING id", conn, tx);
            cmd.Parameters.AddWithValue("k", kind);
            cmd.Parameters.AddWithValue("a", apply);
            cmd.Parameters.Add(new NpgsqlParameter("d", NpgsqlDbType.Date) { Value = date });
            if (await cmd.ExecuteScalarAsync(ct) is long id) created.Add(id);
        }
        await tx.CommitAsync(ct);
        return created;
    }

    public async Task<QdrantSyncJob?> GetJobAsync(long id, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($"SELECT {Columns} FROM {Table} WHERE id = @id", conn);
        cmd.Parameters.AddWithValue("id", id);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        return await r.ReadAsync(ct) ? Read(r) : null;
    }

    /// <summary>Recent jobs, newest first, without their examples.</summary>
    public async Task<List<QdrantSyncJob>> ListJobsAsync(int limit, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(
            $"SELECT {Columns.Replace("examples::text", "NULL::text")} FROM {Table} ORDER BY id DESC LIMIT @l", conn);
        cmd.Parameters.AddWithValue("l", Math.Clamp(limit, 1, 500));
        var jobs = new List<QdrantSyncJob>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct)) jobs.Add(Read(r));
        return jobs;
    }

    /// <summary>Stops a queued or running job; a running one stops at its next checkpoint.</summary>
    public async Task<bool> CancelAsync(long id, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($@"
UPDATE {Table} SET status = 'cancelled', finished_at = now(), updated_at = now()
WHERE id = @id AND status IN ('queued', 'running') RETURNING id", conn);
        cmd.Parameters.AddWithValue("id", id);
        return await cmd.ExecuteScalarAsync(ct) != null;
    }

    /// <summary>
    /// Takes the next job for <paramref name="instance"/>: a stale running one first (resumed from
    /// its last id), else the oldest queued one, and only if no other job is running.
    /// </summary>
    public async Task<QdrantSyncJob?> ClaimNextAsync(string instance, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await LockAsync(conn, tx, ct);
        QdrantSyncJob? job = null;
        await using (var cmd = new NpgsqlCommand($@"
UPDATE {Table} SET status = 'running', claimed_by = @me, started_at = COALESCE(started_at, now()), updated_at = now()
WHERE id = (
    SELECT id FROM {Table}
    WHERE status = 'queued' OR (status = 'running' AND updated_at < now() - make_interval(mins => @stale))
    ORDER BY CASE WHEN status = 'running' THEN 0 ELSE 1 END, id LIMIT 1)
AND NOT EXISTS (
    SELECT 1 FROM {Table} WHERE status = 'running' AND updated_at >= now() - make_interval(mins => @stale))
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

    /// <summary>
    /// Saves progress (also the heartbeat). False when the job is no longer this instance's to run
    /// (cancelled, or taken over after going stale), so the runner should stop.
    /// </summary>
    public async Task<bool> CheckpointAsync(string instance, QdrantSyncJob job, bool examplesChanged, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        var examplesSql = examplesChanged ? ", examples = @x::jsonb" : "";
        await using var cmd = new NpgsqlCommand($@"
UPDATE {Table} SET last_id = @last, scanned = @sc, found = @f, missing = @mi, changed = @ch, unchanged = @un,
    written = @w, field_counts = @fc::jsonb, retries = @re, error = @err, updated_at = now(){examplesSql}
WHERE id = @id AND claimed_by = @me AND status = 'running' RETURNING id", conn);
        cmd.Parameters.AddWithValue("id", job.Id);
        cmd.Parameters.AddWithValue("me", instance);
        cmd.Parameters.AddWithValue("last", job.LastId);
        cmd.Parameters.AddWithValue("sc", job.Scanned);
        cmd.Parameters.AddWithValue("f", job.Found);
        cmd.Parameters.AddWithValue("mi", job.Missing);
        cmd.Parameters.AddWithValue("ch", job.Changed);
        cmd.Parameters.AddWithValue("un", job.Unchanged);
        cmd.Parameters.AddWithValue("w", job.Written);
        cmd.Parameters.AddWithValue("fc", JsonSerializer.Serialize(job.FieldCounts));
        cmd.Parameters.AddWithValue("re", job.Retries);
        cmd.Parameters.Add(new NpgsqlParameter("err", NpgsqlDbType.Text) { Value = (object?)job.Error ?? DBNull.Value });
        if (examplesChanged)
            cmd.Parameters.AddWithValue("x", JsonSerializer.Serialize(job.Examples ?? new()));
        return await cmd.ExecuteScalarAsync(ct) != null;
    }

    /// <summary>Hands a running job back to the queue (shutdown), keeping its checkpoint.</summary>
    public async Task ReleaseAsync(string instance, long id, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($@"
UPDATE {Table} SET status = 'queued', claimed_by = NULL, updated_at = now()
WHERE id = @id AND claimed_by = @me AND status = 'running'", conn);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("me", instance);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<bool> FinishAsync(string instance, long id, string status, string? error, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($@"
UPDATE {Table} SET status = @s, error = @e, finished_at = now(), updated_at = now()
WHERE id = @id AND claimed_by = @me AND status = 'running' RETURNING id", conn);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("me", instance);
        cmd.Parameters.AddWithValue("s", status);
        cmd.Parameters.Add(new NpgsqlParameter("e", NpgsqlDbType.Text)
        {
            Value = error == null ? DBNull.Value : error.Length > 4000 ? error[..4000] : error
        });
        return await cmd.ExecuteScalarAsync(ct) != null;
    }

    public async Task SetEmailStatusAsync(long id, string status, CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($"UPDATE {Table} SET email_status = @s WHERE id = @id", conn);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("s", status.Length > 1000 ? status[..1000] : status);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
