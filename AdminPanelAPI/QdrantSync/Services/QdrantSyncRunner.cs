using AdminPanelAPI.QdrantSync.Models;
using Microsoft.Extensions.Options;
using Npgsql;

namespace AdminPanelAPI.QdrantSync.Services;

/// <summary>
/// The full metadata sync: walks live <c>frl_images</c> in id order, rebuilds each image's managed
/// payload from Postgres, and rewrites the Qdrant payload where it differs. Postgres is read-only;
/// Qdrant vectors and payload keys the sync doesn't own are left alone; images missing from
/// Qdrant are only counted. Progress is checkpointed on the job row after every batch.
/// </summary>
public sealed class QdrantSyncRunner(
    IOptionsMonitor<QdrantSyncOptions> options,
    IConfiguration configuration,
    IHttpClientFactory httpFactory,
    QdrantSyncStore store,
    ILogger<QdrantSyncRunner> logger)
{
    private const int MaxConsecutiveFailures = 5;

    public sealed record BatchChange(long Id, Dictionary<string, object?> Payload, List<FieldDifference> Differences);
    public sealed record BatchResult(List<int> Ids, int Present, int Unchanged, List<BatchChange> Changes, int Written);

    /// <summary>Runs <paramref name="job"/> to the end. False if it was cancelled or claimed elsewhere mid-run.</summary>
    public async Task<bool> RunAsync(string instance, QdrantSyncJob job, CancellationToken ct)
    {
        var opts = options.CurrentValue;
        var connectionString = SourceConnectionString(opts);
        var qdrant = new QdrantRestClient(httpFactory.CreateClient(QdrantSyncSetup.HttpClientName), logger);

        await using (var db = new NpgsqlConnection(connectionString))
        {
            await db.OpenAsync(ct);
            await QdrantPayloads.VerifySourceSchemaAsync(db, ct);
        }

        job.Examples ??= new();
        var loggedRecords = job.Examples.Select(e => e.ImageId).Distinct().Count();
        var failures = 0;
        while (job.MaxRows == 0 || job.Scanned < job.MaxRows)
        {
            ct.ThrowIfCancellationRequested();
            var limit = job.MaxRows == 0 ? opts.BatchSize : (int)Math.Min(opts.BatchSize, job.MaxRows - job.Scanned);
            BatchResult batch;
            try
            {
                batch = await ProcessBatchAsync(connectionString, qdrant, opts, job.LastId, limit, job.Apply, ct);
                failures = 0;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested && IsTransient(ex) && ++failures < MaxConsecutiveFailures)
            {
                job.Retries++;
                job.Error = $"retrying after id {job.LastId}: {ex.Message}";
                logger.LogWarning(ex, "Qdrant sync #{JobId} batch after id {LastId} failed, retry {Attempt}.", job.Id, job.LastId, failures);
                if (!await store.CheckpointAsync(instance, job, false, ct)) return false;
                await Task.Delay(TimeSpan.FromSeconds(30 * failures), ct);
                continue;
            }
            if (batch.Ids.Count == 0) break;

            var examplesChanged = false;
            foreach (var change in batch.Changes)
            {
                foreach (var diff in change.Differences)
                {
                    job.FieldCounts[diff.Field] = job.FieldCounts.GetValueOrDefault(diff.Field) + 1;
                    if (loggedRecords < opts.MaxLoggedRecords)
                        job.Examples.Add(new ChangeExample(change.Id, diff.Field, diff.Old, diff.New));
                }
                if (loggedRecords < opts.MaxLoggedRecords)
                {
                    loggedRecords++;
                    examplesChanged = true;
                }
            }
            job.Scanned += batch.Ids.Count;
            job.Found += batch.Present;
            job.Missing += batch.Ids.Count - batch.Present;
            job.Changed += batch.Changes.Count;
            job.Unchanged += batch.Unchanged;
            job.Written += batch.Written;
            job.LastId = batch.Ids[^1];
            job.Error = null;
            if (!await store.CheckpointAsync(instance, job, examplesChanged, ct)) return false;

            if (job.Scanned % opts.LogEveryRows < batch.Ids.Count)
                logger.LogInformation(
                    "Qdrant sync #{JobId}: scanned {Scanned:N0} found {Found:N0} missing {Missing:N0} changed {Changed:N0} same {Unchanged:N0} written {Written:N0} last id {LastId}.",
                    job.Id, job.Scanned, job.Found, job.Missing, job.Changed, job.Unchanged, job.Written, job.LastId);
        }
        return true;
    }

    /// <summary>One batch: read ids after <paramref name="afterId"/>, diff against Qdrant, write changes when applying.</summary>
    public static async Task<BatchResult> ProcessBatchAsync(
        string connectionString, QdrantRestClient qdrant, QdrantSyncOptions opts,
        int afterId, int limit, bool apply, CancellationToken ct)
    {
        await using var db = new NpgsqlConnection(connectionString);
        await db.OpenAsync(ct);
        var ids = await QdrantPayloads.ReadLiveImageIdsAsync(db, afterId, limit, ct);
        if (ids.Count == 0) return new BatchResult(ids, 0, 0, new(), 0);

        var remote = await qdrant.GetPayloadsAsync(opts.Collection, ids, ct);
        var present = ids.Where(id => remote.ContainsKey(id)).ToArray();
        var changes = new List<BatchChange>();
        var unchanged = 0;
        if (present.Length > 0)
        {
            foreach (var point in await QdrantPayloads.BuildPayloadsAsync(db, present, opts.IncludeColorShades, ct))
            {
                var existing = remote[point.Id];
                var reconciled = QdrantPayloads.Reconcile(existing, point.Payload, opts.IncludeColorShades);
                if (reconciled is null) unchanged++;
                else changes.Add(new BatchChange(point.Id, reconciled,
                    QdrantPayloads.Differences(existing, point.Payload, opts.IncludeColorShades)));
            }
        }

        var written = 0;
        if (apply)
        {
            foreach (var chunk in changes.Chunk(Math.Max(1, opts.WriteBatchSize)))
            {
                await qdrant.SetPayloadsAsync(opts.Collection, chunk.Select(c => (c.Id, c.Payload)), ct);
                written += chunk.Length;
            }
        }
        return new BatchResult(ids, present.Length, unchanged, changes, written);
    }

    private string SourceConnectionString(QdrantSyncOptions opts)
    {
        var raw = !string.IsNullOrWhiteSpace(opts.SourceConnectionString)
            ? opts.SourceConnectionString
            : configuration.GetConnectionString("Default")
              ?? throw new InvalidOperationException("Missing connection string: Default");
        return new NpgsqlConnectionStringBuilder(raw) { CommandTimeout = Math.Max(30, opts.DbTimeoutSeconds) }.ConnectionString;
    }

    private static bool IsTransient(Exception ex) => ex is NpgsqlException or IOException or HttpRequestException
        or TimeoutException or TaskCanceledException or System.Text.Json.JsonException;
}
