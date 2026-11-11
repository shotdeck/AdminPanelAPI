using AdminPanelAPI.QdrantSync.Models;
using Microsoft.Extensions.Options;
using Npgsql;
using System.Text.Json;

namespace AdminPanelAPI.QdrantSync.Services;

/// <summary>
/// The full metadata sync: walks live <c>frl_images</c> in id order, rebuilds each image's managed
/// payload from Postgres, and rewrites the Qdrant payload where it differs. Postgres is read-only;
/// Qdrant vectors and payload keys the sync doesn't own are left alone; images missing from
/// Qdrant are only counted. When the collection has a cinemanet vector, points lacking one get it
/// added from <c>frl_cinemanet_embeddings</c> (vector-only update). Progress is checkpointed on the
/// job row after every batch.
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
    public sealed record BatchResult(List<int> Ids, int Present, int Unchanged, List<BatchChange> Changes, int Written,
        int CinemanetAdded = 0);

    public const string CinemanetField = "cinemanet (vector added)";

    /// <summary>Runs <paramref name="job"/> to the end. False if it was cancelled or claimed elsewhere mid-run.</summary>
    public Task<bool> RunAsync(string instance, QdrantSyncJob job, CancellationToken ct) =>
        job.Kind switch
        {
            "insert" => RunInsertAsync(instance, job, ct),
            "rebuild" => RunRebuildAsync(instance, job, ct),
            _ => RunSyncAsync(instance, job, ct),
        };

    private async Task<bool> RunSyncAsync(string instance, QdrantSyncJob job, CancellationToken ct)
    {
        var opts = options.CurrentValue;
        var connectionString = SourceConnectionString(opts);
        var qdrant = new QdrantRestClient(httpFactory.CreateClient(QdrantSyncSetup.HttpClientName), logger);
        var cinemanet = await HasCinemanetAsync(qdrant, opts, job, ct);

        await using (var db = new NpgsqlConnection(connectionString))
        {
            await db.OpenAsync(ct);
            await QdrantPayloads.VerifySourceSchemaAsync(db, ct, includeCinemanet: cinemanet);
        }
        if (job.Apply) await EnsureIndexesAsync(qdrant, opts, job, ct);

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
                batch = await ProcessBatchAsync(connectionString, qdrant, opts, job.LastId, limit, job.Apply, ct, cinemanet);
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
            if (batch.CinemanetAdded > 0)
                job.FieldCounts[CinemanetField] = job.FieldCounts.GetValueOrDefault(CinemanetField) + batch.CinemanetAdded;
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

    /// <summary>
    /// Insert job: snapshot every Qdrant id, scan Postgres images that have an embedding, then
    /// load and upsert (vector + payload) only the ones Qdrant lacks. No checkpoint across runs on
    /// purpose: each run rescans so older images that were embedded late are picked up; a resumed
    /// job starts over, and the recheck before each upload keeps that idempotent.
    /// </summary>
    private async Task<bool> RunInsertAsync(string instance, QdrantSyncJob job, CancellationToken ct)
    {
        var opts = options.CurrentValue;
        var connectionString = SourceConnectionString(opts);
        var qdrant = new QdrantRestClient(httpFactory.CreateClient(QdrantSyncSetup.HttpClientName), logger);
        var cinemanet = await HasCinemanetAsync(qdrant, opts, job, ct);
        await using (var db = new NpgsqlConnection(connectionString))
        {
            await db.OpenAsync(ct);
            await QdrantPayloads.VerifySourceSchemaAsync(db, ct, includeEmbeddings: true, includeCinemanet: cinemanet);
        }
        if (job.Apply) await EnsureIndexesAsync(qdrant, opts, job, ct);

        job.LastId = job.StartAfterId;
        job.Scanned = job.Found = job.Missing = job.Changed = job.Unchanged = job.Written = 0;
        job.FieldCounts.Clear();
        job.Examples = null;

        var existing = await RetryAsync(job, () => qdrant.ScrollAllIdsAsync(opts.Collection, Math.Max(1, opts.ScrollPageSize), ct), ct);
        logger.LogInformation("Qdrant insert #{JobId}: {Count:N0} ids in Qdrant.", job.Id, existing.Count);
        if (!await store.CheckpointAsync(instance, job, false, ct)) return false;

        var missing = new List<int>();
        while (job.MaxRows == 0 || job.Scanned < job.MaxRows)
        {
            var limit = job.MaxRows == 0 ? opts.IdScanBatchSize : (int)Math.Min(opts.IdScanBatchSize, job.MaxRows - job.Scanned);
            var after = job.LastId;
            var ids = await RetryAsync(job, async () =>
            {
                await using var db = new NpgsqlConnection(connectionString);
                await db.OpenAsync(ct);
                return await QdrantPayloads.ReadEmbeddedImageIdsAsync(db, after, limit, ct);
            }, ct);
            if (ids.Count == 0) break;
            foreach (var id in ids)
                if (existing.Contains(id)) job.Found++;
                else missing.Add(id);
            job.Scanned += ids.Count;
            job.Missing = missing.Count;
            job.LastId = ids[^1];
            if (!await store.CheckpointAsync(instance, job, false, ct)) return false;
        }
        existing.Clear();
        existing.TrimExcess();
        logger.LogInformation("Qdrant insert #{JobId}: {Scanned:N0} embedded, {Found:N0} in Qdrant, {Missing:N0} missing.",
            job.Id, job.Scanned, job.Found, job.Missing);
        if (!job.Apply) return true;

        foreach (var chunk in missing.Chunk(Math.Max(1, opts.InsertBatchSize)))
        {
            ct.ThrowIfCancellationRequested();
            var inserted = await RetryAsync(job, async () =>
            {
                var present = await qdrant.GetExistingIdsAsync(opts.Collection, chunk.Select(x => (long)x).ToArray(), ct);
                var absent = chunk.Where(id => !present.Contains(id)).ToArray();
                if (absent.Length == 0) return 0;
                await using var db = new NpgsqlConnection(connectionString);
                await db.OpenAsync(ct);
                var points = await QdrantPayloads.BuildPayloadsAsync(db, absent, opts.IncludeColorShades, ct, withVectors: true);
                if (points.Count == 0) return 0;
                if (cinemanet)
                {
                    var vectors = await QdrantPayloads.ReadCinemanetVectorsAsync(db, points.Select(p => p.Id).ToArray(), ct);
                    foreach (var p in points) p.Cinemanet = vectors.GetValueOrDefault(p.Id);
                }
                await qdrant.UpsertPointsAsync(opts.Collection, opts.VectorName, points, ct,
                    cinemanet ? opts.CinemanetVectorName : null);
                return points.Count;
            }, ct);
            job.Written += inserted;
            job.LastId = chunk[^1];
            if (!await store.CheckpointAsync(instance, job, false, ct)) return false;
            if (job.Written % 10000 < inserted)
                logger.LogInformation("Qdrant insert #{JobId}: inserted {Written:N0}/{Missing:N0}.", job.Id, job.Written, job.Missing);
        }
        return true;
    }

    /// <summary>
    /// Rebuild job: copies every point of <c>Collection</c> (payload + existing vectors) into
    /// <c>RebuildTargetCollection</c>, which is created with the extra cinemanet vector, adding
    /// each image's cinemanet embedding from Postgres. Walks Qdrant in id order and checkpoints the
    /// last copied id, so a restarted job carries on; re-copying a page is harmless (upsert).
    /// Dry run (counts only, nothing created) unless the job applies.
    /// </summary>
    private async Task<bool> RunRebuildAsync(string instance, QdrantSyncJob job, CancellationToken ct)
    {
        var opts = options.CurrentValue;
        var source = opts.Collection;
        var target = opts.RebuildTargetCollection;
        if (string.IsNullOrWhiteSpace(target) || target == source)
            throw new InvalidOperationException("QdrantSync:RebuildTargetCollection must be set and differ from QdrantSync:Collection.");
        var connectionString = SourceConnectionString(opts);
        var qdrant = new QdrantRestClient(httpFactory.CreateClient(QdrantSyncSetup.HttpClientName), logger);
        await using (var db = new NpgsqlConnection(connectionString))
        {
            await db.OpenAsync(ct);
            await QdrantPayloads.VerifySourceSchemaAsync(db, ct, includeCinemanet: true);
        }
        if (await RetryAsync(job, () => qdrant.AliasTargetAsync(source, ct), ct) == target)
            throw new InvalidOperationException($"'{source}' is already an alias of '{target}'; nothing to rebuild.");
        var sourceInfo = await RetryAsync(job, () => qdrant.GetCollectionAsync(source, ct), ct)
            ?? throw new InvalidOperationException($"Qdrant collection '{source}' not found.");
        if (job.Apply) await EnsureRebuildTargetAsync(qdrant, opts, job, sourceInfo, ct);

        var parallel = job.MaxRows > 0 ? 1 : Math.Max(1, opts.RebuildParallelism);
        if (job.Slices is not { Count: > 0 })
        {
            var resumeAfter = Math.Max(job.LastId, job.StartAfterId);
            var maxId = 0;
            if (parallel > 1)
            {
                await using var db = new NpgsqlConnection(connectionString);
                await db.OpenAsync(ct);
                maxId = await QdrantPayloads.ReadMaxImageIdAsync(db, ct);
            }
            job.Slices = PlanSlices(resumeAfter, maxId, parallel * 4);
        }
        var pending = new System.Collections.Concurrent.ConcurrentQueue<RebuildSlice>(job.Slices.Where(s => !s.Done));
        logger.LogInformation("Qdrant rebuild #{JobId}: {Pending} of {Slices} slices to copy, {Workers} at a time.",
            job.Id, pending.Count, job.Slices.Count, parallel);

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var progress = new SemaphoreSlim(1, 1);
        var stopped = false;
        var workers = Enumerable.Range(0, parallel).Select(_ => Task.Run(async () =>
        {
            try
            {
                while (!stop.IsCancellationRequested && pending.TryDequeue(out var slice))
                    if (!await CopySliceAsync(instance, job, opts, qdrant, connectionString, slice, progress, stop.Token))
                    {
                        stopped = true;
                        stop.Cancel();
                    }
            }
            catch
            {
                stop.Cancel();
                throw;
            }
        })).ToArray();
        try { await Task.WhenAll(workers); }
        catch
        {
            ct.ThrowIfCancellationRequested();
            if (!stopped)
            {
                var failure = workers.Where(t => t.IsFaulted).SelectMany(t => t.Exception!.InnerExceptions)
                    .FirstOrDefault(e => e is not OperationCanceledException);
                if (failure != null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
                throw;
            }
        }
        if (stopped) return false;

        var reachedEnd = job.Slices.All(s => s.Done);
        if (job.Apply && reachedEnd)
        {
            var threshold = QdrantCollectionConfig.IndexingThreshold(sourceInfo);
            await RetryAsync(job, async () =>
            {
                await qdrant.UpdateCollectionAsync(target, new { optimizers_config = new { indexing_threshold = threshold } }, ct);
                return 0;
            }, ct);
            logger.LogInformation("Qdrant rebuild #{JobId}: copy finished, indexing of {Target} turned back on (threshold {Threshold}).",
                job.Id, target, threshold);
        }
        return true;
    }

    /// <summary>
    /// Splits ids after <paramref name="after"/> into <paramref name="count"/> equal ranges up to
    /// <paramref name="maxId"/>; the last range is open-ended so ids above it are still copied.
    /// </summary>
    public static List<RebuildSlice> PlanSlices(int after, int maxId, int count)
    {
        if (count <= 1 || maxId - after < count) return [new RebuildSlice { After = after }];
        var step = (int)Math.Ceiling((maxId - (double)after) / count);
        var slices = new List<RebuildSlice>(count);
        for (var i = 0; i < count; i++)
        {
            var from = after + i * step;
            if (from >= maxId) break;
            slices.Add(new RebuildSlice { After = from, End = from + step });
        }
        slices[^1].End = null;
        return slices;
    }

    /// <summary>Copies one slice page by page, checkpointing after each. False if the job was cancelled or taken over.</summary>
    private async Task<bool> CopySliceAsync(string instance, QdrantSyncJob job, QdrantSyncOptions opts, QdrantRestClient qdrant,
        string connectionString, RebuildSlice slice, SemaphoreSlim progress, CancellationToken ct)
    {
        var source = opts.Collection;
        var target = opts.RebuildTargetCollection;
        var pageSize = Math.Max(1, opts.RebuildBatchSize);
        while (!slice.Done)
        {
            ct.ThrowIfCancellationRequested();
            if (job.MaxRows > 0 && job.Scanned >= job.MaxRows) return true;
            var limit = job.MaxRows == 0 ? pageSize : (int)Math.Min(pageSize, job.MaxRows - job.Scanned);
            long from = slice.After + 1L;
            var (page, next) = await RetryAsync(job, () => qdrant.ScrollPageAsync(source, from, limit, job.Apply, ct), ct);
            var points = slice.End is int end ? page.Where(p => p.Id <= end).ToList() : page;
            var last = next == null || points.Count < page.Count;

            var ids = points.Select(p => p.Id).ToArray();
            var vectors = ids.Length == 0 ? new Dictionary<long, float[]?>() : await RetryAsync(job, async () =>
            {
                await using var db = new NpgsqlConnection(connectionString);
                await db.OpenAsync(ct);
                return await QdrantPayloads.ReadCinemanetVectorsAsync(db, ids, ct, idsOnly: !job.Apply);
            }, ct);
            if (job.Apply && points.Count > 0)
            {
                var copies = points.Select(p =>
                {
                    var named = new Dictionary<string, float[]>(p.Vectors);
                    if (vectors.GetValueOrDefault(p.Id) is { } c) named[opts.CinemanetVectorName] = c;
                    return p with { Vectors = named };
                }).ToList();
                await RetryAsync(job, async () => { await qdrant.UpsertCopiedPointsAsync(target, copies, ct); return 0; }, ct);
            }

            await progress.WaitAsync(ct);
            try
            {
                if (job.Apply) job.Written += points.Count;
                job.Scanned += points.Count;
                job.Found += vectors.Count;
                job.Missing += points.Count - vectors.Count;
                if (points.Count > 0)
                {
                    slice.After = checked((int)points[^1].Id);
                    job.LastId = Math.Max(job.LastId, slice.After);
                }
                slice.Done = last;
                job.Error = null;
                if (!await store.CheckpointAsync(instance, job, false, ct)) return false;
                if (job.Scanned % opts.LogEveryRows < points.Count)
                    logger.LogInformation("Qdrant rebuild #{JobId}: scanned {Scanned:N0} with cinemanet {Found:N0} without {Missing:N0} copied {Written:N0}, {Done}/{Slices} slices done.",
                        job.Id, job.Scanned, job.Found, job.Missing, job.Written, job.Slices!.Count(s => s.Done), job.Slices!.Count);
            }
            finally { progress.Release(); }
        }
        return true;
    }

    /// <summary>Creates the rebuild target from the source's config plus cinemanet, and any payload indexes it lacks.</summary>
    private async Task EnsureRebuildTargetAsync(QdrantRestClient qdrant, QdrantSyncOptions opts, QdrantSyncJob job,
        System.Text.Json.JsonElement sourceInfo, CancellationToken ct)
    {
        var target = opts.RebuildTargetCollection;
        var info = await RetryAsync(job, () => qdrant.GetCollectionAsync(target, ct), ct);
        if (info == null)
        {
            logger.LogInformation("Qdrant rebuild #{JobId}: creating collection {Target}.", job.Id, target);
            await qdrant.CreateCollectionAsync(target, QdrantCollectionConfig.RebuildTarget(sourceInfo, opts), ct);
            info = await qdrant.GetCollectionAsync(target, ct)
                ?? throw new InvalidOperationException($"Qdrant collection '{target}' was not created.");
        }
        var names = QdrantCollectionConfig.VectorNames(info.Value);
        foreach (var name in QdrantCollectionConfig.VectorNames(sourceInfo).Append(opts.CinemanetVectorName))
            if (!names.Contains(name))
                throw new InvalidOperationException($"Existing collection '{target}' has no '{name}' vector; delete it or pick another RebuildTargetCollection.");

        var have = QdrantCollectionConfig.PayloadIndexes(info.Value);
        foreach (var (field, schema) in QdrantCollectionConfig.PayloadIndexes(sourceInfo))
            if (!have.ContainsKey(field))
                await RetryAsync(job, async () => { await qdrant.CreatePayloadIndexAsync(target, field, schema, ct); return 0; }, ct);
    }

    /// <summary>Creates any of <see cref="QdrantPayloads.RequiredIndexes"/> the collection doesn't have yet.</summary>
    private async Task EnsureIndexesAsync(QdrantRestClient qdrant, QdrantSyncOptions opts, QdrantSyncJob job, CancellationToken ct)
    {
        var info = await RetryAsync(job, () => qdrant.GetCollectionAsync(opts.Collection, ct), ct);
        if (info is not { } i) return;
        var schema = i.TryGetProperty("payload_schema", out var s) && s.ValueKind == JsonValueKind.Object ? s : default;
        foreach (var (field, type) in QdrantPayloads.RequiredIndexes)
        {
            if (schema.ValueKind == JsonValueKind.Object && schema.TryGetProperty(field, out _)) continue;
            logger.LogInformation("Qdrant job #{JobId}: creating {Type} index on {Field}.", job.Id, type, field);
            await RetryAsync(job, async () =>
            {
                await qdrant.CreatePayloadIndexAsync(opts.Collection, field, JsonSerializer.SerializeToElement(type), ct, wait: false);
                return 0;
            }, ct);
        }
    }

    private async Task<bool> HasCinemanetAsync(QdrantRestClient qdrant, QdrantSyncOptions opts, QdrantSyncJob job, CancellationToken ct) =>
        !string.IsNullOrWhiteSpace(opts.CinemanetVectorName)
        && (await RetryAsync(job, () => qdrant.VectorNamesAsync(opts.Collection, ct), ct)).Contains(opts.CinemanetVectorName);

    private async Task<T> RetryAsync<T>(QdrantSyncJob job, Func<Task<T>> action, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try { return await action(); }
            catch (Exception ex) when (!ct.IsCancellationRequested && IsTransient(ex) && attempt < MaxConsecutiveFailures)
            {
                lock (job) job.Retries++;
                logger.LogWarning(ex, "Qdrant {Kind} #{JobId} step failed, retry {Attempt}.", job.Kind, job.Id, attempt);
                await Task.Delay(TimeSpan.FromSeconds(30 * attempt), ct);
            }
        }
    }

    /// <summary>One batch: read ids after <paramref name="afterId"/>, diff against Qdrant, write changes when applying.</summary>
    public static async Task<BatchResult> ProcessBatchAsync(
        string connectionString, QdrantRestClient qdrant, QdrantSyncOptions opts,
        int afterId, int limit, bool apply, CancellationToken ct, bool cinemanet = false)
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

        var cinemanetAdded = 0;
        if (cinemanet && present.Length > 0)
        {
            var lacking = await qdrant.IdsWithoutVectorAsync(opts.Collection, opts.CinemanetVectorName, present.Select(x => (long)x).ToArray(), ct);
            if (lacking.Count > 0)
            {
                var vectors = await QdrantPayloads.ReadCinemanetVectorsAsync(db, lacking, ct);
                cinemanetAdded = vectors.Count;
                if (apply)
                    foreach (var chunk in vectors.Chunk(Math.Max(1, opts.WriteBatchSize)))
                        await qdrant.UpdateVectorsAsync(opts.Collection, opts.CinemanetVectorName, chunk.Select(x => (x.Key, x.Value!)), ct);
            }
        }
        return new BatchResult(ids, present.Length, unchanged, changes, written, cinemanetAdded);
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
