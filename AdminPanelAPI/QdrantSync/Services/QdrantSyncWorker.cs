using System.Globalization;
using AdminPanelAPI.QdrantSync.Models;
using Microsoft.Extensions.Options;

namespace AdminPanelAPI.QdrantSync.Services;

/// <summary>
/// Queues the nightly jobs and runs at most one job per kind at a time (a sync and an insert can
/// run side by side). Both slots run this; the job table's claim decides which one does the work. On shutdown the job goes back to queued and the next
/// instance resumes it from its last checkpoint.
/// </summary>
public sealed class QdrantSyncWorker(
    QdrantSyncStore store,
    QdrantSyncRunner runner,
    QdrantSyncMailer mailer,
    IOptionsMonitor<QdrantSyncOptions> options,
    ILogger<QdrantSyncWorker> logger) : BackgroundService
{
    public static readonly string Instance =
        $"{Environment.MachineName}/{Environment.GetEnvironmentVariable("WEBSITE_SLOT_NAME") ?? "local"}/{Guid.NewGuid():N}";

    private static readonly TimeSpan StartupDelay = TimeSpan.FromSeconds(45);
    private static readonly TimeSpan IdleDelay = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan NotConfiguredDelay = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan ErrorDelay = TimeSpan.FromMinutes(2);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try { await Task.Delay(StartupDelay, stoppingToken); }
        catch (OperationCanceledException) { return; }

        var running = new Dictionary<long, Task>();
        try
        {
            await PollAsync(running, stoppingToken);
        }
        finally
        {
            await Task.WhenAll(running.Values);
        }
    }

    private async Task PollAsync(Dictionary<long, Task> running, CancellationToken stoppingToken)
    {
        var tablesReady = false;
        while (!stoppingToken.IsCancellationRequested)
        {
            TimeSpan delay;
            try
            {
                if (!tablesReady)
                {
                    await store.EnsureTablesAsync(stoppingToken);
                    tablesReady = true;
                }
                var opts = options.CurrentValue;
                if (!opts.QdrantConfigured)
                {
                    delay = NotConfiguredDelay;
                }
                else
                {
                    await QueueNightlyIfDueAsync(opts, stoppingToken);
                    foreach (var finished in running.Where(r => r.Value.IsCompleted).Select(r => r.Key).ToList())
                        running.Remove(finished);
                    var claimed = false;
                    while (await store.ClaimNextAsync(Instance, stoppingToken) is { } job)
                    {
                        running[job.Id] = Task.Run(() => RunGuardedAsync(job, stoppingToken), CancellationToken.None);
                        claimed = true;
                    }
                    delay = claimed ? TimeSpan.FromSeconds(5) : IdleDelay;
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Qdrant sync step failed.");
                delay = ErrorDelay;
            }

            try { await Task.Delay(delay, stoppingToken); }
            catch (OperationCanceledException) { return; }
        }
    }

    private async Task QueueNightlyIfDueAsync(QdrantSyncOptions opts, CancellationToken ct)
    {
        if (!opts.Nightly.Enabled) return;
        if (NightlyDue(opts.Nightly, DateTime.UtcNow) is not { } date) return;
        var kinds = opts.Nightly.Insert ? new[] { "insert", "sync" } : new[] { "sync" };
        foreach (var id in await store.CreateNightlyJobsAsync(date, kinds, opts.Nightly.Apply, ct))
            logger.LogInformation("Queued nightly Qdrant job #{JobId} for {Date}.", id, date);
    }

    /// <summary>Today's date while inside [AtUtc, AtUtc + WindowHours), else null.</summary>
    public static DateOnly? NightlyDue(QdrantSyncOptions.NightlyOptions nightly, DateTime utcNow)
    {
        if (!TimeOnly.TryParseExact(nightly.AtUtc, ["HH:mm", "H:mm"], CultureInfo.InvariantCulture, DateTimeStyles.None, out var at))
            return null;
        var today = DateOnly.FromDateTime(utcNow);
        var start = today.ToDateTime(at, DateTimeKind.Utc);
        return utcNow >= start && utcNow < start.AddHours(Math.Max(1, nightly.WindowHours)) ? today : null;
    }

    private async Task RunGuardedAsync(QdrantSyncJob job, CancellationToken ct)
    {
        try { await RunAsync(job, ct); }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex) { logger.LogError(ex, "Qdrant {Kind} #{JobId} crashed.", job.Kind, job.Id); }
    }

    private async Task RunAsync(QdrantSyncJob job, CancellationToken ct)
    {
        logger.LogInformation("Qdrant {Kind} #{JobId} started (apply={Apply}, after id {LastId}).", job.Kind, job.Id, job.Apply, job.LastId);
        string status;
        string? error = null;
        try
        {
            if (!await runner.RunAsync(Instance, job, ct))
            {
                logger.LogInformation("Qdrant sync #{JobId} stopped: cancelled or taken over.", job.Id);
                return;
            }
            status = "done";
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            await store.ReleaseAsync(Instance, job.Id, CancellationToken.None);
            logger.LogInformation("Qdrant sync #{JobId} released at id {LastId} for resume.", job.Id, job.LastId);
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Qdrant sync #{JobId} failed at id {LastId}.", job.Id, job.LastId);
            status = "failed";
            error = ex.Message;
        }

        if (!await store.FinishAsync(Instance, job.Id, status, error, CancellationToken.None)) return;
        logger.LogInformation("Qdrant sync #{JobId} {Status}: scanned {Scanned}, changed {Changed}, written {Written}.",
            job.Id, status, job.Scanned, job.Changed, job.Written);

        var finished = await store.GetJobAsync(job.Id, CancellationToken.None);
        if (finished != null && (finished.Email || finished.TriggeredBy == "nightly"))
            await mailer.SendAndRecordAsync(finished, CancellationToken.None);
    }
}
