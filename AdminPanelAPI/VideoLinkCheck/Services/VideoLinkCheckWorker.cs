using AdminPanelAPI.VideoLinkCheck.Models;
using Microsoft.Extensions.Options;

namespace AdminPanelAPI.VideoLinkCheck.Services;

/// <summary>
/// Runs queued link checks one at a time (across both slots, via the job table's claim). Each
/// batch of movies is checked in parallel and saved with the cursor, so a restart resumes after
/// the last saved movie. Emails the report when a job with email on finishes. No timer: the
/// weekly run is queued by an Azure Logic App calling POST /api/video-link-check/run.
/// </summary>
public sealed class VideoLinkCheckWorker(
    VideoLinkCheckStore store,
    VideoLinkChecker checker,
    VideoLinkCheckMailer mailer,
    IOptionsMonitor<VideoLinkCheckOptions> options,
    ILogger<VideoLinkCheckWorker> logger) : BackgroundService
{
    public static readonly string Instance =
        $"{Environment.MachineName}/{Environment.GetEnvironmentVariable("WEBSITE_SLOT_NAME") ?? "local"}/{Guid.NewGuid():N}";

    private static readonly TimeSpan StartupDelay = TimeSpan.FromSeconds(45);
    private static readonly TimeSpan IdleDelay = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan ErrorDelay = TimeSpan.FromMinutes(2);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try { await Task.Delay(StartupDelay, stoppingToken); }
        catch (OperationCanceledException) { return; }

        var tablesReady = false;
        while (!stoppingToken.IsCancellationRequested)
        {
            var delay = IdleDelay;
            try
            {
                if (!tablesReady)
                {
                    await store.EnsureTablesAsync(stoppingToken);
                    tablesReady = true;
                }
                if (await store.ClaimNextAsync(Instance, stoppingToken) is { } job)
                {
                    await RunAsync(job, stoppingToken);
                    delay = TimeSpan.FromSeconds(5);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Video link check step failed.");
                delay = ErrorDelay;
            }

            try { await Task.Delay(delay, stoppingToken); }
            catch (OperationCanceledException) { return; }
        }
    }

    private async Task RunAsync(VideoLinkCheckJob job, CancellationToken ct)
    {
        logger.LogInformation("Video link check #{JobId} started after id {LastId}.", job.Id, job.LastId);
        string status;
        string? error = null;
        try
        {
            if (!await CheckAllAsync(job, ct))
            {
                logger.LogInformation("Video link check #{JobId} stopped: cancelled or taken over.", job.Id);
                return;
            }
            status = "done";
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            await store.ReleaseAsync(Instance, job.Id, CancellationToken.None);
            logger.LogInformation("Video link check #{JobId} released at id {LastId} for resume.", job.Id, job.LastId);
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Video link check #{JobId} failed after id {LastId}.", job.Id, job.LastId);
            status = "failed";
            error = ex.Message;
        }

        if (!await store.FinishAsync(Instance, job.Id, status, error, CancellationToken.None)) return;
        var finished = await store.GetJobAsync(job.Id, CancellationToken.None);
        if (finished == null) return;
        logger.LogInformation("Video link check #{JobId} {Status}: {Scanned} checked, {Unavailable} unavailable ({New} new).",
            finished.Id, finished.Status, finished.Scanned, finished.Unavailable, finished.NewlyUnavailable);
        if (finished.Email)
            await mailer.SendAndRecordAsync(finished, null, CancellationToken.None);
    }

    /// <summary>False when the job stopped being ours (cancelled or taken over).</summary>
    private async Task<bool> CheckAllAsync(VideoLinkCheckJob job, CancellationToken ct)
    {
        while (true)
        {
            var o = options.CurrentValue;
            var batch = await store.FetchMoviesAsync(job.LastId, Math.Clamp(o.BatchSize, 1, 5000), ct);
            if (batch.Count == 0) return true;

            var results = new LinkCheckResult[batch.Count];
            await Parallel.ForEachAsync(Enumerable.Range(0, batch.Count),
                new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, o.MaxConcurrent), CancellationToken = ct },
                async (i, token) => results[i] = await checker.CheckAsync(batch[i], token));

            var lastId = batch[^1].Id;
            if (!await store.SaveBatchAsync(Instance, job.Id, lastId, results, ct)) return false;
            job.LastId = lastId;
            job.Scanned += results.Length;
            logger.LogInformation("Video link check #{JobId}: {Scanned} checked (last id {LastId}).", job.Id, job.Scanned, lastId);
        }
    }
}
