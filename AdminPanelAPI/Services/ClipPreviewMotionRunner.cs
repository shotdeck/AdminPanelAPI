using AdminPanelAPI.Interfaces;
using AdminPanelAPI.Models;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Walks the clips needing a preview in batches on a background task so the whole
    /// backfill runs from a single call. Progress lives in memory only: an app
    /// restart loses it, and the run is simply started again — previews already in
    /// R2 come back as "exists" rather than being cut twice.
    /// </summary>
    public class ClipPreviewMotionRunner : IClipPreviewMotionRunner
    {
        private const int MaxConsecutiveFailures = 5;
        private static readonly TimeSpan FailureBackoff = TimeSpan.FromSeconds(30);

        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<ClipPreviewMotionRunner> _logger;
        private readonly object _gate = new();

        private ClipPreviewRunStatus _status = new();
        private CancellationTokenSource? _cts;
        private Task? _run;

        public ClipPreviewMotionRunner(
            IServiceScopeFactory scopeFactory,
            ILogger<ClipPreviewMotionRunner> logger)
        {
            _scopeFactory = scopeFactory;
            _logger = logger;
        }

        public (bool Started, ClipPreviewRunStatus Status) Start(
            int afterImageId,
            int batchSize,
            bool overwrite,
            bool motionOnly)
        {
            lock (_gate)
            {
                if (_status.Running)
                {
                    return (false, Snapshot());
                }

                _status = new ClipPreviewRunStatus
                {
                    Running = true,
                    StartedAtUtc = DateTime.UtcNow,
                    StartedAfterImageId = afterImageId,
                    BatchSize = batchSize,
                    Overwrite = overwrite,
                    MotionOnly = motionOnly,
                    Cursor = afterImageId
                };

                _cts = new CancellationTokenSource();
                _run = Task.Run(() => RunAsync(batchSize, overwrite, motionOnly, _cts.Token));

                return (true, Snapshot());
            }
        }

        public ClipPreviewRunStatus Stop()
        {
            lock (_gate)
            {
                if (_status.Running)
                {
                    _status.StopRequested = true;
                    _cts?.Cancel();
                }

                return Snapshot();
            }
        }

        public ClipPreviewRunStatus GetStatus()
        {
            lock (_gate)
            {
                return Snapshot();
            }
        }

        private async Task RunAsync(
            int batchSize,
            bool overwrite,
            bool motionOnly,
            CancellationToken cancellationToken)
        {
            var consecutiveFailures = 0;

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    int cursor;
                    lock (_gate)
                    {
                        cursor = _status.Cursor;
                    }

                    ClipPreviewMotionResult result;

                    try
                    {
                        using var scope = _scopeFactory.CreateScope();
                        var previews = scope.ServiceProvider.GetRequiredService<IClipPreviewService>();

                        result = await previews.GeneratePreviewBatchAsync(
                            batchSize,
                            cursor,
                            overwrite,
                            motionOnly,
                            cancellationToken);

                        consecutiveFailures = 0;
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        consecutiveFailures++;

                        lock (_gate)
                        {
                            _status.LastError = ex.Message;
                            _status.LastErrorAtUtc = DateTime.UtcNow;
                        }

                        _logger.LogError(
                            ex,
                            "Clip preview run batch after image {Cursor} failed ({Failures} in a row)",
                            cursor,
                            consecutiveFailures);

                        if (consecutiveFailures >= MaxConsecutiveFailures)
                        {
                            break;
                        }

                        await Task.Delay(FailureBackoff, cancellationToken);
                        continue;
                    }

                    lock (_gate)
                    {
                        _status.Batches++;
                        _status.Requested += result.Requested;
                        _status.Created += result.Created;
                        _status.Exists += result.Exists;
                        _status.Skipped += result.Skipped;
                        _status.Errors += result.Errors;

                        if (result.NextAfterImageId.HasValue)
                        {
                            _status.Cursor = result.NextAfterImageId.Value;
                        }
                    }

                    // Nothing eligible past the cursor, so every clip is done.
                    if (result.Requested == 0 || !result.NextAfterImageId.HasValue)
                    {
                        lock (_gate)
                        {
                            _status.CompletedAll = true;
                        }

                        break;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Clip preview run stopped on request");
            }
            finally
            {
                lock (_gate)
                {
                    _status.Running = false;
                    _status.FinishedAtUtc = DateTime.UtcNow;
                }

                _logger.LogInformation(
                    "Clip preview run finished: {Created} created, {Exists} already present, {Errors} errors",
                    _status.Created,
                    _status.Exists,
                    _status.Errors);
            }
        }

        private ClipPreviewRunStatus Snapshot()
        {
            var from = _status.StartedAtUtc;
            var to = _status.FinishedAtUtc ?? DateTime.UtcNow;

            return new ClipPreviewRunStatus
            {
                Running = _status.Running,
                StopRequested = _status.StopRequested,
                StartedAtUtc = _status.StartedAtUtc,
                FinishedAtUtc = _status.FinishedAtUtc,
                ElapsedMinutes = from.HasValue
                    ? Math.Round((to - from.Value).TotalMinutes, 1)
                    : null,
                StartedAfterImageId = _status.StartedAfterImageId,
                BatchSize = _status.BatchSize,
                Overwrite = _status.Overwrite,
                MotionOnly = _status.MotionOnly,
                Cursor = _status.Cursor,
                Batches = _status.Batches,
                Requested = _status.Requested,
                Created = _status.Created,
                Exists = _status.Exists,
                Skipped = _status.Skipped,
                Errors = _status.Errors,
                CompletedAll = _status.CompletedAll,
                LastError = _status.LastError,
                LastErrorAtUtc = _status.LastErrorAtUtc
            };
        }
    }
}
