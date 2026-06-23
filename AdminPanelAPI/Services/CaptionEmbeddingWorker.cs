namespace AdminPanelAPI.Services
{
    public class CaptionEmbeddingWorker : BackgroundService
    {
        private const int MaxRetries = 10;
        private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(30);

        private readonly IServiceProvider _serviceProvider;
        private readonly ICaptionEmbeddingJobQueue _queue;
        private readonly ILogger<CaptionEmbeddingWorker> _logger;

        public CaptionEmbeddingWorker(
            IServiceProvider serviceProvider,
            ICaptionEmbeddingJobQueue queue,
            ILogger<CaptionEmbeddingWorker> logger)
        {
            _serviceProvider = serviceProvider;
            _queue = queue;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("CaptionEmbeddingWorker started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                long jobId = 0;

                try
                {
                    jobId = await _queue.DequeueAsync(stoppingToken);

                    using var scope = _serviceProvider.CreateScope();

                    var repo = scope.ServiceProvider.GetRequiredService<ICaptionEmbeddingJobRepository>();
                    var service = scope.ServiceProvider.GetRequiredService<ICaptionEmbeddingService>();

                    var job = await repo.GetJobAsync(jobId, stoppingToken);

                    if (job == null)
                    {
                        _logger.LogWarning("CaptionEmbeddingWorker: Job {JobId} not found.", jobId);
                        continue;
                    }

                    await repo.MarkRunningAsync(jobId, stoppingToken);

                    if (job.BatchSize < 0)
                    {
                        var absBatchSize = Math.Abs(job.BatchSize);
                        var skipCaption = absBatchSize > 1000;
                        var concurrency = skipCaption ? absBatchSize - 1000 : absBatchSize;

                        _logger.LogInformation(
                            "CaptionEmbeddingWorker: Processing job {JobId} (process-all, concurrency={Concurrency}, skipCaption={SkipCaption})",
                            jobId, concurrency, skipCaption);

                        await RunWithAutoRetryAsync(
                            jobId, concurrency, skipCaption, service, repo, stoppingToken);
                    }
                    else
                    {
                        _logger.LogInformation(
                            "CaptionEmbeddingWorker: Processing job {JobId}, batchSize={BatchSize}",
                            jobId, job.BatchSize);

                        await service.ProcessBatchAsync(jobId, job.BatchSize, stoppingToken);
                    }

                    await repo.MarkCompletedAsync(jobId, stoppingToken);

                    _logger.LogInformation(
                        "CaptionEmbeddingWorker: Completed job {JobId}",
                        jobId);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "CaptionEmbeddingWorker: Job {JobId} failed.",
                        jobId);

                    if (jobId > 0)
                    {
                        try
                        {
                            using var scope = _serviceProvider.CreateScope();
                            var repo = scope.ServiceProvider.GetRequiredService<ICaptionEmbeddingJobRepository>();

                            await repo.MarkFailedAsync(
                                jobId,
                                ex.Message,
                                CancellationToken.None);
                        }
                        catch (Exception innerEx)
                        {
                            _logger.LogError(
                                innerEx,
                                "CaptionEmbeddingWorker: Failed to mark job {JobId} as failed.",
                                jobId);
                        }
                    }
                }
            }

            _logger.LogInformation("CaptionEmbeddingWorker stopped.");
        }

        private async Task RunWithAutoRetryAsync(
            long jobId,
            int concurrency,
            bool skipCaption,
            ICaptionEmbeddingService service,
            ICaptionEmbeddingJobRepository repo,
            CancellationToken stoppingToken)
        {
            for (int attempt = 1; attempt <= MaxRetries; attempt++)
            {
                try
                {
                    await service.ProcessAllAsync(jobId, concurrency, stoppingToken, skipCaption);
                    return; // success
                }
                catch (OperationCanceledException)
                {
                    throw; // don't retry cancellations
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "CaptionEmbeddingWorker: Job {JobId} failed on attempt {Attempt}/{MaxRetries}. Retrying in {Delay}s...",
                        jobId, attempt, MaxRetries, RetryDelay.TotalSeconds);

                    if (attempt == MaxRetries)
                        throw; // exhausted retries, let outer handler mark as failed

                    await repo.UpdateProgressAsync(
                        jobId,
                        $"Auto-retry: attempt {attempt} failed ({ex.Message}). Restarting in {RetryDelay.TotalSeconds}s...",
                        null, null, stoppingToken);

                    await Task.Delay(RetryDelay, stoppingToken);

                    _logger.LogInformation(
                        "CaptionEmbeddingWorker: Job {JobId} restarting (attempt {Attempt}/{MaxRetries})",
                        jobId, attempt + 1, MaxRetries);
                }
            }
        }
    }
}
