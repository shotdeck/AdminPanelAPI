namespace AdminPanelAPI.Services
{
    public class CaptionEmbeddingWorker : BackgroundService
    {
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
                        var concurrency = Math.Abs(job.BatchSize);
                        _logger.LogInformation(
                            "CaptionEmbeddingWorker: Processing job {JobId} (process-all, concurrency={Concurrency})",
                            jobId, concurrency);

                        await service.ProcessAllAsync(jobId, concurrency, stoppingToken);
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
    }
}
