namespace AdminPanelAPI.Services
{
    public class MusicIdentificationWorker : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly IMusicJobQueue _queue;
        private readonly ILogger<MusicIdentificationWorker> _logger;
        private readonly int _maxParallelJobs;

        public MusicIdentificationWorker(
            IServiceProvider serviceProvider,
            IMusicJobQueue queue,
            IConfiguration configuration,
            ILogger<MusicIdentificationWorker> logger)
        {
            _serviceProvider = serviceProvider;
            _queue = queue;
            _logger = logger;
            _maxParallelJobs = Math.Max(
                1,
                configuration.GetValue("MusicIdentification:MaxParallelJobs", 3));
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation(
                "MusicIdentificationWorker started (maxParallelJobs={Max}).",
                _maxParallelJobs);

            using var throttler = new SemaphoreSlim(_maxParallelJobs);

            while (!stoppingToken.IsCancellationRequested)
            {
                long jobId;

                try
                {
                    jobId = await _queue.DequeueAsync(stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                try
                {
                    await throttler.WaitAsync(stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                _ = ProcessJobAsync(jobId, throttler, stoppingToken);
            }

            _logger.LogInformation("MusicIdentificationWorker stopped.");
        }

        private async Task ProcessJobAsync(
            long jobId, SemaphoreSlim throttler, CancellationToken stoppingToken)
        {
            try
            {
                using var scope = _serviceProvider.CreateScope();

                var repo = scope.ServiceProvider
                    .GetRequiredService<IMusicIdentificationJobRepository>();
                var service = scope.ServiceProvider
                    .GetRequiredService<IMusicIdentificationService>();

                var job = await repo.GetJobAsync(jobId, stoppingToken);

                if (job == null)
                {
                    _logger.LogWarning("Music job {JobId} not found.", jobId);
                    return;
                }

                await repo.MarkRunningAsync(jobId, stoppingToken);

                _logger.LogInformation(
                    "Processing music job {JobId}, movie {MovieId}",
                    jobId, job.MovieId);

                await service.IdentifyMovieAsync(
                    jobId, job.MovieId, job.R2Key, stoppingToken);

                _logger.LogInformation(
                    "Completed music job {JobId}, movie {MovieId}",
                    jobId, job.MovieId);
            }
            catch (OperationCanceledException)
            {
                // Worker is shutting down; leave the job for a restart to pick up.
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Music job {JobId} failed.", jobId);

                try
                {
                    using var scope = _serviceProvider.CreateScope();
                    var repo = scope.ServiceProvider
                        .GetRequiredService<IMusicIdentificationJobRepository>();

                    await repo.MarkFailedAsync(jobId, ex.Message, CancellationToken.None);
                }
                catch (Exception innerEx)
                {
                    _logger.LogError(
                        innerEx,
                        "Failed to mark music job {JobId} as failed.",
                        jobId);
                }
            }
            finally
            {
                throttler.Release();
            }
        }
    }
}
