namespace AdminPanelAPI.Services
{
    public class MusicIdentificationWorker : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly IMusicJobQueue _queue;
        private readonly ILogger<MusicIdentificationWorker> _logger;

        public MusicIdentificationWorker(
            IServiceProvider serviceProvider,
            IMusicJobQueue queue,
            ILogger<MusicIdentificationWorker> logger)
        {
            _serviceProvider = serviceProvider;
            _queue = queue;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("MusicIdentificationWorker started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                long jobId = 0;

                try
                {
                    jobId = await _queue.DequeueAsync(stoppingToken);

                    using var scope = _serviceProvider.CreateScope();

                    var repo = scope.ServiceProvider
                        .GetRequiredService<IMusicIdentificationJobRepository>();
                    var service = scope.ServiceProvider
                        .GetRequiredService<IMusicIdentificationService>();

                    var job = await repo.GetJobAsync(jobId, stoppingToken);

                    if (job == null)
                    {
                        _logger.LogWarning("Music job {JobId} not found.", jobId);
                        continue;
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
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Music job {JobId} failed.", jobId);

                    if (jobId > 0)
                    {
                        try
                        {
                            using var scope = _serviceProvider.CreateScope();
                            var repo = scope.ServiceProvider
                                .GetRequiredService<IMusicIdentificationJobRepository>();

                            await repo.MarkFailedAsync(
                                jobId, ex.Message, CancellationToken.None);
                        }
                        catch (Exception innerEx)
                        {
                            _logger.LogError(
                                innerEx,
                                "Failed to mark music job {JobId} as failed.",
                                jobId);
                        }
                    }
                }
            }

            _logger.LogInformation("MusicIdentificationWorker stopped.");
        }
    }
}
