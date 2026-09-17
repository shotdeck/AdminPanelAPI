namespace AdminPanelAPI.Services
{
    public class DialogueTranscriptionWorker : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly IDialogueJobQueue _queue;
        private readonly ILogger<DialogueTranscriptionWorker> _logger;

        public DialogueTranscriptionWorker(
            IServiceProvider serviceProvider,
            IDialogueJobQueue queue,
            ILogger<DialogueTranscriptionWorker> logger)
        {
            _serviceProvider = serviceProvider;
            _queue = queue;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("DialogueTranscriptionWorker started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                long jobId = 0;

                try
                {
                    jobId = await _queue.DequeueAsync(stoppingToken);

                    using var scope = _serviceProvider.CreateScope();

                    var repo = scope.ServiceProvider
                        .GetRequiredService<IDialogueTranscriptionJobRepository>();
                    var service = scope.ServiceProvider
                        .GetRequiredService<IDialogueTranscriptionService>();

                    var job = await repo.GetJobAsync(jobId, stoppingToken);

                    if (job == null)
                    {
                        _logger.LogWarning("Dialogue job {JobId} not found.", jobId);
                        continue;
                    }

                    await repo.MarkRunningAsync(jobId, stoppingToken);

                    _logger.LogInformation(
                        "Processing dialogue job {JobId}, movie {MovieId}",
                        jobId, job.MovieId);

                    await service.TranscribeMovieAsync(
                        jobId, job.MovieId, job.R2Key, stoppingToken);

                    // The service doesn't persist the count mid-run, so read the
                    // movie's actual word count here for an accurate status.
                    var wordCount = await repo.GetMovieWordCountAsync(job.MovieId, stoppingToken);
                    await repo.MarkCompletedAsync(jobId, wordCount, stoppingToken);

                    _logger.LogInformation(
                        "Completed dialogue job {JobId}, movie {MovieId}",
                        jobId, job.MovieId);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Dialogue job {JobId} failed.", jobId);

                    if (jobId > 0)
                    {
                        try
                        {
                            using var scope = _serviceProvider.CreateScope();
                            var repo = scope.ServiceProvider
                                .GetRequiredService<IDialogueTranscriptionJobRepository>();

                            await repo.MarkFailedAsync(
                                jobId, ex.Message, CancellationToken.None);
                        }
                        catch (Exception innerEx)
                        {
                            _logger.LogError(
                                innerEx,
                                "Failed to mark dialogue job {JobId} as failed.",
                                jobId);
                        }
                    }
                }
            }

            _logger.LogInformation("DialogueTranscriptionWorker stopped.");
        }
    }
}
