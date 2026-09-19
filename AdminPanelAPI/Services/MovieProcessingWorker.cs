
using Microsoft.Extensions.Hosting;

namespace AdminPanelAPI.Services
{
    public class MovieProcessingWorker : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly IMovieJobQueue _queue;
        private readonly ILogger<MovieProcessingWorker> _logger;
        private readonly IConfiguration _configuration;

        private readonly int _maxParallelMovieJobs;

        public MovieProcessingWorker(
            IServiceProvider serviceProvider,
            IMovieJobQueue queue,
            ILogger<MovieProcessingWorker> logger,
            IConfiguration configuration)
        {
            _serviceProvider = serviceProvider;
            _queue = queue;
            _logger = logger;
            _configuration = configuration;

            _maxParallelMovieJobs =
                int.TryParse(_configuration["MoviePipeline:MaxParallelMovieJobs"], out var parsed)
                    ? Math.Clamp(parsed, 1, 10)
                    : 2;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation(
                "MovieProcessingWorker started with MaxParallelMovieJobs={MaxParallelMovieJobs}",
                _maxParallelMovieJobs);

            var workers = new List<Task>();

            for (int i = 0; i < _maxParallelMovieJobs; i++)
            {
                var workerNumber = i + 1;
                workers.Add(Task.Run(() => WorkerLoopAsync(workerNumber, stoppingToken), stoppingToken));
            }

            return Task.WhenAll(workers);
        }

        private async Task WorkerLoopAsync(int workerNumber, CancellationToken stoppingToken)
        {
            _logger.LogInformation("Movie worker {WorkerNumber} started.", workerNumber);

            while (!stoppingToken.IsCancellationRequested)
            {
                long jobId = 0;

                try
                {
                    jobId = await _queue.DequeueAsync(stoppingToken);

                    using var scope = _serviceProvider.CreateScope();

                    var repo = scope.ServiceProvider.GetRequiredService<IMovieProcessingJobRepository>();
                    var service = scope.ServiceProvider.GetRequiredService<IMovieProcessingService>();

                    var job = await repo.GetJobAsync(jobId, stoppingToken);

                    if (job == null)
                    {
                        _logger.LogWarning(
                            "Worker {WorkerNumber}: Job {JobId} not found.",
                            workerNumber,
                            jobId);

                        continue;
                    }

                    await repo.MarkRunningAsync(jobId, stoppingToken);

                    _logger.LogInformation(
                        "Worker {WorkerNumber}: Processing job {JobId}, movie {MovieId}",
                        workerNumber,
                        jobId,
                        job.MovieId);

                    await service.ProcessMovieAsync(
                        jobId,
                        job.MovieId,
                        job.Threshold,
                        job.Overwrite,
                        job.MissingOnly,
                        stoppingToken);

                    await repo.MarkCompletedAsync(jobId, stoppingToken);

                    _logger.LogInformation(
                        "Worker {WorkerNumber}: Completed job {JobId}, movie {MovieId}",
                        workerNumber,
                        jobId,
                        job.MovieId);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Worker {WorkerNumber}: Job {JobId} failed.",
                        workerNumber,
                        jobId);

                    if (jobId > 0)
                    {
                        try
                        {
                            using var scope = _serviceProvider.CreateScope();
                            var repo = scope.ServiceProvider.GetRequiredService<IMovieProcessingJobRepository>();

                            await repo.MarkFailedAsync(
                                jobId,
                                ex.Message,
                                CancellationToken.None);
                        }
                        catch (Exception innerEx)
                        {
                            _logger.LogError(
                                innerEx,
                                "Worker {WorkerNumber}: Failed to mark job {JobId} as failed.",
                                workerNumber,
                                jobId);
                        }
                    }
                }
            }

            _logger.LogInformation("Movie worker {WorkerNumber} stopped.", workerNumber);
        }
    }
}