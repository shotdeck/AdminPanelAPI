
using Microsoft.Extensions.Hosting;

namespace AdminPanelAPI.Services
{
    public class MovieProcessingWorker : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly IMovieJobQueue _queue;
        private readonly ILogger<MovieProcessingWorker> _logger;
        private readonly IConfiguration _configuration;
        private readonly MovieWorkerDiagnostics _diagnostics;

        private readonly int _maxParallelMovieJobs;

        private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(10);

        /// <summary>Keeps a failing claim (e.g. the DB tunnel still coming up) off a hot loop.</summary>
        private static readonly TimeSpan ErrorBackoff = TimeSpan.FromSeconds(5);

        public MovieProcessingWorker(
            IServiceProvider serviceProvider,
            IMovieJobQueue queue,
            ILogger<MovieProcessingWorker> logger,
            IConfiguration configuration,
            MovieWorkerDiagnostics diagnostics)
        {
            _serviceProvider = serviceProvider;
            _queue = queue;
            _logger = logger;
            _configuration = configuration;
            _diagnostics = diagnostics;

            _maxParallelMovieJobs =
                int.TryParse(_configuration["MoviePipeline:MaxParallelMovieJobs"], out var parsed)
                    ? Math.Clamp(parsed, 1, 10)
                    : 2;

            _diagnostics.ConfiguredParallelism = _maxParallelMovieJobs;
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

            _diagnostics.LoopStarted(workerNumber);

            while (!stoppingToken.IsCancellationRequested)
            {
                long jobId = 0;

                try
                {
                    using var scope = _serviceProvider.CreateScope();

                    var repo = scope.ServiceProvider.GetRequiredService<IMovieProcessingJobRepository>();
                    var service = scope.ServiceProvider.GetRequiredService<IMovieProcessingService>();

                    var job = await repo.ClaimNextQueuedJobAsync(stoppingToken);

                    _diagnostics.Polled(workerNumber);

                    if (job == null)
                    {
                        await WaitForWorkAsync(stoppingToken);
                        continue;
                    }

                    jobId = job.JobId;

                    _diagnostics.Claimed(workerNumber, jobId);

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

                    _diagnostics.Finished(workerNumber);

                    _logger.LogInformation(
                        "Worker {WorkerNumber}: Completed job {JobId}, movie {MovieId}",
                        workerNumber,
                        jobId,
                        job.MovieId);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    if (jobId > 0)
                        await RequeueAsync(workerNumber, jobId);

                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Worker {WorkerNumber}: Job {JobId} failed.",
                        workerNumber,
                        jobId);

                    _diagnostics.Failed(workerNumber, ex.Message);

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
                    else
                    {
                        try
                        {
                            await Task.Delay(ErrorBackoff, stoppingToken);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                    }
                }
            }

            _logger.LogInformation("Movie worker {WorkerNumber} stopped.", workerNumber);
        }

        /// <summary>
        /// Sleeps until a job is queued in this process, or the poll interval
        /// elapses so jobs queued elsewhere (another instance, or before a restart)
        /// are still picked up.
        /// </summary>
        private async Task WaitForWorkAsync(CancellationToken stoppingToken)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            timeout.CancelAfter(PollInterval);

            try
            {
                await _queue.DequeueAsync(timeout.Token);
            }
            catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
            {
            }
        }

        private async Task RequeueAsync(int workerNumber, long jobId)
        {
            try
            {
                using var scope = _serviceProvider.CreateScope();
                var repo = scope.ServiceProvider.GetRequiredService<IMovieProcessingJobRepository>();

                await repo.RequeueJobAsync(jobId, CancellationToken.None);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Worker {WorkerNumber}: Failed to requeue job {JobId} on shutdown.",
                    workerNumber,
                    jobId);
            }
        }
    }
}