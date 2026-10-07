using AdminPanelAPI.Interfaces;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Restarts the clip preview backfill after the app recycles. The run itself
    /// lives in memory, so without this a deploy or an App Service restart leaves
    /// a multi-day backfill silently stopped part way through.
    /// </summary>
    public class ClipPreviewRunResumeService : BackgroundService
    {
        private static readonly TimeSpan StartupDelay = TimeSpan.FromSeconds(20);

        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IClipPreviewMotionRunner _runner;
        private readonly ILogger<ClipPreviewRunResumeService> _logger;

        public ClipPreviewRunResumeService(
            IServiceScopeFactory scopeFactory,
            IClipPreviewMotionRunner runner,
            ILogger<ClipPreviewRunResumeService> logger)
        {
            _scopeFactory = scopeFactory;
            _runner = runner;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // The database tunnel is not up the instant the app starts.
            await Task.Delay(StartupDelay, stoppingToken);

            try
            {
                using var scope = _scopeFactory.CreateScope();
                var repo = scope.ServiceProvider.GetRequiredService<IMovieProcessingJobRepository>();

                var stored = await repo.GetClipPreviewRunAsync(stoppingToken);

                if (stored is null || !stored.Running || stored.CompletedAll)
                    return;

                var (started, status) = _runner.Start(
                    stored.Cursor,
                    stored.BatchSize,
                    stored.Overwrite,
                    stored.MotionOnly);

                _logger.LogInformation(
                    started
                        ? "Resumed clip preview run after image {Cursor} (motionOnly={MotionOnly})"
                        : "Clip preview run already going, nothing to resume (cursor {Cursor}, motionOnly={MotionOnly})",
                    status.Cursor,
                    status.MotionOnly);
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Could not resume the clip preview run");
            }
        }
    }
}
