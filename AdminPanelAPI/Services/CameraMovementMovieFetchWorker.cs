namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Works through "Fetch Movie" jobs: analyses every remaining image of a
    /// title and assigns each one to the job's reviewer as it completes, so a
    /// reviewer can start tagging while the rest of the title is still being
    /// processed. Job state lives in <c>frl_camera_movement_movie_jobs</c>, so
    /// work carries on regardless of the browser and resumes after a restart.
    /// Jobs run one at a time, oldest first.
    /// </summary>
    public sealed class CameraMovementMovieFetchWorker : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<CameraMovementMovieFetchWorker> _logger;
        private readonly int _batchSize;

        private static readonly TimeSpan StartupDelay = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan IdleDelay = TimeSpan.FromSeconds(5);

        /// <summary>Remaining images are claimed by another fetch; wait for them.</summary>
        private static readonly TimeSpan ClaimedElsewhereDelay = TimeSpan.FromSeconds(20);

        /// <summary>
        /// Every failed attempt counts towards an image being parked, so a
        /// whole batch failing is treated as an analysis-API outage.
        /// </summary>
        private static readonly TimeSpan OutageDelay = TimeSpan.FromMinutes(2);

        public CameraMovementMovieFetchWorker(
            IServiceScopeFactory scopeFactory,
            IConfiguration configuration,
            ILogger<CameraMovementMovieFetchWorker> logger)
        {
            _scopeFactory = scopeFactory;
            _logger = logger;
            _batchSize = Math.Clamp(configuration.GetValue("CameraMotion:MovieFetch:BatchSize", 10), 1, 50);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try { await Task.Delay(StartupDelay, stoppingToken); }
            catch (OperationCanceledException) { return; }

            var claimsReleased = false;
            while (!stoppingToken.IsCancellationRequested)
            {
                TimeSpan delay;
                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var svc = scope.ServiceProvider.GetRequiredService<CameraMovementAnalysisService>();
                    await svc.EnsureOpenAsync(stoppingToken);
                    await svc.EnsureTablesAsync(stoppingToken);

                    if (!claimsReleased)
                    {
                        await svc.ReleaseMovieJobClaimsAsync(stoppingToken);
                        claimsReleased = true;
                    }

                    delay = await StepAsync(svc, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Camera-movement movie fetch step failed.");
                    delay = OutageDelay;
                }

                try { await Task.Delay(delay, stoppingToken); }
                catch (OperationCanceledException) { return; }
            }
        }

        /// <summary>Analyse one batch of the oldest active job.</summary>
        private async Task<TimeSpan> StepAsync(CameraMovementAnalysisService svc, CancellationToken ct)
        {
            var job = await svc.NextMovieJobAsync(ct);
            if (job == null) return IdleDelay;

            if (!svc.HasR2Settings())
            {
                _logger.LogWarning("Camera-movement movie fetch: R2 settings missing; not analysing.");
                return OutageDelay;
            }

            if (job.Status == "queued")
                await svc.MarkMovieJobRunningAsync(job.Id, ct);

            var images = await svc.ClaimImagesAsync(job.ClaimId, _batchSize, null, ct, job.MovieId);
            if (images.Count == 0)
            {
                // Anything left is being analysed by another fetch or the bank
                // worker; wait for it rather than calling the title ready.
                if (await svc.CountMovieRemainingAsync(job.MovieId, ct) > 0)
                    return ClaimedElsewhereDelay;

                // Images another path analysed meanwhile (e.g. banked) still
                // belong to this title's fetch.
                var late = await svc.AssignAnalyzedMovieImagesAsync(job.MovieId, job.Owner, ct);
                await svc.FinishMovieJobAsync(job.Id, late, ct);
                _logger.LogInformation(
                    "Camera-movement movie fetch #{JobId}: movie {MovieId} ready for {Owner}.",
                    job.Id, job.MovieId, job.Owner);
                return TimeSpan.Zero;
            }

            CameraMovementAnalysisService.AnalysisOutcome outcome;
            try
            {
                outcome = await svc.AnalyzeAsync(images, job.Owner, bank: false, ct);
            }
            finally
            {
                await svc.ReleaseClaimsAsync(images.Select(i => i.ImageId).ToList(), CancellationToken.None);
            }
            await svc.RecordMovieJobProgressAsync(job.Id, outcome.Processed, outcome.Failed, ct);

            _logger.LogInformation(
                "Camera-movement movie fetch #{JobId}: {Processed} analysed, {Failed} failed.",
                job.Id, outcome.Processed, outcome.Failed);

            if (outcome.Processed == 0 && outcome.Failed > 0)
            {
                _logger.LogWarning("Camera-movement movie fetch: whole batch failed; treating as outage.");
                return OutageDelay;
            }
            return TimeSpan.Zero;
        }
    }
}
