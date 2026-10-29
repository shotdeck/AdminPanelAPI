namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Works through relocate-clips jobs (<c>frl_clip_relocate_jobs</c>) one at a
    /// time: starts each on Modal, polls it until the film search and recuts are
    /// done, then lets <see cref="ClipRelocateService"/> write boundaries,
    /// previews and history. Job state lives in the table, so a restart resumes
    /// polling the same Modal call.
    /// </summary>
    public sealed class ClipRelocateWorker : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<ClipRelocateWorker> _logger;

        private static readonly TimeSpan StartupDelay = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan IdleDelay = TimeSpan.FromSeconds(15);
        private static readonly TimeSpan PollDelay = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan ErrorDelay = TimeSpan.FromMinutes(2);

        public ClipRelocateWorker(IServiceScopeFactory scopeFactory, ILogger<ClipRelocateWorker> logger)
        {
            _scopeFactory = scopeFactory;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try { await Task.Delay(StartupDelay, stoppingToken); }
            catch (OperationCanceledException) { return; }

            var tablesReady = false;
            while (!stoppingToken.IsCancellationRequested)
            {
                TimeSpan delay;
                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var svc = scope.ServiceProvider.GetRequiredService<ClipRelocateService>();
                    if (!tablesReady)
                    {
                        await svc.EnsureTablesAsync(stoppingToken);
                        tablesReady = true;
                    }
                    delay = await StepAsync(svc, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Clip relocate step failed.");
                    delay = ErrorDelay;
                }

                try { await Task.Delay(delay, stoppingToken); }
                catch (OperationCanceledException) { return; }
            }
        }

        private async Task<TimeSpan> StepAsync(ClipRelocateService svc, CancellationToken ct)
        {
            var job = await svc.NextJobAsync(ct);
            if (job == null) return IdleDelay;

            try
            {
                if (job.Status == "queued" || job.ModalCallId == null)
                {
                    await svc.StartAsync(job, ct);
                    _logger.LogInformation("Clip relocate job {JobId}: movie {MovieId} started on Modal.", job.Id, job.MovieId);
                    return PollDelay;
                }

                var poll = await svc.PollAsync(job, ct);
                switch (poll.Status)
                {
                    case "running":
                        return PollDelay;
                    case "done" when poll.Result != null:
                        await svc.FinishAsync(job, poll.Result, ct);
                        _logger.LogInformation("Clip relocate job {JobId}: movie {MovieId} done.", job.Id, job.MovieId);
                        return TimeSpan.Zero;
                    default:
                        await svc.FailJobAsync(job.Id, poll.Error ?? $"Modal returned status '{poll.Status}'.", ct);
                        return TimeSpan.Zero;
                }
            }
            catch (InvalidOperationException ex)
            {
                await svc.FailJobAsync(job.Id, ex.Message, ct);
                return TimeSpan.Zero;
            }
        }
    }
}
