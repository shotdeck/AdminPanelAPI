using Npgsql;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Keeps a bank of pre-analysed camera-movement images topped up so that a
    /// reviewer's "Fetch Next Batch" is an instant assignment rather than a
    /// wait on the GPU.
    ///
    /// For every media type the worker counts the analysed-but-unowned images
    /// in <c>frl_camera_movement_bank</c>; while that is below the target it
    /// claims the next most popular un-analysed images of that type, runs them
    /// through the analysis API and banks them. Claims are shared with the
    /// live fetch path, so the two never analyse the same image. Ownership
    /// (and therefore the reviewer stats) starts only when a fetch takes an
    /// image out of the bank.
    /// </summary>
    public sealed class CameraMovementBankWorker : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<CameraMovementBankWorker> _logger;
        private readonly bool _enabled;
        private readonly int _targetPerMediaType;
        private readonly int _batchSize;

        /// <summary>Give the app and the database tunnel a moment to come up.</summary>
        private static readonly TimeSpan StartupDelay = TimeSpan.FromSeconds(60);

        /// <summary>How long to wait between sweeps once every bank is full.</summary>
        private static readonly TimeSpan IdleDelay = TimeSpan.FromMinutes(1);

        /// <summary>
        /// How long to stand down when the analysis API looks to be down. Every
        /// failed attempt counts towards an image being parked, so a whole
        /// batch failing is treated as an outage rather than ten bad clips.
        /// </summary>
        private static readonly TimeSpan OutageDelay = TimeSpan.FromMinutes(5);

        public CameraMovementBankWorker(
            IServiceScopeFactory scopeFactory,
            IHttpClientFactory httpClientFactory,
            IConfiguration configuration,
            ILogger<CameraMovementBankWorker> logger)
        {
            _scopeFactory = scopeFactory;
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _enabled = configuration.GetValue("CameraMotion:Bank:Enabled", true);
            _targetPerMediaType = Math.Max(0, configuration.GetValue("CameraMotion:Bank:TargetPerMediaType", 1000));
            _batchSize = Math.Clamp(configuration.GetValue("CameraMotion:Bank:BatchSize", 10), 1, 50);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (!_enabled || _targetPerMediaType == 0)
            {
                _logger.LogInformation("Camera-movement bank worker disabled.");
                return;
            }

            _logger.LogInformation(
                "Camera-movement bank worker starting: target {Target} per media type, batches of {Batch}.",
                _targetPerMediaType, _batchSize);

            try { await Task.Delay(StartupDelay, stoppingToken); }
            catch (OperationCanceledException) { return; }

            while (!stoppingToken.IsCancellationRequested)
            {
                TimeSpan delay;
                try
                {
                    delay = await SweepAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Camera-movement bank sweep failed.");
                    delay = OutageDelay;
                }

                try { await Task.Delay(delay, stoppingToken); }
                catch (OperationCanceledException) { return; }
            }
        }

        /// <summary>
        /// One pass over every media type. Returns how long to wait before the
        /// next pass: short when work remains, longer when idle or on outage.
        /// </summary>
        private async Task<TimeSpan> SweepAsync(CancellationToken ct)
        {
            List<string> mediaTypes;
            Dictionary<string, int> counts;

            using (var scope = _scopeFactory.CreateScope())
            {
                var svc = scope.ServiceProvider.GetRequiredService<CameraMovementAnalysisService>();
                await svc.EnsureOpenAsync(ct);
                await svc.EnsureTablesAsync(ct);

                if (!svc.HasR2Settings())
                {
                    _logger.LogWarning("Camera-movement bank: R2 settings missing; not banking.");
                    return OutageDelay;
                }

                mediaTypes = await svc.GetMediaTypesAsync(ct);
                counts = await svc.GetBankCountsAsync(ct);
            }

            var below = mediaTypes
                .Where(t => counts.GetValueOrDefault(t) < _targetPerMediaType)
                .ToList();
            if (below.Count == 0) return IdleDelay;

            if (!await ApiHealthyAsync(ct))
            {
                _logger.LogWarning("Camera-movement bank: analysis API unhealthy; standing down.");
                return OutageDelay;
            }

            var anyWork = false;
            foreach (var mediaType in below)
            {
                ct.ThrowIfCancellationRequested();

                var have = counts.GetValueOrDefault(mediaType);
                var want = Math.Min(_batchSize, _targetPerMediaType - have);
                if (want <= 0) continue;

                using var scope = _scopeFactory.CreateScope();
                var svc = scope.ServiceProvider.GetRequiredService<CameraMovementAnalysisService>();
                await svc.EnsureOpenAsync(ct);

                var images = await svc.ClaimImagesAsync(Guid.NewGuid(), want, mediaType, ct);
                if (images.Count == 0) continue;

                anyWork = true;
                var ids = images.Select(i => i.ImageId).ToList();
                CameraMovementAnalysisService.AnalysisOutcome outcome;
                try
                {
                    outcome = await svc.AnalyzeAsync(images, owner: null, bank: true, ct);
                }
                finally
                {
                    await svc.ReleaseClaimsAsync(ids, CancellationToken.None);
                }

                _logger.LogInformation(
                    "Camera-movement bank [{MediaType}]: {Processed} banked, {Failed} failed ({Have}/{Target}).",
                    mediaType, outcome.Processed, outcome.Failed, have + outcome.Processed, _targetPerMediaType);

                if (outcome.Processed == 0 && outcome.Failed > 0)
                {
                    _logger.LogWarning("Camera-movement bank: whole batch failed; treating as outage.");
                    return OutageDelay;
                }
            }

            // Work remains: go straight round again (a brief pause keeps the
            // loop from spinning if every queue turned out to be empty).
            return anyWork ? TimeSpan.FromSeconds(1) : IdleDelay;
        }

        private async Task<bool> ApiHealthyAsync(CancellationToken ct)
        {
            string apiUrl;
            using (var scope = _scopeFactory.CreateScope())
                apiUrl = scope.ServiceProvider.GetRequiredService<CameraMovementAnalysisService>().ApiUrl;

            try
            {
                var http = _httpClientFactory.CreateClient();
                http.Timeout = TimeSpan.FromMinutes(2); // cold start on a scaled-to-zero container
                using var resp = await http.GetAsync($"{apiUrl.TrimEnd('/')}/healthz", ct);
                return resp.IsSuccessStatusCode;
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                _logger.LogWarning(ex, "Camera-movement bank: health check failed.");
                return false;
            }
        }
    }
}
