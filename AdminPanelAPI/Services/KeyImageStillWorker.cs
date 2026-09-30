using Npgsql;
using System.Collections.Concurrent;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Cuts the picture of a frame a tagger picked while watching, a moment
    /// after they picked it. The cut goes through the key-image service on
    /// Modal and takes seconds, which is the whole of the wait on Keep this
    /// frame if it is done in the request, so the frame is kept on the click
    /// and its picture follows here.
    /// </summary>
    public sealed class KeyImageStillWorker : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<KeyImageStillWorker> _logger;
        private readonly bool _enabled;

        /// <summary>How often the frames waiting on a picture are looked at.</summary>
        private static readonly TimeSpan PollEvery = TimeSpan.FromSeconds(5);

        /// <summary>Let the app and the database tunnel come up first.</summary>
        private static readonly TimeSpan StartupDelay = TimeSpan.FromSeconds(45);

        /// <summary>Frames cut in one pass, so a picking spree is caught up.</summary>
        private const int PassLimit = 20;

        /// <summary>
        /// Tries at a frame whose cut keeps failing, before it is left to the
        /// still endpoint: the pass comes round every few seconds, and a frame
        /// the service cannot cut would otherwise be asked for for ever.
        /// </summary>
        private const int MaxTries = 3;

        private static readonly ConcurrentDictionary<long, int> Failures = new();

        public KeyImageStillWorker(
            IServiceScopeFactory scopeFactory,
            IConfiguration configuration,
            ILogger<KeyImageStillWorker> logger)
        {
            _scopeFactory = scopeFactory;
            _logger = logger;
            _enabled = configuration.GetValue("MovieFiles:AutoCutKeyImageStills", true);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (!_enabled)
            {
                _logger.LogInformation("Cutting picked frames' pictures is switched off.");
                return;
            }

            try
            {
                await Task.Delay(StartupDelay, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await PassAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Cutting picked frames' pictures failed.");
                }

                try
                {
                    await Task.Delay(PollEvery, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        private async Task PassAsync(CancellationToken ct)
        {
            using var scope = _scopeFactory.CreateScope();
            var services = scope.ServiceProvider;
            var connection = services.GetRequiredService<NpgsqlConnection>();
            var stills = services.GetRequiredService<IKeyImageStillService>();

            await connection.OpenAsync(ct);

            var waiting = await KeyImageStillService.PendingAsync(connection, PassLimit, ct);
            foreach (var frame in waiting)
            {
                if (ct.IsCancellationRequested) return;
                if (Failures.GetValueOrDefault(frame.Id) >= MaxTries) continue;
                try
                {
                    var pictureKey = await stills.CutAsync(
                        frame.Id, frame.MovieId, frame.FrameNumber, frame.PositionSeconds, ct);
                    if (pictureKey != null)
                    {
                        Failures.TryRemove(frame.Id, out _);
                        continue;
                    }

                    Failures[frame.Id] = MaxTries;
                    _logger.LogInformation(
                        "Key image {Id} has no video to cut its picture from.", frame.Id);
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    // The frame is worth keeping without its picture: opening
                    // it asks for the cut again, and so does the next pass.
                    Failures[frame.Id] = Failures.GetValueOrDefault(frame.Id) + 1;
                    _logger.LogWarning(
                        ex, "Cutting the picture of key image {Id} failed.", frame.Id);
                }
            }
        }
    }
}
