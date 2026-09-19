using Npgsql;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Reads the technical terms off the frames a tagger has kept, a batch at a
    /// time, so the terms are sitting there when they open an image rather than
    /// being fetched while they wait. A frame is queued when it is kept and read
    /// here; what the model said is stored per category alongside the
    /// alternatives, and the tagger's own choice is written over the top later.
    /// </summary>
    public sealed class KeyImageTagWorker : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<KeyImageTagWorker> _logger;
        private readonly bool _enabled;
        private readonly int _inFlight;

        /// <summary>How often the queue of kept frames is looked at.</summary>
        private static readonly TimeSpan PollEvery = TimeSpan.FromSeconds(20);

        /// <summary>Let the app and the database tunnel come up first.</summary>
        private static readonly TimeSpan StartupDelay = TimeSpan.FromSeconds(60);

        public KeyImageTagWorker(
            IServiceScopeFactory scopeFactory,
            IConfiguration configuration,
            ILogger<KeyImageTagWorker> logger)
        {
            _scopeFactory = scopeFactory;
            _logger = logger;
            _enabled = configuration.GetValue("MovieFiles:AutoTagKeyImages", true);
            // Batches sent at once. The image tagger runs on Modal, which gives
            // a container per request it cannot already answer, so sending one
            // batch at a time reads a movie on a single GPU however many are
            // free. Several in flight is what spreads the frames across them.
            _inFlight = Math.Clamp(configuration.GetValue("MovieFiles:ImageTagBatchesInFlight", 8), 1, 32);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (!_enabled)
            {
                _logger.LogInformation("Automatic key image tagging is switched off.");
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
                    _logger.LogError(ex, "Key image tagging pass failed.");
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
            var storage = services.GetRequiredService<IMovieFileStorageService>();
            var tagger = services.GetRequiredService<IImageTechnicalTagService>();

            await connection.OpenAsync(ct);
            await EnsureSchemaAsync(connection, ct);

            // Keep going while there are frames waiting rather than taking one
            // helping every poll: a movie is thousands of frames, and the wait
            // between passes would be most of the reading time.
            while (!ct.IsCancellationRequested)
            {
                var claims = await KeyImageTagStore.ClaimAsync(
                    connection, tagger.BatchLimit * _inFlight, ct);
                if (claims.Count == 0)
                    return;

                await TagBatchesAsync(connection, storage, tagger, claims, _logger, ct);

                // A movie whose last kept frame has just been read has finished
                // the AI tags stage, so move it on.
                await KeyImageTagStore.AdvanceReadMoviesAsync(connection, ct);
            }
        }

        /// <summary>
        /// Read a run of frames, a batch per request and the requests together,
        /// so the tagger answers them on as many containers as it needs rather
        /// than queueing them all behind one. What comes back is stored one
        /// batch at a time, since the frames share a single database link.
        /// </summary>
        public static async Task<int> TagBatchesAsync(
            NpgsqlConnection connection,
            IMovieFileStorageService storage,
            IImageTechnicalTagService tagger,
            IReadOnlyList<KeyImageTagClaim> claims,
            ILogger logger,
            CancellationToken ct)
        {
            var batches = claims.Chunk(tagger.BatchLimit).ToList();
            var sent = batches
                .Select(batch => tagger.TagAsync(
                    batch.Select(claim => (
                        ImageId: claim.Id.ToString(),
                        ImageUrl: storage.CreateDownloadUrl(claim.ImageKey, false))),
                    ct))
                .ToList();

            var results = await Task.WhenAll(sent);

            var tagged = 0;
            for (var index = 0; index < batches.Count; index++)
                tagged += await StoreBatchAsync(
                    connection, batches[index], results[index], logger, ct);

            return tagged;
        }

        private static async Task<int> StoreBatchAsync(
            NpgsqlConnection connection,
            IReadOnlyList<KeyImageTagClaim> claims,
            TranscodeResult result,
            ILogger logger,
            CancellationToken ct)
        {
            if (!result.IsSuccess)
            {
                foreach (var claim in claims)
                    await KeyImageTagStore.SaveErrorAsync(connection, claim.Id, result.Body, ct);
                logger.LogWarning(
                    "Reading {Count} key images failed: {Body}", claims.Count, result.Body);
                return 0;
            }

            using var document = JsonDocument.Parse(result.Body);
            var root = document.RootElement;
            var modelVersion = Text(root, "model_version");

            var read = new HashSet<long>();
            var tagged = 0;

            if (root.TryGetProperty("results", out var results) &&
                results.ValueKind == JsonValueKind.Array)
            {
                foreach (var entry in results.EnumerateArray())
                {
                    if (Text(entry, "image_id") is not { Length: > 0 } imageId ||
                        !long.TryParse(imageId, out var id))
                        continue;

                    read.Add(id);

                    if (entry.TryGetProperty("tags", out var tags) &&
                        tags.ValueKind == JsonValueKind.Object)
                    {
                        await KeyImageTagStore.SaveReadingAsync(
                            connection, id, modelVersion, tags, ct);
                        tagged += 1;
                    }
                    else
                    {
                        await KeyImageTagStore.SaveErrorAsync(
                            connection, id,
                            Text(entry, "error") ?? "The image tagger returned no terms.", ct);
                    }
                }
            }

            // A frame the service said nothing at all about would otherwise sit
            // as being read for ever.
            foreach (var claim in claims.Where(claim => !read.Contains(claim.Id)))
                await KeyImageTagStore.SaveErrorAsync(
                    connection, claim.Id, "The image tagger did not answer for this frame.", ct);

            if (tagged > 0)
                logger.LogInformation("Read the technical terms of {Count} key images.", tagged);

            return tagged;
        }

        private static async Task EnsureSchemaAsync(NpgsqlConnection connection, CancellationToken ct)
        {
            await using var cmd = new NpgsqlCommand(KeyImageTagStore.Schema, connection);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        private static string? Text(JsonElement element, string name) =>
            element.TryGetProperty(name, out var value) &&
            value.ValueKind == JsonValueKind.String
                ? value.GetString()
                : null;
    }
}
