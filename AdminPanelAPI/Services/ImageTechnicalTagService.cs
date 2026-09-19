using System.Net;
using System.Text;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// The image tagger: a model that reads a still's technical terms — frame
    /// size, lens, lighting, time of day and the rest — and hands back its
    /// choice per category with the alternatives it ranked below. Frames are
    /// sent as presigned R2 links so the service fetches them itself, and up to
    /// a batch at a time.
    /// </summary>
    public interface IImageTechnicalTagService
    {
        /// <summary>Most frames a single request may carry.</summary>
        int BatchLimit { get; }

        /// <summary>
        /// The categories and their legal values, which is what the tagging page
        /// offers a tagger. Kept for as long as the app runs: the taxonomy only
        /// changes when the model does, and asking the tagger for it wakes a
        /// container, which a tagger opening a frame should never wait on.
        /// </summary>
        Task<TranscodeResult> GetTaxonomyAsync(CancellationToken ct);

        /// <summary>
        /// Read the categories again whatever the age of the copy held. Meant for
        /// just after a batch of frames has been read, when the tagger's
        /// container is warm and the answer costs nothing.
        /// </summary>
        Task RefreshTaxonomyAsync(CancellationToken ct);

        /// <summary>
        /// Read a batch of stills. Each image is named by the caller so the
        /// results can be matched back to the frames, and one image failing does
        /// not fail the rest.
        /// </summary>
        Task<TranscodeResult> TagAsync(
            IEnumerable<(string ImageId, string ImageUrl)> images, CancellationToken ct);
    }

    public sealed class ImageTechnicalTagService : IImageTechnicalTagService
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<ImageTechnicalTagService> _logger;
        private readonly string _baseUrl;

        /// <summary>The taxonomy as the service last gave it, and when.</summary>
        private string? _taxonomy;
        private DateTimeOffset _taxonomyAt = DateTimeOffset.MinValue;
        private static readonly TimeSpan TaxonomyFor = TimeSpan.FromHours(6);

        /// <summary>Whether a refresh behind a stale answer is already going.</summary>
        private int _taxonomyRefreshing;

        public ImageTechnicalTagService(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            ILogger<ImageTechnicalTagService> logger)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _baseUrl = (configuration["MovieFiles:ImageTagApiBaseUrl"]
                ?? "https://semanticsearch--image-technical-tagger-serve-tagger-web.modal.run")
                .TrimEnd('/');
        }

        public int BatchLimit => 32;

        public Task<TranscodeResult> GetTaxonomyAsync(CancellationToken ct)
        {
            if (_taxonomy is not { Length: > 0 } cached)
                return FetchTaxonomyAsync(ct);

            // A stale copy is still the right answer — the taxonomy changes with
            // the model, not the hour — so it goes back at once and the refresh
            // happens behind the caller rather than waiting on a cold container.
            if (DateTimeOffset.UtcNow - _taxonomyAt >= TaxonomyFor &&
                Interlocked.CompareExchange(ref _taxonomyRefreshing, 1, 0) == 0)
            {
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await FetchTaxonomyAsync(CancellationToken.None);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Refreshing the taxonomy failed.");
                    }
                    finally
                    {
                        Interlocked.Exchange(ref _taxonomyRefreshing, 0);
                    }
                });
            }

            return Task.FromResult(new TranscodeResult(HttpStatusCode.OK, cached));
        }

        public async Task RefreshTaxonomyAsync(CancellationToken ct)
        {
            if (Interlocked.CompareExchange(ref _taxonomyRefreshing, 1, 0) != 0)
                return;

            try
            {
                await FetchTaxonomyAsync(ct);
            }
            finally
            {
                Interlocked.Exchange(ref _taxonomyRefreshing, 0);
            }
        }

        private async Task<TranscodeResult> FetchTaxonomyAsync(CancellationToken ct)
        {
            var result = await SendAsync(HttpMethod.Get, "/taxonomy", null, ct);
            if (result.IsSuccess)
            {
                _taxonomy = result.Body;
                _taxonomyAt = DateTimeOffset.UtcNow;
                return result;
            }

            // The tagger being down does not make the categories unknown.
            return _taxonomy is { Length: > 0 } stale
                ? new TranscodeResult(HttpStatusCode.OK, stale)
                : result;
        }

        public Task<TranscodeResult> TagAsync(
            IEnumerable<(string ImageId, string ImageUrl)> images, CancellationToken ct)
        {
            var body = JsonSerializer.Serialize(new
            {
                images = images
                    .Select(image => new { image_id = image.ImageId, image_url = image.ImageUrl })
                    .ToArray()
            });

            return SendAsync(HttpMethod.Post, "/tag/batch", body, ct);
        }

        private async Task<TranscodeResult> SendAsync(
            HttpMethod method, string path, string? body, CancellationToken ct)
        {
            var client = _httpClientFactory.CreateClient();
            // A cold container loads the model before it reads anything, and a
            // full batch is a couple of minutes of work after that.
            client.Timeout = TimeSpan.FromMinutes(10);

            using var request = new HttpRequestMessage(method, _baseUrl + path);
            if (body != null)
                request.Content = new StringContent(body, Encoding.UTF8, "application/json");

            try
            {
                using var response = await client.SendAsync(request, ct);
                var text = await response.Content.ReadAsStringAsync(ct);
                if (!response.IsSuccessStatusCode)
                    _logger.LogWarning(
                        "Image tagger {Method} {Path} failed: {Status} {Body}",
                        method, path, (int)response.StatusCode, text);

                return new TranscodeResult(response.StatusCode, text);
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
            {
                _logger.LogError(ex, "Image tagger {Method} {Path} unreachable.", method, path);
                return new TranscodeResult(
                    HttpStatusCode.BadGateway,
                    JsonSerializer.Serialize(new { error = $"Image tagger unreachable: {ex.Message}" }));
            }
        }
    }
}
