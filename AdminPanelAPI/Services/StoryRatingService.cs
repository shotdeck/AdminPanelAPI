using System.Net;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Thin pass-through to the Modal story app, which reads a movie's
    /// walkthrough and rates every shot for how much the moment is worth a
    /// still. The ratings sit in R2 beside the walkthrough, and the key image
    /// analysis reads them from there, so this only has to start a job and
    /// report how it is getting on.
    /// </summary>
    public interface IStoryRatingService
    {
        Task<TranscodeResult> StartAsync(
            string sourceKey, int movieId, string? synopsis, CancellationToken ct);

        Task<TranscodeResult> GetJobAsync(string jobId, CancellationToken ct);

        /// <summary>The finished ratings, read back out of R2.</summary>
        Task<TranscodeResult> GetStoredAsync(string sourceKey, CancellationToken ct);
    }

    public sealed class StoryRatingService : IStoryRatingService
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<StoryRatingService> _logger;
        private readonly string _baseUrl;
        private readonly string? _bucketName;

        public StoryRatingService(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            ILogger<StoryRatingService> logger)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _baseUrl = (configuration["MovieFiles:StoryApiBaseUrl"]
                ?? "https://semanticsearch--shotdeck-story-api.modal.run").TrimEnd('/');
            _bucketName = configuration["MovieFiles:BucketName"];
        }

        public Task<TranscodeResult> StartAsync(
            string sourceKey, int movieId, string? synopsis, CancellationToken ct)
        {
            var query = $"?key={Uri.EscapeDataString(sourceKey)}&movie_id={movieId}";
            if (!string.IsNullOrWhiteSpace(synopsis))
                query += $"&synopsis={Uri.EscapeDataString(synopsis)}";
            if (!string.IsNullOrWhiteSpace(_bucketName))
                query += $"&bucket={Uri.EscapeDataString(_bucketName)}";

            return SendAsync(HttpMethod.Post, "/story" + query, ct);
        }

        public Task<TranscodeResult> GetJobAsync(string jobId, CancellationToken ct) =>
            SendAsync(HttpMethod.Get, $"/story/{Uri.EscapeDataString(jobId)}", ct);

        public Task<TranscodeResult> GetStoredAsync(string sourceKey, CancellationToken ct)
        {
            var query = $"?key={Uri.EscapeDataString(sourceKey)}";
            if (!string.IsNullOrWhiteSpace(_bucketName))
                query += $"&bucket={Uri.EscapeDataString(_bucketName)}";

            return SendAsync(HttpMethod.Get, "/stored" + query, ct);
        }

        private async Task<TranscodeResult> SendAsync(
            HttpMethod method, string pathAndQuery, CancellationToken ct)
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(120);

            using var request = new HttpRequestMessage(method, _baseUrl + pathAndQuery);
            try
            {
                using var response = await client.SendAsync(request, ct);
                var body = await response.Content.ReadAsStringAsync(ct);
                if (!response.IsSuccessStatusCode && response.StatusCode != HttpStatusCode.NotFound)
                    _logger.LogWarning(
                        "Story API {Method} {Path} failed: {Status} {Body}",
                        method, pathAndQuery, (int)response.StatusCode, body);

                return new TranscodeResult(response.StatusCode, body);
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
            {
                _logger.LogError(ex, "Story API {Method} {Path} unreachable.", method, pathAndQuery);
                return new TranscodeResult(
                    HttpStatusCode.BadGateway,
                    JsonSerializer.Serialize(new { error = $"Story API unreachable: {ex.Message}" }));
            }
        }
    }
}
