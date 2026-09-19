using System.Net;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Thin pass-through to the Modal walkthrough app, which cuts a movie's SF
    /// proxy into shots and describes each one, leaving a timestamped account of
    /// the film in R2 beside its key images. A feature takes a quarter of an
    /// hour of GPU time, so a job is started here and polled.
    /// </summary>
    public interface IWalkthroughService
    {
        Task<TranscodeResult> StartAsync(string sourceKey, int movieId, CancellationToken ct);

        Task<TranscodeResult> GetJobAsync(string jobId, CancellationToken ct);

        /// <summary>The finished walkthrough, read back out of R2.</summary>
        Task<TranscodeResult> GetStoredAsync(string sourceKey, CancellationToken ct);
    }

    public sealed class WalkthroughService : IWalkthroughService
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<WalkthroughService> _logger;
        private readonly string _baseUrl;
        private readonly string? _bucketName;

        public WalkthroughService(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            ILogger<WalkthroughService> logger)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _baseUrl = (configuration["MovieFiles:WalkthroughApiBaseUrl"]
                ?? "https://semanticsearch--shotdeck-walkthrough-api.modal.run").TrimEnd('/');
            _bucketName = configuration["MovieFiles:BucketName"];
        }

        public Task<TranscodeResult> StartAsync(string sourceKey, int movieId, CancellationToken ct)
        {
            var query = $"?key={Uri.EscapeDataString(sourceKey)}&movie_id={movieId}";
            if (!string.IsNullOrWhiteSpace(_bucketName))
                query += $"&bucket={Uri.EscapeDataString(_bucketName)}";

            return SendAsync(HttpMethod.Post, "/walkthrough" + query, ct);
        }

        public Task<TranscodeResult> GetJobAsync(string jobId, CancellationToken ct) =>
            SendAsync(HttpMethod.Get, $"/walkthrough/{Uri.EscapeDataString(jobId)}", ct);

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
            // A film's walkthrough is a megabyte of prose, a slower read than
            // the progress polls.
            client.Timeout = TimeSpan.FromSeconds(120);

            using var request = new HttpRequestMessage(method, _baseUrl + pathAndQuery);
            try
            {
                using var response = await client.SendAsync(request, ct);
                var body = await response.Content.ReadAsStringAsync(ct);
                if (!response.IsSuccessStatusCode && response.StatusCode != HttpStatusCode.NotFound)
                    _logger.LogWarning(
                        "Walkthrough API {Method} {Path} failed: {Status} {Body}",
                        method, pathAndQuery, (int)response.StatusCode, body);

                return new TranscodeResult(response.StatusCode, body);
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
            {
                _logger.LogError(ex, "Walkthrough API {Method} {Path} unreachable.", method, pathAndQuery);
                return new TranscodeResult(
                    HttpStatusCode.BadGateway,
                    JsonSerializer.Serialize(new { error = $"Walkthrough API unreachable: {ex.Message}" }));
            }
        }
    }
}
