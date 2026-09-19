using System.Globalization;
using System.Net;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Thin pass-through to the Modal key-image app, which reads a movie's SF
    /// proxy out of R2 and proposes the frames worth putting in front of a
    /// tagger. A feature takes minutes of GPU time, so a job is started here
    /// and polled; the proposals are only pulled back once it finishes.
    /// </summary>
    public interface IKeyImageAnalysisService
    {
        /// <summary>
        /// storyFrom names how the story half is to be judged, "walkthrough"
        /// or "description"; left unset the run takes the walkthrough if the
        /// movie has one.
        /// </summary>
        Task<TranscodeResult> StartAsync(
            string sourceKey, int movieId, string? description, string? storyFrom,
            CancellationToken ct);

        Task<TranscodeResult> GetJobAsync(string jobId, bool includeProposals, CancellationToken ct);

        Task<TranscodeResult> FindJobsAsync(string sourceKey, CancellationToken ct);

        /// <summary>
        /// Cut one frame of a master at its own resolution and put it in R2.
        /// Unlike an analysis this is a single decode, so it answers inline.
        /// </summary>
        Task<TranscodeResult> CutStillAsync(
            string sourceKey, int? frameNumber, double? seconds, CancellationToken ct);
    }

    public sealed class KeyImageAnalysisService : IKeyImageAnalysisService
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<KeyImageAnalysisService> _logger;
        private readonly string _baseUrl;
        private readonly string? _bucketName;

        public KeyImageAnalysisService(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            ILogger<KeyImageAnalysisService> logger)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _baseUrl = (configuration["MovieFiles:KeyImageApiBaseUrl"]
                ?? "https://semanticsearch--shotdeck-key-images-api.modal.run").TrimEnd('/');
            _bucketName = configuration["MovieFiles:BucketName"];
        }

        public Task<TranscodeResult> StartAsync(
            string sourceKey, int movieId, string? description, string? storyFrom,
            CancellationToken ct)
        {
            var query = $"?key={Uri.EscapeDataString(sourceKey)}&movie_id={movieId}";
            if (!string.IsNullOrWhiteSpace(description))
                query += $"&description={Uri.EscapeDataString(description)}";
            if (!string.IsNullOrWhiteSpace(storyFrom))
                query += $"&story_from={Uri.EscapeDataString(storyFrom)}";
            if (!string.IsNullOrWhiteSpace(_bucketName))
                query += $"&bucket={Uri.EscapeDataString(_bucketName)}";

            return SendAsync(HttpMethod.Post, "/analysis" + query, ct);
        }

        public Task<TranscodeResult> GetJobAsync(
            string jobId, bool includeProposals, CancellationToken ct) =>
            SendAsync(
                HttpMethod.Get,
                $"/analysis/{Uri.EscapeDataString(jobId)}" +
                $"?include_proposals={(includeProposals ? "true" : "false")}",
                ct);

        public Task<TranscodeResult> FindJobsAsync(string sourceKey, CancellationToken ct) =>
            SendAsync(HttpMethod.Get, $"/analysis?key={Uri.EscapeDataString(sourceKey)}", ct);

        public Task<TranscodeResult> CutStillAsync(
            string sourceKey, int? frameNumber, double? seconds, CancellationToken ct)
        {
            var query = $"?key={Uri.EscapeDataString(sourceKey)}";
            if (frameNumber.HasValue)
                query += $"&frame={frameNumber.Value}";
            else if (seconds.HasValue)
                query += $"&seconds={seconds.Value.ToString("0.###", CultureInfo.InvariantCulture)}";
            if (!string.IsNullOrWhiteSpace(_bucketName))
                query += $"&bucket={Uri.EscapeDataString(_bucketName)}";

            return SendAsync(HttpMethod.Post, "/still" + query, ct);
        }

        private async Task<TranscodeResult> SendAsync(
            HttpMethod method, string pathAndQuery, CancellationToken ct)
        {
            var client = _httpClientFactory.CreateClient();
            // A finished job hands back a thousand-odd proposals, which is a
            // slower read than the progress polls.
            client.Timeout = TimeSpan.FromSeconds(120);

            using var request = new HttpRequestMessage(method, _baseUrl + pathAndQuery);
            try
            {
                using var response = await client.SendAsync(request, ct);
                var body = await response.Content.ReadAsStringAsync(ct);
                if (!response.IsSuccessStatusCode)
                    _logger.LogWarning(
                        "Key image API {Method} {Path} failed: {Status} {Body}",
                        method, pathAndQuery, (int)response.StatusCode, body);

                return new TranscodeResult(response.StatusCode, body);
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
            {
                _logger.LogError(ex, "Key image API {Method} {Path} unreachable.", method, pathAndQuery);
                return new TranscodeResult(
                    HttpStatusCode.BadGateway,
                    JsonSerializer.Serialize(new { error = $"Key image API unreachable: {ex.Message}" }));
            }
        }
    }
}
