using System.Net;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Thin pass-through to the Modal transcode app, which turns an HD master in
    /// R2 into its slimmed "_SF.mp4" sibling using a HandBrake preset export.
    /// The encode itself runs on Modal (x264 veryslow on a feature-length master
    /// is far past any App Service request timeout), so this only starts jobs and
    /// reports their status.
    /// </summary>
    public interface IMovieTranscodeService
    {
        Task<TranscodeResult> GetPresetsAsync(CancellationToken ct);

        /// <summary>Add presets from a HandBrake export; the JSON is passed through as-is.</summary>
        Task<TranscodeResult> ImportPresetAsync(string document, bool overwrite, CancellationToken ct);

        Task<TranscodeResult> DeletePresetAsync(string name, CancellationToken ct);

        Task<TranscodeResult> CreateJobAsync(
            string sourceKey, string? preset, bool overwrite, CancellationToken ct);

        Task<TranscodeResult> GetJobAsync(string jobId, CancellationToken ct);

        Task<TranscodeResult> FindJobsAsync(string sourceKey, CancellationToken ct);

        Task<TranscodeResult> CancelJobAsync(string jobId, CancellationToken ct);
    }

    /// <summary>Raw JSON plus status, so the controller can relay the upstream body verbatim.</summary>
    public sealed record TranscodeResult(HttpStatusCode Status, string Body)
    {
        public bool IsSuccess => (int)Status is >= 200 and < 300;
    }

    public sealed class MovieTranscodeService : IMovieTranscodeService
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<MovieTranscodeService> _logger;
        private readonly string _baseUrl;
        private readonly string? _bucketName;

        public MovieTranscodeService(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            ILogger<MovieTranscodeService> logger)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _baseUrl = (configuration["MovieFiles:TranscodeApiBaseUrl"]
                ?? "https://semanticsearch--shotdeck-transcode-api.modal.run").TrimEnd('/');
            _bucketName = configuration["MovieFiles:BucketName"];
        }

        public Task<TranscodeResult> GetPresetsAsync(CancellationToken ct) =>
            SendAsync(HttpMethod.Get, "/presets", ct);

        public Task<TranscodeResult> ImportPresetAsync(
            string document, bool overwrite, CancellationToken ct) =>
            SendAsync(
                HttpMethod.Post,
                $"/presets?overwrite={(overwrite ? "true" : "false")}",
                ct,
                document);

        public Task<TranscodeResult> DeletePresetAsync(string name, CancellationToken ct) =>
            SendAsync(HttpMethod.Delete, $"/presets/{Uri.EscapeDataString(name)}", ct);

        public Task<TranscodeResult> CreateJobAsync(
            string sourceKey, string? preset, bool overwrite, CancellationToken ct)
        {
            var query = $"?source_key={Uri.EscapeDataString(sourceKey)}" +
                        $"&overwrite={(overwrite ? "true" : "false")}";
            if (!string.IsNullOrWhiteSpace(preset))
                query += $"&preset={Uri.EscapeDataString(preset)}";
            if (!string.IsNullOrWhiteSpace(_bucketName))
                query += $"&bucket={Uri.EscapeDataString(_bucketName)}";

            return SendAsync(HttpMethod.Post, "/jobs" + query, ct);
        }

        public Task<TranscodeResult> GetJobAsync(string jobId, CancellationToken ct) =>
            SendAsync(HttpMethod.Get, $"/jobs/{Uri.EscapeDataString(jobId)}", ct);

        public Task<TranscodeResult> FindJobsAsync(string sourceKey, CancellationToken ct) =>
            SendAsync(HttpMethod.Get, $"/jobs?source_key={Uri.EscapeDataString(sourceKey)}", ct);

        public Task<TranscodeResult> CancelJobAsync(string jobId, CancellationToken ct) =>
            SendAsync(HttpMethod.Delete, $"/jobs/{Uri.EscapeDataString(jobId)}", ct);

        private async Task<TranscodeResult> SendAsync(
            HttpMethod method, string pathAndQuery, CancellationToken ct, string? jsonBody = null)
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(60);

            using var request = new HttpRequestMessage(method, _baseUrl + pathAndQuery);
            if (jsonBody != null)
                request.Content = new StringContent(
                    jsonBody, System.Text.Encoding.UTF8, "application/json");

            try
            {
                using var response = await client.SendAsync(request, ct);
                var body = await response.Content.ReadAsStringAsync(ct);
                if (!response.IsSuccessStatusCode)
                    _logger.LogWarning(
                        "Transcode API {Method} {Path} failed: {Status} {Body}",
                        method, pathAndQuery, (int)response.StatusCode, body);

                return new TranscodeResult(response.StatusCode, body);
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
            {
                _logger.LogError(ex, "Transcode API {Method} {Path} unreachable.", method, pathAndQuery);
                return new TranscodeResult(
                    HttpStatusCode.BadGateway,
                    JsonSerializer.Serialize(new { error = $"Transcode API unreachable: {ex.Message}" }));
            }
        }
    }
}
