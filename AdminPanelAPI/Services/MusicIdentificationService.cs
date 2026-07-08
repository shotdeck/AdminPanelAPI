using AdminPanelAPI.Models;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    public class MusicIdentificationService : IMusicIdentificationService
    {
        private readonly IConfiguration _configuration;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<MusicIdentificationService> _logger;
        private readonly IMusicIdentificationJobRepository _repo;

        private readonly string _musicApiBaseUrl;

        public MusicIdentificationService(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            ILogger<MusicIdentificationService> logger,
            IMusicIdentificationJobRepository repo)
        {
            _configuration = configuration;
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _repo = repo;

            _musicApiBaseUrl = _configuration["MusicIdentification:MusicApiBaseUrl"]
                ?? "http://localhost:8000";
        }

        public async Task IdentifyMovieAsync(
            long jobId,
            int movieId,
            string? r2Key,
            CancellationToken cancellationToken)
        {
            _logger.LogInformation(
                "Starting music identification. JobId={JobId}, MovieId={MovieId}, R2Key={R2Key}",
                jobId, movieId, r2Key);

            if (string.IsNullOrWhiteSpace(r2Key))
                throw new Exception("r2Key is required for music identification.");

            await _repo.UpdateProgressAsync(jobId, "Sending to music identification API", 5, cancellationToken);

            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromMinutes(60);

            var url = $"{_musicApiBaseUrl.TrimEnd('/')}/identify" +
                      $"?movie_id={movieId}&r2_key={Uri.EscapeDataString(r2Key)}";

            using var response = await client.PostAsync(url, null, cancellationToken);
            var content = await response.Content.ReadAsStringAsync(cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                throw new Exception(
                    $"Music identification API failed. Status: {(int)response.StatusCode}, Body: {content}");
            }

            await _repo.UpdateProgressAsync(jobId, "Parsing results", 70, cancellationToken);

            var result = JsonSerializer.Deserialize<MusicApiResponse>(content);

            if (result == null || result.Error != null)
            {
                throw new Exception(
                    $"Music identification API returned error: {result?.Error ?? "empty response"}");
            }

            await _repo.UpdateProgressAsync(jobId, "Storing segments in database", 85, cancellationToken);

            await _repo.StoreSegmentsAsync(movieId, result, cancellationToken);

            await _repo.MarkCompletedAsync(
                jobId,
                result.MatchedSegments.Count,
                result.UnmatchedWindows.Count,
                cancellationToken);

            _logger.LogInformation(
                "Music identification complete. JobId={JobId}, MovieId={MovieId}, Matched={Matched}, Unmatched={Unmatched}",
                jobId, movieId, result.MatchedSegments.Count, result.UnmatchedWindows.Count);
        }
    }
}
