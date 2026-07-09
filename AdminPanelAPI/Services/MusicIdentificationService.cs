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
            client.Timeout = TimeSpan.FromMinutes(10);

            var baseUrl = _musicApiBaseUrl.TrimEnd('/');

            // The Modal app processes a full movie asynchronously: POST /identify
            // returns a call_id immediately, then we poll GET /result/{call_id}.
            var startUrl = $"{baseUrl}/identify" +
                           $"?movie_id={movieId}&r2_key={Uri.EscapeDataString(r2Key)}";

            using var startResponse = await client.PostAsync(startUrl, null, cancellationToken);
            var startContent = await startResponse.Content.ReadAsStringAsync(cancellationToken);
            if (!startResponse.IsSuccessStatusCode)
            {
                throw new Exception(
                    $"Music identification API failed to start. Status: {(int)startResponse.StatusCode}, Body: {startContent}");
            }

            var start = JsonSerializer.Deserialize<MusicApiStartResponse>(startContent);
            if (start?.CallId == null)
                throw new Exception($"Music identification API did not return a call_id: {startContent}");

            await _repo.UpdateProgressAsync(jobId, "Detecting music and recognizing songs", 30, cancellationToken);

            MusicApiResponse? result = null;
            var resultUrl = $"{baseUrl}/result/{start.CallId}";
            var deadline = DateTime.UtcNow.AddMinutes(55);

            while (DateTime.UtcNow < deadline)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);

                using var poll = await client.GetAsync(resultUrl, cancellationToken);
                var pollContent = await poll.Content.ReadAsStringAsync(cancellationToken);
                if (!poll.IsSuccessStatusCode)
                    throw new Exception(
                        $"Music identification result poll failed. Status: {(int)poll.StatusCode}, Body: {pollContent}");

                var status = JsonSerializer.Deserialize<MusicApiResponse>(pollContent);
                if (status == null)
                    continue;
                if (status.Status == "processing")
                    continue;
                if (status.Status == "error" || status.Error != null)
                    throw new Exception(
                        $"Music identification API returned error: {status.Error ?? "unknown error"}");

                result = status;
                break;
            }

            if (result == null)
                throw new Exception("Music identification timed out waiting for results.");

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
