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
        private readonly IStreamingLinkService _streamingLinkService;
        private readonly ISoundtrackReconciliationService _reconciliationService;
        private readonly ITrackDetailsService _trackDetailsService;

        private readonly string _musicApiBaseUrl;

        public MusicIdentificationService(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            ILogger<MusicIdentificationService> logger,
            IMusicIdentificationJobRepository repo,
            IStreamingLinkService streamingLinkService,
            ISoundtrackReconciliationService reconciliationService,
            ITrackDetailsService trackDetailsService)
        {
            _configuration = configuration;
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _repo = repo;
            _streamingLinkService = streamingLinkService;
            _reconciliationService = reconciliationService;
            _trackDetailsService = trackDetailsService;

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

            var baseUrl = _musicApiBaseUrl.TrimEnd('/');

            // A healthy full-movie scan finishes in ~10-12 min. Modal occasionally
            // stalls at the detect step (an intermittent hang we've observed on
            // larger files); rather than blocking the single-threaded queue on one
            // hung call for the better part of an hour, cap each attempt and
            // re-spawn a fresh Modal call. Re-spawning reliably clears the stall.
            var perAttemptTimeout = TimeSpan.FromMinutes(18);
            const int maxAttempts = 2;

            MusicApiResponse? result = null;
            Exception? lastError = null;

            for (int attempt = 1; attempt <= maxAttempts && result == null; attempt++)
            {
                try
                {
                    result = await RunIdentifyAttemptAsync(
                        jobId, movieId, r2Key, baseUrl, perAttemptTimeout, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    lastError = ex;
                    _logger.LogWarning(
                        ex,
                        "Music job {JobId} attempt {Attempt}/{Max} failed or stalled.",
                        jobId, attempt, maxAttempts);
                    if (attempt < maxAttempts)
                        await _repo.UpdateProgressAsync(
                            jobId, "Modal call stalled — retrying", 10, cancellationToken);
                }
            }

            if (result == null)
                throw lastError ?? new Exception("Music identification failed with no result.");

            await _repo.UpdateProgressAsync(jobId, "Storing segments in database", 85, cancellationToken);

            await _repo.StoreSegmentsAsync(movieId, result, cancellationToken);

            // Enrichment runs as part of the same job so a single upload yields a
            // fully linked + statused movie. Both are best-effort: a failure here
            // must not fail identification (the segments are already stored).
            try
            {
                await _repo.UpdateProgressAsync(jobId, "Finding streaming links", 90, cancellationToken);
                await _streamingLinkService.BackfillAsync(movieId, false, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Streaming-link backfill failed for movie {MovieId}.", movieId);
            }

            try
            {
                await _repo.UpdateProgressAsync(jobId, "Reconciling soundtrack", 93, cancellationToken);
                await _reconciliationService.ReconcileAsync(movieId, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Soundtrack reconciliation failed for movie {MovieId}.", movieId);
            }

            // Pre-warm each track's details (MusicBrainz + Wikipedia + Spotify)
            // and its film-specific AI description so the popup opens instantly
            // instead of fetching on first click. Best-effort and sequential to
            // respect MusicBrainz/OpenAI rate limits.
            try
            {
                await _repo.UpdateProgressAsync(jobId, "Generating track descriptions", 96, cancellationToken);
                var tracks = await _repo.GetMovieTracksAsync(movieId, false, cancellationToken);
                foreach (var track in tracks)
                {
                    if (cancellationToken.IsCancellationRequested) break;
                    try
                    {
                        await _trackDetailsService.GetOrFetchAsync(
                            track.SongId, movieId, false, cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(
                            ex, "Pre-warming details failed for song {SongId} (movie {MovieId}).",
                            track.SongId, movieId);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Track-details pre-warm failed for movie {MovieId}.", movieId);
            }

            await _repo.MarkCompletedAsync(
                jobId,
                result.MatchedSegments.Count,
                result.UnmatchedWindows.Count,
                cancellationToken);

            _logger.LogInformation(
                "Music identification complete. JobId={JobId}, MovieId={MovieId}, Matched={Matched}, Unmatched={Unmatched}",
                jobId, movieId, result.MatchedSegments.Count, result.UnmatchedWindows.Count);
        }

        // Spawns one Modal /identify call and polls /result until it completes.
        // Bounded by `timeout` (a linked CTS): if Modal stalls, this throws so the
        // caller can re-spawn a fresh call instead of blocking the queue.
        private async Task<MusicApiResponse> RunIdentifyAttemptAsync(
            long jobId,
            int movieId,
            string? r2Key,
            string baseUrl,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            using var attemptCts =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            attemptCts.CancelAfter(timeout);
            var ct = attemptCts.Token;

            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromMinutes(10);

            // The Modal app processes a full movie asynchronously: POST /identify
            // returns a call_id immediately, then we poll GET /result/{call_id}.
            var startUrl = $"{baseUrl}/identify" +
                           $"?movie_id={movieId}&r2_key={Uri.EscapeDataString(r2Key!)}";

            using var startResponse = await client.PostAsync(startUrl, null, ct);
            var startContent = await startResponse.Content.ReadAsStringAsync(ct);
            if (!startResponse.IsSuccessStatusCode)
                throw new Exception(
                    $"Music identification API failed to start. Status: {(int)startResponse.StatusCode}, Body: {startContent}");

            var start = JsonSerializer.Deserialize<MusicApiStartResponse>(startContent);
            if (start?.CallId == null)
                throw new Exception($"Music identification API did not return a call_id: {startContent}");

            await _repo.UpdateProgressAsync(jobId, "Detecting music and recognizing songs", 30, cancellationToken);

            var resultUrl = $"{baseUrl}/result/{start.CallId}";

            while (true)
            {
                ct.ThrowIfCancellationRequested();
                await Task.Delay(TimeSpan.FromSeconds(5), ct);

                using var poll = await client.GetAsync(resultUrl, ct);
                var pollContent = await poll.Content.ReadAsStringAsync(ct);
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

                return status;
            }
        }
    }
}
