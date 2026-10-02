using AdminPanelAPI.Interfaces;
using AdminPanelAPI.Models;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Asks the Modal clip-preview app for 480p silent previews of the scene inside
    /// each 9 second clip. Previews land in R2 beside their source clip, at
    /// clips_9s/{movieId}/{randid}_short.mp4.
    /// </summary>
    public class ClipPreviewService : IClipPreviewService
    {
        private const int BatchSize = 250;
        private const string ClipPrefix = "clips_9s/";
        private const string PreviewSuffix = "_short.mp4";

        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<ClipPreviewService> _logger;
        private readonly IMovieProcessingJobRepository _repo;

        private readonly string _clipPreviewBaseUrl;
        private readonly string _r2AccountId;
        private readonly string _r2AccessKey;
        private readonly string _r2SecretKey;
        private readonly string _r2BucketName;

        public ClipPreviewService(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            ILogger<ClipPreviewService> logger,
            IMovieProcessingJobRepository repo)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _repo = repo;

            _clipPreviewBaseUrl = configuration["MoviePipeline:ClipPreviewBaseUrl"]
                                  ?? "https://semanticsearch--clip-previews-batch.modal.run";

            _r2AccountId = (configuration["R2:AccountId"] ?? "").Trim();
            _r2AccessKey = (configuration["R2:AccessKey"] ?? "").Trim();
            _r2SecretKey = (configuration["R2:SecretKey"] ?? "").Trim();
            _r2BucketName = (configuration["R2:BucketName"] ?? "").Trim();
        }

        public async Task<ClipPreviewResult> GeneratePreviewsForMovieAsync(
            int movieId,
            bool overwrite,
            CancellationToken cancellationToken)
        {
            var boundaries = await _repo.GetSceneBoundariesAsync(movieId, cancellationToken);

            return await GeneratePreviewsAsync(movieId, boundaries, overwrite, cancellationToken);
        }

        public async Task<ClipPreviewResult> GeneratePreviewsAsync(
            int movieId,
            IReadOnlyCollection<ClipPreviewBoundary> boundaries,
            bool overwrite,
            CancellationToken cancellationToken)
        {
            var result = new ClipPreviewResult
            {
                MovieId = movieId,
                Requested = boundaries.Count
            };

            for (var i = 0; i < boundaries.Count; i += BatchSize)
            {
                var batch = boundaries.Skip(i).Take(BatchSize).ToList();

                var summary = await SendBatchAsync(batch, overwrite, cancellationToken);

                result.Created += summary.GetValueOrDefault("created");
                result.Exists += summary.GetValueOrDefault("exists");
                result.Skipped += summary.GetValueOrDefault("skipped");
                result.Errors += summary.GetValueOrDefault("error");
            }

            return result;
        }

        /// <summary>
        /// Temporary helper for the motion-autoplay rollout: previews only the clips
        /// a Motion filter can select, i.e. images carrying a camera-movement tag.
        /// </summary>
        public async Task<ClipPreviewMotionResult> GenerateMotionTaggedPreviewsAsync(
            int limit,
            int afterImageId,
            bool overwrite,
            CancellationToken cancellationToken)
        {
            var boundaries = await _repo.GetMotionTaggedSceneBoundariesAsync(
                Math.Clamp(limit, 1, 20000),
                afterImageId,
                cancellationToken);

            var result = new ClipPreviewMotionResult
            {
                Requested = boundaries.Count
            };

            for (var i = 0; i < boundaries.Count; i += BatchSize)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var batch = boundaries.Skip(i).Take(BatchSize).ToList();

                var summary = await SendBatchAsync(batch, overwrite, cancellationToken);

                result.Created += summary.GetValueOrDefault("created");
                result.Exists += summary.GetValueOrDefault("exists");
                result.Skipped += summary.GetValueOrDefault("skipped");
                result.Errors += summary.GetValueOrDefault("error");
                result.NextAfterImageId = batch[^1].ImageId;
            }

            return result;
        }

        /// <summary>
        /// How far the motion preview run has got: eligible clips from the database
        /// against previews present in R2.
        /// </summary>
        public async Task<ClipPreviewProgress> GetMotionProgressAsync(
            int afterImageId,
            int? movieId,
            bool countR2,
            long maxObjectsToCount,
            CancellationToken cancellationToken)
        {
            var counts = await _repo.GetMotionTaggedCountsAsync(afterImageId, movieId, cancellationToken);

            var progress = new ClipPreviewProgress
            {
                MovieId = movieId,
                AfterImageId = afterImageId,
                MotionTaggedTotal = counts.Total,
                PassedCursor = counts.AtOrBeforeCursor,
                RemainingAfterCursor = counts.Total - counts.AtOrBeforeCursor,
                PercentPassedCursor = counts.Total == 0
                    ? 100
                    : Math.Round(counts.AtOrBeforeCursor * 100.0 / counts.Total, 2),
                LastEligibleImageId = counts.LastImageId
            };

            if (!countR2)
            {
                return progress;
            }

            var stopwatch = Stopwatch.StartNew();

            var (previews, truncated) = await CountPreviewObjectsAsync(
                movieId,
                maxObjectsToCount,
                cancellationToken);

            stopwatch.Stop();

            progress.PreviewsInR2 = previews;
            progress.PreviewCountTruncated = truncated;
            progress.PreviewCountSeconds = Math.Round(stopwatch.Elapsed.TotalSeconds, 1);

            return progress;
        }

        private async Task<(long Count, bool Truncated)> CountPreviewObjectsAsync(
            int? movieId,
            long maxObjects,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(_r2AccountId) ||
                string.IsNullOrWhiteSpace(_r2AccessKey) ||
                string.IsNullOrWhiteSpace(_r2SecretKey) ||
                string.IsNullOrWhiteSpace(_r2BucketName))
            {
                throw new InvalidOperationException(
                    "R2 settings are missing. Check R2:AccountId, AccessKey, SecretKey, BucketName.");
            }

            using var client = new AmazonS3Client(
                new BasicAWSCredentials(_r2AccessKey, _r2SecretKey),
                new AmazonS3Config
                {
                    ServiceURL = $"https://{_r2AccountId}.r2.cloudflarestorage.com",
                    ForcePathStyle = true,
                    UseAccelerateEndpoint = false,
                    UseDualstackEndpoint = false,
                    EndpointDiscoveryEnabled = false
                });

            var prefix = movieId.HasValue
                ? $"{ClipPrefix}{movieId.Value}/"
                : ClipPrefix;

            long count = 0;
            string? continuationToken = null;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                var response = await client.ListObjectsV2Async(
                    new ListObjectsV2Request
                    {
                        BucketName = _r2BucketName,
                        Prefix = prefix,
                        MaxKeys = 1000,
                        ContinuationToken = continuationToken
                    },
                    cancellationToken);

                foreach (var obj in response.S3Objects ?? new List<S3Object>())
                {
                    if (obj.Key?.EndsWith(PreviewSuffix, StringComparison.OrdinalIgnoreCase) == true)
                    {
                        count++;
                    }
                }

                if (count >= maxObjects)
                {
                    return (count, true);
                }

                continuationToken = response.IsTruncated == true
                    ? response.NextContinuationToken
                    : null;

            } while (!string.IsNullOrEmpty(continuationToken));

            return (count, false);
        }

        public async Task<ClipPreviewBackfillResult> BackfillAsync(
            int movieLimit,
            int afterMovieId,
            bool overwrite,
            CancellationToken cancellationToken)
        {
            var movieIds = await _repo.GetMovieIdsWithSceneBoundariesAsync(
                Math.Clamp(movieLimit, 1, 500),
                afterMovieId,
                cancellationToken);

            var result = new ClipPreviewBackfillResult();

            foreach (var movieId in movieIds)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var movieResult = await GeneratePreviewsForMovieAsync(movieId, overwrite, cancellationToken);

                result.Movies.Add(movieResult);
                result.MoviesProcessed++;
                result.Requested += movieResult.Requested;
                result.Created += movieResult.Created;
                result.Exists += movieResult.Exists;
                result.Skipped += movieResult.Skipped;
                result.Errors += movieResult.Errors;
                result.NextAfterMovieId = movieId;
            }

            return result;
        }

        private async Task<Dictionary<string, int>> SendBatchAsync(
            IReadOnlyCollection<ClipPreviewBoundary> boundaries,
            bool overwrite,
            CancellationToken cancellationToken)
        {
            if (boundaries.Count == 0)
            {
                return new Dictionary<string, int>();
            }

            var payload = new
            {
                items = boundaries.Select(b => new
                {
                    movie_id = b.MovieId,
                    filename = b.Filename,
                    start_time = b.StartTime,
                    end_time = b.EndTime
                }),
                overwrite
            };

            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromMinutes(30);

            using var request = new HttpRequestMessage(HttpMethod.Post, _clipPreviewBaseUrl);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            request.Content = new StringContent(
                JsonSerializer.Serialize(payload),
                Encoding.UTF8,
                "application/json");

            using var response = await client.SendAsync(request, cancellationToken);
            var content = await response.Content.ReadAsStringAsync(cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                throw new Exception(
                    $"Clip preview API failed. Status: {(int)response.StatusCode}, Body: {content}");
            }

            var parsed = JsonSerializer.Deserialize<ClipPreviewBatchResponse>(
                content,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            var summary = parsed?.Summary ?? new Dictionary<string, int>();

            if (summary.GetValueOrDefault("error") > 0)
            {
                _logger.LogWarning(
                    "Clip preview batch reported {Errors} failures out of {Total}",
                    summary.GetValueOrDefault("error"),
                    boundaries.Count);
            }

            return summary;
        }
    }
}
