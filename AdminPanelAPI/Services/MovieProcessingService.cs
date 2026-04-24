using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using AdminPanelAPI.Models;
using Npgsql;
using NpgsqlTypes;
using System.Globalization;
using System.Net.Http.Headers;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    public class MovieProcessingService : IMovieProcessingService
    {
        private readonly IConfiguration _configuration;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<MovieProcessingService> _logger;
        private readonly IMovieProcessingJobRepository _repo;

        private readonly int _maxParallelSceneJobs;

        private readonly string _connectionString;

        private readonly string _videoInfoBaseUrl;
        private readonly string _generateClipsBaseUrl;
        private readonly string _sceneDetectionBaseUrl;

        private readonly string _r2AccountId;
        private readonly string _r2AccessKey;
        private readonly string _r2SecretKey;
        private readonly string _r2BucketName;

        public MovieProcessingService(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            ILogger<MovieProcessingService> logger,
            IMovieProcessingJobRepository repo)
        {
            _configuration = configuration;
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _repo = repo;

            _connectionString = _configuration.GetConnectionString("Default")
                ?? throw new InvalidOperationException("Missing connection string: Default");

            _videoInfoBaseUrl = _configuration["MoviePipeline:VideoInfoBaseUrl"]
                                ?? "http://35.89.51.60:8888";

            _generateClipsBaseUrl = _configuration["MoviePipeline:GenerateClipsBaseUrl"]
                                    ?? "https://semanticsearch--generate-clips.modal.run";

            _sceneDetectionBaseUrl = _configuration["MoviePipeline:SceneDetectionBaseUrl"]
                                     ?? "https://semanticsearch--r2-transnet-v2.modal.run";

            _r2AccountId = _configuration["R2:AccountId"] ?? "";
            _r2AccessKey = _configuration["R2:AccessKey"] ?? "";
            _r2SecretKey = _configuration["R2:SecretKey"] ?? "";
            _r2BucketName = _configuration["R2:BucketName"] ?? "";

            _maxParallelSceneJobs =
    int.TryParse(_configuration["MoviePipeline:MaxParallelSceneJobs"], out var parsed)
        ? Math.Clamp(parsed, 1, 20)
        : 5;
        }

        public async Task ProcessMovieAsync(
            long jobId,
            int movieId,
            double threshold,
            bool overwrite,
            CancellationToken cancellationToken)
        {
            _logger.LogInformation(
                "Starting movie processing. JobId={JobId}, MovieId={MovieId}",
                jobId,
                movieId);

            await _repo.UpdateProgressAsync(jobId, "Getting movie metadata", null, null, cancellationToken);
            var videoInfo = await GetVideoInfoAsync(movieId, cancellationToken);

            await _repo.UpdateProgressAsync(jobId, "Saving movie metadata", null, null, cancellationToken);
            await InsertMovieInfoAsync(movieId, videoInfo, cancellationToken);

            if (overwrite)
            {
                await _repo.UpdateProgressAsync(jobId, "Deleting existing R2 clip files", null, null, cancellationToken);
                await DeleteAllFilesFromR2PrefixAsync(movieId, cancellationToken);
            }

            await _repo.UpdateProgressAsync(jobId, "Generating 9-second clips", null, null, cancellationToken);
            await GenerateClipsAsync(movieId, cancellationToken);

            await _repo.UpdateProgressAsync(jobId, "Cleaning .txt files from R2", null, null, cancellationToken);
            await DeleteTxtFilesFromR2Async(movieId, cancellationToken);

            if (overwrite)
            {
                await _repo.UpdateProgressAsync(jobId, "Deleting existing scene boundary rows", null, null, cancellationToken);
                await DeleteSceneBoundariesForMovieAsync(movieId, cancellationToken);
            }

            await _repo.UpdateProgressAsync(jobId, "Listing R2 clip files", null, null, cancellationToken);
            var clipFiles = await ListClipFilesFromR2Async(movieId, cancellationToken);

            if (clipFiles.Count == 0)
            {
                await _repo.UpdateProgressAsync(
                    jobId,
                    "No clips found in R2",
                    0,
                    0,
                    cancellationToken);

                throw new Exception($"No clips found in R2 for movie {movieId}.");
            }

            var total = clipFiles.Count;

           
            var processedCount = 0;
            var failedCount = 0;

            await Parallel.ForEachAsync(
                clipFiles,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = _maxParallelSceneJobs,
                    CancellationToken = cancellationToken
                },
                async (clipFilename, ct) =>
                {
                    var currentCount = Interlocked.Increment(ref processedCount);

                    await _repo.UpdateProgressAsync(
                        jobId,
                        $"Scene detection: {clipFilename}",
                        currentCount,
                        total,
                        ct);

                    try
                    {
                        var boundary = await DetectWithRetry(movieId, clipFilename, threshold, ct);

                        await InsertSceneBoundaryAsync(boundary, ct);
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref failedCount);

                        _logger.LogError(
                            ex,
                            "Scene boundary failed. JobId={JobId}, MovieId={MovieId}, Clip={ClipFilename}",
                            jobId,
                            movieId,
                            clipFilename);
                    }
                });

            await _repo.UpdateProgressAsync(
                jobId,
                failedCount == 0
                    ? "Completed"
                    : $"Completed with {failedCount} failed clips",
                total,
                total,
                cancellationToken);

            _logger.LogInformation(
                "Finished movie processing. JobId={JobId}, MovieId={MovieId}",
                jobId,
                movieId);
        }

        private async Task<VideoInfoResponse> GetVideoInfoAsync(
            int movieId,
            CancellationToken cancellationToken)
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromMinutes(5);

            var url = $"{_videoInfoBaseUrl.TrimEnd('/')}/video-info?movie_id={movieId}";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            using var response = await client.SendAsync(request, cancellationToken);
            var content = await response.Content.ReadAsStringAsync(cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                throw new Exception($"Video info API failed. Status: {(int)response.StatusCode}, Body: {content}");
            }

            var result = JsonSerializer.Deserialize<VideoInfoResponse>(content, JsonOptions());

            if (result == null)
            {
                throw new Exception("Video info API returned empty or invalid JSON.");
            }

            return result;
        }

        private async Task InsertMovieInfoAsync(
            int movieId,
            VideoInfoResponse info,
            CancellationToken cancellationToken)
        {
            const string sql = @"
INSERT INTO frl.frl_movie_info_v2 (
    movieid,
    duration,
    fps,
    frame_count,
    width,
    height,
    aspect_ratio,
    aspect_ratio_str,
    dar,
    sar,
    codec,
    profile,
    pix_fmt,
    bit_depth,
    file_size_bytes,
    file_size_mb
)
VALUES (
    @movieid,
    @duration,
    @fps,
    @frame_count,
    @width,
    @height,
    @aspect_ratio,
    @aspect_ratio_str,
    @dar,
    @sar,
    @codec,
    @profile,
    @pix_fmt,
    @bit_depth,
    @file_size_bytes,
    @file_size_mb
)
ON CONFLICT (movieid) DO NOTHING;";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = new NpgsqlCommand(sql, conn);

            cmd.Parameters.AddWithValue("movieid", movieId);
            cmd.Parameters.AddWithValue("duration", (object?)info.Duration ?? DBNull.Value);
            cmd.Parameters.AddWithValue("fps", (object?)info.Fps ?? DBNull.Value);
            cmd.Parameters.AddWithValue("frame_count", (object?)info.FrameCount ?? DBNull.Value);
            cmd.Parameters.AddWithValue("width", (object?)info.Width ?? DBNull.Value);
            cmd.Parameters.AddWithValue("height", (object?)info.Height ?? DBNull.Value);
            cmd.Parameters.AddWithValue("aspect_ratio", (object?)info.AspectRatio ?? DBNull.Value);
            cmd.Parameters.AddWithValue("aspect_ratio_str", (object?)info.AspectRatioStr ?? DBNull.Value);
            cmd.Parameters.AddWithValue("dar", (object?)info.Dar ?? DBNull.Value);
            cmd.Parameters.AddWithValue("sar", (object?)info.Sar ?? DBNull.Value);
            cmd.Parameters.AddWithValue("codec", (object?)info.Codec ?? DBNull.Value);
            cmd.Parameters.AddWithValue("profile", (object?)info.Profile ?? DBNull.Value);
            cmd.Parameters.AddWithValue("pix_fmt", (object?)info.PixFmt ?? DBNull.Value);
            cmd.Parameters.AddWithValue("bit_depth", (object?)info.BitDepth ?? DBNull.Value);
            cmd.Parameters.AddWithValue("file_size_bytes", (object?)info.FileSizeBytes ?? DBNull.Value);
            cmd.Parameters.AddWithValue("file_size_mb", (object?)info.FileSizeMb ?? DBNull.Value);

            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        private async Task<GenerateClipsResponse> GenerateClipsAsync(
            int movieId,
            CancellationToken cancellationToken)
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromMinutes(30);

            var url = $"{_generateClipsBaseUrl.TrimEnd('/')}/?movie_id={movieId}";

            using var request = new HttpRequestMessage(HttpMethod.Post, url);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            request.Content = new StringContent(string.Empty);

            using var response = await client.SendAsync(request, cancellationToken);
            var content = await response.Content.ReadAsStringAsync(cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                throw new Exception($"Generate clips API failed. Status: {(int)response.StatusCode}, Body: {content}");
            }

            var result = JsonSerializer.Deserialize<GenerateClipsResponse>(content, JsonOptions());

            if (result == null)
            {
                throw new Exception("Generate clips API returned empty or invalid JSON.");
            }

            return result;
        }

        private async Task<SceneBoundaryResponse> DetectWithRetry(
    int movieId,
    string filename,
    double threshold,
    CancellationToken ct)
        {
            var attempts = 0;

            while (true)
            {
                try
                {
                    return await DetectSceneBoundaryAsync(movieId, filename, threshold, ct);
                }
                catch
                {
                    attempts++;

                    if (attempts >= 5)
                        throw;

                    await Task.Delay(1000 * attempts, ct); // backoff
                }
            }
        }

        private async Task<SceneBoundaryResponse> DetectSceneBoundaryAsync(
            int movieId,
            string filename,
            double threshold,
            CancellationToken cancellationToken)
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromMinutes(15);

            var url = $"{_sceneDetectionBaseUrl.TrimEnd('/')}/";

            var form = new Dictionary<string, string>
            {
                ["movie_id"] = movieId.ToString(CultureInfo.InvariantCulture),
                ["filename"] = filename,
                ["threshold"] = threshold.ToString(CultureInfo.InvariantCulture)
            };

            using var request = new HttpRequestMessage(HttpMethod.Post, url);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            request.Content = new FormUrlEncodedContent(form);

            using var response = await client.SendAsync(request, cancellationToken);
            var content = await response.Content.ReadAsStringAsync(cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                throw new Exception($"Scene detection API failed for {filename}. Status: {(int)response.StatusCode}, Body: {content}");
            }

            var result = JsonSerializer.Deserialize<SceneBoundaryResponse>(content, JsonOptions());

            if (result == null)
            {
                throw new Exception($"Scene detection API returned empty or invalid JSON for {filename}.");
            }

            return result;
        }

        private async Task InsertSceneBoundaryAsync(
            SceneBoundaryResponse boundary,
            CancellationToken cancellationToken)
        {
            const string sql = @"
INSERT INTO frl.frl_image_scene_boundaries (
    movieid,
    filename,
    start_time,
    end_time,
    cut_times,
    duration,
    fps,
    frame_count,
    target_frame,
    threshold
)
VALUES (
    @movieid,
    @filename,
    @start_time,
    @end_time,
    @cut_times,
    @duration,
    @fps,
    @frame_count,
    @target_frame,
    @threshold
)
ON CONFLICT DO NOTHING;";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = new NpgsqlCommand(sql, conn);

            cmd.Parameters.AddWithValue("movieid", boundary.MovieId);
            cmd.Parameters.AddWithValue("filename", (object?)boundary.Filename ?? DBNull.Value);
            cmd.Parameters.AddWithValue("start_time", (object?)boundary.StartTime ?? DBNull.Value);
            cmd.Parameters.AddWithValue("end_time", (object?)boundary.EndTime ?? DBNull.Value);

            var cutTimesJson = boundary.CutTimes != null
                ? JsonSerializer.Serialize(boundary.CutTimes)
                : "[]";

            cmd.Parameters.AddWithValue("cut_times", NpgsqlDbType.Jsonb, cutTimesJson);

            cmd.Parameters.AddWithValue("duration", (object?)boundary.Duration ?? DBNull.Value);
            cmd.Parameters.AddWithValue("fps", (object?)boundary.Fps ?? DBNull.Value);
            cmd.Parameters.AddWithValue("frame_count", (object?)boundary.FrameCount ?? DBNull.Value);
            cmd.Parameters.AddWithValue("target_frame", (object?)boundary.TargetFrame ?? DBNull.Value);
            cmd.Parameters.AddWithValue("threshold", (object?)boundary.Threshold ?? DBNull.Value);

            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        private async Task<int> DeleteSceneBoundariesForMovieAsync(
            int movieId,
            CancellationToken cancellationToken)
        {
            const string sql = @"
DELETE FROM frl.frl_image_scene_boundaries
WHERE movieid = @movieid;";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("movieid", movieId);

            return await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        private AmazonS3Client CreateR2Client()
        {
            if (string.IsNullOrWhiteSpace(_r2AccountId) ||
                string.IsNullOrWhiteSpace(_r2AccessKey) ||
                string.IsNullOrWhiteSpace(_r2SecretKey) ||
                string.IsNullOrWhiteSpace(_r2BucketName))
            {
                throw new InvalidOperationException(
                    "R2 settings are missing. Check R2:AccountId, AccessKey, SecretKey, BucketName.");
            }

            var creds = new BasicAWSCredentials(
                _r2AccessKey.Trim(),
                _r2SecretKey.Trim());

            var config = new AmazonS3Config
            {
                ServiceURL = $"https://{_r2AccountId}.r2.cloudflarestorage.com",
                ForcePathStyle = true,
                UseAccelerateEndpoint = false,
                UseDualstackEndpoint = false,
                EndpointDiscoveryEnabled = false
            };

            return new AmazonS3Client(creds, config);
        }

        private async Task<List<string>> ListClipFilesFromR2Async(
    int movieId,
    CancellationToken cancellationToken)
        {
            using var client = CreateR2Client();

            var prefix = $"clips_9s/{movieId}/";
            var results = new List<string>();
            string? continuationToken = null;

            do
            {
                var request = new ListObjectsV2Request
                {
                    BucketName = _r2BucketName,
                    Prefix = prefix,
                    ContinuationToken = continuationToken
                };

                var response = await client.ListObjectsV2Async(request, cancellationToken);

                var objects = response.S3Objects ?? new List<S3Object>();

                foreach (var obj in objects)
                {
                    if (string.IsNullOrWhiteSpace(obj.Key))
                        continue;

                    if (!obj.Key.EndsWith(".mp4", StringComparison.OrdinalIgnoreCase))
                        continue;

                    var fileNameWithExtension = Path.GetFileName(obj.Key);
                    var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileNameWithExtension);

                    if (!string.IsNullOrWhiteSpace(fileNameWithoutExtension))
                    {
                        results.Add(fileNameWithoutExtension);
                    }
                }

                continuationToken = response.IsTruncated == true
                    ? response.NextContinuationToken
                    : null;

            } while (!string.IsNullOrEmpty(continuationToken));

            return results
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        private async Task<(int Found, int Deleted)> DeleteTxtFilesFromR2Async(
            int movieId,
            CancellationToken cancellationToken,
            bool dryRun = false)
        {
            using var client = CreateR2Client();

            var prefix = $"clips_9s/{movieId}/";
            int found = 0;
            int deleted = 0;
            string? continuationToken = null;

            do
            {
                var listResponse = await client.ListObjectsV2Async(
                    new ListObjectsV2Request
                    {
                        BucketName = _r2BucketName,
                        Prefix = prefix,
                        ContinuationToken = continuationToken
                    },
                    cancellationToken);

                continuationToken = listResponse.IsTruncated == true
                    ? listResponse.NextContinuationToken
                    : null;

                if (listResponse.S3Objects == null || listResponse.S3Objects.Count == 0)
                {
                    continue;
                }

                var keysToDelete = new List<KeyVersion>();

                foreach (var obj in listResponse.S3Objects)
                {
                    if (!string.IsNullOrWhiteSpace(obj.Key) &&
                        obj.Key.EndsWith(".txt", StringComparison.OrdinalIgnoreCase))
                    {
                        found++;
                        keysToDelete.Add(new KeyVersion
                        {
                            Key = obj.Key
                        });
                    }
                }

                if (keysToDelete.Count == 0)
                {
                    continue;
                }

                if (dryRun)
                {
                    _logger.LogInformation(
                        "DRY RUN: Would delete {Count} .txt files under prefix {Prefix}",
                        keysToDelete.Count,
                        prefix);

                    continue;
                }

                const int batchSize = 1000;

                for (int i = 0; i < keysToDelete.Count; i += batchSize)
                {
                    var batch = keysToDelete
                        .Skip(i)
                        .Take(batchSize)
                        .ToList();

                    await client.DeleteObjectsAsync(
                        new DeleteObjectsRequest
                        {
                            BucketName = _r2BucketName,
                            Objects = batch,
                            Quiet = true
                        },
                        cancellationToken);

                    deleted += batch.Count;
                }
            }
            while (!string.IsNullOrEmpty(continuationToken));

            return (found, deleted);
        }

        private async Task<(int Found, int Deleted)> DeleteAllFilesFromR2PrefixAsync(
            int movieId,
            CancellationToken cancellationToken,
            bool dryRun = false)
        {
            using var client = CreateR2Client();

            var prefix = $"clips_9s/{movieId}/";
            int found = 0;
            int deleted = 0;
            string? continuationToken = null;

            do
            {
                var listResponse = await client.ListObjectsV2Async(
                    new ListObjectsV2Request
                    {
                        BucketName = _r2BucketName,
                        Prefix = prefix,
                        ContinuationToken = continuationToken
                    },
                    cancellationToken);

                continuationToken = listResponse.IsTruncated == true
                    ? listResponse.NextContinuationToken
                    : null;

                if (listResponse.S3Objects == null || listResponse.S3Objects.Count == 0)
                {
                    continue;
                }

                var keysToDelete = new List<KeyVersion>();

                foreach (var obj in listResponse.S3Objects)
                {
                    if (!string.IsNullOrWhiteSpace(obj.Key))
                    {
                        found++;
                        keysToDelete.Add(new KeyVersion
                        {
                            Key = obj.Key
                        });
                    }
                }

                if (keysToDelete.Count == 0)
                {
                    continue;
                }

                if (dryRun)
                {
                    _logger.LogInformation(
                        "DRY RUN: Would delete {Count} files under prefix {Prefix}",
                        keysToDelete.Count,
                        prefix);

                    continue;
                }

                const int batchSize = 1000;

                for (int i = 0; i < keysToDelete.Count; i += batchSize)
                {
                    var batch = keysToDelete
                        .Skip(i)
                        .Take(batchSize)
                        .ToList();

                    await client.DeleteObjectsAsync(
                        new DeleteObjectsRequest
                        {
                            BucketName = _r2BucketName,
                            Objects = batch,
                            Quiet = true
                        },
                        cancellationToken);

                    deleted += batch.Count;
                }
            }
            while (!string.IsNullOrEmpty(continuationToken));

            return (found, deleted);
        }

        private static JsonSerializerOptions JsonOptions()
        {
            return new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };
        }
    }
}