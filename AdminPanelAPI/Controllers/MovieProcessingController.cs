using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Npgsql;
using System.Data.Common;
using System.Globalization;
using System.Net.Http.Headers;
using System.Text.Json;

namespace AdminPanelAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class MovieProcessingController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<MovieProcessingController> _logger;
        private readonly NpgsqlConnection _connection;

      
        private readonly string _videoInfoBaseUrl;
        private readonly string _generateClipsBaseUrl;
        private readonly string _sceneDetectionBaseUrl;

        private readonly string _r2AccountId;
        private readonly string _r2AccessKey;
        private readonly string _r2SecretKey;
        private readonly string _r2BucketName;

        public MovieProcessingController(
            IConfiguration configuration,
            NpgsqlConnection connection,
            IHttpClientFactory httpClientFactory,
            ILogger<MovieProcessingController> logger)
        {
            _configuration = configuration;
            _httpClientFactory = httpClientFactory;
            _logger = logger;

            _connection = connection;

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
        }

        #region Public Endpoints

        [HttpPost("process/{movieId:int}")]
        public async Task<IActionResult> ProcessMovie(
     int movieId,
     [FromQuery] double threshold = 0.7,
     [FromQuery] bool overwrite = false,
     CancellationToken cancellationToken = default)
        {
            var response = new MovieProcessingPipelineResponse
            {
                MovieId = movieId,
                Threshold = threshold,
                Overwrite = overwrite
            };

            try
            {
                // Step 1 - metadata
                var videoInfo = await GetVideoInfoAsync(movieId, cancellationToken);
                response.VideoInfo = videoInfo;

                await InsertMovieInfoAsync(movieId, videoInfo, cancellationToken);
                response.MetadataStored = true;

                // Step 2 - optionally wipe existing clip files first
                if (overwrite)
                {
                    var overwriteCleanup = await DeleteAllFilesFromR2PrefixAsync(movieId, cancellationToken);
                    response.OverwriteFilesFound = overwriteCleanup.Found;
                    response.OverwriteFilesDeleted = overwriteCleanup.Deleted;
                }

                // Step 3 - generate clips
                var clipsResponse = await GenerateClipsAsync(movieId, cancellationToken);
                response.GenerateClipsResponse = clipsResponse;
                response.ClipsGenerated = true;

                // Step 4 - remove stray .txt files from R2 prefix
                var cleanupResult = await DeleteTxtFilesFromR2Async(movieId, cancellationToken);
                response.TxtFilesFound = cleanupResult.Found;
                response.TxtFilesDeleted = cleanupResult.Deleted;

                // Step 5 - if overwriting, clear existing DB scene boundaries too
                if (overwrite)
                {
                    response.DeletedExistingSceneBoundaryRows =
                        await DeleteSceneBoundariesForMovieAsync(movieId, cancellationToken);
                }

                // Step 6 - list clip files from R2
                var clipFiles = await ListClipFilesFromR2Async(movieId, cancellationToken);
                response.TotalClipFilesFound = clipFiles.Count;

                foreach (var clipFilename in clipFiles)
                {
                    try
                    {
                        var boundary = await DetectSceneBoundaryAsync(movieId, clipFilename, threshold, cancellationToken);
                        await InsertSceneBoundaryAsync(boundary, cancellationToken);

                        response.SceneBoundaryResults.Add(new SceneBoundaryProcessItem
                        {
                            Filename = clipFilename,
                            Success = true,
                            StartTime = boundary.StartTime,
                            EndTime = boundary.EndTime,
                            Duration = boundary.Duration,
                            FrameCount = boundary.FrameCount
                        });
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(
                            ex,
                            "Scene boundary processing failed for movie {MovieId}, clip {ClipFilename}",
                            movieId,
                            clipFilename);

                        response.SceneBoundaryResults.Add(new SceneBoundaryProcessItem
                        {
                            Filename = clipFilename,
                            Success = false,
                            Error = ex.Message
                        });
                    }
                }

                response.SceneBoundariesProcessed = response.SceneBoundaryResults.Count(x => x.Success);
                response.SceneBoundariesFailed = response.SceneBoundaryResults.Count(x => !x.Success);
                response.Completed = true;

                return Ok(response);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Pipeline failed for movie {MovieId}", movieId);

                response.Completed = false;
                response.Error = ex.Message;

                return StatusCode(500, response);
            }
        }

        [HttpPost("cleanup-txt/{movieId:int}")]
        public async Task<IActionResult> CleanupTxtFiles(int movieId, CancellationToken cancellationToken = default)
        {
            try
            {
                var result = await DeleteTxtFilesFromR2Async(movieId, cancellationToken);

                return Ok(new
                {
                    movieId,
                    success = true,
                    txtFilesFound = result.Found,
                    txtFilesDeleted = result.Deleted
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed cleaning txt files for movie {MovieId}", movieId);

                return StatusCode(500, new
                {
                    movieId,
                    success = false,
                    error = ex.Message
                });
            }
        }

        [HttpPost("metadata/{movieId:int}")]
        public async Task<IActionResult> ProcessMetadata(int movieId, CancellationToken cancellationToken = default)
        {
            try
            {
                var videoInfo = await GetVideoInfoAsync(movieId, cancellationToken);
                await InsertMovieInfoAsync(movieId, videoInfo, cancellationToken);

                return Ok(new
                {
                    movieId,
                    success = true,
                    message = "Movie metadata stored successfully.",
                    videoInfo
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Metadata processing failed for movie {MovieId}", movieId);
                return StatusCode(500, new
                {
                    movieId,
                    success = false,
                    error = ex.Message
                });
            }
        }

        [HttpPost("clips/{movieId:int}")]
        public async Task<IActionResult> ProcessClips(
      int movieId,
      [FromQuery] bool overwrite = false,
      CancellationToken cancellationToken = default)
        {
            try
            {
                int found = 0;
                int deleted = 0;

                if (overwrite)
                {
                    var cleanup = await DeleteAllFilesFromR2PrefixAsync(movieId, cancellationToken);
                    found = cleanup.Found;
                    deleted = cleanup.Deleted;
                }

                var result = await GenerateClipsAsync(movieId, cancellationToken);

                return Ok(new
                {
                    movieId,
                    overwrite,
                    overwriteFilesFound = found,
                    overwriteFilesDeleted = deleted,
                    success = true,
                    result
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Clip generation failed for movie {MovieId}", movieId);

                return StatusCode(500, new
                {
                    movieId,
                    overwrite,
                    success = false,
                    error = ex.Message
                });
            }
        }

        [HttpPost("scene-boundaries/{movieId:int}")]
        public async Task<IActionResult> ProcessSceneBoundaries(
    int movieId,
    [FromQuery] double threshold = 0.7,
    [FromQuery] bool overwrite = false,
    CancellationToken cancellationToken = default)
        {
            try
            {
                int deletedExistingRows = 0;

                if (overwrite)
                {
                    deletedExistingRows = await DeleteSceneBoundariesForMovieAsync(movieId, cancellationToken);
                }

                var clipFiles = await ListClipFilesFromR2Async(movieId, cancellationToken);

                var results = new List<SceneBoundaryProcessItem>();

                foreach (var clipFilename in clipFiles)
                {
                    try
                    {
                        var boundary = await DetectSceneBoundaryAsync(movieId, clipFilename, threshold, cancellationToken);
                        await InsertSceneBoundaryAsync(boundary, cancellationToken);

                        results.Add(new SceneBoundaryProcessItem
                        {
                            Filename = clipFilename,
                            Success = true,
                            StartTime = boundary.StartTime,
                            EndTime = boundary.EndTime,
                            Duration = boundary.Duration,
                            FrameCount = boundary.FrameCount
                        });
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(
                            ex,
                            "Scene boundary processing failed for movie {MovieId}, clip {ClipFilename}",
                            movieId,
                            clipFilename);

                        results.Add(new SceneBoundaryProcessItem
                        {
                            Filename = clipFilename,
                            Success = false,
                            Error = ex.Message
                        });
                    }
                }

                return Ok(new
                {
                    movieId,
                    threshold,
                    overwrite,
                    deletedExistingRows,
                    totalClipFilesFound = clipFiles.Count,
                    processed = results.Count(x => x.Success),
                    failed = results.Count(x => !x.Success),
                    results
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Scene boundary batch failed for movie {MovieId}", movieId);

                return StatusCode(500, new
                {
                    movieId,
                    overwrite,
                    success = false,
                    error = ex.Message
                });
            }
        }

        [HttpGet("clips/{movieId:int}")]
        public async Task<IActionResult> GetClipFiles(int movieId, CancellationToken cancellationToken = default)
        {
            try
            {
                var files = await ListClipFilesFromR2Async(movieId, cancellationToken);
                return Ok(new
                {
                    movieId,
                    total = files.Count,
                    files
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed listing clips for movie {MovieId}", movieId);
                return StatusCode(500, new
                {
                    movieId,
                    error = ex.Message
                });
            }
        }

        #endregion

        #region Step 1 - Video Info

        private async Task<int> DeleteSceneBoundariesForMovieAsync(
     int movieId,
     CancellationToken cancellationToken)
        {
            const string sql = @"
DELETE FROM frl.frl_image_scene_boundaries
WHERE movieid = @movieid;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("movieid", movieId);

            return await cmd.ExecuteNonQueryAsync(cancellationToken);
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
                    continue;

                var keysToDelete = new List<KeyVersion>();

                foreach (var obj in listResponse.S3Objects)
                {
                    if (!string.IsNullOrWhiteSpace(obj.Key))
                    {
                        found++;
                        keysToDelete.Add(new KeyVersion { Key = obj.Key });
                    }
                }

                if (keysToDelete.Count == 0)
                    continue;

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

        private async Task<VideoInfoResponse> GetVideoInfoAsync(int movieId, CancellationToken cancellationToken)
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
                throw new Exception("Video info API returned empty or invalid JSON.");

            return result;
        }

        private async Task InsertMovieInfoAsync(int movieId, VideoInfoResponse info, CancellationToken cancellationToken)
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

            
            //await _connection.OpenAsync(cancellationToken);

            await using var cmd = new NpgsqlCommand(sql, _connection);
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

        #endregion

        #region Step 2 - Generate Clips

        private async Task<GenerateClipsResponse> GenerateClipsAsync(int movieId, CancellationToken cancellationToken)
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
                throw new Exception("Generate clips API returned empty or invalid JSON.");

            return result;
        }

        #endregion

        #region Step 3 - Detect Scene Boundaries

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
                throw new Exception($"Scene detection API returned empty or invalid JSON for {filename}.");

            return result;
        }

        private async Task InsertSceneBoundaryAsync(SceneBoundaryResponse boundary, CancellationToken cancellationToken)
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
    @cut_times::jsonb,
    @duration,
    @fps,
    @frame_count,
    @target_frame,
    @threshold
)
ON CONFLICT DO NOTHING;";

            await using var cmd = new NpgsqlCommand(sql, _connection);

            cmd.Parameters.AddWithValue("movieid", boundary.MovieId);
            cmd.Parameters.AddWithValue("filename", (object?)boundary.Filename ?? DBNull.Value);
            cmd.Parameters.AddWithValue("start_time", (object?)boundary.StartTime ?? DBNull.Value);
            cmd.Parameters.AddWithValue("end_time", (object?)boundary.EndTime ?? DBNull.Value);

            // cut_times is jsonb in Postgres, so send JSON text
            var cutTimesJson = boundary.CutTimes != null
                ? JsonSerializer.Serialize(boundary.CutTimes)
                : "[]";

            cmd.Parameters.AddWithValue("cut_times", cutTimesJson);

            cmd.Parameters.AddWithValue("duration", (object?)boundary.Duration ?? DBNull.Value);
            cmd.Parameters.AddWithValue("fps", (object?)boundary.Fps ?? DBNull.Value);
            cmd.Parameters.AddWithValue("frame_count", (object?)boundary.FrameCount ?? DBNull.Value);
            cmd.Parameters.AddWithValue("target_frame", (object?)boundary.TargetFrame ?? DBNull.Value);
            cmd.Parameters.AddWithValue("threshold", (object?)boundary.Threshold ?? DBNull.Value);

            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
        #endregion

        #region R2

        private AmazonS3Client CreateR2Client()
        {
            if (string.IsNullOrWhiteSpace(_r2AccountId) ||
                string.IsNullOrWhiteSpace(_r2AccessKey) ||
                string.IsNullOrWhiteSpace(_r2SecretKey) ||
                string.IsNullOrWhiteSpace(_r2BucketName))
            {
                throw new InvalidOperationException("R2 settings are missing. Check R2:AccountId, AccessKey, SecretKey, BucketName.");
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

        private async Task<List<string>> ListClipFilesFromR2Async(int movieId, CancellationToken cancellationToken)
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

                foreach (var obj in response.S3Objects)
                {
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
                    continue;

                var keysToDelete = new List<KeyVersion>();

                foreach (var obj in listResponse.S3Objects)
                {
                    if (!string.IsNullOrWhiteSpace(obj.Key) &&
                        obj.Key.EndsWith(".txt", StringComparison.OrdinalIgnoreCase))
                    {
                        found++;
                        keysToDelete.Add(new KeyVersion { Key = obj.Key });
                    }
                }

                if (keysToDelete.Count == 0)
                    continue;

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

        #endregion

        #region Helpers

        private static JsonSerializerOptions JsonOptions()
        {
            return new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
            };
        }

        #endregion
    }

    #region DTOs

    public class VideoInfoResponse
    {
        public double? Duration { get; set; }
        public double? Fps { get; set; }
        public int? FrameCount { get; set; }
        public int? Width { get; set; }
        public int? Height { get; set; }
        public double? AspectRatio { get; set; }
        public string? AspectRatioStr { get; set; }
        public string? Dar { get; set; }
        public string? Sar { get; set; }
        public string? Codec { get; set; }
        public string? Profile { get; set; }
        public string? PixFmt { get; set; }
        public int? BitDepth { get; set; }
        public long? FileSizeBytes { get; set; }
        public double? FileSizeMb { get; set; }
    }

    public class GenerateClipsResponse
    {
        public int MovieId { get; set; }
        public string? Message { get; set; }
        public int? TotalFrames { get; set; }
        public int? ProcessedFrames { get; set; }
        public bool? SkippedDownload { get; set; }
    }

    public class SceneBoundaryResponse
    {
        public int MovieId { get; set; }
        public string? Filename { get; set; }
        public double? Threshold { get; set; }
        public double? StartTime { get; set; }
        public double? EndTime { get; set; }
        public List<double>? CutTimes { get; set; }
        public double? Duration { get; set; }
        public double? Fps { get; set; }
        public int? FrameCount { get; set; }
        public int? TargetFrame { get; set; }
    }

    public class SceneBoundaryProcessItem
    {
        public string? Filename { get; set; }
        public bool Success { get; set; }
        public string? Error { get; set; }
        public double? StartTime { get; set; }
        public double? EndTime { get; set; }
        public double? Duration { get; set; }
        public int? FrameCount { get; set; }
    }

    public class MovieProcessingPipelineResponse
    {
        public int MovieId { get; set; }
        public double Threshold { get; set; }
        public bool Overwrite { get; set; }
        public int OverwriteFilesFound { get; set; }
        public int OverwriteFilesDeleted { get; set; }

        public bool MetadataStored { get; set; }
        public bool ClipsGenerated { get; set; }
        public bool Completed { get; set; }
        public string? Error { get; set; }

        public int TxtFilesFound { get; set; }
        public int TxtFilesDeleted { get; set; }

        public VideoInfoResponse? VideoInfo { get; set; }
        public GenerateClipsResponse? GenerateClipsResponse { get; set; }

        public int TotalClipFilesFound { get; set; }
        public int SceneBoundariesProcessed { get; set; }
        public int SceneBoundariesFailed { get; set; }

        public List<SceneBoundaryProcessItem> SceneBoundaryResults { get; set; } = new();
        public int DeletedExistingSceneBoundaryRows { get; set; }
    }

    #endregion
}