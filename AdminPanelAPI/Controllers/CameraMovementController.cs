using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Collections.Concurrent;
using System.Data;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ShotDeckSearch.Controllers
{
    [ApiController]
    [Route("api/admin/camera-movements")]
    public sealed class CameraMovementController : ControllerBase
    {
        private const int PresignedUrlExpiryMinutes = 60;

        private readonly NpgsqlConnection _connection;
        private readonly IConfiguration _configuration;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<CameraMovementController> _logger;

        private static readonly JsonSerializerOptions JsonOpts = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };

        public CameraMovementController(
            NpgsqlConnection connection,
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            ILogger<CameraMovementController> logger)
        {
            _connection = connection;
            _configuration = configuration;
            _httpClientFactory = httpClientFactory;
            _logger = logger;
        }

        // ── GET /api/admin/camera-movements/queue ──────────────────────
        // Returns top N images by weighted_score that have NOT been analyzed yet.
        [HttpGet("queue")]
        [ProducesResponseType(typeof(QueueResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<QueueResponse>> GetQueue(
            [FromQuery] int limit = 1000,
            CancellationToken ct = default)
        {
            if (limit < 1) limit = 1;
            if (limit > 5000) limit = 5000;

            await EnsureOpenAsync(ct);

            const string sql = @"
SELECT i.idnum,
       i.movieid,
       i.randid,
       i.weighted_score,
       sb.start_time,
       sb.end_time,
       sb.fps
FROM frl.frl_images i
INNER JOIN frl.frl_image_scene_boundaries sb
    ON sb.movieid = i.movieid AND sb.filename = i.randid
WHERE i.status = 'live'
  AND NOT EXISTS (
      SELECT 1 FROM frl.frl_join_image_camera_movements cm
      WHERE cm.imageid = i.idnum
  )
ORDER BY i.weighted_score DESC
LIMIT @limit;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@limit", limit);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var items = new List<QueueItem>();

            while (await reader.ReadAsync(ct))
            {
                items.Add(new QueueItem
                {
                    ImageId = reader.GetInt32(reader.GetOrdinal("idnum")),
                    MovieId = reader.GetInt32(reader.GetOrdinal("movieid")),
                    RandId = reader.GetString(reader.GetOrdinal("randid")),
                    WeightedScore = reader.IsDBNull(reader.GetOrdinal("weighted_score"))
                        ? null : reader.GetDouble(reader.GetOrdinal("weighted_score")),
                    StartTime = reader.IsDBNull(reader.GetOrdinal("start_time"))
                        ? null : reader.GetDouble(reader.GetOrdinal("start_time")),
                    EndTime = reader.IsDBNull(reader.GetOrdinal("end_time"))
                        ? null : reader.GetDouble(reader.GetOrdinal("end_time")),
                    Fps = reader.IsDBNull(reader.GetOrdinal("fps"))
                        ? null : reader.GetDouble(reader.GetOrdinal("fps")),
                });
            }

            return Ok(new QueueResponse { Images = items, Count = items.Count });
        }

        // ── POST /api/admin/camera-movements/analyze ───────────────────
        // Batch-analyze images: calls VideoMAE API for each, stores results.
        [HttpPost("analyze")]
        [ProducesResponseType(typeof(AnalyzeBatchResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<AnalyzeBatchResponse>> AnalyzeBatch(
            [FromQuery] int limit = 100,
            CancellationToken ct = default)
        {
            if (limit < 1) limit = 1;
            if (limit > 500) limit = 500;

            var cameraMotionApiUrl = _configuration["CameraMotion:ApiUrl"]
                ?? "https://colin-bracey--camera-motion-api-fastapi-app.modal.run";

            await EnsureOpenAsync(ct);

            // 1. Get images that need analysis
            const string queueSql = @"
SELECT i.idnum,
       i.movieid,
       i.randid,
       sb.start_time,
       sb.end_time
FROM frl.frl_images i
INNER JOIN frl.frl_image_scene_boundaries sb
    ON sb.movieid = i.movieid AND sb.filename = i.randid
WHERE i.status = 'live'
  AND NOT EXISTS (
      SELECT 1 FROM frl.frl_join_image_camera_movements cm
      WHERE cm.imageid = i.idnum
  )
ORDER BY i.weighted_score DESC
LIMIT @limit;";

            await using var queueCmd = new NpgsqlCommand(queueSql, _connection);
            queueCmd.Parameters.AddWithValue("@limit", limit);
            await using var queueReader = await queueCmd.ExecuteReaderAsync(ct);

            var images = new List<AnalyzeItem>();
            while (await queueReader.ReadAsync(ct))
            {
                images.Add(new AnalyzeItem
                {
                    ImageId = queueReader.GetInt32(queueReader.GetOrdinal("idnum")),
                    MovieId = queueReader.GetInt32(queueReader.GetOrdinal("movieid")),
                    RandId = queueReader.GetString(queueReader.GetOrdinal("randid")),
                    StartTime = queueReader.IsDBNull(queueReader.GetOrdinal("start_time"))
                        ? null : queueReader.GetDouble(queueReader.GetOrdinal("start_time")),
                    EndTime = queueReader.IsDBNull(queueReader.GetOrdinal("end_time"))
                        ? null : queueReader.GetDouble(queueReader.GetOrdinal("end_time")),
                });
            }
            await queueReader.CloseAsync();

            if (images.Count == 0)
                return Ok(new AnalyzeBatchResponse { Processed = 0, Failed = 0, Message = "No images in queue." });

            // 2. Generate presigned R2 URLs
            var accountId = _configuration["R2:AccountId"] ?? "";
            var accessKey = _configuration["R2:AccessKey"] ?? "";
            var secretKey = _configuration["R2:SecretKey"] ?? "";
            var bucketName = _configuration["R2:BucketName"] ?? "";

            if (string.IsNullOrWhiteSpace(accountId) || string.IsNullOrWhiteSpace(accessKey) ||
                string.IsNullOrWhiteSpace(secretKey) || string.IsNullOrWhiteSpace(bucketName))
            {
                return StatusCode(500, new { error = "R2 settings are missing." });
            }

            var creds = new BasicAWSCredentials(accessKey.Trim(), secretKey.Trim());
            var s3Config = new AmazonS3Config
            {
                ServiceURL = $"https://{accountId.Trim()}.r2.cloudflarestorage.com",
                ForcePathStyle = true,
                UseAccelerateEndpoint = false,
                UseDualstackEndpoint = false,
                EndpointDiscoveryEnabled = false
            };

            using var s3Client = new AmazonS3Client(creds, s3Config);
            var httpClient = _httpClientFactory.CreateClient();
            httpClient.Timeout = TimeSpan.FromMinutes(2);

            int processed = 0, failed = 0;

            // 3. Process images in parallel (up to 5 concurrent VideoMAE calls)
            const int maxConcurrency = 5;
            var throttle = new SemaphoreSlim(maxConcurrency);
            var results = new ConcurrentBag<(int ImageId, List<VideoMaeMovement>? Movements, bool Success)>();

            var tasks = images.Select(async img =>
            {
                await throttle.WaitAsync(ct);
                try
                {
                    var key = $"clips_9s/{img.MovieId}/{img.RandId}.mp4";
                    var clipUrl = s3Client.GetPreSignedURL(new GetPreSignedUrlRequest
                    {
                        BucketName = bucketName,
                        Key = key,
                        Expires = DateTime.UtcNow.AddMinutes(PresignedUrlExpiryMinutes),
                        Verb = HttpVerb.GET
                    });

                    var payload = new
                    {
                        url = clipUrl,
                        start_time = img.StartTime,
                        end_time = img.EndTime,
                        include_camerabench = false,
                    };

                    var jsonContent = new StringContent(
                        JsonSerializer.Serialize(payload, JsonOpts),
                        Encoding.UTF8,
                        "application/json");

                    var response = await httpClient.PostAsync(
                        $"{cameraMotionApiUrl.TrimEnd('/')}/analyze",
                        jsonContent,
                        ct);

                    if (!response.IsSuccessStatusCode)
                    {
                        _logger.LogWarning(
                            "VideoMAE API failed for image {ImageId}: HTTP {Status}",
                            img.ImageId, (int)response.StatusCode);
                        results.Add((img.ImageId, null, false));
                        return;
                    }

                    var responseBody = await response.Content.ReadAsStringAsync(ct);
                    var result = JsonSerializer.Deserialize<VideoMaeResponse>(responseBody, JsonOpts);
                    results.Add((img.ImageId, result?.OverallMovements, true));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to analyze image {ImageId}", img.ImageId);
                    results.Add((img.ImageId, null, false));
                }
                finally
                {
                    throttle.Release();
                }
            });

            await Task.WhenAll(tasks);

            // 4. Write results to DB sequentially (NpgsqlConnection is not thread-safe)
            foreach (var r in results)
            {
                if (!r.Success)
                {
                    failed++;
                    continue;
                }

                if (r.Movements == null || r.Movements.Count == 0)
                {
                    await InsertMovementAsync(r.ImageId, "static", 0, ct);
                    processed++;
                    continue;
                }

                foreach (var movement in r.Movements)
                {
                    if (movement.Label == "too_short") continue;
                    await InsertMovementAsync(r.ImageId, movement.Label, movement.Confidence, ct);
                }

                processed++;
            }

            return Ok(new AnalyzeBatchResponse
            {
                Processed = processed,
                Failed = failed,
                Total = images.Count,
                Message = $"Analyzed {processed} images, {failed} failed."
            });
        }

        // ── POST /api/admin/camera-movements/analyze-movie ─────────────
        // Batch-analyze images for a specific movie: calls VideoMAE API for each, stores results.
        [HttpPost("analyze-movie")]
        [ProducesResponseType(typeof(AnalyzeBatchResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<AnalyzeBatchResponse>> AnalyzeMovie(
            [FromQuery] int movieId,
            [FromQuery] int limit = 100,
            CancellationToken ct = default)
        {
            if (movieId <= 0) return BadRequest(new { error = "movieId is required." });
            if (limit < 1) limit = 1;
            if (limit > 500) limit = 500;

            var cameraMotionApiUrl = _configuration["CameraMotion:ApiUrl"]
                ?? "https://colin-bracey--camera-motion-api-fastapi-app.modal.run";

            await EnsureOpenAsync(ct);

            // 1. Get images for this movie that need analysis
            const string queueSql = @"
SELECT i.idnum,
       i.movieid,
       i.randid,
       sb.start_time,
       sb.end_time
FROM frl.frl_images i
INNER JOIN frl.frl_image_scene_boundaries sb
    ON sb.movieid = i.movieid AND sb.filename = i.randid
WHERE i.status = 'live'
  AND i.movieid = @movieId
  AND NOT EXISTS (
      SELECT 1 FROM frl.frl_join_image_camera_movements cm
      WHERE cm.imageid = i.idnum
  )
ORDER BY i.idnum
LIMIT @limit;";

            await using var queueCmd = new NpgsqlCommand(queueSql, _connection);
            queueCmd.Parameters.AddWithValue("@movieId", movieId);
            queueCmd.Parameters.AddWithValue("@limit", limit);
            await using var queueReader = await queueCmd.ExecuteReaderAsync(ct);

            var images = new List<AnalyzeItem>();
            while (await queueReader.ReadAsync(ct))
            {
                images.Add(new AnalyzeItem
                {
                    ImageId = queueReader.GetInt32(queueReader.GetOrdinal("idnum")),
                    MovieId = queueReader.GetInt32(queueReader.GetOrdinal("movieid")),
                    RandId = queueReader.GetString(queueReader.GetOrdinal("randid")),
                    StartTime = queueReader.IsDBNull(queueReader.GetOrdinal("start_time"))
                        ? null : queueReader.GetDouble(queueReader.GetOrdinal("start_time")),
                    EndTime = queueReader.IsDBNull(queueReader.GetOrdinal("end_time"))
                        ? null : queueReader.GetDouble(queueReader.GetOrdinal("end_time")),
                });
            }
            await queueReader.CloseAsync();

            if (images.Count == 0)
                return Ok(new AnalyzeBatchResponse { Processed = 0, Failed = 0, Message = "No un-analyzed clips for this movie." });

            // 2. Generate presigned R2 URLs
            var accountId = _configuration["R2:AccountId"] ?? "";
            var accessKey = _configuration["R2:AccessKey"] ?? "";
            var secretKey = _configuration["R2:SecretKey"] ?? "";
            var bucketName = _configuration["R2:BucketName"] ?? "";

            if (string.IsNullOrWhiteSpace(accountId) || string.IsNullOrWhiteSpace(accessKey) ||
                string.IsNullOrWhiteSpace(secretKey) || string.IsNullOrWhiteSpace(bucketName))
            {
                return StatusCode(500, new { error = "R2 settings are missing." });
            }

            var creds = new BasicAWSCredentials(accessKey.Trim(), secretKey.Trim());
            var s3Config = new AmazonS3Config
            {
                ServiceURL = $"https://{accountId.Trim()}.r2.cloudflarestorage.com",
                ForcePathStyle = true,
                UseAccelerateEndpoint = false,
                UseDualstackEndpoint = false,
                EndpointDiscoveryEnabled = false
            };

            using var s3Client = new AmazonS3Client(creds, s3Config);
            var httpClient = _httpClientFactory.CreateClient();
            httpClient.Timeout = TimeSpan.FromMinutes(2);

            int processed = 0, failed = 0;

            // 3. Process images in parallel (up to 5 concurrent VideoMAE calls)
            const int maxConcurrency = 5;
            var throttle = new SemaphoreSlim(maxConcurrency);
            var results = new ConcurrentBag<(int ImageId, List<VideoMaeMovement>? Movements, bool Success)>();

            var tasks = images.Select(async img =>
            {
                await throttle.WaitAsync(ct);
                try
                {
                    var key = $"clips_9s/{img.MovieId}/{img.RandId}.mp4";
                    var clipUrl = s3Client.GetPreSignedURL(new GetPreSignedUrlRequest
                    {
                        BucketName = bucketName,
                        Key = key,
                        Expires = DateTime.UtcNow.AddMinutes(PresignedUrlExpiryMinutes),
                        Verb = HttpVerb.GET
                    });

                    var payload = new
                    {
                        url = clipUrl,
                        start_time = img.StartTime,
                        end_time = img.EndTime,
                        include_camerabench = false,
                    };

                    var jsonContent = new StringContent(
                        JsonSerializer.Serialize(payload, JsonOpts),
                        Encoding.UTF8,
                        "application/json");

                    var response = await httpClient.PostAsync(
                        $"{cameraMotionApiUrl.TrimEnd('/')}/analyze",
                        jsonContent,
                        ct);

                    if (!response.IsSuccessStatusCode)
                    {
                        _logger.LogWarning(
                            "VideoMAE API failed for image {ImageId}: HTTP {Status}",
                            img.ImageId, (int)response.StatusCode);
                        results.Add((img.ImageId, null, false));
                        return;
                    }

                    var responseBody = await response.Content.ReadAsStringAsync(ct);
                    var result = JsonSerializer.Deserialize<VideoMaeResponse>(responseBody, JsonOpts);
                    results.Add((img.ImageId, result?.OverallMovements, true));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to analyze image {ImageId}", img.ImageId);
                    results.Add((img.ImageId, null, false));
                }
                finally
                {
                    throttle.Release();
                }
            });

            await Task.WhenAll(tasks);

            // 4. Write results to DB sequentially
            foreach (var r in results)
            {
                if (!r.Success)
                {
                    failed++;
                    continue;
                }

                if (r.Movements == null || r.Movements.Count == 0)
                {
                    await InsertMovementAsync(r.ImageId, "static", 0, ct);
                    processed++;
                    continue;
                }

                foreach (var movement in r.Movements)
                {
                    if (movement.Label == "too_short") continue;
                    await InsertMovementAsync(r.ImageId, movement.Label, movement.Confidence, ct);
                }

                processed++;
            }

            return Ok(new AnalyzeBatchResponse
            {
                Processed = processed,
                Failed = failed,
                Total = images.Count,
                Message = $"Analyzed {processed} clips for movie {movieId}, {failed} failed."
            });
        }

        // ── GET /api/admin/camera-movements/tags ───────────────────────
        // Returns all distinct tags with counts by status.
        [HttpGet("tags")]
        [ProducesResponseType(typeof(TagSummaryResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<TagSummaryResponse>> GetTags(CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            const string sql = @"
SELECT movement,
       COUNT(*) AS total,
       COUNT(*) FILTER (WHERE status = 'ok') AS confirmed,
       COUNT(*) FILTER (WHERE status = 'bad') AS rejected,
       COUNT(*) FILTER (WHERE status = 'not_checked') AS remaining,
       COUNT(*) FILTER (WHERE status = 'flagged') AS flagged
FROM frl.frl_join_image_camera_movements
GROUP BY movement
ORDER BY total DESC;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            var tags = new List<TagSummary>();
            while (await reader.ReadAsync(ct))
            {
                tags.Add(new TagSummary
                {
                    Movement = reader.GetString(reader.GetOrdinal("movement")),
                    Total = reader.GetInt32(reader.GetOrdinal("total")),
                    Confirmed = reader.GetInt32(reader.GetOrdinal("confirmed")),
                    Rejected = reader.GetInt32(reader.GetOrdinal("rejected")),
                    Remaining = reader.GetInt32(reader.GetOrdinal("remaining")),
                    Flagged = reader.GetInt32(reader.GetOrdinal("flagged")),
                });
            }

            return Ok(new TagSummaryResponse { Tags = tags });
        }

        // ── GET /api/admin/camera-movements/tags/{movement}/clips ──────
        // Returns clips tagged with a specific movement, with R2 URLs.
        [HttpGet("tags/{movement}/clips")]
        [ProducesResponseType(typeof(TagClipsResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<TagClipsResponse>> GetClipsByTag(
            string movement,
            [FromQuery] string? status = null,
            [FromQuery] int page = 1,
            [FromQuery] int pageSize = 50,
            CancellationToken ct = default)
        {
            if (page < 1) page = 1;
            if (pageSize < 1) pageSize = 20;
            if (pageSize > 200) pageSize = 200;

            await EnsureOpenAsync(ct);

            var whereClauses = new List<string> { "cm.movement = @movement" };
            if (!string.IsNullOrWhiteSpace(status))
                whereClauses.Add("cm.status = @status");

            var whereStr = string.Join(" AND ", whereClauses);
            var offset = (page - 1) * pageSize;

            var countSql = $@"
SELECT COUNT(*)
FROM frl.frl_join_image_camera_movements cm
WHERE {whereStr};";

            await using var countCmd = new NpgsqlCommand(countSql, _connection);
            countCmd.Parameters.AddWithValue("@movement", movement);
            if (!string.IsNullOrWhiteSpace(status))
                countCmd.Parameters.AddWithValue("@status", status);

            var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct));

            var dataSql = $@"
SELECT cm.imageid,
       cm.movement,
       cm.confidence,
       cm.status,
       i.movieid,
       i.randid,
       sb.start_time,
       sb.end_time,
       sb.fps,
       sb.target_frame,
       m.title AS movie_title
FROM frl.frl_join_image_camera_movements cm
INNER JOIN frl.frl_images i ON i.idnum = cm.imageid
INNER JOIN frl.frl_image_scene_boundaries sb
    ON sb.movieid = i.movieid AND sb.filename = i.randid
LEFT JOIN frl.frl_movies m ON m.idnum = i.movieid
WHERE {whereStr}
ORDER BY cm.confidence DESC
LIMIT @limit OFFSET @offset;";

            await using var cmd = new NpgsqlCommand(dataSql, _connection);
            cmd.Parameters.AddWithValue("@movement", movement);
            if (!string.IsNullOrWhiteSpace(status))
                cmd.Parameters.AddWithValue("@status", status);
            cmd.Parameters.AddWithValue("@limit", pageSize);
            cmd.Parameters.AddWithValue("@offset", offset);

            await using var reader = await cmd.ExecuteReaderAsync(ct);

            var clipRows = new List<TagClipRow>();
            while (await reader.ReadAsync(ct))
            {
                clipRows.Add(new TagClipRow
                {
                    ImageId = reader.GetInt32(reader.GetOrdinal("imageid")),
                    Movement = reader.GetString(reader.GetOrdinal("movement")),
                    Confidence = reader.GetFloat(reader.GetOrdinal("confidence")),
                    Status = reader.GetString(reader.GetOrdinal("status")),
                    MovieId = reader.GetInt32(reader.GetOrdinal("movieid")),
                    RandId = reader.GetString(reader.GetOrdinal("randid")),
                    StartTime = reader.IsDBNull(reader.GetOrdinal("start_time"))
                        ? null : reader.GetDouble(reader.GetOrdinal("start_time")),
                    EndTime = reader.IsDBNull(reader.GetOrdinal("end_time"))
                        ? null : reader.GetDouble(reader.GetOrdinal("end_time")),
                    Fps = reader.IsDBNull(reader.GetOrdinal("fps"))
                        ? null : reader.GetDouble(reader.GetOrdinal("fps")),
                    TargetFrame = reader.IsDBNull(reader.GetOrdinal("target_frame"))
                        ? null : reader.GetInt32(reader.GetOrdinal("target_frame")),
                    MovieTitle = reader.IsDBNull(reader.GetOrdinal("movie_title"))
                        ? "" : reader.GetString(reader.GetOrdinal("movie_title")),
                });
            }
            await reader.CloseAsync();

            // Generate presigned R2 URLs
            var clips = new List<TagClipDto>();
            var accountId = _configuration["R2:AccountId"] ?? "";
            var accessKeyVal = _configuration["R2:AccessKey"] ?? "";
            var secretKeyVal = _configuration["R2:SecretKey"] ?? "";
            var bucketName = _configuration["R2:BucketName"] ?? "";

            if (!string.IsNullOrWhiteSpace(accountId) &&
                !string.IsNullOrWhiteSpace(accessKeyVal) &&
                !string.IsNullOrWhiteSpace(secretKeyVal) &&
                !string.IsNullOrWhiteSpace(bucketName))
            {
                var creds = new BasicAWSCredentials(accessKeyVal.Trim(), secretKeyVal.Trim());
                var s3Config = new AmazonS3Config
                {
                    ServiceURL = $"https://{accountId.Trim()}.r2.cloudflarestorage.com",
                    ForcePathStyle = true,
                    UseAccelerateEndpoint = false,
                    UseDualstackEndpoint = false,
                    EndpointDiscoveryEnabled = false
                };

                using var client = new AmazonS3Client(creds, s3Config);

                // Also fetch ALL movements for each image so the UI knows about multiples
                var imageIds = clipRows.Select(r => r.ImageId).Distinct().ToList();
                var allMovements = new Dictionary<int, List<ImageMovementInfo>>();

                if (imageIds.Count > 0)
                {
                    var idParams = string.Join(",", imageIds.Select((_, idx) => $"@id{idx}"));
                    var movSql = $@"
SELECT imageid, movement, confidence, status
FROM frl.frl_join_image_camera_movements
WHERE imageid IN ({idParams});";

                    await using var movCmd = new NpgsqlCommand(movSql, _connection);
                    for (int idx = 0; idx < imageIds.Count; idx++)
                        movCmd.Parameters.AddWithValue($"@id{idx}", imageIds[idx]);

                    await using var movReader = await movCmd.ExecuteReaderAsync(ct);
                    while (await movReader.ReadAsync(ct))
                    {
                        var imgId = movReader.GetInt32(0);
                        if (!allMovements.ContainsKey(imgId))
                            allMovements[imgId] = new List<ImageMovementInfo>();

                        allMovements[imgId].Add(new ImageMovementInfo
                        {
                            Movement = movReader.GetString(1),
                            Confidence = movReader.GetFloat(2),
                            Status = movReader.GetString(3),
                        });
                    }
                }

                foreach (var row in clipRows)
                {
                    var r2Key = $"clips_9s/{row.MovieId}/{row.RandId}.mp4";
                    var url = client.GetPreSignedURL(new GetPreSignedUrlRequest
                    {
                        BucketName = bucketName,
                        Key = r2Key,
                        Expires = DateTime.UtcNow.AddMinutes(PresignedUrlExpiryMinutes),
                        Verb = HttpVerb.GET
                    });

                    clips.Add(new TagClipDto
                    {
                        ImageId = row.ImageId,
                        MovieId = row.MovieId,
                        MovieTitle = row.MovieTitle,
                        Url = url,
                        StartTime = row.StartTime,
                        EndTime = row.EndTime,
                        Fps = row.Fps,
                        Confidence = row.Confidence,
                        Status = row.Status,
                        AllMovements = allMovements.GetValueOrDefault(row.ImageId)
                            ?? new List<ImageMovementInfo>(),
                    });
                }
            }

            return Ok(new TagClipsResponse
            {
                Movement = movement,
                Clips = clips,
                Page = page,
                PageSize = pageSize,
                TotalCount = totalCount,
                TotalPages = (int)Math.Ceiling(totalCount / (double)pageSize),
            });
        }

        // ── POST /api/admin/camera-movements/clips/filter ────────────────
        // Returns clips matching multi-tag include/exclude criteria.
        [HttpPost("clips/filter")]
        [ProducesResponseType(typeof(TagClipsResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<TagClipsResponse>> FilterClips(
            [FromBody] FilterClipsRequest request,
            CancellationToken ct = default)
        {
            var include = request.Movements ?? new List<string>();
            var exclude = request.ExcludeMovements ?? new List<string>();

            if (include.Count == 0)
                return BadRequest(new { error = "At least one movement to include is required." });

            int page = request.Page < 1 ? 1 : request.Page;
            int pageSize = request.PageSize < 1 ? 20 : request.PageSize > 200 ? 200 : request.PageSize;

            await EnsureOpenAsync(ct);

            // Build parameterised include list
            var includeParams = new List<string>();
            for (int i = 0; i < include.Count; i++)
                includeParams.Add($"@inc{i}");

            // Build parameterised exclude list
            var excludeParams = new List<string>();
            for (int i = 0; i < exclude.Count; i++)
                excludeParams.Add($"@exc{i}");

            // Subquery: images that have ALL included movements
            var imageSubquery = $@"
SELECT imageid
FROM frl.frl_join_image_camera_movements
WHERE movement IN ({string.Join(",", includeParams)})
GROUP BY imageid
HAVING COUNT(DISTINCT movement) = @includeCount";

            // If there are excludes, filter them out
            var excludeClause = exclude.Count > 0
                ? $@" AND imageid NOT IN (
    SELECT DISTINCT imageid FROM frl.frl_join_image_camera_movements
    WHERE movement IN ({string.Join(",", excludeParams)})
)"
                : "";

            // Status filter on the first included movement
            var statusClause = !string.IsNullOrWhiteSpace(request.Status)
                ? " AND cm.status = @status"
                : "";

            var offset = (page - 1) * pageSize;

            // Count query
            var countSql = $@"
SELECT COUNT(*)
FROM frl.frl_join_image_camera_movements cm
WHERE cm.movement = @firstMovement
  AND cm.imageid IN ({imageSubquery}{excludeClause}){statusClause};";

            await using var countCmd = new NpgsqlCommand(countSql, _connection);
            countCmd.Parameters.AddWithValue("@firstMovement", include[0]);
            countCmd.Parameters.AddWithValue("@includeCount", include.Count);
            for (int i = 0; i < include.Count; i++)
                countCmd.Parameters.AddWithValue($"@inc{i}", include[i]);
            for (int i = 0; i < exclude.Count; i++)
                countCmd.Parameters.AddWithValue($"@exc{i}", exclude[i]);
            if (!string.IsNullOrWhiteSpace(request.Status))
                countCmd.Parameters.AddWithValue("@status", request.Status);

            var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct));

            // Data query
            var dataSql = $@"
SELECT cm.imageid,
       cm.movement,
       cm.confidence,
       cm.status,
       i.movieid,
       i.randid,
       sb.start_time,
       sb.end_time,
       sb.fps,
       sb.target_frame,
       m.title AS movie_title
FROM frl.frl_join_image_camera_movements cm
INNER JOIN frl.frl_images i ON i.idnum = cm.imageid
INNER JOIN frl.frl_image_scene_boundaries sb
    ON sb.movieid = i.movieid AND sb.filename = i.randid
LEFT JOIN frl.frl_movies m ON m.idnum = i.movieid
WHERE cm.movement = @firstMovement
  AND cm.imageid IN ({imageSubquery}{excludeClause}){statusClause}
ORDER BY cm.confidence DESC
LIMIT @limit OFFSET @offset;";

            await using var cmd = new NpgsqlCommand(dataSql, _connection);
            cmd.Parameters.AddWithValue("@firstMovement", include[0]);
            cmd.Parameters.AddWithValue("@includeCount", include.Count);
            for (int i = 0; i < include.Count; i++)
                cmd.Parameters.AddWithValue($"@inc{i}", include[i]);
            for (int i = 0; i < exclude.Count; i++)
                cmd.Parameters.AddWithValue($"@exc{i}", exclude[i]);
            if (!string.IsNullOrWhiteSpace(request.Status))
                cmd.Parameters.AddWithValue("@status", request.Status);
            cmd.Parameters.AddWithValue("@limit", pageSize);
            cmd.Parameters.AddWithValue("@offset", offset);

            await using var reader = await cmd.ExecuteReaderAsync(ct);

            var clipRows = new List<TagClipRow>();
            while (await reader.ReadAsync(ct))
            {
                clipRows.Add(new TagClipRow
                {
                    ImageId = reader.GetInt32(reader.GetOrdinal("imageid")),
                    Movement = reader.GetString(reader.GetOrdinal("movement")),
                    Confidence = reader.GetFloat(reader.GetOrdinal("confidence")),
                    Status = reader.GetString(reader.GetOrdinal("status")),
                    MovieId = reader.GetInt32(reader.GetOrdinal("movieid")),
                    RandId = reader.GetString(reader.GetOrdinal("randid")),
                    StartTime = reader.IsDBNull(reader.GetOrdinal("start_time"))
                        ? null : reader.GetDouble(reader.GetOrdinal("start_time")),
                    EndTime = reader.IsDBNull(reader.GetOrdinal("end_time"))
                        ? null : reader.GetDouble(reader.GetOrdinal("end_time")),
                    Fps = reader.IsDBNull(reader.GetOrdinal("fps"))
                        ? null : reader.GetDouble(reader.GetOrdinal("fps")),
                    TargetFrame = reader.IsDBNull(reader.GetOrdinal("target_frame"))
                        ? null : reader.GetInt32(reader.GetOrdinal("target_frame")),
                    MovieTitle = reader.IsDBNull(reader.GetOrdinal("movie_title"))
                        ? "" : reader.GetString(reader.GetOrdinal("movie_title")),
                });
            }
            await reader.CloseAsync();

            // Generate presigned R2 URLs & fetch all movements per image
            var clips = new List<TagClipDto>();
            var accountId = _configuration["R2:AccountId"] ?? "";
            var accessKeyVal = _configuration["R2:AccessKey"] ?? "";
            var secretKeyVal = _configuration["R2:SecretKey"] ?? "";
            var bucketName = _configuration["R2:BucketName"] ?? "";

            if (!string.IsNullOrWhiteSpace(accountId) &&
                !string.IsNullOrWhiteSpace(accessKeyVal) &&
                !string.IsNullOrWhiteSpace(secretKeyVal) &&
                !string.IsNullOrWhiteSpace(bucketName))
            {
                var creds = new BasicAWSCredentials(accessKeyVal.Trim(), secretKeyVal.Trim());
                var s3Config = new AmazonS3Config
                {
                    ServiceURL = $"https://{accountId.Trim()}.r2.cloudflarestorage.com",
                    ForcePathStyle = true,
                    UseAccelerateEndpoint = false,
                    UseDualstackEndpoint = false,
                    EndpointDiscoveryEnabled = false
                };

                using var client = new AmazonS3Client(creds, s3Config);

                var imageIds = clipRows.Select(r => r.ImageId).Distinct().ToList();
                var allMovements = new Dictionary<int, List<ImageMovementInfo>>();

                if (imageIds.Count > 0)
                {
                    var idParams = string.Join(",", imageIds.Select((_, idx) => $"@id{idx}"));
                    var movSql = $@"
SELECT imageid, movement, confidence, status
FROM frl.frl_join_image_camera_movements
WHERE imageid IN ({idParams});";

                    await using var movCmd = new NpgsqlCommand(movSql, _connection);
                    for (int idx = 0; idx < imageIds.Count; idx++)
                        movCmd.Parameters.AddWithValue($"@id{idx}", imageIds[idx]);

                    await using var movReader = await movCmd.ExecuteReaderAsync(ct);
                    while (await movReader.ReadAsync(ct))
                    {
                        var imgId = movReader.GetInt32(0);
                        if (!allMovements.ContainsKey(imgId))
                            allMovements[imgId] = new List<ImageMovementInfo>();

                        allMovements[imgId].Add(new ImageMovementInfo
                        {
                            Movement = movReader.GetString(1),
                            Confidence = movReader.GetFloat(2),
                            Status = movReader.GetString(3),
                        });
                    }
                }

                foreach (var row in clipRows)
                {
                    var r2Key = $"clips_9s/{row.MovieId}/{row.RandId}.mp4";
                    var url = client.GetPreSignedURL(new GetPreSignedUrlRequest
                    {
                        BucketName = bucketName,
                        Key = r2Key,
                        Expires = DateTime.UtcNow.AddMinutes(PresignedUrlExpiryMinutes),
                        Verb = HttpVerb.GET
                    });

                    clips.Add(new TagClipDto
                    {
                        ImageId = row.ImageId,
                        MovieId = row.MovieId,
                        MovieTitle = row.MovieTitle,
                        Url = url,
                        StartTime = row.StartTime,
                        EndTime = row.EndTime,
                        Fps = row.Fps,
                        Confidence = row.Confidence,
                        Status = row.Status,
                        AllMovements = allMovements.GetValueOrDefault(row.ImageId)
                            ?? new List<ImageMovementInfo>(),
                    });
                }
            }

            var label = string.Join(" + ", include.Select(formatMovement));
            if (exclude.Count > 0)
                label += " - " + string.Join(" - ", exclude.Select(formatMovement));

            return Ok(new TagClipsResponse
            {
                Movement = label,
                Clips = clips,
                Page = page,
                PageSize = pageSize,
                TotalCount = totalCount,
                TotalPages = (int)Math.Ceiling(totalCount / (double)pageSize),
            });

            static string formatMovement(string m) =>
                string.IsNullOrEmpty(m) ? m : m.Replace("_", " ");
        }

        // ── PUT /api/admin/camera-movements/review ─────────────────────
        // Update the status of one or more imageid+movement pairs.
        [HttpPut("review")]
        [ProducesResponseType(typeof(ReviewResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<ReviewResponse>> Review(
            [FromBody] ReviewRequest request,
            CancellationToken ct = default)
        {
            if (request.Items == null || request.Items.Count == 0)
                return BadRequest(new { error = "No items provided." });

            var validStatuses = new HashSet<string> { "ok", "bad", "not_checked" };

            await EnsureOpenAsync(ct);

            int updated = 0;
            foreach (var item in request.Items)
            {
                if (!validStatuses.Contains(item.Status))
                    continue;

                const string sql = @"
UPDATE frl.frl_join_image_camera_movements
SET status = @status, updated_at = now()
WHERE imageid = @imageid AND movement = @movement;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@status", item.Status);
                cmd.Parameters.AddWithValue("@imageid", item.ImageId);
                cmd.Parameters.AddWithValue("@movement", item.Movement);

                updated += await cmd.ExecuteNonQueryAsync(ct);
            }

            return Ok(new ReviewResponse { Updated = updated });
        }

        // ── GET /api/admin/camera-movements/image/{imageId} ────────────
        // Returns all movements for a specific image.
        [HttpGet("image/{imageId:int}")]
        [ProducesResponseType(typeof(List<ImageMovementInfo>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<ImageMovementInfo>>> GetImageMovements(
            int imageId,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            const string sql = @"
SELECT movement, confidence, status
FROM frl.frl_join_image_camera_movements
WHERE imageid = @imageid
ORDER BY confidence DESC;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@imageid", imageId);
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            var movements = new List<ImageMovementInfo>();
            while (await reader.ReadAsync(ct))
            {
                movements.Add(new ImageMovementInfo
                {
                    Movement = reader.GetString(0),
                    Confidence = reader.GetFloat(1),
                    Status = reader.GetString(2),
                });
            }

            return Ok(movements);
        }

        // ── GET /api/admin/camera-movements/movements ─────────────────
        // Returns all distinct movement labels (for reassign dropdown).
        [HttpGet("movements")]
        [ProducesResponseType(typeof(MovementListResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<MovementListResponse>> GetMovements(CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            const string sql = @"
SELECT DISTINCT movement
FROM frl.frl_join_image_camera_movements
ORDER BY movement;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            var movements = new List<string>();
            while (await reader.ReadAsync(ct))
            {
                movements.Add(reader.GetString(0));
            }

            return Ok(new MovementListResponse { Movements = movements });
        }

        // ── PUT /api/admin/camera-movements/reassign ─────────────────────
        // Change the movement label for an image (e.g. "static" → "dolly_in").
        [HttpPut("reassign")]
        [ProducesResponseType(typeof(ReassignResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<ReassignResponse>> Reassign(
            [FromBody] ReassignRequest request,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(request.NewMovement))
                return BadRequest(new { error = "newMovement is required." });

            await EnsureOpenAsync(ct);

            // Delete old row + insert new one (movement is part of the PK)
            const string sql = @"
WITH deleted AS (
    DELETE FROM frl.frl_join_image_camera_movements
    WHERE imageid = @imageid AND movement = @oldMovement
    RETURNING imageid, confidence
)
INSERT INTO frl.frl_join_image_camera_movements (imageid, movement, confidence, status)
SELECT imageid, @newMovement, confidence, 'ok'
FROM deleted
ON CONFLICT (imageid, movement) DO UPDATE SET status = 'ok', updated_at = now();";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@imageid", request.ImageId);
            cmd.Parameters.AddWithValue("@oldMovement", request.OldMovement);
            cmd.Parameters.AddWithValue("@newMovement", request.NewMovement);

            var rows = await cmd.ExecuteNonQueryAsync(ct);

            return Ok(new ReassignResponse { Updated = rows > 0 });
        }

        // ── DELETE /api/admin/camera-movements/tag ─────────────────────
        // Removes a specific movement tag from an image.
        [HttpDelete("tag")]
        [ProducesResponseType(typeof(DeleteTagResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<DeleteTagResponse>> DeleteTag(
            [FromBody] DeleteTagRequest request,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(request.Movement))
                return BadRequest(new { error = "movement is required." });

            await EnsureOpenAsync(ct);

            const string sql = @"
DELETE FROM frl.frl_join_image_camera_movements
WHERE imageid = @imageid AND movement = @movement;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@imageid", request.ImageId);
            cmd.Parameters.AddWithValue("@movement", request.Movement);

            var rows = await cmd.ExecuteNonQueryAsync(ct);

            return Ok(new DeleteTagResponse { Deleted = rows > 0 });
        }

        // ── POST /api/admin/camera-movements/tag ──────────────────────
        // Adds a new movement tag to an image.
        [HttpPost("tag")]
        [ProducesResponseType(typeof(AddTagResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<AddTagResponse>> AddTag(
            [FromBody] AddTagRequest request,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(request.Movement))
                return BadRequest(new { error = "movement is required." });

            await EnsureOpenAsync(ct);

            const string sql = @"
INSERT INTO frl.frl_join_image_camera_movements (imageid, movement, confidence, status)
VALUES (@imageid, @movement, 0, 'ok')
ON CONFLICT (imageid, movement) DO NOTHING;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@imageid", request.ImageId);
            cmd.Parameters.AddWithValue("@movement", request.Movement);

            var rows = await cmd.ExecuteNonQueryAsync(ct);

            return Ok(new AddTagResponse { Added = rows > 0 });
        }

        // ── Helpers ────────────────────────────────────────────────────

        private async Task EnsureOpenAsync(CancellationToken ct)
        {
            if (_connection.State != ConnectionState.Open)
                await _connection.OpenAsync(ct);
        }

        private async Task InsertMovementAsync(
            int imageId, string movement, double confidence, CancellationToken ct)
        {
            const string sql = @"
INSERT INTO frl.frl_join_image_camera_movements (imageid, movement, confidence)
VALUES (@imageid, @movement, @confidence)
ON CONFLICT (imageid, movement) DO NOTHING;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@imageid", imageId);
            cmd.Parameters.AddWithValue("@movement", movement);
            cmd.Parameters.AddWithValue("@confidence", (float)confidence);

            await cmd.ExecuteNonQueryAsync(ct);
        }

        // ── DTOs ───────────────────────────────────────────────────────

        public sealed class QueueItem
        {
            public int ImageId { get; set; }
            public int MovieId { get; set; }
            public string RandId { get; set; } = "";
            public double? WeightedScore { get; set; }
            public double? StartTime { get; set; }
            public double? EndTime { get; set; }
            public double? Fps { get; set; }
        }

        public sealed class QueueResponse
        {
            public List<QueueItem> Images { get; set; } = new();
            public int Count { get; set; }
        }

        private sealed class AnalyzeItem
        {
            public int ImageId { get; set; }
            public int MovieId { get; set; }
            public string RandId { get; set; } = "";
            public double? StartTime { get; set; }
            public double? EndTime { get; set; }
        }

        public sealed class AnalyzeBatchResponse
        {
            public int Processed { get; set; }
            public int Failed { get; set; }
            public int Total { get; set; }
            public string Message { get; set; } = "";
        }

        public sealed class TagSummary
        {
            public string Movement { get; set; } = "";
            public int Total { get; set; }
            public int Confirmed { get; set; }
            public int Rejected { get; set; }
            public int Remaining { get; set; }
            public int Flagged { get; set; }
        }

        public sealed class TagSummaryResponse
        {
            public List<TagSummary> Tags { get; set; } = new();
        }

        private sealed class TagClipRow
        {
            public int ImageId { get; set; }
            public string Movement { get; set; } = "";
            public float Confidence { get; set; }
            public string Status { get; set; } = "";
            public int MovieId { get; set; }
            public string RandId { get; set; } = "";
            public double? StartTime { get; set; }
            public double? EndTime { get; set; }
            public double? Fps { get; set; }
            public int? TargetFrame { get; set; }
            public string MovieTitle { get; set; } = "";
        }

        public sealed class ImageMovementInfo
        {
            public string Movement { get; set; } = "";
            public float Confidence { get; set; }
            public string Status { get; set; } = "";
        }

        public sealed class TagClipDto
        {
            public int ImageId { get; set; }
            public int MovieId { get; set; }
            public string MovieTitle { get; set; } = "";
            public string Url { get; set; } = "";
            public double? StartTime { get; set; }
            public double? EndTime { get; set; }
            public double? Fps { get; set; }
            public float Confidence { get; set; }
            public string Status { get; set; } = "";
            public List<ImageMovementInfo> AllMovements { get; set; } = new();
        }

        public sealed class TagClipsResponse
        {
            public string Movement { get; set; } = "";
            public List<TagClipDto> Clips { get; set; } = new();
            public int Page { get; set; }
            public int PageSize { get; set; }
            public int TotalCount { get; set; }
            public int TotalPages { get; set; }
        }

        public sealed class ReviewItem
        {
            public int ImageId { get; set; }
            public string Movement { get; set; } = "";
            public string Status { get; set; } = "";
        }

        public sealed class ReviewRequest
        {
            public List<ReviewItem> Items { get; set; } = new();
        }

        public sealed class ReviewResponse
        {
            public int Updated { get; set; }
        }

        public sealed class MovementListResponse
        {
            public List<string> Movements { get; set; } = new();
        }

        public sealed class ReassignRequest
        {
            public int ImageId { get; set; }
            public string OldMovement { get; set; } = "";
            public string NewMovement { get; set; } = "";
        }

        public sealed class ReassignResponse
        {
            public bool Updated { get; set; }
        }

        public sealed class DeleteTagRequest
        {
            public int ImageId { get; set; }
            public string Movement { get; set; } = "";
        }

        public sealed class DeleteTagResponse
        {
            public bool Deleted { get; set; }
        }

        public sealed class AddTagRequest
        {
            public int ImageId { get; set; }
            public string Movement { get; set; } = "";
        }

        public sealed class AddTagResponse
        {
            public bool Added { get; set; }
        }

        public sealed class FilterClipsRequest
        {
            public List<string> Movements { get; set; } = new();
            public List<string> ExcludeMovements { get; set; } = new();
            public string? Status { get; set; }
            public int Page { get; set; } = 1;
            public int PageSize { get; set; } = 50;
        }

        // VideoMAE API response DTOs
        private sealed class VideoMaeResponse
        {
            [JsonPropertyName("overall_movements")]
            public List<VideoMaeMovement>? OverallMovements { get; set; }
        }

        private sealed class VideoMaeMovement
        {
            [JsonPropertyName("label")]
            public string Label { get; set; } = "";

            [JsonPropertyName("confidence")]
            public double Confidence { get; set; }
        }
    }
}
