using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
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

            // 3. Process each image
            foreach (var img in images)
            {
                if (ct.IsCancellationRequested) break;

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

                    // Call VideoMAE API (no CameraBench needed for QC)
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
                        failed++;
                        continue;
                    }

                    var responseBody = await response.Content.ReadAsStringAsync(ct);
                    var result = JsonSerializer.Deserialize<VideoMaeResponse>(responseBody, JsonOpts);

                    if (result?.OverallMovements == null || result.OverallMovements.Count == 0)
                    {
                        // Insert a 'static' record if no movements detected
                        await InsertMovementAsync(img.ImageId, "static", 0, ct);
                        processed++;
                        continue;
                    }

                    // Insert each detected movement
                    foreach (var movement in result.OverallMovements)
                    {
                        if (movement.Label == "too_short") continue;
                        await InsertMovementAsync(
                            img.ImageId,
                            movement.Label,
                            movement.Confidence,
                            ct);
                    }

                    processed++;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to analyze image {ImageId}", img.ImageId);
                    failed++;
                }
            }

            return Ok(new AnalyzeBatchResponse
            {
                Processed = processed,
                Failed = failed,
                Total = images.Count,
                Message = $"Analyzed {processed} images, {failed} failed."
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
       COUNT(*) FILTER (WHERE status = 'not_checked') AS remaining
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
