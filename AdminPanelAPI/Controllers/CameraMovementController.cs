using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Collections.Concurrent;
using System.Data;
using System.Security.Cryptography;
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
            [FromQuery] Guid? jobId = null,
            CancellationToken ct = default)
        {
            if (limit < 1) limit = 1;
            if (limit > 500) limit = 500;

            var cameraMotionApiUrl = _configuration["CameraMotion:ApiUrl"]
                ?? "https://semanticsearch--camera-motion-api-fastapi-app.modal.run";

            await EnsureOpenAsync(ct);
            await EnsureJobTablesAsync(ct);

            // 1. Atomically claim the next N unanalyzed images so simultaneous
            //    fetches (from other sessions) grab disjoint sets. Claims tie to
            //    a job id; a random one is used when no job is supplied.
            var claimJobId = jobId ?? Guid.NewGuid();
            var images = await ClaimImagesAsync(claimJobId, limit, ct);

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

            // Ensure segments table exists
            await EnsureSegmentsTableAsync(ct);

            // 3. Process images in parallel (up to 5 concurrent VideoMAE calls)
            const int maxConcurrency = 5;
            var throttle = new SemaphoreSlim(maxConcurrency);
            var results = new ConcurrentBag<(int ImageId, List<VideoMaeMovement>? Movements, List<VideoMaeSegment>? Segments, bool Success)>();

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
                        results.Add((img.ImageId, null, null, false));
                        return;
                    }

                    var responseBody = await response.Content.ReadAsStringAsync(ct);
                    var result = JsonSerializer.Deserialize<VideoMaeResponse>(responseBody, JsonOpts);
                    results.Add((img.ImageId, result?.OverallMovements, result?.Segments, true));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to analyze image {ImageId}", img.ImageId);
                    results.Add((img.ImageId, null, null, false));
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
                    await InsertMovementAsync(r.ImageId, "hold", 0, ct);
                    await StoreSegmentsAsync(r.ImageId, r.Segments, ct);
                    await MaybeTagNoMovementAsync(r.ImageId, ct);
                    processed++;
                    continue;
                }

                foreach (var movement in r.Movements)
                {
                    if (movement.Label == "too_short") continue;
                    await InsertMovementAsync(r.ImageId, movement.Label, movement.Confidence, ct);
                }

                await StoreSegmentsAsync(r.ImageId, r.Segments, ct);
                await MaybeTagNoMovementAsync(r.ImageId, ct);
                processed++;
            }

            // Release the claims we took (analyzed rows are excluded by the
            // queue anyway; failed ones become eligible for a later retry).
            await ReleaseClaimsAsync(images.Select(i => i.ImageId).ToList(), ct);

            // Record progress on the job so other sessions see it live.
            if (jobId.HasValue)
                await UpdateJobProgressAsync(jobId.Value, processed, failed, ct);

            return Ok(new AnalyzeBatchResponse
            {
                Processed = processed,
                Failed = failed,
                Total = images.Count,
                Message = $"Analyzed {processed} images, {failed} failed."
            });
        }

        // ── POST /api/admin/camera-movements/verify-password ───────────
        // Simple shared-password gate for the QC dashboard. Compares the
        // submitted value against the CAMERAMOVEMENTPASSWORD app setting
        // (set in Azure). Not per-user auth — a single access password.
        [HttpPost("verify-password")]
        [ProducesResponseType(typeof(VerifyPasswordResponse), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status401Unauthorized)]
        public ActionResult<VerifyPasswordResponse> VerifyPassword([FromBody] VerifyPasswordRequest req)
        {
            var expected = _configuration["CAMERAMOVEMENTPASSWORD"];
            if (string.IsNullOrEmpty(expected))
                return StatusCode(StatusCodes.Status503ServiceUnavailable,
                    new VerifyPasswordResponse { Ok = false, Error = "Password not configured." });

            var supplied = req?.Password ?? "";
            var ok = CryptographicOperations.FixedTimeEquals(
                Encoding.UTF8.GetBytes(supplied),
                Encoding.UTF8.GetBytes(expected));

            if (!ok)
                return Unauthorized(new VerifyPasswordResponse { Ok = false, Error = "Incorrect password." });

            return Ok(new VerifyPasswordResponse { Ok = true });
        }

        // ── POST /api/admin/camera-movements/analyze/jobs/start ────────
        // Register a fetch run so every session can see it's in progress.
        [HttpPost("analyze/jobs/start")]
        [ProducesResponseType(typeof(JobStartResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<JobStartResponse>> StartJob(
            [FromBody] JobStartRequest req,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            await EnsureJobTablesAsync(ct);

            var jobId = Guid.NewGuid();
            var startedBy = string.IsNullOrWhiteSpace(req?.StartedBy)
                ? "Anonymous"
                : req!.StartedBy!.Trim();
            if (startedBy.Length > 120) startedBy = startedBy[..120];
            var requested = req?.Requested ?? 0;
            if (requested < 0) requested = 0;

            const string sql = @"
INSERT INTO frl.frl_camera_movement_jobs (job_id, started_by, requested, status)
VALUES (@jobId, @startedBy, @requested, 'running');";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@jobId", jobId);
            cmd.Parameters.AddWithValue("@startedBy", startedBy);
            cmd.Parameters.AddWithValue("@requested", requested);
            await cmd.ExecuteNonQueryAsync(ct);

            return Ok(new JobStartResponse { JobId = jobId });
        }

        // ── POST /api/admin/camera-movements/analyze/jobs/finish ───────
        // Mark a fetch run finished (done/error) and release its claims.
        [HttpPost("analyze/jobs/finish")]
        public async Task<IActionResult> FinishJob(
            [FromBody] JobFinishRequest req,
            CancellationToken ct = default)
        {
            if (req == null || req.JobId == Guid.Empty)
                return BadRequest(new { error = "jobId is required." });

            var status = req.Status == "error" ? "error" : "done";

            await EnsureOpenAsync(ct);
            await EnsureJobTablesAsync(ct);

            const string sql = @"
UPDATE frl.frl_camera_movement_jobs
SET status = @status, updated_at = now()
WHERE job_id = @jobId;
DELETE FROM frl.frl_camera_movement_claims WHERE job_id = @jobId;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@jobId", req.JobId);
            cmd.Parameters.AddWithValue("@status", status);
            await cmd.ExecuteNonQueryAsync(ct);

            return Ok(new { ok = true });
        }

        // ── GET /api/admin/camera-movements/analyze/jobs/active ────────
        // List fetch runs currently in progress (across all sessions).
        [HttpGet("analyze/jobs/active")]
        [ProducesResponseType(typeof(ActiveJobsResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<ActiveJobsResponse>> ActiveJobs(
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            await EnsureJobTablesAsync(ct);

            // Expire runs that stopped reporting progress (crashed/closed tab),
            // then drop their claims so those images can be picked up again.
            const string maintenanceSql = @"
UPDATE frl.frl_camera_movement_jobs
SET status = 'stale'
WHERE status = 'running' AND updated_at < now() - INTERVAL '5 minutes';

DELETE FROM frl.frl_camera_movement_claims c
USING frl.frl_camera_movement_jobs j
WHERE c.job_id = j.job_id AND j.status <> 'running';";

            await using (var maintCmd = new NpgsqlCommand(maintenanceSql, _connection))
                await maintCmd.ExecuteNonQueryAsync(ct);

            const string sql = @"
SELECT job_id, started_by, requested, processed, failed, status, started_at, updated_at
FROM frl.frl_camera_movement_jobs
WHERE status = 'running'
ORDER BY started_at;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            var jobs = new List<ActiveJob>();
            while (await reader.ReadAsync(ct))
            {
                jobs.Add(new ActiveJob
                {
                    JobId = reader.GetGuid(0),
                    StartedBy = reader.IsDBNull(1) ? "Anonymous" : reader.GetString(1),
                    Requested = reader.GetInt32(2),
                    Processed = reader.GetInt32(3),
                    Failed = reader.GetInt32(4),
                    Status = reader.GetString(5),
                    StartedAt = reader.GetDateTime(6),
                    UpdatedAt = reader.GetDateTime(7),
                });
            }

            return Ok(new ActiveJobsResponse { Jobs = jobs });
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
                ?? "https://semanticsearch--camera-motion-api-fastapi-app.modal.run";

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

            // Ensure segments table exists
            await EnsureSegmentsTableAsync(ct);

            // 3. Process images in parallel (up to 5 concurrent VideoMAE calls)
            const int maxConcurrency = 5;
            var throttle = new SemaphoreSlim(maxConcurrency);
            var results = new ConcurrentBag<(int ImageId, List<VideoMaeMovement>? Movements, List<VideoMaeSegment>? Segments, bool Success)>();

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
                        results.Add((img.ImageId, null, null, false));
                        return;
                    }

                    var responseBody = await response.Content.ReadAsStringAsync(ct);
                    var result = JsonSerializer.Deserialize<VideoMaeResponse>(responseBody, JsonOpts);
                    results.Add((img.ImageId, result?.OverallMovements, result?.Segments, true));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to analyze image {ImageId}", img.ImageId);
                    results.Add((img.ImageId, null, null, false));
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
                    await InsertMovementAsync(r.ImageId, "hold", 0, ct);
                    await StoreSegmentsAsync(r.ImageId, r.Segments, ct);
                    await MaybeTagNoMovementAsync(r.ImageId, ct);
                    processed++;
                    continue;
                }

                foreach (var movement in r.Movements)
                {
                    if (movement.Label == "too_short") continue;
                    await InsertMovementAsync(r.ImageId, movement.Label, movement.Confidence, ct);
                }

                await StoreSegmentsAsync(r.ImageId, r.Segments, ct);
                await MaybeTagNoMovementAsync(r.ImageId, ct);
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

                // Fetch stored segments for all images
                var allSegments = await FetchSegmentsAsync(imageIds, ct);

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
                        TargetFrame = row.TargetFrame,
                        Confidence = row.Confidence,
                        Status = row.Status,
                        AllMovements = allMovements.GetValueOrDefault(row.ImageId)
                            ?? new List<ImageMovementInfo>(),
                        Segments = allSegments.GetValueOrDefault(row.ImageId),
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

                // Fetch stored segments for all images
                var allSegments = await FetchSegmentsAsync(imageIds, ct);

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
                        TargetFrame = row.TargetFrame,
                        Confidence = row.Confidence,
                        Status = row.Status,
                        AllMovements = allMovements.GetValueOrDefault(row.ImageId)
                            ?? new List<ImageMovementInfo>(),
                        Segments = allSegments.GetValueOrDefault(row.ImageId),
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

            var validStatuses = new HashSet<string> { "ok", "bad", "not_checked", "flagged" };

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

                var rows = await cmd.ExecuteNonQueryAsync(ct);
                updated += rows;

                if (rows > 0)
                {
                    var action = item.Status switch
                    {
                        "ok" => "confirmed",
                        "bad" => "rejected",
                        "flagged" => "flagged",
                        _ => (string?)null,
                    };
                    if (action != null)
                    {
                        await LogQcActionAsync(
                            item.ImageId, action, item.Movement, null,
                            item.Confidence, ct);
                    }

                    if (item.Status == "ok")
                    {
                        await PromoteToSubMovementsAsync(item.ImageId, item.Movement, ct);
                    }
                }
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

            // Fetch confidence before deleting
            float? confidence = null;
            {
                const string lookupSql = @"
SELECT confidence FROM frl.frl_join_image_camera_movements
WHERE imageid = @imageid AND movement = @movement LIMIT 1;";
                await using var lookupCmd = new NpgsqlCommand(lookupSql, _connection);
                lookupCmd.Parameters.AddWithValue("@imageid", request.ImageId);
                lookupCmd.Parameters.AddWithValue("@movement", request.OldMovement);
                var val = await lookupCmd.ExecuteScalarAsync(ct);
                if (val is float f) confidence = f;
                else if (val is double d) confidence = (float)d;
            }

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

            if (rows > 0)
            {
                await LogQcActionAsync(
                    request.ImageId, "reassigned", request.OldMovement,
                    request.NewMovement, confidence, ct);
            }

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

            // Fetch confidence before deleting
            float? confidence = null;
            {
                const string lookupSql = @"
SELECT confidence FROM frl.frl_join_image_camera_movements
WHERE imageid = @imageid AND movement = @movement LIMIT 1;";
                await using var lookupCmd = new NpgsqlCommand(lookupSql, _connection);
                lookupCmd.Parameters.AddWithValue("@imageid", request.ImageId);
                lookupCmd.Parameters.AddWithValue("@movement", request.Movement);
                var val = await lookupCmd.ExecuteScalarAsync(ct);
                if (val is float f) confidence = f;
                else if (val is double d) confidence = (float)d;
            }

            const string sql = @"
DELETE FROM frl.frl_join_image_camera_movements
WHERE imageid = @imageid AND movement = @movement;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@imageid", request.ImageId);
            cmd.Parameters.AddWithValue("@movement", request.Movement);

            var rows = await cmd.ExecuteNonQueryAsync(ct);

            if (rows > 0)
            {
                await LogQcActionAsync(
                    request.ImageId, "deleted", request.Movement, null,
                    confidence, ct);
            }

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

            if (rows > 0)
            {
                await LogQcActionAsync(
                    request.ImageId, "added", null, request.Movement,
                    null, ct);
            }

            return Ok(new AddTagResponse { Added = rows > 0 });
        }

        // ── GET /api/admin/camera-movements/training-export ──────────
        // Returns all QC audit log entries for model retraining.
        [HttpGet("training-export")]
        [ProducesResponseType(typeof(TrainingExportResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<TrainingExportResponse>> TrainingExport(
            [FromQuery] string? action = null,
            [FromQuery] string? since = null,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            await EnsureAuditLogTableAsync(ct);

            var conditions = new List<string>();
            var parameters = new List<NpgsqlParameter>();

            if (!string.IsNullOrWhiteSpace(action))
            {
                conditions.Add("action = @action");
                parameters.Add(new NpgsqlParameter("@action", action));
            }
            if (!string.IsNullOrWhiteSpace(since) && DateTime.TryParse(since, out var sinceDate))
            {
                conditions.Add("created_at >= @since");
                parameters.Add(new NpgsqlParameter("@since", sinceDate));
            }

            var where = conditions.Count > 0
                ? "WHERE " + string.Join(" AND ", conditions)
                : "";

            var sql = $@"
SELECT id, imageid, action, original_movement, corrected_movement, confidence, created_at
FROM frl.frl_qc_training_log
{where}
ORDER BY created_at DESC;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            foreach (var p in parameters)
                cmd.Parameters.Add(p);

            await using var reader = await cmd.ExecuteReaderAsync(ct);

            var entries = new List<TrainingLogEntry>();
            while (await reader.ReadAsync(ct))
            {
                entries.Add(new TrainingLogEntry
                {
                    Id = reader.GetInt32(0),
                    ImageId = reader.GetInt32(1),
                    Action = reader.GetString(2),
                    OriginalMovement = reader.IsDBNull(3) ? null : reader.GetString(3),
                    CorrectedMovement = reader.IsDBNull(4) ? null : reader.GetString(4),
                    Confidence = reader.IsDBNull(5) ? null : reader.GetFloat(5),
                    CreatedAt = reader.GetDateTime(6).ToString("o"),
                });
            }

            return Ok(new TrainingExportResponse
            {
                Entries = entries,
                TotalCount = entries.Count,
            });
        }

        // ── Helpers ────────────────────────────────────────────────────

        private async Task EnsureOpenAsync(CancellationToken ct)
        {
            if (_connection.State != ConnectionState.Open)
                await _connection.OpenAsync(ct);
        }

        // How long a claim is honoured before it's treated as abandoned (a
        // crashed/closed session), so the image can be picked up again.
        private const int ClaimTtlMinutes = 15;

        private async Task EnsureJobTablesAsync(CancellationToken ct)
        {
            const string sql = @"
CREATE TABLE IF NOT EXISTS frl.frl_camera_movement_jobs (
    job_id      UUID PRIMARY KEY,
    started_by  VARCHAR(120),
    status      VARCHAR(20)  NOT NULL DEFAULT 'running',
    requested   INTEGER      NOT NULL DEFAULT 0,
    processed   INTEGER      NOT NULL DEFAULT 0,
    failed      INTEGER      NOT NULL DEFAULT 0,
    started_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cmj_status ON frl.frl_camera_movement_jobs (status);
CREATE TABLE IF NOT EXISTS frl.frl_camera_movement_claims (
    imageid     INTEGER      PRIMARY KEY,
    job_id      UUID         NOT NULL,
    claimed_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cmc_job_id     ON frl.frl_camera_movement_claims (job_id);
CREATE INDEX IF NOT EXISTS idx_cmc_claimed_at ON frl.frl_camera_movement_claims (claimed_at);";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // Atomically select and claim the next N unanalyzed images. FOR UPDATE
        // SKIP LOCKED plus a claims table means two concurrent fetches never
        // grab the same images.
        private async Task<List<AnalyzeItem>> ClaimImagesAsync(
            Guid jobId, int limit, CancellationToken ct)
        {
            var sql = $@"
WITH candidates AS (
    SELECT i.idnum, i.movieid, i.randid, sb.start_time, sb.end_time
    FROM frl.frl_images i
    INNER JOIN frl.frl_image_scene_boundaries sb
        ON sb.movieid = i.movieid AND sb.filename = i.randid
    WHERE i.status = 'live'
      AND NOT EXISTS (
          SELECT 1 FROM frl.frl_join_image_camera_movements cm
          WHERE cm.imageid = i.idnum)
      AND NOT EXISTS (
          SELECT 1 FROM frl.frl_camera_movement_claims c
          WHERE c.imageid = i.idnum
            AND c.claimed_at > now() - INTERVAL '{ClaimTtlMinutes} minutes')
    ORDER BY i.weighted_score DESC
    LIMIT @limit
    FOR UPDATE OF i SKIP LOCKED
),
claimed AS (
    INSERT INTO frl.frl_camera_movement_claims (imageid, job_id, claimed_at)
    SELECT idnum, @jobId, now() FROM candidates
    ON CONFLICT (imageid) DO UPDATE SET job_id = EXCLUDED.job_id, claimed_at = now()
    RETURNING imageid
)
SELECT idnum, movieid, randid, start_time, end_time
FROM candidates
WHERE idnum IN (SELECT imageid FROM claimed);";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@limit", limit);
            cmd.Parameters.AddWithValue("@jobId", jobId);

            var images = new List<AnalyzeItem>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                images.Add(new AnalyzeItem
                {
                    ImageId = reader.GetInt32(0),
                    MovieId = reader.GetInt32(1),
                    RandId = reader.GetString(2),
                    StartTime = reader.IsDBNull(3) ? null : reader.GetDouble(3),
                    EndTime = reader.IsDBNull(4) ? null : reader.GetDouble(4),
                });
            }
            return images;
        }

        private async Task ReleaseClaimsAsync(List<int> imageIds, CancellationToken ct)
        {
            if (imageIds.Count == 0) return;

            const string sql = @"
DELETE FROM frl.frl_camera_movement_claims WHERE imageid = ANY(@ids);";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@ids", imageIds.ToArray());
            await cmd.ExecuteNonQueryAsync(ct);
        }

        private async Task UpdateJobProgressAsync(
            Guid jobId, int processed, int failed, CancellationToken ct)
        {
            const string sql = @"
UPDATE frl.frl_camera_movement_jobs
SET processed = processed + @processed,
    failed    = failed + @failed,
    updated_at = now()
WHERE job_id = @jobId;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@jobId", jobId);
            cmd.Parameters.AddWithValue("@processed", processed);
            cmd.Parameters.AddWithValue("@failed", failed);
            await cmd.ExecuteNonQueryAsync(ct);
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

        // Auto-tag "no_movement" when an image's only movement is "hold"
        // (has a hold row and no other movement besides no_movement itself).
        // Inserted as 'not_checked' so it surfaces for QC review.
        private async Task MaybeTagNoMovementAsync(int imageId, CancellationToken ct)
        {
            const string sql = @"
INSERT INTO frl.frl_join_image_camera_movements (imageid, movement, confidence, status)
SELECT @imageid, 'no_movement', 0, 'not_checked'
WHERE EXISTS (
    SELECT 1 FROM frl.frl_join_image_camera_movements
    WHERE imageid = @imageid AND movement = 'hold'
)
AND NOT EXISTS (
    SELECT 1 FROM frl.frl_join_image_camera_movements
    WHERE imageid = @imageid AND movement NOT IN ('hold', 'no_movement')
)
ON CONFLICT (imageid, movement) DO NOTHING;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@imageid", imageId);

            await cmd.ExecuteNonQueryAsync(ct);
        }

        // Parent movement -> more specific "sub" variants. When a parent tag is
        // QC-confirmed (status 'ok'), each sub is queued as 'not_checked' so a
        // reviewer can decide whether the clip is actually the niche variant.
        private static readonly Dictionary<string, string[]> SubMovements = new()
        {
            ["zoom_in"]   = new[] { "crash_zoom_in", "dolly_zoom_in" },
            ["zoom_out"]  = new[] { "crash_zoom_out", "dolly_zoom_out" },
            ["dolly_in"]  = new[] { "push_in", "following" },
            ["dolly_out"] = new[] { "pull_out", "leading" },
            ["pan_left"]  = new[] { "whip_pan_left" },
            ["pan_right"] = new[] { "whip_pan_right" },
        };

        // Queue the sub-variants of a confirmed parent movement for QC review.
        // Existing rows (already reviewed either way) are left untouched.
        private async Task PromoteToSubMovementsAsync(
            int imageId, string parentMovement, CancellationToken ct)
        {
            if (!SubMovements.TryGetValue(parentMovement, out var subs))
                return;

            const string sql = @"
INSERT INTO frl.frl_join_image_camera_movements (imageid, movement, confidence, status)
VALUES (@imageid, @movement, 0, 'not_checked')
ON CONFLICT (imageid, movement) DO NOTHING;";

            foreach (var sub in subs)
            {
                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@imageid", imageId);
                cmd.Parameters.AddWithValue("@movement", sub);
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }

        private async Task<Dictionary<int, List<SegmentDto>>> FetchSegmentsAsync(
            List<int> imageIds, CancellationToken ct)
        {
            var result = new Dictionary<int, List<SegmentDto>>();
            if (imageIds.Count == 0) return result;

            var idParams = string.Join(",", imageIds.Select((_, idx) => $"@sid{idx}"));
            var sql = $@"
SELECT imageid, segments_json
FROM frl.frl_image_analysis_segments
WHERE imageid IN ({idParams});";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            for (int idx = 0; idx < imageIds.Count; idx++)
                cmd.Parameters.AddWithValue($"@sid{idx}", imageIds[idx]);

            try
            {
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    var imgId = reader.GetInt32(0);
                    var json = reader.GetString(1);
                    var segments = JsonSerializer.Deserialize<List<VideoMaeSegment>>(json, JsonOpts);
                    if (segments != null)
                    {
                        result[imgId] = segments.Select(s => new SegmentDto
                        {
                            Start = s.Start,
                            End = s.End,
                            Movements = s.Movements?.Select(m => new SegmentMovementDto
                            {
                                Label = m.Label,
                                Confidence = m.Confidence,
                            }).ToList() ?? new List<SegmentMovementDto>(),
                        }).ToList();
                    }
                }
            }
            catch (Npgsql.PostgresException ex) when (ex.SqlState == "42P01")
            {
                // Table doesn't exist yet — no segments stored
            }

            return result;
        }

        private async Task EnsureSegmentsTableAsync(CancellationToken ct)
        {
            const string sql = @"
CREATE TABLE IF NOT EXISTS frl.frl_image_analysis_segments (
    imageid INTEGER PRIMARY KEY,
    segments_json JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        private async Task StoreSegmentsAsync(
            int imageId, List<VideoMaeSegment>? segments, CancellationToken ct)
        {
            if (segments == null || segments.Count == 0) return;

            var json = JsonSerializer.Serialize(segments, JsonOpts);

            const string sql = @"
INSERT INTO frl.frl_image_analysis_segments (imageid, segments_json)
VALUES (@imageid, @segments::jsonb)
ON CONFLICT (imageid) DO UPDATE SET segments_json = @segments::jsonb, created_at = NOW();";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@imageid", imageId);
            cmd.Parameters.AddWithValue("@segments", json);

            await cmd.ExecuteNonQueryAsync(ct);
        }

        private async Task EnsureAuditLogTableAsync(CancellationToken ct)
        {
            const string sql = @"
CREATE TABLE IF NOT EXISTS frl.frl_qc_training_log (
    id SERIAL PRIMARY KEY,
    imageid INTEGER NOT NULL,
    action TEXT NOT NULL,
    original_movement TEXT,
    corrected_movement TEXT,
    confidence REAL,
    created_at TIMESTAMP DEFAULT NOW()
);";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        private async Task LogQcActionAsync(
            int imageId, string action, string? originalMovement,
            string? correctedMovement, float? confidence, CancellationToken ct)
        {
            await EnsureAuditLogTableAsync(ct);

            const string sql = @"
INSERT INTO frl.frl_qc_training_log (imageid, action, original_movement, corrected_movement, confidence)
VALUES (@imageid, @action, @original, @corrected, @confidence);";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@imageid", imageId);
            cmd.Parameters.AddWithValue("@action", action);
            cmd.Parameters.AddWithValue("@original", (object?)originalMovement ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@corrected", (object?)correctedMovement ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@confidence", (object?)confidence ?? DBNull.Value);

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

        public sealed class VerifyPasswordRequest
        {
            public string? Password { get; set; }
        }

        public sealed class VerifyPasswordResponse
        {
            public bool Ok { get; set; }
            public string? Error { get; set; }
        }

        public sealed class JobStartRequest
        {
            public string? StartedBy { get; set; }
            public int Requested { get; set; }
        }

        public sealed class JobStartResponse
        {
            public Guid JobId { get; set; }
        }

        public sealed class JobFinishRequest
        {
            public Guid JobId { get; set; }
            public string Status { get; set; } = "done";
        }

        public sealed class ActiveJob
        {
            public Guid JobId { get; set; }
            public string StartedBy { get; set; } = "";
            public int Requested { get; set; }
            public int Processed { get; set; }
            public int Failed { get; set; }
            public string Status { get; set; } = "";
            public DateTime StartedAt { get; set; }
            public DateTime UpdatedAt { get; set; }
        }

        public sealed class ActiveJobsResponse
        {
            public List<ActiveJob> Jobs { get; set; } = new();
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
            public int? TargetFrame { get; set; }
            public float Confidence { get; set; }
            public string Status { get; set; } = "";
            public List<ImageMovementInfo> AllMovements { get; set; } = new();
            public List<SegmentDto>? Segments { get; set; }
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
            public float? Confidence { get; set; }
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

            [JsonPropertyName("segments")]
            public List<VideoMaeSegment>? Segments { get; set; }
        }

        private sealed class VideoMaeMovement
        {
            [JsonPropertyName("label")]
            public string Label { get; set; } = "";

            [JsonPropertyName("confidence")]
            public double Confidence { get; set; }
        }

        private sealed class VideoMaeSegment
        {
            [JsonPropertyName("start")]
            public double Start { get; set; }

            [JsonPropertyName("end")]
            public double End { get; set; }

            [JsonPropertyName("movements")]
            public List<VideoMaeMovement>? Movements { get; set; }
        }

        public sealed class SegmentDto
        {
            public double Start { get; set; }
            public double End { get; set; }
            public List<SegmentMovementDto> Movements { get; set; } = new();
        }

        public sealed class SegmentMovementDto
        {
            public string Label { get; set; } = "";
            public double Confidence { get; set; }
        }

        public sealed class TrainingLogEntry
        {
            public int Id { get; set; }
            public int ImageId { get; set; }
            public string Action { get; set; } = "";
            public string? OriginalMovement { get; set; }
            public string? CorrectedMovement { get; set; }
            public float? Confidence { get; set; }
            public string CreatedAt { get; set; } = "";
        }

        public sealed class TrainingExportResponse
        {
            public List<TrainingLogEntry> Entries { get; set; } = new();
            public int TotalCount { get; set; }
        }
    }
}
