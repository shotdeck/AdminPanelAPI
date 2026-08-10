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

        // Movements the QC UI hides and reviewers can never action. They must
        // not keep an image from counting as completed.
        private static readonly string[] NonQcMovements = { "pov" };

        private static readonly string NonQcMovementsSql =
            string.Join(",", NonQcMovements.Select(m => $"'{m}'"));

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

        // ── GET /api/admin/camera-movements/media-types ────────────────
        // Distinct media types available to fetch, from frl_movies.media_type.
        // Trailers are excluded (never fetched). The frontend uses these exact
        // values for the fetch filter so there's no format/casing mismatch.
        [HttpGet("media-types")]
        [ProducesResponseType(typeof(MediaTypesResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<MediaTypesResponse>> GetMediaTypes(CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            // Cast to text: media_type may be a Postgres enum, and btrim/lower
            // are not defined for enum types.
            const string sql = @"
SELECT DISTINCT media_type::text AS media_type
FROM frl.frl_movies
WHERE media_type IS NOT NULL
  AND btrim(media_type::text) <> ''
  AND lower(media_type::text) <> 'trailer'
ORDER BY media_type::text;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            var types = new List<string>();
            while (await reader.ReadAsync(ct))
                types.Add(reader.GetString(0));

            return Ok(new MediaTypesResponse { MediaTypes = types });
        }

        // ── POST /api/admin/camera-movements/analyze ───────────────────
        // Batch-analyze images: calls VideoMAE API for each, stores results.
        [HttpPost("analyze")]
        [ProducesResponseType(typeof(AnalyzeBatchResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<AnalyzeBatchResponse>> AnalyzeBatch(
            [FromQuery] int limit = 100,
            [FromQuery] Guid? jobId = null,
            [FromQuery] string? mediaType = null,
            [FromQuery] string? owner = null,
            CancellationToken ct = default)
        {
            if (limit < 1) limit = 1;
            if (limit > 500) limit = 500;

            var cameraMotionApiUrl = _configuration["CameraMotion:ApiUrl"]
                ?? "https://semanticsearch--camera-motion-api-fastapi-app.modal.run";

            await EnsureOpenAsync(ct);
            await EnsureJobTablesAsync(ct);
            await EnsureUsersTablesAsync(ct);

            // 1. Atomically claim the next N unanalyzed images so simultaneous
            //    fetches (from other sessions) grab disjoint sets. Claims tie to
            //    a job id; a random one is used when no job is supplied.
            var claimJobId = jobId ?? Guid.NewGuid();
            var images = await ClaimImagesAsync(claimJobId, limit, mediaType, ct);

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
                    await AssignImageOwnerAsync(r.ImageId, owner, ct);
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
                await AssignImageOwnerAsync(r.ImageId, owner, ct);
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

        // ── POST /api/admin/camera-movements/login ─────────────────────
        // Per-reviewer login: pick a name + enter that reviewer's password.
        // Admins authenticate against the CAMERAMOVEMENTPASSWORD app setting
        // (changeable in Azure); everyone else against their PBKDF2 hash that
        // an admin set. Never stores or returns the plaintext password.
        [HttpPost("login")]
        [ProducesResponseType(typeof(LoginResponse), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status401Unauthorized)]
        public async Task<ActionResult<LoginResponse>> Login([FromBody] LoginRequest req, CancellationToken ct = default)
        {
            var name = (req?.Name ?? "").Trim();
            var password = req?.Password ?? "";
            if (string.IsNullOrWhiteSpace(name))
                return Unauthorized(new LoginResponse { Ok = false, Error = "Select your name." });

            await EnsureOpenAsync(ct);
            await EnsureUsersTablesAsync(ct);

            const string sql =
                "SELECT name, is_admin, password_hash " +
                "FROM frl.frl_camera_movement_users " +
                "WHERE lower(name) = lower(@name) LIMIT 1;";

            string? canonicalName = null;
            var isAdmin = false;
            string? hash = null;
            var found = false;

            await using (var cmd = new NpgsqlCommand(sql, _connection))
            {
                cmd.Parameters.AddWithValue("@name", name);
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                if (await reader.ReadAsync(ct))
                {
                    found = true;
                    canonicalName = reader.GetString(0);
                    isAdmin = reader.GetBoolean(1);
                    hash = reader.IsDBNull(2) ? null : reader.GetString(2);
                }
            }

            if (!found)
                return Unauthorized(new LoginResponse { Ok = false, Error = "Unknown reviewer." });

            bool ok;
            if (isAdmin)
            {
                var expected = _configuration["CAMERAMOVEMENTPASSWORD"];
                if (string.IsNullOrEmpty(expected))
                    return StatusCode(StatusCodes.Status503ServiceUnavailable,
                        new LoginResponse { Ok = false, Error = "Admin password not configured." });
                ok = CryptographicOperations.FixedTimeEquals(
                    Encoding.UTF8.GetBytes(password),
                    Encoding.UTF8.GetBytes(expected));
            }
            else
            {
                if (string.IsNullOrEmpty(hash))
                    return Unauthorized(new LoginResponse
                    {
                        Ok = false,
                        Error = "No password set yet. Ask MacK to set your password."
                    });
                ok = VerifyHashedPassword(password, hash);
            }

            if (!ok)
                return Unauthorized(new LoginResponse { Ok = false, Error = "Incorrect password." });

            return Ok(new LoginResponse { Ok = true, Name = canonicalName, IsAdmin = isAdmin });
        }

        // PBKDF2 (SHA-256) password hashing. Format:
        //   pbkdf2$<iterations>$<saltB64>$<hashB64>
        private const int Pbkdf2Iterations = 100_000;
        private const int Pbkdf2SaltBytes = 16;
        private const int Pbkdf2HashBytes = 32;

        private static string HashPassword(string password)
        {
            var salt = RandomNumberGenerator.GetBytes(Pbkdf2SaltBytes);
            var hash = Rfc2898DeriveBytes.Pbkdf2(
                Encoding.UTF8.GetBytes(password), salt,
                Pbkdf2Iterations, HashAlgorithmName.SHA256, Pbkdf2HashBytes);
            return $"pbkdf2${Pbkdf2Iterations}${Convert.ToBase64String(salt)}${Convert.ToBase64String(hash)}";
        }

        private static bool VerifyHashedPassword(string password, string? stored)
        {
            if (string.IsNullOrEmpty(stored)) return false;
            var parts = stored.Split('$');
            if (parts.Length != 4 || parts[0] != "pbkdf2") return false;
            if (!int.TryParse(parts[1], out var iterations) || iterations < 1) return false;

            byte[] salt, expected;
            try
            {
                salt = Convert.FromBase64String(parts[2]);
                expected = Convert.FromBase64String(parts[3]);
            }
            catch (FormatException)
            {
                return false;
            }

            var actual = Rfc2898DeriveBytes.Pbkdf2(
                Encoding.UTF8.GetBytes(password), salt,
                iterations, HashAlgorithmName.SHA256, expected.Length);
            return CryptographicOperations.FixedTimeEquals(actual, expected);
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

        // History of fetch runs (any status), newest first. Powers the
        // notifications panel so an admin can see who pulled how many, when.
        [HttpGet("analyze/jobs/history")]
        [ProducesResponseType(typeof(ActiveJobsResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<ActiveJobsResponse>> JobHistory(
            [FromQuery] int limit = 50,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            await EnsureJobTablesAsync(ct);

            if (limit < 1) limit = 1;
            if (limit > 200) limit = 200;

            const string sql = @"
SELECT job_id, started_by, requested, processed, failed, status, started_at, updated_at
FROM frl.frl_camera_movement_jobs
ORDER BY started_at DESC
LIMIT @limit;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@limit", limit);
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
        public async Task<ActionResult<TagSummaryResponse>> GetTags(
            [FromQuery] string? owner = null,
            [FromQuery] int? movieId = null,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            await EnsureUsersTablesAsync(ct);

            var ownerActive = OwnerActive(owner);
            var movieActive = MovieActive(movieId);
            var filters = new List<string>();
            if (ownerActive) filters.Add(OwnerWhereSql);
            if (movieActive) filters.Add(MovieWhereSql);
            var ownerFilter = filters.Count > 0 ? "WHERE " + string.Join(" AND ", filters) : "";

            var sql = $@"
SELECT cm.movement,
       COUNT(*) AS total,
       COUNT(*) FILTER (WHERE cm.status = 'ok') AS confirmed,
       COUNT(*) FILTER (WHERE cm.status = 'bad') AS rejected,
       COUNT(*) FILTER (WHERE cm.status = 'not_checked') AS remaining,
       COUNT(*) FILTER (WHERE cm.status = 'flagged') AS flagged
FROM frl.frl_join_image_camera_movements cm
{ownerFilter}
GROUP BY cm.movement
ORDER BY total DESC;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            if (ownerActive) cmd.Parameters.AddWithValue("@owner", owner!.Trim());
            if (movieActive) cmd.Parameters.AddWithValue("@movieId", movieId!.Value);
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

        // ── GET /api/admin/camera-movements/analyzed-count ─────────────
        // Total distinct images that have been through camera-movement analysis.
        [HttpGet("analyzed-count")]
        [ProducesResponseType(typeof(AnalyzedCountResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<AnalyzedCountResponse>> GetAnalyzedCount(CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            const string sql = @"
SELECT COUNT(DISTINCT imageid)
FROM frl.frl_join_image_camera_movements;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            var count = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

            return Ok(new AnalyzedCountResponse { AnalyzedImages = count });
        }

        // ── GET /api/admin/camera-movements/movies ─────────────────────
        // Distinct movies that have at least one analyzed image, for the
        // movie-title search/filter. Optional owner narrows to that reviewer.
        [HttpGet("movies")]
        [ProducesResponseType(typeof(List<QcMovie>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<QcMovie>>> GetAnalyzedMovies(
            [FromQuery] string? owner = null,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            await EnsureUsersTablesAsync(ct);

            var ownerActive = OwnerActive(owner);
            var ownerFilter = ownerActive ? $" AND {OwnerWhereSql}" : "";

            var sql = $@"
SELECT DISTINCT i.movieid, COALESCE(m.title, '') AS title, m.year AS year
FROM frl.frl_join_image_camera_movements cm
INNER JOIN frl.frl_images i ON i.idnum = cm.imageid
LEFT JOIN frl.frl_movies m ON m.idnum = i.movieid
WHERE TRUE{ownerFilter}
ORDER BY title;";

            var movies = new List<QcMovie>();
            await using var cmd = new NpgsqlCommand(sql, _connection);
            if (ownerActive) cmd.Parameters.AddWithValue("@owner", owner!.Trim());
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                movies.Add(new QcMovie
                {
                    MovieId = reader.GetInt32(0),
                    Title = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    Year = reader.IsDBNull(2) ? null : reader.GetInt32(2)
                });
            }
            return Ok(movies);
        }

        // ── GET /api/admin/camera-movements/users ──────────────────────
        // The reviewer roster picked from at login.
        [HttpGet("users")]
        [ProducesResponseType(typeof(List<QcUser>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<QcUser>>> GetUsers(CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            await EnsureUsersTablesAsync(ct);

            const string sql = @"
SELECT id, name, is_admin, (password_hash IS NOT NULL) AS has_password
FROM frl.frl_camera_movement_users
ORDER BY is_admin DESC, lower(name);";

            var users = new List<QcUser>();
            await using var cmd = new NpgsqlCommand(sql, _connection);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                users.Add(new QcUser
                {
                    Id = reader.GetInt32(0),
                    Name = reader.GetString(1),
                    IsAdmin = reader.GetBoolean(2),
                    HasPassword = reader.GetBoolean(3)
                });
            }
            return Ok(users);
        }

        // ── GET /api/admin/camera-movements/users/stats ───────────────
        // Per-reviewer activity for an optional [from, to] date range:
        //   pulled     = images fetched (owned), assigned in range
        //   tagsAdded  = movement tags created on their images in range
        //   completed  = their images confirmed (status = ok) in range
        // `to` is treated as inclusive (whole day).
        [HttpGet("users/stats")]
        [ProducesResponseType(typeof(List<QcUserStats>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<QcUserStats>>> GetUserStats(
            [FromQuery] string? from = null,
            [FromQuery] string? to = null,
            [FromQuery] string? name = null,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            await EnsureTimestampColumnsAsync(ct);
            await EnsureUsersTablesAsync(ct);

            DateTime? fromDt = null, toExclusive = null;
            if (!string.IsNullOrWhiteSpace(from) && DateTime.TryParse(from, out var f))
                fromDt = f.Date;
            if (!string.IsNullOrWhiteSpace(to) && DateTime.TryParse(to, out var t))
                toExclusive = t.Date.AddDays(1);

            var ownerRange = DateRangeSql("o.assigned_at", fromDt, toExclusive);
            var addedRange = DateRangeSql("cm.created_at", fromDt, toExclusive);
            var doneRange = DateRangeSql("cm.updated_at", fromDt, toExclusive);

            // "Completed" = images whose tags are all actioned (confirmed or
            // incorrect); the range is applied to when a tag was last actioned.
            var completedRange = DateRangeSql("e.updated_at", fromDt, toExclusive);

            // Non-admins may only request their own row.
            var nameFilter = string.IsNullOrWhiteSpace(name)
                ? "" : " WHERE lower(u.name) = lower(@name)";

            var sql = $@"
SELECT u.name,
  (SELECT COUNT(*) FROM frl.frl_camera_movement_image_owner o
     WHERE lower(o.owner) = lower(u.name){ownerRange}) AS pulled,
  (SELECT COUNT(*) FROM frl.frl_join_image_camera_movements cm
     JOIN frl.frl_camera_movement_image_owner o ON o.imageid = cm.imageid
     WHERE lower(o.owner) = lower(u.name){addedRange}) AS tags_added,
  (SELECT COUNT(*) FROM frl.frl_camera_movement_image_owner o
     WHERE lower(o.owner) = lower(u.name)
       AND EXISTS (SELECT 1 FROM frl.frl_join_image_camera_movements e
                   WHERE e.imageid = o.imageid{completedRange})
       AND NOT EXISTS (SELECT 1 FROM frl.frl_join_image_camera_movements n
                   WHERE n.imageid = o.imageid AND n.status NOT IN ('ok','bad')
                     AND n.movement NOT IN ({NonQcMovementsSql}))) AS completed,
  (SELECT COUNT(*) FROM frl.frl_join_image_camera_movements cm
     JOIN frl.frl_camera_movement_image_owner o ON o.imageid = cm.imageid
     WHERE lower(o.owner) = lower(u.name) AND cm.status = 'ok'{doneRange}) AS confirmed_tags,
  (SELECT COUNT(*) FROM frl.frl_join_image_camera_movements cm
     JOIN frl.frl_camera_movement_image_owner o ON o.imageid = cm.imageid
     WHERE lower(o.owner) = lower(u.name) AND cm.status IN ('ok','bad'){doneRange}) AS reviewed_tags
FROM frl.frl_camera_movement_users u{nameFilter}
ORDER BY u.is_admin DESC, lower(u.name);";

            var stats = new List<QcUserStats>();
            await using var cmd = new NpgsqlCommand(sql, _connection);
            if (fromDt.HasValue) cmd.Parameters.AddWithValue("@from", fromDt.Value);
            if (toExclusive.HasValue) cmd.Parameters.AddWithValue("@to", toExclusive.Value);
            if (!string.IsNullOrWhiteSpace(name)) cmd.Parameters.AddWithValue("@name", name.Trim());
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                stats.Add(new QcUserStats
                {
                    Name = reader.GetString(0),
                    Pulled = Convert.ToInt32(reader.GetInt64(1)),
                    TagsAdded = Convert.ToInt32(reader.GetInt64(2)),
                    Completed = Convert.ToInt32(reader.GetInt64(3)),
                    ConfirmedTags = Convert.ToInt32(reader.GetInt64(4)),
                    ReviewedTags = Convert.ToInt32(reader.GetInt64(5))
                });
            }
            return Ok(stats);
        }

        private static string DateRangeSql(string column, DateTime? from, DateTime? to)
        {
            var sb = new System.Text.StringBuilder();
            if (from.HasValue) sb.Append($" AND {column} >= @from");
            if (to.HasValue) sb.Append($" AND {column} < @to");
            return sb.ToString();
        }

        // ── POST /api/admin/camera-movements/users ─────────────────────
        // Add a reviewer. Admin-only (actingUser must be an admin).
        [HttpPost("users")]
        [ProducesResponseType(typeof(QcUser), StatusCodes.Status200OK)]
        public async Task<ActionResult<QcUser>> AddUser([FromBody] UserWriteRequest request, CancellationToken ct = default)
        {
            var name = (request.Name ?? "").Trim();
            if (string.IsNullOrWhiteSpace(name))
                return BadRequest(new { error = "Name is required." });

            await EnsureOpenAsync(ct);
            await EnsureUsersTablesAsync(ct);
            if (!await IsAdminAsync(request.ActingUser, ct))
                return StatusCode(403, new { error = "Only an admin can manage users." });

            const string sql = @"
INSERT INTO frl.frl_camera_movement_users (name, is_admin, password_hash)
VALUES (@name, @isAdmin, @passwordHash)
ON CONFLICT (name) DO NOTHING
RETURNING id, name, is_admin;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@name", name);
            cmd.Parameters.AddWithValue("@isAdmin", request.IsAdmin);
            cmd.Parameters.AddWithValue("@passwordHash",
                string.IsNullOrWhiteSpace(request.Password)
                    ? (object)DBNull.Value
                    : HashPassword(request.Password));
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return Conflict(new { error = "A user with that name already exists." });

            return Ok(new QcUser
            {
                Id = reader.GetInt32(0),
                Name = reader.GetString(1),
                IsAdmin = reader.GetBoolean(2)
            });
        }

        // ── PUT /api/admin/camera-movements/users/{id} ─────────────────
        // Rename / change admin flag for a reviewer. Admin-only.
        [HttpPut("users/{id:int}")]
        public async Task<IActionResult> UpdateUser(int id, [FromBody] UserWriteRequest request, CancellationToken ct = default)
        {
            var name = (request.Name ?? "").Trim();
            if (string.IsNullOrWhiteSpace(name))
                return BadRequest(new { error = "Name is required." });

            await EnsureOpenAsync(ct);
            await EnsureUsersTablesAsync(ct);
            if (!await IsAdminAsync(request.ActingUser, ct))
                return StatusCode(403, new { error = "Only an admin can manage users." });

            // Update the password only when a new one is supplied; a blank
            // password leaves the existing hash untouched.
            var setPassword = !string.IsNullOrWhiteSpace(request.Password);
            var passwordClause = setPassword ? ", password_hash = @passwordHash" : "";

            // Re-point owned images if the name changes, so their work follows.
            var sql = $@"
UPDATE frl.frl_camera_movement_image_owner o
SET owner = @name
FROM frl.frl_camera_movement_users u
WHERE u.id = @id AND o.owner = u.name AND u.name <> @name;

UPDATE frl.frl_camera_movement_users
SET name = @name, is_admin = @isAdmin{passwordClause}
WHERE id = @id;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.AddWithValue("@name", name);
            cmd.Parameters.AddWithValue("@isAdmin", request.IsAdmin);
            if (setPassword)
                cmd.Parameters.AddWithValue("@passwordHash", HashPassword(request.Password!));
            var affected = await cmd.ExecuteNonQueryAsync(ct);
            if (affected == 0) return NotFound(new { error = "User not found." });
            return NoContent();
        }

        // ── DELETE /api/admin/camera-movements/users/{id} ──────────────
        // Remove a reviewer. Admin-only. Their owned images are reassigned to
        // the reviewer named in reassignTo so no work is orphaned.
        [HttpDelete("users/{id:int}")]
        public async Task<IActionResult> DeleteUser(
            int id,
            [FromQuery] string? actingUser = null,
            [FromQuery] string? reassignTo = null,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            await EnsureUsersTablesAsync(ct);
            if (!await IsAdminAsync(actingUser, ct))
                return StatusCode(403, new { error = "Only an admin can manage users." });

            // Resolve the reviewer being removed (and block admin removal).
            const string findSql = "SELECT name, is_admin FROM frl.frl_camera_movement_users WHERE id = @id LIMIT 1;";
            string? removedName = null;
            var isAdmin = false;
            await using (var findCmd = new NpgsqlCommand(findSql, _connection))
            {
                findCmd.Parameters.AddWithValue("@id", id);
                await using var reader = await findCmd.ExecuteReaderAsync(ct);
                if (await reader.ReadAsync(ct))
                {
                    removedName = reader.GetString(0);
                    isAdmin = reader.GetBoolean(1);
                }
            }
            if (removedName == null)
                return BadRequest(new { error = "User not found." });
            if (isAdmin)
                return BadRequest(new { error = "Admins cannot be removed." });

            // Reassign the removed reviewer's owned images to the chosen target.
            if (!string.IsNullOrWhiteSpace(reassignTo))
            {
                if (!await UserExistsAsync(reassignTo, ct))
                    return BadRequest(new { error = "Reassign target is not a valid reviewer." });

                const string moveSql =
                    "UPDATE frl.frl_camera_movement_image_owner " +
                    "SET owner = @to, assigned_at = now() WHERE lower(owner) = lower(@from);";
                await using var moveCmd = new NpgsqlCommand(moveSql, _connection);
                moveCmd.Parameters.AddWithValue("@to", reassignTo.Trim());
                moveCmd.Parameters.AddWithValue("@from", removedName);
                await moveCmd.ExecuteNonQueryAsync(ct);
            }

            const string sql = "DELETE FROM frl.frl_camera_movement_users WHERE id = @id AND is_admin = false;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@id", id);
            var affected = await cmd.ExecuteNonQueryAsync(ct);
            if (affected == 0)
                return BadRequest(new { error = "User not found, or admins cannot be removed." });
            return NoContent();
        }

        private async Task<bool> UserExistsAsync(string? name, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(name)) return false;
            const string sql = "SELECT 1 FROM frl.frl_camera_movement_users WHERE lower(name) = lower(@name) LIMIT 1;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@name", name.Trim());
            var result = await cmd.ExecuteScalarAsync(ct);
            return result != null;
        }

        // A non-admin reviewer may only edit images they own. Requests that
        // omit actingUser are allowed so clients on an older build keep
        // working; the QC frontend always sends it.
        private async Task<bool> CanEditImagesAsync(
            string? actingUser, IEnumerable<int> imageIds, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(actingUser)) return true;
            if (await IsAdminAsync(actingUser, ct)) return true;

            var ids = imageIds.Distinct().ToArray();
            if (ids.Length == 0) return true;

            const string sql = @"
SELECT 1 FROM frl.frl_camera_movement_image_owner
WHERE imageid = ANY(@ids) AND lower(owner) <> lower(@owner)
LIMIT 1;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@ids", ids);
            cmd.Parameters.AddWithValue("@owner", actingUser.Trim());
            return await cmd.ExecuteScalarAsync(ct) == null;
        }

        private async Task<bool> IsAdminAsync(string? name, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(name)) return false;
            const string sql = "SELECT is_admin FROM frl.frl_camera_movement_users WHERE lower(name) = lower(@name) LIMIT 1;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@name", name.Trim());
            var result = await cmd.ExecuteScalarAsync(ct);
            return result is bool b && b;
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
            [FromQuery] string? sort = null,
            [FromQuery] double? minLen = null,
            [FromQuery] double? maxLen = null,
            [FromQuery] string? owner = null,
            [FromQuery] int? movieId = null,
            CancellationToken ct = default)
        {
            if (page < 1) page = 1;
            if (pageSize < 1) pageSize = 20;
            if (pageSize > 200) pageSize = 200;

            await EnsureOpenAsync(ct);
            await EnsureTimestampColumnsAsync(ct);
            await EnsureUsersTablesAsync(ct);

            var orderBy = ResolveClipSort(sort);
            var cutActive = CutLengthActive(minLen, maxLen);
            var ownerActive = OwnerActive(owner);
            var movieActive = MovieActive(movieId);

            var whereClauses = new List<string> { "cm.movement = @movement" };
            if (!string.IsNullOrWhiteSpace(status))
                whereClauses.Add("cm.status = @status");
            if (cutActive)
                whereClauses.AddRange(CutLengthWhere(minLen, maxLen));
            if (ownerActive)
                whereClauses.Add(OwnerWhereSql);
            if (movieActive)
                whereClauses.Add(MovieWhereSql);

            var whereStr = string.Join(" AND ", whereClauses);
            var offset = (page - 1) * pageSize;

            // The cut-length filter needs the scene-boundary join + lateral;
            // the count query normally only touches cm, so add them when active.
            var countJoins = cutActive ? $@"
INNER JOIN frl.frl_images i ON i.idnum = cm.imageid
INNER JOIN frl.frl_image_scene_boundaries sb
    ON sb.movieid = i.movieid AND sb.filename = i.randid{CutLengthLateralSql}" : "";

            var countSql = $@"
SELECT COUNT(*)
FROM frl.frl_join_image_camera_movements cm{countJoins}
WHERE {whereStr};";

            await using var countCmd = new NpgsqlCommand(countSql, _connection);
            countCmd.Parameters.AddWithValue("@movement", movement);
            if (!string.IsNullOrWhiteSpace(status))
                countCmd.Parameters.AddWithValue("@status", status);
            AddCutLengthParams(countCmd, minLen, maxLen);
            if (ownerActive) countCmd.Parameters.AddWithValue("@owner", owner!.Trim());
            if (movieActive) countCmd.Parameters.AddWithValue("@movieId", movieId!.Value);

            var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct));

            var dataLateral = cutActive ? CutLengthLateralSql : "";
            var dataSql = $@"
SELECT cm.imageid,
       cm.movement,
       cm.confidence,
       cm.status,
       i.movieid,
       i.randid,
       i.filename AS image_filename,
       sb.start_time,
       sb.end_time,
       sb.fps,
       sb.target_frame,
       m.title AS movie_title,
       m.media_type AS media_type
FROM frl.frl_join_image_camera_movements cm
INNER JOIN frl.frl_images i ON i.idnum = cm.imageid
INNER JOIN frl.frl_image_scene_boundaries sb
    ON sb.movieid = i.movieid AND sb.filename = i.randid{dataLateral}
LEFT JOIN frl.frl_movies m ON m.idnum = i.movieid
WHERE {whereStr}
ORDER BY {orderBy}
LIMIT @limit OFFSET @offset;";

            await using var cmd = new NpgsqlCommand(dataSql, _connection);
            cmd.Parameters.AddWithValue("@movement", movement);
            if (!string.IsNullOrWhiteSpace(status))
                cmd.Parameters.AddWithValue("@status", status);
            AddCutLengthParams(cmd, minLen, maxLen);
            if (ownerActive) cmd.Parameters.AddWithValue("@owner", owner!.Trim());
            if (movieActive) cmd.Parameters.AddWithValue("@movieId", movieId!.Value);
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
                    Filename = reader.IsDBNull(reader.GetOrdinal("image_filename"))
                        ? "" : reader.GetString(reader.GetOrdinal("image_filename")),
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
                    MediaType = reader.IsDBNull(reader.GetOrdinal("media_type"))
                        ? "" : reader.GetString(reader.GetOrdinal("media_type")),
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
                        MediaType = row.MediaType,
                        Filename = row.Filename,
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
            await EnsureTimestampColumnsAsync(ct);
            await EnsureUsersTablesAsync(ct);

            var orderBy = ResolveClipSort(request.Sort);
            var cutActive = CutLengthActive(request.MinLen, request.MaxLen);
            var cutWhere = cutActive
                ? " AND " + string.Join(" AND ", CutLengthWhere(request.MinLen, request.MaxLen))
                : "";
            var ownerActive = OwnerActive(request.Owner);
            var ownerWhere = ownerActive ? " AND " + OwnerWhereSql : "";
            var movieActive = MovieActive(request.MovieId);
            var movieWhere = movieActive ? " AND " + MovieWhereSql : "";

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

            // The cut-length filter needs the scene-boundary join + lateral;
            // the count query normally only touches cm, so add them when active.
            var countJoins = cutActive ? $@"
INNER JOIN frl.frl_images i ON i.idnum = cm.imageid
INNER JOIN frl.frl_image_scene_boundaries sb
    ON sb.movieid = i.movieid AND sb.filename = i.randid{CutLengthLateralSql}" : "";

            // Count query
            var countSql = $@"
SELECT COUNT(*)
FROM frl.frl_join_image_camera_movements cm{countJoins}
WHERE cm.movement = @firstMovement
  AND cm.imageid IN ({imageSubquery}{excludeClause}){statusClause}{cutWhere}{ownerWhere}{movieWhere};";

            await using var countCmd = new NpgsqlCommand(countSql, _connection);
            countCmd.Parameters.AddWithValue("@firstMovement", include[0]);
            countCmd.Parameters.AddWithValue("@includeCount", include.Count);
            for (int i = 0; i < include.Count; i++)
                countCmd.Parameters.AddWithValue($"@inc{i}", include[i]);
            for (int i = 0; i < exclude.Count; i++)
                countCmd.Parameters.AddWithValue($"@exc{i}", exclude[i]);
            if (!string.IsNullOrWhiteSpace(request.Status))
                countCmd.Parameters.AddWithValue("@status", request.Status);
            AddCutLengthParams(countCmd, request.MinLen, request.MaxLen);
            if (ownerActive) countCmd.Parameters.AddWithValue("@owner", request.Owner!.Trim());
            if (movieActive) countCmd.Parameters.AddWithValue("@movieId", request.MovieId!.Value);

            var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct));

            // Data query
            var dataLateral = cutActive ? CutLengthLateralSql : "";
            var dataSql = $@"
SELECT cm.imageid,
       cm.movement,
       cm.confidence,
       cm.status,
       i.movieid,
       i.randid,
       i.filename AS image_filename,
       sb.start_time,
       sb.end_time,
       sb.fps,
       sb.target_frame,
       m.title AS movie_title,
       m.media_type AS media_type
FROM frl.frl_join_image_camera_movements cm
INNER JOIN frl.frl_images i ON i.idnum = cm.imageid
INNER JOIN frl.frl_image_scene_boundaries sb
    ON sb.movieid = i.movieid AND sb.filename = i.randid{dataLateral}
LEFT JOIN frl.frl_movies m ON m.idnum = i.movieid
WHERE cm.movement = @firstMovement
  AND cm.imageid IN ({imageSubquery}{excludeClause}){statusClause}{cutWhere}{ownerWhere}{movieWhere}
ORDER BY {orderBy}
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
            AddCutLengthParams(cmd, request.MinLen, request.MaxLen);
            if (ownerActive) cmd.Parameters.AddWithValue("@owner", request.Owner!.Trim());
            if (movieActive) cmd.Parameters.AddWithValue("@movieId", request.MovieId!.Value);
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
                    Filename = reader.IsDBNull(reader.GetOrdinal("image_filename"))
                        ? "" : reader.GetString(reader.GetOrdinal("image_filename")),
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
                    MediaType = reader.IsDBNull(reader.GetOrdinal("media_type"))
                        ? "" : reader.GetString(reader.GetOrdinal("media_type")),
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
                        MediaType = row.MediaType,
                        Filename = row.Filename,
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
            await EnsureUsersTablesAsync(ct);
            if (!await CanEditImagesAsync(
                    request.ActingUser, request.Items.Select(i => i.ImageId), ct))
                return StatusCode(403, new { error = "You can only review images assigned to you." });

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
            await EnsureUsersTablesAsync(ct);
            if (!await CanEditImagesAsync(request.ActingUser, new[] { request.ImageId }, ct))
                return StatusCode(403, new { error = "You can only edit images assigned to you." });

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

                // The reassigned tag is confirmed ('ok'), so queue its
                // sub-variants for review just like a QC-confirmed parent.
                await PromoteToSubMovementsAsync(request.ImageId, request.NewMovement, ct);
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
            await EnsureUsersTablesAsync(ct);
            if (!await CanEditImagesAsync(request.ActingUser, new[] { request.ImageId }, ct))
                return StatusCode(403, new { error = "You can only edit images assigned to you." });

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
            await EnsureUsersTablesAsync(ct);
            if (!await CanEditImagesAsync(request.ActingUser, new[] { request.ImageId }, ct))
                return StatusCode(403, new { error = "You can only edit images assigned to you." });

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

                // A manually added tag is confirmed ('ok'), so queue its
                // sub-variants for review just like a QC-confirmed parent.
                await PromoteToSubMovementsAsync(request.ImageId, request.Movement, ct);
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

        // Multi-user QC: the reviewer roster + per-image ownership. Seeds the
        // initial team and backfills existing images to MacK once.
        private async Task EnsureUsersTablesAsync(CancellationToken ct)
        {
            const string sql = @"
CREATE TABLE IF NOT EXISTS frl.frl_camera_movement_users (
    id          SERIAL       PRIMARY KEY,
    name        VARCHAR(120) NOT NULL UNIQUE,
    is_admin    BOOLEAN      NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
ALTER TABLE frl.frl_camera_movement_users
    ADD COLUMN IF NOT EXISTS password_hash TEXT;
CREATE TABLE IF NOT EXISTS frl.frl_camera_movement_image_owner (
    imageid      INTEGER      PRIMARY KEY,
    owner        VARCHAR(120) NOT NULL,
    assigned_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cmio_owner ON frl.frl_camera_movement_image_owner (owner);

INSERT INTO frl.frl_camera_movement_users (name, is_admin)
SELECT v.name, v.is_admin
FROM (VALUES ('MacK', true), ('Sam', false), ('Ethan', false), ('Ajai', false), ('Noah', false))
     AS v(name, is_admin)
WHERE NOT EXISTS (SELECT 1 FROM frl.frl_camera_movement_users);

INSERT INTO frl.frl_camera_movement_image_owner (imageid, owner)
SELECT DISTINCT cm.imageid, 'MacK'
FROM frl.frl_join_image_camera_movements cm
WHERE NOT EXISTS (SELECT 1 FROM frl.frl_camera_movement_image_owner)
ON CONFLICT (imageid) DO NOTHING;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // Stamp an image with the reviewer who fetched it (no-op when no owner
        // supplied). Ownership is per image and does not change on re-fetch.
        private async Task AssignImageOwnerAsync(int imageId, string? owner, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(owner)) return;
            const string sql = @"
INSERT INTO frl.frl_camera_movement_image_owner (imageid, owner)
VALUES (@imageid, @owner)
ON CONFLICT (imageid) DO UPDATE SET owner = EXCLUDED.owner, assigned_at = now();";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@imageid", imageId);
            cmd.Parameters.AddWithValue("@owner", owner.Trim());
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // Atomically select and claim the next N unanalyzed images. FOR UPDATE
        // SKIP LOCKED plus a claims table means two concurrent fetches never
        // grab the same images.
        private async Task<List<AnalyzeItem>> ClaimImagesAsync(
            Guid jobId, int limit, string? mediaType, CancellationToken ct)
        {
            // Media-type filter. A specific type selects only that type; "all"
            // (or null/empty) selects every type except trailers, which are
            // never fetched. Values come straight from frl_movies.media_type
            // (see the media-types endpoint), so match them case-insensitively.
            var isAll = string.IsNullOrWhiteSpace(mediaType)
                || string.Equals(mediaType, "all", StringComparison.OrdinalIgnoreCase);

            var mediaJoin = "LEFT JOIN frl.frl_movies m ON m.idnum = i.movieid";
            var mediaClause = isAll
                ? " AND (m.media_type IS NULL OR lower(m.media_type::text) <> 'trailer')"
                : " AND lower(m.media_type::text) = lower(@mediaType)";

            var sql = $@"
WITH candidates AS (
    SELECT i.idnum, i.movieid, i.randid, sb.start_time, sb.end_time
    FROM frl.frl_images i
    INNER JOIN frl.frl_image_scene_boundaries sb
        ON sb.movieid = i.movieid AND sb.filename = i.randid
    {mediaJoin}
    WHERE i.status = 'live'
      AND NOT EXISTS (
          SELECT 1 FROM frl.frl_join_image_camera_movements cm
          WHERE cm.imageid = i.idnum)
      AND NOT EXISTS (
          SELECT 1 FROM frl.frl_camera_movement_claims c
          WHERE c.imageid = i.idnum
            AND c.claimed_at > now() - INTERVAL '{ClaimTtlMinutes} minutes')
      {mediaClause}
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
            if (!isAll)
                cmd.Parameters.AddWithValue("@mediaType", mediaType!);

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

        // Sort keys for the clip listing endpoints. Values are trusted, fixed
        // ORDER BY fragments (never user input) to avoid SQL injection.
        private static readonly Dictionary<string, string> ClipSortOrders = new(StringComparer.OrdinalIgnoreCase)
        {
            ["edited_desc"] = "cm.updated_at DESC NULLS LAST, cm.imageid DESC",
            ["edited_asc"] = "cm.updated_at ASC NULLS LAST, cm.imageid ASC",
            ["tagged_desc"] = "cm.created_at DESC NULLS LAST, cm.imageid DESC",
            ["tagged_asc"] = "cm.created_at ASC NULLS LAST, cm.imageid ASC",
            ["confidence_desc"] = "cm.confidence DESC",
            ["confidence_asc"] = "cm.confidence ASC",
        };

        private static string ResolveClipSort(string? sort)
        {
            if (!string.IsNullOrWhiteSpace(sort) && ClipSortOrders.TryGetValue(sort, out var order))
                return order;
            return ClipSortOrders["confidence_desc"];
        }

        // Shot-length filter. Each image sits inside one cut; its true cut
        // length is derived from the scene-boundary cut_times array (the QC clip
        // itself is padded, so its start/end can't be used). The lateral finds
        // the cut boundaries surrounding the image's frame time and returns the
        // cut's duration. Depends on the sb alias being in scope.
        private const double CutLengthMax = 9.0;

        private const string CutLengthLateralSql = @"
LEFT JOIN LATERAL (
    SELECT
        COALESCE((SELECT MIN(e.v::double precision)
                  FROM jsonb_array_elements_text(sb.cut_times::jsonb) e(v)
                  WHERE e.v::double precision > (sb.target_frame::double precision / NULLIF(sb.fps, 0))),
                 sb.duration)
      - COALESCE((SELECT MAX(e.v::double precision)
                  FROM jsonb_array_elements_text(sb.cut_times::jsonb) e(v)
                  WHERE e.v::double precision <= (sb.target_frame::double precision / NULLIF(sb.fps, 0))),
                 0.0) AS cut_length
) cl ON TRUE";

        private static bool CutLengthActive(double? minLen, double? maxLen)
            => (minLen.HasValue && minLen.Value > 0)
               || (maxLen.HasValue && maxLen.Value < CutLengthMax);

        private static List<string> CutLengthWhere(double? minLen, double? maxLen)
        {
            var clauses = new List<string>();
            if (minLen.HasValue && minLen.Value > 0) clauses.Add("cl.cut_length >= @minLen");
            if (maxLen.HasValue && maxLen.Value < CutLengthMax) clauses.Add("cl.cut_length <= @maxLen");
            return clauses;
        }

        private static void AddCutLengthParams(NpgsqlCommand cmd, double? minLen, double? maxLen)
        {
            if (minLen.HasValue && minLen.Value > 0) cmd.Parameters.AddWithValue("@minLen", minLen.Value);
            if (maxLen.HasValue && maxLen.Value < CutLengthMax) cmd.Parameters.AddWithValue("@maxLen", maxLen.Value);
        }

        // Owner filter: null/empty or "all" means no restriction. Otherwise limit
        // to images owned by that reviewer via the image-owner table.
        private const string OwnerWhereSql =
            "EXISTS (SELECT 1 FROM frl.frl_camera_movement_image_owner o WHERE o.imageid = cm.imageid AND lower(o.owner) = lower(@owner))";

        private static bool OwnerActive(string? owner)
            => !string.IsNullOrWhiteSpace(owner) && !owner.Trim().Equals("all", StringComparison.OrdinalIgnoreCase);

        // Movie filter: restrict to images belonging to one movie. Uses an
        // EXISTS on frl_images so it works whether or not i is already joined.
        private const string MovieWhereSql =
            "EXISTS (SELECT 1 FROM frl.frl_images im WHERE im.idnum = cm.imageid AND im.movieid = @movieId)";

        private static bool MovieActive(int? movieId) => movieId.HasValue && movieId.Value > 0;

        private static bool _timestampColumnsReady;

        // Ensure created_at/updated_at exist on the join table so the sort
        // options work. Adds nullable columns (metadata-only, instant even on
        // large tables) with a default for future inserts; existing rows keep
        // NULL and sort last. Idempotent.
        private async Task EnsureTimestampColumnsAsync(CancellationToken ct)
        {
            if (_timestampColumnsReady) return;
            const string sql = @"
ALTER TABLE frl.frl_join_image_camera_movements ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
ALTER TABLE frl.frl_join_image_camera_movements ALTER COLUMN created_at SET DEFAULT now();
ALTER TABLE frl.frl_join_image_camera_movements ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE frl.frl_join_image_camera_movements ALTER COLUMN updated_at SET DEFAULT now();";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            await cmd.ExecuteNonQueryAsync(ct);
            _timestampColumnsReady = true;
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

        public sealed class MediaTypesResponse
        {
            public List<string> MediaTypes { get; set; } = new();
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

        public sealed class LoginRequest
        {
            public string? Name { get; set; }
            public string? Password { get; set; }
        }

        public sealed class LoginResponse
        {
            public bool Ok { get; set; }
            public string? Name { get; set; }
            public bool IsAdmin { get; set; }
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

        public sealed class AnalyzedCountResponse
        {
            public int AnalyzedImages { get; set; }
        }

        private sealed class TagClipRow
        {
            public int ImageId { get; set; }
            public string Movement { get; set; } = "";
            public float Confidence { get; set; }
            public string Status { get; set; } = "";
            public int MovieId { get; set; }
            public string RandId { get; set; } = "";
            public string Filename { get; set; } = "";
            public double? StartTime { get; set; }
            public double? EndTime { get; set; }
            public double? Fps { get; set; }
            public int? TargetFrame { get; set; }
            public string MovieTitle { get; set; } = "";
            public string MediaType { get; set; } = "";
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
            public string MediaType { get; set; } = "";
            public string Filename { get; set; } = "";
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
            public string? ActingUser { get; set; }
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
            public string? ActingUser { get; set; }
        }

        public sealed class ReassignResponse
        {
            public bool Updated { get; set; }
        }

        public sealed class DeleteTagRequest
        {
            public int ImageId { get; set; }
            public string Movement { get; set; } = "";
            public string? ActingUser { get; set; }
        }

        public sealed class DeleteTagResponse
        {
            public bool Deleted { get; set; }
        }

        public sealed class AddTagRequest
        {
            public int ImageId { get; set; }
            public string Movement { get; set; } = "";
            public string? ActingUser { get; set; }
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
            public string? Sort { get; set; }
            public double? MinLen { get; set; }
            public double? MaxLen { get; set; }
            public string? Owner { get; set; }
            public int? MovieId { get; set; }
        }

        public sealed class QcUser
        {
            public int Id { get; set; }
            public string Name { get; set; } = "";
            public bool IsAdmin { get; set; }
            public bool HasPassword { get; set; }
        }

        public sealed class QcMovie
        {
            public int MovieId { get; set; }
            public string Title { get; set; } = "";
            public int? Year { get; set; }
        }

        public sealed class QcUserStats
        {
            public string Name { get; set; } = "";
            public int Pulled { get; set; }
            public int TagsAdded { get; set; }
            // Images where every tag is actioned (confirmed or incorrect).
            public int Completed { get; set; }
            // AI accuracy: confirmed tags out of reviewed (confirmed + incorrect).
            public int ConfirmedTags { get; set; }
            public int ReviewedTags { get; set; }
        }

        public sealed class UserWriteRequest
        {
            public string? Name { get; set; }
            public bool IsAdmin { get; set; }
            public string? ActingUser { get; set; }
            public string? Password { get; set; }
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
