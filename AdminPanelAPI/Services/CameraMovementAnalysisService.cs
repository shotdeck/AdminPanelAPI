using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Npgsql;
using System.Collections.Concurrent;
using System.Data;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// The camera-movement analysis pipeline shared by the QC "Fetch Next
    /// Batch" endpoint and the background bank worker: claim the next most
    /// popular un-analysed images, run them through the Modal VideoMAE API,
    /// store the movement tags and per-segment output, and record failures.
    ///
    /// Also owns the "bank" of pre-analysed images. The worker fills it with
    /// analysed-but-unowned images per media type; a fetch then assigns from
    /// it instantly instead of waiting on the GPU.
    /// </summary>
    public sealed class CameraMovementAnalysisService
    {
        private const int PresignedUrlExpiryMinutes = 60;
        private const int MaxConcurrency = 5;
        private const string DefaultApiUrl =
            "https://semanticsearch--camera-motion-api-fastapi-app.modal.run";

        /// <summary>
        /// How long a claim is honoured before it's treated as abandoned (a
        /// crashed/closed session), so the image can be picked up again.
        /// </summary>
        public const int ClaimTtlMinutes = 15;

        /// <summary>
        /// After this many failed attempts an image is parked: kept on the
        /// failures list for inspection but no longer offered to fetches.
        /// </summary>
        public const int MaxFailedAttempts = 3;

        public static readonly JsonSerializerOptions JsonOpts = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };

        private readonly NpgsqlConnection _connection;
        private readonly IConfiguration _configuration;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<CameraMovementAnalysisService> _logger;

        public CameraMovementAnalysisService(
            NpgsqlConnection connection,
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            ILogger<CameraMovementAnalysisService> logger)
        {
            _connection = connection;
            _configuration = configuration;
            _httpClientFactory = httpClientFactory;
            _logger = logger;
        }

        public string ApiUrl => _configuration["CameraMotion:ApiUrl"] ?? DefaultApiUrl;

        public async Task EnsureOpenAsync(CancellationToken ct)
        {
            if (_connection.State != ConnectionState.Open)
                await _connection.OpenAsync(ct);
        }

        public async Task EnsureTablesAsync(CancellationToken ct)
        {
            const string sql = @"
CREATE TABLE IF NOT EXISTS frl.frl_camera_movement_claims (
    imageid     INTEGER      PRIMARY KEY,
    job_id      UUID         NOT NULL,
    claimed_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cmc_job_id     ON frl.frl_camera_movement_claims (job_id);
CREATE INDEX IF NOT EXISTS idx_cmc_claimed_at ON frl.frl_camera_movement_claims (claimed_at);
CREATE TABLE IF NOT EXISTS frl.frl_camera_movement_failures (
    imageid       INTEGER      PRIMARY KEY,
    reason        TEXT         NOT NULL,
    attempts      INTEGER      NOT NULL DEFAULT 1,
    first_failed  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_failed   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cmf_attempts    ON frl.frl_camera_movement_failures (attempts);
CREATE INDEX IF NOT EXISTS idx_cmf_last_failed ON frl.frl_camera_movement_failures (last_failed DESC);
CREATE TABLE IF NOT EXISTS frl.frl_image_analysis_segments (
    imageid INTEGER PRIMARY KEY,
    segments_json JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS frl.frl_camera_movement_bank (
    imageid      INTEGER      PRIMARY KEY,
    media_type   VARCHAR(60)  NOT NULL,
    analyzed_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cmb_media_type ON frl.frl_camera_movement_bank (media_type);";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        /// <summary>Media types that are fetched for QC (everything but trailers).</summary>
        public async Task<List<string>> GetMediaTypesAsync(CancellationToken ct)
        {
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
            return types;
        }

        private static bool IsAllMediaTypes(string? mediaType) =>
            string.IsNullOrWhiteSpace(mediaType)
            || string.Equals(mediaType, "all", StringComparison.OrdinalIgnoreCase);

        // ── Bank ──────────────────────────────────────────────────────

        /// <summary>
        /// Hand the next N most popular banked images of the given media type
        /// to a reviewer. One statement: the image is removed from the bank and
        /// stamped with its owner, so two simultaneous fetches never share one.
        /// </summary>
        public async Task<int> AssignFromBankAsync(
            int limit, string? mediaType, string owner, CancellationToken ct)
        {
            var isAll = IsAllMediaTypes(mediaType);
            var mediaClause = isAll ? "" : "WHERE lower(b.media_type) = lower(@mediaType)";
            var sql = $@"
WITH picked AS (
    SELECT b.imageid
    FROM frl.frl_camera_movement_bank b
    INNER JOIN frl.frl_images i ON i.idnum = b.imageid
    {mediaClause}
    ORDER BY i.weighted_score DESC
    LIMIT @limit
    FOR UPDATE OF b SKIP LOCKED
),
removed AS (
    DELETE FROM frl.frl_camera_movement_bank b
    USING picked p
    WHERE b.imageid = p.imageid
    RETURNING b.imageid
)
INSERT INTO frl.frl_camera_movement_image_owner (imageid, owner)
SELECT imageid, @owner FROM removed
ON CONFLICT (imageid) DO UPDATE SET owner = EXCLUDED.owner, assigned_at = now();";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@limit", limit);
            cmd.Parameters.AddWithValue("@owner", owner.Trim());
            if (!isAll) cmd.Parameters.AddWithValue("@mediaType", mediaType!);
            return await cmd.ExecuteNonQueryAsync(ct);
        }

        /// <summary>Banked (analysed, unowned) image count per media type.</summary>
        public async Task<Dictionary<string, int>> GetBankCountsAsync(CancellationToken ct)
        {
            const string sql = @"
SELECT media_type, count(*)::int
FROM frl.frl_camera_movement_bank
GROUP BY media_type;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            while (await reader.ReadAsync(ct))
                counts[reader.GetString(0)] = reader.GetInt32(1);
            return counts;
        }

        private async Task AddToBankAsync(int imageId, string mediaType, CancellationToken ct)
        {
            const string sql = @"
INSERT INTO frl.frl_camera_movement_bank (imageid, media_type)
VALUES (@imageid, @mediaType)
ON CONFLICT (imageid) DO NOTHING;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@imageid", imageId);
            cmd.Parameters.AddWithValue("@mediaType", mediaType);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // ── Claiming ──────────────────────────────────────────────────

        /// <summary>
        /// Atomically select and claim the next N unanalyzed images. FOR UPDATE
        /// SKIP LOCKED plus a claims table means two concurrent callers (a
        /// fetch and the bank worker, or two fetches) never grab the same images.
        /// </summary>
        public async Task<List<AnalyzeItem>> ClaimImagesAsync(
            Guid jobId, int limit, string? mediaType, CancellationToken ct)
        {
            // Media-type filter. A specific type selects only that type; "all"
            // (or null/empty) selects every type except trailers, which are
            // never fetched. Values come straight from frl_movies.media_type
            // (see the media-types endpoint), so match them case-insensitively.
            var isAll = IsAllMediaTypes(mediaType);
            var mediaClause = isAll
                ? " AND (m.media_type IS NULL OR lower(m.media_type::text) <> 'trailer')"
                : " AND lower(m.media_type::text) = lower(@mediaType)";

            var sql = $@"
WITH candidates AS (
    SELECT i.idnum, i.movieid, i.randid, sb.start_time, sb.end_time,
           m.media_type::text AS media_type
    FROM frl.frl_images i
    INNER JOIN frl.frl_image_scene_boundaries sb
        ON sb.movieid = i.movieid AND sb.filename = i.randid
    LEFT JOIN frl.frl_movies m ON m.idnum = i.movieid
    WHERE i.status = 'live'
      AND NOT EXISTS (
          SELECT 1 FROM frl.frl_join_images_camera_movements cm
          WHERE cm.imageid = i.idnum)
      AND NOT EXISTS (
          SELECT 1 FROM frl.frl_camera_movement_claims c
          WHERE c.imageid = i.idnum
            AND c.claimed_at > now() - INTERVAL '{ClaimTtlMinutes} minutes')
      -- Park clips that keep failing. Without this they sit at the head of the
      -- popularity-ordered queue and are retried on every single fetch.
      AND NOT EXISTS (
          SELECT 1 FROM frl.frl_camera_movement_failures f
          WHERE f.imageid = i.idnum
            AND f.attempts >= {MaxFailedAttempts})
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
SELECT idnum, movieid, randid, start_time, end_time, media_type
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
                    MediaType = reader.IsDBNull(5) ? null : reader.GetString(5),
                });
            }
            return images;
        }

        public async Task ReleaseClaimsAsync(IReadOnlyCollection<int> imageIds, CancellationToken ct)
        {
            if (imageIds.Count == 0) return;
            const string sql = @"
DELETE FROM frl.frl_camera_movement_claims WHERE imageid = ANY(@ids);";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@ids", imageIds.ToArray());
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // ── Analysis ──────────────────────────────────────────────────

        public bool HasR2Settings() =>
            !string.IsNullOrWhiteSpace(_configuration["R2:AccountId"])
            && !string.IsNullOrWhiteSpace(_configuration["R2:AccessKey"])
            && !string.IsNullOrWhiteSpace(_configuration["R2:SecretKey"])
            && !string.IsNullOrWhiteSpace(_configuration["R2:BucketName"]);

        /// <summary>
        /// Run the given clips through the analysis API and persist the result.
        /// With an <paramref name="owner"/> each analysed image is stamped as
        /// theirs; with <paramref name="bank"/> it is instead placed in the bank
        /// for a later fetch to pick up. Claims are not released here.
        /// </summary>
        public async Task<AnalysisOutcome> AnalyzeAsync(
            IReadOnlyList<AnalyzeItem> images,
            string? owner,
            bool bank,
            CancellationToken ct)
        {
            var accountId = _configuration["R2:AccountId"] ?? "";
            var accessKey = _configuration["R2:AccessKey"] ?? "";
            var secretKey = _configuration["R2:SecretKey"] ?? "";
            var bucketName = _configuration["R2:BucketName"] ?? "";
            var apiUrl = ApiUrl.TrimEnd('/');

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

            var throttle = new SemaphoreSlim(MaxConcurrency);
            var results = new ConcurrentBag<(AnalyzeItem Image, List<VideoMaeMovement>? Movements, List<VideoMaeSegment>? Segments, bool Success, string? Reason)>();

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

                    var response = await httpClient.PostAsync($"{apiUrl}/analyze", jsonContent, ct);

                    if (!response.IsSuccessStatusCode)
                    {
                        var reason = await ReadFailureReasonAsync(response, ct);
                        _logger.LogWarning(
                            "VideoMAE API failed for image {ImageId}: HTTP {Status} {Reason}",
                            img.ImageId, (int)response.StatusCode, reason);
                        results.Add((img, null, null, false, reason));
                        return;
                    }

                    var responseBody = await response.Content.ReadAsStringAsync(ct);
                    var result = JsonSerializer.Deserialize<VideoMaeResponse>(responseBody, JsonOpts);
                    results.Add((img, result?.OverallMovements, result?.Segments, true, null));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to analyze image {ImageId}", img.ImageId);
                    results.Add((img, null, null, false, DescribeException(ex)));
                }
                finally
                {
                    throttle.Release();
                }
            });

            await Task.WhenAll(tasks);

            // Write results sequentially (NpgsqlConnection is not thread-safe).
            var outcome = new AnalysisOutcome();
            foreach (var r in results)
            {
                if (!r.Success)
                {
                    await RecordFailureAsync(r.Image.ImageId, r.Reason, ct);
                    outcome.Failed++;
                    continue;
                }

                await ClearFailureAsync(r.Image.ImageId, ct);

                if (r.Movements == null || r.Movements.Count == 0)
                {
                    await InsertMovementAsync(r.Image.ImageId, "hold", 0, ct);
                }
                else
                {
                    foreach (var movement in r.Movements)
                    {
                        if (movement.Label == "too_short") continue;
                        await InsertMovementAsync(r.Image.ImageId, movement.Label, movement.Confidence, ct);
                    }
                }

                await StoreSegmentsAsync(r.Image.ImageId, r.Segments, ct);
                await MaybeTagNoMovementAsync(r.Image.ImageId, ct);

                if (!string.IsNullOrWhiteSpace(owner))
                    await AssignImageOwnerAsync(r.Image.ImageId, owner, ct);
                else if (bank)
                    await AddToBankAsync(r.Image.ImageId, r.Image.MediaType ?? "unknown", ct);

                outcome.Processed++;
            }

            return outcome;
        }

        // ── Persistence helpers ───────────────────────────────────────

        // The analysis API reports the real cause in the response body (e.g.
        // "Failed to download clip: HTTP 404" for a clip missing from R2), so
        // prefer it over the bare status code. FastAPI wraps it in "detail".
        private static async Task<string> ReadFailureReasonAsync(
            HttpResponseMessage response, CancellationToken ct)
        {
            var status = $"HTTP {(int)response.StatusCode}";
            string body;
            try
            {
                body = await response.Content.ReadAsStringAsync(ct);
            }
            catch
            {
                return status;
            }

            if (string.IsNullOrWhiteSpace(body)) return status;

            try
            {
                using var doc = JsonDocument.Parse(body);
                if (doc.RootElement.ValueKind == JsonValueKind.Object &&
                    doc.RootElement.TryGetProperty("detail", out var detail))
                {
                    var text = detail.ValueKind == JsonValueKind.String
                        ? detail.GetString() : detail.ToString();
                    if (!string.IsNullOrWhiteSpace(text)) return $"{status}: {text}";
                }
            }
            catch (JsonException)
            {
                // not JSON; fall through to the raw body
            }

            return $"{status}: {Truncate(body, 400)}";
        }

        private static string DescribeException(Exception ex) =>
            ex is TaskCanceledException or OperationCanceledException
                ? "Timed out waiting for the analysis API"
                : $"{ex.GetType().Name}: {Truncate(ex.Message, 400)}";

        private static string Truncate(string value, int max)
        {
            value = value.Trim();
            return value.Length <= max ? value : value[..max] + "…";
        }

        private async Task RecordFailureAsync(int imageId, string? reason, CancellationToken ct)
        {
            const string sql = @"
INSERT INTO frl.frl_camera_movement_failures (imageid, reason)
VALUES (@imageid, @reason)
ON CONFLICT (imageid) DO UPDATE
SET reason      = EXCLUDED.reason,
    attempts    = frl.frl_camera_movement_failures.attempts + 1,
    last_failed = now();";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@imageid", imageId);
            cmd.Parameters.AddWithValue("@reason", reason ?? "Unknown error");
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // An image that eventually analyses fine shouldn't stay on the list.
        private async Task ClearFailureAsync(int imageId, CancellationToken ct)
        {
            const string sql =
                "DELETE FROM frl.frl_camera_movement_failures WHERE imageid = @imageid;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@imageid", imageId);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        private async Task InsertMovementAsync(
            int imageId, string movement, double confidence, CancellationToken ct)
        {
            const string sql = @"
INSERT INTO frl.frl_join_images_camera_movements (imageid, camera_movements, confidence)
VALUES (@imageid, @movement, @confidence)
ON CONFLICT (imageid, camera_movements) DO NOTHING;";
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
INSERT INTO frl.frl_join_images_camera_movements (imageid, camera_movements, confidence, status)
SELECT @imageid, 'no_movement', 0, 'not_checked'
WHERE EXISTS (
    SELECT 1 FROM frl.frl_join_images_camera_movements
    WHERE imageid = @imageid AND camera_movements = 'hold'
)
AND NOT EXISTS (
    SELECT 1 FROM frl.frl_join_images_camera_movements
    WHERE imageid = @imageid AND camera_movements NOT IN ('hold', 'no_movement')
)
ON CONFLICT (imageid, camera_movements) DO NOTHING;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@imageid", imageId);
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

        // Stamp an image with the reviewer who fetched it.
        private async Task AssignImageOwnerAsync(int imageId, string owner, CancellationToken ct)
        {
            const string sql = @"
INSERT INTO frl.frl_camera_movement_image_owner (imageid, owner)
VALUES (@imageid, @owner)
ON CONFLICT (imageid) DO UPDATE SET owner = EXCLUDED.owner, assigned_at = now();";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@imageid", imageId);
            cmd.Parameters.AddWithValue("@owner", owner.Trim());
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // ── Types ─────────────────────────────────────────────────────

        public sealed class AnalyzeItem
        {
            public int ImageId { get; set; }
            public int MovieId { get; set; }
            public string RandId { get; set; } = "";
            public double? StartTime { get; set; }
            public double? EndTime { get; set; }
            public string? MediaType { get; set; }
        }

        public sealed class AnalysisOutcome
        {
            public int Processed { get; set; }
            public int Failed { get; set; }
        }

        private sealed class VideoMaeResponse
        {
            [JsonPropertyName("overall_movements")]
            public List<VideoMaeMovement>? OverallMovements { get; set; }

            [JsonPropertyName("segments")]
            public List<VideoMaeSegment>? Segments { get; set; }
        }

        public sealed class VideoMaeMovement
        {
            [JsonPropertyName("label")]
            public string Label { get; set; } = "";

            [JsonPropertyName("confidence")]
            public double Confidence { get; set; }
        }

        public sealed class VideoMaeSegment
        {
            [JsonPropertyName("start")]
            public double Start { get; set; }

            [JsonPropertyName("end")]
            public double End { get; set; }

            [JsonPropertyName("movements")]
            public List<VideoMaeMovement>? Movements { get; set; }
        }
    }
}
