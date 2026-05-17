using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Data;

namespace ShotDeckSearch.Controllers
{
    [ApiController]
    [Route("api/admin/movies/{movieId}/clips")]
    public sealed class ClipsController : ControllerBase
    {
        private const int DefaultPageSize = 20;
        private const int MaxPageSize = 100;
        private const int PresignedUrlExpiryMinutes = 60;

        private readonly NpgsqlConnection _connection;
        private readonly IConfiguration _configuration;
        private readonly ILogger<ClipsController> _logger;

        public ClipsController(
            NpgsqlConnection connection,
            IConfiguration configuration,
            ILogger<ClipsController> logger)
        {
            _connection = connection;
            _configuration = configuration;
            _logger = logger;
        }

        [HttpGet]
        [ProducesResponseType(typeof(ClipPageResponse), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public async Task<ActionResult<ClipPageResponse>> GetClips(
            int movieId,
            [FromQuery] int page = 1,
            [FromQuery] int pageSize = DefaultPageSize,
            CancellationToken ct = default)
        {
            if (movieId <= 0)
                return BadRequest("Invalid movieId.");

            if (page < 1) page = 1;
            if (pageSize < 1) pageSize = DefaultPageSize;
            if (pageSize > MaxPageSize) pageSize = MaxPageSize;

            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                // Count total clips for this movie
                var countSql = @"
SELECT COUNT(*)
FROM frl.frl_images i
INNER JOIN frl.frl_image_scene_boundaries sb
    ON sb.movieid = i.movieid AND sb.filename = i.randid
WHERE i.movieid = @movieId
  AND i.status = 'live';";

                await using var countCmd = new NpgsqlCommand(countSql, _connection);
                countCmd.Parameters.AddWithValue("@movieId", movieId);
                var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct));

                var totalPages = (int)Math.Ceiling(totalCount / (double)pageSize);
                var offset = (page - 1) * pageSize;

                // Fetch movie-level QC summary (across all clips, not just current page)
                var qcSummarySql = @"
SELECT COUNT(*) AS qc_total,
       COUNT(*) FILTER (WHERE sub.all_checked) AS qc_checked
FROM (
    SELECT cm.imageid,
           bool_and(cm.status IN ('ok', 'bad', 'flagged')) AS all_checked
    FROM frl.frl_join_image_camera_movements cm
    INNER JOIN frl.frl_images i ON i.idnum = cm.imageid
    WHERE i.movieid = @movieId AND i.status = 'live'
    GROUP BY cm.imageid
) sub;";

                await using var qcCmd = new NpgsqlCommand(qcSummarySql, _connection);
                qcCmd.Parameters.AddWithValue("@movieId", movieId);
                await using var qcReader = await qcCmd.ExecuteReaderAsync(ct);
                int movieQcTotal = 0, movieQcChecked = 0;
                if (await qcReader.ReadAsync(ct))
                {
                    movieQcTotal = Convert.ToInt32(qcReader["qc_total"]);
                    movieQcChecked = Convert.ToInt32(qcReader["qc_checked"]);
                }
                await qcReader.CloseAsync();

                // Fetch paginated clip data with QC status
                var dataSql = @"
SELECT i.randid AS filename,
       sb.start_time,
       sb.end_time,
       sb.duration,
       sb.fps,
       sb.frame_count,
       sb.target_frame,
       COALESCE(qc.qc_status, 'no_tags') AS qc_status
FROM frl.frl_images i
INNER JOIN frl.frl_image_scene_boundaries sb
    ON sb.movieid = i.movieid AND sb.filename = i.randid
LEFT JOIN (
    SELECT cm.imageid,
           CASE
               WHEN bool_and(cm.status IN ('ok', 'bad', 'flagged')) THEN 'checked'
               WHEN bool_or(cm.status IN ('ok', 'bad', 'flagged')) THEN 'partial'
               ELSE 'not_checked'
           END AS qc_status
    FROM frl.frl_join_image_camera_movements cm
    GROUP BY cm.imageid
) qc ON qc.imageid = i.idnum
WHERE i.movieid = @movieId
  AND i.status = 'live'
ORDER BY i.shot_time ASC, i.randid ASC
LIMIT @limit OFFSET @offset;";

                await using var cmd = new NpgsqlCommand(dataSql, _connection);
                cmd.Parameters.AddWithValue("@movieId", movieId);
                cmd.Parameters.AddWithValue("@limit", pageSize);
                cmd.Parameters.AddWithValue("@offset", offset);

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                var clipRows = new List<ClipRow>();
                while (await reader.ReadAsync(ct))
                {
                    clipRows.Add(new ClipRow
                    {
                        Filename = reader.IsDBNull(reader.GetOrdinal("filename"))
                            ? "" : reader.GetString(reader.GetOrdinal("filename")),
                        StartTime = reader.IsDBNull(reader.GetOrdinal("start_time"))
                            ? null : reader.GetDouble(reader.GetOrdinal("start_time")),
                        EndTime = reader.IsDBNull(reader.GetOrdinal("end_time"))
                            ? null : reader.GetDouble(reader.GetOrdinal("end_time")),
                        Duration = reader.IsDBNull(reader.GetOrdinal("duration"))
                            ? null : reader.GetDouble(reader.GetOrdinal("duration")),
                        Fps = reader.IsDBNull(reader.GetOrdinal("fps"))
                            ? null : reader.GetDouble(reader.GetOrdinal("fps")),
                        FrameCount = reader.IsDBNull(reader.GetOrdinal("frame_count"))
                            ? null : reader.GetInt32(reader.GetOrdinal("frame_count")),
                        TargetFrame = reader.IsDBNull(reader.GetOrdinal("target_frame"))
                            ? null : reader.GetInt32(reader.GetOrdinal("target_frame")),
                        QcStatus = reader.GetString(reader.GetOrdinal("qc_status"))
                    });
                }

                // Generate presigned R2 URLs for each clip
                var accountId = _configuration["R2:AccountId"] ?? "";
                var accessKey = _configuration["R2:AccessKey"] ?? "";
                var secretKey = _configuration["R2:SecretKey"] ?? "";
                var bucketName = _configuration["R2:BucketName"] ?? "";

                var clips = new List<ClipDto>();

                if (!string.IsNullOrWhiteSpace(accountId) &&
                    !string.IsNullOrWhiteSpace(accessKey) &&
                    !string.IsNullOrWhiteSpace(secretKey) &&
                    !string.IsNullOrWhiteSpace(bucketName))
                {
                    var creds = new BasicAWSCredentials(accessKey.Trim(), secretKey.Trim());
                    var s3Config = new AmazonS3Config
                    {
                        ServiceURL = $"https://{accountId.Trim()}.r2.cloudflarestorage.com",
                        ForcePathStyle = true,
                        UseAccelerateEndpoint = false,
                        UseDualstackEndpoint = false,
                        EndpointDiscoveryEnabled = false
                    };

                    using var client = new AmazonS3Client(creds, s3Config);

                    foreach (var row in clipRows)
                    {
                        var key = $"clips_9s/{movieId}/{row.Filename}.mp4";
                        var url = client.GetPreSignedURL(new GetPreSignedUrlRequest
                        {
                            BucketName = bucketName,
                            Key = key,
                            Expires = DateTime.UtcNow.AddMinutes(PresignedUrlExpiryMinutes),
                            Verb = HttpVerb.GET
                        });

                        double? targetTime = null;
                        if (row.TargetFrame != null && row.Fps != null && row.Fps > 0)
                        {
                            targetTime = (double)row.TargetFrame.Value / row.Fps.Value;
                        }

                        clips.Add(new ClipDto
                        {
                            FileName = row.Filename + ".mp4",
                            Url = url,
                            StartTime = row.StartTime,
                            EndTime = row.EndTime,
                            Duration = row.Duration,
                            Fps = row.Fps,
                            FrameCount = row.FrameCount,
                            TargetFrame = row.TargetFrame,
                            TargetTime = targetTime,
                            QcStatus = row.QcStatus
                        });
                    }
                }
                else
                {
                    _logger.LogWarning("R2 settings are missing; returning clips without URLs.");
                    foreach (var row in clipRows)
                    {
                            clips.Add(new ClipDto
                            {
                                FileName = row.Filename + ".mp4",
                                Url = "",
                                StartTime = row.StartTime,
                                EndTime = row.EndTime,
                                Duration = row.Duration,
                                Fps = row.Fps,
                                FrameCount = row.FrameCount,
                                TargetFrame = row.TargetFrame,
                                QcStatus = row.QcStatus
                            });
                    }
                }

                return Ok(new ClipPageResponse
                {
                    Clips = clips,
                    Page = page,
                    PageSize = pageSize,
                    TotalCount = totalCount,
                    TotalPages = totalPages,
                    MovieId = movieId,
                    QcTotal = movieQcTotal,
                    QcChecked = movieQcChecked
                });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        private sealed class ClipRow
        {
            public string Filename { get; set; } = "";
            public double? StartTime { get; set; }
            public double? EndTime { get; set; }
            public double? Duration { get; set; }
            public double? Fps { get; set; }
            public int? FrameCount { get; set; }
            public int? TargetFrame { get; set; }
            public string QcStatus { get; set; } = "no_tags";
        }
    }

    public sealed class ClipDto
    {
        public string FileName { get; set; } = "";
        public string Url { get; set; } = "";
        public double? StartTime { get; set; }
        public double? EndTime { get; set; }
        public double? Duration { get; set; }
        public double? Fps { get; set; }
        public int? FrameCount { get; set; }
        public int? TargetFrame { get; set; }
        public double? TargetTime { get; set; }
        public string QcStatus { get; set; } = "no_tags";
    }

    public sealed class ClipPageResponse
    {
        public List<ClipDto> Clips { get; set; } = new();
        public int Page { get; set; }
        public int PageSize { get; set; }
        public int TotalCount { get; set; }
        public int TotalPages { get; set; }
        public int MovieId { get; set; }
        public int QcTotal { get; set; }
        public int QcChecked { get; set; }
    }
}
