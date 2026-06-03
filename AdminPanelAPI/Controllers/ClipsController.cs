using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Data;
using System.Text.Json;

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

        [HttpPost("/api/admin/clips/fix-two-second-boundaries")]
        [ProducesResponseType(typeof(FixSceneBoundaryResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<FixSceneBoundaryResponse>> FixTwoSecondSceneBoundariesAsync(
    [FromQuery] bool dryRun = true,
    [FromQuery] int resultLimit = 100,
    [FromQuery] string? filename = null,
    CancellationToken ct = default)
        {
            if (resultLimit < 0)
                resultLimit = 0;

            var mustClose = false;

            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            var results = new List<FixSceneBoundaryResult>();

            void AddResult(FixSceneBoundaryResult result)
            {
                if (results.Count < resultLimit)
                {
                    results.Add(result);
                }
            }

            var totalUpdated = 0;
            var totalSkipped = 0;

            try
            {
                var records = new List<SceneBoundaryRecord>();

                const string selectSql = @"
SELECT
    movieid,
    filename,
    start_time,
    end_time,
    duration,
    fps,
    frame_count,
    target_frame,
    cut_times::text AS cut_times
FROM frl.frl_image_scene_boundaries
WHERE ABS((end_time - start_time) - 2.0) < 0.000001
  AND fps IS NOT NULL
  AND fps > 0
  AND target_frame IS NOT NULL
  AND cut_times IS NOT NULL
  AND (
        @filename IS NULL
        OR filename = @filename
      );
";

                await using (var cmd = new NpgsqlCommand(selectSql, _connection))
                {
                    cmd.Parameters.AddWithValue(
                        "@filename",
                        string.IsNullOrWhiteSpace(filename)
                            ? DBNull.Value
                            : filename.Trim()
                    );

                    await using var reader = await cmd.ExecuteReaderAsync(ct);

                    while (await reader.ReadAsync(ct))
                    {
                        records.Add(new SceneBoundaryRecord
                        {
                            MovieId = reader.GetInt32(reader.GetOrdinal("movieid")),
                            Filename = reader.GetString(reader.GetOrdinal("filename")),
                            StartTime = reader.GetDouble(reader.GetOrdinal("start_time")),
                            EndTime = reader.GetDouble(reader.GetOrdinal("end_time")),
                            Duration = reader.GetDouble(reader.GetOrdinal("duration")),
                            Fps = reader.GetDouble(reader.GetOrdinal("fps")),
                            FrameCount = reader.GetInt32(reader.GetOrdinal("frame_count")),
                            TargetFrame = reader.GetInt32(reader.GetOrdinal("target_frame")),
                            CutTimesJson = reader.GetString(reader.GetOrdinal("cut_times"))
                        });
                    }
                }

                await using var tx = await _connection.BeginTransactionAsync(ct);

                try
                {
                    foreach (var record in records)
                    {
                        List<double>? cutTimes;

                        try
                        {
                            cutTimes = JsonSerializer.Deserialize<List<double>>(record.CutTimesJson);
                        }
                        catch (Exception ex)
                        {
                            totalSkipped++;

                            AddResult(new FixSceneBoundaryResult
                            {
                                MovieId = record.MovieId,
                                Filename = record.Filename,
                                Updated = false,
                                Message = $"Invalid cut_times JSON: {ex.Message}"
                            });

                            continue;
                        }

                        if (cutTimes == null || cutTimes.Count == 0)
                        {
                            totalSkipped++;

                            AddResult(new FixSceneBoundaryResult
                            {
                                MovieId = record.MovieId,
                                Filename = record.Filename,
                                Updated = false,
                                Message = "No cut times found."
                            });

                            continue;
                        }

                        cutTimes.Sort();

                        var imageTime = record.TargetFrame / record.Fps;

                        var clipStart = 0.0;
                        var clipEnd = record.Duration;

                        foreach (var cut in cutTimes)
                        {
                            if (cut <= imageTime)
                            {
                                clipStart = cut;
                            }
                            else
                            {
                                clipEnd = cut;
                                break;
                            }
                        }

                        var newStartTime = Math.Max(0.0, clipStart - 2.0);
                        var newEndTime = Math.Min(record.Duration, clipEnd + 2.0);

                        var result = new FixSceneBoundaryResult
                        {
                            MovieId = record.MovieId,
                            Filename = record.Filename,

                            OldStartTime = record.StartTime,
                            OldEndTime = record.EndTime,

                            ImageTime = imageTime,
                            ClipStart = clipStart,
                            ClipEnd = clipEnd,

                            NewStartTime = newStartTime,
                            NewEndTime = newEndTime,

                            Updated = false,
                            RowsUpdated = 0,
                            Message = dryRun ? "Dry run only." : "Ready to update."
                        };

                        if (newEndTime <= newStartTime)
                        {
                            totalSkipped++;

                            result.Message = "Invalid calculated range.";
                            AddResult(result);

                            continue;
                        }

                        if (!dryRun)
                        {
                            const string updateSql = @"
UPDATE frl.frl_image_scene_boundaries
SET
    start_time = @start_time,
    end_time = @end_time
WHERE movieid = @movieid
  AND filename = @filename;
";

                            await using var updateCmd = new NpgsqlCommand(updateSql, _connection, tx);

                            updateCmd.Parameters.AddWithValue("@start_time", newStartTime);
                            updateCmd.Parameters.AddWithValue("@end_time", newEndTime);
                            updateCmd.Parameters.AddWithValue("@movieid", record.MovieId);
                            updateCmd.Parameters.AddWithValue("@filename", record.Filename);

                            var rowsUpdated = await updateCmd.ExecuteNonQueryAsync(ct);

                            result.Updated = rowsUpdated > 0;
                            result.RowsUpdated = rowsUpdated;
                            result.Message = rowsUpdated > 0 ? "Updated." : "No matching row updated.";

                            totalUpdated += rowsUpdated;
                        }

                        AddResult(result);
                    }

                    await tx.CommitAsync(ct);
                }
                catch
                {
                    await tx.RollbackAsync(ct);
                    throw;
                }

                return Ok(new FixSceneBoundaryResponse
                {
                    DryRun = dryRun,
                    Filename = string.IsNullOrWhiteSpace(filename) ? null : filename.Trim(),
                    TotalChecked = records.Count,
                    TotalUpdated = totalUpdated,
                    TotalSkipped = totalSkipped,
                    ReturnedResults = results.Count,
                    MaxResultsReturned = resultLimit,
                    Results = results
                });
            }
            finally
            {
                if (mustClose)
                {
                    await _connection.CloseAsync();
                }
            }
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

        private class SceneBoundaryRecord
        {
            public int MovieId { get; set; }
            public string Filename { get; set; } = "";
            public double StartTime { get; set; }
            public double EndTime { get; set; }
            public double Duration { get; set; }
            public double Fps { get; set; }
            public int FrameCount { get; set; }
            public int TargetFrame { get; set; }
            public string CutTimesJson { get; set; } = "";
        }

        public sealed class FixSceneBoundaryResponse
        {
            public bool DryRun { get; set; }

            public string? Filename { get; set; }

            public int TotalChecked { get; set; }
            public int TotalUpdated { get; set; }
            public int TotalSkipped { get; set; }

            public int ReturnedResults { get; set; }
            public int MaxResultsReturned { get; set; }

            public List<FixSceneBoundaryResult> Results { get; set; } = new();
        }

        public sealed class FixSceneBoundaryResult
        {
            public int MovieId { get; set; }
            public string Filename { get; set; } = "";

            public double? OldStartTime { get; set; }
            public double? OldEndTime { get; set; }

            public double? ImageTime { get; set; }
            public double? ClipStart { get; set; }
            public double? ClipEnd { get; set; }

            public double? NewStartTime { get; set; }
            public double? NewEndTime { get; set; }

            public bool Updated { get; set; }
            public int RowsUpdated { get; set; }
            public string Message { get; set; } = "";
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
