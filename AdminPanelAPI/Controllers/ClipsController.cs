using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using NpgsqlTypes;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
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
    [FromQuery] int batchSize = 5000,
    [FromQuery] string? filename = null,
    CancellationToken ct = default)
    {
        if (resultLimit < 0)
            resultLimit = 0;

        if (batchSize <= 0)
            batchSize = 5000;

        var trimmedFilename = string.IsNullOrWhiteSpace(filename)
            ? null
            : filename.Trim();

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

        var totalMatchingRecords = 0;
        var totalProcessed = 0;
        var totalUpdated = 0;
        var totalSkipped = 0;
        var totalFailed = 0;
        var batchNumber = 0;

        try
        {
            Console.WriteLine("--------------------------------------------------");
            Console.WriteLine("Starting FAST fix-two-second-boundaries job");
            Console.WriteLine($"Dry run: {dryRun}");
            Console.WriteLine($"Filename: {trimmedFilename ?? "ALL"}");
            Console.WriteLine($"Batch size: {batchSize:N0}");
            Console.WriteLine($"Result limit: {resultLimit:N0}");
            Console.WriteLine($"Started at: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            Console.WriteLine("--------------------------------------------------");

            const string countSql = @"
SELECT COUNT(*)
FROM frl.frl_image_scene_boundaries
WHERE ABS((end_time - start_time) - 2.0) < 0.000001
  AND fps IS NOT NULL
  AND fps > 0
  AND target_frame IS NOT NULL
  AND cut_times IS NOT NULL
  AND (
        @filename::text IS NULL
        OR filename = @filename::text
      );
";

            await using (var countCmd = new NpgsqlCommand(countSql, _connection))
            {
                countCmd.Parameters.Add(new NpgsqlParameter("@filename", NpgsqlDbType.Text)
                {
                    Value = trimmedFilename == null ? DBNull.Value : trimmedFilename
                });

                var countResult = await countCmd.ExecuteScalarAsync(ct);
                totalMatchingRecords = Convert.ToInt32(countResult);
            }

            Console.WriteLine($"Total matching records found: {totalMatchingRecords:N0}");

            if (totalMatchingRecords == 0)
            {
                Console.WriteLine("No matching records found. Nothing to process.");
                Console.WriteLine("--------------------------------------------------");

                return Ok(new FixSceneBoundaryResponse
                {
                    DryRun = dryRun,
                    Filename = trimmedFilename ?? "ALL",
                    TotalChecked = 0,
                    TotalUpdated = 0,
                    TotalSkipped = 0,
                    ReturnedResults = 0,
                    MaxResultsReturned = resultLimit,
                    Results = results
                });
            }

            if (dryRun)
            {
                Console.WriteLine("Dry run enabled. Fetching sample results only.");

                const string dryRunSql = @"
WITH candidates AS (
    SELECT
        movieid,
        filename,
        start_time,
        end_time,
        duration,
        fps,
        target_frame,
        cut_times
    FROM frl.frl_image_scene_boundaries
    WHERE ABS((end_time - start_time) - 2.0) < 0.000001
      AND fps IS NOT NULL
      AND fps > 0
      AND target_frame IS NOT NULL
      AND cut_times IS NOT NULL
      AND (
            @filename::text IS NULL
            OR filename = @filename::text
          )
    ORDER BY movieid, filename
    LIMIT @result_limit
),
computed AS (
    SELECT
        c.movieid,
        c.filename,
        c.start_time AS old_start_time,
        c.end_time AS old_end_time,
        c.duration,
        c.target_frame::double precision / c.fps AS image_time,

        COALESCE(
            (
                SELECT MAX(x.value::double precision)
                FROM jsonb_array_elements_text(c.cut_times::jsonb) AS x(value)
                WHERE x.value::double precision <= c.target_frame::double precision / c.fps
            ),
            0.0
        ) AS clip_start,

        COALESCE(
            (
                SELECT MIN(x.value::double precision)
                FROM jsonb_array_elements_text(c.cut_times::jsonb) AS x(value)
                WHERE x.value::double precision > c.target_frame::double precision / c.fps
            ),
            c.duration
        ) AS clip_end
    FROM candidates c
)
SELECT
    movieid,
    filename,
    old_start_time,
    old_end_time,
    image_time,
    clip_start,
    clip_end,
    GREATEST(0.0, clip_start - 2.0) AS new_start_time,
    LEAST(duration, clip_end + 2.0) AS new_end_time
FROM computed
ORDER BY movieid, filename;
";

                await using (var dryRunCmd = new NpgsqlCommand(dryRunSql, _connection))
                {
                    dryRunCmd.Parameters.Add(new NpgsqlParameter("@filename", NpgsqlDbType.Text)
                    {
                        Value = trimmedFilename == null ? DBNull.Value : trimmedFilename
                    });

                    dryRunCmd.Parameters.Add(new NpgsqlParameter("@result_limit", NpgsqlDbType.Integer)
                    {
                        Value = resultLimit
                    });

                    await using var reader = await dryRunCmd.ExecuteReaderAsync(ct);

                    while (await reader.ReadAsync(ct))
                    {
                        var newStartTime = reader.GetDouble(reader.GetOrdinal("new_start_time"));
                        var newEndTime = reader.GetDouble(reader.GetOrdinal("new_end_time"));

                        AddResult(new FixSceneBoundaryResult
                        {
                            MovieId = reader.GetInt32(reader.GetOrdinal("movieid")),
                            Filename = reader.GetString(reader.GetOrdinal("filename")),

                            OldStartTime = reader.GetDouble(reader.GetOrdinal("old_start_time")),
                            OldEndTime = reader.GetDouble(reader.GetOrdinal("old_end_time")),

                            ImageTime = reader.GetDouble(reader.GetOrdinal("image_time")),
                            ClipStart = reader.GetDouble(reader.GetOrdinal("clip_start")),
                            ClipEnd = reader.GetDouble(reader.GetOrdinal("clip_end")),

                            NewStartTime = newStartTime,
                            NewEndTime = newEndTime,

                            Updated = false,
                            RowsUpdated = 0,
                            Message = newEndTime > newStartTime
                                ? "Dry run only."
                                : "Invalid calculated range."
                        });
                    }
                }

                Console.WriteLine($"Dry run complete. Returned {results.Count:N0} sample results.");
                Console.WriteLine("--------------------------------------------------");

                return Ok(new FixSceneBoundaryResponse
                {
                    DryRun = true,
                    Filename = trimmedFilename ?? "ALL",
                    TotalChecked = totalMatchingRecords,
                    TotalUpdated = 0,
                    TotalSkipped = 0,
                    ReturnedResults = results.Count,
                    MaxResultsReturned = resultLimit,
                    Results = results
                });
            }

            const string updateBatchSql = @"
WITH candidates AS (
    SELECT
        ctid,
        movieid,
        filename,
        start_time,
        end_time,
        duration,
        fps,
        target_frame,
        cut_times
    FROM frl.frl_image_scene_boundaries
    WHERE ABS((end_time - start_time) - 2.0) < 0.000001
      AND fps IS NOT NULL
      AND fps > 0
      AND target_frame IS NOT NULL
      AND cut_times IS NOT NULL
      AND (
            @filename::text IS NULL
            OR filename = @filename::text
          )
    ORDER BY movieid, filename
    LIMIT @batch_size
    FOR UPDATE SKIP LOCKED
),
computed AS (
    SELECT
        c.ctid,

        GREATEST(
            0.0,
            COALESCE(
                (
                    SELECT MAX(x.value::double precision)
                    FROM jsonb_array_elements_text(c.cut_times::jsonb) AS x(value)
                    WHERE x.value::double precision <= c.target_frame::double precision / c.fps
                ),
                0.0
            ) - 2.0
        ) AS new_start_time,

        LEAST(
            c.duration,
            COALESCE(
                (
                    SELECT MIN(x.value::double precision)
                    FROM jsonb_array_elements_text(c.cut_times::jsonb) AS x(value)
                    WHERE x.value::double precision > c.target_frame::double precision / c.fps
                ),
                c.duration
            ) + 2.0
        ) AS new_end_time
    FROM candidates c
),
valid_updates AS (
    SELECT
        ctid,
        new_start_time,
        new_end_time
    FROM computed
    WHERE new_end_time > new_start_time
)
UPDATE frl.frl_image_scene_boundaries t
SET
    start_time = v.new_start_time,
    end_time = v.new_end_time
FROM valid_updates v
WHERE t.ctid = v.ctid;
";

            while (true)
            {
                batchNumber++;

                var batchUpdated = 0;

                Console.WriteLine("");
                Console.WriteLine($"Batch {batchNumber:N0} starting...");

                await using var tx = await _connection.BeginTransactionAsync(ct);

                try
                {
                    await using var updateCmd = new NpgsqlCommand(updateBatchSql, _connection, tx);

                    updateCmd.Parameters.Add(new NpgsqlParameter("@filename", NpgsqlDbType.Text)
                    {
                        Value = trimmedFilename == null ? DBNull.Value : trimmedFilename
                    });

                    updateCmd.Parameters.Add(new NpgsqlParameter("@batch_size", NpgsqlDbType.Integer)
                    {
                        Value = batchSize
                    });

                    batchUpdated = await updateCmd.ExecuteNonQueryAsync(ct);

                    await tx.CommitAsync(ct);

                    totalUpdated += batchUpdated;
                    totalProcessed += batchUpdated;

                    var left = Math.Max(0, totalMatchingRecords - totalProcessed);

                    Console.WriteLine($"Batch {batchNumber:N0} committed.");
                    Console.WriteLine(
                        $"Processed: {totalProcessed:N0} / {totalMatchingRecords:N0} | " +
                        $"Left: {left:N0} | " +
                        $"Updated: {totalUpdated:N0} | " +
                        $"Skipped: {totalSkipped:N0} | " +
                        $"Failed: {totalFailed:N0}"
                    );
                }
                catch (Exception ex)
                {
                    await tx.RollbackAsync(ct);

                    totalFailed += batchSize;

                    Console.WriteLine($"Batch {batchNumber:N0} failed and rolled back.");
                    Console.WriteLine($"Error: {ex.Message}");

                    throw;
                }

                if (batchUpdated == 0)
                {
                    Console.WriteLine("No more rows updated. Exiting batch loop.");
                    break;
                }

                if (trimmedFilename != null)
                {
                    Console.WriteLine("Filename was supplied, so stopping after this batch.");
                    break;
                }
            }

            totalSkipped = Math.Max(0, totalMatchingRecords - totalUpdated);

            Console.WriteLine("");
            Console.WriteLine("--------------------------------------------------");
            Console.WriteLine("Finished FAST fix-two-second-boundaries job");
            Console.WriteLine($"Dry run: {dryRun}");
            Console.WriteLine($"Filename: {trimmedFilename ?? "ALL"}");
            Console.WriteLine($"Total matching records at start: {totalMatchingRecords:N0}");
            Console.WriteLine($"Total processed: {totalProcessed:N0}");
            Console.WriteLine($"Total updated: {totalUpdated:N0}");
            Console.WriteLine($"Total skipped/not updated: {totalSkipped:N0}");
            Console.WriteLine($"Total failed: {totalFailed:N0}");
            Console.WriteLine($"Finished at: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            Console.WriteLine("--------------------------------------------------");

            return Ok(new FixSceneBoundaryResponse
            {
                DryRun = false,
                Filename = trimmedFilename ?? "ALL",
                TotalChecked = totalProcessed,
                TotalUpdated = totalUpdated,
                TotalSkipped = totalSkipped + totalFailed,
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




        [HttpPost("/api/admin/clips/fix-affected-boundary-records-fast")]
        [ProducesResponseType(typeof(FixSceneBoundaryResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<FixSceneBoundaryResponse>> FixAffectedBoundaryRecordsFastAsync(
    [FromQuery] bool dryRun = true,
    [FromQuery] int resultLimit = 100,
    [FromQuery] string? filename = null,
    [FromQuery] int batchSize = 5000,
    CancellationToken ct = default)
        {
            if (resultLimit < 0)
                resultLimit = 0;

            if (batchSize < 100)
                batchSize = 100;

            if (batchSize > 50000)
                batchSize = 50000;

            var mustClose = false;

            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            var results = new List<FixSceneBoundaryResult>();

            void AddResult(FixSceneBoundaryResult result)
            {
                if (results.Count < resultLimit)
                {
                    results.Add(result);
                }
            }

            var totalChecked = 0;
            var totalUpdated = 0;
            var totalSkipped = 0;
            var totalBoundaryHits = 0;
            var totalBatches = 0;

            var filenameFilter = string.IsNullOrWhiteSpace(filename)
                ? null
                : filename.Trim();

            Console.WriteLine("====================================================");
            Console.WriteLine("Starting targeted boundary fix");
            Console.WriteLine($"DryRun: {dryRun}");
            Console.WriteLine($"Filename: {filenameFilter ?? "[ALL]"}");
            Console.WriteLine($"BatchSize: {batchSize}");
            Console.WriteLine($"ResultLimit: {resultLimit}");
            Console.WriteLine("Only reading rows where current start_time ~= image_time - 2");
            Console.WriteLine("====================================================");

            try
            {
                while (true)
                {
                    ct.ThrowIfCancellationRequested();

                    totalBatches++;

                    var batchRecords = new List<SceneBoundaryRecord>(batchSize);

                    const string selectSql = @"
SELECT
    b.ctid::text AS row_ctid,
    b.movieid,
    b.filename,
    b.start_time,
    b.end_time,
    b.duration,
    b.fps,
    b.frame_count,
    b.target_frame,
    b.cut_times::text AS cut_times
FROM frl.frl_image_scene_boundaries b
WHERE b.fps IS NOT NULL
  AND b.fps > 0
  AND b.target_frame IS NOT NULL
  AND b.cut_times IS NOT NULL
  AND (
        @filename::text IS NULL
        OR b.filename = @filename::text
      )
  AND ABS(
        b.start_time -
        GREATEST(0.0, (b.target_frame::double precision / b.fps) - 2.0)
      ) < 0.000001
ORDER BY b.movieid, b.filename
LIMIT @batchSize;
";

                    await using (var selectCmd = new NpgsqlCommand(selectSql, _connection))
                    {
                        var filenameParam = selectCmd.Parameters.Add("@filename", NpgsqlTypes.NpgsqlDbType.Text);
                        filenameParam.Value = filenameFilter == null
                            ? DBNull.Value
                            : filenameFilter;

                        selectCmd.Parameters.Add("@batchSize", NpgsqlTypes.NpgsqlDbType.Integer).Value = batchSize;

                        await using var reader = await selectCmd.ExecuteReaderAsync(ct);

                        while (await reader.ReadAsync(ct))
                        {
                            batchRecords.Add(new SceneBoundaryRecord
                            {
                                RowCtid = reader.GetString(reader.GetOrdinal("row_ctid")),
                                MovieId = reader.GetInt32(reader.GetOrdinal("movieid")),
                                Filename = reader.GetString(reader.GetOrdinal("filename")),
                                StartTime = reader.GetDouble(reader.GetOrdinal("start_time")),
                                EndTime = reader.GetDouble(reader.GetOrdinal("end_time")),
                                Duration = reader.GetDouble(reader.GetOrdinal("duration")),
                                Fps = reader.GetDouble(reader.GetOrdinal("fps")),
                                FrameCount = reader.IsDBNull(reader.GetOrdinal("frame_count"))
                                    ? 0
                                    : reader.GetInt32(reader.GetOrdinal("frame_count")),
                                TargetFrame = reader.GetInt32(reader.GetOrdinal("target_frame")),
                                CutTimesJson = reader.GetString(reader.GetOrdinal("cut_times"))
                            });
                        }
                    }

                    if (batchRecords.Count == 0)
                    {
                        Console.WriteLine("No more targeted candidate records found.");
                        break;
                    }

                    Console.WriteLine(
                        $"Batch {totalBatches} loaded: {batchRecords.Count:N0} targeted candidates | " +
                        $"Elapsed={stopwatch.Elapsed}"
                    );

                    if (dryRun)
                    {
                        Console.WriteLine("Dry run mode: processing one batch only so it does not loop forever.");
                    }

                    await using var tx = await _connection.BeginTransactionAsync(ct);

                    try
                    {
                        await using var updateCmd = new NpgsqlCommand(@"
UPDATE frl.frl_image_scene_boundaries
SET
    start_time = @start_time,
    end_time = @end_time
WHERE ctid = @row_ctid::tid;
", _connection, tx);

                        updateCmd.Parameters.Add("@start_time", NpgsqlTypes.NpgsqlDbType.Double);
                        updateCmd.Parameters.Add("@end_time", NpgsqlTypes.NpgsqlDbType.Double);
                        updateCmd.Parameters.Add("@row_ctid", NpgsqlTypes.NpgsqlDbType.Text);

                        foreach (var record in batchRecords)
                        {
                            totalChecked++;

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

                            var imageTime = record.TargetFrame / record.Fps;

                            var imageIsOnAnyCut = cutTimes.Any(cut =>
                                Math.Abs(cut - imageTime) <= 0.000001
                            );

                            if (!imageIsOnAnyCut)
                            {
                                totalSkipped++;

                                AddResult(new FixSceneBoundaryResult
                                {
                                    MovieId = record.MovieId,
                                    Filename = record.Filename,
                                    OldStartTime = record.StartTime,
                                    OldEndTime = record.EndTime,
                                    ImageTime = imageTime,
                                    Updated = false,
                                    Message = "Candidate matched start_time pattern, but image was not on a cut."
                                });

                                continue;
                            }

                            totalBoundaryHits++;

                            var foundSection = TryFindContainingCutSectionPreviousCut(
                                cutTimes,
                                imageTime,
                                record.Duration,
                                out var clipStart,
                                out var clipEnd,
                                out var imageIsOnCut
                            );

                            if (!foundSection)
                            {
                                totalSkipped++;

                                AddResult(new FixSceneBoundaryResult
                                {
                                    MovieId = record.MovieId,
                                    Filename = record.Filename,
                                    OldStartTime = record.StartTime,
                                    OldEndTime = record.EndTime,
                                    ImageTime = imageTime,
                                    Updated = false,
                                    Message = "Could not find containing cut section."
                                });

                                continue;
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
                                Message = dryRun
                                    ? "Dry run only. Previous cut section selected."
                                    : "Ready to update. Previous cut section selected."
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
                                updateCmd.Parameters["@start_time"].Value = newStartTime;
                                updateCmd.Parameters["@end_time"].Value = newEndTime;
                                updateCmd.Parameters["@row_ctid"].Value = record.RowCtid;

                                var rowsUpdated = await updateCmd.ExecuteNonQueryAsync(ct);

                                result.Updated = rowsUpdated > 0;
                                result.RowsUpdated = rowsUpdated;
                                result.Message = rowsUpdated > 0
                                    ? "Updated. Previous cut section selected."
                                    : "No matching row updated.";

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

                    var seconds = Math.Max(1, stopwatch.Elapsed.TotalSeconds);
                    var rate = totalChecked / seconds;

                    Console.WriteLine(
                        $"Batch {totalBatches} complete | " +
                        $"Checked={totalChecked:N0} | " +
                        $"BoundaryHits={totalBoundaryHits:N0} | " +
                        $"Updated={totalUpdated:N0} | " +
                        $"Skipped={totalSkipped:N0} | " +
                        $"Rate={rate:N0}/sec | " +
                        $"Elapsed={stopwatch.Elapsed}"
                    );

                    if (dryRun || filenameFilter != null)
                    {
                        Console.WriteLine("Stopping after one targeted pass because dryRun or filename mode is enabled.");
                        break;
                    }
                }

                stopwatch.Stop();

                Console.WriteLine("====================================================");
                Console.WriteLine("Targeted boundary fix complete");
                Console.WriteLine($"DryRun: {dryRun}");
                Console.WriteLine($"Filename: {filenameFilter ?? "[ALL]"}");
                Console.WriteLine($"TotalChecked: {totalChecked:N0}");
                Console.WriteLine($"TotalBoundaryHits: {totalBoundaryHits:N0}");
                Console.WriteLine($"TotalUpdated: {totalUpdated:N0}");
                Console.WriteLine($"TotalSkipped: {totalSkipped:N0}");
                Console.WriteLine($"Elapsed: {stopwatch.Elapsed}");
                Console.WriteLine("====================================================");

                return Ok(new FixSceneBoundaryResponse
                {
                    DryRun = dryRun,
                    Filename = filenameFilter,
                    TotalChecked = totalChecked,
                    TotalUpdated = totalUpdated,
                    TotalSkipped = totalSkipped,
                    TotalBoundaryHits = totalBoundaryHits,
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

        private static bool TryFindContainingCutSectionPreviousCut(
    List<double> originalCutTimes,
    double imageTime,
    double videoDuration,
    out double clipStart,
    out double clipEnd,
    out bool imageIsOnCut)
        {
            const double Tolerance = 0.000001;

            clipStart = 0.0;
            clipEnd = videoDuration;
            imageIsOnCut = false;

            if (originalCutTimes == null || originalCutTimes.Count == 0)
                return false;

            var cutTimes = originalCutTimes
                .Where(x => !double.IsNaN(x) && !double.IsInfinity(x))
                .Distinct()
                .OrderBy(x => x)
                .ToList();

            if (cutTimes.Count == 0)
                return false;

            if (Math.Abs(cutTimes[0]) > Tolerance)
            {
                cutTimes.Insert(0, 0.0);
            }

            if (Math.Abs(cutTimes[^1] - videoDuration) > Tolerance)
            {
                cutTimes.Add(videoDuration);
            }

            for (var i = 0; i < cutTimes.Count - 1; i++)
            {
                var sectionStart = cutTimes[i];
                var sectionEnd = cutTimes[i + 1];

                if (sectionEnd <= sectionStart)
                    continue;

                var isInsideSection =
                    imageTime > sectionStart + Tolerance &&
                    imageTime < sectionEnd - Tolerance;

                var isOnSectionEnd =
                    Math.Abs(imageTime - sectionEnd) <= Tolerance;

                if (isInsideSection || isOnSectionEnd)
                {
                    clipStart = sectionStart;
                    clipEnd = sectionEnd;
                    imageIsOnCut = isOnSectionEnd;
                    return true;
                }
            }

            if (Math.Abs(imageTime - cutTimes[0]) <= Tolerance && cutTimes.Count >= 2)
            {
                clipStart = cutTimes[0];
                clipEnd = cutTimes[1];
                imageIsOnCut = true;
                return true;
            }

            return false;
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

        private sealed class SceneBoundaryRecord
        {
            public string RowCtid { get; set; } = "";

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
            public int TotalBoundaryHits { get; set; }

            public int ReturnedResults { get; set; }
            public int MaxResultsReturned { get; set; }

            public List<FixSceneBoundaryResult> Results { get; set; } = new();
        }

        public class FixSceneBoundaryResult
        {
            public int MovieId { get; set; }
            public string? Filename { get; set; }

            public double? OldStartTime { get; set; }
            public double? OldEndTime { get; set; }

            public double? CurrentStartTime { get; set; }
            public double? CurrentEndTime { get; set; }

            public double? ImageTime { get; set; }
            public double? CutTime { get; set; }

            public double? ClipStart { get; set; }
            public double? ClipEnd { get; set; }

            public double? PreviousShotStart { get; set; }
            public double? PreviousShotEnd { get; set; }
            public double? PreviousShotLength { get; set; }

            public double? NextShotStart { get; set; }
            public double? NextShotEnd { get; set; }
            public double? NextShotLength { get; set; }

            public double? NewStartTime { get; set; }
            public double? NewEndTime { get; set; }

            public bool Updated { get; set; }
            public bool WouldUpdate { get; set; }
            public int RowsUpdated { get; set; }

            public string? Decision { get; set; }
            public string? Message { get; set; }
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
