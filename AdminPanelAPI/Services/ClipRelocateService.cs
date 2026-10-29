using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using AdminPanelAPI.Interfaces;
using AdminPanelAPI.Models;
using Npgsql;
using NpgsqlTypes;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Relocate-clips jobs: Modal (clip-relocate app) checks every clip of a movie
    /// against its still, finds the real frame of any still that isn't at the
    /// clip's centre by searching the whole film, and recuts those clips. With
    /// apply, this service then replaces their scene boundaries (backing up the
    /// old rows, short shots widened ±2s), recuts all 3 preview sizes and, with
    /// writeHistory, records the found frame in frl_imagehistory.
    /// </summary>
    public class ClipRelocateService
    {
        private const string HistoryAction = "Shot Time Autodetected";
        private const double SceneThreshold = 0.7;

        private readonly string _connectionString;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IClipPreviewService _clipPreviews;
        private readonly ILogger<ClipRelocateService> _logger;
        private readonly string _startUrl;
        private readonly string _resultUrl;
        private readonly string _sceneDetectionBaseUrl;

        private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

        public ClipRelocateService(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            IClipPreviewService clipPreviews,
            ILogger<ClipRelocateService> logger)
        {
            _connectionString = configuration.GetConnectionString("Default")
                ?? throw new InvalidOperationException("Missing connection string: Default");
            _httpClientFactory = httpClientFactory;
            _clipPreviews = clipPreviews;
            _logger = logger;
            _startUrl = configuration["MoviePipeline:ClipRelocateStartUrl"]
                        ?? "https://semanticsearch--clip-relocate-start.us-east.modal.run";
            _resultUrl = configuration["MoviePipeline:ClipRelocateResultUrl"]
                         ?? "https://semanticsearch--clip-relocate-result.us-east.modal.run";
            _sceneDetectionBaseUrl = configuration["MoviePipeline:SceneDetectionBaseUrl"]
                                     ?? "https://semanticsearch--r2-transnet-v2.modal.run";
        }

        private async Task<NpgsqlConnection> OpenAsync(CancellationToken ct)
        {
            var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);
            return conn;
        }

        public async Task EnsureTablesAsync(CancellationToken ct)
        {
            const string sql = @"
CREATE TABLE IF NOT EXISTS frl.frl_clip_relocate_jobs (
    id             BIGSERIAL    PRIMARY KEY,
    movie_id       INTEGER      NOT NULL,
    apply          BOOLEAN      NOT NULL DEFAULT FALSE,
    write_history  BOOLEAN      NOT NULL DEFAULT FALSE,
    status         VARCHAR(20)  NOT NULL DEFAULT 'queued',
    modal_call_id  TEXT,
    phase          TEXT,
    progress       JSONB,
    images         INTEGER      NOT NULL DEFAULT 0,
    flagged        INTEGER      NOT NULL DEFAULT 0,
    relocated      INTEGER      NOT NULL DEFAULT 0,
    not_found      INTEGER      NOT NULL DEFAULT 0,
    failed         INTEGER      NOT NULL DEFAULT 0,
    report         JSONB,
    error          TEXT,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    started_at     TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_crj_status ON frl.frl_clip_relocate_jobs (status);
CREATE TABLE IF NOT EXISTS frl.frl_clip_relocate_boundary_backup AS
SELECT 0::BIGINT AS job_id, now() AS backed_up_at, b.*
FROM frl.frl_image_scene_boundaries b
WITH NO DATA;";
            await using var conn = await OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(sql, conn);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task<long> CreateJobAsync(int movieId, bool apply, bool writeHistory, CancellationToken ct)
        {
            await EnsureTablesAsync(ct);
            await using var conn = await OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(@"
INSERT INTO frl.frl_clip_relocate_jobs (movie_id, apply, write_history)
VALUES (@m, @a, @w) RETURNING id;", conn);
            cmd.Parameters.AddWithValue("m", movieId);
            cmd.Parameters.AddWithValue("a", apply);
            cmd.Parameters.AddWithValue("w", writeHistory);
            return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
        }

        private const string JobColumns = @"id, movie_id, apply, write_history, status, modal_call_id, phase,
progress::text, images, flagged, relocated, not_found, failed, report::text, error, created_at, started_at, updated_at, finished_at";

        private static ClipRelocateJob ReadJob(NpgsqlDataReader r)
        {
            static JsonElement? J(NpgsqlDataReader r, int i) =>
                r.IsDBNull(i) ? null : JsonDocument.Parse(r.GetString(i)).RootElement.Clone();
            return new ClipRelocateJob
            {
                Id = r.GetInt64(0),
                MovieId = r.GetInt32(1),
                Apply = r.GetBoolean(2),
                WriteHistory = r.GetBoolean(3),
                Status = r.GetString(4),
                ModalCallId = r.IsDBNull(5) ? null : r.GetString(5),
                Phase = r.IsDBNull(6) ? null : r.GetString(6),
                Progress = J(r, 7),
                Images = r.GetInt32(8),
                Flagged = r.GetInt32(9),
                Relocated = r.GetInt32(10),
                NotFound = r.GetInt32(11),
                Failed = r.GetInt32(12),
                Report = J(r, 13),
                Error = r.IsDBNull(14) ? null : r.GetString(14),
                CreatedAt = r.GetDateTime(15),
                StartedAt = r.IsDBNull(16) ? null : r.GetDateTime(16),
                UpdatedAt = r.GetDateTime(17),
                FinishedAt = r.IsDBNull(18) ? null : r.GetDateTime(18),
            };
        }

        public async Task<ClipRelocateJob?> GetJobAsync(long jobId, CancellationToken ct)
        {
            await EnsureTablesAsync(ct);
            await using var conn = await OpenAsync(ct);
            await using var cmd = new NpgsqlCommand($"SELECT {JobColumns} FROM frl.frl_clip_relocate_jobs WHERE id = @id", conn);
            cmd.Parameters.AddWithValue("id", jobId);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            return await r.ReadAsync(ct) ? ReadJob(r) : null;
        }

        /// <summary>Recent jobs, newest first, without their reports.</summary>
        public async Task<List<ClipRelocateJob>> ListJobsAsync(int? movieId, int limit, CancellationToken ct)
        {
            await EnsureTablesAsync(ct);
            await using var conn = await OpenAsync(ct);
            await using var cmd = new NpgsqlCommand($@"
SELECT {JobColumns.Replace("report::text", "NULL::text")} FROM frl.frl_clip_relocate_jobs
WHERE @m::int IS NULL OR movie_id = @m::int ORDER BY id DESC LIMIT @l", conn);
            cmd.Parameters.Add(new NpgsqlParameter("m", NpgsqlDbType.Integer) { Value = (object?)movieId ?? DBNull.Value });
            cmd.Parameters.AddWithValue("l", Math.Clamp(limit, 1, 500));
            var jobs = new List<ClipRelocateJob>();
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct)) jobs.Add(ReadJob(r));
            return jobs;
        }

        /// <summary>
        /// The job to work on: one already started (so a restart resumes it), else
        /// the oldest queued one.
        /// </summary>
        public async Task<ClipRelocateJob?> NextJobAsync(CancellationToken ct)
        {
            await using var conn = await OpenAsync(ct);
            await using var cmd = new NpgsqlCommand($@"
SELECT {JobColumns} FROM frl.frl_clip_relocate_jobs
WHERE status IN ('queued', 'running', 'finishing')
ORDER BY CASE WHEN status = 'queued' THEN 1 ELSE 0 END, id LIMIT 1", conn);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            return await r.ReadAsync(ct) ? ReadJob(r) : null;
        }

        private async Task UpdateJobAsync(long jobId, string setSql, Action<NpgsqlParameterCollection> bind, CancellationToken ct)
        {
            await using var conn = await OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(
                $"UPDATE frl.frl_clip_relocate_jobs SET {setSql}, updated_at = now() WHERE id = @id", conn);
            cmd.Parameters.AddWithValue("id", jobId);
            bind(cmd.Parameters);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public Task FailJobAsync(long jobId, string error, CancellationToken ct) =>
            UpdateJobAsync(jobId, "status = 'failed', error = @e, finished_at = now()",
                p => p.AddWithValue("e", error.Length > 4000 ? error[..4000] : error), ct);

        /// <summary>Every live image of the movie with its latest autodetected frame, if any.</summary>
        public async Task<(List<ClipRelocateImage> Images, double? Fps)> GetImagesAsync(int movieId, CancellationToken ct)
        {
            await using var conn = await OpenAsync(ct);
            var images = new List<ClipRelocateImage>();
            await using (var cmd = new NpgsqlCommand(@"
SELECT i.idnum, split_part(i.filename, '.', 1),
       (SELECT substring(h.notes FROM 'Frame #([0-9]+)')::int
        FROM frl.frl_imagehistory h
        WHERE h.imageid = i.idnum AND h.action = @action AND h.notes ~ 'Frame #[0-9]+'
        ORDER BY h.timestamp DESC NULLS LAST, h.idnum DESC LIMIT 1)
FROM frl.frl_images i
WHERE i.movieid = @m AND i.status::text = 'live'
ORDER BY i.idnum", conn))
            {
                cmd.Parameters.AddWithValue("m", movieId);
                cmd.Parameters.AddWithValue("action", HistoryAction);
                await using var r = await cmd.ExecuteReaderAsync(ct);
                while (await r.ReadAsync(ct))
                    images.Add(new ClipRelocateImage
                    {
                        ImageId = r.GetInt32(0),
                        Rid = r.GetString(1),
                        Frame = r.IsDBNull(2) ? null : r.GetInt32(2),
                    });
            }

            await using var fpsCmd = new NpgsqlCommand("SELECT fps::float8 FROM frl.frl_movie_info WHERE movieid = @m LIMIT 1", conn);
            fpsCmd.Parameters.AddWithValue("m", movieId);
            var fps = await fpsCmd.ExecuteScalarAsync(ct);
            return (images, fps is double d && d > 0 ? d : null);
        }

        /// <summary>Hands a queued job to Modal.</summary>
        public async Task StartAsync(ClipRelocateJob job, CancellationToken ct)
        {
            var (images, fps) = await GetImagesAsync(job.MovieId, ct);
            if (images.Count == 0)
                throw new InvalidOperationException($"Movie {job.MovieId} has no live images.");
            if (fps == null)
                throw new InvalidOperationException($"Movie {job.MovieId} has no fps in frl_movie_info.");

            var body = JsonSerializer.Serialize(new
            {
                movie_id = job.MovieId,
                fps,
                apply = job.Apply,
                job_tag = $"job{job.Id}",
                images,
            });

            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromMinutes(5);
            using var response = await client.PostAsync(_startUrl, new StringContent(body, Encoding.UTF8, "application/json"), ct);
            var content = await response.Content.ReadAsStringAsync(ct);
            if (!response.IsSuccessStatusCode)
                throw new Exception($"clip-relocate start failed. Status: {(int)response.StatusCode}, Body: {content}");
            var callId = JsonDocument.Parse(content).RootElement.GetProperty("call_id").GetString()
                         ?? throw new Exception("clip-relocate start returned no call_id.");

            await UpdateJobAsync(job.Id,
                "status = 'running', modal_call_id = @c, phase = 'queued', images = @n, started_at = now(), error = NULL",
                p => { p.AddWithValue("c", callId); p.AddWithValue("n", images.Count); }, ct);
        }

        public async Task<ClipRelocatePoll> PollAsync(ClipRelocateJob job, CancellationToken ct)
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromMinutes(5);
            var url = $"{_resultUrl}?call_id={Uri.EscapeDataString(job.ModalCallId!)}&job_tag=job{job.Id}";
            var content = await client.GetStringAsync(url, ct);
            var poll = JsonSerializer.Deserialize<ClipRelocatePoll>(content, Json)
                       ?? throw new Exception("clip-relocate result returned invalid JSON.");

            if (poll.Status == "running")
            {
                var phase = poll.Progress is { ValueKind: JsonValueKind.Object } pr && pr.TryGetProperty("phase", out var ph)
                    ? ph.GetString() : null;
                await UpdateJobAsync(job.Id, "phase = @ph, progress = @pr::jsonb", p =>
                {
                    p.AddWithValue("ph", (object?)phase ?? DBNull.Value);
                    p.AddWithValue("pr", poll.Progress?.GetRawText() ?? "{}");
                }, ct);
            }
            return poll;
        }

        /// <summary>
        /// Stores Modal's report and, for an applied job, replaces the relocated
        /// clips' scene boundaries, cuts their previews and writes history. Safe to
        /// rerun after a restart.
        /// </summary>
        public async Task FinishAsync(ClipRelocateJob job, ClipRelocateModalResult result, CancellationToken ct)
        {
            int Count(string s) => result.Images.Count(i => i.Status == s);
            var reportJson = JsonSerializer.Serialize(result);
            await UpdateJobAsync(job.Id,
                "status = 'finishing', phase = @ph, report = @r::jsonb, flagged = @f, relocated = @rl, not_found = @nf, failed = @fl",
                p =>
                {
                    p.AddWithValue("ph", job.Apply ? "writing boundaries" : "done");
                    p.AddWithValue("r", reportJson);
                    p.AddWithValue("f", result.Images.Count(i => i.Status is "relocated" or "not_found" or "failed"));
                    p.AddWithValue("rl", Count("relocated"));
                    p.AddWithValue("nf", Count("not_found"));
                    p.AddWithValue("fl", Count("failed"));
                }, ct);

            var relocated = result.Images.Where(i => i.Status == "relocated").ToList();
            var errors = new List<string>();

            if (job.Apply && relocated.Count > 0)
            {
                var (images, _) = await GetImagesAsync(job.MovieId, ct);
                var imageIds = images.ToDictionary(i => i.Rid, i => i.ImageId);
                var boundaries = new List<ClipPreviewBoundary>();

                foreach (var img in relocated)
                {
                    try
                    {
                        var b = await DetectSceneBoundaryAsync(job.MovieId, img.Rid, ct);
                        WidenShortShot(b);
                        await ReplaceBoundaryAsync(job.Id, b, ct);
                        if (b.StartTime is double s && b.EndTime is double e)
                            boundaries.Add(new ClipPreviewBoundary
                            {
                                MovieId = job.MovieId,
                                Filename = b.Filename ?? img.Rid,
                                StartTime = s,
                                EndTime = e,
                                ImageId = imageIds.GetValueOrDefault(img.Rid),
                            });
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogWarning(ex, "Relocate job {JobId}: boundary for {Rid} failed.", job.Id, img.Rid);
                        errors.Add($"boundary {img.Rid}: {ex.Message}");
                    }
                }

                await UpdateJobAsync(job.Id, "phase = 'cutting previews'", _ => { }, ct);
                var previews = await _clipPreviews.GeneratePreviewsAsync(job.MovieId, boundaries, overwrite: true, ct);
                if (previews.Errors > 0)
                    errors.Add($"{previews.Errors} preview(s) failed");

                if (job.WriteHistory)
                    await WriteHistoryAsync(job.Id, relocated, imageIds, ct);
            }

            await UpdateJobAsync(job.Id, "status = 'done', phase = 'done', error = @e, finished_at = now()",
                p => p.AddWithValue("e", errors.Count == 0 ? DBNull.Value : string.Join("; ", errors)), ct);
        }

        private async Task<SceneBoundaryResponse> DetectSceneBoundaryAsync(int movieId, string rid, CancellationToken ct)
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromMinutes(15);
            using var request = new HttpRequestMessage(HttpMethod.Post, $"{_sceneDetectionBaseUrl.TrimEnd('/')}/");
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            request.Content = new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["movie_id"] = movieId.ToString(CultureInfo.InvariantCulture),
                ["filename"] = rid,
                ["threshold"] = SceneThreshold.ToString(CultureInfo.InvariantCulture),
            });
            using var response = await client.SendAsync(request, ct);
            var content = await response.Content.ReadAsStringAsync(ct);
            if (!response.IsSuccessStatusCode)
                throw new Exception($"Scene detection failed. Status: {(int)response.StatusCode}, Body: {content}");
            return JsonSerializer.Deserialize<SceneBoundaryResponse>(content, Json)
                   ?? throw new Exception("Scene detection returned invalid JSON.");
        }

        /// <summary>
        /// Same rule as fix-two-second-boundaries: a shot under 2s comes back padded
        /// to exactly 2s, so play from 2s before its first cut to 2s after its last.
        /// </summary>
        public static void WidenShortShot(SceneBoundaryResponse b)
        {
            if (b.StartTime is not double start || b.EndTime is not double end ||
                Math.Abs(end - start - 2.0) >= 0.000001 ||
                b.Fps is not double fps || fps <= 0 || b.TargetFrame is not int target ||
                b.Duration is not double duration)
                return;

            var imageTime = target / fps;
            var cuts = b.CutTimes ?? new List<double>();
            var shotStart = cuts.Where(c => c <= imageTime).DefaultIfEmpty(0.0).Max();
            var shotEnd = cuts.Where(c => c > imageTime).DefaultIfEmpty(duration).Min();
            b.StartTime = Math.Max(0.0, shotStart - 2.0);
            b.EndTime = Math.Min(duration, shotEnd + 2.0);
        }

        private async Task ReplaceBoundaryAsync(long jobId, SceneBoundaryResponse b, CancellationToken ct)
        {
            await using var conn = await OpenAsync(ct);
            await using var tx = await conn.BeginTransactionAsync(ct);

            await using (var backup = new NpgsqlCommand(@"
INSERT INTO frl.frl_clip_relocate_boundary_backup
SELECT @job, now(), b.* FROM frl.frl_image_scene_boundaries b
WHERE b.movieid = @m AND b.filename = @f
  AND NOT EXISTS (SELECT 1 FROM frl.frl_clip_relocate_boundary_backup k
                  WHERE k.job_id = @job AND k.movieid = @m AND k.filename = @f);
DELETE FROM frl.frl_image_scene_boundaries WHERE movieid = @m AND filename = @f;", conn, tx))
            {
                backup.Parameters.AddWithValue("job", jobId);
                backup.Parameters.AddWithValue("m", b.MovieId);
                backup.Parameters.AddWithValue("f", b.Filename ?? "");
                await backup.ExecuteNonQueryAsync(ct);
            }

            await using (var insert = new NpgsqlCommand(@"
INSERT INTO frl.frl_image_scene_boundaries
    (movieid, filename, start_time, end_time, cut_times, duration, fps, frame_count, target_frame, threshold)
VALUES (@movieid, @filename, @start_time, @end_time, @cut_times, @duration, @fps, @frame_count, @target_frame, @threshold);",
                conn, tx))
            {
                insert.Parameters.AddWithValue("movieid", b.MovieId);
                insert.Parameters.AddWithValue("filename", (object?)b.Filename ?? DBNull.Value);
                insert.Parameters.AddWithValue("start_time", (object?)b.StartTime ?? DBNull.Value);
                insert.Parameters.AddWithValue("end_time", (object?)b.EndTime ?? DBNull.Value);
                insert.Parameters.AddWithValue("cut_times", NpgsqlDbType.Jsonb, JsonSerializer.Serialize(b.CutTimes ?? new List<double>()));
                insert.Parameters.AddWithValue("duration", (object?)b.Duration ?? DBNull.Value);
                insert.Parameters.AddWithValue("fps", (object?)b.Fps ?? DBNull.Value);
                insert.Parameters.AddWithValue("frame_count", (object?)b.FrameCount ?? DBNull.Value);
                insert.Parameters.AddWithValue("target_frame", (object?)b.TargetFrame ?? DBNull.Value);
                insert.Parameters.AddWithValue("threshold", (object?)b.Threshold ?? DBNull.Value);
                await insert.ExecuteNonQueryAsync(ct);
            }

            await tx.CommitAsync(ct);
        }

        /// <summary>
        /// Adds a "Shot Time Autodetected" row with the found frame, so the next
        /// reprocess cuts the clip there. Rows this job already wrote are skipped.
        /// </summary>
        private async Task WriteHistoryAsync(
            long jobId,
            List<ClipRelocateImageResult> relocated,
            Dictionary<string, int> imageIds,
            CancellationToken ct)
        {
            var tag = $"clip-relocate job {jobId}";
            await using var conn = await OpenAsync(ct);
            foreach (var img in relocated)
            {
                if (img.FoundFrame is not int frame || !imageIds.TryGetValue(img.Rid, out var imageId))
                    continue;
                await using var cmd = new NpgsqlCommand(@"
INSERT INTO frl.frl_imagehistory (imageid, userid, action, notes, timestamp)
SELECT @i, 0, @action, @notes, now()
WHERE NOT EXISTS (SELECT 1 FROM frl.frl_imagehistory WHERE imageid = @i AND notes LIKE @like);", conn);
                cmd.Parameters.AddWithValue("i", imageId);
                cmd.Parameters.AddWithValue("action", HistoryAction);
                cmd.Parameters.AddWithValue("notes",
                    $"Frame #{frame}, Score: {img.FoundScore?.ToString("F3", CultureInfo.InvariantCulture)}, Relocated: {tag}");
                cmd.Parameters.AddWithValue("like", $"%{tag}");
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }
    }
}
