using AdminPanelAPI.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Collections.Concurrent;
using System.Data;
using System.Text.Json;

namespace ShotDeckSearch.Controllers
{
    /// <summary>
    /// Allocation of uploaded movies to taggers. The admin tagging page assigns
    /// a movie to someone and watches its progress; a tagger reads back only
    /// their own list. Users come from the camera movement roster, which the
    /// dashboard already logs in against.
    ///
    /// A movie moves through hd_uploaded -> sf_created -> tagger_allocated ->
    /// movie_watched -> key_images_extracted. The first two stages are facts
    /// about the movie's R2 folder and so have no row here; a row starts at
    /// tagger_allocated. Reaching movie_watched needs the tagger to play the
    /// HD movie through, which is why the watched position is held here and
    /// only ever advanced by as much as real playback could have covered.
    /// </summary>
    [ApiController]
    [Route("api/admin/movie-tagging")]
    public sealed class MovieTaggingController : ControllerBase
    {
        private const string PosterBaseUrl = "https://image.tmdb.org/t/p/w342";

        private static readonly string[] Statuses =
        {
            "hd_uploaded", "sf_created", "tagger_allocated", "movie_watched", "key_images_extracted"
        };

        /// <summary>
        /// How far a single progress report may move the watched position. The
        /// player reports every 10s, so anything beyond this is a seek and is
        /// clamped away instead of counting as watched.
        /// </summary>
        private const int MaxAdvanceSecondsPerReport = 45;

        /// <summary>Playback this close to the end counts as watched through.</summary>
        private const int WatchedToleranceSeconds = 15;

        /// <summary>Ceiling for a captured frame's preview data URI.</summary>
        private const int MaxThumbnailCharacters = 400_000;

        /// <summary>
        /// Analysis jobs whose proposals have been pulled back already, so the
        /// polls that follow a completed job do not fetch a thousand of them
        /// again. Storing is idempotent, so losing this on a restart only costs
        /// one extra fetch.
        /// </summary>
        private static readonly ConcurrentDictionary<string, int> StoredJobs = new();

        /// <summary>How many proposals one page of the grid may ask for.</summary>
        private const int MaxProposalPageSize = 500;

        /// <summary>
        /// Where the key-image service puts frames cut from a master, as
        /// opposed to the proxy-sized pictures an analysis proposes.
        /// </summary>
        private const string StillsFolder = "/keyimages/stills/";

        private readonly NpgsqlConnection _connection;
        private readonly IMovieFileStorageService _storage;
        private readonly IKeyImageAnalysisService _analysis;
        private readonly IFilmSynopsisService _synopsis;
        private readonly IWalkthroughService _walkthrough;

        public MovieTaggingController(
            NpgsqlConnection connection,
            IMovieFileStorageService storage,
            IKeyImageAnalysisService analysis,
            IFilmSynopsisService synopsis,
            IWalkthroughService walkthrough)
        {
            _connection = connection;
            _storage = storage;
            _analysis = analysis;
            _synopsis = synopsis;
            _walkthrough = walkthrough;
        }

        public sealed class AssignRequest
        {
            public int MovieId { get; set; }
            public string? Tagger { get; set; }
            public string? Status { get; set; }
            public string? Note { get; set; }
            public string? ActingUser { get; set; }
        }

        public sealed class StatusRequest
        {
            public string? Status { get; set; }
            public string? ActingUser { get; set; }
        }

        public sealed class WatchProgressRequest
        {
            public double PositionSeconds { get; set; }
            public double DurationSeconds { get; set; }
            public string? ActingUser { get; set; }
        }

        public sealed class AnalysisRequest
        {
            public int MovieId { get; set; }
            public string? Description { get; set; }
            public string? ActingUser { get; set; }

            /// <summary>
            /// Analyse even though a run started for this movie by itself is
            /// still going, for a tagger who wants their own description used
            /// rather than the film's synopsis.
            /// </summary>
            public bool Force { get; set; }

            /// <summary>
            /// How the story half is to be judged: "walkthrough" from the
            /// ratings read out of the movie's walkthrough, "description" by
            /// matching frames against its plot, or unset for the walkthrough
            /// if there is one. Naming it is what lets both ways be run over
            /// one movie and their frames compared.
            /// </summary>
            public string? StoryFrom { get; set; }
        }

        public sealed class WalkthroughRequest
        {
            public int MovieId { get; set; }
            public string? ActingUser { get; set; }
        }

        public sealed class DecisionRequest
        {
            public long[]? Ids { get; set; }
            public string? Decision { get; set; }
            public string? ActingUser { get; set; }
        }

        public sealed class KeyImageRequest
        {
            public int MovieId { get; set; }
            public double PositionSeconds { get; set; }
            public int? FrameNumber { get; set; }
            public string? Thumbnail { get; set; }
            public string? ActingUser { get; set; }
        }

        /// <summary>
        /// Every allocation, newest first, with the movie's title and poster.
        /// Filtered to one tagger and/or one status when asked, which is what a
        /// tagger's own screen and the status filter request.
        /// </summary>
        [HttpGet("assignments")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> GetAssignments(
            [FromQuery] string? tagger = null,
            [FromQuery] string? status = null,
            CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);

            var normalizedStatus = NormalizeStatus(status);
            if (!string.IsNullOrWhiteSpace(status) && normalizedStatus == null)
                return BadRequest(new { error = "status must be one of: " + string.Join(", ", Statuses) });

            var conditions = new List<string>();
            if (!string.IsNullOrWhiteSpace(tagger))
                conditions.Add("lower(a.tagger) = lower(@tagger)");
            if (normalizedStatus != null)
                conditions.Add("a.status = @status");

            var filter = conditions.Count == 0 ? "" : " WHERE " + string.Join(" AND ", conditions);

            var sql = $@"
SELECT a.movie_id, a.tagger, a.status, a.note, a.assigned_by, a.assigned_at, a.updated_at,
       COALESCE(m.title, '') AS title, m.year, m.media_type::text AS media_type, m.poster,
       a.watch_position_seconds, a.watch_duration_seconds, a.watched_at
FROM frl.frl_movie_tagger_assignments a
LEFT JOIN frl.frl_movies m ON m.idnum = a.movie_id{filter}
ORDER BY a.updated_at DESC;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            if (!string.IsNullOrWhiteSpace(tagger))
                cmd.Parameters.AddWithValue("@tagger", tagger.Trim());
            if (normalizedStatus != null)
                cmd.Parameters.AddWithValue("@status", normalizedStatus);

            var assignments = new List<object>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                assignments.Add(new
                {
                    movieId = reader.GetInt32(0),
                    tagger = reader.GetString(1),
                    status = reader.GetString(2),
                    note = reader.IsDBNull(3) ? null : reader.GetString(3),
                    assignedBy = reader.IsDBNull(4) ? null : reader.GetString(4),
                    assignedAt = reader.GetDateTime(5),
                    updatedAt = reader.GetDateTime(6),
                    title = reader.GetString(7),
                    year = reader.IsDBNull(8) ? (int?)null : reader.GetInt32(8),
                    mediaType = reader.IsDBNull(9) ? "" : reader.GetString(9),
                    poster = reader.IsDBNull(10) ? null : PosterBaseUrl + reader.GetString(10),
                    watchPositionSeconds = reader.GetInt32(11),
                    watchDurationSeconds = reader.IsDBNull(12) ? (int?)null : reader.GetInt32(12),
                    watchedAt = reader.IsDBNull(13) ? (DateTime?)null : reader.GetDateTime(13)
                });
            }

            return Ok(new { assignments });
        }

        /// <summary>Allocate a movie to a tagger, or re-allocate it.</summary>
        [HttpPost("assignments")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<IActionResult> Assign(
            [FromBody] AssignRequest request, CancellationToken ct = default)
        {
            var tagger = (request.Tagger ?? "").Trim();
            if (request.MovieId <= 0)
                return BadRequest(new { error = "movieId is required." });
            if (string.IsNullOrWhiteSpace(tagger))
                return BadRequest(new { error = "tagger is required." });

            var status = NormalizeStatus(request.Status) ?? "tagger_allocated";

            await EnsureReadyAsync(ct);
            if (!await IsAdminAsync(request.ActingUser, ct))
                return StatusCode(403, new { error = "Only an admin can allocate movies." });
            if (!await UserExistsAsync(tagger, ct))
                return BadRequest(new { error = "Unknown tagger." });

            const string sql = @"
INSERT INTO frl.frl_movie_tagger_assignments
    (movie_id, tagger, status, note, assigned_by)
VALUES (@movieId, @tagger, @status, @note, @actingUser)
ON CONFLICT (movie_id) DO UPDATE
SET tagger = EXCLUDED.tagger,
    status = CASE
        WHEN lower(EXCLUDED.tagger) = lower(frl_movie_tagger_assignments.tagger)
            THEN frl_movie_tagger_assignments.status
        ELSE EXCLUDED.status
    END,
    watch_position_seconds = CASE
        WHEN lower(EXCLUDED.tagger) = lower(frl_movie_tagger_assignments.tagger)
            THEN frl_movie_tagger_assignments.watch_position_seconds
        ELSE 0
    END,
    watched_at = CASE
        WHEN lower(EXCLUDED.tagger) = lower(frl_movie_tagger_assignments.tagger)
            THEN frl_movie_tagger_assignments.watched_at
        ELSE NULL
    END,
    note = EXCLUDED.note,
    assigned_by = EXCLUDED.assigned_by,
    updated_at = now()
RETURNING movie_id, tagger, status;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@movieId", request.MovieId);
            cmd.Parameters.AddWithValue("@tagger", tagger);
            cmd.Parameters.AddWithValue("@status", status);
            cmd.Parameters.AddWithValue("@note", (object?)request.Note?.Trim() ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@actingUser", (object?)request.ActingUser?.Trim() ?? DBNull.Value);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return StatusCode(500, new { error = "Assignment failed." });

            return Ok(new
            {
                movieId = reader.GetInt32(0),
                tagger = reader.GetString(1),
                status = reader.GetString(2)
            });
        }

        /// <summary>
        /// Move an allocation along by hand. Only an admin may do this: a tagger
        /// reaches movie_watched by actually watching the movie, not by saying
        /// they have.
        /// </summary>
        [HttpPut("assignments/{movieId:int}/status")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<IActionResult> SetStatus(
            int movieId, [FromBody] StatusRequest request, CancellationToken ct = default)
        {
            var status = NormalizeStatus(request.Status);
            if (status == null)
                return BadRequest(new { error = "status must be one of: " + string.Join(", ", Statuses) });

            var actingUser = (request.ActingUser ?? "").Trim();
            if (string.IsNullOrWhiteSpace(actingUser))
                return BadRequest(new { error = "actingUser is required." });

            await EnsureReadyAsync(ct);
            if (!await IsAdminAsync(actingUser, ct))
                return StatusCode(403, new { error = "Only an admin can set a status by hand." });

            const string sql = @"
UPDATE frl.frl_movie_tagger_assignments
SET status = @status,
    watched_at = CASE WHEN @status = 'movie_watched' THEN COALESCE(watched_at, now()) ELSE NULL END,
    watch_position_seconds = CASE WHEN @status = 'tagger_allocated' THEN 0 ELSE watch_position_seconds END,
    key_images_at = CASE WHEN @status = 'key_images_extracted' THEN COALESCE(key_images_at, now()) ELSE NULL END,
    updated_at = now()
WHERE movie_id = @movieId;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            cmd.Parameters.AddWithValue("@status", status);

            var affected = await cmd.ExecuteNonQueryAsync(ct);
            if (affected == 0)
                return NotFound(new { error = "That movie is not allocated." });
            return Ok(new { movieId, status });
        }

        /// <summary>
        /// Report how far the tagger has played their HD movie. The stored
        /// position only ever moves forward, and by no more than one report's
        /// worth of playback, so seeking ahead or reloading cannot complete the
        /// watch; reaching the end is what flips the movie to movie_watched.
        /// </summary>
        [HttpPut("assignments/{movieId:int}/watch")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<IActionResult> ReportWatchProgress(
            int movieId, [FromBody] WatchProgressRequest request, CancellationToken ct = default)
        {
            var actingUser = (request.ActingUser ?? "").Trim();
            if (string.IsNullOrWhiteSpace(actingUser))
                return BadRequest(new { error = "actingUser is required." });
            if (request.DurationSeconds <= 0)
                return BadRequest(new { error = "durationSeconds is required." });

            await EnsureReadyAsync(ct);

            var ownerFilter = await IsAdminAsync(actingUser, ct)
                ? "" : " AND lower(tagger) = lower(@actingUser)";

            var duration = (int)Math.Round(request.DurationSeconds);
            var reported = (int)Math.Max(0, Math.Round(request.PositionSeconds));

            var sql = $@"
UPDATE frl.frl_movie_tagger_assignments SET
    watch_duration_seconds = @duration,
    watch_position_seconds = GREATEST(
        watch_position_seconds,
        LEAST(@reported, watch_position_seconds + @maxAdvance)),
    status = CASE
        WHEN status IN ('key_images_extracted', 'movie_watched') THEN status
        WHEN GREATEST(
                 watch_position_seconds,
                 LEAST(@reported, watch_position_seconds + @maxAdvance)) >= @duration - @tolerance
            THEN 'movie_watched'
        ELSE status
    END,
    watched_at = CASE
        WHEN watched_at IS NOT NULL THEN watched_at
        WHEN GREATEST(
                 watch_position_seconds,
                 LEAST(@reported, watch_position_seconds + @maxAdvance)) >= @duration - @tolerance
            THEN now()
        ELSE NULL
    END,
    updated_at = now()
WHERE movie_id = @movieId{ownerFilter}
RETURNING watch_position_seconds, watch_duration_seconds, status;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            cmd.Parameters.AddWithValue("@duration", duration);
            cmd.Parameters.AddWithValue("@reported", reported);
            cmd.Parameters.AddWithValue("@maxAdvance", MaxAdvanceSecondsPerReport);
            cmd.Parameters.AddWithValue("@tolerance", WatchedToleranceSeconds);
            cmd.Parameters.AddWithValue("@actingUser", actingUser);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return StatusCode(403, new { error = "Not your assignment, or it no longer exists." });

            return Ok(new
            {
                movieId,
                watchPositionSeconds = reader.GetInt32(0),
                watchDurationSeconds = reader.IsDBNull(1) ? (int?)null : reader.GetInt32(1),
                status = reader.GetString(2)
            });
        }

        /// <summary>
        /// The movie's kept frames, earliest first — the ones a tagger picked
        /// while watching plus the proposals they kept. Proposals still waiting
        /// on a decision are read from key-image-proposals instead.
        /// </summary>
        [HttpGet("key-images")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> GetKeyImages(
            [FromQuery] int movieId, CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);

            const string sql = @"
SELECT id, movie_id, position_seconds, thumbnail, captured_by, created_at,
       frame_number, source, score, image_key, look_score, story_score
FROM frl.frl_movie_key_images
WHERE movie_id = @movieId AND decision = 'kept'
ORDER BY position_seconds;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);

            var images = new List<object>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var imageKey = reader.IsDBNull(9) ? null : reader.GetString(9);
                images.Add(new
                {
                    id = reader.GetInt64(0),
                    movieId = reader.GetInt32(1),
                    positionSeconds = (double)reader.GetDecimal(2),
                    thumbnail = reader.IsDBNull(3) ? null : reader.GetString(3),
                    capturedBy = reader.IsDBNull(4) ? null : reader.GetString(4),
                    createdAt = reader.GetDateTime(5),
                    frameNumber = reader.IsDBNull(6) ? (int?)null : reader.GetInt32(6),
                    source = reader.GetString(7),
                    score = reader.IsDBNull(8) ? (double?)null : (double)reader.GetDecimal(8),
                    imageKey,
                    imageUrl = imageKey == null ? null : _storage.CreateDownloadUrl(imageKey, false),
                    lookScore = reader.IsDBNull(10) ? (double?)null : (double)reader.GetDecimal(10),
                    storyScore = reader.IsDBNull(11) ? (double?)null : (double)reader.GetDecimal(11)
                });
            }

            return Ok(new { images });
        }

        /// <summary>
        /// Keep a frame the tagger picked. The timestamp is what will be cut
        /// from the master later; the thumbnail is only the preview, so picking
        /// the same frame twice just refreshes it.
        /// </summary>
        [HttpPost("key-images")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<IActionResult> AddKeyImage(
            [FromBody] KeyImageRequest request, CancellationToken ct = default)
        {
            var actingUser = (request.ActingUser ?? "").Trim();
            if (string.IsNullOrWhiteSpace(actingUser))
                return BadRequest(new { error = "actingUser is required." });
            if (request.PositionSeconds < 0)
                return BadRequest(new { error = "positionSeconds must not be negative." });
            if (request.Thumbnail != null && request.Thumbnail.Length > MaxThumbnailCharacters)
                return BadRequest(new { error = "thumbnail is too large." });

            await EnsureReadyAsync(ct);
            if (!await CanCaptureAsync(request.MovieId, actingUser, ct))
                return StatusCode(403, new { error = "That movie is not allocated to you." });

            const string sql = @"
INSERT INTO frl.frl_movie_key_images
    (movie_id, position_seconds, frame_number, thumbnail, captured_by)
VALUES (@movieId, @position, @frame, @thumbnail, @actingUser)
ON CONFLICT (movie_id, position_seconds) DO UPDATE
    SET thumbnail = COALESCE(EXCLUDED.thumbnail, frl.frl_movie_key_images.thumbnail),
        frame_number = COALESCE(EXCLUDED.frame_number, frl.frl_movie_key_images.frame_number),
        captured_by = EXCLUDED.captured_by,
        source = 'tagger',
        decision = 'kept'
RETURNING id, created_at;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@movieId", request.MovieId);
            cmd.Parameters.AddWithValue("@position", Math.Round((decimal)request.PositionSeconds, 3));
            cmd.Parameters.AddWithValue("@frame", (object?)request.FrameNumber ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@thumbnail", (object?)request.Thumbnail ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@actingUser", actingUser);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            return Ok(new
            {
                id = reader.GetInt64(0),
                movieId = request.MovieId,
                positionSeconds = request.PositionSeconds,
                frameNumber = request.FrameNumber,
                capturedBy = actingUser,
                createdAt = reader.GetDateTime(1)
            });
        }

        /// <summary>
        /// The kept frame at the master's own resolution. A frame picked while
        /// watching only has the browser's preview, so the still is cut off the
        /// master on first ask and remembered on the row from then on.
        /// </summary>
        [HttpPost("key-images/{id:long}/still")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<IActionResult> CutKeyImageStill(
            long id, CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);

            int movieId;
            int? frameNumber;
            double positionSeconds;
            string? imageKey;

            const string lookupSql = @"
SELECT movie_id, frame_number, position_seconds, image_key
FROM frl.frl_movie_key_images
WHERE id = @id;";
            await using (var lookup = new NpgsqlCommand(lookupSql, _connection))
            {
                lookup.Parameters.AddWithValue("@id", id);
                await using var row = await lookup.ExecuteReaderAsync(ct);
                if (!await row.ReadAsync(ct))
                    return NotFound(new { error = "No such key image." });

                movieId = row.GetInt32(0);
                frameNumber = row.IsDBNull(1) ? null : row.GetInt32(1);
                positionSeconds = (double)row.GetDecimal(2);
                imageKey = row.IsDBNull(3) ? null : row.GetString(3);
            }

            if (imageKey != null && imageKey.Contains(StillsFolder))
                return Ok(new
                {
                    imageKey,
                    imageUrl = _storage.CreateDownloadUrl(imageKey, false),
                    cut = false
                });

            var variants = await _storage.GetMovieVariantsAsync(new[] { movieId }, ct);
            var masterKey = variants.FirstOrDefault()?.HdKey;
            if (string.IsNullOrWhiteSpace(masterKey))
                return BadRequest(new { error = "That movie has no HD master to cut from." });

            var result = await _analysis.CutStillAsync(
                masterKey, frameNumber, frameNumber.HasValue ? null : positionSeconds, ct);
            if (!result.IsSuccess)
                return StatusCode((int)result.Status, result.Body);

            using var document = JsonDocument.Parse(result.Body);
            if (!document.RootElement.TryGetProperty("imageKey", out var cutKey) ||
                cutKey.GetString() is not { Length: > 0 } stillKey)
                return StatusCode(502, new { error = "The still service returned no image." });

            var cutFrame = document.RootElement.TryGetProperty("frame", out var frame) &&
                frame.TryGetInt32(out var number) ? number : frameNumber;

            const string saveSql = @"
UPDATE frl.frl_movie_key_images
SET image_key = @imageKey,
    frame_number = COALESCE(@frame, frame_number)
WHERE id = @id;";
            await using (var save = new NpgsqlCommand(saveSql, _connection))
            {
                save.Parameters.AddWithValue("@imageKey", stillKey);
                save.Parameters.AddWithValue("@frame", (object?)cutFrame ?? DBNull.Value);
                save.Parameters.AddWithValue("@id", id);
                await save.ExecuteNonQueryAsync(ct);
            }

            return Ok(new
            {
                imageKey = stillKey,
                imageUrl = _storage.CreateDownloadUrl(stillKey, false),
                frameNumber = cutFrame,
                cut = true
            });
        }

        /// <summary>Drop a frame the tagger picked by mistake.</summary>
        [HttpDelete("key-images/{id:long}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<IActionResult> DeleteKeyImage(
            long id, [FromQuery] string? actingUser = null, CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);

            const string movieSql =
                "SELECT movie_id FROM frl.frl_movie_key_images WHERE id = @id;";
            await using (var lookup = new NpgsqlCommand(movieSql, _connection))
            {
                lookup.Parameters.AddWithValue("@id", id);
                if (await lookup.ExecuteScalarAsync(ct) is not int movieId)
                    return NoContent();
                if (!await CanCaptureAsync(movieId, (actingUser ?? "").Trim(), ct))
                    return StatusCode(403, new { error = "That movie is not allocated to you." });
            }

            const string sql = "DELETE FROM frl.frl_movie_key_images WHERE id = @id;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@id", id);
            await cmd.ExecuteNonQueryAsync(ct);
            return NoContent();
        }

        /// <summary>
        /// Start the analysis that proposes key frames for a watched movie. It
        /// reads the movie's SF proxy, not the master, and takes minutes, so
        /// this only hands back a job to poll.
        /// </summary>
        [HttpPost("key-image-analysis")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<IActionResult> StartKeyImageAnalysis(
            [FromBody] AnalysisRequest request, CancellationToken ct = default)
        {
            var actingUser = (request.ActingUser ?? "").Trim();
            if (string.IsNullOrWhiteSpace(actingUser))
                return BadRequest(new { error = "actingUser is required." });

            await EnsureReadyAsync(ct);
            if (!await CanCaptureAsync(request.MovieId, actingUser, ct))
                return StatusCode(403, new { error = "That movie is not allocated to you." });

            // A movie is analysed by itself once its SF proxy appears, so the
            // button often has nothing to do: say so rather than paying for the
            // same run twice over.
            var preparation = await MoviePreparationStore.GetAsync(_connection, request.MovieId, ct);
            if (!request.Force &&
                preparation?.AnalysisStatus == MoviePreparationStore.Running &&
                preparation.AnalysisJobId is { Length: > 0 })
            {
                return Conflict(new
                {
                    error = "This movie is already being analysed in the background.",
                    jobId = preparation.AnalysisJobId,
                    stage = preparation.AnalysisStage,
                    progress = preparation.AnalysisProgress,
                    startedAt = preparation.StartedAt,
                    automatic = true
                });
            }

            var variants = await _storage.GetMovieVariantsAsync(new[] { request.MovieId }, ct);
            var sourceKey = variants.FirstOrDefault()?.SlimKey;
            if (string.IsNullOrWhiteSpace(sourceKey))
                return BadRequest(new { error = "That movie has no SF proxy to analyse yet." });

            var description = (request.Description ?? "").Trim();
            if (description.Length == 0)
                description = (await MovieDescriptionAsync(request.MovieId, ct)).Description ?? "";

            var storyFrom = (request.StoryFrom ?? "").Trim().ToLowerInvariant();
            if (storyFrom is not ("" or "walkthrough" or "description"))
                return BadRequest(new
                {
                    error = "storyFrom must be walkthrough or description."
                });

            var result = await _analysis.StartAsync(
                sourceKey, request.MovieId, description,
                storyFrom.Length == 0 ? null : storyFrom, ct);
            return Content(result.Body, "application/json", System.Text.Encoding.UTF8);
        }

        /// <summary>
        /// What has been done to this movie without anybody asking: the
        /// walkthrough, the reading of it that rates which moments are worth a
        /// still, and the key image analysis, all off the back of its SF proxy
        /// appearing. This is what the tagging page shows as the
        /// movie's preparation status, and what tells the Analyse button a run
        /// is already going.
        /// </summary>
        [HttpGet("preparation")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> GetPreparation(
            [FromQuery] int movieId = 0,
            [FromQuery] string? movieIds = null,
            CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);

            if (!string.IsNullOrWhiteSpace(movieIds))
            {
                var ids = movieIds.Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(part => int.TryParse(part.Trim(), out var id) ? id : 0)
                    .Where(id => id > 0);
                var rows = await MoviePreparationStore.ListAsync(_connection, ids, ct);
                return Ok(new { movies = rows.Select(Preparation) });
            }

            var row = await MoviePreparationStore.GetAsync(_connection, movieId, ct);
            return row == null
                ? Ok(new { movieId, prepared = false })
                : Ok(Preparation(row));
        }

        private static object Preparation(MoviePreparationRow row) => new
        {
            movieId = row.MovieId,
            prepared = true,
            sourceKey = row.SourceKey,
            running = row.Running,
            startedAt = row.StartedAt,
            updatedAt = row.UpdatedAt,
            walkthrough = new
            {
                jobId = row.WalkthroughJobId,
                status = row.WalkthroughStatus,
                stage = row.WalkthroughStage,
                progress = row.WalkthroughProgress,
                shots = row.WalkthroughShots,
                error = row.WalkthroughError
            },
            story = new
            {
                jobId = row.StoryJobId,
                status = row.StoryStatus,
                stage = row.StoryStage,
                progress = row.StoryProgress,
                shotsRated = row.StoryRated,
                error = row.StoryError
            },
            analysis = new
            {
                jobId = row.AnalysisJobId,
                status = row.AnalysisStatus,
                stage = row.AnalysisStage,
                progress = row.AnalysisProgress,
                proposals = row.AnalysisProposals,
                error = row.AnalysisError
            }
        };

        /// <summary>
        /// The film described shot by shot: a timestamped account of what is on
        /// screen, cut from the SF proxy. Read back out of R2, so it survives
        /// the job that made it.
        /// </summary>
        [HttpGet("walkthrough")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<IActionResult> GetWalkthrough(
            [FromQuery] int movieId, CancellationToken ct = default)
        {
            var variants = await _storage.GetMovieVariantsAsync(new[] { movieId }, ct);
            var sourceKey = variants.FirstOrDefault()?.SlimKey;
            if (string.IsNullOrWhiteSpace(sourceKey))
                return NotFound(new { error = "That movie has no SF proxy." });

            var result = await _walkthrough.GetStoredAsync(sourceKey, ct);
            return Relay(result);
        }

        /// <summary>
        /// Describe the movie shot by shot now. Movies get this by themselves
        /// once their SF proxy appears, so this is for the ones whose proxy was
        /// made before that, and for a re-run.
        /// </summary>
        [HttpPost("walkthrough")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        public async Task<IActionResult> StartWalkthrough(
            [FromBody] WalkthroughRequest request, CancellationToken ct = default)
        {
            var actingUser = (request.ActingUser ?? "").Trim();
            if (string.IsNullOrWhiteSpace(actingUser))
                return BadRequest(new { error = "actingUser is required." });

            await EnsureReadyAsync(ct);
            if (!await CanCaptureAsync(request.MovieId, actingUser, ct))
                return StatusCode(403, new { error = "That movie is not allocated to you." });

            var existing = await MoviePreparationStore.GetAsync(_connection, request.MovieId, ct);
            if (existing?.WalkthroughStatus == MoviePreparationStore.Running &&
                existing.WalkthroughJobId is { Length: > 0 })
            {
                return Conflict(new
                {
                    error = "This movie is already being described in the background.",
                    jobId = existing.WalkthroughJobId,
                    stage = existing.WalkthroughStage,
                    progress = existing.WalkthroughProgress,
                    automatic = true
                });
            }

            var variants = await _storage.GetMovieVariantsAsync(new[] { request.MovieId }, ct);
            var sourceKey = variants.FirstOrDefault()?.SlimKey;
            if (string.IsNullOrWhiteSpace(sourceKey))
                return BadRequest(new { error = "That movie has no SF proxy to describe yet." });

            var result = await _walkthrough.StartAsync(sourceKey, request.MovieId, ct);
            if (!result.IsSuccess)
                return Relay(result);

            using var document = JsonDocument.Parse(result.Body);
            var jobId = document.RootElement.TryGetProperty("jobId", out var id)
                ? id.GetString() : null;
            if (jobId is { Length: > 0 })
            {
                // Recorded so the worker follows this run too and stores where it
                // got to, rather than the page having to.
                await MoviePreparationStore.TryClaimAsync(
                    _connection, request.MovieId, sourceKey, ct);
                await MoviePreparationStore.SetJobAsync(
                    _connection, request.MovieId, "walkthrough", jobId,
                    MoviePreparationStore.Running, null, ct);
            }

            return Relay(result);
        }

        private ContentResult Relay(TranscodeResult result) => new()
        {
            StatusCode = (int)result.Status,
            Content = result.Body,
            ContentType = "application/json"
        };

        /// <summary>
        /// What the analysis will be told the film is, unless the tagger edits
        /// it: frames are scored partly on how well they match this.
        /// </summary>
        [HttpGet("key-image-analysis/description")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> GetKeyImageDescription(
            [FromQuery] int movieId, CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);
            var (description, source) = await MovieDescriptionAsync(movieId, ct);
            return Ok(new
            {
                description = description ?? "",
                proseColumn = source == MovieDescriptions.WikipediaSource ? null : source,
                proseSource = source,
                proseColumnsAvailable = await DescriptionColumnsAsync(ct)
            });
        }

        /// <summary>
        /// How an analysis job is getting on. Once it finishes, its proposals
        /// are pulled back and stored against the movie as undecided frames,
        /// which is what the tagger's grid then reads.
        /// </summary>
        [HttpGet("key-image-analysis/{jobId}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> GetKeyImageAnalysis(
            string jobId, [FromQuery] int movieId, CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);

            var stored = movieId > 0 ? await CountProposalsAsync(movieId, ct) : 0;
            // Re-analysing a movie that already has proposals has to pull the
            // new run's frames too, so this follows the job rather than
            // whether the movie has any proposals at all.
            var wantProposals = movieId > 0 && !StoredJobs.ContainsKey(jobId);

            var result = await _analysis.GetJobAsync(jobId, wantProposals, ct);
            if (!result.IsSuccess)
                return StatusCode((int)result.Status, result.Body);

            using var document = JsonDocument.Parse(result.Body);
            var job = document.RootElement;
            var status = job.TryGetProperty("status", out var value) ? value.GetString() : null;

            if (wantProposals && status == "completed" &&
                job.TryGetProperty("proposals", out var proposals) &&
                proposals.ValueKind == JsonValueKind.Array)
            {
                stored = await StoreProposalsAsync(
                    movieId, proposals, StoryFrom(job), ct);
                StoredJobs[jobId] = stored;
            }

            return Ok(new
            {
                jobId = job.TryGetProperty("jobId", out var id) ? id.GetString() : jobId,
                status,
                stage = job.TryGetProperty("stage", out var stage) ? stage.GetString() : null,
                progress = job.TryGetProperty("progress", out var done) ? done.GetDouble() : 0,
                error = job.TryGetProperty("error", out var error) ? error.GetString() : null,
                proposed = stored
            });
        }

        /// <summary>
        /// Proposals still waiting on the tagger. Each carries the master frame
        /// number the still will eventually be cut at.
        ///
        /// A blend picks which frames a page holds — 0 weighs how the frame
        /// looks, 1 how much it matches the film's description, between the two
        /// — but the page still arrives in the order the frames appear in the
        /// film, since that is how a tagger reads a movie. byScore = true gives
        /// the page best-first instead.
        ///
        /// fromSeconds/toSeconds narrow the page to one stretch of the film, so
        /// a shot found in the walkthrough can show the frames proposed out of
        /// it; total then counts that stretch rather than the whole movie.
        ///
        /// storyFrom narrows the page to the run that judged the story half
        /// that way, which is how the walkthrough's frames and the plot's are
        /// compared; runs always reports what each way of judging has waiting,
        /// so the page knows which comparisons it can offer.
        /// </summary>
        [HttpGet("key-image-proposals")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> GetKeyImageProposals(
            [FromQuery] int movieId,
            [FromQuery] int limit = 120,
            [FromQuery] int offset = 0,
            [FromQuery] double? blend = null,
            [FromQuery] bool byScore = false,
            [FromQuery] double? fromSeconds = null,
            [FromQuery] double? toSeconds = null,
            [FromQuery] string? storyFrom = null,
            CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);

            var run = (storyFrom ?? "").Trim().ToLowerInvariant();
            if (run is not ("" or "walkthrough" or "description"))
                return BadRequest(new
                {
                    error = "storyFrom must be walkthrough or description."
                });

            const string columns = @"id, position_seconds, frame_number, score, image_key,
       created_at, look_score, story_score, source, story_from";

            var window =
                (fromSeconds.HasValue ? " AND position_seconds >= @fromSeconds" : "") +
                (toSeconds.HasValue ? " AND position_seconds <= @toSeconds" : "") +
                (run.Length == 0 ? "" : " AND story_from = @storyFrom");

            var page = @"
SELECT " + columns + @"
FROM frl.frl_movie_key_images
WHERE movie_id = @movieId AND decision = 'proposed'" + window + @"
ORDER BY " + (blend.HasValue
                ? @"(1 - @blend) * COALESCE(look_score, 0) +
                    @blend * COALESCE(story_score, 0) DESC, position_seconds"
                : byScore
                    ? "score DESC, position_seconds"
                    : "position_seconds") + @"
LIMIT @limit OFFSET @offset";

            var sql = blend.HasValue && !byScore
                ? "SELECT * FROM (" + page + ") ranked ORDER BY position_seconds;"
                : page + ";";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            if (blend.HasValue)
                cmd.Parameters.AddWithValue("@blend", (decimal)Math.Clamp(blend.Value, 0, 1));
            if (fromSeconds.HasValue)
                cmd.Parameters.AddWithValue("@fromSeconds", (decimal)fromSeconds.Value);
            if (toSeconds.HasValue)
                cmd.Parameters.AddWithValue("@toSeconds", (decimal)toSeconds.Value);
            if (run.Length > 0)
                cmd.Parameters.AddWithValue("@storyFrom", run);
            cmd.Parameters.AddWithValue("@limit", Math.Clamp(limit, 1, MaxProposalPageSize));
            cmd.Parameters.AddWithValue("@offset", Math.Max(0, offset));

            var proposals = new List<object>();
            await using (var reader = await cmd.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var imageKey = reader.IsDBNull(4) ? null : reader.GetString(4);
                    proposals.Add(new
                    {
                        id = reader.GetInt64(0),
                        movieId,
                        positionSeconds = (double)reader.GetDecimal(1),
                        frameNumber = reader.IsDBNull(2) ? (int?)null : reader.GetInt32(2),
                        score = reader.IsDBNull(3) ? (double?)null : (double)reader.GetDecimal(3),
                        imageKey,
                        imageUrl = imageKey == null ? null : _storage.CreateDownloadUrl(imageKey, false),
                        createdAt = reader.GetDateTime(5),
                        lookScore = reader.IsDBNull(6) ? (double?)null : (double)reader.GetDecimal(6),
                        storyScore = reader.IsDBNull(7) ? (double?)null : (double)reader.GetDecimal(7),
                        source = reader.IsDBNull(8) ? null : reader.GetString(8),
                        storyFrom = reader.IsDBNull(9) ? null : reader.GetString(9)
                    });
                }
            }

            var runs = await RunsAsync(movieId, ct);
            var total = run.Length > 0
                ? runs.TryGetValue(run, out var counted) ? counted : 0
                : fromSeconds.HasValue || toSeconds.HasValue
                    ? await KeyImageProposalStore.CountAsync(
                        _connection, movieId, fromSeconds, toSeconds, ct)
                    : await CountProposalsAsync(movieId, ct);

            return Ok(new { proposals, total, storyFrom = run.Length == 0 ? null : run, runs });
        }

        /// <summary>
        /// What each way of judging the story half has waiting on this movie,
        /// so the page can offer one run against the other. Frames from before
        /// a run recorded how it judged are counted as "unknown".
        /// </summary>
        private async Task<Dictionary<string, int>> RunsAsync(
            int movieId, CancellationToken ct)
        {
            const string sql = @"
SELECT COALESCE(story_from, 'unknown'), count(*)
FROM frl.frl_movie_key_images
WHERE movie_id = @movieId AND decision = 'proposed' AND source = 'ai'
GROUP BY 1;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);

            var runs = new Dictionary<string, int>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                runs[reader.GetString(0)] = (int)reader.GetInt64(1);
            return runs;
        }

        /// <summary>
        /// Keep or discard a selection of proposals. Discarding leaves the row
        /// behind so the same frame is not proposed again; keeping turns it into
        /// one of the movie's key images.
        /// </summary>
        [HttpPost("key-images/decide")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<IActionResult> DecideKeyImages(
            [FromBody] DecisionRequest request, CancellationToken ct = default)
        {
            var actingUser = (request.ActingUser ?? "").Trim();
            if (string.IsNullOrWhiteSpace(actingUser))
                return BadRequest(new { error = "actingUser is required." });

            var decision = (request.Decision ?? "").Trim().ToLowerInvariant();
            if (decision is not ("kept" or "discarded"))
                return BadRequest(new { error = "decision must be kept or discarded." });

            var ids = (request.Ids ?? Array.Empty<long>()).Distinct().ToArray();
            if (ids.Length == 0)
                return BadRequest(new { error = "ids is required." });

            await EnsureReadyAsync(ct);

            const string movieSql =
                "SELECT DISTINCT movie_id FROM frl.frl_movie_key_images WHERE id = ANY(@ids);";
            var movieIds = new List<int>();
            await using (var lookup = new NpgsqlCommand(movieSql, _connection))
            {
                lookup.Parameters.AddWithValue("@ids", ids);
                await using var reader = await lookup.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                    movieIds.Add(reader.GetInt32(0));
            }

            foreach (var movieId in movieIds)
                if (!await CanCaptureAsync(movieId, actingUser, ct))
                    return StatusCode(403, new { error = "That movie is not allocated to you." });

            const string sql = @"
UPDATE frl.frl_movie_key_images
SET decision = @decision, decided_by = @actingUser, decided_at = now()
WHERE id = ANY(@ids);";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@ids", ids);
            cmd.Parameters.AddWithValue("@decision", decision);
            cmd.Parameters.AddWithValue("@actingUser", actingUser);
            var affected = await cmd.ExecuteNonQueryAsync(ct);

            var remaining = new Dictionary<int, int>();
            foreach (var movieId in movieIds)
                remaining[movieId] = await CountProposalsAsync(movieId, ct);

            return Ok(new { decision, decided = affected, remaining });
        }

        private Task<int> CountProposalsAsync(int movieId, CancellationToken ct) =>
            KeyImageProposalStore.CountAsync(_connection, movieId, ct);

        private Task<int> StoreProposalsAsync(
            int movieId, JsonElement proposals, string? storyFrom, CancellationToken ct) =>
            KeyImageProposalStore.StoreAsync(
                _connection, movieId, proposals, storyFrom, ct);

        /// <summary>
        /// How the run judged the story half: reading the movie's walkthrough,
        /// or matching frames against its plot summary.
        /// </summary>
        private static string? StoryFrom(JsonElement job) =>
            job.TryGetProperty("storyFrom", out var from) ? from.GetString() : null;

        private Task<string[]> DescriptionColumnsAsync(CancellationToken ct) =>
            MovieDescriptions.ColumnsAsync(_connection, ct);

        private Task<(string? Description, string? Source)> MovieDescriptionAsync(
            int movieId, CancellationToken ct) =>
            MovieDescriptions.ForAsync(_connection, _synopsis, movieId, ct);

        /// <summary>Remove an allocation, putting the movie back in the pool.</summary>
        [HttpDelete("assignments/{movieId:int}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<IActionResult> Unassign(
            int movieId, [FromQuery] string? actingUser = null, CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);
            if (!await IsAdminAsync(actingUser, ct))
                return StatusCode(403, new { error = "Only an admin can unallocate movies." });

            const string sql = "DELETE FROM frl.frl_movie_tagger_assignments WHERE movie_id = @movieId;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            await cmd.ExecuteNonQueryAsync(ct);
            return NoContent();
        }

        /// <summary>
        /// Per-tagger counts by status, for the admin's progress table and a
        /// tagger's own profile cards.
        /// </summary>
        [HttpGet("summary")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> GetSummary(CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);

            const string sql = @"
SELECT u.name,
       COUNT(a.movie_id) FILTER (WHERE a.status = 'tagger_allocated') AS allocated,
       COUNT(a.movie_id) FILTER (WHERE a.status = 'movie_watched') AS watched,
       COUNT(a.movie_id) FILTER (WHERE a.status = 'key_images_extracted') AS key_images
FROM frl.frl_camera_movement_users u
LEFT JOIN frl.frl_movie_tagger_assignments a ON lower(a.tagger) = lower(u.name)
GROUP BY u.name
ORDER BY lower(u.name);";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            var rows = new List<object>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                rows.Add(new
                {
                    tagger = reader.GetString(0),
                    allocated = (int)reader.GetInt64(1),
                    watched = (int)reader.GetInt64(2),
                    keyImages = (int)reader.GetInt64(3)
                });
            }

            return Ok(new { taggers = rows });
        }

        private static string? NormalizeStatus(string? status)
        {
            if (string.IsNullOrWhiteSpace(status)) return null;
            var value = status.Trim().ToLowerInvariant().Replace(' ', '_');
            return Array.IndexOf(Statuses, value) >= 0 ? value : null;
        }

        private async Task EnsureReadyAsync(CancellationToken ct)
        {
            if (_connection.State != ConnectionState.Open)
                await _connection.OpenAsync(ct);

            // Mirrors migrations/030 so a slot that hasn't had migrations run
            // still works, matching how the camera movement tables are handled.
            const string sql = @"
CREATE TABLE IF NOT EXISTS frl.frl_movie_tagger_assignments (
    movie_id     INTEGER      PRIMARY KEY,
    tagger       VARCHAR(120) NOT NULL,
    status       VARCHAR(24)  NOT NULL DEFAULT 'tagger_allocated',
    note         TEXT,
    assigned_by  VARCHAR(120),
    assigned_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fmta_tagger
    ON frl.frl_movie_tagger_assignments (lower(tagger));
ALTER TABLE frl.frl_movie_tagger_assignments
    ADD COLUMN IF NOT EXISTS watch_position_seconds INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS watch_duration_seconds INTEGER,
    ADD COLUMN IF NOT EXISTS watched_at             TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS key_images_at          TIMESTAMPTZ;
UPDATE frl.frl_movie_tagger_assignments
SET status = 'movie_watched', watched_at = COALESCE(watched_at, updated_at)
WHERE status = 'done';
UPDATE frl.frl_movie_tagger_assignments
SET status = 'tagger_allocated'
WHERE status IN ('not_started', 'in_progress');
CREATE TABLE IF NOT EXISTS frl.frl_movie_key_images (
    id               BIGSERIAL     PRIMARY KEY,
    movie_id         INTEGER       NOT NULL,
    position_seconds NUMERIC(10,3) NOT NULL,
    thumbnail        TEXT,
    captured_by      VARCHAR(120),
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now()
);
-- Superseded by ux_fmki_movie_position_story below, which allows a second of
-- film to be proposed by both ways of judging the story.
DROP INDEX IF EXISTS frl.ux_fmki_movie_position;
ALTER TABLE frl.frl_movie_key_images
    ADD COLUMN IF NOT EXISTS frame_number INTEGER,
    ADD COLUMN IF NOT EXISTS source       VARCHAR(16)  NOT NULL DEFAULT 'tagger',
    ADD COLUMN IF NOT EXISTS decision     VARCHAR(16)  NOT NULL DEFAULT 'kept',
    ADD COLUMN IF NOT EXISTS score        NUMERIC(6,3),
    ADD COLUMN IF NOT EXISTS look_score   NUMERIC(6,3),
    ADD COLUMN IF NOT EXISTS story_score  NUMERIC(6,3),
    ADD COLUMN IF NOT EXISTS story_from   VARCHAR(16),
    ADD COLUMN IF NOT EXISTS image_key    TEXT,
    ADD COLUMN IF NOT EXISTS decided_by   VARCHAR(120),
    ADD COLUMN IF NOT EXISTS decided_at   TIMESTAMPTZ;
-- One row per second of film per way of judging it, so a movie can hold the
-- walkthrough's run and the plot's at once and the two be compared.
CREATE UNIQUE INDEX IF NOT EXISTS ux_fmki_movie_position_story
    ON frl.frl_movie_key_images (movie_id, position_seconds, COALESCE(story_from, ''));
CREATE INDEX IF NOT EXISTS idx_fmki_movie_decision
    ON frl.frl_movie_key_images (movie_id, decision);"
                + MoviePreparationStore.Schema;
            await using var cmd = new NpgsqlCommand(sql, _connection);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        /// <summary>Admins, and the tagger the movie is allocated to, may pick frames.</summary>
        private async Task<bool> CanCaptureAsync(int movieId, string actingUser, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(actingUser)) return false;
            if (await IsAdminAsync(actingUser, ct)) return true;

            const string sql = @"
SELECT 1 FROM frl.frl_movie_tagger_assignments
WHERE movie_id = @movieId AND lower(tagger) = lower(@actingUser) LIMIT 1;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            cmd.Parameters.AddWithValue("@actingUser", actingUser);
            return await cmd.ExecuteScalarAsync(ct) != null;
        }

        private async Task<bool> IsAdminAsync(string? name, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(name)) return false;
            const string sql =
                "SELECT is_admin FROM frl.frl_camera_movement_users WHERE lower(name) = lower(@name) LIMIT 1;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@name", name.Trim());
            return await cmd.ExecuteScalarAsync(ct) is bool isAdmin && isAdmin;
        }

        private async Task<bool> UserExistsAsync(string name, CancellationToken ct)
        {
            const string sql =
                "SELECT 1 FROM frl.frl_camera_movement_users WHERE lower(name) = lower(@name) LIMIT 1;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@name", name);
            return await cmd.ExecuteScalarAsync(ct) != null;
        }
    }
}
