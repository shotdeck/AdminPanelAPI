using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Data;

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

        private readonly NpgsqlConnection _connection;

        public MovieTaggingController(NpgsqlConnection connection)
        {
            _connection = connection;
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
WHERE status IN ('not_started', 'in_progress');";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            await cmd.ExecuteNonQueryAsync(ct);
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
