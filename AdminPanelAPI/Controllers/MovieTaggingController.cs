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
    /// </summary>
    [ApiController]
    [Route("api/admin/movie-tagging")]
    public sealed class MovieTaggingController : ControllerBase
    {
        private const string PosterBaseUrl = "https://image.tmdb.org/t/p/w342";

        private static readonly string[] Statuses = { "not_started", "in_progress", "done" };

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

        /// <summary>
        /// Every allocation, newest first, with the movie's title and poster.
        /// Filtered to one tagger when asked, which is what a tagger's own
        /// screen requests.
        /// </summary>
        [HttpGet("assignments")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> GetAssignments(
            [FromQuery] string? tagger = null, CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);

            var filter = string.IsNullOrWhiteSpace(tagger)
                ? "" : " WHERE lower(a.tagger) = lower(@tagger)";

            var sql = $@"
SELECT a.movie_id, a.tagger, a.status, a.note, a.assigned_by, a.assigned_at, a.updated_at,
       COALESCE(m.title, '') AS title, m.year, m.media_type::text AS media_type, m.poster
FROM frl.frl_movie_tagger_assignments a
LEFT JOIN frl.frl_movies m ON m.idnum = a.movie_id{filter}
ORDER BY a.updated_at DESC;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            if (!string.IsNullOrWhiteSpace(tagger))
                cmd.Parameters.AddWithValue("@tagger", tagger.Trim());

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
                    poster = reader.IsDBNull(10) ? null : PosterBaseUrl + reader.GetString(10)
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

            var status = NormalizeStatus(request.Status) ?? "not_started";

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
    status = EXCLUDED.status,
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
        /// Move an allocation along. The admin may set any movie's status; a
        /// tagger only their own, which is how progress gets reported.
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
            var ownerFilter = await IsAdminAsync(actingUser, ct)
                ? "" : " AND lower(tagger) = lower(@actingUser)";

            var sql = $@"
UPDATE frl.frl_movie_tagger_assignments
SET status = @status, updated_at = now()
WHERE movie_id = @movieId{ownerFilter};";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            cmd.Parameters.AddWithValue("@status", status);
            cmd.Parameters.AddWithValue("@actingUser", actingUser);

            var affected = await cmd.ExecuteNonQueryAsync(ct);
            if (affected == 0)
                return StatusCode(403, new { error = "Not your assignment, or it no longer exists." });
            return Ok(new { movieId, status });
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
       COUNT(a.movie_id) FILTER (WHERE a.status = 'not_started') AS not_started,
       COUNT(a.movie_id) FILTER (WHERE a.status = 'in_progress') AS in_progress,
       COUNT(a.movie_id) FILTER (WHERE a.status = 'done') AS done
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
                    notStarted = (int)reader.GetInt64(1),
                    inProgress = (int)reader.GetInt64(2),
                    done = (int)reader.GetInt64(3)
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
    status       VARCHAR(24)  NOT NULL DEFAULT 'not_started',
    note         TEXT,
    assigned_by  VARCHAR(120),
    assigned_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fmta_tagger
    ON frl.frl_movie_tagger_assignments (lower(tagger));";
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
