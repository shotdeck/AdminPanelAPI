using AdminPanelAPI.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Data;

namespace ShotDeckSearch.Controllers
{
    /// <summary>
    /// Drops for the tagger hub: an admin creates a drop with its drop and due
    /// dates, puts titles in it, marks any priority, and gives a last-minute
    /// title its own due date. Moving a title to another drop gives it that
    /// drop's due date. Everyone may read; only an admin may change anything.
    /// </summary>
    [ApiController]
    [Route("api/admin/movie-tagging/drops")]
    public sealed class TaggingDropsController : ControllerBase
    {
        private const string PosterBaseUrl = "https://image.tmdb.org/t/p/w342";

        private readonly NpgsqlConnection _connection;

        public TaggingDropsController(NpgsqlConnection connection)
        {
            _connection = connection;
        }

        public sealed class DropRequest
        {
            public string? DropDate { get; set; }
            public string? DueDate { get; set; }
            public string? Note { get; set; }
            public string? ActingUser { get; set; }
        }

        public sealed class TitlesRequest
        {
            public List<int>? MovieIds { get; set; }
            /// <summary>The drop to put the titles in; null takes them out of any drop.</summary>
            public long? DropId { get; set; }
            public string? ActingUser { get; set; }
        }

        public sealed class TitleRequest
        {
            public bool? Priority { get; set; }
            /// <summary>The title's own due date; "" goes back to the drop's.</summary>
            public string? DueDate { get; set; }
            public string? ActingUser { get; set; }
        }

        public sealed class SettingsRequest
        {
            public Dictionary<string, int>? Values { get; set; }
            public string? ActingUser { get; set; }
        }

        /// <summary>Every drop, soonest first, with its titles and the hub's settings.</summary>
        [HttpGet("")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> GetDrops(CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);

            var drops = new List<Dictionary<string, object?>>();
            var byId = new Dictionary<long, List<object>>();
            await using (var cmd = new NpgsqlCommand(@"
SELECT id, drop_date, due_date, note
FROM frl.frl_tagging_drops
ORDER BY drop_date;", _connection))
            await using (var reader = await cmd.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var titles = new List<object>();
                    var id = reader.GetInt64(0);
                    byId[id] = titles;
                    drops.Add(new Dictionary<string, object?>
                    {
                        ["id"] = id,
                        ["dropDate"] = TaggingDropStore.Format(TaggingDropStore.ReadDate(reader, 1)),
                        ["dueDate"] = TaggingDropStore.Format(TaggingDropStore.ReadDate(reader, 2)),
                        ["note"] = reader.IsDBNull(3) ? null : reader.GetString(3),
                        ["titles"] = titles
                    });
                }
            }

            await using (var cmd = new NpgsqlCommand(@"
SELECT t.movie_id, t.drop_id, t.priority, t.due_date_override,
       COALESCE(t.due_date_override, d.due_date),
       COALESCE(m.title, ''), m.year, m.media_type::text, m.poster,
       a.tagger, a.status
FROM frl.frl_tagging_drop_titles t
JOIN frl.frl_tagging_drops d ON d.id = t.drop_id
LEFT JOIN frl.frl_movies m ON m.idnum = t.movie_id
LEFT JOIN frl.frl_movie_tagger_assignments a ON a.movie_id = t.movie_id
ORDER BY t.priority DESC, COALESCE(m.title, ''), t.movie_id;", _connection))
            await using (var reader = await cmd.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    if (!byId.TryGetValue(reader.GetInt64(1), out var titles)) continue;
                    titles.Add(new
                    {
                        movieId = reader.GetInt32(0),
                        priority = reader.GetBoolean(2),
                        dueDateOverride = reader.IsDBNull(3)
                            ? null : TaggingDropStore.Format(TaggingDropStore.ReadDate(reader, 3)),
                        dueDate = TaggingDropStore.Format(TaggingDropStore.ReadDate(reader, 4)),
                        title = reader.GetString(5),
                        year = reader.IsDBNull(6) ? (int?)null : reader.GetInt32(6),
                        mediaType = reader.IsDBNull(7) ? "" : reader.GetString(7),
                        poster = reader.IsDBNull(8) ? null : PosterBaseUrl + reader.GetString(8),
                        tagger = reader.IsDBNull(9) ? null : reader.GetString(9),
                        status = reader.IsDBNull(10) ? null : reader.GetString(10)
                    });
                }
            }

            var settings = await TaggingDropStore.SettingsAsync(_connection, ct);
            return Ok(new { drops, settings });
        }

        /// <summary>
        /// Creates a drop, or updates the one already on that drop date. Without
        /// a due date it is due the set number of days before the drop.
        /// </summary>
        [HttpPost("")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<IActionResult> SaveDrop([FromBody] DropRequest request, CancellationToken ct = default)
        {
            var dropDate = TaggingDropStore.Parse(request.DropDate);
            if (dropDate == null)
                return BadRequest(new { error = "dropDate must be a date (yyyy-MM-dd)." });
            if (!string.IsNullOrWhiteSpace(request.DueDate) && TaggingDropStore.Parse(request.DueDate) == null)
                return BadRequest(new { error = "dueDate must be a date (yyyy-MM-dd)." });

            await EnsureReadyAsync(ct);
            if (!await IsAdminAsync(request.ActingUser, ct))
                return StatusCode(403, new { error = "Only an admin can set up drops." });

            var dueDate = TaggingDropStore.Parse(request.DueDate) ?? TaggingDropStore.DefaultDue(
                dropDate.Value,
                (await TaggingDropStore.SettingsAsync(_connection, ct))[TaggingDropStore.DueDaysBeforeDrop]);

            await using var cmd = new NpgsqlCommand(@"
INSERT INTO frl.frl_tagging_drops (drop_date, due_date, note, created_by)
VALUES (@dropDate, @dueDate, @note, @actingUser)
ON CONFLICT (drop_date) DO UPDATE
SET due_date = EXCLUDED.due_date,
    note = COALESCE(EXCLUDED.note, frl_tagging_drops.note),
    updated_at = now()
RETURNING id;", _connection);
            cmd.Parameters.AddWithValue("@dropDate", dropDate.Value);
            cmd.Parameters.AddWithValue("@dueDate", dueDate);
            cmd.Parameters.AddWithValue("@note", (object?)NullIfBlank(request.Note) ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@actingUser", request.ActingUser!.Trim());
            var id = (long)(await cmd.ExecuteScalarAsync(ct))!;

            return Ok(new
            {
                id,
                dropDate = TaggingDropStore.Format(dropDate),
                dueDate = TaggingDropStore.Format(dueDate)
            });
        }

        /// <summary>Changes a drop's dates or note. Its titles without their own due date follow it.</summary>
        [HttpPut("{id:long}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        public async Task<IActionResult> UpdateDrop(
            long id, [FromBody] DropRequest request, CancellationToken ct = default)
        {
            var dropDate = TaggingDropStore.Parse(request.DropDate);
            var dueDate = TaggingDropStore.Parse(request.DueDate);
            if (!string.IsNullOrWhiteSpace(request.DropDate) && dropDate == null)
                return BadRequest(new { error = "dropDate must be a date (yyyy-MM-dd)." });
            if (!string.IsNullOrWhiteSpace(request.DueDate) && dueDate == null)
                return BadRequest(new { error = "dueDate must be a date (yyyy-MM-dd)." });

            await EnsureReadyAsync(ct);
            if (!await IsAdminAsync(request.ActingUser, ct))
                return StatusCode(403, new { error = "Only an admin can change drops." });

            await using var cmd = new NpgsqlCommand(@"
UPDATE frl.frl_tagging_drops
SET drop_date = COALESCE(@dropDate, drop_date),
    due_date = COALESCE(@dueDate, due_date),
    note = CASE WHEN @hasNote THEN @note ELSE note END,
    updated_at = now()
WHERE id = @id
RETURNING drop_date, due_date;", _connection);
            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.Add(new NpgsqlParameter("@dropDate", NpgsqlTypes.NpgsqlDbType.Date)
                { Value = (object?)dropDate ?? DBNull.Value });
            cmd.Parameters.Add(new NpgsqlParameter("@dueDate", NpgsqlTypes.NpgsqlDbType.Date)
                { Value = (object?)dueDate ?? DBNull.Value });
            cmd.Parameters.AddWithValue("@hasNote", request.Note != null);
            cmd.Parameters.Add(new NpgsqlParameter("@note", NpgsqlTypes.NpgsqlDbType.Text)
                { Value = (object?)NullIfBlank(request.Note) ?? DBNull.Value });

            try
            {
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                if (!await reader.ReadAsync(ct))
                    return NotFound(new { error = "No such drop." });
                return Ok(new
                {
                    id,
                    dropDate = TaggingDropStore.Format(TaggingDropStore.ReadDate(reader, 0)),
                    dueDate = TaggingDropStore.Format(TaggingDropStore.ReadDate(reader, 1))
                });
            }
            catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UniqueViolation)
            {
                return Conflict(new { error = "There is already a drop on that date." });
            }
        }

        /// <summary>Deletes a drop. Its titles are left in no drop.</summary>
        [HttpDelete("{id:long}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<IActionResult> DeleteDrop(
            long id, [FromQuery] string? actingUser = null, CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);
            if (!await IsAdminAsync(actingUser, ct))
                return StatusCode(403, new { error = "Only an admin can delete drops." });

            await using var cmd = new NpgsqlCommand(
                "DELETE FROM frl.frl_tagging_drops WHERE id = @id;", _connection);
            cmd.Parameters.AddWithValue("@id", id);
            var deleted = await cmd.ExecuteNonQueryAsync(ct);
            return Ok(new { deleted = deleted > 0 });
        }

        /// <summary>
        /// Puts titles in a drop, or takes them out of any with no dropId. A
        /// title that changes drop loses its own due date and takes the new
        /// drop's; its priority stays.
        /// </summary>
        [HttpPut("titles")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<IActionResult> SetTitles(
            [FromBody] TitlesRequest request, CancellationToken ct = default)
        {
            var movieIds = (request.MovieIds ?? new List<int>()).Where(id => id > 0).Distinct().ToArray();
            if (movieIds.Length == 0)
                return BadRequest(new { error = "movieIds is required." });

            await EnsureReadyAsync(ct);
            if (!await IsAdminAsync(request.ActingUser, ct))
                return StatusCode(403, new { error = "Only an admin can put titles in drops." });

            if (request.DropId == null)
            {
                await using var remove = new NpgsqlCommand(
                    "DELETE FROM frl.frl_tagging_drop_titles WHERE movie_id = ANY(@ids);", _connection);
                remove.Parameters.AddWithValue("@ids", movieIds);
                return Ok(new { updated = await remove.ExecuteNonQueryAsync(ct) });
            }

            await using (var exists = new NpgsqlCommand(
                "SELECT 1 FROM frl.frl_tagging_drops WHERE id = @id;", _connection))
            {
                exists.Parameters.AddWithValue("@id", request.DropId.Value);
                if (await exists.ExecuteScalarAsync(ct) == null)
                    return NotFound(new { error = "No such drop." });
            }

            await using var cmd = new NpgsqlCommand(@"
INSERT INTO frl.frl_tagging_drop_titles (movie_id, drop_id, updated_by)
SELECT id, @dropId, @actingUser FROM unnest(@ids) AS id
ON CONFLICT (movie_id) DO UPDATE
SET drop_id = EXCLUDED.drop_id,
    due_date_override = CASE
        WHEN frl_tagging_drop_titles.drop_id = EXCLUDED.drop_id
            THEN frl_tagging_drop_titles.due_date_override
        ELSE NULL
    END,
    updated_by = EXCLUDED.updated_by,
    updated_at = now();", _connection);
            cmd.Parameters.AddWithValue("@dropId", request.DropId.Value);
            cmd.Parameters.AddWithValue("@ids", movieIds);
            cmd.Parameters.AddWithValue("@actingUser", request.ActingUser!.Trim());
            return Ok(new { updated = await cmd.ExecuteNonQueryAsync(ct) });
        }

        /// <summary>Marks a title in a drop priority, or gives it its own due date.</summary>
        [HttpPut("titles/{movieId:int}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<IActionResult> SetTitle(
            int movieId, [FromBody] TitleRequest request, CancellationToken ct = default)
        {
            var clearDue = request.DueDate != null && request.DueDate.Trim() == "";
            var dueDate = TaggingDropStore.Parse(request.DueDate);
            if (request.DueDate != null && !clearDue && dueDate == null)
                return BadRequest(new { error = "dueDate must be a date (yyyy-MM-dd), or empty to use the drop's." });

            await EnsureReadyAsync(ct);
            if (!await IsAdminAsync(request.ActingUser, ct))
                return StatusCode(403, new { error = "Only an admin can change a title's dates or priority." });

            await using var cmd = new NpgsqlCommand(@"
UPDATE frl.frl_tagging_drop_titles t
SET priority = COALESCE(@priority, t.priority),
    due_date_override = CASE
        WHEN @clearDue THEN NULL
        WHEN @dueDate IS NOT NULL THEN @dueDate
        ELSE t.due_date_override
    END,
    updated_by = @actingUser,
    updated_at = now()
FROM frl.frl_tagging_drops d
WHERE t.movie_id = @movieId AND d.id = t.drop_id
RETURNING t.priority, t.due_date_override, COALESCE(t.due_date_override, d.due_date);", _connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            cmd.Parameters.Add(new NpgsqlParameter("@priority", NpgsqlTypes.NpgsqlDbType.Boolean)
                { Value = (object?)request.Priority ?? DBNull.Value });
            cmd.Parameters.AddWithValue("@clearDue", clearDue);
            cmd.Parameters.Add(new NpgsqlParameter("@dueDate", NpgsqlTypes.NpgsqlDbType.Date)
                { Value = (object?)dueDate ?? DBNull.Value });
            cmd.Parameters.AddWithValue("@actingUser", request.ActingUser!.Trim());

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return NotFound(new { error = "That title isn't in a drop." });
            return Ok(new
            {
                movieId,
                priority = reader.GetBoolean(0),
                dueDateOverride = reader.IsDBNull(1)
                    ? null : TaggingDropStore.Format(TaggingDropStore.ReadDate(reader, 1)),
                dueDate = TaggingDropStore.Format(TaggingDropStore.ReadDate(reader, 2))
            });
        }

        public sealed class ClaimRequest
        {
            public int MovieId { get; set; }
            public string? ActingUser { get; set; }
        }

        /// <summary>
        /// What a tagger may add to their queue: the unassigned titles of the
        /// released open drops (still before their due date, and within the set
        /// weeks), with how many titles the tagger has in hand and may hold.
        /// </summary>
        [HttpGet("available")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> GetAvailable(
            [FromQuery] string? tagger = null, CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);
            var settings = await TaggingDropStore.SettingsAsync(_connection, ct);
            var released = await ReleasedDropsAsync(settings, ct);
            var inQueue = string.IsNullOrWhiteSpace(tagger) ? 0 : await InQueueAsync(tagger, ct);
            var max = settings[TaggingDropStore.MaxTitlesPerTagger];

            var titles = new List<object>();
            if (released.Count > 0)
            {
                await using var cmd = new NpgsqlCommand(@"
SELECT t.movie_id, d.drop_date, COALESCE(t.due_date_override, d.due_date), t.priority,
       COALESCE(m.title, ''), m.year, m.media_type::text, m.poster
FROM frl.frl_tagging_drop_titles t
JOIN frl.frl_tagging_drops d ON d.id = t.drop_id
LEFT JOIN frl.frl_movies m ON m.idnum = t.movie_id
WHERE t.drop_id = ANY(@drops)
  AND NOT EXISTS (SELECT 1 FROM frl.frl_movie_tagger_assignments a WHERE a.movie_id = t.movie_id)
ORDER BY d.drop_date, t.priority DESC, COALESCE(m.title, ''), t.movie_id;", _connection);
                cmd.Parameters.AddWithValue("@drops", released.Select(d => d.Id).ToArray());
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    titles.Add(new
                    {
                        movieId = reader.GetInt32(0),
                        dropDate = TaggingDropStore.Format(TaggingDropStore.ReadDate(reader, 1)),
                        dueDate = TaggingDropStore.Format(TaggingDropStore.ReadDate(reader, 2)),
                        priority = reader.GetBoolean(3),
                        title = reader.GetString(4),
                        year = reader.IsDBNull(5) ? (int?)null : reader.GetInt32(5),
                        mediaType = reader.IsDBNull(6) ? "" : reader.GetString(6),
                        poster = reader.IsDBNull(7) ? null : PosterBaseUrl + reader.GetString(7)
                    });
                }
            }

            return Ok(new
            {
                titles,
                drops = released.Select(d => TaggingDropStore.Format(d.DropDate)),
                inQueue,
                maxTitles = max,
                canAdd = Math.Max(0, max - inQueue)
            });
        }

        /// <summary>
        /// A tagger adds an available title to their own queue. The title is
        /// then theirs alone, and a full queue is refused.
        /// </summary>
        [HttpPost("claim")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        public async Task<IActionResult> Claim(
            [FromBody] ClaimRequest request, CancellationToken ct = default)
        {
            var tagger = (request.ActingUser ?? "").Trim();
            if (request.MovieId <= 0)
                return BadRequest(new { error = "movieId is required." });
            if (tagger == "")
                return BadRequest(new { error = "actingUser is required." });

            await EnsureReadyAsync(ct);
            await using (var user = new NpgsqlCommand(
                "SELECT 1 FROM frl.frl_camera_movement_users WHERE lower(name) = lower(@name) LIMIT 1;",
                _connection))
            {
                user.Parameters.AddWithValue("@name", tagger);
                if (await user.ExecuteScalarAsync(ct) == null)
                    return BadRequest(new { error = "Unknown tagger." });
            }

            var settings = await TaggingDropStore.SettingsAsync(_connection, ct);
            var released = await ReleasedDropsAsync(settings, ct);
            var max = settings[TaggingDropStore.MaxTitlesPerTagger];

            await using var tx = await _connection.BeginTransactionAsync(ct);
            await using (var lockCmd = new NpgsqlCommand(
                "SELECT pg_advisory_xact_lock(hashtext('tagging-claim:' || lower(@tagger)));",
                _connection, tx))
            {
                lockCmd.Parameters.AddWithValue("@tagger", tagger);
                await lockCmd.ExecuteNonQueryAsync(ct);
            }

            if (await InQueueAsync(tagger, ct, tx) >= max)
                return Conflict(new { error = $"You may have up to {max} titles in your queue." });

            await using (var inDrop = new NpgsqlCommand(
                "SELECT 1 FROM frl.frl_tagging_drop_titles WHERE movie_id = @movieId AND drop_id = ANY(@drops);",
                _connection, tx))
            {
                inDrop.Parameters.AddWithValue("@movieId", request.MovieId);
                inDrop.Parameters.AddWithValue("@drops", released.Select(d => d.Id).ToArray());
                if (await inDrop.ExecuteScalarAsync(ct) == null)
                    return Conflict(new { error = "That title isn't available to add." });
            }

            await using (var insert = new NpgsqlCommand(@"
INSERT INTO frl.frl_movie_tagger_assignments (movie_id, tagger, status, assigned_by)
VALUES (@movieId, @tagger, 'tagger_allocated', @tagger)
ON CONFLICT (movie_id) DO NOTHING;", _connection, tx))
            {
                insert.Parameters.AddWithValue("@movieId", request.MovieId);
                insert.Parameters.AddWithValue("@tagger", tagger);
                if (await insert.ExecuteNonQueryAsync(ct) == 0)
                    return Conflict(new { error = "Someone else has just added that title." });
            }

            await tx.CommitAsync(ct);
            return Ok(new { movieId = request.MovieId, tagger, status = "tagger_allocated" });
        }

        /// <summary>Titles a tagger has in hand: assigned and not yet confirmed.</summary>
        private async Task<int> InQueueAsync(string tagger, CancellationToken ct, NpgsqlTransaction? tx = null)
        {
            await using var cmd = new NpgsqlCommand(@"
SELECT COUNT(*) FROM frl.frl_movie_tagger_assignments
WHERE lower(tagger) = lower(@tagger) AND status <> 'tags_confirmed';", _connection, tx);
            cmd.Parameters.AddWithValue("@tagger", tagger.Trim());
            return (int)(long)(await cmd.ExecuteScalarAsync(ct) ?? 0L);
        }

        private async Task<List<TaggingDropStore.OpenDrop>> ReleasedDropsAsync(
            Dictionary<string, int> settings, CancellationToken ct)
        {
            var open = new List<TaggingDropStore.OpenDrop>();
            await using var cmd = new NpgsqlCommand(@"
SELECT d.id, d.drop_date, COUNT(t.movie_id), COUNT(a.movie_id)
FROM frl.frl_tagging_drops d
LEFT JOIN frl.frl_tagging_drop_titles t ON t.drop_id = d.id
LEFT JOIN frl.frl_movie_tagger_assignments a ON a.movie_id = t.movie_id
WHERE d.due_date >= CURRENT_DATE AND d.drop_date < CURRENT_DATE + @days
GROUP BY d.id, d.drop_date
ORDER BY d.drop_date;", _connection);
            cmd.Parameters.AddWithValue("@days", Math.Max(1, settings[TaggingDropStore.OpenDropWeeks]) * 7);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                open.Add(new TaggingDropStore.OpenDrop(reader.GetInt64(0),
                    reader.GetFieldValue<DateOnly>(1), (int)reader.GetInt64(2), (int)reader.GetInt64(3)));
            }
            return TaggingDropStore.Released(open, settings[TaggingDropStore.ReleaseNextDropPercent]);
        }

        [HttpGet("settings")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> GetSettings(CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);
            return Ok(new { settings = await TaggingDropStore.SettingsAsync(_connection, ct) });
        }

        /// <summary>Changes any of the hub's known settings; unknown names are refused.</summary>
        [HttpPut("settings")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<IActionResult> SetSettings(
            [FromBody] SettingsRequest request, CancellationToken ct = default)
        {
            var values = request.Values ?? new Dictionary<string, int>();
            var unknown = values.Keys.Where(k => !TaggingDropStore.DefaultSettings.ContainsKey(k)).ToList();
            if (unknown.Count > 0)
                return BadRequest(new { error = "Unknown settings: " + string.Join(", ", unknown) });
            if (values.Values.Any(v => v < 0 || v > 365))
                return BadRequest(new { error = "Settings must be between 0 and 365." });
            if (values.TryGetValue(TaggingDropStore.ReleaseNextDropPercent, out var percent) && percent > 100)
                return BadRequest(new { error = "release_next_drop_percent must be between 0 and 100." });

            await EnsureReadyAsync(ct);
            if (!await IsAdminAsync(request.ActingUser, ct))
                return StatusCode(403, new { error = "Only an admin can change these settings." });

            foreach (var (name, value) in values)
            {
                await using var cmd = new NpgsqlCommand(@"
INSERT INTO frl.frl_tagging_settings (name, value, updated_by)
VALUES (@name, @value, @actingUser)
ON CONFLICT (name) DO UPDATE
SET value = EXCLUDED.value, updated_by = EXCLUDED.updated_by, updated_at = now();", _connection);
                cmd.Parameters.AddWithValue("@name", name);
                cmd.Parameters.AddWithValue("@value", value);
                cmd.Parameters.AddWithValue("@actingUser", request.ActingUser!.Trim());
                await cmd.ExecuteNonQueryAsync(ct);
            }

            return Ok(new { settings = await TaggingDropStore.SettingsAsync(_connection, ct) });
        }

        private static string? NullIfBlank(string? value) =>
            string.IsNullOrWhiteSpace(value) ? null : value.Trim();

        private async Task EnsureReadyAsync(CancellationToken ct)
        {
            if (_connection.State != ConnectionState.Open)
                await _connection.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(TaggingDropStore.Schema, _connection);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        private async Task<bool> IsAdminAsync(string? name, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(name)) return false;
            await using var cmd = new NpgsqlCommand(
                "SELECT is_admin FROM frl.frl_camera_movement_users WHERE lower(name) = lower(@name) LIMIT 1;",
                _connection);
            cmd.Parameters.AddWithValue("@name", name.Trim());
            return await cmd.ExecuteScalarAsync(ct) is bool isAdmin && isAdmin;
        }
    }
}
