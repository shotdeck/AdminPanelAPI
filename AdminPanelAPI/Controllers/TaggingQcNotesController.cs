using AdminPanelAPI.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Data;

namespace ShotDeckSearch.Controllers
{
    /// <summary>
    /// QC notes for the tagger hub. An admin leaves notes on a tagged title, or
    /// on one of its frames and tag categories; the title's tagger sees it under
    /// Address QC Notes and marks each note addressed, with an optional reply.
    /// </summary>
    [ApiController]
    [Route("api/admin/movie-tagging/qc-notes")]
    public sealed class TaggingQcNotesController : ControllerBase
    {
        private readonly NpgsqlConnection _connection;

        public TaggingQcNotesController(NpgsqlConnection connection)
        {
            _connection = connection;
        }

        public sealed class NoteRequest
        {
            public int MovieId { get; set; }
            public long? KeyImageId { get; set; }
            public string? Category { get; set; }
            public string? Note { get; set; }
            public string? ActingUser { get; set; }
        }

        public sealed class AddressRequest
        {
            public string? Reply { get; set; }
            public string? ActingUser { get; set; }
        }

        public sealed class ActingRequest
        {
            public string? ActingUser { get; set; }
        }

        /// <summary>Notes, newest first: for one title, one tagger's titles, or all; open ones only if asked.</summary>
        [HttpGet("")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> GetNotes(
            [FromQuery] int? movieId = null,
            [FromQuery] string? tagger = null,
            [FromQuery] bool open = false,
            CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);

            var conditions = new List<string>();
            if (movieId is > 0) conditions.Add("q.movie_id = @movieId");
            if (!string.IsNullOrWhiteSpace(tagger)) conditions.Add("lower(a.tagger) = lower(@tagger)");
            if (open) conditions.Add("q.addressed_at IS NULL");
            var filter = conditions.Count == 0 ? "" : " WHERE " + string.Join(" AND ", conditions);

            await using var cmd = new NpgsqlCommand($@"
SELECT q.id, q.movie_id, q.key_image_id, q.category, q.note, q.created_by, q.created_at,
       q.reply, q.addressed_by, q.addressed_at, a.tagger, COALESCE(k.position_seconds, q.position_seconds)
FROM frl.frl_tagging_qc_notes q
LEFT JOIN frl.frl_movie_tagger_assignments a ON a.movie_id = q.movie_id
LEFT JOIN frl.frl_movie_key_images k ON k.id = q.key_image_id{filter}
ORDER BY q.created_at DESC, q.id DESC;", _connection);
            if (movieId is > 0) cmd.Parameters.AddWithValue("@movieId", movieId.Value);
            if (!string.IsNullOrWhiteSpace(tagger)) cmd.Parameters.AddWithValue("@tagger", tagger.Trim());

            var notes = new List<object>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                notes.Add(new
                {
                    id = reader.GetInt64(0),
                    movieId = reader.GetInt32(1),
                    keyImageId = reader.IsDBNull(2) ? (long?)null : reader.GetInt64(2),
                    category = reader.IsDBNull(3) ? null : reader.GetString(3),
                    note = reader.GetString(4),
                    createdBy = reader.IsDBNull(5) ? null : reader.GetString(5),
                    createdAt = reader.GetDateTime(6),
                    reply = reader.IsDBNull(7) ? null : reader.GetString(7),
                    addressedBy = reader.IsDBNull(8) ? null : reader.GetString(8),
                    addressedAt = reader.IsDBNull(9) ? (DateTime?)null : reader.GetDateTime(9),
                    tagger = reader.IsDBNull(10) ? null : reader.GetString(10),
                    positionSeconds = reader.IsDBNull(11) ? (double?)null : (double)reader.GetDecimal(11)
                });
            }
            return Ok(new { notes });
        }

        /// <summary>An admin leaves a note on a title, or on one of its frames.</summary>
        [HttpPost("")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<IActionResult> AddNote(
            [FromBody] NoteRequest request, CancellationToken ct = default)
        {
            var text = (request.Note ?? "").Trim();
            if (request.MovieId <= 0)
                return BadRequest(new { error = "movieId is required." });
            if (text == "")
                return BadRequest(new { error = "note is required." });
            if (text.Length > TaggingQcNoteStore.MaxNoteLength)
                return BadRequest(new { error = $"note must be {TaggingQcNoteStore.MaxNoteLength} characters or fewer." });

            await EnsureReadyAsync(ct);
            if (!await IsAdminAsync(request.ActingUser, ct))
                return StatusCode(403, new { error = "Only an admin can leave QC notes." });

            if (request.KeyImageId != null)
            {
                await using var frame = new NpgsqlCommand(
                    "SELECT 1 FROM frl.frl_movie_key_images WHERE id = @id AND movie_id = @movieId;", _connection);
                frame.Parameters.AddWithValue("@id", request.KeyImageId.Value);
                frame.Parameters.AddWithValue("@movieId", request.MovieId);
                if (await frame.ExecuteScalarAsync(ct) == null)
                    return BadRequest(new { error = "That frame isn't one of this title's." });
            }

            await using var cmd = new NpgsqlCommand(@"
INSERT INTO frl.frl_tagging_qc_notes (movie_id, key_image_id, position_seconds, category, note, created_by)
VALUES (@movieId, @keyImageId,
        (SELECT position_seconds FROM frl.frl_movie_key_images WHERE id = @keyImageId),
        @category, @note, @actingUser)
RETURNING id;", _connection);
            cmd.Parameters.AddWithValue("@movieId", request.MovieId);
            cmd.Parameters.Add(new NpgsqlParameter("@keyImageId", NpgsqlTypes.NpgsqlDbType.Bigint)
                { Value = (object?)request.KeyImageId ?? DBNull.Value });
            cmd.Parameters.Add(new NpgsqlParameter("@category", NpgsqlTypes.NpgsqlDbType.Varchar)
                { Value = (object?)NullIfBlank(request.Category) ?? DBNull.Value });
            cmd.Parameters.AddWithValue("@note", text);
            cmd.Parameters.AddWithValue("@actingUser", request.ActingUser!.Trim());
            return Ok(new { id = (long)(await cmd.ExecuteScalarAsync(ct))! });
        }

        /// <summary>The title's tagger (or an admin) marks a note addressed.</summary>
        [HttpPut("{id:long}/address")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<IActionResult> Address(
            long id, [FromBody] AddressRequest request, CancellationToken ct = default)
        {
            var actingUser = (request.ActingUser ?? "").Trim();
            if (actingUser == "")
                return BadRequest(new { error = "actingUser is required." });
            var reply = NullIfBlank(request.Reply);
            if (reply != null && reply.Length > TaggingQcNoteStore.MaxNoteLength)
                return BadRequest(new { error = $"reply must be {TaggingQcNoteStore.MaxNoteLength} characters or fewer." });

            await EnsureReadyAsync(ct);
            string? tagger;
            await using (var owner = new NpgsqlCommand(@"
SELECT a.tagger FROM frl.frl_tagging_qc_notes q
LEFT JOIN frl.frl_movie_tagger_assignments a ON a.movie_id = q.movie_id
WHERE q.id = @id;", _connection))
            {
                owner.Parameters.AddWithValue("@id", id);
                await using var reader = await owner.ExecuteReaderAsync(ct);
                if (!await reader.ReadAsync(ct))
                    return NotFound(new { error = "No such note." });
                tagger = reader.IsDBNull(0) ? null : reader.GetString(0);
            }
            var isTagger = tagger != null && string.Equals(tagger, actingUser, StringComparison.OrdinalIgnoreCase);
            if (!isTagger && !await IsAdminAsync(actingUser, ct))
                return StatusCode(403, new { error = "Only the title's tagger can address its QC notes." });

            await using var cmd = new NpgsqlCommand(@"
UPDATE frl.frl_tagging_qc_notes
SET reply = @reply, addressed_by = @actingUser, addressed_at = now()
WHERE id = @id;", _connection);
            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.Add(new NpgsqlParameter("@reply", NpgsqlTypes.NpgsqlDbType.Text)
                { Value = (object?)reply ?? DBNull.Value });
            cmd.Parameters.AddWithValue("@actingUser", actingUser);
            await cmd.ExecuteNonQueryAsync(ct);
            return Ok(new { id, addressed = true });
        }

        /// <summary>An admin sends an addressed note back to the tagger.</summary>
        [HttpPut("{id:long}/reopen")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<IActionResult> Reopen(
            long id, [FromBody] ActingRequest request, CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);
            if (!await IsAdminAsync(request.ActingUser, ct))
                return StatusCode(403, new { error = "Only an admin can reopen QC notes." });
            await using var cmd = new NpgsqlCommand(@"
UPDATE frl.frl_tagging_qc_notes
SET addressed_by = NULL, addressed_at = NULL
WHERE id = @id;", _connection);
            cmd.Parameters.AddWithValue("@id", id);
            var updated = await cmd.ExecuteNonQueryAsync(ct);
            return updated == 0 ? NotFound(new { error = "No such note." }) : Ok(new { id, addressed = false });
        }

        [HttpDelete("{id:long}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<IActionResult> Delete(
            long id, [FromQuery] string? actingUser = null, CancellationToken ct = default)
        {
            await EnsureReadyAsync(ct);
            if (!await IsAdminAsync(actingUser, ct))
                return StatusCode(403, new { error = "Only an admin can delete QC notes." });
            await using var cmd = new NpgsqlCommand(
                "DELETE FROM frl.frl_tagging_qc_notes WHERE id = @id;", _connection);
            cmd.Parameters.AddWithValue("@id", id);
            return Ok(new { deleted = await cmd.ExecuteNonQueryAsync(ct) > 0 });
        }

        private static string? NullIfBlank(string? value) =>
            string.IsNullOrWhiteSpace(value) ? null : value.Trim();

        private async Task EnsureReadyAsync(CancellationToken ct)
        {
            if (_connection.State != ConnectionState.Open)
                await _connection.OpenAsync(ct);
            await SchemaOnce.EnsureAsync(_connection, TaggingQcNoteStore.Schema, ct);
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
