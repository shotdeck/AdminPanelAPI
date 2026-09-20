using Npgsql;
using NpgsqlTypes;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    /// <summary>One category's reading of one key image.</summary>
    public sealed record KeyImageTagRow(
        string Category,
        string? AiValue,
        double? AiConfidence,
        string? Options,
        string? Value,
        string? DecidedBy,
        DateTime? DecidedAt);

    /// <summary>A kept frame and how far its technical tags have got.</summary>
    public sealed record KeyImageTagState(
        long Id,
        int MovieId,
        double PositionSeconds,
        string? ImageKey,
        string? Status,
        string? ModelVersion,
        string? Error,
        DateTime? TaggedAt,
        string? ConfirmedBy,
        DateTime? ConfirmedAt,
        List<KeyImageTagRow> Tags);

    /// <summary>A frame waiting to be read by the image tagger.</summary>
    public sealed record KeyImageTagClaim(long Id, string ImageKey);

    /// <summary>How far a movie's kept frames have got with the image tagger.</summary>
    public sealed record KeyImageTagProgress(
        int MovieId, int Kept, int Waiting, int Tagged, int Failed, int Confirmed);

    /// <summary>
    /// A term a tagger read differently from the model: one training example,
    /// kept whole so a later batch can be assembled without the live tables.
    /// </summary>
    public sealed record KeyImageTagChange(
        long Id,
        long KeyImageId,
        int MovieId,
        string? ImageKey,
        string Category,
        string? AiValue,
        double? AiConfidence,
        string? ModelVersion,
        string? PreviousValue,
        string? Value,
        string? ChangedBy,
        DateTime ChangedAt,
        DateTime? ExportedAt);

    /// <summary>
    /// The technical terms of a movie's kept key images: what the image tagger
    /// read off each frame, and what the tagger settled on. Shared by the
    /// endpoints the tagging page calls and by the worker that reads frames as
    /// they are kept, so both write the terms the same way.
    /// </summary>
    public static class KeyImageTagStore
    {
        public const string Pending = "pending";
        public const string Running = "running";
        public const string Tagged = "tagged";
        public const string Error = "error";

        /// <summary>
        /// Mirrors migrations/040 so a slot that has not had migrations run
        /// still works, as the rest of the tagging tables are handled.
        /// </summary>
        public const string Schema = @"
CREATE TABLE IF NOT EXISTS frl.frl_movie_key_images (
    id               BIGSERIAL     PRIMARY KEY,
    movie_id         INTEGER       NOT NULL,
    position_seconds NUMERIC(10,3) NOT NULL,
    thumbnail        TEXT,
    captured_by      VARCHAR(120),
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now()
);
ALTER TABLE frl.frl_movie_key_images
    ADD COLUMN IF NOT EXISTS decision  VARCHAR(16) NOT NULL DEFAULT 'kept',
    ADD COLUMN IF NOT EXISTS image_key TEXT;
CREATE TABLE IF NOT EXISTS frl.frl_movie_key_image_tags (
    id            BIGSERIAL    PRIMARY KEY,
    key_image_id  BIGINT       NOT NULL
        REFERENCES frl.frl_movie_key_images (id) ON DELETE CASCADE,
    category      VARCHAR(40)  NOT NULL,
    ai_value      TEXT,
    ai_confidence NUMERIC(6,4),
    options       JSONB,
    value         TEXT,
    decided_by    VARCHAR(120),
    decided_at    TIMESTAMPTZ,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_fmkit_image_category
    ON frl.frl_movie_key_image_tags (key_image_id, category);
ALTER TABLE frl.frl_movie_key_images
    ADD COLUMN IF NOT EXISTS tag_status        VARCHAR(16),
    ADD COLUMN IF NOT EXISTS tag_model_version VARCHAR(32),
    ADD COLUMN IF NOT EXISTS tag_error         TEXT,
    ADD COLUMN IF NOT EXISTS tagged_at         TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_fmki_tag_status
    ON frl.frl_movie_key_images (tag_status)
    WHERE tag_status IN ('pending', 'running');
ALTER TABLE frl.frl_movie_key_images
    ADD COLUMN IF NOT EXISTS tags_confirmed_by VARCHAR(120),
    ADD COLUMN IF NOT EXISTS tags_confirmed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS tag_started_at    TIMESTAMPTZ;
CREATE TABLE IF NOT EXISTS frl.frl_movie_key_image_tag_changes (
    id             BIGSERIAL    PRIMARY KEY,
    key_image_id   BIGINT       NOT NULL
        REFERENCES frl.frl_movie_key_images (id) ON DELETE CASCADE,
    movie_id       INTEGER      NOT NULL,
    category       VARCHAR(40)  NOT NULL,
    ai_value       TEXT,
    ai_confidence  NUMERIC(6,4),
    model_version  VARCHAR(32),
    previous_value TEXT,
    value          TEXT,
    changed_by     VARCHAR(120),
    changed_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    exported_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_fmkitc_movie
    ON frl.frl_movie_key_image_tag_changes (movie_id, changed_at);
CREATE INDEX IF NOT EXISTS idx_fmkitc_unexported
    ON frl.frl_movie_key_image_tag_changes (changed_at)
    WHERE exported_at IS NULL;";

        /// <summary>
        /// Queue kept frames of a movie to be read. Only frames with a picture
        /// in R2 can be read, and a frame already read is left alone unless the
        /// tagger asks for it again.
        /// </summary>
        public static async Task<int> QueueAsync(
            NpgsqlConnection connection,
            int movieId,
            long[]? ids,
            bool force,
            CancellationToken ct)
        {
            var sql = @"
UPDATE frl.frl_movie_key_images
SET tag_status = 'pending', tag_error = NULL
WHERE decision = 'kept'
  AND image_key IS NOT NULL
  AND movie_id = @movieId" +
                (ids is { Length: > 0 } ? " AND id = ANY(@ids)" : "") +
                (force ? "" : " AND (tag_status IS NULL OR tag_status = 'error')") + ";";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            if (ids is { Length: > 0 })
                cmd.Parameters.AddWithValue("@ids", ids);
            return await cmd.ExecuteNonQueryAsync(ct);
        }

        /// <summary>
        /// Mark one frame as waiting to be read, for a frame just kept. Nothing
        /// happens to a frame that has been read already.
        /// </summary>
        public static async Task QueueOneAsync(
            NpgsqlConnection connection, long id, CancellationToken ct)
        {
            const string sql = @"
UPDATE frl.frl_movie_key_images
SET tag_status = 'pending', tag_error = NULL
WHERE id = @id AND decision = 'kept' AND tag_status IS NULL;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@id", id);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        /// <summary>
        /// Put back the frames a pass claimed and never finished, so a run that
        /// was cut short does not leave them being read for ever.
        /// </summary>
        public static async Task<int> RequeueStaleAsync(
            NpgsqlConnection connection, int movieId, TimeSpan olderThan,
            CancellationToken ct)
        {
            const string sql = @"
UPDATE frl.frl_movie_key_images
SET tag_status = 'pending'
WHERE movie_id = @movieId
  AND tag_status = 'running'
  AND COALESCE(tag_started_at, now() - interval '1 day') < now() - @olderThan;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            cmd.Parameters.AddWithValue("@olderThan", olderThan);
            return await cmd.ExecuteNonQueryAsync(ct);
        }

        /// <summary>
        /// Take the next frames waiting to be read and mark them as being read,
        /// so two passes cannot send the same frame twice.
        /// </summary>
        public static async Task<List<KeyImageTagClaim>> ClaimAsync(
            NpgsqlConnection connection, int limit, CancellationToken ct,
            int? movieId = null)
        {
            var sql = @"
UPDATE frl.frl_movie_key_images
SET tag_status = 'running', tag_started_at = now()
WHERE id IN (
    SELECT id FROM frl.frl_movie_key_images
    WHERE tag_status = 'pending' AND decision = 'kept' AND image_key IS NOT NULL" +
                (movieId.HasValue ? " AND movie_id = @movieId" : "") + @"
    ORDER BY id
    LIMIT @limit
)
RETURNING id, image_key;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@limit", limit);
            if (movieId.HasValue)
                cmd.Parameters.AddWithValue("@movieId", movieId.Value);

            var claims = new List<KeyImageTagClaim>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                claims.Add(new KeyImageTagClaim(reader.GetInt64(0), reader.GetString(1)));
            return claims;
        }

        /// <summary>
        /// Store one image's reading. A category the tagger has already settled
        /// keeps their value: re-reading refreshes the model's guess, it does not
        /// overrule a person.
        /// </summary>
        public static async Task SaveReadingAsync(
            NpgsqlConnection connection,
            long id,
            string? modelVersion,
            JsonElement tags,
            CancellationToken ct)
        {
            var categories = new List<string>();
            var values = new List<string?>();
            var confidences = new List<decimal?>();
            var options = new List<string>();

            foreach (var tag in tags.EnumerateObject())
            {
                if (tag.Value.ValueKind != JsonValueKind.Object)
                    continue;

                categories.Add(tag.Name);
                values.Add(Text(tag.Value, "value"));
                confidences.Add(
                    tag.Value.TryGetProperty("confidence", out var confidence) &&
                    confidence.ValueKind == JsonValueKind.Number
                        ? Math.Round(confidence.GetDecimal(), 4)
                        : null);
                options.Add(
                    tag.Value.TryGetProperty("options", out var choices) &&
                    choices.ValueKind == JsonValueKind.Array
                        ? choices.GetRawText()
                        : "[]");
            }

            const string sql = @"
INSERT INTO frl.frl_movie_key_image_tags
    (key_image_id, category, ai_value, ai_confidence, options)
SELECT @id, category, ai_value, ai_confidence, options::jsonb
FROM unnest(@categories, @values, @confidences, @options)
    AS tag(category, ai_value, ai_confidence, options)
ON CONFLICT (key_image_id, category) DO UPDATE
    SET ai_value      = EXCLUDED.ai_value,
        ai_confidence = EXCLUDED.ai_confidence,
        options       = EXCLUDED.options;

UPDATE frl.frl_movie_key_images
SET tag_status = 'tagged',
    tag_model_version = @modelVersion,
    tag_error = NULL,
    tagged_at = now()
WHERE id = @id;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.AddWithValue("@categories", categories.ToArray());
            cmd.Parameters.Add(new NpgsqlParameter("@values", NpgsqlDbType.Array | NpgsqlDbType.Text)
            {
                Value = values.Select(value => (object?)value ?? DBNull.Value).ToArray()
            });
            cmd.Parameters.Add(new NpgsqlParameter("@confidences", NpgsqlDbType.Array | NpgsqlDbType.Numeric)
            {
                Value = confidences.Select(value => (object?)value ?? DBNull.Value).ToArray()
            });
            cmd.Parameters.AddWithValue("@options", options.ToArray());
            cmd.Parameters.AddWithValue("@modelVersion", (object?)modelVersion ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        /// <summary>Why a frame could not be read, so the page can say so.</summary>
        public static async Task SaveErrorAsync(
            NpgsqlConnection connection, long id, string? error, CancellationToken ct)
        {
            const string sql = @"
UPDATE frl.frl_movie_key_images
SET tag_status = 'error', tag_error = @error
WHERE id = @id;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.AddWithValue(
                "@error", (object?)Shorten(error) ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        /// <summary>
        /// Every kept frame of a movie with its terms, earliest first. Frames
        /// nobody has read yet come back with no terms and no status, which is
        /// what the page shows as "not read yet".
        /// </summary>
        public static async Task<List<KeyImageTagState>> ReadMovieAsync(
            NpgsqlConnection connection, int movieId, CancellationToken ct)
        {
            const string sql = @"
SELECT k.id, k.movie_id, k.position_seconds, k.image_key, k.tag_status,
       k.tag_model_version, k.tag_error, k.tagged_at,
       k.tags_confirmed_by, k.tags_confirmed_at,
       t.category, t.ai_value, t.ai_confidence, t.options, t.value,
       t.decided_by, t.decided_at
FROM frl.frl_movie_key_images k
LEFT JOIN frl.frl_movie_key_image_tags t ON t.key_image_id = k.id
WHERE k.movie_id = @movieId AND k.decision = 'kept'
ORDER BY k.position_seconds, t.category;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);

            var images = new List<KeyImageTagState>();
            KeyImageTagState? current = null;

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var id = reader.GetInt64(0);
                if (current?.Id != id)
                {
                    current = new KeyImageTagState(
                        id,
                        reader.GetInt32(1),
                        (double)reader.GetDecimal(2),
                        reader.IsDBNull(3) ? null : reader.GetString(3),
                        reader.IsDBNull(4) ? null : reader.GetString(4),
                        reader.IsDBNull(5) ? null : reader.GetString(5),
                        reader.IsDBNull(6) ? null : reader.GetString(6),
                        reader.IsDBNull(7) ? null : reader.GetDateTime(7),
                        reader.IsDBNull(8) ? null : reader.GetString(8),
                        reader.IsDBNull(9) ? null : reader.GetDateTime(9),
                        new List<KeyImageTagRow>());
                    images.Add(current);
                }

                if (reader.IsDBNull(10))
                    continue;

                current.Tags.Add(new KeyImageTagRow(
                    reader.GetString(10),
                    reader.IsDBNull(11) ? null : reader.GetString(11),
                    reader.IsDBNull(12) ? null : (double)reader.GetDecimal(12),
                    reader.IsDBNull(13) ? null : reader.GetString(13),
                    reader.IsDBNull(14) ? null : reader.GetString(14),
                    reader.IsDBNull(15) ? null : reader.GetString(15),
                    reader.IsDBNull(16) ? null : reader.GetDateTime(16)));
            }

            return images;
        }

        /// <summary>
        /// What the tagger settled on for one image. A category they left out is
        /// untouched; a category they cleared is stored as an explicit "none of
        /// these", which is a different thing from never having looked.
        /// </summary>
        public static async Task SaveDecisionsAsync(
            NpgsqlConnection connection,
            long id,
            IReadOnlyDictionary<string, string> chosen,
            string actingUser,
            CancellationToken ct)
        {
            if (chosen.Count == 0)
                return;

            /* Every term the tagger reads differently from the model is written
             * down before the change lands, with what the model said and how
             * sure it was: that row is the training example. */
            const string sql = @"
INSERT INTO frl.frl_movie_key_image_tag_changes
    (key_image_id, movie_id, category, ai_value, ai_confidence, model_version,
     previous_value, value, changed_by)
SELECT k.id, k.movie_id, d.category, t.ai_value, t.ai_confidence,
       k.tag_model_version, t.value, NULLIF(d.value, ''), @actingUser
FROM unnest(@categories, @values) AS d(category, value)
JOIN frl.frl_movie_key_images k ON k.id = @id
LEFT JOIN frl.frl_movie_key_image_tags t
       ON t.key_image_id = k.id AND t.category = d.category
WHERE NULLIF(d.value, '') IS DISTINCT FROM COALESCE(t.value, t.ai_value);

INSERT INTO frl.frl_movie_key_image_tags
    (key_image_id, category, value, decided_by, decided_at)
SELECT @id, category, value, @actingUser, now()
FROM unnest(@categories, @values) AS decision(category, value)
ON CONFLICT (key_image_id, category) DO UPDATE
    SET value      = EXCLUDED.value,
        decided_by = EXCLUDED.decided_by,
        decided_at = EXCLUDED.decided_at;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.AddWithValue("@categories", chosen.Keys.ToArray());
            cmd.Parameters.AddWithValue("@values", chosen.Values.ToArray());
            cmd.Parameters.AddWithValue("@actingUser", actingUser);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        /// <summary>
        /// How many of each movie's kept frames have been read, for the AI tags
        /// stage on the tagging page. Only frames with a picture in R2 count,
        /// since those are the only ones that can be read.
        /// </summary>
        public static async Task<List<KeyImageTagProgress>> ProgressAsync(
            NpgsqlConnection connection, IEnumerable<int> movieIds, CancellationToken ct)
        {
            var ids = movieIds.Distinct().ToArray();
            if (ids.Length == 0)
                return new List<KeyImageTagProgress>();

            const string sql = @"
SELECT movie_id,
       COUNT(*)                                                    AS kept,
       COUNT(*) FILTER (WHERE tag_status IS NULL
                           OR tag_status IN ('pending', 'running')) AS waiting,
       COUNT(*) FILTER (WHERE tag_status = 'tagged')                AS tagged,
       COUNT(*) FILTER (WHERE tag_status = 'error')                 AS failed,
       COUNT(*) FILTER (WHERE tags_confirmed_at IS NOT NULL)         AS confirmed
FROM frl.frl_movie_key_images
WHERE decision = 'kept' AND image_key IS NOT NULL AND movie_id = ANY(@ids)
GROUP BY movie_id;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@ids", ids);

            var rows = new List<KeyImageTagProgress>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                rows.Add(new KeyImageTagProgress(
                    reader.GetInt32(0),
                    (int)reader.GetInt64(1),
                    (int)reader.GetInt64(2),
                    (int)reader.GetInt64(3),
                    (int)reader.GetInt64(4),
                    (int)reader.GetInt64(5)));
            return rows;
        }

        /// <summary>
        /// Tick an image off as reviewed, or take the tick back. The tick is the
        /// tagger's word that the terms on that frame are right, which is what
        /// makes its unchanged terms worth training on as well as its changed
        /// ones.
        /// </summary>
        public static async Task<bool> ConfirmAsync(
            NpgsqlConnection connection,
            long id,
            bool confirmed,
            string actingUser,
            CancellationToken ct)
        {
            const string sql = @"
UPDATE frl.frl_movie_key_images
SET tags_confirmed_by = CASE WHEN @confirmed THEN @actingUser END,
    tags_confirmed_at = CASE WHEN @confirmed THEN now() END
WHERE id = @id;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.AddWithValue("@confirmed", confirmed);
            cmd.Parameters.AddWithValue("@actingUser", actingUser);
            return await cmd.ExecuteNonQueryAsync(ct) > 0;
        }

        /// <summary>
        /// The terms taggers have read differently from the model, newest last,
        /// for the batch that retrains it. Optionally one movie's, and
        /// optionally only the ones no batch has taken yet.
        /// </summary>
        public static async Task<List<KeyImageTagChange>> ChangesAsync(
            NpgsqlConnection connection,
            int movieId,
            bool pendingOnly,
            int limit,
            CancellationToken ct)
        {
            var sql = @"
SELECT c.id, c.key_image_id, c.movie_id, k.image_key, c.category, c.ai_value,
       c.ai_confidence, c.model_version, c.previous_value, c.value,
       c.changed_by, c.changed_at, c.exported_at
FROM frl.frl_movie_key_image_tag_changes c
LEFT JOIN frl.frl_movie_key_images k ON k.id = c.key_image_id
WHERE TRUE" +
                (movieId > 0 ? " AND c.movie_id = @movieId" : "") +
                (pendingOnly ? " AND c.exported_at IS NULL" : "") + @"
ORDER BY c.id
LIMIT @limit;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            if (movieId > 0)
                cmd.Parameters.AddWithValue("@movieId", movieId);
            cmd.Parameters.AddWithValue("@limit", limit);

            var rows = new List<KeyImageTagChange>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                rows.Add(new KeyImageTagChange(
                    reader.GetInt64(0),
                    reader.GetInt64(1),
                    reader.GetInt32(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.GetString(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5),
                    reader.IsDBNull(6) ? null : (double)reader.GetDecimal(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7),
                    reader.IsDBNull(8) ? null : reader.GetString(8),
                    reader.IsDBNull(9) ? null : reader.GetString(9),
                    reader.IsDBNull(10) ? null : reader.GetString(10),
                    reader.GetDateTime(11),
                    reader.IsDBNull(12) ? null : reader.GetDateTime(12)));
            return rows;
        }

        /// <summary>
        /// Mark changes as taken by a retraining batch, so the next batch picks
        /// up where this one left off rather than relearning the same
        /// corrections.
        /// </summary>
        public static async Task<int> MarkChangesExportedAsync(
            NpgsqlConnection connection, long[] ids, CancellationToken ct)
        {
            if (ids.Length == 0)
                return 0;

            const string sql = @"
UPDATE frl.frl_movie_key_image_tag_changes
SET exported_at = now()
WHERE id = ANY(@ids) AND exported_at IS NULL;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@ids", ids);
            return await cmd.ExecuteNonQueryAsync(ct);
        }

        /// <summary>
        /// Move a movie on to the AI tags stage once every kept frame of it has
        /// been read. A frame that could not be read does not hold the movie
        /// back, or a single bad still would strand it for ever. A movie whose
        /// frames were all confirmed before this ran goes straight to the last
        /// stage rather than waiting for the next tick to be taken away and put
        /// back.
        /// </summary>
        public static async Task<int> AdvanceReadMoviesAsync(
            NpgsqlConnection connection, CancellationToken ct)
        {
            const string sql = @"
UPDATE frl.frl_movie_tagger_assignments a
SET status = CASE WHEN NOT EXISTS (
        SELECT 1 FROM frl.frl_movie_key_images k
        WHERE k.movie_id = a.movie_id AND k.decision = 'kept'
          AND k.image_key IS NOT NULL AND k.tag_status = 'tagged'
          AND k.tags_confirmed_at IS NULL)
    THEN 'tags_confirmed' ELSE 'ai_tags_read' END,
    ai_tags_at = COALESCE(a.ai_tags_at, now()),
    tags_confirmed_at = CASE WHEN NOT EXISTS (
        SELECT 1 FROM frl.frl_movie_key_images k
        WHERE k.movie_id = a.movie_id AND k.decision = 'kept'
          AND k.image_key IS NOT NULL AND k.tag_status = 'tagged'
          AND k.tags_confirmed_at IS NULL)
    THEN COALESCE(a.tags_confirmed_at, now()) END,
    updated_at = now()
WHERE a.status = 'key_images_extracted'
  AND EXISTS (
      SELECT 1 FROM frl.frl_movie_key_images k
      WHERE k.movie_id = a.movie_id AND k.decision = 'kept'
        AND k.image_key IS NOT NULL AND k.tag_status = 'tagged')
  AND NOT EXISTS (
      SELECT 1 FROM frl.frl_movie_key_images k
      WHERE k.movie_id = a.movie_id AND k.decision = 'kept' AND k.image_key IS NOT NULL
        AND (k.tag_status IS NULL OR k.tag_status IN ('pending', 'running')));";

            await using var cmd = new NpgsqlCommand(sql, connection);
            return await cmd.ExecuteNonQueryAsync(ct);
        }

        /// <summary>
        /// Settle a movie's stage against its ticks: tags_confirmed once every
        /// read frame has been stood by, back to ai_tags_read the moment one
        /// tick is taken away. A frame the tagger could not read is not waited
        /// on, matching how the movie reached the AI tags stage in the first
        /// place. Returns the stage the movie is left on, or null if it has no
        /// assignment row.
        /// </summary>
        public static async Task<string?> SettleConfirmedStageAsync(
            NpgsqlConnection connection, int movieId, CancellationToken ct)
        {
            const string sql = @"
WITH frames AS (
    SELECT COUNT(*) FILTER (WHERE tag_status = 'tagged')          AS read_frames,
           COUNT(*) FILTER (WHERE tag_status = 'tagged'
                              AND tags_confirmed_at IS NOT NULL)  AS confirmed,
           COUNT(*) FILTER (WHERE tag_status IS NULL
                              OR tag_status IN ('pending', 'running')) AS waiting
    FROM frl.frl_movie_key_images
    WHERE movie_id = @movieId AND decision = 'kept' AND image_key IS NOT NULL
)
UPDATE frl.frl_movie_tagger_assignments a
SET status = CASE WHEN f.done THEN 'tags_confirmed' ELSE 'ai_tags_read' END,
    tags_confirmed_at = CASE WHEN f.done
        THEN COALESCE(a.tags_confirmed_at, now()) END,
    ai_tags_at = COALESCE(a.ai_tags_at, now()),
    updated_at = now()
FROM (SELECT read_frames > 0 AND waiting = 0 AND confirmed = read_frames AS done
      FROM frames) f
WHERE a.movie_id = @movieId
  AND a.status IN ('ai_tags_read', 'tags_confirmed')
RETURNING a.status;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            return await cmd.ExecuteScalarAsync(ct) as string;
        }

        /// <summary>The movie a key image belongs to, for the allocation check.</summary>
        public static async Task<int?> MovieOfAsync(
            NpgsqlConnection connection, long id, CancellationToken ct)
        {
            const string sql =
                "SELECT movie_id FROM frl.frl_movie_key_images WHERE id = @id;";
            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@id", id);
            return await cmd.ExecuteScalarAsync(ct) as int?;
        }

        private static string? Text(JsonElement element, string name) =>
            element.TryGetProperty(name, out var value) &&
            value.ValueKind == JsonValueKind.String
                ? value.GetString()
                : null;

        private static string? Shorten(string? text) =>
            text is { Length: > 400 } ? text[..400] : text;
    }
}
