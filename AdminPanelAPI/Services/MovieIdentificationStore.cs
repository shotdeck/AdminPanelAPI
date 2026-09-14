using Npgsql;
using NpgsqlTypes;

namespace AdminPanelAPI.Services
{
    /// <summary>What film an uploaded master was confirmed to be.</summary>
    public sealed class MovieIdentificationRow
    {
        public string SourceKey { get; init; } = "";
        public int TmdbId { get; init; }
        public int? MovieId { get; init; }
        public string Title { get; init; } = "";
        public int? Year { get; init; }
        public string? ImdbId { get; init; }
        public int? RuntimeMinutes { get; init; }
        public string? PosterPath { get; init; }
        public string Info { get; init; } = "{}";
        public string IdentifiedBy { get; init; } = "";
        public DateTimeOffset IdentifiedAt { get; init; }
        public DateTimeOffset UpdatedAt { get; init; }
    }

    /// <summary>
    /// Reads and writes a master's identification. Mirrors migrations/039 in
    /// code so a slot that has not had migrations run still works, the way the
    /// tagging and preparation tables do.
    /// </summary>
    public static class MovieIdentificationStore
    {
        public const string Schema = @"
CREATE TABLE IF NOT EXISTS frl.frl_movie_file_identification (
    source_key      TEXT         PRIMARY KEY,
    tmdb_id         INTEGER      NOT NULL,
    movie_id        INTEGER,
    title           TEXT         NOT NULL,
    year            INTEGER,
    imdb_id         VARCHAR(16),
    runtime_minutes INTEGER,
    poster_path     TEXT,
    info            JSONB        NOT NULL,
    identified_by   TEXT         NOT NULL,
    identified_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fmfi_movie ON frl.frl_movie_file_identification (movie_id);
CREATE INDEX IF NOT EXISTS idx_fmfi_tmdb  ON frl.frl_movie_file_identification (tmdb_id);";

        private const string Columns = @"source_key, tmdb_id, movie_id, title, year,
       imdb_id, runtime_minutes, poster_path, info::text AS info,
       identified_by, identified_at, updated_at";

        public static async Task EnsureTableAsync(NpgsqlConnection connection, CancellationToken ct)
        {
            await using var cmd = new NpgsqlCommand(Schema, connection);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public static async Task<MovieIdentificationRow?> GetAsync(
            NpgsqlConnection connection, string sourceKey, CancellationToken ct)
        {
            var sql = $@"
SELECT {Columns} FROM frl.frl_movie_file_identification
WHERE source_key = @sourceKey;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@sourceKey", sourceKey);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            return await reader.ReadAsync(ct) ? Read(reader) : null;
        }

        /// <summary>
        /// The identifications inside one folder, so the file browser can mark a
        /// whole listing in one query rather than a request per row.
        /// </summary>
        public static async Task<List<MovieIdentificationRow>> ListAsync(
            NpgsqlConnection connection, string prefix, CancellationToken ct)
        {
            var sql = $@"
SELECT {Columns} FROM frl.frl_movie_file_identification
WHERE source_key LIKE @prefix
ORDER BY source_key;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@prefix", prefix.Replace("%", "\\%") + "%");

            var rows = new List<MovieIdentificationRow>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                rows.Add(Read(reader));
            return rows;
        }

        /// <summary>The identifications held against a set of movies.</summary>
        public static async Task<List<MovieIdentificationRow>> ListForMoviesAsync(
            NpgsqlConnection connection, int[] movieIds, CancellationToken ct)
        {
            var rows = new List<MovieIdentificationRow>();
            if (movieIds.Length == 0) return rows;

            var sql = $@"
SELECT {Columns} FROM frl.frl_movie_file_identification
WHERE movie_id = ANY(@ids)
ORDER BY movie_id, source_key;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@ids", movieIds);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                rows.Add(Read(reader));
            return rows;
        }

        /// <summary>Whether any of a movie's files carry an identification.</summary>
        public static async Task<bool> AnyForMovieAsync(
            NpgsqlConnection connection, int movieId, CancellationToken ct)
        {
            const string sql = @"
SELECT 1 FROM frl.frl_movie_file_identification
WHERE movie_id = @movieId LIMIT 1;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            return await cmd.ExecuteScalarAsync(ct) != null;
        }

        /// <summary>
        /// Record (or correct) which film a master is. Re-identifying the same
        /// file replaces what was there, because the point of the step is to fix
        /// a wrong guess before anything downstream reads it.
        /// </summary>
        public static async Task<MovieIdentificationRow> UpsertAsync(
            NpgsqlConnection connection,
            string sourceKey,
            TmdbMovieDetails details,
            int? movieId,
            string info,
            string identifiedBy,
            CancellationToken ct)
        {
            var sql = $@"
INSERT INTO frl.frl_movie_file_identification
    (source_key, tmdb_id, movie_id, title, year, imdb_id, runtime_minutes,
     poster_path, info, identified_by, identified_at, updated_at)
VALUES
    (@sourceKey, @tmdbId, @movieId, @title, @year, @imdbId, @runtime,
     @posterPath, @info, @identifiedBy, now(), now())
ON CONFLICT (source_key) DO UPDATE
    SET tmdb_id         = EXCLUDED.tmdb_id,
        movie_id        = EXCLUDED.movie_id,
        title           = EXCLUDED.title,
        year            = EXCLUDED.year,
        imdb_id         = EXCLUDED.imdb_id,
        runtime_minutes = EXCLUDED.runtime_minutes,
        poster_path     = EXCLUDED.poster_path,
        info            = EXCLUDED.info,
        identified_by   = EXCLUDED.identified_by,
        updated_at      = now()
RETURNING {Columns};";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@sourceKey", sourceKey);
            cmd.Parameters.AddWithValue("@tmdbId", details.TmdbId);
            cmd.Parameters.AddWithValue("@movieId", (object?)movieId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@title", details.Title);
            cmd.Parameters.AddWithValue("@year", (object?)details.Year ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@imdbId", (object?)details.ImdbId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@runtime", (object?)details.RuntimeMinutes ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@posterPath", (object?)details.PosterPath ?? DBNull.Value);
            cmd.Parameters.Add(new NpgsqlParameter("@info", NpgsqlDbType.Jsonb) { Value = info });
            cmd.Parameters.AddWithValue("@identifiedBy", identifiedBy);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            return Read(reader);
        }

        /// <summary>
        /// Follow a file to its new key, so promoting a staging folder onto its
        /// movie id keeps the identification instead of losing it.
        /// </summary>
        public static async Task<int> RekeyAsync(
            NpgsqlConnection connection,
            string sourcePrefix,
            string targetPrefix,
            int? movieId,
            CancellationToken ct)
        {
            const string sql = @"
UPDATE frl.frl_movie_file_identification
SET source_key = @targetPrefix || substring(source_key from char_length(@sourcePrefix) + 1),
    movie_id   = COALESCE(@movieId, movie_id),
    updated_at = now()
WHERE source_key LIKE @like;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@sourcePrefix", sourcePrefix);
            cmd.Parameters.AddWithValue("@targetPrefix", targetPrefix);
            cmd.Parameters.AddWithValue("@movieId", (object?)movieId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@like", sourcePrefix.Replace("%", "\\%") + "%");
            return await cmd.ExecuteNonQueryAsync(ct);
        }

        private static MovieIdentificationRow Read(NpgsqlDataReader reader) => new()
        {
            SourceKey = reader.GetString(0),
            TmdbId = reader.GetInt32(1),
            MovieId = reader.IsDBNull(2) ? null : reader.GetInt32(2),
            Title = reader.GetString(3),
            Year = reader.IsDBNull(4) ? null : reader.GetInt32(4),
            ImdbId = reader.IsDBNull(5) ? null : reader.GetString(5),
            RuntimeMinutes = reader.IsDBNull(6) ? null : reader.GetInt32(6),
            PosterPath = reader.IsDBNull(7) ? null : reader.GetString(7),
            Info = reader.IsDBNull(8) ? "{}" : reader.GetString(8),
            IdentifiedBy = reader.GetString(9),
            IdentifiedAt = reader.GetFieldValue<DateTimeOffset>(10),
            UpdatedAt = reader.GetFieldValue<DateTimeOffset>(11)
        };
    }
}
