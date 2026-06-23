using AdminPanelAPI.Models;
using Npgsql;

public interface ICaptionEmbeddingJobRepository
{
    Task<long> CreateJobAsync(int batchSize, CancellationToken cancellationToken);
    Task<CaptionEmbeddingJobStatusResponse?> GetJobAsync(long jobId, CancellationToken cancellationToken);
    Task MarkRunningAsync(long jobId, CancellationToken cancellationToken);
    Task MarkCompletedAsync(long jobId, CancellationToken cancellationToken);
    Task MarkFailedAsync(long jobId, string error, CancellationToken cancellationToken);

    Task UpdateProgressAsync(
        long jobId,
        string step,
        int? current,
        int? total,
        CancellationToken cancellationToken);

    Task<List<ImageRecord>> GetUnprocessedImagesAsync(
        int limit,
        int afterId,
        CancellationToken cancellationToken);

    Task<int> GetMaxProcessedIdAsync(CancellationToken cancellationToken);

    Task<ImageRecord?> GetImageByIdAsync(
        int imageId,
        CancellationToken cancellationToken);

    Task<int> GetUnprocessedCountAsync(CancellationToken cancellationToken);

    Task<Dictionary<int, List<string>>> FetchTagsAsync(
        string tableName,
        string columnName,
        List<int> imageIds,
        CancellationToken cancellationToken);

    Task<Dictionary<int, (string? Title, string? Year, string? Genre, string? Product, string? Brand, string? MediaType, string? Director, string? Cinematographer)>>
        FetchMovieFieldsAsync(List<int> movieIds, CancellationToken cancellationToken);
}

public class CaptionEmbeddingJobRepository : ICaptionEmbeddingJobRepository
{
    private readonly string _connectionString;

    public CaptionEmbeddingJobRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("Default")
            ?? throw new InvalidOperationException("Missing connection string: Default");
    }

    public async Task<long> CreateJobAsync(int batchSize, CancellationToken cancellationToken)
    {
        const string sql = @"
CREATE TABLE IF NOT EXISTS frl.frl_caption_embedding_jobs (
    id BIGSERIAL PRIMARY KEY,
    batch_size INT NOT NULL,
    status TEXT NOT NULL DEFAULT 'Queued',
    current_step TEXT,
    progress_current INT,
    progress_total INT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT
);

INSERT INTO frl.frl_caption_embedding_jobs (batch_size, status)
VALUES (@batchSize, 'Queued')
RETURNING id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("batchSize", batchSize);

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(result);
    }

    public async Task<CaptionEmbeddingJobStatusResponse?> GetJobAsync(long jobId, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT
    id,
    batch_size,
    status,
    current_step,
    progress_current,
    progress_total,
    created_at,
    started_at,
    completed_at,
    error
FROM frl.frl_caption_embedding_jobs
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        if (await reader.ReadAsync(cancellationToken))
        {
            return new CaptionEmbeddingJobStatusResponse
            {
                JobId = reader.GetInt64(reader.GetOrdinal("id")),
                BatchSize = reader.GetInt32(reader.GetOrdinal("batch_size")),
                Status = reader.GetString(reader.GetOrdinal("status")),
                CurrentStep = reader.IsDBNull(reader.GetOrdinal("current_step"))
                    ? null : reader.GetString(reader.GetOrdinal("current_step")),
                ProgressCurrent = reader.IsDBNull(reader.GetOrdinal("progress_current"))
                    ? null : reader.GetInt32(reader.GetOrdinal("progress_current")),
                ProgressTotal = reader.IsDBNull(reader.GetOrdinal("progress_total"))
                    ? null : reader.GetInt32(reader.GetOrdinal("progress_total")),
                CreatedAt = reader.GetDateTime(reader.GetOrdinal("created_at")),
                StartedAt = reader.IsDBNull(reader.GetOrdinal("started_at"))
                    ? null : reader.GetDateTime(reader.GetOrdinal("started_at")),
                CompletedAt = reader.IsDBNull(reader.GetOrdinal("completed_at"))
                    ? null : reader.GetDateTime(reader.GetOrdinal("completed_at")),
                Error = reader.IsDBNull(reader.GetOrdinal("error"))
                    ? null : reader.GetString(reader.GetOrdinal("error")),
            };
        }

        return null;
    }

    public async Task MarkRunningAsync(long jobId, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_caption_embedding_jobs
SET status = 'Running', started_at = NOW()
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkCompletedAsync(long jobId, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_caption_embedding_jobs
SET status = 'Completed', completed_at = NOW()
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkFailedAsync(long jobId, string error, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_caption_embedding_jobs
SET status = 'Failed', error = @error, completed_at = NOW()
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);
        cmd.Parameters.AddWithValue("error", error);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpdateProgressAsync(
        long jobId,
        string step,
        int? current,
        int? total,
        CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_caption_embedding_jobs
SET current_step = @step,
    progress_current = @current,
    progress_total = @total
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);
        cmd.Parameters.AddWithValue("step", (object?)step ?? DBNull.Value);
        cmd.Parameters.AddWithValue("current", (object?)current ?? DBNull.Value);
        cmd.Parameters.AddWithValue("total", (object?)total ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<List<ImageRecord>> GetUnprocessedImagesAsync(
        int limit,
        int afterId,
        CancellationToken cancellationToken)
    {
        if (limit <= 0)
            return new List<ImageRecord>();

        const string sql = @"
SELECT i.idnum, i.filename, i.randid, i.movieid,
       i.format, i.optical_format, i.time_period,
       i.setting, i.location, i.filming_location,
       i.actors, i.int_ext,
       old.keyword_vectorized_metadata AS cached_metadata
FROM frl.frl_images i
LEFT JOIN frl.frl_caption_embeddings old ON old.idnum = i.idnum
WHERE i.status = 'live'
  AND i.filename IS NOT NULL
  AND i.idnum > @afterId
ORDER BY i.idnum
LIMIT @limit;";

        var results = new List<ImageRecord>();

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.CommandTimeout = 180;
        cmd.Parameters.AddWithValue("limit", limit);
        cmd.Parameters.AddWithValue("afterId", afterId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(new ImageRecord
            {
                Id = reader.GetInt32(reader.GetOrdinal("idnum")),
                Filename = reader.IsDBNull(reader.GetOrdinal("filename"))
                    ? null : reader.GetString(reader.GetOrdinal("filename")),
                Randid = reader.IsDBNull(reader.GetOrdinal("randid"))
                    ? null : reader.GetString(reader.GetOrdinal("randid")),
                MovieId = reader.IsDBNull(reader.GetOrdinal("movieid"))
                    ? null : reader.GetInt32(reader.GetOrdinal("movieid")),
                Format = reader.IsDBNull(reader.GetOrdinal("format"))
                    ? null : reader.GetValue(reader.GetOrdinal("format"))?.ToString(),
                OpticalFormat = reader.IsDBNull(reader.GetOrdinal("optical_format"))
                    ? null : reader.GetValue(reader.GetOrdinal("optical_format"))?.ToString(),
                TimePeriod = reader.IsDBNull(reader.GetOrdinal("time_period"))
                    ? null : reader.GetValue(reader.GetOrdinal("time_period"))?.ToString(),
                Setting = reader.IsDBNull(reader.GetOrdinal("setting"))
                    ? null : reader.GetValue(reader.GetOrdinal("setting"))?.ToString(),
                Location = reader.IsDBNull(reader.GetOrdinal("location"))
                    ? null : reader.GetValue(reader.GetOrdinal("location"))?.ToString(),
                FilmingLocation = reader.IsDBNull(reader.GetOrdinal("filming_location"))
                    ? null : reader.GetValue(reader.GetOrdinal("filming_location"))?.ToString(),
                Actors = reader.IsDBNull(reader.GetOrdinal("actors"))
                    ? null : reader.GetValue(reader.GetOrdinal("actors"))?.ToString(),
                IntExt = reader.IsDBNull(reader.GetOrdinal("int_ext"))
                    ? null : reader.GetValue(reader.GetOrdinal("int_ext"))?.ToString(),
                CachedMetadata = reader.IsDBNull(reader.GetOrdinal("cached_metadata"))
                    ? null : reader.GetString(reader.GetOrdinal("cached_metadata")),
            });
        }

        return results;
    }

    public async Task<ImageRecord?> GetImageByIdAsync(
        int imageId,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT i.idnum, i.filename, i.randid, i.movieid,
       i.format, i.optical_format, i.time_period,
       i.setting, i.location, i.filming_location,
       i.actors, i.int_ext
FROM frl.frl_images i
WHERE i.idnum = @imageId;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("imageId", imageId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        if (await reader.ReadAsync(cancellationToken))
        {
            return new ImageRecord
            {
                Id = reader.GetInt32(reader.GetOrdinal("idnum")),
                Filename = reader.IsDBNull(reader.GetOrdinal("filename"))
                    ? null : reader.GetString(reader.GetOrdinal("filename")),
                Randid = reader.IsDBNull(reader.GetOrdinal("randid"))
                    ? null : reader.GetString(reader.GetOrdinal("randid")),
                MovieId = reader.IsDBNull(reader.GetOrdinal("movieid"))
                    ? null : reader.GetInt32(reader.GetOrdinal("movieid")),
                Format = reader.IsDBNull(reader.GetOrdinal("format"))
                    ? null : reader.GetValue(reader.GetOrdinal("format"))?.ToString(),
                OpticalFormat = reader.IsDBNull(reader.GetOrdinal("optical_format"))
                    ? null : reader.GetValue(reader.GetOrdinal("optical_format"))?.ToString(),
                TimePeriod = reader.IsDBNull(reader.GetOrdinal("time_period"))
                    ? null : reader.GetValue(reader.GetOrdinal("time_period"))?.ToString(),
                Setting = reader.IsDBNull(reader.GetOrdinal("setting"))
                    ? null : reader.GetValue(reader.GetOrdinal("setting"))?.ToString(),
                Location = reader.IsDBNull(reader.GetOrdinal("location"))
                    ? null : reader.GetValue(reader.GetOrdinal("location"))?.ToString(),
                FilmingLocation = reader.IsDBNull(reader.GetOrdinal("filming_location"))
                    ? null : reader.GetValue(reader.GetOrdinal("filming_location"))?.ToString(),
                Actors = reader.IsDBNull(reader.GetOrdinal("actors"))
                    ? null : reader.GetValue(reader.GetOrdinal("actors"))?.ToString(),
                IntExt = reader.IsDBNull(reader.GetOrdinal("int_ext"))
                    ? null : reader.GetValue(reader.GetOrdinal("int_ext"))?.ToString(),
            };
        }

        return null;
    }

    public async Task<int> GetUnprocessedCountAsync(CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT COUNT(*)
FROM frl.frl_images i
WHERE i.status = 'live'
  AND i.filename IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM frl.frl_caption_embeddings_qwen3 ce
      WHERE ce.idnum = i.idnum
  );";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.CommandTimeout = 180;

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt32(result);
    }

    public async Task<int> GetMaxProcessedIdAsync(CancellationToken cancellationToken)
    {
        const string sql = "SELECT COALESCE(MAX(idnum), 0) FROM frl.frl_caption_embeddings_qwen3;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt32(result);
    }

    public async Task<Dictionary<int, List<string>>> FetchTagsAsync(
        string tableName,
        string columnName,
        List<int> imageIds,
        CancellationToken cancellationToken)
    {
        var result = new Dictionary<int, List<string>>();
        if (imageIds.Count == 0) return result;

        var allowedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "frl_join_images_gender",
            "frl_join_images_subject_age",
            "frl_join_images_subject_ethnicity",
            "frl_join_images_frame_size",
            "frl_join_images_tags",
            "frl_join_images_time_of_day"
        };
        var allowedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "gender", "subject_age", "subject_ethnicity", "frame_size", "tag", "time_of_day"
        };

        if (!allowedTables.Contains(tableName))
            throw new ArgumentException("Invalid table name.", nameof(tableName));
        if (!allowedColumns.Contains(columnName))
            throw new ArgumentException("Invalid column name.", nameof(columnName));

        var sql = $@"
SELECT imageid, {columnName}
FROM frl.{tableName}
WHERE imageid = ANY(@ids);";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.Add(new NpgsqlParameter<int[]>("ids", imageIds.ToArray())
        {
            NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer
        });

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            int imageId = reader.GetInt32(0);
            if (!reader.IsDBNull(1))
            {
                var raw = reader.GetValue(1)?.ToString();
                if (!string.IsNullOrWhiteSpace(raw) &&
                    raw != "--" &&
                    !raw.Equals("null", StringComparison.OrdinalIgnoreCase) &&
                    !System.Text.RegularExpressions.Regex.IsMatch(raw, "^:+$"))
                {
                    if (!result.TryGetValue(imageId, out var list))
                        result[imageId] = list = new List<string>();
                    list.Add(raw.Trim());
                }
            }
        }
        return result;
    }

    public async Task<Dictionary<int, (string? Title, string? Year, string? Genre, string? Product, string? Brand, string? MediaType, string? Director, string? Cinematographer)>>
        FetchMovieFieldsAsync(List<int> movieIds, CancellationToken cancellationToken)
    {
        var result = new Dictionary<int, (string?, string?, string?, string?, string?, string?, string?, string?)>();
        if (movieIds.Count == 0) return result;

        const string sql = @"
SELECT idnum, title, year, genre, comm_product, comm_brand, media_type, director, cinematographer
FROM frl.frl_movies
WHERE idnum = ANY(@ids);";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.Add(new NpgsqlParameter<int[]>("ids", movieIds.ToArray())
        {
            NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer
        });

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        int ordId = reader.GetOrdinal("idnum");
        int ordTitle = reader.GetOrdinal("title");
        int ordYear = reader.GetOrdinal("year");
        int ordGenre = reader.GetOrdinal("genre");
        int ordProd = reader.GetOrdinal("comm_product");
        int ordBrand = reader.GetOrdinal("comm_brand");
        int ordMedia = reader.GetOrdinal("media_type");
        int ordDirector = reader.GetOrdinal("director");
        int ordCinematographer = reader.GetOrdinal("cinematographer");

        while (await reader.ReadAsync(cancellationToken))
        {
            int id = reader.GetInt32(ordId);
            string? t = reader.IsDBNull(ordTitle) ? null : reader.GetValue(ordTitle)?.ToString();
            string? y = reader.IsDBNull(ordYear) ? null : reader.GetValue(ordYear)?.ToString();
            string? g = reader.IsDBNull(ordGenre) ? null : reader.GetValue(ordGenre)?.ToString();
            string? p = reader.IsDBNull(ordProd) ? null : reader.GetValue(ordProd)?.ToString();
            string? b = reader.IsDBNull(ordBrand) ? null : reader.GetValue(ordBrand)?.ToString();
            string? mt = reader.IsDBNull(ordMedia) ? null : reader.GetValue(ordMedia)?.ToString();
            string? d = reader.IsDBNull(ordDirector) ? null : reader.GetValue(ordDirector)?.ToString();
            string? c = reader.IsDBNull(ordCinematographer) ? null : reader.GetValue(ordCinematographer)?.ToString();

            result[id] = (
                AdminPanelAPI.Helpers.KeywordMetadataBuilder.Clean(t),
                AdminPanelAPI.Helpers.KeywordMetadataBuilder.Clean(y),
                AdminPanelAPI.Helpers.KeywordMetadataBuilder.Clean(g),
                AdminPanelAPI.Helpers.KeywordMetadataBuilder.Clean(p),
                AdminPanelAPI.Helpers.KeywordMetadataBuilder.Clean(b),
                AdminPanelAPI.Helpers.KeywordMetadataBuilder.Clean(mt),
                AdminPanelAPI.Helpers.KeywordMetadataBuilder.Clean(d),
                AdminPanelAPI.Helpers.KeywordMetadataBuilder.Clean(c)
            );
        }
        return result;
    }
}
