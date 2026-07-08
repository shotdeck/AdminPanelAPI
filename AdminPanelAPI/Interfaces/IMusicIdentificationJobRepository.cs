using AdminPanelAPI.Models;
using Npgsql;

public interface IMusicIdentificationJobRepository
{
    Task<long> CreateJobAsync(int movieId, string? r2Key, string? r2Url, CancellationToken cancellationToken);
    Task<MusicIdentificationJobStatusResponse?> GetJobAsync(long jobId, CancellationToken cancellationToken);
    Task MarkRunningAsync(long jobId, CancellationToken cancellationToken);
    Task MarkCompletedAsync(long jobId, int matchedCount, int unmatchedCount, CancellationToken cancellationToken);
    Task MarkFailedAsync(long jobId, string error, CancellationToken cancellationToken);
    Task UpdateProgressAsync(long jobId, string step, int progressPct, CancellationToken cancellationToken);
    Task StoreSegmentsAsync(int movieId, MusicApiResponse response, CancellationToken cancellationToken);
    Task<List<MusicSegmentResult>> GetSegmentsAsync(int movieId, CancellationToken cancellationToken);
}

public class MusicIdentificationJobRepository : IMusicIdentificationJobRepository
{
    private readonly string _connectionString;

    public MusicIdentificationJobRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("Default")
            ?? throw new InvalidOperationException("Missing connection string: Default");
    }

    public async Task<long> CreateJobAsync(int movieId, string? r2Key, string? r2Url, CancellationToken cancellationToken)
    {
        const string sql = @"
INSERT INTO frl.frl_join_movies_music_identification_jobs (
    movieid,
    r2_key,
    r2_url,
    status
)
VALUES (
    @movieid,
    @r2_key,
    @r2_url,
    'Queued'
)
RETURNING id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("movieid", movieId);
        cmd.Parameters.AddWithValue("r2_key", (object?)r2Key ?? DBNull.Value);
        cmd.Parameters.AddWithValue("r2_url", (object?)r2Url ?? DBNull.Value);

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(result);
    }

    public async Task UpdateProgressAsync(long jobId, string step, int progressPct, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_join_movies_music_identification_jobs
SET current_step = @step,
    progress_pct = @progress_pct
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);
        cmd.Parameters.AddWithValue("step", (object?)step ?? DBNull.Value);
        cmd.Parameters.AddWithValue("progress_pct", progressPct);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<MusicIdentificationJobStatusResponse?> GetJobAsync(long jobId, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT
    id,
    movieid,
    status,
    current_step,
    progress_pct,
    matched_count,
    unmatched_count,
    r2_key,
    r2_url,
    created_at,
    started_at,
    completed_at,
    error
FROM frl.frl_join_movies_music_identification_jobs
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
            return null;

        return new MusicIdentificationJobStatusResponse
        {
            JobId = reader.GetInt64(0),
            MovieId = reader.GetInt32(1),
            Status = reader.GetString(2),
            CurrentStep = reader.IsDBNull(3) ? null : reader.GetString(3),
            ProgressPct = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
            MatchedCount = reader.IsDBNull(5) ? null : reader.GetInt32(5),
            UnmatchedCount = reader.IsDBNull(6) ? null : reader.GetInt32(6),
            R2Key = reader.IsDBNull(7) ? null : reader.GetString(7),
            R2Url = reader.IsDBNull(8) ? null : reader.GetString(8),
            CreatedAt = reader.GetDateTime(9),
            StartedAt = reader.IsDBNull(10) ? null : reader.GetDateTime(10),
            CompletedAt = reader.IsDBNull(11) ? null : reader.GetDateTime(11),
            Error = reader.IsDBNull(12) ? null : reader.GetString(12)
        };
    }

    public async Task MarkRunningAsync(long jobId, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_join_movies_music_identification_jobs
SET status = 'Running',
    started_at = now(),
    error = null
WHERE id = @id;";

        await ExecuteNonQueryAsync(sql, jobId, null, cancellationToken);
    }

    public async Task MarkCompletedAsync(long jobId, int matchedCount, int unmatchedCount, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_join_movies_music_identification_jobs
SET status = 'Completed',
    completed_at = now(),
    matched_count = @matched_count,
    unmatched_count = @unmatched_count,
    progress_pct = 100,
    error = null
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);
        cmd.Parameters.AddWithValue("matched_count", matchedCount);
        cmd.Parameters.AddWithValue("unmatched_count", unmatchedCount);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkFailedAsync(long jobId, string error, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_join_movies_music_identification_jobs
SET status = 'Failed',
    completed_at = now(),
    error = @error
WHERE id = @id;";

        await ExecuteNonQueryAsync(sql, jobId, error, cancellationToken);
    }

    public async Task StoreSegmentsAsync(int movieId, MusicApiResponse response, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        // Idempotent re-run: clear existing segments for this movie. Artists and
        // songs are shared across movies, so they are left in place.
        await using (var deleteCmd = new NpgsqlCommand(
            "DELETE FROM frl.frl_join_movies_music_segments WHERE movieid = @movieid", conn))
        {
            deleteCmd.Parameters.AddWithValue("movieid", movieId);
            await deleteCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var tx = await conn.BeginTransactionAsync(cancellationToken);
        try
        {
            const string insertSegmentSql = @"
INSERT INTO frl.frl_join_movies_music_segments
    (movieid, song_id, start_time, end_time, matched, score)
VALUES
    (@movieid, @song_id, @start_time, @end_time, @matched, @score);";

            foreach (var seg in response.MatchedSegments)
            {
                // Deduplicate the artist and song across movies, then link the
                // per-movie occurrence to the shared song.
                long? songId = await UpsertSongAsync(conn, tx, seg, cancellationToken);

                await using var cmd = new NpgsqlCommand(insertSegmentSql, conn, tx);
                cmd.Parameters.AddWithValue("movieid", movieId);
                cmd.Parameters.AddWithValue("song_id", (object?)songId ?? DBNull.Value);
                cmd.Parameters.AddWithValue("start_time", seg.Start);
                cmd.Parameters.AddWithValue("end_time", seg.End);
                cmd.Parameters.AddWithValue("matched", songId != null);
                cmd.Parameters.AddWithValue("score", seg.Score);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }

            foreach (var win in response.UnmatchedWindows)
            {
                await using var cmd = new NpgsqlCommand(insertSegmentSql, conn, tx);
                cmd.Parameters.AddWithValue("movieid", movieId);
                cmd.Parameters.AddWithValue("song_id", DBNull.Value);
                cmd.Parameters.AddWithValue("start_time", win.Start);
                cmd.Parameters.AddWithValue("end_time", win.End);
                cmd.Parameters.AddWithValue("matched", false);
                cmd.Parameters.AddWithValue("score", DBNull.Value);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }

            await tx.CommitAsync(cancellationToken);
        }
        catch
        {
            await tx.RollbackAsync(cancellationToken);
            throw;
        }
    }

    /// <summary>
    /// Upsert the segment's artist and song (dedup by artist name and ACRCloud
    /// acrid) and return the song id. Returns null when there is no recording id
    /// to key on.
    /// </summary>
    private static async Task<long?> UpsertSongAsync(
        NpgsqlConnection conn, NpgsqlTransaction tx, MatchedSegment seg, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(seg.RecordingId))
            return null;

        long? artistId = null;
        if (!string.IsNullOrWhiteSpace(seg.Artist))
        {
            const string artistSql = @"
INSERT INTO frl.frl_music_artists (name)
VALUES (@name)
ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
RETURNING id;";
            await using var artistCmd = new NpgsqlCommand(artistSql, conn, tx);
            artistCmd.Parameters.AddWithValue("name", seg.Artist!);
            artistId = Convert.ToInt64(await artistCmd.ExecuteScalarAsync(cancellationToken));
        }

        const string songSql = @"
INSERT INTO frl.frl_music_songs (title, isrc, acrid, artist_id)
VALUES (@title, @isrc, @acrid, @artist_id)
ON CONFLICT (acrid) DO UPDATE SET
    title = EXCLUDED.title,
    isrc = COALESCE(EXCLUDED.isrc, frl.frl_music_songs.isrc),
    artist_id = COALESCE(EXCLUDED.artist_id, frl.frl_music_songs.artist_id)
RETURNING id;";
        await using var songCmd = new NpgsqlCommand(songSql, conn, tx);
        songCmd.Parameters.AddWithValue("title", (object?)seg.Title ?? DBNull.Value);
        songCmd.Parameters.AddWithValue("isrc", (object?)seg.Isrc ?? DBNull.Value);
        songCmd.Parameters.AddWithValue("acrid", seg.RecordingId!);
        songCmd.Parameters.AddWithValue("artist_id", (object?)artistId ?? DBNull.Value);
        return Convert.ToInt64(await songCmd.ExecuteScalarAsync(cancellationToken));
    }

    public async Task<List<MusicSegmentResult>> GetSegmentsAsync(int movieId, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT s.movieid, s.start_time, s.end_time, s.matched,
       so.title, ar.name AS artist, so.acrid, so.isrc, s.score
FROM frl.frl_join_movies_music_segments s
LEFT JOIN frl.frl_music_songs so ON s.song_id = so.id
LEFT JOIN frl.frl_music_artists ar ON so.artist_id = ar.id
WHERE s.movieid = @movieid
ORDER BY s.start_time;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("movieid", movieId);

        var results = new List<MusicSegmentResult>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(new MusicSegmentResult
            {
                MovieId = reader.GetInt32(0),
                StartTime = reader.GetDouble(1),
                EndTime = reader.GetDouble(2),
                Matched = reader.GetBoolean(3),
                Title = reader.IsDBNull(4) ? null : reader.GetString(4),
                Artist = reader.IsDBNull(5) ? null : reader.GetString(5),
                RecordingId = reader.IsDBNull(6) ? null : reader.GetString(6),
                Isrc = reader.IsDBNull(7) ? null : reader.GetString(7),
                Score = reader.IsDBNull(8) ? null : reader.GetDouble(8)
            });
        }

        return results;
    }

    private async Task ExecuteNonQueryAsync(string sql, long jobId, string? error, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);

        if (sql.Contains("@error"))
            cmd.Parameters.AddWithValue("error", (object?)error ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }
}
