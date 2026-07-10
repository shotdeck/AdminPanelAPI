using System.Text.RegularExpressions;
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
    Task<List<MusicTrackGroup>> SearchTracksAsync(string query, int limit, CancellationToken cancellationToken);
    Task<List<MusicTrackGroup>> GetMovieTracksAsync(int movieId, CancellationToken cancellationToken);
    Task<List<MovieMusicSummary>> SearchMoviesByTitleAsync(string query, int limit, CancellationToken cancellationToken);
    Task<MusicSearchOptions> GetSearchOptionsAsync(string query, int limit, CancellationToken cancellationToken);
    Task<MovieInfo?> GetMovieInfoAsync(int movieId, CancellationToken cancellationToken);
    Task<List<MovieSongRow>> GetMovieSongRowsAsync(int movieId, CancellationToken cancellationToken);
    Task SetSongConfidenceAsync(int movieId, IReadOnlyDictionary<long, string> confidenceBySongId, CancellationToken cancellationToken);
    Task<List<MovieSongRow>> GetMovieSongRowsWithLinksAsync(int movieId, CancellationToken cancellationToken);
    Task SetSongLinksAsync(IReadOnlyDictionary<long, (string? spotifyUrl, string? streamingUrl)> linksBySongId, CancellationToken cancellationToken);
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
    (movieid, song_id, start_time, end_time, matched, score, source)
VALUES
    (@movieid, @song_id, @start_time, @end_time, @matched, @score, @source);";

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
                cmd.Parameters.AddWithValue("score", (object?)seg.Score ?? DBNull.Value);
                cmd.Parameters.AddWithValue("source", (object?)seg.Source ?? DBNull.Value);
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
                cmd.Parameters.AddWithValue("source", DBNull.Value);
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
INSERT INTO frl.frl_music_songs (title, isrc, acrid, artist_id, spotify_url)
VALUES (@title, @isrc, @acrid, @artist_id, @spotify_url)
ON CONFLICT (acrid) DO UPDATE SET
    title = EXCLUDED.title,
    isrc = COALESCE(EXCLUDED.isrc, frl.frl_music_songs.isrc),
    artist_id = COALESCE(EXCLUDED.artist_id, frl.frl_music_songs.artist_id),
    spotify_url = COALESCE(EXCLUDED.spotify_url, frl.frl_music_songs.spotify_url)
RETURNING id;";
        await using var songCmd = new NpgsqlCommand(songSql, conn, tx);
        songCmd.Parameters.AddWithValue("title", (object?)seg.Title ?? DBNull.Value);
        songCmd.Parameters.AddWithValue("isrc", (object?)seg.Isrc ?? DBNull.Value);
        songCmd.Parameters.AddWithValue("acrid", seg.RecordingId!);
        songCmd.Parameters.AddWithValue("artist_id", (object?)artistId ?? DBNull.Value);
        songCmd.Parameters.AddWithValue("spotify_url", (object?)seg.SpotifyUrl ?? DBNull.Value);
        return Convert.ToInt64(await songCmd.ExecuteScalarAsync(cancellationToken));
    }

    public async Task<List<MusicSegmentResult>> GetSegmentsAsync(int movieId, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT s.movieid, s.start_time, s.end_time, s.matched,
       so.title, ar.name AS artist, so.acrid, so.isrc, s.score,
       s.source, so.spotify_url, s.confidence, so.streaming_url
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
                Score = reader.IsDBNull(8) ? null : reader.GetDouble(8),
                Source = reader.IsDBNull(9) ? null : reader.GetString(9),
                SpotifyUrl = reader.IsDBNull(10) ? null : reader.GetString(10),
                Confidence = reader.IsDBNull(11) ? null : reader.GetString(11),
                StreamingUrl = reader.IsDBNull(12) ? null : reader.GetString(12)
            });
        }

        return results;
    }

    public async Task<List<MusicTrackGroup>> SearchTracksAsync(string query, int limit, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT so.id, so.title, ar.name AS artist, so.isrc, so.acrid,
       s.movieid, m.title AS movie_title, m.year AS movie_year,
       s.start_time, s.end_time, s.score, so.spotify_url, s.source, s.confidence, so.streaming_url
FROM frl.frl_join_movies_music_segments s
JOIN frl.frl_music_songs so ON s.song_id = so.id
LEFT JOIN frl.frl_music_artists ar ON so.artist_id = ar.id
LEFT JOIN frl.frl_movies m ON m.idnum = s.movieid
WHERE s.matched = true
  AND (so.title ILIKE @q OR ar.name ILIKE @q)
ORDER BY so.title, s.movieid, s.start_time
LIMIT @limit;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("q", $"%{query}%");
        cmd.Parameters.AddWithValue("limit", limit);

        return await ReadTrackGroupsAsync(cmd, cancellationToken);
    }

    public async Task<List<MusicTrackGroup>> GetMovieTracksAsync(int movieId, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT so.id, so.title, ar.name AS artist, so.isrc, so.acrid,
       s.movieid, m.title AS movie_title, m.year AS movie_year,
       s.start_time, s.end_time, s.score, so.spotify_url, s.source, s.confidence, so.streaming_url
FROM frl.frl_join_movies_music_segments s
JOIN frl.frl_music_songs so ON s.song_id = so.id
LEFT JOIN frl.frl_music_artists ar ON so.artist_id = ar.id
LEFT JOIN frl.frl_movies m ON m.idnum = s.movieid
WHERE s.matched = true
  AND s.movieid = @movieid
ORDER BY so.title, s.start_time;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("movieid", movieId);

        return await ReadTrackGroupsAsync(cmd, cancellationToken);
    }

    public async Task<List<MovieMusicSummary>> SearchMoviesByTitleAsync(string query, int limit, CancellationToken cancellationToken)
    {
        // Movies whose title matches and that have at least one identified song.
        const string sql = @"
SELECT m.idnum, m.title, m.year,
       COUNT(DISTINCT s.song_id) AS track_count,
       COUNT(*) AS occurrence_count
FROM frl.frl_movies m
JOIN frl.frl_join_movies_music_segments s
     ON s.movieid = m.idnum AND s.matched = true
WHERE m.title ILIKE @q
GROUP BY m.idnum, m.title, m.year
ORDER BY m.title
LIMIT @limit;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("q", $"%{query}%");
        cmd.Parameters.AddWithValue("limit", limit);

        var results = new List<MovieMusicSummary>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(new MovieMusicSummary
            {
                MovieId = reader.GetInt32(0),
                Title = reader.IsDBNull(1) ? null : reader.GetString(1),
                Year = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                TrackCount = Convert.ToInt32(reader.GetInt64(3)),
                OccurrenceCount = Convert.ToInt32(reader.GetInt64(4))
            });
        }

        return results;
    }

    public async Task<MusicSearchOptions> GetSearchOptionsAsync(string query, int limit, CancellationToken cancellationToken)
    {
        // Distinct artists and song titles that have at least one identified
        // (matched) occurrence. Empty query returns all, for the dropdown.
        const string artistSql = @"
SELECT DISTINCT ar.name
FROM frl.frl_music_artists ar
JOIN frl.frl_music_songs so ON so.artist_id = ar.id
JOIN frl.frl_join_movies_music_segments s ON s.song_id = so.id AND s.matched = true
WHERE ar.name IS NOT NULL AND (@q = '' OR ar.name ILIKE @like)
ORDER BY ar.name
LIMIT @limit;";

        const string songSql = @"
SELECT DISTINCT so.title
FROM frl.frl_music_songs so
JOIN frl.frl_join_movies_music_segments s ON s.song_id = so.id AND s.matched = true
WHERE so.title IS NOT NULL AND (@q = '' OR so.title ILIKE @like)
ORDER BY so.title
LIMIT @limit;";

        var options = new MusicSearchOptions();

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using (var cmd = new NpgsqlCommand(artistSql, conn))
        {
            cmd.Parameters.AddWithValue("q", query);
            cmd.Parameters.AddWithValue("like", $"%{query}%");
            cmd.Parameters.AddWithValue("limit", limit);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                options.Artists.Add(reader.GetString(0));
        }

        await using (var cmd = new NpgsqlCommand(songSql, conn))
        {
            cmd.Parameters.AddWithValue("q", query);
            cmd.Parameters.AddWithValue("like", $"%{query}%");
            cmd.Parameters.AddWithValue("limit", limit);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                options.Songs.Add(reader.GetString(0));
        }

        return options;
    }

    public async Task<MovieInfo?> GetMovieInfoAsync(int movieId, CancellationToken cancellationToken)
    {
        const string sql = @"SELECT idnum, title, year FROM frl.frl_movies WHERE idnum = @movieid;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("movieid", movieId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;

        return new MovieInfo
        {
            MovieId = reader.GetInt32(0),
            Title = reader.IsDBNull(1) ? null : reader.GetString(1),
            Year = reader.IsDBNull(2) ? null : reader.GetInt32(2)
        };
    }

    public async Task<List<MovieSongRow>> GetMovieSongRowsAsync(int movieId, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT DISTINCT so.id, so.title, ar.name AS artist
FROM frl.frl_join_movies_music_segments s
JOIN frl.frl_music_songs so ON s.song_id = so.id
LEFT JOIN frl.frl_music_artists ar ON so.artist_id = ar.id
WHERE s.matched = true AND s.movieid = @movieid;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("movieid", movieId);

        var rows = new List<MovieSongRow>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new MovieSongRow
            {
                SongId = reader.GetInt64(0),
                Title = reader.IsDBNull(1) ? null : reader.GetString(1),
                Artist = reader.IsDBNull(2) ? null : reader.GetString(2)
            });
        }

        return rows;
    }

    public async Task SetSongConfidenceAsync(
        int movieId, IReadOnlyDictionary<long, string> confidenceBySongId, CancellationToken cancellationToken)
    {
        if (confidenceBySongId.Count == 0)
            return;

        const string sql = @"
UPDATE frl.frl_join_movies_music_segments
SET confidence = @confidence
WHERE movieid = @movieid AND song_id = @song_id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);
        try
        {
            foreach (var (songId, confidence) in confidenceBySongId)
            {
                await using var cmd = new NpgsqlCommand(sql, conn, tx);
                cmd.Parameters.AddWithValue("confidence", confidence);
                cmd.Parameters.AddWithValue("movieid", movieId);
                cmd.Parameters.AddWithValue("song_id", songId);
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

    public async Task<List<MovieSongRow>> GetMovieSongRowsWithLinksAsync(int movieId, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT DISTINCT so.id, so.title, ar.name AS artist, so.spotify_url, so.streaming_url
FROM frl.frl_join_movies_music_segments s
JOIN frl.frl_music_songs so ON s.song_id = so.id
LEFT JOIN frl.frl_music_artists ar ON so.artist_id = ar.id
WHERE s.matched = true AND s.movieid = @movieid;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("movieid", movieId);

        var rows = new List<MovieSongRow>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new MovieSongRow
            {
                SongId = reader.GetInt64(0),
                Title = reader.IsDBNull(1) ? null : reader.GetString(1),
                Artist = reader.IsDBNull(2) ? null : reader.GetString(2),
                SpotifyUrl = reader.IsDBNull(3) ? null : reader.GetString(3),
                StreamingUrl = reader.IsDBNull(4) ? null : reader.GetString(4)
            });
        }

        return rows;
    }

    public async Task SetSongLinksAsync(
        IReadOnlyDictionary<long, (string? spotifyUrl, string? streamingUrl)> linksBySongId,
        CancellationToken cancellationToken)
    {
        if (linksBySongId.Count == 0)
            return;

        const string sql = @"
UPDATE frl.frl_music_songs
SET spotify_url = COALESCE(@spotify_url, spotify_url),
    streaming_url = COALESCE(@streaming_url, streaming_url)
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);
        try
        {
            foreach (var (songId, links) in linksBySongId)
            {
                await using var cmd = new NpgsqlCommand(sql, conn, tx);
                cmd.Parameters.AddWithValue("spotify_url", (object?)links.spotifyUrl ?? DBNull.Value);
                cmd.Parameters.AddWithValue("streaming_url", (object?)links.streamingUrl ?? DBNull.Value);
                cmd.Parameters.AddWithValue("id", songId);
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
    /// Read occurrence rows and group them by song, preserving row order so the
    /// group order matches the SQL ORDER BY.
    /// </summary>
    private static async Task<List<MusicTrackGroup>> ReadTrackGroupsAsync(
        NpgsqlCommand cmd, CancellationToken cancellationToken)
    {
        var groups = new List<MusicTrackGroup>();
        // Group by normalized artist+title, not song_id: ACRCloud often returns
        // the same recording under several catalog entries (e.g. "What Is Life"
        // and "What Is Life (2009 Mix)"), which would otherwise show as separate
        // overlapping tracks.
        var byKey = new Dictionary<string, MusicTrackGroup>();

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var songId = reader.GetInt64(0);
            var title = reader.IsDBNull(1) ? null : reader.GetString(1);
            var artist = reader.IsDBNull(2) ? null : reader.GetString(2);
            var isrc = reader.IsDBNull(3) ? null : reader.GetString(3);
            var acrid = reader.IsDBNull(4) ? null : reader.GetString(4);
            var spotifyUrl = reader.IsDBNull(11) ? null : reader.GetString(11);
            var streamingUrl = reader.IsDBNull(14) ? null : reader.GetString(14);

            var key = GroupKey(artist, title);
            if (!byKey.TryGetValue(key, out var group))
            {
                group = new MusicTrackGroup
                {
                    SongId = songId,
                    Title = title,
                    Artist = artist,
                    Isrc = isrc,
                    Acrid = acrid,
                    SpotifyUrl = spotifyUrl,
                    StreamingUrl = streamingUrl
                };
                byKey[key] = group;
                groups.Add(group);
            }
            else if (title != null &&
                     (group.Title == null || title.Length < group.Title.Length))
            {
                // Prefer the plainest variant title (usually the shortest,
                // e.g. "What Is Life" over "What Is Life (2009 Mix)").
                group.SongId = songId;
                group.Title = title;
                group.Isrc = isrc ?? group.Isrc;
                group.Acrid = acrid ?? group.Acrid;
                group.SpotifyUrl = spotifyUrl ?? group.SpotifyUrl;
                group.StreamingUrl = streamingUrl ?? group.StreamingUrl;
            }
            else
            {
                group.SpotifyUrl ??= spotifyUrl;
                group.StreamingUrl ??= streamingUrl;
            }

            group.Occurrences.Add(new MusicTrackOccurrence
            {
                MovieId = reader.GetInt32(5),
                MovieTitle = reader.IsDBNull(6) ? null : reader.GetString(6),
                MovieYear = reader.IsDBNull(7) ? null : reader.GetInt32(7),
                StartTime = reader.GetDouble(8),
                EndTime = reader.GetDouble(9),
                Score = reader.IsDBNull(10) ? null : reader.GetDouble(10),
                Source = reader.IsDBNull(12) ? null : reader.GetString(12),
                Confidence = reader.IsDBNull(13) ? null : reader.GetString(13)
            });
        }

        foreach (var g in groups)
        {
            g.Occurrences = MergeNearbyOccurrences(g.Occurrences);
            g.OccurrenceCount = g.Occurrences.Count;
            g.Confidence = BestConfidence(g.Occurrences);
        }

        return groups;
    }

    // Normalized artist+title key so the same recording matched to different
    // ACRCloud catalog entries (e.g. "What Is Life" vs "What Is Life (2009 Mix)")
    // groups into one track. Lowercased, parenthetical/bracket qualifiers and
    // trailing "- remastered ..." dropped, punctuation stripped.
    private static string GroupKey(string? artist, string? title)
    {
        return $"{Normalize(artist)}|{Normalize(title)}";
    }

    // A track's group-level confidence is the strongest across its occurrences
    // (confirmed > review > unverified). Null when the movie isn't reconciled.
    private static string? BestConfidence(IEnumerable<MusicTrackOccurrence> occurrences)
    {
        string? best = null;
        foreach (var o in occurrences)
        {
            if (o.Confidence == "confirmed") return "confirmed";
            if (o.Confidence == "review") best = "review";
            else if (o.Confidence == "unverified" && best == null) best = "unverified";
        }
        return best;
    }

    private static string Normalize(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return "";
        var s = value.ToLowerInvariant();
        s = Regex.Replace(s, @"\s*[\(\[].*?[\)\]]", " ");     // drop (...) / [...]
        s = Regex.Replace(s, @"\s*-\s*(remaster|remastered|mix|version|edit|mono|stereo|live).*$", " ");
        s = Regex.Replace(s, @"[^a-z0-9]+", " ");             // strip punctuation
        return s.Trim();
    }

    // A song can drop out of ACRCloud recognition for a few seconds during a
    // continuous scene (dialogue over the music), which splits one cue into
    // several rows. Merge occurrences of the same song within the same movie
    // when the gap between them is small, so the UI shows one clip per scene.
    private const double MergeGapSeconds = 90;

    private static List<MusicTrackOccurrence> MergeNearbyOccurrences(List<MusicTrackOccurrence> occurrences)
    {
        var ordered = occurrences
            .OrderBy(o => o.MovieId)
            .ThenBy(o => o.StartTime)
            .ToList();

        var merged = new List<MusicTrackOccurrence>();
        foreach (var o in ordered)
        {
            var last = merged.LastOrDefault();
            if (last != null &&
                last.MovieId == o.MovieId &&
                o.StartTime - last.EndTime <= MergeGapSeconds)
            {
                last.EndTime = Math.Max(last.EndTime, o.EndTime);
                last.Score = Math.Max(last.Score ?? 0, o.Score ?? 0);
            }
            else
            {
                merged.Add(o);
            }
        }

        return merged;
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
