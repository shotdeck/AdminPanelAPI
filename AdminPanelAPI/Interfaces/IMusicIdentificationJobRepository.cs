using System.Text.Json;
using System.Text.RegularExpressions;
using AdminPanelAPI.Models;
using Npgsql;
using NpgsqlTypes;

public interface IMusicIdentificationJobRepository
{
    Task<long> CreateJobAsync(int movieId, string? r2Key, string? r2Url, CancellationToken cancellationToken);
    Task<MusicIdentificationJobStatusResponse?> GetJobAsync(long jobId, CancellationToken cancellationToken);
    Task MarkRunningAsync(long jobId, CancellationToken cancellationToken);
    Task MarkCompletedAsync(long jobId, int matchedCount, int unmatchedCount, string? warning, CancellationToken cancellationToken);
    Task MarkFailedAsync(long jobId, string error, CancellationToken cancellationToken);
    Task UpdateProgressAsync(long jobId, string step, int progressPct, CancellationToken cancellationToken);
    Task StoreSegmentsAsync(int movieId, MusicApiResponse response, CancellationToken cancellationToken);
    Task<List<MusicSegmentResult>> GetSegmentsAsync(int movieId, CancellationToken cancellationToken);
    Task<List<MusicTrackGroup>> SearchTracksAsync(string query, int limit, bool includeRejected, CancellationToken cancellationToken);
    Task<List<MusicTrackGroup>> GetMovieTracksAsync(int movieId, bool includeRejected, CancellationToken cancellationToken);
    Task<List<MovieMusicSummary>> SearchMoviesByTitleAsync(string query, int limit, CancellationToken cancellationToken);
    Task<MusicSearchOptions> GetSearchOptionsAsync(string query, int limit, CancellationToken cancellationToken);
    Task<MovieInfo?> GetMovieInfoAsync(int movieId, CancellationToken cancellationToken);
    Task<List<MovieSongRow>> GetMovieSongRowsAsync(int movieId, CancellationToken cancellationToken);
    Task SetSongConfidenceAsync(int movieId, IReadOnlyDictionary<long, string> confidenceBySongId, CancellationToken cancellationToken);
    Task<double?> GetSongMaxScoreAsync(int movieId, long songId, CancellationToken cancellationToken);
    Task<bool> PromoteUnverifiedToConfirmedAsync(int movieId, long songId, CancellationToken cancellationToken);
    Task<bool> BaselineNullToUnverifiedAsync(int movieId, long songId, CancellationToken cancellationToken);
    Task<SongTrackUpdate> UpdateSongTrackAsync(long songId, string title, string? artist, CancellationToken cancellationToken);
    Task DeleteUnlockedAiDescriptionsForSongAsync(long songId, CancellationToken cancellationToken);
    Task<List<MovieSongRow>> GetMovieSongRowsWithLinksAsync(int movieId, CancellationToken cancellationToken);
    Task SetSongLinksAsync(IReadOnlyDictionary<long, (string? spotifyUrl, string? streamingUrl, string? artworkUrl)> linksBySongId, CancellationToken cancellationToken);
    Task<MovieSoundtrack?> GetMovieSoundtrackAsync(int movieId, CancellationToken cancellationToken);
    Task UpsertMovieSoundtrackAsync(MovieSoundtrack soundtrack, CancellationToken cancellationToken);
    Task SetMovieSoundtrackAlbumAsync(int movieId, string? albumName, string? spotifyUrl, string? artworkUrl, CancellationToken cancellationToken);
    Task<MovieSongRow?> GetSongForDetailsAsync(long songId, CancellationToken cancellationToken);
    Task<TrackDetails?> GetTrackDetailsAsync(long songId, CancellationToken cancellationToken);
    Task UpsertTrackDetailsAsync(TrackDetails details, CancellationToken cancellationToken);
    Task<AiDescription?> GetAiDescriptionAsync(long songId, int movieId, CancellationToken cancellationToken);
    Task UpsertAiDescriptionAsync(long songId, int movieId, AiDescription description, string? model, CancellationToken cancellationToken);
    Task SaveManualDescriptionAsync(long songId, int movieId, string description, CancellationToken cancellationToken);
    Task DeleteAiDescriptionAsync(long songId, int movieId, CancellationToken cancellationToken);
}

public class MusicIdentificationJobRepository : IMusicIdentificationJobRepository
{
    private readonly string _connectionString;
    private const string MoviePosterBaseUrl = "https://image.tmdb.org/t/p/w154";

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

        var response = new MusicIdentificationJobStatusResponse
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
        };

        var message = reader.IsDBNull(12) ? null : reader.GetString(12);
        // A completed job that still carries a message is a non-fatal warning
        // (e.g. AI descriptions skipped); on any other status it's a real error.
        if (string.Equals(response.Status, "Completed", StringComparison.OrdinalIgnoreCase))
            response.Warning = message;
        else
            response.Error = message;

        return response;
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

    public async Task MarkCompletedAsync(long jobId, int matchedCount, int unmatchedCount, string? warning, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_join_movies_music_identification_jobs
SET status = 'Completed',
    completed_at = now(),
    matched_count = @matched_count,
    unmatched_count = @unmatched_count,
    progress_pct = 100,
    error = @warning
WHERE id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", jobId);
        cmd.Parameters.AddWithValue("matched_count", matchedCount);
        cmd.Parameters.AddWithValue("unmatched_count", unmatchedCount);
        cmd.Parameters.AddWithValue("warning", (object?)warning ?? DBNull.Value);

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
       s.source, so.spotify_url, s.confidence, so.streaming_url, so.artwork_url
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
                StreamingUrl = reader.IsDBNull(12) ? null : reader.GetString(12),
                ArtworkUrl = reader.IsDBNull(13) ? null : reader.GetString(13)
            });
        }

        return results;
    }

    public async Task<List<MusicTrackGroup>> SearchTracksAsync(string query, int limit, bool includeRejected, CancellationToken cancellationToken)
    {
        var rejectedFilter = includeRejected
            ? ""
            : "  AND s.confidence IS DISTINCT FROM 'rejected'\n";
        var sql = $@"
SELECT so.id, so.title, ar.name AS artist, so.isrc, so.acrid,
       s.movieid, m.title AS movie_title, m.year AS movie_year,
       s.start_time, s.end_time, s.score, so.spotify_url, s.source, s.confidence, so.streaming_url, so.artwork_url
FROM frl.frl_join_movies_music_segments s
JOIN frl.frl_music_songs so ON s.song_id = so.id
LEFT JOIN frl.frl_music_artists ar ON so.artist_id = ar.id
LEFT JOIN frl.frl_movies m ON m.idnum = s.movieid
WHERE s.matched = true
{rejectedFilter}  AND (so.title ILIKE @q OR ar.name ILIKE @q)
ORDER BY so.title, s.movieid, s.start_time
LIMIT @limit;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("q", $"%{query}%");
        cmd.Parameters.AddWithValue("limit", limit);

        return await ReadTrackGroupsAsync(cmd, cancellationToken);
    }

    public async Task<List<MusicTrackGroup>> GetMovieTracksAsync(int movieId, bool includeRejected, CancellationToken cancellationToken)
    {
        var rejectedFilter = includeRejected
            ? ""
            : "  AND s.confidence IS DISTINCT FROM 'rejected'\n";
        var sql = $@"
SELECT so.id, so.title, ar.name AS artist, so.isrc, so.acrid,
       s.movieid, m.title AS movie_title, m.year AS movie_year,
       s.start_time, s.end_time, s.score, so.spotify_url, s.source, s.confidence, so.streaming_url, so.artwork_url
FROM frl.frl_join_movies_music_segments s
JOIN frl.frl_music_songs so ON s.song_id = so.id
LEFT JOIN frl.frl_music_artists ar ON so.artist_id = ar.id
LEFT JOIN frl.frl_movies m ON m.idnum = s.movieid
WHERE s.matched = true
  AND s.movieid = @movieid
{rejectedFilter}ORDER BY so.title, s.start_time;";

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
SELECT m.idnum, m.title, m.year, m.poster,
       COUNT(DISTINCT s.song_id) AS track_count,
       COUNT(*) AS occurrence_count
FROM frl.frl_movies m
JOIN frl.frl_join_movies_music_segments s
     ON s.movieid = m.idnum AND s.matched = true
     AND s.confidence IS DISTINCT FROM 'rejected'
WHERE m.title ILIKE @q
GROUP BY m.idnum, m.title, m.year, m.poster
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
                PosterUrl = reader.IsDBNull(3) ? null : MoviePosterBaseUrl + reader.GetString(3),
                TrackCount = Convert.ToInt32(reader.GetInt64(4)),
                OccurrenceCount = Convert.ToInt32(reader.GetInt64(5))
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
        const string sql = @"SELECT idnum, title, year, poster FROM frl.frl_movies WHERE idnum = @movieid;";

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
            Year = reader.IsDBNull(2) ? null : reader.GetInt32(2),
            PosterUrl = reader.IsDBNull(3) ? null : MoviePosterBaseUrl + reader.GetString(3)
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

    // Best (highest) fingerprint match score across a song's matched segments
    // in a movie. Used to gate AI-based auto-confirmation on match strength.
    public async Task<double?> GetSongMaxScoreAsync(
        int movieId, long songId, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT MAX(score)
FROM frl.frl_join_movies_music_segments
WHERE movieid = @movieid AND song_id = @song_id AND matched = true;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("movieid", movieId);
        cmd.Parameters.AddWithValue("song_id", songId);
        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is null || result is DBNull ? null : Convert.ToDouble(result);
    }

    // Upgrade a track to "confirmed" for a movie, but only if it hasn't been
    // reconciled/decided yet — i.e. it is currently "unverified" or has no
    // confidence (null). Never overrides a review/rejected/confirmed decision.
    // Returns true if a row was upgraded.
    public async Task<bool> PromoteUnverifiedToConfirmedAsync(
        int movieId, long songId, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_join_movies_music_segments
SET confidence = 'confirmed'
WHERE movieid = @movieid AND song_id = @song_id
  AND (confidence = 'unverified' OR confidence IS NULL);";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("movieid", movieId);
        cmd.Parameters.AddWithValue("song_id", songId);
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return rows > 0;
    }

    // Give any still-unreconciled (null-confidence) track a baseline
    // "unverified" status so it always renders with a badge instead of nothing.
    // Never touches rows that already have a decision.
    public async Task<bool> BaselineNullToUnverifiedAsync(
        int movieId, long songId, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE frl.frl_join_movies_music_segments
SET confidence = 'unverified'
WHERE movieid = @movieid AND song_id = @song_id AND confidence IS NULL;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("movieid", movieId);
        cmd.Parameters.AddWithValue("song_id", songId);
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return rows > 0;
    }

    // Edit a track's title and artist. An empty/blank artist clears it; a
    // non-blank artist is found-or-created in frl_music_artists so admins can
    // enter a brand-new name or reuse an existing one.
    //
    // When the edit actually changes the title/artist, the song's cached
    // enrichment is invalidated in the same transaction: the streaming links
    // and artwork are cleared (they were resolved for the old, wrong song) and
    // the song-level track_details cache row is dropped, so both are re-fetched
    // for the corrected song. The per-(song,movie) AI description is handled by
    // the caller (kept when the AI itself just generated it; cleared on a
    // manual edit). Returns NotFound / Unchanged / Changed.
    public async Task<SongTrackUpdate> UpdateSongTrackAsync(
        long songId, string title, string? artist, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);
        try
        {
            // Read the current title/artist so we can tell whether this edit is
            // a real change (and thus whether cached enrichment is now stale).
            string? currentTitle = null;
            string? currentArtist = null;
            var exists = false;
            const string readSql = @"
SELECT so.title, ar.name
FROM frl.frl_music_songs so
LEFT JOIN frl.frl_music_artists ar ON so.artist_id = ar.id
WHERE so.id = @song_id;";
            await using (var readCmd = new NpgsqlCommand(readSql, conn, tx))
            {
                readCmd.Parameters.AddWithValue("song_id", songId);
                await using var reader = await readCmd.ExecuteReaderAsync(cancellationToken);
                if (await reader.ReadAsync(cancellationToken))
                {
                    exists = true;
                    currentTitle = reader.IsDBNull(0) ? null : reader.GetString(0);
                    currentArtist = reader.IsDBNull(1) ? null : reader.GetString(1);
                }
            }

            if (!exists)
            {
                await tx.RollbackAsync(cancellationToken);
                return SongTrackUpdate.NotFound;
            }

            var newTitle = title.Trim();
            var trimmedArtist = artist?.Trim();

            long? artistId = null;
            if (!string.IsNullOrEmpty(trimmedArtist))
            {
                const string artistSql = @"
INSERT INTO frl.frl_music_artists (name)
VALUES (@name)
ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
RETURNING id;";
                await using var artistCmd = new NpgsqlCommand(artistSql, conn, tx);
                artistCmd.Parameters.AddWithValue("name", trimmedArtist);
                artistId = Convert.ToInt64(await artistCmd.ExecuteScalarAsync(cancellationToken));
            }

            const string songSql = @"
UPDATE frl.frl_music_songs
SET title = @title, artist_id = @artist_id
WHERE id = @song_id;";
            await using (var songCmd = new NpgsqlCommand(songSql, conn, tx))
            {
                songCmd.Parameters.AddWithValue("title", newTitle);
                songCmd.Parameters.AddWithValue("artist_id", (object?)artistId ?? DBNull.Value);
                songCmd.Parameters.AddWithValue("song_id", songId);
                await songCmd.ExecuteNonQueryAsync(cancellationToken);
            }

            static bool SameText(string? a, string? b) =>
                string.Equals((a ?? "").Trim(), (b ?? "").Trim(), StringComparison.OrdinalIgnoreCase);
            var changed = !SameText(currentTitle, newTitle) || !SameText(currentArtist, trimmedArtist);

            if (changed)
            {
                // The stored links/artwork and song-level details belong to the
                // previous (wrong) song — drop them so they're re-fetched.
                const string clearLinksSql = @"
UPDATE frl.frl_music_songs
SET spotify_url = NULL, streaming_url = NULL, artwork_url = NULL
WHERE id = @song_id;";
                await using (var clearCmd = new NpgsqlCommand(clearLinksSql, conn, tx))
                {
                    clearCmd.Parameters.AddWithValue("song_id", songId);
                    await clearCmd.ExecuteNonQueryAsync(cancellationToken);
                }

                await using (var delDetails = new NpgsqlCommand(
                    "DELETE FROM frl.frl_music_track_details WHERE song_id = @song_id;", conn, tx))
                {
                    delDetails.Parameters.AddWithValue("song_id", songId);
                    await delDetails.ExecuteNonQueryAsync(cancellationToken);
                }
            }

            await tx.CommitAsync(cancellationToken);
            return changed ? SongTrackUpdate.Changed : SongTrackUpdate.Unchanged;
        }
        catch
        {
            await tx.RollbackAsync(cancellationToken);
            throw;
        }
    }

    // Drop cached AI descriptions for a song (all movies) so they regenerate
    // against the corrected title/artist. Manually-edited/locked descriptions
    // (edited = true) are preserved. Used after a manual track edit.
    public async Task DeleteUnlockedAiDescriptionsForSongAsync(
        long songId, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        try
        {
            const string sql = @"
DELETE FROM frl.frl_music_track_ai_description
WHERE song_id = @song_id AND NOT edited;";
            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("song_id", songId);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedColumn)
        {
            // migration 022 (the `edited` column) not applied yet: there are no
            // locked rows to preserve, so drop all cached descriptions.
            await using var cmd = new NpgsqlCommand(
                "DELETE FROM frl.frl_music_track_ai_description WHERE song_id = @song_id;", conn);
            cmd.Parameters.AddWithValue("song_id", songId);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    public async Task<List<MovieSongRow>> GetMovieSongRowsWithLinksAsync(int movieId, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT DISTINCT so.id, so.title, ar.name AS artist, so.spotify_url, so.streaming_url, so.artwork_url
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
                StreamingUrl = reader.IsDBNull(4) ? null : reader.GetString(4),
                ArtworkUrl = reader.IsDBNull(5) ? null : reader.GetString(5)
            });
        }

        return rows;
    }

    public async Task SetSongLinksAsync(
        IReadOnlyDictionary<long, (string? spotifyUrl, string? streamingUrl, string? artworkUrl)> linksBySongId,
        CancellationToken cancellationToken)
    {
        if (linksBySongId.Count == 0)
            return;

        const string sql = @"
UPDATE frl.frl_music_songs
SET spotify_url = COALESCE(@spotify_url, spotify_url),
    streaming_url = COALESCE(@streaming_url, streaming_url),
    artwork_url = COALESCE(@artwork_url, artwork_url)
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
                cmd.Parameters.AddWithValue("artwork_url", (object?)links.artworkUrl ?? DBNull.Value);
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

    public async Task<MovieSoundtrack?> GetMovieSoundtrackAsync(int movieId, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT movieid, album_name, spotify_url, artwork_url, wikipedia_url
FROM frl.frl_music_movie_soundtrack
WHERE movieid = @movieid;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("movieid", movieId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;

        return new MovieSoundtrack
        {
            MovieId = reader.GetInt32(0),
            AlbumName = reader.IsDBNull(1) ? null : reader.GetString(1),
            SpotifyUrl = reader.IsDBNull(2) ? null : reader.GetString(2),
            ArtworkUrl = reader.IsDBNull(3) ? null : reader.GetString(3),
            WikipediaUrl = reader.IsDBNull(4) ? null : reader.GetString(4)
        };
    }

    public async Task UpsertMovieSoundtrackAsync(MovieSoundtrack soundtrack, CancellationToken cancellationToken)
    {
        // COALESCE(EXCLUDED, existing) so the two writers never clobber each
        // other: reconciliation fills the Wikipedia URL, the streaming-link
        // backfill fills the Spotify album + cover art.
        const string sql = @"
INSERT INTO frl.frl_music_movie_soundtrack
    (movieid, album_name, spotify_url, artwork_url, wikipedia_url, updated_at)
VALUES (@movieid, @album_name, @spotify_url, @artwork_url, @wikipedia_url, now())
ON CONFLICT (movieid) DO UPDATE SET
    album_name    = COALESCE(EXCLUDED.album_name, frl.frl_music_movie_soundtrack.album_name),
    spotify_url   = COALESCE(EXCLUDED.spotify_url, frl.frl_music_movie_soundtrack.spotify_url),
    artwork_url   = COALESCE(EXCLUDED.artwork_url, frl.frl_music_movie_soundtrack.artwork_url),
    wikipedia_url = COALESCE(EXCLUDED.wikipedia_url, frl.frl_music_movie_soundtrack.wikipedia_url),
    updated_at    = now();";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("movieid", soundtrack.MovieId);
        cmd.Parameters.AddWithValue("album_name", (object?)soundtrack.AlbumName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("spotify_url", (object?)soundtrack.SpotifyUrl ?? DBNull.Value);
        cmd.Parameters.AddWithValue("artwork_url", (object?)soundtrack.ArtworkUrl ?? DBNull.Value);
        cmd.Parameters.AddWithValue("wikipedia_url", (object?)soundtrack.WikipediaUrl ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task SetMovieSoundtrackAlbumAsync(int movieId, string? albumName, string? spotifyUrl, string? artworkUrl, CancellationToken cancellationToken)
    {
        // Authoritatively set the album fields (overwriting, including to NULL to
        // clear a previously-attached wrong album) while preserving the
        // reconciliation-owned wikipedia_url. Unlike UpsertMovieSoundtrackAsync
        // this does NOT COALESCE the album columns — the caller only invokes it
        // once the album search has actually run, so a null means "no match",
        // not "unknown".
        const string sql = @"
INSERT INTO frl.frl_music_movie_soundtrack
    (movieid, album_name, spotify_url, artwork_url, updated_at)
VALUES (@movieid, @album_name, @spotify_url, @artwork_url, now())
ON CONFLICT (movieid) DO UPDATE SET
    album_name  = EXCLUDED.album_name,
    spotify_url = EXCLUDED.spotify_url,
    artwork_url = EXCLUDED.artwork_url,
    updated_at  = now();";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("movieid", movieId);
        cmd.Parameters.AddWithValue("album_name", (object?)albumName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("spotify_url", (object?)spotifyUrl ?? DBNull.Value);
        cmd.Parameters.AddWithValue("artwork_url", (object?)artworkUrl ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
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
            var artworkUrl = reader.IsDBNull(15) ? null : reader.GetString(15);

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
                    StreamingUrl = streamingUrl,
                    ArtworkUrl = artworkUrl
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
                group.ArtworkUrl = artworkUrl ?? group.ArtworkUrl;
            }
            else
            {
                group.SpotifyUrl ??= spotifyUrl;
                group.StreamingUrl ??= streamingUrl;
                group.ArtworkUrl ??= artworkUrl;
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
    // (confirmed > review > unverified > rejected). Null when the movie isn't
    // reconciled. Rejected must be reported (not left null) so the UI can badge
    // it and the status filter can hide/show it.
    private static string? BestConfidence(IEnumerable<MusicTrackOccurrence> occurrences)
    {
        string? best = null;
        foreach (var o in occurrences)
        {
            if (o.Confidence == "confirmed") return "confirmed";
            if (o.Confidence == "review") best = "review";
            else if (o.Confidence == "unverified" && best is null or "rejected") best = "unverified";
            else if (o.Confidence == "rejected" && best == null) best = "rejected";
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

    public async Task<MovieSongRow?> GetSongForDetailsAsync(long songId, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT so.id, so.title, ar.name AS artist, so.spotify_url, so.streaming_url, so.artwork_url, so.isrc
FROM frl.frl_music_songs so
LEFT JOIN frl.frl_music_artists ar ON so.artist_id = ar.id
WHERE so.id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", songId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;

        return new MovieSongRow
        {
            SongId = reader.GetInt64(0),
            Title = reader.IsDBNull(1) ? null : reader.GetString(1),
            Artist = reader.IsDBNull(2) ? null : reader.GetString(2),
            SpotifyUrl = reader.IsDBNull(3) ? null : reader.GetString(3),
            StreamingUrl = reader.IsDBNull(4) ? null : reader.GetString(4),
            ArtworkUrl = reader.IsDBNull(5) ? null : reader.GetString(5),
            Isrc = reader.IsDBNull(6) ? null : reader.GetString(6)
        };
    }

    public async Task<TrackDetails?> GetTrackDetailsAsync(long songId, CancellationToken cancellationToken)
    {
        // The `composition_fallback` column is added by migration 023. If the
        // code is deployed before the migration runs, selecting it throws
        // undefined_column (42703); fall back to reading without it so cached
        // details still show (just never marked as composition-level).
        try
        {
            return await ReadTrackDetailsAsync(songId, withComposition: true, cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedColumn)
        {
            return await ReadTrackDetailsAsync(songId, withComposition: false, cancellationToken);
        }
    }

    private async Task<TrackDetails?> ReadTrackDetailsAsync(long songId, bool withComposition, CancellationToken cancellationToken)
    {
        var sql = @"
SELECT description, description_source, wikipedia_url, writers, composers, producers,
       album, release_date, label, preview_url, musicbrainz_url"
            + (withComposition ? ", composition_fallback" : "") + @"
FROM frl.frl_music_track_details
WHERE song_id = @id;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", songId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;

        static List<MusicCredit> ParseCredits(string? json) =>
            string.IsNullOrWhiteSpace(json)
                ? new List<MusicCredit>()
                : JsonSerializer.Deserialize<List<MusicCredit>>(json) ?? new List<MusicCredit>();

        return new TrackDetails
        {
            SongId = songId,
            Description = reader.IsDBNull(0) ? null : reader.GetString(0),
            DescriptionSource = reader.IsDBNull(1) ? null : reader.GetString(1),
            WikipediaUrl = reader.IsDBNull(2) ? null : reader.GetString(2),
            Writers = ParseCredits(reader.IsDBNull(3) ? null : reader.GetString(3)),
            Composers = ParseCredits(reader.IsDBNull(4) ? null : reader.GetString(4)),
            Producers = ParseCredits(reader.IsDBNull(5) ? null : reader.GetString(5)),
            Album = reader.IsDBNull(6) ? null : reader.GetString(6),
            ReleaseDate = reader.IsDBNull(7) ? null : reader.GetString(7),
            Label = reader.IsDBNull(8) ? null : reader.GetString(8),
            PreviewUrl = reader.IsDBNull(9) ? null : reader.GetString(9),
            MusicbrainzUrl = reader.IsDBNull(10) ? null : reader.GetString(10),
            CompositionFallback = withComposition && !reader.IsDBNull(11) && reader.GetBoolean(11)
        };
    }

    public async Task UpsertTrackDetailsAsync(TrackDetails details, CancellationToken cancellationToken)
    {
        // The `composition_fallback` column is added by migration 023. Persist it
        // when present; if the migration hasn't run yet, retry without it so the
        // rest of the details still cache (undefined_column = 42703).
        try
        {
            await WriteTrackDetailsAsync(details, withComposition: true, cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedColumn)
        {
            await WriteTrackDetailsAsync(details, withComposition: false, cancellationToken);
        }
    }

    private async Task WriteTrackDetailsAsync(TrackDetails details, bool withComposition, CancellationToken cancellationToken)
    {
        var cols = withComposition ? ", composition_fallback" : "";
        var vals = withComposition ? ", @composition_fallback" : "";
        var upd = withComposition ? "\n    composition_fallback = EXCLUDED.composition_fallback," : "";
        var sql = @"
INSERT INTO frl.frl_music_track_details
    (song_id, description, description_source, wikipedia_url, writers, composers,
     producers, album, release_date, label, preview_url, musicbrainz_url" + cols + @", fetched_at)
VALUES
    (@song_id, @description, @description_source, @wikipedia_url, @writers, @composers,
     @producers, @album, @release_date, @label, @preview_url, @musicbrainz_url" + vals + @", now())
ON CONFLICT (song_id) DO UPDATE SET
    description        = EXCLUDED.description,
    description_source = EXCLUDED.description_source,
    wikipedia_url      = EXCLUDED.wikipedia_url,
    writers            = EXCLUDED.writers,
    composers          = EXCLUDED.composers,
    producers          = EXCLUDED.producers,
    album              = EXCLUDED.album,
    release_date       = EXCLUDED.release_date,
    label              = EXCLUDED.label,
    preview_url        = EXCLUDED.preview_url,
    musicbrainz_url    = EXCLUDED.musicbrainz_url," + upd + @"
    fetched_at         = now();";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("song_id", details.SongId);
        cmd.Parameters.AddWithValue("description", (object?)details.Description ?? DBNull.Value);
        cmd.Parameters.AddWithValue("description_source", (object?)details.DescriptionSource ?? DBNull.Value);
        cmd.Parameters.AddWithValue("wikipedia_url", (object?)details.WikipediaUrl ?? DBNull.Value);
        cmd.Parameters.Add(new NpgsqlParameter("writers", NpgsqlDbType.Jsonb) { Value = JsonSerializer.Serialize(details.Writers) });
        cmd.Parameters.Add(new NpgsqlParameter("composers", NpgsqlDbType.Jsonb) { Value = JsonSerializer.Serialize(details.Composers) });
        cmd.Parameters.Add(new NpgsqlParameter("producers", NpgsqlDbType.Jsonb) { Value = JsonSerializer.Serialize(details.Producers) });
        cmd.Parameters.AddWithValue("album", (object?)details.Album ?? DBNull.Value);
        cmd.Parameters.AddWithValue("release_date", (object?)details.ReleaseDate ?? DBNull.Value);
        cmd.Parameters.AddWithValue("label", (object?)details.Label ?? DBNull.Value);
        cmd.Parameters.AddWithValue("preview_url", (object?)details.PreviewUrl ?? DBNull.Value);
        cmd.Parameters.AddWithValue("musicbrainz_url", (object?)details.MusicbrainzUrl ?? DBNull.Value);
        if (withComposition)
            cmd.Parameters.AddWithValue("composition_fallback", details.CompositionFallback);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<AiDescription?> GetAiDescriptionAsync(long songId, int movieId, CancellationToken cancellationToken)
    {
        // The `edited` column is added by migration 022. If the code is deployed
        // before the migration runs, selecting it throws undefined_column
        // (42703). Fall back to reading without it so cached AI descriptions
        // still show (just never treated as locked) instead of vanishing.
        try
        {
            return await ReadAiDescriptionAsync(songId, movieId, withEdited: true, cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedColumn)
        {
            // migration 022 not applied yet — read without `edited` so cached
            // AI descriptions still show instead of vanishing.
            return await ReadAiDescriptionAsync(songId, movieId, withEdited: false, cancellationToken);
        }
    }

    private async Task<AiDescription?> ReadAiDescriptionAsync(long songId, int movieId, bool withEdited, CancellationToken cancellationToken)
    {
        var sql = withEdited
            ? @"SELECT description, sources, edited
FROM frl.frl_music_track_ai_description
WHERE song_id = @song_id AND movieid = @movieid;"
            : @"SELECT description, sources
FROM frl.frl_music_track_ai_description
WHERE song_id = @song_id AND movieid = @movieid;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("song_id", songId);
        cmd.Parameters.AddWithValue("movieid", movieId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;

        var sourcesJson = reader.IsDBNull(1) ? null : reader.GetString(1);
        return new AiDescription
        {
            Description = reader.IsDBNull(0) ? null : reader.GetString(0),
            Sources = string.IsNullOrWhiteSpace(sourcesJson)
                ? new List<LinkRef>()
                : JsonSerializer.Deserialize<List<LinkRef>>(sourcesJson) ?? new List<LinkRef>(),
            Edited = withEdited && !reader.IsDBNull(2) && reader.GetBoolean(2)
        };
    }

    public async Task UpsertAiDescriptionAsync(long songId, int movieId, AiDescription description, string? model, CancellationToken cancellationToken)
    {
        const string sql = @"
INSERT INTO frl.frl_music_track_ai_description (song_id, movieid, description, sources, model, fetched_at)
VALUES (@song_id, @movieid, @description, @sources, @model, now())
ON CONFLICT (song_id, movieid) DO UPDATE SET
    description = EXCLUDED.description,
    sources     = EXCLUDED.sources,
    model       = EXCLUDED.model,
    fetched_at  = now()
WHERE NOT frl.frl_music_track_ai_description.edited;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("song_id", songId);
        cmd.Parameters.AddWithValue("movieid", movieId);
        cmd.Parameters.AddWithValue("description", (object?)description.Description ?? DBNull.Value);
        cmd.Parameters.Add(new NpgsqlParameter("sources", NpgsqlDbType.Jsonb) { Value = JsonSerializer.Serialize(description.Sources) });
        cmd.Parameters.AddWithValue("model", (object?)model ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task SaveManualDescriptionAsync(long songId, int movieId, string description, CancellationToken cancellationToken)
    {
        // Store the human text and lock the row so AI regeneration/backfill
        // never overwrites it. Any existing web citations (sources) are kept.
        const string sql = @"
INSERT INTO frl.frl_music_track_ai_description
    (song_id, movieid, description, sources, model, edited, edited_at, fetched_at)
VALUES (@song_id, @movieid, @description, '[]'::jsonb, NULL, true, now(), now())
ON CONFLICT (song_id, movieid) DO UPDATE SET
    description = EXCLUDED.description,
    model       = NULL,
    edited      = true,
    edited_at   = now(),
    fetched_at  = now();";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("song_id", songId);
        cmd.Parameters.AddWithValue("movieid", movieId);
        cmd.Parameters.AddWithValue("description", description);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task DeleteAiDescriptionAsync(long songId, int movieId, CancellationToken cancellationToken)
    {
        // Drop the cached (or manually-edited) row so the next fetch regenerates
        // a fresh AI description.
        const string sql = @"
DELETE FROM frl.frl_music_track_ai_description
WHERE song_id = @song_id AND movieid = @movieid;";

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("song_id", songId);
        cmd.Parameters.AddWithValue("movieid", movieId);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }
}
