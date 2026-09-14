using AdminPanelAPI.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Data;
using System.Text.Json;

namespace ShotDeckSearch.Controllers
{
    /// <summary>Confirm a master is a given film.</summary>
    public sealed class IdentifyRequest
    {
        public string SourceKey { get; set; } = "";
        public int TmdbId { get; set; }

        /// <summary>The frl_movies record to bind to, where one exists.</summary>
        public int? MovieId { get; set; }

        public string? ActingUser { get; set; }
    }

    /// <summary>
    /// Identifying an uploaded master: what film it is, from TMDB, and which
    /// frl_movies record that film already has.
    ///
    /// An upload lands named after the film and bound to nothing, so before it
    /// can be allocated to a tagger somebody has to say which film it is. The
    /// tagger types, sees TMDB's matches with their posters, opens one to be
    /// sure, and confirms; the confirmation is kept against the R2 object so it
    /// survives the file being promoted onto its movie folder later.
    ///
    /// The TMDB key lives in this API's configuration, never in the browser.
    /// </summary>
    [ApiController]
    [Route("api/admin/movie-identify")]
    public sealed class MovieIdentifyController : ControllerBase
    {
        /// <summary>Stored the way the API hands it out: camelCase either way.</summary>
        private static readonly JsonSerializerOptions InfoJson = new(JsonSerializerDefaults.Web);

        private const int DefaultLimit = 10;
        private const int MaxLimit = 20;

        private readonly ITmdbService _tmdb;
        private readonly Lazy<NpgsqlConnection> _connection;
        private readonly ILogger<MovieIdentifyController> _logger;

        public MovieIdentifyController(
            ITmdbService tmdb,
            Lazy<NpgsqlConnection> connection,
            ILogger<MovieIdentifyController> logger)
        {
            _tmdb = tmdb;
            _connection = connection;
            _logger = logger;
        }

        /// <summary>
        /// TMDB matches for what the tagger has typed so far, each carrying the
        /// movie id it already has in frl_movies where one can be found.
        /// </summary>
        [HttpGet("search")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> Search(
            [FromQuery] string? query,
            [FromQuery] int? year,
            [FromQuery] int limit = DefaultLimit,
            CancellationToken ct = default)
        {
            if (!_tmdb.Configured)
                return StatusCode(StatusCodes.Status503ServiceUnavailable, new
                {
                    error = "No TMDB key is configured for this API."
                });

            var term = (query ?? "").Trim();
            if (term.Length < 2)
                return Ok(new { results = Array.Empty<object>() });

            if (limit < 1) limit = DefaultLimit;
            if (limit > MaxLimit) limit = MaxLimit;

            var found = await _tmdb.SearchAsync(term, year, limit, ct);
            var local = await LocalMatchesAsync(found, ct);

            return Ok(new
            {
                results = found.Select(result => new
                {
                    tmdbId = result.TmdbId,
                    title = result.Title,
                    originalTitle = result.OriginalTitle,
                    year = result.Year,
                    releaseDate = result.ReleaseDate,
                    overview = result.Overview,
                    posterThumbUrl = result.PosterThumbUrl,
                    posterUrl = result.PosterUrl,
                    voteAverage = result.VoteAverage,
                    voteCount = result.VoteCount,
                    movieId = local.TryGetValue(result.TmdbId, out var movie) ? movie.Id : (int?)null,
                    movieTitle = local.TryGetValue(result.TmdbId, out var named) ? named.Title : null,
                    mediaType = local.TryGetValue(result.TmdbId, out var typed) ? typed.MediaType : null
                })
            });
        }

        /// <summary>
        /// Everything TMDB has on one film, for the popup that makes sure it is
        /// the right one: poster, runtime, certificate, credits and cast.
        /// </summary>
        [HttpGet("details/{tmdbId:int}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> Details(int tmdbId, CancellationToken ct = default)
        {
            if (!_tmdb.Configured)
                return StatusCode(StatusCodes.Status503ServiceUnavailable, new
                {
                    error = "No TMDB key is configured for this API."
                });

            var details = await _tmdb.DetailsAsync(tmdbId, ct);
            if (details is null)
                return NotFound(new { error = "TMDB has no film with that id." });

            var local = await LocalMatchesAsync(new[]
            {
                new TmdbSearchResult
                {
                    TmdbId = details.TmdbId,
                    Title = details.Title,
                    Year = details.Year
                }
            }, ct, details.PosterPath);

            return Ok(new
            {
                details,
                movieId = local.TryGetValue(details.TmdbId, out var movie) ? movie.Id : (int?)null,
                movieTitle = local.TryGetValue(details.TmdbId, out var named) ? named.Title : null,
                mediaType = local.TryGetValue(details.TmdbId, out var typed) ? typed.MediaType : null
            });
        }

        /// <summary>What a file, or everything under a folder, was identified as.</summary>
        [HttpGet]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> Get(
            [FromQuery] string? key,
            [FromQuery] string? prefix,
            CancellationToken ct = default)
        {
            var connection = _connection.Value;
            var mustClose = await OpenAsync(connection, ct);

            try
            {
                await MovieIdentificationStore.EnsureTableAsync(connection, ct);

                if (!string.IsNullOrWhiteSpace(key))
                {
                    var row = await MovieIdentificationStore.GetAsync(connection, key.Trim(), ct);
                    return Ok(new { identification = row is null ? null : Describe(row) });
                }

                var rows = await MovieIdentificationStore.ListAsync(
                    connection, (prefix ?? "").Trim(), ct);
                return Ok(new { identifications = rows.Select(Describe) });
            }
            finally
            {
                if (mustClose) await connection.CloseAsync();
            }
        }

        /// <summary>
        /// The identifications held against a set of movies, for the tagging
        /// list: one request for the whole page rather than one per movie.
        /// </summary>
        [HttpGet("for-movies")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> ForMovies(
            [FromQuery] string? movieIds, CancellationToken ct = default)
        {
            var ids = (movieIds ?? "")
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(id => int.TryParse(id, out var parsed) ? parsed : 0)
                .Where(id => id > 0)
                .Distinct()
                .ToArray();

            if (ids.Length == 0)
                return Ok(new { identifications = Array.Empty<object>() });

            var connection = _connection.Value;
            var mustClose = await OpenAsync(connection, ct);

            try
            {
                await MovieIdentificationStore.EnsureTableAsync(connection, ct);
                var rows = await MovieIdentificationStore.ListForMoviesAsync(connection, ids, ct);
                return Ok(new { identifications = rows.Select(Describe) });
            }
            finally
            {
                if (mustClose) await connection.CloseAsync();
            }
        }

        /// <summary>
        /// Confirm the film. The whole TMDB answer is kept, so later steps read
        /// runtime and credits without asking TMDB again, and the runtime is
        /// what a wrong-film upload gets caught by.
        /// </summary>
        [HttpPost]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> Identify(
            [FromBody] IdentifyRequest request, CancellationToken ct = default)
        {
            var sourceKey = (request.SourceKey ?? "").Trim();
            if (sourceKey.Length == 0)
                return BadRequest(new { error = "sourceKey is required." });
            if (request.TmdbId <= 0)
                return BadRequest(new { error = "tmdbId is required." });

            if (!_tmdb.Configured)
                return StatusCode(StatusCodes.Status503ServiceUnavailable, new
                {
                    error = "No TMDB key is configured for this API."
                });

            var details = await _tmdb.DetailsAsync(request.TmdbId, ct);
            if (details is null)
                return NotFound(new { error = "TMDB has no film with that id." });

            var connection = _connection.Value;
            var mustClose = await OpenAsync(connection, ct);

            try
            {
                await MovieIdentificationStore.EnsureTableAsync(connection, ct);

                var movieId = request.MovieId;
                if (movieId is null)
                {
                    var local = await LocalMatchesAsync(new[]
                    {
                        new TmdbSearchResult
                        {
                            TmdbId = details.TmdbId,
                            Title = details.Title,
                            Year = details.Year
                        }
                    }, ct, details.PosterPath);

                    if (local.TryGetValue(details.TmdbId, out var match))
                        movieId = match.Id;
                }

                var row = await MovieIdentificationStore.UpsertAsync(
                    connection, sourceKey, details, movieId,
                    JsonSerializer.Serialize(details, InfoJson),
                    (request.ActingUser ?? "").Trim(), ct);

                _logger.LogInformation(
                    "Identified {SourceKey} as TMDB {TmdbId} ({Title}), movie {MovieId}.",
                    sourceKey, details.TmdbId, details.Title, movieId);

                return Ok(new { identification = Describe(row) });
            }
            finally
            {
                if (mustClose) await connection.CloseAsync();
            }
        }

        /// <summary>Forget an identification, so it can be done again from scratch.</summary>
        [HttpDelete]
        public async Task<IActionResult> Forget(
            [FromQuery] string key, CancellationToken ct = default)
        {
            var sourceKey = (key ?? "").Trim();
            if (sourceKey.Length == 0)
                return BadRequest(new { error = "key is required." });

            var connection = _connection.Value;
            var mustClose = await OpenAsync(connection, ct);

            try
            {
                await MovieIdentificationStore.EnsureTableAsync(connection, ct);

                const string sql = @"
DELETE FROM frl.frl_movie_file_identification WHERE source_key = @sourceKey;";
                await using var cmd = new NpgsqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("@sourceKey", sourceKey);
                var deleted = await cmd.ExecuteNonQueryAsync(ct);

                return Ok(new { deleted });
            }
            finally
            {
                if (mustClose) await connection.CloseAsync();
            }
        }

        private sealed class LocalMovie
        {
            public int Id { get; init; }
            public string Title { get; init; } = "";
            public string? MediaType { get; init; }
        }

        /// <summary>
        /// The frl_movies record each TMDB result already has. frl_movies stores
        /// TMDB's own poster path, so that is the surest match; title and year
        /// catch the rest. A trailer and a feature of the same film both exist
        /// as records, so the feature is preferred.
        /// </summary>
        private async Task<Dictionary<int, LocalMovie>> LocalMatchesAsync(
            IReadOnlyCollection<TmdbSearchResult> results,
            CancellationToken ct,
            string? posterPath = null)
        {
            var matches = new Dictionary<int, LocalMovie>();
            if (results.Count == 0)
                return matches;

            NpgsqlConnection? connection = null;
            var mustClose = false;

            try
            {
                connection = _connection.Value;
                mustClose = await OpenAsync(connection, ct);

                const string sql = @"
SELECT idnum, title, year, media_type::text AS media_type, poster
FROM frl.frl_movies
WHERE (@poster <> '' AND poster = @poster)
   OR (title ILIKE @title AND (@year = 0 OR year = @year OR year IS NULL))
ORDER BY (media_type::text = 'movie') DESC, year DESC NULLS LAST
LIMIT 5;";

                foreach (var result in results)
                {
                    await using var cmd = new NpgsqlCommand(sql, connection);
                    cmd.Parameters.AddWithValue("@poster", posterPath ?? "");
                    cmd.Parameters.AddWithValue("@title", result.Title);
                    cmd.Parameters.AddWithValue("@year", result.Year ?? 0);

                    await using var reader = await cmd.ExecuteReaderAsync(ct);
                    if (await reader.ReadAsync(ct))
                    {
                        matches[result.TmdbId] = new LocalMovie
                        {
                            Id = reader.GetInt32(0),
                            Title = reader.IsDBNull(1) ? "" : reader.GetString(1),
                            MediaType = reader.IsDBNull(3) ? null : reader.GetString(3)
                        };
                    }
                }
            }
            catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
            {
                // No database is a missing movie id, not a failed lookup: the
                // tagger can still read TMDB and confirm.
                _logger.LogWarning(ex, "frl_movies match failed for a TMDB result.");
            }
            finally
            {
                if (mustClose && connection is not null) await connection.CloseAsync();
            }

            return matches;
        }

        private static object Describe(MovieIdentificationRow row) => new
        {
            sourceKey = row.SourceKey,
            tmdbId = row.TmdbId,
            movieId = row.MovieId,
            title = row.Title,
            year = row.Year,
            imdbId = row.ImdbId,
            runtimeMinutes = row.RuntimeMinutes,
            posterUrl = row.PosterPath is { Length: > 0 } path
                ? "https://image.tmdb.org/t/p/w342" + path
                : null,
            identifiedBy = row.IdentifiedBy,
            identifiedAt = row.IdentifiedAt,
            info = JsonSerializer.Deserialize<TmdbMovieDetails>(row.Info, InfoJson)
        };

        private static async Task<bool> OpenAsync(NpgsqlConnection connection, CancellationToken ct)
        {
            if (connection.State == ConnectionState.Open)
                return false;

            await connection.OpenAsync(ct);
            return true;
        }
    }
}
