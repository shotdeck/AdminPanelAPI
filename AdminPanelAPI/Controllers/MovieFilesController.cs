using AdminPanelAPI.Models;
using AdminPanelAPI.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Data;

namespace ShotDeckSearch.Controllers
{
    /// <summary>
    /// File-manager endpoints over the R2 bucket holding movie source files.
    /// Backs the "Movie Files" page of the dashboard: browse folders, upload
    /// HD and slimmed copies, play back and download, and move a staging
    /// folder onto its movie id once the frl_movies record exists.
    /// </summary>
    [ApiController]
    [Route("api/admin/movie-files")]
    public sealed class MovieFilesController : ControllerBase
    {
        private const string PosterBaseUrl = "https://image.tmdb.org/t/p/w342";

        private readonly IMovieFileStorageService _storage;
        private readonly Lazy<NpgsqlConnection> _connection;
        private readonly ILogger<MovieFilesController> _logger;

        public MovieFilesController(
            IMovieFileStorageService storage,
            Lazy<NpgsqlConnection> connection,
            ILogger<MovieFilesController> logger)
        {
            _storage = storage;
            _connection = connection;
            _logger = logger;
        }

        /// <summary>List one folder: sub-folders, files, and a page token.</summary>
        [HttpGet("list")]
        [ProducesResponseType(typeof(MovieFileListResponse), StatusCodes.Status200OK)]
        public async Task<IActionResult> List(
            [FromQuery] string? prefix,
            [FromQuery] bool mp4Only = false,
            [FromQuery] string? token = null,
            [FromQuery] int pageSize = 500,
            CancellationToken ct = default)
        {
            return await GuardAsync(() => _storage.ListAsync(prefix ?? "", mp4Only, token, pageSize, ct));
        }

        [HttpPost("folder")]
        public async Task<IActionResult> CreateFolder(
            [FromBody] CreateFolderRequest request, CancellationToken ct = default)
        {
            return await GuardAsync(async () =>
            {
                await _storage.CreateFolderAsync(request.Prefix, ct);
                return new { prefix = MovieFileStorageService.NormalizePrefix(request.Prefix) };
            });
        }

        /// <summary>
        /// Presigned PUT for a single-shot upload. The browser must send the
        /// returned contentType verbatim or R2 rejects the signature.
        /// </summary>
        [HttpPost("upload-url")]
        [ProducesResponseType(typeof(PresignedUploadResponse), StatusCodes.Status200OK)]
        public async Task<IActionResult> CreateUploadUrl(
            [FromBody] UploadUrlRequest request, CancellationToken ct = default)
        {
            return await GuardAsync(() => _storage.CreateSingleUploadAsync(
                request.Prefix, request.FileName,
                MovieFileVariantParser.Parse(request.Variant), request.ContentType, ct));
        }

        [HttpPost("multipart/initiate")]
        [ProducesResponseType(typeof(MultipartUploadResponse), StatusCodes.Status200OK)]
        public async Task<IActionResult> InitiateMultipart(
            [FromBody] UploadUrlRequest request, CancellationToken ct = default)
        {
            return await GuardAsync(() => _storage.InitiateMultipartUploadAsync(
                request.Prefix, request.FileName,
                MovieFileVariantParser.Parse(request.Variant), request.ContentType, ct));
        }

        [HttpPost("multipart/part-urls")]
        public IActionResult CreatePartUrls([FromBody] PartUrlsRequest request)
        {
            try
            {
                var parts = _storage.CreatePartUrls(request.Key, request.UploadId, request.PartNumbers);
                return Ok(new { parts });
            }
            catch (ArgumentException ex)
            {
                return BadRequest(new { error = ex.Message });
            }
        }

        [HttpPost("multipart/complete")]
        public async Task<IActionResult> CompleteMultipart(
            [FromBody] CompleteMultipartRequest request, CancellationToken ct = default)
        {
            return await GuardAsync(async () =>
            {
                await _storage.CompleteMultipartUploadAsync(request.Key, request.UploadId, request.Parts, ct);
                return new { key = request.Key, parts = request.Parts.Count };
            });
        }

        [HttpPost("multipart/abort")]
        public async Task<IActionResult> AbortMultipart(
            [FromBody] AbortMultipartRequest request, CancellationToken ct = default)
        {
            return await GuardAsync(async () =>
            {
                await _storage.AbortMultipartUploadAsync(request.Key, request.UploadId, ct);
                return new { aborted = true };
            });
        }

        /// <summary>
        /// Presigned GET, minted per request because these expire after an hour
        /// and a long browsing session would otherwise hand out dead URLs.
        /// </summary>
        [HttpGet("download-url")]
        public IActionResult CreateDownloadUrl(
            [FromQuery] string key, [FromQuery] bool download = false)
        {
            try
            {
                return Ok(new { key, url = _storage.CreateDownloadUrl(key, download) });
            }
            catch (ArgumentException ex)
            {
                return BadRequest(new { error = ex.Message });
            }
        }

        [HttpPost("rename")]
        public async Task<IActionResult> Rename(
            [FromBody] RenameObjectRequest request, CancellationToken ct = default)
        {
            return await GuardAsync(async () =>
            {
                await _storage.RenameObjectAsync(request.Key, request.NewName, ct);
                return new { renamed = true };
            });
        }

        [HttpDelete("object")]
        public async Task<IActionResult> DeleteObject(
            [FromQuery] string key, CancellationToken ct = default)
        {
            return await GuardAsync(async () =>
            {
                await _storage.DeleteObjectAsync(key, ct);
                return new { deleted = key };
            });
        }

        /// <summary>Recursive delete, restricted to a folder inside the staging area.</summary>
        [HttpDelete("folder")]
        public async Task<IActionResult> DeleteFolder(
            [FromQuery] string prefix, CancellationToken ct = default)
        {
            return await GuardAsync(async () =>
            {
                var deleted = await _storage.DeletePrefixAsync(prefix, ct);
                return new { prefix, deleted };
            });
        }

        /// <summary>
        /// Server-side copy of a whole folder, then delete of the source: used to
        /// promote "_staging/{batch}/" to "{movieId}/" once the movie exists.
        /// Nothing leaves Cloudflare, so no re-upload and no egress.
        /// </summary>
        [HttpPost("move-folder")]
        [ProducesResponseType(typeof(MovePrefixResponse), StatusCodes.Status200OK)]
        public async Task<IActionResult> MoveFolder(
            [FromBody] MovePrefixRequest request, CancellationToken ct = default)
        {
            return await GuardAsync(() => _storage.MovePrefixAsync(
                request.SourcePrefix, request.TargetPrefix, request.DeleteSource, ct));
        }

        /// <summary>
        /// Title search over frl_movies for the folder-jump dropdown. Unlike
        /// /api/admin/movies this has no live-image requirement, so movies whose
        /// assets are still being uploaded are findable.
        /// </summary>
        [HttpGet("movie-search")]
        public async Task<IActionResult> SearchMovies(
            [FromQuery] string? search,
            [FromQuery] int limit = 20,
            CancellationToken ct = default)
        {
            var term = (search ?? "").Trim();
            if (term.Length < 2)
                return Ok(new { movies = Array.Empty<object>() });

            if (limit < 1) limit = 20;
            if (limit > 50) limit = 50;

            var connection = _connection.Value;
            var mustClose = false;
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"
SELECT idnum, title, year, media_type::text AS media_type, poster
FROM frl.frl_movies
WHERE title ILIKE @search
ORDER BY (title ILIKE @prefix) DESC, title ASC, year DESC NULLS LAST
LIMIT @limit;";

                await using var cmd = new NpgsqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("@search", $"%{term}%");
                cmd.Parameters.AddWithValue("@prefix", $"{term}%");
                cmd.Parameters.AddWithValue("@limit", limit);

                await using var reader = await cmd.ExecuteReaderAsync(ct);
                var movies = new List<object>();
                while (await reader.ReadAsync(ct))
                {
                    movies.Add(new
                    {
                        id = reader.GetInt32(0),
                        title = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        year = reader.IsDBNull(2) ? (int?)null : reader.GetInt32(2),
                        mediaType = reader.IsDBNull(3) ? "" : reader.GetString(3),
                        poster = reader.IsDBNull(4) ? null : PosterBaseUrl + reader.GetString(4)
                    });
                }

                return Ok(new { movies });
            }
            finally
            {
                if (mustClose) await connection.CloseAsync();
            }
        }

        /// <summary>
        /// Titles for numeric folder names, so the browser can show
        /// "1042 — Heat (1995)" instead of a bare id, plus the poster the
        /// grid view draws. Called a page of folders at a time.
        /// </summary>
        [HttpGet("movie-titles")]
        public async Task<IActionResult> GetMovieTitles(
            [FromQuery] string? ids, CancellationToken ct = default)
        {
            var movieIds = (ids ?? "")
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(part => int.TryParse(part, out var id) ? id : (int?)null)
                .Where(id => id.HasValue)
                .Select(id => id!.Value)
                .Distinct()
                .Take(1000)
                .ToArray();

            if (movieIds.Length == 0)
                return Ok(new { movies = Array.Empty<object>() });

            var connection = _connection.Value;
            var mustClose = false;
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"
SELECT idnum, title, year, media_type::text AS media_type, poster
FROM frl.frl_movies
WHERE idnum = ANY(@ids);";

                await using var cmd = new NpgsqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("@ids", movieIds);

                await using var reader = await cmd.ExecuteReaderAsync(ct);
                var movies = new List<object>();
                while (await reader.ReadAsync(ct))
                {
                    movies.Add(new
                    {
                        id = reader.GetInt32(0),
                        title = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        year = reader.IsDBNull(2) ? (int?)null : reader.GetInt32(2),
                        mediaType = reader.IsDBNull(3) ? "" : reader.GetString(3),
                        poster = reader.IsDBNull(4) ? null : PosterBaseUrl + reader.GetString(4)
                    });
                }

                return Ok(new { movies });
            }
            finally
            {
                if (mustClose) await connection.CloseAsync();
            }
        }

        private async Task<IActionResult> GuardAsync<T>(Func<Task<T>> action)
        {
            try
            {
                return Ok(await action());
            }
            catch (ArgumentException ex)
            {
                return BadRequest(new { error = ex.Message });
            }
            catch (Amazon.S3.AmazonS3Exception ex)
            {
                _logger.LogError(ex, "R2 request failed for the movie-files browser.");
                return StatusCode(502, new { error = $"R2 error: {ex.Message}" });
            }
        }
    }
}
