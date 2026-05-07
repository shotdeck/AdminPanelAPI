using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Data;

namespace ShotDeckSearch.Controllers
{
    [ApiController]
    [Route("api/admin/movies")]
    public sealed class MoviesController : ControllerBase
    {
        private const string PosterBaseUrl = "https://image.tmdb.org/t/p/w154";
        private const int DefaultPageSize = 30;
        private const int MaxPageSize = 100;

        private readonly NpgsqlConnection _connection;
        private readonly ILogger<MoviesController> _logger;

        public MoviesController(
            NpgsqlConnection connection,
            ILogger<MoviesController> logger)
        {
            _connection = connection;
            _logger = logger;
        }

        [HttpGet]
        [ProducesResponseType(typeof(MoviePageResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<MoviePageResponse>> GetAll(
            [FromQuery] string? search,
            [FromQuery] string? mediaType,
            [FromQuery] int page = 1,
            [FromQuery] int pageSize = DefaultPageSize,
            CancellationToken ct = default)
        {
            if (page < 1) page = 1;
            if (pageSize < 1) pageSize = DefaultPageSize;
            if (pageSize > MaxPageSize) pageSize = MaxPageSize;

            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                var whereClauses = new List<string>();
                await using var cmd = new NpgsqlCommand();
                cmd.Connection = _connection;

                if (!string.IsNullOrWhiteSpace(search))
                {
                    whereClauses.Add("m.title ILIKE @search");
                    cmd.Parameters.AddWithValue("@search", $"%{search.Trim()}%");
                }

                if (!string.IsNullOrWhiteSpace(mediaType) &&
                    !string.Equals(mediaType, "all", StringComparison.OrdinalIgnoreCase))
                {
                    whereClauses.Add("m.media_type = @mediaType");
                    cmd.Parameters.AddWithValue("@mediaType", mediaType.Trim().ToLower());
                }

                var whereStr = whereClauses.Count > 0
                    ? "WHERE " + string.Join(" AND ", whereClauses)
                    : "";

                var countSql = $"SELECT COUNT(*) FROM frl.frl_movies m {whereStr};";
                cmd.CommandText = countSql;
                var totalCount = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

                var offset = (page - 1) * pageSize;
                var dataSql = $@"
SELECT m.idnum, m.title, m.year, m.media_type, m.poster
FROM frl.frl_movies m
{whereStr}
ORDER BY m.title ASC
LIMIT @limit OFFSET @offset;";

                cmd.CommandText = dataSql;
                cmd.Parameters.AddWithValue("@limit", pageSize);
                cmd.Parameters.AddWithValue("@offset", offset);

                await using var reader = await cmd.ExecuteReaderAsync(ct);
                var movies = new List<MovieDto>();

                while (await reader.ReadAsync(ct))
                {
                    movies.Add(new MovieDto
                    {
                        Id = reader.GetInt32(reader.GetOrdinal("idnum")),
                        Title = reader.IsDBNull(reader.GetOrdinal("title"))
                            ? "" : reader.GetString(reader.GetOrdinal("title")),
                        Year = reader.IsDBNull(reader.GetOrdinal("year"))
                            ? null : reader.GetInt32(reader.GetOrdinal("year")),
                        MediaType = reader.IsDBNull(reader.GetOrdinal("media_type"))
                            ? "" : reader.GetString(reader.GetOrdinal("media_type")),
                        Poster = reader.IsDBNull(reader.GetOrdinal("poster"))
                            ? null : PosterBaseUrl + reader.GetString(reader.GetOrdinal("poster"))
                    });
                }

                var totalPages = (int)Math.Ceiling(totalCount / (double)pageSize);

                return Ok(new MoviePageResponse
                {
                    Movies = movies,
                    Page = page,
                    PageSize = pageSize,
                    TotalCount = totalCount,
                    TotalPages = totalPages
                });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        [HttpGet("media-types")]
        [ProducesResponseType(typeof(List<string>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<string>>> GetMediaTypes(CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"
SELECT DISTINCT media_type
FROM frl.frl_movies
WHERE media_type IS NOT NULL AND media_type <> ''
ORDER BY media_type;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                var types = new List<string>();
                while (await reader.ReadAsync(ct))
                {
                    types.Add(reader.GetString(0));
                }

                return Ok(types);
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }
    }

    public sealed class MovieDto
    {
        public int Id { get; set; }
        public string Title { get; set; } = "";
        public int? Year { get; set; }
        public string MediaType { get; set; } = "";
        public string? Poster { get; set; }
    }

    public sealed class MoviePageResponse
    {
        public List<MovieDto> Movies { get; set; } = new();
        public int Page { get; set; }
        public int PageSize { get; set; }
        public int TotalCount { get; set; }
        public int TotalPages { get; set; }
    }
}
