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
                    whereClauses.Add("m.media_type::text = @mediaType");
                    cmd.Parameters.AddWithValue("@mediaType", mediaType.Trim().ToLower());
                }

                var whereStr = whereClauses.Count > 0
                    ? "WHERE " + string.Join(" AND ", whereClauses)
                    : "";

                // Only include movies that have at least one live image
                var existsClause = "EXISTS (SELECT 1 FROM frl.frl_images ei WHERE ei.movieid = m.idnum AND ei.status = 'live')";
                var countWhere = whereClauses.Count > 0
                    ? "WHERE " + string.Join(" AND ", whereClauses) + " AND " + existsClause
                    : "WHERE " + existsClause;

                var countSql = $"SELECT COUNT(*) FROM frl.frl_movies m {countWhere};";
                cmd.CommandText = countSql;
                var totalCount = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

                var offset = (page - 1) * pageSize;
                var dataSql = $@"
SELECT m.idnum, m.title, m.year, m.media_type::text AS media_type, m.poster,
       COALESCE(ic.image_count, 0) AS image_count,
       COALESCE(cc.clip_count, 0) AS clip_count,
       COALESCE(qc.qc_total, 0) AS qc_total,
       COALESCE(qc.qc_checked, 0) AS qc_checked
FROM frl.frl_movies m
INNER JOIN (
    SELECT movieid, COUNT(*) AS image_count
    FROM frl.frl_images
    WHERE status = 'live'
    GROUP BY movieid
) ic ON ic.movieid = m.idnum
LEFT JOIN (
    SELECT movieid, COUNT(*) AS clip_count
    FROM frl.frl_image_scene_boundaries
    GROUP BY movieid
) cc ON cc.movieid = m.idnum
LEFT JOIN (
    SELECT sub.movieid,
           COUNT(*) AS qc_total,
           COUNT(*) FILTER (WHERE sub.all_checked) AS qc_checked
    FROM (
        SELECT i.movieid, cm.imageid,
               bool_and(cm.status IN ('ok', 'bad', 'flagged')) AS all_checked
        FROM frl.frl_join_image_camera_movements cm
        INNER JOIN frl.frl_images i ON i.idnum = cm.imageid
        WHERE i.status = 'live'
        GROUP BY i.movieid, cm.imageid
    ) sub
    GROUP BY sub.movieid
) qc ON qc.movieid = m.idnum
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
                    var qcTotal = Convert.ToInt32(reader["qc_total"]);
                    var qcChecked = Convert.ToInt32(reader["qc_checked"]);
                    var clipCount = Convert.ToInt32(reader["clip_count"]);
                    string qcStatus;
                    if (qcTotal == 0)
                        qcStatus = "none";
                    else if (qcChecked == 0)
                        qcStatus = "not_started";
                    else if (qcChecked >= clipCount)
                        qcStatus = "processed";
                    else
                        qcStatus = "partial";

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
                            ? null : PosterBaseUrl + reader.GetString(reader.GetOrdinal("poster")),
                        ImageCount = Convert.ToInt32(reader["image_count"]),
                        ClipCount = Convert.ToInt32(reader["clip_count"]),
                        QcStatus = qcStatus,
                        QcTotal = qcTotal,
                        QcChecked = qcChecked
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
SELECT DISTINCT media_type::text AS media_type
FROM frl.frl_movies
WHERE media_type IS NOT NULL
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
        public int ImageCount { get; set; }
        public int ClipCount { get; set; }
        public string QcStatus { get; set; } = "none";
        public int QcTotal { get; set; }
        public int QcChecked { get; set; }
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
