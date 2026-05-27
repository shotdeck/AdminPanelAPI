using AdminPanelAPI.Models;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using NpgsqlTypes;
using System.Data;

namespace AdminPanelAPI.Controllers
{
    [ApiController]
    [Route("api/admin/filming-locations")]
    public sealed class FilmingLocationController : ControllerBase
    {
        private readonly NpgsqlConnection _connection;
        private readonly ILogger<FilmingLocationController> _logger;

        public FilmingLocationController(
            NpgsqlConnection connection,
            ILogger<FilmingLocationController> logger)
        {
            _connection = connection;
            _logger = logger;
        }

        /// <summary>
        /// Reads frl_images.filming_location, parses the colon-delimited text,
        /// and upserts rows into frl_images_location.
        /// Format: "Planet:Continent:Country:StateRegion:City:SpecificLocation"
        /// </summary>
        [HttpPost("parse")]
        [ProducesResponseType(typeof(ParseProgressResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<ParseProgressResponse>> ParseAndStore(
            [FromQuery] int batchSize = 1000,
            CancellationToken ct = default)
        {
            if (batchSize < 1) batchSize = 1000;

            await EnsureOpenAsync(ct);

            try
            {
                var countSql = @"
SELECT COUNT(*)
FROM frl.frl_images i
WHERE i.filming_location IS NOT NULL
  AND TRIM(i.filming_location) <> ''
  AND TRIM(i.filming_location) <> ':::::'
  AND NOT EXISTS (
      SELECT 1 FROM frl.frl_images_location loc WHERE loc.image_id = i.idnum
  );";

                await using var countCmd = new NpgsqlCommand(countSql, _connection);
                var totalImages = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct));

                var processed = 0;
                var skipped = 0;
                var failed = 0;
                var lastId = 0;

                while (true)
                {
                    ct.ThrowIfCancellationRequested();

                    var fetchSql = @"
SELECT i.idnum, i.filming_location
FROM frl.frl_images i
WHERE i.idnum > @lastId
  AND i.filming_location IS NOT NULL
  AND TRIM(i.filming_location) <> ''
  AND TRIM(i.filming_location) <> ':::::'
  AND NOT EXISTS (
      SELECT 1 FROM frl.frl_images_location loc WHERE loc.image_id = i.idnum
  )
ORDER BY i.idnum
LIMIT @limit;";

                    await using var fetchCmd = new NpgsqlCommand(fetchSql, _connection);
                    fetchCmd.Parameters.AddWithValue("@lastId", lastId);
                    fetchCmd.Parameters.AddWithValue("@limit", batchSize);

                    var batch = new List<(int imageId, string rawLocation)>();
                    await using (var reader = await fetchCmd.ExecuteReaderAsync(ct))
                    {
                        while (await reader.ReadAsync(ct))
                        {
                            var imageId = reader.GetInt32(0);
                            var raw = reader.GetString(1);
                            batch.Add((imageId, raw));
                        }
                    }

                    if (batch.Count == 0) break;

                    foreach (var (imageId, rawLocation) in batch)
                    {
                        try
                        {
                            var parsed = ParseFilmingLocation(rawLocation);
                            if (parsed == null)
                            {
                                skipped++;
                                continue;
                            }

                            var insertSql = @"
INSERT INTO frl.frl_images_location
    (image_id, raw_location, planet, continent, country, state_region, city, specific_location)
VALUES
    (@imageId, @raw, @planet, @continent, @country, @stateRegion, @city, @specificLocation)
ON CONFLICT DO NOTHING;";

                            await using var insertCmd = new NpgsqlCommand(insertSql, _connection);
                            insertCmd.Parameters.AddWithValue("@imageId", imageId);
                            insertCmd.Parameters.AddWithValue("@raw", rawLocation.Trim());
                            insertCmd.Parameters.AddWithValue("@planet", (object?)NullIfEmpty(parsed.Planet) ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@continent", (object?)NullIfEmpty(parsed.Continent) ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@country", (object?)NullIfEmpty(parsed.Country) ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@stateRegion", (object?)NullIfEmpty(parsed.StateRegion) ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@city", (object?)NullIfEmpty(parsed.City) ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@specificLocation", (object?)NullIfEmpty(parsed.SpecificLocation) ?? DBNull.Value);

                            await insertCmd.ExecuteNonQueryAsync(ct);
                            processed++;
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Failed to parse/insert location for image {ImageId}", imageId);
                            failed++;
                        }
                    }

                    lastId = batch.Max(b => b.imageId);
                }

                return Ok(new ParseProgressResponse
                {
                    TotalImages = totalImages,
                    Processed = processed,
                    Skipped = skipped,
                    Failed = failed,
                    Status = "Completed"
                });
            }
            finally
            {
                await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// GET all filming locations with optional filtering and pagination.
        /// </summary>
        [HttpGet]
        [ProducesResponseType(typeof(FilmingLocationPageResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<FilmingLocationPageResponse>> GetAll(
            [FromQuery] string? country,
            [FromQuery] string? city,
            [FromQuery] int page = 1,
            [FromQuery] int pageSize = 50,
            CancellationToken ct = default)
        {
            if (page < 1) page = 1;
            if (pageSize < 1 || pageSize > 200) pageSize = 50;

            await EnsureOpenAsync(ct);

            try
            {
                var whereClauses = new List<string>();
                var parameters = new List<NpgsqlParameter>();

                if (!string.IsNullOrWhiteSpace(country))
                {
                    whereClauses.Add("country ILIKE @country");
                    parameters.Add(new NpgsqlParameter("@country", $"%{country.Trim()}%"));
                }

                if (!string.IsNullOrWhiteSpace(city))
                {
                    whereClauses.Add("city ILIKE @city");
                    parameters.Add(new NpgsqlParameter("@city", $"%{city.Trim()}%"));
                }

                var whereStr = whereClauses.Count > 0
                    ? "WHERE " + string.Join(" AND ", whereClauses)
                    : "";

                var countSql = $"SELECT COUNT(*) FROM frl.frl_images_location {whereStr};";
                await using var countCmd = new NpgsqlCommand(countSql, _connection);
                countCmd.Parameters.AddRange(parameters.ToArray());
                var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct));

                var offset = (page - 1) * pageSize;
                var dataSql = $@"
SELECT id, image_id, raw_location, planet, continent, country, state_region,
       city, specific_location, coordinates, created_at, updated_at
FROM frl.frl_images_location
{whereStr}
ORDER BY id
LIMIT @limit OFFSET @offset;";

                await using var dataCmd = new NpgsqlCommand(dataSql, _connection);
                foreach (var p in parameters)
                {
                    dataCmd.Parameters.Add(new NpgsqlParameter(p.ParameterName, p.Value));
                }
                dataCmd.Parameters.AddWithValue("@limit", pageSize);
                dataCmd.Parameters.AddWithValue("@offset", offset);

                await using var reader = await dataCmd.ExecuteReaderAsync(ct);
                var results = new List<FilmingLocationDto>();

                while (await reader.ReadAsync(ct))
                {
                    results.Add(MapToDto(reader));
                }

                return Ok(new FilmingLocationPageResponse
                {
                    TotalCount = totalCount,
                    Page = page,
                    PageSize = pageSize,
                    Items = results
                });
            }
            finally
            {
                await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// GET a single filming location by ID.
        /// </summary>
        [HttpGet("{id:long}")]
        [ProducesResponseType(typeof(FilmingLocationDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<FilmingLocationDto>> GetById(long id, CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            try
            {
                const string sql = @"
SELECT id, image_id, raw_location, planet, continent, country, state_region,
       city, specific_location, coordinates, created_at, updated_at
FROM frl.frl_images_location
WHERE id = @id;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return NotFound(new { Message = $"Filming location with ID {id} not found." });

                return Ok(MapToDto(reader));
            }
            finally
            {
                await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// GET all filming locations for a specific image.
        /// </summary>
        [HttpGet("by-image/{imageId:int}")]
        [ProducesResponseType(typeof(List<FilmingLocationDto>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<FilmingLocationDto>>> GetByImageId(int imageId, CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            try
            {
                const string sql = @"
SELECT id, image_id, raw_location, planet, continent, country, state_region,
       city, specific_location, coordinates, created_at, updated_at
FROM frl.frl_images_location
WHERE image_id = @imageId
ORDER BY id;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@imageId", imageId);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                var results = new List<FilmingLocationDto>();
                while (await reader.ReadAsync(ct))
                {
                    results.Add(MapToDto(reader));
                }

                return Ok(results);
            }
            finally
            {
                await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// POST create a new filming location record.
        /// </summary>
        [HttpPost]
        [ProducesResponseType(typeof(FilmingLocationDto), StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public async Task<ActionResult<FilmingLocationDto>> Create(
            [FromBody] FilmingLocationCreateDto dto,
            CancellationToken ct = default)
        {
            if (dto.ImageId <= 0)
                return BadRequest(new { Message = "ImageId is required and must be > 0." });

            await EnsureOpenAsync(ct);

            try
            {
                var sql = @"
INSERT INTO frl.frl_images_location
    (image_id, raw_location, planet, continent, country, state_region, city, specific_location, coordinates)
VALUES
    (@imageId, @raw, @planet, @continent, @country, @stateRegion, @city, @specificLocation, @coordinates)
RETURNING id, image_id, raw_location, planet, continent, country, state_region,
          city, specific_location, coordinates, created_at, updated_at;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@imageId", dto.ImageId);

                var rawParts = new[]
                {
                    dto.Planet ?? "", dto.Continent ?? "", dto.Country ?? "",
                    dto.StateRegion ?? "", dto.City ?? "", dto.SpecificLocation ?? ""
                };
                cmd.Parameters.AddWithValue("@raw", string.Join(":", rawParts));

                cmd.Parameters.AddWithValue("@planet", (object?)NullIfEmpty(dto.Planet) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@continent", (object?)NullIfEmpty(dto.Continent) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@country", (object?)NullIfEmpty(dto.Country) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@stateRegion", (object?)NullIfEmpty(dto.StateRegion) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@city", (object?)NullIfEmpty(dto.City) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@specificLocation", (object?)NullIfEmpty(dto.SpecificLocation) ?? DBNull.Value);

                if (dto.Latitude.HasValue && dto.Longitude.HasValue)
                {
                    cmd.Parameters.AddWithValue("@coordinates",
                        NpgsqlDbType.Point,
                        new NpgsqlPoint(dto.Longitude.Value, dto.Latitude.Value));
                }
                else
                {
                    cmd.Parameters.AddWithValue("@coordinates", NpgsqlDbType.Point, DBNull.Value);
                }

                await using var reader = await cmd.ExecuteReaderAsync(ct);
                await reader.ReadAsync(ct);
                var created = MapToDto(reader);

                return CreatedAtAction(nameof(GetById), new { id = created.Id }, created);
            }
            finally
            {
                await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// PUT update an existing filming location record.
        /// </summary>
        [HttpPut("{id:long}")]
        [ProducesResponseType(typeof(FilmingLocationDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<FilmingLocationDto>> Update(
            long id,
            [FromBody] FilmingLocationUpdateDto dto,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            try
            {
                var sql = @"
UPDATE frl.frl_images_location
SET planet = @planet,
    continent = @continent,
    country = @country,
    state_region = @stateRegion,
    city = @city,
    specific_location = @specificLocation,
    coordinates = @coordinates,
    updated_at = NOW()
WHERE id = @id
RETURNING id, image_id, raw_location, planet, continent, country, state_region,
          city, specific_location, coordinates, created_at, updated_at;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                cmd.Parameters.AddWithValue("@planet", (object?)NullIfEmpty(dto.Planet) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@continent", (object?)NullIfEmpty(dto.Continent) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@country", (object?)NullIfEmpty(dto.Country) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@stateRegion", (object?)NullIfEmpty(dto.StateRegion) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@city", (object?)NullIfEmpty(dto.City) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@specificLocation", (object?)NullIfEmpty(dto.SpecificLocation) ?? DBNull.Value);

                if (dto.Latitude.HasValue && dto.Longitude.HasValue)
                {
                    cmd.Parameters.AddWithValue("@coordinates",
                        NpgsqlDbType.Point,
                        new NpgsqlPoint(dto.Longitude.Value, dto.Latitude.Value));
                }
                else
                {
                    cmd.Parameters.AddWithValue("@coordinates", NpgsqlDbType.Point, DBNull.Value);
                }

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return NotFound(new { Message = $"Filming location with ID {id} not found." });

                return Ok(MapToDto(reader));
            }
            finally
            {
                await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// DELETE a filming location record.
        /// </summary>
        [HttpDelete("{id:long}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<IActionResult> Delete(long id, CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            try
            {
                const string sql = "DELETE FROM frl.frl_images_location WHERE id = @id;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);

                var affected = await cmd.ExecuteNonQueryAsync(ct);
                if (affected == 0)
                    return NotFound(new { Message = $"Filming location with ID {id} not found." });

                return NoContent();
            }
            finally
            {
                await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// GET distinct values for a specific column (useful for dropdowns/autocomplete).
        /// </summary>
        [HttpGet("distinct/{column}")]
        [ProducesResponseType(typeof(List<string>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public async Task<ActionResult<List<string>>> GetDistinctValues(
            string column,
            [FromQuery] string? search,
            [FromQuery] int limit = 100,
            CancellationToken ct = default)
        {
            var allowedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "planet", "continent", "country", "state_region", "city", "specific_location"
            };

            if (!allowedColumns.Contains(column))
                return BadRequest(new { Message = $"Column '{column}' is not allowed. Allowed: {string.Join(", ", allowedColumns)}" });

            if (limit < 1 || limit > 1000) limit = 100;

            await EnsureOpenAsync(ct);

            try
            {
                var wherePart = string.IsNullOrWhiteSpace(search)
                    ? ""
                    : $"AND {column} ILIKE @search";

                var sql = $@"
SELECT DISTINCT {column}
FROM frl.frl_images_location
WHERE {column} IS NOT NULL AND TRIM({column}) <> ''
{wherePart}
ORDER BY {column}
LIMIT @limit;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                if (!string.IsNullOrWhiteSpace(search))
                    cmd.Parameters.AddWithValue("@search", $"%{search.Trim()}%");
                cmd.Parameters.AddWithValue("@limit", limit);

                await using var reader = await cmd.ExecuteReaderAsync(ct);
                var results = new List<string>();

                while (await reader.ReadAsync(ct))
                {
                    results.Add(reader.GetString(0));
                }

                return Ok(results);
            }
            finally
            {
                await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// POST update coordinates (lat/lng) for a specific location record.
        /// </summary>
        [HttpPost("{id:long}/coordinates")]
        [ProducesResponseType(typeof(FilmingLocationDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<FilmingLocationDto>> SetCoordinates(
            long id,
            [FromBody] CoordinatesDto dto,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            try
            {
                var sql = @"
UPDATE frl.frl_images_location
SET coordinates = @coordinates,
    updated_at = NOW()
WHERE id = @id
RETURNING id, image_id, raw_location, planet, continent, country, state_region,
          city, specific_location, coordinates, created_at, updated_at;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                cmd.Parameters.AddWithValue("@coordinates",
                    NpgsqlDbType.Point,
                    new NpgsqlPoint(dto.Longitude, dto.Latitude));

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return NotFound(new { Message = $"Filming location with ID {id} not found." });

                return Ok(MapToDto(reader));
            }
            finally
            {
                await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// GET summary statistics for the parsed locations.
        /// </summary>
        [HttpGet("stats")]
        [ProducesResponseType(typeof(FilmingLocationStats), StatusCodes.Status200OK)]
        public async Task<ActionResult<FilmingLocationStats>> GetStats(CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            try
            {
                const string sql = @"
SELECT
    COUNT(*) AS total_locations,
    COUNT(coordinates) AS geocoded_count,
    COUNT(DISTINCT country) FILTER (WHERE country IS NOT NULL AND TRIM(country) <> '') AS distinct_countries,
    COUNT(DISTINCT city) FILTER (WHERE city IS NOT NULL AND TRIM(city) <> '') AS distinct_cities,
    COUNT(DISTINCT image_id) AS distinct_images
FROM frl.frl_images_location;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                await reader.ReadAsync(ct);

                return Ok(new FilmingLocationStats
                {
                    TotalLocations = reader.GetInt64(0),
                    GeocodedCount = reader.GetInt64(1),
                    DistinctCountries = reader.GetInt64(2),
                    DistinctCities = reader.GetInt64(3),
                    DistinctImages = reader.GetInt64(4)
                });
            }
            finally
            {
                await _connection.CloseAsync();
            }
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private static ParsedLocation? ParseFilmingLocation(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw))
                return null;

            var trimmed = raw.Trim();
            var parts = trimmed.Split(':');

            if (parts.All(p => string.IsNullOrWhiteSpace(p)))
                return null;

            return new ParsedLocation
            {
                Planet = parts.Length > 0 ? parts[0].Trim() : null,
                Continent = parts.Length > 1 ? parts[1].Trim() : null,
                Country = parts.Length > 2 ? parts[2].Trim() : null,
                StateRegion = parts.Length > 3 ? parts[3].Trim() : null,
                City = parts.Length > 4 ? parts[4].Trim() : null,
                SpecificLocation = parts.Length > 5 ? parts[5].Trim() : null
            };
        }

        private static string? NullIfEmpty(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

        private static FilmingLocationDto MapToDto(NpgsqlDataReader reader)
        {
            double? lat = null;
            double? lng = null;

            var coordOrd = reader.GetOrdinal("coordinates");
            if (!reader.IsDBNull(coordOrd))
            {
                var point = reader.GetFieldValue<NpgsqlPoint>(coordOrd);
                lng = point.X;
                lat = point.Y;
            }

            return new FilmingLocationDto
            {
                Id = reader.GetInt64(reader.GetOrdinal("id")),
                ImageId = reader.GetInt32(reader.GetOrdinal("image_id")),
                RawLocation = reader.IsDBNull(reader.GetOrdinal("raw_location")) ? null : reader.GetString(reader.GetOrdinal("raw_location")),
                Planet = reader.IsDBNull(reader.GetOrdinal("planet")) ? null : reader.GetString(reader.GetOrdinal("planet")),
                Continent = reader.IsDBNull(reader.GetOrdinal("continent")) ? null : reader.GetString(reader.GetOrdinal("continent")),
                Country = reader.IsDBNull(reader.GetOrdinal("country")) ? null : reader.GetString(reader.GetOrdinal("country")),
                StateRegion = reader.IsDBNull(reader.GetOrdinal("state_region")) ? null : reader.GetString(reader.GetOrdinal("state_region")),
                City = reader.IsDBNull(reader.GetOrdinal("city")) ? null : reader.GetString(reader.GetOrdinal("city")),
                SpecificLocation = reader.IsDBNull(reader.GetOrdinal("specific_location")) ? null : reader.GetString(reader.GetOrdinal("specific_location")),
                Latitude = lat,
                Longitude = lng,
                CreatedAt = reader.GetDateTime(reader.GetOrdinal("created_at")),
                UpdatedAt = reader.GetDateTime(reader.GetOrdinal("updated_at"))
            };
        }

        private async Task EnsureOpenAsync(CancellationToken ct)
        {
            if (_connection.State != ConnectionState.Open)
                await _connection.OpenAsync(ct);
        }

        private sealed class ParsedLocation
        {
            public string? Planet { get; set; }
            public string? Continent { get; set; }
            public string? Country { get; set; }
            public string? StateRegion { get; set; }
            public string? City { get; set; }
            public string? SpecificLocation { get; set; }
        }
    }

    public sealed class CoordinatesDto
    {
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }

    public sealed class FilmingLocationPageResponse
    {
        public int TotalCount { get; set; }
        public int Page { get; set; }
        public int PageSize { get; set; }
        public List<FilmingLocationDto> Items { get; set; } = new();
    }

    public sealed class FilmingLocationStats
    {
        public long TotalLocations { get; set; }
        public long GeocodedCount { get; set; }
        public long DistinctCountries { get; set; }
        public long DistinctCities { get; set; }
        public long DistinctImages { get; set; }
    }
}
