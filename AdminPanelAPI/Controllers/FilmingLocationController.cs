using AdminPanelAPI.Models;
using FuzzySharp;
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
        private const int FuzzyAutoMatchThreshold = 85;
        private const int FuzzyReviewThreshold = 65;

        private readonly NpgsqlConnection _connection;
        private readonly ILogger<FilmingLocationController> _logger;

        public FilmingLocationController(
            NpgsqlConnection connection,
            ILogger<FilmingLocationController> logger)
        {
            _connection = connection;
            _logger = logger;
        }

        // ═══════════════════════════════════════════════════════════
        //  PARSE – read frl_images.filming_location → normalised rows
        // ═══════════════════════════════════════════════════════════

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
                // Pre-load lookup caches
                var continentCache = await LoadLookupCacheAsync("frl_location_continents", ct);
                var continentAliasCache = await LoadAliasCacheAsync("frl_location_continent_aliases", "continent_id", ct);
                var countryCache = await LoadLookupCacheAsync("frl_location_countries", ct);
                var countryAliasCache = await LoadAliasCacheAsync("frl_location_country_aliases", "country_id", ct);

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
                var fuzzyMatched = 0;
                var flaggedForReview = 0;
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
                            batch.Add((reader.GetInt32(0), reader.GetString(1)));
                    }

                    if (batch.Count == 0) break;

                    foreach (var (imageId, rawLocation) in batch)
                    {
                        try
                        {
                            var parts = ParseParts(rawLocation);
                            if (parts == null) { skipped++; continue; }

                            var minConfidence = 1.0f;
                            var needsReview = false;

                            // Resolve continent
                            int? continentId = null;
                            if (!string.IsNullOrWhiteSpace(parts.Continent))
                            {
                                var (id, conf) = ResolveWithFuzzy(
                                    parts.Continent, continentCache, continentAliasCache);
                                continentId = id;
                                if (conf < minConfidence) minConfidence = conf;
                                if (id == null && conf < 1.0f) needsReview = true;
                                if (conf < 1.0f && id != null) fuzzyMatched++;
                            }

                            // Resolve country
                            int? countryId = null;
                            if (!string.IsNullOrWhiteSpace(parts.Country))
                            {
                                var (id, conf) = ResolveWithFuzzy(
                                    parts.Country, countryCache, countryAliasCache);
                                countryId = id;
                                if (conf < minConfidence) minConfidence = conf;
                                if (id == null && conf < 1.0f) needsReview = true;
                                if (conf < 1.0f && id != null) fuzzyMatched++;
                            }

                            // Create continent if not resolved and text is present
                            if (continentId == null && !string.IsNullOrWhiteSpace(parts.Continent))
                            {
                                needsReview = true;
                            }

                            // Create country if not resolved and text is present
                            if (countryId == null && !string.IsNullOrWhiteSpace(parts.Country))
                            {
                                countryId = await GetOrCreateCountryAsync(parts.Country, continentId, ct);
                                countryCache[parts.Country.ToLowerInvariant()] = countryId.Value;
                                needsReview = true;
                            }

                            // Resolve/create region
                            int? regionId = null;
                            if (!string.IsNullOrWhiteSpace(parts.StateRegion) && countryId.HasValue)
                            {
                                regionId = await GetOrCreateRegionAsync(parts.StateRegion, countryId.Value, ct);
                            }

                            // Resolve/create city
                            int? cityId = null;
                            if (!string.IsNullOrWhiteSpace(parts.City) && countryId.HasValue)
                            {
                                cityId = await GetOrCreateCityAsync(parts.City, regionId, countryId.Value, ct);
                            }

                            if (needsReview) flaggedForReview++;

                            var insertSql = @"
INSERT INTO frl.frl_images_location
    (image_id, raw_location, continent_id, country_id, region_id, city_id,
     specific_location, confidence, needs_review)
VALUES
    (@imageId, @raw, @continentId, @countryId, @regionId, @cityId,
     @specificLocation, @confidence, @needsReview)
ON CONFLICT DO NOTHING;";

                            await using var insertCmd = new NpgsqlCommand(insertSql, _connection);
                            insertCmd.Parameters.AddWithValue("@imageId", imageId);
                            insertCmd.Parameters.AddWithValue("@raw", rawLocation.Trim());
                            insertCmd.Parameters.AddWithValue("@continentId", (object?)continentId ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@countryId", (object?)countryId ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@regionId", (object?)regionId ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@cityId", (object?)cityId ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@specificLocation",
                                (object?)NullIfEmpty(parts.SpecificLocation) ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@confidence", minConfidence);
                            insertCmd.Parameters.AddWithValue("@needsReview", needsReview);

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
                    FuzzyMatched = fuzzyMatched,
                    FlaggedForReview = flaggedForReview,
                    Failed = failed,
                    Status = "Completed"
                });
            }
            finally
            {
                await _connection.CloseAsync();
            }
        }

        // ═══════════════════════════════════════════════════════════
        //  REVIEW – items flagged for manual correction
        // ═══════════════════════════════════════════════════════════

        [HttpGet("review")]
        [ProducesResponseType(typeof(FilmingLocationPageResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<FilmingLocationPageResponse>> GetReviewItems(
            [FromQuery] int page = 1,
            [FromQuery] int pageSize = 50,
            CancellationToken ct = default)
        {
            if (page < 1) page = 1;
            if (pageSize < 1 || pageSize > 200) pageSize = 50;

            await EnsureOpenAsync(ct);
            try
            {
                var countSql = "SELECT COUNT(*) FROM frl.frl_images_location WHERE needs_review = TRUE;";
                await using var countCmd = new NpgsqlCommand(countSql, _connection);
                var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct));

                var offset = (page - 1) * pageSize;
                var dataSql = @"
SELECT l.id, l.image_id, l.raw_location,
       co.name AS continent_name, c.name AS country_name,
       r.name AS region_name, ci.name AS city_name,
       l.specific_location, l.confidence, l.needs_review,
       l.continent_id, l.country_id, l.region_id, l.city_id,
       l.coordinates, l.created_at, l.updated_at
FROM frl.frl_images_location l
LEFT JOIN frl.frl_location_continents co ON co.id = l.continent_id
LEFT JOIN frl.frl_location_countries c ON c.id = l.country_id
LEFT JOIN frl.frl_location_regions r ON r.id = l.region_id
LEFT JOIN frl.frl_location_cities ci ON ci.id = l.city_id
WHERE l.needs_review = TRUE
ORDER BY l.confidence ASC, l.id
LIMIT @limit OFFSET @offset;";

                await using var dataCmd = new NpgsqlCommand(dataSql, _connection);
                dataCmd.Parameters.AddWithValue("@limit", pageSize);
                dataCmd.Parameters.AddWithValue("@offset", offset);

                await using var reader = await dataCmd.ExecuteReaderAsync(ct);
                var results = new List<FilmingLocationDto>();
                while (await reader.ReadAsync(ct))
                    results.Add(MapToDto(reader));

                return Ok(new FilmingLocationPageResponse
                {
                    TotalCount = totalCount, Page = page,
                    PageSize = pageSize, Items = results
                });
            }
            finally { await _connection.CloseAsync(); }
        }

        [HttpPost("review/{id:long}/approve")]
        [ProducesResponseType(typeof(FilmingLocationDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<FilmingLocationDto>> ApproveReview(
            long id, CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            try
            {
                var sql = @"
UPDATE frl.frl_images_location
SET needs_review = FALSE, confidence = 1.0, updated_at = NOW()
WHERE id = @id
RETURNING id, image_id, raw_location, continent_id, country_id, region_id, city_id,
          specific_location, coordinates, confidence, needs_review, created_at, updated_at;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return NotFound(new { Message = $"Location {id} not found." });

                // Need to re-query to get joined names
                return Ok(await GetLocationByIdInternalAsync(id, ct));
            }
            finally { await _connection.CloseAsync(); }
        }

        // ═══════════════════════════════════════════════════════════
        //  CRUD – filming locations
        // ═══════════════════════════════════════════════════════════

        [HttpGet]
        [ProducesResponseType(typeof(FilmingLocationPageResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<FilmingLocationPageResponse>> GetAll(
            [FromQuery] string? country,
            [FromQuery] string? city,
            [FromQuery] bool? needsReview,
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
                    whereClauses.Add("c.name ILIKE @country");
                    parameters.Add(new NpgsqlParameter("@country", $"%{country.Trim()}%"));
                }
                if (!string.IsNullOrWhiteSpace(city))
                {
                    whereClauses.Add("ci.name ILIKE @city");
                    parameters.Add(new NpgsqlParameter("@city", $"%{city.Trim()}%"));
                }
                if (needsReview.HasValue)
                {
                    whereClauses.Add("l.needs_review = @needsReview");
                    parameters.Add(new NpgsqlParameter("@needsReview", needsReview.Value));
                }

                var whereStr = whereClauses.Count > 0
                    ? "WHERE " + string.Join(" AND ", whereClauses)
                    : "";

                var countSql = $@"
SELECT COUNT(*)
FROM frl.frl_images_location l
LEFT JOIN frl.frl_location_countries c ON c.id = l.country_id
LEFT JOIN frl.frl_location_cities ci ON ci.id = l.city_id
{whereStr};";

                await using var countCmd = new NpgsqlCommand(countSql, _connection);
                countCmd.Parameters.AddRange(parameters.ToArray());
                var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct));

                var offset = (page - 1) * pageSize;
                var dataSql = $@"
SELECT l.id, l.image_id, l.raw_location,
       co.name AS continent_name, c.name AS country_name,
       r.name AS region_name, ci.name AS city_name,
       l.specific_location, l.confidence, l.needs_review,
       l.continent_id, l.country_id, l.region_id, l.city_id,
       l.coordinates, l.created_at, l.updated_at
FROM frl.frl_images_location l
LEFT JOIN frl.frl_location_continents co ON co.id = l.continent_id
LEFT JOIN frl.frl_location_countries c ON c.id = l.country_id
LEFT JOIN frl.frl_location_regions r ON r.id = l.region_id
LEFT JOIN frl.frl_location_cities ci ON ci.id = l.city_id
{whereStr}
ORDER BY l.id
LIMIT @limit OFFSET @offset;";

                await using var dataCmd = new NpgsqlCommand(dataSql, _connection);
                foreach (var p in parameters)
                    dataCmd.Parameters.Add(new NpgsqlParameter(p.ParameterName, p.Value));
                dataCmd.Parameters.AddWithValue("@limit", pageSize);
                dataCmd.Parameters.AddWithValue("@offset", offset);

                await using var reader = await dataCmd.ExecuteReaderAsync(ct);
                var results = new List<FilmingLocationDto>();
                while (await reader.ReadAsync(ct))
                    results.Add(MapToDto(reader));

                return Ok(new FilmingLocationPageResponse
                {
                    TotalCount = totalCount, Page = page,
                    PageSize = pageSize, Items = results
                });
            }
            finally { await _connection.CloseAsync(); }
        }

        [HttpGet("{id:long}")]
        [ProducesResponseType(typeof(FilmingLocationDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<FilmingLocationDto>> GetById(long id, CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            try
            {
                var dto = await GetLocationByIdInternalAsync(id, ct);
                if (dto == null) return NotFound(new { Message = $"Location {id} not found." });
                return Ok(dto);
            }
            finally { await _connection.CloseAsync(); }
        }

        [HttpGet("by-image/{imageId:int}")]
        [ProducesResponseType(typeof(List<FilmingLocationDto>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<FilmingLocationDto>>> GetByImageId(int imageId, CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            try
            {
                var sql = @"
SELECT l.id, l.image_id, l.raw_location,
       co.name AS continent_name, c.name AS country_name,
       r.name AS region_name, ci.name AS city_name,
       l.specific_location, l.confidence, l.needs_review,
       l.continent_id, l.country_id, l.region_id, l.city_id,
       l.coordinates, l.created_at, l.updated_at
FROM frl.frl_images_location l
LEFT JOIN frl.frl_location_continents co ON co.id = l.continent_id
LEFT JOIN frl.frl_location_countries c ON c.id = l.country_id
LEFT JOIN frl.frl_location_regions r ON r.id = l.region_id
LEFT JOIN frl.frl_location_cities ci ON ci.id = l.city_id
WHERE l.image_id = @imageId
ORDER BY l.id;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@imageId", imageId);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                var results = new List<FilmingLocationDto>();
                while (await reader.ReadAsync(ct))
                    results.Add(MapToDto(reader));

                return Ok(results);
            }
            finally { await _connection.CloseAsync(); }
        }

        [HttpPost]
        [ProducesResponseType(typeof(FilmingLocationDto), StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public async Task<ActionResult<FilmingLocationDto>> Create(
            [FromBody] FilmingLocationCreateDto dto, CancellationToken ct = default)
        {
            if (dto.ImageId <= 0)
                return BadRequest(new { Message = "ImageId is required and must be > 0." });

            await EnsureOpenAsync(ct);
            try
            {
                var sql = @"
INSERT INTO frl.frl_images_location
    (image_id, continent_id, country_id, region_id, city_id,
     specific_location, coordinates, confidence, needs_review)
VALUES
    (@imageId, @continentId, @countryId, @regionId, @cityId,
     @specificLocation, @coordinates, 1.0, FALSE)
RETURNING id;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@imageId", dto.ImageId);
                cmd.Parameters.AddWithValue("@continentId", (object?)dto.ContinentId ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@countryId", (object?)dto.CountryId ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@regionId", (object?)dto.RegionId ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@cityId", (object?)dto.CityId ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@specificLocation", (object?)NullIfEmpty(dto.SpecificLocation) ?? DBNull.Value);

                if (dto.Latitude.HasValue && dto.Longitude.HasValue)
                    cmd.Parameters.AddWithValue("@coordinates", NpgsqlDbType.Point,
                        new NpgsqlPoint(dto.Longitude.Value, dto.Latitude.Value));
                else
                    cmd.Parameters.AddWithValue("@coordinates", NpgsqlDbType.Point, DBNull.Value);

                var newId = Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
                var created = await GetLocationByIdInternalAsync(newId, ct);
                return CreatedAtAction(nameof(GetById), new { id = newId }, created);
            }
            finally { await _connection.CloseAsync(); }
        }

        [HttpPut("{id:long}")]
        [ProducesResponseType(typeof(FilmingLocationDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<FilmingLocationDto>> Update(
            long id, [FromBody] FilmingLocationUpdateDto dto, CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            try
            {
                var sql = @"
UPDATE frl.frl_images_location
SET continent_id = @continentId,
    country_id = @countryId,
    region_id = @regionId,
    city_id = @cityId,
    specific_location = @specificLocation,
    coordinates = @coordinates,
    needs_review = @needsReview,
    updated_at = NOW()
WHERE id = @id
RETURNING id;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                cmd.Parameters.AddWithValue("@continentId", (object?)dto.ContinentId ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@countryId", (object?)dto.CountryId ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@regionId", (object?)dto.RegionId ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@cityId", (object?)dto.CityId ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@specificLocation", (object?)NullIfEmpty(dto.SpecificLocation) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@needsReview", dto.NeedsReview ?? false);

                if (dto.Latitude.HasValue && dto.Longitude.HasValue)
                    cmd.Parameters.AddWithValue("@coordinates", NpgsqlDbType.Point,
                        new NpgsqlPoint(dto.Longitude.Value, dto.Latitude.Value));
                else
                    cmd.Parameters.AddWithValue("@coordinates", NpgsqlDbType.Point, DBNull.Value);

                var result = await cmd.ExecuteScalarAsync(ct);
                if (result == null)
                    return NotFound(new { Message = $"Location {id} not found." });

                return Ok(await GetLocationByIdInternalAsync(id, ct));
            }
            finally { await _connection.CloseAsync(); }
        }

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
                    return NotFound(new { Message = $"Location {id} not found." });
                return NoContent();
            }
            finally { await _connection.CloseAsync(); }
        }

        [HttpPost("{id:long}/coordinates")]
        [ProducesResponseType(typeof(FilmingLocationDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<FilmingLocationDto>> SetCoordinates(
            long id, [FromBody] CoordinatesDto dto, CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            try
            {
                var sql = @"
UPDATE frl.frl_images_location
SET coordinates = @coordinates, updated_at = NOW()
WHERE id = @id
RETURNING id;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                cmd.Parameters.AddWithValue("@coordinates", NpgsqlDbType.Point,
                    new NpgsqlPoint(dto.Longitude, dto.Latitude));

                var result = await cmd.ExecuteScalarAsync(ct);
                if (result == null)
                    return NotFound(new { Message = $"Location {id} not found." });

                return Ok(await GetLocationByIdInternalAsync(id, ct));
            }
            finally { await _connection.CloseAsync(); }
        }

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
    COUNT(*) FILTER (WHERE needs_review) AS needs_review_count,
    COUNT(DISTINCT country_id) FILTER (WHERE country_id IS NOT NULL) AS distinct_countries,
    COUNT(DISTINCT city_id) FILTER (WHERE city_id IS NOT NULL) AS distinct_cities,
    COUNT(DISTINCT image_id) AS distinct_images
FROM frl.frl_images_location;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                await reader.ReadAsync(ct);

                return Ok(new FilmingLocationStats
                {
                    TotalLocations = reader.GetInt64(0),
                    GeocodedCount = reader.GetInt64(1),
                    NeedsReviewCount = reader.GetInt64(2),
                    DistinctCountries = reader.GetInt64(3),
                    DistinctCities = reader.GetInt64(4),
                    DistinctImages = reader.GetInt64(5)
                });
            }
            finally { await _connection.CloseAsync(); }
        }

        // ═══════════════════════════════════════════════════════════
        //  LOOKUP CRUD – continents, countries, regions, cities
        // ═══════════════════════════════════════════════════════════

        [HttpGet("continents")]
        public async Task<ActionResult<List<ContinentDto>>> GetContinents(CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            try
            {
                const string sql = "SELECT id, name FROM frl.frl_location_continents ORDER BY name;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                var list = new List<ContinentDto>();
                while (await reader.ReadAsync(ct))
                    list.Add(new ContinentDto { Id = reader.GetInt32(0), Name = reader.GetString(1) });
                return Ok(list);
            }
            finally { await _connection.CloseAsync(); }
        }

        [HttpGet("countries")]
        public async Task<ActionResult<List<CountryDto>>> GetCountries(
            [FromQuery] int? continentId, CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            try
            {
                var where = continentId.HasValue ? "WHERE c.continent_id = @continentId" : "";
                var sql = $@"
SELECT c.id, c.continent_id, co.name AS continent_name, c.name
FROM frl.frl_location_countries c
LEFT JOIN frl.frl_location_continents co ON co.id = c.continent_id
{where}
ORDER BY c.name;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                if (continentId.HasValue)
                    cmd.Parameters.AddWithValue("@continentId", continentId.Value);

                await using var reader = await cmd.ExecuteReaderAsync(ct);
                var list = new List<CountryDto>();
                while (await reader.ReadAsync(ct))
                    list.Add(new CountryDto
                    {
                        Id = reader.GetInt32(0),
                        ContinentId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                        ContinentName = reader.IsDBNull(2) ? null : reader.GetString(2),
                        Name = reader.GetString(3)
                    });
                return Ok(list);
            }
            finally { await _connection.CloseAsync(); }
        }

        [HttpGet("regions")]
        public async Task<ActionResult<List<RegionDto>>> GetRegions(
            [FromQuery] int? countryId, CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            try
            {
                var where = countryId.HasValue ? "WHERE r.country_id = @countryId" : "";
                var sql = $@"
SELECT r.id, r.country_id, c.name AS country_name, r.name
FROM frl.frl_location_regions r
LEFT JOIN frl.frl_location_countries c ON c.id = r.country_id
{where}
ORDER BY r.name;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                if (countryId.HasValue)
                    cmd.Parameters.AddWithValue("@countryId", countryId.Value);

                await using var reader = await cmd.ExecuteReaderAsync(ct);
                var list = new List<RegionDto>();
                while (await reader.ReadAsync(ct))
                    list.Add(new RegionDto
                    {
                        Id = reader.GetInt32(0),
                        CountryId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                        CountryName = reader.IsDBNull(2) ? null : reader.GetString(2),
                        Name = reader.GetString(3)
                    });
                return Ok(list);
            }
            finally { await _connection.CloseAsync(); }
        }

        [HttpGet("cities")]
        public async Task<ActionResult<List<CityDto>>> GetCities(
            [FromQuery] int? countryId,
            [FromQuery] int? regionId,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            try
            {
                var whereClauses = new List<string>();
                var parameters = new List<NpgsqlParameter>();

                if (countryId.HasValue)
                {
                    whereClauses.Add("ci.country_id = @countryId");
                    parameters.Add(new NpgsqlParameter("@countryId", countryId.Value));
                }
                if (regionId.HasValue)
                {
                    whereClauses.Add("ci.region_id = @regionId");
                    parameters.Add(new NpgsqlParameter("@regionId", regionId.Value));
                }

                var where = whereClauses.Count > 0 ? "WHERE " + string.Join(" AND ", whereClauses) : "";
                var sql = $@"
SELECT ci.id, ci.region_id, r.name AS region_name, ci.country_id, c.name AS country_name, ci.name
FROM frl.frl_location_cities ci
LEFT JOIN frl.frl_location_regions r ON r.id = ci.region_id
LEFT JOIN frl.frl_location_countries c ON c.id = ci.country_id
{where}
ORDER BY ci.name;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddRange(parameters.ToArray());

                await using var reader = await cmd.ExecuteReaderAsync(ct);
                var list = new List<CityDto>();
                while (await reader.ReadAsync(ct))
                    list.Add(new CityDto
                    {
                        Id = reader.GetInt32(0),
                        RegionId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                        RegionName = reader.IsDBNull(2) ? null : reader.GetString(2),
                        CountryId = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                        CountryName = reader.IsDBNull(4) ? null : reader.GetString(4),
                        Name = reader.GetString(5)
                    });
                return Ok(list);
            }
            finally { await _connection.CloseAsync(); }
        }

        // ═══════════════════════════════════════════════════════════
        //  ALIAS management
        // ═══════════════════════════════════════════════════════════

        [HttpGet("aliases/{level}")]
        [ProducesResponseType(typeof(List<AliasDto>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<AliasDto>>> GetAliases(
            string level, CancellationToken ct = default)
        {
            var (tableName, fkColumn, canonicalTable) = ResolveAliasTable(level);
            if (tableName == null)
                return BadRequest(new { Message = "Level must be: continent, country, region, or city" });

            await EnsureOpenAsync(ct);
            try
            {
                var sql = $@"
SELECT a.id, a.alias, a.{fkColumn}, c.name
FROM frl.{tableName} a
JOIN frl.{canonicalTable} c ON c.id = a.{fkColumn}
ORDER BY a.alias;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                var list = new List<AliasDto>();
                while (await reader.ReadAsync(ct))
                    list.Add(new AliasDto
                    {
                        Id = reader.GetInt32(0),
                        Alias = reader.GetString(1),
                        CanonicalId = reader.GetInt32(2),
                        CanonicalName = reader.GetString(3)
                    });
                return Ok(list);
            }
            finally { await _connection.CloseAsync(); }
        }

        [HttpPost("aliases/{level}")]
        [ProducesResponseType(typeof(AliasDto), StatusCodes.Status201Created)]
        public async Task<ActionResult<AliasDto>> CreateAlias(
            string level, [FromBody] CreateAliasDto dto, CancellationToken ct = default)
        {
            var (tableName, fkColumn, canonicalTable) = ResolveAliasTable(level);
            if (tableName == null)
                return BadRequest(new { Message = "Level must be: continent, country, region, or city" });

            if (string.IsNullOrWhiteSpace(dto.Alias))
                return BadRequest(new { Message = "Alias text is required." });

            await EnsureOpenAsync(ct);
            try
            {
                var sql = $@"
INSERT INTO frl.{tableName} (alias, {fkColumn})
VALUES (@alias, @canonicalId)
RETURNING id;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@alias", dto.Alias.Trim());
                cmd.Parameters.AddWithValue("@canonicalId", dto.CanonicalId);

                var newId = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

                // Fetch the canonical name
                var nameSql = $"SELECT name FROM frl.{canonicalTable} WHERE id = @id;";
                await using var nameCmd = new NpgsqlCommand(nameSql, _connection);
                nameCmd.Parameters.AddWithValue("@id", dto.CanonicalId);
                var canonicalName = (string?)await nameCmd.ExecuteScalarAsync(ct) ?? "";

                return Created("", new AliasDto
                {
                    Id = newId,
                    Alias = dto.Alias.Trim(),
                    CanonicalId = dto.CanonicalId,
                    CanonicalName = canonicalName
                });
            }
            finally { await _connection.CloseAsync(); }
        }

        [HttpDelete("aliases/{level}/{id:int}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        public async Task<IActionResult> DeleteAlias(
            string level, int id, CancellationToken ct = default)
        {
            var (tableName, _, _) = ResolveAliasTable(level);
            if (tableName == null)
                return BadRequest(new { Message = "Level must be: continent, country, region, or city" });

            await EnsureOpenAsync(ct);
            try
            {
                var sql = $"DELETE FROM frl.{tableName} WHERE id = @id;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                var affected = await cmd.ExecuteNonQueryAsync(ct);
                if (affected == 0)
                    return NotFound(new { Message = $"Alias {id} not found." });
                return NoContent();
            }
            finally { await _connection.CloseAsync(); }
        }

        // ═══════════════════════════════════════════════════════════
        //  HELPERS
        // ═══════════════════════════════════════════════════════════

        private sealed class ParsedParts
        {
            public string? Planet { get; set; }
            public string? Continent { get; set; }
            public string? Country { get; set; }
            public string? StateRegion { get; set; }
            public string? City { get; set; }
            public string? SpecificLocation { get; set; }
        }

        private static ParsedParts? ParseParts(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) return null;
            var parts = raw.Trim().Split(':');
            if (parts.All(p => string.IsNullOrWhiteSpace(p))) return null;
            return new ParsedParts
            {
                Planet = parts.Length > 0 ? parts[0].Trim() : null,
                Continent = parts.Length > 1 ? parts[1].Trim() : null,
                Country = parts.Length > 2 ? parts[2].Trim() : null,
                StateRegion = parts.Length > 3 ? parts[3].Trim() : null,
                City = parts.Length > 4 ? parts[4].Trim() : null,
                SpecificLocation = parts.Length > 5 ? parts[5].Trim() : null
            };
        }

        /// <summary>
        /// Try exact match → alias match → fuzzy match against canonical names.
        /// Returns (resolvedId, confidence). confidence = 1.0 for exact/alias,
        /// ratio/100 for fuzzy, 0 if no match.
        /// </summary>
        private static (int? id, float confidence) ResolveWithFuzzy(
            string input,
            Dictionary<string, int> canonicalCache,
            Dictionary<string, int> aliasCache)
        {
            if (string.IsNullOrWhiteSpace(input))
                return (null, 1.0f);

            var key = input.Trim().ToLowerInvariant();

            // Exact canonical match
            if (canonicalCache.TryGetValue(key, out var exactId))
                return (exactId, 1.0f);

            // Exact alias match
            if (aliasCache.TryGetValue(key, out var aliasId))
                return (aliasId, 1.0f);

            // Fuzzy match against canonical names
            var bestScore = 0;
            var bestId = (int?)null;

            foreach (var (name, id) in canonicalCache)
            {
                var score = Fuzz.Ratio(key, name);
                if (score > bestScore)
                {
                    bestScore = score;
                    bestId = id;
                }
            }

            // Also check aliases
            foreach (var (alias, id) in aliasCache)
            {
                var score = Fuzz.Ratio(key, alias);
                if (score > bestScore)
                {
                    bestScore = score;
                    bestId = id;
                }
            }

            if (bestScore >= FuzzyAutoMatchThreshold)
                return (bestId, bestScore / 100f);

            if (bestScore >= FuzzyReviewThreshold)
                return (bestId, bestScore / 100f);

            return (null, 0f);
        }

        private async Task<Dictionary<string, int>> LoadLookupCacheAsync(
            string tableName, CancellationToken ct)
        {
            var sql = $"SELECT id, name FROM frl.{tableName};";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var cache = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            while (await reader.ReadAsync(ct))
            {
                var name = reader.GetString(1).Trim().ToLowerInvariant();
                cache.TryAdd(name, reader.GetInt32(0));
            }
            return cache;
        }

        private async Task<Dictionary<string, int>> LoadAliasCacheAsync(
            string tableName, string fkColumn, CancellationToken ct)
        {
            var sql = $"SELECT alias, {fkColumn} FROM frl.{tableName};";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var cache = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            while (await reader.ReadAsync(ct))
            {
                var alias = reader.GetString(0).Trim().ToLowerInvariant();
                cache.TryAdd(alias, reader.GetInt32(1));
            }
            return cache;
        }

        private async Task<int> GetOrCreateCountryAsync(
            string name, int? continentId, CancellationToken ct)
        {
            var sql = @"
INSERT INTO frl.frl_location_countries (name, continent_id)
VALUES (@name, @continentId)
ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
RETURNING id;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@name", name.Trim());
            cmd.Parameters.AddWithValue("@continentId", (object?)continentId ?? DBNull.Value);
            return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
        }

        private async Task<int> GetOrCreateRegionAsync(
            string name, int countryId, CancellationToken ct)
        {
            var sql = @"
INSERT INTO frl.frl_location_regions (name, country_id)
VALUES (@name, @countryId)
ON CONFLICT (country_id, name) DO UPDATE SET name = EXCLUDED.name
RETURNING id;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@name", name.Trim());
            cmd.Parameters.AddWithValue("@countryId", countryId);
            return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
        }

        private async Task<int> GetOrCreateCityAsync(
            string name, int? regionId, int countryId, CancellationToken ct)
        {
            var sql = @"
INSERT INTO frl.frl_location_cities (name, region_id, country_id)
VALUES (@name, @regionId, @countryId)
ON CONFLICT (country_id, name) DO UPDATE SET name = EXCLUDED.name
RETURNING id;";
            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@name", name.Trim());
            cmd.Parameters.AddWithValue("@regionId", (object?)regionId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@countryId", countryId);
            return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
        }

        private async Task<FilmingLocationDto?> GetLocationByIdInternalAsync(
            long id, CancellationToken ct)
        {
            var sql = @"
SELECT l.id, l.image_id, l.raw_location,
       co.name AS continent_name, c.name AS country_name,
       r.name AS region_name, ci.name AS city_name,
       l.specific_location, l.confidence, l.needs_review,
       l.continent_id, l.country_id, l.region_id, l.city_id,
       l.coordinates, l.created_at, l.updated_at
FROM frl.frl_images_location l
LEFT JOIN frl.frl_location_continents co ON co.id = l.continent_id
LEFT JOIN frl.frl_location_countries c ON c.id = l.country_id
LEFT JOIN frl.frl_location_regions r ON r.id = l.region_id
LEFT JOIN frl.frl_location_cities ci ON ci.id = l.city_id
WHERE l.id = @id;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@id", id);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct)) return null;
            return MapToDto(reader);
        }

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
                ContinentId = reader.IsDBNull(reader.GetOrdinal("continent_id")) ? null : reader.GetInt32(reader.GetOrdinal("continent_id")),
                ContinentName = reader.IsDBNull(reader.GetOrdinal("continent_name")) ? null : reader.GetString(reader.GetOrdinal("continent_name")),
                CountryId = reader.IsDBNull(reader.GetOrdinal("country_id")) ? null : reader.GetInt32(reader.GetOrdinal("country_id")),
                CountryName = reader.IsDBNull(reader.GetOrdinal("country_name")) ? null : reader.GetString(reader.GetOrdinal("country_name")),
                RegionId = reader.IsDBNull(reader.GetOrdinal("region_id")) ? null : reader.GetInt32(reader.GetOrdinal("region_id")),
                RegionName = reader.IsDBNull(reader.GetOrdinal("region_name")) ? null : reader.GetString(reader.GetOrdinal("region_name")),
                CityId = reader.IsDBNull(reader.GetOrdinal("city_id")) ? null : reader.GetInt32(reader.GetOrdinal("city_id")),
                CityName = reader.IsDBNull(reader.GetOrdinal("city_name")) ? null : reader.GetString(reader.GetOrdinal("city_name")),
                SpecificLocation = reader.IsDBNull(reader.GetOrdinal("specific_location")) ? null : reader.GetString(reader.GetOrdinal("specific_location")),
                Latitude = lat,
                Longitude = lng,
                Confidence = reader.GetFloat(reader.GetOrdinal("confidence")),
                NeedsReview = reader.GetBoolean(reader.GetOrdinal("needs_review")),
                CreatedAt = reader.GetDateTime(reader.GetOrdinal("created_at")),
                UpdatedAt = reader.GetDateTime(reader.GetOrdinal("updated_at"))
            };
        }

        private static (string? tableName, string? fkColumn, string? canonicalTable)
            ResolveAliasTable(string level) => level.ToLowerInvariant() switch
        {
            "continent" => ("frl_location_continent_aliases", "continent_id", "frl_location_continents"),
            "country" => ("frl_location_country_aliases", "country_id", "frl_location_countries"),
            "region" => ("frl_location_region_aliases", "region_id", "frl_location_regions"),
            "city" => ("frl_location_city_aliases", "city_id", "frl_location_cities"),
            _ => (null, null, null)
        };

        private static string? NullIfEmpty(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

        private async Task EnsureOpenAsync(CancellationToken ct)
        {
            if (_connection.State != ConnectionState.Open)
                await _connection.OpenAsync(ct);
        }
    }
}
