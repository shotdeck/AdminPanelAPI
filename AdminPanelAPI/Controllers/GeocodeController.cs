using Microsoft.AspNetCore.Mvc;
using Npgsql;
using NpgsqlTypes;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text.Json;

namespace AdminPanelAPI.Controllers
{
    [ApiController]
    [Route("api/admin/geocode")]
    public sealed class GeocodeController : ControllerBase
    {
        private readonly NpgsqlConnection _connection;
        private readonly ILogger<GeocodeController> _logger;
        private static readonly HttpClient _http = CreateHttpClient();
        private const int MaxRequestSeconds = 180; // Stop before Azure's 230s timeout

        public GeocodeController(
            NpgsqlConnection connection,
            ILogger<GeocodeController> logger)
        {
            _connection = connection;
            _logger = logger;
        }

        private static HttpClient CreateHttpClient()
        {
            var client = new HttpClient();
            client.DefaultRequestHeaders.UserAgent.Add(
                new ProductInfoHeaderValue("ShotDeckAdmin", "1.0"));
            client.DefaultRequestHeaders.UserAgent.Add(
                new ProductInfoHeaderValue("(admin@shotdeck.com)"));
            return client;
        }

        private async Task EnsureOpenAsync(CancellationToken ct)
        {
            if (_connection.State != System.Data.ConnectionState.Open)
                await _connection.OpenAsync(ct);
        }

        // ─── GET /api/admin/geocode/stats ──────────────────────────────
        [HttpGet("stats")]
        [ProducesResponseType(typeof(GeocodeStatsResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<GeocodeStatsResponse>> GetStats(CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            try
            {
                var stats = new GeocodeStatsResponse();

                await using (var cmd = new NpgsqlCommand(
                    "SELECT COUNT(*) FROM frl.frl_images_location", _connection))
                    stats.TotalRecords = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

                await using (var cmd = new NpgsqlCommand(
                    "SELECT COUNT(*) FROM frl.frl_images_location WHERE coordinates IS NOT NULL", _connection))
                    stats.AlreadyGeocoded = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

                await using (var cmd = new NpgsqlCommand(
                    "SELECT COUNT(*) FROM frl.frl_location_cities", _connection))
                    stats.TotalCities = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

                await using (var cmd = new NpgsqlCommand(
                    "SELECT COUNT(*) FROM frl.frl_location_cities WHERE coordinates IS NOT NULL", _connection))
                    stats.GeocodedCities = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

                await using (var cmd = new NpgsqlCommand(
                    "SELECT COUNT(*) FROM frl.frl_location_regions", _connection))
                    stats.TotalRegions = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

                await using (var cmd = new NpgsqlCommand(
                    "SELECT COUNT(*) FROM frl.frl_location_regions WHERE coordinates IS NOT NULL", _connection))
                    stats.GeocodedRegions = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

                await using (var cmd = new NpgsqlCommand(
                    "SELECT COUNT(*) FROM frl.frl_location_countries", _connection))
                    stats.TotalCountries = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

                await using (var cmd = new NpgsqlCommand(
                    "SELECT COUNT(*) FROM frl.frl_location_countries WHERE coordinates IS NOT NULL", _connection))
                    stats.GeocodedCountries = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

                await using (var cmd = new NpgsqlCommand(@"
                    SELECT COUNT(DISTINCT specific_location)
                    FROM frl.frl_images_location
                    WHERE specific_location IS NOT NULL AND TRIM(specific_location) <> ''", _connection))
                    stats.TotalSpecificLocations = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

                await using (var cmd = new NpgsqlCommand(@"
                    SELECT COUNT(DISTINCT il.specific_location)
                    FROM frl.frl_images_location il
                    INNER JOIN frl.frl_geocode_cache gc ON gc.location_key = il.specific_location
                    WHERE il.specific_location IS NOT NULL
                      AND TRIM(il.specific_location) <> ''
                      AND gc.coordinates IS NOT NULL", _connection))
                    stats.GeocodedSpecificLocations = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

                return Ok(stats);
            }
            finally { await _connection.CloseAsync(); }
        }

        // ─── POST /api/admin/geocode/countries ─────────────────────────
        [HttpPost("countries")]
        [ProducesResponseType(typeof(GeocodeResultResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<GeocodeResultResponse>> GeocodeCountries(
            [FromQuery] int batchSize = 50,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            try
            {
                var sql = @"
                    SELECT id, name FROM frl.frl_location_countries
                    WHERE coordinates IS NULL
                    ORDER BY id
                    LIMIT @limit";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@limit", batchSize);

                var items = new List<(int id, string name)>();
                await using (var reader = await cmd.ExecuteReaderAsync(ct))
                {
                    while (await reader.ReadAsync(ct))
                        items.Add((reader.GetInt32(0), reader.GetString(1)));
                }

                int geocoded = 0, failed = 0;
                var sw = Stopwatch.StartNew();
                foreach (var (id, name) in items)
                {
                    if (ct.IsCancellationRequested || sw.Elapsed.TotalSeconds > MaxRequestSeconds) break;

                    var coords = await GeocodeWithNominatim(name, null, null, ct);
                    if (coords.HasValue)
                    {
                        await using var upd = new NpgsqlCommand(
                            "UPDATE frl.frl_location_countries SET coordinates = POINT(@lng, @lat) WHERE id = @id",
                            _connection);
                        upd.Parameters.AddWithValue("@lng", coords.Value.lng);
                        upd.Parameters.AddWithValue("@lat", coords.Value.lat);
                        upd.Parameters.AddWithValue("@id", id);
                        await upd.ExecuteNonQueryAsync(ct);
                        geocoded++;
                    }
                    else
                    {
                        failed++;
                        _logger.LogWarning("Failed to geocode country: {Name}", name);
                    }

                    await Task.Delay(1100, ct);
                }

                await using var remCmd = new NpgsqlCommand(
                    "SELECT COUNT(*) FROM frl.frl_location_countries WHERE coordinates IS NULL", _connection);
                var remaining = Convert.ToInt32(await remCmd.ExecuteScalarAsync(ct));

                return Ok(new GeocodeResultResponse
                {
                    Processed = geocoded + failed,
                    Geocoded = geocoded,
                    Failed = failed,
                    Remaining = remaining,
                    ElapsedSeconds = (int)sw.Elapsed.TotalSeconds
                });
            }
            finally { await _connection.CloseAsync(); }
        }

        // ─── POST /api/admin/geocode/regions ───────────────────────────
        [HttpPost("regions")]
        [ProducesResponseType(typeof(GeocodeResultResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<GeocodeResultResponse>> GeocodeRegions(
            [FromQuery] int batchSize = 50,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            try
            {
                var sql = @"
                    SELECT r.id, r.name, c.name AS country_name
                    FROM frl.frl_location_regions r
                    LEFT JOIN frl.frl_location_countries c ON c.id = r.country_id
                    WHERE r.coordinates IS NULL
                    ORDER BY r.id
                    LIMIT @limit";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@limit", batchSize);

                var items = new List<(int id, string name, string? country)>();
                await using (var reader = await cmd.ExecuteReaderAsync(ct))
                {
                    while (await reader.ReadAsync(ct))
                        items.Add((
                            reader.GetInt32(0),
                            reader.GetString(1),
                            reader.IsDBNull(2) ? null : reader.GetString(2)
                        ));
                }

                int geocoded = 0, failed = 0;
                var sw = Stopwatch.StartNew();
                foreach (var (id, name, country) in items)
                {
                    if (ct.IsCancellationRequested || sw.Elapsed.TotalSeconds > MaxRequestSeconds) break;

                    var coords = await GeocodeWithNominatim(name, null, country, ct);
                    if (coords.HasValue)
                    {
                        await using var upd = new NpgsqlCommand(
                            "UPDATE frl.frl_location_regions SET coordinates = POINT(@lng, @lat) WHERE id = @id",
                            _connection);
                        upd.Parameters.AddWithValue("@lng", coords.Value.lng);
                        upd.Parameters.AddWithValue("@lat", coords.Value.lat);
                        upd.Parameters.AddWithValue("@id", id);
                        await upd.ExecuteNonQueryAsync(ct);
                        geocoded++;
                    }
                    else
                    {
                        failed++;
                        _logger.LogWarning("Failed to geocode region: {Name}, {Country}", name, country);
                    }

                    await Task.Delay(1100, ct);
                }

                await using var remCmd = new NpgsqlCommand(
                    "SELECT COUNT(*) FROM frl.frl_location_regions WHERE coordinates IS NULL", _connection);
                var remaining = Convert.ToInt32(await remCmd.ExecuteScalarAsync(ct));

                return Ok(new GeocodeResultResponse
                {
                    Processed = geocoded + failed,
                    Geocoded = geocoded,
                    Failed = failed,
                    Remaining = remaining,
                    ElapsedSeconds = (int)sw.Elapsed.TotalSeconds
                });
            }
            finally { await _connection.CloseAsync(); }
        }

        // ─── POST /api/admin/geocode/cities ────────────────────────────
        [HttpPost("cities")]
        [ProducesResponseType(typeof(GeocodeResultResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<GeocodeResultResponse>> GeocodeCities(
            [FromQuery] int batchSize = 50,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            try
            {
                var sql = @"
                    SELECT ci.id, ci.name, r.name AS region_name, c.name AS country_name
                    FROM frl.frl_location_cities ci
                    LEFT JOIN frl.frl_location_regions r ON r.id = ci.region_id
                    LEFT JOIN frl.frl_location_countries c ON c.id = ci.country_id
                    WHERE ci.coordinates IS NULL
                    ORDER BY ci.id
                    LIMIT @limit";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@limit", batchSize);

                var items = new List<(int id, string name, string? region, string? country)>();
                await using (var reader = await cmd.ExecuteReaderAsync(ct))
                {
                    while (await reader.ReadAsync(ct))
                        items.Add((
                            reader.GetInt32(0),
                            reader.GetString(1),
                            reader.IsDBNull(2) ? null : reader.GetString(2),
                            reader.IsDBNull(3) ? null : reader.GetString(3)
                        ));
                }

                int geocoded = 0, failed = 0;
                var sw = Stopwatch.StartNew();
                foreach (var (id, name, region, country) in items)
                {
                    if (ct.IsCancellationRequested || sw.Elapsed.TotalSeconds > MaxRequestSeconds) break;

                    var coords = await GeocodeWithNominatim(name, region, country, ct);
                    if (coords.HasValue)
                    {
                        await using var upd = new NpgsqlCommand(
                            "UPDATE frl.frl_location_cities SET coordinates = POINT(@lng, @lat) WHERE id = @id",
                            _connection);
                        upd.Parameters.AddWithValue("@lng", coords.Value.lng);
                        upd.Parameters.AddWithValue("@lat", coords.Value.lat);
                        upd.Parameters.AddWithValue("@id", id);
                        await upd.ExecuteNonQueryAsync(ct);
                        geocoded++;
                    }
                    else
                    {
                        failed++;
                        _logger.LogWarning("Failed to geocode city: {Name}, {Region}, {Country}", name, region, country);
                    }

                    await Task.Delay(1100, ct);
                }

                await using var remCmd = new NpgsqlCommand(
                    "SELECT COUNT(*) FROM frl.frl_location_cities WHERE coordinates IS NULL", _connection);
                var remaining = Convert.ToInt32(await remCmd.ExecuteScalarAsync(ct));

                return Ok(new GeocodeResultResponse
                {
                    Processed = geocoded + failed,
                    Geocoded = geocoded,
                    Failed = failed,
                    Remaining = remaining,
                    ElapsedSeconds = (int)sw.Elapsed.TotalSeconds
                });
            }
            finally { await _connection.CloseAsync(); }
        }

        // ─── POST /api/admin/geocode/specific-locations ────────────────
        [HttpPost("specific-locations")]
        [ProducesResponseType(typeof(GeocodeResultResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<GeocodeResultResponse>> GeocodeSpecificLocations(
            [FromQuery] int batchSize = 50,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            try
            {
                var sql = @"
                    SELECT DISTINCT il.specific_location, ci.name AS city_name,
                           r.name AS region_name, c.name AS country_name
                    FROM frl.frl_images_location il
                    LEFT JOIN frl.frl_location_cities ci ON ci.id = il.city_id
                    LEFT JOIN frl.frl_location_regions r ON r.id = il.region_id
                    LEFT JOIN frl.frl_location_countries c ON c.id = il.country_id
                    WHERE il.specific_location IS NOT NULL
                      AND TRIM(il.specific_location) <> ''
                      AND NOT EXISTS (
                          SELECT 1 FROM frl.frl_geocode_cache gc
                          WHERE gc.location_key = il.specific_location
                      )
                    ORDER BY il.specific_location
                    LIMIT @limit";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@limit", batchSize);

                var items = new List<(string location, string? city, string? region, string? country)>();
                await using (var reader = await cmd.ExecuteReaderAsync(ct))
                {
                    while (await reader.ReadAsync(ct))
                        items.Add((
                            reader.GetString(0),
                            reader.IsDBNull(1) ? null : reader.GetString(1),
                            reader.IsDBNull(2) ? null : reader.GetString(2),
                            reader.IsDBNull(3) ? null : reader.GetString(3)
                        ));
                }

                int geocoded = 0, failed = 0;
                var sw = Stopwatch.StartNew();
                foreach (var (location, city, region, country) in items)
                {
                    if (ct.IsCancellationRequested || sw.Elapsed.TotalSeconds > MaxRequestSeconds) break;

                    var queryParts = new List<string> { location };
                    if (!string.IsNullOrEmpty(city)) queryParts.Add(city);
                    if (!string.IsNullOrEmpty(country)) queryParts.Add(country);
                    var query = string.Join(", ", queryParts);

                    var coords = await GeocodeWithNominatimRaw(query, ct);

                    // Insert into cache (even if null, so we don't retry)
                    if (coords.HasValue)
                    {
                        await using var ins = new NpgsqlCommand(@"
                            INSERT INTO frl.frl_geocode_cache (location_key, coordinates, source)
                            VALUES (@key, POINT(@lng, @lat), 'nominatim')
                            ON CONFLICT (location_key) DO NOTHING",
                            _connection);
                        ins.Parameters.AddWithValue("@key", location);
                        ins.Parameters.AddWithValue("@lng", coords.Value.lng);
                        ins.Parameters.AddWithValue("@lat", coords.Value.lat);
                        await ins.ExecuteNonQueryAsync(ct);
                        geocoded++;
                    }
                    else
                    {
                        await using var ins = new NpgsqlCommand(@"
                            INSERT INTO frl.frl_geocode_cache (location_key, coordinates, source)
                            VALUES (@key, NULL, 'nominatim')
                            ON CONFLICT (location_key) DO NOTHING",
                            _connection);
                        ins.Parameters.AddWithValue("@key", location);
                        await ins.ExecuteNonQueryAsync(ct);
                        failed++;
                        _logger.LogWarning("Failed to geocode specific location: {Location}", location);
                    }

                    await Task.Delay(1100, ct);
                }

                await using var remCmd = new NpgsqlCommand(@"
                    SELECT COUNT(DISTINCT il.specific_location)
                    FROM frl.frl_images_location il
                    WHERE il.specific_location IS NOT NULL
                      AND TRIM(il.specific_location) <> ''
                      AND NOT EXISTS (
                          SELECT 1 FROM frl.frl_geocode_cache gc
                          WHERE gc.location_key = il.specific_location
                      )", _connection);
                var remaining = Convert.ToInt32(await remCmd.ExecuteScalarAsync(ct));

                return Ok(new GeocodeResultResponse
                {
                    Processed = geocoded + failed,
                    Geocoded = geocoded,
                    Failed = failed,
                    Remaining = remaining,
                    ElapsedSeconds = (int)sw.Elapsed.TotalSeconds
                });
            }
            finally { await _connection.CloseAsync(); }
        }

        // ─── POST /api/admin/geocode/propagate ─────────────────────────
        // Updates frl_images_location.coordinates from resolved tiers:
        //   1. specific_location (from geocode_cache) → most precise
        //   2. city (from frl_location_cities.coordinates)
        //   3. region (from frl_location_regions.coordinates)
        //   4. country (from frl_location_countries.coordinates)
        [HttpPost("propagate")]
        [ProducesResponseType(typeof(PropagateResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<PropagateResponse>> Propagate(CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);
            try
            {
                var result = new PropagateResponse();

                // Tier 1: specific_location from geocode_cache
                await using (var cmd = new NpgsqlCommand(@"
                    UPDATE frl.frl_images_location il
                    SET coordinates = gc.coordinates,
                        updated_at = NOW()
                    FROM frl.frl_geocode_cache gc
                    WHERE gc.location_key = il.specific_location
                      AND gc.coordinates IS NOT NULL
                      AND il.coordinates IS NULL", _connection))
                {
                    result.SpecificLocationUpdated = await cmd.ExecuteNonQueryAsync(ct);
                }

                // Tier 2: city coordinates
                await using (var cmd = new NpgsqlCommand(@"
                    UPDATE frl.frl_images_location il
                    SET coordinates = ci.coordinates,
                        updated_at = NOW()
                    FROM frl.frl_location_cities ci
                    WHERE ci.id = il.city_id
                      AND ci.coordinates IS NOT NULL
                      AND il.coordinates IS NULL", _connection))
                {
                    result.CityUpdated = await cmd.ExecuteNonQueryAsync(ct);
                }

                // Tier 3: region coordinates
                await using (var cmd = new NpgsqlCommand(@"
                    UPDATE frl.frl_images_location il
                    SET coordinates = r.coordinates,
                        updated_at = NOW()
                    FROM frl.frl_location_regions r
                    WHERE r.id = il.region_id
                      AND r.coordinates IS NOT NULL
                      AND il.coordinates IS NULL", _connection))
                {
                    result.RegionUpdated = await cmd.ExecuteNonQueryAsync(ct);
                }

                // Tier 4: country coordinates
                await using (var cmd = new NpgsqlCommand(@"
                    UPDATE frl.frl_images_location il
                    SET coordinates = c.coordinates,
                        updated_at = NOW()
                    FROM frl.frl_location_countries c
                    WHERE c.id = il.country_id
                      AND c.coordinates IS NOT NULL
                      AND il.coordinates IS NULL", _connection))
                {
                    result.CountryUpdated = await cmd.ExecuteNonQueryAsync(ct);
                }

                // Final count of still-null coordinates
                await using (var cmd = new NpgsqlCommand(
                    "SELECT COUNT(*) FROM frl.frl_images_location WHERE coordinates IS NULL", _connection))
                    result.StillNull = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

                return Ok(result);
            }
            finally { await _connection.CloseAsync(); }
        }

        // ─── Nominatim Geocoding ───────────────────────────────────────

        private async Task<(double lat, double lng)?> GeocodeWithNominatim(
            string name, string? region, string? country, CancellationToken ct)
        {
            var queryParts = new List<string> { name };
            if (!string.IsNullOrEmpty(region)) queryParts.Add(region);
            if (!string.IsNullOrEmpty(country)) queryParts.Add(country);
            var query = string.Join(", ", queryParts);

            return await GeocodeWithNominatimRaw(query, ct);
        }

        private async Task<(double lat, double lng)?> GeocodeWithNominatimRaw(
            string query, CancellationToken ct)
        {
            try
            {
                var url = $"https://nominatim.openstreetmap.org/search?q={Uri.EscapeDataString(query)}&format=json&limit=1";
                var response = await _http.GetAsync(url, ct);

                if (!response.IsSuccessStatusCode) return null;

                var json = await response.Content.ReadAsStringAsync(ct);
                using var doc = JsonDocument.Parse(json);
                var arr = doc.RootElement;

                if (arr.GetArrayLength() == 0) return null;

                var first = arr[0];
                var lat = double.Parse(first.GetProperty("lat").GetString()!);
                var lng = double.Parse(first.GetProperty("lon").GetString()!);
                return (lat, lng);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Nominatim geocode failed for: {Query}", query);
                return null;
            }
        }

        // ─── DTOs ──────────────────────────────────────────────────────

        public sealed class GeocodeStatsResponse
        {
            public int TotalRecords { get; set; }
            public int AlreadyGeocoded { get; set; }
            public int TotalCities { get; set; }
            public int GeocodedCities { get; set; }
            public int TotalRegions { get; set; }
            public int GeocodedRegions { get; set; }
            public int TotalCountries { get; set; }
            public int GeocodedCountries { get; set; }
            public int TotalSpecificLocations { get; set; }
            public int GeocodedSpecificLocations { get; set; }
        }

        public sealed class GeocodeResultResponse
        {
            public int Processed { get; set; }
            public int Geocoded { get; set; }
            public int Failed { get; set; }
            public int Remaining { get; set; }
            public int ElapsedSeconds { get; set; }
        }

        public sealed class PropagateResponse
        {
            public int SpecificLocationUpdated { get; set; }
            public int CityUpdated { get; set; }
            public int RegionUpdated { get; set; }
            public int CountryUpdated { get; set; }
            public int StillNull { get; set; }
        }
    }
}
