using Npgsql;
using System.Net.Http.Headers;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    public sealed class GeocodeBackgroundService : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<GeocodeBackgroundService> _logger;
        private static readonly HttpClient _http = CreateHttpClient();

        // Shared state for status polling
        private static readonly object _lock = new();
        private static bool _isRunning;
        private static GeocodeProgress _progress = new();

        public GeocodeBackgroundService(
            IServiceScopeFactory scopeFactory,
            ILogger<GeocodeBackgroundService> logger)
        {
            _scopeFactory = scopeFactory;
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

        public static bool IsRunning
        {
            get { lock (_lock) return _isRunning; }
        }

        public static GeocodeProgress GetProgress()
        {
            lock (_lock) return _progress.Clone();
        }

        public static bool TryStart()
        {
            lock (_lock)
            {
                if (_isRunning) return false;
                _isRunning = true;
                _progress = new GeocodeProgress { Status = "starting" };
                _startRequested = true;
                return true;
            }
        }

        private static volatile bool _startRequested;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                if (!_startRequested)
                {
                    await Task.Delay(1000, stoppingToken);
                    continue;
                }

                _startRequested = false;

                try
                {
                    await RunFullGeocodeAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Background geocode failed");
                    lock (_lock)
                    {
                        _progress.Status = "error";
                        _progress.Error = ex.Message;
                        _isRunning = false;
                    }
                }
            }
        }

        private async Task RunFullGeocodeAsync(CancellationToken ct)
        {
            _logger.LogInformation("Starting full geocode run");

            // Phase 1: Countries
            await GeocodeTableAsync("countries",
                @"SELECT id, name, NULL AS region_name, NULL AS country_name
                  FROM frl.frl_location_countries WHERE coordinates IS NULL ORDER BY id",
                "UPDATE frl.frl_location_countries SET coordinates = POINT(@lng, @lat) WHERE id = @id",
                "SELECT COUNT(*) FROM frl.frl_location_countries WHERE coordinates IS NULL",
                ct);

            // Phase 2: Regions
            await GeocodeTableAsync("regions",
                @"SELECT r.id, r.name, NULL AS region_name, c.name AS country_name
                  FROM frl.frl_location_regions r
                  LEFT JOIN frl.frl_location_countries c ON c.id = r.country_id
                  WHERE r.coordinates IS NULL ORDER BY r.id",
                "UPDATE frl.frl_location_regions SET coordinates = POINT(@lng, @lat) WHERE id = @id",
                "SELECT COUNT(*) FROM frl.frl_location_regions WHERE coordinates IS NULL",
                ct);

            // Phase 3: Cities
            await GeocodeTableAsync("cities",
                @"SELECT ci.id, ci.name, r.name AS region_name, c.name AS country_name
                  FROM frl.frl_location_cities ci
                  LEFT JOIN frl.frl_location_regions r ON r.id = ci.region_id
                  LEFT JOIN frl.frl_location_countries c ON c.id = ci.country_id
                  WHERE ci.coordinates IS NULL ORDER BY ci.id",
                "UPDATE frl.frl_location_cities SET coordinates = POINT(@lng, @lat) WHERE id = @id",
                "SELECT COUNT(*) FROM frl.frl_location_cities WHERE coordinates IS NULL",
                ct);

            // Phase 4: Specific locations
            await GeocodeSpecificLocationsAsync(ct);

            // Phase 5: Propagate
            await PropagateAsync(ct);

            lock (_lock)
            {
                _progress.Status = "completed";
                _isRunning = false;
            }

            _logger.LogInformation("Full geocode run completed");
        }

        private async Task GeocodeTableAsync(
            string phase, string selectSql, string updateSql, string countSql, CancellationToken ct)
        {
            lock (_lock)
            {
                _progress.Phase = phase;
                _progress.Status = "running";
            }

            using var scope = _scopeFactory.CreateScope();
            var connStr = scope.ServiceProvider.GetRequiredService<IConfiguration>()
                .GetConnectionString("Default")
                ?? throw new InvalidOperationException("No connection string");

            await using var conn = new NpgsqlConnection(connStr);
            await conn.OpenAsync(ct);

            // Count total remaining
            await using (var countCmd = new NpgsqlCommand(countSql, conn))
            {
                var total = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct));
                lock (_lock)
                {
                    _progress.PhaseTotal = total;
                    _progress.PhaseProcessed = 0;
                    _progress.PhaseGeocoded = 0;
                    _progress.PhaseFailed = 0;
                }
            }

            // Process all items in batches, tracking failed IDs to avoid infinite retry
            var failedIds = new HashSet<int>();
            while (!ct.IsCancellationRequested)
            {
                var items = new List<(int id, string name, string? region, string? country)>();

                // Build query excluding already-failed IDs
                var sql = selectSql + " LIMIT 500";
                await using (var cmd = new NpgsqlCommand(sql, conn))
                await using (var reader = await cmd.ExecuteReaderAsync(ct))
                {
                    while (await reader.ReadAsync(ct))
                    {
                        var id = reader.GetInt32(0);
                        if (!failedIds.Contains(id))
                            items.Add((
                                id,
                                reader.GetString(1),
                                reader.IsDBNull(2) ? null : reader.GetString(2),
                                reader.IsDBNull(3) ? null : reader.GetString(3)
                            ));
                    }
                }

                if (items.Count == 0) break;

                int batchGeocoded = 0;
                foreach (var (id, name, region, country) in items)
                {
                    if (ct.IsCancellationRequested) break;

                    var queryParts = new List<string> { name };
                    if (!string.IsNullOrEmpty(region)) queryParts.Add(region);
                    if (!string.IsNullOrEmpty(country)) queryParts.Add(country);
                    var query = string.Join(", ", queryParts);

                    var coords = await GeocodeWithNominatimRaw(query, ct);
                    if (coords.HasValue)
                    {
                        await using var upd = new NpgsqlCommand(updateSql, conn);
                        upd.Parameters.AddWithValue("@lng", coords.Value.lng);
                        upd.Parameters.AddWithValue("@lat", coords.Value.lat);
                        upd.Parameters.AddWithValue("@id", id);
                        await upd.ExecuteNonQueryAsync(ct);
                        lock (_lock) _progress.PhaseGeocoded++;
                        batchGeocoded++;
                    }
                    else
                    {
                        failedIds.Add(id);
                        lock (_lock) _progress.PhaseFailed++;
                        _logger.LogWarning("Failed to geocode {Phase}: {Name}", phase, name);
                    }

                    lock (_lock) _progress.PhaseProcessed++;
                    await Task.Delay(1100, ct);
                }

                // If nothing was geocoded in this batch, remaining items are all failures — stop
                if (batchGeocoded == 0) break;
            }

            _logger.LogInformation("Phase {Phase} complete: {Geocoded} geocoded, {Failed} failed",
                phase,
                _progress.PhaseGeocoded,
                _progress.PhaseFailed);
        }

        private async Task GeocodeSpecificLocationsAsync(CancellationToken ct)
        {
            lock (_lock)
            {
                _progress.Phase = "specific-locations";
                _progress.Status = "running";
            }

            using var scope = _scopeFactory.CreateScope();
            var connStr = scope.ServiceProvider.GetRequiredService<IConfiguration>()
                .GetConnectionString("Default")
                ?? throw new InvalidOperationException("No connection string");

            await using var conn = new NpgsqlConnection(connStr);
            await conn.OpenAsync(ct);

            // Count total
            await using (var countCmd = new NpgsqlCommand(@"
                SELECT COUNT(DISTINCT il.specific_location)
                FROM frl.frl_images_location il
                WHERE il.specific_location IS NOT NULL
                  AND TRIM(il.specific_location) <> ''
                  AND NOT EXISTS (
                      SELECT 1 FROM frl.frl_geocode_cache gc
                      WHERE gc.location_key = il.specific_location
                  )", conn))
            {
                var total = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct));
                lock (_lock)
                {
                    _progress.PhaseTotal = total;
                    _progress.PhaseProcessed = 0;
                    _progress.PhaseGeocoded = 0;
                    _progress.PhaseFailed = 0;
                }
            }

            while (!ct.IsCancellationRequested)
            {
                var items = new List<(string location, string? city, string? country)>();

                await using (var cmd = new NpgsqlCommand(@"
                    SELECT DISTINCT il.specific_location, ci.name AS city_name, c.name AS country_name
                    FROM frl.frl_images_location il
                    LEFT JOIN frl.frl_location_cities ci ON ci.id = il.city_id
                    LEFT JOIN frl.frl_location_countries c ON c.id = il.country_id
                    WHERE il.specific_location IS NOT NULL
                      AND TRIM(il.specific_location) <> ''
                      AND NOT EXISTS (
                          SELECT 1 FROM frl.frl_geocode_cache gc
                          WHERE gc.location_key = il.specific_location
                      )
                    ORDER BY il.specific_location
                    LIMIT 500", conn))
                await using (var reader = await cmd.ExecuteReaderAsync(ct))
                {
                    while (await reader.ReadAsync(ct))
                        items.Add((
                            reader.GetString(0),
                            reader.IsDBNull(1) ? null : reader.GetString(1),
                            reader.IsDBNull(2) ? null : reader.GetString(2)
                        ));
                }

                if (items.Count == 0) break;

                foreach (var (location, city, country) in items)
                {
                    if (ct.IsCancellationRequested) break;

                    var queryParts = new List<string> { location };
                    if (!string.IsNullOrEmpty(city)) queryParts.Add(city);
                    if (!string.IsNullOrEmpty(country)) queryParts.Add(country);
                    var query = string.Join(", ", queryParts);

                    var coords = await GeocodeWithNominatimRaw(query, ct);

                    if (coords.HasValue)
                    {
                        await using var ins = new NpgsqlCommand(@"
                            INSERT INTO frl.frl_geocode_cache (location_key, coordinates, source)
                            VALUES (@key, POINT(@lng, @lat), 'nominatim')
                            ON CONFLICT (location_key) DO NOTHING", conn);
                        ins.Parameters.AddWithValue("@key", location);
                        ins.Parameters.AddWithValue("@lng", coords.Value.lng);
                        ins.Parameters.AddWithValue("@lat", coords.Value.lat);
                        await ins.ExecuteNonQueryAsync(ct);
                        lock (_lock) _progress.PhaseGeocoded++;
                    }
                    else
                    {
                        await using var ins = new NpgsqlCommand(@"
                            INSERT INTO frl.frl_geocode_cache (location_key, coordinates, source)
                            VALUES (@key, NULL, 'nominatim')
                            ON CONFLICT (location_key) DO NOTHING", conn);
                        ins.Parameters.AddWithValue("@key", location);
                        await ins.ExecuteNonQueryAsync(ct);
                        lock (_lock) _progress.PhaseFailed++;
                    }

                    lock (_lock) _progress.PhaseProcessed++;
                    await Task.Delay(1100, ct);
                }
            }
        }

        private async Task PropagateAsync(CancellationToken ct)
        {
            lock (_lock)
            {
                _progress.Phase = "propagate";
                _progress.Status = "running";
            }

            using var scope = _scopeFactory.CreateScope();
            var connStr = scope.ServiceProvider.GetRequiredService<IConfiguration>()
                .GetConnectionString("Default")
                ?? throw new InvalidOperationException("No connection string");

            await using var conn = new NpgsqlConnection(connStr);
            await conn.OpenAsync(ct);

            // Set long command timeout for bulk updates
            var timeout = 600; // 10 minutes

            await using (var cmd = new NpgsqlCommand(@"
                UPDATE frl.frl_images_location il
                SET coordinates = gc.coordinates, updated_at = NOW()
                FROM frl.frl_geocode_cache gc
                WHERE gc.location_key = il.specific_location
                  AND gc.coordinates IS NOT NULL AND il.coordinates IS NULL", conn))
            { cmd.CommandTimeout = timeout; await cmd.ExecuteNonQueryAsync(ct); }

            await using (var cmd = new NpgsqlCommand(@"
                UPDATE frl.frl_images_location il
                SET coordinates = ci.coordinates, updated_at = NOW()
                FROM frl.frl_location_cities ci
                WHERE ci.id = il.city_id
                  AND ci.coordinates IS NOT NULL AND il.coordinates IS NULL", conn))
            { cmd.CommandTimeout = timeout; await cmd.ExecuteNonQueryAsync(ct); }

            await using (var cmd = new NpgsqlCommand(@"
                UPDATE frl.frl_images_location il
                SET coordinates = r.coordinates, updated_at = NOW()
                FROM frl.frl_location_regions r
                WHERE r.id = il.region_id
                  AND r.coordinates IS NOT NULL AND il.coordinates IS NULL", conn))
            { cmd.CommandTimeout = timeout; await cmd.ExecuteNonQueryAsync(ct); }

            await using (var cmd = new NpgsqlCommand(@"
                UPDATE frl.frl_images_location il
                SET coordinates = c.coordinates, updated_at = NOW()
                FROM frl.frl_location_countries c
                WHERE c.id = il.country_id
                  AND c.coordinates IS NOT NULL AND il.coordinates IS NULL", conn))
            { cmd.CommandTimeout = timeout; await cmd.ExecuteNonQueryAsync(ct); }

            _logger.LogInformation("Propagation complete");
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
    }

    public sealed class GeocodeProgress
    {
        public string Status { get; set; } = "idle";
        public string Phase { get; set; } = "";
        public int PhaseTotal { get; set; }
        public int PhaseProcessed { get; set; }
        public int PhaseGeocoded { get; set; }
        public int PhaseFailed { get; set; }
        public string? Error { get; set; }

        public GeocodeProgress Clone() => new()
        {
            Status = Status,
            Phase = Phase,
            PhaseTotal = PhaseTotal,
            PhaseProcessed = PhaseProcessed,
            PhaseGeocoded = PhaseGeocoded,
            PhaseFailed = PhaseFailed,
            Error = Error
        };
    }
}
