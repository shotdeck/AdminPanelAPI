using Npgsql;
using System.Data;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace AdminPanelAPI.Services
{
    public sealed class MovieLocationBackgroundService : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<MovieLocationBackgroundService> _logger;

        private static readonly HttpClient _http = CreateHttpClient();
        private static readonly object _lock = new();
        private static bool _isRunning;
        private static volatile bool _startRequested;
        private static MovieLocationProgress _progress = new();

        private const string WikidataSparqlUrl = "https://query.wikidata.org/sparql";
        private const string NominatimUrl = "https://nominatim.openstreetmap.org/search";

        public MovieLocationBackgroundService(
            IServiceScopeFactory scopeFactory,
            ILogger<MovieLocationBackgroundService> logger)
        {
            _scopeFactory = scopeFactory;
            _logger = logger;
        }

        private static HttpClient CreateHttpClient()
        {
            var client = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
            client.DefaultRequestHeaders.UserAgent.ParseAdd(
                "ShotDeckAdmin/1.0 (https://shotdeck.com; admin@shotdeck.com)");
            client.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("application/json"));
            return client;
        }

        public static bool IsRunning
        {
            get { lock (_lock) return _isRunning; }
        }

        public static MovieLocationProgress GetProgress()
        {
            lock (_lock) return _progress.Clone();
        }

        public static bool TryStart()
        {
            lock (_lock)
            {
                if (_isRunning) return false;
                _isRunning = true;
                _progress = new MovieLocationProgress { Status = "starting" };
                _startRequested = true;
                return true;
            }
        }

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
                    await RunFullPopulateAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Background movie location populate failed");
                    lock (_lock)
                    {
                        _progress.Status = "error";
                        _progress.Error = ex.Message;
                        _isRunning = false;
                    }
                }
            }
        }

        private async Task RunFullPopulateAsync(CancellationToken ct)
        {
            _logger.LogInformation("Starting full movie location populate");

            lock (_lock)
            {
                _progress.Status = "running";
                _progress.Processed = 0;
                _progress.LocationsFound = 0;
                _progress.NoResults = 0;
                _progress.Failed = 0;
                _progress.TotalMovies = 0;
                _progress.Error = null;
            }

            using var scope = _scopeFactory.CreateScope();
            var conn = scope.ServiceProvider.GetRequiredService<NpgsqlConnection>();
            await conn.OpenAsync(ct);

            // Count total movies to process
            var totalMovies = await CountRemainingMovies(conn, ct);
            lock (_lock) { _progress.TotalMovies = totalMovies; }

            const int batchSize = 50;

            while (true)
            {
                ct.ThrowIfCancellationRequested();

                var movies = await FetchBatch(conn, batchSize, ct);
                if (movies.Count == 0) break;

                foreach (var (movieId, title, imdbId) in movies)
                {
                    ct.ThrowIfCancellationRequested();

                    try
                    {
                        var locations = await FetchLocationsFromWikidata(imdbId, ct);

                        if (locations.Count == 0)
                        {
                            locations = await FetchLocationsFromWikipedia(title, imdbId, ct);
                        }

                        if (locations.Count == 0)
                        {
                            lock (_lock) { _progress.NoResults++; }
                            await InsertMovieLocation(conn, movieId, "(no locations found)", null, null, "none", ct);
                        }
                        else
                        {
                            foreach (var loc in locations)
                            {
                                await InsertMovieLocation(conn, movieId, loc.Name, loc.Lat, loc.Lng, loc.Source, ct);
                                lock (_lock) { _progress.LocationsFound++; }
                            }
                        }

                        lock (_lock) { _progress.Processed++; }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to fetch locations for movie {MovieId} ({Title})", movieId, title);
                        lock (_lock) { _progress.Failed++; _progress.Processed++; }
                    }

                    await Task.Delay(200, ct);
                }
            }

            lock (_lock)
            {
                _progress.Status = "completed";
                _isRunning = false;
            }

            _logger.LogInformation("Full movie location populate completed: {Processed} processed, {Locations} locations found",
                _progress.Processed, _progress.LocationsFound);
        }

        private static async Task<List<(int id, string title, string imdbId)>> FetchBatch(
            NpgsqlConnection conn, int batchSize, CancellationToken ct)
        {
            const string sql = @"
SELECT m.idnum, m.title, m.imdbid
FROM frl.frl_movies m
WHERE m.media_type = 'movie'
  AND m.imdbid IS NOT NULL
  AND m.imdbid <> ''
  AND NOT EXISTS (
      SELECT 1 FROM frl.frl_movie_location ml WHERE ml.movie_id = m.idnum
  )
ORDER BY m.idnum
LIMIT @limit;";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@limit", batchSize);

            var movies = new List<(int, string, string)>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                movies.Add((reader.GetInt32(0), reader.GetString(1), reader.GetString(2)));
            }
            return movies;
        }

        private static async Task<int> CountRemainingMovies(NpgsqlConnection conn, CancellationToken ct)
        {
            const string sql = @"
SELECT COUNT(*)
FROM frl.frl_movies m
WHERE m.media_type = 'movie'
  AND m.imdbid IS NOT NULL
  AND m.imdbid <> ''
  AND NOT EXISTS (
      SELECT 1 FROM frl.frl_movie_location ml WHERE ml.movie_id = m.idnum
  );";

            await using var cmd = new NpgsqlCommand(sql, conn);
            return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
        }

        private static async Task InsertMovieLocation(
            NpgsqlConnection conn, int movieId, string locationName, double? lat, double? lng, string source, CancellationToken ct)
        {
            const string sql = @"
INSERT INTO frl.frl_movie_location (movie_id, location_name, latitude, longitude, source)
VALUES (@movieId, @locationName, @lat, @lng, @source)
ON CONFLICT DO NOTHING;";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            cmd.Parameters.AddWithValue("@locationName", locationName);
            cmd.Parameters.AddWithValue("@lat", (object?)lat ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@lng", (object?)lng ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@source", source);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // ── Wikidata SPARQL ──────────────────────────────────────────

        private static async Task<List<LocationResult>> FetchLocationsFromWikidata(string imdbId, CancellationToken ct)
        {
            var normalizedId = imdbId.Trim();
            if (!normalizedId.StartsWith("tt", StringComparison.OrdinalIgnoreCase))
                normalizedId = "tt" + normalizedId;

            var sparql = $@"
SELECT ?locationLabel ?coord WHERE {{
  ?film wdt:P345 ""{EscapeSparql(normalizedId)}"" .
  ?film wdt:P915 ?location .
  ?location wdt:P625 ?coord .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language ""en"". }}
}}";

            var url = $"{WikidataSparqlUrl}?query={Uri.EscapeDataString(sparql)}&format=json";

            var request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.Accept.Clear();
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/sparql-results+json"));

            var response = await _http.SendAsync(request, ct);
            if (!response.IsSuccessStatusCode) return new List<LocationResult>();

            var json = await response.Content.ReadAsStringAsync(ct);
            var doc = JsonDocument.Parse(json);

            var results = new List<LocationResult>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var binding in doc.RootElement
                .GetProperty("results")
                .GetProperty("bindings")
                .EnumerateArray())
            {
                var label = binding.GetProperty("locationLabel").GetProperty("value").GetString() ?? "";
                var coordStr = binding.GetProperty("coord").GetProperty("value").GetString() ?? "";

                if (string.IsNullOrWhiteSpace(label) || !seen.Add(label)) continue;

                var match = Regex.Match(coordStr, @"Point\(([-\d.]+)\s+([-\d.]+)\)");
                if (!match.Success) continue;

                if (double.TryParse(match.Groups[1].Value, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out var lng) &&
                    double.TryParse(match.Groups[2].Value, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out var lat))
                {
                    results.Add(new LocationResult(label, lat, lng, "wikidata"));
                }
            }

            return results;
        }

        // ── Wikipedia fallback ───────────────────────────────────────

        private static async Task<List<LocationResult>> FetchLocationsFromWikipedia(
            string title, string imdbId, CancellationToken ct)
        {
            var searchUrl = $"https://en.wikipedia.org/w/api.php?action=query&list=search" +
                $"&srsearch={Uri.EscapeDataString(title + " film")}&srlimit=1&format=json";

            var searchResponse = await _http.GetAsync(searchUrl, ct);
            if (!searchResponse.IsSuccessStatusCode) return new List<LocationResult>();

            var searchJson = await searchResponse.Content.ReadAsStringAsync(ct);
            var searchDoc = JsonDocument.Parse(searchJson);
            var searchResults = searchDoc.RootElement
                .GetProperty("query")
                .GetProperty("search");

            if (searchResults.GetArrayLength() == 0) return new List<LocationResult>();

            var pageTitle = searchResults[0].GetProperty("title").GetString() ?? "";

            var contentUrl = $"https://en.wikipedia.org/w/api.php?action=parse&page={Uri.EscapeDataString(pageTitle)}" +
                "&prop=wikitext&format=json";

            var contentResponse = await _http.GetAsync(contentUrl, ct);
            if (!contentResponse.IsSuccessStatusCode) return new List<LocationResult>();

            var contentJson = await contentResponse.Content.ReadAsStringAsync(ct);
            var contentDoc = JsonDocument.Parse(contentJson);

            if (!contentDoc.RootElement.TryGetProperty("parse", out var parseElem))
                return new List<LocationResult>();

            var wikitext = parseElem.GetProperty("wikitext").GetProperty("*").GetString() ?? "";

            var locations = ExtractFilmingLocations(wikitext);
            if (locations.Count == 0) return new List<LocationResult>();

            var results = new List<LocationResult>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var locName in locations)
            {
                if (!seen.Add(locName)) continue;

                var coords = await GeocodeWithNominatim(locName, ct);
                if (coords.HasValue)
                {
                    results.Add(new LocationResult(locName, coords.Value.lat, coords.Value.lng, "wikipedia"));
                }

                await Task.Delay(1100, ct);
            }

            return results;
        }

        private static List<string> ExtractFilmingLocations(string wikitext)
        {
            var locations = new List<string>();

            var sectionPattern = new Regex(
                @"={2,3}\s*(Filming|Production|Filming locations?)\s*={2,3}(.*?)(?=={2,3}[^=]|\z)",
                RegexOptions.Singleline | RegexOptions.IgnoreCase);

            var sectionMatch = sectionPattern.Match(wikitext);
            if (!sectionMatch.Success) return locations;

            var sectionText = sectionMatch.Groups[2].Value;

            var locationPatterns = new[]
            {
                @"(?:filmed|shot|filming|shooting|principal photography)\s+(?:in|at|on location in|took place in|began in|continued in|moved to)\s+\[\[([^\]|]+?)(?:\|[^\]]+)?\]\]",
                @"(?:filmed|shot|filming|shooting)\s+(?:in|at|on location in)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*(?:,\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)*)",
                @"\[\[([^\]|]+?)(?:\|[^\]]+)?\]\](?=.*?(?:served as|was used|doubled for|stood in))",
                @"(?:interior|stage|soundstage|studio)\s+(?:scenes?|shooting|work|filming|sequences?)\s+(?:at|in|on)\s+\[\[([^\]|]+?)(?:\|[^\]]+)?\]\]",
                @"\[\[([^\]|]*?(?:Studios?|Soundstage|Pinewood|Shepperton|Cinecitt[aà])(?:[^\]|]*?))(?:\|[^\]]+)?\]\]"
            };

            foreach (var pattern in locationPatterns)
            {
                foreach (Match match in Regex.Matches(sectionText, pattern, RegexOptions.IgnoreCase))
                {
                    var loc = match.Groups[1].Value.Trim();
                    if (loc.Length > 2 && loc.Length < 100 && !IsCommonWord(loc))
                    {
                        locations.Add(loc);
                    }
                }
            }

            var linkPattern = new Regex(@"\[\[([A-Z][^\]|]{2,50}?)(?:\|[^\]]+)?\]\]");
            foreach (Match match in linkPattern.Matches(sectionText))
            {
                var loc = match.Groups[1].Value.Trim();
                if (IsLikelyPlace(loc) && !locations.Contains(loc, StringComparer.OrdinalIgnoreCase))
                {
                    locations.Add(loc);
                }
            }

            return locations.Distinct(StringComparer.OrdinalIgnoreCase).Take(20).ToList();
        }

        private static bool IsCommonWord(string text)
        {
            var common = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "The", "Film", "Movie", "Production", "Director", "Scene", "Set",
                "Studio", "Filming", "Camera", "Budget", "Cast", "Crew", "Script",
                "Screenplay", "Actor", "Actress", "Producer", "Editor"
            };
            return common.Contains(text);
        }

        private static bool IsLikelyNonPlace(string text)
        {
            var organizationKeywords = new[]
            {
                "Pictures", "Entertainment", "Corporation", "Company", "Department",
                "Defense", "Defence", "Agency", "Association", "Foundation",
                "Institute", "University", "College", "Museum", "Theater", "Theatre",
                "Orchestra", "Network", "Broadcasting", "Television", "Records",
                "Productions", "International", "Committee", "Commission",
                "Council", "Society", "Ministry", "Bureau", "Service",
                "Academy", "Award"
            };

            if (organizationKeywords.Any(kw => text.Contains(kw, StringComparison.OrdinalIgnoreCase)))
                return true;

            var words = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (words.Length >= 2 && words.Length <= 3 && !text.Contains(','))
            {
                var allCapitalizedShort = words.All(w =>
                    w.Length >= 2 && w.Length <= 15 &&
                    char.IsUpper(w[0]) && w.Skip(1).All(c => char.IsLower(c) || c == '\'' || c == '-'));

                if (allCapitalizedShort)
                {
                    var placeSignals = new[]
                    {
                        "City", "County", "State", "Island", "Beach", "Park", "Mountain",
                        "River", "Lake", "Forest", "Desert", "Valley", "Bay", "Harbor",
                        "Street", "Avenue", "Boulevard", "Ranch", "Station", "Base",
                        "Fort", "Port", "Cape", "Point", "Hill", "Hills", "Springs",
                        "Falls", "Creek", "Bridge", "Airport", "North", "South", "East",
                        "West", "New", "San", "Santa", "Los", "Las", "Saint", "Upper",
                        "Lower", "Mount", "Old", "Key", "El", "Studio", "Studios"
                    };
                    if (!words.Any(w => placeSignals.Contains(w, StringComparer.OrdinalIgnoreCase)))
                        return true;
                }
            }

            return false;
        }

        private static bool IsLikelyPlace(string text)
        {
            if (IsLikelyNonPlace(text))
                return false;

            var placeIndicators = new[]
            {
                "City", "County", "State", "Island", "Beach", "Park", "Mountain",
                "River", "Lake", "Forest", "Desert", "Valley", "Bay", "Harbor",
                "Street", "Avenue", "Boulevard", "Ranch", "Station", "Base",
                "Fort", "Port", "Cape", "Point", "Hill", "Hills", "Springs",
                "Falls", "Creek", "Bridge", "Airport", "Territory", "Province",
                "District", "Studio", "Studios", "Soundstage", "Lot"
            };

            if (placeIndicators.Any(p => text.Contains(p, StringComparison.OrdinalIgnoreCase)))
                return true;

            if (text.Contains(','))
            {
                var commaPattern = new Regex(
                    @"^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*$");
                if (commaPattern.IsMatch(text))
                    return true;
            }

            var geoPrefix = new Regex(
                @"^(?:San|Santa|Los|Las|Saint|Mount|Fort|Port|Cape|Key|El|New|North|South|East|West)\s",
                RegexOptions.IgnoreCase);
            if (geoPrefix.IsMatch(text))
                return true;

            return false;
        }

        private static async Task<(double lat, double lng)?> GeocodeWithNominatim(string locationName, CancellationToken ct)
        {
            var url = $"{NominatimUrl}?q={Uri.EscapeDataString(locationName)}&format=json&limit=1";

            var request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.UserAgent.ParseAdd("ShotDeckAdmin/1.0 (admin@shotdeck.com)");

            var response = await _http.SendAsync(request, ct);
            if (!response.IsSuccessStatusCode) return null;

            var json = await response.Content.ReadAsStringAsync(ct);
            var arr = JsonDocument.Parse(json).RootElement;
            if (arr.GetArrayLength() == 0) return null;

            var first = arr[0];
            if (double.TryParse(first.GetProperty("lat").GetString(),
                    System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out var lat) &&
                double.TryParse(first.GetProperty("lon").GetString(),
                    System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out var lng))
            {
                return (lat, lng);
            }

            return null;
        }

        private static string EscapeSparql(string input)
        {
            return input.Replace("\\", "\\\\").Replace("\"", "\\\"");
        }

        private record LocationResult(string Name, double Lat, double Lng, string Source);
    }

    public sealed class MovieLocationProgress
    {
        public string Status { get; set; } = "idle";
        public int Processed { get; set; }
        public int LocationsFound { get; set; }
        public int NoResults { get; set; }
        public int Failed { get; set; }
        public int TotalMovies { get; set; }
        public string? Error { get; set; }

        public MovieLocationProgress Clone() => new()
        {
            Status = Status,
            Processed = Processed,
            LocationsFound = LocationsFound,
            NoResults = NoResults,
            Failed = Failed,
            TotalMovies = TotalMovies,
            Error = Error
        };
    }
}
