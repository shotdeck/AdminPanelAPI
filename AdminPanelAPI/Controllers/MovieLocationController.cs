using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Data;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace AdminPanelAPI.Controllers
{
    [ApiController]
    [Route("api/admin/movie-locations")]
    public sealed class MovieLocationController : ControllerBase
    {
        private static readonly HttpClient Http = new()
        {
            Timeout = TimeSpan.FromSeconds(30)
        };

        private const string WikidataSparqlUrl = "https://query.wikidata.org/sparql";
        private const string NominatimUrl = "https://nominatim.openstreetmap.org/search";

        private readonly NpgsqlConnection _connection;
        private readonly ILogger<MovieLocationController> _logger;

        static MovieLocationController()
        {
            Http.DefaultRequestHeaders.UserAgent.ParseAdd(
                "ShotDeckAdmin/1.0 (https://shotdeck.com; admin@shotdeck.com)");
            Http.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("application/json"));
        }

        public MovieLocationController(
            NpgsqlConnection connection,
            ILogger<MovieLocationController> logger)
        {
            _connection = connection;
            _logger = logger;
        }

        // ── GET all movie locations ─────────────────────────────────

        [HttpGet]
        [ProducesResponseType(typeof(MovieLocationPageResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<MovieLocationPageResponse>> GetAll(
            [FromQuery] int? movieId,
            [FromQuery] string? search,
            [FromQuery] int page = 1,
            [FromQuery] int pageSize = 50,
            CancellationToken ct = default)
        {
            if (page < 1) page = 1;
            if (pageSize < 1 || pageSize > 200) pageSize = 50;

            await EnsureOpenAsync(ct);

            var where = new List<string>();
            await using var cmd = new NpgsqlCommand();
            cmd.Connection = _connection;

            if (movieId.HasValue)
            {
                where.Add("ml.movie_id = @movieId");
                cmd.Parameters.AddWithValue("@movieId", movieId.Value);
            }
            if (!string.IsNullOrWhiteSpace(search))
            {
                where.Add("(m.title ILIKE @search OR ml.location_name ILIKE @search)");
                cmd.Parameters.AddWithValue("@search", $"%{search.Trim()}%");
            }

            var whereStr = where.Count > 0 ? "WHERE " + string.Join(" AND ", where) : "";

            cmd.CommandText = $"SELECT COUNT(*) FROM frl.frl_movie_location ml INNER JOIN frl.frl_movies m ON m.idnum = ml.movie_id {whereStr};";
            var totalCount = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

            var offset = (page - 1) * pageSize;
            cmd.CommandText = $@"
SELECT ml.id, ml.movie_id, m.title AS movie_title, m.year AS movie_year,
       m.poster, ml.location_name, ml.latitude, ml.longitude, ml.source, ml.created_at
FROM frl.frl_movie_location ml
INNER JOIN frl.frl_movies m ON m.idnum = ml.movie_id
{whereStr}
ORDER BY m.title, ml.location_name
LIMIT @limit OFFSET @offset;";
            cmd.Parameters.AddWithValue("@limit", pageSize);
            cmd.Parameters.AddWithValue("@offset", offset);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var items = new List<MovieLocationDto>();
            while (await reader.ReadAsync(ct))
            {
                var posterVal = reader.IsDBNull(reader.GetOrdinal("poster"))
                    ? null : reader.GetString(reader.GetOrdinal("poster"));
                items.Add(new MovieLocationDto
                {
                    Id = reader.GetInt32(reader.GetOrdinal("id")),
                    MovieId = reader.GetInt32(reader.GetOrdinal("movie_id")),
                    MovieTitle = reader.GetString(reader.GetOrdinal("movie_title")),
                    MovieYear = reader.IsDBNull(reader.GetOrdinal("movie_year"))
                        ? null : reader.GetInt32(reader.GetOrdinal("movie_year")),
                    Poster = posterVal != null ? "https://image.tmdb.org/t/p/w154" + posterVal : null,
                    LocationName = reader.GetString(reader.GetOrdinal("location_name")),
                    Latitude = reader.IsDBNull(reader.GetOrdinal("latitude"))
                        ? null : reader.GetDouble(reader.GetOrdinal("latitude")),
                    Longitude = reader.IsDBNull(reader.GetOrdinal("longitude"))
                        ? null : reader.GetDouble(reader.GetOrdinal("longitude")),
                    Source = reader.GetString(reader.GetOrdinal("source")),
                    CreatedAt = reader.GetDateTime(reader.GetOrdinal("created_at"))
                });
            }

            return Ok(new MovieLocationPageResponse
            {
                Items = items,
                Page = page,
                PageSize = pageSize,
                TotalCount = totalCount
            });
        }

        // ── GET locations for a single movie ────────────────────────

        [HttpGet("by-movie/{movieId:int}")]
        [ProducesResponseType(typeof(List<MovieLocationDto>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<MovieLocationDto>>> GetByMovie(int movieId, CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            const string sql = @"
SELECT ml.id, ml.movie_id, m.title AS movie_title, m.year AS movie_year,
       m.poster, ml.location_name, ml.latitude, ml.longitude, ml.source, ml.created_at
FROM frl.frl_movie_location ml
INNER JOIN frl.frl_movies m ON m.idnum = ml.movie_id
WHERE ml.movie_id = @movieId
ORDER BY ml.location_name;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var items = new List<MovieLocationDto>();
            while (await reader.ReadAsync(ct))
            {
                var posterVal = reader.IsDBNull(reader.GetOrdinal("poster"))
                    ? null : reader.GetString(reader.GetOrdinal("poster"));
                items.Add(new MovieLocationDto
                {
                    Id = reader.GetInt32(reader.GetOrdinal("id")),
                    MovieId = reader.GetInt32(reader.GetOrdinal("movie_id")),
                    MovieTitle = reader.GetString(reader.GetOrdinal("movie_title")),
                    MovieYear = reader.IsDBNull(reader.GetOrdinal("movie_year"))
                        ? null : reader.GetInt32(reader.GetOrdinal("movie_year")),
                    Poster = posterVal != null ? "https://image.tmdb.org/t/p/w154" + posterVal : null,
                    LocationName = reader.GetString(reader.GetOrdinal("location_name")),
                    Latitude = reader.IsDBNull(reader.GetOrdinal("latitude"))
                        ? null : reader.GetDouble(reader.GetOrdinal("latitude")),
                    Longitude = reader.IsDBNull(reader.GetOrdinal("longitude"))
                        ? null : reader.GetDouble(reader.GetOrdinal("longitude")),
                    Source = reader.GetString(reader.GetOrdinal("source")),
                    CreatedAt = reader.GetDateTime(reader.GetOrdinal("created_at"))
                });
            }

            return Ok(items);
        }

        // ── GET all geocoded locations (for map view) ───────────────

        [HttpGet("map")]
        [ProducesResponseType(typeof(List<MapPointDto>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<MapPointDto>>> GetMapPoints(
            [FromQuery] string? search,
            CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            var where = "WHERE ml.latitude IS NOT NULL AND ml.longitude IS NOT NULL";
            await using var cmd = new NpgsqlCommand();
            cmd.Connection = _connection;

            if (!string.IsNullOrWhiteSpace(search))
            {
                where += " AND m.title ILIKE @search";
                cmd.Parameters.AddWithValue("@search", $"%{search.Trim()}%");
            }

            cmd.CommandText = $@"
SELECT ml.id, ml.movie_id, m.title AS movie_title, m.year AS movie_year,
       m.poster, ml.location_name, ml.latitude, ml.longitude
FROM frl.frl_movie_location ml
INNER JOIN frl.frl_movies m ON m.idnum = ml.movie_id
{where}
ORDER BY m.title;";

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var points = new List<MapPointDto>();
            while (await reader.ReadAsync(ct))
            {
                var posterVal = reader.IsDBNull(reader.GetOrdinal("poster"))
                    ? null : reader.GetString(reader.GetOrdinal("poster"));
                points.Add(new MapPointDto
                {
                    Id = reader.GetInt32(reader.GetOrdinal("id")),
                    MovieId = reader.GetInt32(reader.GetOrdinal("movie_id")),
                    MovieTitle = reader.GetString(reader.GetOrdinal("movie_title")),
                    MovieYear = reader.IsDBNull(reader.GetOrdinal("movie_year"))
                        ? null : reader.GetInt32(reader.GetOrdinal("movie_year")),
                    Poster = posterVal != null ? "https://image.tmdb.org/t/p/w154" + posterVal : null,
                    LocationName = reader.GetString(reader.GetOrdinal("location_name")),
                    Latitude = reader.GetDouble(reader.GetOrdinal("latitude")),
                    Longitude = reader.GetDouble(reader.GetOrdinal("longitude"))
                });
            }

            return Ok(points);
        }

        // ── GET image locations for a movie (from frl_images_location) ─

        [HttpGet("image-locations/{movieId:int}")]
        [ProducesResponseType(typeof(List<ImageLocationDto>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<ImageLocationDto>>> GetImageLocations(int movieId, CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            const string sql = @"
SELECT
    il.id,
    il.specific_location,
    ci.name AS city_name,
    r.name AS region_name,
    co.name AS country_name,
    il.coordinates[0] AS lng,
    il.coordinates[1] AS lat
FROM frl.frl_images_location il
INNER JOIN frl.frl_images i ON i.idnum = il.image_id
LEFT JOIN frl.frl_location_cities ci ON ci.id = il.city_id
LEFT JOIN frl.frl_location_regions r ON r.id = il.region_id
LEFT JOIN frl.frl_location_countries co ON co.id = il.country_id
WHERE i.movieid = @movieId
  AND il.coordinates IS NOT NULL
ORDER BY co.name, r.name, ci.name, il.specific_location;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var items = new List<ImageLocationDto>();
            while (await reader.ReadAsync(ct))
            {
                var specific = reader.IsDBNull(reader.GetOrdinal("specific_location"))
                    ? null : reader.GetString(reader.GetOrdinal("specific_location"));
                var city = reader.IsDBNull(reader.GetOrdinal("city_name"))
                    ? null : reader.GetString(reader.GetOrdinal("city_name"));
                var region = reader.IsDBNull(reader.GetOrdinal("region_name"))
                    ? null : reader.GetString(reader.GetOrdinal("region_name"));
                var country = reader.IsDBNull(reader.GetOrdinal("country_name"))
                    ? null : reader.GetString(reader.GetOrdinal("country_name"));

                var locationName = specific ?? city ?? region ?? country ?? "Unknown";

                items.Add(new ImageLocationDto
                {
                    Id = reader.GetInt64(reader.GetOrdinal("id")),
                    LocationName = locationName,
                    City = city,
                    Region = region,
                    Country = country,
                    Latitude = reader.GetDouble(reader.GetOrdinal("lat")),
                    Longitude = reader.GetDouble(reader.GetOrdinal("lng"))
                });
            }

            return Ok(items);
        }

        // ── GET image locations for a movie (grouped/deduplicated) ────

        [HttpGet("image-locations/{movieId:int}/grouped")]
        [ProducesResponseType(typeof(List<GroupedImageLocationDto>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<GroupedImageLocationDto>>> GetImageLocationsGrouped(int movieId, CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            const string sql = @"
SELECT
    COALESCE(il.specific_location, ci.name, r.name, co.name, 'Unknown') AS location_name,
    ci.name AS city_name,
    r.name AS region_name,
    co.name AS country_name,
    AVG(il.coordinates[0]) AS lng,
    AVG(il.coordinates[1]) AS lat,
    COUNT(*) AS image_count
FROM frl.frl_images_location il
INNER JOIN frl.frl_images i ON i.idnum = il.image_id
LEFT JOIN frl.frl_location_cities ci ON ci.id = il.city_id
LEFT JOIN frl.frl_location_regions r ON r.id = il.region_id
LEFT JOIN frl.frl_location_countries co ON co.id = il.country_id
WHERE i.movieid = @movieId
  AND il.coordinates IS NOT NULL
GROUP BY COALESCE(il.specific_location, ci.name, r.name, co.name, 'Unknown'),
         ci.name, r.name, co.name
ORDER BY image_count DESC;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var items = new List<GroupedImageLocationDto>();
            while (await reader.ReadAsync(ct))
            {
                items.Add(new GroupedImageLocationDto
                {
                    LocationName = reader.GetString(reader.GetOrdinal("location_name")),
                    City = reader.IsDBNull(reader.GetOrdinal("city_name")) ? null : reader.GetString(reader.GetOrdinal("city_name")),
                    Region = reader.IsDBNull(reader.GetOrdinal("region_name")) ? null : reader.GetString(reader.GetOrdinal("region_name")),
                    Country = reader.IsDBNull(reader.GetOrdinal("country_name")) ? null : reader.GetString(reader.GetOrdinal("country_name")),
                    Latitude = reader.GetDouble(reader.GetOrdinal("lat")),
                    Longitude = reader.GetDouble(reader.GetOrdinal("lng")),
                    ImageCount = reader.GetInt32(reader.GetOrdinal("image_count"))
                });
            }

            return Ok(items);
        }

        // ── GET stats ───────────────────────────────────────────────

        [HttpGet("stats")]
        [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
        public async Task<IActionResult> GetStats(CancellationToken ct = default)
        {
            await EnsureOpenAsync(ct);

            const string sql = @"
SELECT
    COUNT(*) AS total_locations,
    COUNT(*) FILTER (WHERE latitude IS NOT NULL) AS geocoded,
    COUNT(DISTINCT movie_id) AS movies_with_locations,
    (SELECT COUNT(*) FROM frl.frl_movies WHERE media_type = 'movie') AS total_movies
FROM frl.frl_movie_location;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);

            return Ok(new
            {
                totalLocations = reader.GetInt64(0),
                geocoded = reader.GetInt64(1),
                moviesWithLocations = reader.GetInt64(2),
                totalMovies = reader.GetInt64(3)
            });
        }

        // ── POST populate — batch fetch from Wikidata ───────────────

        [HttpPost("populate")]
        [ProducesResponseType(typeof(PopulateResponse), StatusCodes.Status200OK)]
        public async Task<ActionResult<PopulateResponse>> Populate(
            [FromQuery] int batchSize = 50,
            CancellationToken ct = default)
        {
            if (batchSize < 1) batchSize = 50;
            if (batchSize > 500) batchSize = 500;

            await EnsureOpenAsync(ct);

            // Fetch movies (media_type='movie') that have an imdbid and
            // don't already have entries in frl_movie_location.
            const string fetchSql = @"
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

            await using var fetchCmd = new NpgsqlCommand(fetchSql, _connection);
            fetchCmd.Parameters.AddWithValue("@limit", batchSize);

            var movies = new List<(int id, string title, string imdbId)>();
            await using (var reader = await fetchCmd.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    movies.Add((
                        reader.GetInt32(0),
                        reader.GetString(1),
                        reader.GetString(2)
                    ));
                }
            }

            var processed = 0;
            var locationsFound = 0;
            var failed = 0;
            var noResults = 0;

            foreach (var (movieId, title, imdbId) in movies)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    var locations = await FetchLocationsFromWikidata(imdbId, ct);

                    if (locations.Count == 0)
                    {
                        // Fallback: try Wikipedia text parsing + Nominatim geocoding
                        locations = await FetchLocationsFromWikipedia(title, imdbId, ct);
                    }

                    if (locations.Count == 0)
                    {
                        noResults++;
                        // Insert a sentinel row so we don't re-query this movie
                        await InsertMovieLocation(movieId, "(no locations found)", null, null, "none", ct);
                        processed++;
                        continue;
                    }

                    foreach (var loc in locations)
                    {
                        await InsertMovieLocation(movieId, loc.Name, loc.Lat, loc.Lng, loc.Source, ct);
                        locationsFound++;
                    }

                    processed++;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to fetch locations for movie {MovieId} ({Title})", movieId, title);
                    failed++;
                    processed++;
                }

                // Rate limiting: Wikidata asks for polite usage
                await Task.Delay(200, ct);
            }

            return Ok(new PopulateResponse
            {
                Processed = processed,
                LocationsFound = locationsFound,
                NoResults = noResults,
                Failed = failed,
                RemainingMovies = await CountRemainingMovies(ct)
            });
        }

        // ── Wikidata SPARQL query ───────────────────────────────────

        private async Task<List<LocationResult>> FetchLocationsFromWikidata(string imdbId, CancellationToken ct)
        {
            // Normalize IMDB ID format (ensure it starts with "tt")
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

            var response = await Http.SendAsync(request, ct);
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

                // Parse "Point(lng lat)" format from Wikidata
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

        // ── Wikipedia fallback: parse article for filming locations ──

        private async Task<List<LocationResult>> FetchLocationsFromWikipedia(
            string title, string imdbId, CancellationToken ct)
        {
            // Search Wikipedia for the movie article
            var searchUrl = $"https://en.wikipedia.org/w/api.php?action=query&list=search" +
                $"&srsearch={Uri.EscapeDataString(title + " film")}&srlimit=1&format=json";

            var searchResponse = await Http.GetAsync(searchUrl, ct);
            if (!searchResponse.IsSuccessStatusCode) return new List<LocationResult>();

            var searchJson = await searchResponse.Content.ReadAsStringAsync(ct);
            var searchDoc = JsonDocument.Parse(searchJson);
            var searchResults = searchDoc.RootElement
                .GetProperty("query")
                .GetProperty("search");

            if (searchResults.GetArrayLength() == 0) return new List<LocationResult>();

            var pageTitle = searchResults[0].GetProperty("title").GetString() ?? "";

            // Get the article content, focusing on the Production section
            var contentUrl = $"https://en.wikipedia.org/w/api.php?action=parse&page={Uri.EscapeDataString(pageTitle)}" +
                "&prop=wikitext&format=json";

            var contentResponse = await Http.GetAsync(contentUrl, ct);
            if (!contentResponse.IsSuccessStatusCode) return new List<LocationResult>();

            var contentJson = await contentResponse.Content.ReadAsStringAsync(ct);
            var contentDoc = JsonDocument.Parse(contentJson);

            if (!contentDoc.RootElement.TryGetProperty("parse", out var parseElem))
                return new List<LocationResult>();

            var wikitext = parseElem.GetProperty("wikitext").GetProperty("*").GetString() ?? "";

            // Extract filming locations from the Production/Filming section
            var locations = ExtractFilmingLocations(wikitext);
            if (locations.Count == 0) return new List<LocationResult>();

            // Geocode each location using Nominatim
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

                // Nominatim rate limit: 1 request per second
                await Task.Delay(1100, ct);
            }

            return results;
        }

        private static List<string> ExtractFilmingLocations(string wikitext)
        {
            var locations = new List<string>();

            // Find the Filming or Production section
            var sectionPattern = new Regex(
                @"={2,3}\s*(Filming|Production|Filming locations?)\s*={2,3}(.*?)(?=={2,3}[^=]|\z)",
                RegexOptions.Singleline | RegexOptions.IgnoreCase);

            var sectionMatch = sectionPattern.Match(wikitext);
            if (!sectionMatch.Success) return locations;

            var sectionText = sectionMatch.Groups[2].Value;

            // Look for location patterns: "filmed in/at X", "shooting took place in X", etc.
            var locationPatterns = new[]
            {
                @"(?:filmed|shot|filming|shooting|principal photography)\s+(?:in|at|on location in|took place in|began in|continued in|moved to)\s+\[\[([^\]|]+?)(?:\|[^\]]+)?\]\]",
                @"(?:filmed|shot|filming|shooting)\s+(?:in|at|on location in)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*(?:,\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)*)",
                @"\[\[([^\]|]+?)(?:\|[^\]]+)?\]\](?=.*?(?:served as|was used|doubled for|stood in))"
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

            // Also extract standalone wikilinks in the filming section that look like places
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

        private static bool IsLikelyPlace(string text)
        {
            var placeIndicators = new[]
            {
                "City", "County", "State", "Island", "Beach", "Park", "Mountain",
                "River", "Lake", "Forest", "Desert", "Valley", "Bay", "Harbor",
                "Street", "Avenue", "Boulevard", "Studio", "Ranch"
            };

            var stateCountryPattern = new Regex(
                @"^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*(?:,\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)?$");

            return stateCountryPattern.IsMatch(text) ||
                placeIndicators.Any(p => text.Contains(p, StringComparison.OrdinalIgnoreCase));
        }

        // ── Nominatim geocoding ─────────────────────────────────────

        private async Task<(double lat, double lng)?> GeocodeWithNominatim(string locationName, CancellationToken ct)
        {
            var url = $"{NominatimUrl}?q={Uri.EscapeDataString(locationName)}&format=json&limit=1";

            var request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.UserAgent.ParseAdd("ShotDeckAdmin/1.0 (admin@shotdeck.com)");

            var response = await Http.SendAsync(request, ct);
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

        // ── DB helpers ──────────────────────────────────────────────

        private async Task InsertMovieLocation(
            int movieId, string locationName, double? lat, double? lng, string source, CancellationToken ct)
        {
            const string sql = @"
INSERT INTO frl.frl_movie_location (movie_id, location_name, latitude, longitude, source)
VALUES (@movieId, @locationName, @lat, @lng, @source)
ON CONFLICT DO NOTHING;";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            cmd.Parameters.AddWithValue("@locationName", locationName);
            cmd.Parameters.AddWithValue("@lat", (object?)lat ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@lng", (object?)lng ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@source", source);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        private async Task<int> CountRemainingMovies(CancellationToken ct)
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

            await using var cmd = new NpgsqlCommand(sql, _connection);
            return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
        }

        private async Task EnsureOpenAsync(CancellationToken ct)
        {
            if (_connection.State != ConnectionState.Open)
                await _connection.OpenAsync(ct);
        }

        private static string EscapeSparql(string input)
        {
            return input.Replace("\\", "\\\\").Replace("\"", "\\\"");
        }

        // ── DTOs ────────────────────────────────────────────────────

        private record LocationResult(string Name, double Lat, double Lng, string Source);
    }

    public sealed class MovieLocationDto
    {
        public int Id { get; set; }
        public int MovieId { get; set; }
        public string MovieTitle { get; set; } = "";
        public int? MovieYear { get; set; }
        public string? Poster { get; set; }
        public string LocationName { get; set; } = "";
        public double? Latitude { get; set; }
        public double? Longitude { get; set; }
        public string Source { get; set; } = "";
        public DateTime CreatedAt { get; set; }
    }

    public sealed class MapPointDto
    {
        public int Id { get; set; }
        public int MovieId { get; set; }
        public string MovieTitle { get; set; } = "";
        public int? MovieYear { get; set; }
        public string? Poster { get; set; }
        public string LocationName { get; set; } = "";
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }

    public sealed class MovieLocationPageResponse
    {
        public List<MovieLocationDto> Items { get; set; } = new();
        public int Page { get; set; }
        public int PageSize { get; set; }
        public int TotalCount { get; set; }
    }

    public sealed class PopulateResponse
    {
        public int Processed { get; set; }
        public int LocationsFound { get; set; }
        public int NoResults { get; set; }
        public int Failed { get; set; }
        public int RemainingMovies { get; set; }
    }

    public sealed class ImageLocationDto
    {
        public long Id { get; set; }
        public string LocationName { get; set; } = "";
        public string? City { get; set; }
        public string? Region { get; set; }
        public string? Country { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }

    public sealed class GroupedImageLocationDto
    {
        public string LocationName { get; set; } = "";
        public string? City { get; set; }
        public string? Region { get; set; }
        public string? Country { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
        public int ImageCount { get; set; }
    }
}
