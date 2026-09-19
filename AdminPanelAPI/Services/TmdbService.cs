using System.Collections.Concurrent;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    /// <summary>One film as TMDB's search returns it, for the picker's dropdown.</summary>
    public sealed class TmdbSearchResult
    {
        public int TmdbId { get; init; }
        public string Title { get; init; } = "";
        public string OriginalTitle { get; init; } = "";
        public int? Year { get; init; }
        public string? ReleaseDate { get; init; }
        public string Overview { get; init; } = "";
        public string? PosterThumbUrl { get; init; }
        public string? PosterUrl { get; init; }
        public double VoteAverage { get; init; }
        public int VoteCount { get; init; }
        public double Popularity { get; init; }
    }

    /// <summary>A named person from a film's credits.</summary>
    public sealed class TmdbCredit
    {
        public string Name { get; init; } = "";
        public string? Role { get; init; }
        public string? ProfileUrl { get; init; }
    }

    /// <summary>
    /// Everything TMDB knows about one film that helps a tagger be sure it is
    /// the right one before the file is bound to it.
    /// </summary>
    public sealed class TmdbMovieDetails
    {
        public int TmdbId { get; init; }
        public string? ImdbId { get; init; }
        public string Title { get; init; } = "";
        public string OriginalTitle { get; init; } = "";
        public int? Year { get; init; }
        public string? ReleaseDate { get; init; }
        public string? Certification { get; init; }
        public int? RuntimeMinutes { get; init; }
        public string Overview { get; init; } = "";
        public string? Tagline { get; init; }
        public string? Status { get; init; }
        public string? Collection { get; init; }
        public string? HomePage { get; init; }
        public string? PosterThumbUrl { get; init; }
        public string? PosterUrl { get; init; }
        public string? PosterPath { get; init; }
        public string? BackdropUrl { get; init; }
        public double VoteAverage { get; init; }
        public int VoteCount { get; init; }
        public long Budget { get; init; }
        public long Revenue { get; init; }
        public string OriginalLanguage { get; init; } = "";
        public List<string> Genres { get; init; } = new();
        public List<string> Countries { get; init; } = new();
        public List<string> Languages { get; init; } = new();
        public List<string> Companies { get; init; } = new();
        public List<TmdbCredit> Directors { get; init; } = new();
        public List<TmdbCredit> Writers { get; init; } = new();
        public List<TmdbCredit> Cinematographers { get; init; } = new();
        public List<TmdbCredit> Cast { get; init; } = new();
    }

    /// <summary>
    /// TMDB lookups for identifying an uploaded master. A file arrives named
    /// after the film rather than keyed to it, so the tagger searches TMDB as
    /// they type and confirms one result; the details are what makes that
    /// confirmation safe — poster, year, runtime, director and cast.
    ///
    /// The key is read server-side so it never reaches the browser, and
    /// details are cached for the life of the process because a released
    /// film's data does not change and the popup is reopened often.
    /// </summary>
    public interface ITmdbService
    {
        /// <summary>Whether a key is configured at all.</summary>
        bool Configured { get; }

        Task<List<TmdbSearchResult>> SearchAsync(
            string query, int? year, int limit, CancellationToken ct);

        Task<TmdbMovieDetails?> DetailsAsync(int tmdbId, CancellationToken ct);
    }

    public sealed class TmdbService : ITmdbService
    {
        private const string ApiBaseUrl = "https://api.themoviedb.org/3";
        private const string ImageBaseUrl = "https://image.tmdb.org/t/p/";

        /// <summary>Wide enough to judge a frame grab, small enough for a list.</summary>
        private const string ThumbSize = "w92";
        private const string PosterSize = "w342";
        private const string BackdropSize = "w780";

        /// <summary>How many of a big cast a tagger needs to recognise a film.</summary>
        private const int CastShown = 12;

        private static readonly ConcurrentDictionary<int, TmdbMovieDetails> DetailsCache = new();

        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<TmdbService> _logger;
        private readonly string _apiKey;

        public TmdbService(
            IHttpClientFactory httpClientFactory,
            IConfiguration configuration,
            ILogger<TmdbService> logger)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;

            // Azure holds it as an app setting; a shell holds it as TMDB_API_KEY.
            _apiKey = configuration["Tmdb:ApiKey"]
                ?? configuration["TMDB_API_KEY"]
                ?? "";
        }

        public bool Configured => _apiKey.Length > 0;

        public async Task<List<TmdbSearchResult>> SearchAsync(
            string query, int? year, int limit, CancellationToken ct)
        {
            var results = new List<TmdbSearchResult>();
            if (!Configured || string.IsNullOrWhiteSpace(query))
                return results;

            var url = ApiBaseUrl + "/search/movie?include_adult=false&language=en-US&api_key=" +
                Uri.EscapeDataString(_apiKey) +
                "&query=" + Uri.EscapeDataString(query.Trim()) +
                (year.HasValue ? "&year=" + year.Value : "");

            using var document = await GetAsync(url, ct);
            if (document is null || !document.RootElement.TryGetProperty("results", out var found))
                return results;

            foreach (var item in found.EnumerateArray())
            {
                var poster = Text(item, "poster_path");
                var release = Text(item, "release_date");
                results.Add(new TmdbSearchResult
                {
                    TmdbId = Number(item, "id") ?? 0,
                    Title = Text(item, "title") ?? "",
                    OriginalTitle = Text(item, "original_title") ?? "",
                    Year = YearOf(release),
                    ReleaseDate = release,
                    Overview = Text(item, "overview") ?? "",
                    PosterThumbUrl = Image(poster, ThumbSize),
                    PosterUrl = Image(poster, PosterSize),
                    VoteAverage = Decimal(item, "vote_average"),
                    VoteCount = Number(item, "vote_count") ?? 0,
                    Popularity = Decimal(item, "popularity")
                });

                if (results.Count >= limit)
                    break;
            }

            return results;
        }

        public async Task<TmdbMovieDetails?> DetailsAsync(int tmdbId, CancellationToken ct)
        {
            if (!Configured || tmdbId <= 0)
                return null;

            if (DetailsCache.TryGetValue(tmdbId, out var cached))
                return cached;

            var url = ApiBaseUrl + "/movie/" + tmdbId +
                "?language=en-US&append_to_response=credits,release_dates,external_ids" +
                "&api_key=" + Uri.EscapeDataString(_apiKey);

            using var document = await GetAsync(url, ct);
            if (document is null)
                return null;

            var root = document.RootElement;
            if (Number(root, "id") is not { } id)
                return null;

            var poster = Text(root, "poster_path");
            var release = Text(root, "release_date");
            var details = new TmdbMovieDetails
            {
                TmdbId = id,
                ImdbId = Text(root, "imdb_id"),
                Title = Text(root, "title") ?? "",
                OriginalTitle = Text(root, "original_title") ?? "",
                Year = YearOf(release),
                ReleaseDate = release,
                Certification = CertificationOf(root),
                RuntimeMinutes = Number(root, "runtime"),
                Overview = Text(root, "overview") ?? "",
                Tagline = Text(root, "tagline"),
                Status = Text(root, "status"),
                Collection = root.TryGetProperty("belongs_to_collection", out var collection) &&
                    collection.ValueKind == JsonValueKind.Object
                        ? Text(collection, "name")
                        : null,
                HomePage = Text(root, "homepage"),
                PosterThumbUrl = Image(poster, ThumbSize),
                PosterUrl = Image(poster, PosterSize),
                PosterPath = poster,
                BackdropUrl = Image(Text(root, "backdrop_path"), BackdropSize),
                VoteAverage = Decimal(root, "vote_average"),
                VoteCount = Number(root, "vote_count") ?? 0,
                Budget = Money(root, "budget"),
                Revenue = Money(root, "revenue"),
                OriginalLanguage = Text(root, "original_language") ?? "",
                Genres = Names(root, "genres", "name"),
                Countries = Names(root, "production_countries", "name"),
                Languages = Names(root, "spoken_languages", "english_name"),
                Companies = Names(root, "production_companies", "name"),
                Directors = Crew(root, "Director"),
                Writers = Crew(root, "Writer", "Screenplay", "Story"),
                Cinematographers = Crew(root, "Director of Photography"),
                Cast = Cast(root)
            };

            DetailsCache[tmdbId] = details;
            return details;
        }

        private async Task<JsonDocument?> GetAsync(string url, CancellationToken ct)
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(15);

            try
            {
                return JsonDocument.Parse(await client.GetStringAsync(url, ct));
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or JsonException)
            {
                // A failed lookup leaves the file unidentified rather than
                // failing the page, so the caller sees an empty answer.
                _logger.LogWarning(ex, "TMDB request failed.");
                return null;
            }
        }

        /// <summary>The film's own certificate where TMDB carries a US rating.</summary>
        private static string? CertificationOf(JsonElement root)
        {
            if (!root.TryGetProperty("release_dates", out var dates) ||
                !dates.TryGetProperty("results", out var countries))
                return null;

            foreach (var country in countries.EnumerateArray())
            {
                if (Text(country, "iso_3166_1") != "US" ||
                    !country.TryGetProperty("release_dates", out var releases))
                    continue;

                foreach (var release in releases.EnumerateArray())
                {
                    if (Text(release, "certification") is { Length: > 0 } certification)
                        return certification;
                }
            }

            return null;
        }

        private static List<TmdbCredit> Crew(JsonElement root, params string[] jobs)
        {
            var people = new List<TmdbCredit>();
            if (!root.TryGetProperty("credits", out var credits) ||
                !credits.TryGetProperty("crew", out var crew))
                return people;

            foreach (var member in crew.EnumerateArray())
            {
                var job = Text(member, "job") ?? "";
                if (!jobs.Contains(job, StringComparer.OrdinalIgnoreCase))
                    continue;

                var name = Text(member, "name") ?? "";
                if (name.Length == 0 || people.Any(p => p.Name == name))
                    continue;

                people.Add(new TmdbCredit
                {
                    Name = name,
                    Role = job,
                    ProfileUrl = Image(Text(member, "profile_path"), ThumbSize)
                });
            }

            return people;
        }

        private static List<TmdbCredit> Cast(JsonElement root)
        {
            var people = new List<TmdbCredit>();
            if (!root.TryGetProperty("credits", out var credits) ||
                !credits.TryGetProperty("cast", out var cast))
                return people;

            foreach (var member in cast.EnumerateArray())
            {
                people.Add(new TmdbCredit
                {
                    Name = Text(member, "name") ?? "",
                    Role = Text(member, "character"),
                    ProfileUrl = Image(Text(member, "profile_path"), ThumbSize)
                });

                if (people.Count >= CastShown)
                    break;
            }

            return people;
        }

        private static List<string> Names(JsonElement root, string property, string field)
        {
            var names = new List<string>();
            if (!root.TryGetProperty(property, out var items) ||
                items.ValueKind != JsonValueKind.Array)
                return names;

            foreach (var item in items.EnumerateArray())
            {
                if (Text(item, field) is { Length: > 0 } name)
                    names.Add(name);
            }

            return names;
        }

        private static string? Image(string? path, string size) =>
            string.IsNullOrWhiteSpace(path) ? null : ImageBaseUrl + size + path;

        private static int? YearOf(string? releaseDate) =>
            releaseDate is { Length: >= 4 } && int.TryParse(releaseDate[..4], out var year)
                ? year : null;

        private static string? Text(JsonElement element, string property) =>
            element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String
                ? value.GetString()
                : null;

        private static int? Number(JsonElement element, string property) =>
            element.TryGetProperty(property, out var value) &&
            value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var number)
                ? number
                : null;

        private static long Money(JsonElement element, string property) =>
            element.TryGetProperty(property, out var value) &&
            value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var number)
                ? number
                : 0;

        private static double Decimal(JsonElement element, string property) =>
            element.TryGetProperty(property, out var value) &&
            value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out var number)
                ? number
                : 0;
    }
}
