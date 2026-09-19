using System.Collections.Concurrent;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// A film's plot in prose, from Wikipedia. frl_movies holds no synopsis, and
    /// the key-image analysis scores a frame on how much it looks like the text
    /// it is given, so a title alone says almost nothing about the picture. The
    /// plot section names the film's places, people and action, which is what
    /// pushes shots of *this* film above generic pretty frames.
    ///
    /// Keyless and read-only: the search and extract endpoints of the public
    /// MediaWiki API, cached in memory for the life of the process because a
    /// film's plot does not change and the analysis window is reopened often.
    /// </summary>
    public interface IFilmSynopsisService
    {
        Task<string?> LookupAsync(string title, int? year, CancellationToken ct);
    }

    public sealed class FilmSynopsisService : IFilmSynopsisService
    {
        private const string ApiUrl = "https://en.wikipedia.org/w/api.php";

        /// <summary>
        /// Wikipedia asks for a descriptive agent with contact details rather
        /// than a default client string.
        /// </summary>
        private const string UserAgent =
            "ShotDeckAdminPanel/1.0 (https://shotdeck.com; support@shotdeck.com)";

        /// <summary>
        /// How much plot to keep. CLIP reads the first 77 tokens of any one
        /// string, so the analysis splits what it gets into chunks and scores
        /// against each; this bounds how many chunks that comes to.
        /// </summary>
        private const int MaxCharacters = 2000;

        /// <summary>How many search hits are worth opening.</summary>
        private const int Candidates = 5;

        private static readonly ConcurrentDictionary<string, string?> Cache = new();

        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<FilmSynopsisService> _logger;

        public FilmSynopsisService(
            IHttpClientFactory httpClientFactory,
            ILogger<FilmSynopsisService> logger)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;
        }

        public async Task<string?> LookupAsync(string title, int? year, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(title))
                return null;

            var cacheKey = $"{title.Trim().ToLowerInvariant()}|{year}";
            if (Cache.TryGetValue(cacheKey, out var cached))
                return cached;

            string? synopsis = null;
            try
            {
                synopsis = await FetchAsync(title.Trim(), year, ct);
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or JsonException)
            {
                // No synopsis is a worse prompt, not a failed analysis, so the
                // caller carries on with the title.
                _logger.LogWarning(ex, "Wikipedia lookup failed for {Title} ({Year}).", title, year);
                return null;
            }

            Cache[cacheKey] = synopsis;
            return synopsis;
        }

        private async Task<string?> FetchAsync(string title, int? year, CancellationToken ct)
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(15);
            client.DefaultRequestHeaders.UserAgent.ParseAdd(UserAgent);

            foreach (var page in await SearchAsync(client, title, year, ct))
            {
                var extract = await ExtractAsync(client, page, ct);
                if (extract is null || !IsFilm(extract, title))
                    continue;

                var plot = Section(extract, "Plot") ?? Section(extract, "Synopsis") ?? Lead(extract);
                if (!string.IsNullOrWhiteSpace(plot))
                    return Trim(plot);
            }

            return null;
        }

        /// <summary>Page titles that might be the film, best hit first.</summary>
        private static async Task<List<string>> SearchAsync(
            HttpClient client, string title, int? year, CancellationToken ct)
        {
            var search = year is null ? $"{title} film" : $"{title} {year} film";
            var url = ApiUrl +
                "?action=query&list=search&format=json&srlimit=" + Candidates +
                "&srsearch=" + Uri.EscapeDataString(search);

            using var document = JsonDocument.Parse(await client.GetStringAsync(url, ct));
            var pages = new List<string>();
            if (document.RootElement.TryGetProperty("query", out var query) &&
                query.TryGetProperty("search", out var hits))
            {
                foreach (var hit in hits.EnumerateArray())
                {
                    if (hit.TryGetProperty("title", out var found) &&
                        found.GetString() is { Length: > 0 } name)
                        pages.Add(name);
                }
            }

            return pages;
        }

        /// <summary>The whole article as plain text, headings included.</summary>
        private static async Task<string?> ExtractAsync(
            HttpClient client, string page, CancellationToken ct)
        {
            var url = ApiUrl +
                "?action=query&prop=extracts&explaintext=1&redirects=1&format=json&titles=" +
                Uri.EscapeDataString(page);

            using var document = JsonDocument.Parse(await client.GetStringAsync(url, ct));
            if (!document.RootElement.TryGetProperty("query", out var query) ||
                !query.TryGetProperty("pages", out var pages))
                return null;

            foreach (var entry in pages.EnumerateObject())
            {
                if (entry.Value.TryGetProperty("extract", out var extract) &&
                    extract.GetString() is { Length: > 0 } text)
                    return text;
            }

            return null;
        }

        /// <summary>
        /// Guard against the search landing on the novel, the song or an actor:
        /// a film article says so in its opening sentence.
        /// </summary>
        private static bool IsFilm(string extract, string title)
        {
            var lead = Lead(extract);
            return lead.Contains("film", StringComparison.OrdinalIgnoreCase) &&
                lead.Contains(title.Split(':')[0].Trim(), StringComparison.OrdinalIgnoreCase);
        }

        private static string Lead(string extract) =>
            extract.Split("\n==", 2)[0].Trim();

        /// <summary>The body of a named top-level section, if the article has one.</summary>
        private static string? Section(string extract, string name)
        {
            var heading = $"\n== {name} ==";
            var start = extract.IndexOf(heading, StringComparison.OrdinalIgnoreCase);
            if (start < 0)
                return null;

            var body = extract[(start + heading.Length)..];
            var end = body.IndexOf("\n== ", StringComparison.Ordinal);
            return (end < 0 ? body : body[..end]).Trim();
        }

        /// <summary>Whole sentences up to the character budget.</summary>
        private static string Trim(string text)
        {
            var flat = string.Join(" ", text.Split('\n', StringSplitOptions.RemoveEmptyEntries))
                .Replace("  ", " ")
                .Trim();
            if (flat.Length <= MaxCharacters)
                return flat;

            var cut = flat[..MaxCharacters];
            var lastStop = cut.LastIndexOf(". ", StringComparison.Ordinal);
            return lastStop > 0 ? cut[..(lastStop + 1)] : cut;
        }
    }
}
