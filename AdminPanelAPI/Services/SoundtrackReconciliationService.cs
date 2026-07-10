using System.Text.Json;
using System.Text.RegularExpressions;
using AdminPanelAPI.Models;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Cross-checks the tracks we identified in a movie against the film's known
    /// soundtrack (parsed from Wikipedia) so real needle-drops can be confirmed
    /// and UGC junk flagged. The decisive signal is artist agreement: junk
    /// uploads share a movie's song titles ("Back to the Future") but never its
    /// performers, so a title-only match is only "review", not "confirmed".
    /// </summary>
    public class SoundtrackReconciliationService : ISoundtrackReconciliationService
    {
        private readonly IMusicIdentificationJobRepository _repository;
        private readonly HttpClient _http;
        private readonly ILogger<SoundtrackReconciliationService> _logger;

        private static readonly HashSet<string> StopWords = new(StringComparer.Ordinal)
        {
            "the", "a", "an", "of", "and", "feat", "featuring", "from", "version",
            "remaster", "remastered", "soundtrack", "theme", "skit", "live", "mix",
            "will", "you", "be", "mine", "with"
        };

        public SoundtrackReconciliationService(
            IMusicIdentificationJobRepository repository,
            HttpClient http,
            ILogger<SoundtrackReconciliationService> logger)
        {
            _repository = repository;
            _http = http;
            _logger = logger;
            if (!_http.DefaultRequestHeaders.Contains("User-Agent"))
                _http.DefaultRequestHeaders.Add("User-Agent", "shotdeck-music/1.0 (reconciliation)");
        }

        public async Task<ReconcileResult> ReconcileAsync(int movieId, CancellationToken cancellationToken)
        {
            var movie = await _repository.GetMovieInfoAsync(movieId, cancellationToken);
            var result = new ReconcileResult
            {
                MovieId = movieId,
                MovieTitle = movie?.Title,
                MovieYear = movie?.Year
            };

            if (movie?.Title == null)
                return result;

            var (article, pairs) = await FetchSoundtrackAsync(movie.Title, movie.Year, cancellationToken);
            result.SourceArticle = article;
            result.SoundtrackFound = pairs.Count > 0;
            result.AuthoritativeTrackCount = pairs.Count;

            // Performer token bag + per-song title-token sets from the soundtrack.
            var performerTokens = new HashSet<string>(StringComparer.Ordinal);
            var authTitleTokenSets = new List<HashSet<string>>();
            foreach (var (title, artist) in pairs)
            {
                foreach (var t in Tokens(artist)) performerTokens.Add(t);
                var tt = Tokens(title);
                if (tt.Count > 0) authTitleTokenSets.Add(tt);
            }

            var songs = await _repository.GetMovieSongRowsAsync(movieId, cancellationToken);
            var confidenceBySongId = new Dictionary<long, string>();
            var identifiedTitleTokenSets = new List<HashSet<string>>();

            foreach (var song in songs)
            {
                var titleTokens = Tokens(song.Title);
                var artistTokens = Tokens(song.Artist);
                identifiedTitleTokenSets.Add(titleTokens);

                var artistHit = pairs.Count > 0 && artistTokens.Overlaps(performerTokens);
                var titleOverlap = BestOverlap(titleTokens, authTitleTokenSets);

                string confidence;
                if (artistHit) confidence = "confirmed";
                else if (titleOverlap >= 0.6) confidence = "review";
                else confidence = "unverified";

                // With no soundtrack to compare against, don't assert anything.
                if (pairs.Count == 0) confidence = "unverified";

                confidenceBySongId[song.SongId] = confidence;
                var track = new ReconciledTrack
                {
                    SongId = song.SongId,
                    Title = song.Title,
                    Artist = song.Artist,
                    Confidence = confidence
                };
                switch (confidence)
                {
                    case "confirmed": result.Confirmed.Add(track); break;
                    case "review": result.Review.Add(track); break;
                    default: result.Unverified.Add(track); break;
                }
            }

            if (pairs.Count > 0)
            {
                foreach (var (title, artist) in pairs)
                {
                    var tt = Tokens(title);
                    if (tt.Count == 0) continue;
                    var found = identifiedTitleTokenSets.Any(it => it.Count > 0 && Overlap(tt, it) >= 0.6);
                    if (!found)
                        result.Missing.Add(string.IsNullOrWhiteSpace(artist) ? title : $"{artist} - {title}");
                }
            }

            await _repository.SetSongConfidenceAsync(movieId, confidenceBySongId, cancellationToken);

            result.ConfirmedCount = result.Confirmed.Count;
            result.ReviewCount = result.Review.Count;
            result.UnverifiedCount = result.Unverified.Count;
            return result;
        }

        /// <summary>
        /// Resolve the film's soundtrack Wikipedia article and return (article
        /// title, list of (song title, performer) pairs). Tries dedicated
        /// soundtrack pages first, then a search fallback.
        /// </summary>
        private async Task<(string? article, List<(string title, string artist)> pairs)> FetchSoundtrackAsync(
            string movieTitle, int? year, CancellationToken cancellationToken)
        {
            var candidates = new List<string>
            {
                $"{movieTitle} (soundtrack)",
                $"{movieTitle} (film score)",
                $"Music of {movieTitle}",
                $"Music of the {movieTitle} franchise",
                year.HasValue ? $"{movieTitle} ({year} film)" : $"{movieTitle} (film)"
            };

            foreach (var extra in await SearchCandidatesAsync(movieTitle, cancellationToken))
                if (!candidates.Contains(extra))
                    candidates.Add(extra);

            foreach (var candidate in candidates)
            {
                var wikitext = await FetchWikitextAsync(candidate, cancellationToken);
                if (string.IsNullOrEmpty(wikitext)) continue;
                var pairs = ExtractPairs(wikitext, movieTitle);
                if (pairs.Count > 0)
                    return (candidate, pairs);
            }

            return (null, new List<(string, string)>());
        }

        private async Task<List<string>> SearchCandidatesAsync(string movieTitle, CancellationToken cancellationToken)
        {
            var url = "https://en.wikipedia.org/w/api.php?action=query&list=search&srsearch="
                + Uri.EscapeDataString($"{movieTitle} soundtrack") + "&srlimit=5&format=json";
            try
            {
                var json = await _http.GetStringAsync(url, cancellationToken);
                using var doc = JsonDocument.Parse(json);
                var hits = doc.RootElement.GetProperty("query").GetProperty("search");
                var titles = new List<string>();
                foreach (var h in hits.EnumerateArray())
                    titles.Add(h.GetProperty("title").GetString() ?? "");
                return titles.Where(t => t.Length > 0).ToList();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Wikipedia search failed for {Movie}", movieTitle);
                return new List<string>();
            }
        }

        private async Task<string?> FetchWikitextAsync(string page, CancellationToken cancellationToken)
        {
            var url = "https://en.wikipedia.org/w/api.php?action=parse&page="
                + Uri.EscapeDataString(page) + "&prop=wikitext&format=json&redirects=1";
            try
            {
                var json = await _http.GetStringAsync(url, cancellationToken);
                using var doc = JsonDocument.Parse(json);
                if (!doc.RootElement.TryGetProperty("parse", out var parse))
                    return null;
                return parse.GetProperty("wikitext").GetProperty("*").GetString();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Wikipedia fetch failed for page {Page}", page);
                return null;
            }
        }

        /// <summary>
        /// Extract (song title, performer) pairs from the FIRST {{Track listing}}
        /// template (an article can carry several — sequels, a stage musical —
        /// whose titleN keys would otherwise collide and pollute performers),
        /// plus any prose  "Song" ... performed by [[Artist]].
        /// </summary>
        private static List<(string title, string artist)> ExtractPairs(string wikitext, string movieTitle)
        {
            var pairs = new List<(string, string)>();

            var idx = wikitext.IndexOf("{{track listing", StringComparison.OrdinalIgnoreCase);
            if (idx < 0)
                idx = wikitext.IndexOf("{{tracklist", StringComparison.OrdinalIgnoreCase);
            if (idx >= 0)
            {
                var block = wikitext.Substring(idx, Math.Min(3000, wikitext.Length - idx));
                var titles = new Dictionary<string, string>();
                var extras = new Dictionary<string, string>();
                foreach (Match m in Regex.Matches(block, @"\|\s*title(\d+)\s*=\s*([^\n]+)"))
                    titles[m.Groups[1].Value] = Clean(m.Groups[2].Value);
                foreach (Match m in Regex.Matches(block, @"\|\s*extra(\d+)\s*=\s*([^\n]+)"))
                    extras[m.Groups[1].Value] = Clean(m.Groups[2].Value);

                var movieLower = movieTitle.ToLowerInvariant();
                foreach (var (k, title) in titles)
                {
                    var artist = extras.TryGetValue(k, out var a) ? a : "";
                    // Skip score-only cues whose "performer" is really the film name.
                    if (!string.IsNullOrWhiteSpace(artist) &&
                        !artist.ToLowerInvariant().Contains(movieLower))
                        pairs.Add((title, artist));
                }
            }

            foreach (Match m in Regex.Matches(wikitext,
                "\"([^\"\n]{2,60})\"[^\n]{0,40}?performed by \\[\\[([^\\]|]+)"))
                pairs.Add((Clean(m.Groups[1].Value), Clean(m.Groups[2].Value)));

            return pairs;
        }

        private static string Clean(string s)
        {
            s = Regex.Replace(s, @"\{\{ref\|[^}]*\}\}", "");
            s = Regex.Replace(s, @"\{\{hlist\|([^}]*)\}\}", "$1");
            s = s.Replace("|", " ");
            s = Regex.Replace(s, @"[\[\]'""]", "");
            return s.Trim();
        }

        private static HashSet<string> Tokens(string? value)
        {
            var set = new HashSet<string>(StringComparer.Ordinal);
            if (string.IsNullOrWhiteSpace(value)) return set;
            var s = value.ToLowerInvariant();
            s = Regex.Replace(s, @"\(.*?\)", " ");
            s = Regex.Replace(s, @"\[.*?\]", " ");
            s = Regex.Replace(s, @"[^a-z0-9 ]", " ");
            foreach (var t in s.Split(' ', StringSplitOptions.RemoveEmptyEntries))
                if (t.Length > 1 && !StopWords.Contains(t))
                    set.Add(t);
            return set;
        }

        private static double Overlap(HashSet<string> a, HashSet<string> b)
        {
            if (a.Count == 0) return 0;
            var common = a.Count(t => b.Contains(t));
            return (double)common / a.Count;
        }

        private static double BestOverlap(HashSet<string> tokens, List<HashSet<string>> sets)
        {
            double best = 0;
            foreach (var s in sets)
                best = Math.Max(best, Overlap(tokens, s));
            return best;
        }
    }
}
