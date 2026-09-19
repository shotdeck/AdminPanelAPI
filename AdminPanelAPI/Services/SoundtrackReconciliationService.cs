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

        // MusicBrainz asks anonymous clients to stay under ~1 request/second.
        private DateTimeOffset _mbNextAllowed = DateTimeOffset.MinValue;
        private static readonly TimeSpan MusicBrainzSpacing = TimeSpan.FromMilliseconds(1100);
        // Hard per-call timeout so one stalled MusicBrainz response can't block.
        private static readonly TimeSpan MusicBrainzCallTimeout = TimeSpan.FromSeconds(8);
        // Cap total time spent on the MusicBrainz cross-check so a manual
        // reconcile of a large movie can't hang on rate-limited requests.
        private static readonly TimeSpan MusicBrainzBudget = TimeSpan.FromSeconds(120);

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
                _http.DefaultRequestHeaders.Add(
                    "User-Agent",
                    "ShotDeckMusicReconciliation/1.0 (https://www.shotdeck.com; music-id@shotdeck.com)");
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
            }

            // MusicBrainz cross-check. Wikipedia tracklists are incomplete, so a
            // real needle-drop can still read "unverified" (e.g. "Psycho Boy Jack"
            // on the Fight Club score). MusicBrainz's authoritative "appears on
            // releases" data confirms it: if the recording is on a Soundtrack
            // release-group whose title matches the film, mark it confirmed.
            // Only checked for tracks Wikipedia didn't already confirm, and time-
            // boxed so a manual reconcile can't hang on MusicBrainz rate limits.
            var movieTitleTokens = Tokens(movie.Title);
            var mbConfirmedSongIds = new HashSet<long>();
            if (movieTitleTokens.Count > 0)
            {
                // A single linked token bounds the whole cross-check: once the
                // budget elapses (or the request is aborted) every in-flight and
                // subsequent MusicBrainz call cancels promptly, so reconcile can
                // never hang on a stalled/rate-limited MusicBrainz response.
                using var mbCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                mbCts.CancelAfter(MusicBrainzBudget);
                foreach (var song in songs)
                {
                    if (confidenceBySongId[song.SongId] == "confirmed") continue;
                    if (mbCts.IsCancellationRequested) break;
                    if (await MusicBrainzConfirmsSoundtrackAsync(song, movieTitleTokens, mbCts.Token))
                    {
                        confidenceBySongId[song.SongId] = "confirmed";
                        mbConfirmedSongIds.Add(song.SongId);
                    }
                }
            }

            foreach (var song in songs)
            {
                var confidence = confidenceBySongId[song.SongId];
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

            // Only persist a full pass when we actually resolved a soundtrack. If
            // Wikipedia was unreachable/rate-limited (no pairs), skip the full
            // write so we never clobber existing good tags with a batch of
            // "unverified" — but still persist any tracks MusicBrainz upgraded to
            // confirmed, since those are authoritative regardless of Wikipedia.
            if (result.SoundtrackFound)
            {
                await _repository.SetSongConfidenceAsync(movieId, confidenceBySongId, cancellationToken);

                // Persist the resolved Wikipedia soundtrack article so the
                // movie-level soundtrack card can link to it (COALESCE upsert,
                // so it never clobbers the Spotify album fields).
                if (!string.IsNullOrWhiteSpace(article))
                    await _repository.UpsertMovieSoundtrackAsync(new MovieSoundtrack
                    {
                        MovieId = movieId,
                        WikipediaUrl = "https://en.wikipedia.org/wiki/"
                            + Uri.EscapeDataString(article.Replace(' ', '_'))
                    }, cancellationToken);
            }
            else if (mbConfirmedSongIds.Count > 0)
            {
                var mbOnly = mbConfirmedSongIds.ToDictionary(id => id, _ => "confirmed");
                await _repository.SetSongConfidenceAsync(movieId, mbOnly, cancellationToken);
            }

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
            // A film can have both a songs "soundtrack" album and a separate
            // "score" album (e.g. American Beauty). Merge the track lists from
            // every dedicated album page that resolves, rather than stopping at
            // the first, so score cues get reconciled too.
            var albumCandidates = new List<string>
            {
                $"{movieTitle} (soundtrack)",
                $"{movieTitle} (score)",
                $"{movieTitle} (film score)",
                $"{movieTitle}: Original Motion Picture Soundtrack",
                $"{movieTitle} (Original Motion Picture Soundtrack)",
                $"{movieTitle} (Original Motion Picture Score)",
                $"Music of {movieTitle}",
                $"Music of the {movieTitle} franchise"
            };

            var merged = new List<(string title, string artist)>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            string? firstArticle = null;

            void Merge(string candidate, List<(string title, string artist)> found)
            {
                foreach (var p in found)
                {
                    if (seen.Add($"{p.title}|{p.artist}"))
                        merged.Add(p);
                }
                firstArticle ??= candidate;
            }

            foreach (var candidate in albumCandidates)
            {
                var wikitext = await FetchWikitextAsync(candidate, cancellationToken);
                if (string.IsNullOrEmpty(wikitext)) continue;
                var pairs = ExtractPairs(wikitext, movieTitle);
                if (pairs.Count > 0)
                    Merge(candidate, pairs);
            }

            if (merged.Count > 0)
                return (firstArticle, merged);

            // Search fallback only if none of the dedicated album pages resolved
            // a tracklist — keeps request volume (and rate-limit risk) down.
            foreach (var extra in await SearchCandidatesAsync(movieTitle, cancellationToken))
            {
                if (albumCandidates.Contains(extra)) continue;
                var wikitext = await FetchWikitextAsync(extra, cancellationToken);
                if (string.IsNullOrEmpty(wikitext)) continue;
                var pairs = ExtractPairs(wikitext, movieTitle);
                if (pairs.Count > 0)
                    return (extra, pairs);
            }

            // Last resort: the film's own page (often lists needle-drops).
            var filmPage = year.HasValue ? $"{movieTitle} ({year} film)" : $"{movieTitle} (film)";
            var filmText = await FetchWikitextAsync(filmPage, cancellationToken);
            if (!string.IsNullOrEmpty(filmText))
            {
                var pairs = ExtractPairs(filmText, movieTitle);
                if (pairs.Count > 0)
                    return (filmPage, pairs);
            }

            return (null, new List<(string, string)>());
        }

        private async Task<List<string>> SearchCandidatesAsync(string movieTitle, CancellationToken cancellationToken)
        {
            var url = "https://en.wikipedia.org/w/api.php?action=query&list=search&srsearch="
                + Uri.EscapeDataString($"{movieTitle} soundtrack") + "&srlimit=5&format=json";
            try
            {
                var json = await GetStringWithRetryAsync(url, cancellationToken);
                if (json == null) return new List<string>();
                using var doc = JsonDocument.Parse(json);
                var hits = doc.RootElement.GetProperty("query").GetProperty("search");
                var titles = new List<string>();
                foreach (var h in hits.EnumerateArray())
                    titles.Add(h.GetProperty("title").GetString() ?? "");
                // Only trust search hits that actually reference this film, so a
                // vague search doesn't reconcile against an unrelated article
                // (e.g. "Bridesmaids" -> "My Best Friend's Wedding").
                var movieLower = movieTitle.ToLowerInvariant();
                return titles
                    .Where(t => t.Length > 0 && t.ToLowerInvariant().Contains(movieLower))
                    .ToList();
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
                var json = await GetStringWithRetryAsync(url, cancellationToken);
                if (json == null) return null;
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
        /// GET a URL as a string, retrying on transient failures and HTTP 429
        /// (Wikipedia rate limit) with exponential backoff. Returns null if all
        /// attempts fail so callers can treat it as "unresolved" rather than throw.
        /// </summary>
        private async Task<string?> GetStringWithRetryAsync(string url, CancellationToken cancellationToken)
        {
            const int maxAttempts = 4;
            for (var attempt = 1; attempt <= maxAttempts; attempt++)
            {
                try
                {
                    using var resp = await _http.GetAsync(url, cancellationToken);
                    if (resp.StatusCode == (System.Net.HttpStatusCode)429
                        || (int)resp.StatusCode >= 500)
                    {
                        if (attempt < maxAttempts)
                        {
                            var wait = resp.Headers.RetryAfter?.Delta
                                ?? TimeSpan.FromMilliseconds(500 * attempt * attempt);
                            await Task.Delay(wait, cancellationToken);
                            continue;
                        }
                        return null;
                    }
                    resp.EnsureSuccessStatusCode();
                    return await resp.Content.ReadAsStringAsync(cancellationToken);
                }
                catch (Exception ex) when (attempt < maxAttempts && ex is not OperationCanceledException)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(500 * attempt * attempt), cancellationToken);
                }
            }
            return null;
        }

        // ---- MusicBrainz soundtrack cross-check ------------------------------

        /// <summary>
        /// True when the identified recording appears on a Soundtrack
        /// release-group whose title matches this film — MusicBrainz's
        /// authoritative confirmation that the needle-drop is a real soundtrack
        /// track, even when Wikipedia's tracklist omitted it.
        /// </summary>
        private async Task<bool> MusicBrainzConfirmsSoundtrackAsync(
            MovieSongRow song, HashSet<string> movieTitleTokens, CancellationToken cancellationToken)
        {
            try
            {
                var recordingId = await ResolveRecordingIdAsync(song, cancellationToken);
                if (recordingId == null) return false;

                var json = await MbGetAsync(
                    $"https://musicbrainz.org/ws/2/recording/{recordingId}?inc=releases+release-groups&fmt=json",
                    cancellationToken);
                if (json == null) return false;

                using var doc = JsonDocument.Parse(json);
                if (!doc.RootElement.TryGetProperty("releases", out var releases) ||
                    releases.ValueKind != JsonValueKind.Array)
                    return false;

                foreach (var rel in releases.EnumerateArray())
                {
                    var relTitle = rel.TryGetProperty("title", out var rt) ? rt.GetString() : null;
                    string? rgTitle = null;
                    var isSoundtrack = false;
                    if (rel.TryGetProperty("release-group", out var rg))
                    {
                        if (rg.TryGetProperty("title", out var gt)) rgTitle = gt.GetString();
                        if (rg.TryGetProperty("primary-type", out var pt) &&
                            string.Equals(pt.GetString(), "Soundtrack", StringComparison.OrdinalIgnoreCase))
                            isSoundtrack = true;
                        if (rg.TryGetProperty("secondary-types", out var st) &&
                            st.ValueKind == JsonValueKind.Array)
                            foreach (var s in st.EnumerateArray())
                                if (string.Equals(s.GetString(), "Soundtrack", StringComparison.OrdinalIgnoreCase))
                                    isSoundtrack = true;
                    }
                    if (!isSoundtrack) continue;

                    if (TitleMatchesMovie(Tokens(relTitle), movieTitleTokens) ||
                        TitleMatchesMovie(Tokens(rgTitle), movieTitleTokens))
                        return true;
                }
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "MusicBrainz soundtrack check failed for song {SongId}", song.SongId);
                return false;
            }
        }

        /// <summary>Resolve a MusicBrainz recording id by ISRC, else title+artist.</summary>
        private async Task<string?> ResolveRecordingIdAsync(MovieSongRow song, CancellationToken cancellationToken)
        {
            if (!string.IsNullOrWhiteSpace(song.Isrc))
            {
                var json = await MbGetAsync(
                    $"https://musicbrainz.org/ws/2/isrc/{Uri.EscapeDataString(song.Isrc)}?inc=recordings&fmt=json",
                    cancellationToken);
                var id = FirstRecordingId(json);
                if (id != null) return id;
            }

            if (!string.IsNullOrWhiteSpace(song.Title))
            {
                var query = $"recording:\"{song.Title}\"";
                if (!string.IsNullOrWhiteSpace(song.Artist))
                    query += $" AND artist:\"{song.Artist}\"";
                var json = await MbGetAsync(
                    $"https://musicbrainz.org/ws/2/recording?query={Uri.EscapeDataString(query)}&fmt=json&limit=1",
                    cancellationToken);
                return FirstRecordingId(json);
            }

            return null;
        }

        private static string? FirstRecordingId(string? json)
        {
            if (json == null) return null;
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.TryGetProperty("recordings", out var recs) &&
                recs.ValueKind == JsonValueKind.Array && recs.GetArrayLength() > 0 &&
                recs[0].TryGetProperty("id", out var id))
                return id.GetString();
            return null;
        }

        /// <summary>
        /// GET honoring MusicBrainz's ~1 req/sec rate limit, with a hard per-call
        /// timeout so one stalled response can't blow the cross-check budget.
        /// A single failed attempt just returns null (skip the track) rather than
        /// retrying with long backoff, which would compound the rate limiting.
        /// </summary>
        private async Task<string?> MbGetAsync(string url, CancellationToken cancellationToken)
        {
            try
            {
                var now = DateTimeOffset.UtcNow;
                if (_mbNextAllowed > now)
                    await Task.Delay(_mbNextAllowed - now, cancellationToken);
                _mbNextAllowed = DateTimeOffset.UtcNow.Add(MusicBrainzSpacing);

                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(MusicBrainzCallTimeout);
                using var resp = await _http.GetAsync(url, cts.Token);
                if (!resp.IsSuccessStatusCode) return null;
                return await resp.Content.ReadAsStringAsync(cts.Token);
            }
            catch (Exception)
            {
                return null;
            }
        }

        // A soundtrack release/group belongs to this film only if every
        // distinctive word of the movie title is present in its title (e.g.
        // "Fight Club" ⊆ "Fight Club"); guards against unrelated compilations.
        private static bool TitleMatchesMovie(HashSet<string> titleTokens, HashSet<string> movieTitleTokens)
        {
            if (movieTitleTokens.Count == 0 || titleTokens.Count == 0) return false;
            return movieTitleTokens.All(titleTokens.Contains);
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

                // Score albums list only titles (all by one composer), so their
                // per-track "extra" performer is empty. Fall back to the album's
                // infobox artist for those, so e.g. every Hildur Guðnadóttir cue
                // on the Joker score confirms. Skipped for "various artists"
                // compilations, whose per-track extras are authoritative.
                var albumArtist = "";
                var aim = Regex.Match(wikitext, @"\|\s*artist\s*=\s*([^\n]+)");
                if (aim.Success)
                {
                    var aa = Clean(aim.Groups[1].Value);
                    if (!aa.ToLowerInvariant().Contains("various"))
                        albumArtist = aa;
                }

                var movieLower = movieTitle.ToLowerInvariant();
                foreach (var (k, title) in titles)
                {
                    var artist = extras.TryGetValue(k, out var a) && !string.IsNullOrWhiteSpace(a)
                        ? a
                        : albumArtist;
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
