using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using AdminPanelAPI.Models;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Backfills streaming links onto identified tracks, independent of which
    /// engine matched them. For each (title, artist) it searches Spotify, keeps
    /// the top hit only when both artist and title clear a similarity guard (so
    /// UGC junk doesn't get a bogus link), then derives a universal all-services
    /// link from that Spotify URL via Odesli / song.link. Non-destructive:
    /// only fills links.
    /// </summary>
    public class StreamingLinkService : IStreamingLinkService
    {
        private readonly IMusicIdentificationJobRepository _repository;
        private readonly HttpClient _http;
        private readonly IConfiguration _configuration;
        private readonly ILogger<StreamingLinkService> _logger;

        private string? _accessToken;
        private DateTimeOffset _tokenExpiry = DateTimeOffset.MinValue;

        private const double ArtistThreshold = 0.5;
        private const double TitleThreshold = 0.5;

        private static readonly HashSet<string> StopWords = new(StringComparer.Ordinal)
        {
            "the", "a", "an", "of", "and", "feat", "featuring", "from", "version",
            "remaster", "remastered", "soundtrack", "theme", "original", "mix", "with"
        };

        public StreamingLinkService(
            IMusicIdentificationJobRepository repository,
            HttpClient http,
            IConfiguration configuration,
            ILogger<StreamingLinkService> logger)
        {
            _repository = repository;
            _http = http;
            _configuration = configuration;
            _logger = logger;
            if (!_http.DefaultRequestHeaders.Contains("User-Agent"))
                _http.DefaultRequestHeaders.Add("User-Agent", "shotdeck-music/1.0 (streaming-links)");
        }

        public async Task<StreamingLinkResult> BackfillAsync(int movieId, bool force, CancellationToken cancellationToken)
        {
            var movie = await _repository.GetMovieInfoAsync(movieId, cancellationToken);
            var result = new StreamingLinkResult { MovieId = movieId, MovieTitle = movie?.Title };

            var clientId = _configuration["Spotify:ClientId"] ?? _configuration["SPOTIFY_CLIENT_ID"];
            var clientSecret = _configuration["Spotify:ClientSecret"] ?? _configuration["SPOTIFY_CLIENT_SECRET"];
            if (string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(clientSecret))
            {
                _logger.LogWarning("Spotify credentials not configured; streaming-link backfill skipped.");
                result.CredentialsConfigured = false;
                return result;
            }

            var token = await GetTokenAsync(clientId, clientSecret, cancellationToken);
            if (token == null)
            {
                result.CredentialsConfigured = false;
                return result;
            }

            var songs = await _repository.GetMovieSongRowsWithLinksAsync(movieId, cancellationToken);
            result.TotalTracks = songs.Count;

            var odesliKey = _configuration["Odesli:ApiKey"] ?? _configuration["ODESLI_API_KEY"];

            // Flush links to the DB in batches so partial progress survives even
            // if the request is cut short. Re-running is idempotent.
            var pending = new Dictionary<long, (string? spotifyUrl, string? streamingUrl)>();
            async Task FlushAsync()
            {
                if (pending.Count == 0) return;
                await _repository.SetSongLinksAsync(pending, cancellationToken);
                pending.Clear();
            }

            // Phase 1 — resolve and persist Spotify links first. This is fast and
            // must never be blocked by Odesli, which is slow/flaky on its free
            // tier: a track with a Spotify link is the primary deliverable.
            var needUniversal = new List<(long songId, string spotifyUrl, string? title, string? artist)>();
            foreach (var song in songs)
            {
                var haveSpotify = !string.IsNullOrWhiteSpace(song.SpotifyUrl);
                var haveUniversal = !string.IsNullOrWhiteSpace(song.StreamingUrl);
                if (!force && haveSpotify && haveUniversal)
                {
                    result.Skipped++;
                    continue;
                }

                // Reuse the stored Spotify link when present; only search when we
                // don't have one (or force).
                var spotifyUrl = (!force && haveSpotify)
                    ? song.SpotifyUrl
                    : await SearchSpotifyAsync(song.Title, song.Artist, token, cancellationToken);
                if (string.IsNullOrWhiteSpace(spotifyUrl))
                {
                    result.Unmatched++;
                    continue;
                }

                result.ResolvedSpotify++;
                if (!haveSpotify)
                {
                    pending[song.SongId] = (spotifyUrl, null);
                    result.Linked.Add(new StreamingLinkedTrack
                    {
                        SongId = song.SongId,
                        Title = song.Title,
                        Artist = song.Artist,
                        SpotifyUrl = spotifyUrl,
                        StreamingUrl = null
                    });
                    if (pending.Count >= 5)
                        await FlushAsync();
                }

                if (force || !haveUniversal)
                    needUniversal.Add((song.SongId, spotifyUrl!, song.Title, song.Artist));
            }
            await FlushAsync();

            // Phase 2 — best-effort universal links via Odesli, under a wall-clock
            // budget so the request returns cleanly (200) instead of hitting the
            // gateway timeout. Whatever doesn't resolve is picked up on the next
            // (idempotent) run. A configured ODESLI_API_KEY removes the rate limit.
            var deadline = DateTime.UtcNow.AddSeconds(120);
            foreach (var item in needUniversal)
            {
                if (DateTime.UtcNow >= deadline)
                    break;

                var universal = await ResolveUniversalAsync(item.spotifyUrl, odesliKey, cancellationToken);
                if (string.IsNullOrWhiteSpace(universal))
                    continue;

                result.ResolvedUniversal++;
                pending[item.songId] = (item.spotifyUrl, universal);
                if (pending.Count >= 5)
                    await FlushAsync();
            }
            await FlushAsync();
            return result;
        }

        private async Task<string?> GetTokenAsync(string clientId, string clientSecret, CancellationToken cancellationToken)
        {
            if (_accessToken != null && DateTimeOffset.UtcNow < _tokenExpiry)
                return _accessToken;

            try
            {
                using var req = new HttpRequestMessage(HttpMethod.Post, "https://accounts.spotify.com/api/token");
                var basic = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{clientId}:{clientSecret}"));
                req.Headers.Authorization = new AuthenticationHeaderValue("Basic", basic);
                req.Content = new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    ["grant_type"] = "client_credentials"
                });

                using var resp = await _http.SendAsync(req, cancellationToken);
                var body = await resp.Content.ReadAsStringAsync(cancellationToken);
                if (!resp.IsSuccessStatusCode)
                {
                    _logger.LogWarning("Spotify token request failed ({Status}): {Body}", (int)resp.StatusCode, body);
                    return null;
                }

                using var doc = JsonDocument.Parse(body);
                _accessToken = doc.RootElement.GetProperty("access_token").GetString();
                var expiresIn = doc.RootElement.TryGetProperty("expires_in", out var e) ? e.GetInt32() : 3600;
                _tokenExpiry = DateTimeOffset.UtcNow.AddSeconds(expiresIn - 60);
                return _accessToken;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Spotify token request threw.");
                return null;
            }
        }

        /// <summary>
        /// Search Spotify by "artist title" and return the first hit whose artist
        /// and title both clear the similarity guard, else null.
        /// </summary>
        private async Task<string?> SearchSpotifyAsync(string? title, string? artist, string token, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(title))
                return null;

            var query = string.IsNullOrWhiteSpace(artist) ? title : $"{artist} {title}";
            var url = "https://api.spotify.com/v1/search?type=track&limit=5&q=" + Uri.EscapeDataString(query);

            try
            {
                var json = await GetWithRetryAsync(url, token, cancellationToken);
                if (json == null)
                {
                    _logger.LogDebug("Spotify search failed for {Query}", query);
                    return null;
                }

                using var doc = JsonDocument.Parse(json);
                if (!doc.RootElement.TryGetProperty("tracks", out var tracks) ||
                    !tracks.TryGetProperty("items", out var items))
                    return null;

                var titleTokens = Tokens(title);
                var artistTokens = Tokens(artist);

                foreach (var item in items.EnumerateArray())
                {
                    var candTitle = item.TryGetProperty("name", out var n) ? n.GetString() : null;
                    var candArtists = new List<string>();
                    if (item.TryGetProperty("artists", out var arts))
                        foreach (var a in arts.EnumerateArray())
                            if (a.TryGetProperty("name", out var an) && an.GetString() is { } s)
                                candArtists.Add(s);

                    var candArtistTokens = Tokens(string.Join(" ", candArtists));
                    var candTitleTokens = Tokens(candTitle);

                    var titleOk = Overlap(titleTokens, candTitleTokens) >= TitleThreshold;
                    // When we have no artist at all, don't gate on it.
                    var artistOk = artistTokens.Count == 0 || Overlap(artistTokens, candArtistTokens) >= ArtistThreshold;

                    if (titleOk && artistOk &&
                        item.TryGetProperty("external_urls", out var ext) &&
                        ext.TryGetProperty("spotify", out var sp))
                        return sp.GetString();
                }

                return null;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Spotify search threw for {Query}", query);
                return null;
            }
        }

        /// <summary>
        /// GET a Spotify API endpoint, retrying on 429/5xx honoring Retry-After
        /// (capped) so a burst backfill doesn't drop tracks when the search API
        /// rate-limits. Returns the body on success, else null.
        /// </summary>
        private async Task<string?> GetWithRetryAsync(string url, string token, CancellationToken cancellationToken)
        {
            for (var attempt = 0; attempt < 4; attempt++)
            {
                using var req = new HttpRequestMessage(HttpMethod.Get, url);
                req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeout.CancelAfter(TimeSpan.FromSeconds(10));
                using var resp = await _http.SendAsync(req, timeout.Token);
                if (resp.IsSuccessStatusCode)
                    return await resp.Content.ReadAsStringAsync(cancellationToken);

                var status = (int)resp.StatusCode;
                var retryable = status == 429 || status >= 500;
                if (!retryable || attempt == 3)
                    return null;

                var delay = resp.Headers.RetryAfter?.Delta
                    ?? TimeSpan.FromSeconds(Math.Min(8, Math.Pow(2, attempt + 1)));
                await Task.Delay(delay, cancellationToken);
            }
            return null;
        }

        /// <summary>
        /// Ask Odesli / song.link for a universal all-services page for a Spotify
        /// track. Best-effort: returns null (and never throws) on any failure so a
        /// missing universal link never blocks storing the Spotify link.
        /// </summary>
        private async Task<string?> ResolveUniversalAsync(string spotifyUrl, string? apiKey, CancellationToken cancellationToken)
        {
            var url = "https://api.song.link/v1-alpha.1/links?userCountry=US&url=" + Uri.EscapeDataString(spotifyUrl);
            if (!string.IsNullOrWhiteSpace(apiKey))
                url += "&key=" + Uri.EscapeDataString(apiKey);

            // Odesli's free tier is aggressively rate-limited (~10 req/min); a
            // burst backfill hits 429 constantly. Retry on 429/5xx but cap the
            // wait so a single request stays well under the gateway timeout and
            // keeps making progress — links that don't resolve this pass fill in
            // on the next (idempotent) run. A configured API key lifts the limit,
            // so retries are rarely needed then.
            for (var attempt = 0; attempt < 3; attempt++)
            {
                try
                {
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    timeout.CancelAfter(TimeSpan.FromSeconds(8));
                    using var resp = await _http.GetAsync(url, timeout.Token);
                    if (resp.IsSuccessStatusCode)
                    {
                        var json = await resp.Content.ReadAsStringAsync(cancellationToken);
                        using var doc = JsonDocument.Parse(json);
                        return doc.RootElement.TryGetProperty("pageUrl", out var p) ? p.GetString() : null;
                    }

                    var status = (int)resp.StatusCode;
                    var retryable = status == 429 || status >= 500;
                    if (!retryable || attempt == 2)
                        return null;

                    var suggested = resp.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(attempt + 1);
                    var delay = suggested > TimeSpan.FromSeconds(3) ? TimeSpan.FromSeconds(3) : suggested;
                    await Task.Delay(delay, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    // Includes our per-call timeout — treat as a miss, never fatal.
                    _logger.LogDebug(ex, "Odesli lookup failed for {Url}", spotifyUrl);
                    return null;
                }
            }
            return null;
        }

        private static HashSet<string> Tokens(string? value)
        {
            var set = new HashSet<string>(StringComparer.Ordinal);
            if (string.IsNullOrWhiteSpace(value)) return set;
            var s = value.ToLowerInvariant();
            s = Regex.Replace(s, @"\(.*?\)", " ");
            s = Regex.Replace(s, @"\[.*?\]", " ");
            s = s.Replace("&", " and ");
            s = Regex.Replace(s, @"[^a-z0-9 ]", " ");
            foreach (var t in s.Split(' ', StringSplitOptions.RemoveEmptyEntries))
                if (t.Length > 1 && !StopWords.Contains(t))
                    set.Add(t);
            return set;
        }

        /// <summary>Overlap relative to the smaller token set (0..1).</summary>
        private static double Overlap(HashSet<string> a, HashSet<string> b)
        {
            if (a.Count == 0 || b.Count == 0) return 0;
            var common = a.Count(t => b.Contains(t));
            return (double)common / Math.Min(a.Count, b.Count);
        }
    }
}
