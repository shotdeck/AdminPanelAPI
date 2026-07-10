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

            var updates = new Dictionary<long, (string? spotifyUrl, string? streamingUrl)>();
            foreach (var song in songs)
            {
                if (!force && !string.IsNullOrWhiteSpace(song.SpotifyUrl) && !string.IsNullOrWhiteSpace(song.StreamingUrl))
                {
                    result.Skipped++;
                    continue;
                }

                var spotifyUrl = await SearchSpotifyAsync(song.Title, song.Artist, token, cancellationToken);
                if (spotifyUrl == null)
                {
                    result.Unmatched++;
                    continue;
                }

                result.ResolvedSpotify++;
                string? universal = await ResolveUniversalAsync(spotifyUrl, cancellationToken);
                if (universal != null)
                    result.ResolvedUniversal++;

                updates[song.SongId] = (spotifyUrl, universal);
                result.Linked.Add(new StreamingLinkedTrack
                {
                    SongId = song.SongId,
                    Title = song.Title,
                    Artist = song.Artist,
                    SpotifyUrl = spotifyUrl,
                    StreamingUrl = universal
                });
            }

            await _repository.SetSongLinksAsync(updates, cancellationToken);
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
                using var req = new HttpRequestMessage(HttpMethod.Get, url);
                req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
                using var resp = await _http.SendAsync(req, cancellationToken);
                if (!resp.IsSuccessStatusCode)
                {
                    _logger.LogDebug("Spotify search failed ({Status}) for {Query}", (int)resp.StatusCode, query);
                    return null;
                }

                var json = await resp.Content.ReadAsStringAsync(cancellationToken);
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
        /// Ask Odesli / song.link for a universal all-services page for a Spotify
        /// track. Best-effort: returns null (and never throws) on any failure so a
        /// missing universal link never blocks storing the Spotify link.
        /// </summary>
        private async Task<string?> ResolveUniversalAsync(string spotifyUrl, CancellationToken cancellationToken)
        {
            var url = "https://api.song.link/v1-alpha.1/links?userCountry=US&url=" + Uri.EscapeDataString(spotifyUrl);
            try
            {
                using var resp = await _http.GetAsync(url, cancellationToken);
                if (!resp.IsSuccessStatusCode)
                    return null;
                var json = await resp.Content.ReadAsStringAsync(cancellationToken);
                using var doc = JsonDocument.Parse(json);
                return doc.RootElement.TryGetProperty("pageUrl", out var p) ? p.GetString() : null;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Odesli lookup failed for {Url}", spotifyUrl);
                return null;
            }
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
