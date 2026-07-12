using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using AdminPanelAPI.Models;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Fetches track enrichment on demand and caches it. Sources, all free and
    /// keyed off the ISRC / artist+title we already store:
    ///   - MusicBrainz: writers, composers, producers (with artist MBIDs so a
    ///     future "more tracks by this person" link can resolve by id), album,
    ///     release date, label.
    ///   - Wikipedia: a short human-readable description.
    ///   - Spotify: album, release date, 30s preview (reusing the existing
    ///     client-credentials token).
    /// </summary>
    public class TrackDetailsService : ITrackDetailsService
    {
        private readonly IMusicIdentificationJobRepository _repository;
        private readonly HttpClient _http;
        private readonly IConfiguration _configuration;
        private readonly ILogger<TrackDetailsService> _logger;

        private string? _accessToken;
        private DateTimeOffset _tokenExpiry = DateTimeOffset.MinValue;

        public TrackDetailsService(
            IMusicIdentificationJobRepository repository,
            HttpClient http,
            IConfiguration configuration,
            ILogger<TrackDetailsService> logger)
        {
            _repository = repository;
            _http = http;
            _configuration = configuration;
            _logger = logger;
            // MusicBrainz requires a descriptive, contactable User-Agent.
            if (!_http.DefaultRequestHeaders.Contains("User-Agent"))
                _http.DefaultRequestHeaders.Add("User-Agent", "shotdeck-music/1.0 (track-details)");
        }

        public async Task<TrackDetails?> GetOrFetchAsync(long songId, bool refresh, CancellationToken cancellationToken)
        {
            var song = await _repository.GetSongForDetailsAsync(songId, cancellationToken);
            if (song == null) return null;

            if (!refresh)
            {
                var cached = await _repository.GetTrackDetailsAsync(songId, cancellationToken);
                if (cached != null)
                {
                    cached.Title = song.Title;
                    cached.Artist = song.Artist;
                    cached.SpotifyUrl = song.SpotifyUrl;
                    return cached;
                }
            }

            var details = new TrackDetails
            {
                SongId = songId,
                Title = song.Title,
                Artist = song.Artist,
                SpotifyUrl = song.SpotifyUrl
            };

            await EnrichFromMusicBrainzAsync(details, song, cancellationToken);
            await EnrichFromWikipediaAsync(details, song, cancellationToken);
            await EnrichFromSpotifyAsync(details, song, cancellationToken);

            await _repository.UpsertTrackDetailsAsync(details, cancellationToken);
            return details;
        }

        // ---- MusicBrainz -----------------------------------------------------

        private async Task EnrichFromMusicBrainzAsync(TrackDetails details, MovieSongRow song, CancellationToken cancellationToken)
        {
            try
            {
                var recordingId = await ResolveRecordingIdAsync(song, cancellationToken);
                if (recordingId == null) return;

                details.MusicbrainzUrl = "https://musicbrainz.org/recording/" + recordingId;

                var json = await GetStringAsync(
                    $"https://musicbrainz.org/ws/2/recording/{recordingId}?inc=artist-rels+work-rels+releases&fmt=json",
                    cancellationToken);
                if (json == null) return;

                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Producers/engineers are artist-relations on the recording.
                string? workId = null;
                if (root.TryGetProperty("relations", out var rels) && rels.ValueKind == JsonValueKind.Array)
                {
                    foreach (var rel in rels.EnumerateArray())
                    {
                        var type = rel.TryGetProperty("type", out var t) ? t.GetString() : null;
                        if (type == "producer" && rel.TryGetProperty("artist", out var pa))
                            AddCredit(details.Producers, pa);
                        else if (type == "performance" && workId == null && rel.TryGetProperty("work", out var w)
                                 && w.TryGetProperty("id", out var wid))
                            workId = wid.GetString();
                    }
                }

                // Album + release date + label from the first release.
                if (root.TryGetProperty("releases", out var releases) &&
                    releases.ValueKind == JsonValueKind.Array && releases.GetArrayLength() > 0)
                {
                    var rel = releases[0];
                    if (string.IsNullOrWhiteSpace(details.Album) && rel.TryGetProperty("title", out var rt))
                        details.Album = rt.GetString();
                    if (string.IsNullOrWhiteSpace(details.ReleaseDate) && rel.TryGetProperty("date", out var rd))
                        details.ReleaseDate = rd.GetString();
                }

                // Writers/composers live on the work behind the recording.
                if (workId != null)
                    await EnrichFromWorkAsync(details, workId, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "MusicBrainz enrichment failed for song {SongId}", details.SongId);
            }
        }

        private async Task<string?> ResolveRecordingIdAsync(MovieSongRow song, CancellationToken cancellationToken)
        {
            if (!string.IsNullOrWhiteSpace(song.Isrc))
            {
                var json = await GetStringAsync(
                    $"https://musicbrainz.org/ws/2/isrc/{Uri.EscapeDataString(song.Isrc)}?inc=recordings&fmt=json",
                    cancellationToken);
                if (json != null)
                {
                    using var doc = JsonDocument.Parse(json);
                    if (doc.RootElement.TryGetProperty("recordings", out var recs) &&
                        recs.ValueKind == JsonValueKind.Array && recs.GetArrayLength() > 0 &&
                        recs[0].TryGetProperty("id", out var id))
                        return id.GetString();
                }
            }

            if (!string.IsNullOrWhiteSpace(song.Title))
            {
                var query = $"recording:\"{song.Title}\"";
                if (!string.IsNullOrWhiteSpace(song.Artist))
                    query += $" AND artist:\"{song.Artist}\"";
                var json = await GetStringAsync(
                    $"https://musicbrainz.org/ws/2/recording?query={Uri.EscapeDataString(query)}&fmt=json&limit=1",
                    cancellationToken);
                if (json != null)
                {
                    using var doc = JsonDocument.Parse(json);
                    if (doc.RootElement.TryGetProperty("recordings", out var recs) &&
                        recs.ValueKind == JsonValueKind.Array && recs.GetArrayLength() > 0 &&
                        recs[0].TryGetProperty("id", out var id))
                        return id.GetString();
                }
            }

            return null;
        }

        private async Task EnrichFromWorkAsync(TrackDetails details, string workId, CancellationToken cancellationToken)
        {
            var json = await GetStringAsync(
                $"https://musicbrainz.org/ws/2/work/{workId}?inc=artist-rels&fmt=json",
                cancellationToken);
            if (json == null) return;

            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("relations", out var rels) || rels.ValueKind != JsonValueKind.Array)
                return;

            foreach (var rel in rels.EnumerateArray())
            {
                var type = rel.TryGetProperty("type", out var t) ? t.GetString() : null;
                if (!rel.TryGetProperty("artist", out var artist)) continue;
                switch (type)
                {
                    case "composer":
                        AddCredit(details.Composers, artist);
                        break;
                    case "writer":
                    case "lyricist":
                    case "librettist":
                        AddCredit(details.Writers, artist);
                        break;
                    case "producer":
                        AddCredit(details.Producers, artist);
                        break;
                }
            }
        }

        private static void AddCredit(List<MusicCredit> list, JsonElement artist)
        {
            var name = artist.TryGetProperty("name", out var n) ? n.GetString() : null;
            if (string.IsNullOrWhiteSpace(name)) return;
            var mbid = artist.TryGetProperty("id", out var id) ? id.GetString() : null;
            if (list.Any(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase))) return;
            list.Add(new MusicCredit { Name = name, Mbid = mbid });
        }

        // ---- Wikipedia -------------------------------------------------------

        private async Task EnrichFromWikipediaAsync(TrackDetails details, MovieSongRow song, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(song.Title)) return;
            try
            {
                var srsearch = song.Title + (string.IsNullOrWhiteSpace(song.Artist) ? "" : " " + song.Artist) + " song";
                var searchJson = await GetStringAsync(
                    "https://en.wikipedia.org/w/api.php?action=query&list=search&format=json&srlimit=1&srsearch="
                    + Uri.EscapeDataString(srsearch),
                    cancellationToken);
                if (searchJson == null) return;

                string? pageTitle;
                using (var sdoc = JsonDocument.Parse(searchJson))
                {
                    var hits = sdoc.RootElement.GetProperty("query").GetProperty("search");
                    if (hits.GetArrayLength() == 0) return;
                    pageTitle = hits[0].GetProperty("title").GetString();
                }
                if (string.IsNullOrWhiteSpace(pageTitle)) return;

                var summaryJson = await GetStringAsync(
                    "https://en.wikipedia.org/api/rest_v1/page/summary/"
                    + Uri.EscapeDataString(pageTitle.Replace(' ', '_')),
                    cancellationToken);
                if (summaryJson == null) return;

                using var doc = JsonDocument.Parse(summaryJson);
                var root = doc.RootElement;
                var extract = root.TryGetProperty("extract", out var e) ? e.GetString() : null;
                if (string.IsNullOrWhiteSpace(extract)) return;

                details.Description = extract;
                details.DescriptionSource = "wikipedia";
                if (root.TryGetProperty("content_urls", out var cu) &&
                    cu.TryGetProperty("desktop", out var desk) &&
                    desk.TryGetProperty("page", out var pageUrl))
                    details.WikipediaUrl = pageUrl.GetString();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Wikipedia enrichment failed for song {SongId}", details.SongId);
            }
        }

        // ---- Spotify ---------------------------------------------------------

        private async Task EnrichFromSpotifyAsync(TrackDetails details, MovieSongRow song, CancellationToken cancellationToken)
        {
            var trackId = ExtractTrackId(song.SpotifyUrl);
            if (trackId == null) return;

            var clientId = _configuration["Spotify:ClientId"] ?? _configuration["SPOTIFY_CLIENT_ID"];
            var clientSecret = _configuration["Spotify:ClientSecret"] ?? _configuration["SPOTIFY_CLIENT_SECRET"];
            if (string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(clientSecret)) return;

            try
            {
                var token = await GetSpotifyTokenAsync(clientId, clientSecret, cancellationToken);
                if (token == null) return;

                using var req = new HttpRequestMessage(HttpMethod.Get, "https://api.spotify.com/v1/tracks/" + trackId);
                req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
                using var resp = await _http.SendAsync(req, cancellationToken);
                if (!resp.IsSuccessStatusCode) return;

                var body = await resp.Content.ReadAsStringAsync(cancellationToken);
                using var doc = JsonDocument.Parse(body);
                var root = doc.RootElement;

                if (root.TryGetProperty("preview_url", out var pv) && pv.ValueKind == JsonValueKind.String)
                    details.PreviewUrl = pv.GetString();

                if (root.TryGetProperty("album", out var album))
                {
                    if (album.TryGetProperty("name", out var an))
                        details.Album = an.GetString();
                    if (album.TryGetProperty("release_date", out var rd))
                        details.ReleaseDate = rd.GetString();
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Spotify enrichment failed for song {SongId}", details.SongId);
            }
        }

        private async Task<string?> GetSpotifyTokenAsync(string clientId, string clientSecret, CancellationToken cancellationToken)
        {
            if (_accessToken != null && DateTimeOffset.UtcNow < _tokenExpiry)
                return _accessToken;

            using var req = new HttpRequestMessage(HttpMethod.Post, "https://accounts.spotify.com/api/token");
            var basic = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{clientId}:{clientSecret}"));
            req.Headers.Authorization = new AuthenticationHeaderValue("Basic", basic);
            req.Content = new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["grant_type"] = "client_credentials"
            });

            using var resp = await _http.SendAsync(req, cancellationToken);
            if (!resp.IsSuccessStatusCode) return null;

            var body = await resp.Content.ReadAsStringAsync(cancellationToken);
            using var doc = JsonDocument.Parse(body);
            _accessToken = doc.RootElement.GetProperty("access_token").GetString();
            var expiresIn = doc.RootElement.TryGetProperty("expires_in", out var e) ? e.GetInt32() : 3600;
            _tokenExpiry = DateTimeOffset.UtcNow.AddSeconds(expiresIn - 60);
            return _accessToken;
        }

        private static string? ExtractTrackId(string? spotifyUrl)
        {
            if (string.IsNullOrWhiteSpace(spotifyUrl)) return null;
            var m = Regex.Match(spotifyUrl, @"track[/:]([A-Za-z0-9]+)");
            return m.Success ? m.Groups[1].Value : null;
        }

        // ---- HTTP helper -----------------------------------------------------

        private async Task<string?> GetStringAsync(string url, CancellationToken cancellationToken)
        {
            try
            {
                using var resp = await _http.GetAsync(url, cancellationToken);
                if (resp.StatusCode == HttpStatusCode.TooManyRequests || !resp.IsSuccessStatusCode)
                    return null;
                return await resp.Content.ReadAsStringAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "GET failed: {Url}", url);
                return null;
            }
        }
    }
}
