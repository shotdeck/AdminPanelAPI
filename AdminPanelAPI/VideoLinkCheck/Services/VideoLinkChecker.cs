using System.Net;
using System.Text.RegularExpressions;
using AdminPanelAPI.VideoLinkCheck.Models;
using Microsoft.Extensions.Options;

namespace AdminPanelAPI.VideoLinkCheck.Services;

/// <summary>Checks a YouTube/Vimeo link through the platform's oEmbed endpoint.</summary>
public sealed class VideoLinkChecker(IHttpClientFactory httpFactory, IOptionsMonitor<VideoLinkCheckOptions> options)
{
    public enum Platform { Youtube, Vimeo, Unknown }

    private sealed class TransientHttpException(string message) : Exception(message);

    private static readonly TimeSpan[] RetryDelays = { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3) };

    private static readonly Regex BareYoutubeId = new(@"^[a-zA-Z0-9_-]{11}$", RegexOptions.Compiled);
    private static readonly Regex YoutubeQueryId = new(@"[?&]v=([a-zA-Z0-9_-]{11})", RegexOptions.Compiled);
    private static readonly Regex YoutubePathId = new(
        @"(?:youtu\.be/|/(?:embed|shorts|live|v)/)([a-zA-Z0-9_-]{11})", RegexOptions.Compiled | RegexOptions.IgnoreCase);
    private static readonly Regex VimeoIdSegment = new(@"^\d{6,12}$", RegexOptions.Compiled);
    private static readonly HashSet<string> VimeoContainers = new(StringComparer.OrdinalIgnoreCase)
        { "showcase", "album", "groups", "channels" };
    private static readonly Regex VimeoHash = new(@"^[0-9a-f]{6,}$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
    private static readonly Regex VimeoHashQuery = new(@"[?&]h=([0-9a-f]{6,})", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    public static (Platform Platform, string? CanonicalUrl, string? VideoId, string? Note) Detect(string url)
    {
        if (string.IsNullOrWhiteSpace(url)) return (Platform.Unknown, null, null, "empty url");
        url = url.Trim();
        var lower = url.ToLowerInvariant();

        if (BareYoutubeId.IsMatch(url))
            return (Platform.Youtube, $"https://www.youtube.com/watch?v={url}", url, null);

        if (lower.Contains("youtube.com") || lower.Contains("youtu.be") || lower.Contains("youtube-nocookie.com"))
        {
            var m = YoutubeQueryId.Match(url);
            if (!m.Success) m = YoutubePathId.Match(url);
            return m.Success
                ? (Platform.Youtube, $"https://www.youtube.com/watch?v={m.Groups[1].Value}", m.Groups[1].Value, null)
                : (Platform.Unknown, null, null, "bad youtube url");
        }

        if (lower.Contains("vimeo.com"))
        {
            var (id, hash) = VimeoIdAndHash(url);
            if (id is null) return (Platform.Unknown, null, null, "bad vimeo url");
            return (Platform.Vimeo, hash is null ? $"https://vimeo.com/{id}" : $"https://vimeo.com/{id}/{hash}", id, null);
        }

        return (Platform.Unknown, null, null, "not youtube/vimeo");
    }

    /// <summary>
    /// The video id is the first numeric path segment that isn't a showcase/group/channel/album id,
    /// and unlisted videos also need their privacy hash: vimeo.com/{id}/{hash} or player.vimeo.com/video/{id}?h={hash}.
    /// </summary>
    private static (string? Id, string? Hash) VimeoIdAndHash(string url)
    {
        var absolute = url.StartsWith("//") ? "https:" + url : url.Contains("://") ? url : "https://" + url;
        if (!Uri.TryCreate(absolute, UriKind.Absolute, out var uri))
            return (null, null);
        var segments = uri.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var i = Array.FindIndex(segments, VimeoIdSegment.IsMatch);
        while (i > 0 && VimeoContainers.Contains(segments[i - 1]))
            i = Array.FindIndex(segments, i + 1, VimeoIdSegment.IsMatch);
        if (i < 0) return (null, null);
        var hash = i + 1 < segments.Length && VimeoHash.IsMatch(segments[i + 1]) ? segments[i + 1] : null;
        if (hash is null && VimeoHashQuery.Match(uri.Query) is { Success: true } q) hash = q.Groups[1].Value;
        return (segments[i], hash);
    }

    public async Task<LinkCheckResult> CheckAsync(MovieLink row, CancellationToken ct)
    {
        var (platform, canonicalUrl, videoId, parseNote) = Detect(row.Url);
        if (platform == Platform.Unknown)
            return Result(row, "unknown", "", "unknown_platform", parseNote);

        var oembed = platform == Platform.Youtube
            ? $"https://www.youtube.com/oembed?format=json&url={Uri.EscapeDataString(canonicalUrl!)}"
            : $"https://vimeo.com/api/oembed.json?url={Uri.EscapeDataString(canonicalUrl!)}";
        var name = platform.ToString().ToLowerInvariant();
        var maxRetries = Math.Max(0, options.CurrentValue.MaxRetries);

        string? lastError = null;
        for (var attempt = 0; attempt <= maxRetries; attempt++)
        {
            try
            {
                var (status, note) = await OEmbedAsync(platform, oembed, ct);
                return Result(row, name, videoId!, status, note);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested &&
                                       ex is TransientHttpException or HttpRequestException or TaskCanceledException)
            {
                lastError = ex is TaskCanceledException ? "timeout" : ex.Message;
                if (attempt < maxRetries)
                    await Task.Delay(RetryDelays[Math.Min(attempt, RetryDelays.Length - 1)], ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return Result(row, name, videoId!, "error", ex.Message);
            }
        }
        return Result(row, name, videoId!, "error", lastError);
    }

    private async Task<(string Status, string? Note)> OEmbedAsync(Platform platform, string url, CancellationToken ct)
    {
        var http = httpFactory.CreateClient(VideoLinkCheckSetup.HttpClientName);
        using var resp = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, ct);
        if (resp.IsSuccessStatusCode) return ("active", null);
        var code = (int)resp.StatusCode;
        if (resp.StatusCode == HttpStatusCode.TooManyRequests || code >= 500)
            throw new TransientHttpException($"oembed {code} {resp.StatusCode}");
        return ("unavailable", $"oembed {code} {UnavailableReason(platform, code) ?? resp.StatusCode.ToString()}");
    }

    public static string? UnavailableReason(Platform platform, int code) => (platform, code) switch
    {
        (Platform.Youtube, 401) => "embedding disabled",
        (Platform.Youtube, 403) => "private",
        (Platform.Youtube, 404) => "removed or not found",
        (Platform.Vimeo, 403) => "embedding restricted",
        (Platform.Vimeo, 404) => "private, removed or not found",
        _ => null,
    };

    private static LinkCheckResult Result(MovieLink row, string platform, string videoId, string status, string? note) =>
        new(row.Id, row.Title, row.MediaType, row.Url, platform, videoId, status, DateTime.UtcNow, note);
}
