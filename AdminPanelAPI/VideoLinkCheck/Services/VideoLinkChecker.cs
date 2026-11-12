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
    private static readonly Regex VimeoId = new(@"(\d{6,12})", RegexOptions.Compiled);

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
            var m = VimeoId.Match(url);
            return m.Success
                ? (Platform.Vimeo, $"https://vimeo.com/{m.Groups[1].Value}", m.Groups[1].Value, null)
                : (Platform.Unknown, null, null, "bad vimeo url");
        }

        return (Platform.Unknown, null, null, "not youtube/vimeo");
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
                var (status, note) = await OEmbedAsync(oembed, ct);
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

    private async Task<(string Status, string? Note)> OEmbedAsync(string url, CancellationToken ct)
    {
        var http = httpFactory.CreateClient(VideoLinkCheckSetup.HttpClientName);
        using var resp = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, ct);
        if (resp.IsSuccessStatusCode) return ("active", null);
        var code = (int)resp.StatusCode;
        if (resp.StatusCode == HttpStatusCode.TooManyRequests || code >= 500)
            throw new TransientHttpException($"oembed {code} {resp.StatusCode}");
        return ("unavailable", resp.StatusCode == HttpStatusCode.NotFound ? "oembed 404" : $"oembed {code} {resp.StatusCode}");
    }

    private static LinkCheckResult Result(MovieLink row, string platform, string videoId, string status, string? note) =>
        new(row.Id, row.Title, row.MediaType, row.Url, platform, videoId, status, DateTime.UtcNow, note);
}
