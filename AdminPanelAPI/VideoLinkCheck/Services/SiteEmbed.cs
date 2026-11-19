using System.Text.RegularExpressions;

namespace AdminPanelAPI.VideoLinkCheck.Services;

/// <summary>
/// The player URL the website's PHP embeddedVideoFromLink/getYoutubeId builds from a video_link, so the report can
/// flag links that are live on YouTube/Vimeo but that the site embeds with the wrong id (or not at all).
/// </summary>
public static class SiteEmbed
{
    private static readonly Regex YoutubePathId = new(@"/(embed|shorts)/([^/?]+)", RegexOptions.Compiled);

    /// <summary>The iframe src the site uses, or null when it shows no video.</summary>
    public static string? Url(string link)
    {
        if (string.IsNullOrEmpty(link)) return null;
        if (link.Contains("youtube") || link.Contains("youtu.be"))
        {
            var id = YoutubeId(link);
            return id == "" ? null : $"https://www.youtube-nocookie.com/embed/{id}?modestbranding=1";
        }
        if (link.Contains("vimeo"))
            return $"https://player.vimeo.com/video/{link[(link.LastIndexOf('/') + 1)..]}";
        return null;
    }

    /// <summary>Why the site's embed won't play the video the checker found, or null when it matches.</summary>
    public static string? Problem(string link)
    {
        link = link.Trim();
        var (platform, canonicalUrl, videoId, _) = VideoLinkChecker.Detect(link);
        if (platform == VideoLinkChecker.Platform.Unknown || string.IsNullOrEmpty(videoId)) return null;
        var src = Url(link);
        if (src == null) return "site shows no video for this link";

        if (platform == VideoLinkChecker.Platform.Vimeo)
        {
            var embedded = link[(link.LastIndexOf('/') + 1)..];
            var q = embedded.IndexOf('?');
            var embeddedId = q < 0 ? embedded : embedded[..q];
            if (embeddedId != videoId) return $"site embeds {src} (wrong id)";
            var hash = canonicalUrl!.EndsWith("/" + videoId) ? null : canonicalUrl[(canonicalUrl.LastIndexOf('/') + 1)..];
            if (hash != null && !Regex.IsMatch(embedded, $@"[?&]h={Regex.Escape(hash)}(&|$)"))
                return $"site embeds {src} without the unlisted hash";
            return null;
        }

        return YoutubeId(link) == videoId ? null : $"site embeds {src} (wrong id)";
    }

    /// <summary>PHP parse_url + getYoutubeId: youtu.be path, /embed/ or /shorts/, then ?v=.</summary>
    private static string YoutubeId(string link)
    {
        var (host, path, query) = ParseUrl(link);
        if (host.Contains("youtu.be")) return path.Trim('/');
        var m = YoutubePathId.Match(path);
        if (m.Success) return m.Groups[2].Value;
        foreach (var pair in query.Split('&'))
        {
            var eq = pair.IndexOf('=');
            if (eq > 0 && pair[..eq] == "v") return Uri.UnescapeDataString(pair[(eq + 1)..]);
        }
        return "";
    }

    /// <summary>Like PHP parse_url: no host unless the link has a scheme or starts with //.</summary>
    private static (string Host, string Path, string Query) ParseUrl(string link)
    {
        var rest = link;
        var hash = rest.IndexOf('#');
        if (hash >= 0) rest = rest[..hash];
        var qi = rest.IndexOf('?');
        var query = qi >= 0 ? rest[(qi + 1)..] : "";
        if (qi >= 0) rest = rest[..qi];

        var host = "";
        var scheme = rest.IndexOf("://", StringComparison.Ordinal);
        if (scheme >= 0) rest = rest[(scheme + 1)..];
        if (rest.StartsWith("//"))
        {
            rest = rest[2..];
            var slash = rest.IndexOf('/');
            host = slash < 0 ? rest : rest[..slash];
            rest = slash < 0 ? "" : rest[slash..];
        }
        return (host, rest, query);
    }
}
