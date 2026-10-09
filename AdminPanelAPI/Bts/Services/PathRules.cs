namespace AdminPanelAPI.Bts.Services;

/// <summary>
/// Turns a path sent by the browser into a safe key fragment inside a space.
/// The browser only ever names paths relative to its space; the space's R2
/// prefix is added on the server, so nothing here may let a path climb out.
/// </summary>
public static class PathRules
{
    public const int MaxSegmentLength = 200;
    public const int MaxPathLength = 800;

    private static readonly Dictionary<string, string> MediaTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        [".jpg"] = "image/jpeg",
        [".jpeg"] = "image/jpeg",
        [".png"] = "image/png",
        [".gif"] = "image/gif",
        [".webp"] = "image/webp",
        [".heic"] = "image/heic",
        [".heif"] = "image/heif",
        [".tif"] = "image/tiff",
        [".tiff"] = "image/tiff",
        [".bmp"] = "image/bmp",
        [".avif"] = "image/avif",
        [".dng"] = "image/x-adobe-dng",
        [".cr2"] = "image/x-canon-cr2",
        [".cr3"] = "image/x-canon-cr3",
        [".nef"] = "image/x-nikon-nef",
        [".arw"] = "image/x-sony-arw",
        [".raf"] = "image/x-fuji-raf",
        [".mp4"] = "video/mp4",
        [".m4v"] = "video/x-m4v",
        [".mov"] = "video/quicktime",
        [".mkv"] = "video/x-matroska",
        [".webm"] = "video/webm",
        [".avi"] = "video/x-msvideo",
        [".mxf"] = "application/mxf",
        [".mts"] = "video/mp2t",
        [".m2ts"] = "video/mp2t",
        [".ts"] = "video/mp2t",
        [".mpg"] = "video/mpeg",
        [".mpeg"] = "video/mpeg",
        [".3gp"] = "video/3gpp",
        [".r3d"] = "application/octet-stream",
        [".braw"] = "application/octet-stream",
    };

    /// <summary>Extensions an upload may carry, for showing in the UI.</summary>
    public static IReadOnlyCollection<string> AllowedExtensions => MediaTypes.Keys;

    /// <summary>
    /// A folder path relative to the space: "" for the root, otherwise
    /// "a/b/" with a trailing slash.
    /// </summary>
    public static string Folder(string? path)
    {
        var segments = Segments(path);
        return segments.Length == 0 ? "" : string.Join('/', segments) + "/";
    }

    /// <summary>A file path relative to the space, e.g. "a/b/photo.jpg".</summary>
    public static string File(string? path)
    {
        var segments = Segments(path);
        if (segments.Length == 0)
            throw new ArgumentException("A file name is required.");
        if (path!.TrimEnd().EndsWith('/'))
            throw new ArgumentException("A file path cannot end with '/'.");
        return string.Join('/', segments);
    }

    /// <summary>A single file or folder name: no slashes.</summary>
    public static string Name(string? name)
    {
        var trimmed = (name ?? "").Trim();
        if (trimmed.Contains('/'))
            throw new ArgumentException("A name cannot contain '/'.");
        return CheckSegment(trimmed);
    }

    public static string Combine(string folder, string name) => Folder(folder) + Name(name);

    public static string ParentOf(string relative)
    {
        var trimmed = relative.TrimEnd('/');
        var slash = trimmed.LastIndexOf('/');
        return slash < 0 ? "" : trimmed[..(slash + 1)];
    }

    public static string LastSegment(string relative)
    {
        var trimmed = relative.TrimEnd('/');
        return trimmed[(trimmed.LastIndexOf('/') + 1)..];
    }

    public static bool IsAllowedMedia(string fileName) =>
        MediaTypes.ContainsKey(Path.GetExtension(fileName));

    public static string ContentTypeFor(string fileName) =>
        MediaTypes.TryGetValue(Path.GetExtension(fileName), out var type) ? type : "application/octet-stream";

    public static bool IsImage(string fileName) => ContentTypeFor(fileName).StartsWith("image/", StringComparison.Ordinal);

    public static bool IsVideo(string fileName) =>
        ContentTypeFor(fileName).StartsWith("video/", StringComparison.Ordinal) ||
        Path.GetExtension(fileName).Equals(".mxf", StringComparison.OrdinalIgnoreCase);

    private static string[] Segments(string? path)
    {
        var raw = (path ?? "").Trim();
        if (raw.Length > MaxPathLength)
            throw new ArgumentException("That path is too long.");
        if (raw.Contains('\\'))
            throw new ArgumentException("Paths use '/' as the separator.");

        var parts = raw.Split('/', StringSplitOptions.RemoveEmptyEntries);
        for (var i = 0; i < parts.Length; i++)
            parts[i] = CheckSegment(parts[i].Trim());
        return parts;
    }

    private static string CheckSegment(string segment)
    {
        if (segment.Length == 0)
            throw new ArgumentException("A name is required.");
        if (segment is "." or "..")
            throw new ArgumentException("'.' and '..' are not allowed in a path.");
        if (segment.Length > MaxSegmentLength)
            throw new ArgumentException($"Names are limited to {MaxSegmentLength} characters.");
        if (segment.Any(c => char.IsControl(c) || c == '\\'))
            throw new ArgumentException("That name contains characters that are not allowed.");
        return segment;
    }
}
