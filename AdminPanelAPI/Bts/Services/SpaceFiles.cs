using System.Globalization;
using AdminPanelAPI.Bts.Models;
using Microsoft.Extensions.Caching.Memory;

namespace AdminPanelAPI.Bts.Services;

public sealed class ConflictException(string message) : Exception(message);

public sealed class TooLargeException(string message) : Exception(message);

public sealed class NotFoundException(string message) : Exception(message);

/// <summary>
/// File operations inside one space. Every path that arrives here is relative
/// to the space and goes through <see cref="PathRules"/> before the space's
/// root is put in front of it, so a caller can only ever touch its own space.
/// </summary>
public sealed class SpaceFiles
{
    public const long MultipartThresholdBytes = 100L * 1024 * 1024;
    private const long MinPartSizeBytes = 16L * 1024 * 1024;
    private const int MaxParts = 9000;
    private const int MaxListEntries = 5000;
    private const string TrashStampFormat = "yyyyMMdd'T'HHmmssfff'Z'";
    public const int MaxNoteLength = 2000;

    /// <summary>Notes on trashed items are keyed under this; real paths never start with '/'.</summary>
    private const string TrashNotePrefix = "/trash/";

    private readonly BucketClient _bucket;
    private readonly SpaceStore _store;
    private readonly IMemoryCache _cache;

    public long MaxFileBytes { get; }

    public SpaceFiles(BucketClient bucket, SpaceStore store, IMemoryCache cache, IConfiguration configuration)
    {
        _bucket = bucket;
        _store = store;
        _cache = cache;
        MaxFileBytes = configuration.GetValue("Bts:MaxFileBytes", 50L * 1024 * 1024 * 1024);
    }

    public async Task<FolderListing> ListAsync(SpaceContext ctx, string? folder, CancellationToken ct)
    {
        var rel = PathRules.Folder(folder);
        var root = ctx.Space.Root;
        var (prefixes, objects, truncated) = await _bucket.ListLevelAsync(root + rel, MaxListEntries, ct);

        if (rel.Length > 0 && prefixes.Count == 0 && objects.Count == 0)
            throw new NotFoundException("That folder does not exist.");

        var folders = prefixes
            .Select(p => p[root.Length..])
            .Select(p => new FolderEntry(PathRules.LastSegment(p), p))
            .OrderBy(f => f.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var files = objects
            .Where(o => !o.Key.EndsWith('/'))
            .Select(o =>
            {
                var path = o.Key[root.Length..];
                var name = PathRules.LastSegment(path);
                var kind = PathRules.IsImage(name) ? "image" : PathRules.IsVideo(name) ? "video" : "file";
                var url = kind == "file" ? null : _bucket.PresignGet(o.Key, BucketClient.ViewUrlLifetime, null);
                return new FileEntry(name, path, o.Size ?? 0, o.LastModified is { } m ? new DateTimeOffset(m.ToUniversalTime()) : null, kind, url);
            })
            .OrderBy(f => f.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var notes = await _store.NotesAsync(ctx.Space.Id, folders.Select(f => f.Path).Concat(files.Select(f => f.Path)).ToList(), ct);
        if (notes.Count > 0)
        {
            folders = folders.Select(f => notes.TryGetValue(f.Path, out var n) ? f with { Note = n } : f).ToList();
            files = files.Select(f => notes.TryGetValue(f.Path, out var n) ? f with { Note = n } : f).ToList();
        }

        return new FolderListing(rel, rel.Length == 0 ? null : PathRules.ParentOf(rel), folders, files, truncated);
    }

    public async Task<string> CreateFolderAsync(SpaceContext ctx, string? parent, string? name, string? ip, CancellationToken ct)
    {
        var rel = PathRules.Folder(PathRules.Folder(parent) + PathRules.Name(name));
        if (await _bucket.AnyUnderAsync(ctx.Space.Root + rel, ct))
            throw new ConflictException("A folder with that name already exists.");

        await _bucket.PutEmptyAsync(ctx.Space.Root + rel, ct);
        await _store.LogAsync(ctx.Space.Id, ctx.Actor, "create-folder", rel, null, null, ip);
        return rel;
    }

    public async Task<UploadStart> StartUploadAsync(SpaceContext ctx, UploadStartRequest req, CancellationToken ct)
    {
        var name = PathRules.Name(req.FileName);
        if (!PathRules.IsAllowedMedia(name))
            throw new ArgumentException("Only image and video files can be uploaded here.");
        if (req.SizeBytes < 0)
            throw new ArgumentException("The file size is invalid.");
        if (req.SizeBytes > MaxFileBytes)
            throw new TooLargeException($"Files are limited to {FormatBytes(MaxFileBytes)}.");

        await EnforceQuotaAsync(ctx.Space, req.SizeBytes, ct);

        var rel = PathRules.Folder(req.Folder) + name;
        var key = ctx.Space.Root + rel;
        var contentType = PathRules.ContentTypeFor(name);
        var overwrites = await _bucket.SizeAsync(key, ct) != null;

        if (req.SizeBytes <= MultipartThresholdBytes)
            return new UploadStart("single", rel, contentType, overwrites, _bucket.PresignPut(key, contentType), null, null);

        var partSize = Math.Max(MinPartSizeBytes, (req.SizeBytes + MaxParts - 1) / MaxParts);
        var uploadId = await _bucket.InitiateMultipartAsync(key, contentType, ct);
        return new UploadStart("multipart", rel, contentType, overwrites, null, uploadId, partSize);
    }

    public PartUrlsResponse PartUrls(SpaceContext ctx, PartUrlsRequest req)
    {
        var key = ctx.Space.Root + PathRules.File(req.Path);
        var uploadId = RequireUploadId(req.UploadId);
        if (req.PartNumbers.Count is 0 or > 100 || req.PartNumbers.Any(n => n is < 1 or > 10000))
            throw new ArgumentException("Ask for between 1 and 100 part numbers from 1 to 10000.");

        return new PartUrlsResponse(req.PartNumbers.Distinct()
            .Select(n => new PartUrl(n, _bucket.PresignPart(key, uploadId, n)))
            .ToList());
    }

    public async Task CompleteUploadAsync(SpaceContext ctx, CompleteRequest req, string? ip, CancellationToken ct)
    {
        var rel = PathRules.File(req.Path);
        var key = ctx.Space.Root + rel;
        var uploadId = RequireUploadId(req.UploadId);
        if (req.Parts.Count == 0)
            throw new ArgumentException("No parts were uploaded.");

        await _bucket.CompleteMultipartAsync(key, uploadId, req.Parts.Select(p => (p.PartNumber, p.ETag.Trim('"'))), ct);
        await AfterUploadAsync(ctx, rel, key, ip, ct);
    }

    /// <summary>
    /// Called by the browser once a single presigned PUT has finished: checks
    /// the size R2 actually stored and records the upload.
    /// </summary>
    public async Task ConfirmUploadAsync(SpaceContext ctx, string? path, string? ip, CancellationToken ct)
    {
        var rel = PathRules.File(path);
        await AfterUploadAsync(ctx, rel, ctx.Space.Root + rel, ip, ct);
    }

    public async Task AbortUploadAsync(SpaceContext ctx, PathRequest req, CancellationToken ct)
    {
        var key = ctx.Space.Root + PathRules.File(req.Path);
        await _bucket.AbortMultipartAsync(key, RequireUploadId(req.UploadId), ct);
    }

    public async Task<string> DownloadUrlAsync(SpaceContext ctx, string? path, bool attachment, CancellationToken ct)
    {
        var rel = PathRules.File(path);
        var key = ctx.Space.Root + rel;
        if (await _bucket.SizeAsync(key, ct) == null)
            throw new NotFoundException("That file does not exist.");
        return _bucket.PresignGet(key, BucketClient.DownloadUrlLifetime, attachment ? PathRules.LastSegment(rel) : null);
    }

    /// <summary>Renames a file, or a folder when the path ends with '/'.</summary>
    public async Task<string> RenameAsync(SpaceContext ctx, RenameRequest req, string? ip, CancellationToken ct)
    {
        var isFolder = (req.Path ?? "").TrimEnd().EndsWith('/');
        var source = isFolder ? PathRules.Folder(req.Path) : PathRules.File(req.Path);
        if (source.Length == 0)
            throw new ArgumentException("The top folder cannot be renamed.");

        var newName = PathRules.Name(req.NewName);
        var parent = PathRules.ParentOf(source);
        var target = parent + newName + (isFolder ? "/" : "");
        if (!isFolder && !PathRules.IsAllowedMedia(newName))
            throw new ArgumentException("Keep an image or video extension on the file name.");

        return await MoveInternalAsync(ctx, source, target, isFolder, "rename", ip, ct);
    }

    /// <summary>Moves a file, or a folder when the path ends with '/', into another folder.</summary>
    public async Task<string> MoveAsync(SpaceContext ctx, MoveRequest req, string? ip, CancellationToken ct)
    {
        var isFolder = (req.Path ?? "").TrimEnd().EndsWith('/');
        var source = isFolder ? PathRules.Folder(req.Path) : PathRules.File(req.Path);
        if (source.Length == 0)
            throw new ArgumentException("The top folder cannot be moved.");

        var target = PathRules.Folder(req.ToFolder) + PathRules.LastSegment(source) + (isFolder ? "/" : "");
        if (isFolder && target.StartsWith(source, StringComparison.Ordinal))
            throw new ArgumentException("A folder cannot be moved inside itself.");

        return await MoveInternalAsync(ctx, source, target, isFolder, "move", ip, ct);
    }

    /// <summary>Moves a file or folder into the space's trash; returns how many files went.</summary>
    public async Task<int> DeleteAsync(SpaceContext ctx, string? path, string? ip, CancellationToken ct)
    {
        var isFolder = (path ?? "").TrimEnd().EndsWith('/');
        var rel = isFolder ? PathRules.Folder(path) : PathRules.File(path);
        if (rel.Length == 0)
            throw new ArgumentException("The top folder cannot be deleted.");

        var trashPrefix = ctx.Space.TrashRoot + DateTime.UtcNow.ToString(TrashStampFormat, CultureInfo.InvariantCulture) + "/";
        int moved;
        if (isFolder)
        {
            moved = await _bucket.MovePrefixAsync(ctx.Space.Root + rel, trashPrefix + rel, ct);
            if (moved == 0) throw new NotFoundException("That folder does not exist.");
        }
        else
        {
            if (await _bucket.SizeAsync(ctx.Space.Root + rel, ct) == null)
                throw new NotFoundException("That file does not exist.");
            await _bucket.MoveAsync(ctx.Space.Root + rel, trashPrefix + rel, ct);
            moved = 1;
        }

        await _store.MoveNotesAsync(ctx.Space.Id, rel, TrashNotePrefix + trashPrefix[ctx.Space.TrashRoot.Length..] + rel, isFolder, ct);
        await KeepParentVisibleAsync(ctx.Space, rel, ct);
        ForgetUsage(ctx.Space);
        await _store.LogAsync(ctx.Space.Id, ctx.Actor, "delete", rel, null, null, ip);
        return moved;
    }

    public async Task<IReadOnlyList<TrashItem>> TrashAsync(Space space, CancellationToken ct)
    {
        var items = new List<TrashItem>();
        await foreach (var obj in _bucket.ListAllAsync(space.TrashRoot, ct))
        {
            if (obj.Key.EndsWith('/')) continue;
            var trashPath = obj.Key[space.TrashRoot.Length..];
            var slash = trashPath.IndexOf('/');
            if (slash < 0) continue;
            DateTimeOffset? deletedAt = DateTime.TryParseExact(trashPath[..slash], TrashStampFormat,
                CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var at)
                ? new DateTimeOffset(at, TimeSpan.Zero) : null;
            items.Add(new TrashItem(trashPath, trashPath[(slash + 1)..], deletedAt, obj.Size ?? 0));
        }
        return items.OrderByDescending(i => i.DeletedAt).ThenBy(i => i.OriginalPath).ToList();
    }

    /// <summary>Puts one trashed file back where it was. Refuses to overwrite.</summary>
    public async Task<string> RestoreAsync(SpaceContext ctx, string? trashPath, string? ip, CancellationToken ct)
    {
        var rel = PathRules.File(trashPath);
        var slash = rel.IndexOf('/');
        if (slash < 0) throw new ArgumentException("That is not a trash path.");

        var original = rel[(slash + 1)..];
        var sourceKey = ctx.Space.TrashRoot + rel;
        var targetKey = ctx.Space.Root + original;

        if (await _bucket.SizeAsync(sourceKey, ct) == null)
            throw new NotFoundException("That item is no longer in the trash.");
        if (await _bucket.SizeAsync(targetKey, ct) != null)
            throw new ConflictException("A file already exists at that path. Rename it first.");

        await _bucket.MoveAsync(sourceKey, targetKey, ct);
        await _store.MoveNotesAsync(ctx.Space.Id, TrashNotePrefix + rel, original, false, ct);
        ForgetUsage(ctx.Space);
        await _store.LogAsync(ctx.Space.Id, ctx.Actor, "restore", original, null, null, ip);
        return original;
    }

    /// <summary>Sets the note on a file, or a folder when the path ends with '/'. Blank removes it.</summary>
    public async Task<NoteResult> SetNoteAsync(SpaceContext ctx, string? path, string? note, string? ip, CancellationToken ct)
    {
        var isFolder = (path ?? "").TrimEnd().EndsWith('/');
        var rel = isFolder ? PathRules.Folder(path) : PathRules.File(path);
        if (rel.Length == 0)
            throw new ArgumentException("Notes go on a file or folder.");

        var text = (note ?? "").Replace("\r\n", "\n").Trim();
        if (text.Length > MaxNoteLength)
            throw new ArgumentException($"Notes are limited to {MaxNoteLength} characters.");
        if (text.Any(c => char.IsControl(c) && c != '\n' && c != '\t'))
            throw new ArgumentException("The note contains characters that are not allowed.");

        var exists = isFolder
            ? await _bucket.AnyUnderAsync(ctx.Space.Root + rel, ct)
            : await _bucket.SizeAsync(ctx.Space.Root + rel, ct) != null;
        if (!exists)
            throw new NotFoundException(isFolder ? "That folder does not exist." : "That file does not exist.");

        var saved = await _store.SetNoteAsync(ctx.Space.Id, rel, text.Length == 0 ? null : text, ctx.Actor, ct);
        await _store.LogAsync(ctx.Space.Id, ctx.Actor, saved is null ? "remove-note" : "note", rel, null, null, ip);
        return new NoteResult(rel, saved);
    }

    public async Task<UsageResponse> UsageAsync(Space space, CancellationToken ct)
    {
        var (bytes, files) = await MeasureAsync(space, ct);
        return new UsageResponse(bytes, files, space.QuotaBytes);
    }

    // ── Internals ──────────────────────────────────────────────

    private async Task<string> MoveInternalAsync(
        SpaceContext ctx, string source, string target, bool isFolder, string action, string? ip, CancellationToken ct)
    {
        if (source == target) return target;
        var root = ctx.Space.Root;

        if (isFolder)
        {
            if (!await _bucket.AnyUnderAsync(root + source, ct))
                throw new NotFoundException("That folder does not exist.");
            if (await _bucket.AnyUnderAsync(root + target, ct))
                throw new ConflictException("A folder with that name already exists there.");
            await _bucket.MovePrefixAsync(root + source, root + target, ct);
        }
        else
        {
            if (await _bucket.SizeAsync(root + source, ct) == null)
                throw new NotFoundException("That file does not exist.");
            if (await _bucket.SizeAsync(root + target, ct) != null)
                throw new ConflictException("A file with that name already exists there.");
            await _bucket.MoveAsync(root + source, root + target, ct);
        }

        await _store.MoveNotesAsync(ctx.Space.Id, source, target, isFolder, ct);
        await KeepParentVisibleAsync(ctx.Space, source, ct);
        await _store.LogAsync(ctx.Space.Id, ctx.Actor, action, source, target, null, ip);
        return target;
    }

    private async Task AfterUploadAsync(SpaceContext ctx, string rel, string key, string? ip, CancellationToken ct)
    {
        var size = await _bucket.SizeAsync(key, ct) ?? throw new NotFoundException("The upload did not arrive.");
        if (size > MaxFileBytes)
        {
            await _bucket.DeleteAsync(key, ct);
            throw new TooLargeException($"Files are limited to {FormatBytes(MaxFileBytes)}.");
        }

        ForgetUsage(ctx.Space);
        await _store.LogAsync(ctx.Space.Id, ctx.Actor, "upload", rel, null, size, ip);
    }

    /// <summary>
    /// Folders exist only while something is under them, so moving the last
    /// file out would make its folder vanish; leave a marker behind instead.
    /// </summary>
    private async Task KeepParentVisibleAsync(Space space, string movedPath, CancellationToken ct)
    {
        var parent = PathRules.ParentOf(movedPath);
        if (parent.Length == 0) return;
        if (!await _bucket.AnyUnderAsync(space.Root + parent, ct))
            await _bucket.PutEmptyAsync(space.Root + parent, ct);
    }

    private async Task EnforceQuotaAsync(Space space, long incoming, CancellationToken ct)
    {
        if (space.QuotaBytes is not { } quota) return;
        var (used, _) = await MeasureAsync(space, ct);
        if (used + incoming > quota)
            throw new TooLargeException(
                $"This space is limited to {FormatBytes(quota)} and has {FormatBytes(Math.Max(0, quota - used))} left.");
    }

    private async Task<(long Bytes, int Files)> MeasureAsync(Space space, CancellationToken ct)
    {
        var cacheKey = "usage:" + space.Id;
        if (_cache.TryGetValue(cacheKey, out (long, int) cached)) return cached;

        long bytes = 0;
        var files = 0;
        await foreach (var obj in _bucket.ListAllAsync(space.Root, ct))
        {
            if (obj.Key.EndsWith('/')) continue;
            bytes += obj.Size ?? 0;
            files++;
        }

        _cache.Set(cacheKey, (bytes, files), TimeSpan.FromSeconds(60));
        return (bytes, files);
    }

    private void ForgetUsage(Space space) => _cache.Remove("usage:" + space.Id);

    private static string RequireUploadId(string? uploadId)
    {
        var id = (uploadId ?? "").Trim();
        if (id.Length is 0 or > 1024) throw new ArgumentException("An upload id is required.");
        return id;
    }

    internal static string FormatBytes(long bytes)
    {
        string[] units = ["B", "KB", "MB", "GB", "TB"];
        double value = bytes;
        var unit = 0;
        while (value >= 1024 && unit < units.Length - 1)
        {
            value /= 1024;
            unit++;
        }
        return unit == 0 ? $"{bytes} B" : $"{value:0.#} {units[unit]}";
    }
}
