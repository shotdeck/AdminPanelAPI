namespace AdminPanelAPI.Bts.Models;

public sealed record Space(
    long Id,
    string Name,
    string? Notes,
    long? QuotaBytes,
    DateTimeOffset? ExpiresAt,
    DateTimeOffset? RevokedAt,
    string CreatedBy,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    byte[] TokenSealed)
{
    public bool IsActive => RevokedAt is null && (ExpiresAt is null || ExpiresAt > DateTimeOffset.UtcNow);

    /// <summary>Where the space's files live in the bucket.</summary>
    public string Root => $"spaces/{Id}/";

    /// <summary>Where its deleted files wait until the lifecycle rule purges them.</summary>
    public string TrashRoot => $"trash/{Id}/";
}

/// <summary>Who is acting on a space: the customer holding its link, or a named admin.</summary>
public sealed record SpaceContext(Space Space, string Actor, bool IsAdmin);

public sealed record AdminUser(string Name, bool IsAdmin, string? PasswordHash);

public sealed record ActivityEntry(
    long Id, string Actor, string Action, string? Path, string? NewPath, long? Bytes, string? Ip, DateTimeOffset At);

// ── Responses ──────────────────────────────────────────────────

public sealed record ErrorResponse(string Error);

public sealed record NoteInfo(string Text, string UpdatedBy, DateTimeOffset UpdatedAt);

public sealed record NoteResult(string Path, NoteInfo? Note);

public sealed record FolderEntry(string Name, string Path, NoteInfo? Note = null);

public sealed record FileEntry(
    string Name, string Path, long SizeBytes, DateTimeOffset? LastModified, string Kind, string? Url, NoteInfo? Note = null);

public sealed record FolderListing(
    string Path, string? Parent, IReadOnlyList<FolderEntry> Folders, IReadOnlyList<FileEntry> Files, bool Truncated);

public sealed record SpaceInfo(
    string Name, DateTimeOffset? ExpiresAt, long? QuotaBytes, long MaxFileBytes, IReadOnlyCollection<string> AllowedExtensions);

public sealed record UploadStart(
    string Mode, string Path, string ContentType, bool Overwrites,
    string? Url, string? UploadId, long? PartSizeBytes);

public sealed record PartUrl(int PartNumber, string Url);

public sealed record PartUrlsResponse(IReadOnlyList<PartUrl> Parts);

public sealed record DownloadUrlResponse(string Url);

public sealed record UsageResponse(long Bytes, int Files, long? QuotaBytes);

public sealed record TrashItem(string TrashPath, string OriginalPath, DateTimeOffset? DeletedAt, long SizeBytes);

public sealed record MoveResult(int Moved);

public sealed record AdminSpaceDto(
    long Id, string Name, string? Notes, long? QuotaBytes, DateTimeOffset? ExpiresAt, DateTimeOffset? RevokedAt,
    bool Active, string CreatedBy, DateTimeOffset CreatedAt, DateTimeOffset UpdatedAt, string? Link);

public sealed record LoginResponse(string Token, string Name, DateTimeOffset ExpiresAt);

// ── Requests ───────────────────────────────────────────────────

public sealed class LoginRequest
{
    public string? Name { get; set; }
    public string? Password { get; set; }
}

public sealed class FolderRequest
{
    public string? Parent { get; set; }
    public string? Name { get; set; }
}

public sealed class UploadStartRequest
{
    public string? Folder { get; set; }
    public string? FileName { get; set; }
    public long SizeBytes { get; set; }
}

public sealed class ZipTicketRequest
{
    public string? Folder { get; set; }
    public List<string>? Paths { get; set; }
}

public sealed record ZipTicketResponse(string Ticket, DateTimeOffset ExpiresAt);

public sealed class PartUrlsRequest
{
    public string? Path { get; set; }
    public string? UploadId { get; set; }
    public List<int> PartNumbers { get; set; } = new();
}

public sealed class CompletedPart
{
    public int PartNumber { get; set; }
    public string ETag { get; set; } = "";
}

public sealed class CompleteRequest
{
    public string? Path { get; set; }
    public string? UploadId { get; set; }
    public List<CompletedPart> Parts { get; set; } = new();
}

public sealed class PathRequest
{
    public string? Path { get; set; }
    public string? UploadId { get; set; }
}

public sealed class NoteRequest
{
    public string? Path { get; set; }
    public string? Note { get; set; }
}

public sealed class RenameRequest
{
    public string? Path { get; set; }
    public string? NewName { get; set; }
}

public sealed class MoveRequest
{
    public string? Path { get; set; }
    public string? ToFolder { get; set; }
}

public sealed class SpaceRequest
{
    public string? Name { get; set; }
    public string? Notes { get; set; }
    public long? QuotaBytes { get; set; }
    public DateTimeOffset? ExpiresAt { get; set; }
}

public sealed class RestoreRequest
{
    public string? TrashPath { get; set; }
}
