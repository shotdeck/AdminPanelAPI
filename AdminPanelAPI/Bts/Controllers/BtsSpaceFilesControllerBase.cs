using Microsoft.AspNetCore.RateLimiting;
using Microsoft.AspNetCore.Cors;
using AdminPanelAPI.Bts.Models;
using AdminPanelAPI.Bts.Services;
using Microsoft.AspNetCore.Mvc;

namespace AdminPanelAPI.Bts.Controllers;

/// <summary>
/// The file routes, shared by the customer (space link) and admin (signed in,
/// space named in the route) controllers. The access filter on the derived
/// controller decides the space; these actions only see paths inside it.
/// </summary>
[ApiController]
public abstract class BtsSpaceFilesControllerBase(SpaceFiles files) : ControllerBase
{
    protected SpaceContext Context => (SpaceContext)HttpContext.Items[AccessKeys.Space]!;

    protected string? ClientIp => HttpContext.Connection.RemoteIpAddress?.ToString();

    [HttpGet("list")]
    public Task<FolderListing> List([FromQuery] string? path, CancellationToken ct) =>
        files.ListAsync(Context, path, ct);

    [HttpPost("folder")]
    public async Task<FolderEntry> CreateFolder([FromBody] FolderRequest req, CancellationToken ct)
    {
        var path = await files.CreateFolderAsync(Context, req.Parent, req.Name, ClientIp, ct);
        return new FolderEntry(PathRules.LastSegment(path), path);
    }

    [HttpPost("upload/start")]
    public Task<UploadStart> StartUpload([FromBody] UploadStartRequest req, CancellationToken ct) =>
        files.StartUploadAsync(Context, req, ct);

    [HttpPost("upload/parts")]
    public PartUrlsResponse PartUrls([FromBody] PartUrlsRequest req) => files.PartUrls(Context, req);

    [HttpPost("upload/complete")]
    public async Task<IActionResult> CompleteUpload([FromBody] CompleteRequest req, CancellationToken ct)
    {
        await files.CompleteUploadAsync(Context, req, ClientIp, ct);
        return NoContent();
    }

    [HttpPost("upload/confirm")]
    public async Task<IActionResult> ConfirmUpload([FromBody] PathRequest req, CancellationToken ct)
    {
        await files.ConfirmUploadAsync(Context, req.Path, ClientIp, ct);
        return NoContent();
    }

    [HttpPost("upload/abort")]
    public async Task<IActionResult> AbortUpload([FromBody] PathRequest req, CancellationToken ct)
    {
        await files.AbortUploadAsync(Context, req, ct);
        return NoContent();
    }

    [HttpGet("download-url")]
    public async Task<DownloadUrlResponse> DownloadUrl(
        [FromQuery] string? path, [FromQuery] bool attachment = true, CancellationToken ct = default) =>
        new(await files.DownloadUrlAsync(Context, path, attachment, ct));

    [HttpPost("rename")]
    public async Task<FolderEntry> Rename([FromBody] RenameRequest req, CancellationToken ct)
    {
        var path = await files.RenameAsync(Context, req, ClientIp, ct);
        return new FolderEntry(PathRules.LastSegment(path), path);
    }

    [HttpPost("move")]
    public async Task<FolderEntry> Move([FromBody] MoveRequest req, CancellationToken ct)
    {
        var path = await files.MoveAsync(Context, req, ClientIp, ct);
        return new FolderEntry(PathRules.LastSegment(path), path);
    }

    [HttpPost("note")]
    public Task<NoteResult> Note([FromBody] NoteRequest req, CancellationToken ct) =>
        files.SetNoteAsync(Context, req.Path, req.Note, ClientIp, ct);

    [HttpPost("delete")]
    public async Task<MoveResult> Delete([FromBody] PathRequest req, CancellationToken ct) =>
        new(await files.DeleteAsync(Context, req.Path, ClientIp, ct));
}
