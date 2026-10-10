using AdminPanelAPI.Bts.Services;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Net.Http.Headers;

namespace AdminPanelAPI.Bts.Controllers;

/// <summary>
/// Zip downloads. The browser submits a plain form here so the download goes
/// through its own download manager; the signed ticket from
/// <c>zip-ticket</c> is the only authorisation and names exactly what to include.
/// </summary>
[ApiController]
[Route("api/bts/zip")]
[EnableRateLimiting("bts")]
public sealed class BtsZipController(Tokens tokens, SpaceStore store, SpaceFiles files, ILogger<BtsZipController> logger) : ControllerBase
{
    [HttpPost]
    [Consumes("application/x-www-form-urlencoded")]
    public async Task Download([FromForm] string? ticket, CancellationToken ct)
    {
        var t = tokens.ValidateZipTicket(ticket);
        var space = t is null ? null : await store.GetAsync(t.SpaceId, ct);
        if (t is null || space is null)
        {
            // 204 leaves the page the form was submitted from as it is.
            Response.StatusCode = StatusCodes.Status204NoContent;
            return;
        }

        var name = ZipName(space.Name, t.Folder);
        Response.ContentType = "application/zip";
        Response.Headers[HeaderNames.ContentDisposition] = new ContentDispositionHeaderValue("attachment")
        {
            FileName = new string(name.Select(c => c < 128 && c != '"' ? c : '_').ToArray()),
            FileNameStar = name
        }.ToString();
        Response.Headers[HeaderNames.CacheControl] = "no-store";

        // ZipArchive only writes synchronously.
        var body = HttpContext.Features.Get<IHttpBodyControlFeature>();
        if (body is not null) body.AllowSynchronousIO = true;

        await store.LogAsync(space.Id, t.Actor, "zip-download", t.Folder.Length == 0 ? null : t.Folder, null, null,
            HttpContext.Connection.RemoteIpAddress?.ToString());
        try
        {
            await files.WriteZipAsync(space, t.Folder, t.Paths, Response.Body, ct);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception e)
        {
            logger.LogError(e, "BTS zip download failed for space {SpaceId}", space.Id);
            HttpContext.Abort();
        }
    }

    private static string ZipName(string spaceName, string folder)
    {
        var last = folder.Length == 0 ? null : PathRules.LastSegment(folder);
        var name = last is null ? spaceName : $"{spaceName} - {last}";
        var safe = new string(name.Select(c => char.IsControl(c) || "<>:\"/\\|?*".Contains(c) ? '_' : c).ToArray()).Trim(' ', '.');
        return (safe.Length == 0 ? "BTS" : safe) + ".zip";
    }
}
