using Microsoft.AspNetCore.RateLimiting;
using Microsoft.AspNetCore.Cors;
using AdminPanelAPI.Bts.Models;
using AdminPanelAPI.Bts.Services;
using Microsoft.AspNetCore.Mvc;

namespace AdminPanelAPI.Bts.Controllers;

/// <summary>What the customer page needs to know about the space its link opens.</summary>
[ApiController]
[Route("api/bts/space")]
[ServiceFilter(typeof(SpaceLinkFilter))]
[DisableCors]
[EnableRateLimiting("bts")]
[ServiceFilter(typeof(ErrorFilter))]
public sealed class BtsSpaceController(SpaceFiles files) : ControllerBase
{
    [HttpGet]
    public SpaceInfo Get()
    {
        var space = ((SpaceContext)HttpContext.Items[AccessKeys.Space]!).Space;
        return new SpaceInfo(space.Name, space.ExpiresAt, space.QuotaBytes, files.MaxFileBytes, PathRules.AllowedExtensions);
    }

    [HttpGet("usage")]
    public Task<UsageResponse> Usage(CancellationToken ct) =>
        files.UsageAsync(((SpaceContext)HttpContext.Items[AccessKeys.Space]!).Space, ct);
}

[Route("api/bts/space/files")]
[ServiceFilter(typeof(SpaceLinkFilter))]
[DisableCors]
[EnableRateLimiting("bts")]
[ServiceFilter(typeof(ErrorFilter))]
public sealed class BtsCustomerFilesController(SpaceFiles files) : BtsSpaceFilesControllerBase(files);

[Route("api/bts/admin/spaces/{spaceId:long}/files")]
[ServiceFilter(typeof(AdminFilter))]
[DisableCors]
[EnableRateLimiting("bts")]
[ServiceFilter(typeof(ErrorFilter))]
public sealed class BtsAdminFilesController(SpaceFiles files) : BtsSpaceFilesControllerBase(files);
