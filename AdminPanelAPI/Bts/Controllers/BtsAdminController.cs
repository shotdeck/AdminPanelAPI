using Microsoft.AspNetCore.Cors;
using AdminPanelAPI.Bts.Models;
using AdminPanelAPI.Bts.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.RateLimiting;

namespace AdminPanelAPI.Bts.Controllers;

[ApiController]
[Route("api/bts/admin")]
[EnableCors(BtsSetup.CorsPolicy)]
[EnableRateLimiting("bts")]
[ServiceFilter(typeof(ErrorFilter))]
public sealed class BtsAdminLoginController(SpaceStore store, Tokens tokens, IConfiguration configuration) : ControllerBase
{
    /// <summary>
    /// Admins on the shared camera movement roster sign in with the admin
    /// password (<c>Bts:AdminPassword</c>, falling back to
    /// <c>CAMERAMOVEMENTPASSWORD</c>) or their own stored password.
    /// </summary>
    [HttpPost("login")]
    [EnableRateLimiting("bts-login")]
    public async Task<IActionResult> Login([FromBody] LoginRequest req, CancellationToken ct)
    {
        var name = (req.Name ?? "").Trim();
        var password = req.Password ?? "";
        if (name.Length == 0 || password.Length == 0)
            return Unauthorized(new ErrorResponse("Enter your name and password."));

        var user = await store.FindRosterUserAsync(name, ct);
        var shared = configuration["Bts:AdminPassword"];
        if (string.IsNullOrEmpty(shared)) shared = configuration["CAMERAMOVEMENTPASSWORD"];

        var ok = user is { IsAdmin: true } &&
                 (Passwords.SameSecret(password, shared) || Passwords.Verify(password, user.PasswordHash));
        if (!ok)
            return Unauthorized(new ErrorResponse("Incorrect name or password."));

        var (token, expires) = tokens.IssueAdmin(user!.Name);
        return Ok(new LoginResponse(token, user.Name, expires));
    }
}

[ApiController]
[Route("api/bts/admin/spaces")]
[ServiceFilter(typeof(AdminFilter))]
[EnableCors(BtsSetup.CorsPolicy)]
[EnableRateLimiting("bts")]
[ServiceFilter(typeof(ErrorFilter))]
public sealed class BtsAdminSpacesController(
    SpaceStore store, SpaceFiles files, Tokens tokens, SpaceCache cache, IConfiguration configuration) : ControllerBase
{
    private string Admin => (string)HttpContext.Items[AccessKeys.Admin]!;
    private SpaceContext Context => (SpaceContext)HttpContext.Items[AccessKeys.Space]!;

    [HttpGet]
    public async Task<IEnumerable<AdminSpaceDto>> List(CancellationToken ct) =>
        (await store.ListAsync(ct)).Select(ToDto);

    [HttpPost]
    public async Task<AdminSpaceDto> Create([FromBody] SpaceRequest req, CancellationToken ct)
    {
        var (name, notes, quota, expires) = Validate(req);
        var token = Tokens.NewSpaceToken();
        var space = await store.CreateAsync(
            name, notes, quota, expires, Admin, Tokens.HashSpaceToken(token), tokens.EncryptSpaceToken(token), ct);
        await store.LogAsync(space.Id, Admin, "create-space", null, null, null, ClientIp);
        return ToDto(space);
    }

    [HttpGet("{spaceId:long}")]
    public AdminSpaceDto Get() => ToDto(Context.Space);

    [HttpPut("{spaceId:long}")]
    public async Task<AdminSpaceDto> Update([FromBody] SpaceRequest req, CancellationToken ct)
    {
        var (name, notes, quota, expires) = Validate(req);
        var space = await store.UpdateAsync(Context.Space.Id, name, notes, quota, expires, ct);
        cache.Invalidate(Context.Space.Id);
        await store.LogAsync(Context.Space.Id, Admin, "update-space", null, null, null, ClientIp);
        return ToDto(space!);
    }

    /// <summary>Issues a new link; the old one stops working within seconds.</summary>
    [HttpPost("{spaceId:long}/rotate-link")]
    public async Task<AdminSpaceDto> RotateLink(CancellationToken ct)
    {
        var token = Tokens.NewSpaceToken();
        var space = await store.SetTokenAsync(
            Context.Space.Id, Tokens.HashSpaceToken(token), tokens.EncryptSpaceToken(token), ct);
        cache.Invalidate(Context.Space.Id);
        await store.LogAsync(Context.Space.Id, Admin, "rotate-link", null, null, null, ClientIp);
        return ToDto(space!);
    }

    [HttpPost("{spaceId:long}/revoke")]
    public async Task<AdminSpaceDto> Revoke(CancellationToken ct)
    {
        var space = await store.RevokeAsync(Context.Space.Id, ct);
        cache.Invalidate(Context.Space.Id);
        await store.LogAsync(Context.Space.Id, Admin, "revoke-link", null, null, null, ClientIp);
        return ToDto(space!);
    }

    [HttpGet("{spaceId:long}/usage")]
    public Task<UsageResponse> Usage(CancellationToken ct) => files.UsageAsync(Context.Space, ct);

    [HttpGet("{spaceId:long}/activity")]
    public Task<IReadOnlyList<ActivityEntry>> Activity([FromQuery] int limit = 200, CancellationToken ct = default) =>
        store.ActivityAsync(Context.Space.Id, limit, ct);

    [HttpGet("{spaceId:long}/trash")]
    public Task<IReadOnlyList<TrashItem>> Trash(CancellationToken ct) => files.TrashAsync(Context.Space, ct);

    [HttpPost("{spaceId:long}/trash/restore")]
    public async Task<FolderEntry> Restore([FromBody] RestoreRequest req, CancellationToken ct)
    {
        var path = await files.RestoreAsync(Context, req.TrashPath, ClientIp, ct);
        return new FolderEntry(PathRules.LastSegment(path), path);
    }

    private string? ClientIp => HttpContext.Connection.RemoteIpAddress?.ToString();

    private static (string Name, string? Notes, long? Quota, DateTimeOffset? Expires) Validate(SpaceRequest req)
    {
        var name = (req.Name ?? "").Trim();
        if (name.Length is 0 or > 200)
            throw new ArgumentException("Give the space a name of up to 200 characters.");
        var notes = string.IsNullOrWhiteSpace(req.Notes) ? null : req.Notes.Trim();
        if (req.QuotaBytes is <= 0)
            throw new ArgumentException("The storage limit must be positive, or empty for none.");
        return (name, notes, req.QuotaBytes, req.ExpiresAt);
    }

    private AdminSpaceDto ToDto(Space s)
    {
        var site = (configuration["Bts:PublicSiteUrl"] ?? "").Trim().TrimEnd('/');
        if (site.Length == 0) site = $"{Request.Scheme}://{Request.Host}";
        string? link;
        try
        {
            link = $"{site}/bts/space.html#{tokens.DecryptSpaceToken(s.TokenSealed)}";
        }
        catch (System.Security.Cryptography.CryptographicException)
        {
            link = null;
        }
        return new AdminSpaceDto(s.Id, s.Name, s.Notes, s.QuotaBytes, s.ExpiresAt, s.RevokedAt, s.IsActive,
            s.CreatedBy, s.CreatedAt, s.UpdatedAt, link);
    }
}
