using System.Threading.RateLimiting;
using AdminPanelAPI.Bts.Services;
using Microsoft.AspNetCore.HttpOverrides;
using Npgsql;

namespace AdminPanelAPI.Bts;

/// <summary>
/// Wires BTS (customer behind-the-scenes media spaces) into the host. Kept in one
/// place so it can move into its own API later. BTS routes carry their own auth
/// (space link token / signed admin session), rate limits and security headers;
/// nothing here changes the rest of AdminPanelAPI.
/// </summary>
public static class BtsSetup
{
    public const string CorsPolicy = "bts";

    public static void AddBts(this WebApplicationBuilder builder)
    {
        var services = builder.Services;
        services.AddMemoryCache();

        // Only the hosted BTS pages may call /api/bts from another origin (Bts:AllowedOrigins,
        // comma-separated); the API-wide allow-any-origin policy doesn't apply to BTS routes.
        var origins = (builder.Configuration["Bts:AllowedOrigins"] ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(o => o.TrimEnd('/'))
            .ToArray();
        services.AddCors(o => o.AddPolicy(CorsPolicy, p => p
            .WithOrigins(origins)
            .WithHeaders("Authorization", "Content-Type", "Accept")
            .WithMethods("GET", "POST", "PUT", "PATCH", "DELETE")
            .SetPreflightMaxAge(TimeSpan.FromHours(1))));

        services.AddSingleton<BtsDataSource>(_ =>
        {
            var connStr = builder.Configuration["ConnectionStrings:Default"];
            if (string.IsNullOrWhiteSpace(connStr))
                throw new InvalidOperationException("ConnectionStrings:Default is not configured.");
            return new BtsDataSource(NpgsqlDataSource.Create(connStr));
        });
        services.AddSingleton(sp => sp.GetRequiredService<BtsDataSource>().Source);

        services.AddSingleton<Tokens>();
        services.AddSingleton<BucketClient>();
        services.AddSingleton<SpaceStore>();
        services.AddSingleton<SpaceFiles>();
        services.AddSingleton<SpaceCache>();
        services.AddScoped<SpaceLinkFilter>();
        services.AddScoped<AdminFilter>();
        services.AddScoped<ErrorFilter>();

        services.AddRateLimiter(o =>
        {
            o.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
            o.AddPolicy("bts", ctx => PerIp(ctx, 600, TimeSpan.FromMinutes(1)));
            o.AddPolicy("bts-login", ctx => PerIp(ctx, 10, TimeSpan.FromMinutes(5)));
        });

        // BTS request/response models share names with existing ones (PathRequest, ...).
        services.ConfigureSwaggerGen(c => c.CustomSchemaIds(t =>
            (t.Namespace?.StartsWith("AdminPanelAPI.Bts") == true ? "Bts" : "") + SchemaName(t)));
    }

    public static void UseBts(this WebApplication app)
    {
        var forwarded = new ForwardedHeadersOptions
        {
            // Azure's front end is the only way in, so trust its X-Forwarded-For.
            ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto,
            ForwardLimit = 1
        };
        forwarded.KnownNetworks.Clear();
        forwarded.KnownProxies.Clear();

        app.UseWhen(IsBts, branch =>
        {
            branch.UseForwardedHeaders(forwarded);
            branch.Use(async (ctx, next) =>
            {
                var headers = ctx.Response.Headers;
                // The space link's token lives in the URL fragment; never leak the page URL.
                headers["Referrer-Policy"] = "no-referrer";
                headers["X-Content-Type-Options"] = "nosniff";
                headers["X-Frame-Options"] = "DENY";
                headers.CacheControl = "no-store";
                await next();
            });
        });

        app.UseRateLimiter();
    }

    private static bool IsBts(HttpContext ctx) => ctx.Request.Path.StartsWithSegments("/api/bts");

    private static RateLimitPartition<string> PerIp(HttpContext ctx, int permits, TimeSpan window) =>
        RateLimitPartition.GetFixedWindowLimiter(
            ctx.Connection.RemoteIpAddress?.ToString() ?? "unknown",
            _ => new FixedWindowRateLimiterOptions { PermitLimit = permits, Window = window });

    private static string SchemaName(Type t)
    {
        if (!t.IsGenericType) return t.Name;
        var name = t.Name[..t.Name.IndexOf('`')];
        return name + "Of" + string.Join("And", t.GetGenericArguments().Select(SchemaName));
    }
}

/// <summary>BTS's own pool, so it never shares the per-request connection the rest of the API uses.</summary>
public sealed class BtsDataSource(NpgsqlDataSource source) : IAsyncDisposable
{
    public NpgsqlDataSource Source { get; } = source;
    public ValueTask DisposeAsync() => Source.DisposeAsync();
}
