using AdminPanelAPI.Bts.Services;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.RateLimiting;
using Npgsql;

namespace AdminPanelAPI.Bts.Controllers;

/// <summary>Temporary: reports which BTS settings are present (never their values) and whether R2 and the database are reachable.</summary>
[ApiController]
[Route("api/bts/admin/diagnostics")]
[ServiceFilter(typeof(AdminFilter))]
[EnableCors(BtsSetup.CorsPolicy)]
[EnableRateLimiting("bts")]
public sealed class BtsDiagnosticsController(IConfiguration configuration, NpgsqlDataSource db) : ControllerBase
{
    [HttpGet]
    public async Task<IActionResult> Get(CancellationToken ct)
    {
        var settings = new Dictionary<string, object>
        {
            ["Bts:AllowedOrigins"] = Plain("Bts:AllowedOrigins"),
            ["Bts:PublicSiteUrl"] = Plain("Bts:PublicSiteUrl"),
            ["Bts:R2:AccountId"] = Plain("Bts:R2:AccountId"),
            ["Bts:R2:BucketName"] = Plain("Bts:R2:BucketName"),
            ["Bts:R2:ServiceUrl"] = Plain("Bts:R2:ServiceUrl"),
            ["Bts:R2:AccessKey"] = Secret("Bts:R2:AccessKey", showPrefix: true),
            ["Bts:R2:SecretKey"] = Secret("Bts:R2:SecretKey"),
            ["Bts:AdminTokenKey"] = Key("Bts:AdminTokenKey"),
            ["Bts:LinkKey"] = Key("Bts:LinkKey"),
            ["Bts:AdminPassword"] = Secret("Bts:AdminPassword"),
        };

        return Ok(new
        {
            settings,
            r2 = await CheckR2(ct),
            database = await CheckDatabase(ct),
        });
    }

    private object Plain(string name)
    {
        var v = configuration[name];
        return new { set = !string.IsNullOrWhiteSpace(v), value = v };
    }

    private object Secret(string name, bool showPrefix = false)
    {
        var v = configuration[name] ?? "";
        var t = v.Trim();
        return new
        {
            set = t.Length > 0,
            length = v.Length,
            hasSurroundingWhitespace = v.Length != t.Length,
            prefix = showPrefix && t.Length >= 4 ? t[..4] : null,
            looksLikePlaceholder = t.StartsWith("PASTE_", StringComparison.OrdinalIgnoreCase),
        };
    }

    private object Key(string name)
    {
        var v = (configuration[name] ?? "").Trim();
        int? bytes = null;
        try { if (v.Length > 0) bytes = Convert.FromBase64String(v).Length; } catch (FormatException) { }
        return new { set = v.Length > 0, validBase64 = bytes != null, decodedBytes = bytes };
    }

    private async Task<object> CheckR2(CancellationToken ct)
    {
        try
        {
            using var bucket = new BucketClient(configuration);
            await bucket.AnyUnderAsync("spaces/", ct);
            return new { ok = true, bucket = bucket.Bucket, endpoint = bucket.Origin };
        }
        catch (Amazon.S3.AmazonS3Exception e)
        {
            return new { ok = false, error = e.Message, code = e.ErrorCode, status = (int)e.StatusCode };
        }
        catch (Exception e)
        {
            return new { ok = false, error = e.Message, code = e.GetType().Name };
        }
    }

    private async Task<object> CheckDatabase(CancellationToken ct)
    {
        try
        {
            await using var cmd = db.CreateCommand(
                "SELECT current_user, (SELECT count(*) FROM bts.spaces), (SELECT count(*) FROM bts.notes), (SELECT count(*) FROM bts.activity)");
            await using var r = await cmd.ExecuteReaderAsync(ct);
            await r.ReadAsync(ct);
            return new { ok = true, user = r.GetString(0), spaces = r.GetInt64(1), notes = r.GetInt64(2), activity = r.GetInt64(3) };
        }
        catch (Exception e)
        {
            return new { ok = false, error = e.Message };
        }
    }
}
