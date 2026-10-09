using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace AdminPanelAPI.Bts.Services;

/// <summary>
/// Space links and admin sessions.
/// <para>
/// A space link carries 32 random bytes. Only its SHA-256 is used to find the
/// space; a copy encrypted with <c>Bts:LinkKey</c> is kept so an admin can copy
/// the link again later, which a database dump alone cannot reverse.
/// </para>
/// <para>
/// An admin session is a small HMAC-signed claim (name + expiry) signed with
/// <c>Bts:AdminTokenKey</c>; the API checks the signature on every admin call.
/// </para>
/// </summary>
public sealed class Tokens
{
    private const string SpacePrefix = "bts_";
    private readonly byte[] _adminKey;
    private readonly byte[] _linkKey;
    private readonly TimeSpan _adminLifetime;

    public Tokens(IConfiguration configuration)
        : this(
            ReadKey(configuration, "Bts:AdminTokenKey"),
            ReadKey(configuration, "Bts:LinkKey"),
            TimeSpan.FromHours(configuration.GetValue("Bts:AdminTokenHours", 12)))
    {
    }

    internal Tokens(byte[] adminKey, byte[] linkKey, TimeSpan adminLifetime)
    {
        if (adminKey.Length < 32) throw new InvalidOperationException("Bts:AdminTokenKey must be at least 32 bytes.");
        if (linkKey.Length != 32) throw new InvalidOperationException("Bts:LinkKey must be exactly 32 bytes.");
        _adminKey = adminKey;
        _linkKey = linkKey;
        _adminLifetime = adminLifetime;
    }

    // ── Space links ────────────────────────────────────────────

    public static string NewSpaceToken() =>
        SpacePrefix + Base64Url(RandomNumberGenerator.GetBytes(32));

    public static bool LooksLikeSpaceToken(string? token) =>
        token is { Length: > 40 and < 100 } && token.StartsWith(SpacePrefix, StringComparison.Ordinal);

    public static byte[] HashSpaceToken(string token) =>
        SHA256.HashData(Encoding.UTF8.GetBytes(token));

    public byte[] EncryptSpaceToken(string token)
    {
        var nonce = RandomNumberGenerator.GetBytes(12);
        var plain = Encoding.UTF8.GetBytes(token);
        var cipher = new byte[plain.Length];
        var tag = new byte[16];
        using var aes = new AesGcm(_linkKey, 16);
        aes.Encrypt(nonce, plain, cipher, tag);
        return [.. nonce, .. tag, .. cipher];
    }

    public string DecryptSpaceToken(byte[] sealedToken)
    {
        var nonce = sealedToken.AsSpan(0, 12);
        var tag = sealedToken.AsSpan(12, 16);
        var cipher = sealedToken.AsSpan(28);
        var plain = new byte[cipher.Length];
        using var aes = new AesGcm(_linkKey, 16);
        aes.Decrypt(nonce, cipher, tag, plain);
        return Encoding.UTF8.GetString(plain);
    }

    // ── Admin sessions ─────────────────────────────────────────

    public (string Token, DateTimeOffset ExpiresAt) IssueAdmin(string name)
    {
        var expires = DateTimeOffset.UtcNow.Add(_adminLifetime);
        var payload = JsonSerializer.SerializeToUtf8Bytes(new AdminClaim(name, expires.ToUnixTimeSeconds()));
        var body = Base64Url(payload);
        return ($"{body}.{Sign(body)}", expires);
    }

    /// <summary>The admin's name, or null when the token is forged or expired.</summary>
    public string? ValidateAdmin(string? token)
    {
        if (string.IsNullOrEmpty(token)) return null;
        var dot = token.IndexOf('.');
        if (dot <= 0 || dot == token.Length - 1) return null;

        var body = token[..dot];
        var signature = token[(dot + 1)..];
        if (!CryptographicOperations.FixedTimeEquals(
                Encoding.ASCII.GetBytes(Sign(body)), Encoding.ASCII.GetBytes(signature)))
            return null;

        try
        {
            var claim = JsonSerializer.Deserialize<AdminClaim>(FromBase64Url(body));
            if (claim is null || string.IsNullOrWhiteSpace(claim.Name)) return null;
            return DateTimeOffset.UtcNow.ToUnixTimeSeconds() < claim.Exp ? claim.Name : null;
        }
        catch (Exception ex) when (ex is JsonException or FormatException)
        {
            return null;
        }
    }

    private string Sign(string body) =>
        Base64Url(HMACSHA256.HashData(_adminKey, Encoding.ASCII.GetBytes(body)));

    private sealed record AdminClaim(string Name, long Exp);

    // ── Helpers ────────────────────────────────────────────────

    private static byte[] ReadKey(IConfiguration configuration, string name)
    {
        var value = (configuration[name] ?? "").Trim();
        if (value.Length == 0)
            throw new InvalidOperationException($"{name} is not configured (base64, 32 random bytes).");
        try
        {
            return Convert.FromBase64String(value);
        }
        catch (FormatException)
        {
            throw new InvalidOperationException($"{name} must be base64.");
        }
    }

    internal static string Base64Url(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    internal static byte[] FromBase64Url(string value)
    {
        var s = value.Replace('-', '+').Replace('_', '/');
        return Convert.FromBase64String(s.PadRight(s.Length + (4 - s.Length % 4) % 4, '='));
    }
}
