using System.Security.Cryptography;
using System.Text;

namespace AdminPanelAPI.Bts.Services;

/// <summary>
/// Verifies the PBKDF2 hashes the camera movement roster stores
/// (<c>pbkdf2$&lt;iterations&gt;$&lt;saltB64&gt;$&lt;hashB64&gt;</c>), so BTS admins sign in
/// with the same password they use in the dashboard.
/// </summary>
public static class Passwords
{
    public static bool Verify(string password, string? stored)
    {
        if (string.IsNullOrEmpty(stored)) return false;
        var parts = stored.Split('$');
        if (parts.Length != 4 || parts[0] != "pbkdf2") return false;
        if (!int.TryParse(parts[1], out var iterations) || iterations < 1) return false;

        byte[] salt, expected;
        try
        {
            salt = Convert.FromBase64String(parts[2]);
            expected = Convert.FromBase64String(parts[3]);
        }
        catch (FormatException)
        {
            return false;
        }

        var actual = Rfc2898DeriveBytes.Pbkdf2(
            Encoding.UTF8.GetBytes(password), salt, iterations, HashAlgorithmName.SHA256, expected.Length);
        return CryptographicOperations.FixedTimeEquals(actual, expected);
    }

    public static bool SameSecret(string supplied, string? expected) =>
        !string.IsNullOrEmpty(expected) &&
        CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(supplied), Encoding.UTF8.GetBytes(expected));
}
