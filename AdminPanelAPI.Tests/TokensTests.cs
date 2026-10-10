using System.Security.Cryptography;
using AdminPanelAPI.Bts.Services;

namespace AdminPanelAPI.Tests.Bts;

public class TokensTests
{
    private static Tokens Make(TimeSpan? lifetime = null) =>
        new(RandomNumberGenerator.GetBytes(32), RandomNumberGenerator.GetBytes(32), lifetime ?? TimeSpan.FromHours(1));

    [Fact]
    public void Space_tokens_are_unique_and_recognised()
    {
        var a = Tokens.NewSpaceToken();
        var b = Tokens.NewSpaceToken();
        Assert.NotEqual(a, b);
        Assert.True(Tokens.LooksLikeSpaceToken(a));
        Assert.False(Tokens.LooksLikeSpaceToken("bts_short"));
        Assert.False(Tokens.LooksLikeSpaceToken(null));
    }

    [Fact]
    public void Sealed_link_round_trips()
    {
        var tokens = Make();
        var token = Tokens.NewSpaceToken();
        Assert.Equal(token, tokens.DecryptSpaceToken(tokens.EncryptSpaceToken(token)));
    }

    [Fact]
    public void Sealed_link_needs_the_same_key()
    {
        var sealedToken = Make().EncryptSpaceToken(Tokens.NewSpaceToken());
        Assert.ThrowsAny<CryptographicException>(() => Make().DecryptSpaceToken(sealedToken));
    }

    [Fact]
    public void Admin_token_validates()
    {
        var tokens = Make();
        var (token, _) = tokens.IssueAdmin("Admin");
        Assert.Equal("Admin", tokens.ValidateAdmin(token));
    }

    [Fact]
    public void Admin_token_rejects_tampering_and_other_keys()
    {
        var tokens = Make();
        var (token, _) = tokens.IssueAdmin("Admin");
        var forgedBody = Tokens.Base64Url(System.Text.Encoding.UTF8.GetBytes("{\"Name\":\"Mallory\",\"Exp\":9999999999}"));
        Assert.Null(tokens.ValidateAdmin(forgedBody + token[token.IndexOf('.')..]));
        Assert.Null(Make().ValidateAdmin(token));
        Assert.Null(tokens.ValidateAdmin("garbage"));
        Assert.Null(tokens.ValidateAdmin(Tokens.NewSpaceToken()));
    }

    [Fact]
    public void Admin_token_expires()
    {
        var tokens = Make(TimeSpan.FromSeconds(-1));
        var (token, _) = tokens.IssueAdmin("Admin");
        Assert.Null(tokens.ValidateAdmin(token));
    }

    [Fact]
    public void Zip_ticket_round_trips()
    {
        var tokens = Make();
        var (ticket, _) = tokens.IssueZipTicket(7, "Admin", "Day 1/", new[] { "Day 1/a.jpg", "Day 1/Stills/" });
        var t = tokens.ValidateZipTicket(ticket);
        Assert.NotNull(t);
        Assert.Equal(7, t!.SpaceId);
        Assert.Equal("Day 1/", t.Folder);
        Assert.Equal(new[] { "Day 1/a.jpg", "Day 1/Stills/" }, t.Paths);
    }

    [Fact]
    public void Zip_ticket_rejects_tampering_other_keys_and_admin_tokens()
    {
        var tokens = Make();
        var (ticket, _) = tokens.IssueZipTicket(7, "Admin", "", new[] { "" });
        var dot = ticket.IndexOf('.');
        var forged = Tokens.Base64Url(System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
            new Tokens.ZipTicket(8, "Admin", "", new[] { "" }, long.MaxValue))) + ticket[dot..];
        Assert.Null(tokens.ValidateZipTicket(forged));
        Assert.Null(Make().ValidateZipTicket(ticket));
        Assert.Null(tokens.ValidateAdmin(ticket));
        Assert.Null(tokens.ValidateZipTicket(tokens.IssueAdmin("Admin").Token));
        Assert.Null(tokens.ValidateZipTicket(null));
    }
}
