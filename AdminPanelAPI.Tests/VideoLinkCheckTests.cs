using AdminPanelAPI.QdrantSync.Models;
using AdminPanelAPI.VideoLinkCheck.Models;
using AdminPanelAPI.VideoLinkCheck.Services;
using static AdminPanelAPI.VideoLinkCheck.Services.VideoLinkChecker;

namespace AdminPanelAPI.Tests;

public class VideoLinkCheckTests
{
    [Theory]
    [InlineData("dQw4w9WgXcQ", "dQw4w9WgXcQ")]
    [InlineData("https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=10", "dQw4w9WgXcQ")]
    [InlineData("https://youtu.be/dQw4w9WgXcQ?si=abc", "dQw4w9WgXcQ")]
    [InlineData("https://www.youtube.com/embed/dQw4w9WgXcQ", "dQw4w9WgXcQ")]
    [InlineData("https://youtube.com/shorts/dQw4w9WgXcQ", "dQw4w9WgXcQ")]
    [InlineData(" https://m.youtube.com/watch?feature=share&v=dQw4w9WgXcQ ", "dQw4w9WgXcQ")]
    public void Detect_Youtube(string url, string id)
    {
        var (platform, canonical, videoId, _) = Detect(url);
        Assert.Equal(Platform.Youtube, platform);
        Assert.Equal(id, videoId);
        Assert.Equal($"https://www.youtube.com/watch?v={id}", canonical);
    }

    [Theory]
    [InlineData("https://vimeo.com/123456789", "123456789")]
    [InlineData("https://player.vimeo.com/video/76979871?h=abc", "76979871")]
    [InlineData("https://vimeo.com/channels/staffpicks/987654", "987654")]
    public void Detect_Vimeo(string url, string id)
    {
        var (platform, canonical, videoId, _) = Detect(url);
        Assert.Equal(Platform.Vimeo, platform);
        Assert.Equal(id, videoId);
        Assert.Equal($"https://vimeo.com/{id}", canonical);
    }

    [Theory]
    [InlineData("", "empty url")]
    [InlineData("https://www.youtube.com/channel/UCabc", "bad youtube url")]
    [InlineData("https://vimeo.com/user/abc", "bad vimeo url")]
    [InlineData("https://www.dailymotion.com/video/x7", "not youtube/vimeo")]
    public void Detect_Unknown(string url, string note)
    {
        var (platform, _, _, n) = Detect(url);
        Assert.Equal(Platform.Unknown, platform);
        Assert.Equal(note, n);
    }

    [Fact]
    public void Csv_EscapesAndKeepsColumnOrder()
    {
        var at = new DateTime(2026, 11, 12, 10, 0, 0, DateTimeKind.Utc);
        var csv = VideoLinkCheckReport.Csv(new[]
        {
            new LinkCheckResult(7, "Say \"Hi\", Bye", "music video", "https://youtu.be/x", "youtube", "x", "unavailable", at, "oembed 404", "active"),
        });
        var lines = csv.Split("\r\n");
        Assert.Equal("id,title,media_type,url,platform,video_id,status,checked_at_iso,note,previous_status", lines[0]);
        Assert.Equal("7,\"Say \"\"Hi\"\", Bye\",music video,https://youtu.be/x,youtube,x,unavailable,2026-11-12T10:00:00.0000000Z,oembed 404,active", lines[1]);
    }

    [Fact]
    public void NormalizeRecipients_SplitsDedupesAndRejectsBad()
    {
        Assert.Equal("a@x.com,b@y.com", VideoLinkCheckReport.NormalizeRecipients(" a@x.com; b@y.com,A@x.com ", out var bad));
        Assert.Null(bad);
        Assert.Null(VideoLinkCheckReport.NormalizeRecipients("  ", out bad));
        Assert.Null(bad);
        Assert.Null(VideoLinkCheckReport.NormalizeRecipients("a@x.com,not-an-email", out bad));
        Assert.Equal("not-an-email", bad);
    }

    [Fact]
    public void ResolveSmtp_FallsBackToQdrantSyncEmail_ButNotItsRecipients()
    {
        var shared = new QdrantSyncOptions.EmailOptions
        {
            SmtpHost = "smtp.gmail.com", SmtpPort = 587, Username = "colin@x.com", Password = "pw", From = "colin@x.com", To = "qdrant@x.com",
        };
        var smtp = VideoLinkCheckMailer.Resolve(new VideoLinkCheckOptions.EmailOptions { To = "team@x.com" }, shared);
        Assert.Equal(new VideoLinkCheckMailer.Smtp("smtp.gmail.com", 587, "colin@x.com", "pw", "colin@x.com", "team@x.com"), smtp);
        Assert.True(smtp.Configured);

        var own = VideoLinkCheckMailer.Resolve(new VideoLinkCheckOptions.EmailOptions { Password = "own", From = "links@x.com" }, shared);
        Assert.Equal("own", own.Password);
        Assert.Equal("links@x.com", own.From);
        Assert.Equal("", own.To);
    }

    [Fact]
    public void MediaTypeList_IsLowercasedAndTrimmed()
    {
        Assert.Equal(new[] { "music video", "commercial" }, new VideoLinkCheckOptions { MediaTypes = " Music Video , commercial,music video" }.MediaTypeList);
    }
}
