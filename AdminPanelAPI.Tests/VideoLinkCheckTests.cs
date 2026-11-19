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
    [InlineData("https://www.youtube.com/watch?v=MS91knuzoOA&rco=1&ab_channel=PearljamVEVO", "MS91knuzoOA")]
    [InlineData("https://www.youtube.com/watch?app=desktop&v=SfWx2-L71dY", "SfWx2-L71dY")]
    public void Detect_Youtube(string url, string id)
    {
        var (platform, canonical, videoId, _) = Detect(url);
        Assert.Equal(Platform.Youtube, platform);
        Assert.Equal(id, videoId);
        Assert.Equal($"https://www.youtube.com/watch?v={id}", canonical);
    }

    [Theory]
    [InlineData("https://vimeo.com/123456789", "https://vimeo.com/123456789")]
    [InlineData("https://vimeo.com/1087852317?activityReferer=1", "https://vimeo.com/1087852317")]
    [InlineData("https://vimeo.com/channels/staffpicks/987654", "https://vimeo.com/987654")]
    [InlineData("https://vimeo.com/showcase/8872348/video/305495226", "https://vimeo.com/305495226")]
    [InlineData("https://vimeo.com/groups/708643/videos/850581957", "https://vimeo.com/850581957")]
    [InlineData("https://vimeo.com/882661335/03dc74d2f5", "https://vimeo.com/882661335/03dc74d2f5")]
    [InlineData("https://vimeo.com/1172123163/fd716c5967?fl=pl&fe=vl", "https://vimeo.com/1172123163/fd716c5967")]
    [InlineData("https://vimeo.com/829232989/9218dc744f?turnstile=0.0Hhl.9ixP.aebe", "https://vimeo.com/829232989/9218dc744f")]
    [InlineData("https://player.vimeo.com/video/880468217?h=c983fbb771", "https://vimeo.com/880468217/c983fbb771")]
    [InlineData("https://player.vimeo.com/video/76979871?h=abc", "https://vimeo.com/76979871")]
    [InlineData("https://vimeo.com/882661335/1234567890", "https://vimeo.com/882661335/1234567890")]
    [InlineData("https://vimeo.com/channels/1234567/987654", "https://vimeo.com/987654")]
    [InlineData("//player.vimeo.com/video/76979871", "https://vimeo.com/76979871")]
    [InlineData("vimeo.com/76979871", "https://vimeo.com/76979871")]
    public void Detect_Vimeo(string url, string expectedCanonical)
    {
        var (platform, canonical, videoId, _) = Detect(url);
        Assert.Equal(Platform.Vimeo, platform);
        Assert.Equal(expectedCanonical, canonical);
        Assert.StartsWith($"https://vimeo.com/{videoId}", canonical);
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
        Assert.Equal("id,title,media_type,url,platform,video_id,status,checked_at_iso,note,previous_status,site_embed_issue", lines[0]);
        Assert.Equal("7,\"Say \"\"Hi\"\", Bye\",music video,https://youtu.be/x,youtube,x,unavailable,2026-11-12T10:00:00.0000000Z,oembed 404,active,", lines[1]);
    }

    [Theory]
    [InlineData("https://vimeo.com/305495226")]
    [InlineData("https://vimeo.com/showcase/8872348/video/305495226")]
    [InlineData("https://vimeo.com/1087852317?activityReferer=1")]
    [InlineData("https://player.vimeo.com/video/880468217?h=c983fbb771")]
    [InlineData("https://www.youtube.com/watch?v=MS91knuzoOA&rco=1&ab_channel=PearljamVEVO")]
    [InlineData("https://youtu.be/dQw4w9WgXcQ")]
    [InlineData("https://youtu.be/dQw4w9WgXcQ?si=abc")]
    [InlineData("https://youtube.com/shorts/dQw4w9WgXcQ")]
    [InlineData("https://www.dailymotion.com/video/x7")]
    public void SiteEmbed_MatchesChecker(string url) => Assert.Null(SiteEmbed.Problem(url));

    [Theory]
    [InlineData("https://vimeo.com/882661335/03dc74d2f5", "site embeds https://player.vimeo.com/video/03dc74d2f5 (wrong id)")]
    [InlineData("https://vimeo.com/829232989/9218dc744f?turnstile=0.abc", "site embeds https://player.vimeo.com/video/9218dc744f?turnstile=0.abc (wrong id)")]
    [InlineData("https://vimeo.com/305495226/", "site embeds https://player.vimeo.com/video/ (wrong id)")]
    [InlineData("dQw4w9WgXcQ", "site shows no video for this link")]
    [InlineData("https://www.youtube.com/live/dQw4w9WgXcQ", "site shows no video for this link")]
    public void SiteEmbed_FlagsBrokenEmbeds(string url, string problem) => Assert.Equal(problem, SiteEmbed.Problem(url));

    [Fact]
    public void IssuesInclude_ActiveLinksBrokenOnSite()
    {
        var at = DateTime.UtcNow;
        var broken = new LinkCheckResult(1, "a", "music video", "https://vimeo.com/882661335/03dc74d2f5", "vimeo", "882661335", "active", at, null);
        var fine = broken with { Url = "https://vimeo.com/305495226", VideoId = "305495226" };
        Assert.True(VideoLinkCheckReport.IsIssue(broken));
        Assert.False(VideoLinkCheckReport.IsIssue(fine));
        Assert.True(VideoLinkCheckReport.IsIssue(fine with { Status = "unavailable" }));
        Assert.Null(VideoLinkCheckReport.SiteEmbedIssue(broken with { Status = "unavailable" }));
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
