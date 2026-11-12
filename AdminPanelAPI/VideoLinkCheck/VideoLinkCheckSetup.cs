using System.Net;
using AdminPanelAPI.VideoLinkCheck.Models;
using AdminPanelAPI.VideoLinkCheck.Services;
using Microsoft.Extensions.Options;

namespace AdminPanelAPI.VideoLinkCheck;

/// <summary>Wires the weekly music video / commercial link check (/api/video-link-check, background worker, report email).</summary>
public static class VideoLinkCheckSetup
{
    public const string HttpClientName = "VideoLinkCheck";

    public static void AddVideoLinkCheck(this WebApplicationBuilder builder)
    {
        var services = builder.Services;
        services.Configure<VideoLinkCheckOptions>(builder.Configuration.GetSection("VideoLinkCheck"));
        services.AddHttpClient(HttpClientName, (sp, c) =>
            {
                var o = sp.GetRequiredService<IOptionsMonitor<VideoLinkCheckOptions>>().CurrentValue;
                c.Timeout = TimeSpan.FromSeconds(Math.Max(1, o.RequestTimeoutSeconds));
                c.DefaultRequestHeaders.UserAgent.ParseAdd("Mozilla/5.0 (LinkChecker/1.0)");
            })
            .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
            {
                AllowAutoRedirect = true,
                AutomaticDecompression = DecompressionMethods.All,
                PooledConnectionLifetime = TimeSpan.FromMinutes(10),
            });
        services.AddSingleton<VideoLinkCheckStore>();
        services.AddSingleton<VideoLinkChecker>();
        services.AddSingleton<VideoLinkCheckMailer>();
        services.AddHostedService<VideoLinkCheckWorker>();
    }
}
