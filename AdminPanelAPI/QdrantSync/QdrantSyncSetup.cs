using AdminPanelAPI.QdrantSync.Models;
using AdminPanelAPI.QdrantSync.Services;
using Microsoft.Extensions.Options;

namespace AdminPanelAPI.QdrantSync;

/// <summary>Wires the Postgres → Qdrant sync (/api/qdrant-sync, nightly worker, report email).</summary>
public static class QdrantSyncSetup
{
    public const string HttpClientName = "QdrantSync";

    public static void AddQdrantSync(this WebApplicationBuilder builder)
    {
        var services = builder.Services;
        services.Configure<QdrantSyncOptions>(builder.Configuration.GetSection("QdrantSync"));
        services.AddHttpClient(HttpClientName, (sp, c) =>
        {
            var o = sp.GetRequiredService<IOptionsMonitor<QdrantSyncOptions>>().CurrentValue;
            if (!string.IsNullOrWhiteSpace(o.Url)) c.BaseAddress = new Uri(o.Url.TrimEnd('/') + "/");
            if (!string.IsNullOrWhiteSpace(o.ApiKey)) c.DefaultRequestHeaders.Add("api-key", o.ApiKey);
            c.Timeout = TimeSpan.FromMinutes(5);
        });
        services.AddSingleton<QdrantSyncStore>();
        services.AddSingleton<QdrantSyncRunner>();
        services.AddSingleton<QdrantSyncMailer>();
        services.AddHostedService<QdrantSyncWorker>();
    }
}
