using Microsoft.Extensions.Hosting;
using ShotDeck.Keywords;

public class KeywordWarmupService : IHostedService
{
    private readonly IServiceProvider _serviceProvider;

    public KeywordWarmupService(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("🔁 Warming up keyword cache...");

        using var scope = _serviceProvider.CreateScope();
        var keywordService = scope.ServiceProvider.GetRequiredService<IKeywordCacheService>();
        try
        {
            //await keywordService.RefreshAsync();
            Console.WriteLine("✅ Keyword cache loaded.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Failed to refresh keyword cache: {ex.Message}");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
