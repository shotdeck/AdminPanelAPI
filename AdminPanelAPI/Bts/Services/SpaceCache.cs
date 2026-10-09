using System.Collections.Concurrent;
using AdminPanelAPI.Bts.Models;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Primitives;

namespace AdminPanelAPI.Bts.Services;

/// <summary>
/// Short-lived cache of link token → space, so each customer request doesn't
/// hit Postgres. Admin changes to a space (revoke, new link, expiry, quota)
/// evict it immediately.
/// </summary>
public sealed class SpaceCache(IMemoryCache cache)
{
    private static readonly TimeSpan CacheFor = TimeSpan.FromSeconds(30);
    private readonly ConcurrentDictionary<long, CancellationTokenSource> _signals = new();

    public bool TryGet(byte[] tokenHash, out Space? space) => cache.TryGetValue(Key(tokenHash), out space);

    public void Set(byte[] tokenHash, Space? space)
    {
        var options = new MemoryCacheEntryOptions { AbsoluteExpirationRelativeToNow = CacheFor };
        if (space is not null)
            options.AddExpirationToken(new CancellationChangeToken(Signal(space.Id).Token));
        cache.Set(Key(tokenHash), space, options);
    }

    public void Invalidate(long spaceId)
    {
        if (_signals.TryRemove(spaceId, out var cts))
        {
            cts.Cancel();
            cts.Dispose();
        }
    }

    private CancellationTokenSource Signal(long spaceId) => _signals.GetOrAdd(spaceId, _ => new CancellationTokenSource());

    private static string Key(byte[] tokenHash) => "space-token:" + Convert.ToHexString(tokenHash);
}
