using AdminPanelAPI.Models;

/// <summary>
/// Resolves rich metadata for a single identified track (description,
/// writers/composers/producers, release info) from public sources, caching
/// the result so subsequent lookups are instant.
/// </summary>
public interface ITrackDetailsService
{
    Task<TrackDetails?> GetOrFetchAsync(long songId, int? movieId, bool refresh, CancellationToken cancellationToken);
}
