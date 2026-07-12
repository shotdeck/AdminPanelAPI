using AdminPanelAPI.Models;

/// <summary>
/// Resolves rich metadata for a single identified track (description,
/// writers/composers/producers, release info) from public sources, caching
/// the result so subsequent lookups are instant.
/// </summary>
public interface ITrackDetailsService
{
    Task<TrackDetails?> GetOrFetchAsync(long songId, int? movieId, bool refresh, CancellationToken cancellationToken);

    /// <summary>
    /// Save a human-authored description for a track in a specific movie and
    /// lock it so AI regeneration never overwrites it. Returns the refreshed
    /// details, or null if the song does not exist.
    /// </summary>
    Task<TrackDetails?> SaveDescriptionAsync(long songId, int movieId, string description, CancellationToken cancellationToken);

    /// <summary>
    /// Revert a track's description in a movie back to AI-generated: drops the
    /// stored (edited or cached) row and regenerates. Returns the refreshed
    /// details, or null if the song does not exist.
    /// </summary>
    Task<TrackDetails?> RevertDescriptionAsync(long songId, int movieId, CancellationToken cancellationToken);
}
