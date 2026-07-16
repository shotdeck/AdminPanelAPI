using AdminPanelAPI.Models;

public interface IStreamingLinkService
{
    /// <summary>
    /// Resolve streaming links for a movie's identified tracks: search Spotify
    /// for each (title, artist), store the matched Spotify URL, and derive a
    /// universal all-services link (Odesli / song.link). Non-destructive: only
    /// fills links. By default tracks that already have links are skipped; pass
    /// force to re-resolve them.
    /// </summary>
    Task<StreamingLinkResult> BackfillAsync(int movieId, bool force, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the track/cue titles of the movie's official soundtrack album
    /// (from the stored Spotify album), for use as a grounding hint. Empty when
    /// no soundtrack album is known or Spotify is unavailable.
    /// </summary>
    Task<IReadOnlyList<string>> GetSoundtrackCueTitlesAsync(int movieId, CancellationToken cancellationToken);
}
