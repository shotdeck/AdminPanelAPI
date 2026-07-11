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
}
