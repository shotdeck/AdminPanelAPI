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
    /// Returns the track/cue listing of the movie's official soundtrack album
    /// (from the stored Spotify album) — title, artist and Spotify track link —
    /// for use both as a grounding hint and to build a shortlist of candidate
    /// cues with listen links. Empty when no soundtrack album is known or
    /// Spotify is unavailable.
    /// </summary>
    Task<IReadOnlyList<SoundtrackCue>> GetSoundtrackCuesAsync(int movieId, CancellationToken cancellationToken);
}
