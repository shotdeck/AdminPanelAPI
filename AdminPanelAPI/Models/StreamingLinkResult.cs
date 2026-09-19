namespace AdminPanelAPI.Models
{
    /// <summary>A single track's resolved streaming links.</summary>
    public class StreamingLinkedTrack
    {
        public long SongId { get; set; }
        public string? Title { get; set; }
        public string? Artist { get; set; }
        public string? SpotifyUrl { get; set; }
        public string? StreamingUrl { get; set; }
    }

    /// <summary>
    /// Report from a streaming-link backfill: how many of a movie's identified
    /// tracks resolved to a Spotify track (and a universal all-services link).
    /// Non-destructive: only fills links, never removes tracks.
    /// </summary>
    public class StreamingLinkResult
    {
        public int MovieId { get; set; }
        public string? MovieTitle { get; set; }
        public bool CredentialsConfigured { get; set; } = true;
        public int TotalTracks { get; set; }
        public int ResolvedSpotify { get; set; }
        public int ResolvedUniversal { get; set; }
        public int Unmatched { get; set; }
        public int Skipped { get; set; }
        public List<StreamingLinkedTrack> Linked { get; set; } = new();
    }
}
