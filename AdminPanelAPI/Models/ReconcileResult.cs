namespace AdminPanelAPI.Models
{
    /// <summary>A single identified track classified against the film's soundtrack.</summary>
    public class ReconciledTrack
    {
        public long SongId { get; set; }
        public string? Title { get; set; }
        public string? Artist { get; set; }
        public string Confidence { get; set; } = "unverified";
    }

    /// <summary>
    /// The outcome of reconciling a movie's identified tracks against its known
    /// soundtrack (from Wikipedia). Non-destructive: nothing is deleted, every
    /// occurrence is just tagged with a confidence level.
    /// </summary>
    public class ReconcileResult
    {
        public int MovieId { get; set; }
        public string? MovieTitle { get; set; }
        public int? MovieYear { get; set; }

        /// <summary>The Wikipedia article the soundtrack was read from, if found.</summary>
        public string? SourceArticle { get; set; }
        public bool SoundtrackFound { get; set; }
        public int AuthoritativeTrackCount { get; set; }

        public int ConfirmedCount { get; set; }
        public int ReviewCount { get; set; }
        public int UnverifiedCount { get; set; }

        public List<ReconciledTrack> Confirmed { get; set; } = new();
        public List<ReconciledTrack> Review { get; set; } = new();
        public List<ReconciledTrack> Unverified { get; set; } = new();

        /// <summary>Songs on the known soundtrack that were NOT identified.</summary>
        public List<string> Missing { get; set; } = new();
    }
}
