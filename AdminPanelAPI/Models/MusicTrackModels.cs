namespace AdminPanelAPI.Models
{
    /// <summary>
    /// A single occurrence of a song inside a movie (one stored segment),
    /// with enough info to play it back from R2.
    /// </summary>
    public class MusicTrackOccurrence
    {
        public int MovieId { get; set; }
        public string? MovieTitle { get; set; }
        public int? MovieYear { get; set; }
        public double StartTime { get; set; }
        public double EndTime { get; set; }
        public double? Score { get; set; }
    }

    /// <summary>
    /// A song grouped with every occurrence of it (across one or many movies).
    /// </summary>
    public class MusicTrackGroup
    {
        public long SongId { get; set; }
        public string? Title { get; set; }
        public string? Artist { get; set; }
        public string? Isrc { get; set; }
        public string? Acrid { get; set; }
        public int OccurrenceCount { get; set; }
        public List<MusicTrackOccurrence> Occurrences { get; set; } = new();
    }
}
