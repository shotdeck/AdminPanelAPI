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
        public string? Source { get; set; }
        public string? Confidence { get; set; }
    }

    /// <summary>Basic movie metadata used for soundtrack reconciliation.</summary>
    public class MovieInfo
    {
        public int MovieId { get; set; }
        public string? Title { get; set; }
        public int? Year { get; set; }
    }

    /// <summary>A distinct identified song in a movie (for reconciliation).</summary>
    public class MovieSongRow
    {
        public long SongId { get; set; }
        public string? Title { get; set; }
        public string? Artist { get; set; }
        public string? SpotifyUrl { get; set; }
        public string? StreamingUrl { get; set; }
    }

    /// <summary>
    /// A movie that has identified music, with how many distinct songs and
    /// occurrences were found in it.
    /// </summary>
    public class MovieMusicSummary
    {
        public int MovieId { get; set; }
        public string? Title { get; set; }
        public int? Year { get; set; }
        public int TrackCount { get; set; }
        public int OccurrenceCount { get; set; }
    }

    /// <summary>
    /// The distinct artists and song titles that have identified music, used to
    /// populate the Band/Song search dropdown.
    /// </summary>
    public class MusicSearchOptions
    {
        public List<string> Artists { get; set; } = new();
        public List<string> Songs { get; set; } = new();
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
        public string? SpotifyUrl { get; set; }
        public string? StreamingUrl { get; set; }
        public string? Confidence { get; set; }
        public int OccurrenceCount { get; set; }
        public List<MusicTrackOccurrence> Occurrences { get; set; } = new();
    }
}
