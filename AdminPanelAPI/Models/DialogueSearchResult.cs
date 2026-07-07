namespace AdminPanelAPI.Models
{
    public class DialogueSearchResult
    {
        public int MovieId { get; set; }
        public string? MovieTitle { get; set; }
        public string Phrase { get; set; } = "";
        public string Context { get; set; } = "";
        public double StartTime { get; set; }
        public double EndTime { get; set; }

        /// <summary>
        /// Index of the R2 segment containing the clip's first word, or null if
        /// the movie has not been segmented yet (player falls back to the full file).
        /// </summary>
        public int? SegmentIndex { get; set; }

        /// <summary>
        /// Real start offset (seconds) of that segment within the movie. The
        /// player seeks (StartTime - SegmentStart) inside the small segment file.
        /// </summary>
        public double? SegmentStart { get; set; }
    }

    public class DialogueSearchResponse
    {
        public string Query { get; set; } = "";
        public int TotalResults { get; set; }
        public List<DialogueSearchResult> Results { get; set; } = new();
    }
}
