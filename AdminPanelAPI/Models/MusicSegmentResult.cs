namespace AdminPanelAPI.Models
{
    /// <summary>
    /// A stored music segment for a movie (matched song or unmatched music window).
    /// </summary>
    public class MusicSegmentResult
    {
        public int MovieId { get; set; }
        public double StartTime { get; set; }
        public double EndTime { get; set; }
        public bool Matched { get; set; }
        public string? Title { get; set; }
        public string? Artist { get; set; }
        public string? RecordingId { get; set; }
        public double? Score { get; set; }
    }
}
