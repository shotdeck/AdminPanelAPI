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
    }

    public class DialogueSearchResponse
    {
        public string Query { get; set; } = "";
        public int TotalResults { get; set; }
        public List<DialogueSearchResult> Results { get; set; } = new();
    }
}
