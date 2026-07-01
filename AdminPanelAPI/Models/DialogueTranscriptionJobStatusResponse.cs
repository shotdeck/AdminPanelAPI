namespace AdminPanelAPI.Models
{
    public class DialogueTranscriptionJobStatusResponse
    {
        public long JobId { get; set; }
        public int MovieId { get; set; }

        public string Status { get; set; } = "";
        public string? CurrentStep { get; set; }
        public int ProgressPct { get; set; }
        public int? WordCount { get; set; }

        public string? R2Key { get; set; }
        public string? R2Url { get; set; }

        public DateTime CreatedAt { get; set; }
        public DateTime? StartedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public string? Error { get; set; }
    }
}
